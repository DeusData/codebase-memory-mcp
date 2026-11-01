#ifndef CBM_RESOLVE_BINDING_H
#define CBM_RESOLVE_BINDING_H

#include "../cbm.h"
#include "model.h"
#include "lookup.h"

typedef struct cbm_resolve_binding_index cbm_resolve_binding_index_t;

/* One ECMAScript module binding, from `import` or a `require("literal")`
 * initializer. A NULL member names the whole module (or its default when
 * is_default). A NULL source means the specifier is not a plain literal. */
typedef struct {
    const char *local;
    const char *source;
    const char *member;
    bool is_default;
    bool is_require;
    bool type_only;
} cbm_resolve_js_import_t;

/* One exported name of an ECMAScript module, in source terms:
 *   target set:  the name is this module's own declaration
 *   source set:  it is re-exported from that module (member NULL: its namespace)
 *   name NULL:   `export * from source`
 *   whole:       `module.exports = value` / `export = value`; a whole export
 *                with is_object is the module's named exports themselves
 *   !known:      the name is exported, but its value is not a proved declaration */
typedef struct {
    const char *name;
    const char *target;
    const char *source;
    const char *member;
    unsigned kind;
    bool known;
    bool whole;
    bool is_object;
    bool default_import;
} cbm_resolve_js_export_t;
/* A Python import inside a function or under a statement. It binds in the scope
 * that executes it, unlike the top-level imports the extractor records. */
typedef struct {
    CBMImport import;
    uint32_t at, scope;
    bool annotation_only; /* under `if TYPE_CHECKING:`: no runtime binding or load */
} cbm_resolve_python_nested_import_t;

/* How a name reaches its value in one scope. A chain is an access path with
 * call arguments dropped; see python_chain. */
typedef enum {
    CBM_PY_ROOT_UNKNOWN = 0,
    CBM_PY_ROOT_UNBOUND,           /* no scope binds it: a builtin, or a star import */
    CBM_PY_ROOT_IMPORT,            /* an import binding */
    CBM_PY_ROOT_ANNOTATION_IMPORT, /* bound for annotations only */
    CBM_PY_ROOT_DECLARATION,       /* a def or class */
    CBM_PY_ROOT_MODULE_VALUE,      /* one module-level assignment */
    CBM_PY_ROOT_ASSIGNED,          /* one local assignment; chain is its value */
    CBM_PY_ROOT_ASSIGNED_MANY,     /* only plain assignments and None; chains lists them */
    CBM_PY_ROOT_TYPED,             /* one annotated binder; chain names its class */
    CBM_PY_ROOT_SELF_PARAMETER,    /* a method's first parameter: an instance of owner */
    CBM_PY_ROOT_CLASS_PARAMETER    /* classmethod first parameter: owner itself */
} cbm_resolve_python_root_kind_t;

typedef struct {
    cbm_resolve_python_root_kind_t kind;
    const char *target; /* declaration or variable QN */
    const char *owner;  /* class QN for the two parameter kinds */
    const char *chain;
    const char *const *chains;
    uint32_t chain_count;
    uint32_t chain_scope, at;
    bool value_typed; /* MODULE_VALUE: chain is an annotation, not the value */
    const cbm_resolve_python_nested_import_t *nested;
} cbm_resolve_python_root_t;

typedef enum {
    CBM_PY_MEMBER_FUNCTION = 0, /* a def in the class body */
    CBM_PY_MEMBER_VALUE,        /* anything else the class body binds */
    CBM_PY_MEMBER_INSTANCE,     /* self.x = ..., or a bare `x: T` declaration */
    CBM_PY_MEMBER_OPAQUE        /* conditionally defined: the name stops lookup */
} cbm_resolve_python_member_kind_t;

/* What a function does when used as a decorator, proven from its source. */
enum {
    CBM_PY_SHAPE_DIRECT = 1u << 0,
    CBM_PY_SHAPE_FACTORY = 1u << 1,
    /* It hands the function to another decorator, named by delegate_chain. */
    CBM_PY_SHAPE_DELEGATES = 1u << 2
};

typedef struct {
    uint32_t owner_scope, chain_scope, at;
    const char *owner, *name, *target;
    cbm_resolve_python_member_kind_t kind;
    const char *const *decorators;
    const char *return_chain, *value_chain, *type_chain;
    bool none_value;
    unsigned shape;
    const char *delegate_chain;
    bool delegate_applied;
    /* What the function returns, as access paths in its own scope. NULL when a
     * return is not followable or the function is a generator. */
    const char *const *returned;
    uint32_t returned_scope;
} cbm_resolve_python_member_t;

/* A def or class under decorators. canonical holds the imported spelling of each
 * decorator, NULL where the root is not a top-level import. */
typedef struct {
    const char *target, *name;
    uint32_t scope;
    bool is_class;
    const char *const *decorators;
    const char *const *canonical;
} cbm_resolve_python_decorated_t;

typedef struct {
    const char *qualified_name;
    uint32_t scope, header_scope;
    const char **bases; /* access paths, source order */
    uint32_t base_count;
    uint64_t subscripted; /* bit per base: written Base[...] */
    bool metaclass;       /* written metaclass=..., or a stdlib base that replaces its body */
    bool unknown_base;    /* a base expression that has no access path */
} cbm_resolve_python_class_t;

/* `from module_path import *` at module level; at is the statement's end. */
typedef struct {
    const char *module_path;
    uint32_t at;
} cbm_resolve_python_star_t;

/* Build once per source file. Borrows result/source until the arena is freed.
 * Supported syntax is deliberately explicit: Python module functions and
 * direct imports, Lua local functions, JavaScript/TypeScript modules, plus one
 * inline Vue/Svelte script with an audited inert host. An ECMAScript file is a
 * module when it has import/export, CommonJS syntax or a module extension, or
 * is TypeScript (a duplicate script-global implementation is a compile error).
 * A classic browser script has no such signal; another script can replace its
 * globals, so it remains unsupported. Lexical scopes are real, so a local name
 * blocks only the scope it is visible in. Unknown syntax, dynamic namespace
 * effects and partial parses fail closed. The source must be the raw buffer
 * used to produce result->cached_tree. */
cbm_resolve_binding_index_t *cbm_resolve_binding_build(CBMArena *arena, const CBMFileResult *result,
                                                       CBMLanguage language, const char *source,
                                                       size_t source_len);
/* As above, for a .js file its package manifest declares a module (`"type"`). */
cbm_resolve_binding_index_t *cbm_resolve_binding_build_declared_module(
    CBMArena *arena, const CBMFileResult *result, CBMLanguage language, const char *source,
    size_t source_len, bool declared_module);

/* The module binding visible at this exact occurrence, by the lexical scope of
 * its root identifier. NULL when the root is anything but such a binding. *path
 * receives the whole `a.b.c` spelling that binds, when name is only `c`, and
 * *type_position whether the occurrence names a type. */
const cbm_resolve_js_import_t *cbm_resolve_binding_js_import(const cbm_resolve_binding_index_t *,
                                                             const char *name, uint32_t start,
                                                             uint32_t end, CBMSourceOrigin origin,
                                                             const char **path,
                                                             bool *type_position);
/* `Owner.member()` or `this.member()` where the file itself assigns that member:
 * `Owner.member = function`, `Owner.prototype.member = function`, or a member of
 * the literal `const Owner = {...}`. Replaced or computed members abstain. */
cbm_resolve_result_t cbm_resolve_binding_js_member(const cbm_resolve_binding_index_t *,
                                                   const char *name, uint32_t start, uint32_t end,
                                                   CBMSourceOrigin origin);
/* This occurrence names a type, in a type annotation or as its `Ns` path root. */
bool cbm_resolve_binding_js_type_occurrence(const cbm_resolve_binding_index_t *, const char *name,
                                            uint32_t start, uint32_t end, CBMSourceOrigin origin);
/* The exact source lines of the declaration this occurrence lexically names. */
bool cbm_resolve_binding_js_declaration_lines(const cbm_resolve_binding_index_t *, const char *name,
                                              uint32_t start, uint32_t end, CBMSourceOrigin origin,
                                              uint32_t *start_line, uint32_t *end_line);
/* False when the export list cannot be trusted as this file's complete surface. */
bool cbm_resolve_binding_js_exports_complete(const cbm_resolve_binding_index_t *);
/* True when CommonJS syntax makes `require()` of this file yield module.exports. */
bool cbm_resolve_binding_js_commonjs(const cbm_resolve_binding_index_t *);
size_t cbm_resolve_binding_js_export_count(const cbm_resolve_binding_index_t *);
const cbm_resolve_js_export_t *cbm_resolve_binding_js_export_at(const cbm_resolve_binding_index_t *,
                                                                size_t);

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

/* Exact whole-tag AST classification only, not target authority. This permits
 * simple uppercase JSX names to request the ordinary lexical/import proof;
 * all scope, export, target and projection checks still apply. */
bool cbm_resolve_binding_jsx_occurrence(const cbm_resolve_binding_index_t *index, const char *name,
                                        uint32_t start, uint32_t end, CBMSourceOrigin origin);

/* Attests one exact occurrence for a decision a source adapter already made
 * under its language's rule. Adds no authority of its own: the rule, its domain
 * and the selected declaration are the adapter's, and each is recorded. */
/* An index that only attests adapter sites, for a language that also has its own
 * binding index whose completeness must not be claimed by those sites. */
cbm_resolve_binding_index_t *cbm_resolve_binding_source_index(CBMArena *arena,
                                                              const CBMFileResult *result,
                                                              CBMLanguage language,
                                                              const char *source,
                                                              size_t source_len);
cbm_resolve_result_t cbm_resolve_binding_source_site(const cbm_resolve_binding_index_t *index,
                                                     const char *name, uint32_t start, uint32_t end,
                                                     CBMSourceOrigin origin, const char *domain,
                                                     const char *target, cbm_resolve_rule_t rule,
                                                     uint32_t facts, const char *decision);

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
    /* Exact raw URI token identity for the audited extraction bridge. */
    uint32_t uri_start_byte, uri_end_byte;
    const char *prefix;
    const char *const *show;
    size_t show_count;
    const char *const *hide;
    size_t hide_count;
    bool has_show;
} cbm_resolve_source_import_t;

/* Source-audited Python absence domain: every statement that binds at module
 * level was seen, and module __getattr__ is absent. Star imports are judged by
 * the shared resolver. Names are sorted and may include NULL entries for
 * non-module scopes; copy while the index is alive. */
/* Dynamic execution/module registry access remains an unknown project effect. */
bool cbm_resolve_binding_python_effects_complete(const cbm_resolve_binding_index_t *);
/* Conservatively includes writes and unknown alias/callee escapes. */
bool cbm_resolve_binding_python_import_escaped(const cbm_resolve_binding_index_t *, const char *);
bool cbm_resolve_binding_python_namespace_closed(const cbm_resolve_binding_index_t *index);
size_t cbm_resolve_binding_python_namespace_size(const cbm_resolve_binding_index_t *index);
const char *cbm_resolve_binding_python_namespace_name(const cbm_resolve_binding_index_t *index,
                                                      size_t offset, uint32_t *at);
size_t cbm_resolve_binding_python_class_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_python_class_t *cbm_resolve_binding_python_class_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
size_t cbm_resolve_binding_python_member_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_python_member_t *cbm_resolve_binding_python_member_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
/* The access path and lexical scope of an attribute or call occurrence. */
bool cbm_resolve_binding_python_site(const cbm_resolve_binding_index_t *index, const char *name,
                                     uint32_t start, uint32_t end, CBMSourceOrigin origin,
                                     const char **chain, uint32_t *scope);
/* The class a method scope belongs to; NULL outside a method. */
const char *cbm_resolve_binding_python_method_owner(const cbm_resolve_binding_index_t *index,
                                                    uint32_t scope);
/* What `name` denotes from this scope. at != 0 refuses a binder after that byte. */
cbm_resolve_python_root_t cbm_resolve_binding_python_root(const cbm_resolve_binding_index_t *index,
                                                          const char *name, size_t length,
                                                          uint32_t scope, uint32_t at);
cbm_resolve_result_t cbm_resolve_binding_python_member(const cbm_resolve_binding_index_t *index,
                                                       const char *name, uint32_t start,
                                                       uint32_t end, CBMSourceOrigin origin,
                                                       const char *owner, const char *target,
                                                       bool class_target, bool property_read);
/* The reason on a read that runs a property: a usage of it, never a callable reference. */
#define CBM_RESOLVE_PYTHON_PROPERTY_READ "python_receiver_property_read"
size_t cbm_resolve_binding_python_decorated_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_python_decorated_t *cbm_resolve_binding_python_decorated_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
/* The decorators keep this declaration's identity: treat it as undecorated. */
void cbm_resolve_binding_python_keep_decorated(cbm_resolve_binding_index_t *index,
                                               const cbm_resolve_python_decorated_t *item);
/* The chain's root is a top-level import, and together they spell this path. */
bool cbm_resolve_binding_python_import_is(const cbm_resolve_binding_index_t *index,
                                          const char *chain, const char *canonical);
size_t cbm_resolve_binding_python_star_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_python_star_t *cbm_resolve_binding_python_star_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
/* Names bound before this byte are unknown: a star import of an unknown module
 * can rebind them. Build sets the conservative last star; the shared resolver
 * lowers it once it knows which star sources are closed. */
void cbm_resolve_binding_python_set_star_barrier(cbm_resolve_binding_index_t *index,
                                                 uint32_t barrier);
size_t cbm_resolve_binding_python_nested_count(const cbm_resolve_binding_index_t *index);
const cbm_resolve_python_nested_import_t *cbm_resolve_binding_python_nested_at(
    const cbm_resolve_binding_index_t *index, size_t offset);
/* True when this import is its scope's only binder of the name. */
bool cbm_resolve_binding_python_nested_audited(const cbm_resolve_binding_index_t *index,
                                               const cbm_resolve_python_nested_import_t *import);
/* The nested import that the exact occurrence's root name lexically resolves to. */
const cbm_resolve_python_nested_import_t *cbm_resolve_binding_python_nested_site(
    const cbm_resolve_binding_index_t *index, const char *name, uint32_t start, uint32_t end,
    CBMSourceOrigin origin);

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
