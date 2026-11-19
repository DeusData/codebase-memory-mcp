/*
 * type_scope.h — declared types resolved once, in the scope that wrote them.
 *
 * A definition's declared types (its return type, parameter types, field
 * types) travel to other files as source text inside CBMLSPDef. Each engine
 * used to qualify that text against the defining MODULE only, so a type the
 * defining file imported from a third file became `<defining module>.<Name>`,
 * a name nobody registered, and every call through it was lost.
 *
 * This step resolves every type name such a text mentions ONCE, in the scope
 * of the file that wrote it: the file's own module, then what its imports
 * bind. A name gets an identity only when that scope gives exactly one answer
 * and that answer is a project definition of a type label the consuming
 * engine registers. A name that is not found, is ambiguous, is shadowed by a
 * scope the step cannot see into, or is bound to anything outside the project
 * keeps no identity, and the engine keeps doing what it did before. There is
 * no project-wide name match here.
 *
 * The result is stored with the definition (CBMLSPDef.type_scope, persisted in
 * the file's LSP surface) as "spelling=QN|spelling=QN", the spelling exactly
 * as the type text writes it. Only identities the engine cannot derive itself
 * are stored: where the engine already qualifies a bare name to
 * `<module>.<spelling>`, a name the file's own module declares is left out.
 * The engines read an identity through cbm_type_scope_find (lsp/type_rep.h)
 * wherever they would otherwise qualify a type name against the module.
 *
 * An identity depends on two inputs: the writing file's own content (its type
 * texts, its imports, its module) and which project definitions are types.
 * The surface hash covers the first and leaves the identities themselves out;
 * the incremental planner declines a repair whenever the second changes
 * (pipeline_incremental.c, closure_identity_decline).
 *
 * What is language-specific is a description (cbm_type_scope_lang_t): which
 * definition labels an identity may name, how the language's imports and
 * scopes bind names, and which CBMLSPDef fields carry raw type text for that
 * language. A further language plugs in by adding its row.
 */
#ifndef CBM_PIPELINE_TYPE_SCOPE_H
#define CBM_PIPELINE_TYPE_SCOPE_H

#include "cbm.h"
#include "foundation/arena.h"
#include "foundation/hash_table.h"
#include "lsp/go_lsp.h" /* CBMLSPDef */
#include "pipeline/pipeline.h"

#include <stdbool.h>
#include <stddef.h>

/* Where the declarations a spelling can name come from, beyond the
 * definition's own enclosing scopes. */
typedef enum {
    /* The file's own module, then what its imports bind (the import map). */
    CBM_TYPE_SCOPE_MODULE_IMPORTS = 0,
    /* The definitions visible through the file's include closure (C, C++):
     * the caller's cbm_type_scope_t.visible answers a name; without it no
     * name has an identity. */
    CBM_TYPE_SCOPE_INCLUDE_CLOSURE,
    /* Lexical nesting (Ruby constants): the namespaces of this file that
     * enclose the definition, innermost first; a name none of them declares
     * has no identity. */
    CBM_TYPE_SCOPE_LEXICAL_NESTING,
} cbm_type_scope_source_t;

/* The import-map key of an open import (see open_imports). */
#define CBM_TYPE_SCOPE_OPEN_KEY "*"
/* An open import's value that starts with this byte names a package (the
 * rest), answered through cbm_type_scope_t.open_package, instead of a module
 * or a type QN. */
#define CBM_TYPE_SCOPE_OPEN_PACKAGE '\x1c'

/* How one language writes type names, and what its engine consumes. */
typedef struct {
    CBMLanguage lang;
    cbm_type_scope_source_t scope_source;
    /* A name inside quotes is a type name (Python forward references); in
     * every other language a quoted run is a literal and is skipped. */
    bool quoted_names;
    /* `::` joins the parts of a qualified name as `.` does (Rust, C++, Ruby
     * `a::B`): the spelling stays as written, its QN joins with `.`. */
    bool colon_paths;
    /* Labels of the definitions an identity may name: the type kinds the
     * engine registers and can look members up on. NULL-terminated. */
    const char *const *labels;
    /* The engine qualifies a bare name against the defining module itself
     * (Go), so an own-module identity need not be stored. Python's cross
     * registrar keeps the bare spelling: it is stored there. */
    bool engine_qualifies_own_module;
    /* An import binds a module only (Go: a package), so only a qualified
     * `pkg.T` resolves through it and a bare name never does. Without it
     * (Python) `from m import T` binds the type itself. */
    bool imports_bind_modules;
    /* Python: an annotation is evaluated in the def's enclosing scopes before
     * the module. A def nested in a function sees that function's locals,
     * which the step cannot list, so it gets no identity; a name the
     * immediately enclosing class body declares or binds (any binding:
     * CBMDefTypeFacts.class_bindings) and a PEP 695 type parameter of the
     * file get none, and neither does a name the module binds more than once
     * (cbm_type_scope_t.module_facts), nor a type whose QN the graph holds as
     * a module (a class `Thing` in pkg/a/__init__.py beside pkg/a/Thing.py:
     * their members share QNs, so no member answer is that class's). */
    bool lexical_scopes;
    /* Open (on-demand) imports (Java and Kotlin `.*`, C# `using N;`): each
     * import-map entry under CBM_TYPE_SCOPE_OPEN_KEY names a package whose
     * every type is visible (a module QN, a type QN, or a package through
     * open_package). A bare name the own module and the named imports leave
     * unbound is `<package>.<name>` in each open package: one target across
     * all of them is its identity, two or more are none. Without the flag
     * the key is an ordinary local name. */
    bool open_imports;
    /* The language's implicit external scope (java.lang, kotlin.*, the C#
     * global namespace): true when a bare name is declared there. A name the
     * own module and the named imports leave unbound and this answers true
     * for has no identity (it is the external type, or ambiguous with an open
     * import). NULL: no such scope. */
    bool (*implicit_external)(const char *name, size_t len);
    /* Member types (Java, C#, Kotlin): a bare name written in a definition
     * nested in a type names that type's member type of the name first, then
     * each enclosing type's, innermost first (`Inner` written inside `Outer`
     * is `Outer.Inner`), before the module and the imports. A member type a
     * supertype declares is not seen: past an enclosing type with a supertype
     * the answer is a possible type, not a proven one. */
    bool nested_types;
    /* Type parameters (Java): a bare name the file declares as a type
     * parameter of a class, interface, method or constructor
     * (CBMDefTypeFacts.type_params) gets no identity anywhere in the file;
     * where it is in scope it is that parameter, and the step does not track
     * each one's scope. Needs cbm_type_scope_t.file_defs. (A lexical row
     * already gives its file's type parameters none.) */
    bool type_params_shadow;
    /* Which CBMLSPDef texts hold raw type spellings for this language. Every
     * row states each one. */
    bool return_types;
    bool param_types;
    bool field_types;    /* "name:type|name:type" in field_defs */
    bool embedded_types; /* "|"-joined raw spellings in embedded_types */
} cbm_type_scope_lang_t;

/* The description for `lang`, or NULL when no engine consumes identities. */
const cbm_type_scope_lang_t *cbm_type_scope_lang(CBMLanguage lang);

/* One file's scope: the project's names, the file's module, what the file's
 * imports bind (local name -> import QN, the LSP import map), and for a row
 * with lexical_scopes the file's own definitions. */
typedef struct {
    const cbm_registry_t *reg;
    const char *module_qn;
    const char *const *imp_keys;
    const char *const *imp_vals;
    int imp_count;
    /* lexical_scopes (and type_params_shadow): the file's extraction
     * definitions, in any order (their QNs, labels and type facts); NULL with
     * def_count 0 = none. */
    const CBMDefinition *file_defs;
    int file_def_count;
    /* lexical_scopes only: the module's ordered namespace facts. Without a
     * complete record, with a star import or with a parse error the file gets
     * no identity; a name the module binds twice (two imports, an import and
     * a definition, a later assignment) gets none. */
    const CBMPyNamespaceFacts *module_facts;
    /* lexical_scopes only: the graph the import map reads (its nodes'
     * labels), so a QN that is a module's gets no identity; NULL = not
     * checked. */
    const struct cbm_gbuf *graph;
    /* CBM_TYPE_SCOPE_INCLUDE_CLOSURE only: the one definition named `name`
     * (len bytes) the file sees through its include closure, or NULL (none,
     * several, or the closure is incomplete). */
    const char *(*visible)(void *ud, const char *name, size_t len);
    void *visible_ud;
    /* open_imports with CBM_TYPE_SCOPE_OPEN_PACKAGE values: the one QN
     * `<module>.<dotted>` a module of `package` defines, or NULL; *two when
     * two or more modules of it do (then the name has no identity). NULL:
     * such a value answers nothing. */
    const char *(*open_package)(const void *ud, const char *package, const char *dotted, bool *two);
    const void *open_package_ud;
} cbm_type_scope_t;

/* Per-file working state: the import map by key, the file's own scopes, and
 * a memo of the spellings already resolved. Build once per file, free after
 * the file's defs. */
typedef struct cbm_type_scope_file cbm_type_scope_file_t;

cbm_type_scope_file_t *cbm_type_scope_file_begin(const cbm_type_scope_lang_t *lang,
                                                 const cbm_type_scope_t *scope);
void cbm_type_scope_file_end(cbm_type_scope_file_t *file);

/* The identity of one spelling (`T`, `pkg.T`) in the file's module and import
 * scope, or NULL (the def's own enclosing scopes are applied by the attach).
 * The returned QN is owned by the file state (valid until file_end). Never a
 * short-name guess. */
const char *cbm_type_scope_resolve(cbm_type_scope_file_t *file, const char *spelling, size_t len);

/* Set def->type_scope from the def's type texts (per the description), or
 * leave it NULL when no name the def writes needs an identity. The string is
 * allocated in `arena`. */
void cbm_type_scope_attach(cbm_type_scope_file_t *file, CBMArena *arena, CBMLSPDef *def);

/* The calling file's own identity table: each local name the file's named
 * imports bind, with the type identity it resolves to in the file's scope
 * (one answer of a label the engine registers, as for a declared type), in
 * import-map order, each local once. An engine resolving a bare name at a
 * call site reads it instead of an import's unverified target. The arrays
 * and strings are owned by the file state (valid until file_end). Returns
 * the count (0: none). */
int cbm_type_scope_file_imports(cbm_type_scope_file_t *file, const char *const **out_locals,
                                const char *const **out_qns);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Work counter for the growth tests: one per byte of type text scanned, per
 * hash probe and per enclosing-scope step. Test builds only. */
uint64_t cbm_type_scope_test_work(void);
#endif

#endif /* CBM_PIPELINE_TYPE_SCOPE_H */
