#ifndef CBM_LSP_OO_INTERNAL_H
#define CBM_LSP_OO_INTERNAL_H

#include "oo_strict.h"
#include "../resolve/types.h"
#include "foundation/hash_table.h"
#include <stdint.h>
#include "../resolve/syntax_index.h"
#include "lsp_name_rows.h"

/* Shared model of the Java, Kotlin and C# adapters. Identity of a type is its
 * language-level qualified name; graph identity is carried separately because
 * graph QNs are path-derived. */

enum {
    OO_F_STATIC = 1u << 0,
    OO_F_PRIVATE = 1u << 1,
    OO_F_CTOR = 1u << 2,
    OO_F_OVERRIDE = 1u << 3,
    OO_F_ABSTRACT = 1u << 4,
    OO_F_PARTIAL = 1u << 5,
    OO_F_GENERATED = 1u << 6,
    OO_F_INTERFACE = 1u << 7,
    OO_F_OBJECT = 1u << 8, /* Kotlin object: the name denotes the instance */
    OO_F_ENUM = 1u << 9,
    OO_F_AMBIGUOUS = 1u << 10, /* Two declarations claim this qualified name. */
    OO_F_COMPANION = 1u << 11,
    OO_F_INCOMPLETE = 1u << 12, /* Syntax errors hid part of this declaration. */
    OO_F_PROPERTY_GET = 1u << 13,
    OO_F_INNER = 1u << 14,
    OO_F_DELEGATE = 1u << 15, /* C#: its Invoke is the type's one callable shape. */
    OO_F_CONVERTS = 1u << 16, /* C#: declares a user-defined implicit conversion. */
    OO_F_VALUE = 1u << 18,    /* C#: a struct. Its object members come through ValueType. */
    OO_F_DATA = 1u << 17      /* Kotlin data class: its primary constructor gives componentN. */
};

typedef struct oo_typeref {
    const char *name;               /* Dotted, no type arguments. NULL: unsupported syntax. */
    const struct oo_typeref **args; /* NULL-terminated type arguments, or NULL. */
    int dims;                       /* Array dimensions. */
    bool function;                  /* Kotlin function type: args are its parameters. */
    const struct oo_typeref *fn_receiver;
    const struct oo_typeref *fn_return;
    bool by_ref;       /* C# `ref` or `out` parameter. */
    bool ref_optional; /* C# `in` or `ref readonly`: takes `ref` and a plain argument. */
    const struct oo_typeref *lower; /* Java `? super X`: reads as nothing, takes an X. */
} oo_typeref_t;

typedef struct {
    const char *name;
    const oo_typeref_t *bound;
} oo_tparam_t;

typedef struct {
    const char *local; /* NULL for a star import. */
    const char *path;  /* Dotted path; for star imports the part before ".*". */
    const char *ns;    /* C#: namespace the directive is written in ("" = file). */
    bool star, statik;
} oo_import_t;

typedef struct oo_file {
    const char *rel, *package;
    CBMLanguage language;
    oo_import_t *imports;
    int import_count;
    CBMNameRows import_rows; /* explicit imports by local name, in a long list */
    bool has_error;
    bool reference;      /* C#: a reference assembly's source. */
    bool test_source;    /* Its project's tests: nothing outside that project reads them. */
    const char *project; /* The build project this file compiles in, or NULL. */
} oo_file_t;

typedef struct oo_type oo_type_t;

typedef struct {
    const oo_typeref_t *ref;
    const oo_file_t *file; /* Spelling resolves in this declaration's file. */
    const char *ns;        /* C#: namespace of this declaration. */
    const oo_type_t *resolved;
    bool external;
} oo_super_t;

struct oo_type {
    const char *qn, *name;
    const char *graph_qn; /* A partial type's node for the whole type; see oo_index_type. */
    int graph_count;
    const char *graph_rel; /* The file of graph_qn. */
    const oo_file_t *file;
    const char *ns;
    const oo_type_t *owner;
    uint32_t flags;
    oo_tparam_t *tparams;
    int tparam_count;
    oo_super_t *supers;
    int super_count;
    bool supers_complete;
    const oo_typeref_t *alias_of;
    const oo_type_t *companion; /* Kotlin companion object. */
    oo_type_t *next_in_index;
    const struct oo_callable *abstract_method; /* The last one declared here, for a lambda. */
    int abstract_count;
    /* A name several declarations claim. The index holds one OO_F_AMBIGUOUS entry for the name;
     * `variants` lists the declarations, each with its own members. A variant's qn is unique. */
    const oo_type_t *variants;
    const oo_type_t *next_variant;
    const struct oo_callable *primary; /* Kotlin primary constructor. */
};

typedef struct oo_exprsum oo_exprsum_t;

typedef struct oo_callable {
    const char *graph_qn, *name, *ns;
    const oo_type_t *owner;
    const oo_file_t *file;
    uint32_t flags;
    int min_args, max_args; /* max_args -1: variadic. */
    int vararg_position;    /* Kotlin: 1 + the vararg's index; it need not be last. */
    const oo_typeref_t **params;
    const char **param_names; /* Kotlin and C#: for a named argument. */
    int param_count;
    const oo_typeref_t *ret;
    const oo_exprsum_t *ret_expr; /* Kotlin expression body without an annotation. */
    const oo_typeref_t *receiver; /* Extension receiver. */
    oo_tparam_t *tparams;
    int tparam_count;
    struct oo_callable *next;
} oo_callable_t;

typedef struct oo_field {
    const char *graph_qn, *name, *ns;
    const oo_type_t *owner;
    const oo_file_t *file;
    uint32_t flags;
    const oo_typeref_t *ref;
    const oo_exprsum_t *init; /* Kotlin property without an annotation. */
    const oo_typeref_t *receiver;
    oo_tparam_t *tparams;
    int tparam_count;
    struct oo_field *next;
} oo_field_t;

/* The receiver-free shape of an initializer or expression body, enough to
 * derive a declared type lazily in the declaring scope. */
typedef enum {
    OO_SUM_NAME,  /* Bare name, possibly a constructor or function call. */
    OO_SUM_MEMBER /* Member of the previous step. */
} oo_sum_kind_t;

typedef struct oo_sumstep {
    const char *name;
    int argc; /* -1: not a call. */
    oo_sum_kind_t kind;
    const oo_typeref_t **type_args;
} oo_sumstep_t;

struct oo_exprsum {
    oo_sumstep_t *steps;
    int count;
};

struct cbm_oo_index {
    CBMArena *arena;
    CBMHashTable *types;      /* qn -> oo_type_t */
    CBMHashTable *methods;    /* qn \x1f name -> oo_callable_t list */
    CBMHashTable *fields;     /* qn \x1f name -> oo_field_t list */
    CBMHashTable *functions;  /* ns \x1f name -> oo_callable_t list (top-level, extensions) */
    CBMHashTable *props;      /* ns \x1f name -> oo_field_t list */
    CBMHashTable *ext_static; /* C#: ns \x1f name -> extension methods */
    oo_type_t *all_types;
    bool sealed, failed;
    bool reference_files; /* C#: the files being added are reference assemblies. */
    const char *project;  /* The build project of the files being added. */
    CBMHashTable *files;  /* rel -> oo_file_t, for a file with a project. */
    bool has_variants;
    int variant_serial; /* Keys each later declaration of a several-claimed name. */
};

/* Members an anonymous object declares itself: they have no index entry and
 * hide whatever its supertype offers under the name. */
typedef struct {
    const char **names;
    int count;
    struct oo_value *values; /* A property's type as its declaration gave it; unknown otherwise. */
} oo_hidden_t;

typedef struct oo_value {
    const oo_type_t *type;
    const char *external; /* Known name with no project declaration. */
    const struct oo_value *args;
    int arg_count;
    int dims;
    bool classref;             /* Denotes the type itself rather than an instance. */
    bool null_literal;         /* C#: `null`, which reaches any reference parameter. */
    bool default_literal;      /* C#: `default`, which reaches any parameter. */
    const char *argument_name; /* C#: `name: value`, which lands on the parameter it names. */
    bool function_literal;     /* An argument spelled as a lambda: its own type is not read. */
    bool small_literal;        /* An integer literal: it fits a narrower or wider parameter. */
    bool by_ref;               /* C#: passed with `ref` or `out`. */
    int lambda_parameters; /* A function literal's parameter count plus one; 0 when not known. */
    const oo_hidden_t *hidden;
    /* Kotlin function value. */
    const struct oo_value *fn_receiver;
    const struct oo_value *fn_return;
    const oo_typeref_t *fn_ref;
    const struct oo_value *lower; /* `? super X`: unknown as a value, X as a lambda parameter. */
} oo_value_t;

typedef enum {
    OO_NAME_UNKNOWN = 0,
    OO_NAME_TYPE,
    OO_NAME_TPARAM,
    OO_NAME_EXTERNAL,
    OO_NAME_AMBIGUOUS
} oo_name_status_t;

typedef struct {
    oo_name_status_t status;
    const oo_type_t *type;
    const oo_tparam_t *tparam;
    const char *external;
    bool via_import;
} oo_name_t;

/* A name resolves through one declaration's lexical surroundings. */
typedef struct {
    const oo_file_t *file;
    const char *ns;
    const oo_type_t *owner;
    const oo_tparam_t *tparams;
    int tparam_count;
} oo_scope_t;

typedef struct oo_subst {
    const oo_tparam_t *tparams;
    int count;
    const oo_value_t *args;
    int arg_count;
    const struct oo_subst *next;
} oo_subst_t;

typedef enum {
    OO_SEL_NONE = 0,   /* The complete domain has no such member. */
    OO_SEL_INCOMPLETE, /* Nothing found, but an unknown supertype could declare it. */
    OO_SEL_MISMATCH,   /* The name exists; no declaration fits this call. */
    OO_SEL_UNIQUE,
    OO_SEL_AMBIGUOUS
} oo_select_status_t;

typedef struct {
    oo_select_status_t status;
    const oo_callable_t *callable;
    oo_subst_t *subst;
    bool overload_set; /* Several declarations remain, all on one graph node. */
} oo_select_t;

typedef struct oo_local {
    const char *name;
    oo_value_t value;
    const oo_callable_t *callable_alias;
    struct oo_local *next;
    struct oo_local *shadowed; /* the binding of this name it hides, until its frame ends */
} oo_local_t;

typedef struct oo_frame {
    oo_local_t *locals;
    struct oo_frame *parent;
} oo_frame_t;

/* One implicit receiver, innermost first. */
typedef struct oo_receiver {
    oo_value_t value;
    bool unknown;           /* A receiver exists whose member set cannot be listed. */
    bool anonymous;         /* Own members have no index entry. */
    bool object_super;      /* A supertype of the anonymous object being walked. */
    const char **own_names; /* Anonymous body member names. */
    int own_count;
    const bool *own_function;          /* Kotlin: each own name is a function, not a property. */
    const struct oo_value *own_values; /* Kotlin: what each own property holds, once walked. */
    bool static_context;
    bool extension;
    struct oo_receiver *parent;
} oo_receiver_t;

typedef struct oo_carrier {
    const char *caller, *spelling;
    struct oo_carrier *next;
    bool may_be_call_reference;
} oo_carrier_t;

typedef struct {
    const cbm_oo_index_t *ix;
    CBMArena *arena;
    const char *source;
    size_t length;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    CBMResolveTypeMap types;
    const oo_file_t *f;
    const char *rel_path, *file_qn, *dir_module_scope;
    CBMHashTable *calls, *usages, *rws, *type_refs;
    CBMHashTable *locals; /* name -> its innermost live binding; a body can bind thousands */
    oo_frame_t *frame;
    oo_receiver_t *receiver;
    oo_scope_t scope;
    int depth;
    size_t visits;
    bool failed;
} oo_walk_t;

/* Index construction. */
/* Chains the file's explicit imports, before anyone reads the file. */
void oo_file_chain_imports(CBMArena *arena, oo_file_t *file);
/* The next import worth a look for this name, after row (-1 starts): those of the name in a
 * chained list, every one in a short list, so a caller still compares names. */
int oo_import_after(const oo_file_t *file, const char *name, int row);
const oo_file_t *oo_index_file(cbm_oo_index_t *ix, CBMLanguage lang, const char *rel,
                               const char *package, oo_import_t *imports, int import_count,
                               bool has_error);
oo_type_t *oo_index_type(cbm_oo_index_t *ix, const char *qn, const char *name, const oo_file_t *f,
                         const char *ns, const oo_type_t *owner, const char *graph_qn,
                         uint32_t flags);
void oo_type_add_super(cbm_oo_index_t *ix, oo_type_t *type, const oo_typeref_t *ref,
                       const oo_file_t *f, const char *ns);
void oo_index_method(cbm_oo_index_t *ix, const oo_type_t *owner, oo_callable_t *callable);
void oo_index_field(cbm_oo_index_t *ix, const oo_type_t *owner, oo_field_t *field);
void oo_index_function(cbm_oo_index_t *ix, const char *ns, oo_callable_t *callable);
void oo_index_prop(cbm_oo_index_t *ix, const char *ns, oo_field_t *field);
void oo_index_ext_static(cbm_oo_index_t *ix, const char *ns, oo_callable_t *callable);

/* A reference assembly declares this type's name, and an implementation has it:
 * the declaration in `file` adds nothing to the type. */
bool oo_type_is_implemented_elsewhere(const oo_type_t *type, const oo_file_t *file);

const oo_type_t *oo_lookup_type(const cbm_oo_index_t *ix, const char *qn);
/* The declaration this file is lexically inside of, when `qn` has several. */
const oo_type_t *oo_lookup_declared(const cbm_oo_index_t *ix, const char *qn, const char *rel);
/* The type a dotted name means from this file: one declaration of a name several claim is meant
 * when it is in the file itself, or is the only one in the file's project. Otherwise the entry
 * comes back with OO_F_AMBIGUOUS. */
const oo_type_t *oo_lookup_visible(const cbm_oo_index_t *ix, const char *qn, const oo_file_t *file);
const oo_callable_t *oo_lookup_methods(const cbm_oo_index_t *ix, const char *qn, const char *name);
const oo_field_t *oo_lookup_fields(const cbm_oo_index_t *ix, const char *qn, const char *name);
const oo_callable_t *oo_lookup_functions(const cbm_oo_index_t *ix, const char *ns,
                                         const char *name);
const oo_field_t *oo_lookup_props(const cbm_oo_index_t *ix, const char *ns, const char *name);
const oo_callable_t *oo_lookup_ext_static(const cbm_oo_index_t *ix, const char *ns,
                                          const char *name);

/* Per-file definition lookup while harvesting: extraction's exact node names. */
typedef struct {
    CBMHashTable *by_line;
    CBMArena scratch; /* the table's keys and definition copies, gone with the harvest */
} oo_defs_t;
bool oo_defs_init(oo_defs_t *defs, CBMArena *arena, const CBMFileResult *file);
void oo_defs_free(oo_defs_t *defs);
/* kind: 'T' type, 'C' callable, 'F' field or variable. */
const CBMDefinition *oo_defs_find(const oo_defs_t *defs, char kind, uint32_t line,
                                  const char *name);

/* Names. */
oo_name_t oo_resolve_name(const cbm_oo_index_t *ix, const oo_scope_t *scope, const char *name);

/* Values. */
oo_value_t oo_value_unknown(void);
bool oo_value_known(const oo_value_t *value);
oo_value_t oo_value_of_type(const oo_type_t *type);
oo_value_t oo_value_external(const char *name);
oo_value_t oo_value_from_ref(oo_walk_t *w, const oo_typeref_t *ref, const oo_scope_t *scope,
                             const oo_subst_t *subst);
oo_scope_t oo_scope_of_callable(const oo_callable_t *callable);
oo_scope_t oo_scope_of_field(const oo_field_t *field);
oo_scope_t oo_scope_of_type(const oo_type_t *type);
oo_subst_t *oo_subst_of(oo_walk_t *w, const oo_value_t *value);
bool oo_is_subtype(const cbm_oo_index_t *ix, const oo_type_t *type, const oo_type_t *super);
/* An upper-case initial and at least one lower-case letter. */
bool oo_type_shaped(const char *name);
/* A member of a well-known external supertype whose member list is closed. A
 * free function of this name cannot be told apart from the inherited member. */
bool oo_closed_member(const oo_walk_t *w, const char *name);
/* A type parameter's name mentions the parameter, never its bound. */
bool oo_names_tparam(const oo_walk_t *w, const char *name);
/* The element a collection-like value yields, or unknown. */
oo_value_t oo_element_of(oo_walk_t *w, const oo_value_t *value);
/* The declared result of a standard collection accessor, or unknown. */
oo_value_t oo_external_member_return(oo_walk_t *w, const oo_value_t *value, const char *name);
/* The parameters a lambda takes for a standard collection method on a known element type: how many,
 * or -1 when the method is not known to take one here. `position` is the lambda's argument index.
 */
int oo_stdlib_lambda_parameters(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                int position, int argc, oo_value_t *out, int capacity);
/* The same collection with these element types: `map` on a stream or an optional. */
oo_value_t oo_external_with_element(oo_walk_t *w, const oo_value_t *value, oo_value_t element);
/* `Optional.of(x)`, `List.of(a, b)`, `Arrays.stream(items)`: what a JDK factory holds is spelled by
 * its arguments. Arguments that disagree settle nothing: that is a least upper bound. */
oo_value_t oo_jdk_factory_result(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                 const oo_value_t *args, int argc);
/* A lambda's parameters when the target is a project interface with one abstract method, or a C#
 * delegate: how many, or -1 when it has no such single shape. */
int oo_functional_parameters(oo_walk_t *w, const oo_value_t *function, oo_value_t *out,
                             int capacity);
/* Follows a type's aliases to the declared type. */
const oo_type_t *oo_unalias(const cbm_oo_index_t *ix, const oo_type_t *type);

/* Static member selection over a receiver's whole known hierarchy. */
/* An error directly among a declaration's members, or in its head, can hide a
 * member. One inside a member's body cannot. */
bool oo_members_broken(TSNode declaration);
typedef bool (*oo_extension_accepts_fn)(oo_walk_t *w, const oo_callable_t *callable,
                                        const oo_value_t *value);
/* A member extension of `dispatch`'s hierarchy whose receiver takes `extended`. */
oo_select_t oo_select_member_extension(oo_walk_t *w, const oo_value_t *dispatch,
                                       const oo_value_t *extended, const char *name, int argc,
                                       const oo_value_t *args, oo_extension_accepts_fn accepts);
oo_select_t oo_select_method(oo_walk_t *w, const oo_value_t *receiver, const char *name, int argc,
                             const oo_value_t *args, bool static_only);
oo_select_t oo_select_constructor(oo_walk_t *w, const oo_value_t *receiver, int argc,
                                  const oo_value_t *args);
/* Any member of this name at all in the known hierarchy. */
bool oo_has_method_named(oo_walk_t *w, const oo_value_t *receiver, const char *name);
const oo_field_t *oo_find_field(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                oo_subst_t **subst, bool *incomplete);
const oo_type_t *oo_find_nested(const cbm_oo_index_t *ix, const oo_type_t *type, const char *name);
bool oo_hierarchy_complete(const cbm_oo_index_t *ix, const oo_type_t *type);
/* Overload choice among free candidates (imports, packages, extensions). */
oo_select_t oo_select_from_list(oo_walk_t *w, const oo_callable_t *list, int argc,
                                const oo_value_t *args, const oo_value_t *receiver);
bool oo_callable_visible(const oo_walk_t *w, const oo_callable_t *callable);
oo_value_t oo_callable_return(oo_walk_t *w, const oo_callable_t *callable, const oo_subst_t *subst);
/* `subst` plus the callee's own type parameters that one argument (or the receiver of an
 * extension) settles. Two arguments that disagree settle nothing: that is a least upper bound. */
/* The parameter called `name`, or -1. */
int oo_parameter_named(const oo_callable_t *callable, const char *name);
/* `ref` names one of the callee's type parameters that nothing settled: its value is only a bound,
 * which says what the type at least has, not what it lacks. */
bool oo_ref_unsettled(const oo_callable_t *callee, const oo_typeref_t *ref,
                      const oo_subst_t *subst);
const oo_subst_t *oo_infer_call_subst(oo_walk_t *w, const oo_callable_t *callee,
                                      const oo_subst_t *subst, const oo_value_t *receiver,
                                      const oo_value_t *args, int argc);

/* Walk state. */
bool oo_walk_init(oo_walk_t *w, const cbm_oo_index_t *ix, CBMArena *arena, const char *source,
                  int source_len, const CBMFileResult *file, CBMResolvedCallArray *out);
void oo_walk_dispose(oo_walk_t *w);
bool oo_budget(oo_walk_t *w);
void oo_frame_push(oo_walk_t *w, oo_frame_t *frame);
void oo_frame_pop(oo_walk_t *w);
void oo_bind(oo_walk_t *w, const char *name, oo_value_t value);
const oo_local_t *oo_lookup_local(const oo_walk_t *w, const char *name);
const char *oo_text(const oo_walk_t *w, TSNode node);
oo_receiver_t *oo_receiver_push(oo_walk_t *w, oo_value_t value, bool static_context);
void oo_receiver_pop(oo_walk_t *w);

/* Occurrence rows. The carrier supplies the extracted caller and spelling. */
void oo_emit_call(oo_walk_t *w, TSNode site, const char *target, cbm_resolve_rule_t rule,
                  uint32_t extra_facts, const char *decision);
void oo_emit_usage(oo_walk_t *w, TSNode site, const char *target, CBMResolvedKind kind,
                   cbm_resolve_rule_t rule, const char *decision);
void oo_emit_type_ref(oo_walk_t *w, TSNode site, const char *target, const char *decision);
void oo_emit_rw(oo_walk_t *w, TSNode site, const char *target, const char *decision);

/* Language harvest and walk. */
bool oo_java_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                     const char *source, size_t length, TSNode root);
void oo_java_resolve(oo_walk_t *w, TSNode root);
bool oo_kotlin_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                       const char *source, size_t length, TSNode root);
void oo_kotlin_resolve(oo_walk_t *w, TSNode root);
bool oo_csharp_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                       const char *source, size_t length, TSNode root);
void oo_csharp_resolve(oo_walk_t *w, TSNode root);

#endif
