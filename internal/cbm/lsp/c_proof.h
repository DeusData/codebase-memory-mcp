#ifndef CBM_LSP_C_PROOF_H
#define CBM_LSP_C_PROOF_H

#include "go_lsp.h"

/* Source modifiers and complete direct class domains. No inherited, template,
 * macro-generated or inferred member becomes authoritative through this audit. */
bool cbm_c_collect_source_facts(CBMArena *arena, const char *source, int length, const char *module,
                                CBMLSPDef *defs, int count);

/* What a file contributes to the translation units that include it, beyond its
 * graph definitions. A prototype, an extern declaration or a macro has no graph
 * node, yet a file's declarations decide what a name means in every includer.
 * These are source facts: nothing here selects an edge target. */
typedef enum {
    CBM_C_DECL_PROTOTYPE = 0, /* a function declared without a body */
    CBM_C_DECL_MACRO,         /* a #define of this name, in any branch */
    CBM_C_DECL_EXTERN_VALUE,  /* an extern object declaration */
    CBM_C_DECL_VALUE,         /* a graph Variable: object definition or enumerator */
    CBM_C_DECL_METHOD,        /* an out-of-line or inline member definition */
    CBM_C_DECL_MEMBER_NAME,   /* any name a class body declares; type_name is the class */
} CBMCDeclKind;

enum {
    CBM_C_DECL_EXTERNAL = 1u << 0,    /* external linkage: not static, not an enumerator */
    CBM_C_DECL_C_LINKAGE = 1u << 1,   /* written under extern "C" */
    CBM_C_DECL_CONDITIONAL = 1u << 2, /* inside a preprocessor branch */
    CBM_C_DECL_NOT_PUBLIC = 1u << 3,  /* a class member under private: or protected: */
    CBM_C_DECL_PROTECTED = 1u << 4,   /* with NOT_PUBLIC: reachable from a derived class */
};

typedef struct CBMCDecl {
    const char *file;         /* the declaring file's identity (see CBMCFileFacts) */
    const char *name;         /* namespace-relative name, dotted */
    const char *target;       /* VALUE: exact graph QN. Otherwise NULL. */
    const char *type_name;    /* VALUE, EXTERN_VALUE: written base type. METHOD: owner's name */
    const CBMType *signature; /* PROTOTYPE: builtin-only audited signature, else NULL */
    int arity;                /* PROTOTYPE: fixed parameter count, -1 when not fixed */
    uint32_t flags;
    CBMCDeclKind kind;
} CBMCDecl;

typedef struct CBMCDeclLink {
    const CBMCDecl *decl;
    struct CBMCDeclLink *next;
} CBMCDeclLink;

typedef struct CBMCFileFacts {
    /* A file's identity is its File QN. A module QN will not do: `sds.h` and `sds.c`
     * share one, and an includer of the header does not see the source's statics. */
    const char *file_qn;
    const char *module_qn;
    const char *guard; /* whole-file include guard macro, or NULL */
    bool plain_header; /* a .h: included by C and C++ alike, so linkage is the includer's */
    bool cpp_syntax;   /* a namespace, class or template: this .h is C++ whatever its name */
    CBMCDecl *decls;
    int decl_count, decl_cap;
    const char **includes; /* proven direct includes, as file identities */
    int include_count, include_cap;
    const char **generated_for; /* source files declared through a generated header */
    int generated_count, generated_cap;
} CBMCFileFacts;

/* As cbm_c_collect_source_facts, also filling facts when it is not NULL. */
bool cbm_c_collect_file_facts(CBMArena *arena, const char *source, int length, const char *module,
                              CBMLanguage language, CBMLSPDef *defs, int count,
                              CBMCFileFacts *facts);

/* Each adds to facts, copying nothing: strings must outlive the facts. */
bool cbm_c_facts_add_include(CBMArena *arena, CBMCFileFacts *facts, const char *file);
bool cbm_c_facts_add_generated(CBMArena *arena, CBMCFileFacts *facts, const char *file);
/* A member definition: `size_t StringRef::find(char C) const { ... }` has its class
 * in another file, so the class alone cannot reach it. */
bool cbm_c_facts_add_method(CBMArena *arena, CBMCFileFacts *facts, const char *name,
                            const char *target, const char *owner, int arity);

/* Immutable index over every file's facts. Built with the shared registry. */
typedef struct CBMCDeclIndex CBMCDeclIndex;
const CBMCDeclIndex *cbm_c_decl_index_build(CBMArena *arena, const CBMLSPDef *defs, int count);
/* The bucket holding this name, which may also hold others: compare ->decl->name. */
const CBMCDeclLink *cbm_c_decl_lookup(const CBMCDeclIndex *index, const char *name);
const CBMCFileFacts *cbm_c_file_facts(const CBMCDeclIndex *index, const char *file);

#endif
