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
    CBM_C_DECL_PROTOTYPE = 0,  /* a function declared without a body */
    CBM_C_DECL_MACRO,          /* a #define of this name, in any branch */
    CBM_C_DECL_EXTERN_VALUE,   /* an extern object declaration */
    CBM_C_DECL_VALUE,          /* a graph Variable: object definition or enumerator */
    CBM_C_DECL_METHOD,         /* an out-of-line or inline member definition */
    CBM_C_DECL_MEMBER_NAME,    /* any name a class body declares; type_name is the class */
    CBM_C_DECL_SPECIALIZATION, /* `template <> class Name<int>`: name is the template's */
    CBM_C_DECL_CLASS_TEMPLATE, /* `template <class T> class Name`: a base of this name can depend */
    CBM_C_DECL_TEMPLATE_PARAMETER, /* `template <class Value> class Pair`: name is Value, type_name
                                    * is Pair. What it means comes with each instantiation. */
    CBM_C_DECL_UNNAMED_TYPEDEF,    /* `typedef struct {...} Name;`: no tag is called Name */
    CBM_C_DECL_FORWARD_CLASS,      /* `class Name;` in a namespace or at file scope: name is dotted
                                    * from the module, and it is the C++ class of that name */
    CBM_C_DECL_ENUMERATOR,         /* `enum E { A };`: name is A, type_name is E, target its QN */
} CBMCDeclKind;

enum {
    CBM_C_DECL_EXTERNAL = 1u << 0,    /* external linkage: not static, not an enumerator */
    CBM_C_DECL_C_LINKAGE = 1u << 1,   /* written under extern "C" */
    CBM_C_DECL_CONDITIONAL = 1u << 2, /* inside a preprocessor branch */
    CBM_C_DECL_NOT_PUBLIC = 1u << 3,  /* a class member under private: or protected: */
    CBM_C_DECL_PROTECTED = 1u << 4,   /* with NOT_PUBLIC: reachable from a derived class */
};

/* One place a macro hands a parameter on: `callee(..., param, ...)` in its replacement list.
 * Whether callee is itself a macro depends on the translation unit using this one. */
typedef struct CBMCMacroHandoff {
    const char *callee;
    uint8_t param, position;
} CBMCMacroHandoff;

/* `RM_ ## name` in a replacement: with a bare name as the argument, the expansion's identifier. */
typedef struct CBMCMacroPaste {
    const char *prefix, *suffix;
    uint8_t param;
} CBMCMacroPaste;

/* `list_for_each_entry(obj, &patch->obj_list, node)` in a replacement: a bare word handed to a
 * callee. What it names depends on that callee, which may read it as a member. */
typedef struct CBMCMacroNameArgument {
    const char *name, *callee;
    uint8_t position;
} CBMCMacroNameArgument;

/* `(l)->len` and `L->ci->top` in a replacement: what it reads through a parameter, dotted. */
typedef struct CBMCMacroMember {
    const char *path;
    uint8_t param;
} CBMCMacroMember;

/* What a function-like macro's replacement list does with each parameter, by the C rules for
 * substitution. A parameter next to `#` or `##` is tokens, a parameter after `.` or `->` names a
 * member, and one after a type is what the macro declares. Anything else is the argument, so a
 * name written there means what that name means at the use. */
typedef struct CBMCMacroShape {
    uint32_t plain;    /* bit i: parameter i only ever appears as the argument itself */
    uint32_t declared; /* bit i: the replacement declares parameter i as a variable */
    uint8_t param_count;
    bool variadic;    /* the last parameter takes every further argument */
    bool object_like; /* `#define NAME replacement`: no parameter list, used without parentheses */
    const CBMCMacroHandoff *handoffs;
    int handoff_count;
    /* Names the replacement declares itself, which would capture an argument of that name. */
    const char **locals;
    int local_count;
    /* For each bit of declared, in bit order: the base type written, and how many `*` follow. */
    const char **declared_types;
    const uint8_t *declared_pointers;
    /* Names the replacement itself writes: called, or read as values. */
    const char **calls;
    int call_count;
    const char **values;
    int value_count;
    const CBMCMacroPaste *pastes;
    int paste_count;
    const CBMCMacroMember *members;
    int member_count;
    const CBMCMacroNameArgument *name_arguments;
    int name_argument_count;
    /* Words a member is read from, which are objects whatever types share their spelling. */
    const char **objects;
    int object_count;
    /* `#define w_p_list w_onebuf_opt.wo_list`: an object-like replacement that is nothing but
     * dotted names, which can stand after `->` and name members there. */
    const char *field_path;
    /* `#define hl_table ((HlGroup *)(ga.ga_data))`, `#define SDS_HDR(T, s) ((struct sdshdr##T
     * *)...)`: a replacement that is one cast as a whole writes the type of what it gives. */
    const char *cast_type;
    uint8_t cast_pointers;
    bool cast_pastes; /* the type's name is cast_type, then the argument cast_param */
    uint8_t cast_param;
} CBMCMacroShape;

typedef struct CBMCDecl {
    const char *file;         /* the declaring file's identity (see CBMCFileFacts) */
    const char *name;         /* namespace-relative name, dotted */
    const char *target;       /* VALUE, or MEMBER_NAME of an enumerator: exact graph QN. */
    const char *type_name;    /* VALUE, EXTERN_VALUE: written base type. METHOD: owner's name */
    const CBMType *signature; /* PROTOTYPE: builtin-only audited signature, else NULL */
    const CBMCMacroShape *macro; /* MACRO: what its replacement names, or NULL when nothing */
    int arity;                /* PROTOTYPE: fixed parameter count, -1 when not fixed */
    uint32_t flags;
    /* The innermost `#if` around this declaration and which of its branches holds it. Non-zero
     * only inside a conditional; two declarations of one group and different branches exclude
     * each other. */
    uint32_t branch_group, branch;
    /* MACRO in the first branch of `#ifdef X` or `#if defined(X)`: X. */
    const char *needs_macro;
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
    bool source_file;  /* .c, .cc, .cpp or .cxx: a build may compile it on its own */
    CBMCDecl *decls;
    int decl_count, decl_cap;
    const char **includes; /* proven direct includes, as file identities */
    int include_count, include_cap;
    const char **generated_for; /* source files declared through a generated header */
    int generated_count, generated_cap;
    const char **quoted_includes; /* every `#include "x"`, as spelled */
    int quoted_include_count, quoted_include_cap;
    bool has_unknown_include; /* one of them has nothing in the index behind it */
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
/* Does this text hold a `#define` directive? Most function bodies do not. */
bool cbm_c_text_defines_macro(const char *text, uint32_t length);
/* The one project file that includes this one, when exactly one does, else NULL. A fragment with
 * one includer reaches a compiler only through it. */
const CBMCFileFacts *cbm_c_sole_includer(const CBMCDeclIndex *index, const char *file);

#endif
