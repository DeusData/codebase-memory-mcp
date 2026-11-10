/*
 * doc_links_cs.c — C# cref resolution for doc_links.h.
 *
 * The project-wide index is built from every C# file's doc-link scope and
 * every MSBuild project file's (internal/cbm/doclink_cs.c). Nothing is read
 * from the disk. Graph nodes are only looked up by qualified name, never by
 * line, so the closure-repair route (whose unchanged nodes are line-less
 * proxies) resolves exactly as a full build does.
 *
 * Assemblies. The index does not know which files a project compiles; it
 * goes by where a file stands. A project is a directory that holds a
 * *.csproj -- one whose SDK compiles something: a project file of the
 * NoTargets or Traversal SDK only runs build steps, and is the project of no
 * file. A file belongs to the nearest project at or above it. An
 * assembly is named by its project file: projects whose project files have
 * the same name are one assembly (a reference source beside its
 * implementation, the flavours of one library). A directory that holds
 * several project files is an assembly of its own: which of them a file
 * there is compiled into is not known. A file no project file stands above
 * is of a shared tree -- the largest directory around it with no project
 * file in or below it. A shared tree belongs to no assembly: the projects
 * that include its files compile them. (A repository without any project
 * file is one tree.)
 *
 * Entities. A type entity is (scope, name, generic arity, owner): the scope
 * is its namespace, or its outer type's entity, so `Outer.Inner` and
 * `Outer<T>.Inner` are two types. The owner of a top-level type is its
 * assembly: the declarations of one full name in one assembly are one type
 * with several declarations (its parts, a stub beside the implementation,
 * one per flavour), and declarations in two assemblies are two types, which
 * see nothing of each other. The shared trees' declarations have no
 * assembly. A complete declaration there is a type of its tree. The parts of
 * a `partial` type there are one entity whatever tree they stand in, and
 * they are parts of every assembly's type of that name whose implementation
 * is itself declared partial: what a reference sees of a partial type is its
 * own assembly's parts and the shared trees'. Seen without an assembly's own
 * parts -- from a shared tree, or from an assembly that has none --, a name
 * those parts declare is ambiguous: it is neither bound to what else has
 * that name nor missing.
 *
 * Contracts. A declaration in a project directory named `ref` is a stub:
 * `ref` is the .NET convention for reference-assembly sources. Inside one
 * assembly the stub stands behind the implementation: the implementation's
 * node binds, the stub's only where the implementation has none (its file
 * did not parse, it lacks the member, or a parse error hides it). An
 * assembly whose every declaration of a type is a stub holds only the
 * contract, and the rule joins a contract to the one implementation it can
 * belong to: the declaration of that full name and arity the shared trees
 * hold, when they hold exactly one and no assembly holds another
 * implementation of the name (entity_twins). The stub is then no second
 * type; a binding that exists only through the join is never exact.
 * Nothing is chosen between two implementations, two assemblies or two
 * shared trees.
 *
 * What a reference sees of a name: its own assembly's type -- for a file of
 * a shared tree, what its own tree declares. Else every shared tree's and
 * every other assembly's type of that name alike: one binds, several are
 * ambiguous. Which assemblies a project references is not known, so nothing
 * chooses between two of them -- no stub, no nearer directory.
 *
 * Alternatives. Complete declarations of one type in two or more projects
 * (the flavours of one assembly) are alternatives: the one of the
 * referencing file's own project binds, and from anywhere else the
 * reference is ambiguous. The same holds for one member declared in two or
 * more projects or shared trees, and for the parts of a type in two or more
 * shared trees. The parts of a partial type in one place are no
 * alternatives: the type's node is the part's that stands nearest to the
 * reference (a choice of presentation: every part is the type).
 *
 * Nodes. A declaration owns the node at <module>.<path> when it is the last
 * declaration of that path in its file (the extractor keeps one node per
 * qualified name and the last declaration wins, so `Foo` / `Foo<T>` in one
 * file leave one of them without a node: a graph gap, never a fallback to
 * the other). The same holds for members: overloads share one node; `Foo.X`
 * and `Foo<T>.X` share one, and it belongs to the later declaration.
 * Operators, indexers, events, delegates, implicit and primary constructors
 * have no node at all: a reference to one that is declared is a graph gap.
 *
 * Lookup is the C# compiler's cref binding, and it guesses nothing: the first
 * scope level that has the name decides, and more than one candidate there
 * is ambiguous.
 *   - a simple name: the documented method's type parameters; then for every
 *     enclosing type, innermost first: its type parameters, its own nested
 *     types and members; then for every enclosing namespace, innermost
 *     first: the types and namespaces in it, then (where a namespace
 *     declaration of the file stands) that declaration's aliases and after
 *     them its usings. The file's own usings and the project's global ones
 *     belong to the outermost level. A using of an inner namespace
 *     declaration is therefore asked before an outer namespace
 *   - a qualified name: its first segment is looked up as a simple name that
 *     names a type or a namespace; every further segment is a member of what
 *     the one before named. There is no second try from another level, and
 *     no fully-qualified shortcut past a nearer namespace of that name
 *   - inherited members are not looked up: the compiler does not consider
 *     them in a cref, in a class or in an interface, by a simple name or
 *     through the derived type's name. Such a reference is a row
 *     (CS_BIND_INHERITED says what else it could be)
 *   - arity: a name without type arguments is the arity-0 type. A generic
 *     type of that name never stands in (no cross-arity fallback); type
 *     arguments select the types and the generic methods of that arity. A
 *     method name without type arguments takes methods of any arity, the
 *     non-generic ones first
 *   - the level that has the name decides also when a parameter list
 *     follows: a written signature binds the overload of THAT level with
 *     exactly these parameter types; a type variable matches by its position
 *     only (`{K}` ... `(K)` against `<TKey>` ... `(TKey)`); only when no
 *     overload matches does one with a parameter type nothing is known about
 *     count, and several of those are ambiguous. Without a parameter list an
 *     overload group is ambiguous
 *   - constructors: a parameter list on a type's name (`Foo(int)`,
 *     `Ns.Foo(int)`, `Outer.Inner(int)`), `Foo.Foo`, and `Foo(int)` written
 *     inside a generic `Foo<T>`; a bare `Foo` is the type; no constructor is
 *     inherited
 *   - `using static` brings in a type's nested types and its static members,
 *     extension methods excepted
 *   - explicit interface implementations are not addressable by name
 *   - visibility: product code never binds a test-only declaration
 *     (test_only_target, no fallback), and a namespace that only test code
 *     declares is no name in its scope; a test program does not bind another
 *     program's global-namespace test type
 *
 * Reasons.
 *   - external is structural: a name through a namespace the repository does
 *     not declare (a using of one, a qualifier that is one, an open
 *     hierarchy: a base outside the repository), a keyword alias the
 *     repository does not declare, a project whose global usings could not
 *     be evaluated. There is no list of well-known outside names; the one
 *     namespace treated as never the repository's own is `System` with what
 *     is under it (CS_STANDARD_ROOT)
 *   - missing: a name every scope level was asked for, in declared
 *     namespaces only; a name whose level has no overload with the written
 *     parameters; an inherited member named through a derived type or by a
 *     simple name (the compiler binds no such cref) when the hierarchy is
 *     the repository's
 *   - graph_gap: declared, and no node (see Nodes); a namespace; what the
 *     parser could not place (a type whose members a parse error hides
 *     answers a member it does not show with graph_gap; a name some file
 *     declares without an establishable scope is a gap wherever it is
 *     written)
 *   - ambiguous: several candidates at the level that has the name; two
 *     assemblies' (or two shared trees') types of one name; the flavours of
 *     one type or member seen from outside their projects; a name that parts
 *     of the type out of the reference's view declare; also an overload
 *     group -- or one project's complete declarations of one type -- larger
 *     than CS_MAX_OVERLOADS that would have to be compared, and a name more
 *     than CS_MAX_FOREIGN assemblies declare. The index's log line
 *     doc_links.cs.ambiguous counts the references by these causes
 *   - unparseable: a reference longer than CS_REF_BUF, with more segments
 *     than CS_MAX_SEGS or more parameters than CS_MAX_PARAMS
 * No limit decides silently: every one of them ends in one of these rows.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "helpers.h" /* cbm_fqn_module_source_lang */
#include "pipeline/doc_links_msbuild.h"
#include "foundation/arena.h"
#include "foundation/compat_thread.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    CS_MAX_SEGS = 16,       /* segments of a written reference */
    CS_MAX_PARAMS = 24,     /* its parameters */
    CS_MAX_SUPERS = 64,     /* supertypes one lookup follows */
    CS_MAX_OVERLOADS = 256, /* declarations of one name one lookup compares */
    CS_MAX_FOREIGN = 64,    /* other owners' types of one name one lookup compares */
    CS_MAX_NEST = 64,       /* the scanner's nesting limits (types, namespace segments) */
    CS_KEY_BUF = 2048,      /* a node's qualified name */
    CS_NAME_BUF = 513,      /* one name (the scanner's CS_NAME_MAX and its terminator) */
    CS_PARAM_BUF = 256,     /* one normalized parameter type */
    CS_REF_BUF = 1024,      /* a written reference */
    CS_ARITY_NONE = -1,
    CS_NONE = -1,
    CS_AMBIGUOUS = -2,
    /* An entity's owner: an assembly (>= 0); CS_POOL, the shared trees' parts
     * of a partial type; below it, the one shared tree that holds a complete
     * declaration (shared_owner()). */
    CS_POOL = -1,
    CS_FIELDS = 9,    /* the most fields a scope record has (T) */
    CS_MAX_ROOTS = 3, /* Enum, ValueType, Object */
    /* 0: the compiler's rule, inherited members are never bound. 1: where the
     * compiler's lookup binds nothing, the nearest supertype that has the
     * member binds (a class's base classes, an interface's base interfaces,
     * then the implicit roots the repository declares). Never a different
     * binding than the compiler's: only one where it has none. */
    CS_BIND_INHERITED = 0,
};

/* ── Index data ──────────────────────────────────────────────────── */

/* A namespace of the repository: the global one (id 0), every declared one,
 * and every namespace above a declared one. */
typedef struct {
    int parent;
    int depth;        /* segments of its full name (the global namespace: 0) */
    const char *name; /* its own segment */
    bool declared;    /* a file declares it; else it is only above a declared one */
    bool prod;        /* product code declares it, or a namespace under it */
    bool standard;    /* `System`, or a namespace under it (see CS_STANDARD_ROOT) */
} cs_ns_t;

/* The one namespace no repository owns by declaring it. `System` is the
 * standard library's namespace by the language standard, and user code adds
 * to it (polyfills) without owning it: a name of `System`, or of a namespace
 * under it, that the repository does not have is the standard library's --
 * outside -- also where the repository declares that namespace. No other
 * root is treated so, and no list of names stands behind this: every other
 * namespace is the repository's as soon as a file declares it. */
static const char CS_STANDARD_ROOT[] = "System";

/* The IDs below retain the original directive order. Two written directives
 * remain two candidates even when they name the same target. */
typedef struct {
    const char *name;
    int id;
} cs_alias_id_t;

typedef struct {
    int scope; /* namespace or entity, according to the array */
    int id;
} cs_using_id_t;

typedef struct {
    cs_alias_id_t *aliases; /* by (name, directive ID) */
    size_t naliases;
    cs_using_id_t *namespaces; /* by (namespace, directive ID) */
    size_t nnamespaces;
    cs_using_id_t *entities; /* by (view entity, directive ID), at most two per using */
    size_t nentities;
    bool ready; /* published only after all targets and arrays are complete */
} cs_using_index_t;

typedef struct {
    const char *name;
    int scope;
    bool top; /* a namespace's top-level type, else an entity's named declaration */
} cs_name_scope_t;

typedef struct {
    int parent; /* region index; CS_NONE for the file's own region 0 */
    int ns;
    uint32_t start; /* its lines (see the note on lines below) */
    uint32_t end;
    int u_lo; /* its usings: [u_lo, u_hi) of the file's */
    int u_hi;
    cs_using_index_t using_index;
    bool open; /* a using of its own, or of a region around it, names something
                * outside the repository */
} cs_region_t;

typedef struct {
    int region;
    char kind; /* n namespace, s static, a alias */
    bool global;
    const char *alias;
    const char *target; /* as written */
    int ns;             /* what it names: a namespace (CS_NONE: none of the repository) */
    int ent;            /* ... or a type (CS_NONE; CS_AMBIGUOUS) */
    bool joined;        /* ... one type only because a contract is joined to it */
} cs_using_t;

typedef struct {
    int region;
    uint32_t start;
    uint32_t end;
    char kind; /* c s i e r t d */
    bool partial;
    bool incomplete; /* a parse error hides some of its members */
    bool owns_node;  /* the last declaration of its path in the file */
    int outer;       /* the enclosing type (index in the file), or CS_NONE */
    int depth;
    const char *name;
    const char *bases;
    int arity;
    int entity;
    int gid;                     /* types of one path in the file share it */
    const cbm_gbuf_node_t *node; /* its graph node, when it owns one */
} cs_type_t;

/* Line numbers (start, end) are meaningful only in a file re-extracted by
 * this run: the persisted scope of every other file carries zeros (so an edit
 * that only moves lines keeps the file's surface). Lines are therefore read
 * only for the SOURCE file's own context, never for a target. */
typedef struct {
    uint32_t start;
    int type;  /* the declaration it belongs to (index into the file's types) */
    int arity; /* generic method arity */
    char kind; /* c method or constructor, v field or enum member, p property, e event,
                  o operator, x indexer */
    bool explicit_impl;
    bool is_static; /* what `using static` brings in: static, and no extension method */
    const char *name;
    const char *sig;             /* '|'-joined parameter types; NULL for v, p and e */
    int nparams;                 /* how many `sig` holds */
    const cbm_gbuf_node_t *node; /* the node a reference to it binds; NULL: none */
} cs_member_t;

/* A type parameter: of a type (owner = its index) or of a generic method
 * (owner = the file's type count + the member's index). */
typedef struct {
    int owner;
    int pos;
    const char *name;
} cs_tparam_t;

typedef struct {
    uint32_t from;
    uint32_t to;
} cs_span_lines_t;

typedef struct {
    const char *rel_path;
    const char *module_qn;
    bool is_test;
    bool is_ref; /* of a project directory named `ref`: a reference assembly's source */
    int unit;
    cs_span_lines_t *unplaced; /* line ranges without a scope: sorted, disjoint */
    int nunplaced;
    cs_region_t *regions;
    int nregions;
    cs_using_t *usings; /* by region */
    int nusings;
    cs_type_t *types; /* document order: index = the T record's ordinal */
    int ntypes;
    cs_member_t *members; /* document order */
    int nmembers;
    int *members_by_start;
    cs_tparam_t *tparams; /* by (owner, name) */
    int ntparams;
    /* Its scope was written in this run and this reader refuses it: the file
     * declares nothing here, and its own references are graph gaps. */
    bool rejected;
} cs_file_t;

typedef struct {
    int file;
    int type;
} cs_decl_t;

/* Where a declaration stands for choosing the one a reference binds: with a
 * node before without, an implementation before a reference assembly's stub,
 * product code and test code apart. Entries of one class are in path order. */
enum { CS_CLS_TEST = 1, CS_CLS_REF = 2, CS_CLS_NO_NODE = 4, CS_CLS_COUNT = 8 };

typedef struct {
    int file;
    int type;
    unsigned char cls;
} cs_bind_t;

/* A complete (not `partial`) declaration outside a reference assembly's
 * source, by the project or shared tree it stands in. */
typedef struct {
    int unit;
    int file;
    int type;
} cs_full_t;

typedef struct {
    int ns;     /* a top-level type's namespace; CS_NONE for a nested type */
    int parent; /* a nested type's outer entity; CS_NONE */
    int owner;  /* a top-level type's: its assembly, CS_POOL, or a shared tree */
    int twin;   /* the shared trees' parts that are parts of this type too; CS_NONE */
    /* For the shared trees' declaration, what the assemblies make of it: */
    bool used;        /* an assembly's type has it as its twin */
    int stub;         /* the assembly's type that has only stubs and takes it as its
                       * implementation; CS_NONE: none does, CS_AMBIGUOUS: several do */
    bool rival_known; /* `rival` was asked for */
    bool rival;       /* an assembly holds an implementation of the name that is no part
                       * of it (see rival_implementation) */
    const char *name;
    int arity;
    char kind;
    cs_decl_t *decls;
    int ndecls;
    int dcap;
    int *units; /* the projects and shared trees that declare it: sorted, unique */
    int nunits;
    int *bases;
    int nbases;
    cs_bind_t *binds; /* its declarations, by (class, file, type) */
    /* Its complete declarations, when two or more projects (or shared trees)
     * hold one: alternatives, by (unit, file, type). NULL when at most one
     * does. */
    cs_full_t *fulls;
    int nfulls;
    int full_units;      /* how many units hold a complete declaration */
    int full_units_prod; /* ... one that is not test code */
    bool open;           /* a base of its own is outside the repository */
    bool open_any;       /* ... or one of a supertype's is */
    bool incomplete;     /* a declaration of it has members a parse error hides */
    bool any_prod;
    bool all_test;
    bool has_impl;     /* a declaration outside a reference assembly's source */
    bool impl_partial; /* ... that is declared partial */
    bool shared_parts; /* the shared trees' parts of a partial type, or nested in them */
    bool joined;       /* stubs only, joined to the shared trees' one implementation (twin) */
} cs_entity_t;

/* A type by the scope that declares it: `scope` is a namespace for a
 * top-level type and the outer entity for a nested one. */
typedef struct {
    int scope;
    const char *name;
    int arity;
    int owner; /* the entity's (a nested type has its outer type's: 0 here) */
    int ent;
} cs_named_t;

/* A member by the entity that declares it. */
typedef struct {
    int ent;
    int file;
    int midx;
    unsigned char group; /* 0 value or event, 1 callable, 2 operator or indexer */
    unsigned char cls;
} cs_mref_t;

/* A name that an assembly's own parts of a type declare beyond the shared
 * trees' declaration `twin` they belong to: a nested type the shared trees
 * do not have (`ent`), or members (`ent` and `arity` CS_NONE; one entry for
 * all of a name, `prod`: one of them is no test declaration). */
typedef struct {
    int twin;
    const char *name;
    int arity;
    int ent;
    bool prod;
} cs_extra_t;

/* A project (a directory with a *.csproj and what is below it, up to the
 * next one), or a shared tree. */
typedef struct {
    const char *dir;
    int group;          /* its assembly; CS_NONE for a shared tree */
    bool is_ref;        /* a project directory named `ref` */
    cs_using_t *usings; /* global usings: its files' and its project files' */
    int nusings;
    int cap;
    cs_using_index_t using_index;
    bool open; /* a global using could not be evaluated, or names something outside */
} cs_unit_t;

/* A directory that holds project files. */
typedef struct {
    int count;        /* its *.csproj files */
    const char *stem; /* the name of the first, without the extension */
} cs_pdir_t;

/* Why a reference is ambiguous. The row says `ambiguous`; the index's log
 * line says how many references of each kind there were. */
typedef enum {
    CS_WHY_SCOPE = 0,  /* by the language's rules: candidates of one scope level, overloads */
    CS_WHY_SHARED,     /* two or more shared trees declare the type */
    CS_WHY_ASSEMBLIES, /* two or more assemblies do */
    CS_WHY_FLAVOURS,   /* declarations of one type or member in several projects of one
                          assembly, seen from outside them */
    CS_WHY_PARTS,      /* declared by a part of the type the reference does not see */
    CS_WHY_LIMIT,      /* more candidates than one lookup compares */
    CS_WHY_COUNT,
} cs_why_t;

/* Counted while references are resolved (by every worker at once). */
typedef struct {
    _Atomic uint64_t ambiguous[CS_WHY_COUNT];
    _Atomic uint64_t rejected; /* references of files whose fresh scope was refused */
} cs_stats_t;

/* What parsing one scope set in the tables all files share: so that a scope
 * written in this run that this reader refuses can be taken back whole, and
 * costs only its own file. Reset for every file. */
typedef struct {
    int ns;
    bool declared;
    bool prod;
} cs_ns_was_t;

typedef struct {
    CBMHashTable *ht;
    const char *key;
} cs_mark_was_t;

typedef struct {
    int nnss; /* namespaces before the file */
    cs_ns_was_t *flags;
    int nflags;
    int cap_flags;
    cs_mark_was_t *marks;
    int nmarks;
    int cap_marks;
} cs_undo_t;

typedef struct {
    CBMArena arena;
    bool oom; /* an index allocation failed */
    const char *project;
    cs_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    cs_ns_t *nss;
    int nnss;
    int nscap;
    CBMHashTable *ns_by_key; /* "<parent>\x1f<segment>" -> id + 1 */
    cs_entity_t *ents;
    int nents;
    int ecap;
    CBMHashTable *ent_by_key;
    cs_named_t *tops; /* by (namespace, name, arity, entity) */
    int ntops;
    cs_named_t *kids; /* by (outer entity, name, arity, entity) */
    int nkids;
    cs_mref_t *mrefs; /* by (entity, name, group, signature, class, file, order) */
    int nmrefs;
    cs_extra_t *extras; /* by (twin, name, arity, entity): a name's members first */
    int nextras;
    cs_name_scope_t *name_scopes; /* distinct (name, kind, scope) reverse postings */
    size_t nname_scopes;
    bool name_scopes_ready;
    /* "P<entity>\x1f<name>": the entity declares an operator or indexer of
     * that name outside test code; "A...": it declares one at all */
    CBMHashTable *specials;
    CBMHashTable *type_names;      /* every type's simple name */
    CBMHashTable *quarantine;      /* names of types declared where no scope is known */
    CBMHashTable *quarantine_test; /* the same, declared by test code only */
    CBMHashTable *unit_by_dir;     /* dir -> unit + 1 */
    cs_unit_t *units;
    int nunits;
    int ucap;
    cbm_msb_t *msb;             /* the repository's MSBuild project files */
    CBMHashTable *project_dirs; /* directory -> cs_pdir_t: the ones that hold a *.csproj */
    const char **projects;      /* the *.csproj files, in path order */
    int nprojects;
    CBMHashTable *project_above; /* directories with a *.csproj in or below them */
    CBMHashTable *group_by_stem; /* project file name -> assembly + 1 */
    int ngroups;                 /* assemblies */
    int nshared;                 /* shared trees */
    cs_stats_t *stats;
    bool bind_inherited; /* CS_BIND_INHERITED */
    cs_undo_t undo;      /* of the file being parsed */
    int rejected;        /* files whose scope, written in this run, was refused */
    /* the unit part of the using steps, shared by the resolve workers; made
     * once the index is complete (NULL before, and when memory ran out) */
    struct cs_unit_memo *unit_memo;
} cs_index_t;

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: scope levels, supertypes and overloads the resolver looked at,
 * and the directories it walked for the files' projects, since the last
 * reset. A test holds it against the size of its input. */
static _Atomic uint64_t cs_work_counter;
/* New import index construction is measured separately from lookup work. */
static _Atomic uint64_t cs_index_work_counter;
/* Buckets of the node pass's scratch table walked when it is emptied. */
static _Atomic uint64_t cs_scratch_work_counter;
static _Atomic bool cs_fail_candidate_alloc;
static _Atomic bool cs_candidate_alloc_failed;

void cbm_doclink_cs_test_fail_candidate_alloc(bool enabled) {
    atomic_store(&cs_candidate_alloc_failed, false);
    atomic_store(&cs_fail_candidate_alloc, enabled);
}

bool cbm_doclink_cs_test_candidate_alloc_failed(void) {
    return atomic_load(&cs_candidate_alloc_failed);
}

void cbm_doclink_cs_test_work_reset(void) {
    atomic_store(&cs_work_counter, 0);
    atomic_store(&cs_index_work_counter, 0);
    atomic_store(&cs_scratch_work_counter, 0);
}

uint64_t cbm_doclink_cs_test_work(void) {
    return atomic_load(&cs_work_counter);
}

uint64_t cbm_doclink_cs_test_index_work(void) {
    return atomic_load(&cs_index_work_counter);
}

uint64_t cbm_doclink_cs_test_scratch_work(void) {
    return atomic_load(&cs_scratch_work_counter);
}

static void cs_work(uint64_t n) {
    atomic_fetch_add_explicit(&cs_work_counter, n, memory_order_relaxed);
}

static void cs_index_work(uint64_t n) {
    atomic_fetch_add_explicit(&cs_index_work_counter, n, memory_order_relaxed);
}

static void cs_scratch_work(uint64_t n) {
    atomic_fetch_add_explicit(&cs_scratch_work_counter, n, memory_order_relaxed);
}
#else
static void cs_work(uint64_t n) {
    (void)n;
}

static void cs_index_work(uint64_t n) {
    (void)n;
}

static void cs_scratch_work(uint64_t n) {
    (void)n;
}
#endif

/* ── Small helpers ───────────────────────────────────────────────── */

/* Index memory. A failed allocation is remembered: whatever a caller does
 * with its NULL, the build as a whole reports failure instead of handing out
 * an index that silently lacks a declaration. */
static void *ix_alloc(cs_index_t *ix, size_t n) {
    void *p = cbm_arena_alloc(&ix->arena, n ? n : SKIP_ONE);
    ix->oom = ix->oom || !p;
    return p;
}

static void *ix_zalloc(cs_index_t *ix, size_t n) {
    void *p = ix_alloc(ix, n);
    if (p) {
        memset(p, 0, n ? n : SKIP_ONE);
    }
    return p;
}

static char *ix_strndup(cs_index_t *ix, const char *s, size_t n) {
    char *p = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !p;
    return p;
}

static char *ix_strdup(cs_index_t *ix, const char *s) {
    char *p = cbm_arena_strdup(&ix->arena, s ? s : "");
    ix->oom = ix->oom || !p;
    return p;
}

/* Add `key` to a name set; false when memory ran out. */
static bool ht_mark(cs_index_t *ix, CBMHashTable *ht, const char *key) {
    if (cbm_ht_get(ht, key)) {
        return true;
    }
    char *k = ix_strdup(ix, key);
    if (!k) {
        return false;
    }
    cbm_ht_set(ht, k, (void *)k);
    return true;
}

/* Grow an undo array to hold one more entry. false when memory ran out. */
static bool undo_room(cs_index_t *ix, void **arr, int n, int *cap, size_t size) {
    if (n < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_16;
    void *grown = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * size);
    if (!grown) {
        ix->oom = true;
        return false;
    }
    if (n > 0) {
        memcpy(grown, *arr, (size_t)n * size);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, *arr);
    *arr = grown;
    *cap = ncap;
    return true;
}

/* Remember the flags of namespace `ns` before the file's scope sets them. */
static bool undo_note_ns(cs_index_t *ix, int ns) {
    cs_undo_t *u = &ix->undo;
    if (ns >= u->nnss) {
        return true; /* made by this file: taken back as a whole */
    }
    if (!undo_room(ix, (void **)&u->flags, u->nflags, &u->cap_flags, sizeof(cs_ns_was_t))) {
        return false;
    }
    u->flags[u->nflags++] =
        (cs_ns_was_t){.ns = ns, .declared = ix->nss[ns].declared, .prod = ix->nss[ns].prod};
    return true;
}

/* A type name declared in `f` at a place no namespace or outer type could be
 * established for. Product code never binds test-only declarations, so a name
 * only test files quarantine blocks references from test files only. */
static bool quarantine_name(cs_index_t *ix, const cs_file_t *f, const char *name) {
    CBMHashTable *ht = f->is_test ? ix->quarantine_test : ix->quarantine;
    if (cbm_ht_get(ht, name)) {
        return true;
    }
    char *k = ix_strdup(ix, name);
    cs_undo_t *u = &ix->undo;
    if (!k || !undo_room(ix, (void **)&u->marks, u->nmarks, &u->cap_marks, sizeof(cs_mark_was_t))) {
        return false;
    }
    cbm_ht_set(ht, k, (void *)k);
    u->marks[u->nmarks++] = (cs_mark_was_t){.ht = ht, .key = k};
    return true;
}

static bool cs_is_test_path(const char *rel) {
    /* C# test code: a directory named tests/test, or a *.Tests / *.UnitTests /
     * *.FunctionalTests project directory (the prototype's rule). */
    const char *p = rel;
    for (;;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            return false;
        }
        size_t n = (size_t)(slash - p);
        char seg[CBM_SZ_256];
        if (n < sizeof(seg)) {
            for (size_t i = 0; i < n; i++) {
                seg[i] = (char)tolower((unsigned char)p[i]);
            }
            seg[n] = '\0';
            if (strcmp(seg, "tests") == 0 || strcmp(seg, "test") == 0) {
                return true;
            }
            static const char *const sfx[] = {".tests", ".unittests", ".functionaltests"};
            for (size_t k = 0; k < sizeof(sfx) / sizeof(sfx[0]); k++) {
                size_t sl = strlen(sfx[k]);
                if (n >= sl && strcmp(seg + n - sl, sfx[k]) == 0) {
                    return true;
                }
            }
        }
        p = slash + SKIP_ONE;
    }
}

static bool cs_ci_suffix(const char *s, const char *sfx) {
    size_t n = strlen(s);
    size_t sl = strlen(sfx);
    if (n < sl) {
        return false;
    }
    for (size_t i = 0; i < sl; i++) {
        if (tolower((unsigned char)s[n - sl + i]) != sfx[i]) {
            return false;
        }
    }
    return true;
}

/* Split `s` in place at tabs into at most `max` fields; returns the count. */
static int split_fields(char *s, char **out, int max) {
    int n = 0;
    out[n++] = s;
    for (char *p = s; *p && n < max; p++) {
        if (*p == '\t') {
            *p = '\0';
            out[n++] = p + SKIP_ONE;
        }
    }
    return n;
}

static int count_list(const char *s, char sep) {
    if (!s || !s[0]) {
        return 0;
    }
    int n = 1;
    for (const char *p = s; *p; p++) {
        n += *p == sep;
    }
    return n;
}

/* A decimal index of a scope record: its value when it is one below `limit`,
 * else CS_NONE. A scope comes from the store: nothing in it is trusted to be
 * in range. */
static int field_index(const char *s, int limit) {
    if (!s[0] || strlen(s) > CBM_SZ_8) {
        return CS_NONE;
    }
    int v = 0;
    for (const char *p = s; *p; p++) {
        if (!isdigit((unsigned char)*p)) {
            return CS_NONE;
        }
        v = (v * 10) + (*p - '0');
    }
    return v < limit ? v : CS_NONE;
}

/* ── Namespaces ──────────────────────────────────────────────────── */

static bool ns_key(char *key, size_t cap, int parent, const char *seg, size_t len) {
    if (len == 0 || len >= CS_NAME_BUF) {
        return false;
    }
    int kl = snprintf(key, cap, "%d\x1f%.*s", parent, (int)len, seg);
    return kl > 0 && (size_t)kl < cap;
}

/* The namespace `seg` directly under `parent`, or CS_NONE. */
static int ns_find(const cs_index_t *ix, int parent, const char *seg, size_t len) {
    char key[CS_NAME_BUF + CBM_SZ_16];
    if (!ns_key(key, sizeof(key), parent, seg, len)) {
        return CS_NONE;
    }
    intptr_t v = (intptr_t)cbm_ht_get(ix->ns_by_key, key);
    return v > 0 ? (int)(v - SKIP_ONE) : CS_NONE;
}

/* The same, created when it is not there yet. CS_NONE for a name that is
 * none, and when memory ran out. */
static int ns_make(cs_index_t *ix, int parent, const char *seg, size_t len) {
    int found = ns_find(ix, parent, seg, len);
    char key[CS_NAME_BUF + CBM_SZ_16];
    if (found >= 0 || !ns_key(key, sizeof(key), parent, seg, len)) {
        return found;
    }
    if (ix->nnss >= ix->nscap) {
        int ncap = ix->nscap ? ix->nscap * PAIR_LEN : CBM_SZ_256;
        cs_ns_t *grown = (cs_ns_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_ns_t));
        if (!grown) {
            return CS_NONE;
        }
        if (ix->nnss > 0) {
            memcpy(grown, ix->nss, (size_t)ix->nnss * sizeof(cs_ns_t));
        }
        ix->nss = grown;
        ix->nscap = ncap;
    }
    char *k = ix_strdup(ix, key);
    char *name = ix_strndup(ix, seg, len);
    if (!k || !name) {
        return CS_NONE;
    }
    int id = ix->nnss++;
    ix->nss[id] =
        (cs_ns_t){.parent = parent, .depth = ix->nss[parent].depth + SKIP_ONE, .name = name};
    cbm_ht_set(ix->ns_by_key, k, (void *)(intptr_t)(id + SKIP_ONE));
    return id;
}

/* The namespace the dotted `path` names under `from`, every segment created
 * on the way; CS_NONE for a bad name, when memory ran out, and past the
 * scanner's nesting limit: a scope read back from the store is held to the
 * limit its writer keeps (every enclosing namespace is a lookup step). */
static int ns_make_path(cs_index_t *ix, int from, const char *path) {
    int ns = from;
    for (const char *p = path; ns >= 0 && *p;) {
        if (ix->nss[ns].depth >= CS_MAX_NEST) {
            return CS_NONE;
        }
        const char *dot = strchr(p, '.');
        size_t n = dot ? (size_t)(dot - p) : strlen(p);
        ns = ns_make(ix, ns, p, n);
        p = dot ? dot + SKIP_ONE : p + n;
    }
    return ns;
}

/* ── Scope blob parsing ──────────────────────────────────────────── */

static int tparam_cmp(const void *a, const void *b) {
    const cs_tparam_t *x = (const cs_tparam_t *)a;
    const cs_tparam_t *y = (const cs_tparam_t *)b;
    if (x->owner != y->owner) {
        return x->owner < y->owner ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    return c ? c : (x->pos > y->pos) - (x->pos < y->pos);
}

/* Position of the type parameter `name` of `owner` in `f`, or CS_NONE. */
static int tparam_find(const cs_file_t *f, int owner, const char *name, size_t len) {
    int lo = 0;
    int hi = f->ntparams;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const cs_tparam_t *t = &f->tparams[mid];
        int c = t->owner != owner ? (t->owner < owner ? -1 : 1) : strncmp(t->name, name, len);
        if (c == 0 && t->name[len] != '\0') {
            c = 1;
        }
        if (c < 0) {
            lo = mid + SKIP_ONE;
        } else if (c > 0) {
            hi = mid;
        } else {
            return t->pos;
        }
    }
    return CS_NONE;
}

static int span_cmp(const void *a, const void *b) {
    const cs_span_lines_t *x = (const cs_span_lines_t *)a;
    const cs_span_lines_t *y = (const cs_span_lines_t *)b;
    if (x->from != y->from) {
        return x->from < y->from ? -1 : 1;
    }
    return (x->to > y->to) - (x->to < y->to);
}

typedef struct {
    uint32_t start;
    bool plain; /* no generic method: those stand first among one line's members */
    int idx;
} cs_start_key_t;

static int start_key_cmp(const void *a, const void *b) {
    const cs_start_key_t *x = (const cs_start_key_t *)a;
    const cs_start_key_t *y = (const cs_start_key_t *)b;
    if (x->start != y->start) {
        return x->start < y->start ? -1 : 1;
    }
    if (x->plain != y->plain) {
        return x->plain ? 1 : -1;
    }
    return (x->idx > y->idx) - (x->idx < y->idx);
}

static int using_region_cmp(const void *a, const void *b) {
    const cs_using_t *x = (const cs_using_t *)a;
    const cs_using_t *y = (const cs_using_t *)b;
    if (x->region != y->region) {
        return x->region < y->region ? -1 : 1;
    }
    /* document order within a region: the targets are fields of one buffer,
     * in the order they were read */
    return (x->target > y->target) - (x->target < y->target);
}

/* What a scan of the blob's lines found, to size the file's arrays. */
typedef struct {
    int regions;
    int usings;
    int types;
    int members;
    int unplaced;
    int tparams;
} cs_counts_t;

static void count_records(const char *buf, cs_counts_t *n) {
    memset(n, 0, sizeof(*n));
    n->regions = SKIP_ONE; /* region 0: the file */
    for (const char *p = buf; *p;) {
        const char *nl = strchr(p, '\n');
        switch (*p) {
        case 'R':
            n->regions++;
            break;
        case 'U':
            n->usings++;
            break;
        case 'T':
            n->types++;
            break;
        case 'M':
            n->members++;
            break;
        case 'X':
            n->unplaced++;
            break;
        default:
            break;
        }
        /* an upper bound for the type parameters: one per record that can
         * have a list, and one more per comma */
        if (*p == 'T' || *p == 'M') {
            n->tparams++;
            for (const char *q = p; *q && *q != '\n'; q++) {
                n->tparams += *q == ',';
            }
        }
        if (!nl) {
            break;
        }
        p = nl + SKIP_ONE;
    }
}

/* Add the ','-joined type parameters `list` of `owner`. The list is cut at
 * its commas in place, so every name is a string of its own afterwards. */
static void add_tparams(cs_file_t *f, int owner, char *list) {
    int pos = 0;
    for (char *p = list; p && *p;) {
        char *e = strchr(p, ',');
        if (e) {
            *e = '\0';
        }
        f->tparams[f->ntparams++] = (cs_tparam_t){.owner = owner, .pos = pos++, .name = p};
        p = e ? e + SKIP_ONE : NULL;
    }
}

static bool parse_region(cs_index_t *ix, cs_file_t *f, char **fld, int n, int *next_region) {
    /* R id parent start end name */
    if (n != 6 || !fld[5][0]) {
        return false; /* the scanner names every region: an empty name would
                       * nest one without deepening the namespace */
    }
    int id = field_index(fld[1], f->nregions);
    int parent = field_index(fld[2], f->nregions);
    if (id != *next_region || parent < 0 || parent >= id) {
        return false; /* regions are numbered in order, each under an earlier one */
    }
    (*next_region)++;
    int ns = ns_make_path(ix, f->regions[parent].ns, fld[5]);
    if (ns < 0 || !undo_note_ns(ix, ns)) {
        return false;
    }
    ix->nss[ns].declared = true;
    /* product code declares it, and with it every namespace above: one that
     * is marked has its upper ones marked already */
    for (int up = ns; !f->is_test && up > 0 && !ix->nss[up].prod; up = ix->nss[up].parent) {
        if (!undo_note_ns(ix, up)) {
            return false;
        }
        ix->nss[up].prod = true;
    }
    f->regions[id] = (cs_region_t){.parent = parent,
                                   .ns = ns,
                                   .start = (uint32_t)strtoul(fld[3], NULL, 10),
                                   .end = (uint32_t)strtoul(fld[4], NULL, 10)};
    return true;
}

static bool parse_using(cs_file_t *f, char **fld, int n, int regions_seen) {
    /* U region kind alias target */
    if (n != 5) {
        return false;
    }
    int region = field_index(fld[1], regions_seen);
    char kind = fld[2][0];
    bool global = kind && fld[2][1] == 'g';
    if (region < 0 || !kind || !strchr("nsa", kind) || (fld[2][1] && !(global && !fld[2][2]))) {
        return false;
    }
    f->usings[f->nusings++] = (cs_using_t){.region = region,
                                           .kind = kind,
                                           .global = global,
                                           .alias = strcmp(fld[3], "-") == 0 ? "" : fld[3],
                                           .target = fld[4],
                                           .ns = CS_NONE,
                                           .ent = CS_NONE};
    return true;
}

static bool parse_type(cs_file_t *f, char **fld, int n, int regions_seen) {
    /* T region start end kind outer name tparams bases */
    if (n != 9) {
        return false;
    }
    cs_type_t *t = &f->types[f->ntypes];
    memset(t, 0, sizeof(*t));
    t->region = field_index(fld[1], regions_seen);
    t->start = (uint32_t)strtoul(fld[2], NULL, 10);
    t->end = (uint32_t)strtoul(fld[3], NULL, 10);
    t->kind = fld[4][0];
    const char *flags = t->kind ? fld[4] + SKIP_ONE : "";
    t->partial = flags[0] == 'p';
    flags += t->partial;
    t->incomplete = flags[0] == '!';
    flags += t->incomplete;
    bool top_level = strcmp(fld[5], "-") == 0;
    t->outer = top_level ? CS_NONE : field_index(fld[5], f->ntypes);
    t->depth = t->outer >= 0 ? f->types[t->outer].depth + SKIP_ONE : 0;
    t->name = fld[6];
    t->bases = fld[8];
    t->arity = count_list(fld[7], ',');
    t->entity = CS_NONE;
    size_t nl = strlen(t->name);
    if (t->region < 0 || !t->kind || !strchr("csiertd", t->kind) || flags[0] ||
        (t->outer < 0 && !top_level) || nl == 0 || nl >= CS_NAME_BUF || t->depth >= CS_MAX_NEST) {
        return false;
    }
    add_tparams(f, f->ntypes, fld[7]);
    f->ntypes++;
    return true;
}

static bool parse_member(cs_file_t *f, char **fld, int n) {
    /* M start kind explicit type name tparams sig */
    if (n != 8) {
        return false;
    }
    cs_member_t *m = &f->members[f->nmembers];
    memset(m, 0, sizeof(*m));
    m->start = (uint32_t)strtoul(fld[1], NULL, 10);
    m->kind = fld[2][0];
    m->is_static = m->kind && fld[2][1] == 's';
    m->explicit_impl = fld[3][0] == '1';
    m->type = field_index(fld[4], f->ntypes);
    m->name = fld[5];
    m->arity = count_list(fld[6], ',');
    bool callable_like = m->kind == 'c' || m->kind == 'o' || m->kind == 'x';
    m->sig = callable_like ? fld[7] : NULL;
    m->nparams = (m->sig && m->sig[0]) ? count_list(m->sig, '|') : 0;
    size_t nl = strlen(m->name);
    if (m->type < 0 || !m->kind || !strchr("cvpeox", m->kind) ||
        fld[2][m->is_static ? PAIR_LEN : SKIP_ONE] || nl == 0 || nl >= CS_NAME_BUF) {
        return false;
    }
    /* owners of type parameters: the types come first, the members after
     * the LAST type the blob has, whose count is known only at the end */
    add_tparams(f, -(f->nmembers + PAIR_LEN), fld[6]);
    f->nmembers++;
    return true;
}

/* Everything the records of one blob say, into `f`. false for a blob this
 * code did not write (or one the store damaged), and when memory ran out. */
static bool parse_records(cs_index_t *ix, cs_file_t *f, char *buf) {
    int next_region = SKIP_ONE;
    bool first = true;
    for (char *line = buf; line && *line;) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        bool ok = true;
        if (first) {
            first = false;
            ok = strcmp(line, CBM_DOCLINK_CS_SCOPE_TAG) == 0;
        } else {
            char *fld[CS_FIELDS + SKIP_ONE];
            int n = split_fields(line, fld, CS_FIELDS + SKIP_ONE);
            switch (line[0]) {
            case 'R':
                ok = parse_region(ix, f, fld, n, &next_region);
                break;
            case 'U':
                ok = parse_using(f, fld, n, next_region);
                break;
            case 'T':
                ok = parse_type(f, fld, n, next_region);
                break;
            case 'M':
                ok = parse_member(f, fld, n);
                break;
            case 'X':
                ok = n == 3;
                if (ok) {
                    f->unplaced[f->nunplaced++] =
                        (cs_span_lines_t){.from = (uint32_t)strtoul(fld[1], NULL, 10),
                                          .to = (uint32_t)strtoul(fld[2], NULL, 10)};
                }
                break;
            case 'Q':
                ok = n == PAIR_LEN && fld[1][0] && quarantine_name(ix, f, fld[1]);
                break;
            default:
                ok = false;
                break;
            }
        }
        if (!ok) {
            return false;
        }
        line = nl ? nl + SKIP_ONE : NULL;
    }
    return !first && next_region == f->nregions;
}

/* Sort the usings by region, give every region its range, and close the
 * unplaced ranges into sorted, disjoint ones. */
static void finish_ranges(cs_file_t *f) {
    if (f->nusings > 1) {
        qsort(f->usings, (size_t)f->nusings, sizeof(cs_using_t), using_region_cmp);
    }
    int u = 0;
    for (int r = 0; r < f->nregions; r++) {
        f->regions[r].u_lo = u;
        while (u < f->nusings && f->usings[u].region == r) {
            u++;
        }
        f->regions[r].u_hi = u;
    }
    if (f->nunplaced > 1) {
        qsort(f->unplaced, (size_t)f->nunplaced, sizeof(cs_span_lines_t), span_cmp);
        int w = 0;
        for (int i = 1; i < f->nunplaced; i++) {
            if (f->unplaced[i].from <= f->unplaced[w].to) {
                if (f->unplaced[i].to > f->unplaced[w].to) {
                    f->unplaced[w].to = f->unplaced[i].to;
                }
            } else {
                f->unplaced[++w] = f->unplaced[i];
            }
        }
        f->nunplaced = w + SKIP_ONE;
    }
}

/* The type parameters by (owner, name), and the members in line order -- of
 * the members that start at one line, the generic methods first.
 * false when memory ran out. */
static bool finish_lookups(cs_index_t *ix, cs_file_t *f) {
    /* a member's parameters were filed under -(index + 2) while the type
     * count was still growing */
    for (int i = 0; i < f->ntparams; i++) {
        if (f->tparams[i].owner < 0) {
            f->tparams[i].owner = f->ntypes + (-f->tparams[i].owner - PAIR_LEN);
        }
    }
    if (f->ntparams > 1) {
        qsort(f->tparams, (size_t)f->ntparams, sizeof(cs_tparam_t), tparam_cmp);
    }
    if (f->nmembers == 0) {
        return true;
    }
    f->members_by_start = (int *)ix_alloc(ix, (size_t)f->nmembers * sizeof(int));
    cs_start_key_t *keys = (cs_start_key_t *)cbm_alloc(
        CBM_MEM_CLASS_OTHER, (size_t)f->nmembers * sizeof(cs_start_key_t));
    if (!f->members_by_start || !keys) {
        cbm_free(CBM_MEM_CLASS_OTHER, keys);
        ix->oom = true;
        return false;
    }
    for (int i = 0; i < f->nmembers; i++) {
        const cs_member_t *m = &f->members[i];
        keys[i] = (cs_start_key_t){
            .start = m->start, .plain = !(m->kind == 'c' && m->arity > 0), .idx = i};
    }
    qsort(keys, (size_t)f->nmembers, sizeof(*keys), start_key_cmp);
    for (int i = 0; i < f->nmembers; i++) {
        f->members_by_start[i] = keys[i].idx;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, keys);
    return true;
}

static bool parse_scope(cs_index_t *ix, cs_file_t *f, const char *blob) {
    char *buf = ix_strdup(ix, blob);
    if (!buf) {
        return false;
    }
    cs_counts_t n;
    count_records(buf, &n);
    f->nregions = n.regions;
    f->regions = (cs_region_t *)ix_zalloc(ix, (size_t)n.regions * sizeof(cs_region_t));
    f->usings = (cs_using_t *)ix_alloc(ix, (size_t)n.usings * sizeof(cs_using_t));
    f->types = (cs_type_t *)ix_alloc(ix, (size_t)n.types * sizeof(cs_type_t));
    f->members = (cs_member_t *)ix_alloc(ix, (size_t)n.members * sizeof(cs_member_t));
    f->unplaced = (cs_span_lines_t *)ix_alloc(ix, (size_t)n.unplaced * sizeof(cs_span_lines_t));
    f->tparams = (cs_tparam_t *)ix_alloc(ix, (size_t)n.tparams * sizeof(cs_tparam_t));
    if (ix->oom) {
        return false;
    }
    f->regions[0].parent = CS_NONE;
    if (!parse_records(ix, f, buf)) {
        return false;
    }
    finish_ranges(f);
    return finish_lookups(ix, f);
}

/* ── Units (C# projects) and their global usings ─────────────────── */

/* Nothing here reads the disk: a project file is one the index holds (its
 * path among the resolver's files, its content in its scope blob). What
 * discovery did not take -- a symbolic link, a named pipe -- is no project
 * file. */

static const char CS_PROJECT_EXT[] = ".csproj";

/* The name of a project directory that holds a reference assembly's source:
 * the .NET convention (<library>/ref beside <library>/src). It tells the
 * stub from the implementation inside one assembly, and nothing else. */
static const char CS_REF_DIR[] = "ref";

/* Length of the directory above the one of length `len` in `path`. */
static size_t parent_dir_len(const char *path, size_t len) {
    while (len > 0 && path[len - SKIP_ONE] != '/') {
        len--;
    }
    return len > 0 ? len - SKIP_ONE : 0;
}

/* Take the project files out of the resolver's file list: every MSBuild blob
 * goes to the evaluator, every *.csproj marks its directory as a project and
 * names it. false when memory ran out. */
static bool collect_projects(cs_index_t *ix, const cbm_doclink_build_in_t *in) {
    int n = 0;
    for (int i = 0; i < in->file_count; i++) {
        n += cs_ci_suffix(in->files[i].rel_path, CS_PROJECT_EXT);
    }
    ix->projects = (const char **)ix_alloc(ix, (size_t)n * sizeof(char *));
    if (!ix->projects) {
        return false;
    }
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        if (cbm_msb_is_project_scope(src->scope) &&
            !cbm_msb_add(ix->msb, src->rel_path, src->scope)) {
            return false;
        }
        /* a project file of an SDK that compiles nothing (it runs build steps
         * or builds other projects) is the project of no source file */
        if (!cs_ci_suffix(src->rel_path, CS_PROJECT_EXT) ||
            !cbm_msb_compiles(ix->msb, src->rel_path)) {
            continue;
        }
        const char *slash = strrchr(src->rel_path, '/');
        const char *name = slash ? slash + SKIP_ONE : src->rel_path;
        char *dir = ix_strndup(ix, src->rel_path, slash ? (size_t)(slash - src->rel_path) : 0);
        char *path = ix_strdup(ix, src->rel_path);
        if (!dir || !path) {
            return false;
        }
        ix->projects[ix->nprojects++] = path; /* the file list is in path order */
        cs_pdir_t *pd = (cs_pdir_t *)cbm_ht_get(ix->project_dirs, dir);
        if (pd) {
            pd->count++;
            continue;
        }
        pd = (cs_pdir_t *)ix_zalloc(ix, sizeof(*pd));
        char *stem = ix_strndup(ix, name, strlen(name) - (sizeof(CS_PROJECT_EXT) - SKIP_ONE));
        if (!pd || !stem) {
            return false;
        }
        pd->count = SKIP_ONE;
        pd->stem = stem;
        cbm_ht_set(ix->project_dirs, dir, pd);
        /* the directory and every one above it has a project in or below
         * it; one that is marked has its upper ones marked already */
        for (size_t len = strlen(dir);; len = parent_dir_len(dir, len)) {
            char *above = ix_strndup(ix, dir, len);
            if (!above) {
                return false;
            }
            if (cbm_ht_get(ix->project_above, above)) {
                break;
            }
            cbm_ht_set(ix->project_above, above, above);
            if (len == 0) {
                break;
            }
        }
    }
    return true;
}

/* The assembly of a project directory: the one its project file names --
 * projects whose project files have the same name are one assembly. A
 * directory with several project files is an assembly of its own (which of
 * them compiles a file there is not known). CS_NONE when memory ran out. */
static int group_of(cs_index_t *ix, const cs_pdir_t *pd) {
    if (pd->count != SKIP_ONE) {
        return ix->ngroups++;
    }
    intptr_t g = (intptr_t)cbm_ht_get(ix->group_by_stem, pd->stem);
    if (g <= 0) {
        g = (intptr_t)ix->ngroups + SKIP_ONE;
        ix->ngroups++;
        cbm_ht_set(ix->group_by_stem, pd->stem, (void *)g);
    }
    return (int)(g - SKIP_ONE);
}

/* The unit of directory `dir`, made on first sight: a project when `pd` says
 * which project files the directory holds, else a shared tree. CS_NONE when
 * memory ran out. */
static int unit_get(cs_index_t *ix, const char *dir, const cs_pdir_t *pd) {
    intptr_t v = (intptr_t)cbm_ht_get(ix->unit_by_dir, dir);
    if (v > 0) {
        return (int)(v - SKIP_ONE);
    }
    if (ix->nunits >= ix->ucap) {
        int ncap = ix->ucap ? ix->ucap * PAIR_LEN : CBM_SZ_64;
        cs_unit_t *grown = (cs_unit_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_unit_t));
        if (!grown) {
            return CS_NONE;
        }
        if (ix->nunits > 0) {
            memcpy(grown, ix->units, (size_t)ix->nunits * sizeof(cs_unit_t));
        }
        ix->units = grown;
        ix->ucap = ncap;
    }
    char *key = ix_strdup(ix, dir);
    if (!key) {
        return CS_NONE;
    }
    int id = ix->nunits++;
    cs_unit_t *u = &ix->units[id];
    memset(u, 0, sizeof(*u));
    u->dir = key;
    u->group = CS_NONE;
    if (pd) {
        const char *base = strrchr(key, '/');
        u->is_ref = strcmp(base ? base + SKIP_ONE : key, CS_REF_DIR) == 0;
        u->group = group_of(ix, pd);
    } else {
        ix->nshared++;
    }
    cbm_ht_set(ix->unit_by_dir, key, (void *)(intptr_t)(id + SKIP_ONE));
    return id;
}

/* The tree of the project-less directory rel[0, len): the length of its root
 * -- the largest directory around it that has no project in or below it; the
 * directory itself when a project stands below it (*whole stays unset: what
 * is under it is not all of its tree). `memo` is what `dir_unit` holds for
 * the directory the walk for a project stopped at: a tree's root when that
 * directory is of a known tree, and then everything under it is of that tree
 * too. `dir` is scratch for the paths asked. */
static size_t tree_root(const cs_index_t *ix, const char *rel, size_t len, intptr_t memo, char *dir,
                        bool *whole) {
    if (memo < CS_NONE) {
        *whole = true;
        return (size_t)(-(memo + PAIR_LEN));
    }
    memcpy(dir, rel, len);
    dir[len] = '\0';
    if (cbm_ht_get(ix->project_above, dir)) {
        return len;
    }
    *whole = true;
    size_t root = len;
    while (root > 0) {
        cs_work(SKIP_ONE);
        size_t up = parent_dir_len(rel, root);
        dir[up] = '\0';
        if (cbm_ht_get(ix->project_above, dir)) {
            break;
        }
        root = up;
    }
    return root;
}

/* The unit a file belongs to: its project -- the nearest directory at or
 * above its own that holds a *.csproj. A file no project file stands above
 * is of a shared tree (sources that projects elsewhere compile in): which
 * programs it is part of is not known, and all such files of a repository
 * are not one program. Its unit is the tree: the largest directory around it
 * that has no project in or below it -- the file's own directory when a
 * project stands below that. (A repository without any project file is one
 * tree.) Every directory walked on the way up is remembered in `dir_unit`:
 * its project's unit + 1; for one no project stands at or above, -1, or
 * -(length of its tree's root + 2) when it has no project below it either.
 * So a directory is walked once, for its project and for its tree, however
 * many files it and the directories under it hold. CS_NONE when memory ran
 * out. */
static int unit_of(cs_index_t *ix, CBMHashTable *dir_unit, const char *rel) {
    const char *slash = strrchr(rel, '/');
    size_t len = slash ? (size_t)(slash - rel) : 0;
    char *dir = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, len + SKIP_ONE);
    if (!dir) {
        ix->oom = true;
        return CS_NONE;
    }
    memcpy(dir, rel, len);
    size_t cur = len;
    int unit = CS_NONE;
    intptr_t memo = 0;
    bool projectless = false;
    bool failed = false;
    for (;;) {
        cs_work(SKIP_ONE);
        dir[cur] = '\0';
        memo = (intptr_t)cbm_ht_get(dir_unit, dir);
        if (memo > 0) {
            unit = (int)(memo - SKIP_ONE);
            break;
        }
        const cs_pdir_t *pd = (const cs_pdir_t *)cbm_ht_get(ix->project_dirs, dir);
        if (pd) {
            unit = unit_get(ix, dir, pd);
            failed = unit < 0;
            break;
        }
        if (memo < 0 || cur == 0) {
            projectless = true;
            break;
        }
        cur = parent_dir_len(rel, cur);
    }
    bool whole = false;
    size_t root = projectless ? tree_root(ix, rel, len, memo, dir, &whole) : 0;
    for (size_t l = len; !failed;) {
        memcpy(dir, rel, l);
        dir[l] = '\0';
        if (!cbm_ht_get(dir_unit, dir)) {
            char *key = ix_strdup(ix, dir);
            if (!key) {
                failed = true;
                break;
            }
            intptr_t known = unit + SKIP_ONE;
            if (projectless) {
                known = (whole && l >= root) ? -(intptr_t)(root + PAIR_LEN) : CS_NONE;
            }
            cbm_ht_set(dir_unit, key, (void *)known);
        }
        if (l <= cur) {
            break;
        }
        l = parent_dir_len(rel, l);
    }
    if (projectless && !failed) {
        memcpy(dir, rel, root);
        dir[root] = '\0';
        unit = unit_get(ix, dir, NULL);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, dir);
    return failed ? CS_NONE : unit;
}

static bool unit_add_using(cs_index_t *ix, cs_unit_t *u, char kind, const char *alias,
                           const char *target) {
    if (u->nusings >= u->cap) {
        int ncap = u->cap ? u->cap * PAIR_LEN : CBM_SZ_8;
        cs_using_t *grown = (cs_using_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_using_t));
        if (!grown) {
            return false;
        }
        if (u->nusings > 0) {
            memcpy(grown, u->usings, (size_t)u->nusings * sizeof(cs_using_t));
        }
        u->usings = grown;
        u->cap = ncap;
    }
    char *a = ix_strdup(ix, alias);
    char *t = ix_strdup(ix, target);
    if (!a || !t) {
        return false;
    }
    u->usings[u->nusings++] = (cs_using_t){
        .kind = kind, .global = true, .alias = a, .target = t, .ns = CS_NONE, .ent = CS_NONE};
    return true;
}

static int unit_using_cmp(const void *a, const void *b) {
    const cs_using_t *x = (const cs_using_t *)a;
    const cs_using_t *y = (const cs_using_t *)b;
    if (x->kind != y->kind) {
        return x->kind < y->kind ? -1 : 1;
    }
    int c = strcmp(x->alias, y->alias);
    return c ? c : strcmp(x->target, y->target);
}

/* What the project files could not tell, over all of them. */
typedef struct {
    int unevaluable;
    int outside;
} cs_msb_totals_t;

/* Global usings of every unit: the `global using` directives of its files
 * (of a namespace, of a type's static members, of an alias alike) and the
 * <Using> items of every project file in its directory -- all of them, in
 * path order, so the union does not depend on how a directory is listed.
 * false when memory ran out. */
static bool units_collect_usings(cs_index_t *ix, cs_msb_totals_t *totals) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const cs_file_t *f = &ix->files[fi];
        for (int u = 0; f->unit >= 0 && u < f->nusings; u++) {
            const cs_using_t *us = &f->usings[u];
            if (us->global &&
                !unit_add_using(ix, &ix->units[f->unit], us->kind, us->alias, us->target)) {
                return false;
            }
        }
    }
    cbm_msb_eval_context_t *eval_context = cbm_msb_eval_context_new(ix->msb);
    if (!eval_context) {
        return false;
    }
    for (int p = 0; p < ix->nprojects; p++) {
        const char *slash = strrchr(ix->projects[p], '/');
        char *dir = ix_strndup(ix, ix->projects[p], slash ? (size_t)(slash - ix->projects[p]) : 0);
        intptr_t unit = dir ? (intptr_t)cbm_ht_get(ix->unit_by_dir, dir) : 0;
        if (unit <= 0) {
            continue; /* a project directory without a C# file */
        }
        cs_unit_t *u = &ix->units[unit - SKIP_ONE];
        if (!cbm_msb_has(ix->msb, ix->projects[p])) {
            /* a *.csproj the extractor has no blob for: what it sets is not known */
            totals->unevaluable++;
            u->open = true;
            continue;
        }
        cbm_msb_result_t res;
        if (!cbm_msb_eval_context_eval(eval_context, ix->projects[p], &res)) {
            cbm_msb_eval_context_free(eval_context);
            return false;
        }
        bool ok = true;
        for (int k = 0; ok && k < res.count; k++) {
            ok = unit_add_using(ix, u, res.usings[k].kind, res.usings[k].alias,
                                res.usings[k].target);
        }
        u->open = u->open || res.open;
        totals->unevaluable += res.unevaluable;
        totals->outside += res.outside;
        cbm_msb_result_free(&res);
        if (!ok) {
            cbm_msb_eval_context_free(eval_context);
            return false;
        }
    }
    cbm_msb_eval_context_free(eval_context);
    for (int ui = 0; ui < ix->nunits; ui++) {
        cs_unit_t *u = &ix->units[ui];
        if (u->nusings < PAIR_LEN) {
            continue;
        }
        qsort(u->usings, (size_t)u->nusings, sizeof(cs_using_t), unit_using_cmp);
        int w = 0;
        for (int i = 1; i < u->nusings; i++) {
            if (unit_using_cmp(&u->usings[w], &u->usings[i]) != 0) {
                u->usings[++w] = u->usings[i];
            }
        }
        u->nusings = w + SKIP_ONE;
    }
    return !ix->oom;
}

/* ── Entities ────────────────────────────────────────────────────── */

/* The owner of a complete declaration in the shared tree `unit`. */
static int shared_owner(int unit) {
    return -(unit + PAIR_LEN);
}

/* The entity of this scope, name, arity and owner, created on first sight. A
 * top-level type has a namespace `ns` and an `owner`; a nested type has its
 * outer entity `parent` (and the owner 0: its outer type's is the one that
 * counts). CS_NONE when memory ran out. */
static int entity_get(cs_index_t *ix, int ns, int parent, int owner, const char *name, int arity,
                      char kind) {
    char key[CS_NAME_BUF + CBM_SZ_64];
    int kl = snprintf(key, sizeof(key), "%c%d\x1f%d\x1f%d\x1f%s", parent >= 0 ? 'E' : 'N',
                      parent >= 0 ? parent : ns, owner, arity, name);
    if (kl < 0 || kl >= (int)sizeof(key)) {
        ix->oom = true; /* cannot be: parse_type bounds the name */
        return CS_NONE;
    }
    intptr_t v = (intptr_t)cbm_ht_get(ix->ent_by_key, key);
    if (v > 0) {
        return (int)(v - SKIP_ONE);
    }
    if (ix->nents >= ix->ecap) {
        int ncap = ix->ecap ? ix->ecap * PAIR_LEN : CBM_SZ_1K;
        cs_entity_t *grown =
            (cs_entity_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * sizeof(cs_entity_t));
        if (!grown) {
            ix->oom = true;
            return CS_NONE;
        }
        if (ix->nents > 0) {
            memcpy(grown, ix->ents, (size_t)ix->nents * sizeof(cs_entity_t));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
        ix->ents = grown;
        ix->ecap = ncap;
    }
    char *k = ix_strdup(ix, key);
    if (!k) {
        return CS_NONE;
    }
    int id = ix->nents++;
    cs_entity_t *e = &ix->ents[id];
    memset(e, 0, sizeof(*e));
    e->ns = parent >= 0 ? CS_NONE : ns;
    e->parent = parent >= 0 ? parent : CS_NONE;
    e->owner = owner;
    e->twin = CS_NONE;
    e->stub = CS_NONE;
    e->shared_parts = parent >= 0 ? ix->ents[parent].shared_parts : owner == CS_POOL;
    e->name = name;
    e->arity = arity;
    e->kind = kind;
    e->all_test = true;
    cbm_ht_set(ix->ent_by_key, k, (void *)(intptr_t)(id + SKIP_ONE));
    return id;
}

static bool entity_add_decl(cs_index_t *ix, cs_entity_t *e, int file, int type) {
    if (e->ndecls >= e->dcap) {
        int ncap = e->dcap ? e->dcap * PAIR_LEN : CBM_SZ_2;
        cs_decl_t *grown = (cs_decl_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_decl_t));
        if (!grown) {
            return false;
        }
        if (e->ndecls > 0) {
            memcpy(grown, e->decls, (size_t)e->ndecls * sizeof(cs_decl_t));
        }
        e->decls = grown;
        e->dcap = ncap;
    }
    e->decls[e->ndecls++] = (cs_decl_t){.file = file, .type = type};
    const cs_file_t *f = &ix->files[file];
    e->any_prod = e->any_prod || !f->is_test;
    e->all_test = e->all_test && f->is_test;
    e->has_impl = e->has_impl || !f->is_ref;
    e->impl_partial = e->impl_partial || (!f->is_ref && f->types[type].partial);
    e->incomplete = e->incomplete || f->types[type].incomplete;
    return true;
}

/* A top-level type as one shared tree declares it. false when it does not
 * fit (cannot be: parse_type bounds the name). */
static bool tree_type_key(char *key, size_t cap, const cs_file_t *f, const cs_type_t *t) {
    int n = snprintf(key, cap, "%d\x1f%d\x1f%d\x1f%s", f->regions[t->region].ns, f->unit, t->arity,
                     t->name);
    return n > 0 && (size_t)n < cap;
}

/* The top-level types the shared trees declare partial, by tree: into
 * `partials` (keys in `keys`). false when memory ran out. */
static bool tree_partials(const cs_index_t *ix, CBMHashTable *partials, CBMArena *keys) {
    char key[CS_NAME_BUF + CBM_SZ_64];
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const cs_file_t *f = &ix->files[fi];
        for (int ti = 0; ix->units[f->unit].group < 0 && ti < f->ntypes; ti++) {
            const cs_type_t *t = &f->types[ti];
            if (t->outer >= 0 || !t->partial || !tree_type_key(key, sizeof(key), f, t) ||
                cbm_ht_get(partials, key)) {
                continue;
            }
            char *k = cbm_arena_strdup(keys, key);
            if (!k) {
                return false;
            }
            cbm_ht_set(partials, k, k);
        }
    }
    return true;
}

/* The owner of a top-level type declared in `f`: the file's assembly. For a
 * file of a shared tree: the shared trees' parts of that name when the
 * declaration is partial -- or stands in a tree that has partial ones of the
 * name: one tree's declarations of a name are one type --, else the tree. */
static int decl_owner(const cs_index_t *ix, const cs_file_t *f, const cs_type_t *t,
                      const CBMHashTable *partials) {
    int group = ix->units[f->unit].group;
    if (group >= 0) {
        return group;
    }
    char key[CS_NAME_BUF + CBM_SZ_64];
    bool parts = t->partial || (tree_type_key(key, sizeof(key), f, t) && cbm_ht_get(partials, key));
    return parts ? CS_POOL : shared_owner(f->unit);
}

/* The entity of every declared type. An outer type stands before the types
 * it holds, so its entity is known when theirs is asked for. false when
 * memory ran out. */
static bool build_entities(cs_index_t *ix) {
    CBMHashTable *partials = cbm_ht_create(CBM_SZ_1K);
    CBMArena keys;
    cbm_arena_init(&keys);
    bool ok = partials && tree_partials(ix, partials, &keys);
    for (int fi = 0; ok && fi < ix->nfiles; fi++) {
        cs_file_t *f = &ix->files[fi];
        for (int ti = 0; ok && ti < f->ntypes; ti++) {
            cs_type_t *t = &f->types[ti];
            int ent = t->outer >= 0
                          ? entity_get(ix, CS_NONE, f->types[t->outer].entity, 0, t->name, t->arity,
                                       t->kind)
                          : entity_get(ix, f->regions[t->region].ns, CS_NONE,
                                       decl_owner(ix, f, t, partials), t->name, t->arity, t->kind);
            ok = ent >= 0 && entity_add_decl(ix, &ix->ents[ent], fi, ti) &&
                 ht_mark(ix, ix->type_names, t->name);
            t->entity = ent;
        }
    }
    cbm_ht_free(partials);
    cbm_arena_destroy(&keys);
    ix->oom = ix->oom || !ok;
    return ok;
}

static int int_cmp(const void *a, const void *b) {
    int x = *(const int *)a;
    int y = *(const int *)b;
    return (x > y) - (x < y);
}

static int full_cmp(const void *a, const void *b) {
    const cs_full_t *x = (const cs_full_t *)a;
    const cs_full_t *y = (const cs_full_t *)b;
    if (x->unit != y->unit) {
        return x->unit < y->unit ? -1 : 1;
    }
    if (x->file != y->file) {
        return x->file < y->file ? -1 : 1;
    }
    return (x->type > y->type) - (x->type < y->type);
}

/* The complete declarations of every entity that has them in two or more
 * projects (or shared trees): the flavours of one type, alternatives of each
 * other. A reference assembly's stubs stand behind and are not
 * counted. false when memory ran out. */
static bool entity_fulls(cs_index_t *ix) {
    for (int i = 0; i < ix->nents; i++) {
        cs_entity_t *e = &ix->ents[i];
        int n = 0;
        for (int d = 0; d < e->ndecls; d++) {
            const cs_file_t *f = &ix->files[e->decls[d].file];
            n += !f->is_ref && !f->types[e->decls[d].type].partial;
        }
        if (n < PAIR_LEN) {
            continue;
        }
        cs_full_t *fulls = (cs_full_t *)ix_alloc(ix, (size_t)n * sizeof(cs_full_t));
        if (!fulls) {
            return false;
        }
        int w = 0;
        for (int d = 0; d < e->ndecls; d++) {
            const cs_file_t *f = &ix->files[e->decls[d].file];
            if (!f->is_ref && !f->types[e->decls[d].type].partial) {
                fulls[w++] = (cs_full_t){
                    .unit = f->unit, .file = e->decls[d].file, .type = e->decls[d].type};
            }
        }
        qsort(fulls, (size_t)n, sizeof(cs_full_t), full_cmp);
        int units = 0;
        int prod = 0;
        for (int k = 0; k < n;) {
            int unit = fulls[k].unit;
            bool product = false;
            for (; k < n && fulls[k].unit == unit; k++) {
                product = product || !ix->files[fulls[k].file].is_test;
            }
            units++;
            prod += product;
        }
        if (units >= PAIR_LEN) {
            e->fulls = fulls;
            e->nfulls = n;
            e->full_units = units;
            e->full_units_prod = prod;
        }
    }
    return true;
}

/* The projects and shared trees that declare each entity: sorted, unique. */
static bool entity_units(cs_index_t *ix) {
    for (int i = 0; i < ix->nents; i++) {
        cs_entity_t *e = &ix->ents[i];
        e->units = (int *)ix_alloc(ix, (size_t)e->ndecls * sizeof(int));
        if (!e->units) {
            return false;
        }
        for (int d = 0; d < e->ndecls; d++) {
            e->units[d] = ix->files[e->decls[d].file].unit;
        }
        qsort(e->units, (size_t)e->ndecls, sizeof(int), int_cmp);
        int w = 0;
        for (int d = 1; d < e->ndecls; d++) {
            if (e->units[d] != e->units[w]) {
                e->units[++w] = e->units[d];
            }
        }
        e->nunits = e->ndecls > 0 ? w + SKIP_ONE : 0;
    }
    return true;
}

static bool ent_in_unit(const cs_entity_t *e, int unit) {
    return bsearch(&unit, e->units, (size_t)e->nunits, sizeof(int), int_cmp) != NULL;
}

/* ── Types by scope and name ─────────────────────────────────────── */

/* Order: scope, name, arity, then the owner -- the shared trees' (the
 * directories' complete declarations, then the parts: CS_POOL) before the
 * assemblies' -- then the entity. */
static int named_key_cmp(const cs_named_t *e, int scope, const char *name, int arity) {
    if (e->scope != scope) {
        return e->scope < scope ? -1 : 1;
    }
    int c = strcmp(e->name, name);
    if (c) {
        return c;
    }
    return (e->arity > arity) - (e->arity < arity);
}

static int named_cmp(const void *a, const void *b) {
    const cs_named_t *x = (const cs_named_t *)a;
    const cs_named_t *y = (const cs_named_t *)b;
    int c = named_key_cmp(x, y->scope, y->name, y->arity);
    if (c) {
        return c;
    }
    if (x->owner != y->owner) {
        return x->owner < y->owner ? -1 : 1;
    }
    return (x->ent > y->ent) - (x->ent < y->ent);
}

/* The entry of `owner` within [lo, hi) (one scope, name and arity: sorted by
 * owner, and an owner has one entity there), or CS_NONE. */
static int named_of_owner(const cs_named_t *arr, int lo, int hi, int owner) {
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (arr[mid].owner < owner) {
            lo = mid + SKIP_ONE;
        } else if (arr[mid].owner > owner) {
            hi = mid;
        } else {
            return mid;
        }
    }
    return CS_NONE;
}

/* Where the assemblies' entries start within [lo, hi): before it stand the
 * shared trees'. */
static int named_first_assembly(const cs_named_t *arr, int lo, int hi) {
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (arr[mid].owner < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* [return, *hi) of arr[0..n): the entries of this scope, name and arity. */
static int named_range(const cs_named_t *arr, int n, int scope, const char *name, int arity,
                       int *hi) {
    int lo = 0;
    int end = n;
    while (lo < end) {
        int mid = lo + ((end - lo) / PAIR_LEN);
        if (named_key_cmp(&arr[mid], scope, name, arity) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    int a = lo;
    end = n;
    while (a < end) {
        int mid = a + ((end - a) / PAIR_LEN);
        if (named_key_cmp(&arr[mid], scope, name, arity) <= 0) {
            a = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    *hi = a;
    return lo;
}

/* The two tables: top-level types by namespace, nested types by their outer
 * entity. */
static bool build_named(cs_index_t *ix) {
    for (int i = 0; i < ix->nents; i++) {
        ix->ntops += ix->ents[i].parent < 0;
    }
    ix->nkids = ix->nents - ix->ntops;
    ix->tops = (cs_named_t *)ix_alloc(ix, (size_t)ix->ntops * sizeof(cs_named_t));
    ix->kids = (cs_named_t *)ix_alloc(ix, (size_t)ix->nkids * sizeof(cs_named_t));
    if (!ix->tops || !ix->kids) {
        return false;
    }
    int t = 0;
    int k = 0;
    for (int i = 0; i < ix->nents; i++) {
        const cs_entity_t *e = &ix->ents[i];
        cs_named_t row = {.scope = e->parent >= 0 ? e->parent : e->ns,
                          .name = e->name,
                          .arity = e->arity,
                          .owner = e->owner,
                          .ent = i};
        if (e->parent >= 0) {
            ix->kids[k++] = row;
        } else {
            ix->tops[t++] = row;
        }
    }
    if (ix->ntops > 1) {
        qsort(ix->tops, (size_t)ix->ntops, sizeof(cs_named_t), named_cmp);
    }
    if (ix->nkids > 1) {
        qsort(ix->kids, (size_t)ix->nkids, sizeof(cs_named_t), named_cmp);
    }
    return true;
}

/* True when an assembly among tops[lo, hi) -- the assemblies' types of the
 * shared declaration `shared`'s name -- holds an implementation of the name
 * that is no part of that declaration: a complete type, or parts where the
 * shared declaration is no partial type. Every contract of the name asks;
 * the assemblies are walked for the first, and the answer is kept with the
 * shared declaration. */
static bool rival_implementation(cs_index_t *ix, int lo, int hi, int shared) {
    cs_entity_t *s = &ix->ents[shared];
    if (!s->rival_known) {
        bool pool = s->owner == CS_POOL;
        for (int i = lo; !s->rival && i < hi; i++) {
            const cs_entity_t *e = &ix->ents[ix->tops[i].ent];
            s->rival = e->has_impl && !(pool && e->impl_partial);
        }
        s->rival_known = true;
    }
    return s->rival;
}

/* The shared trees' declaration that belongs to an assembly's type: its
 * twin. Two rules give a top-level type one:
 *   - parts: a type an assembly's implementation declares `partial` has the
 *     shared trees' parts of that name (CS_POOL) as parts of it -- what a
 *     reference sees of a partial type is its own assembly's parts and the
 *     shared trees'. A complete type has no further parts;
 *   - contract: an assembly whose every declaration of the type stands in a
 *     project directory named `ref` holds only the contract. `ref` is the
 *     .NET convention for reference-assembly sources, and the rule joins a
 *     contract to the one implementation it can belong to: the declaration
 *     the shared trees hold of that full name and arity, when they hold
 *     exactly one (the parts of one tree count as one) and no assembly
 *     holds another implementation of it. The stub is then no second type.
 *     Nothing is chosen between two trees' declarations, two
 *     implementations or two assemblies: no join there.
 * A type nested in a type that has a twin has the one of its name nested in
 * the twin. An entity is made after its outer type's, so one pass in order
 * sees every outer type first. What the twin says of the type (test code,
 * hidden members) is folded into it. */
static void entity_twins(cs_index_t *ix) {
    for (int i = 0; i < ix->nents; i++) {
        cs_entity_t *e = &ix->ents[i];
        int hi = 0;
        if (e->parent >= 0) {
            int outer = ix->ents[e->parent].twin;
            if (outer < 0) {
                continue;
            }
            int lo = named_range(ix->kids, ix->nkids, outer, e->name, e->arity, &hi);
            e->twin = lo < hi ? ix->kids[lo].ent : CS_NONE;
            e->joined = e->twin >= 0 && ix->ents[e->parent].joined;
        } else {
            if (e->owner < 0 || (e->has_impl && !e->impl_partial)) {
                continue;
            }
            int lo = named_range(ix->tops, ix->ntops, e->ns, e->name, e->arity, &hi);
            int split = named_first_assembly(ix->tops, lo, hi);
            int at = CS_NONE;
            if (e->has_impl) {
                at = named_of_owner(ix->tops, lo, split, CS_POOL);
            } else if (split - lo == SKIP_ONE && ix->ents[ix->tops[lo].ent].nunits == SKIP_ONE &&
                       !rival_implementation(ix, split, hi, ix->tops[lo].ent)) {
                at = lo;
                e->joined = true;
            }
            e->twin = at >= 0 ? ix->tops[at].ent : CS_NONE;
        }
        if (e->twin >= 0) {
            cs_entity_t *parts = &ix->ents[e->twin];
            e->incomplete = e->incomplete || parts->incomplete;
            e->any_prod = e->any_prod || parts->any_prod;
            e->all_test = e->all_test && parts->all_test;
            parts->used = true;
            if (!e->has_impl) {
                parts->stub = parts->stub == CS_NONE ? i : CS_AMBIGUOUS;
            }
        }
    }
}

/* ── Node binding ────────────────────────────────────────────────── */

static bool label_is_callable(const char *l) {
    return l && (strcmp(l, "Method") == 0 || strcmp(l, "Function") == 0);
}

static bool label_is_value(const char *l) {
    return l && (strcmp(l, "Field") == 0 || strcmp(l, "Variable") == 0 ||
                 strcmp(l, "Property") == 0 || strcmp(l, "Constant") == 0);
}

/* <module>.<Outer>.<Inner> of type `t` into `buf`, its length into *len.
 * false when it does not fit: such a declaration has no node to be found. */
static bool type_qn(const cs_file_t *f, int t, char *buf, size_t cap, size_t *len) {
    size_t ml = strlen(f->module_qn);
    size_t total = ml;
    for (int x = t; x >= 0; x = f->types[x].outer) {
        total += strlen(f->types[x].name) + SKIP_ONE;
    }
    if (total >= cap) {
        return false;
    }
    size_t w = total;
    buf[w] = '\0';
    for (int x = t; x >= 0; x = f->types[x].outer) {
        size_t nl = strlen(f->types[x].name);
        w -= nl;
        memcpy(buf + w, f->types[x].name, nl);
        buf[--w] = '.';
    }
    memcpy(buf, f->module_qn, ml);
    *len = total;
    return true;
}

/* The node a member's own qualified name has, when it is one a reference to
 * a member of this kind can bind. */
static const cbm_gbuf_node_t *member_node_at(const cbm_gbuf_t *g, const cs_member_t *m, char *qn,
                                             size_t type_len, size_t cap) {
    size_t nl = strlen(m->name);
    if (m->kind == 'o' || m->kind == 'x' || type_len + nl + PAIR_LEN > cap) {
        return NULL; /* operators and indexers have no node */
    }
    qn[type_len] = '.';
    memcpy(qn + type_len + SKIP_ONE, m->name, nl + SKIP_ONE);
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(g, qn);
    qn[type_len] = '\0';
    if (!n) {
        return NULL;
    }
    return (m->kind == 'c' ? label_is_callable(n->label) : label_is_value(n->label)) ? n : NULL;
}

/* Scratch tables of the node pass: emptied for every file. */
typedef struct {
    CBMHashTable *names; /* "<gid>\x1f<name>" -> index + 1 */
    uint32_t sized;      /* what the table is sized for: the most keys it held, or
                          * its initial capacity -- what emptying it walks */
    CBMArena keys;
    int *last; /* per gid: the last type that has it */
    int cap_last;
} cs_node_pass_t;

enum { CS_SCRATCH_MIN = 64, CS_SCRATCH_SLACK = 4 };

/* Empty the scratch table for a file that puts about `need` keys into it.
 * Emptying a table walks every bucket it has, and a table never shrinks: one
 * that a far larger file grew is made anew, so that one large file does not
 * make every later file pay for its size. false when memory ran out. */
static bool node_pass_reset(cs_index_t *ix, cs_node_pass_t *np, int need) {
    uint32_t want = need > CS_SCRATCH_MIN ? (uint32_t)need : CS_SCRATCH_MIN;
    if (np->sized > want * CS_SCRATCH_SLACK) {
        cbm_ht_free(np->names);
        np->names = cbm_ht_create(want);
        np->sized = want;
        if (!np->names) {
            ix->oom = true;
            return false;
        }
    } else {
        cs_scratch_work(np->sized);
        cbm_ht_clear(np->names);
    }
    cbm_arena_rewind(&np->keys); /* the emptied table held the only pointers into it */
    return true;
}

/* Note how many keys the scratch table holds now. */
static void node_pass_held(cs_node_pass_t *np) {
    uint32_t held = cbm_ht_count(np->names);
    np->sized = held > np->sized ? held : np->sized;
}

/* The path group of every type of the file (types with one path -- `Foo` and
 * `Foo<T>`, and what is nested in them under one name -- share the node
 * <module>.<path>), and which declaration owns that node: the last one. */
static bool assign_gids(cs_index_t *ix, cs_file_t *f, cs_node_pass_t *np) {
    if (f->ntypes > np->cap_last) {
        cbm_free(CBM_MEM_CLASS_OTHER, np->last);
        np->last = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)f->ntypes * sizeof(int));
        np->cap_last = np->last ? f->ntypes : 0;
        if (!np->last) {
            ix->oom = true;
            return false;
        }
    }
    if (!node_pass_reset(ix, np, f->ntypes)) {
        return false;
    }
    int gids = 0;
    for (int t = 0; t < f->ntypes; t++) {
        cs_type_t *ty = &f->types[t];
        char key[CS_NAME_BUF + CBM_SZ_16];
        snprintf(key, sizeof(key), "%d\x1f%s", ty->outer >= 0 ? f->types[ty->outer].gid : CS_NONE,
                 ty->name);
        intptr_t v = (intptr_t)cbm_ht_get(np->names, key);
        if (v > 0) {
            ty->gid = (int)(v - SKIP_ONE);
        } else {
            char *k = cbm_arena_strdup(&np->keys, key);
            if (!k) {
                ix->oom = true;
                return false;
            }
            ty->gid = gids++;
            cbm_ht_set(np->names, k, (void *)(intptr_t)(ty->gid + SKIP_ONE));
        }
        np->last[ty->gid] = t;
    }
    for (int t = 0; t < f->ntypes; t++) {
        f->types[t].owns_node = np->last[f->types[t].gid] == t;
    }
    return true;
}

/* The graph node of every type and member of the file that has one a
 * reference may bind. A member's node <module>.<path>.<name> belongs to the
 * last declaration of that qualified name in the file: overloads of one type
 * share it, but when `Foo` and `Foo<T>` both declare the member, the earlier
 * type's member has none of its own (the same rule as for the types). No
 * node either where the slot holds a member of the other kind. */
static bool bind_nodes(cs_index_t *ix, cs_file_t *f, const cbm_gbuf_t *g, cs_node_pass_t *np) {
    if (!assign_gids(ix, f, np)) {
        return false;
    }
    char qn[CS_KEY_BUF];
    size_t len = 0;
    for (int t = 0; t < f->ntypes; t++) {
        cs_type_t *ty = &f->types[t];
        if (ty->owns_node && ty->kind != 'd' && type_qn(f, t, qn, sizeof(qn), &len)) {
            const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(g, qn);
            ty->node = (n && cbm_label_is_type_like(n->label)) ? n : NULL;
        }
    }
    /* which member is the last of its (path, name) */
    node_pass_held(np);
    if (!node_pass_reset(ix, np, f->nmembers)) {
        return false;
    }
    for (int m = 0; m < f->nmembers; m++) {
        char key[(CS_NAME_BUF) + CBM_SZ_16];
        snprintf(key, sizeof(key), "%d\x1f%s", f->types[f->members[m].type].gid,
                 f->members[m].name);
        const char *k = cbm_ht_get_key(np->names, key);
        if (!k) {
            k = cbm_arena_strdup(&np->keys, key);
            if (!k) {
                ix->oom = true;
                return false;
            }
        }
        cbm_ht_set(np->names, k, (void *)(intptr_t)(m + SKIP_ONE));
    }
    node_pass_held(np);
    int cached = CS_NONE;
    bool fits = false;
    for (int m = 0; m < f->nmembers; m++) {
        cs_member_t *mem = &f->members[m];
        char key[(CS_NAME_BUF) + CBM_SZ_16];
        snprintf(key, sizeof(key), "%d\x1f%s", f->types[mem->type].gid, mem->name);
        intptr_t last = (intptr_t)cbm_ht_get(np->names, key);
        if (last <= 0 ||
            f->types[f->members[last - SKIP_ONE].type].entity != f->types[mem->type].entity) {
            continue; /* a declaration of another type took the name's node */
        }
        if (mem->type != cached) {
            cached = mem->type;
            fits = type_qn(f, cached, qn, sizeof(qn), &len);
        }
        mem->node = fits ? member_node_at(g, mem, qn, len, sizeof(qn)) : NULL;
    }
    return true;
}

static unsigned char decl_class(const cs_file_t *f, bool has_node) {
    return (unsigned char)((has_node ? 0 : CS_CLS_NO_NODE) | (f->is_ref ? CS_CLS_REF : 0) |
                           (f->is_test ? CS_CLS_TEST : 0));
}

static int bind_cmp(const void *a, const void *b) {
    const cs_bind_t *x = (const cs_bind_t *)a;
    const cs_bind_t *y = (const cs_bind_t *)b;
    if (x->cls != y->cls) {
        return x->cls < y->cls ? -1 : 1;
    }
    if (x->file != y->file) {
        return x->file < y->file ? -1 : 1;
    }
    return (x->type > y->type) - (x->type < y->type);
}

/* Every entity's declarations by (class, path). */
static bool build_binds(cs_index_t *ix) {
    for (int i = 0; i < ix->nents; i++) {
        cs_entity_t *e = &ix->ents[i];
        e->binds = (cs_bind_t *)ix_alloc(ix, (size_t)e->ndecls * sizeof(cs_bind_t));
        if (!e->binds) {
            return false;
        }
        for (int d = 0; d < e->ndecls; d++) {
            const cs_file_t *f = &ix->files[e->decls[d].file];
            e->binds[d] =
                (cs_bind_t){.file = e->decls[d].file,
                            .type = e->decls[d].type,
                            .cls = decl_class(f, f->types[e->decls[d].type].node != NULL)};
        }
        if (e->ndecls > 1) {
            qsort(e->binds, (size_t)e->ndecls, sizeof(cs_bind_t), bind_cmp);
        }
    }
    return true;
}

/* ── Members by entity and name ──────────────────────────────────── */

/* One member for sorting: the sort keys carry their own comparison data (no
 * global sort context). */
typedef struct {
    cs_mref_t ref;
    const char *name;
    const char *sig;
} cs_mkey_t;

static int mkey_cmp(const void *a, const void *b) {
    const cs_mkey_t *x = (const cs_mkey_t *)a;
    const cs_mkey_t *y = (const cs_mkey_t *)b;
    if (x->ref.ent != y->ref.ent) {
        return x->ref.ent < y->ref.ent ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    if (c) {
        return c;
    }
    if (x->ref.group != y->ref.group) {
        return x->ref.group < y->ref.group ? -1 : 1;
    }
    c = strcmp(x->sig, y->sig);
    if (c) {
        return c;
    }
    if (x->ref.cls != y->ref.cls) {
        return x->ref.cls < y->ref.cls ? -1 : 1;
    }
    if (x->ref.file != y->ref.file) {
        return x->ref.file < y->ref.file ? -1 : 1;
    }
    return (x->ref.midx > y->ref.midx) - (x->ref.midx < y->ref.midx);
}

/* The key of the operators and indexers named `name` that `ent` declares:
 * those outside test code (`prod`), or all of them. false when it does not
 * fit (cannot be for a declared one: parse_member bounds the name). */
static bool special_key(char *key, size_t cap, bool prod, int ent, const char *name) {
    int n = snprintf(key, cap, "%c%d\x1f%s", prod ? 'P' : 'A', ent, name);
    return n > 0 && (size_t)n < cap;
}

/* Note an operator or indexer of `ent`. false when memory ran out. */
static bool special_mark(cs_index_t *ix, int ent, const char *name, unsigned char cls) {
    char key[CS_NAME_BUF + CBM_SZ_64];
    if (!special_key(key, sizeof(key), false, ent, name)) {
        return true;
    }
    if (!ht_mark(ix, ix->specials, key)) {
        return false;
    }
    key[0] = 'P';
    return (cls & CS_CLS_TEST) != 0 || ht_mark(ix, ix->specials, key);
}

/* Every member a name can address, by (entity, name, group, signature,
 * class, path, order). Explicit interface implementations are left out: no
 * name addresses one. */
static bool build_mrefs(cs_index_t *ix) {
    size_t total = 0;
    for (int fi = 0; fi < ix->nfiles; fi++) {
        total += (size_t)ix->files[fi].nmembers;
    }
    cs_mkey_t *keys =
        (cs_mkey_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (total ? total : SKIP_ONE) * sizeof(cs_mkey_t));
    ix->mrefs = (cs_mref_t *)ix_alloc(ix, total * sizeof(cs_mref_t));
    if (!keys || !ix->mrefs) {
        cbm_free(CBM_MEM_CLASS_OTHER, keys);
        ix->oom = true;
        return false;
    }
    size_t n = 0;
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const cs_file_t *f = &ix->files[fi];
        for (int m = 0; m < f->nmembers; m++) {
            const cs_member_t *mem = &f->members[m];
            if (mem->explicit_impl) {
                continue;
            }
            unsigned char group = mem->kind == 'c'
                                      ? SKIP_ONE
                                      : ((mem->kind == 'o' || mem->kind == 'x') ? PAIR_LEN : 0);
            unsigned char cls = decl_class(f, mem->node != NULL);
            int ent = f->types[mem->type].entity;
            if (group == PAIR_LEN && !special_mark(ix, ent, mem->name, cls)) {
                cbm_free(CBM_MEM_CLASS_OTHER, keys);
                return false;
            }
            keys[n++] =
                (cs_mkey_t){.ref = {.ent = ent, .file = fi, .midx = m, .group = group, .cls = cls},
                            .name = mem->name,
                            .sig = mem->sig ? mem->sig : ""};
        }
    }
    if (n > 1) {
        qsort(keys, n, sizeof(cs_mkey_t), mkey_cmp);
    }
    for (size_t i = 0; i < n; i++) {
        ix->mrefs[i] = keys[i].ref;
    }
    ix->nmrefs = (int)n;
    cbm_free(CBM_MEM_CLASS_OTHER, keys);
    return true;
}

static const cs_member_t *mref_member(const cs_index_t *ix, const cs_mref_t *r) {
    return &ix->files[r->file].members[r->midx];
}

/* Order of a member entry against a key (entity, name, and when `group` is
 * not negative: group and signature). */
static int mref_key_cmp(const cs_index_t *ix, const cs_mref_t *r, int ent, const char *name,
                        int group, const char *sig) {
    if (r->ent != ent) {
        return r->ent < ent ? -1 : 1;
    }
    const cs_member_t *m = mref_member(ix, r);
    int c = strcmp(m->name, name);
    if (c || group < 0) {
        return c;
    }
    if (r->group != group) {
        return (int)r->group < group ? -1 : 1;
    }
    return strcmp(m->sig ? m->sig : "", sig);
}

/* [return, *hi): the entity's members named `name`; with `group` >= 0 only
 * those of that group with exactly the signature `sig`. */
static int mref_range(const cs_index_t *ix, int ent, const char *name, int group, const char *sig,
                      int *hi) {
    int lo = 0;
    int end = ix->nmrefs;
    while (lo < end) {
        int mid = lo + ((end - lo) / PAIR_LEN);
        if (mref_key_cmp(ix, &ix->mrefs[mid], ent, name, group, sig) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    int a = lo;
    end = ix->nmrefs;
    while (a < end) {
        int mid = a + ((end - a) / PAIR_LEN);
        if (mref_key_cmp(ix, &ix->mrefs[mid], ent, name, group, sig) <= 0) {
            a = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    *hi = a;
    return lo;
}

/* ── What an assembly's own parts add to a shared declaration ────── */

static int extra_key_cmp(const cs_extra_t *x, int twin, const char *name, int arity) {
    if (x->twin != twin) {
        return x->twin < twin ? -1 : 1;
    }
    int c = strcmp(x->name, name);
    if (c) {
        return c;
    }
    return (x->arity > arity) - (x->arity < arity);
}

static int extra_cmp(const void *a, const void *b) {
    const cs_extra_t *x = (const cs_extra_t *)a;
    const cs_extra_t *y = (const cs_extra_t *)b;
    int c = extra_key_cmp(x, y->twin, y->name, y->arity);
    return c ? c : (x->ent > y->ent) - (x->ent < y->ent);
}

/* The shared trees' declaration beside which the type `ent` stands: a type
 * nested in an assembly's own parts of a type, implemented there, that the
 * shared trees' declaration of that type does not have. CS_NONE for every
 * other type. */
static int extra_type_twin(const cs_index_t *ix, int ent) {
    const cs_entity_t *e = &ix->ents[ent];
    if (e->parent < 0 || !e->has_impl || e->twin >= 0) {
        return CS_NONE;
    }
    return ix->ents[e->parent].twin;
}

/* The same for a member: one an assembly's own parts of a type declare. A
 * stub does not count: it declares what the implementation declares. */
static int extra_member_twin(const cs_index_t *ix, const cs_mref_t *r) {
    return (r->cls & CS_CLS_REF) ? CS_NONE : ix->ents[r->ent].twin;
}

/* The names the assemblies' own parts declare beyond the shared trees'
 * declarations they belong to, by that declaration and name: what a
 * reference that sees the shared declaration without those parts cannot tell
 * from what is not there (unseen_part_declares). One walk over the types and
 * the members, so that no lookup walks the assemblies. false when memory ran
 * out. */
static bool build_extras(cs_index_t *ix) {
    size_t n = 0;
    for (int i = 0; i < ix->nents; i++) {
        n += extra_type_twin(ix, i) >= 0;
    }
    for (int i = 0; i < ix->nmrefs; i++) {
        n += extra_member_twin(ix, &ix->mrefs[i]) >= 0;
    }
    cs_extra_t *arr = (cs_extra_t *)ix_alloc(ix, n * sizeof(cs_extra_t));
    if (!arr) {
        return false;
    }
    size_t w = 0;
    for (int i = 0; i < ix->nents; i++) {
        int twin = extra_type_twin(ix, i);
        if (twin >= 0) {
            arr[w++] = (cs_extra_t){
                .twin = twin, .name = ix->ents[i].name, .arity = ix->ents[i].arity, .ent = i};
        }
    }
    for (int i = 0; i < ix->nmrefs; i++) {
        const cs_mref_t *r = &ix->mrefs[i];
        int twin = extra_member_twin(ix, r);
        if (twin >= 0) {
            arr[w++] = (cs_extra_t){.twin = twin,
                                    .name = mref_member(ix, r)->name,
                                    .arity = CS_NONE,
                                    .ent = CS_NONE,
                                    .prod = !(r->cls & CS_CLS_TEST)};
        }
    }
    if (n > 1) {
        qsort(arr, n, sizeof(cs_extra_t), extra_cmp);
    }
    /* a name's members are one entry */
    size_t out = 0;
    for (size_t i = 0; i < n; i++) {
        if (out > 0 && arr[i].ent < 0 && arr[out - SKIP_ONE].ent < 0 &&
            extra_key_cmp(&arr[out - SKIP_ONE], arr[i].twin, arr[i].name, arr[i].arity) == 0) {
            arr[out - SKIP_ONE].prod = arr[out - SKIP_ONE].prod || arr[i].prod;
        } else {
            arr[out++] = arr[i];
        }
    }
    ix->extras = arr;
    ix->nextras = (int)out;
    return true;
}

/* [return, *hi): the extras of this shared declaration, name and arity
 * (CS_NONE: the name's members). */
static int extra_range(const cs_index_t *ix, int twin, const char *name, int arity, int *hi) {
    int lo = 0;
    int end = ix->nextras;
    while (lo < end) {
        int mid = lo + ((end - lo) / PAIR_LEN);
        if (extra_key_cmp(&ix->extras[mid], twin, name, arity) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    int a = lo;
    end = ix->nextras;
    while (a < end) {
        int mid = a + ((end - a) / PAIR_LEN);
        if (extra_key_cmp(&ix->extras[mid], twin, name, arity) <= 0) {
            a = mid + SKIP_ONE;
        } else {
            end = mid;
        }
    }
    *hi = a;
    return lo;
}

/* ── Import candidate indexes ───────────────────────────────────── */

static int alias_id_cmp(const void *a, const void *b) {
    const cs_alias_id_t *x = (const cs_alias_id_t *)a;
    const cs_alias_id_t *y = (const cs_alias_id_t *)b;
    cs_index_work(SKIP_ONE);
    int order = strcmp(x->name, y->name);
    return order ? order : (x->id > y->id) - (x->id < y->id);
}

static int using_id_cmp(const void *a, const void *b) {
    const cs_using_id_t *x = (const cs_using_id_t *)a;
    const cs_using_id_t *y = (const cs_using_id_t *)b;
    cs_index_work(SKIP_ONE);
    if (x->scope != y->scope) {
        return (x->scope > y->scope) - (x->scope < y->scope);
    }
    return (x->id > y->id) - (x->id < y->id);
}

static int name_scope_cmp(const void *a, const void *b) {
    const cs_name_scope_t *x = (const cs_name_scope_t *)a;
    const cs_name_scope_t *y = (const cs_name_scope_t *)b;
    cs_index_work(SKIP_ONE);
    int order = strcmp(x->name, y->name);
    if (order) {
        return order;
    }
    if (x->top != y->top) {
        return x->top ? 1 : -1;
    }
    return (x->scope > y->scope) - (x->scope < y->scope);
}

static void *import_array(cs_index_t *ix, size_t n, size_t size) {
    if (n > SIZE_MAX / size) {
        ix->oom = true;
        return NULL;
    }
    return n ? ix_alloc(ix, n * size) : NULL;
}

/* One repository-wide pass, never one member-table expansion per import.
 * Include test-only names and extras: the ordinary resolver, not this
 * candidate filter, decides visibility, arity and unseen-part ambiguity. */
static bool build_name_scopes(cs_index_t *ix) {
    size_t n = 0;
    const int counts[] = {ix->ntops, ix->nkids, ix->nmrefs, ix->nextras};
    for (size_t i = 0; i < sizeof(counts) / sizeof(counts[0]); i++) {
        if ((size_t)counts[i] > SIZE_MAX - n) {
            ix->oom = true;
            return false;
        }
        n += (size_t)counts[i];
    }
    cs_name_scope_t *rows = (cs_name_scope_t *)import_array(ix, n, sizeof(*rows));
    if (n && !rows) {
        return false;
    }
    size_t w = 0;
    for (int i = 0; i < ix->ntops; i++) {
        cs_index_work(SKIP_ONE);
        rows[w++] =
            (cs_name_scope_t){.name = ix->tops[i].name, .scope = ix->tops[i].scope, .top = true};
    }
    for (int i = 0; i < ix->nkids; i++) {
        cs_index_work(SKIP_ONE);
        rows[w++] = (cs_name_scope_t){.name = ix->kids[i].name, .scope = ix->kids[i].scope};
    }
    for (int i = 0; i < ix->nmrefs; i++) {
        cs_index_work(SKIP_ONE);
        rows[w++] = (cs_name_scope_t){.name = mref_member(ix, &ix->mrefs[i])->name,
                                      .scope = ix->mrefs[i].ent};
    }
    for (int i = 0; i < ix->nextras; i++) {
        cs_index_work(SKIP_ONE);
        rows[w++] = (cs_name_scope_t){.name = ix->extras[i].name, .scope = ix->extras[i].twin};
    }
    if (w > 1) {
        qsort(rows, w, sizeof(*rows), name_scope_cmp);
    }
    size_t out = 0;
    for (size_t i = 0; i < w; i++) {
        if (!out || name_scope_cmp(&rows[out - SKIP_ONE], &rows[i])) {
            rows[out++] = rows[i];
        }
    }
    ix->name_scopes = rows;
    ix->nname_scopes = out;
    ix->name_scopes_ready = true;
    return true;
}

/* Build only after these directives have resolved. A static import is
 * associated with its own entity and its twin, without copying either
 * entity's declarations into every region that imports it. */
static bool build_using_index(cs_index_t *ix, const cs_using_t *us, int n,
                              cs_using_index_t *index) {
    cs_using_index_t out = {0};
    for (int i = 0; i < n; i++) {
        cs_index_work(SKIP_ONE);
        const cs_using_t *u = &us[i];
        if (u->kind == 'a') {
            out.naliases++;
        } else if (u->kind == 'n' && u->ns >= 0) {
            out.nnamespaces++;
        } else if (u->kind == 's' && u->ent >= 0) {
            size_t extra = ix->ents[u->ent].twin >= 0 ? PAIR_LEN : SKIP_ONE;
            if (extra > SIZE_MAX - out.nentities) {
                ix->oom = true;
                return false;
            }
            out.nentities += extra;
        }
    }
    out.aliases = (cs_alias_id_t *)import_array(ix, out.naliases, sizeof(*out.aliases));
    out.namespaces = (cs_using_id_t *)import_array(ix, out.nnamespaces, sizeof(*out.namespaces));
    out.entities = (cs_using_id_t *)import_array(ix, out.nentities, sizeof(*out.entities));
    if (ix->oom) {
        return false;
    }
    size_t a = 0;
    size_t ns = 0;
    size_t e = 0;
    for (int i = 0; i < n; i++) {
        cs_index_work(SKIP_ONE);
        const cs_using_t *u = &us[i];
        if (u->kind == 'a') {
            out.aliases[a++] = (cs_alias_id_t){.name = u->alias, .id = i};
        } else if (u->kind == 'n' && u->ns >= 0) {
            out.namespaces[ns++] = (cs_using_id_t){.scope = u->ns, .id = i};
        } else if (u->kind == 's' && u->ent >= 0) {
            out.entities[e++] = (cs_using_id_t){.scope = u->ent, .id = i};
            int twin = ix->ents[u->ent].twin;
            if (twin >= 0) {
                out.entities[e++] = (cs_using_id_t){.scope = twin, .id = i};
            }
        }
    }
    if (a > 1) {
        qsort(out.aliases, a, sizeof(*out.aliases), alias_id_cmp);
    }
    if (ns > 1) {
        qsort(out.namespaces, ns, sizeof(*out.namespaces), using_id_cmp);
    }
    if (e > 1) {
        qsort(out.entities, e, sizeof(*out.entities), using_id_cmp);
    }
    out.ready = true;
    *index = out;
    return true;
}

/* ── Choosing the declaration a reference binds ──────────────────── */

/* Number of leading directories two paths share. */
static int common_dir_prefix(const char *a, const char *b) {
    int n = 0;
    for (;;) {
        const char *sa = strchr(a, '/');
        const char *sb = strchr(b, '/');
        if (!sa || !sb) {
            return n;
        }
        size_t la = (size_t)(sa - a);
        if (la != (size_t)(sb - b) || memcmp(a, b, la) != 0) {
            return n;
        }
        n++;
        a = sa + SKIP_ONE;
        b = sb + SKIP_ONE;
    }
}

/* Length of the first `dirs` directories of `path`, each with its '/'. */
static size_t dir_prefix_len(const char *path, int dirs) {
    const char *p = path;
    for (int i = 0; i < dirs; i++) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            break;
        }
        p = slash + SKIP_ONE;
    }
    return (size_t)(p - path);
}

/* Of the declarations [lo, hi) -- one class, so in path order -- the one a
 * reference written in file `src` binds (a choice of presentation among the
 * parts of one type, made without a scan): the one in `src` itself; else the
 * first one in the directory that shares the longest leading path with it;
 * else the first. */
static int nearest_bind(const cs_index_t *ix, const cs_bind_t *arr, int lo, int hi, int src) {
    int a = lo;
    int b = hi;
    while (a < b) {
        int mid = a + ((b - a) / PAIR_LEN);
        if (arr[mid].file < src) {
            a = mid + SKIP_ONE;
        } else {
            b = mid;
        }
    }
    if (a < hi && arr[a].file == src) {
        return a;
    }
    const char *sp = ix->files[src].rel_path;
    int left = a > lo ? common_dir_prefix(ix->files[arr[a - SKIP_ONE].file].rel_path, sp) : 0;
    int right = a < hi ? common_dir_prefix(ix->files[arr[a].file].rel_path, sp) : 0;
    int best = left > right ? left : right;
    if (best == 0) {
        return lo;
    }
    /* the entries before `a` that share those directories are the last ones
     * before it: the first of them */
    size_t plen = dir_prefix_len(sp, best);
    int x = lo;
    int y = a;
    while (x < y) {
        int mid = x + ((y - x) / PAIR_LEN);
        if (strncmp(ix->files[arr[mid].file].rel_path, sp, plen) < 0) {
            x = mid + SKIP_ONE;
        } else {
            y = mid;
        }
    }
    return x;
}

/* [return, *hi): the declarations of class `cls` among arr[0, n), which is
 * sorted by class first. */
static int class_range(const cs_bind_t *arr, int n, unsigned char cls, int *out_hi) {
    int a = 0;
    int b = n;
    while (a < b) {
        int mid = a + ((b - a) / PAIR_LEN);
        if (arr[mid].cls < cls) {
            a = mid + SKIP_ONE;
        } else {
            b = mid;
        }
    }
    int first = a;
    b = n;
    while (a < b) {
        int mid = a + ((b - a) / PAIR_LEN);
        if (arr[mid].cls <= cls) {
            a = mid + SKIP_ONE;
        } else {
            b = mid;
        }
    }
    *out_hi = a;
    return first;
}

/* True when a declaration in file `fa` is the better one to bind than one in
 * `fb`, for a reference written in `src`: its own file, then the longer
 * shared path, then the path order. */
static bool file_nearer(const cs_index_t *ix, int fa, int fb, int src) {
    if ((fa == src) != (fb == src)) {
        return fa == src;
    }
    const char *sp = ix->files[src].rel_path;
    int pa = common_dir_prefix(ix->files[fa].rel_path, sp);
    int pb = common_dir_prefix(ix->files[fb].rel_path, sp);
    return pa != pb ? pa > pb : fa < fb;
}

/* ── Resolution context ──────────────────────────────────────────── */

enum { CS_OK = 0, CS_UNRES, CS_LOCAL };

typedef struct {
    int st;
    const cbm_gbuf_node_t *node;
    bool exact;
    int reason;
    cs_why_t why; /* of an ambiguous one */
} cs_res_t;

typedef struct cs_memo cs_memo_t;

typedef struct {
    const cs_index_t *ix;
    int file;
    const cs_file_t *f;
    int type;   /* the innermost type declaration around the definition, or CS_NONE */
    int member; /* the documented member, when it has type parameters; CS_NONE */
    int region; /* the namespace declaration around it */
    int unit;   /* the file's project, or its directory in a shared tree */
    int group;  /* its assembly; CS_NONE in a shared tree */
    bool prod;  /* product code: test-only declarations are not bound */
    bool glob;  /* the reference starts with global:: */
    /* The namespace declaration whose own aliases and usings are not asked
     * (CS_NONE: none): a using directive's target is resolved without the
     * directives beside it. */
    int skip_region;
    /* Set when a namespace this context does not see was passed over (NULL:
     * nobody asks). */
    bool *passed_over;
    /* What the using directives of a scope level gave a query, remembered
     * for the pass the caller runs (lookup); NULL: nothing is remembered. */
    cs_memo_t *memo;
} cs_ctx_t;

static cs_res_t res_edge(const cbm_gbuf_node_t *n, bool exact) {
    return (cs_res_t){.st = CS_OK, .node = n, .exact = exact};
}

static cs_res_t res_unres(int reason) {
    return (cs_res_t){.st = CS_UNRES, .reason = reason};
}

static cs_res_t res_ambiguous(cs_why_t why) {
    return (cs_res_t){.st = CS_UNRES, .reason = CBM_DOCLINK_REASON_AMBIGUOUS, .why = why};
}

static bool res_is(const cs_res_t *r, int reason) {
    return r->st == CS_UNRES && r->reason == reason;
}

/* What choosing among declarations came to. */
typedef enum { CS_PICK_NODE = 0, CS_PICK_GAP, CS_PICK_AMBIGUOUS } cs_pick_t;

/* An entity and the shared trees' declaration that belongs to it: what a
 * reference sees of one type is in at most these two. */
typedef struct {
    int ent[PAIR_LEN];
    int n;
} cs_view_t;

static cs_view_t view_of(const cs_index_t *ix, int ent) {
    int twin = ix->ents[ent].twin;
    return (cs_view_t){.ent = {ent, twin}, .n = twin >= 0 ? PAIR_LEN : SKIP_ONE};
}

/* The node of the nearest of `e`'s own declarations of one class -- an
 * implementation's, or (`stub`) a reference assembly's stub's -- that has
 * one; never a test declaration's for product code. NULL when none has. */
static const cbm_gbuf_node_t *class_node(const cs_ctx_t *c, const cs_entity_t *e, bool stub) {
    const cs_index_t *ix = c->ix;
    const cs_bind_t *best = NULL;
    for (int test = 0; test <= (c->prod ? 0 : SKIP_ONE); test++) {
        int b = 0;
        int a =
            class_range(e->binds, e->ndecls,
                        (unsigned char)((stub ? CS_CLS_REF : 0) | (test ? CS_CLS_TEST : 0)), &b);
        if (a >= b) {
            continue;
        }
        const cs_bind_t *at = &e->binds[nearest_bind(ix, e->binds, a, b, c->file)];
        if (!best || file_nearer(ix, at->file, best->file, c->file)) {
            best = at;
        }
    }
    return best ? ix->files[best->file].types[best->type].node : NULL;
}

/* True when one of `e`'s own declarations is in view, with a node or
 * without: for product code one that is not test code. */
static bool declared_in_view(const cs_ctx_t *c, const cs_entity_t *e) {
    for (int cls = 0; cls < CS_CLS_COUNT; cls++) {
        int b = 0;
        if (!(c->prod && (cls & CS_CLS_TEST)) &&
            class_range(e->binds, e->ndecls, (unsigned char)cls, &b) < b) {
            return true;
        }
    }
    return false;
}

/* ── Visibility and candidates ───────────────────────────────────── */

static bool ent_visible(const cs_ctx_t *c, int ent) {
    const cs_index_t *ix = c->ix;
    const cs_entity_t *e = &ix->ents[ent];
    if (c->prod) {
        return e->any_prod;
    }
    /* test code: a global-namespace test type (and what it holds) is local
     * to its own program */
    int top = ent;
    while (ix->ents[top].parent >= 0) {
        top = ix->ents[top].parent;
    }
    if (ix->ents[top].all_test && ix->ents[top].ns == 0) {
        return ent_in_unit(&ix->ents[top], c->unit);
    }
    return true;
}

/* What a name was found to be at one scope level. */
typedef struct {
    char kind; /* T type, M member(s) of entity `id`, N namespace, L type parameter, X outside */
    int id;
} cs_cand_t;

typedef struct {
    cs_cand_t first;
    int n;             /* distinct candidates of the deciding level; > 1 is ambiguous */
    bool invisible;    /* a level had only what this code may not bind (test code) */
    bool exact;        /* decided by an alias */
    bool in_namespace; /* decided by an enclosing namespace's own types and namespaces */
    bool joined;       /* one type only because a contract is joined to its implementation */
    cs_why_t why;      /* what made several of them, when it was not the scope's rules */
} cs_found_t;

/* The result for a name with several candidates. */
static cs_res_t found_ambiguous(const cs_found_t *fd) {
    return res_ambiguous(fd->why);
}

static void found_add(cs_found_t *fd, char kind, int id) {
    if (fd->n > 0 && fd->first.kind == kind && fd->first.id == id) {
        return;
    }
    if (fd->n == 0) {
        fd->first = (cs_cand_t){.kind = kind, .id = id};
    }
    fd->n++;
}

static void found_add_type(const cs_ctx_t *c, cs_found_t *fd, int ent) {
    if (ent_visible(c, ent)) {
        found_add(fd, 'T', ent);
    } else {
        fd->invisible = true;
    }
}

/* The namespace `seg` under `parent` as this context sees it. For product
 * code a namespace that only test code declares does not exist: it is of no
 * program product code is compiled with, so its name neither stands in the
 * way of what the scope has further out nor makes a name under it the
 * repository's. CS_NONE when none is in view. That one was passed over is
 * noted in the context (what the reference names may then be test code's:
 * resolve_ref asks). */
static int ns_in_view(const cs_ctx_t *c, int parent, const char *seg, size_t len) {
    int child = ns_find(c->ix, parent, seg, len);
    if (child >= 0 && c->prod && !c->ix->nss[child].prod) {
        if (c->passed_over) {
            *c->passed_over = true;
        }
        return CS_NONE;
    }
    return child;
}

/* Every type among tops[lo, hi) -- other owners' types of one name -- is a
 * candidate: one binds, several are ambiguous (`why`). Nothing chooses
 * between two of them. */
static void add_owners(const cs_ctx_t *c, int lo, int hi, cs_why_t why, cs_found_t *fd) {
    if (hi - lo > CS_MAX_FOREIGN) {
        fd->n += PAIR_LEN; /* more of them than one lookup compares: ambiguous */
        fd->why = CS_WHY_LIMIT;
        return;
    }
    int before = fd->n;
    for (int i = lo; i < hi; i++) {
        found_add_type(c, fd, c->ix->tops[i].ent);
    }
    if (fd->n - before > SKIP_ONE) {
        fd->why = why;
    }
}

/* The entry among tops[lo, split) -- the shared trees' declarations of one
 * name -- that is the context's own: what its own tree declares (a complete
 * type, or a part of the shared trees' partial one). CS_NONE for a file of a
 * project, and when the tree declares none. */
static int own_shared(const cs_ctx_t *c, int lo, int split) {
    const cs_index_t *ix = c->ix;
    if (c->group >= 0 || c->unit < 0 || lo >= split) {
        return CS_NONE;
    }
    int mine = named_of_owner(ix->tops, lo, split, shared_owner(c->unit));
    int last = split - SKIP_ONE; /* the parts stand last among the shared trees' */
    if (mine < 0 && ix->tops[last].owner == CS_POOL &&
        ent_in_unit(&ix->ents[ix->tops[last].ent], c->unit)) {
        mine = last;
    }
    return mine;
}

/* The top-level types of one namespace, name and arity a reference from this
 * context can mean: its own assembly's type (for a file of a shared tree:
 * what its own tree declares). Else every shared tree's and every other
 * assembly's type of that name alike: one binds, several are ambiguous. An
 * assembly's type that has the shared trees' declaration as its twin is that
 * declaration seen from the assembly, and no second type. */
static void add_top_types(const cs_ctx_t *c, int ns, const char *name, int arity, cs_found_t *fd) {
    const cs_named_t *arr = c->ix->tops;
    int hi = 0;
    int lo = named_range(arr, c->ix->ntops, ns, name, arity, &hi);
    if (lo >= hi) {
        return;
    }
    int split = named_first_assembly(arr, lo, hi);
    int mine = c->group >= 0 ? named_of_owner(arr, split, hi, c->group) : own_shared(c, lo, split);
    if (mine >= 0) {
        found_add_type(c, fd, arr[mine].ent);
        return;
    }
    int before = fd->n;
    add_owners(c, lo, split, CS_WHY_SHARED, fd);
    int shared = fd->n - before;
    if (hi - split > CS_MAX_FOREIGN) {
        fd->n += PAIR_LEN; /* more of them than one lookup compares: ambiguous */
        fd->why = CS_WHY_LIMIT;
        return;
    }
    bool joined = false;
    for (int i = split; i < hi; i++) {
        const cs_entity_t *e = &c->ix->ents[arr[i].ent];
        if (e->twin < 0 || !ent_visible(c, e->twin)) {
            found_add_type(c, fd, arr[i].ent);
        } else {
            joined = joined || e->joined;
        }
    }
    if (fd->n - before > SKIP_ONE && shared < PAIR_LEN) {
        fd->why = CS_WHY_ASSEMBLIES;
    }
    /* the name is one type only because a stub was joined to it */
    fd->joined = fd->joined || (joined && fd->n - before == SKIP_ONE);
}

/* The type of this name and arity nested in `outer`: declared in that type
 * itself, or in the shared trees' declaration that belongs to it. */
static void add_nested_types(const cs_ctx_t *c, int outer, const char *name, int arity,
                             cs_found_t *fd) {
    const cs_index_t *ix = c->ix;
    cs_view_t v = view_of(ix, outer);
    for (int k = 0; k < v.n; k++) {
        int hi = 0;
        int lo = named_range(ix->kids, ix->nkids, v.ent[k], name, arity, &hi);
        if (lo < hi) {
            /* one entity per outer type; the one of the type itself has the
             * twin's as its own twin */
            found_add_type(c, fd, ix->kids[lo].ent);
            /* a contract's nested type that only the joined implementation has */
            fd->joined = fd->joined || (k > 0 && ix->ents[outer].joined);
            return;
        }
    }
}

static void add_types(const cs_ctx_t *c, bool top, int scope, const char *name, int arity,
                      cs_found_t *fd) {
    if (top) {
        add_top_types(c, scope, name, arity, fd);
    } else {
        add_nested_types(c, scope, name, arity, fd);
    }
}

/* What a lookup asks one scope for. A parameter list is no part of it: the
 * compiler finds the name first and matches the overloads afterwards. */
typedef struct {
    const char *name;
    int arity;       /* written type arguments; CS_ARITY_NONE when none */
    bool types_only; /* a qualifier: a namespace or a type */
    bool statics;    /* through `using static`: static members only */
    bool ctors;      /* the type's own name: its constructors (`statics`: the static one) */
    char kind;       /* 0, or the only member kind a doc ID names: c v p e */
} cs_query_t;

static int type_arity(int written) {
    return written > 0 ? written : 0;
}

/* Members of one type under one key: those the entity declares and those the
 * shared trees' parts that belong to it declare -- at most two ranges of the
 * member table. */
typedef struct {
    int lo[PAIR_LEN];
    int hi[PAIR_LEN];
    int n;
    int total;
} cs_mspans_t;

/* The members of `ent` named `name`; with `group` >= 0 only those of that
 * group with exactly the signature `sig`. */
static cs_mspans_t mref_spans(const cs_index_t *ix, int ent, const char *name, int group,
                              const char *sig) {
    cs_mspans_t sp = {0};
    cs_view_t v = view_of(ix, ent);
    for (int k = 0; k < v.n; k++) {
        int hi = 0;
        int lo = mref_range(ix, v.ent[k], name, group, sig, &hi);
        if (lo < hi) {
            sp.lo[sp.n] = lo;
            sp.hi[sp.n] = hi;
            sp.n++;
            sp.total += hi - lo;
        }
    }
    return sp;
}

/* The members `ent` declares under the query's name. A type's own name names
 * its constructors, which no lookup by name finds. */
static cs_mspans_t member_spans(const cs_ctx_t *c, int ent, const cs_query_t *q) {
    if (!q->ctors && strcmp(q->name, c->ix->ents[ent].name) == 0) {
        return (cs_mspans_t){0};
    }
    return mref_spans(c->ix, ent, q->name, CS_NONE, NULL);
}

/* True when the member takes part in a lookup of this query: methods of any
 * arity for a name without type arguments, of that arity with them; every
 * other member only without them. */
static bool member_viable(const cs_ctx_t *c, const cs_mref_t *r, const cs_query_t *q) {
    const cs_member_t *m = mref_member(c->ix, r);
    if (r->group == PAIR_LEN || (q->kind && m->kind != q->kind)) {
        return false; /* an operator or indexer has no identifier */
    }
    if (q->ctors) {
        return m->kind == 'c' && m->is_static == q->statics;
    }
    if (q->statics && !m->is_static) {
        return false;
    }
    return m->kind == 'c' ? (q->arity <= 0 || m->arity == q->arity) : q->arity <= 0;
}

static bool mref_visible(const cs_ctx_t *c, const cs_mref_t *r) {
    return !(c->prod && (r->cls & CS_CLS_TEST));
}

enum { CS_HAS_NONE = 0, CS_HAS_VISIBLE, CS_HAS_INVISIBLE };

/* Does `ent` itself declare a member the query finds? A group larger than a
 * lookup compares counts as there (and comes out ambiguous). */
static int members_named(const cs_ctx_t *c, int ent, const cs_query_t *q) {
    const cs_index_t *ix = c->ix;
    cs_mspans_t sp = member_spans(c, ent, q);
    if (sp.total > CS_MAX_OVERLOADS) {
        return CS_HAS_VISIBLE;
    }
    int state = CS_HAS_NONE;
    for (int s = 0; s < sp.n; s++) {
        for (int i = sp.lo[s]; i < sp.hi[s]; i++) {
            cs_work(SKIP_ONE);
            if (!member_viable(c, &ix->mrefs[i], q)) {
                continue;
            }
            if (mref_visible(c, &ix->mrefs[i])) {
                return CS_HAS_VISIBLE;
            }
            state = CS_HAS_INVISIBLE;
        }
    }
    return state;
}

/* True when `ent` -- or the shared trees' declaration that belongs to it --
 * declares an operator or an indexer by this name (its token, or `this`)
 * that the context may see: asked of what was noted when the members were
 * listed (special_mark), whatever the number of such members. */
static bool special_declared(const cs_ctx_t *c, int ent, const char *name) {
    const cs_index_t *ix = c->ix;
    cs_view_t v = view_of(ix, ent);
    char key[CS_NAME_BUF + CBM_SZ_64];
    for (int k = 0; k < v.n; k++) {
        cs_work(SKIP_ONE);
        if (special_key(key, sizeof(key), c->prod, v.ent[k], name) &&
            cbm_ht_get(ix->specials, key)) {
            return true;
        }
    }
    return false;
}

/* The supertypes of `ent`, nearest first: a class's base classes, an
 * interface's base interfaces. At most CS_MAX_SUPERS; *more when the
 * hierarchy goes on behind them. (The compiler's cref lookup never comes
 * here: see CS_BIND_INHERITED.) */
static int supers_of(const cs_index_t *ix, int ent, int *out, bool *more) {
    int n = 0;
    *more = false;
    bool iface = ix->ents[ent].kind == 'i';
    int cur = ent;
    for (int head = -SKIP_ONE; head < n; head++) {
        if (head >= 0) {
            cur = out[head];
        }
        /* the bases its own declarations write, and those the shared trees'
         * parts of it write */
        cs_view_t v = view_of(ix, cur);
        for (int k = 0; k < v.n; k++) {
            const cs_entity_t *e = &ix->ents[v.ent[k]];
            for (int b = 0; b < e->nbases; b++) {
                char bk = ix->ents[e->bases[b]].kind;
                if (iface ? bk != 'i' : !(bk == 'c' || bk == 'r')) {
                    continue;
                }
                bool seen = e->bases[b] == ent;
                for (int j = 0; !seen && j < n; j++) {
                    seen = out[j] == e->bases[b];
                }
                if (seen) {
                    continue;
                }
                if (n >= CS_MAX_SUPERS) {
                    *more = true;
                    return n;
                }
                out[n++] = e->bases[b];
            }
        }
    }
    return n;
}

/* ── Reference syntax ────────────────────────────────────────────── */

typedef struct {
    char name[CS_NAME_BUF];
    int arity;         /* CS_ARITY_NONE: no type arguments written */
    const char *targs; /* the type arguments as written, between the brackets; NULL: none */
    size_t targs_len;
} cs_seg_t;

enum { CS_OP_NAME = 16 };

typedef struct {
    char text[CS_REF_BUF]; /* the reference, trimmed: the segments' type arguments are in it */
    cs_seg_t segs[CS_MAX_SEGS];
    int nsegs;
    bool has_params;
    char params[CS_MAX_PARAMS][CS_PARAM_BUF];
    int nparams;
    char sig[CS_MAX_PARAMS * CS_PARAM_BUF]; /* the parameters as a declaration's signature */
    bool sig_unknown;                       /* a parameter type nothing is known about */
    char docid;                             /* 0, or T M P F E N; O: DocFX's overload group */
    bool glob;                              /* starts at the global namespace */
    bool op;                                /* an operator, a conversion or an indexer */
    char op_name[CS_OP_NAME];               /* the name the scope records it under */
    bool maybe_indexer; /* `Item(...)`: a method of that name, else the indexer */
    bool keyword;       /* a keyword alias, rewritten to its System type */
} cs_ref_t;

static bool is_open_bracket(char c) {
    return c == '<' || c == '{' || c == '[' || c == '(';
}

static bool is_close_bracket(char c) {
    return c == '>' || c == '}' || c == ']' || c == ')';
}

/* Index just past the bracket group opened at s[i] (< { [ ( nest together). */
static size_t group_end(const char *s, size_t n, size_t i) {
    int depth = 0;
    for (size_t k = i; k < n; k++) {
        if (is_open_bracket(s[k])) {
            depth++;
        } else if (is_close_bracket(s[k]) && --depth == 0) {
            return k + SKIP_ONE;
        }
    }
    return n;
}

/* Count top-level comma-separated items of s[0..n). */
static int count_top(const char *s, size_t n) {
    bool any = false;
    int items = 1;
    int depth = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (is_open_bracket(c)) {
            depth++;
        } else if (is_close_bracket(c)) {
            depth--;
        } else if (c == ',' && depth == 0) {
            items++;
        }
        any = any || !isspace((unsigned char)c);
    }
    return any ? items : 0;
}

/* An identifier as the scanner takes one: a letter of any script, a digit
 * after the first position, an underscore. */
static bool ident_ok(const char *s) {
    if (strcmp(s, "#ctor") == 0 || strcmp(s, "#cctor") == 0) {
        return true;
    }
    unsigned char first = (unsigned char)s[0];
    if (!(isalpha(first) || first == '_' || first >= CBM_SZ_128)) {
        return false;
    }
    for (const char *p = s + SKIP_ONE; *p; p++) {
        unsigned char ch = (unsigned char)*p;
        if (!(isalnum(ch) || ch == '_' || ch >= CBM_SZ_128)) {
            return false;
        }
    }
    return true;
}

/* One dotted segment: `Name`, `Name{T,U}`, `Name<T>`, `Name``2`. */
static bool parse_seg(const char *s, size_t n, cs_seg_t *out) {
    while (n > 0 && isspace((unsigned char)*s)) {
        s++;
        n--;
    }
    while (n > 0 && isspace((unsigned char)s[n - SKIP_ONE])) {
        n--;
    }
    out->arity = CS_ARITY_NONE;
    out->targs = NULL;
    out->targs_len = 0;
    size_t name_end = n;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '`') {
            name_end = i;
            size_t d = i;
            while (d < n && s[d] == '`') {
                d++;
            }
            out->arity = atoi(s + d);
            break;
        }
        if (s[i] == '{' || s[i] == '<') {
            name_end = i;
            size_t e = group_end(s, n, i);
            if (e < n) {
                return false; /* text after the type arguments: no name */
            }
            size_t inner = e > i + PAIR_LEN ? e - i - PAIR_LEN : 0;
            out->arity = count_top(s + i + SKIP_ONE, inner);
            out->targs = s + i + SKIP_ONE;
            out->targs_len = inner;
            break;
        }
    }
    const char *name = s;
    if (name_end > 0 && name[0] == '@') {
        name++;
        name_end--;
    }
    if (name_end == 0 || name_end >= sizeof(out->name)) {
        return false;
    }
    memcpy(out->name, name, name_end);
    out->name[name_end] = '\0';
    return ident_ok(out->name);
}

/* Split a dotted path (dots inside type-argument groups do not split). */
/* A path whose brackets do not pair is not read at all: its last segment
 * would be dropped and the reference resolved by the segments before it. */
static bool parse_path(const char *s, size_t n, cs_seg_t *segs, int *nsegs) {
    *nsegs = 0;
    size_t start = 0;
    int depth = 0;
    for (size_t i = 0; i <= n; i++) {
        char c = i < n ? s[i] : '.';
        if (is_open_bracket(c)) {
            depth++;
        } else if (is_close_bracket(c)) {
            if (--depth < 0) {
                return false;
            }
        } else if (c == '.' && depth == 0) {
            if (*nsegs >= CS_MAX_SEGS || !parse_seg(s + start, i - start, &segs[*nsegs])) {
                return false;
            }
            (*nsegs)++;
            start = i + SKIP_ONE;
        }
    }
    return depth == 0 && *nsegs > 0;
}

static const char *const CS_KEYWORD_TYPES[][2] = {
    {"int", "Int32"},     {"string", "String"},   {"object", "Object"}, {"bool", "Boolean"},
    {"byte", "Byte"},     {"sbyte", "SByte"},     {"short", "Int16"},   {"ushort", "UInt16"},
    {"uint", "UInt32"},   {"long", "Int64"},      {"ulong", "UInt64"},  {"float", "Single"},
    {"double", "Double"}, {"decimal", "Decimal"}, {"char", "Char"},     {"nint", "IntPtr"},
    {"nuint", "UIntPtr"}, {"void", "Void"},
};

static const char *keyword_type(const char *s) {
    for (size_t i = 0; i < sizeof(CS_KEYWORD_TYPES) / sizeof(CS_KEYWORD_TYPES[0]); i++) {
        if (strcmp(s, CS_KEYWORD_TYPES[i][0]) == 0) {
            return CS_KEYWORD_TYPES[i][1];
        }
    }
    return NULL;
}

/* The token a metadata operator name stands for (`op_Addition` -> `+`): the
 * name the scope records an operator under. NULL for a name that is none. */
static const char *operator_token(const char *name, size_t len) {
    static const struct {
        const char *meta;
        const char *token;
    } ops[] = {
        {"Addition", "+"},
        {"UnaryPlus", "+"},
        {"Subtraction", "-"},
        {"UnaryNegation", "-"},
        {"Multiply", "*"},
        {"Division", "/"},
        {"Modulus", "%"},
        {"BitwiseAnd", "&"},
        {"BitwiseOr", "|"},
        {"ExclusiveOr", "^"},
        {"LeftShift", "<<"},
        {"RightShift", ">>"},
        {"UnsignedRightShift", ">>>"},
        {"Equality", "=="},
        {"Inequality", "!="},
        {"LessThan", "<"},
        {"GreaterThan", ">"},
        {"LessThanOrEqual", "<="},
        {"GreaterThanOrEqual", ">="},
        {"LogicalNot", "!"},
        {"OnesComplement", "~"},
        {"Increment", "++"},
        {"Decrement", "--"},
        {"True", "true"},
        {"False", "false"},
        {"Implicit", "implicit"},
        {"Explicit", "explicit"},
        {"CheckedAddition", "+"},
        {"CheckedSubtraction", "-"},
        {"CheckedMultiply", "*"},
        {"CheckedDivision", "/"},
        {"CheckedUnaryNegation", "-"},
        {"CheckedIncrement", "++"},
        {"CheckedDecrement", "--"},
        {"CheckedExplicit", "explicit"},
    };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        if (strlen(ops[i].meta) == len && memcmp(ops[i].meta, name, len) == 0) {
            return ops[i].token;
        }
    }
    return NULL;
}

static bool starts_word(const char *s, const char *word) {
    size_t n = strlen(word);
    return strncmp(s, word, n) == 0 && !isalnum((unsigned char)s[n]) && s[n] != '_';
}

static const char *skip_blanks(const char *s) {
    while (*s == ' ') {
        s++;
    }
    return s;
}

/* The operator, conversion or indexer written at `q` (the start of a
 * segment): its recorded name into `name`. false when `q` starts none. */
static bool operator_at(const char *q, char name[CS_OP_NAME]) {
    static const char op_prefix[] = "op_";
    q = skip_blanks(q);
    bool implicit = starts_word(q, "implicit");
    if (implicit || starts_word(q, "explicit")) {
        if (!starts_word(skip_blanks(q + strlen("implicit")), "operator")) {
            return false;
        }
        snprintf(name, CS_OP_NAME, "%s", implicit ? "implicit" : "explicit");
        return true;
    }
    if (starts_word(q, "operator")) {
        const char *t = skip_blanks(q + strlen("operator"));
        if (starts_word(t, "checked")) {
            t = skip_blanks(t + strlen("checked"));
        }
        size_t n = 0;
        while (t[n] && t[n] != '(' && t[n] != ' ' && n + SKIP_ONE < CS_OP_NAME) {
            n++;
        }
        snprintf(name, CS_OP_NAME, "%.*s", (int)n, n > 0 ? t : "?");
        return true;
    }
    if (starts_word(q, "this")) {
        const char *t = skip_blanks(q + strlen("this"));
        if (*t != '[' && *t != '\0') {
            return false;
        }
        snprintf(name, CS_OP_NAME, "this");
        return true;
    }
    size_t pl = sizeof(op_prefix) - SKIP_ONE;
    if (strncmp(q, op_prefix, pl) == 0 && isupper((unsigned char)q[pl])) {
        size_t n = 0;
        while (isalnum((unsigned char)q[pl + n])) {
            n++;
        }
        const char *token = operator_token(q + pl, n);
        if (!token) {
            return false; /* `op_Custom`: an identifier like any other */
        }
        snprintf(name, CS_OP_NAME, "%s", token);
        return true;
    }
    return false;
}

/* Where the reference's last segment starts an operator, a conversion or an
 * indexer: its offset in `s`, or CS_NONE. */
static int operator_start(const char *s, char name[CS_OP_NAME]) {
    int depth = 0;
    for (size_t i = 0; s[i]; i++) {
        if ((i == 0 || (s[i - SKIP_ONE] == '.' && depth == 0)) && operator_at(s + i, name)) {
            return (int)i;
        }
        if (is_open_bracket(s[i])) {
            depth++;
        } else if (is_close_bracket(s[i])) {
            depth--;
        }
    }
    return CS_NONE;
}

/* A doc ID writes a type parameter by its position: `0 (of the type and the
 * types around it), ``0 (of the method). Such a parameter is kept as it
 * stands (without a by-reference mark): its position is what is compared. */
static bool slot_param(const char *s, size_t n, char *out, size_t cap) {
    while (n > 0 && isspace((unsigned char)*s)) {
        s++;
        n--;
    }
    while (n > 0 && (isspace((unsigned char)s[n - SKIP_ONE]) || s[n - SKIP_ONE] == '@')) {
        n--;
    }
    size_t ticks = 0;
    while (ticks < n && s[ticks] == '`') {
        ticks++;
    }
    if (ticks == 0 || ticks > PAIR_LEN || ticks >= n || !isdigit((unsigned char)s[ticks])) {
        return false;
    }
    size_t d = ticks;
    while (d < n && isdigit((unsigned char)s[d])) {
        d++;
    }
    for (size_t k = d; k < n; k++) {
        if (!strchr("[],*", s[k])) {
            return false;
        }
    }
    if (n >= cap) {
        return false;
    }
    memcpy(out, s, n);
    out[n] = '\0';
    return true;
}

/* The written parameter list s[from, to) into the reference: every parameter
 * normalized, and all of them as one signature. false for more parameters
 * than a reference may have. */
static bool parse_params(cs_ref_t *r, const char *s, size_t from, size_t to) {
    bool any = false;
    for (size_t i = from; i < to && !any; i++) {
        any = !isspace((unsigned char)s[i]);
    }
    size_t start = from;
    int depth = 0;
    size_t w = 0;
    for (size_t i = from; any && i <= to; i++) {
        char c = i < to ? s[i] : ',';
        if (is_open_bracket(c)) {
            depth++;
        } else if (is_close_bracket(c)) {
            if (--depth < 0) {
                return false; /* a bracket that closes nothing */
            }
        } else if (c == ',' && depth == 0) {
            if (r->nparams >= CS_MAX_PARAMS) {
                return false;
            }
            char *p = r->params[r->nparams++];
            if (!slot_param(s + start, i - start, p, CS_PARAM_BUF)) {
                (void)cbm_doclink_cs_norm_type(s + start, i - start, p, CS_PARAM_BUF);
            }
            size_t pl = strlen(p);
            r->sig_unknown = r->sig_unknown || strchr(p, '?') != NULL;
            if (w > 0) {
                r->sig[w++] = '|';
            }
            memcpy(r->sig + w, p, pl);
            w += pl;
            start = i + SKIP_ONE;
        }
    }
    r->sig[w] = '\0';
    /* a bracket left open would have swallowed the last parameter: the
     * reference would be matched without it */
    return depth == 0;
}

/* A written reference into its parts. false for one this code does not
 * understand -- and for one longer than CS_REF_BUF, which is never cut and
 * resolved by what is left of it. */
static bool parse_cref(const char *raw, cs_ref_t *r) {
    static const char global_prefix[] = "global::";
    r->nsegs = 0;
    r->nparams = 0;
    r->has_params = r->sig_unknown = r->glob = r->op = r->maybe_indexer = r->keyword = false;
    r->docid = 0;
    r->sig[0] = '\0';
    r->op_name[0] = '\0';
    const char *in = raw ? raw : "";
    while (isspace((unsigned char)*in)) {
        in++;
    }
    size_t n = strlen(in);
    while (n > 0 && isspace((unsigned char)in[n - SKIP_ONE])) {
        n--;
    }
    if (n == 0 || n >= sizeof(r->text)) {
        return false;
    }
    memcpy(r->text, in, n);
    r->text[n] = '\0';
    char *s = r->text;
    if (n > PAIR_LEN && s[1] == ':' && strchr("TMPFENO!", s[0])) {
        char k = s[0];
        if (k == '!') {
            return false; /* the compiler's error marker: it could not bind the reference */
        }
        s += PAIR_LEN;
        while (isspace((unsigned char)*s)) {
            s++;
        }
        if (k == 'O') { /* DocFX's overload group: a member without a signature */
            char *paren = strchr(s, '(');
            if (paren) {
                *paren = '\0';
            }
        }
        r->docid = k;
        r->glob = true; /* a doc ID is a full name */
    }
    size_t gl = sizeof(global_prefix) - SKIP_ONE;
    if (strncmp(s, global_prefix, gl) == 0) {
        r->glob = true;
        s += gl;
    }
    n = strlen(s);
    int op = operator_start(s, r->op_name);
    if (op >= 0) {
        r->op = true;
        return op == 0 || parse_path(s, (size_t)op - SKIP_ONE, r->segs, &r->nsegs);
    }
    /* `Path(params)`: the first top-level parenthesis starts the list. */
    size_t paren = n;
    int depth = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c == '<' || c == '{' || c == '[') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']') {
            depth--;
        } else if (c == '(' && depth == 0) {
            paren = i;
            break;
        }
    }
    if (!parse_path(s, paren, r->segs, &r->nsegs)) {
        return false;
    }
    if (paren < n) {
        r->has_params = true;
        size_t close = group_end(s, n, paren);
        bool closed = close > paren && s[close - SKIP_ONE] == ')';
        /* a parameter list that is not closed, or text after it, is not
         * matched by what can be read of it */
        for (size_t k = close; closed && k < n; k++) {
            closed = isspace((unsigned char)s[k]) != 0;
        }
        if (!closed || !parse_params(r, s, paren + SKIP_ONE, close - SKIP_ONE)) {
            return false;
        }
    }
    r->maybe_indexer = r->has_params && strcmp(r->segs[r->nsegs - SKIP_ONE].name, "Item") == 0;
    return true;
}

/* ── Results ─────────────────────────────────────────────────────── */

/* Where the complete declarations of `e` in units before `unit` end. */
static int fulls_before(const cs_entity_t *e, int unit) {
    int lo = 0;
    int hi = e->nfulls;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (e->fulls[mid].unit < unit) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* The complete declaration of `e` that stands in the referencing file's own
 * project (or shared tree) and that it may bind: one with a node when
 * there is one, the nearest of several. NULL when there is none -- and when
 * that unit holds more of them than one lookup compares (*too_many). */
static const cs_full_t *own_full(const cs_ctx_t *c, const cs_entity_t *e, bool *too_many) {
    const cs_index_t *ix = c->ix;
    int lo = fulls_before(e, c->unit);
    int hi = fulls_before(e, c->unit + SKIP_ONE);
    if (hi - lo > CS_MAX_OVERLOADS) {
        *too_many = true;
        return NULL;
    }
    const cs_full_t *best = NULL;
    bool best_node = false;
    for (int i = lo; i < hi; i++) {
        const cs_full_t *d = &e->fulls[i];
        if (c->prod && ix->files[d->file].is_test) {
            continue;
        }
        bool node = ix->files[d->file].types[d->type].node != NULL;
        if (!best || (node && !best_node) ||
            (node == best_node && file_nearer(ix, d->file, best->file, c->file))) {
            best = d;
            best_node = node;
        }
    }
    return best;
}

/* The one assembly that has only stubs of the type and takes the shared
 * trees' declaration `ent` as its implementation; CS_NONE when there is
 * none, or more than one. Its stubs are the type's stubs: where the
 * implementation has no node, or a parse error hides a member of it, the
 * stub's stands in -- as it does inside one assembly. */
static int stub_user(const cs_index_t *ix, int ent) {
    int stub = ix->ents[ent].stub;
    return stub >= 0 ? stub : CS_NONE;
}

/* The implementation's node among one entity's own declarations. What is an
 * alternative is not chosen between:
 *   - complete declarations of the type in two or more projects (the
 *     flavours of one assembly's type): the one of the referencing file's own
 *     project is meant -- and when that one has no node, no other flavour's
 *     stands in for it;
 *   - parts in two or more shared trees, which may or may not be compiled
 *     together: the ones of the referencing file's own tree.
 * From anywhere else the type is AMBIGUOUS (*why). GAP when no
 * implementation in view has a node. */
static cs_pick_t impl_node(const cs_ctx_t *c, const cs_entity_t *e, const cbm_gbuf_node_t **node,
                           cs_why_t *why) {
    if ((c->prod ? e->full_units_prod : e->full_units) >= PAIR_LEN) {
        bool too_many = false;
        const cs_full_t *own = own_full(c, e, &too_many);
        if (!own) {
            *why = too_many ? CS_WHY_LIMIT : CS_WHY_FLAVOURS;
            return CS_PICK_AMBIGUOUS;
        }
        *node = c->ix->files[own->file].types[own->type].node;
        return *node ? CS_PICK_NODE : CS_PICK_GAP;
    }
    if (e->shared_parts && e->nunits >= PAIR_LEN && !ent_in_unit(e, c->unit)) {
        *why = CS_WHY_SHARED;
        return CS_PICK_AMBIGUOUS;
    }
    *node = class_node(c, e, false);
    return *node ? CS_PICK_NODE : CS_PICK_GAP;
}

/* The type's node for a reference from this context: an implementation's --
 * of the type's own declarations, then of the shared trees' declaration that
 * belongs to it -- and a reference assembly's stub's only when no
 * implementation has one. Never a test declaration for product code. A
 * visible type whose eligible declarations all lack a node is a graph gap. A
 * node that is the type's only because a contract is joined to its
 * implementation is never an exact binding. */
static cs_res_t type_result(const cs_ctx_t *c, int ent, bool exact) {
    const cs_index_t *ix = c->ix;
    cs_view_t v = view_of(ix, ent);
    const cbm_gbuf_node_t *node = NULL;
    bool in_view = false;
    for (int k = 0; k < v.n; k++) {
        const cs_entity_t *e = &ix->ents[v.ent[k]];
        cs_why_t why = CS_WHY_SCOPE;
        cs_pick_t p = impl_node(c, e, &node, &why);
        if (p == CS_PICK_NODE) {
            return res_edge(node, exact && !(k > 0 && ix->ents[ent].joined));
        }
        if (p == CS_PICK_AMBIGUOUS) {
            return res_ambiguous(why);
        }
        in_view = in_view || declared_in_view(c, e);
    }
    for (int k = 0; k < v.n; k++) {
        node = class_node(c, &ix->ents[v.ent[k]], true);
        if (node) {
            return res_edge(node, exact);
        }
    }
    int stubs = stub_user(ix, ent);
    node = stubs >= 0 ? class_node(c, &ix->ents[stubs], true) : NULL;
    if (node) {
        return res_edge(node, false);
    }
    return res_unres(in_view ? CBM_DOCLINK_REASON_GRAPH_GAP : CBM_DOCLINK_REASON_TEST_ONLY);
}

/* Bind the members in `sp` -- one name, group and signature: the
 * declarations of one member. An implementation's node before a reference
 * assembly's stub's; never a test declaration for product code; the nearest
 * of several. Declarations of the member in two or more projects (or shared
 * trees) are alternatives: the one of the referencing file's own is meant,
 * and from anywhere else none can be chosen. More declarations than
 * one lookup compares are ambiguous as well. `view`: the type the members
 * were looked up in (CS_NONE: none to speak of) -- a member a contract has
 * only through the implementation joined to it is never an exact binding. */
static cs_res_t bind_members(const cs_ctx_t *c, const cs_mspans_t *sp, bool exact, int view) {
    const cs_index_t *ix = c->ix;
    if (sp->total > CS_MAX_OVERLOADS) {
        return res_ambiguous(CS_WHY_LIMIT);
    }
    int first_unit = CS_NONE;
    bool several = false;
    bool own = false;
    bool visible = false;
    for (int s = 0; s < sp->n; s++) {
        for (int i = sp->lo[s]; i < sp->hi[s]; i++) {
            const cs_mref_t *r = &ix->mrefs[i];
            if (!mref_visible(c, r)) {
                continue;
            }
            visible = true;
            if (r->cls & CS_CLS_REF) {
                continue;
            }
            int unit = ix->files[r->file].unit;
            own = own || unit == c->unit;
            several = several || (first_unit >= 0 && unit != first_unit);
            first_unit = first_unit >= 0 ? first_unit : unit;
        }
    }
    if (!visible) {
        return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    if (several && !own) {
        return res_ambiguous(CS_WHY_FLAVOURS);
    }
    /* an implementation's node (the own one of alternatives), else a stub's */
    const cs_mref_t *best[PAIR_LEN] = {NULL, NULL};
    for (int s = 0; s < sp->n; s++) {
        for (int i = sp->lo[s]; i < sp->hi[s]; i++) {
            const cs_mref_t *r = &ix->mrefs[i];
            bool stub = (r->cls & CS_CLS_REF) != 0;
            if (!mref_visible(c, r) || (r->cls & CS_CLS_NO_NODE) ||
                (!stub && several && ix->files[r->file].unit != c->unit)) {
                continue;
            }
            if (!best[stub] || file_nearer(ix, r->file, best[stub]->file, c->file)) {
                best[stub] = r;
            }
        }
    }
    const cs_mref_t *pick = best[0] ? best[0] : best[SKIP_ONE];
    if (!pick) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    bool through_join = view >= 0 && ix->ents[view].joined && pick->ent != view;
    return res_edge(mref_member(ix, pick)->node, exact && !through_join);
}

/* Bind the type's members of one name, group and signature. A member whose
 * implementation has no node is its stub's, where one assembly's stubs stand
 * for this declaration (stub_user): never an exact binding. */
static cs_res_t bind_signature(const cs_ctx_t *c, int ent, const char *name, int group,
                               const char *sig, bool exact) {
    cs_mspans_t sp = mref_spans(c->ix, ent, name, group, sig);
    cs_res_t res = bind_members(c, &sp, exact, ent);
    int stubs = res_is(&res, CBM_DOCLINK_REASON_GRAPH_GAP) ? stub_user(c->ix, ent) : CS_NONE;
    if (stubs >= 0) {
        cs_mspans_t of_stubs = mref_spans(c->ix, stubs, name, group, sig);
        cs_res_t stub = bind_members(c, &of_stubs, false, CS_NONE);
        if (stub.st == CS_OK) {
            return stub;
        }
    }
    return res;
}

/* Which written segments name the declaring type and the member: a written
 * type argument stands for the type parameter at its position there. */
typedef struct {
    int type_seg;   /* CS_NONE: the type is not written (a simple name in scope) */
    int member_seg; /* CS_NONE: the member is not written (a constructor by its type) */
} cs_where_t;

/* How a found name is used. */
typedef struct {
    const cs_ref_t *r;
    bool has_params; /* a parameter list follows the name */
    cs_where_t where;
    bool exact;      /* tier of a member bound through the written path */
    bool exact_type; /* ... and of a type the whole path names */
} cs_use_t;

static size_t suffix_start(const char *s, size_t n) {
    size_t e = n;
    while (e > 0 && strchr("[],*", s[e - SKIP_ONE])) {
        e--;
    }
    return e;
}

/* Position of `name` among the ','-separated type arguments of a segment. */
static int targ_position(const cs_seg_t *seg, const char *name, size_t len) {
    int pos = 0;
    int depth = 0;
    size_t start = 0;
    for (size_t i = 0; seg->targs && i <= seg->targs_len; i++) {
        char ch = i < seg->targs_len ? seg->targs[i] : ',';
        if (is_open_bracket(ch)) {
            depth++;
        } else if (is_close_bracket(ch)) {
            depth--;
        } else if (ch == ',' && depth == 0) {
            const char *a = seg->targs + start;
            size_t al = i - start;
            while (al > 0 && isspace((unsigned char)*a)) {
                a++;
                al--;
            }
            while (al > 0 && isspace((unsigned char)a[al - SKIP_ONE])) {
                al--;
            }
            if (al == len && memcmp(a, name, len) == 0) {
                return pos;
            }
            pos++;
            start = i + SKIP_ONE;
        }
    }
    return CS_NONE;
}

/* The position a doc ID's `N (*method false) or ``N (*method true) names;
 * CS_NONE for any other text. */
static int slot_of(const char *s, size_t n, bool *method) {
    size_t ticks = 0;
    while (ticks < n && s[ticks] == '`') {
        ticks++;
    }
    if (ticks == 0 || ticks > PAIR_LEN || ticks == n) {
        return CS_NONE;
    }
    int pos = 0;
    for (size_t i = ticks; i < n; i++) {
        if (!isdigit((unsigned char)s[i]) || pos > CBM_SZ_4K) {
            return CS_NONE;
        }
        pos = (pos * CBM_DECIMAL_BASE) + (s[i] - '0');
    }
    *method = ticks == PAIR_LEN;
    return pos;
}

/* A written type variable and a declared one are the same when they stand at
 * the same position of the same list: the method's own list, the declaring
 * type's, its outer type's, and so on. A doc ID writes the position itself;
 * there the positions of a type's list count on from its outer types'. */
static bool same_type_variable(const cs_ctx_t *c, const cs_ref_t *r, cs_where_t w,
                               const cs_mref_t *mr, const char *written, size_t wl,
                               const char *declared, size_t dl) {
    const cs_file_t *f = &c->ix->files[mr->file];
    bool slot_method = false;
    int slot = slot_of(written, wl, &slot_method);
    int pos = tparam_find(f, f->ntypes + mr->midx, declared, dl);
    if (pos >= 0) {
        if (slot >= 0) {
            return slot_method && slot == pos;
        }
        return w.member_seg >= 0 && targ_position(&r->segs[w.member_seg], written, wl) == pos;
    }
    int seg = w.type_seg;
    for (int t = f->members[mr->midx].type; t >= 0; t = f->types[t].outer, seg--) {
        pos = tparam_find(f, t, declared, dl);
        if (pos < 0) {
            continue;
        }
        if (slot >= 0) {
            int before = 0;
            for (int o = f->types[t].outer; o >= 0; o = f->types[o].outer) {
                before += f->types[o].arity;
            }
            return !slot_method && slot == before + pos;
        }
        return seg >= 0 && targ_position(&r->segs[seg], written, wl) == pos;
    }
    return false;
}

enum { CS_FIT_NO = 0, CS_FIT_UNKNOWN, CS_FIT_EXACT };

/* How the written parameter list fits a declared signature: EXACT when every
 * parameter type is the declared one (the same text, or the same type
 * variable), UNKNOWN when the rest are types nothing is known about on
 * either side ("?"), NO otherwise. */
static int sig_fit(const cs_ctx_t *c, const cs_ref_t *r, cs_where_t w, const cs_mref_t *mr) {
    const char *sig = mref_member(c->ix, mr)->sig;
    /* counted when the declaration was read: a declared signature is not
     * walked for every reference that cannot mean it */
    int nd = mref_member(c->ix, mr)->nparams;
    if (nd != r->nparams) {
        return CS_FIT_NO;
    }
    cs_work((uint64_t)nd);
    int fit = CS_FIT_EXACT;
    const char *p = sig;
    for (int i = 0; i < nd; i++) {
        const char *e = strchr(p, '|');
        size_t dn = e ? (size_t)(e - p) : strlen(p);
        const char *a = r->params[i];
        size_t an = strlen(a);
        if (!(an == dn && memcmp(a, p, an) == 0)) {
            size_t ab = suffix_start(a, an);
            size_t db = suffix_start(p, dn);
            bool same_suffix = (an - ab) == (dn - db) && memcmp(a + ab, p + db, an - ab) == 0;
            if (same_suffix && same_type_variable(c, r, w, mr, a, ab, p, db)) {
                /* the same type variable */
            } else if ((an == SKIP_ONE && a[0] == '?') || (dn == SKIP_ONE && p[0] == '?')) {
                fit = CS_FIT_UNKNOWN;
            } else {
                return CS_FIT_NO;
            }
        }
        p = e ? e + SKIP_ONE : p + dn;
    }
    return fit;
}

typedef enum {
    CS_MB_NONE = 0,  /* the entity declares no such member */
    CS_MB_INVISIBLE, /* only its test declarations do, and the reference is product code */
    CS_MB_MISMATCH,  /* it declares the name; nothing of it takes the written parameters */
    CS_MB_RES,       /* *res is the answer */
} cs_mb_t;

/* The selected members of one side (generic callables, non-generic ones,
 * values): how many, and whether they are one declaration's worth -- one
 * signature and arity. */
typedef struct {
    int n;
    const char *sig;
    int arity;
    bool many;
} cs_side_t;

static void side_add(cs_side_t *s, const cs_member_t *m) {
    const char *sig = m->sig ? m->sig : "";
    if (s->n == 0) {
        s->sig = sig;
        s->arity = m->arity;
    } else if (strcmp(s->sig, sig) != 0 || s->arity != m->arity) {
        s->many = true;
    }
    s->n++;
}

/* The members the query finds in `ent`, for a reference without a parameter
 * list: one member binds; a method name without type arguments means the
 * non-generic methods when there are any; an overload group, and a method
 * beside a value of the same name, are ambiguous. */
static cs_mb_t members_plain(const cs_ctx_t *c, int ent, const cs_query_t *q, bool exact,
                             cs_res_t *res) {
    const cs_index_t *ix = c->ix;
    cs_mspans_t sp = member_spans(c, ent, q);
    if (sp.total == 0) {
        return CS_MB_NONE;
    }
    if (sp.total > CS_MAX_OVERLOADS) {
        *res = res_ambiguous(CS_WHY_LIMIT); /* more than one lookup compares */
        return CS_MB_RES;
    }
    cs_side_t plain = {0};
    cs_side_t generic = {0};
    cs_side_t values = {0};
    bool invisible = false;
    for (int s = 0; s < sp.n; s++) {
        for (int i = sp.lo[s]; i < sp.hi[s]; i++) {
            cs_work(SKIP_ONE);
            const cs_mref_t *mr = &ix->mrefs[i];
            if (!member_viable(c, mr, q)) {
                continue;
            }
            if (!mref_visible(c, mr)) {
                invisible = true;
                continue;
            }
            const cs_member_t *m = mref_member(ix, mr);
            side_add(m->kind != 'c' ? &values : (m->arity > 0 ? &generic : &plain), m);
        }
    }
    const cs_side_t *calls = plain.n > 0 ? &plain : &generic;
    if (calls->n + values.n == 0) {
        return invisible ? CS_MB_INVISIBLE : CS_MB_NONE;
    }
    if ((calls->n > 0 && values.n > 0) || calls->many) {
        *res = res_ambiguous(CS_WHY_SCOPE);
    } else if (values.n > 0) {
        *res = bind_signature(c, ent, q->name, 0, "", exact);
    } else {
        *res = bind_signature(c, ent, q->name, SKIP_ONE, calls->sig, exact);
    }
    return CS_MB_RES;
}

/* The same for a reference WITH a parameter list: the overload with exactly
 * the written parameter types (the same text, or the same type variables);
 * only when there is none, one whose difference is a type nothing is known
 * about. A non-generic match stands before a generic one; several distinct
 * matches are ambiguous. A value never takes a parameter list. */
static cs_mb_t members_signed(const cs_ctx_t *c, int ent, const cs_query_t *q, const cs_use_t *u,
                              cs_res_t *res) {
    const cs_index_t *ix = c->ix;
    cs_mspans_t sp = member_spans(c, ent, q);
    if (sp.total == 0) {
        return CS_MB_NONE;
    }
    if (sp.total > CS_MAX_OVERLOADS) {
        /* too many to compare one by one: the written text itself can still
         * be looked up */
        cs_mspans_t written = mref_spans(ix, ent, q->name, SKIP_ONE, u->r->sig);
        *res = (written.total > 0 && !u->r->sig_unknown) ? bind_members(c, &written, u->exact, ent)
                                                         : res_ambiguous(CS_WHY_LIMIT);
        return CS_MB_RES;
    }
    bool named = false;
    bool invisible = false;
    for (int pass = CS_FIT_EXACT; pass >= CS_FIT_UNKNOWN; pass--) {
        cs_side_t plain = {0};
        cs_side_t generic = {0};
        for (int s = 0; s < sp.n; s++) {
            for (int i = sp.lo[s]; i < sp.hi[s]; i++) {
                cs_work(SKIP_ONE);
                const cs_mref_t *mr = &ix->mrefs[i];
                if (!member_viable(c, mr, q)) {
                    continue;
                }
                if (!mref_visible(c, mr)) {
                    invisible = true;
                    continue;
                }
                named = true;
                const cs_member_t *m = mref_member(ix, mr);
                if (m->kind == 'c' && sig_fit(c, u->r, u->where, mr) == pass) {
                    side_add(m->arity > 0 ? &generic : &plain, m);
                }
            }
        }
        const cs_side_t *s = plain.n > 0 ? &plain : &generic;
        if (s->n > 0) {
            *res = s->many ? res_ambiguous(CS_WHY_SCOPE)
                           : bind_signature(c, ent, q->name, SKIP_ONE, s->sig, u->exact);
            return CS_MB_RES;
        }
    }
    if (named) {
        return CS_MB_MISMATCH;
    }
    return invisible ? CS_MB_INVISIBLE : CS_MB_NONE;
}

static cs_mb_t members_result(const cs_ctx_t *c, int ent, const cs_query_t *q, const cs_use_t *u,
                              cs_res_t *res) {
    return u->has_params ? members_signed(c, ent, q, u, res)
                         : members_plain(c, ent, q, u->exact, res);
}

/* True when a part of the type `ent` that this reference does not see
 * declares `name`. `ent` is a shared trees' declaration that assemblies have
 * as a part of their type, seen here without one of them: the reference
 * stands in a shared tree, or in an assembly that has no part of its own.
 * One of those assemblies' own parts declares an implementation of that name
 * -- a nested type that the shared trees do not have, or (`types_only`
 * unset) a member. Which assembly the reference is compiled into is not
 * known, so the name is neither bound nor missing. A stub does not count: it
 * declares what the implementation declares. The assemblies are not walked:
 * what their parts add is looked up by the name (build_extras). *why: when
 * more assemblies declare a nested type of the name than one lookup compares
 * (and none of those compared is in view), that is said. */
static bool unseen_part_declares(const cs_ctx_t *c, int ent, const char *name, int arity,
                                 bool types_only, cs_why_t *why) {
    const cs_index_t *ix = c->ix;
    int hi = 0;
    int lo = extra_range(ix, ent, name, type_arity(arity), &hi);
    for (int i = lo; i < hi; i++) {
        if (i - lo >= CS_MAX_FOREIGN) {
            *why = CS_WHY_LIMIT;
            return true;
        }
        cs_work(SKIP_ONE);
        if (ent_visible(c, ix->extras[i].ent)) {
            return true;
        }
    }
    if (types_only) {
        return false;
    }
    lo = extra_range(ix, ent, name, CS_NONE, &hi);
    return lo < hi && (ix->extras[lo].prod || !c->prod);
}

/* The reason for a member a type does not show (`name`, when it has one). */
static cs_res_t member_unfound(const cs_ctx_t *c, int ent, const char *name, int arity) {
    const cs_entity_t *e = &c->ix->ents[ent];
    cs_why_t why = CS_WHY_PARTS;
    if (name && unseen_part_declares(c, ent, name, arity, false, &why)) {
        return res_ambiguous(why);
    }
    if (e->incomplete) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* a parse error hides members */
    }
    return res_unres(e->open_any ? CBM_DOCLINK_REASON_EXTERNAL : CBM_DOCLINK_REASON_MISSING);
}

/* A member the implementation does not show because a parse error hides it,
 * looked up in the stubs that stand for this declaration (stub_user). true
 * when a stub binds it: never an exact binding. */
static bool stub_member(const cs_ctx_t *c, int ent, const cs_query_t *q, const cs_use_t *u,
                        cs_res_t *res) {
    int stubs = c->ix->ents[ent].incomplete ? stub_user(c->ix, ent) : CS_NONE;
    cs_res_t of_stubs = res_unres(CBM_DOCLINK_REASON_MISSING);
    cs_use_t via = *u;
    via.exact = false;
    via.exact_type = false;
    if (stubs < 0 || members_result(c, stubs, q, &via, &of_stubs) != CS_MB_RES ||
        of_stubs.st != CS_OK) {
        return false;
    }
    of_stubs.exact = false;
    *res = of_stubs;
    return true;
}

/* True when `ent` declares an instance constructor without parameters. */
static bool declares_default_ctor(const cs_ctx_t *c, int ent) {
    const cs_index_t *ix = c->ix;
    cs_mspans_t sp = mref_spans(ix, ent, ix->ents[ent].name, SKIP_ONE, "");
    for (int s = 0; s < sp.n; s++) {
        for (int i = sp.lo[s]; i < sp.hi[s]; i++) {
            if (!mref_member(ix, &ix->mrefs[i])->is_static) {
                return true;
            }
        }
    }
    return false;
}

/* A constructor of `ent`. A type has the instance constructors its source
 * writes (a primary constructor among them: the scope records it), and ones
 * no source writes: the parameterless one of a struct, and of a class or
 * record that writes none; a record's copy constructor. Those are declared
 * and have no node. No constructor is inherited. */
static cs_res_t ctor_result(const cs_ctx_t *c, int ent, const cs_use_t *u) {
    const cs_entity_t *e = &c->ix->ents[ent];
    cs_query_t q = {.name = e->name, .arity = CS_ARITY_NONE, .ctors = true, .kind = 'c'};
    cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
    cs_mb_t st = members_result(c, ent, &q, u, &res);
    bool value_type = e->kind == 's' || e->kind == 't';
    bool record = e->kind == 'r' || e->kind == 't';
    if (st == CS_MB_RES) {
        /* a struct has its parameterless constructor beside the one it writes */
        bool second =
            !u->has_params && value_type && res.st == CS_OK && !declares_default_ctor(c, ent);
        return second ? res_ambiguous(CS_WHY_SCOPE) : res;
    }
    if (st == CS_MB_INVISIBLE) {
        return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    if (e->incomplete) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    bool supplied = value_type || (st == CS_MB_NONE && (e->kind == 'c' || e->kind == 'r'));
    if (supplied && (!u->has_params || u->r->nparams == 0)) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (record && u->has_params && u->r->nparams == SKIP_ONE &&
        strcmp(u->r->params[0], e->name) == 0) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    return res_unres(CBM_DOCLINK_REASON_MISSING);
}

/* The static constructor of `ent` (a doc ID's `#cctor`). */
static cs_res_t static_ctor_result(const cs_ctx_t *c, int ent, bool exact) {
    cs_query_t q = {.name = c->ix->ents[ent].name,
                    .arity = CS_ARITY_NONE,
                    .statics = true,
                    .ctors = true,
                    .kind = 'c'};
    cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
    cs_mb_t st = members_plain(c, ent, &q, exact, &res);
    if (st == CS_MB_RES) {
        return res;
    }
    return st == CS_MB_INVISIBLE ? res_unres(CBM_DOCLINK_REASON_TEST_ONLY)
                                 : member_unfound(c, ent, NULL, CS_ARITY_NONE);
}

/* ── Scope lookup ────────────────────────────────────────────────── */

/* The types nested in `ent` and the members it declares itself, by the
 * query. A doc ID's member kind names a member, never a nested type. */
static void level_entity(const cs_ctx_t *c, int ent, const cs_query_t *q, cs_found_t *fd) {
    /* the shared trees' declaration seen without an assembly's own parts of
     * the type: a name those parts declare is not told from what is here */
    cs_why_t why = CS_WHY_PARTS;
    if (c->ix->ents[ent].used &&
        unseen_part_declares(c, ent, q->name, q->arity, q->types_only, &why)) {
        fd->n += PAIR_LEN;
        fd->why = why;
        return;
    }
    if (!q->kind) {
        add_types(c, false, ent, q->name, type_arity(q->arity), fd);
    }
    if (q->types_only) {
        return;
    }
    int has = members_named(c, ent, q);
    if (has == CS_HAS_VISIBLE) {
        found_add(fd, 'M', ent);
    } else if (has == CS_HAS_INVISIBLE) {
        fd->invisible = true;
    }
}

/* What an alias was resolved to, as a candidate. */
static void add_alias_target(const cs_ctx_t *c, const cs_using_t *u, cs_found_t *fd) {
    if (u->ent >= 0) {
        found_add_type(c, fd, u->ent);
        fd->joined = fd->joined || u->joined;
    } else if (u->ent == CS_AMBIGUOUS) {
        fd->n += PAIR_LEN;
    } else if (u->ns >= 0) {
        found_add(fd, 'N', u->ns);
    } else {
        found_add(fd, 'X', CS_NONE);
    }
}

/* True once a level has two candidates: the lookup is ambiguous whatever the
 * directives after them bring, so they are not asked (S5). Which candidate
 * came first, and whether there was one, never depends on the ones skipped. */
static bool found_decided(const cs_found_t *fd) {
    return fd->n > SKIP_ONE;
}

static void level_aliases(const cs_ctx_t *c, const cs_using_t *us, int n,
                          const cs_using_index_t *index, const cs_query_t *q, cs_found_t *fd) {
    /* an alias names a type or a namespace: no candidate for `Name{T}` */
    if (q->arity > 0) {
        return;
    }
    if (!index || !index->ready) {
        for (int i = 0; i < n && !found_decided(fd); i++) {
            cs_work(SKIP_ONE);
            if (us[i].kind == 'a' && strcmp(us[i].alias, q->name) == 0) {
                add_alias_target(c, &us[i], fd);
            }
        }
        return;
    }
    size_t lo = 0;
    size_t hi = index->naliases;
    while (lo < hi) {
        cs_work(SKIP_ONE);
        size_t mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(index->aliases[mid].name, q->name) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (size_t i = lo; i < index->naliases && !found_decided(fd); i++) {
        cs_work(SKIP_ONE);
        if (strcmp(index->aliases[i].name, q->name)) {
            break;
        }
        add_alias_target(c, &us[index->aliases[i].id], fd);
    }
}

/* The original resolver is shared by the full scan and candidate path. */
static void level_using(const cs_ctx_t *c, const cs_using_t *u, const cs_query_t *q,
                        bool a_type_name, cs_found_t *fd) {
    cs_work(SKIP_ONE);
    if (u->kind == 'n' && u->ns >= 0 && a_type_name) {
        /* a using brings a namespace's types, not the namespaces in it */
        add_types(c, true, u->ns, q->name, type_arity(q->arity), fd);
    } else if (u->kind == 's' && u->ent >= 0) {
        cs_query_t statics = *q;
        statics.statics = true;
        level_entity(c, u->ent, &statics, fd);
    }
}

static size_t name_scope_range(const cs_index_t *ix, const char *name, size_t *end) {
    size_t lo = 0;
    size_t hi = ix->nname_scopes;
    while (lo < hi) {
        cs_work(SKIP_ONE);
        size_t mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(ix->name_scopes[mid].name, name) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    size_t first = lo;
    hi = ix->nname_scopes;
    while (lo < hi) {
        cs_work(SKIP_ONE);
        size_t mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(ix->name_scopes[mid].name, name) <= 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    *end = lo;
    return first;
}

/* IDs are accumulated before any result is added: if growth fails, a full
 * scan can retry without duplicating a partially resolved candidate set. */
enum { CS_CANDIDATE_STACK = 32 };
typedef struct {
    int local[CS_CANDIDATE_STACK];
    int *ids;
    size_t n;
    size_t cap;
} cs_candidate_ids_t;

static bool candidate_add(cs_candidate_ids_t *ids, int id) {
    if (ids->n == ids->cap) {
        if (ids->cap > SIZE_MAX / PAIR_LEN / sizeof(int)) {
            return false;
        }
        size_t cap = ids->cap * PAIR_LEN;
        int *grown = NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        bool fail = atomic_exchange(&cs_fail_candidate_alloc, false);
        if (fail) {
            atomic_store(&cs_candidate_alloc_failed, true);
        } else
#endif
        {
            grown = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, cap * sizeof(int));
        }
        if (!grown) {
            return false;
        }
        memcpy(grown, ids->ids, ids->n * sizeof(int));
        cs_work(ids->n);
        if (ids->ids != ids->local) {
            cbm_free(CBM_MEM_CLASS_OTHER, ids->ids);
        }
        ids->ids = grown;
        ids->cap = cap;
    }
    ids->ids[ids->n++] = id;
    return true;
}

static bool candidate_scope(const cs_using_id_t *rows, size_t n, int scope,
                            cs_candidate_ids_t *ids) {
    size_t lo = 0;
    size_t hi = n;
    while (lo < hi) {
        cs_work(SKIP_ONE);
        size_t mid = lo + (hi - lo) / PAIR_LEN;
        if (rows[mid].scope < scope) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (size_t i = lo; i < n; i++) {
        cs_work(SKIP_ONE);
        if (rows[i].scope != scope) {
            break;
        }
        if (!candidate_add(ids, rows[i].id)) {
            return false;
        }
    }
    return true;
}

static int candidate_id_cmp(const void *a, const void *b) {
    cs_work(SKIP_ONE);
    return int_cmp(a, b);
}

/* The directives of a using step that give a query anything, in order (R1):
 * noted while the step is walked, for the unit memo. */
typedef struct {
    int *ids;
    int n;
    int cap;
    bool failed; /* memory ran out: the list is not whole */
} cs_active_rec_t;

static void active_add(cs_active_rec_t *rec, int id) {
    if (rec->n == rec->cap) {
        int cap = rec->cap ? rec->cap * PAIR_LEN : CBM_SZ_16;
        int *grown = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)cap * sizeof(int));
        if (!grown) {
            rec->failed = true;
            return;
        }
        if (rec->n > 0) {
            memcpy(grown, rec->ids, (size_t)rec->n * sizeof(int));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, rec->ids);
        rec->ids = grown;
        rec->cap = cap;
    }
    rec->ids[rec->n++] = id;
}

/* Directive `id` of a step, for the query. With `rec`, it is noted when it
 * gives the query anything at all: one that leaves an empty step empty adds
 * no candidate and sets no flag whatever the step holds, so leaving it out
 * changes no lookup. */
static void using_visit(const cs_ctx_t *c, const cs_using_t *us, int id, const cs_query_t *q,
                        bool a_type_name, cs_found_t *fd, cs_active_rec_t *rec) {
    if (rec) {
        cs_found_t probe = {0};
        level_using(c, &us[id], q, a_type_name, &probe);
        if (probe.n == 0 && !probe.invisible && !probe.joined && probe.why == CS_WHY_SCOPE) {
            return;
        }
        active_add(rec, id);
    }
    level_using(c, &us[id], q, a_type_name, fd);
}

/* False when none of a step's n directives can give the query anything:
 * there are none, or the index shows no static candidates and no namespace
 * that can contribute this name. *a_type_name: the name is some type's. */
static bool usings_may_give(const cs_ctx_t *c, int n, const cs_using_index_t *index,
                            const cs_query_t *q, bool *a_type_name) {
    if (!n) {
        return false;
    }
    *a_type_name = cbm_ht_get(c->ix->type_names, q->name) != NULL;
    bool indexed = index && index->ready && c->ix->name_scopes_ready;
    return !(indexed && !index->nentities && (!index->nnamespaces || !*a_type_name));
}

static void level_usings(const cs_ctx_t *c, const cs_using_t *us, int n,
                         const cs_using_index_t *index, const cs_query_t *q, cs_found_t *fd,
                         cs_active_rec_t *rec) {
    bool a_type_name = false;
    if (!usings_may_give(c, n, index, q, &a_type_name)) {
        return;
    }
    bool indexed = index && index->ready && c->ix->name_scopes_ready;
    size_t hi = 0;
    size_t lo = indexed ? name_scope_range(c->ix, q->name, &hi) : 0;
    /* A common name in a large repository must not make a small scope walk
     * more postings than it has directives. The baseline scan stays cheap. */
    if (!indexed || hi - lo >= (size_t)n) {
        for (int i = 0; i < n && !found_decided(fd); i++) {
            using_visit(c, us, i, q, a_type_name, fd, rec);
        }
        return;
    }
    cs_candidate_ids_t ids = {.cap = CS_CANDIDATE_STACK};
    ids.ids = ids.local;
    bool complete = true;
    for (size_t i = lo; complete && i < hi; i++) {
        cs_work(SKIP_ONE);
        const cs_name_scope_t *row = &c->ix->name_scopes[i];
        complete = row->top
                       ? candidate_scope(index->namespaces, index->nnamespaces, row->scope, &ids)
                       : candidate_scope(index->entities, index->nentities, row->scope, &ids);
    }
    if (complete) {
        if (ids.n > 1) {
            qsort(ids.ids, ids.n, sizeof(int), candidate_id_cmp);
        }
        for (size_t i = 0; i < ids.n && !found_decided(fd); i++) {
            cs_work(SKIP_ONE);
            /* Own and twin postings may select the SAME directive twice.
             * Distinct directive IDs must still be resolved separately. */
            if (!i || ids.ids[i] != ids.ids[i - SKIP_ONE]) {
                using_visit(c, us, ids.ids[i], q, a_type_name, fd, rec);
            }
        }
    }
    if (ids.ids != ids.local) {
        cbm_free(CBM_MEM_CLASS_OTHER, ids.ids);
    }
    if (!complete) {
        for (int i = 0; i < n && !found_decided(fd); i++) {
            using_visit(c, us, i, q, a_type_name, fd, rec);
        }
    }
}

/* The using step of a lookup -- what the directives of one region, and at
 * the global namespace the unit's, give one query -- remembered for one pass
 * over a file's references (or one pass of the build): a name looked up
 * again in the same region costs one probe, not the directives (S5). The key
 * holds everything the step reads: the file, the region and unit asked, the
 * context's visibility (product code, unit, assembly) and the query. A memo
 * that ran out of memory stops remembering; the lookups stay exact. */
struct cs_memo {
    CBMHashTable *steps; /* key -> cs_found_t in `arena` */
    CBMArena arena;
    bool off;
};

enum { CS_MEMO_KEY = CS_NAME_BUF + CBM_SZ_128 };

/* Holds no memory until the first step is remembered: most files ask few
 * using steps, and a memo is made for every file that has references. */
static void memo_init(cs_memo_t *m) {
    memset(m, 0, sizeof(*m));
    cbm_arena_init_lazy(&m->arena, CBM_ARENA_APPEND_BLOCK);
}

static void memo_destroy(cs_memo_t *m) {
    cbm_ht_free(m->steps);
    cbm_arena_destroy(&m->arena);
    memset(m, 0, sizeof(*m));
}

/* The key of one using step; false when it does not fit (not remembered). */
static bool memo_key(char *buf, size_t cap, const cs_ctx_t *c, int region, int unit,
                     const cs_query_t *q) {
    int n = snprintf(buf, cap, "%d|%d|%d|%d|%d|%d|%d|%d%d%d%c|%s", c->file, region, unit, c->unit,
                     c->group, (int)c->prod, q->arity, (int)q->types_only, (int)q->statics,
                     (int)q->ctors, q->kind ? q->kind : '-', q->name);
    return n > 0 && (size_t)n < cap;
}

static bool memo_get(const cs_memo_t *m, const char *key, cs_found_t *out) {
    cs_work(SKIP_ONE);
    const cs_found_t *hit = m->steps ? (const cs_found_t *)cbm_ht_get(m->steps, key) : NULL;
    if (hit) {
        *out = *hit;
    }
    return hit != NULL;
}

static void memo_put(cs_memo_t *m, const char *key, const cs_found_t *step) {
    if (!m->steps) {
        m->steps = cbm_ht_create(CBM_SZ_16);
        if (!m->steps) {
            m->off = true;
            return;
        }
    }
    char *k = cbm_arena_strdup(&m->arena, key);
    cs_found_t *v = (cs_found_t *)cbm_arena_alloc(&m->arena, sizeof(*v));
    if (!k || !v) {
        m->off = true;
        return;
    }
    *v = *step;
    cbm_ht_set(m->steps, k, v);
    m->off = cbm_ht_get(m->steps, k) != v; /* an insert that did not take */
}

/* The unit part of a using step (R1): which of a unit's directives give a
 * query anything, in order, up to where they alone decide it. What a
 * directive gives reads only the context's unit, assembly and product flag
 * (level_using), so the list is the same for every file of the unit that
 * asks the query: it is found once per run, shared by the resolve workers
 * (guarded), and each file then asks only those directives after its own --
 * stopping where the walk of all of them would, which is never later than
 * where they alone are decided. Each list is found once: by the first
 * worker that asks for it, while the others that ask for the same key hold
 * on until it is there (a step's cost never depends on the scheduling). A
 * memo that ran out of memory stops remembering; the lookups stay exact. */
typedef struct cs_unit_step {
    cbm_mutex_t mu; /* held by the worker that finds the list, until it is there */
    int *ids;       /* the directives, in order (memory-core block, or NULL) */
    int n;
    bool failed;               /* memory ran out finding it: askers walk all directives */
    struct cs_unit_step *next; /* every step, for the memo's release */
} cs_unit_step_t;

typedef struct cs_unit_memo {
    cbm_mutex_t mu;      /* guards steps, arena, all and off */
    CBMHashTable *steps; /* key -> cs_unit_step_t in `arena` */
    CBMArena arena;
    cs_unit_step_t *all;
    bool off;
} cs_unit_memo_t;

static cs_unit_memo_t *unit_memo_new(void) {
    cs_unit_memo_t *m = (cs_unit_memo_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
    if (!m) {
        return NULL;
    }
    cbm_mutex_init(&m->mu);
    cbm_arena_init_lazy(&m->arena, CBM_ARENA_APPEND_BLOCK);
    return m;
}

static void unit_memo_free(cs_unit_memo_t *m) {
    if (!m) {
        return;
    }
    for (cs_unit_step_t *e = m->all; e; e = e->next) {
        cbm_mutex_destroy(&e->mu);
        cbm_free(CBM_MEM_CLASS_OTHER, e->ids);
    }
    cbm_ht_free(m->steps);
    cbm_arena_destroy(&m->arena);
    cbm_mutex_destroy(&m->mu);
    cbm_free(CBM_MEM_CLASS_OTHER, m);
}

/* The key of one unit step: everything it reads. false when it does not fit. */
static bool unit_key(char *buf, size_t cap, const cs_ctx_t *c, const cs_query_t *q) {
    int n = snprintf(buf, cap, "%d|%d|%d|%d|%d%d%d%c|%s", c->unit, c->group, (int)c->prod, q->arity,
                     (int)q->types_only, (int)q->statics, (int)q->ctors, q->kind ? q->kind : '-',
                     q->name);
    return n > 0 && (size_t)n < cap;
}

/* The step under `key` (the caller holds the memo's lock); a new one is
 * added with its lock held, and *mine set: the caller finds its list. NULL
 * when memory ran out. */
static cs_unit_step_t *unit_step_at(cs_unit_memo_t *m, const char *key, bool *mine) {
    *mine = false;
    if (!m->steps) {
        m->steps = cbm_ht_create(CBM_SZ_64);
        if (!m->steps) {
            return NULL;
        }
    }
    cs_unit_step_t *had = (cs_unit_step_t *)cbm_ht_get(m->steps, key);
    if (had) {
        return had;
    }
    char *k = cbm_arena_strdup(&m->arena, key);
    cs_unit_step_t *e = (cs_unit_step_t *)cbm_arena_alloc(&m->arena, sizeof(*e));
    if (!k || !e) {
        return NULL;
    }
    memset(e, 0, sizeof(*e));
    cbm_mutex_init(&e->mu);
    cbm_mutex_lock(&e->mu);
    e->next = m->all;
    m->all = e;
    cbm_ht_set(m->steps, k, e);
    if (cbm_ht_get(m->steps, k) != e) {
        e->failed = true; /* an insert that did not take: kept for release only */
        cbm_mutex_unlock(&e->mu);
        return NULL;
    }
    *mine = true;
    return e;
}

/* What the unit's directives give the query after the region's (`fd`). */
static void unit_usings(const cs_ctx_t *c, const cs_unit_t *unit, const cs_query_t *q,
                        cs_found_t *fd) {
    bool a_type_name = false;
    if (!usings_may_give(c, unit->nusings, &unit->using_index, q, &a_type_name)) {
        return; /* nothing to ask, nothing to remember */
    }
    cs_unit_memo_t *m = c->ix->unit_memo;
    char key[CS_MEMO_KEY];
    cs_unit_step_t *e = NULL;
    bool mine = false;
    if (m && unit_key(key, sizeof(key), c, q)) {
        cs_work(SKIP_ONE);
        cbm_mutex_lock(&m->mu);
        e = m->off ? NULL : unit_step_at(m, key, &mine);
        m->off = m->off || !e;
        cbm_mutex_unlock(&m->mu);
    }
    if (e && mine) {
        cs_active_rec_t rec = {0};
        cs_found_t alone = {0};
        level_usings(c, unit->usings, unit->nusings, &unit->using_index, q, &alone, &rec);
        e->failed = rec.failed;
        e->ids = rec.failed ? NULL : rec.ids;
        e->n = rec.failed ? 0 : rec.n;
        if (rec.failed) {
            cbm_free(CBM_MEM_CLASS_OTHER, rec.ids);
        }
        cbm_mutex_unlock(&e->mu);
    } else if (e) {
        cbm_mutex_lock(&e->mu); /* the list is there once its finder lets go */
        cbm_mutex_unlock(&e->mu);
    }
    if (!e || e->failed) {
        level_usings(c, unit->usings, unit->nusings, &unit->using_index, q, fd, NULL);
        return;
    }
    for (int i = 0; i < e->n && !found_decided(fd); i++) {
        level_using(c, &unit->usings[e->ids[i]], q, a_type_name, fd);
    }
}

/* What the directives of this scope level give the query: the region's own
 * (`us`, when asked) and, at the global namespace, the unit's. Asked once per
 * key when the context has a memo. */
static void using_step(const cs_ctx_t *c, int region, const cs_using_t *us, int nus,
                       const cs_using_index_t *usi, const cs_unit_t *unit, const cs_query_t *q,
                       cs_found_t *step) {
    char key[CS_MEMO_KEY];
    bool keyed = c->memo && !c->memo->off &&
                 memo_key(key, sizeof(key), c, region, unit ? c->unit : CS_NONE, q);
    if (keyed && memo_get(c->memo, key, step)) {
        return;
    }
    level_usings(c, us, nus, usi, q, step, NULL);
    if (unit && !found_decided(step)) {
        unit_usings(c, unit, q, step);
    }
    if (keyed) {
        memo_put(c->memo, key, step);
    }
}

/* True when the lookup is settled by what the last level added. A level that
 * had only invisible candidates does not bind: the lookup goes on, and
 * remembers. */
static bool settled(cs_found_t *fd, bool *invisible) {
    *invisible = *invisible || fd->invisible;
    fd->invisible = false;
    return fd->n > 0;
}

/* Look a simple name up the way the compiler does in a cref: the documented
 * method's type parameters; for every enclosing type its type parameters and
 * what it declares itself; for every enclosing namespace its types and
 * namespaces, and where a namespace declaration of the file stands, that
 * declaration's aliases and then its usings. The first level that has the
 * name decides. One step per enclosing type and per enclosing namespace: the
 * scanner bounds both nestings. *statics: the name came through a `using
 * static`. */
static void lookup(const cs_ctx_t *c, const cs_query_t *q, cs_found_t *fd, bool *statics) {
    const cs_index_t *ix = c->ix;
    const cs_file_t *f = c->f;
    size_t nl = strlen(q->name);
    bool invisible = false;
    memset(fd, 0, sizeof(*fd));
    *statics = false;
    if (q->arity <= 0 && c->member >= 0 &&
        tparam_find(f, f->ntypes + c->member, q->name, nl) >= 0) {
        found_add(fd, 'L', CS_NONE);
        return;
    }
    for (int t = c->type; t >= 0; t = f->types[t].outer) {
        cs_work(SKIP_ONE);
        if (q->arity <= 0 && tparam_find(f, t, q->name, nl) >= 0) {
            found_add(fd, 'L', CS_NONE);
            return;
        }
        level_entity(c, f->types[t].entity, q, fd);
        if (settled(fd, &invisible)) {
            return;
        }
    }
    int reg = c->region;
    for (int ns = f->regions[reg].ns;; ns = ix->nss[ns].parent) {
        cs_work(SKIP_ONE);
        add_types(c, true, ns, q->name, type_arity(q->arity), fd);
        int child = q->arity > 0 ? CS_NONE : ns_in_view(c, ns, q->name, nl);
        if (child >= 0) {
            found_add(fd, 'N', child);
        }
        /* a declaration of this namespace stands here: its directives are
         * asked, unless they are the peers of the directive being resolved */
        bool here = reg >= 0 && f->regions[reg].ns == ns;
        bool asked = here && reg != c->skip_region;
        const cs_using_t *us = asked ? f->usings + f->regions[reg].u_lo : NULL;
        int nus = asked ? f->regions[reg].u_hi - f->regions[reg].u_lo : 0;
        const cs_using_index_t *usi = asked ? &f->regions[reg].using_index : NULL;
        const cs_unit_t *unit = (ns == 0 && c->unit >= 0) ? &ix->units[c->unit] : NULL;
        if (fd->n > 0) {
            /* C# rejects an alias beside a type or namespace of its name */
            cs_found_t alias = {0};
            level_aliases(c, us, nus, usi, q, &alias);
            if (unit) {
                level_aliases(c, unit->usings, unit->nusings, &unit->using_index, q, &alias);
            }
            fd->n += alias.n > 0;
            fd->in_namespace = true;
        }
        if (settled(fd, &invisible)) {
            return;
        }
        level_aliases(c, us, nus, usi, q, fd);
        if (unit) {
            level_aliases(c, unit->usings, unit->nusings, &unit->using_index, q, fd);
        }
        if (settled(fd, &invisible)) {
            fd->exact = true;
            return;
        }
        if (nus > 0 || unit) {
            /* Nothing is found yet (settled), and a step only adds: the step
             * is asked on its own and its candidates are the level's. */
            cs_found_t step = {0};
            using_step(c, asked ? reg : CS_NONE, us, nus, usi, unit, q, &step);
            fd->first = step.first;
            fd->n = step.n;
            fd->invisible = step.invisible;
            fd->why = step.why;
            fd->joined = fd->joined || step.joined;
        }
        if (settled(fd, &invisible)) {
            *statics = fd->first.kind == 'M';
            return;
        }
        if (here) {
            reg = f->regions[reg].parent;
        }
        if (ns == 0) {
            break;
        }
    }
    fd->invisible = invisible;
}

/* ── Paths ───────────────────────────────────────────────────────── */

typedef enum {
    CS_TP_TYPE = 0, /* the path names the type `ent` */
    CS_TP_NS,       /* ... the namespace `ns` */
    CS_TP_PARTIAL,  /* the type `ent` was reached; it has no type by the next segment */
    CS_TP_NSFAIL,   /* the namespace `ns` was reached; it has nothing by the next segment */
    CS_TP_RES,      /* `res` is the answer */
} cs_tp_kind_t;

typedef struct {
    cs_tp_kind_t st;
    int ent;
    int ns;
    bool exact;           /* the path itself is a qualified name (or an alias, or absolute) */
    bool in_ns;           /* its first segment is of an enclosing namespace (or absolute): one
                           * more segment makes a qualified name of it */
    const cs_seg_t *next; /* CS_TP_PARTIAL: the segment the type has nothing by */
    bool joined;          /* a segment is one type only because a contract is joined to its
                           * implementation: nothing bound through the path is exact */
    cs_res_t res;
} cs_tp_t;

static cs_tp_t tp_res(cs_res_t r) {
    return (cs_tp_t){.st = CS_TP_RES, .res = r};
}

/* Follow segs[from, n) from where `cur` stands: each is a member of what the
 * one before named -- a namespace or type of a namespace, a type nested in a
 * type. There is no second try from anywhere else. */
static cs_tp_t path_from(const cs_ctx_t *c, cs_tp_t cur, const cs_seg_t *segs, int from, int n) {
    for (int i = from; i < n; i++) {
        cs_found_t fd = {0};
        if (cur.st == CS_TP_NS) {
            int child = segs[i].arity > 0
                            ? CS_NONE
                            : ns_in_view(c, cur.ns, segs[i].name, strlen(segs[i].name));
            add_types(c, true, cur.ns, segs[i].name, type_arity(segs[i].arity), &fd);
            if (child >= 0 && fd.n == 0 && !fd.invisible) {
                cur.ns = child;
                continue;
            }
            fd.n += child >= 0;
        } else {
            add_types(c, false, cur.ent, segs[i].name, type_arity(segs[i].arity), &fd);
        }
        if (fd.n > SKIP_ONE) {
            return tp_res(found_ambiguous(&fd));
        }
        if (fd.n == 0) {
            if (fd.invisible) {
                return tp_res(res_unres(CBM_DOCLINK_REASON_TEST_ONLY));
            }
            cur.st = cur.st == CS_TP_NS ? CS_TP_NSFAIL : CS_TP_PARTIAL;
            cur.next = &segs[i];
            return cur;
        }
        cur.st = CS_TP_TYPE;
        cur.ent = fd.first.id;
        if (fd.joined) {
            cur.joined = true;
            cur.exact = false;
            cur.in_ns = false;
        }
    }
    return cur;
}

/* An open scope: a using in view names a namespace or type the repository
 * does not declare, or the project's global usings could not be evaluated.
 * A name found nowhere may come from there. */
static bool scope_open(const cs_ctx_t *c) {
    if (c->unit >= 0 && c->ix->units[c->unit].open) {
        return true;
    }
    return c->f->regions[c->region].open;
}

/* Why a simple name was found at no scope level. */
static cs_res_t simple_unfound(const cs_ctx_t *c) {
    const cs_index_t *ix = c->ix;
    if (scope_open(c)) {
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    bool hidden = false;
    for (int t = c->type; t >= 0; t = c->f->types[t].outer) {
        const cs_entity_t *e = &ix->ents[c->f->types[t].entity];
        if (e->open_any) {
            return res_unres(CBM_DOCLINK_REASON_EXTERNAL); /* a base outside the repository */
        }
        hidden = hidden || e->incomplete;
    }
    /* An enclosing type with members a parse error hides: the name may be
     * one of them. */
    return res_unres(hidden ? CBM_DOCLINK_REASON_GRAPH_GAP : CBM_DOCLINK_REASON_MISSING);
}

/* The path segs[0, n): a type, a namespace, or how far it got. The first
 * segment is a simple name that names a type or a namespace in scope;
 * `global::` and a doc ID start at the global namespace instead. `qualifier`:
 * the path qualifies a name that follows it. */
static cs_tp_t resolve_path(const cs_ctx_t *c, const cs_seg_t *segs, int n, bool qualifier) {
    if (c->glob) {
        return path_from(c, (cs_tp_t){.st = CS_TP_NS, .ns = 0, .exact = true, .in_ns = true}, segs,
                         0, n);
    }
    if (n <= 0) {
        return tp_res(res_unres(CBM_DOCLINK_REASON_UNPARSEABLE));
    }
    cs_query_t q = {.name = segs[0].name, .arity = segs[0].arity, .types_only = true};
    cs_found_t fd;
    bool statics = false;
    lookup(c, &q, &fd, &statics);
    if (fd.n > SKIP_ONE) {
        return tp_res(found_ambiguous(&fd));
    }
    if (fd.n == 0) {
        if (fd.invisible) {
            return tp_res(res_unres(CBM_DOCLINK_REASON_TEST_ONLY));
        }
        if (n == SKIP_ONE && !qualifier) {
            return tp_res(simple_unfound(c));
        }
        /* a qualifier that is no namespace and no type in scope: a type of
         * the repository that is not in scope here, or a namespace the
         * repository does not declare */
        return tp_res(res_unres(cbm_ht_get(c->ix->type_names, segs[0].name)
                                    ? CBM_DOCLINK_REASON_MISSING
                                    : CBM_DOCLINK_REASON_EXTERNAL));
    }
    /* exact: a name through an alias, and a qualified name -- two segments
     * or more, the first found among an enclosing namespace's own types and
     * namespaces. A name found by its simple name alone, or through a using,
     * is what the scope makes of it. */
    bool exact = (fd.exact || (n > SKIP_ONE && fd.in_namespace)) && !fd.joined;
    bool in_ns = (fd.exact || fd.in_namespace) && !fd.joined;
    switch (fd.first.kind) {
    case 'T':
        return path_from(c,
                         (cs_tp_t){.st = CS_TP_TYPE,
                                   .ent = fd.first.id,
                                   .exact = exact,
                                   .in_ns = in_ns,
                                   .joined = fd.joined},
                         segs, SKIP_ONE, n);
    case 'N':
        return path_from(
            c, (cs_tp_t){.st = CS_TP_NS, .ns = fd.first.id, .exact = exact, .in_ns = in_ns}, segs,
            SKIP_ONE, n);
    case 'L':
        /* a type parameter: nothing is a member of one */
        return tp_res(n == SKIP_ONE ? (cs_res_t){.st = CS_LOCAL}
                                    : res_unres(CBM_DOCLINK_REASON_MISSING));
    default:
        return tp_res(res_unres(CBM_DOCLINK_REASON_EXTERNAL));
    }
}

/* A namespace as a reference's target: declared, and without a node. */
static cs_res_t namespace_result(void) {
    return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
}

/* The reason for a path that ended at a namespace without the next name: a
 * namespace the repository declares has no such type (missing); one it only
 * has namespaces under, and the global one, hold nothing of the repository
 * (the name is outside) -- and so is what a standard-library namespace lacks,
 * declared or not. */
static cs_res_t namespace_unfound(const cs_ctx_t *c, int ns) {
    const cs_ns_t *n = &c->ix->nss[ns];
    return res_unres((n->declared && !n->standard) ? CBM_DOCLINK_REASON_MISSING
                                                   : CBM_DOCLINK_REASON_EXTERNAL);
}

/* The reason for a path that did not come to a type or a namespace. */
static cs_res_t path_unfound(const cs_ctx_t *c, const cs_tp_t *tp) {
    if (tp->st == CS_TP_RES) {
        return tp->res;
    }
    if (tp->st == CS_TP_NSFAIL) {
        return namespace_unfound(c, tp->ns);
    }
    return member_unfound(c, tp->ent, tp->next ? tp->next->name : NULL,
                          tp->next ? tp->next->arity : CS_ARITY_NONE);
}

/* ── Members of a type ───────────────────────────────────────────── */

/* The implicit roots a type of this kind derives from without naming them,
 * as far as the repository declares them. */
static int implicit_roots(const cs_ctx_t *c, char kind, int *out) {
    static const char *const enum_roots[] = {"Enum", "ValueType", "Object", NULL};
    static const char *const value_roots[] = {"ValueType", "Object", NULL};
    static const char *const object_root[] = {"Object", NULL};
    static const char system_ns[] = "System";
    const char *const *roots = object_root;
    if (kind == 'e') {
        roots = enum_roots;
    } else if (kind == 's' || kind == 't') {
        roots = value_roots;
    } else if (kind == 'i' || kind == 'd') {
        return 0;
    }
    int sys = ns_find(c->ix, 0, system_ns, sizeof(system_ns) - SKIP_ONE);
    int n = 0;
    for (int i = 0; sys >= 0 && roots[i]; i++) {
        cs_found_t fd = {0};
        add_types(c, true, sys, roots[i], 0, &fd);
        if (fd.n == SKIP_ONE) {
            out[n++] = fd.first.id;
        }
    }
    return n;
}

/* NOT the compiler's rule (see CS_BIND_INHERITED): where the compiler's
 * lookup has bound nothing, the member `seg` of the nearest supertype of
 * `ent` that has one. false when the index does not bind inherited members,
 * and when no supertype in view has the name. */
static bool inherited_result(const cs_ctx_t *c, int ent, const cs_seg_t *seg, const cs_use_t *u,
                             char kind, cs_res_t *res) {
    const cs_index_t *ix = c->ix;
    if (!ix->bind_inherited) {
        return false;
    }
    int sup[CS_MAX_SUPERS + CS_MAX_ROOTS];
    bool more = false;
    int n = supers_of(ix, ent, sup, &more);
    n += implicit_roots(c, ix->ents[ent].kind, sup + n);
    cs_query_t q = {.name = seg->name, .arity = seg->arity, .kind = kind};
    for (int i = 0; i < n; i++) {
        cs_work(SKIP_ONE);
        cs_found_t fd = {0};
        level_entity(c, sup[i], &q, &fd);
        if (fd.n > SKIP_ONE) {
            *res = found_ambiguous(&fd);
            return true;
        }
        if (fd.n == SKIP_ONE && fd.first.kind == 'T' && !u->has_params) {
            *res = type_result(c, fd.first.id, u->exact);
            return true;
        }
        cs_mb_t st = fd.n == SKIP_ONE && fd.first.kind == 'M'
                         ? members_result(c, sup[i], &q, u, res)
                         : CS_MB_NONE;
        if (st == CS_MB_RES) {
            return true;
        }
        if (st == CS_MB_INVISIBLE || fd.invisible) {
            *res = res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
            return true;
        }
    }
    if (more) {
        *res = res_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* a hierarchy past CS_MAX_SUPERS */
        return true;
    }
    return false;
}

/* A property or an event by its accessor's name (get_X, set_X; add_X,
 * remove_X). The compiler binds the accessor; the graph keeps an accessor in
 * its property or event, so that is the node. An indexer's accessors
 * (get_Item, set_Item) have none. */
static bool accessor_result(const cs_ctx_t *c, int ent, const cs_seg_t *seg, bool exact,
                            cs_res_t *res) {
    static const struct {
        const char *prefix;
        char kind;
    } acc[] = {{"get_", 'p'}, {"set_", 'p'}, {"add_", 'e'}, {"remove_", 'e'}};
    for (size_t a = 0; seg->arity <= 0 && a < sizeof(acc) / sizeof(acc[0]); a++) {
        size_t al = strlen(acc[a].prefix);
        if (strncmp(seg->name, acc[a].prefix, al) != 0 || !seg->name[al]) {
            continue;
        }
        cs_query_t q = {.name = seg->name + al, .arity = CS_ARITY_NONE, .kind = acc[a].kind};
        cs_mb_t st = members_plain(c, ent, &q, exact, res);
        if (st == CS_MB_INVISIBLE) {
            *res = res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
        } else if (st != CS_MB_RES && acc[a].kind == 'p' && strcmp(q.name, "Item") == 0 &&
                   special_declared(c, ent, "this")) {
            *res = res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
        } else if (st != CS_MB_RES) {
            continue;
        }
        return true;
    }
    return false;
}

/* `seg` as a member of the type `ent`, looked up in `ent` itself: a member
 * it declares, a type nested in it (with a parameter list: that type's
 * constructor), its own name (`Foo.Foo`: its constructor), a property or
 * event by its accessor's name. `kind` is the member kind a doc ID names. */
static cs_res_t member_in(const cs_ctx_t *c, int ent, const cs_seg_t *seg, const cs_use_t *u,
                          char kind) {
    const cs_entity_t *e = &c->ix->ents[ent];
    cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
    cs_use_t ctor_use = *u;
    ctor_use.where.member_seg = CS_NONE;
    if (strcmp(seg->name, "#ctor") == 0) {
        return ctor_result(c, ent, &ctor_use);
    }
    if (strcmp(seg->name, "#cctor") == 0) {
        return static_ctor_result(c, ent, u->exact);
    }
    cs_query_t q = {.name = seg->name, .arity = seg->arity, .kind = kind};
    cs_found_t fd = {0};
    level_entity(c, ent, &q, &fd);
    if (fd.n > SKIP_ONE) {
        return found_ambiguous(&fd);
    }
    if (fd.n == SKIP_ONE && fd.first.kind == 'T') {
        if (!u->has_params) {
            return type_result(c, fd.first.id, u->exact_type && !fd.joined);
        }
        ctor_use.where.type_seg = u->where.member_seg; /* the nested type is the last segment */
        ctor_use.exact = u->exact_type && !fd.joined;
        return ctor_result(c, fd.first.id, &ctor_use);
    }
    if (fd.n == SKIP_ONE) {
        cs_mb_t st = members_result(c, ent, &q, u, &res);
        if (st == CS_MB_RES) {
            return res;
        }
        if (st == CS_MB_INVISIBLE) {
            return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
        }
        /* it has the name, and nothing of it takes the written parameters */
    } else {
        if (fd.invisible) {
            return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
        }
        /* `Foo.Foo`: the constructor -- unless it is a bare `Foo{T}.Foo`
         * written on that generic type itself, which the compiler leaves
         * unbound */
        bool own_name = !kind && seg->arity <= 0 && strcmp(seg->name, e->name) == 0;
        bool on_type = c->type >= 0 && c->f->types[c->type].entity == ent;
        if (own_name && (u->has_params || e->arity == 0 || !on_type)) {
            return ctor_result(c, ent, &ctor_use);
        }
        if (!kind && accessor_result(c, ent, seg, u->exact, &res)) {
            return res;
        }
        if (u->r->maybe_indexer && (!kind || kind == 'p') && special_declared(c, ent, "this")) {
            return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* `Item(int)`: the indexer */
        }
    }
    if (stub_member(c, ent, &q, u, &res) || inherited_result(c, ent, seg, u, kind, &res)) {
        return res;
    }
    return member_unfound(c, ent, seg->name, seg->arity);
}

/* ── Simple names, qualified names, operators, doc IDs ───────────── */

/* A simple name no scope level has. */
static cs_res_t simple_fallback(const cs_ctx_t *c, const cs_ref_t *r, const cs_use_t *u) {
    const cs_file_t *f = c->f;
    const cs_seg_t *seg = &r->segs[0];
    cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
    /* `Foo(int)` written in a generic `Foo<T>`: no type `Foo` is in scope,
     * and the compiler takes the constructor of the type the reference
     * stands in */
    if (r->has_params && seg->arity <= 0 && c->type >= 0 &&
        strcmp(f->types[c->type].name, seg->name) == 0) {
        cs_use_t ctor_use = *u;
        ctor_use.where = (cs_where_t){.type_seg = CS_NONE, .member_seg = CS_NONE};
        return ctor_result(c, f->types[c->type].entity, &ctor_use);
    }
    cs_query_t q = {.name = seg->name, .arity = seg->arity};
    for (int t = c->type; t >= 0; t = f->types[t].outer) {
        int ent = f->types[t].entity;
        if (accessor_result(c, ent, seg, u->exact, &res)) {
            return res;
        }
        if (r->maybe_indexer && special_declared(c, ent, "this")) {
            return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* `Item(int)`: the indexer */
        }
        if (stub_member(c, ent, &q, u, &res) || inherited_result(c, ent, seg, u, 0, &res)) {
            return res;
        }
    }
    return simple_unfound(c);
}

static cs_res_t resolve_simple(const cs_ctx_t *c, const cs_ref_t *r) {
    const cs_seg_t *seg = &r->segs[0];
    cs_query_t q = {.name = seg->name, .arity = seg->arity};
    cs_found_t fd;
    bool statics = false;
    lookup(c, &q, &fd, &statics);
    if (fd.n > SKIP_ONE) {
        return found_ambiguous(&fd);
    }
    q.statics = statics; /* through a `using static`: its static members are meant */
    bool exact = fd.exact && !fd.joined;
    cs_use_t u = {.r = r,
                  .has_params = r->has_params,
                  .where = {.type_seg = CS_NONE, .member_seg = 0},
                  .exact = exact};
    if (fd.n == 0) {
        return fd.invisible ? res_unres(CBM_DOCLINK_REASON_TEST_ONLY) : simple_fallback(c, r, &u);
    }
    cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
    switch (fd.first.kind) {
    case 'L':
        /* a type parameter of the definition: no reference to code elsewhere */
        return r->has_params ? res : (cs_res_t){.st = CS_LOCAL};
    case 'N':
        return r->has_params ? res : namespace_result();
    case 'T':
        if (!r->has_params) {
            return type_result(c, fd.first.id, exact);
        }
        if (fd.exact) {
            return res; /* the compiler matches no parameter list against an alias */
        }
        /* a parameter list on a type's name: its constructor */
        u.where = (cs_where_t){.type_seg = 0, .member_seg = CS_NONE};
        return ctor_result(c, fd.first.id, &u);
    case 'M': {
        cs_mb_t st = members_result(c, fd.first.id, &q, &u, &res);
        if (st == CS_MB_RES) {
            return res;
        }
        if (st == CS_MB_INVISIBLE) {
            return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
        }
        /* the level that has the name decides: nothing further out is asked */
        if (stub_member(c, fd.first.id, &q, &u, &res) ||
            inherited_result(c, fd.first.id, seg, &u, 0, &res)) {
            return res;
        }
        return member_unfound(c, fd.first.id, seg->name, seg->arity);
    }
    default:
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
}

/* `Path.Last`: the last segment is a member of what the path before it
 * names. Of a type: member_in. Of a namespace: a type (with a parameter
 * list: its constructor) or a namespace. */
static cs_res_t resolve_qualified(const cs_ctx_t *c, const cs_ref_t *r, char kind,
                                  bool has_params) {
    int n = r->nsegs;
    const cs_seg_t *last = &r->segs[n - SKIP_ONE];
    cs_tp_t tp = resolve_path(c, r->segs, n - SKIP_ONE, true);
    cs_use_t u = {.r = r,
                  .has_params = has_params,
                  .where = {.type_seg = n - PAIR_LEN, .member_seg = n - SKIP_ONE},
                  .exact = tp.exact,
                  .exact_type = tp.exact || tp.in_ns};
    if (tp.st == CS_TP_TYPE) {
        return member_in(c, tp.ent, last, &u, kind);
    }
    if (tp.st != CS_TP_NS) {
        return path_unfound(c, &tp);
    }
    cs_found_t fd = {0};
    if (!kind) {
        add_types(c, true, tp.ns, last->name, type_arity(last->arity), &fd);
    }
    bool is_ns =
        !kind && last->arity <= 0 && ns_in_view(c, tp.ns, last->name, strlen(last->name)) >= 0;
    if (fd.n + (is_ns ? SKIP_ONE : 0) > SKIP_ONE) {
        return found_ambiguous(&fd);
    }
    if (is_ns) {
        return has_params ? res_unres(CBM_DOCLINK_REASON_MISSING) : namespace_result();
    }
    if (fd.n == 0) {
        return fd.invisible ? res_unres(CBM_DOCLINK_REASON_TEST_ONLY) : namespace_unfound(c, tp.ns);
    }
    u.exact_type = u.exact_type && !fd.joined;
    if (!has_params) {
        return type_result(c, fd.first.id, u.exact_type);
    }
    u.where = (cs_where_t){.type_seg = n - SKIP_ONE, .member_seg = CS_NONE};
    u.exact = u.exact_type;
    return ctor_result(c, fd.first.id, &u);
}

/* An operator, a conversion or an indexer: of the type the path before it
 * names, or of the nearest enclosing type that declares one. None has a
 * node: one that is declared is a graph gap. */
static cs_res_t resolve_operator(const cs_ctx_t *c, const cs_ref_t *r) {
    if (r->nsegs > 0) {
        cs_tp_t tp = resolve_path(c, r->segs, r->nsegs, true);
        if (tp.st == CS_TP_NS) {
            return namespace_unfound(c, tp.ns);
        }
        if (tp.st != CS_TP_TYPE) {
            return path_unfound(c, &tp);
        }
        return special_declared(c, tp.ent, r->op_name)
                   ? res_unres(CBM_DOCLINK_REASON_GRAPH_GAP)
                   : member_unfound(c, tp.ent, r->op_name, CS_ARITY_NONE);
    }
    for (int t = c->type; t >= 0; t = c->f->types[t].outer) {
        if (special_declared(c, c->f->types[t].entity, r->op_name)) {
            return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
        }
    }
    return c->type >= 0 ? member_unfound(c, c->f->types[c->type].entity, r->op_name, CS_ARITY_NONE)
                        : res_unres(CBM_DOCLINK_REASON_MISSING);
}

/* A doc ID names its target by its full name, from the global namespace:
 * `T:` a type, `N:` a namespace, `M:` `P:` `F:` `E:` a member of the kind
 * the letter says. An `M:` without parentheses is the overload without
 * parameters; DocFX's `O:` is the whole group. */
static cs_res_t resolve_docid(const cs_ctx_t *c, const cs_ref_t *r) {
    cs_ctx_t abs = *c; /* what stands around the reference is no part of the name */
    abs.type = CS_NONE;
    abs.member = CS_NONE;
    if (r->docid == 'T' || r->docid == 'N') {
        cs_tp_t tp = resolve_path(&abs, r->segs, r->nsegs, false);
        if (tp.st == CS_TP_RES) {
            return tp.res;
        }
        if (r->docid == 'N') {
            if (tp.st == CS_TP_NS) {
                return namespace_result();
            }
            /* a type, or a namespace the repository does not have */
            return res_unres(tp.st == CS_TP_NSFAIL ? CBM_DOCLINK_REASON_EXTERNAL
                                                   : CBM_DOCLINK_REASON_MISSING);
        }
        if (tp.st == CS_TP_TYPE) {
            return type_result(&abs, tp.ent, !tp.joined);
        }
        return tp.st == CS_TP_NS ? res_unres(CBM_DOCLINK_REASON_MISSING) : path_unfound(&abs, &tp);
    }
    if (r->nsegs < PAIR_LEN) {
        return res_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    switch (r->docid) {
    case 'M':
        return resolve_qualified(&abs, r, 'c', true);
    case 'O':
        return resolve_qualified(&abs, r, 'c', false);
    case 'P':
        return resolve_qualified(&abs, r, 'p', r->has_params);
    case 'E':
        return resolve_qualified(&abs, r, 'e', false);
    default:
        return resolve_qualified(&abs, r, 'v', false);
    }
}

static cs_res_t resolve_form(const cs_ctx_t *c, const cs_ref_t *r) {
    if (r->op) {
        return resolve_operator(c, r);
    }
    if (r->docid) {
        return resolve_docid(c, r);
    }
    if (r->nsegs == SKIP_ONE && !c->glob) {
        return resolve_simple(c, r);
    }
    return resolve_qualified(c, r, 0, r->has_params);
}

static bool reason_is_nothing(const cs_res_t *res) {
    return res->st == CS_UNRES && (res->reason == CBM_DOCLINK_REASON_MISSING ||
                                   res->reason == CBM_DOCLINK_REASON_EXTERNAL);
}

/* A reference in this context. For product code a namespace that only test
 * code declares does not exist. When the reference came to nothing and such
 * a namespace was passed over on the way, what it names may be a test
 * declaration: asked once more the way test code sees it, a name that is
 * there is test_only_target; one that is not there either keeps the reason
 * it has without that namespace. */
static cs_res_t resolve_ref(const cs_ctx_t *c, const cs_ref_t *r) {
    bool passed_over = false;
    cs_ctx_t seen = *c;
    seen.passed_over = &passed_over;
    cs_res_t res = resolve_form(&seen, r);
    if (!passed_over || !reason_is_nothing(&res)) {
        return res;
    }
    cs_ctx_t as_test = *c;
    as_test.prod = false;
    as_test.passed_over = NULL;
    cs_res_t there = resolve_form(&as_test, r);
    bool nothing = reason_is_nothing(&there) ||
                   (there.st == CS_UNRES && there.reason == CBM_DOCLINK_REASON_UNPARSEABLE);
    return nothing ? res : res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
}

/* ── Context ─────────────────────────────────────────────────────── */

/* The innermost type of `f` around `line`, or CS_NONE: the last type that
 * starts at or before the line, or the nearest of its outer types that
 * reaches the line (the types are in document order). */
static int type_at(const cs_file_t *f, uint32_t line) {
    int lo = 0;
    int hi = f->ntypes;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->types[mid].start <= line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int t = lo - SKIP_ONE; t >= 0; t = f->types[t].outer) {
        if (f->types[t].end >= line) {
            return t;
        }
    }
    return CS_NONE;
}

/* The innermost namespace declaration around `line` (0: the file itself). */
static int region_at(const cs_file_t *f, uint32_t line) {
    int lo = SKIP_ONE;
    int hi = f->nregions;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->regions[mid].start <= line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int r = lo - SKIP_ONE; r > 0; r = f->regions[r].parent) {
        if (f->regions[r].end >= line) {
            return r;
        }
    }
    return 0;
}

/* The generic method declared at `line`, or CS_NONE: its type parameters are
 * in scope in its documentation. Of the members that start at one line the
 * generic methods stand first (finish_lookups), so the first one tells --
 * however many members a line holds. */
static int generic_method_at(const cs_file_t *f, uint32_t line) {
    int lo = 0;
    int hi = f->nmembers;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->members[f->members_by_start[mid]].start < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    if (lo < f->nmembers) {
        cs_work(SKIP_ONE);
        int mi = f->members_by_start[lo];
        const cs_member_t *m = &f->members[mi];
        if (m->start == line && m->kind == 'c' && m->arity > 0) {
            return mi;
        }
    }
    return CS_NONE;
}

/* A scope of file `file`: the namespace declaration `region`, the type
 * `type` and the ones around it. */
static void ctx_at(cs_ctx_t *c, const cs_index_t *ix, int file, int region, int type) {
    memset(c, 0, sizeof(*c));
    c->ix = ix;
    c->file = file;
    c->f = &ix->files[file];
    c->unit = c->f->unit;
    c->group = c->unit >= 0 ? ix->units[c->unit].group : CS_NONE;
    c->prod = !c->f->is_test;
    c->region = region;
    c->type = type;
    c->member = CS_NONE;
    c->skip_region = CS_NONE;
}

/* The scope of the definition that starts at `line` (the documented type is
 * itself the innermost one: its members and type parameters are in scope in
 * its own documentation). A file's own doc stands outside every declaration. */
static void ctx_init(cs_ctx_t *c, const cs_index_t *ix, int file, uint32_t line, bool file_doc) {
    const cs_file_t *f = &ix->files[file];
    int type = file_doc ? CS_NONE : type_at(f, line);
    int region = file_doc ? 0 : (type >= 0 ? f->types[type].region : region_at(f, line));
    ctx_at(c, ix, file, region, type);
    c->member = (type >= 0 && f->nmembers > 0) ? generic_method_at(f, line) : CS_NONE;
}

/* True when `line` is in a part of the file whose declarations could not be
 * placed (the ranges are sorted and disjoint). */
static bool line_unplaced(const cs_file_t *f, uint32_t line) {
    int lo = 0;
    int hi = f->nunplaced;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->unplaced[mid].to < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < f->nunplaced && f->unplaced[lo].from <= line;
}

/* ── Usings and base lists ───────────────────────────────────────── */

static const char CS_GLOBAL_PREFIX[] = "global::";

/* A written type or namespace name into `segs`; *glob when it starts at the
 * global namespace. false for what is no dotted name (a tuple, an array, a
 * pointer, a text this code did not understand). */
static bool parse_name(const char *s, size_t n, cs_seg_t *segs, int *nsegs, bool *glob) {
    size_t gl = sizeof(CS_GLOBAL_PREFIX) - SKIP_ONE;
    *glob = n >= gl && strncmp(s, CS_GLOBAL_PREFIX, gl) == 0;
    if (*glob) {
        s += gl;
        n -= gl;
    }
    return parse_path(s, n, segs, nsegs);
}

/* What a using directive names: a namespace into u->ns, a type into u->ent
 * (CS_AMBIGUOUS for several). Both stay CS_NONE for what the repository does
 * not have. `c` is the scope the directive is resolved in. */
static void resolve_using(cs_using_t *u, const cs_ctx_t *c) {
    cs_seg_t segs[CS_MAX_SEGS];
    int n = 0;
    cs_ctx_t at = *c;
    bool glob = false;
    u->ns = CS_NONE;
    u->ent = CS_NONE;
    if (!parse_name(u->target, strlen(u->target), segs, &n, &glob)) {
        return;
    }
    at.glob = at.glob || glob;
    cs_tp_t tp = resolve_path(&at, segs, n, false);
    if (tp.st == CS_TP_NS && u->kind != 's') {
        u->ns = tp.ns;
    } else if (tp.st == CS_TP_TYPE && u->kind != 'n') {
        u->ent = tp.ent;
        u->joined = tp.joined;
    } else if (tp.st == CS_TP_RES && res_is(&tp.res, CBM_DOCLINK_REASON_AMBIGUOUS) &&
               u->kind != 'n') {
        u->ent = CS_AMBIGUOUS;
    }
}

/* True when the directive brings in names the repository does not declare:
 * a namespace it has no declaration of (or a standard-library one, which it
 * never has all of), a type it does not have. (An alias brings one name, and
 * says so itself where it is used.) */
static bool using_opens(const cs_index_t *ix, const cs_using_t *u) {
    if (u->kind == 'n') {
        return u->ns < 0 || !ix->nss[u->ns].declared || ix->nss[u->ns].standard;
    }
    return u->kind == 's' && u->ent == CS_NONE;
}

/* What every using directive names, and which scopes are open. A directive
 * at the top of a file, a `global using` and a project file's <Using> are
 * resolved from the global namespace: nothing else is in scope there (the
 * directives beside it are not). A directive inside a namespace declaration
 * is resolved in that namespace, without that declaration's own directives. */
static bool resolve_usings(cs_index_t *ix) {
    for (int ui = 0; ui < ix->nunits; ui++) {
        cs_unit_t *unit = &ix->units[ui];
        cs_ctx_t c = {.ix = ix,
                      .type = CS_NONE,
                      .member = CS_NONE,
                      .unit = ui,
                      .group = unit->group,
                      .glob = true,
                      .skip_region = CS_NONE};
        for (int i = 0; i < unit->nusings; i++) {
            resolve_using(&unit->usings[i], &c);
            unit->open = unit->open || using_opens(ix, &unit->usings[i]);
        }
        if (!build_using_index(ix, unit->usings, unit->nusings, &unit->using_index)) {
            return false;
        }
    }
    /* A lookup asks only regions above the directive's own, whose directives
     * are resolved already: what it remembers of them stays true. */
    cs_memo_t memo;
    memo_init(&memo);
    for (int fi = 0; fi < ix->nfiles; fi++) {
        cs_file_t *f = &ix->files[fi];
        /* Regions are in ancestor order. Publish an outer region's index
         * before resolving its children's targets; a region's own imports
         * remain excluded by skip_region during their resolution. */
        for (int r = 0; r < f->nregions; r++) {
            cs_region_t *reg = &f->regions[r];
            for (int i = reg->u_lo; i < reg->u_hi; i++) {
                cs_ctx_t c;
                ctx_at(&c, ix, fi, r, CS_NONE);
                c.prod = false; /* a directive names whatever the compiler bound */
                c.glob = r == 0;
                c.skip_region = r;
                c.memo = &memo;
                resolve_using(&f->usings[i], &c);
            }
            const cs_using_t *us = reg->u_hi > reg->u_lo ? f->usings + reg->u_lo : NULL;
            if (!build_using_index(ix, us, reg->u_hi - reg->u_lo, &reg->using_index)) {
                memo_destroy(&memo);
                return false;
            }
        }
        for (int r = 0; r < f->nregions; r++) {
            cs_region_t *reg = &f->regions[r];
            reg->open = r > 0 && f->regions[reg->parent].open;
            for (int i = reg->u_lo; !reg->open && i < reg->u_hi; i++) {
                reg->open = using_opens(ix, &f->usings[i]);
            }
        }
    }
    memo_destroy(&memo);
    return true;
}

/* The entity a written base type names in the scope of its declaration, or
 * CS_NONE. */
static int base_entity(const cs_ctx_t *c, const char *s, size_t n) {
    cs_seg_t segs[CS_MAX_SEGS];
    int nsegs = 0;
    cs_ctx_t at = *c;
    bool glob = false;
    if (!parse_name(s, n, segs, &nsegs, &glob)) {
        return CS_NONE;
    }
    at.glob = glob;
    cs_tp_t tp = resolve_path(&at, segs, nsegs, false);
    return tp.st == CS_TP_TYPE ? tp.ent : CS_NONE;
}

/* The base types of every entity, from the base lists of all its
 * declarations. A base that names nothing of the repository leaves the
 * hierarchy open. false when memory ran out. */
static bool resolve_bases(cs_index_t *ix) {
    /* listed[b] == e + 1: b is in e's base list already (a partial type's
     * declarations may each write the same base) */
    int *listed =
        (int *)cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)ix->nents + SKIP_ONE) * sizeof(int));
    bool ok = listed != NULL;
    cs_memo_t memo; /* every directive is resolved: what a lookup remembers stays true */
    memo_init(&memo);
    for (int ei = 0; ok && ei < ix->nents; ei++) {
        cs_entity_t *e = &ix->ents[ei];
        int written = 0;
        for (int d = 0; d < e->ndecls; d++) {
            written += count_list(ix->files[e->decls[d].file].types[e->decls[d].type].bases, '|');
        }
        if (written == 0) {
            continue;
        }
        e->bases = (int *)ix_alloc(ix, (size_t)written * sizeof(int));
        ok = e->bases != NULL;
        for (int d = 0; ok && d < e->ndecls; d++) {
            const cs_type_t *t = &ix->files[e->decls[d].file].types[e->decls[d].type];
            cs_ctx_t c;
            /* a base list is written outside the type it belongs to */
            ctx_at(&c, ix, e->decls[d].file, t->region, t->outer);
            c.prod = false; /* a declared base is whatever the compiler bound */
            c.memo = &memo;
            for (const char *p = t->bases; p && *p;) {
                const char *bar = strchr(p, '|');
                size_t n = bar ? (size_t)(bar - p) : strlen(p);
                int base = base_entity(&c, p, n);
                if (base < 0) {
                    e->open = true; /* the hierarchy goes on outside the repository */
                } else if (base != ei && listed[base] != ei + SKIP_ONE) {
                    listed[base] = ei + SKIP_ONE;
                    e->bases[e->nbases++] = base;
                }
                p = bar ? bar + SKIP_ONE : NULL;
            }
        }
    }
    memo_destroy(&memo);
    cbm_free(CBM_MEM_CLASS_OTHER, listed);
    ix->oom = ix->oom || !ok;
    return ok;
}

/* How many entities `e` takes its openness from: its bases, and the shared
 * trees' parts that belong to it (what they derive from, it derives from). */
static int upper_count(const cs_entity_t *e) {
    return e->nbases + (e->twin >= 0 ? SKIP_ONE : 0);
}

static int upper_at(const cs_entity_t *e, int k) {
    return k < e->nbases ? e->bases[k] : e->twin;
}

/* open_any: a type whose own base, or a base of one of its supertypes, is
 * outside the repository. Spread from the open types to everything derived
 * from them, breadth first over the reversed base lists (a base list that
 * goes round in a circle ends at the types already marked). false when
 * memory ran out. */
static bool spread_open(cs_index_t *ix) {
    size_t n = (size_t)ix->nents;
    int *first = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER, (n + PAIR_LEN) * sizeof(int));
    int *queue = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (n + SKIP_ONE) * sizeof(int));
    size_t edges = 0;
    for (int i = 0; i < ix->nents; i++) {
        edges += (size_t)upper_count(&ix->ents[i]);
    }
    int *derived = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (edges + SKIP_ONE) * sizeof(int));
    bool ok = first && queue && derived;
    if (ok) {
        int qn = 0;
        /* first[b + 2] counts the types derived from b; after the running
         * sum first[b + 1] is where b's list starts, and filling the lists
         * moves it to where the list ends: first[b] .. first[b + 1] */
        for (int i = 0; i < ix->nents; i++) {
            for (int b = 0; b < upper_count(&ix->ents[i]); b++) {
                first[upper_at(&ix->ents[i], b) + PAIR_LEN]++;
            }
        }
        for (size_t i = PAIR_LEN; i < n + PAIR_LEN; i++) {
            first[i] += first[i - SKIP_ONE];
        }
        for (int i = 0; i < ix->nents; i++) {
            for (int b = 0; b < upper_count(&ix->ents[i]); b++) {
                derived[first[upper_at(&ix->ents[i], b) + SKIP_ONE]++] = i;
            }
            if (ix->ents[i].open) {
                ix->ents[i].open_any = true;
                queue[qn++] = i;
            }
        }
        for (int head = 0; head < qn; head++) {
            int b = queue[head];
            for (int k = first[b]; k < first[b + SKIP_ONE]; k++) {
                if (!ix->ents[derived[k]].open_any) {
                    ix->ents[derived[k]].open_any = true;
                    queue[qn++] = derived[k];
                }
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, first);
    cbm_free(CBM_MEM_CLASS_OTHER, queue);
    cbm_free(CBM_MEM_CLASS_OTHER, derived);
    ix->oom = ix->oom || !ok;
    return ok;
}

/* ── Index lifecycle and the resolver hooks ──────────────────────── */

/* What made the run's ambiguous references ambiguous, by kind: the rows all
 * say `ambiguous`, and only the scope's rules and the limits are the same in
 * every repository. Nothing is logged for a run without one. */
static void log_ambiguous(const cs_stats_t *st) {
    char b[CS_WHY_COUNT][CBM_SZ_32];
    uint64_t total = 0;
    for (int i = 0; i < CS_WHY_COUNT; i++) {
        uint64_t n = atomic_load(&st->ambiguous[i]);
        total += n;
        snprintf(b[i], sizeof(b[i]), "%llu", (unsigned long long)n);
    }
    if (total > 0) {
        cbm_log_info("doc_links.cs.ambiguous", "scope_rules", b[CS_WHY_SCOPE], "shared_trees",
                     b[CS_WHY_SHARED], "assemblies", b[CS_WHY_ASSEMBLIES], "flavours",
                     b[CS_WHY_FLAVOURS], "unseen_parts", b[CS_WHY_PARTS], "limits",
                     b[CS_WHY_LIMIT]);
    }
}

static void cs_destroy(void *index) {
    cs_index_t *ix = (cs_index_t *)index;
    if (!ix) {
        return;
    }
    if (ix->stats) {
        log_ambiguous(ix->stats);
    }
    if (ix->rejected > 0) {
        /* scopes written in this run that this reader refused: each cost
         * only its own file, whose references are graph gaps */
        char files[CBM_SZ_32];
        char refs[CBM_SZ_32];
        snprintf(files, sizeof(files), "%d", ix->rejected);
        snprintf(refs, sizeof(refs), "%llu",
                 (unsigned long long)(ix->stats ? atomic_load(&ix->stats->rejected) : 0));
        cbm_log_warn("doc_links.cs.rejected_scopes", "files", files, "references", refs);
    }
    unit_memo_free(ix->unit_memo);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->undo.flags);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->undo.marks);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->stats);
    cbm_ht_free(ix->project_above);
    cbm_ht_free(ix->ns_by_key);
    cbm_ht_free(ix->ent_by_key);
    cbm_ht_free(ix->type_names);
    cbm_ht_free(ix->quarantine);
    cbm_ht_free(ix->quarantine_test);
    cbm_ht_free(ix->unit_by_dir);
    cbm_ht_free(ix->project_dirs);
    cbm_ht_free(ix->group_by_stem);
    cbm_ht_free(ix->specials);
    cbm_msb_free(ix->msb);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

/* Log why the index could not be built, and drop it. */
static void *build_failed(cs_index_t *ix, const char *step, const char *reason, const char *path) {
    cbm_log_error("doc_links.cs.error", "step", step, "reason", reason, "file", path ? path : "");
    cs_destroy(ix);
    return NULL;
}

/* The index's tables and the project files. false when memory ran out. */
static bool build_tables(cs_index_t *ix, const cbm_doclink_build_in_t *in) {
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->bind_inherited = CS_BIND_INHERITED;
    ix->ns_by_key = cbm_ht_create(CBM_SZ_1K);
    ix->ent_by_key = cbm_ht_create(CBM_SZ_4K);
    ix->type_names = cbm_ht_create(CBM_SZ_4K);
    ix->quarantine = cbm_ht_create(CBM_SZ_64);
    ix->quarantine_test = cbm_ht_create(CBM_SZ_64);
    ix->unit_by_dir = cbm_ht_create(CBM_SZ_256);
    ix->project_dirs = cbm_ht_create(CBM_SZ_256);
    ix->project_above = cbm_ht_create(CBM_SZ_256);
    ix->group_by_stem = cbm_ht_create(CBM_SZ_256);
    ix->specials = cbm_ht_create(CBM_SZ_256);
    ix->stats = (cs_stats_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(cs_stats_t));
    ix->msb = cbm_msb_new();
    ix->nfiles = in->file_count;
    ix->files = (cs_file_t *)ix_zalloc(ix, (size_t)in->file_count * sizeof(cs_file_t));
    ix->run_count = in->run_file_count;
    ix->run_to_file = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER,
                                       ((size_t)in->run_file_count + SKIP_ONE) * sizeof(int));
    if (!ix->ns_by_key || !ix->ent_by_key || !ix->type_names || !ix->quarantine ||
        !ix->quarantine_test || !ix->unit_by_dir || !ix->project_dirs || !ix->project_above ||
        !ix->group_by_stem || !ix->specials || !ix->stats || !ix->msb || !ix->files ||
        !ix->run_to_file) {
        return false;
    }
    for (int i = 0; i < in->run_file_count; i++) {
        ix->run_to_file[i] = CS_NONE;
    }
    /* namespace 0: the global one */
    ix->nss = (cs_ns_t *)ix_zalloc(ix, CBM_SZ_256 * sizeof(cs_ns_t));
    if (!ix->nss) {
        return false;
    }
    ix->nscap = CBM_SZ_256;
    ix->nnss = SKIP_ONE;
    ix->nss[0] = (cs_ns_t){.parent = CS_NONE, .name = ""};
    return collect_projects(ix, in);
}

/* Start the undo record of one file's scope. */
static void undo_begin(cs_index_t *ix) {
    ix->undo.nnss = ix->nnss;
    ix->undo.nflags = 0;
    ix->undo.nmarks = 0;
}

/* Take back what the file's scope set in the shared tables: the names it
 * quarantined, the flags it set on namespaces that were there before it, and
 * the namespaces it made. */
static void undo_scope(cs_index_t *ix) {
    cs_undo_t *u = &ix->undo;
    for (int i = u->nmarks - SKIP_ONE; i >= 0; i--) {
        cbm_ht_delete(u->marks[i].ht, u->marks[i].key);
    }
    for (int i = u->nflags - SKIP_ONE; i >= 0; i--) {
        ix->nss[u->flags[i].ns].declared = u->flags[i].declared;
        ix->nss[u->flags[i].ns].prod = u->flags[i].prod;
    }
    for (int id = ix->nnss - SKIP_ONE; id >= u->nnss; id--) {
        char key[CS_NAME_BUF + CBM_SZ_16];
        const cs_ns_t *n = &ix->nss[id];
        if (ns_key(key, sizeof(key), n->parent, n->name, strlen(n->name))) {
            cbm_ht_delete(ix->ns_by_key, key);
        }
    }
    ix->nnss = u->nnss;
    u->nflags = 0;
    u->nmarks = 0;
}

/* Scope line fields (0-based, the tag is field 0; internal/cbm/doclink_cs.c):
 * `U region kind alias target`, `T region start end kind outer name tparams
 * bases`, `M start kind explicit type name tparams sig` and `Q name`. */
enum {
    CS_SCOPE_Q_NAME = 1,
    CS_SCOPE_U_KIND = 2,
    CS_SCOPE_M_NAME = 5,
    CS_SCOPE_T_NAME = 6,
    CS_SCOPE_T_BASES = 8
};

static const char *delta_field(const char *line, size_t len, int idx, size_t *flen) {
    int f = 0;
    size_t s = 0;
    for (size_t i = 0; i <= len; i++) {
        if (i == len || line[i] == '\t') {
            if (f == idx) {
                *flen = i - s;
                return line + s;
            }
            f++;
            s = i + SKIP_ONE;
        }
    }
    *flen = 0;
    return NULL;
}

/* True when s[0, n) is a name a reference can carry (ident_ok). */
static bool cs_name_ok(const char *s, size_t n) {
    char buf[CS_NAME_BUF];
    if (n == 0 || n >= sizeof(buf)) {
        return false;
    }
    memcpy(buf, s, n);
    buf[n] = '\0';
    return ident_ok(buf);
}

/* The next type name a T or Q record of a scope blob carries, read leniently
 * (the blob is one the reader refused): a line is taken when its name field
 * is there and is a name, however the rest of it reads. Moves *cursor past
 * the lines it read; false at the end of the blob. */
static bool cs_next_type_name(const char **cursor, const char **name, size_t *len) {
    while (**cursor) {
        const char *line = *cursor;
        const char *nl = strchr(line, '\n');
        size_t n = nl ? (size_t)(nl - line) : strlen(line);
        *cursor = line + n + (nl ? SKIP_ONE : 0);
        int idx = line[0] == 'T' ? CS_SCOPE_T_NAME : line[0] == 'Q' ? CS_SCOPE_Q_NAME : CS_NONE;
        *name = idx > 0 && n > SKIP_ONE && line[SKIP_ONE] == '\t' ? delta_field(line, n, idx, len)
                                                                  : NULL;
        if (*name && cs_name_ok(*name, *len)) {
            return true;
        }
    }
    return false;
}

/* What is stored, in place of its scope blob, for a file whose scope the
 * reader refused in the run that wrote it (cs_scope_accepted, lsp_surface.c):
 * the tag, one `!` record, and one Q record per type name the T and Q records
 * of the refused blob carry (cs_next_type_name). A later run that reads it
 * back treats the file as that run did -- it declares nothing, those names
 * are quarantined, its references are graph gaps -- where the refused blob
 * itself would fail that run as a stored scope this reader does not take. */
static const char CS_REJECTED_SCOPE[] = CBM_DOCLINK_CS_SCOPE_TAG "\n!\trejected\n";

/* The marker, followed by nothing but Q records of names. */
static bool cs_scope_marked_rejected(const char *scope) {
    size_t n = sizeof(CS_REJECTED_SCOPE) - SKIP_ONE;
    if (!scope || strncmp(scope, CS_REJECTED_SCOPE, n) != 0) {
        return false;
    }
    for (const char *line = scope + n; *line;) {
        const char *nl = strchr(line, '\n');
        if (!nl || line[0] != 'Q' || line[SKIP_ONE] != '\t' ||
            !cs_name_ok(line + PAIR_LEN, (size_t)(nl - line) - PAIR_LEN)) {
            return false;
        }
        line = nl + SKIP_ONE;
    }
    return true;
}

/* The marker stored for the refused blob `scope` (the resolver's
 * rejected_scope): a memory-core block, NULL when memory ran out. A Q record
 * is never longer than the line its name is read from plus a newline. */
static char *cs_rejected_scope(const char *scope) {
    size_t w = sizeof(CS_REJECTED_SCOPE) - SKIP_ONE;
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, w + strlen(scope) + PAIR_LEN);
    if (!out) {
        return NULL;
    }
    memcpy(out, CS_REJECTED_SCOPE, w);
    const char *cursor = scope;
    const char *name = NULL;
    size_t len = 0;
    while (cs_next_type_name(&cursor, &name, &len)) {
        out[w++] = 'Q';
        out[w++] = '\t';
        memcpy(out + w, name, len);
        w += len;
        out[w++] = '\n';
    }
    out[w] = '\0';
    return out;
}

/* Quarantine the type names a refused scope blob, or its stored marker,
 * carries: what the file declares is unknown, so a reference to one of them
 * from any file is a graph gap. false when memory ran out. */
static bool quarantine_refused(cs_index_t *ix, const cs_file_t *f, const char *scope) {
    const char *cursor = scope;
    const char *name = NULL;
    size_t len = 0;
    while (cs_next_type_name(&cursor, &name, &len)) {
        char buf[CS_NAME_BUF];
        memcpy(buf, name, len); /* shorter than the buffer: cs_name_ok */
        buf[len] = '\0';
        if (!quarantine_name(ix, f, buf)) {
            return false;
        }
    }
    return true;
}

/* Whether this reader takes a scope blob written in this run: its record
 * checks, on an index of the blob's own (what makes a blob refused depends on
 * the blob alone). 1 taken, 0 refused, -1 memory ran out. A project file's
 * blob goes to the MSBuild evaluator, which takes every blob. */
static int cs_scope_accepted(const char *scope) {
    if (!scope || cbm_msb_is_project_scope(scope) || cs_scope_marked_rejected(scope)) {
        return SKIP_ONE;
    }
    cs_index_t *ix = (cs_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return CBM_NOT_FOUND;
    }
    cbm_arena_init_lazy(&ix->arena, CBM_ARENA_APPEND_BLOCK);
    ix->ns_by_key = cbm_ht_create(CBM_SZ_16);
    ix->quarantine = cbm_ht_create(CBM_SZ_16);
    ix->quarantine_test = cbm_ht_create(CBM_SZ_16);
    ix->nss = (cs_ns_t *)ix_zalloc(ix, CBM_SZ_16 * sizeof(cs_ns_t));
    int accepted = CBM_NOT_FOUND;
    if (ix->ns_by_key && ix->quarantine && ix->quarantine_test && ix->nss) {
        ix->nscap = CBM_SZ_16;
        ix->nnss = SKIP_ONE;
        ix->nss[0] = (cs_ns_t){.parent = CS_NONE, .name = ""};
        cs_file_t f = {.rel_path = "Scope.cs"};
        undo_begin(ix);
        bool parsed = parse_scope(ix, &f, scope);
        accepted = ix->oom ? CBM_NOT_FOUND : (parsed ? SKIP_ONE : 0);
    }
    cbm_ht_free(ix->ns_by_key);
    cbm_ht_free(ix->quarantine);
    cbm_ht_free(ix->quarantine_test);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->undo.flags);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->undo.marks);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
    return accepted;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: true when this reader takes the scope blob `scope`. A test holds
 * every blob the scanner writes against it. */
bool cbm_doclink_cs_test_scope_parses(const char *scope) {
    return cs_scope_accepted(scope) == SKIP_ONE;
}

/* Test seam: set, the indexes are built without the unit memo. */
static atomic_bool cs_test_no_unit_memo;

void cbm_doclink_cs_test_unit_memo(bool on) {
    atomic_store(&cs_test_no_unit_memo, !on);
}
#endif

/* Every file of the index with what its scope declares. A scope read back
 * from the store that this reader refuses is not one this code wrote: the
 * index is not built over it (nothing may be resolved around a declaration
 * that is not known), and its path is returned. A scope written in this run
 * that the reader refuses is the scanner's and this reader's disagreement,
 * which costs only its own file: what its parse set is taken back, the file
 * declares nothing, and its references are graph gaps (counted, logged). The
 * names of its types are quarantined: what it declares is unknown, so a
 * reference to one of them from any file is a graph gap too. A scope stored
 * as the rejected marker is read as in the run that refused it.
 * Returns "" when memory ran out, NULL when all is well. */
static const char *build_files(cs_index_t *ix, const cbm_doclink_build_in_t *in) {
    CBMHashTable *dir_unit = cbm_ht_create(CBM_SZ_1K);
    const char *bad = dir_unit ? NULL : "";
    for (int i = 0; !bad && i < in->file_count; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        cs_file_t *f = &ix->files[i];
        f->rel_path = src->rel_path;
        f->module_qn =
            cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_CSHARP);
        f->is_test = cs_is_test_path(src->rel_path);
        /* a project file declares nothing: its blob went to the evaluator */
        const char *scope = cbm_msb_is_project_scope(src->scope) ? NULL : src->scope;
        /* a scope its own run refused, stored as the marker: as in that run */
        bool marked = cs_scope_marked_rejected(scope);
        undo_begin(ix);
        if (marked || (scope && !parse_scope(ix, f, scope))) {
            bool fresh = src->run_file >= 0 && src->run_file < in->run_file_count;
            if (!marked && (ix->oom || !fresh)) {
                bad = ix->oom ? "" : src->rel_path;
                break;
            }
            undo_scope(ix);
            *f = (cs_file_t){.rel_path = f->rel_path,
                             .module_qn = f->module_qn,
                             .is_test = f->is_test,
                             .rejected = true};
            ix->rejected++;
            if (!quarantine_refused(ix, f, scope)) {
                bad = ""; /* memory ran out */
                break;
            }
            scope = NULL;
        }
        if (!scope) {
            /* no scope: an empty file (only its own region) */
            f->regions = (cs_region_t *)ix_zalloc(ix, sizeof(cs_region_t));
            f->nregions = SKIP_ONE;
            if (f->regions) {
                f->regions[0].parent = CS_NONE;
            }
        }
        f->unit = unit_of(ix, dir_unit, src->rel_path);
        f->is_ref = f->unit >= 0 && ix->units[f->unit].is_ref;
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (!f->module_qn || ix->oom) {
            bad = "";
        }
    }
    cbm_ht_free(dir_unit);
    /* which namespaces are the standard library's: a namespace is made after
     * the one above it, so one pass in order sees every parent first */
    for (int i = SKIP_ONE; i < ix->nnss; i++) {
        cs_ns_t *n = &ix->nss[i];
        n->standard =
            n->parent == 0 ? strcmp(n->name, CS_STANDARD_ROOT) == 0 : ix->nss[n->parent].standard;
    }
    return bad;
}

typedef struct {
    const char *path;
    int idx;
} cs_file_order_t;

static int file_order_cmp(const void *a, const void *b) {
    const cs_file_order_t *x = (const cs_file_order_t *)a;
    const cs_file_order_t *y = (const cs_file_order_t *)b;
    int c = strcmp(x->path ? x->path : "", y->path ? y->path : "");
    return c ? c : (x->idx > y->idx) - (x->idx < y->idx);
}

/* The graph node of every declaration. false when memory ran out. Files are
 * bound in path order: a file's bindings do not depend on the others, but the
 * scratch table's cost does on the order (it is emptied per file), and the
 * order the file system listed the files in differs between systems. */
static bool build_nodes(cs_index_t *ix, const cbm_gbuf_t *g) {
    cs_node_pass_t np = {.names = cbm_ht_create(CBM_SZ_256), .sized = CBM_SZ_256};
    cbm_arena_init(&np.keys);
    cs_file_order_t *order = (cs_file_order_t *)cbm_alloc(
        CBM_MEM_CLASS_OTHER, (size_t)(ix->nfiles ? ix->nfiles : 1) * sizeof(*order));
    bool ok = np.names != NULL && order != NULL;
    if (!order) {
        ix->oom = true;
    }
    for (int i = 0; ok && i < ix->nfiles; i++) {
        order[i] = (cs_file_order_t){ix->files[i].rel_path, i};
    }
    if (ok && ix->nfiles > 1) {
        qsort(order, (size_t)ix->nfiles, sizeof(*order), file_order_cmp);
    }
    for (int i = 0; ok && i < ix->nfiles; i++) {
        ok = bind_nodes(ix, &ix->files[order[i].idx], g, &np);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, order);
    cbm_ht_free(np.names);
    cbm_arena_destroy(&np.keys);
    cbm_free(CBM_MEM_CLASS_OTHER, np.last);
    return ok;
}

static void log_index(const cs_index_t *ix, const cs_msb_totals_t *msb) {
    int incomplete = 0;
    int open_units = 0;
    for (int i = 0; i < ix->nents; i++) {
        incomplete += ix->ents[i].incomplete;
    }
    for (int i = 0; i < ix->nunits; i++) {
        open_units += ix->units[i].open;
    }
    char b[CBM_SZ_7][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[1], sizeof(b[1]), "%d", ix->nents);
    snprintf(b[2], sizeof(b[2]), "%d", ix->nunits - ix->nshared);
    snprintf(b[3], sizeof(b[3]), "%d", incomplete);
    snprintf(b[4], sizeof(b[4]), "%u",
             (unsigned)(cbm_ht_count(ix->quarantine) + cbm_ht_count(ix->quarantine_test)));
    snprintf(b[5], sizeof(b[5]), "%d", ix->ngroups);
    snprintf(b[6], sizeof(b[6]), "%d", ix->nshared);
    cbm_log_info("doc_links.cs.index", "files", b[0], "types", b[1], "projects", b[2], "assemblies",
                 b[5], "shared_trees", b[6], "incomplete_types", b[3], "quarantined_names", b[4]);
    /* what the MSBuild project files could not tell: conditions, values and
     * constructs that were not evaluated, and imports of files the index
     * does not hold */
    snprintf(b[0], sizeof(b[0]), "%d", ix->nprojects);
    snprintf(b[1], sizeof(b[1]), "%d", msb->unevaluable);
    snprintf(b[2], sizeof(b[2]), "%d", msb->outside);
    snprintf(b[3], sizeof(b[3]), "%d", open_units);
    cbm_log_info("doc_links.cs.msbuild", "project_files", b[0], "unevaluable", b[1],
                 "imports_outside", b[2], "open_projects", b[3]);
}

static void *cs_build(const cbm_doclink_build_in_t *in) {
    cs_index_t *ix = (cs_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    if (!build_tables(ix, in)) {
        return build_failed(ix, "tables", "alloc", NULL);
    }
    const char *bad = build_files(ix, in);
    if (bad) {
        return build_failed(ix, "scopes", bad[0] ? "bad_scope" : "alloc", bad);
    }
    cs_msb_totals_t msb = {0};
    if (!units_collect_usings(ix, &msb)) {
        return build_failed(ix, "projects", "alloc", NULL);
    }
    if (!build_entities(ix) || !entity_units(ix) || !entity_fulls(ix) || !build_named(ix)) {
        return build_failed(ix, "entities", "alloc", NULL);
    }
    entity_twins(ix);
    if (!build_nodes(ix, in->graph) || !build_binds(ix) || !build_mrefs(ix) || !build_extras(ix)) {
        return build_failed(ix, "nodes", "alloc", NULL);
    }
    if (!build_name_scopes(ix) || !resolve_usings(ix)) {
        return build_failed(ix, "usings", "alloc", NULL);
    }
    if (!resolve_bases(ix) || !spread_open(ix) || ix->oom) {
        return build_failed(ix, "bases", "alloc", NULL);
    }
    /* only now: what the build's own lookups saw of the units' directives
     * was not yet the whole index (NULL when memory ran out: no memo) */
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (!atomic_load(&cs_test_no_unit_memo))
#endif
    {
        ix->unit_memo = unit_memo_new();
    }
    log_index(ix, &msb);
    return ix;
}

/* The reason for a reference this code does not understand: one that names a
 * keyword type in a form that is no name (`int[]`, `string?`) is the BCL's;
 * one too long to be read is never judged by its beginning. */
static int unparsed_reason(const char *raw) {
    const char *s = raw ? raw : "";
    while (isspace((unsigned char)*s)) {
        s++;
    }
    char head[CBM_SZ_32];
    size_t n = strcspn(s, "({<.[?* \t");
    if (strlen(s) >= CS_REF_BUF || n == 0 || n >= sizeof(head)) {
        return CBM_DOCLINK_REASON_UNPARSEABLE;
    }
    memcpy(head, s, n);
    head[n] = '\0';
    return keyword_type(head) ? CBM_DOCLINK_REASON_EXTERNAL : CBM_DOCLINK_REASON_UNPARSEABLE;
}

/* A keyword alias (`int`, `string.Empty`) names its System type whatever
 * stands around the reference. */
static void rewrite_keyword(cs_ref_t *r) {
    static const char system_ns[] = "System";
    if (r->docid || r->nsegs == 0 || r->nsegs >= CS_MAX_SEGS || r->segs[0].arity > 0) {
        return;
    }
    const char *bcl = keyword_type(r->segs[0].name);
    if (!bcl) {
        return;
    }
    memmove(&r->segs[1], &r->segs[0], (size_t)r->nsegs * sizeof(cs_seg_t));
    r->segs[0] = (cs_seg_t){.arity = CS_ARITY_NONE};
    snprintf(r->segs[0].name, sizeof(r->segs[0].name), "%s", system_ns);
    snprintf(r->segs[1].name, sizeof(r->segs[1].name), "%s", bcl);
    r->nsegs++;
    r->keyword = true;
    r->glob = true;
}

/* True when the reference names a type some file declares at a place no
 * scope could be established for: it could be that declaration. */
static bool names_quarantined(const cs_index_t *ix, const cs_file_t *f, const cs_ref_t *r) {
    for (int i = 0; i < r->nsegs; i++) {
        if (cbm_ht_get(ix->quarantine, r->segs[i].name) ||
            (f->is_test && cbm_ht_get(ix->quarantine_test, r->segs[i].name))) {
            return true;
        }
    }
    return false;
}

/* The working state of one file's references: the memo of their lookups. */
static void *cs_file_begin(const void *index, int run_file) {
    (void)index;
    (void)run_file;
    cs_memo_t *m = (cs_memo_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
    if (m) {
        memo_init(m);
    }
    return m;
}

static void cs_file_end(void *state) {
    cs_memo_t *m = (cs_memo_t *)state;
    if (m) {
        memo_destroy(m);
        cbm_free(CBM_MEM_CLASS_OTHER, m);
    }
}

static void cs_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    const cs_index_t *ix = (const cs_index_t *)index;
    (void)graph; /* every node was looked up when the index was built */
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || run_file < 0 || run_file >= ix->run_count || ix->run_to_file[run_file] < 0) {
        return;
    }
    cs_ref_t r;
    if (!parse_cref(link->raw, &r)) {
        out->reason = unparsed_reason(link->raw);
        return;
    }
    rewrite_keyword(&r);
    /* What could not be placed is not resolved around: a definition in the
     * part of its file where the braces stop pairing has no known scope, and
     * a name some file declares without a known namespace could be that
     * declaration. Both are declared-but-unplaced, i.e. graph gaps. */
    int file = ix->run_to_file[run_file];
    const cs_file_t *f = &ix->files[file];
    if (f->rejected) {
        /* its scope was refused: nothing is known around the reference */
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        atomic_fetch_add_explicit(&ix->stats->rejected, 1, memory_order_relaxed);
        return;
    }
    bool file_doc = (link->flags & CBM_DOCLINK_FLAG_FILE) != 0;
    if ((!file_doc && line_unplaced(f, link->def_line)) || names_quarantined(ix, f, &r)) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    cs_ctx_t c;
    ctx_init(&c, ix, file, link->def_line, file_doc);
    c.glob = r.glob;
    c.memo = (cs_memo_t *)state;
    cs_res_t res = resolve_ref(&c, &r);
    if (res.st == CS_LOCAL) {
        out->kind = CBM_DOCLINK_LOCAL;
    } else if (res.st == CS_OK && res.node) {
        out->kind = CBM_DOCLINK_EDGE;
        out->target = res.node;
        out->exact = res.exact;
    } else if (r.keyword && res.reason == CBM_DOCLINK_REASON_MISSING) {
        out->reason = CBM_DOCLINK_REASON_EXTERNAL; /* the repository does not declare it */
    } else {
        out->reason = res.reason;
        if (res.reason == CBM_DOCLINK_REASON_AMBIGUOUS && res.why < CS_WHY_COUNT) {
            atomic_fetch_add_explicit(&ix->stats->ambiguous[res.why], 1, memory_order_relaxed);
        }
    }
}

/* ── Incremental scope rules ─────────────────────────────────────── */

/* A using directive only its own file sees: one that is not `global`. */
static bool delta_local_using(const char *line, size_t len) {
    if (line[0] != 'U') {
        return false;
    }
    size_t klen = 0;
    const char *kind = delta_field(line, len, CS_SCOPE_U_KIND, &klen);
    return kind && klen > 0 && !memchr(kind, 'g', klen);
}

/* The next scope line -- with `local_usings` only the file's own using
 * directives, without it every other line; false at the end. */
static bool delta_next_line(const char **cursor, bool local_usings, const char **line,
                            size_t *len) {
    for (const char *p = *cursor; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t n = nl ? (size_t)(nl - p) : strlen(p);
        const char *next = nl ? nl + SKIP_ONE : p + n;
        if (n > 0 && delta_local_using(p, n) == local_usings) {
            *line = p;
            *len = n;
            *cursor = next;
            return true;
        }
        p = next;
    }
    *cursor = NULL;
    return false;
}

/* True when the two scopes have the same own using directives, in order. */
static bool delta_same_usings(const char *stored, const char *fresh) {
    const char *sl = NULL;
    const char *fl = NULL;
    size_t slen = 0;
    size_t flen = 0;
    for (;;) {
        bool hs = delta_next_line(&stored, true, &sl, &slen);
        bool hf = delta_next_line(&fresh, true, &fl, &flen);
        if (!hs || !hf) {
            return hs == hf;
        }
        if (slen != flen || memcmp(sl, fl, slen) != 0) {
            return false;
        }
    }
}

/* True when the scope declares a type that has a base list. */
static bool delta_has_bases(const char *scope) {
    for (const char *p = scope; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t n = nl ? (size_t)(nl - p) : strlen(p);
        size_t blen = 0;
        if (p[0] == 'T' && delta_field(p, n, CS_SCOPE_T_BASES, &blen) && blen > 0) {
            return true;
        }
        p = nl ? nl + SKIP_ONE : NULL;
    }
    return false;
}

/* Compare a changed file's stored and fresh scopes line by line, in order.
 * Two differences leave every other file's resolution alone:
 *   - the file's own using directives (namespace, static, alias), as long as
 *     the file declares no type with a base list: they scope the file
 *     itself, and it is re-extracted anyway. A base list is resolved through
 *     them, and what a type derives from decides how OTHER files' references
 *     to its members come out;
 *   - a member the fresh scope no longer has. Every member line is a method,
 *     constructor, property, field, event, operator, indexer or enum member,
 *     so a reference that depended on it either bound it (an edge into this
 *     file) or names it in its unresolved row: the name is reported.
 * Everything else is GLOBAL: a new or changed line (a type, a member, a
 * signature, a namespace, a global using), a removed type (it may be another
 * type's base or an alias target, which changes how references THROUGH those
 * classify), a removed namespace, global using, quarantined name or unplaced
 * range, and a changed order (same-path declarations own their node by
 * order, and a record names its type by its position).
 *
 * An MSBuild project file's blob sets the global usings of every C# file of
 * its project: any difference between two of those is GLOBAL. An edit that
 * leaves the blob as it is -- a target, a package reference -- changes
 * nobody's scope. */
static int cs_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    /* a rejected file declares nothing: unchanged while it stays rejected */
    if (cbm_msb_is_project_scope(stored) || cbm_msb_is_project_scope(fresh) ||
        cs_scope_marked_rejected(stored) || cs_scope_marked_rejected(fresh)) {
        return strcmp(stored, fresh) == 0 ? CBM_DOCLINK_DELTA_LOCAL : CBM_DOCLINK_DELTA_GLOBAL;
    }
    if (!delta_same_usings(stored, fresh) && (delta_has_bases(stored) || delta_has_bases(fresh))) {
        return CBM_DOCLINK_DELTA_GLOBAL;
    }
    const char *sp = stored;
    const char *fp = fresh;
    const char *sl = NULL;
    const char *fl = NULL;
    size_t slen = 0;
    size_t flen = 0;
    bool hs = delta_next_line(&sp, false, &sl, &slen);
    bool hf = delta_next_line(&fp, false, &fl, &flen);
    while (hs || hf) {
        if (hs && hf && slen == flen && memcmp(sl, fl, slen) == 0) {
            hs = delta_next_line(&sp, false, &sl, &slen);
            hf = delta_next_line(&fp, false, &fl, &flen);
            continue;
        }
        if (!hs || sl[0] != 'M') {
            return CBM_DOCLINK_DELTA_GLOBAL;
        }
        size_t nlen = 0;
        const char *name = delta_field(sl, slen, CS_SCOPE_M_NAME, &nlen);
        if (!name || nlen == 0 || !removed || !removed(ud, name, nlen)) {
            return CBM_NOT_FOUND; /* not a member line this code wrote, or not recordable */
        }
        hs = delta_next_line(&sp, false, &sl, &slen);
    }
    return CBM_DOCLINK_DELTA_LOCAL;
}

/* XML is listed for the MSBuild project files: their scope blobs carry the C#
 * tag, and the index needs the ones of this run as it needs the stored ones.
 * An XML file that is no project file has no blob and no references. */
const cbm_doclink_resolver_t cbm_doclink_cs_resolver = {
    .langs = {CBM_LANG_CSHARP, CBM_LANG_XML},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_CS_SCOPE_TAG,
    .build = cs_build,
    .destroy = cs_destroy,
    .file_begin = cs_file_begin,
    .file_end = cs_file_end,
    .resolve = cs_resolve,
    .scope_delta = cs_scope_delta,
    .scope_accepted = cs_scope_accepted,
    .rejected_scope = cs_rejected_scope,
};
