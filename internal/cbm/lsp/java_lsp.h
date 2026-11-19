#ifndef CBM_LSP_JAVA_LSP_H
#define CBM_LSP_JAVA_LSP_H

/*
 * java_lsp.h — Pure-C Java semantic resolver.
 *
 * Reverse-engineered from JLS §6 (Names) and §15 (Expressions) plus the
 * algorithm shape used by Eclipse JDT-LS / java-language-server (which both
 * delegate to javac.com.sun.source). The goal is parity with what JDT-LS
 * exposes through textDocument/definition + textDocument/references for
 * call-site resolution, *without* shelling out to javac.
 *
 * Mirrors the structure of go_lsp.h / c_lsp.h / php_lsp.h / py_lsp.h.
 *
 * Resolution scheme (single file):
 *   1. Tree-sitter parses Java source into AST.
 *   2. Build a CBMTypeRegistry from this file's CBMDefinitions + Java
 *      stdlib (java.lang.*, java.util.*, java.io.*, java.util.function.*,
 *      java.util.stream.*).
 *   3. Walk the AST and for every method_invocation / object_creation /
 *      field_access expression, evaluate the expression's type using
 *      JLS-style scope chains (block → method params → class members →
 *      superclass chain → outer class → import single → import on-demand →
 *      java.lang → same package).
 *   4. Match the textual call (callee name + arity) against the resolved
 *      receiver type's method set, walking superclasses and interfaces.
 *      Best-overload resolution falls back to argument count match.
 *   5. Emit CBMResolvedCall entries with confidence ≥ 0.6 (the LSP floor
 *      enforced by lsp_resolve.h).
 */

#include "lsp_name_rows.h"
#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "go_lsp.h" /* CBMLSPDef, CBMResolvedCallArray reused across languages */

#define JAVA_EXT_MEMO 8

/* Java `use`-style import kinds. Mirrors PHP's enum with Java semantics. */
enum {
    CBM_JAVA_IMPORT_TYPE = 0,      /* import com.foo.Bar; — type import */
    CBM_JAVA_IMPORT_STATIC = 1,    /* import static com.foo.Bar.method; */
    CBM_JAVA_IMPORT_ON_DEMAND = 2, /* import com.foo.*; — package on-demand */
    CBM_JAVA_IMPORT_STATIC_OD = 3, /* import static com.foo.Bar.*; */
    /* A name the calling file's imports bind to a project type, verified by
     * the declared-type step (src/pipeline/type_scope.h): the registered QN of
     * that type. Handed in by the cross-file walks. */
    CBM_JAVA_IMPORT_IDENTITY = 4,
};

/* True when `name` (len bytes) is one of the java.lang types the engine knows:
 * the implicit on-demand import of every compilation unit (the declared-type
 * step's implicit external scope for Java). */
bool cbm_java_lang_type(const char *name, size_t len);

/* Per-file resolution context. */
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    CBMScope *current_scope;

    /* Java package — the package declared in the file ("com.example"), or
     * empty string for the unnamed package. Stored in dotted form. */
    const char *package_name;

    /* The path-derived module QN for this file (passed in from the caller),
     * used as the QN prefix for types defined here. */
    const char *module_qn;

    /* Import map. Each entry is one of CBM_JAVA_IMPORT_*.
     *   - TYPE:      local_name="Bar",   target_qn="com.foo.Bar"
     *   - STATIC:    local_name="sqrt",  target_qn="java.lang.Math.sqrt"
     *   - ON_DEMAND: local_name="*",     target_qn="com.foo"
     *   - STATIC_OD: local_name="*",     target_qn="java.lang.Math"
     */
    const char **import_local_names;
    const char **import_target_qns;
    int *import_kinds;
    int import_count;
    int import_cap;
    CBMNameRows import_rows; /* import_local_names by name, rows in table order */
    bool imports_scanned;    /* the file's package and import declarations are read */
    /* The run's project packages, for a cross run without a shared registry
     * (java_receiver_is_external); NULL when not known. */
    const struct CBMJavaProjectPackages *project_packages;
    bool cross_run;            /* a cross-file run (counts answers, see below) */
    void *unknown_scope_types; /* CBMHashTable of types a simple name resolved to inside a
                                * class with a supertype the registry does not know */

    /* While signatures are patched: the registered functions' QNs, by name
     * (overloads share a QN). NULL func_names: a scan answers instead. */
    const char **func_names;
    int func_name_count;
    CBMNameRows func_rows;
    CBMArena *func_rows_arena;

    /* Current enclosing context. */
    const char *enclosing_method_qn;    /* QN of the nearest method/ctor */
    const char *enclosing_class_qn;     /* QN of the nearest class/interface/enum */
    const char *enclosing_super_qn;     /* QN of the immediate superclass (NULL ⇒ Object) */
    const char *enclosing_class_short;  /* short name of enclosing class — for "this" + ctor */
    const char **enclosing_class_stack; /* nested-class stack (enclosing_class_qn at each depth) */
    int enclosing_class_depth;
    int enclosing_class_cap;

    /* Output: resolved + diagnostic call edges. */
    CBMResolvedCallArray *resolved_calls;

    /* Recursion guards. */
    int eval_depth;
    int statement_depth;
    int walk_depth; /* java_resolve_calls_in_node self-recursion (AST nesting) */

    /* Debug mode (CBM_LSP_DEBUG env). */
    bool debug;

    /* java_receiver_is_external's answers for the packages probed so far
     * (a few per file), so each costs one registry walk. */
    const char *ext_memo_pkg[JAVA_EXT_MEMO];
    bool ext_memo_val[JAVA_EXT_MEMO];
    int ext_memo_next;
} JavaLSPContext;

/* ── Initialization / configuration ───────────────────────────────── */

void java_lsp_init(JavaLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *package_name, const char *module_qn,
                   CBMResolvedCallArray *out);

void java_lsp_add_import(JavaLSPContext *ctx, const char *local_name, const char *target_qn,
                         int kind);

/* ── Walking / resolution ─────────────────────────────────────────── */

void java_lsp_process_file(JavaLSPContext *ctx, TSNode root);

/* Evaluate the type of an arbitrary Java expression node. May return
 * cbm_type_unknown(); never returns NULL. */
const CBMType *java_eval_expr_type(JavaLSPContext *ctx, TSNode node);

/* Convert a Java type AST node (type_identifier, generic_type, array_type,
 * scoped_type_identifier, void_type, integral_type, ...) into a CBMType. */
const CBMType *java_parse_type_node(JavaLSPContext *ctx, TSNode node);

/* Process a Java statement, binding any declared variables/parameters into
 * the current scope. */
void java_process_statement(JavaLSPContext *ctx, TSNode node);

/* Resolve a Java type name (bare or qualified) against the current scope
 * (imports + java.lang + same package). Returns the registered FQN or NULL. */
const char *java_resolve_type_name(JavaLSPContext *ctx, const char *name);

/* Lookup a method on a class, walking the super-chain and implemented
 * interfaces. Returns the matched function or NULL. */
const CBMRegisteredFunc *java_lookup_method(JavaLSPContext *ctx, const char *class_qn,
                                            const char *method_name, int arg_count);

/* Lookup a field on a class, walking the super-chain. Returns the field's
 * type, or cbm_type_unknown() on miss. */
const CBMType *java_lookup_field_type(JavaLSPContext *ctx, const char *class_qn,
                                      const char *field_name);

/* ── Top-level entry points ───────────────────────────────────────── */

/* Single-file LSP: build registry from file defs + stdlib, walk and resolve. */
void cbm_run_java_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root);

/* Cross-file LSP: register defs into a registry. A def's declared types (its
 * return, parameter and field types and its supertypes) take the identities
 * the declared-type step resolved in the scope of the file that wrote them
 * (CBMLSPDef.type_scope, in the registry's QN form); a spelling without one
 * gets no project type. Register every type def first and call
 * cbm_java_mark_ambiguous_types before the methods: an identity two defs
 * share is no answer. */
void cbm_java_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                int def_count);

/* Mark every type QN that two or more of the type defs register (a nested
 * type flattened onto a same-named one of its package, one class in two
 * source trees): no identity naming it is used. Before finalize. */
void cbm_java_mark_ambiguous_types(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                   int def_count);

/* Tier 2 (#1669): build the shared JVM cross registry ONCE per run, then
 * resolve each file against it with a small own-module overlay. Skips the
 * per-file registry build that made cross-file LSP O(files x corpus_defs).
 * In both cross entry points below, import_names / import_qns are the calling
 * file's verified import identities (CBM_JAVA_IMPORT_IDENTITY), local name ->
 * registered QN; the file's own import declarations are read from its AST. */
CBMTypeRegistry *cbm_java_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count);
void cbm_run_java_lsp_cross_with_registry(CBMArena *arena, CBMFileResult *result,
                                          const char *source, int source_len, const char *module_qn,
                                          CBMTypeRegistry *reg, const char **import_names,
                                          const char **import_qns, int import_count,
                                          TSTree *cached_tree, CBMResolvedCallArray *out);

void cbm_run_java_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out);

/* The run's project packages: declared(ud, package, len) is true when some
 * project file declares `package` (len bytes, no trailing dot) or a package
 * below it. A cross run over a shared registry reads that from the registry;
 * the per-file run below has none and asks this instead. */
typedef struct CBMJavaProjectPackages {
    bool (*declared)(const void *ud, const char *package, size_t len);
    const void *ud;
} CBMJavaProjectPackages;

/* cbm_run_java_lsp_cross with the run's project packages (NULL: as
 * cbm_run_java_lsp_cross), so a receiver of a library type is told apart
 * from an unresolved project type as on a run over the shared registry. */
void cbm_run_java_lsp_cross_packages(CBMArena *arena, const char *source, int source_len,
                                     const char *module_qn, CBMLSPDef *defs, int def_count,
                                     const char **import_names, const char **import_qns,
                                     int import_count, TSTree *cached_tree,
                                     const CBMJavaProjectPackages *packages, CBMFileResult *result,
                                     CBMResolvedCallArray *out);

/* Engine answers of cross runs that rest on a simple type name resolved
 * inside a class whose supertypes (its own or an enclosing class's) include
 * one the registry does not know (a library type, which may declare member
 * types no answer sees): the count since the last call, which restarts it. A
 * measurement only (CBM_PROFILE line java.unknown_scope, end of a resolve). */
uint64_t cbm_java_take_unknown_scope_answers(void);

/* The supertypes a cross run's registry does not hold, since the last call
 * (which restarts them; CBM_PROFILE line java.unseen, end of a resolve):
 * *project, answers withheld because the supertype is a project type the
 * registry does not hold or a spelling no type has (rule i; on a full run
 * each is a defect); *external and *unknown, the answers of
 * cbm_java_take_unknown_scope_answers split by the supertypes they rest on
 * (rule ii): all known outside the project, or one of unknown package. */
void cbm_java_take_unseen_counts(uint64_t *project, uint64_t *external, uint64_t *unknown);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* The member type `name` that the type `cls` inherits along its registered
 * supertypes (JLS 8.5), or NULL: the walk the resolver runs, for the growth
 * tests (its work counts in cbm_lsp_work_steps). */
const char *cbm_java_test_inherited_member(const CBMTypeRegistry *reg, CBMArena *arena,
                                           const char *cls, const char *name);
#endif

/* Register the Java standard library (java.lang/util/io/etc.) into reg.
 * Implementation lives in generated/java_stdlib_data.c. */
void cbm_java_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

/* ── Batch cross-file LSP ─────────────────────────────────────────── */

typedef struct {
    const char *source;
    int source_len;
    const char *module_qn;
    TSTree *cached_tree;
    CBMLSPDef *defs;
    int def_count;
    const char **import_names;
    const char **import_qns;
    int import_count;
} CBMBatchJavaLSPFile;

void cbm_batch_java_lsp_cross(CBMArena *arena, CBMBatchJavaLSPFile *files, int file_count,
                              CBMResolvedCallArray *out);

#endif /* CBM_LSP_JAVA_LSP_H */
