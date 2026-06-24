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

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "go_lsp.h" /* CBMLSPDef, CBMResolvedCallArray reused across languages */

/* Java `use`-style import kinds. Mirrors PHP's enum with Java semantics. */
enum {
    CBM_JAVA_IMPORT_TYPE = 0,      /* import com.foo.Bar; — type import */
    CBM_JAVA_IMPORT_STATIC = 1,    /* import static com.foo.Bar.method; */
    CBM_JAVA_IMPORT_ON_DEMAND = 2, /* import com.foo.*; — package on-demand */
    CBM_JAVA_IMPORT_STATIC_OD = 3, /* import static com.foo.Bar.*; */
};

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

/* Cross-file LSP: build registry from defs + stdlib, re-parse if needed,
 * walk and resolve. defs include both local + cross-file definitions. */
void cbm_run_java_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out);

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
