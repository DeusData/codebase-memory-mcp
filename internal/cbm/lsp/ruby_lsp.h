#ifndef CBM_LSP_RUBY_LSP_H
#define CBM_LSP_RUBY_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"

/* Ruby mixin kinds. Lookup order for an instance method on class C:
 *   prepended modules (last prepended first) → C's own methods →
 *   included modules (last included first) → superclass chain.
 * `extend` adds a module's instance methods to C's singleton (class-side). */
typedef enum {
    RUBY_MIXIN_INCLUDE = 0,
    RUBY_MIXIN_PREPEND,
    RUBY_MIXIN_EXTEND,
} RubyMixinKind;

/* One class/module discovered in the file (or, cross-file, in the project).
 * `path` is the dotted source-level constant path (e.g. "Admin.User" for
 * `Admin::User`); `qn` is the full graph QN (module_qn.path for file-local
 * classes; the extractor QN for cross-file defs). `superclass_qn` is resolved
 * against the class table + stdlib after collection; NULL when unknown. */
typedef struct {
    const char *path;
    const char *qn;
    const char *superclass_ref; /* raw dotted superclass spelling, or NULL */
    const char *superclass_qn;  /* resolved QN (table/stdlib), or NULL */
    bool is_module;             /* `module` (mixin source) vs `class` */
} RubyClassInfo;

/* One `include`/`prepend`/`extend` record: owner class QN → module ref. */
typedef struct {
    const char *owner_qn;   /* class/module whose body held the call */
    const char *module_ref; /* raw dotted module spelling */
    const char *module_qn;  /* resolved QN, or NULL */
    RubyMixinKind kind;
} RubyMixinInfo;

/* One inferred instance-variable type: (class QN, @name) → type QN.
 * `conflicted` latches when two assignments disagree — lookup then returns
 * nothing (zero-edge guarantee beats a coin-flip type). */
typedef struct {
    const char *class_qn;
    const char *ivar_name; /* including the leading '@' */
    const char *type_qn;
    bool conflicted;
} RubyIvarInfo;

/* RubyLSPContext — per-file state for Ruby type-aware call resolution.
 * Mirrors PerlLSPContext / PHPLSPContext structure.
 *
 * Ruby specifics that shape this context:
 *   - Classes/modules nest lexically (`module A; class B`), and constants
 *     resolve through the lexical nesting outward (A::B sees A's constants
 *     before top-level ones). `nesting` tracks the current dotted path.
 *   - Inheritance is `class C < Base`; mixins arrive via include/prepend/
 *     extend and participate in method lookup (see RubyMixinKind).
 *   - `Foo.new` constructs a Foo and dispatches to Foo#initialize.
 *   - `def m` defines an instance method; `def self.m` a singleton (class)
 *     method. Singleton methods are registered under the receiver key
 *     "<class_qn>.self" — constants cannot be lowercase, so the suffix can
 *     never collide with a real nested-constant path. */
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    /* Mutable alias of `registry`, set only while PASS 1 populates method
     * tables (mirrors perl_lsp's mutable-reg build phase). NULL afterwards. */
    CBMTypeRegistry *build_reg;
    CBMScope *current_scope;

    /* Lexical nesting: dotted constant path of the enclosing class/module
     * chain ("" at top level, "A.B" inside `module A; class B`). */
    const char *nesting;

    /* Class/module table (pass 1). */
    RubyClassInfo *classes;
    int class_count;
    int class_cap;

    /* Mixin table (pass 1). */
    RubyMixinInfo *mixins;
    int mixin_count;
    int mixin_cap;

    /* Instance-variable type table (pass 1). */
    RubyIvarInfo *ivars;
    int ivar_count;
    int ivar_cap;

    /* Cross-file import map (require/require_relative resolved by the
     * pipeline). Unused per-file: Ruby constants are file-global. */
    const char **import_names;
    const char **import_qns;
    int import_count;

    /* Current enclosing context during the resolution walk. */
    const char *enclosing_class_qn; /* class QN, or NULL at top level */
    const char *enclosing_func_qn;  /* enclosing method QN, or NULL */
    bool in_singleton_method;       /* `def self.m` / class << self scope */
    const char *module_qn;

    /* Output: resolved calls accumulate here. */
    CBMResolvedCallArray *resolved_calls;

    /* Recursion guards. */
    int eval_depth;
    int walk_depth;

    /* Debug mode (CBM_LSP_DEBUG env). */
    bool debug;
} RubyLSPContext;

/* Initialize a RubyLSPContext for processing one file. */
void ruby_lsp_init(RubyLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *module_qn,
                   CBMResolvedCallArray *out);

/* Resolve a dotted constant path (e.g. "Foo", "A.B") against the class
 * table using the current lexical nesting, then the stdlib. Returns the
 * resolved QN or NULL. */
const char *ruby_resolve_constant(RubyLSPContext *ctx, const char *path);

/* Look up an instance method on a class, walking prepends → own → includes →
 * superclass chain. Returns the resolved CBMRegisteredFunc or NULL. */
const CBMRegisteredFunc *ruby_lookup_instance_method(RubyLSPContext *ctx, const char *class_qn,
                                                     const char *method_name);

/* Look up a singleton (class-side) method: own singleton methods →
 * `extend`ed modules' instance methods → superclass singleton chain. */
const CBMRegisteredFunc *ruby_lookup_singleton_method(RubyLSPContext *ctx, const char *class_qn,
                                                      const char *method_name);

/* Evaluate a Ruby expression's type. May return CBM_TYPE_UNKNOWN. */
const CBMType *ruby_eval_expr_type(RubyLSPContext *ctx, TSNode node);

/* Entry point: build registry from file defs + stdlib, then run resolution.
 * Called from cbm_extract_file() via the language dispatch in cbm.c. */
void cbm_run_ruby_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root);

/* Register Ruby core stdlib types/methods + a curated Rails surface
 * (ActiveRecord/ActiveSupport/ActionController) into a registry. */
void cbm_ruby_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

#endif /* CBM_LSP_RUBY_LSP_H */
