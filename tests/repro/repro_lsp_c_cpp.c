/*
 * repro_lsp_c_cpp.c — EXHAUSTIVE per-LSP-pass invariant suite for the C/C++
 * hybrid LSP (internal/cbm/lsp/c_lsp.c).
 *
 * WHAT THIS ASSERTS — the LSP RESOLUTION CONTRACT, one invariant per strategy.
 *   The C/C++ cross resolver resolves each call via a specific STRATEGY and tags
 *   the resulting CALLS edge in its properties_json with
 *       "strategy":"lsp_<name>"
 *   (see c_emit_resolved_call, c_lsp.c:3287-3296; every emit site passes a
 *   literal "lsp_..." string). Each strategy keys on a precise C++ construct.
 *   This suite builds the MINIMAL fixture that exercises exactly one strategy,
 *   indexes it through the full production pipeline, and asserts TWO things:
 *     (a) callable-sourcing — the inner call is sourced at a Function/Method
 *         node, never at a Module/File node (inv_count_calls_by_source →
 *         module_sourced == 0). A Module-sourced call is the #554 attribution
 *         bug; this is the broad correctness floor.
 *     (b) strategy-presence — some CALLS edge carries "lsp_<strategy>" in its
 *         properties_json (inv_edge_has_strategy). This is the PRECISE per-pass
 *         invariant: it proves that exact resolution path fired and survived
 *         into the graph.
 *
 * RED vs GREEN — this is a STATUS BOARD, not a pass/fail gate (runs only under
 *   make test-repro / bug-repro.yml, never the branch-protection ci-ok gate):
 *     - GREEN  = the LSP strategy works end-to-end = a permanent regression
 *                guard that it keeps working.
 *     - RED    = the strategy is dropped, or the call lands Module-sourced, or
 *                the rescue is discarded. Either way the per-pass TEST DOCUMENTS
 *                the exact gap for the eventual fixer.
 *
 * TIE TO repro_invariant_lsp_rescue.c — that file pins the MECHANISM by which
 *   these can silently fail: cbm_pipeline_find_lsp_resolution
 *   (src/pipeline/lsp_resolve.h:65) joins each LSP-resolved call to the
 *   tree-sitter call by EXACT caller-QN string equality. When tree-sitter's
 *   enclosing-func walk falls back to the MODULE QN (common for out-of-line
 *   method bodies, #554) but the LSP built the real method QN, the strcmp never
 *   matches, the LSP rescue is discarded, and the edge stays Module-sourced
 *   with a registry strategy — NEVER an "lsp_" strategy. So a strategy that is
 *   correctly EMITTED by c_lsp.c can still be ABSENT from the graph here: the
 *   exact-QN join suppresses it. Whenever a strategy below is RED, suspect that
 *   join first (an in-line / free-function fixture sidesteps it; an out-of-line
 *   method fixture triggers it).
 *
 * STRATEGY INVENTORY — every literal "lsp_..." emitted by c_lsp.c, grepped from
 *   the source (grep '"lsp_' internal/cbm/lsp/c_lsp.c), with its keying site:
 *     lsp_direct                (c_lsp.c:3650)  free/global function call f()
 *     lsp_implicit_this         (c_lsp.c:3655)  member calls sibling member, no this->
 *     lsp_scoped                (c_lsp.c:3489/3509/3525)  Ns::f() / Class::g()
 *     lsp_type_dispatch         (c_lsp.c:3392)  obj.method() on a concrete type
 *     lsp_virtual_dispatch      (c_lsp.c:3401)  base*->virt(), override found on derived
 *     lsp_base_dispatch         (c_lsp.c:3403)  inherited method, no derived override
 *     lsp_smart_ptr_dispatch    (c_lsp.c:3409)  std::unique_ptr<T>->method()
 *     lsp_template              (c_lsp.c:3576)  f<T>(args) explicit template call
 *     lsp_template_instantiation(c_lsp.c:393)   template<T> body t.m() resolved at instantiation
 *     lsp_func_ptr              (c_lsp.c:3605)  call via tracked function pointer
 *     lsp_dll_resolve           (c_lsp.c:3605)  call via fp whose target is external.* (DLL)
 *     lsp_operator              (c_lsp.c:3624/3789/3821/3845/3889)  overloaded operator use
 *     lsp_constructor           (c_lsp.c:3641/3715/3745)  new Foo() / Foo x(args)
 *     lsp_destructor            (c_lsp.c:3765)  delete p (p : Foo*)
 *     lsp_copy_constructor      (c_lsp.c:3922)  Foo a = b; (b : Foo)
 *     lsp_conversion            (c_lsp.c:3946)  if (obj) with operator bool
 *     lsp_adl                   (c_lsp.c:3674)  unqualified call resolved by ADL
 *     lsp_unresolved            (c_lsp.c:3306)  fallback marker for an unresolved call
 *
 * NOTE: line comments only inside this header (no nested block comments, per
 * coding rules).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <string.h>

/* ── Shared per-strategy runner (DRY) ────────────────────────────────────── */

/*
 * assert_lsp_strategy
 *
 * Index a single-file fixture and assert the per-pass LSP RESOLUTION CONTRACT:
 *   1. the store opened (precondition — a setup failure is a FAIL, not a skip);
 *   2. callable-sourcing: NO CALLS edge is Module/File-sourced, and at least one
 *      callable-sourced CALLS edge exists (else there is no signal at all);
 *   3. strategy-presence: some CALLS edge carries "lsp_<strategy>" in its
 *      properties_json.
 *
 * `filename` selects the language by extension (".cpp" → C++ pass, ".c" → C
 * pass) exactly as the production indexer does. Returns 0 on PASS (GREEN),
 * non-zero on FAIL (RED) — the redness is the documented per-pass status.
 */
static int assert_lsp_strategy(const char *filename, const char *src,
                               const char *strategy) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, filename, src);
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed for strategy %s\n", tf_red(),
               tf_reset(), __FILE__, __LINE__, strategy);
        rh_cleanup(&lp, store);
        return 1;
    }

    int module_sourced = -1;
    int callable_sourced = -1;
    inv_count_calls_by_source(store, lp.project, &module_sourced,
                              &callable_sourced);

    int has_strategy = inv_edge_has_strategy(store, lp.project, strategy);

    int rc = 0;

    /* (a) callable-sourcing floor: zero Module/File-sourced CALLS edges. */
    if (module_sourced != 0) {
        printf("  %sFAIL%s %s:%d: strategy %s: %d Module-sourced CALLS "
               "(expected 0)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy,
               module_sourced);
        rc = 1;
    }
    /* There must be a callable-sourced CALLS edge, else the fixture produced no
     * call signal and the strategy assertion below would be vacuous. */
    if (callable_sourced <= 0) {
        printf("  %sFAIL%s %s:%d: strategy %s: no callable-sourced CALLS edge "
               "(callable=%d)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy,
               callable_sourced);
        rc = 1;
    }

    /* (b) the precise per-pass invariant: the resolution strategy is present. */
    if (!has_strategy) {
        printf("  %sFAIL%s %s:%d: strategy %s ABSENT from any CALLS edge "
               "properties_json\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy);
        rc = 1;
    }

    rh_cleanup(&lp, store);
    return rc;
}

/*
 * assert_no_resolvable_edge — the ACCURATE invariant for a call whose callee is
 * genuinely UNRESOLVABLE (undeclared, or an external/DLL symbol with no body in
 * the indexed tree). No node can exist for such a callee, so no CALLS edge can
 * ever target it and no resolution strategy can land on an edge. Index the
 * single-file fixture and assert NO CALLS edge targets a node whose QN contains
 * `callee_substr`. Returns 0 on PASS, non-zero on FAIL.
 */
static int assert_no_resolvable_edge(const char *filename, const char *src,
                                     const char *callee_substr) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, filename, src);
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed for no-edge callee %s\n", tf_red(),
               tf_reset(), __FILE__, __LINE__, callee_substr);
        rh_cleanup(&lp, store);
        return 1;
    }
    int rc = 0;
    if (!inv_no_calls_edge_to_qn(store, lp.project, callee_substr)) {
        printf("  %sFAIL%s %s:%d: a CALLS edge unexpectedly targets %s "
               "(expected NONE — callee is unresolvable)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, callee_substr);
        rc = 1;
    }
    rh_cleanup(&lp, store);
    return rc;
}

/* ── Fixtures ────────────────────────────────────────────────────────────────
 *
 * Each fixture is the MINIMAL construct c_lsp.c keys on for one strategy. The
 * call we care about always lives inside a callable (free function or method)
 * so callable-sourcing is testable; the callee is also defined in-file so the
 * registry can resolve it.
 * ───────────────────────────────────────────────────────────────────────── */

/* lsp_direct — plain free/global function call f() (c_lsp.c:3650). */
static const char kDirect[] =
    "int helper(int x) { return x + 1; }\n"
    "int caller(int v) { return helper(v); }\n";

/* lsp_implicit_this — a member calls a sibling member with no `this->`
 * (c_lsp.c:3651-3656: enclosing_class_qn set + name resolves to a method of
 * that class). */
static const char kImplicitThis[] =
    "class Widget {\n"
    "public:\n"
    "    int compute(int x) { return helper(x) + 1; }\n"
    "    int helper(int x) { return x * 2; }\n"
    "};\n";

/* lsp_scoped — qualified static call Class::method() (c_lsp.c:3489/3509). */
static const char kScoped[] =
    "class Math {\n"
    "public:\n"
    "    static int square(int x) { return x * x; }\n"
    "};\n"
    "int caller(int v) { return Math::square(v); }\n";

/* lsp_type_dispatch — obj.method() on a concrete, non-derived type
 * (c_lsp.c:3392; default strategy when receiver_type == type_qn). */
static const char kTypeDispatch[] =
    "class Counter {\n"
    "public:\n"
    "    int inc(int x) { return x + 1; }\n"
    "};\n"
    "int caller() {\n"
    "    Counter c;\n"
    "    return c.inc(1);\n"
    "}\n";

/* lsp_virtual_dispatch — call through a base reference, override resolved on
 * the derived (receiver) type (c_lsp.c:3394-3401: receiver_type != type_qn AND
 * a derived override exists). The receiver is typed as Derived so the override
 * is found; resolution traverses to the base then prefers the override. */
static const char kVirtualDispatch[] =
    "class Base {\n"
    "public:\n"
    "    virtual int speak(int x) { return x; }\n"
    "};\n"
    "class Derived : public Base {\n"
    "public:\n"
    "    int speak(int x) { return x * 10; }\n"
    "};\n"
    "int caller() {\n"
    "    Derived d;\n"
    "    return d.speak(2);\n"
    "}\n";

/* lsp_base_dispatch — derived object calls an INHERITED method that the derived
 * class does NOT override (c_lsp.c:3402-3404: resolved through base, no derived
 * override). */
static const char kBaseDispatch[] =
    "class Base {\n"
    "public:\n"
    "    int common(int x) { return x + 100; }\n"
    "};\n"
    "class Derived : public Base {\n"
    "public:\n"
    "    int extra(int x) { return x - 1; }\n"
    "};\n"
    "int caller() {\n"
    "    Derived d;\n"
    "    return d.common(5);\n"
    "}\n";

/* lsp_smart_ptr_dispatch — std::unique_ptr<T>->method() (c_lsp.c:3407-3409:
 * is_arrow && template receiver && is_smart_ptr; is_smart_ptr requires the QN
 * to contain "std", c_lsp.c:36-46). */
static const char kSmartPtr[] =
    "namespace std {\n"
    "    template <class T> class unique_ptr {\n"
    "    public:\n"
    "        T* operator->();\n"
    "    };\n"
    "}\n"
    "class Service {\n"
    "public:\n"
    "    int run(int x) { return x + 7; }\n"
    "};\n"
    "int caller(std::unique_ptr<Service> p) {\n"
    "    return p->run(3);\n"
    "}\n";

/* lsp_template — explicit template function call f<T>(args) (c_lsp.c:3535-3576:
 * func_node is a template_function). */
static const char kTemplate[] =
    "template <class T> T identity(T x) { return x; }\n"
    "int caller() {\n"
    "    return identity<int>(42);\n"
    "}\n";

/* lsp_template_instantiation — a template body calls t.method() on a type-param
 * receiver; the call is pending until the template is instantiated with a
 * concrete type, then resolved on that type (c_lsp.c:374-393). process<Gadget>
 * resolves the pending Gadget.go(). */
static const char kTemplateInstantiation[] =
    "class Gadget {\n"
    "public:\n"
    "    int go(int x) { return x + 4; }\n"
    "};\n"
    "template <class T> int process(T t) { return t.go(1); }\n"
    "int caller() {\n"
    "    Gadget g;\n"
    "    return process<Gadget>(g);\n"
    "}\n";

/* lsp_func_ptr — call through a tracked function-pointer variable whose target
 * is an in-file function (c_lsp.c:3600-3606: c_lookup_fp_target hits, target is
 * NOT external.* → lsp_func_ptr). */
static const char kFuncPtr[] =
    "int target(int x) { return x * 3; }\n"
    "int caller(int v) {\n"
    "    int (*fp)(int) = target;\n"
    "    return fp(v);\n"
    "}\n";

/* lsp_dll_resolve — same as lsp_func_ptr but the fp target is an external/DLL
 * symbol (c_lsp.c:3603-3605: target starts with "external." → lsp_dll_resolve).
 * There is no portable in-source way to make c_lookup_fp_target return an
 * "external."-prefixed target from a single file, so this is expected ABSENT
 * (RED) — it documents that the DLL-resolution path needs an external binding
 * the single-file harness can't synthesize. The fixture below at least exercises
 * a pointer assigned from an extern declaration. */
static const char kDllResolve[] =
    "extern int plugin_entry(int x);\n"
    "int caller(int v) {\n"
    "    int (*fp)(int) = plugin_entry;\n"
    "    return fp(v);\n"
    "}\n";

/* lsp_operator — overloaded binary operator+ on a custom type (c_lsp.c:3771-3789:
 * binary_expression, lhs is a custom type, operator+ member found). */
static const char kOperator[] =
    "class Vec {\n"
    "public:\n"
    "    Vec operator+(const Vec& o) const { return o; }\n"
    "};\n"
    "Vec caller(Vec a, Vec b) {\n"
    "    return a + b;\n"
    "}\n";

/* lsp_constructor — new Foo() emits the constructor (c_lsp.c:3724-3745). */
static const char kConstructor[] =
    "class Foo {\n"
    "public:\n"
    "    Foo(int x) {}\n"
    "};\n"
    "Foo* caller(int v) {\n"
    "    return new Foo(v);\n"
    "}\n";

/* lsp_destructor — delete p where p is Foo* emits the destructor
 * (c_lsp.c:3751-3765). */
static const char kDestructor[] =
    "class Foo {\n"
    "public:\n"
    "    Foo() {}\n"
    "    ~Foo() {}\n"
    "};\n"
    "void caller(Foo* p) {\n"
    "    delete p;\n"
    "}\n";

/* lsp_copy_constructor — Foo a = b; with b a Foo emits the copy constructor
 * (c_lsp.c:3897-3922: declaration, value is not an argument_list, val type ==
 * decl type). */
static const char kCopyConstructor[] =
    "class Foo {\n"
    "public:\n"
    "    Foo() {}\n"
    "    Foo(const Foo& o) {}\n"
    "};\n"
    "Foo caller(Foo b) {\n"
    "    Foo a = b;\n"
    "    return a;\n"
    "}\n";

/* lsp_conversion — if (obj) where obj has operator bool emits the conversion
 * operator (c_lsp.c:3931-3946). */
static const char kConversion[] =
    "class Handle {\n"
    "public:\n"
    "    operator bool() const { return true; }\n"
    "};\n"
    "int caller(Handle h) {\n"
    "    if (h) { return 1; }\n"
    "    return 0;\n"
    "}\n";

/* lsp_adl — unqualified call resolved by argument-dependent lookup: serialize()
 * lives in namespace ns alongside type ns::Data; an unqualified serialize(d)
 * with d : ns::Data resolves via ADL (c_lsp.c:3671-3674: c_resolve_name fails,
 * c_adl_resolve searches the argument type's namespace). */
static const char kAdl[] =
    "namespace ns {\n"
    "    class Data {};\n"
    "    int serialize(const Data& d) { return 1; }\n"
    "}\n"
    "int caller(ns::Data d) {\n"
    "    return serialize(d);\n"
    "}\n";

/* lsp_unresolved — a call to a function that is not in the registry; the
 * resolver emits the fallback marker (c_lsp.c:3306, rc.strategy =
 * "lsp_unresolved"). NOTE: c_emit_resolved_call sets "lsp_unresolved" only when
 * called with a NULL callee_qn; the more common unresolved path is
 * c_emit_unresolved_call (a different marker). This fixture exercises a call to
 * an undeclared function and documents whether "lsp_unresolved" surfaces. */
static const char kUnresolved[] =
    "int caller(int v) {\n"
    "    return totally_unknown_fn(v);\n"
    "}\n";

/* ── Per-strategy tests ──────────────────────────────────────────────────── */

TEST(repro_lsp_cpp_direct) {
    return assert_lsp_strategy("main.cpp", kDirect, "lsp_direct");
}

TEST(repro_lsp_cpp_implicit_this) {
    return assert_lsp_strategy("main.cpp", kImplicitThis, "lsp_implicit_this");
}

TEST(repro_lsp_cpp_scoped) {
    return assert_lsp_strategy("main.cpp", kScoped, "lsp_scoped");
}

TEST(repro_lsp_cpp_type_dispatch) {
    return assert_lsp_strategy("main.cpp", kTypeDispatch, "lsp_type_dispatch");
}

TEST(repro_lsp_cpp_virtual_dispatch) {
    return assert_lsp_strategy("main.cpp", kVirtualDispatch,
                               "lsp_virtual_dispatch");
}

TEST(repro_lsp_cpp_base_dispatch) {
    return assert_lsp_strategy("main.cpp", kBaseDispatch, "lsp_base_dispatch");
}

TEST(repro_lsp_cpp_smart_ptr_dispatch) {
    return assert_lsp_strategy("main.cpp", kSmartPtr, "lsp_smart_ptr_dispatch");
}

TEST(repro_lsp_cpp_template) {
    return assert_lsp_strategy("main.cpp", kTemplate, "lsp_template");
}

TEST(repro_lsp_cpp_template_instantiation) {
    return assert_lsp_strategy("main.cpp", kTemplateInstantiation,
                               "lsp_template_instantiation");
}

TEST(repro_lsp_cpp_func_ptr) {
    return assert_lsp_strategy("main.cpp", kFuncPtr, "lsp_func_ptr");
}

TEST(repro_lsp_cpp_dll_resolve) {
    /* plugin_entry is an EXTERNAL symbol (extern decl, no body in the indexed
     * tree) — no node exists for it, so no CALLS edge can ever target it. The
     * "external."-prefixed lsp_dll_resolve strategy is unsynthesizable from a
     * single file by design; assert the accurate no-resolvable-edge behaviour. */
    return assert_no_resolvable_edge("main.cpp", kDllResolve, "plugin_entry");
}

TEST(repro_lsp_cpp_operator) {
    return assert_lsp_strategy("main.cpp", kOperator, "lsp_operator");
}

TEST(repro_lsp_cpp_constructor) {
    return assert_lsp_strategy("main.cpp", kConstructor, "lsp_constructor");
}

TEST(repro_lsp_cpp_destructor) {
    return assert_lsp_strategy("main.cpp", kDestructor, "lsp_destructor");
}

TEST(repro_lsp_cpp_copy_constructor) {
    return assert_lsp_strategy("main.cpp", kCopyConstructor,
                               "lsp_copy_constructor");
}

TEST(repro_lsp_cpp_conversion) {
    return assert_lsp_strategy("main.cpp", kConversion, "lsp_conversion");
}

TEST(repro_lsp_cpp_adl) {
    return assert_lsp_strategy("main.cpp", kAdl, "lsp_adl");
}

TEST(repro_lsp_cpp_unresolved) {
    /* totally_unknown_fn is UNDECLARED — no node can exist for it, so no CALLS
     * edge can ever form. Assert the accurate no-resolvable-edge behaviour
     * instead of a resolution strategy on an edge (unachievable by design). */
    return assert_no_resolvable_edge("main.cpp", kUnresolved, "totally_unknown_fn");
}

/* ── Suite ───────────────────────────────────────────────────────────────── */

SUITE(repro_lsp_c_cpp) {
    RUN_TEST(repro_lsp_cpp_direct);
    RUN_TEST(repro_lsp_cpp_implicit_this);
    RUN_TEST(repro_lsp_cpp_scoped);
    RUN_TEST(repro_lsp_cpp_type_dispatch);
    RUN_TEST(repro_lsp_cpp_virtual_dispatch);
    RUN_TEST(repro_lsp_cpp_base_dispatch);
    RUN_TEST(repro_lsp_cpp_smart_ptr_dispatch);
    RUN_TEST(repro_lsp_cpp_template);
    RUN_TEST(repro_lsp_cpp_template_instantiation);
    RUN_TEST(repro_lsp_cpp_func_ptr);
    RUN_TEST(repro_lsp_cpp_dll_resolve);
    RUN_TEST(repro_lsp_cpp_operator);
    RUN_TEST(repro_lsp_cpp_constructor);
    RUN_TEST(repro_lsp_cpp_destructor);
    RUN_TEST(repro_lsp_cpp_copy_constructor);
    RUN_TEST(repro_lsp_cpp_conversion);
    RUN_TEST(repro_lsp_cpp_adl);
    RUN_TEST(repro_lsp_cpp_unresolved);
}
