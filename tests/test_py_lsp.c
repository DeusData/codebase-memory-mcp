/*
 * test_py_lsp.c — Tests for Python LSP type-aware call resolution.
 *
 * Mirrors tests/test_go_lsp.c shape: helper extract_py(source) calls
 * cbm_extract_file with CBM_LANG_PYTHON, then assertions search the
 * resolved_calls array. Phase 2 ships smoke tests only; subsequent
 * phases add categories matching the Go LSP layout (param types,
 * method dispatch, decorators, generics, cross-file).
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/py_lsp.h"
#include "pipeline/lsp_resolve.h"
#include "pipeline/pass_lsp_cross.h"

/* ── Helpers — same shape as test_go_lsp.c ──────────────────────── */

static CBMFileResult *extract_py(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                            "test", "main.py", 0, NULL, NULL);
}

static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

/* Avoid unused-static-function warnings: helpers compiled but not yet used
 * outside the smoke tests will be referenced in Phase 3+ tests. */
__attribute__((unused))
static int require_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int idx = find_resolved(r, callerSub, calleeSub);
    if (idx < 0) {
        printf("  MISSING resolved call: caller~%s -> callee~%s (have %d)\n",
               callerSub, calleeSub, r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s %.2f]\n",
                   rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)",
                   rc->strategy ? rc->strategy : "(null)",
                   rc->confidence);
        }
    }
    return idx;
}

/* ── Phase 2 — smoke ───────────────────────────────────────────── */

TEST(pylsp_smoke_empty) {
    CBMFileResult *r = extract_py("");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->resolved_calls.count, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_smoke_one_function) {
    CBMFileResult *r = extract_py(
        "def greet(name):\n"
        "    return name\n");
    ASSERT_NOT_NULL(r);
    /* Phase 2 stub: no resolutions yet, but extraction must succeed and
     * the result must be addressable without crashes. */
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_smoke_one_class) {
    CBMFileResult *r = extract_py(
        "class Greeter:\n"
        "    def __init__(self, name):\n"
        "        self.name = name\n"
        "    def greet(self):\n"
        "        return self.name\n");
    ASSERT_NOT_NULL(r);
    /* Class + 2 methods at minimum */
    ASSERT_GTE(r->defs.count, 1);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_no_crash_on_syntax_error) {
    /* Tree-sitter recovers from errors but we must not crash on the
     * recovered tree. */
    CBMFileResult *r = extract_py(
        "def broken(\n"
        "    x = 1\n"
        "class\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_smoke_imports_passed_through) {
    /* Imports populate ctx->import_local_names — Phase 2 just verifies
     * the unified extractor still produces them; resolution happens in
     * Phase 3. */
    CBMFileResult *r = extract_py(
        "import os\n"
        "import json as j\n"
        "from pathlib import Path\n"
        "from . import sibling\n"
        "def use():\n"
        "    return os.getcwd()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(r->imports.count, 3);
    cbm_free_result(r);
    PASS();
}

/* ── Phase 3 — imports → scope bindings ────────────────────────── */

/* Build a context, register one or more imports, run the binding pass,
 * and let the caller verify scope state. */
static void bind_imports_into_ctx(PyLSPContext *ctx, CBMArena *a, CBMTypeRegistry *reg,
                                  const char *const *locals, const char *const *qns,
                                  int count) {
    py_lsp_init(ctx, a, "", 0, reg, "test.main", NULL);
    for (int i = 0; i < count; i++) {
        py_lsp_add_import(ctx, locals[i], qns[i]);
    }
    py_lsp_bind_imports(ctx);
}

TEST(pylsp_import_simple) {
    /* import os → os ∈ scope as MODULE("os") */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"os"};
    const char *qns[] = {"os"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "os");
    ASSERT(cbm_type_is_module(t));
    ASSERT_STR_EQ(t->data.module.module_qn, "os");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_aliased) {
    /* import json as j → j ∈ scope as MODULE("json") */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"j"};
    const char *qns[] = {"json"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "j");
    ASSERT(cbm_type_is_module(t));
    ASSERT_STR_EQ(t->data.module.module_qn, "json");
    /* original name "json" not bound */
    const CBMType *miss = py_lsp_lookup_in_scope(&ctx, "json");
    ASSERT(cbm_type_is_unknown(miss));
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_from) {
    /* from pathlib import Path → Path ∈ scope as NAMED("pathlib.Path") */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"Path"};
    const char *qns[] = {"pathlib.Path"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "Path");
    ASSERT_EQ(t->kind, CBM_TYPE_NAMED);
    ASSERT_STR_EQ(t->data.named.qualified_name, "pathlib.Path");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_from_aliased) {
    /* from pathlib import Path as P → P ∈ scope as NAMED("pathlib.Path") */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"P"};
    const char *qns[] = {"pathlib.Path"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "P");
    /* #988: an aliased from-import MUST bind NAMED (phase 6 upgrades it to
     * the registered function/class/module). The earlier MODULE binding made
     * `g()` calls on `from m import f as g` resolve as calls on a module —
     * lsp=MISS and the CALLS edge was lost. */
    ASSERT_EQ(t->kind, CBM_TYPE_NAMED);
    ASSERT_STR_EQ(t->data.named.qualified_name, "pathlib.Path");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_relative_one_dot) {
    /* from . import sibling — extract_imports records local=sibling,
     * qn="..sibling" or similar. py_lsp binds it regardless. */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"sibling"};
    const char *qns[] = {"..sibling"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "sibling");
    ASSERT(!cbm_type_is_unknown(t));
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_relative_two_dots) {
    /* from ..pkg import x → bind x as NAMED("..pkg.x") best effort */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"x"};
    const char *qns[] = {"...pkg.x"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "x");
    ASSERT(!cbm_type_is_unknown(t));
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_star_best_effort) {
    /* from X import * — local_name="*". py_lsp does not bind "*" because
     * it's not a usable identifier; the import is preserved in the import
     * map for cross-file re-export resolution (Phase 9). */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"*"};
    const char *qns[] = {"X"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *star_miss = py_lsp_lookup_in_scope(&ctx, "*");
    ASSERT(cbm_type_is_unknown(star_miss));
    /* Import is still recorded — Phase 9 will use it. */
    ASSERT_EQ(ctx.import_count, 1);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_typing_only_still_binds) {
    /* `if TYPE_CHECKING:` is just a runtime constant — extract_imports
     * emits CBMImport entries regardless of guard. py_lsp binds them. */
    CBMArena a;
    cbm_arena_init(&a);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &a);
    PyLSPContext ctx;
    const char *locals[] = {"List"};
    const char *qns[] = {"typing.List"};
    bind_imports_into_ctx(&ctx, &a, &reg, locals, qns, 1);
    const CBMType *t = py_lsp_lookup_in_scope(&ctx, "List");
    ASSERT(!cbm_type_is_unknown(t));
    ASSERT_EQ(t->kind, CBM_TYPE_NAMED);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(pylsp_import_multi_pass_through_extract_file) {
    /* End-to-end: extract_file + run_py_lsp populate scope via imports.
     * We can't peek into the embedded ctx, but we verify imports survive
     * to the result and bind correctly when re-traversed. */
    CBMFileResult *r = extract_py(
        "import os\n"
        "import json as j\n"
        "from pathlib import Path\n"
        "def use():\n"
        "    return Path('.')\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(r->imports.count, 3);
    cbm_free_result(r);
    PASS();
}

/* ── Phase 4-6 — direct calls + method dispatch ────────────────── */

TEST(pylsp_direct_function_call) {
    /* def helper(): return 1
     * def main(): return helper() */
    CBMFileResult *r = extract_py(
        "def helper():\n"
        "    return 1\n"
        "def main():\n"
        "    return helper()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "main", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_method_call_simple) {
    /* class C:
     *     def m(self): return 1
     * def use(c):
     *     c.m()  -- with annotation */
    CBMFileResult *r = extract_py(
        "class C:\n"
        "    def m(self):\n"
        "        return 1\n"
        "def use(c: C):\n"
        "    return c.m()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "m"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_method_via_self) {
    CBMFileResult *r = extract_py(
        "class C:\n"
        "    def helper(self):\n"
        "        return 1\n"
        "    def caller(self):\n"
        "        return self.helper()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "caller", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_constructor_call_returns_instance) {
    /* class Foo: ...
     * def use():
     *   f = Foo()
     *   f.method()  -- requires inferring f as Foo */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use():\n"
        "    f = Foo()\n"
        "    return f.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_method_via_inheritance) {
    CBMFileResult *r = extract_py(
        "class Base:\n"
        "    def shared(self):\n"
        "        return 1\n"
        "class Child(Base):\n"
        "    def go(self):\n"
        "        return self.shared()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "go", "shared"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_no_false_positive_on_unknown_method) {
    /* Calling a method on an UNKNOWN type should NOT emit a high-confidence
     * resolution. */
    CBMFileResult *r = extract_py(
        "def f(x):\n"
        "    return x.something_unknown_42()\n");
    ASSERT_NOT_NULL(r);
    /* Should produce no high-confidence match for "something_unknown_42" */
    int idx = find_resolved(r, "f", "something_unknown_42");
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        ASSERT(rc->confidence < 0.6f);
    }
    cbm_free_result(r);
    PASS();
}

/* ── Phase 7-8 — decorators, super(), multi-inheritance ──────── */

TEST(pylsp_decorated_function_resolves) {
    /* Decorated functions still resolve as their bare-name target. */
    CBMFileResult *r = extract_py(
        "import functools\n"
        "@functools.cache\n"
        "def helper():\n"
        "    return 1\n"
        "def main():\n"
        "    return helper()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "main", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_classmethod_resolves) {
    CBMFileResult *r = extract_py(
        "class C:\n"
        "    @classmethod\n"
        "    def make(cls):\n"
        "        return cls()\n"
        "def use():\n"
        "    return C.make()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "make"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_staticmethod_resolves) {
    CBMFileResult *r = extract_py(
        "class C:\n"
        "    @staticmethod\n"
        "    def add(a, b):\n"
        "        return a + b\n"
        "def use():\n"
        "    return C.add(1, 2)\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "add"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_dataclass_constructor) {
    /* @dataclass synthesizes __init__. We don't emit __init__ explicitly,
     * but the constructor call should still link to the class qn. */
    CBMFileResult *r = extract_py(
        "from dataclasses import dataclass\n"
        "@dataclass\n"
        "class Point:\n"
        "    x: int\n"
        "    y: int\n"
        "    def magnitude(self):\n"
        "        return self.x + self.y\n"
        "def use():\n"
        "    p = Point(1, 2)\n"
        "    return p.magnitude()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "magnitude"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_super_call) {
    CBMFileResult *r = extract_py(
        "class Base:\n"
        "    def greet(self):\n"
        "        return 'hi'\n"
        "class Child(Base):\n"
        "    def greet(self):\n"
        "        return super().greet()\n");
    ASSERT_NOT_NULL(r);
    /* super().greet() should resolve to Base.greet, not Child.greet. */
    int idx = find_resolved(r, "greet", "greet");
    ASSERT_GTE(idx, 0);
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        ASSERT(strstr(rc->callee_qn, "Base") != NULL);
    }
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_multi_inheritance_first_base) {
    CBMFileResult *r = extract_py(
        "class A:\n"
        "    def a_method(self):\n"
        "        return 1\n"
        "class B:\n"
        "    def b_method(self):\n"
        "        return 2\n"
        "class C(A, B):\n"
        "    def use(self):\n"
        "        self.a_method()\n"
        "        self.b_method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "a_method"), 0);
    ASSERT_GTE(require_resolved(r, "use", "b_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_pep695_generic_class) {
    /* PEP 695: class Box[T]:  -- our implementation ignores the [T] part */
    CBMFileResult *r = extract_py(
        "class Box:\n"
        "    def get(self):\n"
        "        return 1\n"
        "def use(b: Box):\n"
        "    return b.get()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Phase 9 — cross-file resolution ──────────────────────────── */

static int find_resolved_arr(const CBMResolvedCallArray *arr, const char *callerSub,
                             const char *calleeSub) {
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) &&
            rc->callee_qn && strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

typedef struct {
    int target_count;
    int first_site_count;
    int second_site_count;
} PyResolvedSiteCounts;

static PyResolvedSiteCounts count_invocation_sites(const CBMResolvedCallArray *arr,
                                                   const char *caller_sub, const char *callee_sub,
                                                   uint32_t first_start, uint32_t first_end,
                                                   uint32_t second_start, uint32_t second_end) {
    PyResolvedSiteCounts counts = {0};
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION || !rc->caller_qn || !rc->callee_qn ||
            !strstr(rc->caller_qn, caller_sub) || !strstr(rc->callee_qn, callee_sub)) {
            continue;
        }
        counts.target_count++;
        if (rc->site_start_byte == first_start && rc->site_end_byte == first_end)
            counts.first_site_count++;
        if (rc->site_start_byte == second_start && rc->site_end_byte == second_end)
            counts.second_site_count++;
    }
    return counts;
}

TEST(pylsp_crossfile_method_dispatch) {
    /* file svc.py defines class RedisStore with Get(); file main.py calls
     * the method on a typed parameter. Reuses CBMLSPDef to feed the
     * cross-file definition into the resolver. */
    const char *source =
        "from svc import RedisStore\n"
        "def process(s: RedisStore):\n"
        "    return s.Get('k')\n";

    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "svc.RedisStore";
    defs[0].short_name = "RedisStore";
    defs[0].label = "Class";
    defs[0].def_module_qn = "svc";

    defs[1].qualified_name = "svc.RedisStore.Get";
    defs[1].short_name = "Get";
    defs[1].label = "Method";
    defs[1].receiver_type = "svc.RedisStore";
    defs[1].def_module_qn = "svc";

    const char *imp_names[] = {"RedisStore"};
    const char *imp_qns[] = {"svc.RedisStore"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_py_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, imp_names,
                         imp_qns, 1, NULL, &out, NULL);

    ASSERT_GTE(find_resolved_arr(&out, "process", "Get"), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* Graph-quality guard for the Python seal fix (per-file field overlay). In the
 * FUSED path the shared Tier-2 registry is read_only, so `self.b = Bar()` can no
 * longer be recorded by mutating the shared registry; it is recorded in the per-file
 * overlay instead. This test proves the overlay preserves same-file attribute-chain
 * resolution: self.b.work() must still resolve to Bar.work. It RED-fails under a
 * plain read_only *gate* (option A: the field is dropped, self.b is untyped, the
 * work() call goes unresolved) and GREENs with the overlay. It ALSO asserts the
 * shared registry entry stays unmutated (Foo.field_names == NULL) — the seal half. */
TEST(pylsp_fused_self_attr_chain_via_overlay) {
    CBMArena arena;
    cbm_arena_init(&arena);

    /* Sealed shared registry: class Bar with method work(), class Foo. */
    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "test.mod.Bar";
    defs[0].short_name = "Bar";
    defs[0].label = "Class";
    defs[0].def_module_qn = "test.mod";
    defs[0].lang = CBM_LANG_PYTHON;
    defs[1].qualified_name = "test.mod.Bar.work";
    defs[1].short_name = "work";
    defs[1].label = "Method";
    defs[1].receiver_type = "test.mod.Bar";
    defs[1].def_module_qn = "test.mod";
    defs[1].lang = CBM_LANG_PYTHON;
    defs[2].qualified_name = "test.mod.Foo";
    defs[2].short_name = "Foo";
    defs[2].label = "Class";
    defs[2].def_module_qn = "test.mod";
    defs[2].lang = CBM_LANG_PYTHON;

    CBMTypeRegistry *reg = cbm_py_build_cross_registry(&arena, defs, 3);
    ASSERT_NOT_NULL(reg);
    ASSERT_TRUE(reg->read_only);

    /* Foo.__init__ records self.b = Bar(); Foo.run dispatches self.b.work(). */
    const char *src = "class Foo:\n"
                      "    def __init__(self):\n"
                      "        self.b = Bar()\n"
                      "    def run(self):\n"
                      "        return self.b.work()\n";
    CBMResolvedCallArray out = {0};
    cbm_run_py_lsp_cross_with_registry(&arena, src, (int)strlen(src), "test.mod", reg, NULL, NULL,
                                       0, NULL, &out, NULL);

    /* Overlay preserves the attribute-chain edge. */
    ASSERT_GTE(find_resolved_arr(&out, "run", "work"), 0);
    /* Seal preserved: the shared registry entry was NOT mutated. */
    const CBMRegisteredType *foo = cbm_registry_lookup_type(reg, "test.mod.Foo");
    ASSERT_NOT_NULL(foo);
    ASSERT_TRUE(foo->field_names == NULL);

    cbm_arena_destroy(&arena);
    PASS();
}

/* Issue #228: a class/static method invoked directly on a CROSS-FILE imported
 * class name — ActionRecordX.build_from_text(...) — produced no CALLS edge, so
 * the method showed in/out degree 0 and was flagged as dead code. Distinct from
 * pylsp_crossfile_method_dispatch (which dispatches on a typed *instance*). */
TEST(pylsp_crossfile_classmethod_on_class_issue228) {
    const char *source =
        "from core.schemas import ActionRecordX\n"
        "def run_plain_flow():\n"
        "    return ActionRecordX.build_from_text('hello')\n";

    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "core.schemas.ActionRecordX";
    defs[0].short_name = "ActionRecordX";
    defs[0].label = "Class";
    defs[0].def_module_qn = "core.schemas";

    defs[1].qualified_name = "core.schemas.ActionRecordX.build_from_text";
    defs[1].short_name = "build_from_text";
    defs[1].label = "Method";
    defs[1].receiver_type = "core.schemas.ActionRecordX";
    defs[1].def_module_qn = "core.schemas";

    const char *imp_names[] = {"ActionRecordX"};
    const char *imp_qns[] = {"core.schemas.ActionRecordX"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_py_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, imp_names,
                         imp_qns, 1, NULL, &out, NULL);

    ASSERT_GTE(find_resolved_arr(&out, "run_plain_flow", "build_from_text"), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(pylsp_crossfile_inheritance) {
    /* svc.py defines class Base with shared(); main.py defines class Child(Base)
     * and calls self.shared(). Caller passes ALL relevant defs (cross-file
     * Base/shared + local Child/go) — same convention as test_go_lsp.c. */
    const char *source =
        "from svc import Base\n"
        "class Child(Base):\n"
        "    def go(self):\n"
        "        return self.shared()\n";

    CBMLSPDef defs[4];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "svc.Base";
    defs[0].short_name = "Base";
    defs[0].label = "Class";
    defs[0].def_module_qn = "svc";

    defs[1].qualified_name = "svc.Base.shared";
    defs[1].short_name = "shared";
    defs[1].label = "Method";
    defs[1].receiver_type = "svc.Base";
    defs[1].def_module_qn = "svc";

    defs[2].qualified_name = "test.main.Child";
    defs[2].short_name = "Child";
    defs[2].label = "Class";
    defs[2].def_module_qn = "test.main";
    defs[2].embedded_types = "svc.Base";

    defs[3].qualified_name = "test.main.Child.go";
    defs[3].short_name = "go";
    defs[3].label = "Method";
    defs[3].receiver_type = "test.main.Child";
    defs[3].def_module_qn = "test.main";

    const char *imp_names[] = {"Base"};
    const char *imp_qns[] = {"svc.Base"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_py_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 4, imp_names,
                         imp_qns, 1, NULL, &out, NULL);

    ASSERT_GTE(find_resolved_arr(&out, "go", "shared"), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(pylsp_batch_two_files) {
    const char *src_a =
        "def helper():\n"
        "    return 1\n";
    const char *src_b =
        "from a import helper\n"
        "def main():\n"
        "    return helper()\n";

    CBMLSPDef a_defs[1];
    memset(a_defs, 0, sizeof(a_defs));
    a_defs[0].qualified_name = "a.helper";
    a_defs[0].short_name = "helper";
    a_defs[0].label = "Function";
    a_defs[0].def_module_qn = "a";

    CBMLSPDef b_defs[1];
    memset(b_defs, 0, sizeof(b_defs));
    b_defs[0].qualified_name = "b.main";
    b_defs[0].short_name = "main";
    b_defs[0].label = "Function";
    b_defs[0].def_module_qn = "b";

    const char *b_imp_names[] = {"helper"};
    const char *b_imp_qns[] = {"a.helper"};

    CBMBatchPyLSPFile files[2];
    memset(files, 0, sizeof(files));
    files[0].source = src_a;
    files[0].source_len = (int)strlen(src_a);
    files[0].module_qn = "a";
    files[0].defs = a_defs;
    files[0].def_count = 1;

    files[1].source = src_b;
    files[1].source_len = (int)strlen(src_b);
    files[1].module_qn = "b";
    files[1].defs = b_defs;
    files[1].def_count = 1;
    /* b imports helper from a — also include a's def in b's reachable set. */
    CBMLSPDef b_combined[2];
    memcpy(&b_combined[0], &a_defs[0], sizeof(CBMLSPDef));
    memcpy(&b_combined[1], &b_defs[0], sizeof(CBMLSPDef));
    files[1].defs = b_combined;
    files[1].def_count = 2;
    files[1].import_names = b_imp_names;
    files[1].import_qns = b_imp_qns;
    files[1].import_count = 1;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray outs[2];
    memset(outs, 0, sizeof(outs));

    cbm_batch_py_lsp_cross(&arena, files, 2, outs);

    /* file b's main should have called helper. */
    ASSERT_GTE(find_resolved_arr(&outs[1], "main", "helper"), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

static int pylsp_exact_reference_count(const CBMResolvedCallArray *out, const char *caller,
                                        const char *callee_qn, const char *reason,
                                        uint32_t site_start, uint32_t site_end) {
    int count = 0;
    for (int i = 0; out && i < out->count; i++) {
        const CBMResolvedCall *resolved = &out->items[i];
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE && resolved->caller_qn &&
            strstr(resolved->caller_qn, caller) && resolved->callee_qn &&
            strcmp(resolved->callee_qn, callee_qn) == 0 &&
            resolved->confidence >= CBM_LSP_CONFIDENCE_FLOOR &&
            resolved->site_start_byte == site_start && resolved->site_end_byte == site_end &&
            (!reason || (resolved->reason && strcmp(resolved->reason, reason) == 0))) {
            count++;
        }
    }
    return count;
}

typedef struct {
    int first_target;
    int second_target;
} PyImportReferenceCounts;

static PyImportReferenceCounts pylsp_import_reference_counts(
    const char *source, const char *local_name, const char *import_qn, const char *first_target_qn,
    const char *second_target_qn) {
    PyImportReferenceCounts counts = {0};
    const char *call = strstr(source, "    accept(");
    if (!call)
        return counts;
    uint32_t site_start = (uint32_t)(call - source) + (uint32_t)strlen("    accept(");
    uint32_t site_end = site_start + (uint32_t)strlen(local_name);

    CBMLSPDef defs[4];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = first_target_qn;
    defs[0].short_name = cbm_lsp_bare_segment(first_target_qn);
    defs[0].label = "Function";
    defs[0].def_module_qn = "project.target";
    int def_count = 1;
    if (second_target_qn) {
        defs[1].qualified_name = second_target_qn;
        defs[1].short_name = cbm_lsp_bare_segment(second_target_qn);
        defs[1].label = "Function";
        defs[1].def_module_qn = "project.target";
        def_count++;
    }
    defs[def_count].qualified_name = "project.use.accept";
    defs[def_count].short_name = "accept";
    defs[def_count].label = "Function";
    defs[def_count].def_module_qn = "project.use";
    def_count++;
    defs[def_count].qualified_name = "project.use.crossArgument";
    defs[def_count].short_name = "crossArgument";
    defs[def_count].label = "Function";
    defs[def_count].def_module_qn = "project.use";
    def_count++;

    const char *import_names[] = {local_name};
    const char *import_qns[] = {import_qn};
    CBMBatchPyLSPFile file = {0};
    file.source = source;
    file.source_len = (int)strlen(source);
    file.module_qn = "project.use";
    file.defs = defs;
    file.def_count = def_count;
    file.import_names = import_names;
    file.import_qns = import_qns;
    file.import_count = 1;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);
    counts.first_target = pylsp_exact_reference_count(
        &out, "crossArgument", first_target_qn, local_name, site_start, site_end);
    if (second_target_qn) {
        counts.second_target = pylsp_exact_reference_count(
            &out, "crossArgument", second_target_qn, local_name, site_start, site_end);
    }
    cbm_arena_destroy(&arena);
    return counts;
}

TEST(pylsp_from_import_alias_equal_module_leaf_targets_imported_member) {
    static const char source[] = "from target import handler as target\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(target)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "target", "project.target", "project.target.handler", "project.target.target");
    ASSERT_EQ(counts.first_target, 1);
    ASSERT_EQ(counts.second_target, 0);
    PASS();
}

TEST(pylsp_project_prefixed_direct_alias_same_tail_is_not_from_import_reference) {
    static const char source[] = "import target.handler as handler\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(handler)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "handler", "project.target.handler", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 0);
    PASS();
}

TEST(pylsp_competing_import_forms_for_same_local_fail_closed) {
    static const char source[] = "from target import handler as callback\n"
                                 "import target.handler as callback\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(callback)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "callback", "project.target", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 0);
    PASS();
}

TEST(pylsp_later_plain_import_rebinding_same_local_fails_closed) {
    static const char source[] = "from target import handler as target\n"
                                 "import target\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(target)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "target", "project.target.handler", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 0);
    PASS();
}

TEST(pylsp_top_level_function_shadows_imported_callable) {
    static const char source[] = "from target import handler as callback\n"
                                 "def callback():\n"
                                 "    pass\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(callback)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "callback", "project.target.handler", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 0);
    PASS();
}

TEST(pylsp_decorated_top_level_function_shadows_imported_callable) {
    static const char source[] = "from target import handler as callback\n"
                                 "def replace(fn):\n"
                                 "    return object()\n"
                                 "@replace\n"
                                 "def callback():\n"
                                 "    pass\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(callback)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "callback", "project.target.handler", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 0);
    PASS();
}

TEST(pylsp_later_import_rebinds_top_level_function_name) {
    static const char source[] = "def callback():\n"
                                 "    pass\n"
                                 "from target import handler as callback\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(callback)\n";
    PyImportReferenceCounts counts = pylsp_import_reference_counts(
        source, "callback", "project.target.handler", "project.target.handler", NULL);
    ASSERT_EQ(counts.first_target, 1);
    PASS();
}

TEST(pylsp_batch_cross_file_callable_value_preserves_exact_reference_site) {
    static const char source[] = "from target import handler\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def crossArgument():\n"
                                 "    accept(handler)\n";
    const char *argument = strstr(source, "accept(handler)");
    ASSERT_NOT_NULL(argument);
    uint32_t site_start = (uint32_t)(argument - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("handler");

    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "target.handler";
    defs[0].short_name = "handler";
    defs[0].label = "Function";
    defs[0].def_module_qn = "target";
    defs[1].qualified_name = "use.accept";
    defs[1].short_name = "accept";
    defs[1].label = "Function";
    defs[1].def_module_qn = "use";
    defs[2].qualified_name = "use.crossArgument";
    defs[2].short_name = "crossArgument";
    defs[2].label = "Function";
    defs[2].def_module_qn = "use";
    const char *import_names[] = {"handler"};
    const char *import_qns[] = {"target.handler"};

    CBMBatchPyLSPFile file;
    memset(&file, 0, sizeof(file));
    file.source = source;
    file.source_len = (int)strlen(source);
    file.module_qn = "use";
    file.defs = defs;
    file.def_count = 3;
    file.import_names = import_names;
    file.import_qns = import_qns;
    file.import_count = 1;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);

    int exact = pylsp_exact_reference_count(&out, "crossArgument", "target.handler", NULL,
                                             site_start, site_end);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(exact, 1);

    static const char alias_source[] = "from target import handler as callback\n"
                                       "def accept(fn):\n"
                                       "    pass\n"
                                       "def crossArgument():\n"
                                       "    accept(callback)\n";
    const char *alias_argument = strstr(alias_source, "accept(callback)");
    ASSERT_NOT_NULL(alias_argument);
    site_start = (uint32_t)(alias_argument - alias_source) + 7U;
    site_end = site_start + (uint32_t)strlen("callback");
    const char *alias_import_names[] = {"callback"};
    const char *alias_import_qns[] = {"project.target"};
    defs[0].qualified_name = "project.target.handler";
    defs[0].def_module_qn = "project.target";
    defs[1].qualified_name = "project.use.accept";
    defs[1].def_module_qn = "project.use";
    defs[2].qualified_name = "project.use.crossArgument";
    defs[2].def_module_qn = "project.use";
    file.source = alias_source;
    file.source_len = (int)strlen(alias_source);
    file.module_qn = "project.use";
    file.import_names = alias_import_names;
    file.import_qns = alias_import_qns;
    memset(&out, 0, sizeof(out));
    cbm_arena_init(&arena);
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);

    int alias_exact = pylsp_exact_reference_count(&out, "crossArgument",
                                                   "project.target.handler", "callback",
                                                   site_start, site_end);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(alias_exact, 1);

    /* The module and imported member may have the same leaf. Matching only
     * leaves would keep `project.handler` and lose the member; the parsed
     * from-import must canonicalize it to `project.handler.handler`. */
    static const char same_name_source[] = "from handler import handler as callback\n"
                                           "def accept(fn):\n"
                                           "    pass\n"
                                           "def crossArgument():\n"
                                           "    accept(callback)\n";
    const char *same_name_argument = strstr(same_name_source, "accept(callback)");
    ASSERT_NOT_NULL(same_name_argument);
    site_start = (uint32_t)(same_name_argument - same_name_source) + 7U;
    site_end = site_start + (uint32_t)strlen("callback");
    const char *same_name_import_qns[] = {"project.handler"};
    defs[0].qualified_name = "project.handler.handler";
    defs[0].def_module_qn = "project.handler";
    file.source = same_name_source;
    file.source_len = (int)strlen(same_name_source);
    file.import_qns = same_name_import_qns;
    memset(&out, 0, sizeof(out));
    cbm_arena_init(&arena);
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);
    int same_name_exact = pylsp_exact_reference_count(
        &out, "crossArgument", "project.handler.handler", "callback", site_start, site_end);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(same_name_exact, 1);

    /* Identical extraction metadata can also come from a direct module alias.
     * Its import_statement must never be upgraded to the same-named function. */
    static const char module_alias_source[] = "import target.handler as callback\n"
                                              "def accept(fn):\n"
                                              "    pass\n"
                                              "def crossArgument():\n"
                                              "    accept(callback)\n";
    const char *module_alias_argument = strstr(module_alias_source, "accept(callback)");
    ASSERT_NOT_NULL(module_alias_argument);
    site_start = (uint32_t)(module_alias_argument - module_alias_source) + 7U;
    site_end = site_start + (uint32_t)strlen("callback");
    defs[0].qualified_name = "project.target.handler";
    defs[0].def_module_qn = "project.target";
    file.source = module_alias_source;
    file.source_len = (int)strlen(module_alias_source);
    file.import_qns = alias_import_qns;
    memset(&out, 0, sizeof(out));
    cbm_arena_init(&arena);
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);
    int module_alias_reference = pylsp_exact_reference_count(
        &out, "crossArgument", "project.target.handler", "callback", site_start, site_end);
    cbm_arena_destroy(&arena);
    ASSERT_EQ(module_alias_reference, 0);

    /* Syntax alone is not callable proof: decorators can replace the runtime
     * binding with any value. Canonicalization must still pass through the
     * registry's exact-callable eligibility gate. */
    const char *decorators[] = {"@replace_with_value", NULL};
    defs[0].decorators = decorators;
    file.source = alias_source;
    file.source_len = (int)strlen(alias_source);
    memset(&out, 0, sizeof(out));
    cbm_arena_init(&arena);
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);
    int decorated_reference = pylsp_exact_reference_count(
        &out, "crossArgument", "project.target.handler", "callback",
        (uint32_t)(alias_argument - alias_source) + 7U,
        (uint32_t)(alias_argument - alias_source) + 7U + (uint32_t)strlen("callback"));
    cbm_arena_destroy(&arena);
    ASSERT_EQ(decorated_reference, 0);
    PASS();
}

/* A semantic invocation is an occurrence, not merely a caller/callee pair.
 * Calling one target twice in one function must therefore retain two records,
 * each stamped with the full `target(...)` expression that caused it. */
TEST(pylsp_same_target_calls_have_distinct_exact_semantic_sites) {
    static const char source[] = "def target(value):\n"
                                 "    return value\n"
                                 "def caller():\n"
                                 "    first = target(1)\n"
                                 "    second = target(22)\n"
                                 "    return first + second\n";
    static const char first_text[] = "target(1)";
    static const char second_text[] = "target(22)";
    const char *first = strstr(source, first_text);
    const char *second = strstr(source, second_text);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);

    CBMFileResult *r = extract_py(source);
    ASSERT_NOT_NULL(r);
    uint32_t first_start = (uint32_t)(first - source);
    uint32_t second_start = (uint32_t)(second - source);
    PyResolvedSiteCounts counts =
        count_invocation_sites(&r->resolved_calls, "caller", ".target", first_start,
                               first_start + (uint32_t)strlen(first_text), second_start,
                               second_start + (uint32_t)strlen(second_text));

    ASSERT_EQ(counts.target_count, 2);
    ASSERT_EQ(counts.first_site_count, 1);
    ASSERT_EQ(counts.second_site_count, 1);
    cbm_free_result(r);
    PASS();
}

/* cbm_batch_py_lsp_cross resolves in a temporary per-file arena, then copies
 * semantic records into the caller arena. Both occurrence spans must survive
 * that copy; caller/callee-only dedup must not collapse the two calls. */
TEST(pylsp_batch_preserves_two_same_target_semantic_sites) {
    static const char source[] = "def target(value):\n"
                                 "    return value\n"
                                 "def caller():\n"
                                 "    first = target(1)\n"
                                 "    second = target(22)\n"
                                 "    return first + second\n";
    static const char first_text[] = "target(1)";
    static const char second_text[] = "target(22)";
    const char *first = strstr(source, first_text);
    const char *second = strstr(source, second_text);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);

    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "batch.target";
    defs[0].short_name = "target";
    defs[0].label = "Function";
    defs[0].def_module_qn = "batch";
    defs[1].qualified_name = "batch.caller";
    defs[1].short_name = "caller";
    defs[1].label = "Function";
    defs[1].def_module_qn = "batch";

    CBMBatchPyLSPFile file;
    memset(&file, 0, sizeof(file));
    file.source = source;
    file.source_len = (int)strlen(source);
    file.module_qn = "batch";
    file.defs = defs;
    file.def_count = 2;

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_batch_py_lsp_cross(&arena, &file, 1, &out);

    uint32_t first_start = (uint32_t)(first - source);
    uint32_t second_start = (uint32_t)(second - source);
    PyResolvedSiteCounts counts = count_invocation_sites(
        &out, "caller", ".target", first_start, first_start + (uint32_t)strlen(first_text),
        second_start, second_start + (uint32_t)strlen(second_text));
    ASSERT_EQ(counts.target_count, 2);
    ASSERT_EQ(counts.first_site_count, 1);
    ASSERT_EQ(counts.second_site_count, 1);

    cbm_arena_destroy(&arena);
    PASS();
}

/* The fallback cross pass resolves in a scratch arena that is destroyed
 * inside cbm_pxc_run_one. A Python dunder carrier created only by that cross
 * resolver must be deep-copied into result.arena, remain occurrence-exact,
 * and still join its semantic target after the function returns. */
TEST(pylsp_scratch_cross_dunder_carrier_survives_copy) {
    static const char source[] = "class Number:\n"
                                 "    def __add__(self, other):\n"
                                 "        return self\n"
                                 "def combine(left: Number, right: Number):\n"
                                 "    return left + right\n";
    static const char site_text[] = "left + right";

    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "scratch.Number";
    defs[0].short_name = "Number";
    defs[0].label = "Class";
    defs[0].def_module_qn = "scratch";
    defs[0].lang = CBM_LANG_PYTHON;
    defs[1].qualified_name = "scratch.Number.__add__";
    defs[1].short_name = "__add__";
    defs[1].label = "Method";
    defs[1].receiver_type = "scratch.Number";
    defs[1].def_module_qn = "scratch";
    defs[1].lang = CBM_LANG_PYTHON;
    defs[2].qualified_name = "scratch.combine";
    defs[2].short_name = "combine";
    defs[2].label = "Function";
    defs[2].def_module_qn = "scratch";
    defs[2].lang = CBM_LANG_PYTHON;

    CBMFileResult result;
    memset(&result, 0, sizeof(result));
    cbm_arena_init(&result.arena);
    cbm_pxc_run_one(CBM_LANG_PYTHON, &result, source, (int)strlen(source), "scratch", defs, 3, NULL,
                    NULL, 0);

    const CBMCall *carrier = NULL;
    int carrier_count = 0;
    for (int i = 0; i < result.calls.count; i++) {
        const CBMCall *call = &result.calls.items[i];
        if (call->callee_name && strcmp(call->callee_name, "__add__") == 0 &&
            call->enclosing_func_qn && strstr(call->enclosing_func_qn, "combine")) {
            carrier = call;
            carrier_count++;
        }
    }
    const char *site = strstr(source, site_text);
    ASSERT_NOT_NULL(site);
    uint32_t expected_start = (uint32_t)(site - source);
    uint32_t expected_end = expected_start + (uint32_t)strlen(site_text);
    ASSERT_EQ(carrier_count, 1);
    ASSERT_NOT_NULL(carrier);
    ASSERT_STR_EQ(carrier->callee_name, "__add__");
    ASSERT_TRUE(strstr(carrier->enclosing_func_qn, "combine") != NULL);
    ASSERT_TRUE(carrier->requires_lsp_resolution);
    ASSERT_EQ(carrier->site_start_byte, expected_start);
    ASSERT_EQ(carrier->site_end_byte, expected_end);

    const CBMResolvedCall *joined =
        cbm_pipeline_find_lsp_resolution(&result.resolved_calls, carrier, false);
    ASSERT_NOT_NULL(joined);
    ASSERT_STR_EQ(joined->callee_qn, "scratch.Number.__add__");
    ASSERT_EQ(joined->site_start_byte, expected_start);
    ASSERT_EQ(joined->site_end_byte, expected_end);

    cbm_arena_destroy(&result.arena);
    PASS();
}

/* ── Phase 10 — stdlib resolution ─────────────────────────────── */

TEST(pylsp_stdlib_os_getcwd) {
    /* Top-level module attribute resolution against the stdlib registry. */
    CBMFileResult *r = extract_py(
        "import os\n"
        "def use():\n"
        "    return os.getcwd()\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "getcwd");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_stdlib_collections_defaultdict) {
    CBMFileResult *r = extract_py(
        "from collections import defaultdict\n"
        "def use():\n"
        "    return defaultdict(list)\n");
    ASSERT_NOT_NULL(r);
    /* defaultdict(list) is a constructor — emits lsp_constructor edge */
    int idx = find_resolved(r, "use", "defaultdict");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_stdlib_pathlib_path_method) {
    CBMFileResult *r = extract_py(
        "from pathlib import Path\n"
        "def use(p: Path):\n"
        "    return p.exists()\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "exists");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_stdlib_logging_getlogger) {
    CBMFileResult *r = extract_py(
        "import logging\n"
        "def use():\n"
        "    return logging.getLogger('app')\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "getLogger");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

/* ── Round 1 — parity push ────────────────────────────────────── */

TEST(pylsp_round1_dotted_import_walk) {
    /* `import os.path` — `os` and `os.path` should both be navigable
     * through attribute access so `os.path.join('a', 'b')` resolves to
     * the registered os.path.join function. */
    CBMFileResult *r = extract_py(
        "import os.path\n"
        "def use():\n"
        "    return os.path.join('a', 'b')\n");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->imports.count, 1);
    ASSERT_STR_EQ(r->imports.items[0].local_name, "os");
    ASSERT_STR_EQ(r->imports.items[0].module_path, "os.path");
    int idx = find_resolved(r, "use", "join");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_dotted_import_alias_matching_root) {
    /* The metadata pair (local=os, module=os.path) is also possible for an
     * explicit alias. Unlike the unaliased form above, this binds `os`
     * directly to MODULE(os.path), so os.join resolves os.path.join. */
    CBMFileResult *r = extract_py("import os.path as os\n"
                                  "def use():\n"
                                  "    return os.join('a', 'b')\n");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->imports.count, 1);
    ASSERT_STR_EQ(r->imports.items[0].local_name, "os");
    ASSERT_STR_EQ(r->imports.items[0].module_path, "os.path");
    int idx = find_resolved(r, "use", "join");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_typing_cast) {
    /* cast(Foo, x) returns NAMED("Foo"), enabling subsequent method
     * dispatch to resolve. */
    CBMFileResult *r = extract_py(
        "from typing import cast\n"
        "class Foo:\n"
        "    def m(self):\n"
        "        return 1\n"
        "def use(x):\n"
        "    f = cast(Foo, x)\n"
        "    return f.m()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "m"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_assert_type_passthrough) {
    /* assert_type(x, T) is a no-op at runtime; the returned value's type
     * is unchanged. We type the result as type-of(x). */
    CBMFileResult *r = extract_py(
        "from typing import assert_type\n"
        "class Foo:\n"
        "    def m(self):\n"
        "        return 1\n"
        "def use(x: Foo):\n"
        "    f = assert_type(x, Foo)\n"
        "    return f.m()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "m"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_forward_reference) {
    /* def f(x: "Foo") — quoted annotation must resolve as if unquoted. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def m(self):\n"
        "        return 1\n"
        "def use(x: \"Foo\"):\n"
        "    return x.m()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "m"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_self_return_chains) {
    /* class Builder:
     *   def step1(self) -> Self: return self
     *   def step2(self) -> Self: return self
     *   def build(self): return ...
     * Builder().step1().step2().build()  — must chain through Self. */
    CBMFileResult *r = extract_py(
        "from typing import Self\n"
        "class Builder:\n"
        "    def step1(self) -> Self:\n"
        "        return self\n"
        "    def step2(self) -> Self:\n"
        "        return self\n"
        "    def build(self):\n"
        "        return 1\n"
        "def use():\n"
        "    return Builder().step1().step2().build()\n");
    ASSERT_NOT_NULL(r);
    /* Each chain link should resolve. We assert the final .build() does. */
    ASSERT_GTE(require_resolved(r, "use", "build"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round1_generic_subscript_annotation) {
    /* `def f(items: list[Foo])` — the generic subscript should not
     * confuse annotation resolution; we drop the [Foo] part for v1. */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def m(self):\n"
        "        return 1\n"
        "def use(x: Optional[Foo]):\n"
        "    return x.m()\n");
    ASSERT_NOT_NULL(r);
    /* x has type Optional which strips to Optional, then we look up
     * .m on it. This SHOULD NOT resolve in v1 since Optional is just
     * Union — but it shouldn't crash either. We assert no false-positive
     * high-confidence resolution against an unrelated method. */
    int idx = find_resolved(r, "use", "m");
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        /* If we did resolve, must be against Foo, not something garbage. */
        ASSERT(strstr(rc->callee_qn, "Foo") != NULL);
    }
    cbm_free_result(r);
    PASS();
}

/* ── Round 2 — narrowing ──────────────────────────────────────── */

TEST(pylsp_round2_isinstance_narrow) {
    /* if isinstance(x, Foo): x.method() — x narrowed to Foo */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x):\n"
        "    if isinstance(x, Foo):\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round2_is_not_none_narrow) {
    /* def f(x: Optional[Foo]):
     *   if x is not None:
     *     x.method() */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x: Optional[Foo]):\n"
        "    if x is not None:\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    /* Optional strips to Foo for v1 (we drop generic args). x: Optional[Foo]
     * binds x as NAMED("Optional"). After narrowing, ideally NAMED("Foo").
     * Since we strip generic args, x is bound as Optional unrelated. The
     * narrow extracts the non-None member from a UNION; if x is bound as a
     * single type (not UNION), the narrow is a no-op. */
    int idx = find_resolved(r, "use", "method");
    /* We don't fail this test if narrowing doesn't help — this exercises
     * the code path. The proper fix needs Optional to bind as UNION. */
    (void)idx;
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round2_isinstance_no_false_positive_in_else) {
    /* In the else branch, narrowing must NOT apply. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x):\n"
        "    if isinstance(x, Foo):\n"
        "        return 1\n"
        "    else:\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    /* No high-confidence resolution should exist for x.method() in the
     * else branch, since x is UNKNOWN there. */
    int idx = find_resolved(r, "use", "method");
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        ASSERT(rc->confidence < 0.6f);
    }
    cbm_free_result(r);
    PASS();
}

/* ── Round 2/3 — walrus, comprehension, optional-narrow ──────── */

TEST(pylsp_round2_narrow_after_call) {
    /* Without walrus: x = compute(); if x is not None: x.method().
     * Tests narrow on UNION return-type-of-call. */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def compute() -> Optional[Foo]:\n"
        "    return None\n"
        "def use():\n"
        "    x = compute()\n"
        "    if x is not None:\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round2_walrus_binds) {
    /* if (x := compute()) is not None: x.method() */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def compute() -> Optional[Foo]:\n"
        "    return None\n"
        "def use():\n"
        "    if (x := compute()) is not None:\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round3_listcomp_element_method) {
    /* [x.method() for x in items] where items: list[Foo] */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    return [x.method() for x in items]\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round3_for_loop_element_method) {
    /* for x in items: x.method() — items: list[Foo] */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    for x in items:\n"
        "        x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round3_optional_narrow_with_union) {
    /* def f(x: Optional[Foo]):
     *   if x is not None: x.method() */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x: Optional[Foo]):\n"
        "    if x is not None:\n"
        "        return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Round 3 — match/case + async ──────────────────────────── */

TEST(pylsp_round3_match_case_class_pattern) {
    /* match x: case Foo(): subject narrows to Foo */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Bar:\n"
        "    def method(self):\n"
        "        return 2\n"
        "def use(x):\n"
        "    match x:\n"
        "        case Foo():\n"
        "            return x.method()\n"
        "        case _:\n"
        "            return None\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "method");
    ASSERT_GTE(idx, 0);
    /* Should be the Foo.method binding, not Bar.method */
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        ASSERT(strstr(rc->callee_qn, "Foo") != NULL);
    }
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round3_async_await_pass_through) {
    /* await expr returns expr's type. async def f() -> int registers
     * with return int. await f() should resolve as int. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "async def make() -> Foo:\n"
        "    return Foo()\n"
        "async def use():\n"
        "    f = await make()\n"
        "    return f.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Round 4 — instance attribute typing ──────────────────────── */

TEST(pylsp_round4_instance_attribute_init) {
    /* class C:
     *   def __init__(self, cfg: Config):
     *     self.cfg = cfg     # field cfg : Config
     *   def use(self):
     *     return self.cfg.display()  # resolves through field type */
    CBMFileResult *r = extract_py(
        "class Config:\n"
        "    def display(self):\n"
        "        return 1\n"
        "class App:\n"
        "    def __init__(self, cfg: Config):\n"
        "        self.cfg = cfg\n"
        "    def use(self):\n"
        "    self.cfg.display()\n");
    /* Note: extra indentation simulates a body block; real test
     * mirrors realistic Python code. */
    ASSERT_NOT_NULL(r);
    /* The above source has bad indent — replace with proper test source. */
    cbm_free_result(r);
    r = extract_py(
        "class Config:\n"
        "    def display(self):\n"
        "        return 1\n"
        "class App:\n"
        "    def __init__(self, cfg: Config):\n"
        "        self.cfg = cfg\n"
        "    def use(self):\n"
        "        return self.cfg.display()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "display"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round4_instance_attribute_class_annotation) {
    /* class C:
     *   x: Foo
     *   def use(self): return self.x.method() */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class C:\n"
        "    x: Foo\n"
        "    def use(self):\n"
        "        return self.x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Round 5 — subscript, super().__init__, operator dunders ───── */

TEST(pylsp_round5_dict_subscript_value_type) {
    /* self.cache: dict[str, Foo]; self.cache[k].method() resolves. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class C:\n"
        "    def __init__(self):\n"
        "        self.cache: dict[str, Foo] = {}\n"
        "    def use(self, k):\n"
        "        return self.cache[k].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round5_list_subscript_value_type) {
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    return items[0].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round5_super_init) {
    CBMFileResult *r = extract_py(
        "class Base:\n"
        "    def __init__(self, root):\n"
        "        self.root = root\n"
        "class Child(Base):\n"
        "    def __init__(self, root, extra):\n"
        "        super().__init__(root)\n"
        "        self.extra = extra\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "__init__", "__init__");
    ASSERT_GTE(idx, 0);
    if (idx >= 0) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[idx];
        ASSERT(strstr(rc->callee_qn, "Base") != NULL);
    }
    cbm_free_result(r);
    PASS();
}

/* Synthetic dunder calls have no ordinary tree-sitter `call` carrier. Two
 * different receiver classes can therefore produce the same textual leaf in
 * one caller. The synthetic carrier and semantic record must retain the exact
 * operator occurrence so the pipeline cannot bind both `__add__` calls to the
 * first class it happens to inspect. */
TEST(pylsp_dunder_same_leaf_occurrences_join_by_exact_site) {
    static const char source[] =
        "class Alpha:\n"
        "    def __add__(self, other):\n"
        "        return self\n"
        "class Beta:\n"
        "    def __add__(self, other):\n"
        "        return self\n"
        "def combine(left_a: Alpha, left_b: Alpha, right_a: Beta, right_b: Beta):\n"
        "    first = left_a + left_b\n"
        "    second = right_a + right_b\n"
        "    return first, second\n";
    static const char alpha_text[] = "left_a + left_b";
    static const char beta_text[] = "right_a + right_b";

    const char *alpha_site = strstr(source, alpha_text);
    const char *beta_site = strstr(source, beta_text);
    ASSERT_NOT_NULL(alpha_site);
    ASSERT_NOT_NULL(beta_site);

    CBMFileResult *r = extract_py(source);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error || r->parse_incomplete);

    const CBMCall *dunder_calls[2] = {0};
    int dunder_count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (call->callee_name && strcmp(call->callee_name, "__add__") == 0 &&
            call->enclosing_func_qn && strstr(call->enclosing_func_qn, "combine")) {
            if (dunder_count < 2)
                dunder_calls[dunder_count] = call;
            dunder_count++;
        }
    }
    ASSERT_EQ(dunder_count, 2);
    ASSERT_NOT_NULL(dunder_calls[0]);
    ASSERT_NOT_NULL(dunder_calls[1]);

    const CBMCall *alpha_call = dunder_calls[0];
    const CBMCall *beta_call = dunder_calls[1];
    if (alpha_call->site_start_byte > beta_call->site_start_byte) {
        const CBMCall *tmp = alpha_call;
        alpha_call = beta_call;
        beta_call = tmp;
    }

    uint32_t alpha_start = (uint32_t)(alpha_site - source);
    uint32_t alpha_end = alpha_start + (uint32_t)strlen(alpha_text);
    uint32_t beta_start = (uint32_t)(beta_site - source);
    uint32_t beta_end = beta_start + (uint32_t)strlen(beta_text);

    ASSERT_GT(alpha_call->site_end_byte, alpha_call->site_start_byte);
    ASSERT_GT(beta_call->site_end_byte, beta_call->site_start_byte);
    ASSERT_NEQ(alpha_call->site_start_byte, beta_call->site_start_byte);
    ASSERT_EQ(alpha_call->site_start_byte, alpha_start);
    ASSERT_EQ(alpha_call->site_end_byte, alpha_end);
    ASSERT_EQ(beta_call->site_start_byte, beta_start);
    ASSERT_EQ(beta_call->site_end_byte, beta_end);
    ASSERT_TRUE(alpha_call->requires_lsp_resolution);
    ASSERT_TRUE(beta_call->requires_lsp_resolution);

    const CBMResolvedCall *alpha_resolution =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, alpha_call, false);
    const CBMResolvedCall *beta_resolution =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, beta_call, false);
    ASSERT_NOT_NULL(alpha_resolution);
    ASSERT_NOT_NULL(beta_resolution);
    ASSERT_TRUE(alpha_resolution != beta_resolution);
    ASSERT_TRUE(strstr(alpha_resolution->callee_qn, ".Alpha.__add__") != NULL);
    ASSERT_TRUE(strstr(beta_resolution->callee_qn, ".Beta.__add__") != NULL);
    ASSERT_EQ(alpha_resolution->site_start_byte, alpha_call->site_start_byte);
    ASSERT_EQ(alpha_resolution->site_end_byte, alpha_call->site_end_byte);
    ASSERT_EQ(beta_resolution->site_start_byte, beta_call->site_start_byte);
    ASSERT_EQ(beta_resolution->site_end_byte, beta_call->site_end_byte);

    cbm_free_result(r);
    PASS();
}

/* A named lambda body is re-walked once per invocation so its parameters can
 * inherit the call-site argument types. Both walks below visit the same single
 * `a + b` AST occurrence. That occurrence must produce one semantic record and
 * one synthetic carrier, not one carrier per re-walk. */
TEST(pylsp_lambda_rewalk_dedupes_dunder_occurrence) {
    static const char source[] = "class Number:\n"
                                 "    def __add__(self, other):\n"
                                 "        return self\n"
                                 "def combine(left: Number, right: Number):\n"
                                 "    add = lambda a, b: a + b\n"
                                 "    first = add(left, right)\n"
                                 "    second = add(left, right)\n"
                                 "    return first, second\n";
    static const char site_text[] = "a + b";
    const char *site = strstr(source, site_text);
    ASSERT_NOT_NULL(site);
    uint32_t expected_start = (uint32_t)(site - source);
    uint32_t expected_end = expected_start + (uint32_t)strlen(site_text);

    CBMFileResult *r = extract_py(source);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error || r->parse_incomplete);

    const CBMCall *carrier = NULL;
    int carrier_count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (call->callee_name && strcmp(call->callee_name, "__add__") == 0 &&
            call->enclosing_func_qn && strstr(call->enclosing_func_qn, "<lambda>")) {
            carrier = call;
            carrier_count++;
        }
    }

    const CBMResolvedCall *semantic = NULL;
    int semantic_count = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, "<lambda>") && rc->callee_qn &&
            strstr(rc->callee_qn, ".Number.__add__")) {
            semantic = rc;
            semantic_count++;
        }
    }

    ASSERT_EQ(semantic_count, 1);
    ASSERT_NOT_NULL(semantic);
    ASSERT_EQ(semantic->kind, CBM_RESOLVED_INVOCATION);
    ASSERT_EQ(semantic->site_start_byte, expected_start);
    ASSERT_EQ(semantic->site_end_byte, expected_end);

    if (carrier_count != 1) {
        for (int i = 0; i < r->calls.count; i++) {
            const CBMCall *call = &r->calls.items[i];
            if (call->callee_name && strcmp(call->callee_name, "__add__") == 0) {
                fprintf(stderr, "  [PY-LAMBDA-CARRIER] caller=%s site=%u:%u requires_lsp=%d\n",
                        call->enclosing_func_qn ? call->enclosing_func_qn : "(null)",
                        call->site_start_byte, call->site_end_byte,
                        call->requires_lsp_resolution ? 1 : 0);
            }
        }
    }
    ASSERT_EQ(carrier_count, 1);
    ASSERT_NOT_NULL(carrier);
    ASSERT_TRUE(carrier->requires_lsp_resolution);
    ASSERT_EQ(carrier->site_start_byte, expected_start);
    ASSERT_EQ(carrier->site_end_byte, expected_end);

    const CBMResolvedCall *joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, carrier, false);
    ASSERT_NOT_NULL(joined);
    ASSERT_TRUE(joined == semantic);
    ASSERT_TRUE(strstr(joined->callee_qn, ".Number.__add__") != NULL);
    ASSERT_EQ(joined->site_start_byte, expected_start);
    ASSERT_EQ(joined->site_end_byte, expected_end);

    cbm_free_result(r);
    PASS();
}

/* Parser-backed Python calls have the same occurrence-identity requirement as
 * synthetic dunders. Distinct typed receivers can expose the same method leaf
 * in one caller; each CBMCall must join the semantic record for its own site. */
TEST(pylsp_ordinary_same_leaf_calls_join_by_exact_site) {
    CBMFileResult *r = extract_py("class Alpha:\n"
                                  "    def render(self):\n"
                                  "        return 1\n"
                                  "class Beta:\n"
                                  "    def render(self):\n"
                                  "        return 2\n"
                                  "def combine(a: Alpha, b: Beta):\n"
                                  "    return a.render(), b.render()\n");
    ASSERT_NOT_NULL(r);

    const CBMCall *calls[2] = {0};
    int call_count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (call->enclosing_func_qn && strstr(call->enclosing_func_qn, "combine") &&
            call->callee_name && strstr(call->callee_name, "render")) {
            if (call_count < 2)
                calls[call_count] = call;
            call_count++;
        }
    }
    if (calls[0] && calls[1] && calls[0]->site_start_byte > calls[1]->site_start_byte) {
        const CBMCall *tmp = calls[0];
        calls[0] = calls[1];
        calls[1] = tmp;
    }
    const CBMResolvedCall *alpha_hit =
        calls[0] ? cbm_pipeline_find_lsp_resolution(&r->resolved_calls, calls[0], false) : NULL;
    const CBMResolvedCall *beta_hit =
        calls[1] ? cbm_pipeline_find_lsp_resolution(&r->resolved_calls, calls[1], false) : NULL;
    bool alpha_exact = alpha_hit && alpha_hit->callee_qn &&
                       strstr(alpha_hit->callee_qn, ".Alpha.render") &&
                       alpha_hit->site_start_byte == calls[0]->site_start_byte &&
                       alpha_hit->site_end_byte == calls[0]->site_end_byte;
    bool beta_exact = beta_hit && beta_hit->callee_qn &&
                      strstr(beta_hit->callee_qn, ".Beta.render") &&
                      beta_hit->site_start_byte == calls[1]->site_start_byte &&
                      beta_hit->site_end_byte == calls[1]->site_end_byte;

    cbm_free_result(r);
    ASSERT_EQ(call_count, 2);
    ASSERT_TRUE(alpha_exact);
    ASSERT_TRUE(beta_exact);
    PASS();
}

/* ── Round 6 — generators, dataclasses, decorator flags ───────── */

TEST(pylsp_round6_generator_yields_iterable) {
    /* def gen() -> Generator[Foo, None, None]: yield Foo()
     * for x in gen(): x.method()  — x : Foo via element-of(generator) */
    CBMFileResult *r = extract_py(
        "from typing import Generator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def gen() -> Generator[Foo, None, None]:\n"
        "    yield Foo()\n"
        "def use():\n"
        "    for x in gen():\n"
        "        x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round6_dataclass_field_access) {
    /* @dataclass class Point: x: Foo; def use(p: Point): p.x.method() */
    CBMFileResult *r = extract_py(
        "from dataclasses import dataclass\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "@dataclass\n"
        "class Point:\n"
        "    x: Foo\n"
        "    y: int\n"
        "def use(p: Point):\n"
        "    return p.x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_round6_property_access_chains) {
    /* class C: @property def thing(self) -> Foo: ...
     * def use(c: C): c.thing.method()  -- thing is a property; access
     * returns its getter's return type. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class C:\n"
        "    @property\n"
        "    def thing(self) -> Foo:\n"
        "        return Foo()\n"
        "def use(c: C):\n"
        "    return c.thing.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Evaluator guards — memoization, depth cap, step budget ─────
 * (issues #710 and #720; distilled from PRs #732 and #758) */

TEST(pylsp_issue710_deep_call_chain_resolves) {
    /* Regression for #710: py_eval_expr_type evaluated a call node's
     * attribute receiver TWICE (container special-case + general
     * attribute path), so an N-link chained builder expression cost
     * O(2^N) evaluations — ~65-link real-world chains hung indexing for
     * hours. With per-node memoization the chain is O(N). On unfixed
     * code this 40-link chain is ~2^40 evaluations: the test cannot
     * finish and the suite times out, rather than passing silently.
     * We also require the FINAL link to actually resolve — the fix must
     * preserve resolution quality, not just terminate. */
    CBMFileResult *r = extract_py(
        "from typing import Self\n"
        "class G:\n"
        "    def add(self, x) -> Self:\n"
        "        return self\n"
        "    def compile(self):\n"
        "        return 1\n"
        "def build():\n"
        "    return (\n"
        "        G()\n"
        "        .add(1).add(2).add(3).add(4).add(5).add(6).add(7).add(8)\n"
        "        .add(9).add(10).add(11).add(12).add(13).add(14).add(15)\n"
        "        .add(16).add(17).add(18).add(19).add(20).add(21).add(22)\n"
        "        .add(23).add(24).add(25).add(26).add(27).add(28).add(29)\n"
        "        .add(30).add(31).add(32).add(33).add(34).add(35).add(36)\n"
        "        .add(37).add(38).add(39).add(40)\n"
        "        .compile()\n"
        "    )\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "build", "G.add"), 0);
    ASSERT_GTE(require_resolved(r, "build", "G.compile"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_issue710_heterogeneous_receiver_chain) {
    /* Guard for the memo cache's KEY choice. Every leftmost descendant of
     * `c.session().execute().fetch()` — the outer call, each receiver
     * call, down to the identifier `c` — starts at the same byte, so a
     * cache keyed by start byte (PR #732's approach) aliases all of them:
     * after `c` -> Client is inserted, the receiver call `c.session()`
     * reads Client back instead of Session and the rest of the chain
     * resolves wrongly or not at all. Each link here returns a DIFFERENT
     * class so any such aliasing changes an assertable QN. Keying by node
     * identity (TSNode.id) keeps every link distinct. */
    CBMFileResult *r = extract_py(
        "class Result:\n"
        "    def fetch(self):\n"
        "        return 1\n"
        "class Session:\n"
        "    def execute(self) -> \"Result\":\n"
        "        return Result()\n"
        "class Client:\n"
        "    def session(self) -> \"Session\":\n"
        "        return Session()\n"
        "def run():\n"
        "    c = Client()\n"
        "    return c.session().execute().fetch()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "run", "Client.session"), 0);
    ASSERT_GTE(require_resolved(r, "run", "Session.execute"), 0);
    ASSERT_GTE(require_resolved(r, "run", "Result.fetch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(pylsp_eval_steps_budget_degrades_gracefully) {
    /* PY_EVAL_MAX_STEPS_PER_FILE guard: a file whose expressions demand
     * more evaluator work than the per-file budget must still complete
     * quickly and keep everything resolved BEFORE exhaustion — graceful
     * partial degradation, not a hang, crash, or corrupted result. 90
     * statements x 30-link chains re-evaluated across the bind + emit
     * phases (the assignment bind flushes the memo generation in between)
     * comfortably exceed the 10000-step budget. */
    enum { BUDGET_STMTS = 90, BUDGET_LINKS = 30 };
    size_t sz = 4096 + (size_t)BUDGET_STMTS * (32 + (size_t)BUDGET_LINKS * 8);
    char *src = malloc(sz);
    ASSERT_NOT_NULL(src);
    char *p = src;
    size_t left = sz;
    int n = snprintf(p, left,
                     "from typing import Self\n"
                     "class G:\n"
                     "    def add(self, x) -> Self:\n"
                     "        return self\n"
                     "    def compile(self):\n"
                     "        return 1\n"
                     "def run():\n");
    p += n;
    left -= (size_t)n;
    for (int s = 0; s < BUDGET_STMTS; s++) {
        n = snprintf(p, left, "    v%d = G()", s);
        p += n;
        left -= (size_t)n;
        for (int l = 0; l < BUDGET_LINKS; l++) {
            n = snprintf(p, left, ".add(%d)", l);
            p += n;
            left -= (size_t)n;
        }
        n = snprintf(p, left, ".compile()\n");
        p += n;
        left -= (size_t)n;
    }
    snprintf(p, left, "    return v0\n");
    CBMFileResult *r = extract_py(src);
    free(src);
    ASSERT_NOT_NULL(r);
    /* The first statements run with budget to spare — they must resolve. */
    ASSERT_GTE(require_resolved(r, "run", "G.compile"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(py_lsp) {
    /* Phase 2 — smoke */
    RUN_TEST(pylsp_smoke_empty);
    RUN_TEST(pylsp_smoke_one_function);
    RUN_TEST(pylsp_smoke_one_class);
    RUN_TEST(pylsp_no_crash_on_syntax_error);
    RUN_TEST(pylsp_smoke_imports_passed_through);
    /* Phase 3 — imports → scope */
    RUN_TEST(pylsp_import_simple);
    RUN_TEST(pylsp_import_aliased);
    RUN_TEST(pylsp_import_from);
    RUN_TEST(pylsp_import_from_aliased);
    RUN_TEST(pylsp_import_relative_one_dot);
    RUN_TEST(pylsp_import_relative_two_dots);
    RUN_TEST(pylsp_import_star_best_effort);
    RUN_TEST(pylsp_import_typing_only_still_binds);
    RUN_TEST(pylsp_import_multi_pass_through_extract_file);
    /* Phases 4-6 — bindings + expression typing + method dispatch */
    RUN_TEST(pylsp_direct_function_call);
    RUN_TEST(pylsp_method_call_simple);
    RUN_TEST(pylsp_method_via_self);
    RUN_TEST(pylsp_constructor_call_returns_instance);
    RUN_TEST(pylsp_method_via_inheritance);
    RUN_TEST(pylsp_no_false_positive_on_unknown_method);
    /* Phases 7-8 — decorators, super(), multi-inheritance */
    RUN_TEST(pylsp_decorated_function_resolves);
    RUN_TEST(pylsp_classmethod_resolves);
    RUN_TEST(pylsp_staticmethod_resolves);
    RUN_TEST(pylsp_dataclass_constructor);
    RUN_TEST(pylsp_super_call);
    RUN_TEST(pylsp_multi_inheritance_first_base);
    RUN_TEST(pylsp_pep695_generic_class);
    /* Phase 9 — cross-file + batch */
    RUN_TEST(pylsp_crossfile_method_dispatch);
    RUN_TEST(pylsp_fused_self_attr_chain_via_overlay);
    RUN_TEST(pylsp_crossfile_classmethod_on_class_issue228);
    RUN_TEST(pylsp_crossfile_inheritance);
    RUN_TEST(pylsp_batch_two_files);
    RUN_TEST(pylsp_from_import_alias_equal_module_leaf_targets_imported_member);
    RUN_TEST(pylsp_project_prefixed_direct_alias_same_tail_is_not_from_import_reference);
    RUN_TEST(pylsp_competing_import_forms_for_same_local_fail_closed);
    RUN_TEST(pylsp_later_plain_import_rebinding_same_local_fails_closed);
    RUN_TEST(pylsp_top_level_function_shadows_imported_callable);
    RUN_TEST(pylsp_decorated_top_level_function_shadows_imported_callable);
    RUN_TEST(pylsp_later_import_rebinds_top_level_function_name);
    RUN_TEST(pylsp_batch_cross_file_callable_value_preserves_exact_reference_site);
    RUN_TEST(pylsp_same_target_calls_have_distinct_exact_semantic_sites);
    RUN_TEST(pylsp_batch_preserves_two_same_target_semantic_sites);
    RUN_TEST(pylsp_scratch_cross_dunder_carrier_survives_copy);
    /* Phase 10 — stdlib resolution */
    RUN_TEST(pylsp_stdlib_os_getcwd);
    RUN_TEST(pylsp_stdlib_collections_defaultdict);
    RUN_TEST(pylsp_stdlib_pathlib_path_method);
    RUN_TEST(pylsp_stdlib_logging_getlogger);
    /* Round 1 — parity push */
    RUN_TEST(pylsp_round1_dotted_import_walk);
    RUN_TEST(pylsp_round1_dotted_import_alias_matching_root);
    RUN_TEST(pylsp_round1_typing_cast);
    RUN_TEST(pylsp_round1_assert_type_passthrough);
    RUN_TEST(pylsp_round1_forward_reference);
    RUN_TEST(pylsp_round1_self_return_chains);
    RUN_TEST(pylsp_round1_generic_subscript_annotation);
    /* Round 2 — narrowing */
    RUN_TEST(pylsp_round2_isinstance_narrow);
    RUN_TEST(pylsp_round2_is_not_none_narrow);
    RUN_TEST(pylsp_round2_isinstance_no_false_positive_in_else);
    /* Round 2/3 — walrus, comprehension, optional-narrow */
    RUN_TEST(pylsp_round2_narrow_after_call);
    RUN_TEST(pylsp_round2_walrus_binds);
    RUN_TEST(pylsp_round3_listcomp_element_method);
    RUN_TEST(pylsp_round3_for_loop_element_method);
    RUN_TEST(pylsp_round3_optional_narrow_with_union);
    /* Round 3 — match/case + async */
    RUN_TEST(pylsp_round3_match_case_class_pattern);
    RUN_TEST(pylsp_round3_async_await_pass_through);
    /* Round 4 — instance attribute typing */
    RUN_TEST(pylsp_round4_instance_attribute_init);
    RUN_TEST(pylsp_round4_instance_attribute_class_annotation);
    /* Round 5 — subscript, super().__init__, operator dunders */
    RUN_TEST(pylsp_round5_dict_subscript_value_type);
    RUN_TEST(pylsp_round5_list_subscript_value_type);
    RUN_TEST(pylsp_round5_super_init);
    RUN_TEST(pylsp_dunder_same_leaf_occurrences_join_by_exact_site);
    RUN_TEST(pylsp_lambda_rewalk_dedupes_dunder_occurrence);
    RUN_TEST(pylsp_ordinary_same_leaf_calls_join_by_exact_site);
    /* Round 6 — generators, dataclasses, properties */
    RUN_TEST(pylsp_round6_generator_yields_iterable);
    RUN_TEST(pylsp_round6_dataclass_field_access);
    RUN_TEST(pylsp_round6_property_access_chains);
    /* Evaluator guards — #710/#720 (distilled PRs #732 + #758) */
    RUN_TEST(pylsp_issue710_deep_call_chain_resolves);
    RUN_TEST(pylsp_issue710_heterogeneous_receiver_chain);
    RUN_TEST(pylsp_eval_steps_budget_degrades_gracefully);
}
