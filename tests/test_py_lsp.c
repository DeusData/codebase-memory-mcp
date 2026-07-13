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
    /* Aliased name doesn't end module_qn with .P, so this binds as MODULE.
     * Phase 6 registry lookup will downgrade to NAMED if registry has no
     * matching module entry. Both behaviors are acceptable for v1; the
     * test asserts the entry exists with the correct QN. */
    ASSERT(t->kind == CBM_TYPE_NAMED || t->kind == CBM_TYPE_MODULE);
    if (t->kind == CBM_TYPE_NAMED) {
        ASSERT_STR_EQ(t->data.named.qualified_name, "pathlib.Path");
    } else {
        ASSERT_STR_EQ(t->data.module.module_qn, "pathlib.Path");
    }
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

    cbm_run_py_lsp_cross(&arena, source, (int)strlen(source), "test.main",
                         defs, 2, imp_names, imp_qns, 1, NULL, &out);

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
    cbm_run_py_lsp_cross_with_registry(&arena, src, (int)strlen(src), "test.mod", reg, NULL, NULL, 0,
                                       NULL, &out);

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
                         imp_qns, 1, NULL, &out);

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

    cbm_run_py_lsp_cross(&arena, source, (int)strlen(source), "test.main",
                         defs, 4, imp_names, imp_qns, 1, NULL, &out);

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

/* ── Phase 10 — stdlib resolution ─────────────────────────────── */

TEST(pylsp_stdlib_os_getcwd) {
    /* Top-level module attribute resolution against the stdlib registry.
     * Note: `import os.path` only binds `path` in scope (the leaf binding
     * extracted from the dotted import) — full submodule traversal of
     * `os.path.join` style accesses needs a Phase 10.5 follow-up where we
     * also stamp parent-module bindings. For v1, top-level module calls
     * resolve correctly. */
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
    /* Phase 10 — stdlib resolution */
    RUN_TEST(pylsp_stdlib_os_getcwd);
    RUN_TEST(pylsp_stdlib_collections_defaultdict);
    RUN_TEST(pylsp_stdlib_pathlib_path_method);
    RUN_TEST(pylsp_stdlib_logging_getlogger);
    /* Round 1 — parity push */
    RUN_TEST(pylsp_round1_dotted_import_walk);
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
    /* Round 6 — generators, dataclasses, properties */
    RUN_TEST(pylsp_round6_generator_yields_iterable);
    RUN_TEST(pylsp_round6_dataclass_field_access);
    RUN_TEST(pylsp_round6_property_access_chains);
    /* Evaluator guards — #710/#720 (distilled PRs #732 + #758) */
    RUN_TEST(pylsp_issue710_deep_call_chain_resolves);
    RUN_TEST(pylsp_issue710_heterogeneous_receiver_chain);
    RUN_TEST(pylsp_eval_steps_budget_degrades_gracefully);
}
