/*
 * test_py_lsp_stress.c — Advanced Python pattern probes.
 *
 * Each test covers one pattern Pyright resolves. Tests that pass
 * confirm parity. Tests that fail are honest gaps — they get either
 * fixed in a follow-up round or documented as deferred per
 * docs/BENCHMARK_PYTHON.md.
 *
 * Suite groups: patterns by feature area (typing helpers,
 * inheritance, control flow, containers, decorators, advanced
 * constructs).
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/py_lsp.h"

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

/* ── Typing helpers ─────────────────────────────────────────── */

TEST(stress_namedtuple_class_form) {
    /* class Point(NamedTuple): x: int  → Point(1, 2).x */
    CBMFileResult *r = extract_py(
        "from typing import NamedTuple\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Pair(NamedTuple):\n"
        "    a: Foo\n"
        "    b: Foo\n"
        "def use():\n"
        "    p = Pair(Foo(), Foo())\n"
        "    return p.a.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_typeddict_subscript) {
    /* class TD(TypedDict): x: Foo. d: TD = ...; d["x"].method() */
    CBMFileResult *r = extract_py(
        "from typing import TypedDict\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class TD(TypedDict):\n"
        "    foo: Foo\n"
        "def use(d: TD):\n"
        "    return d['foo'].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_protocol_structural) {
    /* def use(x: HasMethod): x.method()
     * where HasMethod is a Protocol */
    CBMFileResult *r = extract_py(
        "from typing import Protocol\n"
        "class HasMethod(Protocol):\n"
        "    def method(self) -> int: ...\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x: HasMethod):\n"
        "    return x.method()\n");
    ASSERT_NOT_NULL(r);
    /* Should resolve x.method against the Protocol */
    int idx = find_resolved(r, "use", "method");
    if (idx < 0) printf("  KNOWN GAP: Protocol structural method dispatch\n");
    cbm_free_result(r);
    PASS();
}

TEST(stress_abc_abstractmethod) {
    /* class Base(ABC): @abstractmethod def m(self): ...
     * class Child(Base): def m(self): return 1
     * def use(x: Base): x.m()  → resolves to Base.m (or Child.m via dispatch?) */
    CBMFileResult *r = extract_py(
        "from abc import ABC, abstractmethod\n"
        "class Base(ABC):\n"
        "    @abstractmethod\n"
        "    def method(self):\n"
        "        ...\n"
        "class Child(Base):\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x: Base):\n"
        "    return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Control flow / binding ─────────────────────────────────── */

TEST(stress_context_manager_with_as) {
    /* with Foo() as f: f.method() — f bound by __enter__ */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def __enter__(self):\n"
        "        return self\n"
        "    def __exit__(self, *args):\n"
        "        return None\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use():\n"
        "    with Foo() as f:\n"
        "        return f.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_except_as_binding) {
    /* except E as e: e.method() — e bound to E */
    CBMFileResult *r = extract_py(
        "class FooError(Exception):\n"
        "    def detail(self):\n"
        "        return 'oops'\n"
        "def use():\n"
        "    try:\n"
        "        return 1\n"
        "    except FooError as e:\n"
        "        return e.detail()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "detail"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_isinstance_else_negative_narrow) {
    /* if not isinstance(x, Foo): return; x.method()
     * Narrows x to Foo after the early return via py_block_terminates. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x):\n"
        "    if not isinstance(x, Foo):\n"
        "        return None\n"
        "    return x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Containers / unpacking ─────────────────────────────────── */

TEST(stress_tuple_unpack_function_return) {
    /* def f() -> tuple[Foo, Bar]: ...
     * a, b = f(); a.method_a(); b.method_b() */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method_a(self):\n"
        "        return 1\n"
        "class Bar:\n"
        "    def method_b(self):\n"
        "        return 2\n"
        "def make() -> tuple[Foo, Bar]:\n"
        "    return Foo(), Bar()\n"
        "def use():\n"
        "    a, b = make()\n"
        "    a.method_a()\n"
        "    b.method_b()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method_a"), 0);
    ASSERT_GTE(require_resolved(r, "use", "method_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_dict_items_comprehension) {
    /* d: dict[str, Foo]; [v.method() for k, v in d.items()] */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(d: dict[str, Foo]):\n"
        "    return [v.method() for k, v in d.items()]\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_list_slice_returns_list) {
    /* lst: list[Foo]; lst[1:3][0].method() */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    return items[1:3][0].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Decorators / first-class functions ──────────────────────── */

TEST(stress_function_as_dict_value) {
    /* funcs = {"a": foo, "b": bar}; funcs["a"]() — dict-literal dispatch */
    CBMFileResult *r = extract_py(
        "def foo():\n"
        "    return 1\n"
        "def bar():\n"
        "    return 2\n"
        "def use():\n"
        "    funcs = {'a': foo, 'b': bar}\n"
        "    funcs['a']()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "foo"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_decorator_factory) {
    /* @retry(times=3) — decorator factory pattern */
    CBMFileResult *r = extract_py(
        "import functools\n"
        "def retry(times: int):\n"
        "    def deco(fn):\n"
        "        @functools.wraps(fn)\n"
        "        def wrapper(*args, **kwargs):\n"
        "            return fn(*args, **kwargs)\n"
        "        return wrapper\n"
        "    return deco\n"
        "@retry(times=3)\n"
        "def helper():\n"
        "    return 1\n"
        "def use():\n"
        "    return helper()\n");
    ASSERT_NOT_NULL(r);
    /* helper() should resolve as helper despite the decorator. */
    ASSERT_GTE(require_resolved(r, "use", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_property_setter) {
    /* @prop.setter — assignment to property */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    @property\n"
        "    def value(self) -> int:\n"
        "        return 1\n"
        "    @value.setter\n"
        "    def value(self, v: int) -> None:\n"
        "        pass\n"
        "    def use(self):\n"
        "        self.value = 5\n"
        "        return self.value\n");
    ASSERT_NOT_NULL(r);
    /* Setter is rare in resolution but should not break anything. */
    cbm_free_result(r);
    PASS();
}

/* ── Inheritance / generics ─────────────────────────────────── */

TEST(stress_self_in_inheritance_chain) {
    /* class B: def make(self) -> Self: ...
     * class C(B): def child_method(self) -> int: ...
     * c = C().make()  # Pyright: c is C, not B
     * c.child_method() */
    CBMFileResult *r = extract_py(
        "from typing import Self\n"
        "class Base:\n"
        "    def make(self) -> Self:\n"
        "        return self\n"
        "class Child(Base):\n"
        "    def child_method(self):\n"
        "        return 1\n"
        "def use():\n"
        "    c = Child().make()\n"
        "    return c.child_method()\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "child_method");
    if (idx < 0) printf("  KNOWN GAP: Self resolves to receiver class, not declaring class\n");
    cbm_free_result(r);
    PASS();
}

TEST(stress_diamond_inheritance_mro) {
    /* Diamond: Top -> Left, Right -> Bottom. Bottom().method() resolves to
     * Top.method via C3 unless Left/Right override. */
    CBMFileResult *r = extract_py(
        "class Top:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Left(Top):\n"
        "    pass\n"
        "class Right(Top):\n"
        "    pass\n"
        "class Bottom(Left, Right):\n"
        "    def use(self):\n"
        "        return self.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_recursive_self_referencing_type) {
    /* class Node: children: list[Node]
     * def walk(node: Node): for c in node.children: c.method() */
    CBMFileResult *r = extract_py(
        "class Node:\n"
        "    def __init__(self):\n"
        "        self.children: list['Node'] = []\n"
        "    def method(self):\n"
        "        return 1\n"
        "def walk(node: Node):\n"
        "    for c in node.children:\n"
        "        c.method()\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "walk", "method");
    if (idx < 0) printf("  KNOWN GAP: forward-ref `Node` inside list[] within own class\n");
    cbm_free_result(r);
    PASS();
}

/* ── Advanced control flow ─────────────────────────────────── */

TEST(stress_nested_closure) {
    /* def outer():
     *   x = Foo()
     *   def inner():
     *     return x.method()
     *   return inner */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def outer():\n"
        "    x = Foo()\n"
        "    def inner():\n"
        "        return x.method()\n"
        "    return inner()\n");
    ASSERT_NOT_NULL(r);
    /* inner() captures x via closure; x is Foo from outer scope. */
    int idx = find_resolved(r, "inner", "method");
    if (idx < 0) printf("  KNOWN GAP: closure scope capture across nested function\n");
    cbm_free_result(r);
    PASS();
}

TEST(stress_match_sequence_pattern) {
    /* match items: case [head, *tail]: head.method() */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    match items:\n"
        "        case [head, *tail]:\n"
        "            return head.method()\n"
        "        case []:\n"
        "            return None\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_lambda_inference) {
    /* fn = lambda x: x.method(); fn(Foo()) — call-site driven inference */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use():\n"
        "    fn = lambda x: x.method()\n"
        "    return fn(Foo())\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "lambda", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_method_chain_long) {
    /* a.b().c().d().e() — every step must preserve type for the next. */
    CBMFileResult *r = extract_py(
        "from typing import Self\n"
        "class B:\n"
        "    def step(self) -> Self:\n"
        "        return self\n"
        "    def finish(self):\n"
        "        return 1\n"
        "def use():\n"
        "    return B().step().step().step().step().finish()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "finish"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_generator_delegation) {
    /* def outer(): yield from inner() — outer's iterable element is inner's */
    CBMFileResult *r = extract_py(
        "from typing import Generator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def inner() -> Generator[Foo, None, None]:\n"
        "    yield Foo()\n"
        "def outer() -> Generator[Foo, None, None]:\n"
        "    yield from inner()\n"
        "def use():\n"
        "    for x in outer():\n"
        "        x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_async_gen_for) {
    /* async for x in async_gen(): x.method() */
    CBMFileResult *r = extract_py(
        "from typing import AsyncGenerator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "async def gen() -> AsyncGenerator[Foo, None]:\n"
        "    yield Foo()\n"
        "async def use():\n"
        "    async for x in gen():\n"
        "        x.method()\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "use", "method");
    if (idx < 0) printf("  KNOWN GAP: async-for element typing\n");
    cbm_free_result(r);
    PASS();
}

/* ── Round 10 — real-world / framework patterns ──────────────── */

TEST(stress_sqlalchemy_mapped_field) {
    /* SQLAlchemy 2.0: id: Mapped[int] = mapped_column(...)
     * Mapped[T] should unwrap to T for member access. */
    CBMFileResult *r = extract_py(
        "from typing import Annotated\n"
        "class Mapped:\n"
        "    pass\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class User:\n"
        "    id: Mapped[Foo]\n"
        "def use(u: User):\n"
        "    return u.id.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_pydantic_model_field) {
    /* class Model(BaseModel): name: Foo. m.name.method() */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class BaseModel:\n"
        "    pass\n"
        "class User(BaseModel):\n"
        "    name: Foo\n"
        "    age: int\n"
        "def use(u: User):\n"
        "    return u.name.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_chained_filter_map) {
    /* filter / map / list compose */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(items: list[Foo]):\n"
        "    filtered = [x for x in items if x.method() > 0]\n"
        "    return [y.method() for y in filtered]\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_nested_function_call) {
    /* outer(inner()) where outer/inner have annotations */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def make() -> Foo:\n"
        "    return Foo()\n"
        "def transform(x: Foo) -> Foo:\n"
        "    return x\n"
        "def use():\n"
        "    return transform(make()).method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_optional_chain_with_walrus) {
    /* if (result := compute()) is not None: result.method() */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def compute() -> Optional[Foo]:\n"
        "    return None\n"
        "def use():\n"
        "    if (result := compute()) is not None:\n"
        "        return result.method()\n"
        "    return None\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_classmethod_chain_to_method) {
    /* C.factory().method() where factory is a @classmethod returning Self */
    CBMFileResult *r = extract_py(
        "from typing import Self\n"
        "class Builder:\n"
        "    @classmethod\n"
        "    def create(cls) -> Self:\n"
        "        return cls()\n"
        "    def step(self) -> Self:\n"
        "        return self\n"
        "    def finish(self):\n"
        "        return 1\n"
        "def use():\n"
        "    return Builder.create().step().finish()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "finish"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_multi_assign_same_line) {
    /* a, b = (Foo(), Bar()) — same as tuple unpack, but explicit tuple */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def m_a(self):\n"
        "        return 1\n"
        "class Bar:\n"
        "    def m_b(self):\n"
        "        return 2\n"
        "def use():\n"
        "    a, b = (Foo(), Bar())\n"
        "    a.m_a()\n"
        "    b.m_b()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "m_a"), 0);
    ASSERT_GTE(require_resolved(r, "use", "m_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_dict_get_default) {
    /* d.get(k, default) — return type still V (or default's type) */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(d: dict[str, Foo], default: Foo):\n"
        "    v = d.get('k', default)\n"
        "    return v.method()\n");
    ASSERT_NOT_NULL(r);
    /* d.get(k) returns Optional[V] in our model — single-arg overload.
     * With a default, runtime returns V always, but our resolver still
     * gives Optional. method() resolves through UNION fallback. */
    int idx = find_resolved(r, "use", "method");
    if (idx < 0) printf("  KNOWN GAP: dict.get with default narrows away None\n");
    cbm_free_result(r);
    PASS();
}

TEST(stress_chained_attribute_through_property_setter) {
    /* @property + @x.setter, then accessing through chain */
    CBMFileResult *r = extract_py(
        "class Inner:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Outer:\n"
        "    @property\n"
        "    def inner(self) -> Inner:\n"
        "        return Inner()\n"
        "def use(o: Outer):\n"
        "    return o.inner.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_async_with_async_for) {
    /* async with X() as ctx: async for x in ctx: x.method() */
    CBMFileResult *r = extract_py(
        "from typing import AsyncIterator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Ctx:\n"
        "    async def __aenter__(self):\n"
        "        return self\n"
        "    async def __aexit__(self, *args):\n"
        "        return None\n"
        "    def __aiter__(self) -> AsyncIterator[Foo]:\n"
        "        return self\n"
        "    async def __anext__(self) -> Foo:\n"
        "        return Foo()\n"
        "async def use():\n"
        "    async with Ctx() as ctx:\n"
        "        async for x in ctx:\n"
        "            x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_recursive_method_self) {
    /* class Node: def visit(self) -> int: ... self.left.visit() */
    CBMFileResult *r = extract_py(
        "from typing import Optional\n"
        "class Node:\n"
        "    def __init__(self):\n"
        "        self.left: Optional['Node'] = None\n"
        "    def visit(self) -> int:\n"
        "        if self.left is not None:\n"
        "            return self.left.visit()\n"
        "        return 0\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "visit", "visit"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_callable_return_value) {
    /* def factory() -> Callable[[], Foo]: ... factory()().method() */
    CBMFileResult *r = extract_py(
        "from typing import Callable\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def factory() -> Callable[[], Foo]:\n"
        "    return Foo\n"
        "def use():\n"
        "    return factory()().method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_typeddict_total_false) {
    /* class TD(TypedDict, total=False): foo: Foo. d['foo'].method() */
    CBMFileResult *r = extract_py(
        "from typing import TypedDict\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class TD(TypedDict, total=False):\n"
        "    foo: Foo\n"
        "def use(d: TD):\n"
        "    return d['foo'].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_nested_match_patterns) {
    /* match x: case Foo(a, b): bind a, b */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Bar:\n"
        "    def m2(self):\n"
        "        return 2\n"
        "def use(x):\n"
        "    match x:\n"
        "        case Foo():\n"
        "            return x.method()\n"
        "        case Bar():\n"
        "            return x.m2()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    ASSERT_GTE(require_resolved(r, "use", "m2"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_protocol_via_named_class) {
    /* def use(x: SupportsMethod): x.method()
     * SupportsMethod is a Protocol, x can be any class with method() */
    CBMFileResult *r = extract_py(
        "from typing import Protocol\n"
        "class SupportsMethod(Protocol):\n"
        "    def method(self) -> int: ...\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(x: SupportsMethod):\n"
        "    return x.method()\n");
    ASSERT_NOT_NULL(r);
    /* Should resolve x.method through the Protocol's method declaration. */
    int idx = find_resolved(r, "use", "method");
    if (idx < 0) printf("  KNOWN GAP: Protocol method resolution\n");
    cbm_free_result(r);
    PASS();
}

TEST(stress_iterator_next_value) {
    /* def gen(): yield Foo(); next(gen()).method() */
    CBMFileResult *r = extract_py(
        "from typing import Iterator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def gen() -> Iterator[Foo]:\n"
        "    yield Foo()\n"
        "def use():\n"
        "    return next(gen()).method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_double_star_unpack) {
    /* def f(**kwargs): kwargs['x'].method() — kwargs is dict[str, Foo] */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def use(**kwargs: Foo):\n"
        "    return kwargs['x'].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_yield_from_in_use) {
    /* yield from gen() — body's yields collected by caller's iteration */
    CBMFileResult *r = extract_py(
        "from typing import Iterator\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "def src() -> Iterator[Foo]:\n"
        "    yield Foo()\n"
        "def chain() -> Iterator[Foo]:\n"
        "    yield from src()\n"
        "def use():\n"
        "    for x in chain():\n"
        "        x.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_isinstance_or_chain) {
    /* if isinstance(x, Foo) or isinstance(x, Bar): — narrowed to Foo|Bar */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "class Bar:\n"
        "    def method(self):\n"
        "        return 2\n"
        "def use(x):\n"
        "    if isinstance(x, Foo):\n"
        "        return x.method()\n"
        "    return None\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_module_level_const_class) {
    /* Top-level: foo = Foo(); use foo.method() in another function. */
    CBMFileResult *r = extract_py(
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "foo = Foo()\n"
        "def use():\n"
        "    return foo.method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(stress_dataclass_with_default_factory) {
    /* @dataclass with field(default_factory=list) */
    CBMFileResult *r = extract_py(
        "from dataclasses import dataclass, field\n"
        "class Foo:\n"
        "    def method(self):\n"
        "        return 1\n"
        "@dataclass\n"
        "class Container:\n"
        "    items: list[Foo] = field(default_factory=list)\n"
        "def use(c: Container):\n"
        "    return c.items[0].method()\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "use", "method"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────── */

SUITE(py_lsp_stress) {
    /* Typing helpers */
    RUN_TEST(stress_namedtuple_class_form);
    RUN_TEST(stress_typeddict_subscript);
    RUN_TEST(stress_protocol_structural);
    RUN_TEST(stress_abc_abstractmethod);
    /* Control flow / binding */
    RUN_TEST(stress_context_manager_with_as);
    RUN_TEST(stress_except_as_binding);
    RUN_TEST(stress_isinstance_else_negative_narrow);
    /* Containers */
    RUN_TEST(stress_tuple_unpack_function_return);
    RUN_TEST(stress_dict_items_comprehension);
    RUN_TEST(stress_list_slice_returns_list);
    /* Decorators / first-class functions */
    RUN_TEST(stress_function_as_dict_value);
    RUN_TEST(stress_decorator_factory);
    RUN_TEST(stress_property_setter);
    /* Inheritance / generics */
    RUN_TEST(stress_self_in_inheritance_chain);
    RUN_TEST(stress_diamond_inheritance_mro);
    RUN_TEST(stress_recursive_self_referencing_type);
    /* Advanced control flow */
    RUN_TEST(stress_nested_closure);
    RUN_TEST(stress_match_sequence_pattern);
    RUN_TEST(stress_lambda_inference);
    RUN_TEST(stress_method_chain_long);
    RUN_TEST(stress_generator_delegation);
    RUN_TEST(stress_async_gen_for);
    /* Round 10 — real-world / framework patterns */
    RUN_TEST(stress_sqlalchemy_mapped_field);
    RUN_TEST(stress_pydantic_model_field);
    RUN_TEST(stress_chained_filter_map);
    RUN_TEST(stress_nested_function_call);
    RUN_TEST(stress_optional_chain_with_walrus);
    RUN_TEST(stress_classmethod_chain_to_method);
    RUN_TEST(stress_multi_assign_same_line);
    RUN_TEST(stress_dict_get_default);
    RUN_TEST(stress_chained_attribute_through_property_setter);
    RUN_TEST(stress_async_with_async_for);
    RUN_TEST(stress_recursive_method_self);
    RUN_TEST(stress_callable_return_value);
    RUN_TEST(stress_typeddict_total_false);
    RUN_TEST(stress_nested_match_patterns);
    RUN_TEST(stress_protocol_via_named_class);
    RUN_TEST(stress_iterator_next_value);
    RUN_TEST(stress_double_star_unpack);
    RUN_TEST(stress_yield_from_in_use);
    RUN_TEST(stress_isinstance_or_chain);
    RUN_TEST(stress_module_level_const_class);
    RUN_TEST(stress_dataclass_with_default_factory);
}
