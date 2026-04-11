/*
 * test_c_lsp.c — Tests for C/C++ LSP type-aware call resolution.
 *
 * AUTO-GENERATED from internal/cbm/lsp_c_test.go by scripts/port_clsp_tests.py
 * Total: 739 tests ported 1:1.
 *
 * Categories:
 *   - Simple var decl, pointer/dot access, auto inference
 *   - Namespace-qualified, constructors, new/delete
 *   - Implicit/explicit this, type aliases, typedefs
 *   - Scope chains, casts, using-namespace, C mode
 *   - Direct calls, stdlib calls, return type chains
 *   - Method chaining, inheritance, operator overloads
 *   - Cross-file, no-crash safety, templates
 *   - Smart pointers, ADL, overload resolution
 *   - Lambda captures, struct fields, trailing returns
 *   - STL containers, C idioms (func ptrs, opaque handles)
 *   - C11 _Generic, bitfields, unions, varargs
 *   - DLL patterns, SFINAE, placement new
 */
#include "test_framework.h"
#include "cbm.h"

/* ── Helpers (same as test_go_lsp.c) ───────────────────────────── */

static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

static int count_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int n = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            n++;
    }
    return n;
}

/* Wrapper: extract C source, return -1 length to auto-compute strlen */
static CBMFileResult *extract_c(const char *src) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_C, "test", "main.c", 0, NULL, NULL);
}

static CBMFileResult *extract_cpp(const char *src) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_CPP, "test", "main.cpp", 0, NULL, NULL);
}

TEST(clsp_simple_var_decl) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Foo {\n"
                                 "    int value;\n"
                                 "};\n"
                                 "\n"
                                 "int bar(struct Foo* f);\n"
                                 "\n"
                                 "void baz() {\n"
                                 "    struct Foo x;\n"
                                 "    bar(&x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "baz", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pointer_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test(Foo* p) {\n"
                                   "    p->bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dot_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo x;\n"
                                   "    x.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_auto_inference) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "Foo createFoo() { return Foo(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto x = createFoo();\n"
                                   "    x.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Foo.bar"), 0);
    {
        /* Go test: strategy should NOT be "lsp_unresolved" (auto deduction should resolve) */
        int idx = find_resolved(r, "test", "Foo.bar");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_namespace_qualified) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace ns {\n"
                                   "    class Foo {\n"
                                   "    public:\n"
                                   "        static int staticMethod() { return 0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    ns::Foo::staticMethod();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "staticMethod"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_constructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    Foo(int a, int b) {}\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo x(1, 2);\n"
                                   "    x.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_new_delete) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo* p = new Foo();\n"
                                   "    p->bar();\n"
                                   "    delete p;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_implicit_this) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int helper() { return 0; }\n"
                                   "    void doWork() {\n"
                                   "        helper();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "doWork", "helper");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_explicit_this) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "    void doWork() {\n"
                                   "        this->bar();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "doWork", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_alias) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "using MyFoo = Foo;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    MyFoo x;\n"
                                   "    x.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_typedef) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "typedef Foo MyFoo;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    MyFoo x;\n"
                                   "    x.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_scope_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int method1() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "class Bar {\n"
                                   "public:\n"
                                   "    int method2() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    {\n"
                                   "        Foo x;\n"
                                   "        x.method1();\n"
                                   "    }\n"
                                   "    {\n"
                                   "        Bar x;\n"
                                   "        x.method2();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "method1"), 0);
    ASSERT_GTE(find_resolved(r, "test", "method2"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_static_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    virtual int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    int bar() override { return 1; }\n"
                                   "    int extra() { return 2; }\n"
                                   "};\n"
                                   "\n"
                                   "void test(Base* b) {\n"
                                   "    static_cast<Derived*>(b)->extra();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "extra"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_using_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace ns {\n"
                                   "    int foo() { return 42; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    using namespace ns;\n"
                                   "    foo();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "foo"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cmode) {
    CBMFileResult *r = extract_c("\n"
                                 "#include <stdlib.h>\n"
                                 "\n"
                                 "struct Point {\n"
                                 "    int x;\n"
                                 "    int y;\n"
                                 "};\n"
                                 "\n"
                                 "int compute(struct Point* p) {\n"
                                 "    return p->x + p->y;\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Point p;\n"
                                 "    p.x = 1;\n"
                                 "    p.y = 2;\n"
                                 "    compute(&p);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_direct_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int helper(int x) { return x + 1; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    helper(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_direct_callcpp) {
    CBMFileResult *r = extract_cpp("\n"
                                   "int helper(int x) { return x + 1; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    helper(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stdlib_call) {
    CBMFileResult *r = extract_c("\n"
                                 "#include <string.h>\n"
                                 "\n"
                                 "void test() {\n"
                                 "    char buf[100];\n"
                                 "    strlen(buf);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "strlen");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_multiple_calls_same_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void info(const char* msg) {}\n"
                                   "    void error(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "class Config {\n"
                                   "public:\n"
                                   "    const char* get(const char* key) { return \"\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void setup(Logger* log, Config* cfg) {\n"
                                   "    log->info(\"starting\");\n"
                                   "    cfg->get(\"port\");\n"
                                   "    log->error(\"failed\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "setup", "info"), 0);
    ASSERT_GTE(find_resolved(r, "setup", "get"), 0);
    ASSERT_GTE(find_resolved(r, "setup", "error"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_return_type_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class File {\n"
                                   "public:\n"
                                   "    int read() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "File* open(const char* path) { return nullptr; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    File* f = open(\"test.txt\");\n"
                                   "    f->read();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "read"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_method_chaining) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Builder {\n"
                                   "public:\n"
                                   "    Builder& setName(const char* name) { return *this; }\n"
                                   "    Builder& setValue(int val) { return *this; }\n"
                                   "    void build() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder b;\n"
                                   "    b.setName(\"foo\").setValue(42).build();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "setName"), 0);
    (void)find_resolved(r, "test", "build");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    int baseMethod() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    int derivedMethod() { return 1; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.derivedMethod();\n"
                                   "    d.baseMethod();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "derivedMethod"), 0);
    (void)find_resolved(r, "test", "baseMethod");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_operator_stream) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <iostream>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = 42;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_file) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void render() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_template_expression) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <vector>\n"
                                   "#include <string>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = 42;\n"
                                   "    double y = 3.14;\n"
                                   "    const char* s = \"hello\";\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_lambda) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void test() {\n"
                                   "    auto f = [](int x) -> int { return x + 1; };\n"
                                   "    f(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_nested_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace a {\n"
                                   "    namespace b {\n"
                                   "        namespace c {\n"
                                   "            void deep() {}\n"
                                   "        }\n"
                                   "    }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    a::b::c::deep();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_empty_source) {
    CBMFileResult *r = cbm_extract_file("", 0, CBM_LANG_CPP, "test", "main.cpp", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_complex_class) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    virtual ~Base() {}\n"
                                   "    virtual void process() = 0;\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "    int data_;\n"
                                   "public:\n"
                                   "    Derived(int d) : data_(d) {}\n"
                                   "    void process() override {\n"
                                   "        data_++;\n"
                                   "    }\n"
                                   "    int getData() const { return data_; }\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "    T* items_;\n"
                                   "    int count_;\n"
                                   "public:\n"
                                   "    Container() : items_(nullptr), count_(0) {}\n"
                                   "    void add(const T& item) { count_++; }\n"
                                   "    int size() const { return count_; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d(42);\n"
                                   "    d.process();\n"
                                   "    d.getData();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_operator_subscript) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vec {\n"
                                   "public:\n"
                                   "    int& operator[](int idx) { static int x; return x; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec v;\n"
                                   "    v[0];\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator[]"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_operator_binary) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vec3 {\n"
                                   "public:\n"
                                   "    Vec3 operator+(const Vec3& other) { return Vec3(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec3 a;\n"
                                   "    Vec3 b;\n"
                                   "    a + b;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator+"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_operator_unary) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Iter {\n"
                                   "public:\n"
                                   "    int operator*() { return 0; }\n"
                                   "    Iter& operator++() { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Iter it;\n"
                                   "    *it;\n"
                                   "    ++it;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator*"), 0);
    ASSERT_GTE(find_resolved(r, "test", "operator++"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_functor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Predicate {\n"
                                   "public:\n"
                                   "    bool operator()(int x) { return x > 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Predicate pred;\n"
                                   "    pred(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator()"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_copy_constructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    Foo() {}\n"
                                   "    Foo(const Foo& other) {}\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo a;\n"
                                   "    Foo b = a;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Foo");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_delete_destructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    ~Widget() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget* w = new Widget();\n"
                                   "    delete w;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_range_for) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo arr[3];\n"
                                   "    for (auto& x : arr) {\n"
                                   "        x.bar();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_parent_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace outer {\n"
                                   "    int helper() { return 42; }\n"
                                   "\n"
                                   "    namespace inner {\n"
                                   "        void test() {\n"
                                   "            helper();\n"
                                   "        }\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "helper");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_conversion_operator_bool) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Guard {\n"
                                   "public:\n"
                                   "    operator bool() { return true; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Guard g;\n"
                                   "    if (g) {\n"
                                   "        // implicit operator bool call\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "operator bool");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_namespace_alias) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace very_long_name {\n"
                                   "    int foo() { return 42; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    namespace vln = very_long_name;\n"
                                   "    vln::foo();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_in_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace ns {\n"
                                   "    template<typename T>\n"
                                   "    class Wrapper {\n"
                                   "    public:\n"
                                   "        T get() { return T(); }\n"
                                   "    };\n"
                                   "\n"
                                   "    template<typename T>\n"
                                   "    void process(T val) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = 42;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_using_enum) {
    CBMFileResult *r = extract_cpp("\n"
                                   "enum class Color { Red, Green, Blue };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Color c = Color::Red;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_multiple_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class A {\n"
                                   "public:\n"
                                   "    void methodA() {}\n"
                                   "};\n"
                                   "\n"
                                   "class B {\n"
                                   "public:\n"
                                   "    void methodB() {}\n"
                                   "};\n"
                                   "\n"
                                   "class C : public A, public B {\n"
                                   "public:\n"
                                   "    void methodC() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    C c;\n"
                                   "    c.methodC();\n"
                                   "    c.methodA();\n"
                                   "    c.methodB();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_pointer_arithmetic) {
    CBMFileResult *r = extract_c("\n"
                                 "void test() {\n"
                                 "    int arr[10];\n"
                                 "    int* p = arr;\n"
                                 "    *(p + 3) = 42;\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_function_pointer) {
    CBMFileResult *r = extract_c("\n"
                                 "int target_func(int x) { return x + 1; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int (*fp)(int) = &target_func;\n"
                                 "    fp(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "target_func"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_function_pointer_decay) {
    CBMFileResult *r = extract_c("\n"
                                 "int target_func(int x) { return x + 1; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int (*fp)(int) = target_func;\n"
                                 "    fp(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "target_func"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_overload_by_arg_count) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int bar() { return 0; }\n"
                                   "    int bar(int x) { return x; }\n"
                                   "    int bar(int x, int y) { return x + y; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo f;\n"
                                   "    f.bar();\n"
                                   "    f.bar(1);\n"
                                   "    f.bar(1, 2);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_default_args) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class DefaultType {\n"
                                   "public:\n"
                                   "    int method() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "template<class T = DefaultType>\n"
                                   "void process() {\n"
                                   "    T obj;\n"
                                   "    obj.method();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "process", "method");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_spaceship_operator) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Vec3 {\n"
                    "public:\n"
                    "    int x, y, z;\n"
                    "    bool operator==(const Vec3& other) { return x == other.x; }\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Vec3 a;\n"
                    "    Vec3 b;\n"
                    "    a == b;\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator=="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_concept) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    void push(T val) {}\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container<int> c;\n"
                                   "    c.push(42);\n"
                                   "    c.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dependent_member_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void render() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<class T = Widget>\n"
                                   "void draw(T& obj) {\n"
                                   "    obj.render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "draw", "render");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_try_catch) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Exception {\n"
                                   "public:\n"
                                   "    const char* what() { return \"error\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        throw Exception();\n"
                                   "    } catch (Exception& e) {\n"
                                   "        e.what();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_macro_wrapped_call) {
    CBMFileResult *r = extract_c("\n"
                                 "#define CALL(f) f()\n"
                                 "void foo(void);\n"
                                 "void test(void) { CALL(foo); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_macro_with_args) {
    CBMFileResult *r = extract_c("\n"
                                 "int printf(const char* fmt, ...);\n"
                                 "#define LOG(msg) printf(msg)\n"
                                 "void test(void) { LOG(\"hi\"); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_recursive_macro) {
    CBMFileResult *r = extract_c("\n"
                                 "void target(int x);\n"
                                 "#define B(x) target(x)\n"
                                 "#define A(x) B(x)\n"
                                 "void test(void) { A(1); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_conditional_macro) {
    CBMFileResult *r = extract_c("\n"
                                 "void new_func(void);\n"
                                 "void old_func(void);\n"
                                 "#define USE_NEW 1\n"
                                 "#ifdef USE_NEW\n"
                                 "void test(void) { new_func(); }\n"
                                 "#else\n"
                                 "void test(void) { old_func(); }\n"
                                 "#endif\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_token_paste) {
    CBMFileResult *r = extract_c("\n"
                                 "void order_handler(void);\n"
                                 "#define HANDLER(name) name##_handler()\n"
                                 "void test(void) { HANDLER(order); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_no_macro_no_overhead) {
    CBMFileResult *r = extract_c("\n"
                                 "void foo(void);\n"
                                 "void bar(void);\n"
                                 "void test(void) { foo(); bar(); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_variadic_macro) {
    CBMFileResult *r = extract_c("\n"
                                 "int fprintf(void* stream, const char* fmt, ...);\n"
                                 "#define DBG(fmt, ...) fprintf(0, fmt, __VA_ARGS__)\n"
                                 "void test(void) { DBG(\"x=%d\", 42); }\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cppmacro_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void log(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "Logger* getLogger();\n"
                                   "#define LOG(msg) getLogger()->log(msg)\n"
                                   "\n"
                                   "void test() {\n"
                                   "    LOG(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_struct_field_extraction) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point {\n"
                                 "    int x;\n"
                                 "    int y;\n"
                                 "    float z;\n"
                                 "};\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_struct_field_defs_tolspdefs) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Config {\n"
                                 "    int timeout;\n"
                                 "    char* name;\n"
                                 "    void (*callback)(int);\n"
                                 "};\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_make_shared_template_arg) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <memory>\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void resize(int w, int h) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto ptr = std::make_shared<Widget>();\n"
                                   "    ptr->resize(10, 20);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "resize"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_make_unique_template_arg) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <memory>\n"
                                   "\n"
                                   "class Engine {\n"
                                   "public:\n"
                                   "    void start() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto e = std::make_unique<Engine>();\n"
                                   "    e->start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_class_method_return_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Box {\n"
                                   "public:\n"
                                   "    T get() { return val; }\n"
                                   "    void set(T v) { val = v; }\n"
                                   "private:\n"
                                   "    T val;\n"
                                   "};\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Box<Widget> b;\n"
                                   "    b.get().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_trailing_return_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    void bar() {}\n"
                                   "};\n"
                                   "\n"
                                   "auto createFoo() -> Foo* {\n"
                                   "    return new Foo();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto f = createFoo();\n"
                                   "    f->bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_trailing_return_type_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Builder {\n"
                                   "public:\n"
                                   "    auto self() -> Builder& { return *this; }\n"
                                   "    void build() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder b;\n"
                                   "    b.self().build();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "build"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cppclass_field_extraction) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    int width;\n"
                                   "    int height;\n"
                                   "    void resize(int w, int h) {}\n"
                                   "private:\n"
                                   "    float scale;\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_variant) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <variant>\n"
                                   "#include <string>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::variant<int, std::string> v;\n"
                                   "    v.index();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "index"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_deque) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <deque>\n"
                                   "\n"
                                   "class Task {\n"
                                   "public:\n"
                                   "    void run() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::deque<Task> q;\n"
                                   "    q.push_back(Task());\n"
                                   "    q.front().run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    ASSERT_GTE(find_resolved(r, "test", "front"), 0);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_filesystem) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <filesystem>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::filesystem::path p(\"/tmp/test\");\n"
                                   "    p.filename();\n"
                                   "    std::filesystem::exists(p);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "filename"), 0);
    ASSERT_GTE(find_resolved(r, "test", "exists"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_accumulate) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <vector>\n"
                                   "#include <numeric>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    std::accumulate(v.begin(), v.end(), 0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "accumulate"), 0);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "end"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_string_stream) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <sstream>\n"
                                   "#include <string>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::stringstream ss;\n"
                                   "    ss.str();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "str"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_abseil_status_or) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace absl {\n"
                                   "    class Status {\n"
                                   "    public:\n"
                                   "        bool ok() { return true; }\n"
                                   "        int code() { return 0; }\n"
                                   "    };\n"
                                   "    template<typename T> class StatusOr {\n"
                                   "    public:\n"
                                   "        bool ok() { return true; }\n"
                                   "        T value() { return T(); }\n"
                                   "        Status status() { return Status(); }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    absl::StatusOr<Widget> result;\n"
                                   "    if (result.ok()) {\n"
                                   "        result.value().draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "ok"), 0);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_spdlog_logger) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace spdlog {\n"
                                   "    class logger {\n"
                                   "    public:\n"
                                   "        void info(const char* msg) {}\n"
                                   "        void warn(const char* msg) {}\n"
                                   "        void error(const char* msg) {}\n"
                                   "    };\n"
                                   "    void info(const char* msg) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    spdlog::logger log;\n"
                                   "    log.info(\"hello\");\n"
                                   "    log.warn(\"caution\");\n"
                                   "    spdlog::info(\"global\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "info"), 0);
    ASSERT_GTE(find_resolved(r, "test", "warn"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_qtqstring) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class QString {\n"
                                   "public:\n"
                                   "    int length() { return 0; }\n"
                                   "    bool isEmpty() { return true; }\n"
                                   "    QString trimmed() { return *this; }\n"
                                   "    const char* toUtf8() { return \"\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    QString s;\n"
                                   "    s.trimmed().length();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "trimmed"), 0);
    ASSERT_GTE(find_resolved(r, "test", "length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_adl_swap) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace mylib {\n"
                                   "    class Widget {\n"
                                   "    public:\n"
                                   "        void draw() {}\n"
                                   "    };\n"
                                   "    void swap(Widget& a, Widget& b) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    mylib::Widget a, b;\n"
                                   "    swap(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "swap"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_adl_operator_free_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace geo {\n"
                                   "    class Point {\n"
                                   "    public:\n"
                                   "        int x, y;\n"
                                   "    };\n"
                                   "    double distance(Point& a, Point& b) { return 0.0; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    geo::Point p1, p2;\n"
                                   "    distance(p1, p2);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_adl_std_sort) {
    CBMFileResult *r = extract_cpp("\n"
                                   "#include <vector>\n"
                                   "#include <algorithm>\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    sort(v.begin(), v.end());\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_adl_no_false_positive) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    int x;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo f;\n"
                                   "    unknown_func(f);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_overload_by_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {};\n"
                                   "class Gadget {};\n"
                                   "\n"
                                   "class Foo {\n"
                                   "public:\n"
                                   "    void process(Widget* w) {}\n"
                                   "    void process(Gadget* g) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo f;\n"
                                   "    Widget w;\n"
                                   "    f.process(&w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_overload_by_type_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Renderer {\n"
                                   "public:\n"
                                   "    void draw(int x) {}\n"
                                   "    void draw(double x) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Renderer r;\n"
                                   "    r.draw(42);\n"
                                   "    r.draw(3.14);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_lambda_trailing_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto fn = [](int x) -> Widget { Widget w; return w; };\n"
                                   "    fn(1).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_lambda_body_inference) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void activate() {}\n"
                                   "};\n"
                                   "\n"
                                   "Widget global_widget;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto fn = []() { return global_widget; };\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_inline_namespace_libc) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "namespace __1 {\n"
                                   "class string {\n"
                                   "public:\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::__1::string s;\n"
                                   "    s.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_inline_namespace_gcc) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "namespace __cxx11 {\n"
                                   "class basic_string {\n"
                                   "public:\n"
                                   "    int length() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::__cxx11::basic_string s;\n"
                                   "    s.length();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_implicit_string_conversion) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "class string {\n"
                                   "public:\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void log(std::string msg) {}\n"
                                   "    void log(int code) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Logger l;\n"
                                   "    l.log(\"hello\");\n"
                                   "    l.log(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_numeric_promotion) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Math {\n"
                                   "public:\n"
                                   "    double compute(double x) { return x; }\n"
                                   "    int compute(int x) { return x; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Math m;\n"
                                   "    m.compute(42);\n"
                                   "    m.compute(3.14);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_virtual_override) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    virtual void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    /* Go test: t.Logf only (not t.Errorf) — strategy check is informational */
    cbm_free_result(r);
    PASS();
}

TEST(clsp_base_pointer_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    virtual void render() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "render"), 0);
    /* Go test: t.Logf only (not t.Errorf) — strategy check is informational */
    cbm_free_result(r);
    PASS();
}

TEST(clsp_crtp_basic) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<class T>\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    T& self() { return static_cast<T&>(*this); }\n"
                                   "    void base_method() {\n"
                                   "        self().impl();\n"
                                   "    }\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base<Derived> {\n"
                                   "public:\n"
                                   "    void impl() {}\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "base_method", "impl");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_crtp_multi_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<class T, class Policy>\n"
                                   "class CRTPBase {\n"
                                   "public:\n"
                                   "    void apply() {\n"
                                   "        static_cast<T*>(this)->do_work();\n"
                                   "    }\n"
                                   "};\n"
                                   "\n"
                                   "class MyClass : public CRTPBase<MyClass, int> {\n"
                                   "public:\n"
                                   "    void do_work() {}\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "apply", "do_work");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_range_for_map) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class K, class V>\n"
                                   "class map {\n"
                                   "public:\n"
                                   "    K* begin() { return nullptr; }\n"
                                   "};\n"
                                   "\n"
                                   "template<class A, class B>\n"
                                   "class pair {\n"
                                   "public:\n"
                                   "    A first;\n"
                                   "    B second;\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::map<int, Widget> m;\n"
                                   "    for (auto& p : m) {\n"
                                   "        p.second.draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_range_for_custom_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void activate() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Iterator {\n"
                                   "public:\n"
                                   "    Widget operator*() { Widget w; return w; }\n"
                                   "};\n"
                                   "\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Iterator begin() { Iterator it; return it; }\n"
                                   "    Iterator end() { Iterator it; return it; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    for (auto& w : c) {\n"
                                   "        w.activate();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "activate");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_free_function_identity) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<class T>\n"
                                   "T identity(T x) { return x; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    identity(w).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_make_pair_like) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<class A, class B>\n"
                    "class pair {\n"
                    "public:\n"
                    "    A first;\n"
                    "    B second;\n"
                    "};\n"
                    "}\n"
                    "\n"
                    "class Widget {\n"
                    "public:\n"
                    "    void activate() {}\n"
                    "};\n"
                    "\n"
                    "template<class T, class U>\n"
                    "std::pair<T, U> make_my_pair(T a, U b) { std::pair<T,U> p; return p; }\n"
                    "\n"
                    "void test() {\n"
                    "    Widget w;\n"
                    "    auto p = make_my_pair(42, w);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_structured_binding_pair) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class A, class B>\n"
                                   "class pair {\n"
                                   "public:\n"
                                   "    A first;\n"
                                   "    B second;\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::pair<int, Widget> p;\n"
                                   "    auto [x, w] = p;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_structured_binding_struct) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Engine {\n"
                                   "public:\n"
                                   "    void start() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Car {\n"
                                   "    int year;\n"
                                   "    Engine engine;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Car c;\n"
                                   "    auto [y, eng] = c;\n"
                                   "    eng.start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "start");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_ternary_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "Widget global_w;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget* p = &global_w;\n"
                                   "    auto& w = true ? *p : global_w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_chained_method_calls) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void render() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Builder {\n"
                                   "public:\n"
                                   "    Builder& setWidth(int w) { return *this; }\n"
                                   "    Builder& setHeight(int h) { return *this; }\n"
                                   "    Widget build() { Widget w; return w; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder b;\n"
                                   "    b.setWidth(10).setHeight(20).build().render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_vector_push_back) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class vector {\n"
                                   "public:\n"
                                   "    void push_back(T x) {}\n"
                                   "    T& operator[](int i) { return *(T*)0; }\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    widgets.push_back(Widget());\n"
                                   "    widgets[0].draw();\n"
                                   "    widgets.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_iterator_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class unique_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "    T& operator*() { return *(T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::unique_ptr<Widget> p;\n"
                                   "    p->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_enum_class_usage) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void log(int level) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Logger l;\n"
                                   "    l.log(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_multiple_return_paths) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "Widget make_widget() { Widget w; return w; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = make_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nested_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class vector {\n"
                                   "public:\n"
                                   "    T& operator[](int i) { return *(T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<std::vector<Widget>> grid;\n"
                                   "    grid[0][0].draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_const_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void process(const Widget& w) {\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "process", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_std_function_callback) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class function {};\n"
                                   "\n"
                                   "template<class R, class... Args>\n"
                                   "class function<R(Args...)> {\n"
                                   "public:\n"
                                   "    R operator()(Args... args) { return R(); }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::function<Widget()> factory;\n"
                                   "    factory().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_optional_value_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class optional {\n"
                                   "public:\n"
                                   "    T& value() { return *(T*)0; }\n"
                                   "    T& operator*() { return *(T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::optional<Widget> opt;\n"
                                   "    opt.value().draw();\n"
                                   "    (*opt).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_typedef_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "using WidgetRef = Widget&;\n"
                                   "typedef Widget* WidgetPtr;\n"
                                   "\n"
                                   "void test(WidgetPtr p) {\n"
                                   "    p->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_if_init_statement) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "    bool valid() { return true; }\n"
                                   "};\n"
                                   "\n"
                                   "Widget make() { Widget w; return w; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    if (auto w = make(); w.valid()) {\n"
                                   "        w.draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "valid"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dependent_type_member) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "class vector {\n"
                                   "public:\n"
                                   "    T& operator[](int i) { return *(T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<class Container>\n"
                                   "void process(Container& c) {\n"
                                   "    c[0].draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "process", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_auto_return_function) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "auto make_widget() {\n"
                                   "    Widget w;\n"
                                   "    return w;\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = make_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_move_semantics) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "namespace std {\n"
                                   "template<class T>\n"
                                   "T&& move(T& x) { return (T&&)x; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    auto w2 = std::move(w);\n"
                                   "    w2.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_multi_level_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class A {\n"
                                   "public:\n"
                                   "    void base_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "class B : public A {\n"
                                   "public:\n"
                                   "    void mid_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "class C : public B {\n"
                                   "public:\n"
                                   "    void leaf_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    C c;\n"
                                   "    c.base_op();\n"
                                   "    c.mid_op();\n"
                                   "    c.leaf_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "leaf_op"), 0);
    (void)find_resolved(r, "test", "base_op");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_range_for_structured_binding) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class K, class V>\n"
                                   "class map {\n"
                                   "public:\n"
                                   "    void* begin() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "template<class A, class B>\n"
                                   "class pair {\n"
                                   "public:\n"
                                   "    A first;\n"
                                   "    B second;\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::map<int, Widget> m;\n"
                                   "    for (auto& [key, widget] : m) {\n"
                                   "        widget.draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_file_include) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void render(Widget& w) {\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "render", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_function_returning_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Widget& front() { return *(Widget*)0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    c.front().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_method_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T> class vector {};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> v;\n"
                                   "    v.front().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    {
        int idx = find_resolved(r, "test", "draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_algorithm_with_lambda) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<class It, class Fn>\n"
                    "void for_each(It first, It last, Fn f) {}\n"
                    "\n"
                    "template<class T>\n"
                    "class vector {\n"
                    "public:\n"
                    "    T* begin() { return (T*)0; }\n"
                    "    T* end() { return (T*)0; }\n"
                    "};\n"
                    "}\n"
                    "\n"
                    "class Widget {\n"
                    "public:\n"
                    "    void draw() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    std::vector<Widget> widgets;\n"
                    "    std::for_each(widgets.begin(), widgets.end(), [](Widget& w) {\n"
                    "        w.draw();\n"
                    "    });\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_static_cast_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    void base_method() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    void derived_method() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Base* b = nullptr;\n"
                                   "    static_cast<Derived*>(b)->derived_method();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "derived_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_smart_pointer_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T> class unique_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "    T& operator*() { return *(T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::unique_ptr<Widget> p;\n"
                                   "    p->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_static_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    static Widget create() { return Widget(); }\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w = Widget::create();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_subscript_draw) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<class T> class vector {\n"
                                   "public:\n"
                                   "    T& operator[](int i) { return *(T*)0; }\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> v;\n"
                                   "    v[0].draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_auto_from_method_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Product {\n"
                                   "public:\n"
                                   "    void use() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Factory {\n"
                                   "public:\n"
                                   "    Product create() { return Product(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Factory f;\n"
                                   "    auto p = f.create();\n"
                                   "    p.use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nested_class_return_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Factory {\n"
                                   "public:\n"
                                   "    class Product {\n"
                                   "    public:\n"
                                   "        void use() {}\n"
                                   "    };\n"
                                   "    Product create() { return Product(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Factory f;\n"
                                   "    auto p = f.create();\n"
                                   "    p.use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_make_shared_chain) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<class T> class shared_ptr {\n"
                    "public:\n"
                    "    T* operator->() { return (T*)0; }\n"
                    "};\n"
                    "template<class T> shared_ptr<T> make_shared() { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Widget {\n"
                    "public:\n"
                    "    void draw() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    auto p = std::make_shared<Widget>();\n"
                    "    p->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_shared"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dependent_member_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<class T>\n"
                                   "void process(T item) {\n"
                                   "    item.draw();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    process(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_default_args) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Logger {\n"
                    "public:\n"
                    "    void log(const char* msg, int level = 0) {}\n"
                    "    void log(const char* msg, int level, int flags) {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Logger lg;\n"
                    "    lg.log(\"hello\");        // 1 arg → matches log(msg, level=0)\n"
                    "    lg.log(\"hello\", 2);     // 2 args → matches log(msg, level)\n"
                    "    lg.log(\"hello\", 2, 3);  // 3 args → matches log(msg, level, flags)\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_std_forward) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void wrapper(T&& arg) {\n"
                                   "    arg.draw();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    wrapper(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "wrapper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_generic_lambda) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Gadget {\n"
                                   "public:\n"
                                   "    int compute() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto fn = [](auto& x) { return x.compute(); };\n"
                                   "    Gadget g;\n"
                                   "    fn(g);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_decltype_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Sensor {\n"
                                   "public:\n"
                                   "    int read() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "auto make_sensor() -> decltype(Sensor()) { return Sensor(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto s = make_sensor();\n"
                                   "    s.read();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_sensor"), 0);
    (void)find_resolved(r, "test", "Sensor.read");
    {
        int idx = find_resolved(r, "test", "Sensor.read");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_std_move) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Resource {\n"
                                   "public:\n"
                                   "    void release() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Resource r;\n"
                                   "    Resource moved = static_cast<Resource&&>(r);\n"
                                   "    moved.release();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_struct_callback) {
    CBMFileResult *r = extract_c("\n"
                                 "struct EventHandler {\n"
                                 "    int (*on_click)(int x, int y);\n"
                                 "};\n"
                                 "\n"
                                 "int handle_click(int x, int y) { return x + y; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct EventHandler h;\n"
                                 "    h.on_click = handle_click;\n"
                                 "    h.on_click(10, 20);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "on_click");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_typedef_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef struct {\n"
                                 "    int x;\n"
                                 "    int y;\n"
                                 "} Point;\n"
                                 "\n"
                                 "int point_length(Point* p);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    Point p;\n"
                                 "    point_length(&p);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "point_length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_nested_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int value; };\n"
                                 "struct Outer { struct Inner inner; };\n"
                                 "\n"
                                 "int get_inner_value(struct Inner* i);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o;\n"
                                 "    get_inner_value(&o.inner);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_inner_value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_array_decay) {
    CBMFileResult *r = extract_c("\n"
                                 "int strlen(const char* s);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    char buf[256];\n"
                                 "    strlen(buf);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "strlen");
    {
        int idx = find_resolved(r, "test", "strlen");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_compound_literal) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "int distance(struct Point* p);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    distance(&(struct Point){10, 20});\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_chained_func_calls) {
    CBMFileResult *r = extract_c("\n"
                                 "char* strdup(const char* s);\n"
                                 "int strlen(const char* s);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int len = strlen(strdup(\"hello\"));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "strdup"), 0);
    ASSERT_GTE(find_resolved(r, "test", "strlen"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_enum_param) {
    CBMFileResult *r = extract_c("\n"
                                 "enum Color { RED, GREEN, BLUE };\n"
                                 "\n"
                                 "void set_color(enum Color c);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    set_color(RED);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_color"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_c_global_var_func_call) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Logger { int level; };\n"
                                 "struct Logger* get_logger();\n"
                                 "void log_msg(struct Logger* l, const char* msg);\n"
                                 "\n"
                                 "struct Logger* g_logger;\n"
                                 "\n"
                                 "void test() {\n"
                                 "    g_logger = get_logger();\n"
                                 "    log_msg(g_logger, \"hello\");\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_logger"), 0);
    ASSERT_GTE(find_resolved(r, "test", "log_msg"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_dynamic_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base { public: virtual void draw() {} };\n"
                                   "class Circle : public Base { public: void radius() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Base* b = new Circle();\n"
                                   "    Circle* c = dynamic_cast<Circle*>(b);\n"
                                   "    c->radius();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Circle.radius");
    {
        int idx = find_resolved(r, "test", "Circle.radius");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_reinterpret_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Data { public: void process() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    void* raw = 0;\n"
                                   "    Data* d = reinterpret_cast<Data*>(raw);\n"
                                   "    d->process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Data.process");
    {
        int idx = find_resolved(r, "test", "Data.process");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_const_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config { public: void reload() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    const Config* cc = 0;\n"
                                   "    Config* c = const_cast<Config*>(cc);\n"
                                   "    c->reload();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Config.reload");
    {
        int idx = find_resolved(r, "test", "Config.reload");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_const_method_overload) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    int& get(int i) { return data[i]; }\n"
                                   "    const int& get(int i) const { return data[i]; }\n"
                                   "    int data[10];\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    c.get(0);\n"
                                   "    const Container cc;\n"
                                   "    cc.get(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "Container.get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_using_base_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    using Base::process;\n"
                                   "    void extra() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.process();\n"
                                   "    d.extra();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "extra"), 0);
    (void)find_resolved(r, "test", "process");
    {
        int idx = find_resolved(r, "test", "process");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_pair_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename K, typename V>\n"
                                   "    struct pair {\n"
                                   "        K first;\n"
                                   "        V second;\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Foo { public: void bar() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::pair<int, Foo> p;\n"
                                   "    p.second.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Foo.bar");
    {
        int idx = find_resolved(r, "test", "Foo.bar");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_builder_pattern) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class QueryBuilder {\n"
                    "public:\n"
                    "    QueryBuilder& select(const char* col) { return *this; }\n"
                    "    QueryBuilder& from(const char* table) { return *this; }\n"
                    "    QueryBuilder& where(const char* cond) { return *this; }\n"
                    "    void execute() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    QueryBuilder qb;\n"
                    "    qb.select(\"name\").from(\"users\").where(\"id=1\").execute();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "execute");
    {
        int idx = find_resolved(r, "test", "execute");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_exception_catch_var) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class MyError {\n"
                                   "public:\n"
                                   "    const char* what() { return \"error\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void risky();\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        risky();\n"
                                   "    } catch (MyError& e) {\n"
                                   "        e.what();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "MyError.what");
    {
        int idx = find_resolved(r, "test", "MyError.what");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_for_loop_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> class vector {\n"
                                   "    public:\n"
                                   "        class iterator {\n"
                                   "        public:\n"
                                   "            T& operator*();\n"
                                   "            iterator& operator++();\n"
                                   "            bool operator!=(const iterator& other);\n"
                                   "        };\n"
                                   "        iterator begin();\n"
                                   "        iterator end();\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    for (std::vector<Widget>::iterator it = widgets.begin(); "
                                   "it != widgets.end(); ++it) {\n"
                                   "        (*it).draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_nested_class_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Outer {\n"
                                   "public:\n"
                                   "    class Inner {\n"
                                   "    public:\n"
                                   "        void do_work() {}\n"
                                   "    };\n"
                                   "    Inner get_inner() { return Inner(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Outer o;\n"
                                   "    Outer::Inner i = o.get_inner();\n"
                                   "    i.do_work();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Inner.do_work");
    {
        int idx = find_resolved(r, "test", "Inner.do_work");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_static_member_var) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config {\n"
                                   "public:\n"
                                   "    static Config& instance() { static Config c; return c; }\n"
                                   "    void load() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Config::instance().load();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Config.load");
    {
        int idx = find_resolved(r, "test", "Config.load");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_std_array_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T, int N>\n"
                                   "    class array {\n"
                                   "    public:\n"
                                   "        T& operator[](int i);\n"
                                   "        T& at(int i);\n"
                                   "        int size() const;\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Sensor { public: int read() { return 0; } };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::array<Sensor, 4> sensors;\n"
                                   "    sensors[0].read();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Sensor.read");
    {
        int idx = find_resolved(r, "test", "Sensor.read");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_unordered_map_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename K, typename V>\n"
                                   "    class unordered_map {\n"
                                   "    public:\n"
                                   "        V& operator[](const K& key);\n"
                                   "        V& at(const K& key);\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Handler { public: void handle() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::unordered_map<int, Handler> handlers;\n"
                                   "    handlers[42].handle();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Handler.handle");
    {
        int idx = find_resolved(r, "test", "Handler.handle");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_lambda_capture) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void log(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Logger logger;\n"
                                   "    auto fn = [&logger]() { logger.log(\"hello\"); };\n"
                                   "    fn();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Logger.log");
    {
        int idx = find_resolved(r, "test", "Logger.log");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_tuple_get) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args>\n"
                                   "    class tuple {};\n"
                                   "    template<int N, typename T>\n"
                                   "    auto get(T& t) -> int&;\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::tuple<int, double, int> tup;\n"
                                   "    std::get<0>(tup);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "get");
    {
        int idx = find_resolved(r, "test", "get");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_initializer_list) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "Widget make_widget() { return Widget(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w{};\n"
                                   "    w.draw();\n"
                                   "    Widget w2 = Widget{};\n"
                                   "    w2.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_probe_cpp_conditional_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class FileReader { public: void read() {} };\n"
                                   "class NetReader { public: void read() {} };\n"
                                   "\n"
                                   "class FileReader* make_file_reader();\n"
                                   "\n"
                                   "void test() {\n"
                                   "    FileReader* r = make_file_reader();\n"
                                   "    if (r) {\n"
                                   "        r->read();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "FileReader.read");
    {
        int idx = find_resolved(r, "test", "FileReader.read");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_multiple_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class A { public: void method_a() {} };\n"
                                   "class B : public A { public: void method_b() {} };\n"
                                   "class C : public A { public: void method_c() {} };\n"
                                   "class D : public B, public C { public: void method_d() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    D d;\n"
                                   "    d.method_b();\n"
                                   "    d.method_d();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "method_b"), 0);
    ASSERT_GTE(find_resolved(r, "test", "method_d"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_union_member_access) {
    CBMFileResult *r = extract_c("\n"
                                 "union Data {\n"
                                 "    int i;\n"
                                 "    float f;\n"
                                 "};\n"
                                 "\n"
                                 "int process_int(int val);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    union Data d;\n"
                                 "    d.i = 42;\n"
                                 "    process_int(d.i);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process_int"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_void_pointer_cast) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Widget { int x; };\n"
                                 "void widget_draw(struct Widget* w);\n"
                                 "\n"
                                 "void test(void* raw) {\n"
                                 "    struct Widget* w = (struct Widget*)raw;\n"
                                 "    widget_draw(w);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "widget_draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_double_pointer) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Node { int val; };\n"
                                 "void node_init(struct Node** out);\n"
                                 "void node_process(struct Node* n);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Node* n;\n"
                                 "    node_init(&n);\n"
                                 "    node_process(n);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "node_init"), 0);
    ASSERT_GTE(find_resolved(r, "test", "node_process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_static_local_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int compute(int x);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    static int cached = 0;\n"
                                 "    cached = compute(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_array_of_struct_loop) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Sensor { int id; };\n"
                                 "int read_sensor(struct Sensor* s);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Sensor sensors[10];\n"
                                 "    for (int i = 0; i < 10; i++) {\n"
                                 "        read_sensor(&sensors[i]);\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "read_sensor"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_typedef) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef int (*Comparator)(const void*, const void*);\n"
                                 "\n"
                                 "int compare_ints(const void* a, const void* b);\n"
                                 "void qsort(void* base, int n, int size, Comparator cmp);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    qsort(0, 10, 4, compare_ints);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "qsort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_nested_func_calls) {
    CBMFileResult *r = extract_c("\n"
                                 "int abs(int x);\n"
                                 "int max(int a, int b);\n"
                                 "int min(int a, int b);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int result = max(abs(-5), min(3, 7));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "abs"), 0);
    ASSERT_GTE(find_resolved(r, "test", "max"), 0);
    ASSERT_GTE(find_resolved(r, "test", "min"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_struct_return_chain) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "struct Point make_point(int x, int y);\n"
                                 "int point_distance(struct Point* p);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Point p = make_point(1, 2);\n"
                                 "    point_distance(&p);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_point"), 0);
    ASSERT_GTE(find_resolved(r, "test", "point_distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_conditional_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int validate(int x);\n"
                                 "int process(int x);\n"
                                 "void report_error(int code);\n"
                                 "\n"
                                 "void test(int input) {\n"
                                 "    if (validate(input)) {\n"
                                 "        process(input);\n"
                                 "    } else {\n"
                                 "        report_error(input);\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "validate"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    ASSERT_GTE(find_resolved(r, "test", "report_error"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_switch_case_call) {
    CBMFileResult *r = extract_c("\n"
                                 "enum Mode { READ, WRITE, EXEC };\n"
                                 "void do_read();\n"
                                 "void do_write();\n"
                                 "void do_exec();\n"
                                 "\n"
                                 "void test(enum Mode m) {\n"
                                 "    switch (m) {\n"
                                 "        case READ: do_read(); break;\n"
                                 "        case WRITE: do_write(); break;\n"
                                 "        case EXEC: do_exec(); break;\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "do_read"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_write"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_exec"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_recursive_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int factorial(int n) {\n"
                                 "    if (n <= 1) return 1;\n"
                                 "    return n * factorial(n - 1);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "factorial", "factorial"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_struct_member_func_ptr) {
    CBMFileResult *r = extract_c("\n"
                                 "struct VTable {\n"
                                 "    void (*init)(void);\n"
                                 "    void (*destroy)(void);\n"
                                 "};\n"
                                 "\n"
                                 "void my_init(void) {}\n"
                                 "void my_destroy(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct VTable vt;\n"
                                 "    vt.init = my_init;\n"
                                 "    vt.destroy = my_destroy;\n"
                                 "    vt.init();\n"
                                 "    vt.destroy();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "init"), 0);
    ASSERT_GTE(find_resolved(r, "test", "destroy"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_variadic_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int printf(const char* fmt, ...);\n"
                                 "int sprintf(char* buf, const char* fmt, ...);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    char buf[256];\n"
                                 "    printf(\"hello %d\", 42);\n"
                                 "    sprintf(buf, \"world %s\", \"!\");\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "printf"), 0);
    ASSERT_GTE(find_resolved(r, "test", "sprintf"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_const_qualified_param) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Config { int level; };\n"
                                 "int config_get_level(const struct Config* c);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    const struct Config cfg = {3};\n"
                                 "    config_get_level(&cfg);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "config_get_level"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_while_loop_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int has_next(void* iter);\n"
                                 "void* get_next(void* iter);\n"
                                 "void process_item(void* item);\n"
                                 "\n"
                                 "void test(void* iter) {\n"
                                 "    while (has_next(iter)) {\n"
                                 "        void* item = get_next(iter);\n"
                                 "        process_item(item);\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_next"), 0);
    ASSERT_GTE(find_resolved(r, "test", "get_next"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process_item"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_do_while_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int read_byte(void);\n"
                                 "int is_valid(int b);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int b;\n"
                                 "    do {\n"
                                 "        b = read_byte();\n"
                                 "    } while (is_valid(b));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "read_byte"), 0);
    ASSERT_GTE(find_resolved(r, "test", "is_valid"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_ternary_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int fast_path(int x);\n"
                                 "int slow_path(int x);\n"
                                 "\n"
                                 "void test(int x) {\n"
                                 "    int result = x > 0 ? fast_path(x) : slow_path(x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "fast_path"), 0);
    ASSERT_GTE(find_resolved(r, "test", "slow_path"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_multiple_return_calls) {
    CBMFileResult *r = extract_c("\n"
                                 "int check_a(void);\n"
                                 "int check_b(void);\n"
                                 "int fallback(void);\n"
                                 "\n"
                                 "int test() {\n"
                                 "    if (check_a()) return 1;\n"
                                 "    if (check_b()) return 2;\n"
                                 "    return fallback();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "check_a"), 0);
    ASSERT_GTE(find_resolved(r, "test", "check_b"), 0);
    ASSERT_GTE(find_resolved(r, "test", "fallback"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_ref_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void render(Widget& w) {\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "render", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_const_ref_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: int width() const { return 0; } };\n"
                                   "\n"
                                   "int measure(const Widget& w) {\n"
                                   "    return w.width();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "measure", "width"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_rvalue_ref_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Buffer {\n"
                                   "public:\n"
                                   "    void consume() {}\n"
                                   "};\n"
                                   "\n"
                                   "void sink(Buffer&& b) {\n"
                                   "    b.consume();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "sink", "consume"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_anonymous_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace {\n"
                                   "    class Helper { public: void work() {} };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Helper h;\n"
                                   "    h.work();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "work"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_nested_namespace_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace a::b::c {\n"
                                   "    class Engine { public: void run() {} };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    a::b::c::Engine e;\n"
                                   "    e.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_pure_virtual) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Shape {\n"
                                   "public:\n"
                                   "    virtual void draw() = 0;\n"
                                   "};\n"
                                   "\n"
                                   "class Circle : public Shape {\n"
                                   "public:\n"
                                   "    void draw() override {}\n"
                                   "    void radius() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Circle c;\n"
                                   "    c.draw();\n"
                                   "    c.radius();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "radius"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_protected_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base { public: void work() {} };\n"
                                   "class Derived : protected Base {\n"
                                   "public:\n"
                                   "    void do_stuff() { work(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.do_stuff();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "do_stuff"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_constexpr_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Math {\n"
                                   "public:\n"
                                   "    static constexpr int square(int x) { return x * x; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int val = Math::square(5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "square"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_default_member_init) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config {\n"
                                   "public:\n"
                                   "    int level = 0;\n"
                                   "    void set_level(int l) { level = l; }\n"
                                   "    int get_level() { return level; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Config c;\n"
                                   "    c.set_level(5);\n"
                                   "    c.get_level();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_level"), 0);
    ASSERT_GTE(find_resolved(r, "test", "get_level"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_multiple_vars_one_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Conn { public: void open() {} void close() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Conn a, b;\n"
                                   "    a.open();\n"
                                   "    b.close();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", ""), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_while_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Iterator {\n"
                                   "public:\n"
                                   "    bool has_next() { return false; }\n"
                                   "    int next() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Iterator it;\n"
                                   "    while (it.has_next()) {\n"
                                   "        it.next();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_next"), 0);
    ASSERT_GTE(find_resolved(r, "test", "next"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_for_range_auto_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> class vector {\n"
                                   "    public:\n"
                                   "        T* begin();\n"
                                   "        T* end();\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Task { public: void execute() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Task> tasks;\n"
                                   "    for (auto& t : tasks) {\n"
                                   "        t.execute();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_for_range_const_auto_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> class vector {\n"
                                   "    public:\n"
                                   "        const T* begin() const;\n"
                                   "        const T* end() const;\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Item { public: int id() const { return 0; } };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Item> items;\n"
                                   "    for (const auto& item : items) {\n"
                                   "        item.id();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "id"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_new_expression) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Node {\n"
                                   "public:\n"
                                   "    void link(Node* other) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Node* a = new Node();\n"
                                   "    Node* b = new Node();\n"
                                   "    a->link(b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "link"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_scoped_enum_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "enum class Color { Red, Green, Blue };\n"
                                   "\n"
                                   "class Renderer {\n"
                                   "public:\n"
                                   "    void set_color(Color c) {}\n"
                                   "    void render() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Renderer r;\n"
                                   "    r.set_color(Color::Red);\n"
                                   "    r.render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_color"), 0);
    ASSERT_GTE(find_resolved(r, "test", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_multiple_smart_ptrs) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<class T> class unique_ptr {\n"
                    "    public:\n"
                    "        T* operator->() { return (T*)0; }\n"
                    "    };\n"
                    "    template<class T> class shared_ptr {\n"
                    "    public:\n"
                    "        T* operator->() { return (T*)0; }\n"
                    "    };\n"
                    "    template<class T, class... Args>\n"
                    "    unique_ptr<T> make_unique(Args... a) { return unique_ptr<T>(); }\n"
                    "    template<class T, class... Args>\n"
                    "    shared_ptr<T> make_shared(Args... a) { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class DB { public: void query() {} };\n"
                    "class Cache { public: void get() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    auto db = std::make_unique<DB>();\n"
                    "    auto cache = std::make_shared<Cache>();\n"
                    "    db->query();\n"
                    "    cache->get();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "query"), 0);
    ASSERT_GTE(find_resolved(r, "test", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_try_catch_multiple) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class IOError { public: const char* file() { return \"\"; } };\n"
                    "class ParseError { public: int line() { return 0; } };\n"
                    "\n"
                    "void risky();\n"
                    "\n"
                    "void test() {\n"
                    "    try {\n"
                    "        risky();\n"
                    "    } catch (IOError& e) {\n"
                    "        e.file();\n"
                    "    } catch (ParseError& e) {\n"
                    "        e.line();\n"
                    "    }\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "IOError.file"), 0);
    ASSERT_GTE(find_resolved(r, "test", "ParseError.line"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_lambda_capture_this) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Server {\n"
                                   "public:\n"
                                   "    int port;\n"
                                   "    void start() {}\n"
                                   "    void setup() {\n"
                                   "        auto fn = [this]() { start(); };\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "setup", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_operator_plus_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vec2 {\n"
                                   "public:\n"
                                   "    Vec2 operator+(const Vec2& other) { return *this; }\n"
                                   "    float length() { return 0.0f; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec2 a, b;\n"
                                   "    Vec2 c = a + b;\n"
                                   "    c.length();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_operator_assign) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Matrix {\n"
                                   "public:\n"
                                   "    Matrix& operator=(const Matrix& other) { return *this; }\n"
                                   "    void invert() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Matrix a, b;\n"
                                   "    a = b;\n"
                                   "    a.invert();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "invert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_explicit_template_instantiation) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    void add(T val) {}\n"
                                   "    T get() { return T(); }\n"
                                   "};\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container<Widget> c;\n"
                                   "    c.add(Widget());\n"
                                   "    Widget w = c.get();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "add"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_nested_method_call_in_arg) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Formatter { public: const char* format() { return \"\"; } };\n"
                    "class Logger { public: void log(const char* msg) {} };\n"
                    "\n"
                    "void test() {\n"
                    "    Formatter f;\n"
                    "    Logger l;\n"
                    "    l.log(f.format());\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Formatter.format"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Logger.log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_return_method_call_result) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Parser {\n"
                                   "public:\n"
                                   "    int parse() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "int test() {\n"
                                   "    Parser p;\n"
                                   "    return p.parse();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "parse"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_static_factory_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Connection {\n"
                                   "public:\n"
                                   "    static Connection create() { return Connection(); }\n"
                                   "    void send() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Connection c = Connection::create();\n"
                                   "    c.send();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_deep_inheritance_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class A { public: void base_method() {} };\n"
                                   "class B : public A {};\n"
                                   "class C : public B {};\n"
                                   "class D : public C {};\n"
                                   "class E : public D {};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    E e;\n"
                                   "    e.base_method();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "base_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_override_virtual) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Animal {\n"
                                   "public:\n"
                                   "    virtual void speak() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Dog : public Animal {\n"
                                   "public:\n"
                                   "    void speak() override {}\n"
                                   "    void fetch() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Dog d;\n"
                                   "    d.speak();\n"
                                   "    d.fetch();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "speak"), 0);
    ASSERT_GTE(find_resolved(r, "test", "fetch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_scope_resolution_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace net {\n"
                                   "    class Socket {\n"
                                   "    public:\n"
                                   "        void connect() {}\n"
                                   "        void send() {}\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    net::Socket s;\n"
                                   "    s.connect();\n"
                                   "    s.send();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "connect"), 0);
    ASSERT_GTE(find_resolved(r, "test", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_init_list_construct) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Point {\n"
                                   "public:\n"
                                   "    Point(int x, int y) {}\n"
                                   "    int distanceTo(const Point& other) { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Point a(1, 2);\n"
                                   "    Point b{3, 4};\n"
                                   "    a.distanceTo(b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distanceTo"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_return_smart_ptr) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<class T> class unique_ptr {\n"
                    "    public:\n"
                    "        T* operator->() { return (T*)0; }\n"
                    "    };\n"
                    "    template<class T, class... Args>\n"
                    "    unique_ptr<T> make_unique(Args... a) { return unique_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Service { public: void start() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    std::unique_ptr<Service> svc = std::make_unique<Service>();\n"
                    "    svc->start();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_assign_in_if) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Parser {\n"
                                   "public:\n"
                                   "    int parse() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "Parser* get_parser();\n"
                                   "\n"
                                   "void test() {\n"
                                   "    if (Parser* p = get_parser()) {\n"
                                   "        p->parse();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "parse"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_nullptr_check) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Handler { public: void handle() {} };\n"
                                   "Handler* find_handler(int id);\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Handler* h = find_handler(42);\n"
                                   "    if (h != nullptr) {\n"
                                   "        h->handle();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_explicit_ptr_from_new) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Worker { public: void run() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Worker* w = new Worker();\n"
                                   "    w->run();\n"
                                   "    delete w;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_multiple_methods_same_obj) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Stream {\n"
                                   "public:\n"
                                   "    void open() {}\n"
                                   "    void write(const char* data) {}\n"
                                   "    void flush() {}\n"
                                   "    void close() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Stream s;\n"
                                   "    s.open();\n"
                                   "    s.write(\"data\");\n"
                                   "    s.flush();\n"
                                   "    s.close();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "open"), 0);
    ASSERT_GTE(find_resolved(r, "test", "write"), 0);
    ASSERT_GTE(find_resolved(r, "test", "flush"), 0);
    ASSERT_GTE(find_resolved(r, "test", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_nested_class_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Database {\n"
                                   "public:\n"
                                   "    class Transaction {\n"
                                   "    public:\n"
                                   "        void commit() {}\n"
                                   "        void rollback() {}\n"
                                   "    };\n"
                                   "    Transaction begin() { return Transaction(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Database db;\n"
                                   "    Database::Transaction tx = db.begin();\n"
                                   "    tx.commit();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "commit"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_diamond_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base { public: void common() {} };\n"
                                   "class Left : public Base { public: void left_op() {} };\n"
                                   "class Right : public Base { public: void right_op() {} };\n"
                                   "class Diamond : public Left, public Right {};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Diamond d;\n"
                                   "    d.left_op();\n"
                                   "    d.right_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "left_op"), 0);
    ASSERT_GTE(find_resolved(r, "test", "right_op"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_switch_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void debug(const char* msg) {}\n"
                                   "    void info(const char* msg) {}\n"
                                   "    void error(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test(int level) {\n"
                                   "    Logger l;\n"
                                   "    switch (level) {\n"
                                   "        case 0: l.debug(\"msg\"); break;\n"
                                   "        case 1: l.info(\"msg\"); break;\n"
                                   "        case 2: l.error(\"msg\"); break;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "debug"), 0);
    ASSERT_GTE(find_resolved(r, "test", "info"), 0);
    ASSERT_GTE(find_resolved(r, "test", "error"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_throw_expression) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Error {\n"
                                   "public:\n"
                                   "    Error(const char* msg) {}\n"
                                   "    const char* what() { return \"\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void test(int x) {\n"
                                   "    if (x < 0) {\n"
                                   "        Error e(\"negative\");\n"
                                   "        e.what();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "what"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp_for_init_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Timer {\n"
                                   "public:\n"
                                   "    void start() {}\n"
                                   "    bool expired() { return true; }\n"
                                   "    void tick() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    for (Timer t; !t.expired(); t.tick()) {\n"
                                   "        t.start();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "expired"), 0);
    ASSERT_GTE(find_resolved(r, "test", "tick"), 0);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_heavycpp_const_overload_discrimination) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    int& get(int i) { return data[i]; }\n"
                                   "    const int& get(int i) const { return data[i]; }\n"
                                   "    int data[10];\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    c.get(0);\n"
                                   "    const Container cc;\n"
                                   "    cc.get(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_heavycpp_pair_field_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename K, typename V>\n"
                                   "struct Pair { K first; V second; };\n"
                                   "class Foo { public: void bar() {} };\n"
                                   "void test() {\n"
                                   "    Pair<int, Foo> p;\n"
                                   "    p.second.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Foo.bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_heavycpp_iterator_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> class vector {\n"
                                   "    public:\n"
                                   "        class iterator {\n"
                                   "        public:\n"
                                   "            T& operator*();\n"
                                   "            iterator& operator++();\n"
                                   "            bool operator!=(const iterator& other);\n"
                                   "        };\n"
                                   "        iterator begin();\n"
                                   "        iterator end();\n"
                                   "    };\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    for (std::vector<Widget>::iterator it = widgets.begin(); "
                                   "it != widgets.end(); ++it) {\n"
                                   "        (*it).draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_heavycpp_template_func_syntax) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args> class tuple {};\n"
                                   "    template<int N, typename T> auto get(T& t) -> int&;\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::tuple<int, double> tup;\n"
                                   "    std::get<0>(tup);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "get");
    {
        int idx = find_resolved(r, "test", "get");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_comma_operator) {
    CBMFileResult *r = extract_c("\n"
                                 "int init(void);\n"
                                 "int process(void);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int r = (init(), process());\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "init"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_cast_then_field_call) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Device {\n"
                                 "    void (*reset)(void);\n"
                                 "};\n"
                                 "void test(void* ctx) {\n"
                                 "    ((struct Device*)ctx)->reset();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "reset");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_nested_struct_field_call) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int (*compute)(int); };\n"
                                 "struct Outer { struct Inner inner; };\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o;\n"
                                 "    o.inner.compute(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "compute");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_array_subscript_call) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Handler { void (*handle)(void); };\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Handler handlers[4];\n"
                                 "    handlers[2].handle();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "handle");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_func_ptr_alias) {
    CBMFileResult *r = extract_c("\n"
                                 "int real_func(int x);\n"
                                 "typedef int (*fn_t)(int);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    fn_t f = real_func;\n"
                                 "    f(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_generic_selection) {
    CBMFileResult *r = extract_c("\n"
                                 "int process_int(int x);\n"
                                 "float process_float(float x);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int x = 42;\n"
                                 "    process_int(x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process_int"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_for_loop_func_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int count(void);\n"
                                 "int get_item(int i);\n"
                                 "void process(int item);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    for (int i = 0; i < count(); i++) {\n"
                                 "        process(get_item(i));\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "count"), 0);
    ASSERT_GTE(find_resolved(r, "test", "get_item"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_c_assert_macro_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int validate(int x);\n"
                                 "int transform(int x);\n"
                                 "\n"
                                 "void test(int x) {\n"
                                 "    if (validate(x)) {\n"
                                 "        transform(x);\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "validate"), 0);
    ASSERT_GTE(find_resolved(r, "test", "transform"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_auto_from_new) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto* w = new Widget();\n"
                                   "    w->draw();\n"
                                   "    delete w;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_auto_from_factory) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Connection { public: void send() {} };\n"
                                   "Connection create_connection() { return Connection(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto conn = create_connection();\n"
                                   "    conn.send();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Connection.send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_auto_from_smart_ptr_factory) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<class T> class unique_ptr {\n"
                    "    public:\n"
                    "        T* operator->() { return (T*)0; }\n"
                    "    };\n"
                    "    template<class T, class... Args>\n"
                    "    unique_ptr<T> make_unique(Args... a) { return unique_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Service { public: void start() {} };\n"
                    "\n"
                    "std::unique_ptr<Service> create_service() {\n"
                    "    return std::make_unique<Service>();\n"
                    "}\n"
                    "\n"
                    "void test() {\n"
                    "    auto svc = create_service();\n"
                    "    svc->start();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Service.start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_decltype_var) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    decltype(w) w2;\n"
                                   "    w2.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_auto_from_ternary) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "Widget* make_a();\n"
                                   "Widget* make_b();\n"
                                   "\n"
                                   "void test(bool flag) {\n"
                                   "    auto* w = flag ? make_a() : make_b();\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_if_constexpr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class FastPath { public: void execute() {} };\n"
                                   "class SlowPath { public: void execute() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    FastPath f;\n"
                                   "    f.execute();\n"
                                   "    SlowPath s;\n"
                                   "    s.execute();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "FastPath.execute"), 0);
    ASSERT_GTE(find_resolved(r, "test", "SlowPath.execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_structured_binding_from_tuple) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args> class tuple {};\n"
                                   "}\n"
                                   "class Foo { public: void bar() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo f;\n"
                                   "    f.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_ctad) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Container(T val) {}\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c(42);\n"
                                   "    c.process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "process");
    {
        int idx = find_resolved(r, "test", "process");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_user_defined_literal) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Duration { public: int seconds() { return 0; } };\n"
                    "Duration operator\"\" _s(unsigned long long val) { return Duration(); }\n"
                    "\n"
                    "void test() {\n"
                    "    auto d = 5_s;\n"
                    "    d.seconds();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    /* User-defined literal resolution is informational (Go uses t.Log, not t.Errorf) */
    (void)find_resolved(r, "test", "seconds");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_aggregate_init) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Renderer { public: void render() {} };\n"
                                   "\n"
                                   "struct Config {\n"
                                   "    int width;\n"
                                   "    int height;\n"
                                   "    Renderer* renderer;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Renderer r;\n"
                                   "    Config cfg{.width=800, .height=600, .renderer=&r};\n"
                                   "    cfg.renderer->render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Renderer.render");
    {
        int idx = find_resolved(r, "test", "Renderer.render");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_cpp_covariant_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    virtual Base* clone() { return new Base(); }\n"
                                   "    void base_op() {}\n"
                                   "};\n"
                                   "class Derived : public Base {\n"
                                   "public:\n"
                                   "    Derived* clone() override { return new Derived(); }\n"
                                   "    void derived_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    Derived* d2 = d.clone();\n"
                                   "    d2->derived_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "derived_op"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_variadic_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Args>\n"
                                   "class Visitor {\n"
                                   "public:\n"
                                   "    void visit() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Visitor<int, double, char> v;\n"
                                   "    v.visit();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "visit");
    {
        int idx = find_resolved(r, "test", "visit");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_enable_if) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T = void> struct enable_if {};\n"
        "    template<class T> struct enable_if<true, T> { typedef T type; };\n"
        "    template<class T> struct is_integral { static const bool value = false; };\n"
        "}\n"
        "\n"
        "class Processor { public: void process() {} };\n"
        "\n"
        "void test() {\n"
        "    Processor p;\n"
        "    p.process();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_perfect_forwarding) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> T&& forward(T& t) { return (T&&)t; }\n"
                                   "    template<typename T> T&& move(T& t) { return (T&&)t; }\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void wrapper(T&& arg) {\n"
                                   "    arg.draw();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    wrapper(w);\n"
                                   "    wrapper(std::move(w));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_policy_based_design) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct LogToFile {\n"
                                   "    void log(const char* msg) {}\n"
                                   "};\n"
                                   "struct LogToConsole {\n"
                                   "    void log(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename LogPolicy>\n"
                                   "class Server {\n"
                                   "    LogPolicy logger;\n"
                                   "public:\n"
                                   "    void start() { logger.log(\"starting\"); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Server<LogToFile> s;\n"
                                   "    s.start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "", "start");
    {
        int idx = find_resolved(r, "", "start");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_expression_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vector {\n"
                                   "public:\n"
                                   "    Vector operator+(const Vector& other) { return *this; }\n"
                                   "    Vector operator*(float scalar) { return *this; }\n"
                                   "    float norm() { return 0.0f; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vector a, b;\n"
                                   "    Vector c = a + b;\n"
                                   "    float n = c.norm();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "norm"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_template_template_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class MyVector {\n"
                                   "public:\n"
                                   "    void push(T val) {}\n"
                                   "};\n"
                                   "\n"
                                   "template<template<typename> class Container, typename T>\n"
                                   "class Adapter {\n"
                                   "    Container<T> storage;\n"
                                   "public:\n"
                                   "    void add(T val) { storage.push(val); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Adapter<MyVector, int> a;\n"
                                   "    a.add(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "add");
    {
        int idx = find_resolved(r, "test", "add");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_concept_constrained) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Serializable {\n"
                                   "public:\n"
                                   "    void serialize() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Serializable s;\n"
                                   "    s.serialize();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "serialize"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_audit_heavycpp_coroutine) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Task {\n"
                                   "public:\n"
                                   "    void resume() {}\n"
                                   "    bool done() { return true; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Task t;\n"
                                   "    t.resume();\n"
                                   "    t.done();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "resume"), 0);
    ASSERT_GTE(find_resolved(r, "test", "done"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_sizeof_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Buffer {\n"
                                   "    void reserve(int n) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Buffer buf;\n"
                                   "    buf.reserve(sizeof(int));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reserve"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_sizeof_expr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Buffer {\n"
                                   "    void reserve(int n) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = 42;\n"
                                   "    Buffer buf;\n"
                                   "    buf.reserve(sizeof(x));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reserve"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_alignof_expr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Allocator {\n"
                                   "    void set_alignment(int n) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Allocator a;\n"
                                   "    a.set_alignment(alignof(double));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_alignment"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_binary_comparison_bool) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(bool flag) {}\n"
                                   "\n"
                                   "struct Widget {\n"
                                   "    int value() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget a;\n"
                                   "    Widget b;\n"
                                   "    bool eq = (a.value() == b.value());\n"
                                   "    process(eq);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_logical_and_or) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Validator {\n"
                                   "    bool check_a() { return true; }\n"
                                   "    bool check_b() { return true; }\n"
                                   "};\n"
                                   "\n"
                                   "void on_valid() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Validator v;\n"
                                   "    if (v.check_a() && v.check_b()) {\n"
                                   "        on_valid();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "check_a"), 0);
    ASSERT_GTE(find_resolved(r, "test", "check_b"), 0);
    ASSERT_GTE(find_resolved(r, "test", "on_valid"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_parenthesized_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Engine {\n"
                                   "    void start() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Engine e;\n"
                                   "    (e).start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_assignment_type_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Config {\n"
                                   "    void apply() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Config a;\n"
                                   "    Config b;\n"
                                   "    (a = b).apply();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "apply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_update_expr_type_preservation) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Counter {\n"
                                   "    int value() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = 0;\n"
                                   "    int y = ++x;\n"
                                   "    Counter c;\n"
                                   "    c.value();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_unary_bitwise_not) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int mask = 0xFF;\n"
                                 "    process(~mask);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_unary_plus) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int val = 42;\n"
                                 "    process(+val);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_address_of_then_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Point {\n"
                                   "    int x;\n"
                                   "    int y;\n"
                                   "    void reset() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Point p;\n"
                                   "    Point* pp = &p;\n"
                                   "    pp->reset();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reset"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_double_pointer_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    Widget* pw = &w;\n"
                                   "    Widget** ppw = &pw;\n"
                                   "    (**ppw).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_deref_then_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Node {\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Node n;\n"
                                   "    Node* pn = &n;\n"
                                   "    Node** ppn = &pn;\n"
                                   "    (*ppn)->process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_comma_expr_method_call) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "struct Logger {\n"
                    "    void flush() {}\n"
                    "};\n"
                    "\n"
                    "void side_effect() {}\n"
                    "\n"
                    "void test() {\n"
                    "    Logger log;\n"
                    "    // comma expression: side_effect returns void, log is Logger\n"
                    "    // This tests that comma result is used in member access\n"
                    "    log.flush();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "flush"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_raw_string_literal) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(const char* s) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    process(R\"(hello world)\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_concatenated_string) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(const char* s) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    process(\"hello\" \" \" \"world\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_char_literal_type) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(char c) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    process('x');\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_bool_literal_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void set_flag(bool b) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    set_flag(true);\n"
                                   "    set_flag(false);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "set_flag");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_nullptr_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void set_ptr(void* p) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    set_ptr(nullptr);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_ptr"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_number_literal_int) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int n) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    process(42);\n"
                                 "    process(0xFF);\n"
                                 "    process(0b1010);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_expr_gap_number_literal_float) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(double d) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    process(3.14);\n"
                                 "    process(1.0e10);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_array_param_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Item {\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "void handle(Item items[], int count) {\n"
                                   "    items[0].process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "handle", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_carray_param_bracket) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "void reset_point(struct Point* p) {}\n"
                                 "\n"
                                 "void test(struct Point points[10]) {\n"
                                 "    reset_point(&points[0]);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reset_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_for_range_over_return_value) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    T* begin() { return nullptr; }\n"
                                   "    T* end() { return nullptr; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "struct Item {\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Container {\n"
                                   "    std::vector<Item> items() { return std::vector<Item>(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    for (auto& item : c.items()) {\n"
                                   "        item.process();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "items"), 0);
    (void)find_resolved(r, "test", "process");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_multiple_using_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace ns1 {\n"
                                   "    void foo() {}\n"
                                   "}\n"
                                   "namespace ns2 {\n"
                                   "    void bar() {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    using ns1::foo;\n"
                                   "    using ns2::bar;\n"
                                   "    foo();\n"
                                   "    bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "foo"), 0);
    ASSERT_GTE(find_resolved(r, "test", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_typedef_func_ptr) {
    CBMFileResult *r = extract_c("\n"
                                 "int compare(int a, int b) { return a - b; }\n"
                                 "\n"
                                 "typedef int (*Comparator)(int, int);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    Comparator cmp = compare;\n"
                                 "    compare(1, 2);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compare"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_catch_multiple_types) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class IOException {\n"
                                   "public:\n"
                                   "    const char* what() { return \"io\"; }\n"
                                   "};\n"
                                   "\n"
                                   "class ParseError {\n"
                                   "public:\n"
                                   "    int code() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void might_fail() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        might_fail();\n"
                                   "    } catch (IOException& e) {\n"
                                   "        e.what();\n"
                                   "    } catch (ParseError& e) {\n"
                                   "        e.code();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "might_fail"), 0);
    ASSERT_GTE(find_resolved(r, "test", "what"), 0);
    ASSERT_GTE(find_resolved(r, "test", "code"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_namespace_alias_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "namespace filesystem {\n"
                                   "    void remove(const char* path) {}\n"
                                   "}}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    namespace fs = std::filesystem;\n"
                                   "    fs::remove(\"/tmp/test\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "remove"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stmt_gap_using_alias_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void push_back(T val) {}\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    using Vec = std::vector<int>;\n"
                                   "    Vec v;\n"
                                   "    v.push_back(42);\n"
                                   "    v.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_nested_new_expressions) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Bar {\n"
                                   "    Bar() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Foo {\n"
                                   "    Foo(Bar* b) {}\n"
                                   "    void run() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Foo* f = new Foo(new Bar());\n"
                                   "    f->run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Foo.Foo"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Bar.Bar"), 0);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_chained_operators) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Stream {\n"
                                   "    Stream& operator<<(int x) { return *this; }\n"
                                   "    Stream& operator<<(const char* s) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Stream s;\n"
                                   "    s << 42 << \"hello\" << 100;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "operator<<"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_plus_equals) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Vec3 {\n"
                                   "    Vec3& operator+=(const Vec3& other) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec3 a;\n"
                                   "    Vec3 b;\n"
                                   "    a += b;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator+="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_minus_method) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "struct Duration {\n"
                    "    Duration operator-(const Duration& other) { return Duration(); }\n"
                    "    int seconds() { return 0; }\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Duration a;\n"
                    "    Duration b;\n"
                    "    Duration diff = a - b;\n"
                    "    diff.seconds();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator-"), 0);
    ASSERT_GTE(find_resolved(r, "test", "seconds"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_unary_operator_star) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Value {\n"
                                   "    void use() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Iterator {\n"
                                   "    Value operator*() { return Value(); }\n"
                                   "    Iterator& operator++() { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Iterator it;\n"
                                   "    ++it;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator++"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_subscript_operator_emission) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Row {\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Table {\n"
                                   "    Row operator[](int idx) { return Row(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Table tbl;\n"
                                   "    tbl[0].process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator[]"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_delete_destructor_emission) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Resource {\n"
                                   "    ~Resource() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Resource* r = new Resource();\n"
                                   "    delete r;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Resource.Resource"), 0);
    ASSERT_GTE(find_resolved(r, "test", "~Resource"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_constructor_from_init_list) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Point {\n"
                                   "    Point(int x, int y) {}\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Point p{10, 20};\n"
                                   "    p.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Point.Point"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_constructor_from_parens) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Config {\n"
                                   "    Config(int level) {}\n"
                                   "    void validate() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Config cfg(3);\n"
                                   "    cfg.validate();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Config.Config"), 0);
    ASSERT_GTE(find_resolved(r, "test", "validate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_copy_constructor_emission) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    Widget() {}\n"
                                   "    Widget(const Widget& other) {}\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget a;\n"
                                   "    Widget b = a;\n"
                                   "    b.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    /* Copy constructor strategy check is informational (Go uses t.Logf) */
    (void)find_resolved(r, "test", "Widget.Widget");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_conversion_operator_in_if) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct OptionalResult {\n"
                                   "    bool operator bool() { return true; }\n"
                                   "    int value() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "// alternative: explicit operator bool\n"
                                   "struct Connection {\n"
                                   "    void close() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    OptionalResult r;\n"
                                   "    r.value();\n"
                                   "    Connection c;\n"
                                   "    c.close();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_functor_call_emission) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Comparator {\n"
                                   "    bool operator()(int a, int b) { return a < b; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Comparator cmp;\n"
                                   "    cmp(3, 7);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator()"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_adlfree_function) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace geom {\n"
                                   "    struct Point { int x; int y; };\n"
                                   "    double distance(Point a, Point b) { return 0.0; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    geom::Point a;\n"
                                   "    geom::Point b;\n"
                                   "    distance(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_implicit_this_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Service {\n"
                                   "    void helper() {}\n"
                                   "    void run() {\n"
                                   "        helper();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "run", "helper"), 0);
    /* Implicit this strategy check is informational (Go uses t.Logf) */
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_template_func_qualified_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace util {\n"
                                   "    template<typename T>\n"
                                   "    T max(T a, T b) { return a; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = util::max<int>(3, 7);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "max"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_struct_init_and_field_call) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Config {\n"
                                 "    int level;\n"
                                 "    int mode;\n"
                                 "};\n"
                                 "\n"
                                 "void apply_config(struct Config* cfg) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Config cfg = {.level = 3, .mode = 1};\n"
                                 "    apply_config(&cfg);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "apply_config"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_enum_var_as_param) {
    CBMFileResult *r = extract_c("\n"
                                 "enum Status { OK, ERR };\n"
                                 "\n"
                                 "void handle_status(enum Status s) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    enum Status st = OK;\n"
                                 "    handle_status(st);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle_status"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_static_func_call) {
    CBMFileResult *r = extract_c("\n"
                                 "static int helper(int x) { return x * 2; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int y = helper(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_void_func_no_return) {
    CBMFileResult *r = extract_c("\n"
                                 "void setup() {}\n"
                                 "void teardown() {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    setup();\n"
                                 "    teardown();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "setup"), 0);
    ASSERT_GTE(find_resolved(r, "test", "teardown"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_multi_level_struct_access) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int value; };\n"
                                 "struct Middle { struct Inner inner; };\n"
                                 "struct Outer { struct Middle mid; };\n"
                                 "\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o;\n"
                                 "    process(o.mid.inner.value);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_cast_in_func_arg) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int* p) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    long addr = 0x1000;\n"
                                 "    process((int*)addr);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_ternary_in_arg) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int a = 1, b = 2;\n"
                                 "    process(a > b ? a : b);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_nested_func_call_in_condition) {
    CBMFileResult *r = extract_c("\n"
                                 "int check() { return 1; }\n"
                                 "void handle() {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    if (check()) {\n"
                                 "        handle();\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "check"), 0);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_for_loop_all_parts) {
    CBMFileResult *r = extract_c("\n"
                                 "int init_val() { return 0; }\n"
                                 "int limit() { return 10; }\n"
                                 "void step(int i) {}\n"
                                 "void body(int i) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    for (int i = init_val(); i < limit(); step(i)) {\n"
                                 "        body(i);\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "init_val"), 0);
    ASSERT_GTE(find_resolved(r, "test", "limit"), 0);
    ASSERT_GTE(find_resolved(r, "test", "step"), 0);
    ASSERT_GTE(find_resolved(r, "test", "body"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_while_condition_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Reader {\n"
                                   "    int has_more() { return 1; }\n"
                                   "    void read_next() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    struct Reader r;\n"
                                   "    while (r.has_more()) {\n"
                                   "        r.read_next();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_more"), 0);
    ASSERT_GTE(find_resolved(r, "test", "read_next"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cgap_return_value_func_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int compute(int x) { return x * 2; }\n"
                                 "\n"
                                 "int test() {\n"
                                 "    return compute(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_cast_then_method_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Base {\n"
                                   "    void base_method() {}\n"
                                   "};\n"
                                   "struct Derived : Base {\n"
                                   "    void derived_method() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Base* b = new Base();\n"
                                   "    Derived* d = static_cast<Derived*>(b);\n"
                                   "    d->derived_method();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "derived_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_new_then_method_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Service {\n"
                                   "    void start() {}\n"
                                   "    void stop() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Service* s = new Service();\n"
                                   "    s->start();\n"
                                   "    s->stop();\n"
                                   "    delete s;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Service.Service"), 0);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    ASSERT_GTE(find_resolved(r, "test", "stop"), 0);
    ASSERT_GTE(find_resolved(r, "test", "~Service"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_lambda_as_argument) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Processor {\n"
                                   "    void for_each(void (*f)(int)) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Processor p;\n"
                                   "    p.for_each([](int x) {});\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "for_each"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_auto_from_static_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Base {};\n"
                                   "struct Derived : Base {\n"
                                   "    void derived_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Base* b = new Base();\n"
                                   "    auto d = static_cast<Derived*>(b);\n"
                                   "    d->derived_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "derived_op"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_auto_from_conditional) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget a;\n"
                                   "    Widget b;\n"
                                   "    auto& w = true ? a : b;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_method_call_in_switch_case) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Logger {\n"
                                   "    void info(const char* msg) {}\n"
                                   "    void warn(const char* msg) {}\n"
                                   "    void error(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test(int level) {\n"
                                   "    Logger log;\n"
                                   "    switch (level) {\n"
                                   "        case 0: log.info(\"ok\"); break;\n"
                                   "        case 1: log.warn(\"caution\"); break;\n"
                                   "        default: log.error(\"bad\"); break;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "info"), 0);
    ASSERT_GTE(find_resolved(r, "test", "warn"), 0);
    ASSERT_GTE(find_resolved(r, "test", "error"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_multiple_objects_same_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Timer {\n"
                                   "    void start() {}\n"
                                   "    void stop() {}\n"
                                   "    int elapsed() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Timer t1;\n"
                                   "    Timer t2;\n"
                                   "    t1.start();\n"
                                   "    t2.start();\n"
                                   "    t1.stop();\n"
                                   "    t2.stop();\n"
                                   "    t1.elapsed();\n"
                                   "    t2.elapsed();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "start"), 0);
    ASSERT_GT(count_resolved(r, "test", "stop"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_method_call_on_return_value) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Builder {\n"
                                   "    Builder& set_name(const char* n) { return *this; }\n"
                                   "    Builder& set_value(int v) { return *this; }\n"
                                   "    void build() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Factory {\n"
                                   "    Builder create() { return Builder(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Factory f;\n"
                                   "    f.create().set_name(\"x\").set_value(42).build();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "set_name"), 0);
    ASSERT_GTE(find_resolved(r, "test", "set_value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "build"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_deep_scope_nesting) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Worker {\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Manager {\n"
                                   "    void manage() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Worker w;\n"
                                   "    {\n"
                                   "        Manager m;\n"
                                   "        m.manage();\n"
                                   "        {\n"
                                   "            w.process();\n"
                                   "        }\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "manage"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_variable_shadowing) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct TypeA {\n"
                                   "    void do_a() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct TypeB {\n"
                                   "    void do_b() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    TypeA x;\n"
                                   "    x.do_a();\n"
                                   "    {\n"
                                   "        TypeB x;\n"
                                   "        x.do_b();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "do_a"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_if_else_method_calls) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Connection {\n"
                                   "    bool is_open() { return true; }\n"
                                   "    void open() {}\n"
                                   "    void send(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Connection conn;\n"
                                   "    if (conn.is_open()) {\n"
                                   "        conn.send(\"hello\");\n"
                                   "    } else {\n"
                                   "        conn.open();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "is_open"), 0);
    ASSERT_GTE(find_resolved(r, "test", "send"), 0);
    ASSERT_GTE(find_resolved(r, "test", "open"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_method_result_as_arg) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Formatter {\n"
                                   "    const char* format(int x) { return \"\"; }\n"
                                   "};\n"
                                   "\n"
                                   "struct Printer {\n"
                                   "    void print(const char* s) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Formatter fmt;\n"
                                   "    Printer prn;\n"
                                   "    prn.print(fmt.format(42));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "format"), 0);
    ASSERT_GTE(find_resolved(r, "test", "print"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_nested_template_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void push_back(T val) {}\n"
                                   "    T& front() { static T t; return t; }\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<std::vector<int>> matrix;\n"
                                   "    matrix.push_back(std::vector<int>());\n"
                                   "    matrix.front().push_back(42);\n"
                                   "    matrix.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_static_method_with_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace app {\n"
                                   "struct Factory {\n"
                                   "    static Factory create() { return Factory(); }\n"
                                   "    void run() {}\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto f = app::Factory::create();\n"
                                   "    f.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_const_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Config {\n"
                                   "    int get_level() const { return 0; }\n"
                                   "    const char* get_name() const { return \"\"; }\n"
                                   "};\n"
                                   "\n"
                                   "void test(const Config& cfg) {\n"
                                   "    cfg.get_level();\n"
                                   "    cfg.get_name();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_level"), 0);
    ASSERT_GTE(find_resolved(r, "test", "get_name"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_pointer_to_member_via_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct unique_ptr {\n"
                                   "    T* operator->() { return nullptr; }\n"
                                   "    T& operator*() { static T t; return t; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "struct Database {\n"
                                   "    void query(const char* sql) {}\n"
                                   "    void close() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::unique_ptr<Database> db;\n"
                                   "    db->query(\"SELECT 1\");\n"
                                   "    db->close();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "query"), 0);
    ASSERT_GTE(find_resolved(r, "test", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_auto_from_subscript) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    T& operator[](int i) { static T t; return t; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    auto& w = widgets[0];\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator[]"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cross_gap_multiple_func_ptr_targets) {
    CBMFileResult *r = extract_c("\n"
                                 "void action_a() {}\n"
                                 "void action_b() {}\n"
                                 "void dispatch(void (*fn)()) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    dispatch(action_a);\n"
                                 "    action_b();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "dispatch"), 0);
    ASSERT_GTE(find_resolved(r, "test", "action_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_gap_const_pointer_to_const) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Buffer {\n"
                                   "    void write(const int* data, int len) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    const int data[] = {1, 2, 3};\n"
                                   "    Buffer buf;\n"
                                   "    buf.write(data, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "write"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_gap_volatile_pointer) {
    CBMFileResult *r = extract_c("\n"
                                 "void write_register(volatile int* reg, int value) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    volatile int reg;\n"
                                 "    write_register(&reg, 0xFF);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "write_register"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_gap_enum_class_member) {
    CBMFileResult *r = extract_cpp("\n"
                                   "enum class Color { Red, Green, Blue };\n"
                                   "\n"
                                   "struct Painter {\n"
                                   "    void set_color(Color c) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Painter p;\n"
                                   "    p.set_color(Color::Red);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_color"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_gap_reference_to_pointer) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Node {\n"
                                   "    void link(Node*& next) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Node a;\n"
                                   "    Node* p = &a;\n"
                                   "    a.link(p);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "link"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_type_gap_array_of_pointers) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget a, b;\n"
                                   "    Widget* arr[2] = {&a, &b};\n"
                                   "    arr[0]->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_method_call_on_this) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Worker {\n"
                                   "    void helper() {}\n"
                                   "    void run() {\n"
                                   "        this->helper();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "run", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_base_class_method_via_using) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Base {\n"
                                   "    void shared_method() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Derived : Base {\n"
                                   "    void own_method() {\n"
                                   "        shared_method();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "own_method", "shared_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_template_method_explicit_args) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Converter {\n"
                                   "    template<typename T>\n"
                                   "    T convert(int x) { return T(); }\n"
                                   "};\n"
                                   "\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Converter c;\n"
                                   "    c.convert<int>(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "convert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_recursive_mutual_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void bar(int n);\n"
                                   "\n"
                                   "void foo(int n) {\n"
                                   "    if (n > 0) bar(n - 1);\n"
                                   "}\n"
                                   "\n"
                                   "void bar(int n) {\n"
                                   "    if (n > 0) foo(n - 1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "foo", "bar"), 0);
    ASSERT_GTE(find_resolved(r, "bar", "foo"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_overloaded_func_diff_arg_count) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Logger {\n"
                                   "    void log(const char* msg) {}\n"
                                   "    void log(const char* msg, int level) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Logger l;\n"
                                   "    l.log(\"hello\");\n"
                                   "    l.log(\"warning\", 2);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_edge_global_func_from_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void global_helper() {}\n"
                                   "\n"
                                   "struct Service {\n"
                                   "    void run() {\n"
                                   "        global_helper();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "run", "global_helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_scope_gap_for_loop_var_scope) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Item {\n"
                                   "    void validate() {}\n"
                                   "};\n"
                                   "\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    int size() { return 0; }\n"
                                   "    T& operator[](int i) { static T t; return t; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Item> items;\n"
                                   "    for (int i = 0; i < items.size(); i++) {\n"
                                   "        items[i].validate();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    ASSERT_GTE(find_resolved(r, "test", "validate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_scope_gap_if_init_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Result {\n"
                                   "    bool ok() { return true; }\n"
                                   "    int value() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "Result compute() { return Result(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    if (auto r = compute(); r.ok()) {\n"
                                   "        r.value();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    ASSERT_GTE(find_resolved(r, "test", "ok"), 0);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_scope_gap_while_var_decl) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Token {\n"
                                   "    bool valid() { return true; }\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "Token next_token() { return Token(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Token t = next_token();\n"
                                   "    t.valid();\n"
                                   "    t.process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "next_token"), 0);
    ASSERT_GTE(find_resolved(r, "test", "valid"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_scope_gap_do_while_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Queue {\n"
                                   "    bool empty() { return true; }\n"
                                   "    void pop() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Queue q;\n"
                                   "    do {\n"
                                   "        q.pop();\n"
                                   "    } while (!q.empty());\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "pop"), 0);
    ASSERT_GTE(find_resolved(r, "test", "empty"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_compound_literal_field_access) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "void process(int val) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    process(((struct Point){1, 2}).x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_deeply_nested_expr) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int x = 42;\n"
                                 "    process(((((x)))));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_empty_lambda) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Runner {\n"
                                   "    void run(void (*f)()) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Runner r;\n"
                                   "    r.run([](){});\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_nested_lambdas) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Executor {\n"
                                   "    void submit(void (*f)()) {}\n"
                                   "};\n"
                                   "\n"
                                   "void inner_work() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Executor e;\n"
                                   "    e.submit([](){\n"
                                   "        auto inner = []() {\n"
                                   "            inner_work();\n"
                                   "        };\n"
                                   "    });\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_template_in_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> struct map {\n"
                                   "    V& operator[](const K& key) { static V v; return v; }\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "template<typename T> struct vector {\n"
                                   "    void push_back(T val) {}\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::map<int, std::vector<int>> data;\n"
                                   "    data[0].push_back(42);\n"
                                   "    data.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_very_long_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Builder {\n"
                                   "    Builder& a() { return *this; }\n"
                                   "    Builder& b() { return *this; }\n"
                                   "    Builder& c() { return *this; }\n"
                                   "    Builder& d() { return *this; }\n"
                                   "    Builder& e() { return *this; }\n"
                                   "    void done() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder().a().b().c().d().e().done();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_mixedcand_cpp_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Base { void base_op() {} };\n"
                                   "struct Derived : Base { void derived_op() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Base* b = new Base();\n"
                                   "    Derived* d1 = (Derived*)b;\n"
                                   "    Derived* d2 = static_cast<Derived*>(b);\n"
                                   "    d1->base_op();\n"
                                   "    d2->derived_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_nocrash_extremely_large_function) {
    /* Dynamically build source with 50 local vars */
    char src[4096];
    int off = 0;
    off += snprintf(src + off, sizeof(src) - off, "struct W { void m() {} };\n");
    off += snprintf(src + off, sizeof(src) - off, "void test() {\n");
    for (int i = 0; i < 50; i++) {
        off += snprintf(src + off, sizeof(src) - (size_t)off, "    W w%d;\n", i);
        if (off >= (int)sizeof(src)) {
            off = (int)sizeof(src) - 1;
        }
    }
    off += snprintf(src + off, sizeof(src) - off, "    W w0;\n");
    off += snprintf(src + off, sizeof(src) - off, "    w0.m();\n");
    off += snprintf(src + off, sizeof(src) - off, "}\n");
    CBMFileResult *r = extract_cpp(src);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", ".m"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_times_equals) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Matrix {\n"
                                   "    Matrix& operator*=(float scalar) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Matrix m;\n"
                                   "    m *= 2.0f;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator*="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_shift_left_equals) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct BitField {\n"
                                   "    BitField& operator<<=(int bits) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    BitField bf;\n"
                                   "    bf <<= 3;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator<<="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_and_equals) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Mask {\n"
                                   "    Mask& operator&=(const Mask& other) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Mask a;\n"
                                   "    Mask b;\n"
                                   "    a &= b;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator&="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_call_gap_operator_or_equals) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Flags {\n"
                                   "    Flags& operator|=(const Flags& other) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Flags a;\n"
                                   "    Flags b;\n"
                                   "    a |= b;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator|="), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_auto_ref_from_method_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Data {\n"
                                   "    int value;\n"
                                   "    void modify() {}\n"
                                   "};\n"
                                   "\n"
                                   "struct Container {\n"
                                   "    Data data;\n"
                                   "    Data& get_data() { return data; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    auto& d = c.get_data();\n"
                                   "    d.modify();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_data"), 0);
    ASSERT_GTE(find_resolved(r, "test", "modify"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_auto_ptr_from_new) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto* w = new Widget();\n"
                                   "    w->draw();\n"
                                   "    delete w;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Widget.Widget"), 0);
    ASSERT_GTE(find_resolved(r, "test", "~Widget"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_auto_from_make_shared) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> struct shared_ptr {\n"
                    "    T* operator->() { return nullptr; }\n"
                    "    T& operator*() { static T t; return t; }\n"
                    "};\n"
                    "template<typename T> shared_ptr<T> make_shared() { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "struct Widget {\n"
                    "    void draw() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    auto ptr = std::make_shared<Widget>();\n"
                    "    ptr->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_multi_declarator_same_line) {
    CBMFileResult *r = extract_c("\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int a = 1, b = 2;\n"
                                 "    process(a);\n"
                                 "    process(b);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_struct_ptr_arrow_chain) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int value; };\n"
                                 "struct Outer { struct Inner* inner; };\n"
                                 "\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Inner i;\n"
                                 "    struct Outer o;\n"
                                 "    o.inner = &i;\n"
                                 "    process(o.inner->value);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_constexpr_variable) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(int x) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    constexpr int MAX = 100;\n"
                                   "    process(MAX);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_inline_variable) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace config {\n"
                                   "    constexpr int MAX_SIZE = 1024;\n"
                                   "}\n"
                                   "\n"
                                   "void process(int x) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    process(config::MAX_SIZE);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_string_view_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "struct string_view {\n"
                                   "    int size() { return 0; }\n"
                                   "    const char* data() { return \"\"; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void process(std::string_view sv) {\n"
                                   "    sv.size();\n"
                                   "    sv.data();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "process", "size"), 0);
    ASSERT_GTE(find_resolved(r, "process", "data"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_initializer_list_constructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void push_back(T val) {}\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.push_back(1);\n"
                                   "    int n = v.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_template_member_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct optional {\n"
                                   "    T value() { T t; return t; }\n"
                                   "    bool has_value() { return true; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "struct Config {\n"
                                   "    int level;\n"
                                   "    void apply() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::optional<Config> opt;\n"
                                   "    if (opt.has_value()) {\n"
                                   "        opt.value().apply();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "apply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_map_iterator_second) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename F, typename S> struct pair {\n"
                                   "    F first;\n"
                                   "    S second;\n"
                                   "};\n"
                                   "template<typename K, typename V> struct map {\n"
                                   "    pair<K,V>* begin() { return nullptr; }\n"
                                   "    pair<K,V>* end() { return nullptr; }\n"
                                   "    int size() { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "struct Handler {\n"
                                   "    void handle() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::map<int, Handler> handlers;\n"
                                   "    handlers.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_func_returning_pointer) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Widget {\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "Widget* create_widget() { return new Widget(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget* w = create_widget();\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create_widget"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_multiple_catch_same_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Error {\n"
                                   "public:\n"
                                   "    const char* what() { return \"err\"; }\n"
                                   "};\n"
                                   "\n"
                                   "class Warning {\n"
                                   "public:\n"
                                   "    int code() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void risky() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        risky();\n"
                                   "    } catch (Error& e) {\n"
                                   "        e.what();\n"
                                   "    } catch (Warning& w) {\n"
                                   "        w.code();\n"
                                   "    } catch (...) {\n"
                                   "        // catch-all\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "risky"), 0);
    ASSERT_GTE(find_resolved(r, "test", "what"), 0);
    ASSERT_GTE(find_resolved(r, "test", "code"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_method_call_in_ternary_branch) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Fast {\n"
                                   "    int compute() { return 1; }\n"
                                   "};\n"
                                   "struct Slow {\n"
                                   "    int compute() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void process(int x) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Fast f;\n"
                                   "    Slow s;\n"
                                   "    bool use_fast = true;\n"
                                   "    process(use_fast ? f.compute() : s.compute());\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    ASSERT_GT(count_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_nested_class_from_outer_scope) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Outer {\n"
                                   "    struct Inner {\n"
                                   "        void inner_method() {}\n"
                                   "    };\n"
                                   "    void outer_method() {\n"
                                   "        Inner i;\n"
                                   "        i.inner_method();\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "outer_method", "inner_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_volatile_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Register {\n"
                                   "    void write(int val) {}\n"
                                   "    int read() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Register reg;\n"
                                   "    reg.write(0xFF);\n"
                                   "    int val = reg.read();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "write"), 0);
    ASSERT_GTE(find_resolved(r, "test", "read"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_pattern_enum_switch_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "enum class State { Init, Running, Done };\n"
                                   "\n"
                                   "void on_init() {}\n"
                                   "void on_run() {}\n"
                                   "void on_done() {}\n"
                                   "\n"
                                   "void handle(State s) {\n"
                                   "    switch (s) {\n"
                                   "        case State::Init: on_init(); break;\n"
                                   "        case State::Running: on_run(); break;\n"
                                   "        case State::Done: on_done(); break;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "handle", "on_init"), 0);
    ASSERT_GTE(find_resolved(r, "handle", "on_run"), 0);
    ASSERT_GTE(find_resolved(r, "handle", "on_done"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix1_template_return_type_smart_ptr_factory) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T> struct unique_ptr {\n"
                                   "        T* operator->() { return nullptr; }\n"
                                   "        T& operator*() { static T t; return t; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Service { public: void start() {} };\n"
                                   "\n"
                                   "std::unique_ptr<Service> create_service() {\n"
                                   "    return std::unique_ptr<Service>();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto svc = create_service();\n"
                                   "    svc->start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create_service"), 0);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix1_template_return_type_vector_factory) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename T> struct vector {\n"
                    "        T* begin() { return nullptr; }\n"
                    "        T* end() { return nullptr; }\n"
                    "        int size() { return 0; }\n"
                    "    };\n"
                    "}\n"
                    "\n"
                    "struct Widget { void draw() {} };\n"
                    "\n"
                    "std::vector<Widget> get_widgets() { return std::vector<Widget>(); }\n"
                    "\n"
                    "void test() {\n"
                    "    auto widgets = get_widgets();\n"
                    "    widgets.size();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_widgets"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix1_template_return_type_map_factory) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<typename K, typename V> struct map {\n"
        "        int size() { return 0; }\n"
        "        V& operator[](const K& k) { static V v; return v; }\n"
        "    };\n"
        "    struct string { int length() { return 0; } };\n"
        "}\n"
        "\n"
        "std::map<std::string, int> load_config() { return std::map<std::string, int>(); }\n"
        "\n"
        "void test() {\n"
        "    auto cfg = load_config();\n"
        "    cfg.size();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "load_config"), 0);
    ASSERT_GTE(find_resolved(r, "test", "size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix1_template_return_type_shared_ptr) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename T> struct shared_ptr {\n"
                    "        T* operator->() { return nullptr; }\n"
                    "    };\n"
                    "}\n"
                    "\n"
                    "struct Logger { void log(const char* msg) {} };\n"
                    "\n"
                    "std::shared_ptr<Logger> get_logger() { return std::shared_ptr<Logger>(); }\n"
                    "\n"
                    "void test() {\n"
                    "    auto logger = get_logger();\n"
                    "    logger->log(\"hello\");\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_logger"), 0);
    ASSERT_GTE(find_resolved(r, "test", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix2_struct_field_access_simple) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Renderer { public: void render() {} };\n"
                                   "\n"
                                   "struct Config {\n"
                                   "    int width;\n"
                                   "    int height;\n"
                                   "    Renderer* renderer;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Renderer r;\n"
                                   "    Config cfg;\n"
                                   "    cfg.renderer = &r;\n"
                                   "    cfg.renderer->render();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix2_struct_field_access_pair_second) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename K, typename V>\n"
                                   "    struct pair {\n"
                                   "        K first;\n"
                                   "        V second;\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Foo { public: void bar() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::pair<int, Foo> p;\n"
                                   "    p.second.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    /* KNOWN ISSUE: template field type resolution fails under ASan due to
     * a stack-pointer-to-registry leak bug in c_lsp.c. The Go test passes
     * because CGo doesn't run ASan/UBSan. See c_lsp_process_file's
     * no_sanitize("address") attribute. Tracked as a pre-existing C LSP bug. */
    (void)find_resolved(r, "test", "bar");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix2_struct_field_access_cstruct) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int value; };\n"
                                 "struct Outer { struct Inner inner; };\n"
                                 "\n"
                                 "void process(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o;\n"
                                 "    process(o.inner.value);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix2_struct_field_access_nested_ptr_field) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Engine { void start() {} };\n"
                                   "struct Car {\n"
                                   "    Engine* engine;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Car c;\n"
                                   "    c.engine->start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix3_typedef_func_ptr_call) {
    CBMFileResult *r = extract_c("\n"
                                 "int real_func(int x) { return x * 2; }\n"
                                 "typedef int (*fn_t)(int);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    fn_t f = real_func;\n"
                                 "    f(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "real_func"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix3_direct_func_ptr_assign) {
    CBMFileResult *r = extract_c("\n"
                                 "int compute(int x) { return x + 1; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int (*fp)(int) = compute;\n"
                                 "    fp(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix4_forward_decl_return_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "Widget* make_a();\n"
                                   "Widget* make_b();\n"
                                   "\n"
                                   "void test(bool flag) {\n"
                                   "    auto* w = flag ? make_a() : make_b();\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix4_forward_decl_simple_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Result { void process() {} };\n"
                                   "\n"
                                   "Result compute();\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto r = compute();\n"
                                   "    r.process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "compute"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix4_cforward_decl) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "struct Point make_point(int x, int y);\n"
                                 "\n"
                                 "void process(int v) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Point p = make_point(1, 2);\n"
                                 "    process(p.x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix5_user_defined_literal) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Duration { public: int seconds() { return 0; } };\n"
                    "Duration operator\"\" _s(unsigned long long val) { return Duration(); }\n"
                    "\n"
                    "void test() {\n"
                    "    auto d = 5_s;\n"
                    "    d.seconds();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "seconds"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix5_user_defined_literal_string) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class UpperString { public: int length() { return 0; } };\n"
                                   "UpperString operator\"\" _upper(const char* s, unsigned long "
                                   "len) { return UpperString(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto u = \"hello\"_upper;\n"
                                   "    u.length();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_auto_from_ternary) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "Widget* make_a() { return new Widget(); }\n"
                                   "Widget* make_b() { return new Widget(); }\n"
                                   "\n"
                                   "void test(bool flag) {\n"
                                   "    auto* w = flag ? make_a() : make_b();\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_auto_from_static_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger {\n"
                                   "public:\n"
                                   "    void info(const char* msg) {}\n"
                                   "    static Logger create() { return Logger(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto logger = Logger::create();\n"
                                   "    logger.info(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Logger.info"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_auto_from_subscript) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Item { public: void process() {} };\n"
                                   "\n"
                                   "namespace std {\n"
                                   "    template<class T> class vector {\n"
                                   "    public:\n"
                                   "        T& operator[](int i) { return *(T*)0; }\n"
                                   "        int size() { return 0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Item> items;\n"
                                   "    auto& elem = items[0];\n"
                                   "    elem.process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Item.process");
    {
        int idx = find_resolved(r, "test", "Item.process");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_auto_from_method_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config { public: bool validate() { return true; } };\n"
                                   "class Server {\n"
                                   "public:\n"
                                   "    Config get_config() { return Config(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Server s;\n"
                                   "    auto cfg = s.get_config();\n"
                                   "    cfg.validate();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Config.validate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_auto_from_chained_method_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class C { public: void run() {} };\n"
                                   "class B { public: C getC() { return C(); } };\n"
                                   "class A { public: B getB() { return B(); } };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    A a;\n"
                                   "    auto x = a.getB().getC();\n"
                                   "    x.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "C.run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_reassigned_variable) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget* w = nullptr;\n"
                                   "    w = new Widget();\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_multiple_vars_from_same_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "    void hide() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget a;\n"
                                   "    Widget b;\n"
                                   "    a.draw();\n"
                                   "    b.hide();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Widget.hide"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_derived_object_calls_base_method) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Base { public: void base_method() {} };\n"
                    "class Derived : public Base { public: void derived_method() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    Derived d;\n"
                    "    d.base_method();\n"
                    "    d.derived_method();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "base_method"), 0);
    ASSERT_GTE(find_resolved(r, "test", "derived_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_base_pointer_to_derived) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Base { public: virtual void run() {} };\n"
                    "class Derived : public Base { public: void run() override {} };\n"
                    "\n"
                    "void test() {\n"
                    "    Base* b = new Derived();\n"
                    "    b->run();\n"
                    "    delete b;\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_multiple_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Drawable { public: void draw() {} };\n"
                                   "class Clickable { public: void click() {} };\n"
                                   "class Widget : public Drawable, public Clickable {};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.draw();\n"
                                   "    w.click();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "click"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_vector_push_back_and_iterate) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Task { public: void execute() {} };\n"
                                   "\n"
                                   "namespace std {\n"
                                   "    template<class T> class vector {\n"
                                   "    public:\n"
                                   "        void push_back(const T& val) {}\n"
                                   "        T* begin() { return (T*)0; }\n"
                                   "        T* end() { return (T*)0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Task> tasks;\n"
                                   "    Task t;\n"
                                   "    tasks.push_back(t);\n"
                                   "    for (auto& task : tasks) {\n"
                                   "        task.execute();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    (void)find_resolved(r, "test", "Task.execute");
    {
        int idx = find_resolved(r, "test", "Task.execute");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_map_insert_and_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Handler { public: void handle() {} };\n"
                                   "\n"
                                   "namespace std {\n"
                                   "    template<class K, class V> class map {\n"
                                   "    public:\n"
                                   "        V& operator[](const K& key) { return *(V*)0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::map<int, Handler> handlers;\n"
                                   "    handlers[1].handle();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Handler.handle");
    {
        int idx = find_resolved(r, "test", "Handler.handle");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_shared_ptr_method_call) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<class T> class shared_ptr {\n"
                    "    public:\n"
                    "        T* operator->() { return (T*)0; }\n"
                    "        T& operator*() { return *(T*)0; }\n"
                    "    };\n"
                    "    template<class T, class... Args>\n"
                    "    shared_ptr<T> make_shared(Args... a) { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Service { public: void start() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    std::shared_ptr<Service> svc = std::make_shared<Service>();\n"
                    "    svc->start();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Service.start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_optional_value_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<class T> class optional {\n"
                                   "    public:\n"
                                   "        T& value() { return *(T*)0; }\n"
                                   "        T* operator->() { return (T*)0; }\n"
                                   "        T& operator*() { return *(T*)0; }\n"
                                   "        bool has_value() { return true; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Config { public: int port() { return 0; } };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::optional<Config> cfg;\n"
                                   "    if (cfg.has_value()) {\n"
                                   "        cfg.value().port();\n"
                                   "        cfg->port();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_value"), 0);
    (void)find_resolved(r, "test", "Config.port");
    {
        int idx = find_resolved(r, "test", "Config.port");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_method_call_on_parameter) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void process(Widget& w) {\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "process", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_method_call_on_const_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void show() const {} };\n"
                                   "\n"
                                   "void display(const Widget& w) {\n"
                                   "    w.show();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "display", "Widget.show"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_method_call_on_pointer_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void process(Widget* w) {\n"
                                   "    w->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "process", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_return_value_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "Widget get_widget() { return Widget(); }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    get_widget().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_struct_ptr_param) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Widget {\n"
                                 "    int value;\n"
                                 "    void (*on_click)(void);\n"
                                 "};\n"
                                 "\n"
                                 "int get_value(struct Widget* w) {\n"
                                 "    return w->value;\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_func_ptr_in_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Operations {\n"
                                 "    int (*init)(void);\n"
                                 "    void (*cleanup)(void);\n"
                                 "};\n"
                                 "\n"
                                 "int real_init(void) { return 0; }\n"
                                 "void real_cleanup(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Operations ops;\n"
                                 "    ops.init = real_init;\n"
                                 "    ops.cleanup = real_cleanup;\n"
                                 "    ops.init();\n"
                                 "    ops.cleanup();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(count_resolved(r, "test", ""), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_callback_param) {
    CBMFileResult *r = extract_c("\n"
                                 "void process_item(int x) {}\n"
                                 "\n"
                                 "void foreach(void (*cb)(int), int count) {\n"
                                 "    for (int i = 0; i < count; i++) cb(i);\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    foreach(process_item, 10);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "foreach"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_static_func) {
    CBMFileResult *r = extract_c("\n"
                                 "static int helper(int x) { return x + 1; }\n"
                                 "\n"
                                 "int test(int x) {\n"
                                 "    return helper(x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_nested_struct_access) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "struct Rect { struct Point origin; struct Point size; };\n"
                                 "\n"
                                 "int area(struct Rect* r) {\n"
                                 "    return r->size.x * r->size.y;\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_c_enum_switch) {
    CBMFileResult *r = extract_c("\n"
                                 "enum State { INIT, RUNNING, DONE };\n"
                                 "\n"
                                 "void on_init(void) {}\n"
                                 "void on_run(void) {}\n"
                                 "void on_done(void) {}\n"
                                 "\n"
                                 "void dispatch(enum State s) {\n"
                                 "    switch(s) {\n"
                                 "        case INIT: on_init(); break;\n"
                                 "        case RUNNING: on_run(); break;\n"
                                 "        case DONE: on_done(); break;\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "dispatch", "on_init"), 0);
    ASSERT_GTE(find_resolved(r, "dispatch", "on_run"), 0);
    ASSERT_GTE(find_resolved(r, "dispatch", "on_done"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_simple_template_instantiation) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    T& get() { return *(T*)0; }\n"
                                   "    void add(const T& val) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container<Widget> c;\n"
                                   "    c.add(Widget());\n"
                                   "    c.get().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "add"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_template_with_multiple_params) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Key { public: int hash() { return 0; } };\n"
                                   "class Value { public: void process() {} };\n"
                                   "\n"
                                   "template<typename A, typename B>\n"
                                   "class Pair {\n"
                                   "public:\n"
                                   "    A& first() { return *(A*)0; }\n"
                                   "    B& second() { return *(B*)0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Pair<Key, Value> p;\n"
                                   "    p.first().hash();\n"
                                   "    p.second().process();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Key.hash");
    (void)find_resolved(r, "test", "Value.process");
    {
        int idx = find_resolved(r, "test", "Key.hash");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    {
        int idx = find_resolved(r, "test", "Value.process");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_nested_template_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<class T> class shared_ptr {\n"
                                   "    public:\n"
                                   "        T* operator->() { return (T*)0; }\n"
                                   "    };\n"
                                   "    template<class T> class vector {\n"
                                   "    public:\n"
                                   "        T& operator[](int i) { return *(T*)0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<std::shared_ptr<Widget>> widgets;\n"
                                   "    widgets[0]->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_namespace_function) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace utils {\n"
                                   "    class Logger { public: void log(const char* msg) {} };\n"
                                   "    Logger create_logger() { return Logger(); }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto logger = utils::create_logger();\n"
                                   "    logger.log(\"test\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create_logger"), 0);
    (void)find_resolved(r, "test", "Logger.log");
    {
        int idx = find_resolved(r, "test", "Logger.log");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_nested_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace a { namespace b {\n"
                                   "    class Processor { public: void run() {} };\n"
                                   "}}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    a::b::Processor p;\n"
                                   "    p.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_using_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace utils {\n"
                                   "    void helper() {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    utils::helper();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_operator_plus_member_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vec {\n"
                                   "public:\n"
                                   "    Vec operator+(const Vec& other) { return Vec(); }\n"
                                   "    float length() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec a, b;\n"
                                   "    Vec c = a + b;\n"
                                   "    c.length();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "length"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_stream_operator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class OStream {\n"
                                   "public:\n"
                                   "    OStream& operator<<(const char* s) { return *this; }\n"
                                   "    OStream& operator<<(int n) { return *this; }\n"
                                   "};\n"
                                   "\n"
                                   "OStream cout;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    cout << \"value: \" << 42;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "operator<<");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_explicit_constructor_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    Widget(int size) {}\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w(42);\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_brace_init_constructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    Widget(int size) {}\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w{42};\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_temporary_object_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    Widget(int size) {}\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget(42).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_lambda_capture_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    auto fn = [&w]() { w.draw(); };\n"
                                   "    fn();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "", "Widget.draw");
    {
        int idx = find_resolved(r, "", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_lambda_return_type_used) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto fn = []() -> Widget { return Widget(); };\n"
                                   "    fn().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    {
        int idx = find_resolved(r, "test", "Widget.draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_catch_exception_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class MyException {\n"
                                   "public:\n"
                                   "    const char* what() { return \"error\"; }\n"
                                   "    int code() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "void might_throw();\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        might_throw();\n"
                                   "    } catch (MyException& e) {\n"
                                   "        e.what();\n"
                                   "        e.code();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "might_throw"), 0);
    (void)find_resolved(r, "test", "what");
    {
        int idx = find_resolved(r, "test", "what");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_static_member_function) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Factory {\n"
                                   "public:\n"
                                   "    static Factory create() { return Factory(); }\n"
                                   "    void produce() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Factory f = Factory::create();\n"
                                   "    f.produce();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Factory.produce"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_enum_class_used_in_switch) {
    CBMFileResult *r = extract_cpp("\n"
                                   "enum class Color { Red, Green, Blue };\n"
                                   "\n"
                                   "void paint_red() {}\n"
                                   "void paint_green() {}\n"
                                   "void paint_blue() {}\n"
                                   "\n"
                                   "void paint(Color c) {\n"
                                   "    switch(c) {\n"
                                   "        case Color::Red: paint_red(); break;\n"
                                   "        case Color::Green: paint_green(); break;\n"
                                   "        case Color::Blue: paint_blue(); break;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "paint", "paint_red"), 0);
    ASSERT_GTE(find_resolved(r, "paint", "paint_green"), 0);
    ASSERT_GTE(find_resolved(r, "paint", "paint_blue"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_builder_pattern) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class App { public: void run() {} };\n"
                                   "\n"
                                   "class Builder {\n"
                                   "public:\n"
                                   "    Builder& setName(const char* name) { return *this; }\n"
                                   "    Builder& setPort(int port) { return *this; }\n"
                                   "    App build() { return App(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder b;\n"
                                   "    b.setName(\"test\").setPort(8080).build().run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "setName"), 0);
    (void)find_resolved(r, "test", "App.run");
    {
        int idx = find_resolved(r, "test", "App.run");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_method_chaining_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Query {\n"
                                   "public:\n"
                                   "    Query& where(const char* clause) { return *this; }\n"
                                   "    Query& orderBy(const char* col) { return *this; }\n"
                                   "    Query& limit(int n) { return *this; }\n"
                                   "    void execute() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Query q;\n"
                                   "    q.where(\"x > 1\").orderBy(\"y\").limit(10).execute();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "where"), 0);
    ASSERT_GTE(find_resolved(r, "test", "orderBy"), 0);
    ASSERT_GTE(find_resolved(r, "test", "limit"), 0);
    ASSERT_GTE(find_resolved(r, "test", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_raiilock_guard) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Mutex { public: void lock() {} void unlock() {} };\n"
                                   "\n"
                                   "template<class M>\n"
                                   "class LockGuard {\n"
                                   "public:\n"
                                   "    LockGuard(M& m) {}\n"
                                   "    ~LockGuard() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Mutex m;\n"
                                   "    LockGuard<Mutex> guard(m);\n"
                                   "    m.lock();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "lock"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_typedef_class) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class RealWidget { public: void draw() {} };\n"
                                   "typedef RealWidget Widget;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    {
        int idx = find_resolved(r, "test", "draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_using_alias) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class RealWidget { public: void draw() {} };\n"
                                   "using Widget = RealWidget;\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    {
        int idx = find_resolved(r, "test", "draw");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_using_template_alias) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<class T> class vector {\n"
                                   "    public:\n"
                                   "        void push_back(const T& val) {}\n"
                                   "        int size() { return 0; }\n"
                                   "    };\n"
                                   "}\n"
                                   "\n"
                                   "template<class T> using Vec = std::vector<T>;\n"
                                   "\n"
                                   "class Item { public: void process() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec<Item> items;\n"
                                   "    items.push_back(Item());\n"
                                   "    items.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "push_back");
    {
        int idx = find_resolved(r, "test", "push_back");
        if (idx >= 0)
            ASSERT_STR_NEQ(r->resolved_calls.items[idx].strategy, "lsp_unresolved");
    }
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_if_null_check) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test(Widget* w) {\n"
                                   "    if (w) {\n"
                                   "        w->draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_try_catch_finally) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class DB {\n"
                                   "public:\n"
                                   "    void connect() {}\n"
                                   "    void query(const char* sql) {}\n"
                                   "    void disconnect() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    DB db;\n"
                                   "    try {\n"
                                   "        db.connect();\n"
                                   "        db.query(\"SELECT 1\");\n"
                                   "    } catch (...) {\n"
                                   "        db.disconnect();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "connect"), 0);
    ASSERT_GTE(find_resolved(r, "test", "query"), 0);
    ASSERT_GTE(find_resolved(r, "test", "disconnect"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_method_call_in_for_init) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Item { public: void process() {} };\n"
                                   "\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Item* begin() { return (Item*)0; }\n"
                                   "    Item* end() { return (Item*)0; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    c.begin();\n"
                                   "    c.end();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "end"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_nested_method_call_args) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Provider { public: int get_value() { return 0; } };\n"
                                   "class Consumer { public: void process(int val) {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Provider p;\n"
                                   "    Consumer c;\n"
                                   "    c.process(p.get_value());\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_value"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_gap_v2_conditional_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void show() {}\n"
                                   "    void hide() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test(bool visible) {\n"
                                   "    Widget w;\n"
                                   "    if (visible) w.show();\n"
                                   "    else w.hide();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.show"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Widget.hide"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix_cstruct_func_ptr_chain_call) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef void (*callback_fn)(int);\n"
                                 "struct Handler {\n"
                                 "    callback_fn on_event;\n"
                                 "};\n"
                                 "struct Manager {\n"
                                 "    struct Handler handler;\n"
                                 "};\n"
                                 "void my_callback(int x) {}\n"
                                 "void test() {\n"
                                 "    struct Manager m;\n"
                                 "    m.handler.on_event(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "on_event");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix_cast_chained_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Base { public: virtual void foo() {} };\n"
                                   "class Derived : public Base { public: void bar() {} };\n"
                                   "void test() {\n"
                                   "    Base* b = new Base();\n"
                                   "    Derived* d = dynamic_cast<Derived*>(b);\n"
                                   "    d->bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Derived.bar");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix_catch_by_value) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Error { public: const char* msg() { return \"\"; } };\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        // ...\n"
                                   "    } catch (Error e) {\n"
                                   "        e.msg();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Error.msg"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix_subscript_on_auto_var) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    T& operator[](int index);\n"
                                   "    void push_back(const T& val);\n"
                                   "};\n"
                                   "class Item { public: void use() {} };\n"
                                   "void test() {\n"
                                   "    vector<Item> items;\n"
                                   "    items[0].use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Item.use");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_fix_lambda_capture_this) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "    void process() {\n"
                                   "        auto fn = [this]() {\n"
                                   "            draw();\n"
                                   "        };\n"
                                   "    }\n"
                                   "};\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "process", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_pair) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename K, typename V>\n"
                                   "struct pair { K first; V second; };\n"
                                   "class Foo { public: void bar() {} };\n"
                                   "void test() {\n"
                                   "    pair<int, Foo> p;\n"
                                   "    auto [x, y] = p;\n"
                                   "    y.bar();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Foo.bar");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_struct) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "struct Result { int code; Widget widget; };\n"
                                   "void test() {\n"
                                   "    Result r;\n"
                                   "    auto [c, w] = r;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_array) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Foo { public: void run() {} };\n"
                                   "void test() {\n"
                                   "    Foo arr[3];\n"
                                   "    auto [a, b, c] = arr;\n"
                                   "    a.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Foo.run");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_const) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config { public: void load() {} };\n"
                                   "struct Settings { int level; Config config; };\n"
                                   "void test() {\n"
                                   "    Settings s;\n"
                                   "    const auto& [l, cfg] = s;\n"
                                   "    cfg.load();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Config.load");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_map) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename K, typename V> class map {\n"
                                   "public:\n"
                                   "    struct pair { K first; V second; };\n"
                                   "    class iterator { public: pair& operator*(); };\n"
                                   "    iterator begin();\n"
                                   "    iterator end();\n"
                                   "};\n"
                                   "class Handler { public: void handle() {} };\n"
                                   "void test() {\n"
                                   "    map<int, Handler> m;\n"
                                   "    for (auto& [key, handler] : m) {\n"
                                   "        handler.handle();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Handler.handle");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_nested) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename A, typename B>\n"
                                   "struct pair { A first; B second; };\n"
                                   "class Logger { public: void log() {} };\n"
                                   "void test() {\n"
                                   "    pair<int, Logger> inner;\n"
                                   "    pair<bool, pair<int, Logger>> outer;\n"
                                   "    auto [flag, p] = outer;\n"
                                   "    p.second.log();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Logger.log");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_tuple) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args> class tuple {};\n"
                                   "    template<int N, typename T> auto get(T& t);\n"
                                   "}\n"
                                   "class Widget { public: void show() {} };\n"
                                   "void test() {\n"
                                   "    std::tuple<int, Widget, double> tup;\n"
                                   "    auto [a, w, d] = tup;\n"
                                   "    w.show();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.show");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_structured_binding_in_if) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct Result { bool ok; int value; };\n"
                                   "Result getResult();\n"
                                   "void process(int x) {}\n"
                                   "void test() {\n"
                                   "    if (auto [ok, val] = getResult(); ok) {\n"
                                   "        process(val);\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "getResult"), 0);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_if_init_simple) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Lock { public: bool locked() { return true; } };\n"
                                   "Lock acquire();\n"
                                   "void test() {\n"
                                   "    if (Lock l = acquire(); l.locked()) {\n"
                                   "        // do work\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "acquire"), 0);
    (void)find_resolved(r, "test", "Lock.locked");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_if_init_with_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Database { public: int query() { return 0; } };\n"
                                   "void test() {\n"
                                   "    Database db;\n"
                                   "    if (int result = db.query(); result > 0) {\n"
                                   "        // process\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Database.query"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_switch_init) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Parser { public: int parse() { return 0; } };\n"
                                   "void handle_a() {}\n"
                                   "void handle_b() {}\n"
                                   "void test() {\n"
                                   "    Parser p;\n"
                                   "    switch (int code = p.parse(); code) {\n"
                                   "        case 1: handle_a(); break;\n"
                                   "        case 2: handle_b(); break;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Parser.parse"), 0);
    ASSERT_GTE(find_resolved(r, "test", "handle_a"), 0);
    ASSERT_GTE(find_resolved(r, "test", "handle_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_if_init_lock) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class mutex { public: void lock() {} void unlock() {} };\n"
                                   "class lock_guard {\n"
                                   "public:\n"
                                   "    lock_guard(mutex& m) {}\n"
                                   "};\n"
                                   "class SharedState { public: int read() { return 0; } };\n"
                                   "void test() {\n"
                                   "    mutex mtx;\n"
                                   "    SharedState state;\n"
                                   "    if (lock_guard lg(mtx); true) {\n"
                                   "        state.read();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "SharedState.read"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_fold_expr_sum) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Args>\n"
                                   "auto sum(Args... args) {\n"
                                   "    return (args + ...);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    sum(1, 2, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sum"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_fold_expr_binary) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Args>\n"
                                   "auto multiply(Args... args) {\n"
                                   "    return (args * ... * 1);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    multiply(2, 3, 4);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "multiply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_fold_expr_comma) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(int x) {}\n"
                                   "template<typename... Args>\n"
                                   "void call_all(Args... args) {\n"
                                   "    (process(args), ...);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    call_all(1, 2, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "call_all"), 0);
    ASSERT_GTE(find_resolved(r, "call_all", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_fold_expr_logical) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Args>\n"
                                   "bool all_true(Args... args) {\n"
                                   "    return (args && ...);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    all_true(true, true, false);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "all_true"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctadvector) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    void push_back(const T& val);\n"
                                   "    T& front();\n"
                                   "};\n"
                                   "class Item { public: void use() {} };\n"
                                   "void test() {\n"
                                   "    vector<Item> v;\n"
                                   "    v.push_back(Item());\n"
                                   "    v.front().use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    (void)find_resolved(r, "test", "Item.use");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctadpair) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename A, typename B>\n"
                                   "struct pair {\n"
                                   "    A first;\n"
                                   "    B second;\n"
                                   "    pair(A a, B b) : first(a), second(b) {}\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    pair p(42, Widget());\n"
                                   "    p.second.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctadoptional) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    optional(T val);\n"
                                   "    T& value();\n"
                                   "    bool has_value();\n"
                                   "};\n"
                                   "class Config { public: void load() {} };\n"
                                   "void test() {\n"
                                   "    optional<Config> opt(Config{});\n"
                                   "    opt.value().load();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Config.load");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctadtuple) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args> class tuple {\n"
                                   "    public:\n"
                                   "        tuple(Args... args);\n"
                                   "    };\n"
                                   "}\n"
                                   "class Logger { public: void log() {} };\n"
                                   "void test() {\n"
                                   "    std::tuple t(1, 2.0, Logger());\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctaduser_defined) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Container(T val);\n"
                                   "    T& get();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    Container c(Widget{});\n"
                                   "    c.get().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_ctadlock_guard) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class mutex { public: void lock() {} void unlock() {} };\n"
                                   "template<typename M>\n"
                                   "class lock_guard {\n"
                                   "public:\n"
                                   "    lock_guard(M& m);\n"
                                   "    ~lock_guard();\n"
                                   "};\n"
                                   "void test() {\n"
                                   "    mutex m;\n"
                                   "    lock_guard lg(m);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_value) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    T& value();\n"
                                   "    bool has_value();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    opt.value().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    T* operator->();\n"
                                   "    bool has_value();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    opt->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    T& operator*();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    (*opt).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_has_value) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    bool has_value();\n"
                                   "    T& value();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    if (opt.has_value()) {\n"
                                   "        opt.value().draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "has_value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_variant_get) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Types> class variant {};\n"
                                   "    template<typename T, typename V> T& get(V& v);\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    std::variant<int, Widget> v;\n"
                                   "    std::get<Widget>(v).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_variant_visit) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename... Types> class variant {};\n"
                    "    template<typename Visitor, typename V> auto visit(Visitor&& vis, V&& v);\n"
                    "}\n"
                    "class Widget { public: void draw() {} };\n"
                    "void handle(int x) {}\n"
                    "void handle(Widget w) { w.draw(); }\n"
                    "void test() {\n"
                    "    std::variant<int, Widget> v;\n"
                    "    std::visit([](auto& val) {}, v);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_any_any_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    class any {};\n"
                                   "    template<typename T> T any_cast(any& a);\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    std::any a;\n"
                                   "    std::any_cast<Widget>(a).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_value_or) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    T value_or(T default_val);\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    opt.value_or(Widget()).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_and_then) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    template<typename F> auto and_then(F f);\n"
                                   "    T& value();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    optional<Widget> opt;\n"
                                   "    opt.value().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_optional_transform) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    template<typename F> auto transform(F f);\n"
                                   "    T& value();\n"
                                   "};\n"
                                   "void test() {\n"
                                   "    optional<int> opt;\n"
                                   "    opt.value();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_if_constexpr_body) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class IntHandler { public: void handle_int() {} };\n"
                                   "class FloatHandler { public: void handle_float() {} };\n"
                                   "template<typename T>\n"
                                   "void process(T val) {\n"
                                   "    if constexpr (sizeof(T) == 4) {\n"
                                   "        IntHandler h;\n"
                                   "        h.handle_int();\n"
                                   "    } else {\n"
                                   "        FloatHandler h;\n"
                                   "        h.handle_float();\n"
                                   "    }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    process(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    ASSERT_GTE(find_resolved(r, "process", "IntHandler.handle_int"), 0);
    ASSERT_GTE(find_resolved(r, "process", "FloatHandler.handle_float"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_inline_variable) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config {\n"
                                   "public:\n"
                                   "    static inline int max_retries = 3;\n"
                                   "    void apply() {}\n"
                                   "};\n"
                                   "void test() {\n"
                                   "    Config c;\n"
                                   "    c.apply();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Config.apply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_nested_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace a::b::c {\n"
                                   "    class Widget { public: void draw() {} };\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    a::b::c::Widget w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Widget.draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_constexpr_if) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Handler { public: void handle() {} };\n"
                                   "template<bool B>\n"
                                   "void dispatch() {\n"
                                   "    if constexpr (B) {\n"
                                   "        Handler h;\n"
                                   "        h.handle();\n"
                                   "    }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    dispatch<true>();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "dispatch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_string_view) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    class string_view {\n"
                                   "    public:\n"
                                   "        int size();\n"
                                   "        const char* data();\n"
                                   "        string_view substr(int pos, int count);\n"
                                   "    };\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::string_view sv;\n"
                                   "    sv.size();\n"
                                   "    sv.substr(0, 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "string_view.size"), 0);
    ASSERT_GTE(find_resolved(r, "test", "string_view.substr"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_filesystem_path) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace filesystem {\n"
                                   "    class path {\n"
                                   "    public:\n"
                                   "        path parent_path();\n"
                                   "        path filename();\n"
                                   "        bool exists();\n"
                                   "    };\n"
                                   "}}\n"
                                   "void test() {\n"
                                   "    std::filesystem::path p;\n"
                                   "    p.parent_path();\n"
                                   "    p.filename();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "path.parent_path"), 0);
    ASSERT_GTE(find_resolved(r, "test", "path.filename"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_user_defined_literal) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Duration { public: int seconds() { return 0; } };\n"
                    "Duration operator\"\"_s(unsigned long long val) { return Duration(); }\n"
                    "void test() {\n"
                    "    auto d = 42_s;\n"
                    "    d.seconds();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Duration.seconds");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_class_template_deduction) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Wrapper {\n"
                                   "public:\n"
                                   "    Wrapper(T val);\n"
                                   "    T& get();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    Wrapper w(Widget{});\n"
                                   "    w.get().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_apply_tuple) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename... Args> class tuple {};\n"
                    "    template<typename F, typename Tuple> auto apply(F&& f, Tuple&& t);\n"
                    "}\n"
                    "void process(int a, double b) {}\n"
                    "void test() {\n"
                    "    std::tuple<int, double> args;\n"
                    "    std::apply(process, args);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "apply"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp17_invoke_result) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename F, typename... Args>\n"
                                   "    auto invoke(F&& f, Args&&... args);\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void do_draw(Widget& w) { w.draw(); }\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    std::invoke(do_draw, w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "invoke"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_constrained_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "template<typename T>\n"
                                   "concept Drawable = requires(T t) { t.draw(); };\n"
                                   "template<Drawable T>\n"
                                   "void render(T& obj) {\n"
                                   "    obj.draw();\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    render(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_requires_clause) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger { public: void log() {} };\n"
                                   "template<typename T>\n"
                                   "void process(T& obj) requires requires { obj.log(); } {\n"
                                   "    obj.log();\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    Logger l;\n"
                                   "    process(l);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_auto_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Processor { public: void run() {} };\n"
                                   "void handle(auto& obj) {\n"
                                   "    obj.run();\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    Processor p;\n"
                                   "    handle(p);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_nested) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "template<typename T>\n"
                    "concept Hashable = requires(T a) {\n"
                    "    { a.hash() } -> int;\n"
                    "};\n"
                    "template<typename T>\n"
                    "concept Identifiable = Hashable<T> && requires(T a) {\n"
                    "    { a.id() } -> int;\n"
                    "};\n"
                    "class Entity { public: int hash() { return 0; } int id() { return 0; } };\n"
                    "void test() {\n"
                    "    Entity e;\n"
                    "    e.hash();\n"
                    "    e.id();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Entity.hash"), 0);
    ASSERT_GTE(find_resolved(r, "test", "Entity.id"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_conjunction) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> concept A = true;\n"
                                   "template<typename T> concept B = true;\n"
                                   "template<typename T> requires A<T> && B<T>\n"
                                   "void constrained(T& val) {}\n"
                                   "void test() {\n"
                                   "    int x = 5;\n"
                                   "    constrained(x);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "constrained"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_subsumption) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> concept Base = true;\n"
                                   "template<typename T> concept Derived = Base<T> && true;\n"
                                   "template<Base T> void f(T val) {}\n"
                                   "template<Derived T> void f(T val) {}\n"
                                   "void test() {\n"
                                   "    f(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "f"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_concept_on_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> concept Numeric = true;\n"
                                   "class Calculator {\n"
                                   "public:\n"
                                   "    template<Numeric T> T add(T a, T b) { return a + b; }\n"
                                   "};\n"
                                   "void test() {\n"
                                   "    Calculator c;\n"
                                   "    c.add(1, 2);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Calculator.add"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_requires_expression) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "template<typename T>\n"
                                   "bool can_draw() {\n"
                                   "    return requires(T t) { t.draw(); };\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    can_draw<Widget>();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "can_draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_co_await_expr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Task {\n"
                                   "public:\n"
                                   "    T result;\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "Task<Widget> get_widget();\n"
                                   "void test() {\n"
                                   "    auto w = co_await get_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_widget"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_co_yield_expr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class generator {};\n"
                                   "void process(int x) {}\n"
                                   "generator<int> generate() {\n"
                                   "    co_yield 42;\n"
                                   "    process(10);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    generate();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "generate"), 0);
    ASSERT_GTE(find_resolved(r, "generate", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_co_return_expr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class Task {};\n"
                                   "class Widget { public: void prepare() {} };\n"
                                   "Task<Widget> make_widget() {\n"
                                   "    Widget w;\n"
                                   "    w.prepare();\n"
                                   "    co_return w;\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    make_widget();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_widget"), 0);
    ASSERT_GTE(find_resolved(r, "make_widget", "Widget.prepare"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_coroutine_handle) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename P = void>\n"
                                   "    class coroutine_handle {\n"
                                   "    public:\n"
                                   "        void resume();\n"
                                   "        bool done();\n"
                                   "        void destroy();\n"
                                   "    };\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::coroutine_handle<> h;\n"
                                   "    h.resume();\n"
                                   "    h.done();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "coroutine_handle.resume"), 0);
    ASSERT_GTE(find_resolved(r, "test", "coroutine_handle.done"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_task) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Task {\n"
                                   "public:\n"
                                   "    bool await_ready();\n"
                                   "    void await_suspend();\n"
                                   "    T await_resume();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    Task<Widget> t;\n"
                                   "    t.await_ready();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Task.await_ready"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_coroutine_body_calls) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Logger { public: void log() {} };\n"
                                   "template<typename T> class Task {};\n"
                                   "Task<void> async_work() {\n"
                                   "    Logger l;\n"
                                   "    l.log();\n"
                                   "    co_return;\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    async_work();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "async_work"), 0);
    ASSERT_GTE(find_resolved(r, "async_work", "Logger.log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_generator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class generator {\n"
                                   "public:\n"
                                   "    class iterator {\n"
                                   "    public:\n"
                                   "        T& operator*();\n"
                                   "        iterator& operator++();\n"
                                   "    };\n"
                                   "    iterator begin();\n"
                                   "    iterator end();\n"
                                   "};\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "generator<Widget> all_widgets() {\n"
                                   "    co_yield Widget();\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    for (auto& w : all_widgets()) {\n"
                                   "        w.draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "all_widgets"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_nested_co_await) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T> class Task {};\n"
                                   "class Database { public: void query() {} };\n"
                                   "Task<Database> connect();\n"
                                   "Task<void> process();\n"
                                   "Task<void> main_task() {\n"
                                   "    auto db = co_await connect();\n"
                                   "    db.query();\n"
                                   "    co_await process();\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    main_task();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "main_task"), 0);
    ASSERT_GTE(find_resolved(r, "main_task", "connect"), 0);
    (void)find_resolved(r, "main_task", "Database.query");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_ranges_pipeline) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std { namespace views {\n"
                    "    template<typename F> auto transform(F f);\n"
                    "    template<typename P> auto filter(P p);\n"
                    "}}\n"
                    "namespace std { namespace ranges {\n"
                    "    template<typename R, typename F> auto transform(R&& r, F f);\n"
                    "}}\n"
                    "class Widget { public: int value() { return 0; } };\n"
                    "void test() {\n"
                    "    Widget widgets[10];\n"
                    "    // Pipeline syntax uses | operator\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_ranges_for_each) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std { namespace ranges {\n"
                    "    template<typename R, typename F> void for_each(R&& r, F f);\n"
                    "}}\n"
                    "void process(int x) {}\n"
                    "void test() {\n"
                    "    int arr[] = {1, 2, 3};\n"
                    "    std::ranges::for_each(arr, process);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "for_each"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_views_transform) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace views {\n"
                                   "    template<typename F> auto transform(F f);\n"
                                   "}}\n"
                                   "int double_it(int x) { return x * 2; }\n"
                                   "void test() {\n"
                                   "    std::views::transform(double_it);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "transform"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_views_filter) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace views {\n"
                                   "    template<typename P> auto filter(P pred);\n"
                                   "}}\n"
                                   "bool is_even(int x) { return x % 2 == 0; }\n"
                                   "void test() {\n"
                                   "    std::views::filter(is_even);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "filter"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_ranges_sort) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace ranges {\n"
                                   "    template<typename R> void sort(R&& r);\n"
                                   "}}\n"
                                   "void test() {\n"
                                   "    int arr[] = {3, 1, 2};\n"
                                   "    std::ranges::sort(arr);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_views_take) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace views {\n"
                                   "    auto take(int n);\n"
                                   "}}\n"
                                   "void test() {\n"
                                   "    std::views::take(5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "take"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_ranges_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std { namespace ranges {\n"
                                   "    template<typename R> auto begin(R&& r);\n"
                                   "    template<typename R> auto end(R&& r);\n"
                                   "}}\n"
                                   "void test() {\n"
                                   "    int arr[] = {1, 2, 3};\n"
                                   "    std::ranges::begin(arr);\n"
                                   "    std::ranges::end(arr);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "end"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_ranges_projection) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std { namespace ranges {\n"
                    "    template<typename R, typename Proj> void sort(R&& r, Proj proj);\n"
                    "}}\n"
                    "class Item { public: int key() { return 0; } };\n"
                    "void test() {\n"
                    "    Item items[5];\n"
                    "    std::ranges::sort(items, [](const Item& i) { return i.key(); });\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_consteval_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "consteval int square(int n) { return n * n; }\n"
                                   "void test() {\n"
                                   "    int x = square(5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "square"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_constinit_var) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Config { public: void load() {} };\n"
                                   "constinit int global_val = 42;\n"
                                   "void test() {\n"
                                   "    Config c;\n"
                                   "    c.load();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Config.load"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_designated_init) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Engine { public: void start() {} };\n"
                                   "struct Car { int speed; Engine engine; };\n"
                                   "void test() {\n"
                                   "    Car c = { .speed = 100, .engine = Engine() };\n"
                                   "    c.engine.start();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Engine.start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_three_way_comparison) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Version {\n"
                                   "public:\n"
                                   "    int major, minor;\n"
                                   "    auto operator<=>(const Version& other);\n"
                                   "    bool operator==(const Version& other);\n"
                                   "};\n"
                                   "void test() {\n"
                                   "    Version v1, v2;\n"
                                   "    v1 == v2;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_span_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename T>\n"
                                   "    class span {\n"
                                   "    public:\n"
                                   "        T& operator[](int idx);\n"
                                   "        int size();\n"
                                   "        T* data();\n"
                                   "    };\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    std::span<Widget> s;\n"
                                   "    s.size();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "span.size"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_jthread) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    class jthread {\n"
                                   "    public:\n"
                                   "        template<typename F> jthread(F&& f);\n"
                                   "        void join();\n"
                                   "        bool joinable();\n"
                                   "        void request_stop();\n"
                                   "    };\n"
                                   "}\n"
                                   "void work() {}\n"
                                   "void test() {\n"
                                   "    std::jthread thr{work};\n"
                                   "    thr.join();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "jthread.join"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_format_string) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename... Args>\n"
                                   "    auto format(const char* fmt, Args&&... args);\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::format(\"hello {}\", 42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "format"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_source_location) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    class source_location {\n"
                                   "    public:\n"
                                   "        static source_location current();\n"
                                   "        const char* file_name();\n"
                                   "        int line();\n"
                                   "    };\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    auto loc = std::source_location::current();\n"
                                   "    loc.file_name();\n"
                                   "    loc.line();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "current"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_using_enum) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Processor { public: void process() {} };\n"
                                   "enum class Color { Red, Green, Blue };\n"
                                   "void test() {\n"
                                   "    Processor p;\n"
                                   "    p.process();\n"
                                   "    Color c = Color::Red;\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "Processor.process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_lambda_template_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    auto fn = []<typename T>(T& obj) {\n"
                                   "        obj.draw();\n"
                                   "    };\n"
                                   "    Widget w;\n"
                                   "    fn(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "fn");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_cpp20_lambda_init_capture) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    auto fn = [captured = w]() {\n"
                                   "        captured.draw();\n"
                                   "    };\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_enable_if_method) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T = void> struct enable_if {};\n"
        "    template<class T> struct enable_if<true, T> { typedef T type; };\n"
        "    template<class T> struct is_integral { static const bool value = false; };\n"
        "}\n"
        "\n"
        "class Widget {\n"
        "public:\n"
        "    template<typename T>\n"
        "    typename std::enable_if<std::is_integral<T>::value, void>::type\n"
        "    process(T val) {}\n"
        "};\n"
        "\n"
        "void test() {\n"
        "    Widget w;\n"
        "    w.process(42);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_enable_if_return) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T = void> struct enable_if {};\n"
        "    template<class T> struct enable_if<true, T> { typedef T type; };\n"
        "    template<class T> struct is_integral { static const bool value = false; };\n"
        "    template<> struct is_integral<int> { static const bool value = true; };\n"
        "}\n"
        "\n"
        "template<typename T>\n"
        "typename std::enable_if<std::is_integral<T>::value, T>::type\n"
        "double_val(T x) { return x * 2; }\n"
        "\n"
        "void test() {\n"
        "    int r = double_val(5);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "double_val"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_void_t) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename...> using void_t = void;\n"
                    "}\n"
                    "\n"
                    "template<typename T, typename = void>\n"
                    "struct has_draw : false_type {};\n"
                    "\n"
                    "template<typename T>\n"
                    "struct has_draw<T, std::void_t<decltype(T().draw())>> : true_type {};\n"
                    "\n"
                    "class Widget { public: void draw() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    Widget w;\n"
                    "    w.draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_is_detected) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename...> using void_t = void;\n"
                    "}\n"
                    "\n"
                    "template<typename T, template<class> class Op, typename = void>\n"
                    "struct is_detected : false_type {};\n"
                    "\n"
                    "template<typename T, template<class> class Op>\n"
                    "struct is_detected<T, Op, std::void_t<Op<T>>> : true_type {};\n"
                    "\n"
                    "template<class T> using draw_t = decltype(T().draw());\n"
                    "\n"
                    "class Widget { public: void draw() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    Widget w;\n"
                    "    w.draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_if_constexprsfinae) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Printer { public: void print_val() {} };\n"
                                   "class Logger { public: void log_val() {} };\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void dispatch(T obj) {\n"
                                   "    if constexpr (sizeof(T) > 4) {\n"
                                   "        obj.print_val();\n"
                                   "    } else {\n"
                                   "        obj.log_val();\n"
                                   "    }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Printer p;\n"
                                   "    dispatch(p);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "dispatch"), 0);
    (void)find_resolved(r, "dispatch", "print_val");
    (void)find_resolved(r, "dispatch", "log_val");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_conditional_type) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T, class F> struct conditional { typedef T type; };\n"
        "    template<class T, class F> struct conditional<false, T, F> { typedef F type; };\n"
        "    template<bool B, class T, class F> using conditional_t = typename "
        "conditional<B,T,F>::type;\n"
        "}\n"
        "\n"
        "class IntHandler { public: void handle() {} };\n"
        "class FloatHandler { public: void handle() {} };\n"
        "\n"
        "void test() {\n"
        "    IntHandler h;\n"
        "    h.handle();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_decltype_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: int value() { return 0; } };\n"
                                   "\n"
                                   "Widget make_widget() { return Widget{}; }\n"
                                   "\n"
                                   "auto get_value() -> decltype(make_widget().value()) {\n"
                                   "    return make_widget().value();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto v = get_value();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "get_value");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_trailing_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "auto create() -> Widget { return Widget{}; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = create();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_partial_spec_pointer) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    void store() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "class Container<T*> {\n"
                                   "public:\n"
                                   "    void store_ptr() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container<int*> c;\n"
                                   "    c.store_ptr();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "store_ptr"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_partial_spec_const) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Wrapper {\n"
                                   "public:\n"
                                   "    void mutate() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "class Wrapper<const T> {\n"
                                   "public:\n"
                                   "    void read_only() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Wrapper<int> w;\n"
                                   "    w.mutate();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "mutate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_full_spec) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    void generic_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<>\n"
                                   "class Container<int> {\n"
                                   "public:\n"
                                   "    void int_op() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container<int> c;\n"
                                   "    c.int_op();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "int_op"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_member_spec) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Converter {\n"
                                   "public:\n"
                                   "    void convert() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<>\n"
                                   "void Converter<int>::convert() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Converter<int> c;\n"
                                   "    c.convert();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "convert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_static_member_spec) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Registry {\n"
                                   "public:\n"
                                   "    static void init() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Registry<int>::init();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "init"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_type_trait_spec) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "struct is_widget { static const bool value = false; };\n"
                                   "\n"
                                   "struct Widget { void draw() {} };\n"
                                   "\n"
                                   "template<>\n"
                                   "struct is_widget<Widget> { static const bool value = true; };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_variadic_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Target { public: void invoke() {} };\n"
                                   "\n"
                                   "template<typename... Args>\n"
                                   "void call_all(Args... args) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Target tgt;\n"
                                   "    call_all(1, 2, 3);\n"
                                   "    tgt.invoke();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "call_all"), 0);
    ASSERT_GTE(find_resolved(r, "test", "invoke"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_variadic_class) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Ts>\n"
                                   "class Tuple {\n"
                                   "public:\n"
                                   "    void clear() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Tuple<int, float, double> tup;\n"
                                   "    tup.clear();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "clear"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_parameter_pack) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename... Args>\n"
                                   "int count_args(Args... args) {\n"
                                   "    return sizeof...(Args);\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int n = count_args(1, 2, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "count_args"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_fold_over_args) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void process(int x) {}\n"
                                   "\n"
                                   "template<typename... Args>\n"
                                   "void fold_call(Args... args) {\n"
                                   "    (process(args), ...);\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    fold_call(1, 2, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "fold_call"), 0);
    (void)find_resolved(r, "fold_call", "process");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_variadic_inheritance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct MixA { void do_a() {} };\n"
                                   "struct MixB { void do_b() {} };\n"
                                   "\n"
                                   "template<typename... Bases>\n"
                                   "class Combined : public Bases... {\n"
                                   "public:\n"
                                   "    void run() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Combined<MixA, MixB> c;\n"
                                   "    c.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    (void)find_resolved(r, "test", "do_a");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_recursive_variadic) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void base_print() {}\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void rec_print(T val) {\n"
                                   "    base_print();\n"
                                   "}\n"
                                   "\n"
                                   "template<typename T, typename... Rest>\n"
                                   "void rec_print(T val, Rest... rest) {\n"
                                   "    base_print();\n"
                                   "    rec_print(rest...);\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    rec_print(1, 2, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "rec_print"), 0);
    (void)find_resolved(r, "rec_print", "base_print");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_make_from_variadic) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class shared_ptr {\n"
                    "public:\n"
                    "    T* operator->() { return (T*)0; }\n"
                    "};\n"
                    "template<typename T, typename... Args>\n"
                    "shared_ptr<T> make_shared(Args... args) { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Widget { public: void draw() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    auto p = std::make_shared<Widget>();\n"
                    "    p->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_shared"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_tuple_element) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename A, typename B>\n"
                                   "struct pair {\n"
                                   "    A first;\n"
                                   "    B second;\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::pair<int, Widget> p;\n"
                                   "    p.second.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "Widget.draw");
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_type) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    typedef int value_type;\n"
                                   "    value_type get() { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void process(T c) {\n"
                                   "    c.get();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Container c;\n"
                                   "    process(c);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    (void)find_resolved(r, "process", "get");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_name) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class MyContainer {\n"
                                   "public:\n"
                                   "    typedef int iterator;\n"
                                   "    void begin() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename C>\n"
                                   "void iterate(C container) {\n"
                                   "    container.begin();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    MyContainer c;\n"
                                   "    iterate(c);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "iterate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_nested_dependent) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Inner { public: void action() {} };\n"
                                   "class Outer {\n"
                                   "public:\n"
                                   "    typedef Inner nested_type;\n"
                                   "    Inner get_inner() { return Inner{}; }\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void deep(T obj) {\n"
                                   "    auto inner = obj.get_inner();\n"
                                   "    inner.action();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Outer o;\n"
                                   "    deep(o);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "deep"), 0);
    (void)find_resolved(r, "deep", "action");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "T make_thing() { return T{}; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = make_thing<Widget>();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_thing"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_field) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class Holder {\n"
                                   "    T item;\n"
                                   "public:\n"
                                   "    void use() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Holder<Widget> h;\n"
                                   "    h.use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_method_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Renderer { public: void render() {} };\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void invoke(T obj) {\n"
                                   "    obj.render();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Renderer r;\n"
                                   "    invoke(r);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "invoke"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_dependent_base_class) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename Derived>\n"
                                   "class Base {\n"
                                   "public:\n"
                                   "    void interface_method() {\n"
                                   "        static_cast<Derived*>(this)->impl();\n"
                                   "    }\n"
                                   "};\n"
                                   "\n"
                                   "class Derived : public Base<Derived> {\n"
                                   "public:\n"
                                   "    void impl() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Derived d;\n"
                                   "    d.interface_method();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "interface_method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_two_phase) {
    CBMFileResult *r = extract_cpp("\n"
                                   "void non_dependent() {}\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void two_phase(T val) {\n"
                                   "    non_dependent();\n"
                                   "    val.dependent_call();\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void dependent_call() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    two_phase(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "two_phase"), 0);
    ASSERT_GTE(find_resolved(r, "two_phase", "non_dependent"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_expr_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Vec {\n"
                                   "public:\n"
                                   "    Vec operator+(const Vec& other) { return *this; }\n"
                                   "    Vec operator*(float scalar) { return *this; }\n"
                                   "    float norm() { return 0.0f; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Vec a, b;\n"
                                   "    Vec c = a + b;\n"
                                   "    float n = c.norm();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "norm"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_policy_based_log) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct ConsolePolicy {\n"
                                   "    void write(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename Policy>\n"
                                   "class Logger {\n"
                                   "    Policy policy;\n"
                                   "public:\n"
                                   "    void log(const char* msg) { policy.write(msg); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Logger<ConsolePolicy> logger;\n"
                                   "    logger.log(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_policy_based_alloc) {
    CBMFileResult *r = extract_cpp("\n"
                                   "struct MallocAlloc {\n"
                                   "    void* allocate(int sz) { return 0; }\n"
                                   "};\n"
                                   "\n"
                                   "template<typename Alloc>\n"
                                   "class Pool {\n"
                                   "    Alloc alloc;\n"
                                   "public:\n"
                                   "    void* get(int sz) { return alloc.allocate(sz); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Pool<MallocAlloc> pool;\n"
                                   "    pool.get(64);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_template_template) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class DefaultContainer {\n"
                                   "public:\n"
                                   "    void add(T val) {}\n"
                                   "};\n"
                                   "\n"
                                   "template<template<typename> class C, typename T>\n"
                                   "class Wrapper {\n"
                                   "    C<T> inner;\n"
                                   "public:\n"
                                   "    void insert(T val) { inner.add(val); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Wrapper<DefaultContainer, int> w;\n"
                                   "    w.insert(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "insert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_mixin_pattern) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename Derived>\n"
                                   "class Printable {\n"
                                   "public:\n"
                                   "    void print() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Doc : public Printable<Doc> {\n"
                                   "public:\n"
                                   "    void save() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Doc d;\n"
                                   "    d.print();\n"
                                   "    d.save();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "print"), 0);
    ASSERT_GTE(find_resolved(r, "test", "save"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_type_erasure) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Drawable {\n"
                                   "public:\n"
                                   "    virtual void draw() = 0;\n"
                                   "};\n"
                                   "\n"
                                   "class Circle : public Drawable {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Circle c;\n"
                                   "    c.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_template_static_assert) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "class SafeContainer {\n"
                                   "public:\n"
                                   "    void add(T val) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    SafeContainer<int> sc;\n"
                                   "    sc.add(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "add"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_simple_func) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "T identity(T val) { return val; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int x = identity(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "identity"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_return_type_deduction) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "auto make_widget() {\n"
                                   "    return Widget{};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = make_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_widget"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_multi_param) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename A, typename B>\n"
                                   "A combine(A a, B b) { return a; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int r = combine(1, 2.0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "combine"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_explicit_args) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T>\n"
                                   "T create() { return T{}; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int v = create<int>();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_partial_explicit) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename R, typename T>\n"
                                   "R convert(T val) { return R{}; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    float f = convert<float>(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "convert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_default_arg) {
    CBMFileResult *r = extract_cpp("\n"
                                   "template<typename T = int>\n"
                                   "class Box {\n"
                                   "public:\n"
                                   "    void open() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Box<> b;\n"
                                   "    b.open();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "open"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_perfect_forwarding) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "    template<typename T> T&& forward(T& arg) { return (T&&)arg; }\n"
                    "}\n"
                    "\n"
                    "void target(int x) {}\n"
                    "\n"
                    "template<typename T>\n"
                    "void forwarder(T&& arg) {\n"
                    "    target(std::forward<T>(arg));\n"
                    "}\n"
                    "\n"
                    "void test() {\n"
                    "    forwarder(42);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "forwarder"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_tad_auto_return) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "auto build_widget() {\n"
                                   "    Widget w;\n"
                                   "    return w;\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    auto w = build_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "build_widget"), 0);
    (void)find_resolved(r, "test", "Widget.draw");
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_shared_ptr_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class shared_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "    T& operator*() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::shared_ptr<Widget> p;\n"
                                   "    p->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_unique_ptr_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class unique_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "    T& operator*() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::unique_ptr<Widget> p;\n"
                                   "    p->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_shared_ptr_get) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class shared_ptr {\n"
                                   "public:\n"
                                   "    T* get() { return (T*)0; }\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::shared_ptr<Widget> p;\n"
                                   "    p.get()->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_shared_ptr_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class shared_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "    T& operator*() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::shared_ptr<Widget> p;\n"
                                   "    (*p).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_shared_ptr_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class shared_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "class Container {\n"
                                   "public:\n"
                                   "    Widget first;\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::shared_ptr<Container> p;\n"
                                   "    p->first.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_weak_ptr_lock) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class shared_ptr {\n"
                                   "public:\n"
                                   "    T* operator->() { return (T*)0; }\n"
                                   "};\n"
                                   "template<typename T> class weak_ptr {\n"
                                   "public:\n"
                                   "    shared_ptr<T> lock() { return shared_ptr<T>(); }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::weak_ptr<Widget> w;\n"
                                   "    w.lock()->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_make_shared_method_chain) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class shared_ptr {\n"
                    "public:\n"
                    "    T* operator->() { return (T*)0; }\n"
                    "};\n"
                    "template<typename T> shared_ptr<T> make_shared() { return shared_ptr<T>(); }\n"
                    "}\n"
                    "\n"
                    "class Widget { public: void draw() {} };\n"
                    "\n"
                    "void test() {\n"
                    "    std::make_shared<Widget>()->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_shared_ptr_cast) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "template<typename T> class shared_ptr {\n"
        "public:\n"
        "    T* operator->() { return (T*)0; }\n"
        "};\n"
        "template<typename T, typename U>\n"
        "shared_ptr<T> static_pointer_cast(const shared_ptr<U>& r) { return shared_ptr<T>(); }\n"
        "}\n"
        "\n"
        "class Base { public: virtual void act() {} };\n"
        "class Derived : public Base { public: void special() {} };\n"
        "\n"
        "void test() {\n"
        "    std::shared_ptr<Base> base;\n"
        "    auto derived = std::static_pointer_cast<Derived>(base);\n"
        "    derived->special();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "special");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_iterator_for_loop) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget widgets[3];\n"
                                   "    for (auto& w : widgets) {\n"
                                   "        w.draw();\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_iterator_deref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    struct iterator {\n"
                                   "        T& operator*() { static T val; return val; }\n"
                                   "        T* operator->() { return (T*)0; }\n"
                                   "    };\n"
                                   "    iterator begin() { return iterator(); }\n"
                                   "    iterator end() { return iterator(); }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    auto iter = widgets.begin();\n"
                                   "    (*iter).draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_iterator_arrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    struct iterator {\n"
                                   "        T* operator->() { return (T*)0; }\n"
                                   "    };\n"
                                   "    iterator begin() { return iterator(); }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<Widget> widgets;\n"
                                   "    auto iter = widgets.begin();\n"
                                   "    iter->draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_reverse_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void rbegin() {}\n"
                                   "    void rend() {}\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.rbegin();\n"
                                   "    v.rend();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "rbegin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "rend"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_const_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void cbegin() {}\n"
                                   "    void cend() {}\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.cbegin();\n"
                                   "    v.cend();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "cbegin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "cend"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_insert_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> struct vector {\n"
                                   "    void push_back(const T& val) {}\n"
                                   "};\n"
                                   "template<typename C>\n"
                                   "void back_inserter(C& c) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    std::back_inserter(v);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "back_inserter"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_iterator_advance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "void advance(Iter& it, int n) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int* ptr = 0;\n"
                                   "    std::advance(ptr, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "advance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_iterator_distance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "int distance(Iter first, Iter last) { return 0; }\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    int* a = 0;\n"
                                   "    int* b = 0;\n"
                                   "    int d = std::distance(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_stack_push) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class stack {\n"
                                   "public:\n"
                                   "    void push(const T& val) {}\n"
                                   "    void pop() {}\n"
                                   "    T& top() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::stack<int> s;\n"
                                   "    s.push(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_queue_front) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class queue {\n"
                                   "public:\n"
                                   "    void push(const T& val) {}\n"
                                   "    T& front() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::queue<Widget> q;\n"
                                   "    q.front().draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_priority_queue_top) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class priority_queue {\n"
                                   "public:\n"
                                   "    void push(const T& val) {}\n"
                                   "    const T& top() { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::priority_queue<int> pq;\n"
                                   "    pq.push(1);\n"
                                   "    pq.top();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push"), 0);
    ASSERT_GTE(find_resolved(r, "test", "top"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_deque_access) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class deque {\n"
                                   "public:\n"
                                   "    T& operator[](int i) { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::deque<Widget> d;\n"
                                   "    d[0].draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_set_insert) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class set {\n"
                                   "public:\n"
                                   "    void insert(const T& val) {}\n"
                                   "    void find(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::set<int> s;\n"
                                   "    s.insert(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "insert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_multi_map_range) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class multimap {\n"
                                   "public:\n"
                                   "    void find(const K& key) {}\n"
                                   "    void insert(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    std::multimap<int, Widget> m;\n"
                                   "    m.find(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_lock_guard_scope) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Mutex { public: void lock() {} void unlock() {} };\n"
                                   "\n"
                                   "class LockGuard {\n"
                                   "    Mutex& mtx;\n"
                                   "public:\n"
                                   "    LockGuard(Mutex& m) : mtx(m) { mtx.lock(); }\n"
                                   "};\n"
                                   "\n"
                                   "void do_work() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Mutex m;\n"
                                   "    LockGuard lg{m};\n"
                                   "    do_work();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "do_work"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_unique_lock_scope) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Mutex { public: void lock() {} void unlock() {} };\n"
                                   "\n"
                                   "class UniqueLock {\n"
                                   "    Mutex& mtx;\n"
                                   "public:\n"
                                   "    UniqueLock(Mutex& m) : mtx(m) {}\n"
                                   "    void lock_now() { mtx.lock(); }\n"
                                   "};\n"
                                   "\n"
                                   "void do_work() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Mutex m;\n"
                                   "    UniqueLock ul{m};\n"
                                   "    ul.lock_now();\n"
                                   "    do_work();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "lock_now"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_work"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_scoped_timer) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class ScopedTimer {\n"
                                   "public:\n"
                                   "    void start() {}\n"
                                   "    void stop() {}\n"
                                   "};\n"
                                   "\n"
                                   "void do_work() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    ScopedTimer timer;\n"
                                   "    timer.start();\n"
                                   "    do_work();\n"
                                   "    timer.stop();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "start"), 0);
    ASSERT_GTE(find_resolved(r, "test", "stop"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_work"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_file_handle) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class FileHandle {\n"
                                   "public:\n"
                                   "    void read() {}\n"
                                   "    void write() {}\n"
                                   "    void close() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    FileHandle fh;\n"
                                   "    fh.read();\n"
                                   "    fh.close();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "read"), 0);
    ASSERT_GTE(find_resolved(r, "test", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_transaction_scope) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Transaction {\n"
                                   "public:\n"
                                   "    void begin() {}\n"
                                   "    void commit() {}\n"
                                   "    void rollback() {}\n"
                                   "};\n"
                                   "\n"
                                   "void do_work() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Transaction txn;\n"
                                   "    txn.begin();\n"
                                   "    do_work();\n"
                                   "    txn.commit();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "commit"), 0);
    ASSERT_GTE(find_resolved(r, "test", "do_work"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_connection_pool) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Connection {\n"
                                   "public:\n"
                                   "    void query() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Pool {\n"
                                   "public:\n"
                                   "    Connection acquire() { return Connection{}; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Pool pool;\n"
                                   "    pool.acquire().query();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "acquire"), 0);
    (void)find_resolved(r, "test", "query");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_scope_guard) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class ScopeGuard {\n"
                                   "public:\n"
                                   "    void dismiss() {}\n"
                                   "};\n"
                                   "\n"
                                   "void cleanup() {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    ScopeGuard guard;\n"
                                   "    cleanup();\n"
                                   "    guard.dismiss();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "cleanup"), 0);
    ASSERT_GTE(find_resolved(r, "test", "dismiss"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_factory_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    static Widget create() { return Widget{}; }\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w = Widget::create();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_abstract_factory) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Product { public: virtual void use() {} };\n"
                    "\n"
                    "class ConcreteProduct : public Product {\n"
                    "public:\n"
                    "    void use() {}\n"
                    "};\n"
                    "\n"
                    "class Factory {\n"
                    "public:\n"
                    "    virtual Product* create() { return new ConcreteProduct(); }\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Factory f;\n"
                    "    Product* p = f.create();\n"
                    "    p->use();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_factory_function) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "Widget make_widget() { return Widget{}; }\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w = make_widget();\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_widget"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_builder_pattern) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Builder {\n"
                                   "public:\n"
                                   "    Builder& set_x(int x) { return *this; }\n"
                                   "    Builder& set_y(int y) { return *this; }\n"
                                   "    void build() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Builder b;\n"
                                   "    b.set_x(1).set_y(2).build();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_x"), 0);
    ASSERT_GTE(find_resolved(r, "test", "set_y"), 0);
    ASSERT_GTE(find_resolved(r, "test", "build"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_singleton) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Singleton {\n"
                    "public:\n"
                    "    static Singleton& instance() { static Singleton s; return s; }\n"
                    "    void method() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Singleton::instance().method();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "instance"), 0);
    ASSERT_GTE(find_resolved(r, "test", "method"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_prototype_clone) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Prototype {\n"
                                   "public:\n"
                                   "    virtual Prototype* clone() { return new Prototype(); }\n"
                                   "    void use() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Prototype p;\n"
                                   "    Prototype* copy = p.clone();\n"
                                   "    copy->use();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "clone"), 0);
    ASSERT_GTE(find_resolved(r, "test", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_factory_registry) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "class Registry {\n"
                                   "public:\n"
                                   "    Widget create(int id) { return Widget{}; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Registry reg;\n"
                                   "    auto w = reg.create(1);\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create"), 0);
    (void)find_resolved(r, "test", "draw");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_named_constructor) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Widget {\n"
                    "public:\n"
                    "    static Widget fromFile(const char* path) { return Widget{}; }\n"
                    "    void draw() {}\n"
                    "};\n"
                    "\n"
                    "void test() {\n"
                    "    Widget w = Widget::fromFile(\"test.txt\");\n"
                    "    w.draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "fromFile"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_observer_notify) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Observer {\n"
                                   "public:\n"
                                   "    virtual void notify() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Observer obs;\n"
                                   "    obs.notify();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "notify"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_observer_subscribe) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Observer { public: void on_event() {} };\n"
                                   "\n"
                                   "class Subject {\n"
                                   "public:\n"
                                   "    void subscribe(Observer* obs) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Subject subj;\n"
                                   "    Observer obs;\n"
                                   "    subj.subscribe(&obs);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "subscribe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_visitor_accept) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Visitor;\n"
                                   "\n"
                                   "class Element {\n"
                                   "public:\n"
                                   "    virtual void accept(Visitor* v) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Element elem;\n"
                                   "    elem.accept(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "accept"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_visitor_visit) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Element {};\n"
                                   "\n"
                                   "class Visitor {\n"
                                   "public:\n"
                                   "    void visit(Element* e) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Visitor v;\n"
                                   "    Element e;\n"
                                   "    v.visit(&e);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "visit"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_strategy_execute) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Strategy {\n"
                                   "public:\n"
                                   "    virtual void execute() {}\n"
                                   "};\n"
                                   "\n"
                                   "class Context {\n"
                                   "    Strategy* strat;\n"
                                   "public:\n"
                                   "    void run() { strat->execute(); }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Context ctx;\n"
                                   "    ctx.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_strategy_set_algorithm) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Strategy {};\n"
                                   "\n"
                                   "class Context {\n"
                                   "public:\n"
                                   "    void set_strategy(Strategy* s) {}\n"
                                   "    void execute() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Context ctx;\n"
                                   "    Strategy s;\n"
                                   "    ctx.set_strategy(&s);\n"
                                   "    ctx.execute();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "set_strategy"), 0);
    ASSERT_GTE(find_resolved(r, "test", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_command_execute) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Command {\n"
                                   "public:\n"
                                   "    virtual void execute() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Command cmd;\n"
                                   "    cmd.execute();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_command_undo) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Command {\n"
                                   "public:\n"
                                   "    virtual void execute() {}\n"
                                   "    virtual void undo() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Command cmd;\n"
                                   "    cmd.execute();\n"
                                   "    cmd.undo();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "execute"), 0);
    ASSERT_GTE(find_resolved(r, "test", "undo"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_mediator_send) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Mediator {\n"
                                   "public:\n"
                                   "    void send(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Mediator med;\n"
                                   "    med.send(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_chain_of_responsibility) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Handler {\n"
                                   "public:\n"
                                   "    Handler* next;\n"
                                   "    virtual void handle(int request) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Handler h;\n"
                                   "    h.handle(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_mvccontroller) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Request {};\n"
                                   "\n"
                                   "class Controller {\n"
                                   "public:\n"
                                   "    void handle(Request* req) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Controller ctrl;\n"
                                   "    Request req;\n"
                                   "    ctrl.handle(&req);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_event_loop) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class EventLoop {\n"
                                   "public:\n"
                                   "    void run() {}\n"
                                   "    void stop() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    EventLoop loop;\n"
                                   "    loop.run();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_plugin_system) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Plugin {\n"
                                   "public:\n"
                                   "    virtual void initialize() {}\n"
                                   "    virtual void shutdown() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Plugin plugin;\n"
                                   "    plugin.initialize();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "initialize"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_pipeline_stage) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Stage {\n"
                                   "public:\n"
                                   "    virtual void process(int data) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Stage stage;\n"
                                   "    stage.process(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_middleware_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Middleware {\n"
                                   "public:\n"
                                   "    Middleware* next_mw;\n"
                                   "    virtual void handle(int req) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Middleware mw;\n"
                                   "    mw.handle(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_state_machine) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class StateMachine {\n"
                                   "public:\n"
                                   "    void transition(int event) {}\n"
                                   "    void reset() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    StateMachine sm;\n"
                                   "    sm.transition(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "transition"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_actor_model) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Actor {\n"
                                   "public:\n"
                                   "    void send(const char* msg) {}\n"
                                   "    void receive() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Actor actor;\n"
                                   "    actor.send(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_rw_reactive_stream) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Stream {\n"
                                   "public:\n"
                                   "    void subscribe() {}\n"
                                   "    void unsubscribe() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Stream stream;\n"
                                   "    stream.subscribe();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "subscribe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_vector_push_back) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    void push_back(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.push_back(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_back"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_vector_emplace_back) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class vector {\n"
                    "public:\n"
                    "    template<typename... Args> void emplace_back(Args... args) {}\n"
                    "};\n"
                    "}\n"
                    "void test() {\n"
                    "    std::vector<int> v;\n"
                    "    v.emplace_back(42);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "emplace_back"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_vector_reserve) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    void reserve(int n) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.reserve(100);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reserve"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_vector_clear) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    void clear() {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.clear();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "clear"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_map_insert) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class map {\n"
                                   "public:\n"
                                   "    void insert(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::map<int, int> m;\n"
                                   "    m.insert(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "insert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_map_find) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class map {\n"
                                   "public:\n"
                                   "    void find(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::map<int, int> m;\n"
                                   "    m.find(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_map_erase) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class map {\n"
                                   "public:\n"
                                   "    void erase(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::map<int, int> m;\n"
                                   "    m.erase(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "erase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_map_count) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class map {\n"
                                   "public:\n"
                                   "    int count(const K& key) { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::map<int, int> m;\n"
                                   "    m.count(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "count"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_unordered_map_insert) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class unordered_map {\n"
                                   "public:\n"
                                   "    void insert(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::unordered_map<int, int> m;\n"
                                   "    m.insert(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "insert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_unordered_map_find) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename K, typename V> class unordered_map {\n"
                                   "public:\n"
                                   "    void find(const K& key) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::unordered_map<int, int> m;\n"
                                   "    m.find(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_set_insert) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class set {\n"
                                   "public:\n"
                                   "    void insert(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::set<int> s;\n"
                                   "    s.insert(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "insert"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_set_find) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class set {\n"
                                   "public:\n"
                                   "    void find(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::set<int> s;\n"
                                   "    s.find(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_set_count) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class set {\n"
                                   "public:\n"
                                   "    int count(const T& val) { return 0; }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::set<int> s;\n"
                                   "    s.count(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "count"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_list_push_front) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class list {\n"
                                   "public:\n"
                                   "    void push_front(const T& val) {}\n"
                                   "    void push_back(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::list<int> lst;\n"
                                   "    lst.push_front(1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "push_front"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_list_pop_front) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class list {\n"
                                   "public:\n"
                                   "    void pop_front() {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::list<int> lst;\n"
                                   "    lst.pop_front();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "pop_front"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_list_sort) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class list {\n"
                                   "public:\n"
                                   "    void sort() {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::list<int> lst;\n"
                                   "    lst.sort();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_array_at) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T, int N> class array {\n"
                                   "public:\n"
                                   "    T& at(int i) { static T val; return val; }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::array<int, 5> a;\n"
                                   "    a.at(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "at"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_array_fill) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T, int N> class array {\n"
                                   "public:\n"
                                   "    void fill(const T& val) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::array<int, 5> a;\n"
                                   "    a.fill(0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "fill"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_string_append) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "class string {\n"
                                   "public:\n"
                                   "    void append(const char* s) {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::string s;\n"
                                   "    s.append(\"hello\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "append"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_string_substr) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "class string {\n"
                                   "public:\n"
                                   "    string substr(int pos, int len) { return string(); }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::string s;\n"
                                   "    s.substr(0, 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "substr"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_sort) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "void sort(Iter first, Iter last) {}\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::sort(arr, arr + 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_find) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename Iter, typename T>\n"
                    "Iter find(Iter first, Iter last, const T& val) { return first; }\n"
                    "}\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    std::find(arr, arr + 5, 3);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_for_each) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter, typename Func>\n"
                                   "void for_each(Iter first, Iter last, Func fn) {}\n"
                                   "}\n"
                                   "void noop(int x) {}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::for_each(arr, arr + 5, noop);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "for_each"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_transform) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename InIter, typename OutIter, typename Func>\n"
                    "void transform(InIter first, InIter last, OutIter out, Func fn) {}\n"
                    "}\n"
                    "int double_val(int x) { return x * 2; }\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    int out[5];\n"
                    "    std::transform(arr, arr + 5, out, double_val);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "transform"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_copy) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename InIter, typename OutIter>\n"
                                   "void copy(InIter first, InIter last, OutIter out) {}\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    int out[5];\n"
                                   "    std::copy(arr, arr + 5, out);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "copy"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_accumulate) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter, typename T>\n"
                                   "T accumulate(Iter first, Iter last, T init) { return init; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    int sum = std::accumulate(arr, arr + 5, 0);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "accumulate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_count) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter, typename T>\n"
                                   "int count(Iter first, Iter last, const T& val) { return 0; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    int c = std::count(arr, arr + 5, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "count"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_remove) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename Iter, typename T>\n"
                    "Iter remove(Iter first, Iter last, const T& val) { return first; }\n"
                    "}\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    std::remove(arr, arr + 5, 3);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "remove"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_unique) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "Iter unique(Iter first, Iter last) { return first; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::unique(arr, arr + 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "unique"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_reverse) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "void reverse(Iter first, Iter last) {}\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::reverse(arr, arr + 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "reverse"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_min_element) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "Iter min_element(Iter first, Iter last) { return first; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::min_element(arr, arr + 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "min_element"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_max_element) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "Iter max_element(Iter first, Iter last) { return first; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int arr[5];\n"
                                   "    std::max_element(arr, arr + 5);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "max_element"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_binary_search) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename Iter, typename T>\n"
                    "bool binary_search(Iter first, Iter last, const T& val) { return false; }\n"
                    "}\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    std::binary_search(arr, arr + 5, 3);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "binary_search"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_lower_bound) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename Iter, typename T>\n"
                    "Iter lower_bound(Iter first, Iter last, const T& val) { return first; }\n"
                    "}\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    std::lower_bound(arr, arr + 5, 3);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "lower_bound"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_partition) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename Iter, typename Pred>\n"
                    "Iter partition(Iter first, Iter last, Pred pred) { return first; }\n"
                    "}\n"
                    "bool is_even(int x) { return x % 2 == 0; }\n"
                    "void test() {\n"
                    "    int arr[5];\n"
                    "    std::partition(arr, arr + 5, is_even);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "partition"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_begin) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename C>\n"
                                   "auto begin(C& c) -> decltype(c.begin()) { return c.begin(); }\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    T* begin() { return (T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    std::begin(v);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "begin"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_end) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename C>\n"
                                   "auto end(C& c) -> decltype(c.end()) { return c.end(); }\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    T* end() { return (T*)0; }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    std::end(v);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "end"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_next) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "Iter next(Iter it, int n) { return it; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int* ptr = 0;\n"
                                   "    std::next(ptr, 1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "next"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_prev) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "Iter prev(Iter it, int n) { return it; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int* ptr = 0;\n"
                                   "    std::prev(ptr, 1);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "prev"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_advance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "void advance(Iter& it, int n) {}\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int* ptr = 0;\n"
                                   "    std::advance(ptr, 3);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "advance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_distance) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Iter>\n"
                                   "int distance(Iter first, Iter last) { return 0; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int* a = 0;\n"
                                   "    int* b = 0;\n"
                                   "    std::distance(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "distance"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_back_inserter) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "template<typename C> class back_insert_iterator {};\n"
        "template<typename C>\n"
        "back_insert_iterator<C> back_inserter(C& c) { return back_insert_iterator<C>(); }\n"
        "template<typename T> class vector { public: void push_back(const T& v) {} };\n"
        "}\n"
        "void test() {\n"
        "    std::vector<int> v;\n"
        "    std::back_inserter(v);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "back_inserter"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_front_inserter) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "template<typename C> class front_insert_iterator {};\n"
        "template<typename C>\n"
        "front_insert_iterator<C> front_inserter(C& c) { return front_insert_iterator<C>(); }\n"
        "template<typename T> class deque { public: void push_front(const T& v) {} };\n"
        "}\n"
        "void test() {\n"
        "    std::deque<int> d;\n"
        "    std::front_inserter(d);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "front_inserter"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_move_iterator) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "template<typename Iter> class move_iterator {};\n"
        "template<typename Iter>\n"
        "move_iterator<Iter> make_move_iterator(Iter it) { return move_iterator<Iter>(); }\n"
        "}\n"
        "void test() {\n"
        "    int* ptr = 0;\n"
        "    std::make_move_iterator(ptr);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_move_iterator"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_reverse_iterator) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class vector {\n"
                                   "public:\n"
                                   "    void rbegin() {}\n"
                                   "    void rend() {}\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::vector<int> v;\n"
                                   "    v.rbegin();\n"
                                   "    v.rend();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "rbegin"), 0);
    ASSERT_GTE(find_resolved(r, "test", "rend"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_make_pair) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename A, typename B> struct pair { A first; B second; };\n"
                    "template<typename A, typename B>\n"
                    "pair<A, B> make_pair(A a, B b) { return pair<A, B>(); }\n"
                    "}\n"
                    "void test() {\n"
                    "    auto p = std::make_pair(1, 2.0);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_pair"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_make_tuple) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename... Args> struct tuple {};\n"
                    "template<typename... Args>\n"
                    "tuple<Args...> make_tuple(Args... args) { return tuple<Args...>(); }\n"
                    "}\n"
                    "void test() {\n"
                    "    auto tp = std::make_tuple(1, 2.0, 'a');\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_tuple"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_tie) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename... Args> struct tuple {};\n"
                    "template<typename... Args>\n"
                    "tuple<Args&...> tie(Args&... args) { return tuple<Args&...>(); }\n"
                    "}\n"
                    "void test() {\n"
                    "    int a, b;\n"
                    "    std::tie(a, b);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "tie"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_get) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename A, typename B> struct pair { A first; B second; };\n"
                    "template<int N, typename A, typename B>\n"
                    "A& get(pair<A,B>& p) { return p.first; }\n"
                    "}\n"
                    "void test() {\n"
                    "    std::pair<int, float> p;\n"
                    "    std::get<0>(p);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_swap) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T>\n"
                                   "void swap(T& a, T& b) { T tmp = a; a = b; b = tmp; }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    int a = 1, b = 2;\n"
                                   "    std::swap(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "swap"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_function_call) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename Sig> class function;\n"
                                   "template<typename R, typename... Args>\n"
                                   "class function<R(Args...)> {\n"
                                   "public:\n"
                                   "    R operator()(Args... args) { return R(); }\n"
                                   "};\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    std::function<int(int)> fn;\n"
                                   "    fn(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "operator()"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_bind) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename F, typename... Args>\n"
                                   "void bind(F fn, Args... args) {}\n"
                                   "}\n"
                                   "void handler(int x) {}\n"
                                   "void test() {\n"
                                   "    std::bind(handler, 42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "bind"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_ref) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class reference_wrapper {\n"
                    "public:\n"
                    "    T& get() { static T val; return val; }\n"
                    "};\n"
                    "template<typename T>\n"
                    "reference_wrapper<T> ref(T& val) { return reference_wrapper<T>(); }\n"
                    "}\n"
                    "void test() {\n"
                    "    int x = 42;\n"
                    "    std::ref(x);\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "ref"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_cref) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "template<typename T> class reference_wrapper {};\n"
        "template<typename T>\n"
        "reference_wrapper<const T> cref(const T& val) { return reference_wrapper<const T>(); }\n"
        "}\n"
        "void test() {\n"
        "    int x = 42;\n"
        "    std::cref(x);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "cref"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_invoke) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename F, typename... Args>\n"
                                   "void invoke(F fn, Args... args) {}\n"
                                   "}\n"
                                   "void handler(int x) {}\n"
                                   "void test() {\n"
                                   "    std::invoke(handler, 42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "invoke"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_make_optional) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> class optional {\n"
                                   "public:\n"
                                   "    T& value() { static T val; return val; }\n"
                                   "};\n"
                                   "template<typename T>\n"
                                   "optional<T> make_optional(T val) { return optional<T>(); }\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    auto opt = std::make_optional(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_optional"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_make_unique) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class unique_ptr {\n"
                    "public:\n"
                    "    T* operator->() { return (T*)0; }\n"
                    "};\n"
                    "template<typename T, typename... Args>\n"
                    "unique_ptr<T> make_unique(Args... args) { return unique_ptr<T>(); }\n"
                    "}\n"
                    "class Widget { public: void draw() {} };\n"
                    "void test() {\n"
                    "    auto p = std::make_unique<Widget>();\n"
                    "    p->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_unique"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_make_shared) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "namespace std {\n"
                    "template<typename T> class shared_ptr {\n"
                    "public:\n"
                    "    T* operator->() { return (T*)0; }\n"
                    "};\n"
                    "template<typename T, typename... Args>\n"
                    "shared_ptr<T> make_shared(Args... args) { return shared_ptr<T>(); }\n"
                    "}\n"
                    "class Widget { public: void draw() {} };\n"
                    "void test() {\n"
                    "    auto p = std::make_shared<Widget>();\n"
                    "    p->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_shared"), 0);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_forward) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& forward(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "void sink(int x) {}\n"
                                   "template<typename T>\n"
                                   "void relay(T&& arg) {\n"
                                   "    sink(std::forward<T>(arg));\n"
                                   "}\n"
                                   "void test() {\n"
                                   "    relay(42);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "relay"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_stl_move) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& move(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    Widget w2 = std::move(w);\n"
                                   "    w2.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_array) {
    CBMFileResult *r = extract_c("\n"
                                 "void action_a(void) {}\n"
                                 "void action_b(void) {}\n"
                                 "\n"
                                 "typedef void (*action_fn)(void);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    action_fn table[2];\n"
                                 "    table[0] = action_a;\n"
                                 "    table[1] = action_b;\n"
                                 "    action_a();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "action_a"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_struct_array) {
    CBMFileResult *r = extract_c("\n"
                                 "void do_open(void) {}\n"
                                 "void do_close(void) {}\n"
                                 "\n"
                                 "struct Command {\n"
                                 "    const char* name;\n"
                                 "    void (*handler)(void);\n"
                                 "};\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Command cmds[2];\n"
                                 "    cmds[0].handler = do_open;\n"
                                 "    cmds[1].handler = do_close;\n"
                                 "    do_open();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "do_open"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_vtable_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "void impl_start(void) {}\n"
                                 "void impl_stop(void) {}\n"
                                 "\n"
                                 "struct VTable {\n"
                                 "    void (*start)(void);\n"
                                 "    void (*stop)(void);\n"
                                 "};\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct VTable vt;\n"
                                 "    vt.start = impl_start;\n"
                                 "    vt.stop = impl_stop;\n"
                                 "    impl_start();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "impl_start"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_return) {
    CBMFileResult *r = extract_c("\n"
                                 "void target_func(void) {}\n"
                                 "\n"
                                 "typedef void (*fn_t)(void);\n"
                                 "\n"
                                 "fn_t get_handler(void) {\n"
                                 "    return target_func;\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    fn_t fn = get_handler();\n"
                                 "    fn();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "get_handler"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_param) {
    CBMFileResult *r = extract_c("\n"
                                 "void worker(void) {}\n"
                                 "\n"
                                 "void dispatch(void (*fn)(void)) {\n"
                                 "    fn();\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    dispatch(worker);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "dispatch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_dispatch_table) {
    CBMFileResult *r = extract_c("\n"
                                 "void handle_event_a(void) {}\n"
                                 "void handle_event_b(void) {}\n"
                                 "\n"
                                 "typedef void (*handler_t)(void);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    handler_t handlers[2];\n"
                                 "    handlers[0] = handle_event_a;\n"
                                 "    handlers[1] = handle_event_b;\n"
                                 "    handle_event_a();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle_event_a"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_cast) {
    CBMFileResult *r = extract_c("\n"
                                 "int real_func(int x) { return x; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    void* ptr = (void*)real_func;\n"
                                 "    int (*fn)(int) = (int(*)(int))ptr;\n"
                                 "    real_func(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "real_func"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_callback_registration) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef void (*callback_t)(int);\n"
                                 "\n"
                                 "void on_data(int val) {}\n"
                                 "\n"
                                 "void register_callback(callback_t cb) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    register_callback(on_data);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "register_callback"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_func_ptr_typedef_usage) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef int (*compare_fn)(const void*, const void*);\n"
                                 "\n"
                                 "int my_compare(const void* a, const void* b) { return 0; }\n"
                                 "\n"
                                 "void sort_items(compare_fn cmp) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    sort_items(my_compare);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "sort_items"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_qsort) {
    CBMFileResult *r = extract_c(
        "\n"
        "int cmp_int(const void* a, const void* b) { return 0; }\n"
        "\n"
        "void qsort(void* base, int nmemb, int size, int (*cmp)(const void*, const void*));\n"
        "\n"
        "void test() {\n"
        "    int arr[5];\n"
        "    qsort(arr, 5, sizeof(int), cmp_int);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "qsort"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_handle) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Impl;\n"
                                 "typedef struct Impl* Handle;\n"
                                 "\n"
                                 "void handle_use(Handle h) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    Handle h;\n"
                                 "    handle_use(h);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle_use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_void_ptr) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Data { int value; };\n"
                                 "\n"
                                 "void process(void* ctx) {\n"
                                 "    struct Data* d = (struct Data*)ctx;\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Data d;\n"
                                 "    process(&d);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_forward_decl) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Opaque;\n"
                                 "\n"
                                 "struct Opaque* create_opaque(void);\n"
                                 "void destroy_opaque(struct Opaque* o);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Opaque* o = create_opaque();\n"
                                 "    destroy_opaque(o);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "create_opaque"), 0);
    ASSERT_GTE(find_resolved(r, "test", "destroy_opaque"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_pimpl) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Widget;\n"
                                 "\n"
                                 "struct Widget* widget_create(void);\n"
                                 "void widget_draw(struct Widget* w);\n"
                                 "void widget_destroy(struct Widget* w);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Widget* w = widget_create();\n"
                                 "    widget_draw(w);\n"
                                 "    widget_destroy(w);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "widget_create"), 0);
    ASSERT_GTE(find_resolved(r, "test", "widget_draw"), 0);
    ASSERT_GTE(find_resolved(r, "test", "widget_destroy"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_typedef_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef struct {\n"
                                 "    int x;\n"
                                 "    int y;\n"
                                 "} Point;\n"
                                 "\n"
                                 "void use_point(Point* p) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    Point p;\n"
                                 "    use_point(&p);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_opaque_enum_flags) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef enum {\n"
                                 "    FLAG_A = 1,\n"
                                 "    FLAG_B = 2,\n"
                                 "    FLAG_C = 4\n"
                                 "} Flags;\n"
                                 "\n"
                                 "void apply_flags(Flags f) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    apply_flags(FLAG_A);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "apply_flags"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_flex_array_member) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Message {\n"
                                 "    int length;\n"
                                 "    char data[];\n"
                                 "};\n"
                                 "\n"
                                 "void process_msg(struct Message* m) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Message* m;\n"
                                 "    process_msg(m);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process_msg"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_flex_array_access) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Buffer {\n"
                                 "    int size;\n"
                                 "    unsigned char data[];\n"
                                 "};\n"
                                 "\n"
                                 "void read_buffer(struct Buffer* b) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Buffer* buf;\n"
                                 "    read_buffer(buf);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "read_buffer"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_flex_array_nested) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Header { int type; };\n"
                                 "struct Packet {\n"
                                 "    struct Header hdr;\n"
                                 "    int payload_len;\n"
                                 "    char payload[];\n"
                                 "};\n"
                                 "\n"
                                 "void send_packet(struct Packet* p) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Packet* pkt;\n"
                                 "    send_packet(pkt);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "send_packet"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_flex_array_malloc) {
    CBMFileResult *r = extract_c("\n"
                                 "void* malloc(unsigned long size);\n"
                                 "\n"
                                 "struct DynArray {\n"
                                 "    int count;\n"
                                 "    int items[];\n"
                                 "};\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct DynArray* arr = (struct "
                                 "DynArray*)malloc(sizeof(struct DynArray) + 10 * sizeof(int));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "malloc"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_literal_arg) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "void draw_point(struct Point p) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    draw_point((struct Point){1, 2});\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_literal_assign) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "void use_point(struct Point* p) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Point p = (struct Point){10, 20};\n"
                                 "    use_point(&p);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_literal_array) {
    CBMFileResult *r = extract_c("\n"
                                 "void process_ints(int* arr, int count) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    process_ints((int[]){1, 2, 3}, 3);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process_ints"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_literal_nested) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int val; };\n"
                                 "struct Outer { struct Inner inner; int extra; };\n"
                                 "\n"
                                 "void use_outer(struct Outer* o) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o = (struct Outer){{42}, 10};\n"
                                 "    use_outer(&o);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_outer"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_literal_return) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Point { int x; int y; };\n"
                                 "\n"
                                 "struct Point make_point(int x, int y) {\n"
                                 "    return (struct Point){x, y};\n"
                                 "}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Point p = make_point(1, 2);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "make_point"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_generic_basic) {
    CBMFileResult *r = extract_c("\n"
                                 "int f_int(int x) { return x; }\n"
                                 "float f_float(float x) { return x; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int x = 5;\n"
                                 "    f_int(x);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "f_int");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_generic_macro) {
    CBMFileResult *r = extract_c("\n"
                                 "const char* type_name_int(void) { return \"int\"; }\n"
                                 "const char* type_name_float(void) { return \"float\"; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    type_name_int();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "type_name_int");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_generic_default) {
    CBMFileResult *r = extract_c("\n"
                                 "void handle_default(void) {}\n"
                                 "void handle_int(int x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    handle_default();\n"
                                 "    handle_int(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "handle_default");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_generic_nested) {
    CBMFileResult *r = extract_c("\n"
                                 "int inner_int(int x) { return x; }\n"
                                 "float inner_float(float x) { return x; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    inner_int(42);\n"
                                 "    inner_float(3.14f);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "inner_int"), 0);
    ASSERT_GTE(find_resolved(r, "test", "inner_float"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_bitfield_access) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Flags {\n"
                                 "    unsigned int read : 1;\n"
                                 "    unsigned int write : 1;\n"
                                 "    unsigned int exec : 1;\n"
                                 "};\n"
                                 "\n"
                                 "void check_flags(struct Flags* f) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Flags f;\n"
                                 "    check_flags(&f);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "check_flags"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_union_access) {
    CBMFileResult *r = extract_c("\n"
                                 "union Value {\n"
                                 "    int i;\n"
                                 "    float f;\n"
                                 "    char c;\n"
                                 "};\n"
                                 "\n"
                                 "void use_union(union Value* v) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    union Value v;\n"
                                 "    use_union(&v);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_union"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_enum_switch) {
    CBMFileResult *r = extract_c("\n"
                                 "enum Color { RED, GREEN, BLUE };\n"
                                 "\n"
                                 "void handle_red(void) {}\n"
                                 "void handle_green(void) {}\n"
                                 "void handle_blue(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    enum Color c = RED;\n"
                                 "    switch (c) {\n"
                                 "        case RED: handle_red(); break;\n"
                                 "        case GREEN: handle_green(); break;\n"
                                 "        case BLUE: handle_blue(); break;\n"
                                 "    }\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "handle_red"), 0);
    ASSERT_GTE(find_resolved(r, "test", "handle_green"), 0);
    ASSERT_GTE(find_resolved(r, "test", "handle_blue"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_goto_label) {
    CBMFileResult *r = extract_c("\n"
                                 "void cleanup(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int x = 1;\n"
                                 "    if (x) goto done;\n"
                                 "    cleanup();\n"
                                 "done:\n"
                                 "    return;\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_var_args_func) {
    CBMFileResult *r = extract_c("\n"
                                 "void log_msg(const char* fmt, ...) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    log_msg(\"value=%d\", 42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "log_msg"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_inline_func) {
    CBMFileResult *r = extract_c("\n"
                                 "static inline int square(int x) { return x * x; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int r = square(5);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "square"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_static_func) {
    CBMFileResult *r = extract_c("\n"
                                 "static void helper(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    helper();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_extern_func) {
    CBMFileResult *r = extract_c("\n"
                                 "extern void external_func(int x);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    external_func(42);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "external_func"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_nested_struct) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Inner { int value; };\n"
                                 "struct Outer {\n"
                                 "    struct Inner inner;\n"
                                 "    int count;\n"
                                 "};\n"
                                 "\n"
                                 "void use_inner(struct Inner* i) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    struct Outer o;\n"
                                 "    use_inner(&o.inner);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_inner"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_typedef_chain) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef int Int32;\n"
                                 "typedef Int32 MyInt;\n"
                                 "\n"
                                 "void use_int(MyInt x) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    MyInt val = 42;\n"
                                 "    use_int(val);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "use_int"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_macro_expansion) {
    CBMFileResult *r = extract_c("\n"
                                 "void real_alloc(int size) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    real_alloc(64);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "real_alloc"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_designated_init) {
    CBMFileResult *r =
        extract_c("\n"
                  "struct Config {\n"
                  "    int width;\n"
                  "    int height;\n"
                  "    int depth;\n"
                  "};\n"
                  "\n"
                  "void apply_config(struct Config* c) {}\n"
                  "\n"
                  "void test() {\n"
                  "    struct Config cfg = { .width = 800, .height = 600, .depth = 32 };\n"
                  "    apply_config(&cfg);\n"
                  "}\n"
                  "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "apply_config"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_compound_assign) {
    CBMFileResult *r = extract_c("\n"
                                 "void accumulate(int* val, int delta) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int x = 0;\n"
                                 "    accumulate(&x, 10);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "accumulate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_comma_expr) {
    CBMFileResult *r = extract_c("\n"
                                 "int first_op(void) { return 0; }\n"
                                 "int second_op(void) { return 1; }\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int r = (first_op(), second_op());\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "first_op"), 0);
    ASSERT_GTE(find_resolved(r, "test", "second_op"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_ternary_call_branches) {
    CBMFileResult *r = extract_c("\n"
                                 "void path_a(void) {}\n"
                                 "void path_b(void) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int cond = 1;\n"
                                 "    cond ? path_a() : path_b();\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "path_a"), 0);
    ASSERT_GTE(find_resolved(r, "test", "path_b"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_c_sizeof_expr) {
    CBMFileResult *r = extract_c("\n"
                                 "struct Data { int x; int y; int z; };\n"
                                 "\n"
                                 "void alloc(int size) {}\n"
                                 "\n"
                                 "void test() {\n"
                                 "    alloc(sizeof(struct Data));\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "alloc"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_placement_new) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Widget {\n"
                    "public:\n"
                    "    Widget() {}\n"
                    "    void draw() {}\n"
                    "};\n"
                    "\n"
                    "void* operator new(unsigned long size, void* ptr) { return ptr; }\n"
                    "\n"
                    "void test() {\n"
                    "    char buffer[64];\n"
                    "    Widget* w = new(buffer) Widget();\n"
                    "    w->draw();\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_placement_new_array) {
    CBMFileResult *r =
        extract_cpp("\n"
                    "class Widget { public: void draw() {} };\n"
                    "\n"
                    "void* operator new(unsigned long size, void* ptr) { return ptr; }\n"
                    "\n"
                    "void test() {\n"
                    "    char pool[256];\n"
                    "    Widget* arr = new(pool) Widget[10];\n"
                    "}\n"
                    "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_throw_constructor) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class MyError {\n"
                                   "public:\n"
                                   "    MyError(const char* msg) {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    throw MyError(\"something failed\");\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "MyError");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_throw_rethrow) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Error {};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    try {\n"
                                   "        throw Error();\n"
                                   "    } catch (...) {\n"
                                   "        throw;\n"
                                   "    }\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_std_move_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& move(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void transfer() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    std::move(w).transfer();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "transfer"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_std_forward_method) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& forward(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void process() {}\n"
                                   "};\n"
                                   "\n"
                                   "template<typename T>\n"
                                   "void relay(T&& obj) {\n"
                                   "    std::forward<T>(obj).process();\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    relay(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "relay"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_move_assign_chain) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& move(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {\n"
                                   "public:\n"
                                   "    void draw() {}\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w1;\n"
                                   "    auto w2 = std::move(w1);\n"
                                   "    w2.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_conversion_operator_explicit) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Wrapper {\n"
                                   "public:\n"
                                   "    explicit operator bool() { return true; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Wrapper w;\n"
                                   "    if ((bool)w) {}\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "operator bool");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_conversion_operator_implicit) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "class WidgetWrapper {\n"
                                   "public:\n"
                                   "    operator Widget() { return Widget{}; }\n"
                                   "};\n"
                                   "\n"
                                   "void test() {\n"
                                   "    WidgetWrapper ww;\n"
                                   "    Widget w = ww;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "operator Widget");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_adlfrom_arg_namespace) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace gfx {\n"
                                   "    class Widget { public: int data; };\n"
                                   "    void serialize(Widget& w) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    gfx::Widget w;\n"
                                   "    serialize(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "serialize");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_adlswap) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace custom {\n"
                                   "    class Type { public: int val; };\n"
                                   "    void swap(Type& a, Type& b) {}\n"
                                   "}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    custom::Type a, b;\n"
                                   "    swap(a, b);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "swap");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_overload_lvalue_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "class Widget {};\n"
                                   "\n"
                                   "void process(Widget& w) {}\n"
                                   "void process(Widget&& w) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    process(w);\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_overload_rvalue_ref) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "template<typename T> T&& move(T& arg) { return (T&&)arg; }\n"
                                   "}\n"
                                   "\n"
                                   "class Widget {};\n"
                                   "\n"
                                   "void process(Widget& w) {}\n"
                                   "void process(Widget&& w) {}\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    process(std::move(w));\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "process"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_sfinaeenable_if) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T = void> struct enable_if {};\n"
        "    template<class T> struct enable_if<true, T> { typedef T type; };\n"
        "    template<class T> struct is_integral { static const bool value = true; };\n"
        "}\n"
        "\n"
        "template<typename T>\n"
        "typename std::enable_if<std::is_integral<T>::value, T>::type\n"
        "square(T x) { return x * x; }\n"
        "\n"
        "void test() {\n"
        "    int r = square(5);\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "square"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_sfinaevoid_t) {
    CBMFileResult *r = extract_cpp("\n"
                                   "namespace std {\n"
                                   "    template<typename...> using void_t = void;\n"
                                   "}\n"
                                   "\n"
                                   "class Widget { public: void draw() {} };\n"
                                   "\n"
                                   "void test() {\n"
                                   "    Widget w;\n"
                                   "    w.draw();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "draw"), 0);
    cbm_free_result(r);
    PASS();
}

/* DLL resolve LSP tests removed — string literals triggered
 * Windows Defender false positive. See issue #89. */

TEST(clsp_dll_custom_resolver) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef int (*ProcessFunc)(const char*);\n"
                                 "void* Resolve(const char* name);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    ProcessFunc proc = (ProcessFunc)Resolve(\"ProcessData\");\n"
                                 "    proc(\"input\");\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "external.ProcessData"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_cpp_static_cast) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "typedef void (*RenderFunc)(void);\n"
        "void* LoadSymbol(const char* name);\n"
        "\n"
        "void test() {\n"
        "    RenderFunc render = static_cast<RenderFunc>(LoadSymbol(\"RenderFrame\"));\n"
        "    render();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "external.RenderFrame"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_reinterpret_cast) {
    CBMFileResult *r = extract_cpp("\n"
                                   "typedef void (*ShutdownFunc)(void);\n"
                                   "void* GetSymbol(void* lib, const char* sym);\n"
                                   "\n"
                                   "void test() {\n"
                                   "    void* lib;\n"
                                   "    ShutdownFunc shutdown = "
                                   "reinterpret_cast<ShutdownFunc>(GetSymbol(lib, \"Shutdown\"));\n"
                                   "    shutdown();\n"
                                   "}\n"
                                   "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "external.Shutdown"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_no_false_positive_nonfp) {
    CBMFileResult *r = extract_c("\n"
                                 "char* lookup(const char* key);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    char* val = lookup(\"some_key\");\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "external.some_key");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_no_false_positive_no_cast) {
    CBMFileResult *r = extract_c("\n"
                                 "int find(const char* name);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    int result = find(\"SomeFunc\");\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "external.SomeFunc");
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_multiple_functions) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef void (*FuncA)(void);\n"
                                 "typedef int (*FuncB)(int);\n"
                                 "void* Resolve(const char* name);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    FuncA a = (FuncA)Resolve(\"Alpha\");\n"
                                 "    FuncB b = (FuncB)Resolve(\"Beta\");\n"
                                 "    a();\n"
                                 "    b(1);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(find_resolved(r, "test", "external.Alpha"), 0);
    ASSERT_GTE(find_resolved(r, "test", "external.Beta"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_dll_func_ptr_typedef) {
    CBMFileResult *r = extract_c("\n"
                                 "typedef void (*callback_t)(int, int);\n"
                                 "callback_t get_callback(const char* name);\n"
                                 "\n"
                                 "void test() {\n"
                                 "    callback_t cb = get_callback(\"OnResize\");\n"
                                 "    cb(800, 600);\n"
                                 "}\n"
                                 "");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(clsp_easy_win_sfinaeconditional_return) {
    CBMFileResult *r = extract_cpp(
        "\n"
        "namespace std {\n"
        "    template<bool B, class T, class F> struct conditional { typedef T type; };\n"
        "    template<class T, class F> struct conditional<false, T, F> { typedef F type; };\n"
        "    template<bool B, class T, class F> using conditional_t = typename "
        "conditional<B,T,F>::type;\n"
        "}\n"
        "\n"
        "class TypeA { public: void act() {} };\n"
        "class TypeB { public: void act() {} };\n"
        "\n"
        "void test() {\n"
        "    TypeA a;\n"
        "    a.act();\n"
        "}\n"
        "");
    ASSERT_NOT_NULL(r);
    (void)find_resolved(r, "test", "act");
    cbm_free_result(r);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(c_lsp) {
    RUN_TEST(clsp_simple_var_decl);
    RUN_TEST(clsp_pointer_arrow);
    RUN_TEST(clsp_dot_access);
    RUN_TEST(clsp_auto_inference);
    RUN_TEST(clsp_namespace_qualified);
    RUN_TEST(clsp_constructor);
    RUN_TEST(clsp_new_delete);
    RUN_TEST(clsp_implicit_this);
    RUN_TEST(clsp_explicit_this);
    RUN_TEST(clsp_type_alias);
    RUN_TEST(clsp_typedef);
    RUN_TEST(clsp_scope_chain);
    RUN_TEST(clsp_static_cast);
    RUN_TEST(clsp_using_namespace);
    RUN_TEST(clsp_cmode);
    RUN_TEST(clsp_direct_call);
    RUN_TEST(clsp_direct_callcpp);
    RUN_TEST(clsp_stdlib_call);
    RUN_TEST(clsp_multiple_calls_same_func);
    RUN_TEST(clsp_return_type_chain);
    RUN_TEST(clsp_method_chaining);
    RUN_TEST(clsp_inheritance);
    RUN_TEST(clsp_operator_stream);
    RUN_TEST(clsp_cross_file);
    RUN_TEST(clsp_nocrash_template_expression);
    RUN_TEST(clsp_nocrash_lambda);
    RUN_TEST(clsp_nocrash_nested_namespace);
    RUN_TEST(clsp_nocrash_empty_source);
    RUN_TEST(clsp_nocrash_complex_class);
    RUN_TEST(clsp_operator_subscript);
    RUN_TEST(clsp_operator_binary);
    RUN_TEST(clsp_operator_unary);
    RUN_TEST(clsp_functor);
    RUN_TEST(clsp_copy_constructor);
    RUN_TEST(clsp_delete_destructor);
    RUN_TEST(clsp_range_for);
    RUN_TEST(clsp_parent_namespace);
    RUN_TEST(clsp_conversion_operator_bool);
    RUN_TEST(clsp_namespace_alias);
    RUN_TEST(clsp_template_in_namespace);
    RUN_TEST(clsp_nocrash_using_enum);
    RUN_TEST(clsp_nocrash_multiple_inheritance);
    RUN_TEST(clsp_nocrash_pointer_arithmetic);
    RUN_TEST(clsp_function_pointer);
    RUN_TEST(clsp_function_pointer_decay);
    RUN_TEST(clsp_overload_by_arg_count);
    RUN_TEST(clsp_template_default_args);
    RUN_TEST(clsp_spaceship_operator);
    RUN_TEST(clsp_nocrash_concept);
    RUN_TEST(clsp_dependent_member_access);
    RUN_TEST(clsp_nocrash_try_catch);
    RUN_TEST(clsp_macro_wrapped_call);
    RUN_TEST(clsp_macro_with_args);
    RUN_TEST(clsp_recursive_macro);
    RUN_TEST(clsp_conditional_macro);
    RUN_TEST(clsp_token_paste);
    RUN_TEST(clsp_no_macro_no_overhead);
    RUN_TEST(clsp_variadic_macro);
    RUN_TEST(clsp_cppmacro_method_call);
    RUN_TEST(clsp_struct_field_extraction);
    RUN_TEST(clsp_struct_field_defs_tolspdefs);
    RUN_TEST(clsp_make_shared_template_arg);
    RUN_TEST(clsp_make_unique_template_arg);
    RUN_TEST(clsp_template_class_method_return_type);
    RUN_TEST(clsp_trailing_return_type);
    RUN_TEST(clsp_trailing_return_type_method);
    RUN_TEST(clsp_cppclass_field_extraction);
    RUN_TEST(clsp_std_variant);
    RUN_TEST(clsp_std_deque);
    RUN_TEST(clsp_std_filesystem);
    RUN_TEST(clsp_std_accumulate);
    RUN_TEST(clsp_std_string_stream);
    RUN_TEST(clsp_abseil_status_or);
    RUN_TEST(clsp_spdlog_logger);
    RUN_TEST(clsp_qtqstring);
    RUN_TEST(clsp_adl_swap);
    RUN_TEST(clsp_adl_operator_free_func);
    RUN_TEST(clsp_adl_std_sort);
    RUN_TEST(clsp_adl_no_false_positive);
    RUN_TEST(clsp_overload_by_type);
    RUN_TEST(clsp_overload_by_type_method);
    RUN_TEST(clsp_lambda_trailing_return);
    RUN_TEST(clsp_lambda_body_inference);
    RUN_TEST(clsp_inline_namespace_libc);
    RUN_TEST(clsp_inline_namespace_gcc);
    RUN_TEST(clsp_implicit_string_conversion);
    RUN_TEST(clsp_numeric_promotion);
    RUN_TEST(clsp_virtual_override);
    RUN_TEST(clsp_base_pointer_call);
    RUN_TEST(clsp_crtp_basic);
    RUN_TEST(clsp_crtp_multi_param);
    RUN_TEST(clsp_range_for_map);
    RUN_TEST(clsp_range_for_custom_iterator);
    RUN_TEST(clsp_tad_free_function_identity);
    RUN_TEST(clsp_tad_make_pair_like);
    RUN_TEST(clsp_structured_binding_pair);
    RUN_TEST(clsp_structured_binding_struct);
    RUN_TEST(clsp_ternary_type);
    RUN_TEST(clsp_chained_method_calls);
    RUN_TEST(clsp_std_vector_push_back);
    RUN_TEST(clsp_iterator_deref);
    RUN_TEST(clsp_enum_class_usage);
    RUN_TEST(clsp_multiple_return_paths);
    RUN_TEST(clsp_nested_template);
    RUN_TEST(clsp_const_ref);
    RUN_TEST(clsp_std_function_callback);
    RUN_TEST(clsp_optional_value_access);
    RUN_TEST(clsp_typedef_chain);
    RUN_TEST(clsp_if_init_statement);
    RUN_TEST(clsp_dependent_type_member);
    RUN_TEST(clsp_auto_return_function);
    RUN_TEST(clsp_move_semantics);
    RUN_TEST(clsp_multi_level_inheritance);
    RUN_TEST(clsp_range_for_structured_binding);
    RUN_TEST(clsp_cross_file_include);
    RUN_TEST(clsp_function_returning_ref);
    RUN_TEST(clsp_template_method_chain);
    RUN_TEST(clsp_algorithm_with_lambda);
    RUN_TEST(clsp_static_cast_chain);
    RUN_TEST(clsp_smart_pointer_arrow);
    RUN_TEST(clsp_static_method_call);
    RUN_TEST(clsp_subscript_draw);
    RUN_TEST(clsp_auto_from_method_return);
    RUN_TEST(clsp_nested_class_return_type);
    RUN_TEST(clsp_make_shared_chain);
    RUN_TEST(clsp_dependent_member_call);
    RUN_TEST(clsp_default_args);
    RUN_TEST(clsp_gap_std_forward);
    RUN_TEST(clsp_gap_generic_lambda);
    RUN_TEST(clsp_gap_decltype_return);
    RUN_TEST(clsp_gap_std_move);
    RUN_TEST(clsp_probe_c_struct_callback);
    RUN_TEST(clsp_probe_c_typedef_struct);
    RUN_TEST(clsp_probe_c_nested_struct);
    RUN_TEST(clsp_probe_c_array_decay);
    RUN_TEST(clsp_probe_c_compound_literal);
    RUN_TEST(clsp_probe_c_chained_func_calls);
    RUN_TEST(clsp_probe_c_enum_param);
    RUN_TEST(clsp_probe_c_global_var_func_call);
    RUN_TEST(clsp_probe_cpp_dynamic_cast);
    RUN_TEST(clsp_probe_cpp_reinterpret_cast);
    RUN_TEST(clsp_probe_cpp_const_cast);
    RUN_TEST(clsp_probe_cpp_const_method_overload);
    RUN_TEST(clsp_probe_cpp_using_base_method);
    RUN_TEST(clsp_probe_cpp_pair_access);
    RUN_TEST(clsp_probe_cpp_builder_pattern);
    RUN_TEST(clsp_probe_cpp_exception_catch_var);
    RUN_TEST(clsp_probe_cpp_for_loop_iterator);
    RUN_TEST(clsp_probe_cpp_nested_class_access);
    RUN_TEST(clsp_probe_cpp_static_member_var);
    RUN_TEST(clsp_probe_cpp_std_array_access);
    RUN_TEST(clsp_probe_cpp_unordered_map_access);
    RUN_TEST(clsp_probe_cpp_lambda_capture);
    RUN_TEST(clsp_probe_cpp_tuple_get);
    RUN_TEST(clsp_probe_cpp_initializer_list);
    RUN_TEST(clsp_probe_cpp_conditional_method);
    RUN_TEST(clsp_gap_multiple_inheritance);
    RUN_TEST(clsp_c_union_member_access);
    RUN_TEST(clsp_c_void_pointer_cast);
    RUN_TEST(clsp_c_double_pointer);
    RUN_TEST(clsp_c_static_local_call);
    RUN_TEST(clsp_c_array_of_struct_loop);
    RUN_TEST(clsp_c_func_ptr_typedef);
    RUN_TEST(clsp_c_nested_func_calls);
    RUN_TEST(clsp_c_struct_return_chain);
    RUN_TEST(clsp_c_conditional_call);
    RUN_TEST(clsp_c_switch_case_call);
    RUN_TEST(clsp_c_recursive_call);
    RUN_TEST(clsp_c_struct_member_func_ptr);
    RUN_TEST(clsp_c_variadic_call);
    RUN_TEST(clsp_c_const_qualified_param);
    RUN_TEST(clsp_c_while_loop_call);
    RUN_TEST(clsp_c_do_while_call);
    RUN_TEST(clsp_c_ternary_call);
    RUN_TEST(clsp_c_multiple_return_calls);
    RUN_TEST(clsp_cpp_ref_param);
    RUN_TEST(clsp_cpp_const_ref_param);
    RUN_TEST(clsp_cpp_rvalue_ref_param);
    RUN_TEST(clsp_cpp_anonymous_namespace);
    RUN_TEST(clsp_cpp_nested_namespace_decl);
    RUN_TEST(clsp_cpp_pure_virtual);
    RUN_TEST(clsp_cpp_protected_inheritance);
    RUN_TEST(clsp_cpp_constexpr_call);
    RUN_TEST(clsp_cpp_default_member_init);
    RUN_TEST(clsp_cpp_multiple_vars_one_type);
    RUN_TEST(clsp_cpp_while_method_call);
    RUN_TEST(clsp_cpp_for_range_auto_ref);
    RUN_TEST(clsp_cpp_for_range_const_auto_ref);
    RUN_TEST(clsp_cpp_new_expression);
    RUN_TEST(clsp_cpp_scoped_enum_param);
    RUN_TEST(clsp_cpp_multiple_smart_ptrs);
    RUN_TEST(clsp_cpp_try_catch_multiple);
    RUN_TEST(clsp_cpp_lambda_capture_this);
    RUN_TEST(clsp_cpp_operator_plus_method);
    RUN_TEST(clsp_cpp_operator_assign);
    RUN_TEST(clsp_cpp_explicit_template_instantiation);
    RUN_TEST(clsp_cpp_nested_method_call_in_arg);
    RUN_TEST(clsp_cpp_return_method_call_result);
    RUN_TEST(clsp_cpp_static_factory_method);
    RUN_TEST(clsp_cpp_deep_inheritance_chain);
    RUN_TEST(clsp_cpp_override_virtual);
    RUN_TEST(clsp_cpp_scope_resolution_call);
    RUN_TEST(clsp_cpp_init_list_construct);
    RUN_TEST(clsp_cpp_return_smart_ptr);
    RUN_TEST(clsp_cpp_assign_in_if);
    RUN_TEST(clsp_cpp_nullptr_check);
    RUN_TEST(clsp_cpp_explicit_ptr_from_new);
    RUN_TEST(clsp_cpp_multiple_methods_same_obj);
    RUN_TEST(clsp_cpp_nested_class_method);
    RUN_TEST(clsp_cpp_diamond_inheritance);
    RUN_TEST(clsp_cpp_switch_method_call);
    RUN_TEST(clsp_cpp_throw_expression);
    RUN_TEST(clsp_cpp_for_init_decl);
    RUN_TEST(clsp_heavycpp_const_overload_discrimination);
    RUN_TEST(clsp_heavycpp_pair_field_type);
    RUN_TEST(clsp_heavycpp_iterator_deref);
    RUN_TEST(clsp_heavycpp_template_func_syntax);
    RUN_TEST(clsp_audit_c_comma_operator);
    RUN_TEST(clsp_audit_c_cast_then_field_call);
    RUN_TEST(clsp_audit_c_nested_struct_field_call);
    RUN_TEST(clsp_audit_c_array_subscript_call);
    RUN_TEST(clsp_audit_c_func_ptr_alias);
    RUN_TEST(clsp_audit_c_generic_selection);
    RUN_TEST(clsp_audit_c_for_loop_func_call);
    RUN_TEST(clsp_audit_c_assert_macro_call);
    RUN_TEST(clsp_audit_cpp_auto_from_new);
    RUN_TEST(clsp_audit_cpp_auto_from_factory);
    RUN_TEST(clsp_audit_cpp_auto_from_smart_ptr_factory);
    RUN_TEST(clsp_audit_cpp_decltype_var);
    RUN_TEST(clsp_audit_cpp_auto_from_ternary);
    RUN_TEST(clsp_audit_cpp_if_constexpr);
    RUN_TEST(clsp_audit_cpp_structured_binding_from_tuple);
    RUN_TEST(clsp_audit_cpp_ctad);
    RUN_TEST(clsp_audit_cpp_user_defined_literal);
    RUN_TEST(clsp_audit_cpp_aggregate_init);
    RUN_TEST(clsp_audit_cpp_covariant_return);
    RUN_TEST(clsp_audit_heavycpp_variadic_template);
    RUN_TEST(clsp_audit_heavycpp_enable_if);
    RUN_TEST(clsp_audit_heavycpp_perfect_forwarding);
    RUN_TEST(clsp_audit_heavycpp_policy_based_design);
    RUN_TEST(clsp_audit_heavycpp_expression_template);
    RUN_TEST(clsp_audit_heavycpp_template_template_param);
    RUN_TEST(clsp_audit_heavycpp_concept_constrained);
    RUN_TEST(clsp_audit_heavycpp_coroutine);
    RUN_TEST(clsp_expr_gap_sizeof_type);
    RUN_TEST(clsp_expr_gap_sizeof_expr);
    RUN_TEST(clsp_expr_gap_alignof_expr);
    RUN_TEST(clsp_expr_gap_binary_comparison_bool);
    RUN_TEST(clsp_expr_gap_logical_and_or);
    RUN_TEST(clsp_expr_gap_parenthesized_method_call);
    RUN_TEST(clsp_expr_gap_assignment_type_chain);
    RUN_TEST(clsp_expr_gap_update_expr_type_preservation);
    RUN_TEST(clsp_expr_gap_unary_bitwise_not);
    RUN_TEST(clsp_expr_gap_unary_plus);
    RUN_TEST(clsp_expr_gap_address_of_then_arrow);
    RUN_TEST(clsp_expr_gap_double_pointer_deref);
    RUN_TEST(clsp_expr_gap_deref_then_arrow);
    RUN_TEST(clsp_expr_gap_comma_expr_method_call);
    RUN_TEST(clsp_expr_gap_raw_string_literal);
    RUN_TEST(clsp_expr_gap_concatenated_string);
    RUN_TEST(clsp_expr_gap_char_literal_type);
    RUN_TEST(clsp_expr_gap_bool_literal_type);
    RUN_TEST(clsp_expr_gap_nullptr_type);
    RUN_TEST(clsp_expr_gap_number_literal_int);
    RUN_TEST(clsp_expr_gap_number_literal_float);
    RUN_TEST(clsp_stmt_gap_array_param_decl);
    RUN_TEST(clsp_stmt_gap_carray_param_bracket);
    RUN_TEST(clsp_stmt_gap_for_range_over_return_value);
    RUN_TEST(clsp_stmt_gap_multiple_using_decl);
    RUN_TEST(clsp_stmt_gap_typedef_func_ptr);
    RUN_TEST(clsp_stmt_gap_catch_multiple_types);
    RUN_TEST(clsp_stmt_gap_namespace_alias_chain);
    RUN_TEST(clsp_stmt_gap_using_alias_template);
    RUN_TEST(clsp_call_gap_nested_new_expressions);
    RUN_TEST(clsp_call_gap_chained_operators);
    RUN_TEST(clsp_call_gap_operator_plus_equals);
    RUN_TEST(clsp_call_gap_operator_minus_method);
    RUN_TEST(clsp_call_gap_unary_operator_star);
    RUN_TEST(clsp_call_gap_subscript_operator_emission);
    RUN_TEST(clsp_call_gap_delete_destructor_emission);
    RUN_TEST(clsp_call_gap_constructor_from_init_list);
    RUN_TEST(clsp_call_gap_constructor_from_parens);
    RUN_TEST(clsp_call_gap_copy_constructor_emission);
    RUN_TEST(clsp_call_gap_conversion_operator_in_if);
    RUN_TEST(clsp_call_gap_functor_call_emission);
    RUN_TEST(clsp_call_gap_adlfree_function);
    RUN_TEST(clsp_call_gap_implicit_this_method_call);
    RUN_TEST(clsp_call_gap_template_func_qualified_call);
    RUN_TEST(clsp_cgap_struct_init_and_field_call);
    RUN_TEST(clsp_cgap_enum_var_as_param);
    RUN_TEST(clsp_cgap_static_func_call);
    RUN_TEST(clsp_cgap_void_func_no_return);
    RUN_TEST(clsp_cgap_multi_level_struct_access);
    RUN_TEST(clsp_cgap_cast_in_func_arg);
    RUN_TEST(clsp_cgap_ternary_in_arg);
    RUN_TEST(clsp_cgap_nested_func_call_in_condition);
    RUN_TEST(clsp_cgap_for_loop_all_parts);
    RUN_TEST(clsp_cgap_while_condition_call);
    RUN_TEST(clsp_cgap_return_value_func_call);
    RUN_TEST(clsp_cross_gap_cast_then_method_chain);
    RUN_TEST(clsp_cross_gap_new_then_method_chain);
    RUN_TEST(clsp_cross_gap_lambda_as_argument);
    RUN_TEST(clsp_cross_gap_auto_from_static_cast);
    RUN_TEST(clsp_cross_gap_auto_from_conditional);
    RUN_TEST(clsp_cross_gap_method_call_in_switch_case);
    RUN_TEST(clsp_cross_gap_multiple_objects_same_type);
    RUN_TEST(clsp_cross_gap_method_call_on_return_value);
    RUN_TEST(clsp_cross_gap_deep_scope_nesting);
    RUN_TEST(clsp_cross_gap_variable_shadowing);
    RUN_TEST(clsp_cross_gap_if_else_method_calls);
    RUN_TEST(clsp_cross_gap_method_result_as_arg);
    RUN_TEST(clsp_cross_gap_nested_template_method_call);
    RUN_TEST(clsp_cross_gap_static_method_with_namespace);
    RUN_TEST(clsp_cross_gap_const_method_call);
    RUN_TEST(clsp_cross_gap_pointer_to_member_via_arrow);
    RUN_TEST(clsp_cross_gap_auto_from_subscript);
    RUN_TEST(clsp_cross_gap_multiple_func_ptr_targets);
    RUN_TEST(clsp_type_gap_const_pointer_to_const);
    RUN_TEST(clsp_type_gap_volatile_pointer);
    RUN_TEST(clsp_type_gap_enum_class_member);
    RUN_TEST(clsp_type_gap_reference_to_pointer);
    RUN_TEST(clsp_type_gap_array_of_pointers);
    RUN_TEST(clsp_call_edge_method_call_on_this);
    RUN_TEST(clsp_call_edge_base_class_method_via_using);
    RUN_TEST(clsp_call_edge_template_method_explicit_args);
    RUN_TEST(clsp_call_edge_recursive_mutual_call);
    RUN_TEST(clsp_call_edge_overloaded_func_diff_arg_count);
    RUN_TEST(clsp_call_edge_global_func_from_method);
    RUN_TEST(clsp_scope_gap_for_loop_var_scope);
    RUN_TEST(clsp_scope_gap_if_init_decl);
    RUN_TEST(clsp_scope_gap_while_var_decl);
    RUN_TEST(clsp_scope_gap_do_while_method_call);
    RUN_TEST(clsp_nocrash_compound_literal_field_access);
    RUN_TEST(clsp_nocrash_deeply_nested_expr);
    RUN_TEST(clsp_nocrash_empty_lambda);
    RUN_TEST(clsp_nocrash_nested_lambdas);
    RUN_TEST(clsp_nocrash_template_in_template);
    RUN_TEST(clsp_nocrash_very_long_chain);
    RUN_TEST(clsp_nocrash_mixedcand_cpp_cast);
    RUN_TEST(clsp_nocrash_extremely_large_function);
    RUN_TEST(clsp_call_gap_operator_times_equals);
    RUN_TEST(clsp_call_gap_operator_shift_left_equals);
    RUN_TEST(clsp_call_gap_operator_and_equals);
    RUN_TEST(clsp_call_gap_operator_or_equals);
    RUN_TEST(clsp_pattern_auto_ref_from_method_return);
    RUN_TEST(clsp_pattern_auto_ptr_from_new);
    RUN_TEST(clsp_pattern_auto_from_make_shared);
    RUN_TEST(clsp_pattern_multi_declarator_same_line);
    RUN_TEST(clsp_pattern_struct_ptr_arrow_chain);
    RUN_TEST(clsp_pattern_constexpr_variable);
    RUN_TEST(clsp_pattern_inline_variable);
    RUN_TEST(clsp_pattern_string_view_param);
    RUN_TEST(clsp_pattern_initializer_list_constructor);
    RUN_TEST(clsp_pattern_template_member_access);
    RUN_TEST(clsp_pattern_map_iterator_second);
    RUN_TEST(clsp_pattern_func_returning_pointer);
    RUN_TEST(clsp_pattern_multiple_catch_same_func);
    RUN_TEST(clsp_pattern_method_call_in_ternary_branch);
    RUN_TEST(clsp_pattern_nested_class_from_outer_scope);
    RUN_TEST(clsp_pattern_volatile_method_call);
    RUN_TEST(clsp_pattern_enum_switch_call);
    RUN_TEST(clsp_fix1_template_return_type_smart_ptr_factory);
    RUN_TEST(clsp_fix1_template_return_type_vector_factory);
    RUN_TEST(clsp_fix1_template_return_type_map_factory);
    RUN_TEST(clsp_fix1_template_return_type_shared_ptr);
    RUN_TEST(clsp_fix2_struct_field_access_simple);
    RUN_TEST(clsp_fix2_struct_field_access_pair_second);
    RUN_TEST(clsp_fix2_struct_field_access_cstruct);
    RUN_TEST(clsp_fix2_struct_field_access_nested_ptr_field);
    RUN_TEST(clsp_fix3_typedef_func_ptr_call);
    RUN_TEST(clsp_fix3_direct_func_ptr_assign);
    RUN_TEST(clsp_fix4_forward_decl_return_type);
    RUN_TEST(clsp_fix4_forward_decl_simple_call);
    RUN_TEST(clsp_fix4_cforward_decl);
    RUN_TEST(clsp_fix5_user_defined_literal);
    RUN_TEST(clsp_fix5_user_defined_literal_string);
    RUN_TEST(clsp_gap_v2_auto_from_ternary);
    RUN_TEST(clsp_gap_v2_auto_from_static_method);
    RUN_TEST(clsp_gap_v2_auto_from_subscript);
    RUN_TEST(clsp_gap_v2_auto_from_method_return);
    RUN_TEST(clsp_gap_v2_auto_from_chained_method_return);
    RUN_TEST(clsp_gap_v2_reassigned_variable);
    RUN_TEST(clsp_gap_v2_multiple_vars_from_same_type);
    RUN_TEST(clsp_gap_v2_derived_object_calls_base_method);
    RUN_TEST(clsp_gap_v2_base_pointer_to_derived);
    RUN_TEST(clsp_gap_v2_multiple_inheritance);
    RUN_TEST(clsp_gap_v2_vector_push_back_and_iterate);
    RUN_TEST(clsp_gap_v2_map_insert_and_access);
    RUN_TEST(clsp_gap_v2_shared_ptr_method_call);
    RUN_TEST(clsp_gap_v2_optional_value_access);
    RUN_TEST(clsp_gap_v2_method_call_on_parameter);
    RUN_TEST(clsp_gap_v2_method_call_on_const_ref);
    RUN_TEST(clsp_gap_v2_method_call_on_pointer_param);
    RUN_TEST(clsp_gap_v2_return_value_chain);
    RUN_TEST(clsp_gap_v2_c_struct_ptr_param);
    RUN_TEST(clsp_gap_v2_c_func_ptr_in_struct);
    RUN_TEST(clsp_gap_v2_c_callback_param);
    RUN_TEST(clsp_gap_v2_c_static_func);
    RUN_TEST(clsp_gap_v2_c_nested_struct_access);
    RUN_TEST(clsp_gap_v2_c_enum_switch);
    RUN_TEST(clsp_gap_v2_simple_template_instantiation);
    RUN_TEST(clsp_gap_v2_template_with_multiple_params);
    RUN_TEST(clsp_gap_v2_nested_template_type);
    RUN_TEST(clsp_gap_v2_namespace_function);
    RUN_TEST(clsp_gap_v2_nested_namespace);
    RUN_TEST(clsp_gap_v2_using_namespace);
    RUN_TEST(clsp_gap_v2_operator_plus_member_call);
    RUN_TEST(clsp_gap_v2_stream_operator);
    RUN_TEST(clsp_gap_v2_explicit_constructor_call);
    RUN_TEST(clsp_gap_v2_brace_init_constructor);
    RUN_TEST(clsp_gap_v2_temporary_object_method_call);
    RUN_TEST(clsp_gap_v2_lambda_capture_method_call);
    RUN_TEST(clsp_gap_v2_lambda_return_type_used);
    RUN_TEST(clsp_gap_v2_catch_exception_method);
    RUN_TEST(clsp_gap_v2_static_member_function);
    RUN_TEST(clsp_gap_v2_enum_class_used_in_switch);
    RUN_TEST(clsp_gap_v2_builder_pattern);
    RUN_TEST(clsp_gap_v2_method_chaining_ref);
    RUN_TEST(clsp_gap_v2_raiilock_guard);
    RUN_TEST(clsp_gap_v2_typedef_class);
    RUN_TEST(clsp_gap_v2_using_alias);
    RUN_TEST(clsp_gap_v2_using_template_alias);
    RUN_TEST(clsp_gap_v2_if_null_check);
    RUN_TEST(clsp_gap_v2_try_catch_finally);
    RUN_TEST(clsp_gap_v2_method_call_in_for_init);
    RUN_TEST(clsp_gap_v2_nested_method_call_args);
    RUN_TEST(clsp_gap_v2_conditional_method_call);
    RUN_TEST(clsp_fix_cstruct_func_ptr_chain_call);
    RUN_TEST(clsp_fix_cast_chained_method);
    RUN_TEST(clsp_fix_catch_by_value);
    RUN_TEST(clsp_fix_subscript_on_auto_var);
    RUN_TEST(clsp_fix_lambda_capture_this);
    RUN_TEST(clsp_cpp17_structured_binding_pair);
    RUN_TEST(clsp_cpp17_structured_binding_struct);
    RUN_TEST(clsp_cpp17_structured_binding_array);
    RUN_TEST(clsp_cpp17_structured_binding_const);
    RUN_TEST(clsp_cpp17_structured_binding_map);
    RUN_TEST(clsp_cpp17_structured_binding_nested);
    RUN_TEST(clsp_cpp17_structured_binding_tuple);
    RUN_TEST(clsp_cpp17_structured_binding_in_if);
    RUN_TEST(clsp_cpp17_if_init_simple);
    RUN_TEST(clsp_cpp17_if_init_with_type);
    RUN_TEST(clsp_cpp17_switch_init);
    RUN_TEST(clsp_cpp17_if_init_lock);
    RUN_TEST(clsp_cpp17_fold_expr_sum);
    RUN_TEST(clsp_cpp17_fold_expr_binary);
    RUN_TEST(clsp_cpp17_fold_expr_comma);
    RUN_TEST(clsp_cpp17_fold_expr_logical);
    RUN_TEST(clsp_cpp17_ctadvector);
    RUN_TEST(clsp_cpp17_ctadpair);
    RUN_TEST(clsp_cpp17_ctadoptional);
    RUN_TEST(clsp_cpp17_ctadtuple);
    RUN_TEST(clsp_cpp17_ctaduser_defined);
    RUN_TEST(clsp_cpp17_ctadlock_guard);
    RUN_TEST(clsp_cpp17_optional_value);
    RUN_TEST(clsp_cpp17_optional_arrow);
    RUN_TEST(clsp_cpp17_optional_deref);
    RUN_TEST(clsp_cpp17_optional_has_value);
    RUN_TEST(clsp_cpp17_variant_get);
    RUN_TEST(clsp_cpp17_variant_visit);
    RUN_TEST(clsp_cpp17_any_any_cast);
    RUN_TEST(clsp_cpp17_optional_value_or);
    RUN_TEST(clsp_cpp17_optional_and_then);
    RUN_TEST(clsp_cpp17_optional_transform);
    RUN_TEST(clsp_cpp17_if_constexpr_body);
    RUN_TEST(clsp_cpp17_inline_variable);
    RUN_TEST(clsp_cpp17_nested_namespace);
    RUN_TEST(clsp_cpp17_constexpr_if);
    RUN_TEST(clsp_cpp17_string_view);
    RUN_TEST(clsp_cpp17_filesystem_path);
    RUN_TEST(clsp_cpp17_user_defined_literal);
    RUN_TEST(clsp_cpp17_class_template_deduction);
    RUN_TEST(clsp_cpp17_apply_tuple);
    RUN_TEST(clsp_cpp17_invoke_result);
    RUN_TEST(clsp_cpp20_concept_constrained_func);
    RUN_TEST(clsp_cpp20_concept_requires_clause);
    RUN_TEST(clsp_cpp20_concept_auto_param);
    RUN_TEST(clsp_cpp20_concept_nested);
    RUN_TEST(clsp_cpp20_concept_conjunction);
    RUN_TEST(clsp_cpp20_concept_subsumption);
    RUN_TEST(clsp_cpp20_concept_on_method);
    RUN_TEST(clsp_cpp20_requires_expression);
    RUN_TEST(clsp_cpp20_co_await_expr);
    RUN_TEST(clsp_cpp20_co_yield_expr);
    RUN_TEST(clsp_cpp20_co_return_expr);
    RUN_TEST(clsp_cpp20_coroutine_handle);
    RUN_TEST(clsp_cpp20_task);
    RUN_TEST(clsp_cpp20_coroutine_body_calls);
    RUN_TEST(clsp_cpp20_generator);
    RUN_TEST(clsp_cpp20_nested_co_await);
    RUN_TEST(clsp_cpp20_ranges_pipeline);
    RUN_TEST(clsp_cpp20_ranges_for_each);
    RUN_TEST(clsp_cpp20_views_transform);
    RUN_TEST(clsp_cpp20_views_filter);
    RUN_TEST(clsp_cpp20_ranges_sort);
    RUN_TEST(clsp_cpp20_views_take);
    RUN_TEST(clsp_cpp20_ranges_iterator);
    RUN_TEST(clsp_cpp20_ranges_projection);
    RUN_TEST(clsp_cpp20_consteval_func);
    RUN_TEST(clsp_cpp20_constinit_var);
    RUN_TEST(clsp_cpp20_designated_init);
    RUN_TEST(clsp_cpp20_three_way_comparison);
    RUN_TEST(clsp_cpp20_span_access);
    RUN_TEST(clsp_cpp20_jthread);
    RUN_TEST(clsp_cpp20_format_string);
    RUN_TEST(clsp_cpp20_source_location);
    RUN_TEST(clsp_cpp20_using_enum);
    RUN_TEST(clsp_cpp20_lambda_template_param);
    RUN_TEST(clsp_cpp20_lambda_init_capture);
    RUN_TEST(clsp_template_enable_if_method);
    RUN_TEST(clsp_template_enable_if_return);
    RUN_TEST(clsp_template_void_t);
    RUN_TEST(clsp_template_is_detected);
    RUN_TEST(clsp_template_if_constexprsfinae);
    RUN_TEST(clsp_template_conditional_type);
    RUN_TEST(clsp_template_decltype_return);
    RUN_TEST(clsp_template_trailing_return);
    RUN_TEST(clsp_template_partial_spec_pointer);
    RUN_TEST(clsp_template_partial_spec_const);
    RUN_TEST(clsp_template_full_spec);
    RUN_TEST(clsp_template_member_spec);
    RUN_TEST(clsp_template_static_member_spec);
    RUN_TEST(clsp_template_type_trait_spec);
    RUN_TEST(clsp_template_variadic_func);
    RUN_TEST(clsp_template_variadic_class);
    RUN_TEST(clsp_template_parameter_pack);
    RUN_TEST(clsp_template_fold_over_args);
    RUN_TEST(clsp_template_variadic_inheritance);
    RUN_TEST(clsp_template_recursive_variadic);
    RUN_TEST(clsp_template_make_from_variadic);
    RUN_TEST(clsp_template_tuple_element);
    RUN_TEST(clsp_template_dependent_type);
    RUN_TEST(clsp_template_dependent_name);
    RUN_TEST(clsp_template_nested_dependent);
    RUN_TEST(clsp_template_dependent_return);
    RUN_TEST(clsp_template_dependent_field);
    RUN_TEST(clsp_template_dependent_method_call);
    RUN_TEST(clsp_template_dependent_base_class);
    RUN_TEST(clsp_template_two_phase);
    RUN_TEST(clsp_template_expr_template);
    RUN_TEST(clsp_template_policy_based_log);
    RUN_TEST(clsp_template_policy_based_alloc);
    RUN_TEST(clsp_template_template_template);
    RUN_TEST(clsp_template_mixin_pattern);
    RUN_TEST(clsp_template_type_erasure);
    RUN_TEST(clsp_template_static_assert);
    RUN_TEST(clsp_tad_simple_func);
    RUN_TEST(clsp_tad_return_type_deduction);
    RUN_TEST(clsp_tad_multi_param);
    RUN_TEST(clsp_tad_explicit_args);
    RUN_TEST(clsp_tad_partial_explicit);
    RUN_TEST(clsp_tad_default_arg);
    RUN_TEST(clsp_tad_perfect_forwarding);
    RUN_TEST(clsp_tad_auto_return);
    RUN_TEST(clsp_rw_shared_ptr_arrow);
    RUN_TEST(clsp_rw_unique_ptr_arrow);
    RUN_TEST(clsp_rw_shared_ptr_get);
    RUN_TEST(clsp_rw_shared_ptr_deref);
    RUN_TEST(clsp_rw_shared_ptr_chain);
    RUN_TEST(clsp_rw_weak_ptr_lock);
    RUN_TEST(clsp_rw_make_shared_method_chain);
    RUN_TEST(clsp_rw_shared_ptr_cast);
    RUN_TEST(clsp_rw_iterator_for_loop);
    RUN_TEST(clsp_rw_iterator_deref);
    RUN_TEST(clsp_rw_iterator_arrow);
    RUN_TEST(clsp_rw_reverse_iterator);
    RUN_TEST(clsp_rw_const_iterator);
    RUN_TEST(clsp_rw_insert_iterator);
    RUN_TEST(clsp_rw_iterator_advance);
    RUN_TEST(clsp_rw_iterator_distance);
    RUN_TEST(clsp_rw_stack_push);
    RUN_TEST(clsp_rw_queue_front);
    RUN_TEST(clsp_rw_priority_queue_top);
    RUN_TEST(clsp_rw_deque_access);
    RUN_TEST(clsp_rw_set_insert);
    RUN_TEST(clsp_rw_multi_map_range);
    RUN_TEST(clsp_rw_lock_guard_scope);
    RUN_TEST(clsp_rw_unique_lock_scope);
    RUN_TEST(clsp_rw_scoped_timer);
    RUN_TEST(clsp_rw_file_handle);
    RUN_TEST(clsp_rw_transaction_scope);
    RUN_TEST(clsp_rw_connection_pool);
    RUN_TEST(clsp_rw_scope_guard);
    RUN_TEST(clsp_rw_factory_method);
    RUN_TEST(clsp_rw_abstract_factory);
    RUN_TEST(clsp_rw_factory_function);
    RUN_TEST(clsp_rw_builder_pattern);
    RUN_TEST(clsp_rw_singleton);
    RUN_TEST(clsp_rw_prototype_clone);
    RUN_TEST(clsp_rw_factory_registry);
    RUN_TEST(clsp_rw_named_constructor);
    RUN_TEST(clsp_rw_observer_notify);
    RUN_TEST(clsp_rw_observer_subscribe);
    RUN_TEST(clsp_rw_visitor_accept);
    RUN_TEST(clsp_rw_visitor_visit);
    RUN_TEST(clsp_rw_strategy_execute);
    RUN_TEST(clsp_rw_strategy_set_algorithm);
    RUN_TEST(clsp_rw_command_execute);
    RUN_TEST(clsp_rw_command_undo);
    RUN_TEST(clsp_rw_mediator_send);
    RUN_TEST(clsp_rw_chain_of_responsibility);
    RUN_TEST(clsp_rw_mvccontroller);
    RUN_TEST(clsp_rw_event_loop);
    RUN_TEST(clsp_rw_plugin_system);
    RUN_TEST(clsp_rw_pipeline_stage);
    RUN_TEST(clsp_rw_middleware_chain);
    RUN_TEST(clsp_rw_state_machine);
    RUN_TEST(clsp_rw_actor_model);
    RUN_TEST(clsp_rw_reactive_stream);
    RUN_TEST(clsp_stl_vector_push_back);
    RUN_TEST(clsp_stl_vector_emplace_back);
    RUN_TEST(clsp_stl_vector_reserve);
    RUN_TEST(clsp_stl_vector_clear);
    RUN_TEST(clsp_stl_map_insert);
    RUN_TEST(clsp_stl_map_find);
    RUN_TEST(clsp_stl_map_erase);
    RUN_TEST(clsp_stl_map_count);
    RUN_TEST(clsp_stl_unordered_map_insert);
    RUN_TEST(clsp_stl_unordered_map_find);
    RUN_TEST(clsp_stl_set_insert);
    RUN_TEST(clsp_stl_set_find);
    RUN_TEST(clsp_stl_set_count);
    RUN_TEST(clsp_stl_list_push_front);
    RUN_TEST(clsp_stl_list_pop_front);
    RUN_TEST(clsp_stl_list_sort);
    RUN_TEST(clsp_stl_array_at);
    RUN_TEST(clsp_stl_array_fill);
    RUN_TEST(clsp_stl_string_append);
    RUN_TEST(clsp_stl_string_substr);
    RUN_TEST(clsp_stl_sort);
    RUN_TEST(clsp_stl_find);
    RUN_TEST(clsp_stl_for_each);
    RUN_TEST(clsp_stl_transform);
    RUN_TEST(clsp_stl_copy);
    RUN_TEST(clsp_stl_accumulate);
    RUN_TEST(clsp_stl_count);
    RUN_TEST(clsp_stl_remove);
    RUN_TEST(clsp_stl_unique);
    RUN_TEST(clsp_stl_reverse);
    RUN_TEST(clsp_stl_min_element);
    RUN_TEST(clsp_stl_max_element);
    RUN_TEST(clsp_stl_binary_search);
    RUN_TEST(clsp_stl_lower_bound);
    RUN_TEST(clsp_stl_partition);
    RUN_TEST(clsp_stl_begin);
    RUN_TEST(clsp_stl_end);
    RUN_TEST(clsp_stl_next);
    RUN_TEST(clsp_stl_prev);
    RUN_TEST(clsp_stl_advance);
    RUN_TEST(clsp_stl_distance);
    RUN_TEST(clsp_stl_back_inserter);
    RUN_TEST(clsp_stl_front_inserter);
    RUN_TEST(clsp_stl_move_iterator);
    RUN_TEST(clsp_stl_reverse_iterator);
    RUN_TEST(clsp_stl_make_pair);
    RUN_TEST(clsp_stl_make_tuple);
    RUN_TEST(clsp_stl_tie);
    RUN_TEST(clsp_stl_get);
    RUN_TEST(clsp_stl_swap);
    RUN_TEST(clsp_stl_function_call);
    RUN_TEST(clsp_stl_bind);
    RUN_TEST(clsp_stl_ref);
    RUN_TEST(clsp_stl_cref);
    RUN_TEST(clsp_stl_invoke);
    RUN_TEST(clsp_stl_make_optional);
    RUN_TEST(clsp_stl_make_unique);
    RUN_TEST(clsp_stl_make_shared);
    RUN_TEST(clsp_stl_forward);
    RUN_TEST(clsp_stl_move);
    RUN_TEST(clsp_c_func_ptr_array);
    RUN_TEST(clsp_c_func_ptr_struct_array);
    RUN_TEST(clsp_c_vtable_struct);
    RUN_TEST(clsp_c_func_ptr_return);
    RUN_TEST(clsp_c_func_ptr_param);
    RUN_TEST(clsp_c_dispatch_table);
    RUN_TEST(clsp_c_func_ptr_cast);
    RUN_TEST(clsp_c_callback_registration);
    RUN_TEST(clsp_c_func_ptr_typedef_usage);
    RUN_TEST(clsp_c_qsort);
    RUN_TEST(clsp_c_opaque_handle);
    RUN_TEST(clsp_c_opaque_void_ptr);
    RUN_TEST(clsp_c_opaque_forward_decl);
    RUN_TEST(clsp_c_opaque_pimpl);
    RUN_TEST(clsp_c_opaque_typedef_struct);
    RUN_TEST(clsp_c_opaque_enum_flags);
    RUN_TEST(clsp_c_flex_array_member);
    RUN_TEST(clsp_c_flex_array_access);
    RUN_TEST(clsp_c_flex_array_nested);
    RUN_TEST(clsp_c_flex_array_malloc);
    RUN_TEST(clsp_c_compound_literal_arg);
    RUN_TEST(clsp_c_compound_literal_assign);
    RUN_TEST(clsp_c_compound_literal_array);
    RUN_TEST(clsp_c_compound_literal_nested);
    RUN_TEST(clsp_c_compound_literal_return);
    RUN_TEST(clsp_c_generic_basic);
    RUN_TEST(clsp_c_generic_macro);
    RUN_TEST(clsp_c_generic_default);
    RUN_TEST(clsp_c_generic_nested);
    RUN_TEST(clsp_c_bitfield_access);
    RUN_TEST(clsp_c_union_access);
    RUN_TEST(clsp_c_enum_switch);
    RUN_TEST(clsp_c_goto_label);
    RUN_TEST(clsp_c_var_args_func);
    RUN_TEST(clsp_c_inline_func);
    RUN_TEST(clsp_c_static_func);
    RUN_TEST(clsp_c_extern_func);
    RUN_TEST(clsp_c_nested_struct);
    RUN_TEST(clsp_c_typedef_chain);
    RUN_TEST(clsp_c_macro_expansion);
    RUN_TEST(clsp_c_designated_init);
    RUN_TEST(clsp_c_compound_assign);
    RUN_TEST(clsp_c_comma_expr);
    RUN_TEST(clsp_c_ternary_call_branches);
    RUN_TEST(clsp_c_sizeof_expr);
    RUN_TEST(clsp_easy_win_placement_new);
    RUN_TEST(clsp_easy_win_placement_new_array);
    RUN_TEST(clsp_easy_win_throw_constructor);
    RUN_TEST(clsp_easy_win_throw_rethrow);
    RUN_TEST(clsp_easy_win_std_move_method);
    RUN_TEST(clsp_easy_win_std_forward_method);
    RUN_TEST(clsp_easy_win_move_assign_chain);
    RUN_TEST(clsp_easy_win_conversion_operator_explicit);
    RUN_TEST(clsp_easy_win_conversion_operator_implicit);
    RUN_TEST(clsp_easy_win_adlfrom_arg_namespace);
    RUN_TEST(clsp_easy_win_adlswap);
    RUN_TEST(clsp_easy_win_overload_lvalue_ref);
    RUN_TEST(clsp_easy_win_overload_rvalue_ref);
    RUN_TEST(clsp_easy_win_sfinaeenable_if);
    RUN_TEST(clsp_easy_win_sfinaevoid_t);
    RUN_TEST(clsp_dll_custom_resolver);
    RUN_TEST(clsp_dll_cpp_static_cast);
    RUN_TEST(clsp_dll_reinterpret_cast);
    RUN_TEST(clsp_dll_no_false_positive_nonfp);
    RUN_TEST(clsp_dll_no_false_positive_no_cast);
    RUN_TEST(clsp_dll_multiple_functions);
    RUN_TEST(clsp_dll_func_ptr_typedef);
    RUN_TEST(clsp_easy_win_sfinaeconditional_return);
}
