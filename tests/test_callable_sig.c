/*
 * test_callable_sig.c — golden tables for the signature-qualified callable
 * identity builder and its inverse (#2061, plumbing: no language enabled).
 */
#include "test_framework.h"
#include "callable_sig.h"
#include "lang_specs.h"
#include "foundation/arena.h"
#include "pipeline/pipeline.h"
#include "pipeline/lsp_resolve.h"
#include "tree_sitter/api.h"
#include <string.h>

typedef struct {
    CBMLanguage lang;
    CBMCallableIdentity mode;
    const char *source;
    const char *kind; /* node kind of the callable */
    int nth;          /* 0-based occurrence of `kind` in preorder */
    const char *expected;
} sig_case_t;

static bool find_nth(TSNode n, const char *kind, int *left, TSNode *out) {
    if (strcmp(ts_node_type(n), kind) == 0) {
        if (*left == 0) {
            *out = n;
            return true;
        }
        (*left)--;
    }
    uint32_t count = ts_node_child_count(n);
    for (uint32_t i = 0; i < count; i++) {
        if (find_nth(ts_node_child(n, i), kind, left, out)) {
            return true;
        }
    }
    return false;
}

/* Parse `source`, build the nth `kind` node's suffix in `mode`; the returned
 * string is arena-owned (NULL when absent). */
static const char *sig_of(CBMArena *a, CBMLanguage lang, CBMCallableIdentity mode,
                          const char *source, const char *kind, int nth) {
    TSParser *parser = ts_parser_new();
    ts_parser_set_language(parser, cbm_ts_language(lang));
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    TSNode node = {0};
    int left = nth;
    const char *sig = NULL;
    if (tree && find_nth(ts_tree_root_node(tree), kind, &left, &node)) {
        sig = cbm_callable_sig_mode(a, node, source, lang, mode);
    }
    ts_tree_delete(tree);
    ts_parser_delete(parser);
    return sig;
}

static const sig_case_t k_cases[] = {
    /* Java: generics kept, qualifiers and annotations dropped, varargs = []. */
    {CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED,
     "class A { <T extends Comparable<T>> void work(final java.util.List<String> xs, int[] a, "
     "String... rest) {} }",
     "method_declaration", 0, "(List<String>,int[],String[])"},
    {CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, "class A { void f(@Ann Map<K, V>  m, int a[]){} }",
     "method_declaration", 0, "(Map<K,V>,int[])"},
    {CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED,
     "class A { void f(\n  @Ann Map< K ,\n V > m, /* c */ int a [ ]\n) {} }", "method_declaration",
     0, "(Map<K,V>,int[])"},
    {CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, "class A { A(int x){} void g(){} }",
     "constructor_declaration", 0, "(int)"},
    {CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, "class A { A(int x){} void g(){} }",
     "method_declaration", 0, "()"},
    /* C#: generic arity, ref/out, params inlined by the grammar, tuple names
     * dropped, operators and indexers. */
    {CBM_LANG_CSHARP, CBM_CALLABLE_ID_TYPED,
     "class A { public void Work<T, U>(ref int x, out string y, params object[] rest, int? n = 3, "
     "List<Dictionary<string,int>> m, (int a, string b) t, this Foo z) {} }",
     "method_declaration", 0,
     "<T,U>(ref int,out string,object[],int?,List<Dictionary<string,int>>,(int,string),Foo)"},
    {CBM_LANG_CSHARP, CBM_CALLABLE_ID_TYPED,
     "class A { public static A operator +(A a, A b) => a; }", "operator_declaration", 0, "(A,A)"},
    {CBM_LANG_CSHARP, CBM_CALLABLE_ID_TYPED, "class A { int this[int i] { get => 0; } }",
     "indexer_declaration", 0, "(int)"},
    /* C++: template kinds, declarator markers, top-level cv dropped, arrays
     * decay, function pointers, C variadic, packs, cvref, operators. */
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED,
     "struct S { template <typename T, int N = 3> void work(const std::vector<T> & v, "
     "int (*cb)(int), unsigned  long x, ...) const && noexcept; };",
     "template_declaration", 0,
     "<typename,int>(const vector<T>&,int(*)(int),unsigned long,~)const&&"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED, "struct S { S& operator=(S&&) &; };", "field_declaration",
     0, "(S&&)&"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED, "struct S { bool operator()(int) volatile; };",
     "field_declaration", 0, "(int)volatile"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED,
     "struct S { template<class... Args> void emplace(Args&&... args); };", "template_declaration",
     0, "<typename~>(Args&&~)"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED, "void n::S::arr(int a[10], char *const p, int q) {}",
     "function_definition", 0, "(int*,char*,int)"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED,
     "void w(\n    const  int   a ,   // trailing\n    char const * /* c */ const p\n);",
     "declaration", 0, "(int,const char*)"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED, "void v(void);", "declaration", 0, "()"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED, "void f(int (*(*g)(int x))(double), void h(char *s));",
     "declaration", 0, "(int(*(*)(int))(double),void(*)(char*))"},
    {CBM_LANG_CPP, CBM_CALLABLE_ID_TYPED,
     "template <class T> struct S { void f(int); };\ntemplate <class T> void S<T>::f(int x) {}",
     "function_definition", 0, "(int)"},
    /* Kotlin: extension receiver, vararg, function types, nullable,
     * qualified types, suspend. */
    {CBM_LANG_KOTLIN, CBM_CALLABLE_ID_TYPED,
     "class A {\n  fun <T : Comparable<T>> String.work(vararg xs: Int, f: (Int) -> Unit, "
     "m: Map<String, List<Int>>? = null, @Ann q: kotlin.collections.List<*>) {}\n}",
     "function_declaration", 0, "(this:String,Int~,(Int)=>Unit,Map<String,List<Int>>?,List<*>)"},
    {CBM_LANG_KOTLIN, CBM_CALLABLE_ID_TYPED, "fun top(x: Int) = x", "function_declaration", 0,
     "(Int)"},
    {CBM_LANG_KOTLIN, CBM_CALLABLE_ID_TYPED, "class A { constructor(x: Int) {} }",
     "secondary_constructor", 0, "(Int)"},
    /* Swift: labels + types (the #2061 Alamofire shape), variadic, inout,
     * attributes dropped, qualified types, init/subscript/operators. */
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED,
     "class Session {\n  func upload<T: Encodable>(_ data: Data, to url: URLConvertible, method: "
     "HTTPMethod = .post, headers: [String: String]? = nil, handler: @escaping (Int) -> Void, "
     "xs: Int..., io: inout Swift.Int, t: (a: Int, String)) -> UploadRequest { }\n}",
     "function_declaration", 0,
     "(_:Data,to:URLConvertible,method:HTTPMethod,headers:[String:String]?,handler:(Int)=>Void,"
     "xs:Int~,io:inout Int,t:(a:Int,String))"},
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED,
     "class Session {\n  func upload(_ data: Data, with request: URLRequest) {}\n}",
     "function_declaration", 0, "(_:Data,with:URLRequest)"},
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED, "class C { init(frame: CGRect) {} }",
     "init_declaration", 0, "(frame:CGRect)"},
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED,
     "class C { subscript(index i: Int) -> Int { return 0 } }", "subscript_declaration", 0,
     "(index:Int)"},
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED,
     "class C { static func + (l: C, r: C) -> C { l } }", "function_declaration", 0, "(l:C,r:C)"},
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_LABELED_TYPED, "func extra() {}", "function_declaration", 0,
     "()"},
    /* Scala: every clause flattened, by-name and repeated parameters. */
    {CBM_LANG_SCALA, CBM_CALLABLE_ID_TYPED,
     "class A {\n  def work[T <: AnyRef](xs: List[T], n: => Int, rest: String*)(implicit ord: "
     "Ordering[T]): Unit = {}\n}",
     "function_definition", 0, "(List[T],=>Int,String*,Ordering[T])"},
    {CBM_LANG_SCALA, CBM_CALLABLE_ID_TYPED,
     "class A {\n  def f(g: (Int, String) => scala.collection.immutable.Map[String, Int]): Unit\n}",
     "function_declaration", 0, "((Int,String)=>Map[String,Int])"},
    /* Objective-C: the selector's keyword labels. */
    {CBM_LANG_OBJC, CBM_CALLABLE_ID_LABELED,
     "@implementation Foo\n- (instancetype)initWithFrame:(CGRect)frame style:(NSInteger)style "
     "{ return self; }\n@end",
     "method_definition", 0, "(_:style:)"},
    {CBM_LANG_OBJC, CBM_CALLABLE_ID_LABELED, "@implementation Foo\n- (void)run { }\n@end",
     "method_definition", 0, "()"},
    {CBM_LANG_OBJC, CBM_CALLABLE_ID_LABELED,
     "@implementation Foo\n- (void)setX:(int)x :(int)y {}\n@end", "method_definition", 0, "(_:_:)"},
    {CBM_LANG_OBJC, CBM_CALLABLE_ID_LABELED,
     "@implementation Foo\n+ (id)make:(int)a, ... { return nil; }\n@end", "method_definition", 0,
     "(_:~)"},
    /* ARITY (dynamic tier-2 languages), shown on a Swift declaration. */
    {CBM_LANG_SWIFT, CBM_CALLABLE_ID_ARITY, "class C { func upload(_ data: Data, to url: URL) {} }",
     "function_declaration", 0, "(2)"},
};

TEST(callable_sig_golden_table) {
    for (size_t i = 0; i < sizeof(k_cases) / sizeof(k_cases[0]); i++) {
        const sig_case_t *tc = &k_cases[i];
        CBMArena a;
        cbm_arena_init(&a);
        const char *sig = sig_of(&a, tc->lang, tc->mode, tc->source, tc->kind, tc->nth);
        if (!sig || strcmp(sig, tc->expected) != 0) {
            printf("  case %zu: got %s, want %s\n", i, sig ? sig : "(null)", tc->expected);
        }
        ASSERT_NOT_NULL(sig);
        ASSERT_STR_EQ(sig, tc->expected);
        ASSERT_TRUE(strlen(sig) <= CBM_CALLABLE_SIG_MAX);
        /* Contract: no separator a leaf splitter uses, and the inverse
         * recovers the base from a real-shaped QN. */
        ASSERT_NULL(strchr(sig, '.'));
        ASSERT_NULL(strstr(sig, "::"));
        ASSERT_NULL(strstr(sig, "->"));
        char *qn = cbm_arena_sprintf(&a, "proj.pkg.Cls.name%s", sig);
        ASSERT_EQ(cbm_qn_callable_base_len(qn), strlen("proj.pkg.Cls.name"));
        cbm_arena_destroy(&a);
    }
    PASS();
}

/* Long signatures cap at CBM_CALLABLE_SIG_MAX with a hash of the FULL
 * suffix: deterministic, still distinct, still invertible. */
TEST(callable_sig_cap_keeps_identity) {
    char src_a[4096];
    char src_b[4096];
    size_t n = (size_t)snprintf(src_a, sizeof(src_a), "class A { void f(");
    for (int i = 0; i < 30; i++) {
        n += (size_t)snprintf(src_a + n, sizeof(src_a) - n, "%sVeryLongParameterTypeName%d p%d",
                              i ? ", " : "", i, i);
    }
    memcpy(src_b, src_a, n);
    snprintf(src_a + n, sizeof(src_a) - n, ", int last) {} }");
    snprintf(src_b + n, sizeof(src_b) - n, ", long last) {} }");
    CBMArena a;
    cbm_arena_init(&a);
    const char *sa =
        sig_of(&a, CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, src_a, "method_declaration", 0);
    const char *sa2 =
        sig_of(&a, CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, src_a, "method_declaration", 0);
    const char *sb =
        sig_of(&a, CBM_LANG_JAVA, CBM_CALLABLE_ID_TYPED, src_b, "method_declaration", 0);
    ASSERT_NOT_NULL(sa);
    ASSERT_NOT_NULL(sb);
    ASSERT_TRUE(strlen(sa) <= CBM_CALLABLE_SIG_MAX);
    ASSERT_TRUE(strlen(sb) <= CBM_CALLABLE_SIG_MAX);
    ASSERT_STR_EQ(sa, sa2);
    ASSERT_STR_NEQ(sa, sb); /* they differ only past the cap: the hash tells */
    ASSERT_TRUE(strncmp(sa, "(VeryLongParameterTypeName0,", 28) == 0);
    const char *hash = strchr(sa, '#');
    ASSERT_NOT_NULL(hash);
    ASSERT_EQ(strlen(hash), (size_t)18); /* "#" + 16 hex + ")" */
    char *qn = cbm_arena_sprintf(&a, "p.A.f%s", sa);
    ASSERT_EQ(cbm_qn_callable_base_len(qn), strlen("p.A.f"));
    cbm_arena_destroy(&a);
    PASS();
}

/* PR1 is plumbing: no language mints a suffix yet. */
TEST(callable_sig_every_language_is_none) {
    CBMArena a;
    cbm_arena_init(&a);
    const char *src = "class A { void f(int x) {} }";
    for (int lang = 0; lang < CBM_LANG_COUNT; lang++) {
        ASSERT_EQ(cbm_callable_identity((CBMLanguage)lang), CBM_CALLABLE_ID_NONE);
    }
    TSParser *parser = ts_parser_new();
    ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_JAVA));
    TSTree *tree = ts_parser_parse_string(parser, NULL, src, (uint32_t)strlen(src));
    TSNode m = {0};
    int left = 0;
    ASSERT_TRUE(find_nth(ts_tree_root_node(tree), "method_declaration", &left, &m));
    ASSERT_NULL(cbm_callable_sig(&a, m, src, CBM_LANG_JAVA));
    ts_tree_delete(tree);
    ts_parser_delete(parser);
    cbm_arena_destroy(&a);
    PASS();
}

/* The inverse is the identity on every historical QN shape. */
TEST(callable_sig_base_len_unsuffixed_is_full) {
    static const char *const qns[] = {
        "proj.pkg.Cls.method",
        "proj.S.operator()",
        "proj.S.operator<",
        "proj.S.operator<=>",
        "proj.S.operator->",
        "proj.S.operator>>",
        "proj.S.operator&&",
        "proj.app/(auth)",
        "proj.Makefile.$(OBJ)",
        "proj.f.(anon)",
        "",
        "proj.x.list<int>",
        "proj.x.make_const",
        "a::b::c",
        "proj.~Dtor",
        "(x)",
        "proj.Makefile.foo$(EXT)",
        "proj.lib/(group)/page",
        "proj.C.+",
        "proj.A.unary_!",
    };
    for (size_t i = 0; i < sizeof(qns) / sizeof(qns[0]); i++) {
        ASSERT_EQ(cbm_qn_callable_base_len(qns[i]), strlen(qns[i]));
    }
    ASSERT_EQ(cbm_qn_callable_base_len(NULL), (size_t)0);
    PASS();
}

TEST(callable_sig_base_len_suffixed) {
    static const struct {
        const char *qn;
        const char *base;
    } rows[] = {
        {"p.S.work(int,String)", "p.S.work"},
        {"p.S.work()", "p.S.work"},
        {"p.S.operator()()", "p.S.operator()"},
        {"p.S.operator()(int)const", "p.S.operator()"},
        {"p.S.operator<<typename>(T)", "p.S.operator<"},
        {"p.S.operator<=>(S)", "p.S.operator<=>"},
        {"p.S.operator->()", "p.S.operator->"},
        {"p.S.f<typename,int>(const vector<T>&,int(*)(int))const&&", "p.S.f"},
        {"p.C.Work<T,U>(ref int)", "p.C.Work"},
        {"p.Session.upload(_:Data,to:URL)", "p.Session.upload"},
        {"p.K.work(this:String,(Int)=>Unit)", "p.K.work"},
        {"p.O.initWithFrame(_:style:)", "p.O.initWithFrame"},
        {"p.C.+(l:C,r:C)", "p.C.+"},
        {"p.A.unary_!()", "p.A.unary_!"},
        {"p.S.operator/(S)", "p.S.operator/"},
        {"p.S.~S()", "p.S.~S"},
        {"p.J.f(VeryLong0,#0123456789abcdef)", "p.J.f"},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        ASSERT_EQ(cbm_qn_callable_base_len(rows[i].qn), strlen(rows[i].base));
    }
    PASS();
}

/* The leaf splitters look only at the base: Swift labels carry ':' and a
 * suffix may carry '>' — neither is a separator. */
TEST(callable_sig_leaf_splitters_skip_suffix) {
    const char *swift = "p.Session.upload(_:Data,to:URL)";
    ASSERT_STR_EQ(cbm_lsp_bare_segment(swift), "upload(_:Data,to:URL)");
    ASSERT_STR_EQ(cbm_pipeline_qn_class_method_tail(swift), "Session.upload(_:Data,to:URL)");
    /* Unsuffixed spellings are unchanged. */
    ASSERT_STR_EQ(cbm_lsp_bare_segment("Math::square"), "square");
    ASSERT_STR_EQ(cbm_lsp_bare_segment("p->run"), "run");
    ASSERT_STR_EQ(cbm_lsp_bare_segment("identity<int>"), "identity<int>");
    ASSERT_STR_EQ(cbm_pipeline_qn_class_method_tail("p.q.Cls.m"), "Cls.m");
    PASS();
}

/* The registry's by-name bucket is the base leaf, so every overload of a
 * signature-qualified callable is found by its bare name; an unsuffixed QN
 * keeps its historical key (last '.' or "::" segment). */
TEST(callable_sig_registry_by_name_uses_base_leaf) {
    cbm_registry_t *r = cbm_registry_new();
    cbm_registry_add(r, "upload", "p.Session.upload(_:Data,to:URL)", "Method");
    cbm_registry_add(r, "upload", "p.Session.upload(_:Data,with:URLRequest)", "Method");
    cbm_registry_add(r, "square", "p.Math::square", "Function");
    const char **out = NULL;
    int count = 0;
    cbm_registry_find_by_name(r, "upload", &out, &count);
    ASSERT_EQ(count, 2);
    count = 0;
    cbm_registry_find_by_name(r, "square", &out, &count);
    ASSERT_EQ(count, 1);
    cbm_registry_free(r);
    PASS();
}

SUITE(callable_sig) {
    RUN_TEST(callable_sig_golden_table);
    RUN_TEST(callable_sig_cap_keeps_identity);
    RUN_TEST(callable_sig_every_language_is_none);
    RUN_TEST(callable_sig_base_len_unsuffixed_is_full);
    RUN_TEST(callable_sig_base_len_suffixed);
    RUN_TEST(callable_sig_leaf_splitters_skip_suffix);
    RUN_TEST(callable_sig_registry_by_name_uses_base_leaf);
}
