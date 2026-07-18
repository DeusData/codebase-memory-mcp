/*
 * test_go_lsp.c — Tests for Go LSP type-aware call resolution.
 *
 * Ports from internal/cbm/lsp_test.go (49 tests).
 * Categories:
 *   - Single-file: param type inference, return type propagation, method chaining,
 *     multi-return, channels, range, type switch, closures, composites, builtins,
 *     type assertions, embedding, interfaces, generics, stdlib, diagnostics, etc.
 *   - Cross-file: method dispatch, return type chains, interface dispatch,
 *     field chains, map index, stdlib interfaces
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/go_lsp.h"
#include "pipeline/lsp_resolve.h"

/* ── Helpers ───────────────────────────────────────────────────── */

/* Extract a Go file using cbm_extract_file (runs single-file LSP internally) */
static CBMFileResult *extract_go(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", "main.go", 0, NULL,
                            NULL);
}

/* Search resolved_calls for a match where caller contains callerSub
 * and callee contains calleeSub. Returns index or -1. */
static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

/* Assert that a resolved call exists. Returns the index. */
static int require_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int idx = find_resolved(r, callerSub, calleeSub);
    if (idx < 0) {
        printf("  MISSING resolved call: caller~%s -> callee~%s (have %d)\n", callerSub, calleeSub,
               r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)", rc->strategy ? rc->strategy : "(null)",
                   rc->confidence);
        }
    }
    return idx;
}

/* Count resolved calls matching pattern */
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

/* Search cross-file resolved calls array */
static int find_resolved_arr(const CBMResolvedCallArray *arr, const char *callerSub,
                             const char *calleeSub) {
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

static int find_resolved_arr_confident(const CBMResolvedCallArray *arr, const char *callerSub,
                                       const char *calleeSub) {
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->confidence > 0 && rc->caller_qn && strstr(rc->caller_qn, callerSub) &&
            rc->callee_qn && strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

/* ── Category 1: Parameter type inference ──────────────────────── */

TEST(golsp_param_type_simple) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Database struct{}\n\n"
                                  "func (d *Database) Query(sql string) string { return \"\" }\n\n"
                                  "func doWork(db *Database) {\n\tdb.Query(\"SELECT 1\")\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doWork", "Query"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_param_type_multi) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Logger struct{}\ntype Config struct{}\n\n"
                                  "func (l *Logger) Info(msg string) {}\n"
                                  "func (c *Config) Get(key string) string { return \"\" }\n\n"
                                  "func setup(log *Logger, cfg *Config) {\n"
                                  "\tlog.Info(\"starting\")\n\tcfg.Get(\"port\")\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "setup", "Info"), 0);
    ASSERT_GTE(require_resolved(r, "setup", "Get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 2: Return type propagation ───────────────────────── */

TEST(golsp_return_type) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type File struct{}\n\n"
                   "func (f *File) Read(buf []byte) int { return 0 }\n"
                   "func Open(path string) *File { return nil }\n\n"
                   "func doRead() {\n\tf := Open(\"/tmp/test\")\n\tf.Read(nil)\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doRead", "Read"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_return_type_chain) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Builder struct{}\ntype Result struct{}\n\n"
                   "func (b *Builder) Build() *Result { return nil }\n"
                   "func (r *Result) String() string { return \"\" }\n"
                   "func NewBuilder() *Builder { return nil }\n\n"
                   "func doChain() {\n\tb := NewBuilder()\n\tr := b.Build()\n\tr.String()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doChain", "Build"), 0);
    ASSERT_GTE(require_resolved(r, "doChain", "String"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 3: Method chaining ───────────────────────────────── */

TEST(golsp_method_chaining) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Query struct{}\n\n"
                   "func (q *Query) Where(cond string) *Query { return q }\n"
                   "func (q *Query) Limit(n int) *Query { return q }\n"
                   "func (q *Query) Execute() int { return 0 }\n"
                   "func NewQuery() *Query { return nil }\n\n"
                   "func doQuery() {\n\tNewQuery().Where(\"x=1\").Limit(10).Execute()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doQuery", "Where"), 0);
    ASSERT_GTE(require_resolved(r, "doQuery", "Limit"), 0);
    ASSERT_GTE(require_resolved(r, "doQuery", "Execute"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 4: Multi-return ──────────────────────────────────── */

TEST(golsp_multi_return) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Conn struct{}\n\n"
                   "func (c *Conn) Close() error { return nil }\n"
                   "func Dial(addr string) (*Conn, error) { return nil, nil }\n\n"
                   "func doConnect() {\n\tc, _ := Dial(\"localhost\")\n\tc.Close()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doConnect", "Close"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 5: Channel receive ───────────────────────────────── */

TEST(golsp_channel_receive) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Event struct{}\n\n"
                   "func (e *Event) Process() {}\n\n"
                   "func handleEvents(ch chan *Event) {\n\te := <-ch\n\te.Process()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "handleEvents", "Process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 6: Range variables ───────────────────────────────── */

TEST(golsp_range_slice) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type User struct{}\n\n"
                                  "func (u *User) Name() string { return \"\" }\n\n"
                                  "func listNames(users []*User) {\n"
                                  "\tfor _, u := range users {\n\t\tu.Name()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "listNames", "Name"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_range_map) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Service struct{}\n\n"
                                  "func (s *Service) Start() {}\n\n"
                                  "func startAll(services map[string]*Service) {\n"
                                  "\tfor _, svc := range services {\n\t\tsvc.Start()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "startAll", "Start"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 7: Type switch ───────────────────────────────────── */

TEST(golsp_type_switch) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Dog struct{}\ntype Cat struct{}\n\n"
                                  "func (d *Dog) Speak() string { return \"woof\" }\n"
                                  "func (c *Cat) Speak() string { return \"meow\" }\n\n"
                                  "func describe(animal interface{}) {\n"
                                  "\tswitch a := animal.(type) {\n"
                                  "\tcase *Dog:\n\t\ta.Speak()\n"
                                  "\tcase *Cat:\n\t\ta.Speak()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    int n = count_resolved(r, "describe", "Speak");
    ASSERT_GT(n, 0);
    /* Verify strategy is type_dispatch */
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, "describe") && rc->callee_qn &&
            strstr(rc->callee_qn, "Speak") && rc->confidence > 0) {
            ASSERT_STR_EQ(rc->strategy, "lsp_type_dispatch");
        }
    }
    cbm_free_result(r);
    PASS();
}

/* ── Category 8: Closures ──────────────────────────────────────── */

TEST(golsp_closure) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Database struct{}\n\n"
                                  "func (db *Database) Query() {}\n\n"
                                  "func startWorker(db *Database) {\n"
                                  "\tgo func() {\n\t\tdb.Query()\n\t}()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "startWorker", "Query"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 9: Composite literals ────────────────────────────── */

TEST(golsp_composite_literal) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Config struct{}\n\n"
                                  "func (c *Config) Validate() bool { return true }\n\n"
                                  "func makeConfig() {\n\tc := &Config{}\n\tc.Validate()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "makeConfig", "Validate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_composite_literal_direct) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Handler struct{}\n\n"
                                  "func (h *Handler) ServeHTTP() {}\n\n"
                                  "func serve() {\n\t(&Handler{}).ServeHTTP()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "serve", "ServeHTTP"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 10: Builtins (make) ──────────────────────────────── */

TEST(golsp_make_slice) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Item struct{}\n\n"
                   "func (it *Item) Process() {}\n\n"
                   "func work() {\n\titems := make([]*Item, 0)\n\titems[0].Process()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "work", "Process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 11: Type assertions ──────────────────────────────── */

TEST(golsp_type_assertion) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Writer struct{}\n\n"
                   "func (w *Writer) Write(data []byte) int { return 0 }\n\n"
                   "func writeData(x interface{}) {\n\tw := x.(*Writer)\n\tw.Write(nil)\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "writeData", "Write"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 12: Struct embedding ─────────────────────────────── */

TEST(golsp_struct_embedding) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Base struct{}\n\n"
                                  "func (b *Base) Save() {}\n\n"
                                  "type Extended struct {\n\tBase\n}\n\n"
                                  "func persist(e *Extended) {\n\te.Save()\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = require_resolved(r, "persist", "Save");
    ASSERT_GTE(idx, 0);
    ASSERT_STR_EQ(r->resolved_calls.items[idx].strategy, "lsp_embed_dispatch");
    cbm_free_result(r);
    PASS();
}

/* ── Category 13: Interface dispatch ───────────────────────────── */

TEST(golsp_interface_dispatch) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Writer interface {\n\tWrite(data []byte) int\n}\n\n"
                                  "func writeAll(w Writer) {\n\tw.Write(nil)\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = require_resolved(r, "writeAll", "Write");
    ASSERT_GTE(idx, 0);
    ASSERT_STR_EQ(r->resolved_calls.items[idx].strategy, "lsp_interface_dispatch");
    ASSERT_TRUE(r->resolved_calls.items[idx].confidence <= 0.90f);
    cbm_free_result(r);
    PASS();
}

/* ── Category 14-15: Generics ──────────────────────────────────── */

TEST(golsp_explicit_generics) {
    CBMFileResult *r = extract_go(
        "package main\n\n"
        "type User struct{}\nfunc (u User) Name() string { return \"\" }\n\n"
        "type Result struct{}\nfunc (r Result) Value() int { return 0 }\n\n"
        "func Filter[T any, U any](s []T, pred func(T) bool) []U {\n\treturn nil\n}\n\n"
        "func Transform[T any, R any](input T, f func(T) R) R {\n\treturn f(input)\n}\n\n"
        "func main() {\n"
        "\tvar users []User\n"
        "\tu := Filter[User, User](users, nil)\n"
        "\t_ = u\n\n"
        "\tr := Transform[User, Result](users[0], func(u User) Result { return Result{} })\n"
        "\tr.Value()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "main", "Filter"), 0);
    ASSERT_GTE(require_resolved(r, "main", "Transform"), 0);
    ASSERT_GTE(require_resolved(r, "main", "Value"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_implicit_generics) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type User struct{}\nfunc (u User) Name() string { return \"\" }\n\n"
                   "func Filter[T any](s []T, pred func(T) bool) []T { return nil }\n\n"
                   "func main() {\n"
                   "\tvar users []User\n"
                   "\tresult := Filter(users, func(u User) bool { return true })\n"
                   "\tfor _, u := range result {\n\t\tu.Name()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "main", "Filter"), 0);
    ASSERT_GTE(require_resolved(r, "main", "Name"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Return types + param names ────────────────────────────────── */

TEST(golsp_return_types_extracted) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Foo struct{}\n\n"
                                  "func GetFoo() *Foo { return nil }\n"
                                  "func Multi() (int, error) { return 0, nil }\n");
    ASSERT_NOT_NULL(r);
    /* Verify that return_types are populated on definitions */
    int found_getfoo = 0, found_multi = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, "GetFoo") == 0) {
            found_getfoo = 1;
            /* return_type should contain "Foo" or "*Foo" */
            ASSERT_NOT_NULL(r->defs.items[i].return_type);
        }
        if (strcmp(r->defs.items[i].name, "Multi") == 0) {
            found_multi = 1;
        }
    }
    ASSERT_TRUE(found_getfoo);
    ASSERT_TRUE(found_multi);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_param_names_extracted) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "func Process(name string, count int, verbose bool) {}\n");
    ASSERT_NOT_NULL(r);
    /* Verify param_names on the Process def */
    int found = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, "Process") == 0) {
            found = 1;
            /* param_names should be populated in signature */
            ASSERT_NOT_NULL(r->defs.items[i].signature);
        }
    }
    ASSERT_TRUE(found);
    cbm_free_result(r);
    PASS();
}

/* ── Direct function calls ─────────────────────────────────────── */

TEST(golsp_direct_func_call) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "func helper() int { return 42 }\n\n"
                                  "func caller() {\n\thelper()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "caller", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Stdlib integration ────────────────────────────────────────── */

TEST(golsp_stdlib_os_open) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "import \"os\"\n\n"
                                  "func readFile(path string) {\n"
                                  "\tf, _ := os.Open(path)\n\tf.Close()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "readFile", "os.Open"), 0);
    ASSERT_GTE(require_resolved(r, "readFile", "Close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_stdlib_fmt_sprintf) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "import \"fmt\"\n\n"
                                  "func format(name string) string {\n"
                                  "\treturn fmt.Sprintf(\"hello %s\", name)\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "format", "fmt.Sprintf"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Select receive ────────────────────────────────────────────── */

TEST(golsp_select_receive) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Msg struct{}\n\n"
                                  "func (m *Msg) Process() {}\n\n"
                                  "func worker(ch chan *Msg, done chan bool) {\n"
                                  "\tselect {\n\tcase msg := <-ch:\n\t\tmsg.Process()\n"
                                  "\tcase <-done:\n\t\treturn\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "worker", "Process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Struct field access ───────────────────────────────────────── */

TEST(golsp_struct_field_access) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type User struct {\n\tName    string\n\tAge     int\n\tProfile *Profile\n}\n\n"
                   "type Profile struct {\n\tBio string\n}\n\n"
                   "func (p *Profile) Summary() string { return \"\" }\n\n"
                   "func showUser(u *User) {\n\t_ = u.Name\n\tp := u.Profile\n\tp.Summary()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "showUser", "Summary"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Pointer/value receivers ───────────────────────────────────── */

TEST(golsp_pointer_value_receivers) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Conn struct{}\n\n"
                                  "func (c *Conn) Close() {}\n"
                                  "func (c Conn) Status() string { return \"\" }\n\n"
                                  "func usePointer(c *Conn) {\n\tc.Close()\n\tc.Status()\n}\n\n"
                                  "func useValue(c Conn) {\n\tc.Status()\n\tc.Close()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "usePointer", "Close"), 0);
    ASSERT_GTE(require_resolved(r, "usePointer", "Status"), 0);
    ASSERT_GTE(require_resolved(r, "useValue", "Status"), 0);
    ASSERT_GTE(require_resolved(r, "useValue", "Close"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Diagnostics ───────────────────────────────────────────────── */

TEST(golsp_diagnostics) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "import \"unknownpkg\"\n\n"
                                  "func doStuff() {\n"
                                  "\tunknownpkg.Foo()\n\tx := getUnknown()\n\tx.Bar()\n}\n");
    ASSERT_NOT_NULL(r);
    /* Should have at least one diagnostic (confidence==0, reason non-null) */
    int diag_count = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence == 0 && rc->reason != NULL && strlen(rc->reason) > 0) {
            diag_count++;
            ASSERT_STR_EQ(rc->strategy, "lsp_unresolved");
        }
    }
    ASSERT_GT(diag_count, 0);
    cbm_free_result(r);
    PASS();
}

/* ── Variadic args ─────────────────────────────────────────────── */

TEST(golsp_variadic_args) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Logger struct{}\n\n"
                                  "func (l *Logger) Info(msg string) {}\n\n"
                                  "func logAll(loggers ...*Logger) {\n"
                                  "\tfor _, l := range loggers {\n\t\tl.Info(\"hello\")\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = require_resolved(r, "logAll", "Info");
    ASSERT_GTE(idx, 0);
    ASSERT_TRUE(r->resolved_calls.items[idx].confidence > 0);
    cbm_free_result(r);
    PASS();
}

/* ── Named returns ─────────────────────────────────────────────── */

TEST(golsp_named_returns) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Conn struct{}\n\n"
                                  "func (c *Conn) Close() {}\n\n"
                                  "func open() (conn *Conn, err error) {\n"
                                  "\tconn = &Conn{}\n\tconn.Close()\n\treturn\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = require_resolved(r, "open", "Close");
    ASSERT_GTE(idx, 0);
    ASSERT_TRUE(r->resolved_calls.items[idx].confidence > 0);
    cbm_free_result(r);
    PASS();
}

/* ── Type alias ────────────────────────────────────────────────── */

TEST(golsp_type_alias) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Base struct{}\n\n"
                                  "func (b *Base) DoWork() {}\n\n"
                                  "type Alias = Base\n\n"
                                  "func useAlias(a *Alias) {\n\ta.DoWork()\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = require_resolved(r, "useAlias", "DoWork");
    ASSERT_GTE(idx, 0);
    ASSERT_TRUE(r->resolved_calls.items[idx].confidence > 0);
    cbm_free_result(r);
    PASS();
}

/* ── Interface satisfaction (single implementer) ───────────────── */

TEST(golsp_interface_satisfaction) {
    CBMFileResult *r = extract_go(
        "package main\n\n"
        "type DataProcessor interface {\n"
        "\tProcessChunk(data []byte) (int, error)\n"
        "\tFinalize() error\n}\n\n"
        "type MyProcessor struct{}\n\n"
        "func (p *MyProcessor) ProcessChunk(data []byte) (int, error) { return 0, nil }\n"
        "func (p *MyProcessor) Finalize() error { return nil }\n\n"
        "func runProcessor(dp DataProcessor) {\n"
        "\tdp.ProcessChunk([]byte(\"hello\"))\n"
        "\tdp.Finalize()\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "runProcessor", "ProcessChunk");
    ASSERT_GTE(idx, 0);
    ASSERT_STR_EQ(r->resolved_calls.items[idx].strategy, "lsp_interface_resolve");
    ASSERT_TRUE(r->resolved_calls.items[idx].confidence >= 0.89f);

    int idx2 = find_resolved(r, "runProcessor", "Finalize");
    ASSERT_GTE(idx2, 0);
    ASSERT_STR_EQ(r->resolved_calls.items[idx2].strategy, "lsp_interface_resolve");
    cbm_free_result(r);
    PASS();
}

/* ── Package-level var/const ───────────────────────────────────── */

TEST(golsp_package_level_var) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Database struct{}\n\n"
                                  "func (d *Database) Query(sql string) string { return \"\" }\n\n"
                                  "func NewDatabase() *Database { return &Database{} }\n\n"
                                  "var db = NewDatabase()\n\n"
                                  "func handler() {\n\tdb.Query(\"SELECT 1\")\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "handler", "Query"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(golsp_package_level_const) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Logger struct{}\n\n"
                                  "func (l *Logger) Info(msg string) {}\n\n"
                                  "func NewLogger() *Logger { return &Logger{} }\n\n"
                                  "var logger = NewLogger()\n\n"
                                  "func doWork() {\n\tlogger.Info(\"starting\")\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doWork", "Info"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── If-init ───────────────────────────────────────────────────── */

TEST(golsp_if_init) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type MyError struct{}\n\n"
                                  "func (e *MyError) Error() string { return \"\" }\n"
                                  "func (e *MyError) Code() int { return 0 }\n\n"
                                  "func getError() *MyError { return nil }\n\n"
                                  "func handle() {\n"
                                  "\tif err := getError(); err != nil {\n\t\terr.Code()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "handle", "Code"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Embedded field promotion ──────────────────────────────────── */

TEST(golsp_embedded_field_promotion) {
    CBMFileResult *r = extract_go(
        "package main\n\n"
        "type Inner struct {\n\tName string\n}\n\n"
        "type Outer struct {\n\tInner\n}\n\n"
        "type Processor struct{}\n\n"
        "func (p *Processor) Process(name string) {}\n\n"
        "func doWork() {\n\to := &Outer{}\n\tp := &Processor{}\n\tp.Process(o.Name)\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doWork", "Process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── For-init ──────────────────────────────────────────────────── */

TEST(golsp_for_init) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Counter struct{}\n\n"
                   "func (c *Counter) Value() int { return 0 }\n\n"
                   "func NewCounter() *Counter { return &Counter{} }\n\n"
                   "func loop() {\n"
                   "\tfor c := NewCounter(); c.Value() < 10; {\n\t\tc.Value()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "loop", "Value"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Type conversion ───────────────────────────────────────────── */

TEST(golsp_type_conversion) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type MyString string\n\n"
                                  "func (s MyString) Upper() string { return \"\" }\n\n"
                                  "func convert() {\n\ts := MyString(\"hello\")\n\ts.Upper()\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "convert", "Upper"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Multi-name var ────────────────────────────────────────────── */

TEST(golsp_multi_name_var) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Config struct{}\n\n"
                   "func (c *Config) Get(key string) string { return \"\" }\n\n"
                   "var cfg1, cfg2 *Config\n\n"
                   "func readConfig() {\n\tcfg1.Get(\"port\")\n\tcfg2.Get(\"host\")\n}\n");
    ASSERT_NOT_NULL(r);
    int n = count_resolved(r, "readConfig", "Get");
    ASSERT_GTE(n, 2);
    cbm_free_result(r);
    PASS();
}

/* ── Switch init ───────────────────────────────────────────────── */

TEST(golsp_switch_init) {
    CBMFileResult *r = extract_go("package main\n\n"
                                  "type Validator struct{}\n\n"
                                  "func (v *Validator) Check() bool { return true }\n\n"
                                  "func NewValidator() *Validator { return &Validator{} }\n\n"
                                  "func validate() {\n"
                                  "\tswitch v := NewValidator(); v.Check() {\n"
                                  "\tcase true:\n\t\tv.Check()\n\t}\n}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "validate", "Check"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Interface method dispatch (single file) ───────────────────── */

TEST(golsp_interface_method_single_file) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Binder interface {\n\tBind(target any) error\n}\n\n"
                   "type DefaultBinder struct{}\n\n"
                   "func (d *DefaultBinder) Bind(target any) error { return nil }\n\n"
                   "func process(b Binder) {\n\tb.Bind(\"hello\")\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "process", "Bind");
    ASSERT_GTE(idx, 0);
    const char *s = r->resolved_calls.items[idx].strategy;
    ASSERT_TRUE(strcmp(s, "lsp_interface_resolve") == 0 ||
                strcmp(s, "lsp_interface_dispatch") == 0);
    cbm_free_result(r);
    PASS();
}

/* ── Interface method dispatch (field chain) ───────────────────── */

TEST(golsp_interface_method_field_chain) {
    CBMFileResult *r =
        extract_go("package main\n\n"
                   "type Binder interface {\n\tBind(target any) error\n}\n\n"
                   "type DefaultBinder struct{}\n"
                   "func (d *DefaultBinder) Bind(target any) error { return nil }\n\n"
                   "type App struct {\n\tBinder Binder\n}\n\n"
                   "type Context struct {\n\tapp *App\n}\n\n"
                   "func (c *Context) process() {\n\tc.app.Binder.Bind(\"hello\")\n}\n");
    ASSERT_NOT_NULL(r);
    int idx = find_resolved(r, "process", "Bind");
    ASSERT_GTE(idx, 0);
    const char *s = r->resolved_calls.items[idx].strategy;
    ASSERT_TRUE(strcmp(s, "lsp_interface_resolve") == 0 ||
                strcmp(s, "lsp_interface_dispatch") == 0);
    cbm_free_result(r);
    PASS();
}

/* Parser-backed calls already carry their full call-expression spans. The Go
 * semantic resolver must preserve the same occurrence identity; otherwise two
 * same-leaf calls in one caller both join whichever Render record appears
 * first in resolved_calls. Use concrete and ambiguous-interface dispatch so
 * the targets are distinct under Go's existing flat concrete-method QN model. */
TEST(golsp_ordinary_same_leaf_calls_join_by_exact_site) {
    const char *source = "package main\n\n"
                         "type Renderer interface { Render(label string) }\n\n"
                         "type A struct{}\n"
                         "func (A) Render(label string) {}\n\n"
                         "type B struct{}\n"
                         "func (B) Render(label string) {}\n\n"
                         "func run(a A, r Renderer) {\n"
                         "\ta.Render(\"alpha\")\n"
                         "\tr.Render(\"beta\")\n"
                         "}\n";
    static const char a_text[] = "a.Render(\"alpha\")";
    static const char b_text[] = "r.Render(\"beta\")";

    const char *a_site = strstr(source, a_text);
    const char *b_site = strstr(source, b_text);
    ASSERT_NOT_NULL(a_site);
    ASSERT_NOT_NULL(b_site);
    uint32_t a_start = (uint32_t)(a_site - source);
    uint32_t a_end = a_start + (uint32_t)strlen(a_text);
    uint32_t b_start = (uint32_t)(b_site - source);
    uint32_t b_end = b_start + (uint32_t)strlen(b_text);

    CBMFileResult *r = extract_go(source);
    ASSERT_NOT_NULL(r);

    const CBMCall *a_call = NULL;
    const CBMCall *b_call = NULL;
    int render_carriers = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (!call->enclosing_func_qn || !strstr(call->enclosing_func_qn, "run") ||
            !call->callee_name || !strstr(call->callee_name, "Render")) {
            continue;
        }
        render_carriers++;
        if (strstr(call->callee_name, "a.Render"))
            a_call = call;
        if (strstr(call->callee_name, "r.Render"))
            b_call = call;
    }

    ASSERT_EQ(render_carriers, 2);
    ASSERT_NOT_NULL(a_call);
    ASSERT_NOT_NULL(b_call);
    ASSERT_EQ(a_call->site_start_byte, a_start);
    ASSERT_EQ(a_call->site_end_byte, a_end);
    ASSERT_EQ(b_call->site_start_byte, b_start);
    ASSERT_EQ(b_call->site_end_byte, b_end);
    ASSERT_NEQ(a_call->site_start_byte, b_call->site_start_byte);

    const CBMResolvedCall *a_semantic = NULL;
    const CBMResolvedCall *b_semantic = NULL;
    int render_semantics = 0;
    int zero_span_hijackers = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION || rc->confidence <= 0.0f || !rc->caller_qn ||
            !strstr(rc->caller_qn, "run") || !rc->callee_qn || !strstr(rc->callee_qn, ".Render")) {
            continue;
        }
        render_semantics++;
        if (rc->site_end_byte <= rc->site_start_byte)
            zero_span_hijackers++;
        if (rc->site_start_byte == a_start && rc->site_end_byte == a_end && rc->strategy &&
            strcmp(rc->strategy, "lsp_type_dispatch") == 0) {
            a_semantic = rc;
        }
        if (rc->site_start_byte == b_start && rc->site_end_byte == b_end && rc->strategy &&
            strcmp(rc->strategy, "lsp_interface_dispatch") == 0 &&
            strstr(rc->callee_qn, ".Renderer.Render")) {
            b_semantic = rc;
        }
    }

    ASSERT_EQ(render_semantics, 2);
    ASSERT_EQ(zero_span_hijackers, 0);
    ASSERT_NOT_NULL(a_semantic);
    ASSERT_NOT_NULL(b_semantic);
    ASSERT_TRUE(a_semantic != b_semantic);
    ASSERT_NEQ(a_semantic->site_start_byte, b_semantic->site_start_byte);

    const CBMResolvedCall *a_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, a_call, false);
    const CBMResolvedCall *b_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, b_call, false);
    ASSERT_NOT_NULL(a_joined);
    ASSERT_NOT_NULL(b_joined);
    ASSERT_TRUE(a_joined == a_semantic);
    ASSERT_TRUE(b_joined == b_semantic);
    ASSERT_TRUE(strcmp(a_joined->callee_qn, b_joined->callee_qn) != 0);
    ASSERT_EQ(a_joined->site_start_byte, a_start);
    ASSERT_EQ(a_joined->site_end_byte, a_end);
    ASSERT_EQ(b_joined->site_start_byte, b_start);
    ASSERT_EQ(b_joined->site_end_byte, b_end);

    cbm_free_result(r);
    PASS();
}

/* ── Cross-file tests (use cbm_run_go_lsp_cross directly) ──────── */

/* The production Go Tier-3 path promotes per-file unresolved records through
 * registry lookups without another AST walk. That promotion must copy the
 * original occurrence span: with the shared Render leaf, a zero-span promoted
 * record is otherwise eligible for both textual carriers and hijacks one edge. */
TEST(golsp_fast_crossfile_same_leaf_calls_preserve_exact_sites) {
    const char *source = "package main\n\n"
                         "type A struct{}\n"
                         "type B struct{}\n\n"
                         "func run(a A, b B) {\n"
                         "\ta.Render(\"alpha\")\n"
                         "\tb.Render(\"beta\")\n"
                         "}\n";
    static const char a_text[] = "a.Render(\"alpha\")";
    static const char b_text[] = "b.Render(\"beta\")";

    const char *a_site = strstr(source, a_text);
    const char *b_site = strstr(source, b_text);
    ASSERT_NOT_NULL(a_site);
    ASSERT_NOT_NULL(b_site);
    uint32_t a_start = (uint32_t)(a_site - source);
    uint32_t a_end = a_start + (uint32_t)strlen(a_text);
    uint32_t b_start = (uint32_t)(b_site - source);
    uint32_t b_end = b_start + (uint32_t)strlen(b_text);

    CBMFileResult *r = extract_go(source);
    ASSERT_NOT_NULL(r);

    const CBMCall *a_call = NULL;
    const CBMCall *b_call = NULL;
    int render_carriers = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (!call->enclosing_func_qn || !strstr(call->enclosing_func_qn, "run") ||
            !call->callee_name || !strstr(call->callee_name, "Render")) {
            continue;
        }
        render_carriers++;
        if (strstr(call->callee_name, "a.Render"))
            a_call = call;
        if (strstr(call->callee_name, "b.Render"))
            b_call = call;
    }

    ASSERT_EQ(render_carriers, 2);
    ASSERT_NOT_NULL(a_call);
    ASSERT_NOT_NULL(b_call);
    ASSERT_EQ(a_call->site_start_byte, a_start);
    ASSERT_EQ(a_call->site_end_byte, a_end);
    ASSERT_EQ(b_call->site_start_byte, b_start);
    ASSERT_EQ(b_call->site_end_byte, b_end);
    ASSERT_NEQ(a_call->site_start_byte, b_call->site_start_byte);

    int unresolved_render_semantics = 0;
    const char *a_unresolved_qn = NULL;
    const char *b_unresolved_qn = NULL;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->strategy && strcmp(rc->strategy, "lsp_unresolved") == 0 && rc->reason &&
            strcmp(rc->reason, "method_not_found") == 0 && rc->caller_qn &&
            strstr(rc->caller_qn, "run") && rc->callee_qn && strstr(rc->callee_qn, ".Render")) {
            unresolved_render_semantics++;
            if (strstr(rc->callee_qn, ".A.Render"))
                a_unresolved_qn = rc->callee_qn;
            if (strstr(rc->callee_qn, ".B.Render"))
                b_unresolved_qn = rc->callee_qn;
        }
    }
    ASSERT_EQ(unresolved_render_semantics, 2);
    ASSERT_NOT_NULL(a_unresolved_qn);
    ASSERT_NOT_NULL(b_unresolved_qn);

    const char *a_leaf = strrchr(a_unresolved_qn, '.');
    const char *b_leaf = strrchr(b_unresolved_qn, '.');
    ASSERT_NOT_NULL(a_leaf);
    ASSERT_NOT_NULL(b_leaf);
    const char *a_receiver =
        cbm_arena_strndup(&r->arena, a_unresolved_qn, (size_t)(a_leaf - a_unresolved_qn));
    const char *b_receiver =
        cbm_arena_strndup(&r->arena, b_unresolved_qn, (size_t)(b_leaf - b_unresolved_qn));
    ASSERT_NOT_NULL(a_receiver);
    ASSERT_NOT_NULL(b_receiver);

    CBMLSPDef defs[2];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = a_unresolved_qn;
    defs[0].short_name = "Render";
    defs[0].label = "Method";
    defs[0].receiver_type = a_receiver;
    defs[0].def_module_qn = r->module_qn;
    defs[0].lang = CBM_LANG_GO;
    defs[1] = defs[0];
    defs[1].qualified_name = b_unresolved_qn;
    defs[1].receiver_type = b_receiver;

    CBMTypeRegistry *reg = cbm_go_build_cross_registry(&r->arena, defs, 2);
    ASSERT_NOT_NULL(reg);
    ASSERT_EQ(cbm_go_fast_resolve_qualified_calls(r, reg, NULL, NULL, 0), 0);

    const CBMResolvedCall *a_semantic = NULL;
    const CBMResolvedCall *b_semantic = NULL;
    int promoted_render_semantics = 0;
    int zero_span_hijackers = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (!rc->strategy || strcmp(rc->strategy, "lsp_strategy_cross_file") != 0 ||
            rc->kind != CBM_RESOLVED_INVOCATION || !rc->caller_qn ||
            !strstr(rc->caller_qn, "run") || !rc->callee_qn || !strstr(rc->callee_qn, ".Render")) {
            continue;
        }
        promoted_render_semantics++;
        if (rc->site_end_byte <= rc->site_start_byte)
            zero_span_hijackers++;
        if (strcmp(rc->callee_qn, a_unresolved_qn) == 0 && rc->site_start_byte == a_start &&
            rc->site_end_byte == a_end) {
            a_semantic = rc;
        }
        if (strcmp(rc->callee_qn, b_unresolved_qn) == 0 && rc->site_start_byte == b_start &&
            rc->site_end_byte == b_end) {
            b_semantic = rc;
        }
    }

    if (promoted_render_semantics != 2) {
        printf("  fast-cross Render diagnostics (%d records):\n", r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s reason=%s site=%u:%u]\n",
                   rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)", rc->strategy ? rc->strategy : "(null)",
                   rc->reason ? rc->reason : "(null)", rc->site_start_byte, rc->site_end_byte);
        }
    }

    ASSERT_EQ(promoted_render_semantics, 2);
    ASSERT_EQ(zero_span_hijackers, 0);
    ASSERT_NOT_NULL(a_semantic);
    ASSERT_NOT_NULL(b_semantic);
    ASSERT_TRUE(a_semantic != b_semantic);
    ASSERT_NEQ(a_semantic->site_start_byte, b_semantic->site_start_byte);

    const CBMResolvedCall *a_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, a_call, false);
    const CBMResolvedCall *b_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, b_call, false);
    ASSERT_NOT_NULL(a_joined);
    ASSERT_NOT_NULL(b_joined);
    ASSERT_STR_EQ(a_joined->callee_qn, a_unresolved_qn);
    ASSERT_STR_EQ(b_joined->callee_qn, b_unresolved_qn);
    ASSERT_EQ(a_joined->site_start_byte, a_start);
    ASSERT_EQ(a_joined->site_end_byte, a_end);
    ASSERT_EQ(b_joined->site_start_byte, b_start);
    ASSERT_EQ(b_joined->site_end_byte, b_end);

    cbm_free_result(r);
    PASS();
}

TEST(golsp_crossfile_method_dispatch) {
    const char *source = "package main\n\n"
                         "import \"myapp/db\"\n\n"
                         "func doQuery() {\n"
                         "\tconn := db.Connect(\"localhost\")\n"
                         "\tconn.Query(\"SELECT 1\")\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.doQuery",
         .short_name = "doQuery",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "myapp/db.Connect",
         .short_name = "Connect",
         .label = "Function",
         .def_module_qn = "myapp/db",
         .return_types = "*Conn"},
        {.qualified_name = "myapp/db.Conn",
         .short_name = "Conn",
         .label = "Type",
         .def_module_qn = "myapp/db"},
        {.qualified_name = "myapp/db.Conn.Query",
         .short_name = "Query",
         .label = "Method",
         .def_module_qn = "myapp/db",
         .receiver_type = "myapp/db.Conn"},
    };
    const char *imp_names[] = {"db"};
    const char *imp_qns[] = {"myapp/db"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 4, imp_names,
                         imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr(&out, "doQuery", "Connect"), 0);
    int idx = find_resolved_arr(&out, "doQuery", "Query");
    ASSERT_GTE(idx, 0);
    ASSERT_STR_EQ(out.items[idx].strategy, "lsp_type_dispatch");

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_return_type_chain) {
    const char *source = "package main\n\n"
                         "import \"myapp/repo\"\n\n"
                         "func showUser() {\n"
                         "\tuser := repo.GetUser(1)\n"
                         "\tuser.Name()\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.showUser",
         .short_name = "showUser",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "myapp/repo.GetUser",
         .short_name = "GetUser",
         .label = "Function",
         .def_module_qn = "myapp/repo",
         .return_types = "*User"},
        {.qualified_name = "myapp/repo.User",
         .short_name = "User",
         .label = "Type",
         .def_module_qn = "myapp/repo"},
        {.qualified_name = "myapp/repo.User.Name",
         .short_name = "Name",
         .label = "Method",
         .def_module_qn = "myapp/repo",
         .receiver_type = "myapp/repo.User"},
    };
    const char *imp_names[] = {"repo"};
    const char *imp_qns[] = {"myapp/repo"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 4, imp_names,
                         imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr(&out, "showUser", "Name"), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_interface_dispatch) {
    const char *source = "package main\n\n"
                         "import \"myapp/svc\"\n\n"
                         "func handler(b svc.Binder) {\n\tb.Bind(\"data\")\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.handler",
         .short_name = "handler",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "myapp/svc.Binder",
         .short_name = "Binder",
         .label = "Interface",
         .def_module_qn = "myapp/svc",
         .is_interface = true},
        {.qualified_name = "myapp/svc.DefaultBinder",
         .short_name = "DefaultBinder",
         .label = "Type",
         .def_module_qn = "myapp/svc"},
        {.qualified_name = "myapp/svc.DefaultBinder.Bind",
         .short_name = "Bind",
         .label = "Method",
         .def_module_qn = "myapp/svc",
         .receiver_type = "myapp/svc.DefaultBinder"},
    };
    const char *imp_names[] = {"svc"};
    const char *imp_qns[] = {"myapp/svc"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 4, imp_names,
                         imp_qns, 1, NULL, &out);

    int idx = find_resolved_arr_confident(&out, "handler", "Bind");
    ASSERT_GTE(idx, 0);
    const char *s = out.items[idx].strategy;
    ASSERT_TRUE(strcmp(s, "lsp_interface_resolve") == 0 ||
                strcmp(s, "lsp_interface_dispatch") == 0);

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_interface_field_chain) {
    const char *source = "package main\n\n"
                         "import \"myapp/echo\"\n\n"
                         "type Context struct {\n\techo *echo.Echo\n}\n\n"
                         "func (c *Context) process() {\n\tc.echo.Binder.Bind(\"hello\")\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.Context",
         .short_name = "Context",
         .label = "Type",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.main.Context.process",
         .short_name = "process",
         .label = "Method",
         .def_module_qn = "test.main",
         .receiver_type = "test.main.Context"},
        {.qualified_name = "myapp/echo.Echo",
         .short_name = "Echo",
         .label = "Type",
         .def_module_qn = "myapp/echo",
         .field_defs = "Binder:Binder"},
        {.qualified_name = "myapp/echo.Binder",
         .short_name = "Binder",
         .label = "Interface",
         .def_module_qn = "myapp/echo",
         .is_interface = true},
        {.qualified_name = "myapp/echo.DefaultBinder",
         .short_name = "DefaultBinder",
         .label = "Type",
         .def_module_qn = "myapp/echo"},
        {.qualified_name = "myapp/echo.DefaultBinder.Bind",
         .short_name = "Bind",
         .label = "Method",
         .def_module_qn = "myapp/echo",
         .receiver_type = "myapp/echo.DefaultBinder"},
    };
    const char *imp_names[] = {"echo"};
    const char *imp_qns[] = {"myapp/echo"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 6, imp_names,
                         imp_qns, 1, NULL, &out);

    int idx = find_resolved_arr_confident(&out, "process", "Bind");
    ASSERT_GTE(idx, 0);
    ASSERT_TRUE(out.items[idx].confidence >= 0.8f);

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_map_index) {
    const char *source = "package main\n\n"
                         "import \"myapp/echo\"\n\n"
                         "func dispatch(vhosts map[string]*echo.Echo, host string) {\n"
                         "\tif vh, ok := vhosts[host]; ok {\n"
                         "\t\tvh.ServeHTTP(nil, nil)\n\t}\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.dispatch",
         .short_name = "dispatch",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "myapp/echo.Echo",
         .short_name = "Echo",
         .label = "Type",
         .def_module_qn = "myapp/echo"},
        {.qualified_name = "myapp/echo.Echo.ServeHTTP",
         .short_name = "ServeHTTP",
         .label = "Method",
         .def_module_qn = "myapp/echo",
         .receiver_type = "myapp/echo.Echo"},
    };
    const char *imp_names[] = {"echo"};
    const char *imp_qns[] = {"myapp/echo"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 3, imp_names,
                         imp_qns, 1, NULL, &out);

    int idx = find_resolved_arr_confident(&out, "dispatch", "ServeHTTP");
    ASSERT_GTE(idx, 0);
    ASSERT_STR_EQ(out.items[idx].strategy, "lsp_type_dispatch");

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_stdlib_interface) {
    const char *source = "package main\n\n"
                         "import \"context\"\n\n"
                         "func process(ctx context.Context) {\n"
                         "\t<-ctx.Done()\n\tctx.Err()\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.process",
         .short_name = "process",
         .label = "Function",
         .def_module_qn = "test.main"},
    };
    const char *imp_names[] = {"context"};
    const char *imp_qns[] = {"context"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 1, imp_names,
                         imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr_confident(&out, "process", "Done"), 0);
    ASSERT_GTE(find_resolved_arr_confident(&out, "process", "Err"), 0);

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(golsp_crossfile_local_interface_single_impl) {
    const char *source =
        "package main\n\n"
        "import \"myapp/svc\"\n\n"
        "func process(s svc.Store) {\n\ts.Get(\"key\")\n\ts.Put(\"key\", \"val\")\n}\n";

    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.process",
         .short_name = "process",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "myapp/svc.Store",
         .short_name = "Store",
         .label = "Interface",
         .def_module_qn = "myapp/svc",
         .is_interface = true,
         .method_names_str = "Get|Put"},
        {.qualified_name = "myapp/svc.RedisStore",
         .short_name = "RedisStore",
         .label = "Class",
         .def_module_qn = "myapp/svc"},
        {.qualified_name = "myapp/svc.RedisStore.Get",
         .short_name = "Get",
         .label = "Method",
         .def_module_qn = "myapp/svc",
         .receiver_type = "myapp/svc.RedisStore"},
        {.qualified_name = "myapp/svc.RedisStore.Put",
         .short_name = "Put",
         .label = "Method",
         .def_module_qn = "myapp/svc",
         .receiver_type = "myapp/svc.RedisStore"},
    };
    const char *imp_names[] = {"svc"};
    const char *imp_qns[] = {"myapp/svc"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_go_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 5, imp_names,
                         imp_qns, 1, NULL, &out);

    int idxGet = find_resolved_arr_confident(&out, "process", "Get");
    ASSERT_GTE(idxGet, 0);
    ASSERT_STR_EQ(out.items[idxGet].strategy, "lsp_interface_resolve");
    ASSERT_STR_EQ(out.items[idxGet].callee_qn, "myapp/svc.RedisStore.Get");

    int idxPut = find_resolved_arr_confident(&out, "process", "Put");
    ASSERT_GTE(idxPut, 0);
    ASSERT_STR_EQ(out.items[idxPut].strategy, "lsp_interface_resolve");

    cbm_arena_destroy(&arena);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(go_lsp) {
    /* Single-file: param type inference */
    RUN_TEST(golsp_param_type_simple);
    RUN_TEST(golsp_param_type_multi);

    /* Return type propagation */
    RUN_TEST(golsp_return_type);
    RUN_TEST(golsp_return_type_chain);

    /* Method chaining */
    RUN_TEST(golsp_method_chaining);

    /* Multi-return */
    RUN_TEST(golsp_multi_return);

    /* Channel receive */
    RUN_TEST(golsp_channel_receive);

    /* Range variables */
    RUN_TEST(golsp_range_slice);
    RUN_TEST(golsp_range_map);

    /* Type switch */
    RUN_TEST(golsp_type_switch);

    /* Closures */
    RUN_TEST(golsp_closure);

    /* Composite literals */
    RUN_TEST(golsp_composite_literal);
    RUN_TEST(golsp_composite_literal_direct);

    /* Builtins */
    RUN_TEST(golsp_make_slice);

    /* Type assertions */
    RUN_TEST(golsp_type_assertion);

    /* Struct embedding */
    RUN_TEST(golsp_struct_embedding);

    /* Interface dispatch */
    RUN_TEST(golsp_interface_dispatch);

    /* Generics */
    RUN_TEST(golsp_explicit_generics);
    RUN_TEST(golsp_implicit_generics);

    /* Return types + param names */
    RUN_TEST(golsp_return_types_extracted);
    RUN_TEST(golsp_param_names_extracted);

    /* Direct calls */
    RUN_TEST(golsp_direct_func_call);

    /* Stdlib */
    RUN_TEST(golsp_stdlib_os_open);
    RUN_TEST(golsp_stdlib_fmt_sprintf);

    /* Select receive */
    RUN_TEST(golsp_select_receive);

    /* Struct field access */
    RUN_TEST(golsp_struct_field_access);

    /* Pointer/value receivers */
    RUN_TEST(golsp_pointer_value_receivers);

    /* Diagnostics */
    RUN_TEST(golsp_diagnostics);

    /* Variadic */
    RUN_TEST(golsp_variadic_args);

    /* Named returns */
    RUN_TEST(golsp_named_returns);

    /* Type alias */
    RUN_TEST(golsp_type_alias);

    /* Interface satisfaction */
    RUN_TEST(golsp_interface_satisfaction);

    /* Package-level vars */
    RUN_TEST(golsp_package_level_var);
    RUN_TEST(golsp_package_level_const);

    /* If-init, for-init, switch-init */
    RUN_TEST(golsp_if_init);
    RUN_TEST(golsp_embedded_field_promotion);
    RUN_TEST(golsp_for_init);
    RUN_TEST(golsp_type_conversion);
    RUN_TEST(golsp_multi_name_var);
    RUN_TEST(golsp_switch_init);

    /* Interface method dispatch variants */
    RUN_TEST(golsp_interface_method_single_file);
    RUN_TEST(golsp_interface_method_field_chain);
    RUN_TEST(golsp_ordinary_same_leaf_calls_join_by_exact_site);

    /* Cross-file */
    RUN_TEST(golsp_fast_crossfile_same_leaf_calls_preserve_exact_sites);
    RUN_TEST(golsp_crossfile_method_dispatch);
    RUN_TEST(golsp_crossfile_return_type_chain);
    RUN_TEST(golsp_crossfile_interface_dispatch);
    RUN_TEST(golsp_crossfile_interface_field_chain);
    RUN_TEST(golsp_crossfile_map_index);
    RUN_TEST(golsp_crossfile_stdlib_interface);
    RUN_TEST(golsp_crossfile_local_interface_single_impl);
}
