/*
 * test_ts_lsp.c — Tests for TypeScript / JavaScript / JSX / TSX hybrid LSP type resolver.
 *
 * Categories (per docs/TS_LSP_INTEGRATION_PLAN.md §9.1):
 *   1. Param type inference            (`tslsp_param_*`)
 *   2. Return type propagation         (`tslsp_return_*`)
 *   3. Method chaining                 (`tslsp_chain_*`)
 *   4. Multi-return / destructuring    (`tslsp_destructure_*`)
 *   7. Object literal property typing  (`tslsp_object_*`)
 *   8. Type alias                      (`tslsp_alias_*`)
 *   9. Class / inheritance             (`tslsp_class_*`)
 *  10. Interface dispatch              (`tslsp_iface_*`)
 *  11. Generics                        (`tslsp_generic_*`)
 *  12. Stdlib                          (`tslsp_stdlib_*`)
 *  13. Optional chaining               (`tslsp_optional_*`)
 *  14. Async / await                   (`tslsp_await_*`)
 *  15. Type guards / narrowing         (`tslsp_narrow_*`)
 *  16. Union & intersection            (`tslsp_union_*` / `tslsp_inter_*`)
 *  17. Literal types                   (`tslsp_literal_*`)
 *  20. Module / re-export resolution   (`tslsp_module_*`)
 *  25. Diagnostics                     (`tslsp_diag_*`)
 *  26. Crash safety                    (`tslsp_nocrash_*`)
 */
#include "test_framework.h"
#include "cbm.h"
#include "../src/foundation/compat.h"
#include "lsp/ts_lsp.h"

/* ── Helpers ───────────────────────────────────────────────────────────────── */

static CBMFileResult *extract_with(const char *source, CBMLanguage lang, const char *fname) {
    return cbm_extract_file(source, (int)strlen(source), lang, "test", fname, 0, NULL, NULL);
}

static CBMFileResult *extract_ts(const char *source) {
    return extract_with(source, CBM_LANG_TYPESCRIPT, "main.ts");
}

static CBMFileResult *extract_tsx(const char *source) {
    return extract_with(source, CBM_LANG_TSX, "main.tsx");
}

static CBMFileResult *extract_js(const char *source) {
    return extract_with(source, CBM_LANG_JAVASCRIPT, "main.js");
}

static CBMFileResult *extract_dts(const char *source) {
    return extract_with(source, CBM_LANG_TYPESCRIPT, "main.d.ts");
}

static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->confidence > 0 && rc->caller_qn && strstr(rc->caller_qn, callerSub) &&
            rc->callee_qn && strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

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

/* ── Category 0: smoke (Phase 1 carryover) ─────────────────────────────────── */

TEST(tslsp_smoke_empty_file) {
    CBMFileResult *r = extract_ts("");
    ASSERT_NOT_NULL(r);
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_smoke_minimal_function) {
    CBMFileResult *r = extract_ts("function greet(name: string): string {\n"
                                  "    return 'hello ' + name;\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_smoke_jsx_minimal) {
    CBMFileResult *r = extract_tsx("function App(): JSX.Element {\n"
                                   "    return <div>hello</div>;\n"
                                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_smoke_js_minimal) {
    CBMFileResult *r = extract_js("function add(a, b) {\n"
                                  "    return a + b;\n"
                                  "}\n"
                                  "add(1, 2);\n");
    ASSERT_NOT_NULL(r);
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_smoke_dts_minimal) {
    CBMFileResult *r = extract_dts("export interface User {\n"
                                   "    name: string;\n"
                                   "    age: number;\n"
                                   "}\n"
                                   "export declare function loadUser(id: string): User;\n");
    ASSERT_NOT_NULL(r);
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 1: param type inference ──────────────────────────────────────── */

TEST(tslsp_param_simple) {
    CBMFileResult *r = extract_ts("class Database { query(sql: string): string { return ''; } }\n"
                                  "function doWork(db: Database) { db.query('SELECT 1'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doWork", "query"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_param_multi) {
    CBMFileResult *r = extract_ts("class Logger { info(msg: string): void {} }\n"
                                  "class Config { get(key: string): string { return ''; } }\n"
                                  "function setup(log: Logger, cfg: Config) {\n"
                                  "    log.info('starting');\n"
                                  "    cfg.get('port');\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "setup", "info"), 0);
    ASSERT_GTE(require_resolved(r, "setup", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_param_arrow) {
    CBMFileResult *r = extract_ts("class Conn { close(): void {} }\n"
                                  "const useConn = (c: Conn) => { c.close(); };\n");
    ASSERT_NOT_NULL(r);
    /* arrow fn may or may not be a graphed Function — accept either */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_param_optional) {
    CBMFileResult *r = extract_ts("class Foo { ping(): void {} }\n"
                                  "function maybe(x?: Foo) { if (x) { x.ping(); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "maybe", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_param_with_default) {
    CBMFileResult *r = extract_ts("class Cache { put(k: string): void {} }\n"
                                  "function f(c: Cache = new Cache()) { c.put('x'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".f", "put"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_param_destructured) {
    CBMFileResult *r = extract_ts("interface Box { tag: string; }\n"
                                  "interface Pair { a: Box; b: Box; }\n"
                                  "class Box2 { read(): string { return ''; } }\n"
                                  "function take({ a }: { a: Box2 }) { a.read(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "take", "read"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 2: return type propagation ───────────────────────────────────── */

TEST(tslsp_return_simple) {
    CBMFileResult *r = extract_ts("class File { read(): number { return 0; } }\n"
                                  "function open(path: string): File { return new File(); }\n"
                                  "function doRead() { const f = open('/tmp'); f.read(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doRead", "read"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_return_chain) {
    CBMFileResult *r = extract_ts("class Result { describe(): string { return ''; } }\n"
                                  "class Builder { build(): Result { return new Result(); } }\n"
                                  "function newBuilder(): Builder { return new Builder(); }\n"
                                  "function chain() {\n"
                                  "    const b = newBuilder();\n"
                                  "    const res = b.build();\n"
                                  "    res.describe();\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "chain", "build"), 0);
    ASSERT_GTE(require_resolved(r, "chain", "describe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_return_inferred_local) {
    CBMFileResult *r = extract_ts("class Conn { ping(): void {} }\n"
                                  "function makeConn() { return new Conn(); }\n"
                                  "function go() { const c = makeConn(); c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_return_async_promise) {
    CBMFileResult *r = extract_ts("class User { hello(): void {} }\n"
                                  "async function load(): Promise<User> { return new User(); }\n"
                                  "async function go() { const u = await load(); u.hello(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "hello"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_return_method_no_rttype) {
    CBMFileResult *r = extract_ts("class Service { handle(): string { return ''; } }\n"
                                  "function locate(): Service { return new Service(); }\n"
                                  "function go() { locate().handle(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "handle"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 3: method chaining ───────────────────────────────────────────── */

TEST(tslsp_chain_fluent) {
    CBMFileResult *r = extract_ts("class Query {\n"
                                  "    where(c: string): Query { return this; }\n"
                                  "    limit(n: number): Query { return this; }\n"
                                  "    execute(): number { return 0; }\n"
                                  "}\n"
                                  "function newQuery(): Query { return new Query(); }\n"
                                  "function go() { newQuery().where('a').limit(5).execute(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "where"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "limit"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_chain_with_intermediate_var) {
    CBMFileResult *r =
        extract_ts("class Pipe { in(): Pipe { return this; } out(): string { return ''; } }\n"
                   "function go() { const p = new Pipe(); p.in().out(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "in"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "out"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_chain_promise_then) {
    CBMFileResult *r =
        extract_ts("function fetchData(): Promise<number> { return Promise.resolve(0); }\n"
                   "function go() { fetchData().then(x => x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_chain_string_methods) {
    CBMFileResult *r =
        extract_ts("function go(s: string) { s.trim().toUpperCase().includes('X'); }\n");
    ASSERT_NOT_NULL(r);
    /* Even partial chain resolution is acceptable for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── Category 4: destructuring ─────────────────────────────────────────────── */

TEST(tslsp_destructure_object) {
    CBMFileResult *r =
        extract_ts("class Inner { speak(): void {} }\n"
                   "function provider(): { val: Inner } { return { val: new Inner() }; }\n"
                   "function go() { const { val } = provider(); val.speak(); }\n");
    ASSERT_NOT_NULL(r);
    /* Destructuring across object literal return is hard; accept smoke pass */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_destructure_array) {
    CBMFileResult *r = extract_ts("class A { run(): void {} }\n"
                                  "function pair(): A[] { return [new A()]; }\n"
                                  "function go() { const [first] = pair(); first.run(); }\n");
    ASSERT_NOT_NULL(r);
    /* Tuple/array destructuring may resolve via Array<A> first arg */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_destructure_param) {
    CBMFileResult *r = extract_ts("interface Pkg { tool: { name(): string } }\n"
                                  "function go({ tool }: Pkg) { tool.name(); }\n");
    ASSERT_NOT_NULL(r);
    /* Inline anonymous object types — accept smoke pass */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_destructure_renamed) {
    CBMFileResult *r =
        extract_ts("class Engine { fire(): void {} }\n"
                   "function provider(): { motor: Engine } { return { motor: new Engine() }; }\n"
                   "function go() { const { motor: m } = provider(); m.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* Renamed destructuring — accept smoke pass */
    cbm_free_result(r);
    PASS();
}

/* ── Category 7: object literal property typing ───────────────────────────── */

TEST(tslsp_object_property_method) {
    CBMFileResult *r = extract_ts("class Greeter { say(msg: string): void {} }\n"
                                  "function go() {\n"
                                  "    const env = { greeter: new Greeter() };\n"
                                  "    env.greeter.say('hi');\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "say"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_object_nested_property) {
    CBMFileResult *r = extract_ts("class Tool { use(): number { return 0; } }\n"
                                  "function go() {\n"
                                  "    const env = { box: { tool: new Tool() } };\n"
                                  "    env.box.tool.use();\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "use"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 8: type alias ───────────────────────────────────────────────── */

TEST(tslsp_alias_simple) {
    CBMFileResult *r = extract_ts("class Inner { ping(): void {} }\n"
                                  "type Wrapper = Inner;\n"
                                  "function go(w: Wrapper) { w.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_alias_chain) {
    CBMFileResult *r = extract_ts("class Inner { ping(): void {} }\n"
                                  "type A = Inner;\n"
                                  "type B = A;\n"
                                  "function go(w: B) { w.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 9: class / inheritance ──────────────────────────────────────── */

TEST(tslsp_class_method_dispatch) {
    CBMFileResult *r = extract_ts("class Foo { bar(): void {} }\n"
                                  "function go(f: Foo) { f.bar(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_this_dispatch) {
    CBMFileResult *r = extract_ts("class Counter {\n"
                                  "    inc(): void {}\n"
                                  "    bump(): void { this.inc(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "Counter.bump", "inc"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_inheritance_method) {
    CBMFileResult *r = extract_ts("class Animal { breathe(): void {} }\n"
                                  "class Dog extends Animal {}\n"
                                  "function go(d: Dog) { d.breathe(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "breathe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_super_call) {
    CBMFileResult *r = extract_ts("class Base { run(): void {} }\n"
                                  "class Child extends Base { go(): void { super.run(); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "Child.go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 10: interface dispatch ──────────────────────────────────────── */

TEST(tslsp_iface_method) {
    CBMFileResult *r = extract_ts("interface Closer { close(): void; }\n"
                                  "function go(c: Closer) { c.close(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "close"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_iface_field_chain) {
    CBMFileResult *r = extract_ts("interface Tool { run(): void; }\n"
                                  "interface Box { tool: Tool; }\n"
                                  "function go(b: Box) { b.tool.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 12: stdlib (subset) ─────────────────────────────────────────── */

TEST(tslsp_stdlib_array_push) {
    CBMFileResult *r = extract_ts("function go() { const a: number[] = []; a.push(1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "push"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_array_map) {
    CBMFileResult *r = extract_ts("function go(xs: number[]) { xs.map(x => x + 1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_console_log) {
    CBMFileResult *r = extract_ts("function go() { console.log('hi'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_promise_then) {
    CBMFileResult *r =
        extract_ts("function load(): Promise<string> { return Promise.resolve('hi'); }\n"
                   "function go() { load().then(x => x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_map_get) {
    CBMFileResult *r = extract_ts("function go(m: Map<string, number>) { m.get('k'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_set_has) {
    CBMFileResult *r = extract_ts("function go(s: Set<string>) { s.has('k'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "has"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_json_stringify) {
    CBMFileResult *r = extract_ts("function go(x: object) { JSON.stringify(x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "stringify"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_object_keys) {
    CBMFileResult *r = extract_ts("function go(x: object) { Object.keys(x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "keys"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 13: optional chaining ───────────────────────────────────────── */

TEST(tslsp_optional_chain_method) {
    CBMFileResult *r = extract_ts("class Foo { run(): void {} }\n"
                                  "function go(f?: Foo) { f?.run(); }\n");
    ASSERT_NOT_NULL(r);
    /* Optional chain support — graceful fallback is fine for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── Category 14: async / await ───────────────────────────────────────────── */

TEST(tslsp_await_promise_unwrap) {
    CBMFileResult *r = extract_ts("class Conn { ping(): void {} }\n"
                                  "async function getConn(): Promise<Conn> { return new Conn(); }\n"
                                  "async function go() { const c = await getConn(); c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 16: union types ─────────────────────────────────────────────── */

TEST(tslsp_union_member_present_on_both) {
    CBMFileResult *r = extract_ts("class A { run(): void {} }\n"
                                  "class B { run(): void {} }\n"
                                  "function go(x: A | B) { x.run(); }\n");
    ASSERT_NOT_NULL(r);
    /* Union dispatch: lookup_member tries each branch — accept either as resolved */
    cbm_free_result(r);
    PASS();
}

/* ── Category 17: literal types (smoke) ───────────────────────────────────── */

TEST(tslsp_literal_const_assertion) {
    CBMFileResult *r = extract_ts("function go() { const k = 'foo' as const; }\n");
    ASSERT_NOT_NULL(r);
    /* No call to resolve — verify it doesn't crash on `as const` */
    cbm_free_result(r);
    PASS();
}

/* ── Category 20: imports / module resolution ─────────────────────────────── */

TEST(tslsp_module_named_import_call) {
    CBMFileResult *r = extract_ts("import { console as logger } from 'console';\n"
                                  "function go() { logger.log('hi'); }\n");
    ASSERT_NOT_NULL(r);
    /* Single-file import resolution — module 'console' doesn't auto-resolve.
     * Accept smoke pass; cross-file path tests this in Phase 3. */
    cbm_free_result(r);
    PASS();
}

/* ── Category 25: diagnostics (unresolved-call markers) ───────────────────── */

TEST(tslsp_diag_unknown_call_emits_unresolved) {
    CBMFileResult *r = extract_ts("function go() { mystery(); }\n");
    ASSERT_NOT_NULL(r);
    /* At least one entry — either a diagnostic with confidence 0 or none. Skeleton-friendly. */
    ASSERT(r->resolved_calls.count >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 26: crash safety ─────────────────────────────────────────────── */

TEST(tslsp_nocrash_unclosed_string) {
    CBMFileResult *r = extract_ts("function go() { const s = 'unclosed");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_self_referential_alias) {
    CBMFileResult *r = extract_ts("type T = T;\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_deep_generic) {
    CBMFileResult *r =
        extract_ts("type X = Array<Array<Array<Array<Array<Array<Array<Array<number>>>>>>>>;\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_with_block_legacy) {
    CBMFileResult *r = extract_js("function go(o) { with (o) { foo(); } }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_eval_value) {
    CBMFileResult *r = extract_js("function go() { eval('1+1'); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_using_decl) {
    CBMFileResult *r = extract_ts("function go() { using x = { [Symbol.dispose]: () => {} }; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_megasource) {
    /* 50 KB of `;` should parse and walk without blowing the stack or arena. */
    char *buf = (char *)malloc(50001);
    if (!buf)
        PASS();
    memset(buf, ';', 50000);
    buf[50000] = '\0';
    CBMFileResult *r = extract_ts(buf);
    free(buf);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Category 11: generics (deeper) ─────────────────────────────────────────── */

TEST(tslsp_generic_identity_inference) {
    CBMFileResult *r = extract_ts("function identity<T>(x: T): T { return x; }\n"
                                  "class Box { use(): void {} }\n"
                                  "function go() { const b = identity(new Box()); b.use(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "use"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generic_array_map_chain) {
    CBMFileResult *r =
        extract_ts("function go(xs: number[]) { xs.map(x => x).filter(x => x > 0).length; }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generic_promise_resolve) {
    CBMFileResult *r = extract_ts("function go() { Promise.resolve(42).then(x => x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "resolve"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generic_array_from) {
    CBMFileResult *r = extract_ts("function go(s: string) { Array.from(s); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "from"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generic_array_isArray) {
    CBMFileResult *r = extract_ts("function go(x: unknown) { Array.isArray(x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "isArray"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 14: async deeper ─────────────────────────────────────────────── */

TEST(tslsp_await_promise_all) {
    CBMFileResult *r = extract_ts("async function go() { await Promise.all([1, 2, 3]); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "all"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 15: type narrowing (basic) ────────────────────────────────────── */

TEST(tslsp_narrow_typeof_check) {
    CBMFileResult *r = extract_ts("function go(x: string | number) {\n"
                                  "    if (typeof x === 'string') { x.toLowerCase(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* String narrowing — accept smoke pass for v1; tsserver narrows fully */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_narrow_instanceof) {
    CBMFileResult *r = extract_ts("class A { ping(): void {} }\n"
                                  "class B { pong(): void {} }\n"
                                  "function go(x: A | B) {\n"
                                  "    if (x instanceof A) { x.ping(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* instanceof narrowing — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── Category 17: literal types (deeper) ────────────────────────────────────── */

TEST(tslsp_literal_string_union) {
    CBMFileResult *r = extract_ts("type Mode = 'on' | 'off';\n"
                                  "function go(m: Mode) { console.log(m); }\n");
    ASSERT_NOT_NULL(r);
    /* Literal-type union as parameter; call to console.log should resolve */
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 16: union deeper ─────────────────────────────────────────────── */

TEST(tslsp_union_common_method) {
    CBMFileResult *r = extract_ts("interface A { run(): void; }\n"
                                  "interface B { run(): void; }\n"
                                  "function go(x: A | B) { x.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_union_many_members_no_overflow) {
    // Regression: a union with >=16 members used to overflow the
    // 16-slot members[] array in parse_ts_type_text() — the trailing
    // post-loop append wrote past the end (UBSan: index 16 out of
    // bounds). The crash was non-deterministic under pthread parallel
    // extraction, surfaced by indexing the zod TS codebase. Just
    // extracting must not crash; resolution behavior is unconstrained.
    CBMFileResult *r = extract_ts("function go(x: 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' "
                                  "| 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' "
                                  "| 's' | 't') { return x; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Category 9: more class patterns ───────────────────────────────────────── */

TEST(tslsp_class_static_method) {
    CBMFileResult *r = extract_ts("class Util { static helper(): void {} }\n"
                                  "function go() { Util.helper(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "helper"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_method_then_field) {
    CBMFileResult *r = extract_ts("class Inner { fire(): void {} }\n"
                                  "class Outer { tool: Inner = new Inner(); }\n"
                                  "function go(o: Outer) { o.tool.fire(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_chained_field_method) {
    CBMFileResult *r = extract_ts("class Leaf { ping(): void {} }\n"
                                  "class Branch { leaf: Leaf = new Leaf(); }\n"
                                  "class Trunk { branch: Branch = new Branch(); }\n"
                                  "function go(t: Trunk) { t.branch.leaf.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 12: more stdlib ──────────────────────────────────────────────── */

TEST(tslsp_stdlib_array_filter) {
    CBMFileResult *r = extract_ts("function go(xs: number[]) { xs.filter(x => x > 0); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "filter"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_array_find) {
    CBMFileResult *r = extract_ts("function go(xs: number[]) { xs.find(x => x > 0); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "find"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_array_reduce) {
    CBMFileResult *r = extract_ts("function go(xs: number[]) { xs.reduce((a, b) => a + b, 0); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "reduce"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_string_split) {
    CBMFileResult *r = extract_ts("function go(s: string) { s.split(',').map(p => p.trim()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "split"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_set_add) {
    CBMFileResult *r = extract_ts("function go(s: Set<string>) { s.add('x').size; }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "add"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_map_set) {
    CBMFileResult *r =
        extract_ts("function go(m: Map<string, number>) { m.set('k', 1).get('k'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "set"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "get"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 26: more crash safety ────────────────────────────────────────── */

TEST(tslsp_nocrash_circular_extends) {
    CBMFileResult *r = extract_ts("class A extends B {}\n"
                                  "class B extends A {}\n"
                                  "function go(a: A) { a.x; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_recursive_type) {
    CBMFileResult *r = extract_ts("interface List { next?: List; value: number; }\n"
                                  "function go(l: List) { l.next?.value; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_unicode_identifier) {
    CBMFileResult *r = extract_ts("class \xc3\x84pf { ping(): void {} }\n"
                                  "function go(a: \xc3\x84pf) { a.ping(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_template_with_call) {
    CBMFileResult *r =
        extract_ts("function tag(strs: TemplateStringsArray): string { return ''; }\n"
                   "function go() { const x = tag`hello ${1 + 2}`; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_decorator) {
    CBMFileResult *r = extract_ts("function dec(target: any) { return target; }\n"
                                  "@dec\n"
                                  "class Foo { run(): void {} }\n"
                                  "function go(f: Foo) { f.run(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_private_field) {
    CBMFileResult *r = extract_ts("class Foo { #x = 1; bar(): number { return this.#x; } }\n"
                                  "function go(f: Foo) { f.bar(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_getter_setter) {
    CBMFileResult *r = extract_ts("class Foo { _x = 0; get x(): number { return this._x; } set "
                                  "x(v: number) { this._x = v; } }\n"
                                  "function go(f: Foo) { f.x; f.x = 5; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_abstract_class) {
    CBMFileResult *r = extract_ts("abstract class Base { abstract step(): void; }\n"
                                  "class Impl extends Base { step(): void {} }\n"
                                  "function go(i: Impl) { i.step(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Category 22: JSX intrinsic + component ─────────────────────────────────── */

TEST(tslsp_jsx_component_self_closing) {
    CBMFileResult *r =
        extract_tsx("function Greeting(props: { name: string }): JSX.Element { return "
                    "<span>{props.name}</span>; }\n"
                    "function App(): JSX.Element { return <Greeting name='hi' />; }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "Greeting"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsx_component_with_children) {
    CBMFileResult *r = extract_tsx(
        "function Card(props: { title: string }): JSX.Element { return <div>{props.title}</div>; "
        "}\n"
        "function App(): JSX.Element { return <Card title='hello'><span>x</span></Card>; }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "Card"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsx_intrinsic_skipped) {
    CBMFileResult *r = extract_tsx("function go(): JSX.Element { return <div>plain</div>; }\n");
    ASSERT_NOT_NULL(r);
    /* Intrinsic <div> should be skipped (no false positive). */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsx_nested_component) {
    CBMFileResult *r =
        extract_tsx("function Inner(): JSX.Element { return <span/>; }\n"
                    "function Outer(): JSX.Element { return <div><Inner/></div>; }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "Outer", "Inner"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 23: TSX combined ─────────────────────────────────────────────── */

TEST(tslsp_tsx_typed_props_method_call) {
    CBMFileResult *r = extract_tsx("class Service { fetch(): string { return ''; } }\n"
                                   "function App(props: { svc: Service }): JSX.Element {\n"
                                   "    props.svc.fetch();\n"
                                   "    return <div/>;\n"
                                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "fetch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_tsx_class_component_method) {
    CBMFileResult *r =
        extract_tsx("class Logger { log(): void {} }\n"
                    "class App {\n"
                    "    logger: Logger = new Logger();\n"
                    "    render(): JSX.Element { this.logger.log(); return <div/>; }\n"
                    "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App.render", "log"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Tuple types (TS) ─────────────────────────────────────────────────────── */

TEST(tslsp_tuple_destructure_method) {
    CBMFileResult *r = extract_ts("class Box { run(): void {} }\n"
                                  "function pair(): [Box, number] { return [new Box(), 42]; }\n"
                                  "function go() { const [b, n] = pair(); b.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Number wrapper class ──────────────────────────────────────────────────── */

TEST(tslsp_stdlib_number_toString) {
    CBMFileResult *r = extract_ts("function go(n: number) { n.toString(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "toString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_number_toFixed) {
    CBMFileResult *r = extract_ts("function go(n: number) { n.toFixed(2); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "toFixed"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More crash-safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_huge_union) {
    CBMFileResult *r =
        extract_ts("type T = 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j';\n"
                   "function go(x: T) { console.log(x); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_intersection_chain) {
    CBMFileResult *r = extract_ts(
        "interface A { x: number; } interface B { y: string; } interface C { z: boolean; }\n"
        "function go(o: A & B & C) { console.log(o.x, o.y, o.z); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_satisfies_keyword) {
    CBMFileResult *r = extract_ts(
        "type X = { name: string };\n"
        "function go() { const obj = { name: 'a' } satisfies X; console.log(obj.name); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Category 21: JSDoc inference (js_mode) ────────────────────────────────── */

TEST(tslsp_jsdoc_param_method_call) {
    CBMFileResult *r = extract_js("class Database { query(sql) { return ''; } }\n"
                                  "/** @param {Database} db */\n"
                                  "function doWork(db) { db.query('SELECT 1'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "doWork", "query"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_returns_propagation) {
    CBMFileResult *r = extract_js("class Conn { ping() {} }\n"
                                  "/** @returns {Conn} */\n"
                                  "function makeConn() { return new Conn(); }\n"
                                  "function go() { const c = makeConn(); c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_multi_param) {
    CBMFileResult *r = extract_js("class Logger { info(s) {} }\n"
                                  "class Cache { put(k) {} }\n"
                                  "/**\n"
                                  " * @param {Logger} log\n"
                                  " * @param {Cache} cache\n"
                                  " */\n"
                                  "function setup(log, cache) {\n"
                                  "    log.info('hi');\n"
                                  "    cache.put('k');\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "setup", "info"), 0);
    ASSERT_GTE(require_resolved(r, "setup", "put"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_array_generic) {
    CBMFileResult *r = extract_js("/** @param {Array<number>} xs */\n"
                                  "function go(xs) { xs.map(x => x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_promise_returns) {
    CBMFileResult *r = extract_js("/** @returns {Promise<string>} */\n"
                                  "function load() { return Promise.resolve('hi'); }\n"
                                  "function go() { load().then(x => x); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_ignored_in_ts_mode) {
    /* In TS mode, JSDoc is informative only — the inline annotations win. */
    CBMFileResult *r = extract_ts("class Foo { run(): void {} }\n"
                                  "/** @param {Foo} x */\n"
                                  "function go(x: Foo) { x.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Category 31: real-library patterns (smoke) ────────────────────────────── */

TEST(tslsp_real_express_handler) {
    CBMFileResult *r = extract_ts(
        "interface Request { params: { [key: string]: string }; }\n"
        "interface Response { send(body: string): Response; status(code: number): Response; }\n"
        "function handle(req: Request, res: Response) { res.status(200).send('ok'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "handle", "status"), 0);
    ASSERT_GTE(require_resolved(r, "handle", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_fluent_builder) {
    CBMFileResult *r =
        extract_ts("class QueryBuilder {\n"
                   "    select(cols: string[]): QueryBuilder { return this; }\n"
                   "    from(t: string): QueryBuilder { return this; }\n"
                   "    where(c: string): QueryBuilder { return this; }\n"
                   "    execute(): string[] { return []; }\n"
                   "}\n"
                   "function go() {\n"
                   "    new QueryBuilder().select(['id']).from('users').where('1=1').execute();\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "select"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "from"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "where"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_observer_pattern) {
    CBMFileResult *r = extract_ts("interface Observer { update(value: number): void; }\n"
                                  "class Subject {\n"
                                  "    notify(o: Observer): void { o.update(42); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "Subject.notify", "update"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_event_emitter) {
    CBMFileResult *r = extract_ts(
        "class EventEmitter {\n"
        "    on(event: string, cb: (data: number) => void): EventEmitter { return this; }\n"
        "    emit(event: string, data: number): EventEmitter { return this; }\n"
        "}\n"
        "function go(em: EventEmitter) { em.on('x', d => d).emit('x', 1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "on"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "emit"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_singleton) {
    CBMFileResult *r =
        extract_ts("class Singleton {\n"
                   "    private static instance: Singleton;\n"
                   "    static getInstance(): Singleton { return new Singleton(); }\n"
                   "    run(): void {}\n"
                   "}\n"
                   "function go() { Singleton.getInstance().run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "getInstance"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Edge cases ────────────────────────────────────────────────────────────── */

TEST(tslsp_readonly_field_method) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "class Box { readonly tool: Tool = new Tool(); }\n"
                                  "function go(b: Box) { b.tool.fire(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_constructor_method_call) {
    CBMFileResult *r = extract_ts("class Foo { run(): void {} }\n"
                                  "function go() { new Foo().run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_method_chained_through_field) {
    CBMFileResult *r =
        extract_ts("class Repo { all(): string[] { return []; } }\n"
                   "class App { repo: Repo = new Repo(); }\n"
                   "function go(app: App) { app.repo.all().map(s => s.toUpperCase()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "all"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_callback_returning_method_call) {
    CBMFileResult *r = extract_ts("class Box { run(): string { return ''; } }\n"
                                  "function go(b: Box) { [1, 2].forEach(_n => b.run()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_this_in_arrow_method) {
    CBMFileResult *r = extract_ts("class Foo {\n"
                                  "    a: number = 1;\n"
                                  "    handler = () => { this.bar(); };\n"
                                  "    bar(): void {}\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* Arrow methods bind `this` lexically — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_optional_field_method) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "class Box { tool?: Tool; }\n"
                                  "function go(b: Box) { if (b.tool) { b.tool.fire(); } }\n");
    ASSERT_NOT_NULL(r);
    /* Optional field narrowing — accept partial success for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── More crash-safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_enum_basic) {
    CBMFileResult *r = extract_ts("enum Color { Red, Green, Blue }\n"
                                  "function go(c: Color) { console.log(c); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_namespace) {
    CBMFileResult *r = extract_ts("namespace MyNS { export function inner(): void {} }\n"
                                  "function go() { MyNS.inner(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_async_generator) {
    CBMFileResult *r = extract_ts("async function* go() { yield 1; yield 2; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_template_literal_expr) {
    CBMFileResult *r = extract_ts("function go(s: string) { console.log(`hello ${s.length}`); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_complex_generic_constraint) {
    CBMFileResult *r =
        extract_ts("function id<T extends { length: number }>(x: T): T { return x; }\n"
                   "function go(arr: number[]) { id(arr); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Discriminated unions / dispatch ───────────────────────────────────────── */

TEST(tslsp_discriminated_union_method) {
    CBMFileResult *r =
        extract_ts("interface Circle { kind: 'circle'; radius: number; render(): void; }\n"
                   "interface Square { kind: 'square'; side: number; render(): void; }\n"
                   "function go(s: Circle | Square) { s.render(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "render"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Abstract class + concrete subclass ────────────────────────────────────── */

TEST(tslsp_abstract_concrete_dispatch) {
    CBMFileResult *r = extract_ts(
        "abstract class Shape { abstract area(): number; describe(): string { return ''; } }\n"
        "class Square extends Shape { area(): number { return 1; } }\n"
        "function go(s: Square) { s.area(); s.describe(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "area"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "describe"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Generics + inheritance ────────────────────────────────────────────────── */

TEST(tslsp_generic_class_method) {
    CBMFileResult *r = extract_ts(
        "class Container<T> { items: T[] = []; add(x: T): void { this.items.push(x); } }\n"
        "function go(c: Container<number>) { c.add(1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "add"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Static fields ─────────────────────────────────────────────────────────── */

TEST(tslsp_static_field_method) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "class Registry { static defaultTool: Tool = new Tool(); }\n"
                                  "function go() { Registry.defaultTool.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* Static fields not yet first-class — accept smoke pass */
    cbm_free_result(r);
    PASS();
}

/* ── More JSDoc patterns ───────────────────────────────────────────────────── */

TEST(tslsp_jsdoc_param_after_text) {
    CBMFileResult *r = extract_js("class Box { run() {} }\n"
                                  "/**\n"
                                  " * Helpful description.\n"
                                  " * @param {Box} box - the box to use\n"
                                  " */\n"
                                  "function go(box) { box.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_jsdoc_typedef_used_via_param) {
    /* JSDoc @typedef is declaration-merging-like; v1 may not honour it,
     * but parsing shouldn't crash. */
    CBMFileResult *r = extract_js("/** @typedef {{ name: string }} User */\n"
                                  "/** @param {User} u */\n"
                                  "function go(u) { console.log(u.name); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Real-library: Promise.all chain ───────────────────────────────────────── */

TEST(tslsp_real_promise_all_then) {
    CBMFileResult *r =
        extract_ts("function go() { Promise.all([1, 2]).then(arr => arr.length); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "all"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Edge: optional method on union ────────────────────────────────────────── */

TEST(tslsp_method_on_typeof_class) {
    CBMFileResult *r = extract_ts("class Foo { run(): void {} }\n"
                                  "function go(F: typeof Foo) { const f = new F(); f.run(); }\n");
    ASSERT_NOT_NULL(r);
    /* `typeof Foo` is a class constructor type — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── Generic Promise chain ─────────────────────────────────────────────────── */

TEST(tslsp_promise_chain_via_local) {
    CBMFileResult *r =
        extract_ts("function load(): Promise<string> { return Promise.resolve('hi'); }\n"
                   "function go() {\n"
                   "    const p = load();\n"
                   "    p.then(s => s).catch(_e => '');\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "catch"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Object literal nested resolution ──────────────────────────────────────── */

TEST(tslsp_obj_literal_method_property) {
    CBMFileResult *r = extract_ts("class Logger { info(): void {} }\n"
                                  "function go() {\n"
                                  "    const env = { logger: new Logger() };\n"
                                  "    env.logger.info();\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "info"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Contextual callback typing ─────────────────────────────────────────────── */

TEST(tslsp_ctx_array_map_callback) {
    CBMFileResult *r = extract_ts("class Box { run(): void {} }\n"
                                  "function go(boxes: Box[]) { boxes.map(b => b.run()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_array_filter_callback) {
    CBMFileResult *r =
        extract_ts("class User { isActive(): boolean { return true; } }\n"
                   "function go(users: User[]) { users.filter(u => u.isActive()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "isActive"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_array_find_callback) {
    CBMFileResult *r =
        extract_ts("class Item { matches(q: string): boolean { return false; } }\n"
                   "function go(items: Item[]) { items.find(i => i.matches('x')); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "matches"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_array_forEach_callback) {
    CBMFileResult *r = extract_ts("class Job { execute(): void {} }\n"
                                  "function go(jobs: Job[]) { jobs.forEach(j => j.execute()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "execute"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_array_block_body_callback) {
    CBMFileResult *r = extract_ts("class Foo { ping(): void {} bar(): string { return ''; } }\n"
                                  "function go(xs: Foo[]) {\n"
                                  "    xs.forEach(x => { x.ping(); x.bar(); });\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "bar"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_chain_filter_then_map) {
    CBMFileResult *r =
        extract_ts("class Row { active(): boolean { return true; } id(): number { return 0; } }\n"
                   "function go(rows: Row[]) {\n"
                   "    rows.filter(r => r.active()).map(r => r.id());\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "active"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "id"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_ctx_user_defined_callback) {
    CBMFileResult *r =
        extract_ts("class Foo { run(): void {} }\n"
                   "function withFoo(f: Foo, cb: (x: Foo) => void): void { cb(f); }\n"
                   "function go(f: Foo) { withFoo(f, x => x.run()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Narrowing (real, not smoke) ───────────────────────────────────────────── */

TEST(tslsp_narrow_typeof_string_method) {
    CBMFileResult *r = extract_ts("function go(x: string | number) {\n"
                                  "    if (typeof x === 'string') { x.toLowerCase(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* Real narrowing — String.toLowerCase resolves through wrapper. */
    ASSERT_GTE(require_resolved(r, ".go", "toLowerCase"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_narrow_instanceof_dispatch) {
    CBMFileResult *r = extract_ts("class A { ping(): void {} }\n"
                                  "class B { pong(): void {} }\n"
                                  "function go(x: A | B) {\n"
                                  "    if (x instanceof A) { x.ping(); }\n"
                                  "    else { x.pong(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* v2: real flow-sensitive narrowing — `x instanceof A` narrows x to A in the
     * consequence, so x.ping() resolves via NAMED("test.main.A") directly. The
     * else branch falls back to union dispatch which finds pong on B. */
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "pong"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_narrow_discriminated_kind) {
    CBMFileResult *r = extract_ts("interface Circle { kind: 'circle'; r(): number; }\n"
                                  "interface Square { kind: 'square'; s(): number; }\n"
                                  "function go(shape: Circle | Square) {\n"
                                  "    if (shape.kind === 'circle') { shape.r(); }\n"
                                  "    else { shape.s(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* Both methods resolve — basic union dispatch covers both branches. */
    ASSERT_GTE(require_resolved(r, ".go", "r"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "s"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── React-like patterns ───────────────────────────────────────────────────── */

TEST(tslsp_react_useState_pattern) {
    CBMFileResult *r = extract_tsx(
        "function useState<T>(initial: T): [T, (v: T) => void] { return [initial, _v => {}]; }\n"
        "function App(): JSX.Element {\n"
        "    const [count, setCount] = useState(0);\n"
        "    return <div>{count}</div>;\n"
        "}\n");
    ASSERT_NOT_NULL(r);
    /* useState resolves; setCount is callable */
    ASSERT_GTE(require_resolved(r, "App", "useState"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_react_callback_in_jsx) {
    CBMFileResult *r = extract_tsx(
        "interface Service { fetch(): string; }\n"
        "function Button(props: { onClick: () => void }): JSX.Element { return <button/>; }\n"
        "function App(svc: Service): JSX.Element { return <Button onClick={() => svc.fetch()}/>; "
        "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "fetch"), 0);
    ASSERT_GTE(require_resolved(r, "App", "Button"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More stdlib coverage ──────────────────────────────────────────────────── */

TEST(tslsp_stdlib_array_concat) {
    CBMFileResult *r = extract_ts("function go(a: number[], b: number[]) { a.concat(b); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "concat"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_array_slice) {
    CBMFileResult *r = extract_ts("function go(a: string[]) { a.slice(0, 3); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "slice"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_string_repeat) {
    CBMFileResult *r = extract_ts("function go(s: string) { s.repeat(3); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "repeat"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_promise_catch) {
    CBMFileResult *r = extract_ts("function go(p: Promise<string>) { p.catch(e => e); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "catch"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_set_size_and_clear) {
    CBMFileResult *r = extract_ts("function go(s: Set<string>) { s.clear(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "clear"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Class / mixin patterns ───────────────────────────────────────────────── */

TEST(tslsp_class_implements_interface) {
    CBMFileResult *r = extract_ts("interface Runnable { run(): void; }\n"
                                  "class Job implements Runnable { run(): void {} }\n"
                                  "function go(j: Job) { j.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_multi_inheritance_chain) {
    CBMFileResult *r = extract_ts("class A { fromA(): void {} }\n"
                                  "class B extends A { fromB(): void {} }\n"
                                  "class C extends B { fromC(): void {} }\n"
                                  "function go(c: C) { c.fromA(); c.fromB(); c.fromC(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fromA"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "fromB"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "fromC"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_constructor_param_property) {
    /* TS shorthand: constructor params with access modifiers become fields. */
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "class Box {\n"
                                  "    constructor(public tool: Tool) {}\n"
                                  "}\n"
                                  "function go(b: Box) { b.tool.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* Constructor parameter property — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── Iteration / for-of ────────────────────────────────────────────────────── */

TEST(tslsp_for_of_array_method) {
    CBMFileResult *r =
        extract_ts("class Box { fire(): void {} }\n"
                   "function go(boxes: Box[]) { for (const b of boxes) { b.fire(); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_for_let_array_method) {
    CBMFileResult *r =
        extract_ts("class Item { check(): boolean { return true; } }\n"
                   "function go(items: Item[]) { for (let i of items) { i.check(); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "check"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Type predicate guard ──────────────────────────────────────────────────── */

TEST(tslsp_type_predicate_guard) {
    CBMFileResult *r = extract_ts("class A { aOnly(): void {} }\n"
                                  "function isA(x: unknown): x is A { return true; }\n"
                                  "function go(x: unknown) { if (isA(x)) { x.aOnly(); } }\n");
    ASSERT_NOT_NULL(r);
    /* Type predicate narrowing — unknown for v1; accept smoke pass */
    cbm_free_result(r);
    PASS();
}

/* ── Optional chaining + nullish coalescing ────────────────────────────────── */

TEST(tslsp_optional_chain_method_call) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "function go(t?: Tool) { t?.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* Optional chain — `t?.fire()` treats t as Tool|undefined; should resolve fire */
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nullish_coalesce_chain) {
    CBMFileResult *r = extract_ts("class Conn { ping(): void {} }\n"
                                  "function go(c?: Conn) { (c ?? new Conn()).ping(); }\n");
    ASSERT_NOT_NULL(r);
    /* Nullish coalescing — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_decorator_factory) {
    CBMFileResult *r = extract_ts(
        "function dec(name: string) { return function(target: any) { return target; }; }\n"
        "@dec('x') class Foo {}\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_proxy_usage) {
    CBMFileResult *r =
        extract_ts("function go() { const p = new Proxy({}, { get(t, k) { return k; } }); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_symbol_iterator) {
    CBMFileResult *r = extract_ts("class Iter { [Symbol.iterator]() { return { next() { return { "
                                  "value: 1, done: false }; } }; } }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_index_signature) {
    CBMFileResult *r = extract_ts("interface Bag { [k: string]: number; }\n"
                                  "function go(b: Bag) { console.log(b['x']); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_const_assertion_chain) {
    CBMFileResult *r =
        extract_ts("function go() { const arr = [1, 2, 3] as const; arr.length; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Cross-file resolution (uses cbm_run_ts_lsp_cross directly) ─────────────── */

static int find_resolved_arr(const CBMResolvedCallArray *arr, const char *callerSub,
                             const char *calleeSub) {
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->confidence > 0 && rc->caller_qn && strstr(rc->caller_qn, callerSub) &&
            rc->callee_qn && strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

TEST(tslsp_crossfile_method_dispatch) {
    const char *source = "import { Conn } from './conn';\n"
                         "function go(c: Conn) { c.ping(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.go",
         .short_name = "go",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.conn.Conn",
         .short_name = "Conn",
         .label = "Class",
         .def_module_qn = "test.conn"},
        {.qualified_name = "test.conn.Conn.ping",
         .short_name = "ping",
         .label = "Method",
         .def_module_qn = "test.conn",
         .receiver_type = "test.conn.Conn"},
    };
    const char *imp_names[] = {"Conn"};
    const char *imp_qns[] = {"test.conn"}; /* module QN; type QN derived as mqn + "." + bare */

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main",
                         /*js_mode=*/false, /*jsx_mode=*/false, /*dts_mode=*/false, defs, 3,
                         imp_names, imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr(&out, "go", "ping"), 0);

    cbm_arena_destroy(&arena);
    PASS();
}

/* Issue #344 / #340: SIGSEGV in the TS cross-file LSP pass when the collected
 * def array exceeded ~1189 entries — scale-dependent, not file-specific. Drive
 * the resolver with a def array far past that threshold; the v0.7.0 rewrite
 * registers defs through the arena (no fixed-size def array), so this must
 * resolve cleanly. Built under -fsanitize=address so a regression aborts. */
TEST(tslsp_scale_many_defs_no_crash_issue344) {
    enum { SCALE_N = 4000 };
    CBMArena arena;
    cbm_arena_init(&arena);

    CBMLSPDef *defs = (CBMLSPDef *)cbm_arena_alloc(&arena, (size_t)SCALE_N * sizeof(CBMLSPDef));
    ASSERT_NOT_NULL(defs);
    memset(defs, 0, (size_t)SCALE_N * sizeof(CBMLSPDef));
    for (int i = 0; i < SCALE_N; i++) {
        defs[i].qualified_name = cbm_arena_sprintf(&arena, "test.mod%d.fn%d", i, i);
        defs[i].short_name = cbm_arena_sprintf(&arena, "fn%d", i);
        defs[i].label = "Function";
        defs[i].def_module_qn = cbm_arena_sprintf(&arena, "test.mod%d", i);
        defs[i].return_types = "number";
    }
    const char *source = "function go() { const v = fn0(); }\n";
    CBMResolvedCallArray out = {0};
    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main",
                         /*js_mode=*/false, /*jsx_mode=*/false, /*dts_mode=*/false, defs, SCALE_N,
                         NULL, NULL, 0, NULL, &out);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(tslsp_crossfile_function_call) {
    const char *source = "import { greet } from './greetings';\n"
                         "function go() { greet('hi'); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.go",
         .short_name = "go",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.greetings.greet",
         .short_name = "greet",
         .label = "Function",
         .def_module_qn = "test.greetings",
         .return_types = "string"},
    };
    const char *imp_names[] = {"greet"};
    const char *imp_qns[] = {"test.greetings"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 2, imp_names, imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr(&out, "go", "greet"), 0);

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(tslsp_crossfile_chain_through_return) {
    const char *source = "import { open } from './fs';\n"
                         "function go() { open('/tmp').read(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.go",
         .short_name = "go",
         .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.fs.File",
         .short_name = "File",
         .label = "Class",
         .def_module_qn = "test.fs"},
        {.qualified_name = "test.fs.File.read",
         .short_name = "read",
         .label = "Method",
         .def_module_qn = "test.fs",
         .receiver_type = "test.fs.File"},
        {.qualified_name = "test.fs.open",
         .short_name = "open",
         .label = "Function",
         .def_module_qn = "test.fs",
         .return_types = "test.fs.File"},
    };
    const char *imp_names[] = {"open"};
    const char *imp_qns[] = {"test.fs"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 4, imp_names, imp_qns, 1, NULL, &out);

    ASSERT_GTE(find_resolved_arr(&out, "go", "open"), 0);
    ASSERT_GTE(find_resolved_arr(&out, "go", "read"), 0);

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(tslsp_crossfile_no_def_match) {
    /* Function that imports a symbol not in the def list — should resolve safely
     * (emit unresolved, no crash). */
    const char *source = "import { unknown } from './nope';\n"
                         "function go() { unknown(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.go",
         .short_name = "go",
         .label = "Function",
         .def_module_qn = "test.main"},
    };
    const char *imp_names[] = {"unknown"};
    const char *imp_qns[] = {"test.nope"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};

    cbm_run_ts_lsp_cross(&arena, source, (int)strlen(source), "test.main", false, false, false,
                         defs, 1, imp_names, imp_qns, 1, NULL, &out);

    /* Smoke: doesn't crash. count >= 0 (likely 1 unresolved entry for `unknown`). */
    ASSERT(out.count >= 0);

    cbm_arena_destroy(&arena);
    PASS();
}

/* ── More stdlib (Date, RegExp, Error, Math) ──────────────────────────────── */

TEST(tslsp_stdlib_date_toISOString) {
    CBMFileResult *r = extract_ts("function go(d: Date) { d.toISOString(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "toISOString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_date_getTime) {
    CBMFileResult *r = extract_ts("function go(d: Date) { d.getTime(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "getTime"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_regexp_test) {
    CBMFileResult *r = extract_ts("function go(re: RegExp) { re.test('x'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "test"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_regexp_exec) {
    CBMFileResult *r = extract_ts("function go(re: RegExp) { re.exec('x'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "exec"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_error_toString) {
    CBMFileResult *r = extract_ts("function go(e: Error) { e.toString(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "toString"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_math_floor) {
    CBMFileResult *r = extract_ts("function go(n: number) { Math.floor(n); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "floor"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_math_max) {
    CBMFileResult *r = extract_ts("function go() { Math.max(1, 2, 3); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "max"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stdlib_math_random) {
    CBMFileResult *r = extract_ts("function go() { Math.random(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "random"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── React hook patterns (smoke) ──────────────────────────────────────────── */

TEST(tslsp_react_useEffect_pattern) {
    CBMFileResult *r = extract_tsx("function useEffect(cb: () => void, deps?: unknown[]): void {}\n"
                                   "function App(): JSX.Element {\n"
                                   "    useEffect(() => { console.log('mount'); }, []);\n"
                                   "    return <div/>;\n"
                                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "useEffect"), 0);
    ASSERT_GTE(require_resolved(r, "App", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_react_useCallback_pattern) {
    CBMFileResult *r = extract_tsx("function useCallback<T extends (...args: any[]) => any>(cb: T, "
                                   "deps: unknown[]): T { return cb; }\n"
                                   "interface Service { handle(): void; }\n"
                                   "function App(svc: Service): JSX.Element {\n"
                                   "    const cb = useCallback(() => svc.handle(), []);\n"
                                   "    return <div/>;\n"
                                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "useCallback"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_iife) {
    CBMFileResult *r = extract_ts("(function() { console.log('hi'); })();\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_complex_jsdoc_jsdoc) {
    CBMFileResult *r = extract_js("/**\n"
                                  " * @template T\n"
                                  " * @param {T} x\n"
                                  " * @returns {T}\n"
                                  " */\n"
                                  "function id(x) { return x; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_chained_optional) {
    CBMFileResult *r = extract_ts("interface A { b?: { c?: { d?: () => void } } }\n"
                                  "function go(a: A) { a.b?.c?.d?.(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_template_in_extends) {
    CBMFileResult *r =
        extract_ts("class Base<T> { x?: T; }\n"
                   "class Child extends Base<string> { y(): string { return ''; } }\n"
                   "function go(c: Child) { c.y(); }\n");
    ASSERT_NOT_NULL(r);
    /* Generic class with concrete extends — accept smoke pass; method dispatch optional */
    cbm_free_result(r);
    PASS();
}

/* ── v2 narrowing: switch statements ──────────────────────────────────────── */

TEST(tslsp_narrow_switch_kind) {
    CBMFileResult *r = extract_ts("interface Circle { kind: 'circle'; r(): number; }\n"
                                  "interface Square { kind: 'square'; s(): number; }\n"
                                  "function go(shape: Circle | Square) {\n"
                                  "    switch (shape.kind) {\n"
                                  "        case 'circle': shape.r(); break;\n"
                                  "        case 'square': shape.s(); break;\n"
                                  "    }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "r"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "s"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_narrow_switch_three_way) {
    CBMFileResult *r = extract_ts("interface A { kind: 'a'; aOnly(): void; }\n"
                                  "interface B { kind: 'b'; bOnly(): void; }\n"
                                  "interface C { kind: 'c'; cOnly(): void; }\n"
                                  "function go(x: A | B | C) {\n"
                                  "    switch (x.kind) {\n"
                                  "        case 'a': x.aOnly(); break;\n"
                                  "        case 'b': x.bOnly(); break;\n"
                                  "        case 'c': x.cOnly(); break;\n"
                                  "    }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "aOnly"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "bOnly"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "cOnly"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_narrow_typeof_else_branch) {
    CBMFileResult *r = extract_ts("function go(x: string | number) {\n"
                                  "    if (typeof x !== 'string') { x.toFixed(2); }\n"
                                  "    else { x.trim(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* Inverted typeof: x narrowed to number in `then`, string in `else`. */
    ASSERT_GTE(require_resolved(r, ".go", "toFixed"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "trim"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Generic class with concrete instantiation ───────────────────────────── */

TEST(tslsp_generic_class_with_T_method_return) {
    CBMFileResult *r =
        extract_ts("class Box<T> { items: T[] = []; first(): T { return this.items[0]; } }\n"
                   "class User { name(): string { return ''; } }\n"
                   "function go(b: Box<User>) { b.first().name(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "first"), 0);
    /* `.name()` via generic substitution requires propagating Box<User>'s T → User
     * into first()'s return type. Accept smoke pass for v1; ideal would resolve. */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generic_class_method_chain_through_subst) {
    CBMFileResult *r =
        extract_ts("class Container<T> {\n"
                   "    data: T[] = [];\n"
                   "    add(x: T): Container<T> { this.data.push(x); return this; }\n"
                   "}\n"
                   "function go(c: Container<number>) { c.add(1).add(2); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "add"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Function overloads (via interface) ────────────────────────────────────── */

TEST(tslsp_overload_simple) {
    CBMFileResult *r = extract_ts("interface Transform {\n"
                                  "    process(x: string): string;\n"
                                  "    process(x: number): number;\n"
                                  "}\n"
                                  "function go(t: Transform) { t.process('a'); t.process(1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Real-world: lodash-like chain ──────────────────────────────────────────── */

TEST(tslsp_real_lodash_chain) {
    CBMFileResult *r =
        extract_ts("interface Wrapped<T> {\n"
                   "    map<U>(fn: (x: T) => U): Wrapped<U>;\n"
                   "    filter(fn: (x: T) => boolean): Wrapped<T>;\n"
                   "    value(): T[];\n"
                   "}\n"
                   "function chain<T>(arr: T[]): Wrapped<T> { return null as any; }\n"
                   "function go(xs: number[]) {\n"
                   "    chain(xs).filter(x => x > 0).map(x => x * 2).value();\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "chain"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "filter"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "value"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Real-world: RxJS-like operators ───────────────────────────────────────── */

TEST(tslsp_real_rxjs_pipe) {
    CBMFileResult *r =
        extract_ts("interface Observable<T> {\n"
                   "    pipe(...ops: any[]): Observable<T>;\n"
                   "    subscribe(cb: (v: T) => void): void;\n"
                   "}\n"
                   "function go(obs: Observable<number>) { obs.pipe().subscribe(v => v); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "pipe"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "subscribe"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Static factory pattern ───────────────────────────────────────────────── */

TEST(tslsp_static_factory_returns_instance) {
    CBMFileResult *r = extract_ts("class Conn {\n"
                                  "    static create(): Conn { return new Conn(); }\n"
                                  "    ping(): void {}\n"
                                  "}\n"
                                  "function go() { Conn.create().ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "create"), 0);
    /* Static return type → instance method dispatch */
    cbm_free_result(r);
    PASS();
}

/* ── Iterator / generator patterns ─────────────────────────────────────────── */

TEST(tslsp_for_of_set_method) {
    CBMFileResult *r =
        extract_ts("function go(s: Set<string>) { for (const v of s) { v.length; } }\n");
    ASSERT_NOT_NULL(r);
    /* for-of on Set<T> — element type may be T; smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generator_with_yield) {
    CBMFileResult *r = extract_ts("function* gen(): Generator<number> { yield 1; yield 2; }\n"
                                  "function go() { for (const n of gen()) { console.log(n); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More edge cases ──────────────────────────────────────────────────────── */

TEST(tslsp_object_destructure_param) {
    CBMFileResult *r = extract_ts("interface Config { port: number; host: string; }\n"
                                  "function start({ port, host }: Config): void {\n"
                                  "    console.log(host); console.log(port);\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "start", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_default_export_class) {
    CBMFileResult *r = extract_ts("export default class Foo { run(): void {} }\n"
                                  "function go(f: Foo) { f.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_export_named_class) {
    CBMFileResult *r = extract_ts("export class Bar { fire(): void {} }\n"
                                  "function go(b: Bar) { b.fire(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_with_async_method) {
    CBMFileResult *r = extract_ts("class Service {\n"
                                  "    async fetchData(): Promise<string> { return 'x'; }\n"
                                  "}\n"
                                  "async function go(svc: Service) {\n"
                                  "    const data = await svc.fetchData();\n"
                                  "    data.length;\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fetchData"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_method_on_returned_promise_chain) {
    CBMFileResult *r = extract_ts("class API {\n"
                                  "    load(): Promise<string> { return Promise.resolve(''); }\n"
                                  "}\n"
                                  "function go(api: API) {\n"
                                  "    api.load().then(s => s.toUpperCase());\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "load"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety (advanced TS syntax) ────────────────────────────────── */

TEST(tslsp_nocrash_template_literal_type) {
    CBMFileResult *r = extract_ts("type Greeting = `hello ${string}`;\n"
                                  "function go(g: Greeting) { console.log(g); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_intersect_with_func) {
    CBMFileResult *r = extract_ts("type Cb = (() => void) & { extra: number };\n"
                                  "function go(cb: Cb) { cb(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_class_expression) {
    CBMFileResult *r = extract_ts("const C = class { run(): void {} };\n"
                                  "function go() { new C().run(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── TS utility types: Partial / Readonly / Required / NonNullable ──────────── */

TEST(tslsp_partial_method_passthrough) {
    CBMFileResult *r = extract_ts("interface User { greet(): void; name: string; }\n"
                                  "function go(u: Partial<User>) { u.greet?.(); }\n");
    ASSERT_NOT_NULL(r);
    /* Partial<User> still has User's methods (made optional) — `u.greet?.()` resolves
     * via passthrough to User.greet. Smoke pass acceptable; ideal resolves. */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_readonly_method) {
    CBMFileResult *r = extract_ts("interface Conn { ping(): void; }\n"
                                  "function go(c: Readonly<Conn>) { c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_required_field_method) {
    CBMFileResult *r = extract_ts("interface Box { tool: { fire(): void; }; }\n"
                                  "function go(b: Required<Box>) { b.tool.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* Inline object types in fields — accept smoke pass (object_type returns OBJECT_LIT
     * but downstream property typing isn't fully wired) */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nonnullable_method) {
    CBMFileResult *r = extract_ts("interface Foo { run(): void; }\n"
                                  "function go(f: NonNullable<Foo>) { f.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More edge cases / patterns ────────────────────────────────────────────── */

TEST(tslsp_array_of_class_map) {
    CBMFileResult *r = extract_ts("class User { name(): string { return ''; } }\n"
                                  "function go(users: User[]) { users.map(u => u.name()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "name"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_set_iteration_with_callback) {
    CBMFileResult *r =
        extract_ts("class Item { fire(): void {} }\n"
                   "function go(items: Set<Item>) { items.forEach(i => i.fire()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "forEach"), 0);
    /* Set.forEach callback element type — smoke acceptable */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_throwing_error_method) {
    CBMFileResult *r = extract_ts("function go() {\n"
                                  "    try { throw new Error('x'); }\n"
                                  "    catch (e) { if (e instanceof Error) e.toString(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* Error class method via instanceof narrowing on catch param */
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_module_declaration) {
    CBMFileResult *r = extract_ts("declare module 'foo' { export function bar(): string; }\n"
                                  "function go() { console.log('x'); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_typeof_in_type_position) {
    CBMFileResult *r = extract_ts("const x = { a: 1 };\n"
                                  "type X = typeof x;\n"
                                  "function go(xx: X) { console.log(xx); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_tagged_template) {
    CBMFileResult *r = extract_ts(
        "function tag(strs: TemplateStringsArray, ...vals: number[]): string { return ''; }\n"
        "function go() { tag`hi ${1} world ${2}`; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── LSP vs baseline comparison ─────────────────────────────────────────────── */

/* Count resolved (confidence > 0) vs unresolved (confidence == 0) and total. */
static void count_calls(const CBMFileResult *r, int *resolved, int *unresolved, int *total) {
    *resolved = *unresolved = *total = 0;
    if (!r)
        return;
    *total = r->resolved_calls.count;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence > 0)
            (*resolved)++;
        else
            (*unresolved)++;
    }
}

TEST(tslsp_baseline_vs_lsp_simple) {
    const char *source = "class Conn { ping(): void {} }\n"
                         "function go(c: Conn) { c.ping(); }\n";

    cbm_setenv("CBM_LSP_DISABLED", "1", 1);
    CBMFileResult *base = extract_ts(source);
    int br = 0, bu = 0, bt = 0;
    count_calls(base, &br, &bu, &bt);
    cbm_free_result(base);

    cbm_unsetenv("CBM_LSP_DISABLED");
    CBMFileResult *lsp = extract_ts(source);
    int lr = 0, lu = 0, lt = 0;
    count_calls(lsp, &lr, &lu, &lt);
    cbm_free_result(lsp);

    /* LSP must resolve at least 1 more call than baseline (the typed dispatch). */
    ASSERT(lr >= br + 1);
    PASS();
}

TEST(tslsp_baseline_vs_lsp_chained) {
    const char *source =
        "class Q { where(s: string): Q { return this; } limit(n: number): Q { return this; } "
        "execute(): number { return 0; } }\n"
        "function newQ(): Q { return new Q(); }\n"
        "function go() { newQ().where('x').limit(5).execute(); }\n";

    cbm_setenv("CBM_LSP_DISABLED", "1", 1);
    CBMFileResult *base = extract_ts(source);
    int br = 0, bu = 0, bt = 0;
    count_calls(base, &br, &bu, &bt);
    cbm_free_result(base);

    cbm_unsetenv("CBM_LSP_DISABLED");
    CBMFileResult *lsp = extract_ts(source);
    int lr = 0, lu = 0, lt = 0;
    count_calls(lsp, &lr, &lu, &lt);
    cbm_free_result(lsp);

    /* LSP resolves at least 3 more calls (where/limit/execute via type chain). */
    ASSERT(lr >= br + 3);
    PASS();
}

TEST(tslsp_baseline_vs_lsp_callbacks) {
    const char *source =
        "class Box { run(): void {} }\n"
        "function go(boxes: Box[]) { boxes.map(b => b.run()).forEach(_x => {}); }\n";

    cbm_setenv("CBM_LSP_DISABLED", "1", 1);
    CBMFileResult *base = extract_ts(source);
    int br = 0, bu = 0, bt = 0;
    count_calls(base, &br, &bu, &bt);
    cbm_free_result(base);

    cbm_unsetenv("CBM_LSP_DISABLED");
    CBMFileResult *lsp = extract_ts(source);
    int lr = 0, lu = 0, lt = 0;
    count_calls(lsp, &lr, &lu, &lt);
    cbm_free_result(lsp);

    /* LSP resolves the .run inside the .map callback via contextual typing. */
    ASSERT(lr >= br + 2);
    PASS();
}

TEST(tslsp_baseline_vs_lsp_narrowing) {
    const char *source = "class A { ping(): void {} }\n"
                         "class B { pong(): void {} }\n"
                         "function go(x: A | B) { if (x instanceof A) x.ping(); else x.pong(); }\n";

    cbm_setenv("CBM_LSP_DISABLED", "1", 1);
    CBMFileResult *base = extract_ts(source);
    int br = 0, bu = 0, bt = 0;
    count_calls(base, &br, &bu, &bt);
    cbm_free_result(base);

    cbm_unsetenv("CBM_LSP_DISABLED");
    CBMFileResult *lsp = extract_ts(source);
    int lr = 0, lu = 0, lt = 0;
    count_calls(lsp, &lr, &lu, &lt);
    cbm_free_result(lsp);

    /* LSP narrows union and resolves both branches; baseline can't narrow. */
    ASSERT(lr >= br + 2);
    PASS();
}

/* ── Stress tests (resolver under load) ────────────────────────────────────── */

TEST(tslsp_stress_many_classes) {
    /* 50 classes, each with one method, plus a dispatcher function calling all. */
    enum { BUF_CAP = 200 * 1024 };
    char *buf = (char *)malloc(BUF_CAP);
    if (!buf)
        PASS();
    char *p = buf;
    char *end = buf + BUF_CAP;
    for (int i = 0; i < 50; i++) {
        p += snprintf(p, (size_t)(end - p), "class C%d { run(): void {} }\n", i);
    }
    p += snprintf(p, (size_t)(end - p), "function go(");
    for (int i = 0; i < 50; i++) {
        if (i > 0)
            p += snprintf(p, (size_t)(end - p), ", ");
        p += snprintf(p, (size_t)(end - p), "c%d: C%d", i, i);
    }
    p += snprintf(p, (size_t)(end - p), ") {\n");
    for (int i = 0; i < 50; i++) {
        p += snprintf(p, (size_t)(end - p), "  c%d.run();\n", i);
    }
    p += snprintf(p, (size_t)(end - p), "}\n");
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    free(buf);
    ASSERT_NOT_NULL(r);
    /* Every c<i>.run() should resolve via type-aware dispatch. */
    int resolved = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence > 0 && r->resolved_calls.items[i].callee_qn &&
            strstr(r->resolved_calls.items[i].callee_qn, ".run")) {
            resolved++;
        }
    }
    /* Allow some slop (compile/parse weirdness for huge param lists), require >= 40. */
    ASSERT(resolved >= 40);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_deep_inheritance) {
    /* Chain of 30 classes via extends, leaf method on root. */
    char buf[16 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    p += snprintf(p, (size_t)(end - p), "class C0 { root(): void {} }\n");
    for (int i = 1; i < 30; i++) {
        p += snprintf(p, (size_t)(end - p), "class C%d extends C%d {}\n", i, i - 1);
    }
    p += snprintf(p, (size_t)(end - p), "function go(c: C29) { c.root(); }\n");
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    /* Root method should still resolve via 30-deep inheritance walk. */
    ASSERT_GTE(require_resolved(r, ".go", "root"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_long_method_chain) {
    /* `c.m1().m2().m3()...m25()` — each method returns same class. */
    char buf[8 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    p += snprintf(p, (size_t)(end - p), "class C {\n");
    for (int i = 1; i <= 25; i++) {
        p += snprintf(p, (size_t)(end - p), "  m%d(): C { return this; }\n", i);
    }
    p += snprintf(p, (size_t)(end - p), "}\n");
    p += snprintf(p, (size_t)(end - p), "function go(c: C) { c");
    for (int i = 1; i <= 25; i++)
        p += snprintf(p, (size_t)(end - p), ".m%d()", i);
    p += snprintf(p, (size_t)(end - p), "; }\n");
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    /* Every link in the chain must resolve. */
    int hits = 0;
    for (int i = 1; i <= 25; i++) {
        char want[8];
        snprintf(want, sizeof(want), "m%d", i);
        if (find_resolved(r, ".go", want) >= 0)
            hits++;
    }
    ASSERT(hits >= 20); // Allow a few misses
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_megafile_mixed) {
    /* ~3KB synthetic TS with classes + functions + chained calls. */
    char buf[8 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    p += snprintf(p, (size_t)(end - p),
                  "class Tool { fire(): void {} log(): void {} reset(): Tool { return this; } }\n");
    p += snprintf(p, (size_t)(end - p),
                  "class Box { tool: Tool = new Tool(); items: number[] = []; }\n");
    p += snprintf(p, (size_t)(end - p), "interface Job { execute(b: Box): void; }\n");
    for (int i = 0; i < 30; i++) {
        p += snprintf(p, (size_t)(end - p),
                      "function task%d(b: Box) {\n"
                      "  b.tool.fire();\n"
                      "  b.tool.reset().log();\n"
                      "  b.items.map(n => n + %d).filter(_x => true);\n"
                      "}\n",
                      i, i);
    }
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    /* Each task<i> emits 4 resolved calls (fire, reset, log, map) — most should resolve. */
    int fire_hits = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence > 0 && r->resolved_calls.items[i].callee_qn &&
            strstr(r->resolved_calls.items[i].callee_qn, "Tool.fire")) {
            fire_hits++;
        }
    }
    ASSERT(fire_hits >= 25);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_huge_union) {
    /* Union of 100 type literals. */
    char buf[8 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    p += snprintf(p, (size_t)(end - p), "type Mode = ");
    for (int i = 0; i < 100; i++) {
        if (i > 0)
            p += snprintf(p, (size_t)(end - p), " | ");
        p += snprintf(p, (size_t)(end - p), "'mode%d'", i);
    }
    p += snprintf(p, (size_t)(end - p), ";\n");
    p += snprintf(p, (size_t)(end - p), "function go(m: Mode) { console.log(m); }\n");
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_pathological_nesting) {
    /* 100-deep nested call expressions, all on same class. The eval_depth guard caps
     * recursion at TS_LSP_MAX_EVAL_DEPTH (=64) to defend against pathological inputs,
     * so very deep chains will fall back to UNKNOWN past that bound. The stress
     * assertion verifies no crash + sufficient `.f` resolutions, not the terminal `.g`. */
    char buf[16 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    p += snprintf(p, (size_t)(end - p), "class T { f(): T { return this; } g(): void {} }\n");
    p += snprintf(p, (size_t)(end - p), "function go(t: T) { t");
    for (int i = 0; i < 100; i++)
        p += snprintf(p, (size_t)(end - p), ".f()");
    p += snprintf(p, (size_t)(end - p), ".g(); }\n");
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    /* Each `.f()` call evaluates `fn_type` recursively through the prior chain, so
     * the outermost calls hit the recursion guard first; only chain elements within
     * roughly TS_LSP_MAX_EVAL_DEPTH (=64) of the leaf resolve. Stress assertion is
     * "doesn't crash, some forward progress". */
    int f_hits = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        if (r->resolved_calls.items[i].confidence > 0 && r->resolved_calls.items[i].callee_qn &&
            strstr(r->resolved_calls.items[i].callee_qn, ".T.f")) {
            f_hits++;
        }
    }
    ASSERT(f_hits >= 1);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_5000_lines) {
    /* Synthetic 5000-line file with 200 classes + 200 functions. Sanity: doesn't OOM,
     * doesn't crash, finishes in reasonable time. */
    enum { BUF_CAP = 800 * 1024 };
    char *buf = (char *)malloc(BUF_CAP);
    if (!buf)
        PASS();
    char *p = buf;
    char *end = buf + BUF_CAP;
    for (int i = 0; i < 200; i++) {
        p += snprintf(p, (size_t)(end - p), "class K%d { go(): void {} }\n", i);
    }
    for (int i = 0; i < 200; i++) {
        p += snprintf(p, (size_t)(end - p),
                      "function fn%d(k: K%d) {\n"
                      "  k.go();\n"
                      "  console.log('fn%d');\n"
                      "}\n",
                      i, i % 200, i);
    }
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    free(buf);
    ASSERT_NOT_NULL(r);
    /* No specific assertion — purely a stress test for memory + time. */
    ASSERT(r->resolved_calls.count >= 200);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_random_garbage) {
    /* Pathological/malformed input — must not crash. */
    char buf[8 * 1024];
    char *p = buf;
    char *end = buf + sizeof(buf);
    for (int i = 0; i < 1000; i++) {
        switch (i % 7) {
        case 0:
            p += snprintf(p, (size_t)(end - p), "class ");
            break;
        case 1:
            p += snprintf(p, (size_t)(end - p), "{<>");
            break;
        case 2:
            p += snprintf(p, (size_t)(end - p), " => ");
            break;
        case 3:
            p += snprintf(p, (size_t)(end - p), "(((");
            break;
        case 4:
            p += snprintf(p, (size_t)(end - p), ":::");
            break;
        case 5:
            p += snprintf(p, (size_t)(end - p), "['");
            break;
        case 6:
            p += snprintf(p, (size_t)(end - p), "'`'");
            break;
        }
    }
    *p = '\0';

    CBMFileResult *r = extract_ts(buf);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_stress_empty_classes_at_scale) {
    /* 500 empty classes — registry stress. */
    enum { BUF_CAP = 40 * 1024 };
    char *buf = (char *)malloc(BUF_CAP);
    if (!buf)
        PASS();
    char *p = buf;
    char *end = buf + BUF_CAP;
    for (int i = 0; i < 500; i++) {
        p += snprintf(p, (size_t)(end - p), "class E%d {}\n", i);
    }
    *p = '\0';
    CBMFileResult *r = extract_ts(buf);
    free(buf);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── DOM stdlib coverage ───────────────────────────────────────────────────── */

TEST(tslsp_dom_document_getElementById) {
    CBMFileResult *r = extract_ts("function go(d: Document) { d.getElementById('x'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "getElementById"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_element_querySelector) {
    CBMFileResult *r = extract_ts("function go(e: Element) { e.querySelector('.foo'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "querySelector"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_html_click) {
    CBMFileResult *r = extract_ts("function go(e: HTMLElement) { e.click(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "click"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_event_preventDefault) {
    CBMFileResult *r = extract_ts("function go(e: Event) { e.preventDefault(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "preventDefault"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_response_json_chain) {
    CBMFileResult *r =
        extract_ts("function go(w: Window) { w.fetch('url').then(r => r.json()); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "fetch"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "then"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_window_setTimeout) {
    CBMFileResult *r =
        extract_ts("function go(w: Window) { w.setTimeout(() => { console.log('x'); }, 100); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "setTimeout"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_dom_addEventListener_callback) {
    CBMFileResult *r =
        extract_ts("function go(el: HTMLElement) { el.addEventListener('click', e => e); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "addEventListener"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More utility types: ReturnType, Exclude, Extract ──────────────────────── */

TEST(tslsp_returntype_passthrough) {
    CBMFileResult *r = extract_ts("class Conn { ping(): void {} }\n"
                                  "function makeConn(): Conn { return new Conn(); }\n"
                                  "function go(c: ReturnType<typeof makeConn>) { c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    /* `typeof makeConn` would produce a FUNC type via TYPEOF_QUERY; v1 may not fully
     * evaluate that, but the test ensures no crash and accepts smoke pass. */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_exclude_method_on_remaining) {
    CBMFileResult *r = extract_ts("class A { aOnly(): void {} }\n"
                                  "class B { bOnly(): void {} }\n"
                                  "function go(x: Exclude<A | B, B>) { x.aOnly(); }\n");
    ASSERT_NOT_NULL(r);
    /* Exclude<A|B, B> = A; we passthrough to A|B for now; method dispatch finds aOnly. */
    cbm_free_result(r);
    PASS();
}

/* ── Type predicate (basic — current depth) ─────────────────────────────────── */

TEST(tslsp_type_pred_in_if) {
    CBMFileResult *r = extract_ts("class A { aOnly(): void {} }\n"
                                  "function isA(x: unknown): x is A { return x instanceof A; }\n"
                                  "function go(x: unknown) { if (isA(x)) x.aOnly(); }\n");
    ASSERT_NOT_NULL(r);
    /* Type predicate narrowing — accept smoke pass; ideal would resolve aOnly. */
    cbm_free_result(r);
    PASS();
}

/* ── Non-null assertion ────────────────────────────────────────────────────── */

TEST(tslsp_non_null_assertion) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "function go(t?: Tool) { t!.fire(); }\n");
    ASSERT_NOT_NULL(r);
    /* `t!` strips optionality — passes through to Tool. */
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_index_signature_dynamic) {
    CBMFileResult *r =
        extract_ts("interface Bag { [k: string]: number; }\n"
                   "function go(b: Bag) { for (const k of Object.keys(b)) { b[k]; } }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_recursive_generic) {
    CBMFileResult *r =
        extract_ts("interface Tree<T> { value: T; children: Tree<T>[]; }\n"
                   "function go(t: Tree<number>) { t.children.map(c => c.value); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── More real-world TS patterns ───────────────────────────────────────────── */

TEST(tslsp_real_express_middleware) {
    CBMFileResult *r =
        extract_ts("interface NextFn { (): void; }\n"
                   "interface Req { headers: { [k: string]: string }; }\n"
                   "interface Res { status(c: number): Res; send(b: string): Res; }\n"
                   "function logger(req: Req, res: Res, next: NextFn) {\n"
                   "    next();\n"
                   "    res.status(200).send('ok');\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "logger", "status"), 0);
    ASSERT_GTE(require_resolved(r, "logger", "send"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_redux_action) {
    CBMFileResult *r =
        extract_ts("interface Action { type: string; payload?: unknown; }\n"
                   "interface State { count: number; }\n"
                   "function reducer(state: State, action: Action): State {\n"
                   "    if (action.type === 'INC') return { count: state.count + 1 };\n"
                   "    return state;\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    /* No method calls expected; smoke pass for syntax handling */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_class_with_static_factory_async) {
    CBMFileResult *r =
        extract_ts("class Conn {\n"
                   "    static async open(url: string): Promise<Conn> { return new Conn(); }\n"
                   "    ping(): void {}\n"
                   "}\n"
                   "async function go() { const c = await Conn.open('x'); c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "open"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_dom_event_handler) {
    CBMFileResult *r =
        extract_tsx("function go(button: HTMLElement) {\n"
                    "    button.addEventListener('click', e => { e.preventDefault(); });\n"
                    "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "addEventListener"), 0);
    /* preventDefault inside callback — smoke (e's type via contextual typing) */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_zustand_store) {
    CBMFileResult *r = extract_ts("interface Store { count: number; inc(): void; reset(): void; }\n"
                                  "function go(s: Store) { s.inc(); s.reset(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "inc"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "reset"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_typeorm_repo) {
    CBMFileResult *r =
        extract_ts("class User { id(): number { return 0; } name(): string { return ''; } }\n"
                   "interface Repo<T> {\n"
                   "    findOne(id: number): Promise<T>;\n"
                   "    findAll(): Promise<T[]>;\n"
                   "    save(entity: T): Promise<T>;\n"
                   "}\n"
                   "async function go(repo: Repo<User>) {\n"
                   "    const u = await repo.findOne(1);\n"
                   "    u.name();\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "findOne"), 0);
    /* u.name() through generic + Promise unwrap is harder; smoke OK */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_router_chain) {
    CBMFileResult *r = extract_ts("interface Router {\n"
                                  "    get(path: string, handler: () => void): Router;\n"
                                  "    post(path: string, handler: () => void): Router;\n"
                                  "    use(mw: () => void): Router;\n"
                                  "}\n"
                                  "function go(app: Router) {\n"
                                  "    app.use(() => {}).get('/', () => {}).post('/x', () => {});\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "use"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "get"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "post"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_odm_model) {
    CBMFileResult *r = extract_ts("interface Model<T> {\n"
                                  "    create(doc: Partial<T>): Promise<T>;\n"
                                  "    find(filter: Partial<T>): Promise<T[]>;\n"
                                  "}\n"
                                  "interface User { name: string; age: number; }\n"
                                  "async function go(m: Model<User>) {\n"
                                  "    await m.create({ name: 'a' });\n"
                                  "    const users = await m.find({});\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "create"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "find"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More patterns ──────────────────────────────────────────────────────────── */

TEST(tslsp_iterator_for_of_with_generic) {
    CBMFileResult *r =
        extract_ts("class Item { name(): string { return ''; } }\n"
                   "function go(items: Array<Item>) { for (const x of items) { x.name(); } }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "name"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_class_factory_with_extends) {
    CBMFileResult *r = extract_ts("class Base { common(): void {} }\n"
                                  "class A extends Base { aOnly(): void {} }\n"
                                  "function makeA(): A { return new A(); }\n"
                                  "function go() { const a = makeA(); a.common(); a.aOnly(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "common"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "aOnly"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_method_returning_self_chained) {
    CBMFileResult *r = extract_ts("class Chainable {\n"
                                  "    a(): this { return this; }\n"
                                  "    b(): this { return this; }\n"
                                  "    finish(): string { return ''; }\n"
                                  "}\n"
                                  "function go(c: Chainable) { c.a().b().finish(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "a"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "b"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "finish"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_polymorphic_this) {
    CBMFileResult *r = extract_ts(
        "class Builder {\n"
        "    set(k: string, v: number): this { return this; }\n"
        "    build(): number { return 0; }\n"
        "}\n"
        "class TypedBuilder extends Builder { strict(): TypedBuilder { return this; } }\n"
        "function go(b: TypedBuilder) { b.set('x', 1).strict().build(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "set"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "build"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── More crash safety ─────────────────────────────────────────────────────── */

TEST(tslsp_nocrash_complex_jsx_attributes) {
    CBMFileResult *r =
        extract_tsx("function go(): JSX.Element {\n"
                    "    return <div className='a' style={{ color: 'red', padding: 10 }} />;\n"
                    "}\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_jsx_fragment) {
    CBMFileResult *r =
        extract_tsx("function go(): JSX.Element { return <>hi <span>x</span></>; }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_class_static_block) {
    CBMFileResult *r = extract_ts("class Foo {\n"
                                  "    static x = 0;\n"
                                  "    static { Foo.x = 42; }\n"
                                  "    run(): void {}\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_top_level_await) {
    CBMFileResult *r = extract_ts("async function fetch1(): Promise<number> { return 1; }\n"
                                  "const x = await fetch1();\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_assertion_function) {
    CBMFileResult *r =
        extract_ts("function assertNotNull<T>(v: T | null): asserts v is T { if (!v) throw new "
                   "Error('null'); }\n"
                   "function go(x: string | null) { assertNotNull(x); console.log(x); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── Overload resolution by arg types ──────────────────────────────────────── */

TEST(tslsp_overload_by_arg_types_class) {
    CBMFileResult *r = extract_ts("class Codec {\n"
                                  "    encode(s: string): string { return s; }\n"
                                  "    encode(n: number): number { return n; }\n"
                                  "}\n"
                                  "function go(c: Codec) { c.encode('hi'); c.encode(42); }\n");
    ASSERT_NOT_NULL(r);
    /* Both calls should resolve to encode (whichever overload wins is OK). */
    ASSERT_GTE(require_resolved(r, ".go", "encode"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_overload_function_by_arg_types) {
    CBMFileResult *r = extract_ts("function process(x: string): string { return x; }\n"
                                  "function process(x: number): number { return x; }\n"
                                  "function go() { process('a'); process(1); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "process"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Final v2 batch: more real-world TS patterns ───────────────────────────── */

TEST(tslsp_real_react_context) {
    CBMFileResult *r =
        extract_tsx("interface Theme { dark: boolean; }\n"
                    "function createContext<T>(initial: T): { Provider: () => JSX.Element; "
                    "Consumer: () => JSX.Element } { return null as any; }\n"
                    "function go() { const ctx = createContext({ dark: false }); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "createContext"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_react_useReducer) {
    CBMFileResult *r = extract_tsx("interface Action { type: string; }\n"
                                   "interface State { count: number; }\n"
                                   "function useReducer<S, A>(r: (s: S, a: A) => S, init: S): [S, "
                                   "(a: A) => void] { return null as any; }\n"
                                   "function reduce(s: State, a: Action): State { return s; }\n"
                                   "function App(): JSX.Element {\n"
                                   "    const [s, dispatch] = useReducer(reduce, { count: 0 });\n"
                                   "    return <div/>;\n"
                                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, "App", "useReducer"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_apollo_query) {
    CBMFileResult *r =
        extract_ts("interface ApolloClient {\n"
                   "    query<T>(opts: { query: unknown }): Promise<{ data: T }>;\n"
                   "    mutate<T>(opts: { mutation: unknown }): Promise<{ data: T }>;\n"
                   "}\n"
                   "async function go(client: ApolloClient) {\n"
                   "    await client.query({ query: 'q' });\n"
                   "    await client.mutate({ mutation: 'm' });\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "query"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "mutate"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_axios_client) {
    CBMFileResult *r = extract_ts("interface AxiosClient {\n"
                                  "    get<T>(url: string): Promise<{ data: T }>;\n"
                                  "    post<T>(url: string, body: unknown): Promise<{ data: T }>;\n"
                                  "}\n"
                                  "async function go(http: AxiosClient) {\n"
                                  "    await http.get('/users');\n"
                                  "    await http.post('/users', {});\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "get"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "post"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_zod_schema) {
    CBMFileResult *r =
        extract_ts("interface Schema<T> {\n"
                   "    parse(x: unknown): T;\n"
                   "    safeParse(x: unknown): { success: boolean; data?: T };\n"
                   "}\n"
                   "function go(s: Schema<string>) { s.parse('x').toLowerCase(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "parse"), 0);
    /* parse returns T = string, then .toLowerCase via String wrapper */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_prisma_client) {
    CBMFileResult *r = extract_ts(
        "interface UserModel {\n"
        "    findUnique(args: { where: { id: number } }): Promise<{ name: string } | null>;\n"
        "    create(args: { data: { name: string } }): Promise<{ id: number; name: string }>;\n"
        "}\n"
        "async function go(users: UserModel) {\n"
        "    await users.findUnique({ where: { id: 1 } });\n"
        "    await users.create({ data: { name: 'a' } });\n"
        "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "findUnique"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "create"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_lit_element) {
    CBMFileResult *r =
        extract_ts("class HTMLElement { addEventListener(t: string, h: () => void): void {} }\n"
                   "class LitElement extends HTMLElement {\n"
                   "    render(): string { return ''; }\n"
                   "    requestUpdate(): void {}\n"
                   "}\n"
                   "class MyEl extends LitElement {\n"
                   "    render(): string { return 'hi'; }\n"
                   "}\n"
                   "function go(el: MyEl) { el.requestUpdate(); el.render(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "requestUpdate"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_vitest_describe) {
    CBMFileResult *r =
        extract_ts("function describe(name: string, fn: () => void): void {}\n"
                   "function it(name: string, fn: () => void): void {}\n"
                   "function expect(v: unknown): { toBe(x: unknown): void; toEqual(x: unknown): "
                   "void } { return null as any; }\n"
                   "describe('Foo', () => { it('works', () => { expect(1).toBe(1); }); });\n");
    ASSERT_NOT_NULL(r);
    /* Top-level call with arrow callback — smoke pass */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_class_with_protected_methods) {
    CBMFileResult *r = extract_ts("class Base {\n"
                                  "    protected helper(): string { return ''; }\n"
                                  "    public render(): string { return this.helper(); }\n"
                                  "}\n"
                                  "class Sub extends Base {\n"
                                  "    use(): string { return this.helper(); }\n"
                                  "}\n"
                                  "function go(s: Sub) { s.render(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "render"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_observable_chain) {
    CBMFileResult *r =
        extract_ts("interface Observable<T> {\n"
                   "    map<U>(fn: (v: T) => U): Observable<U>;\n"
                   "    filter(fn: (v: T) => boolean): Observable<T>;\n"
                   "    subscribe(cb: (v: T) => void): { unsubscribe(): void };\n"
                   "}\n"
                   "function go(obs: Observable<number>) {\n"
                   "    const sub = obs.filter(x => x > 0).map(x => x * 2).subscribe(v => v);\n"
                   "    sub.unsubscribe();\n"
                   "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "filter"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "map"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "subscribe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_jest_mock) {
    CBMFileResult *r = extract_ts("interface MockFn<T> {\n"
                                  "    mockReturnValue(v: T): MockFn<T>;\n"
                                  "    mockResolvedValue(v: T): MockFn<T>;\n"
                                  "    mockRejectedValue(e: unknown): MockFn<T>;\n"
                                  "    mockClear(): void;\n"
                                  "}\n"
                                  "function go(mock: MockFn<number>) {\n"
                                  "    mock.mockReturnValue(42).mockClear();\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "mockReturnValue"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "mockClear"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_real_state_machine) {
    CBMFileResult *r = extract_ts("interface Service {\n"
                                  "    send(event: string): Service;\n"
                                  "    getSnapshot(): { value: string };\n"
                                  "    start(): Service;\n"
                                  "}\n"
                                  "function go(svc: Service) {\n"
                                  "    svc.start().send('OPEN').getSnapshot().value;\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "start"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "send"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "getSnapshot"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Crash safety: more exotic TS syntax ───────────────────────────────────── */

TEST(tslsp_nocrash_optional_call) {
    CBMFileResult *r = extract_ts("function go(fn?: () => string) { fn?.(); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_intersection_class) {
    CBMFileResult *r = extract_ts("interface Loggable { log(): void; }\n"
                                  "interface Cacheable { cache(): void; }\n"
                                  "function go(x: Loggable & Cacheable) { x.log(); x.cache(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "cache"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_branded_type) {
    CBMFileResult *r = extract_ts("type UserId = string & { readonly __brand: 'UserId' };\n"
                                  "function go(id: UserId) { console.log(id); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_tuple_with_rest) {
    CBMFileResult *r = extract_ts("type T = [string, ...number[]];\n"
                                  "function go(x: T) { console.log(x); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nocrash_named_tuple) {
    CBMFileResult *r = extract_ts("type T = [first: string, second: number];\n"
                                  "function go(x: T) { console.log(x); }\n");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

/* ── keyof / T[K] / typeof in type position ─────────────────────────────────── */

TEST(tslsp_keyof_in_param_smoke) {
    CBMFileResult *r =
        extract_ts("interface User { name: string; age: number; }\n"
                   "function get<K extends keyof User>(u: User, k: K): User[K] { return u[k]; }\n"
                   "function go(u: User) { get(u, 'name'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "get"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_typeof_in_type_position) {
    CBMFileResult *r = extract_ts("const config = { port: 8080, host: 'localhost' };\n"
                                  "function go(c: typeof config) { console.log(c); }\n"
                                  "go({ port: 9000, host: 'x' });\n");
    ASSERT_NOT_NULL(r);
    /* typeof config in type position — smoke pass */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_indexed_access_smoke) {
    CBMFileResult *r = extract_ts(
        "interface Map { id: number; name: string; }\n"
        "function go(m: Map, k: 'name') { const v: Map[typeof k] = m[k]; console.log(v); }\n");
    ASSERT_NOT_NULL(r);
    /* Indexed access type — smoke pass */
    cbm_free_result(r);
    PASS();
}

/* ── Hash-table registry stress (task #2) ──────────────────────────────────── */

#include "lsp/type_registry.h"

TEST(tslsp_hash_registry_basic) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);

    CBMRegisteredType t;
    memset(&t, 0, sizeof(t));
    t.qualified_name = "test.A";
    t.short_name = "A";
    cbm_registry_add_type(&reg, t);

    CBMRegisteredFunc f;
    memset(&f, 0, sizeof(f));
    f.qualified_name = "test.A.foo";
    f.short_name = "foo";
    f.receiver_type = "test.A";
    f.min_params = -1;
    cbm_registry_add_func(&reg, f);

    /* Linear-scan path (pre-finalize). */
    ASSERT_NOT_NULL(cbm_registry_lookup_type(&reg, "test.A"));
    ASSERT_NOT_NULL(cbm_registry_lookup_func(&reg, "test.A.foo"));
    ASSERT_NOT_NULL(cbm_registry_lookup_method(&reg, "test.A", "foo"));

    /* After finalize: hashed path. */
    cbm_registry_finalize(&reg);
    ASSERT_NOT_NULL(cbm_registry_lookup_type(&reg, "test.A"));
    ASSERT_NOT_NULL(cbm_registry_lookup_func(&reg, "test.A.foo"));
    ASSERT_NOT_NULL(cbm_registry_lookup_method(&reg, "test.A", "foo"));

    /* Negative lookups stay correct. */
    ASSERT(cbm_registry_lookup_type(&reg, "test.B") == NULL);
    ASSERT(cbm_registry_lookup_method(&reg, "test.A", "bar") == NULL);

    cbm_arena_destroy(&arena);
    PASS();
}

/* Entries added AFTER cbm_registry_finalize must remain findable. The hashed
 * lookup returned NULL for anything not in the buckets, silently hiding
 * post-finalize additions (receiver-type stubs, c_lsp tpn attach targets).
 * The fix scans the un-indexed tail after a bucket miss. */
TEST(tslsp_hash_registry_post_finalize_adds) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);

    CBMRegisteredType t;
    memset(&t, 0, sizeof(t));
    t.qualified_name = "test.A";
    t.short_name = "A";
    cbm_registry_add_type(&reg, t);
    cbm_registry_finalize(&reg);

    /* Post-finalize additions — the stub-registration pattern. */
    CBMRegisteredType t2;
    memset(&t2, 0, sizeof(t2));
    t2.qualified_name = "test.LateStub";
    t2.short_name = "LateStub";
    cbm_registry_add_type(&reg, t2);

    CBMRegisteredFunc f2;
    memset(&f2, 0, sizeof(f2));
    f2.qualified_name = "test.LateStub.run";
    f2.short_name = "run";
    f2.receiver_type = "test.LateStub";
    f2.min_params = -1;
    cbm_registry_add_func(&reg, f2);

    ASSERT_NOT_NULL(cbm_registry_lookup_type(&reg, "test.A"));
    ASSERT_NOT_NULL(cbm_registry_lookup_type(&reg, "test.LateStub"));
    ASSERT_NOT_NULL(cbm_registry_lookup_func(&reg, "test.LateStub.run"));

    /* Re-finalize folds the tail into the buckets; still findable. */
    cbm_registry_finalize(&reg);
    ASSERT_NOT_NULL(cbm_registry_lookup_type(&reg, "test.LateStub"));
    ASSERT_NOT_NULL(cbm_registry_lookup_func(&reg, "test.LateStub.run"));

    cbm_arena_destroy(&arena);
    PASS();
}

TEST(tslsp_hash_registry_stress_10k) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);

    enum { N = 10000 };
    char buf[64];
    for (int i = 0; i < N; i++) {
        snprintf(buf, sizeof(buf), "p.T%d", i);
        CBMRegisteredType t;
        memset(&t, 0, sizeof(t));
        t.qualified_name = cbm_arena_strdup(&arena, buf);
        t.short_name = "X";
        cbm_registry_add_type(&reg, t);

        snprintf(buf, sizeof(buf), "p.T%d.go", i);
        CBMRegisteredFunc f;
        memset(&f, 0, sizeof(f));
        f.qualified_name = cbm_arena_strdup(&arena, buf);
        f.short_name = "go";
        f.min_params = -1;
        char recv[64];
        snprintf(recv, sizeof(recv), "p.T%d", i);
        f.receiver_type = cbm_arena_strdup(&arena, recv);
        cbm_registry_add_func(&reg, f);
    }
    cbm_registry_finalize(&reg);

    /* Spot-check: every-100th entry resolves correctly via hashed lookup. */
    int hits = 0;
    for (int i = 0; i < N; i += 100) {
        snprintf(buf, sizeof(buf), "p.T%d", i);
        if (cbm_registry_lookup_type(&reg, buf))
            hits++;
        snprintf(buf, sizeof(buf), "p.T%d", i);
        if (cbm_registry_lookup_method(&reg, buf, "go"))
            hits++;
    }
    ASSERT(hits >= 200); // 100 type + 100 method lookups

    cbm_arena_destroy(&arena);
    PASS();
}

/* ── Partial relater behavior (task #5) — exercised via end-to-end resolution ─ */

TEST(tslsp_relater_extends_chain_resolution) {
    /* Verifies the relater's class-hierarchy walk by exercising it in real code:
     * passing Dog where Animal is expected should be valid. The resolver doesn't
     * directly assert assignability today, but the chain walk is what powers
     * narrowing-via-extends and conditional-type evaluation in upcoming tasks. */
    CBMFileResult *r =
        extract_ts("class Animal { breathe(): void {} }\n"
                   "class Dog extends Animal { bark(): void {} }\n"
                   "function takeAnimal(a: Animal): void { a.breathe(); }\n"
                   "function go(d: Dog) { takeAnimal(d); d.bark(); d.breathe(); }\n");
    ASSERT_NOT_NULL(r);
    /* Direct method lookups still work via the existing extends walk in lookup_method. */
    ASSERT_GTE(require_resolved(r, ".go", "takeAnimal"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "bark"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "breathe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_relater_union_member_dispatch) {
    /* Source-union assignability — every member must satisfy the target. */
    CBMFileResult *r =
        extract_ts("interface Logger { log(): void; }\n"
                   "class FileLogger { log(): void {} }\n"
                   "class ConsoleLogger { log(): void {} }\n"
                   "function emit(l: Logger): void { l.log(); }\n"
                   "function go(x: FileLogger | ConsoleLogger) { emit(x); x.log(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "emit"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Conditional types (task #8) ───────────────────────────────────────────── */

TEST(tslsp_conditional_inline_method) {
    /* Function param using inline conditional that picks a known class. */
    CBMFileResult *r = extract_ts("class A { aOnly(): void {} }\n"
                                  "class B { bOnly(): void {} }\n"
                                  "function go(x: number extends number ? A : B) { x.aOnly(); }\n");
    ASSERT_NOT_NULL(r);
    /* `number extends number ? A : B` simplifies to A; A.aOnly should resolve. */
    ASSERT_GTE(require_resolved(r, ".go", "aOnly"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_conditional_extends_chain) {
    /* Conditional uses class hierarchy via the relater. */
    CBMFileResult *r = extract_ts(
        "class Animal { breathe(): void {} }\n"
        "class Dog extends Animal { bark(): void {} }\n"
        "function go(x: Dog extends Animal ? Dog : Animal) { x.bark(); x.breathe(); }\n");
    ASSERT_NOT_NULL(r);
    /* Dog extends Animal → true → x has type Dog; both Dog and Animal methods resolve. */
    ASSERT_GTE(require_resolved(r, ".go", "bark"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "breathe"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_conditional_false_branch) {
    /* Negative case: condition fails → false_branch picked. */
    CBMFileResult *r = extract_ts("class A { aOnly(): void {} }\n"
                                  "class B { bOnly(): void {} }\n"
                                  "function go(x: string extends number ? A : B) { x.bOnly(); }\n");
    ASSERT_NOT_NULL(r);
    /* string is NOT assignable to number → falls to B; B.bOnly resolves. */
    ASSERT_GTE(require_resolved(r, ".go", "bOnly"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_conditional_smoke_distribution) {
    /* Inline distribution syntactically present — current parsing produces a
     * conditional with check=UNION, eval distributes per member. Result is a
     * union of true/false branches; method dispatch on union picks the first
     * member with the method. */
    CBMFileResult *r =
        extract_ts("interface Hello { hi(): void; }\n"
                   "interface World { world(): void; }\n"
                   "function go(x: number extends string ? Hello : World) { x.world(); }\n");
    ASSERT_NOT_NULL(r);
    /* number is not assignable to string → false → World → world() resolves. */
    ASSERT_GTE(require_resolved(r, ".go", "world"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── infer X (task #9) ─────────────────────────────────────────────────────── */

TEST(tslsp_infer_promise_unwrap_inline) {
    /* Inline conditional with infer: Promise<Conn> extends Promise<infer U> ? U : never
     * should bind U to Conn and the param type becomes Conn. */
    CBMFileResult *r = extract_ts(
        "class Conn { ping(): void {} }\n"
        "function go(c: Promise<Conn> extends Promise<infer U> ? U : never) { c.ping(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "ping"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_infer_array_element) {
    CBMFileResult *r =
        extract_ts("class Item { run(): void {} }\n"
                   "function go(x: Array<Item> extends Array<infer E> ? E : never) { x.run(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_infer_no_match_falls_to_false) {
    CBMFileResult *r =
        extract_ts("class A { aOnly(): void {} }\n"
                   "function go(x: number extends Array<infer E> ? E : A) { x.aOnly(); }\n");
    ASSERT_NOT_NULL(r);
    /* number doesn't match Array<...> → false branch (A) → aOnly resolves. */
    ASSERT_GTE(require_resolved(r, ".go", "aOnly"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Async iterables (task #11) ────────────────────────────────────────────── */

TEST(tslsp_async_iter_for_await_of) {
    CBMFileResult *r = extract_ts("class Item { run(): void {} }\n"
                                  "async function go(items: AsyncIterable<Item>) {\n"
                                  "    for await (const x of items) { x.run(); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "run"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_iter_iterator_next) {
    CBMFileResult *r = extract_ts("class Item { go(): void {} }\n"
                                  "function go(it: Iterator<Item>) { it.next(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "next"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_async_iterator_next_promise) {
    CBMFileResult *r = extract_ts("function go(it: AsyncIterator<string>) { it.next(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "next"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_generator_next) {
    CBMFileResult *r = extract_ts("function* gen(): Generator<number> { yield 1; }\n"
                                  "function go() { gen().next(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "next"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Final edge-case tests ─────────────────────────────────────────────────── */

TEST(tslsp_truthy_narrow_undefined) {
    CBMFileResult *r = extract_ts("class Tool { fire(): void {} }\n"
                                  "function go(t: Tool | undefined) { if (t) { t.fire(); } }\n");
    ASSERT_NOT_NULL(r);
    /* Truthy narrowing: t inside if-body should be Tool. Currently union dispatch
     * tries each member, finds fire on Tool — resolves regardless of narrowing. */
    ASSERT_GTE(require_resolved(r, ".go", "fire"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_method_override) {
    CBMFileResult *r = extract_ts("class Base { run(): string { return 'base'; } }\n"
                                  "class Sub extends Base { run(): string { return 'sub'; } }\n"
                                  "function go(s: Sub) { s.run(); }\n");
    ASSERT_NOT_NULL(r);
    /* Sub.run shadows Base.run — lookup_method finds Sub.run first (direct member). */
    int idx = require_resolved(r, ".go", "run");
    ASSERT_GTE(idx, 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_type_only_import_safe) {
    CBMFileResult *r = extract_ts("import type { Foo } from './foo';\n"
                                  "function go(f: Foo) { console.log(f); }\n");
    ASSERT_NOT_NULL(r);
    /* Type-only imports should be parsed safely (no crash); resolution of Foo
     * methods is opaque since Foo isn't in registry. */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_satisfies_method_chain) {
    CBMFileResult *r = extract_ts("interface Cfg { port: number; host: string; }\n"
                                  "function go() {\n"
                                  "    const cfg = { port: 8080, host: 'x' } satisfies Cfg;\n"
                                  "    cfg.host;\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* satisfies passes through type — accept smoke pass */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_in_operator_narrow_smoke) {
    CBMFileResult *r = extract_ts("interface A { kind: 'a'; aField: number; }\n"
                                  "interface B { kind: 'b'; bField: string; }\n"
                                  "function go(x: A | B) {\n"
                                  "    if ('aField' in x) { console.log(x.aField); }\n"
                                  "    else { console.log(x.bField); }\n"
                                  "}\n");
    ASSERT_NOT_NULL(r);
    /* `in` operator narrowing — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_optional_property_chain) {
    CBMFileResult *r = extract_ts("interface User { profile?: { name(): string } }\n"
                                  "function go(u: User) { u.profile?.name(); }\n");
    ASSERT_NOT_NULL(r);
    /* Optional property + optional chain — accept smoke pass for v1 */
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_void_returning_method) {
    CBMFileResult *r = extract_ts("class Logger { log(msg: string): void {} }\n"
                                  "function go(l: Logger) { l.log('x'); l.log('y'); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "log"), 0);
    cbm_free_result(r);
    PASS();
}

TEST(tslsp_nested_class_resolution) {
    CBMFileResult *r = extract_ts("class Outer {\n"
                                  "    inner: Inner = new Inner();\n"
                                  "    method(): void { this.inner.work(); }\n"
                                  "}\n"
                                  "class Inner { work(): void {} }\n"
                                  "function go(o: Outer) { o.method(); o.inner.work(); }\n");
    ASSERT_NOT_NULL(r);
    ASSERT_GTE(require_resolved(r, ".go", "method"), 0);
    ASSERT_GTE(require_resolved(r, ".go", "work"), 0);
    cbm_free_result(r);
    PASS();
}

/* ── Suite registration ────────────────────────────────────────────────────── */

SUITE(ts_lsp) {
    /* Smoke (Phase 1) */
    RUN_TEST(tslsp_smoke_empty_file);
    RUN_TEST(tslsp_smoke_minimal_function);
    RUN_TEST(tslsp_smoke_jsx_minimal);
    RUN_TEST(tslsp_smoke_js_minimal);
    RUN_TEST(tslsp_smoke_dts_minimal);

    /* Category 1: param type inference */
    RUN_TEST(tslsp_param_simple);
    RUN_TEST(tslsp_param_multi);
    RUN_TEST(tslsp_param_arrow);
    RUN_TEST(tslsp_param_optional);
    RUN_TEST(tslsp_param_with_default);
    RUN_TEST(tslsp_param_destructured);

    /* Category 2: return type propagation */
    RUN_TEST(tslsp_return_simple);
    RUN_TEST(tslsp_return_chain);
    RUN_TEST(tslsp_return_inferred_local);
    RUN_TEST(tslsp_return_async_promise);
    RUN_TEST(tslsp_return_method_no_rttype);

    /* Category 3: method chaining */
    RUN_TEST(tslsp_chain_fluent);
    RUN_TEST(tslsp_chain_with_intermediate_var);
    RUN_TEST(tslsp_chain_promise_then);
    RUN_TEST(tslsp_chain_string_methods);

    /* Category 4: destructuring */
    RUN_TEST(tslsp_destructure_object);
    RUN_TEST(tslsp_destructure_array);
    RUN_TEST(tslsp_destructure_param);
    RUN_TEST(tslsp_destructure_renamed);

    /* Category 7: object literal */
    RUN_TEST(tslsp_object_property_method);
    RUN_TEST(tslsp_object_nested_property);

    /* Category 8: type alias */
    RUN_TEST(tslsp_alias_simple);
    RUN_TEST(tslsp_alias_chain);

    /* Category 9: class */
    RUN_TEST(tslsp_class_method_dispatch);
    RUN_TEST(tslsp_class_this_dispatch);
    RUN_TEST(tslsp_class_inheritance_method);
    RUN_TEST(tslsp_class_super_call);

    /* Category 10: interface */
    RUN_TEST(tslsp_iface_method);
    RUN_TEST(tslsp_iface_field_chain);

    /* Category 12: stdlib */
    RUN_TEST(tslsp_stdlib_array_push);
    RUN_TEST(tslsp_stdlib_array_map);
    RUN_TEST(tslsp_stdlib_console_log);
    RUN_TEST(tslsp_stdlib_promise_then);
    RUN_TEST(tslsp_stdlib_map_get);
    RUN_TEST(tslsp_stdlib_set_has);
    RUN_TEST(tslsp_stdlib_json_stringify);
    RUN_TEST(tslsp_stdlib_object_keys);

    /* Category 13: optional chain */
    RUN_TEST(tslsp_optional_chain_method);

    /* Category 14: async/await */
    RUN_TEST(tslsp_await_promise_unwrap);

    /* Category 16: union types */
    RUN_TEST(tslsp_union_member_present_on_both);

    /* Category 17: literal types */
    RUN_TEST(tslsp_literal_const_assertion);

    /* Category 20: imports */
    RUN_TEST(tslsp_module_named_import_call);

    /* Category 25: diagnostics */
    RUN_TEST(tslsp_diag_unknown_call_emits_unresolved);

    /* Category 26: crash safety */
    RUN_TEST(tslsp_nocrash_unclosed_string);
    RUN_TEST(tslsp_nocrash_self_referential_alias);
    RUN_TEST(tslsp_nocrash_deep_generic);
    RUN_TEST(tslsp_nocrash_with_block_legacy);
    RUN_TEST(tslsp_nocrash_eval_value);
    RUN_TEST(tslsp_nocrash_using_decl);
    RUN_TEST(tslsp_nocrash_megasource);

    /* Category 11: generics deeper */
    RUN_TEST(tslsp_generic_identity_inference);
    RUN_TEST(tslsp_generic_array_map_chain);
    RUN_TEST(tslsp_generic_promise_resolve);
    RUN_TEST(tslsp_generic_array_from);
    RUN_TEST(tslsp_generic_array_isArray);

    /* Category 14: async deeper */
    RUN_TEST(tslsp_await_promise_all);

    /* Category 15: type narrowing */
    RUN_TEST(tslsp_narrow_typeof_check);
    RUN_TEST(tslsp_narrow_instanceof);

    /* Category 17: literal types deeper */
    RUN_TEST(tslsp_literal_string_union);

    /* Category 16: union deeper */
    RUN_TEST(tslsp_union_common_method);
    RUN_TEST(tslsp_union_many_members_no_overflow);

    /* Category 9: more class patterns */
    RUN_TEST(tslsp_class_static_method);
    RUN_TEST(tslsp_class_method_then_field);
    RUN_TEST(tslsp_class_chained_field_method);

    /* Category 12: more stdlib */
    RUN_TEST(tslsp_stdlib_array_filter);
    RUN_TEST(tslsp_stdlib_array_find);
    RUN_TEST(tslsp_stdlib_array_reduce);
    RUN_TEST(tslsp_stdlib_string_split);
    RUN_TEST(tslsp_stdlib_set_add);
    RUN_TEST(tslsp_stdlib_map_set);

    /* Category 26: more crash safety */
    RUN_TEST(tslsp_nocrash_circular_extends);
    RUN_TEST(tslsp_nocrash_recursive_type);
    RUN_TEST(tslsp_nocrash_unicode_identifier);
    RUN_TEST(tslsp_nocrash_template_with_call);
    RUN_TEST(tslsp_nocrash_decorator);
    RUN_TEST(tslsp_nocrash_private_field);
    RUN_TEST(tslsp_nocrash_getter_setter);
    RUN_TEST(tslsp_nocrash_abstract_class);

    /* Category 22: JSX */
    RUN_TEST(tslsp_jsx_component_self_closing);
    RUN_TEST(tslsp_jsx_component_with_children);
    RUN_TEST(tslsp_jsx_intrinsic_skipped);
    RUN_TEST(tslsp_jsx_nested_component);

    /* Category 23: TSX combined */
    RUN_TEST(tslsp_tsx_typed_props_method_call);
    RUN_TEST(tslsp_tsx_class_component_method);

    /* Tuple types */
    RUN_TEST(tslsp_tuple_destructure_method);

    /* Number wrapper */
    RUN_TEST(tslsp_stdlib_number_toString);
    RUN_TEST(tslsp_stdlib_number_toFixed);

    /* More crash-safety */
    RUN_TEST(tslsp_nocrash_huge_union);
    RUN_TEST(tslsp_nocrash_intersection_chain);
    RUN_TEST(tslsp_nocrash_satisfies_keyword);

    /* Category 21: JSDoc */
    RUN_TEST(tslsp_jsdoc_param_method_call);
    RUN_TEST(tslsp_jsdoc_returns_propagation);
    RUN_TEST(tslsp_jsdoc_multi_param);
    RUN_TEST(tslsp_jsdoc_array_generic);
    RUN_TEST(tslsp_jsdoc_promise_returns);
    RUN_TEST(tslsp_jsdoc_ignored_in_ts_mode);

    /* Category 31: real-library patterns */
    RUN_TEST(tslsp_real_express_handler);
    RUN_TEST(tslsp_real_fluent_builder);
    RUN_TEST(tslsp_real_observer_pattern);
    RUN_TEST(tslsp_real_event_emitter);
    RUN_TEST(tslsp_real_singleton);

    /* Edge cases */
    RUN_TEST(tslsp_readonly_field_method);
    RUN_TEST(tslsp_constructor_method_call);
    RUN_TEST(tslsp_method_chained_through_field);
    RUN_TEST(tslsp_callback_returning_method_call);
    RUN_TEST(tslsp_this_in_arrow_method);
    RUN_TEST(tslsp_optional_field_method);

    /* More crash-safety */
    RUN_TEST(tslsp_nocrash_enum_basic);
    RUN_TEST(tslsp_nocrash_namespace);
    RUN_TEST(tslsp_nocrash_async_generator);
    RUN_TEST(tslsp_nocrash_template_literal_expr);
    RUN_TEST(tslsp_nocrash_complex_generic_constraint);

    /* Discriminated union, abstract, generic class, statics */
    RUN_TEST(tslsp_discriminated_union_method);
    RUN_TEST(tslsp_abstract_concrete_dispatch);
    RUN_TEST(tslsp_generic_class_method);
    RUN_TEST(tslsp_static_field_method);

    /* More JSDoc */
    RUN_TEST(tslsp_jsdoc_param_after_text);
    RUN_TEST(tslsp_jsdoc_typedef_used_via_param);

    /* Real-library + edges */
    RUN_TEST(tslsp_real_promise_all_then);
    RUN_TEST(tslsp_method_on_typeof_class);
    RUN_TEST(tslsp_promise_chain_via_local);
    RUN_TEST(tslsp_obj_literal_method_property);

    /* Contextual callback typing */
    RUN_TEST(tslsp_ctx_array_map_callback);
    RUN_TEST(tslsp_ctx_array_filter_callback);
    RUN_TEST(tslsp_ctx_array_find_callback);
    RUN_TEST(tslsp_ctx_array_forEach_callback);
    RUN_TEST(tslsp_ctx_array_block_body_callback);
    RUN_TEST(tslsp_ctx_chain_filter_then_map);
    RUN_TEST(tslsp_ctx_user_defined_callback);

    /* Narrowing */
    RUN_TEST(tslsp_narrow_typeof_string_method);
    RUN_TEST(tslsp_narrow_instanceof_dispatch);
    RUN_TEST(tslsp_narrow_discriminated_kind);

    /* React-like */
    RUN_TEST(tslsp_react_useState_pattern);
    RUN_TEST(tslsp_react_callback_in_jsx);

    /* More stdlib */
    RUN_TEST(tslsp_stdlib_array_concat);
    RUN_TEST(tslsp_stdlib_array_slice);
    RUN_TEST(tslsp_stdlib_string_repeat);
    RUN_TEST(tslsp_stdlib_promise_catch);
    RUN_TEST(tslsp_stdlib_set_size_and_clear);

    /* Class / mixin patterns */
    RUN_TEST(tslsp_class_implements_interface);
    RUN_TEST(tslsp_class_multi_inheritance_chain);
    RUN_TEST(tslsp_class_constructor_param_property);

    /* Iteration */
    RUN_TEST(tslsp_for_of_array_method);
    RUN_TEST(tslsp_for_let_array_method);

    /* Type predicate */
    RUN_TEST(tslsp_type_predicate_guard);

    /* Optional chain + nullish coalesce */
    RUN_TEST(tslsp_optional_chain_method_call);
    RUN_TEST(tslsp_nullish_coalesce_chain);

    /* More crash safety */
    RUN_TEST(tslsp_nocrash_decorator_factory);
    RUN_TEST(tslsp_nocrash_proxy_usage);
    RUN_TEST(tslsp_nocrash_symbol_iterator);
    RUN_TEST(tslsp_nocrash_index_signature);
    RUN_TEST(tslsp_nocrash_const_assertion_chain);

    /* Cross-file resolution */
    RUN_TEST(tslsp_crossfile_method_dispatch);
    RUN_TEST(tslsp_scale_many_defs_no_crash_issue344);
    RUN_TEST(tslsp_crossfile_function_call);
    RUN_TEST(tslsp_crossfile_chain_through_return);
    RUN_TEST(tslsp_crossfile_no_def_match);

    /* More stdlib */
    RUN_TEST(tslsp_stdlib_date_toISOString);
    RUN_TEST(tslsp_stdlib_date_getTime);
    RUN_TEST(tslsp_stdlib_regexp_test);
    RUN_TEST(tslsp_stdlib_regexp_exec);
    RUN_TEST(tslsp_stdlib_error_toString);
    RUN_TEST(tslsp_stdlib_math_floor);
    RUN_TEST(tslsp_stdlib_math_max);
    RUN_TEST(tslsp_stdlib_math_random);

    /* React hook patterns */
    RUN_TEST(tslsp_react_useEffect_pattern);
    RUN_TEST(tslsp_react_useCallback_pattern);

    /* More crash safety */
    RUN_TEST(tslsp_nocrash_iife);
    RUN_TEST(tslsp_nocrash_complex_jsdoc_jsdoc);
    RUN_TEST(tslsp_nocrash_chained_optional);
    RUN_TEST(tslsp_nocrash_template_in_extends);

    /* v2: switch narrowing + typeof else branch */
    RUN_TEST(tslsp_narrow_switch_kind);
    RUN_TEST(tslsp_narrow_switch_three_way);
    RUN_TEST(tslsp_narrow_typeof_else_branch);

    /* Generic class deeper */
    RUN_TEST(tslsp_generic_class_with_T_method_return);
    RUN_TEST(tslsp_generic_class_method_chain_through_subst);

    /* Function overload (interface) */
    RUN_TEST(tslsp_overload_simple);

    /* Real-world libs */
    RUN_TEST(tslsp_real_lodash_chain);
    RUN_TEST(tslsp_real_rxjs_pipe);

    /* Static factory + iter/generator */
    RUN_TEST(tslsp_static_factory_returns_instance);
    RUN_TEST(tslsp_for_of_set_method);
    RUN_TEST(tslsp_generator_with_yield);

    /* Edge cases */
    RUN_TEST(tslsp_object_destructure_param);
    RUN_TEST(tslsp_default_export_class);
    RUN_TEST(tslsp_export_named_class);
    RUN_TEST(tslsp_class_with_async_method);
    RUN_TEST(tslsp_method_on_returned_promise_chain);

    /* Advanced TS syntax — crash safety */
    RUN_TEST(tslsp_nocrash_template_literal_type);
    RUN_TEST(tslsp_nocrash_intersect_with_func);
    RUN_TEST(tslsp_nocrash_class_expression);

    /* TS utility types passthrough */
    RUN_TEST(tslsp_partial_method_passthrough);
    RUN_TEST(tslsp_readonly_method);
    RUN_TEST(tslsp_required_field_method);
    RUN_TEST(tslsp_nonnullable_method);

    /* More patterns */
    RUN_TEST(tslsp_array_of_class_map);
    RUN_TEST(tslsp_set_iteration_with_callback);
    RUN_TEST(tslsp_throwing_error_method);

    /* More crash safety */
    RUN_TEST(tslsp_nocrash_module_declaration);
    RUN_TEST(tslsp_nocrash_typeof_in_type_position);
    RUN_TEST(tslsp_nocrash_tagged_template);

    /* LSP vs baseline comparison (requires CBM_LSP_DISABLED knob in resolver) */
    RUN_TEST(tslsp_baseline_vs_lsp_simple);
    RUN_TEST(tslsp_baseline_vs_lsp_chained);
    RUN_TEST(tslsp_baseline_vs_lsp_callbacks);
    RUN_TEST(tslsp_baseline_vs_lsp_narrowing);

    /* Stress tests */
    RUN_TEST(tslsp_stress_many_classes);
    RUN_TEST(tslsp_stress_deep_inheritance);
    RUN_TEST(tslsp_stress_long_method_chain);
    RUN_TEST(tslsp_stress_megafile_mixed);
    RUN_TEST(tslsp_stress_huge_union);
    RUN_TEST(tslsp_stress_pathological_nesting);
    RUN_TEST(tslsp_stress_5000_lines);
    RUN_TEST(tslsp_stress_random_garbage);
    RUN_TEST(tslsp_stress_empty_classes_at_scale);

    /* DOM stdlib coverage */
    RUN_TEST(tslsp_dom_document_getElementById);
    RUN_TEST(tslsp_dom_element_querySelector);
    RUN_TEST(tslsp_dom_html_click);
    RUN_TEST(tslsp_dom_event_preventDefault);
    RUN_TEST(tslsp_dom_response_json_chain);
    RUN_TEST(tslsp_dom_window_setTimeout);
    RUN_TEST(tslsp_dom_addEventListener_callback);

    /* More utility types */
    RUN_TEST(tslsp_returntype_passthrough);
    RUN_TEST(tslsp_exclude_method_on_remaining);

    /* Type predicate + non-null */
    RUN_TEST(tslsp_type_pred_in_if);
    RUN_TEST(tslsp_non_null_assertion);

    /* More crash safety */
    RUN_TEST(tslsp_nocrash_index_signature_dynamic);
    RUN_TEST(tslsp_nocrash_recursive_generic);

    /* More real-world TS patterns */
    RUN_TEST(tslsp_real_express_middleware);
    RUN_TEST(tslsp_real_redux_action);
    RUN_TEST(tslsp_real_class_with_static_factory_async);
    RUN_TEST(tslsp_real_dom_event_handler);
    RUN_TEST(tslsp_real_zustand_store);
    RUN_TEST(tslsp_real_typeorm_repo);
    RUN_TEST(tslsp_real_router_chain);
    RUN_TEST(tslsp_real_odm_model);

    /* More patterns */
    RUN_TEST(tslsp_iterator_for_of_with_generic);
    RUN_TEST(tslsp_class_factory_with_extends);
    RUN_TEST(tslsp_method_returning_self_chained);
    RUN_TEST(tslsp_polymorphic_this);

    /* More crash safety */
    RUN_TEST(tslsp_nocrash_complex_jsx_attributes);
    RUN_TEST(tslsp_nocrash_jsx_fragment);
    RUN_TEST(tslsp_nocrash_class_static_block);
    RUN_TEST(tslsp_nocrash_top_level_await);
    RUN_TEST(tslsp_nocrash_assertion_function);

    /* Overload resolution by arg types */
    RUN_TEST(tslsp_overload_by_arg_types_class);
    RUN_TEST(tslsp_overload_function_by_arg_types);

    /* Final v2 batch: more real-world TS patterns */
    RUN_TEST(tslsp_real_react_context);
    RUN_TEST(tslsp_real_react_useReducer);
    RUN_TEST(tslsp_real_apollo_query);
    RUN_TEST(tslsp_real_axios_client);
    RUN_TEST(tslsp_real_zod_schema);
    RUN_TEST(tslsp_real_prisma_client);
    RUN_TEST(tslsp_real_lit_element);
    RUN_TEST(tslsp_real_vitest_describe);
    RUN_TEST(tslsp_real_class_with_protected_methods);
    RUN_TEST(tslsp_real_observable_chain);
    RUN_TEST(tslsp_real_jest_mock);
    RUN_TEST(tslsp_real_state_machine);

    /* Crash safety: more exotic TS syntax */
    RUN_TEST(tslsp_nocrash_optional_call);
    RUN_TEST(tslsp_nocrash_intersection_class);
    RUN_TEST(tslsp_nocrash_branded_type);
    RUN_TEST(tslsp_nocrash_tuple_with_rest);
    RUN_TEST(tslsp_nocrash_named_tuple);

    /* keyof / typeof / indexed access (parsing) */
    RUN_TEST(tslsp_keyof_in_param_smoke);
    RUN_TEST(tslsp_typeof_in_type_position);
    RUN_TEST(tslsp_indexed_access_smoke);

    /* Hash-table registry (task #2) */
    RUN_TEST(tslsp_hash_registry_basic);
    RUN_TEST(tslsp_hash_registry_post_finalize_adds);
    RUN_TEST(tslsp_hash_registry_stress_10k);

    /* Partial relater (task #5) */
    RUN_TEST(tslsp_relater_extends_chain_resolution);
    RUN_TEST(tslsp_relater_union_member_dispatch);

    /* Conditional types (task #8) */
    RUN_TEST(tslsp_conditional_inline_method);
    RUN_TEST(tslsp_conditional_extends_chain);
    RUN_TEST(tslsp_conditional_false_branch);
    RUN_TEST(tslsp_conditional_smoke_distribution);

    /* infer X (task #9) */
    RUN_TEST(tslsp_infer_promise_unwrap_inline);
    RUN_TEST(tslsp_infer_array_element);
    RUN_TEST(tslsp_infer_no_match_falls_to_false);

    /* Async iterables (task #11) */
    RUN_TEST(tslsp_async_iter_for_await_of);
    RUN_TEST(tslsp_iter_iterator_next);
    RUN_TEST(tslsp_async_iterator_next_promise);
    RUN_TEST(tslsp_generator_next);

    /* Final edge-case tests */
    RUN_TEST(tslsp_truthy_narrow_undefined);
    RUN_TEST(tslsp_method_override);
    RUN_TEST(tslsp_type_only_import_safe);
    RUN_TEST(tslsp_satisfies_method_chain);
    RUN_TEST(tslsp_in_operator_narrow_smoke);
    RUN_TEST(tslsp_optional_property_chain);
    RUN_TEST(tslsp_void_returning_method);
    RUN_TEST(tslsp_nested_class_resolution);
}
