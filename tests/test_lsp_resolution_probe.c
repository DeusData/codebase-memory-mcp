/*
 * test_lsp_resolution_probe.c — End-to-end cross-file resolution probes.
 *
 * This suite is registered in test_main.c and runs the full index pipeline.
 * The hybrid engines cover Go, C/C++ (including CUDA), Python, JavaScript,
 * TypeScript/TSX, PHP, Rust, Java, Kotlin and C#. Engine wiring alone does not
 * establish that a particular syntax form has an admissible semantic proof.
 *
 * The shared resolver publishes an edge only when the occurrence, source
 * binding, lookup domain and exact target are proven. Legacy name matches,
 * confidence scores and strategy labels are not authority. A false positive
 * is a correctness bug; a missing genuine edge is a resolver quality issue
 * that remains counted even when extraction or materialization is missing.
 *
 * Scenarios, where applicable:
 *   S1 — cross-file function call
 *   S2 — method dispatch through a known receiver
 *   S3 — constructor or instantiation
 *   S4 — static or class-qualified call
 *   S5 — chained call through a return type
 *   S6 — inherited, embedded or trait-selected method
 *   S7 — generic, template or callable-alias use
 *   S8 — member call through a declared field type
 *
 * Every independently labelled occurrence remains in the quality denominator.
 * Supported positive controls gate recovery; audited missing cases carry
 * individual WHY and TRIED entries. Wrong emitted identities always fail.
 * Both persisted occurrences and their graph projections are checked.
 * Failed probes print persisted
 * resolver outcomes before deleting their temporary graph databases.
 *
 * Harness helpers use the LRP_ prefix to avoid link conflicts with other
 * suites. The test runner supplies the shared framework counters.
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "callable_sig.h"
#include "preprocessor.h"
#include <foundation/sha256.h>
#include <mcp/mcp.h>
#include <store/store.h>
#include <sqlite3.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <sys/wait.h>
#endif

/* ══════════════════════════════════════════════════════════════════════
 * Harness helpers (LRP_ prefix — avoids collision with other suites).
 * ══════════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} LRP_Proj;

typedef struct {
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} LRP_File;

static void lrp_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

static cbm_store_t *lrp_open_indexed(LRP_Proj *lp) {
    /* Freed before reassigning: a fixture that indexes more than once would
     * otherwise drop the previous heap name on the floor. Teardown frees the
     * last one. */
    free(lp->project);
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project)
        return NULL;
    const char *home = getenv("HOME");
    if (!home)
        home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv)
        return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp)
        free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *lrp_index(LRP_Proj *lp, const LRP_File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_lrp_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
    lrp_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = cbm_fopen(path, "wb");
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return lrp_open_indexed(lp);
}

static void lrp_cleanup(LRP_Proj *lp, cbm_store_t *store) {
    if (store)
        cbm_store_close(store);
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal);
    unlink(shm);
}

static void lrp_diag(cbm_store_t *store, const char *project, const char *scenario);

/* Diagnostic: dump edge histogram to stderr on failure. */
static const char *LRP_ALL_EDGE_TYPES[] = {
    "CALLS",      "CALL_REFERENCE", "DEFINES",    "DEFINES_METHOD", "IMPORTS",    "INHERITS",
    "IMPLEMENTS", "USAGE",          "DECORATES",  "HANDLES",        "HTTP_CALLS", "ASYNC_CALLS",
    "OVERRIDE",   "TESTS",          "TESTS_FILE", "DATA_FLOWS",     NULL};

/* Failed probes retain exact admission diagnostics before their temporary DB is
 * deleted. Bound the output and report omitted rows rather than silently truncating. */
static void lrp_diag_resolver(cbm_store_t *store, const char *project, const char *scenario) {
    const char *sql =
        "SELECT r.file_path,r.start_byte,r.end_byte,r.spelling,r.relation,r.binding,r.domain,"
        "r.analysis,r.target,r.reason,r.emitted,COALESCE(e.proof,0),"
        "COALESCE(e.source_binding_proof,0) FROM resolver_occurrences r LEFT JOIN "
        "resolver_occurrence_evidence e ON e.project=r.project AND e.ordinal=r.ordinal "
        "WHERE r.project=? ORDER BY r.file_path,r.start_byte,r.end_byte,r.relation,r.ordinal "
        "LIMIT 33";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &stmt, NULL) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        fprintf(stderr, "    [LRP] %s resolver diagnostics unavailable\n", scenario);
        sqlite3_finalize(stmt);
        return;
    }
    int rows = 0;
    int step;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (rows++ == 32) {
            fprintf(stderr, "    [LRP] %s resolver diagnostics truncated after 32 rows\n",
                    scenario);
            break;
        }
        fprintf(
            stderr,
            "    [LRP] %s resolver %s:%d-%d spelling=%s relation=%s binding=%d "
            "domain=%d analysis=%d target=%s reason=%s emitted=%d proof=%lld binding_proof=%lld\n",
            scenario, (const char *)sqlite3_column_text(stmt, 0), sqlite3_column_int(stmt, 1),
            sqlite3_column_int(stmt, 2), (const char *)sqlite3_column_text(stmt, 3),
            (const char *)sqlite3_column_text(stmt, 4), sqlite3_column_int(stmt, 5),
            sqlite3_column_int(stmt, 6), sqlite3_column_int(stmt, 7),
            (const char *)sqlite3_column_text(stmt, 8), (const char *)sqlite3_column_text(stmt, 9),
            sqlite3_column_int(stmt, 10), (long long)sqlite3_column_int64(stmt, 11),
            (long long)sqlite3_column_int64(stmt, 12));
    }
    if (!rows || (step != SQLITE_DONE && step != SQLITE_ROW))
        fprintf(stderr, "    [LRP] %s resolver rows=%d step=%d\n", scenario, rows, step);
    sqlite3_finalize(stmt);
}

static void lrp_diag(cbm_store_t *store, const char *project, const char *scenario) {
    if (!store) {
        fprintf(stderr, "    [LRP] %s: no graph DB\n", scenario);
        return;
    }
    char line[512] = {0};
    for (int i = 0; LRP_ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, LRP_ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 40) {
            char one[48];
            snprintf(one, sizeof(one), "%s=%d ", LRP_ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "    [LRP] %s edges=[%s]\n", scenario, line[0] ? line : "(none)");
    lrp_diag_resolver(store, project, scenario);
}

typedef struct {
    const char *scenario;
    const char *file;
    const char *caller;
    const char *occurrence;
    unsigned occurrence_index;
    const char *target_file;
    const char *target;
    const char *relation;
    int declaration_line;
    const char *declaration_text;
    bool required;
    bool forbidden;
    const char *quality_issue;
    const char *tried;
} LRP_Golden;

static const LRP_Golden LRP_GOLDEN[] = {
    {"lrp_java_s1_crossfile_call", "Main.java", "Main.run", "Util.square(n)", 0, "Util.java",
     "Util.square", "CALLS", 4, "    static int square(int x) { return x * x; }", false, false,
     "Same-package class/method visibility is not proved: java_proof_member/java_proof_candidate "
     "require public. Nonempty int argument applicability is also unsupported at java_lsp.c:839.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s2_method_dispatch", "Runner.java", "Runner.run", "c.inc()", 0, "Counter.java",
     "Counter.inc", "CALLS", 5, "    public void inc() { n++; }", false, false,
     "Typed local Counter receiver exists in source, but package-private Counter fails the "
     "public-only owner audit in java_proof_member; same-package visibility needs explicit "
     "support.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s2_method_dispatch", "Runner.java", "Runner.run", "c.value()", 0, "Counter.java",
     "Counter.value", "CALLS", 6, "    public int value() { return n; }", false, false,
     "Typed local Counter receiver exists in source, but package-private Counter fails the "
     "public-only owner audit in java_proof_member; same-package visibility needs explicit "
     "support.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s3_constructor", "Factory.java", "Factory.make", "new Widget(name)", 0,
     "Widget.java", "Widget.Widget", "CALLS", 5,
     "    public Widget(String name) { this.name = name; }", false, false,
     "Explicit Widget(String) constructor exists, but the semantic constructor route is absent; "
     "generic Class target is correctly rejected as noncallable. Constructor "
     "overload/applicability and package visibility need proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s4_static_method", "App.java", "App.run", "MathUtil.clamp(n, 0, 100)", 0,
     "MathUtil.java", "MathUtil.clamp", "CALLS", 4,
     "    public static int clamp(int v, int lo, int hi) {", false, false,
     "Same-package MathUtil is package-private; primitive argument conversion/applicability proof "
     "is missing for the three arguments.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s5_chained", "Main.java", "Main.run", "new Builder().add(1)", 0, "Builder.java",
     "Builder.add", "CALLS", 8, "    public Builder add(int x) { items.add(x); return this; }",
     false, false,
     "Fluent construction/return type is not represented by the strict map; java_proof_member "
     "accepts only identifier receivers. Nonempty add arguments and package-private owner need "
     "separate proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s5_chained", "Main.java", "Main.run", "new Builder().add(1).add(2)", 0,
     "Builder.java", "Builder.add", "CALLS", 8,
     "    public Builder add(int x) { items.add(x); return this; }", false, false,
     "Fluent construction/return type is not represented by the strict map; java_proof_member "
     "accepts only identifier receivers. Nonempty add arguments and package-private owner need "
     "separate proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s5_chained", "Main.java", "Main.run", "new Builder().add(1).add(2).build()", 0,
     "Builder.java", "Builder.build", "CALLS", 9,
     "    public List<Integer> build() { return items; }", false, false,
     "Fluent construction/return type is not represented by the strict map; java_proof_member "
     "accepts only identifier receivers. Nonempty add arguments and package-private owner need "
     "separate proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s6_inherited_method", "Dog.java", "Dog.run", "describe()", 0, "Animal.java",
     "Animal.describe", "CALLS", 4, "    public String describe() { return \"animal\"; }", false,
     false,
     "Bare inherited invocation requires enclosing receiver and complete superclass lookup; "
     "java_proof_member requires identifier receiver and rejects non-Object inheritance.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s7_generic", "App.java", "App.run", "Util.first(xs)", 0, "Util.java", "Util.first",
     "CALLS", 6, "    public static <T> T first(List<T> list) {", false, false,
     "Package-private Util plus generic method substitution and List<String> argument "
     "applicability are unsupported; java_proof_candidate excludes type parameters.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_java_s8_field_type_hint", "Service.java", "Service.run", "logger.log(msg)", 0,
     "Logger.java", "Logger.log", "CALLS", 4,
     "    public void log(String msg) { System.out.println(msg); }", false, false,
     "The strict map lacks class-field receiver provenance; Logger is package-private and nonempty "
     "String argument applicability is unsupported.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s1_crossfile_call", "Main.cs", "Main.Run", "Util.Square(n)", 0, "Util.cs",
     "Util.Square", "CALLS", 3, "        public static int Square(int x) { return x * x; }", false,
     false,
     "Default-internal Util needs assembly-domain access proof; csp_namespace_type requires public "
     "and csp_call accepts zero arguments only.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s2_method_dispatch", "Runner.cs", "Runner.Run", "c.Inc()", 0, "Counter.cs",
     "Counter.Inc", "CALLS", 4, "        public void Inc() { n++; }", false, false,
     "new Counter() receiver provenance is not added to the strict map; object_creation_expression "
     "fails csp_audit and internal Counter/fields also fail the restricted class domain.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s2_method_dispatch", "Runner.cs", "Runner.Run", "c.Value()", 0, "Counter.cs",
     "Counter.Value", "CALLS", 5, "        public int Value() { return n; }", false, false,
     "new Counter() receiver provenance is not added to the strict map; object_creation_expression "
     "fails csp_audit and internal Counter/fields also fail the restricted class domain.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s3_constructor", "Factory.cs", "Factory.Make", "new Widget(name)", 0, "Widget.cs",
     "Widget.Widget", "CALLS", 4, "        public Widget(string name) { Name = name; }", false,
     false,
     "Source-declared Widget(string) exists, but csp_calls handles invocation_expression only; "
     "constructor identity, assembly accessibility and argument applicability require proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s4_static_method", "App.cs", "AppEntry.Run", "MathHelper.Clamp(n, 0, 100)", 0,
     "MathHelper.cs", "MathHelper.Clamp", "CALLS", 3,
     "        public static int Clamp(int v, int lo, int hi) {", false, false,
     "Internal class access and three-argument applicability are outside "
     "csp_namespace_type/csp_call supported domains.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s5_chained", "Main.cs", "Main.Run", "new Builder().Add(1)", 0, "Builder.cs",
     "Builder.Add", "CALLS", 5, "        public Builder Add(int x) { items.Add(x); return this; }",
     false, false,
     "Constructor/return type propagation and non-identifier fluent receivers are unsupported; "
     "object_creation_expression fails body audit and Add has a nonempty argument list.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s5_chained", "Main.cs", "Main.Run", "new Builder().Add(1).Add(2)", 0, "Builder.cs",
     "Builder.Add", "CALLS", 5, "        public Builder Add(int x) { items.Add(x); return this; }",
     false, false,
     "Constructor/return type propagation and non-identifier fluent receivers are unsupported; "
     "object_creation_expression fails body audit and Add has a nonempty argument list.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s5_chained", "Main.cs", "Main.Run", "new Builder().Add(1).Add(2).Build()", 0,
     "Builder.cs", "Builder.Build", "CALLS", 6,
     "        public List<int> Build() { return items; }", false, false,
     "Constructor/return type propagation and non-identifier fluent receivers are unsupported; "
     "object_creation_expression fails body audit and Add has a nonempty argument list.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s6_inherited_method", "Child.cs", "Child.Run", "Describe()", 0, "Base.cs",
     "Base.Describe", "CALLS", 3, "        public virtual string Describe() { return \"base\"; }",
     false, false,
     "Static declaration is Base.Describe, not a guessed override; csp_call requires explicit "
     "member access and csp_simple_class does not audit inherited domains.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s7_generic", "App.cs", "AppEntry.Run", "Container.First(xs)", 0, "Container.cs",
     "Container.First", "CALLS", 4, "        public static T First<T>(List<T> xs) {", false, false,
     "Generic method/type inference, internal assembly access and argument applicability are "
     "absent; csp_candidate excludes generic and nonzero-parameter candidates.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_csharp_s8_field_type_hint", "Service.cs", "Service.Run", "logger.Log(msg)", 0,
     "Logger.cs", "Logger.Log", "CALLS", 3, "        public void Log(string msg) { }", false, false,
     "Field receiver provenance is missing; csp_function only establishes annotated parameter "
     "facts and the nonempty argument needs applicability proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s1_crossfile_call", "main.php", "run", "triple($n)", 0, "util.php", "triple", "CALLS",
     2, "function triple($x) { return $x * 3; }", false, false,
     "Literal require IMPORTS is proved, but php_proof_calls never resolves "
     "function_call_expression. Needs executed-include-to-global-function binding proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s2_method_dispatch", "main.php", "run", "$c->inc()", 0, "Counter.php", "Counter.inc",
     "CALLS", 4, "    public function inc() { $this->n++; }", false, false,
     "Strict receiver facts come only from named parameter annotations; new Counter() assignment "
     "and required-file class visibility are not proved.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s2_method_dispatch", "main.php", "run", "$c->value()", 0, "Counter.php",
     "Counter.value", "CALLS", 5, "    public function value() { return $this->n; }", false, false,
     "Strict receiver facts come only from named parameter annotations; new Counter() assignment "
     "and required-file class visibility are not proved.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s3_constructor", "factory.php", "make", "new Widget($name)", 0, "Widget.php",
     "Widget.__construct", "CALLS", 4,
     "    public function __construct($name) { $this->name = $name; }", false, false,
     "Explicit __construct exists, but object_creation_expression has no strict proof emission "
     "path; include visibility and argument applicability also need evidence.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s4_static_method", "app.php", "run", "Config::version()", 0, "Config.php",
     "Config.version", "CALLS", 3, "    public static function version() { return '1.0'; }", false,
     false,
     "Zero-argument static syntax is supported, but php_proof_indexes/definition lookup only "
     "accept explicit local import bindings; require-only class namespace exposure is absent.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s5_chained", "main.php", "run", "(new Builder())->add(1)", 0, "Builder.php",
     "Builder.add", "CALLS", 4,
     "    public function add($x) { $this->items[] = $x; return $this; }", false, false,
     "Required-file class identity, constructed receiver/return type propagation, nested receiver "
     "and add argument facts are outside php_proof_call.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s5_chained", "main.php", "run", "(new Builder())->add(1)->add(2)", 0, "Builder.php",
     "Builder.add", "CALLS", 4,
     "    public function add($x) { $this->items[] = $x; return $this; }", false, false,
     "Required-file class identity, constructed receiver/return type propagation, nested receiver "
     "and add argument facts are outside php_proof_call.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s5_chained", "main.php", "run", "(new Builder())->add(1)->add(2)->build()", 0,
     "Builder.php", "Builder.build", "CALLS", 5,
     "    public function build() { return $this->items; }", false, false,
     "Required-file class identity, constructed receiver/return type propagation, nested receiver "
     "and add argument facts are outside php_proof_call.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s6_inherited_method", "Child.php", "Child.run", "$this->describe()", 0, "Base.php",
     "Base.describe", "CALLS", 3, "    public function describe() { return 'base'; }", false, false,
     "Requires proven this-type plus complete inherited-member lookup; the strict bridge binds "
     "annotated parameters only and has no inheritance proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s7_interface_call", "Service.php", "Service.run", "$this->logger->log($msg)", 0,
     "LoggerInterface.php", "LoggerInterface.log", "CALLS", 3, "    public function log($msg);",
     false, false,
     "Static declaration is LoggerInterface.log. Constructor parameter-to-property provenance and "
     "nested receiver/nonempty arguments are not proved; FileLogger.log must never substitute by "
     "implementation guess.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_php_s8_field_type_hint", "Notifier.php", "Notifier.notify",
     "$this->mailer->send($to, $msg)", 0, "Mailer.php", "Mailer.send", "CALLS", 3,
     "    public function send($to, $body) { return true; }", false, false,
     "Constructor parameter-to-property provenance and nested receiver/nonempty arguments are "
     "unsupported; docblock text alone is not typed binding proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s1_crossfile_call", "main.rs", "run", "lib::square(n)", 0, "lib.rs", "square",
     "CALLS", 1, "pub fn square(x: i32) -> i32 { x * x }", false, false,
     "Module-qualified free function is excluded by rsp_audit, which accepts zero-argument "
     "field_expression calls only; no module-to-free-function proof bridge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s2_method_dispatch", "runner.rs", "run", "c.inc()", 0, "counter.rs", "Counter.inc",
     "CALLS", 5, "    pub fn inc(&mut self) { self.n += 1; }", false, false,
     "Parameter &mut counter::Counter is outside immutable simple imported type reference facts; "
     "mutable receiver and qualified path need proof. inc additionally requires &mut self source "
     "facts.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s2_method_dispatch", "runner.rs", "run", "c.value()", 0, "counter.rs",
     "Counter.value", "CALLS", 6, "    pub fn value(&self) -> i32 { self.n }", false, false,
     "Parameter &mut counter::Counter is outside immutable simple imported type reference facts; "
     "mutable receiver and qualified path need proof. inc additionally requires &mut self source "
     "facts.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s3_constructor", "main.rs", "run", "point::Point::new(3.0, 4.0)", 0, "point.rs",
     "Point.new", "CALLS", 4, "    pub fn new(x: f64, y: f64) -> Self { Point { x, y } }", false,
     false,
     "Source-declared associated function new is genuine CALLS, but scoped/associated calls and "
     "nonempty arguments are excluded by rsp_audit; no guessed language constructor target.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s3_constructor", "main.rs", "run", "p.dist()", 0, "point.rs", "Point.dist", "CALLS",
     5, "    pub fn dist(&self) -> f64 { (self.x * self.x + self.y * self.y).sqrt() }", false,
     false,
     "Result of Point::new is not a strict receiver fact; construction-result/return type "
     "propagation is missing.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s4_static_method", "app.rs", "run", "config::Config::default()", 0, "config.rs",
     "Config.default", "CALLS", 4, "    pub fn default() -> Self { Config { debug: false } }",
     false, false,
     "Zero-argument associated function still uses scoped identifier, which rsp_audit excludes; "
     "requires exact module/type namespace proof.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s4_static_method", "app.rs", "run", "cfg.is_debug()", 0, "config.rs",
     "Config.is_debug", "CALLS", 5, "    pub fn is_debug(&self) -> bool { self.debug }", false,
     false,
     "Strict receiver map does not propagate the Config return type from associated default().",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s5_chained", "main.rs", "run", "builder::Builder::new()", 0, "builder.rs",
     "Builder.new", "CALLS", 4, "    pub fn new() -> Self { Builder { items: vec![] } }", false,
     false,
     "Associated new, owned-self methods, fluent return types and nonempty add arguments are "
     "outside the immutable &self zero-argument member bridge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s5_chained", "main.rs", "run", "builder::Builder::new().add(1)", 0, "builder.rs",
     "Builder.add", "CALLS", 5,
     "    pub fn add(mut self, x: i32) -> Self { self.items.push(x); self }", false, false,
     "Associated new, owned-self methods, fluent return types and nonempty add arguments are "
     "outside the immutable &self zero-argument member bridge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s5_chained", "main.rs", "run", "builder::Builder::new().add(1).add(2)", 0,
     "builder.rs", "Builder.add", "CALLS", 5,
     "    pub fn add(mut self, x: i32) -> Self { self.items.push(x); self }", false, false,
     "Associated new, owned-self methods, fluent return types and nonempty add arguments are "
     "outside the immutable &self zero-argument member bridge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s5_chained", "main.rs", "run", "builder::Builder::new().add(1).add(2).build()", 0,
     "builder.rs", "Builder.build", "CALLS", 6, "    pub fn build(self) -> Vec<i32> { self.items }",
     false, false,
     "Associated new, owned-self methods, fluent return types and nonempty add arguments are "
     "outside the immutable &self zero-argument member bridge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s6_trait_method", "main.rs", "run", "d.show()", 0, "dog.rs", "Dog.show", "CALLS", 4,
     "    fn show(&self) -> String { self.name.clone() }", false, false,
     "Corrected fixture imports Display and supplies exact impl Display for Dog. Static receiver "
     "&Dog selects that source impl declaration, but rsp_impl_facts excludes trait implementations "
     "and rsp_function excludes qualified dog::Dog annotation.",
     "Reviewed corrected main module/import topology and explicit impl declaration; old invalid "
     "dog.rs mod display fixture is not used. Source bridge still excludes trait impl facts."},
    {"lrp_rust_s7_generic", "main.rs", "run", "algo::max_of(x, y)", 0, "algo.rs", "max_of", "CALLS",
     1, "pub fn max_of<T: PartialOrd>(a: T, b: T) -> T {", false, false,
     "Scoped free function plus generic constraint/substitution and argument type proof are "
     "absent; no bare-name fallback is admissible.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_s8_field_call", "service.rs", "Service.run", "self.logger.log(msg)", 0, "logger.rs",
     "Logger.log", "CALLS", 4, "    pub fn log(&self, msg: &str) { let _ = msg; }", false, false,
     "Field receiver and nonempty argument provenance are absent; rsp_member accepts identifier "
     "receivers and rsp_impl_facts only zero-explicit-argument &self methods.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_crossfile_macro_hidden_call_carrier", "main.rs", "visible", "lib::render()", 0,
     "lib.rs", "render", "CALLS", 1, "pub fn render() -> &'static str { \"ok\" }", false, false,
     "Ordinary scoped free-function bridge is absent even with zero arguments; this independent "
     "visible-call control must remain a genuine quality item.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_rust_crossfile_macro_hidden_call_carrier", "main.rs", "hidden", "lib::render()", 1,
     "lib.rs", "render", "CALLS", 1, "pub fn render() -> &'static str { \"ok\" }", false, false,
     "Macro token-tree call needs exact expanded occurrence/source-origin and module-function "
     "proof; synthetic carrier existence alone cannot authorize an edge.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_python_crossfile_dunder_carrier", "main.py", "combine", "left + right", 0, "alpha.py",
     "Alpha.__add__", "CALLS", 2, "    def __add__(self, other):", false, false,
     "Strict Python proof bridge lacks operator-dispatch proof for the synthetic __add__ carrier; "
     "relative-import binding repair is prerequisite but not sufficient. Annotation-based static "
     "declaration must not be replaced by a name-only target.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_go_usage_struct_literal", "main.go", "Make", "Config", 1, "types.go", "Config", "USAGE",
     3, "type Config struct{ Port int }", false, false,
     "Composite literal type usage is genuine, but generic Go binding validation is unsupported "
     "and the strict Go bridge does not publish the struct-literal type occurrence.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_ts_usage_type_param", "main.ts", "run", "Config", 1, "types.ts", "Config", "USAGE", 1,
     "export interface Config { timeout: number; }", false, false,
     "Source annotation occurrence is not recorded by generic binding audit; Source audit "
     "exact import "
     "candidate was rejected with binding_occurrence_missing. The independent source span must "
     "target Config, not the wrapper : Config.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_ts_usage_import_type_with_decoy", "main.ts", "choose", "Config", 1, "a/types.ts",
     "Config", "USAGE", 1, "export interface Config { selected: number; }", false, false,
     "Exact import-type annotation identity is genuine but binding audit omits the type "
     "occurrence. Must preserve ./a/types namespace identity and reject same-name b/types.ts "
     "decoy.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_tsx_usage_import_type_with_decoy", "main.tsx", "choose", "Config", 1, "a/types.ts",
     "Config", "USAGE", 1, "export interface Config { selected: number; }", false, false,
     "Exact import-type annotation identity is genuine but binding audit omits the type "
     "occurrence. Must preserve ./a/types namespace identity and reject same-name b/types.ts "
     "decoy.",
     "Prior exact fixture audit: expected target absent; read-only trace of exact fixture and the "
     "cited source proof guard. Legacy confidence/name matches were not accepted."},
    {"lrp_python_usage_instantiation", "main.py", "create", "User(name)", 0, "model.py", "User",
     "CALLS", 1, "class User:", true, false,
     "Existing explicit Python Class-call contract requires exact imported User and CLASS_CALLABLE "
     "evidence. Relative-import syntax audit repair should retain this control; do not redirect to "
     "__init__.",
     "Source audit missed due to missing relative_import/import_prefix grammar capability; "
     "The relative-import source proof guard must retain this exact "
     "constructor/Class-call regression required."},
    {"lrp_java_s2_method_dispatch", "Runner.java", "Runner.run", "new Counter()", 0, "Counter.java",
     NULL, "CALLS", 0, NULL, false, false,
     "Java supplies a default constructor, but no source constructor declaration exists and "
     "projection must not redirect its call to the Class.",
     "Source fixture has no constructor declaration; do not invent a Class target or silently omit "
     "this semantic invocation from the denominator. A stable implicit declaration identity is "
     "needed before an exact graph suffix can be asserted."},
    {"lrp_java_s5_chained", "Main.java", "Main.run", "new Builder()", 0, "Builder.java", NULL,
     "CALLS", 0, NULL, false, false,
     "Java default Builder constructor has no materialized source declaration; this missing "
     "constructor identity must remain visible independently of the fluent method misses.",
     "Source fixture has no constructor declaration; do not invent a Class target or silently omit "
     "this semantic invocation from the denominator. A stable implicit declaration identity is "
     "needed before an exact graph suffix can be asserted."},
    {"lrp_java_s8_field_type_hint", "Service.java", NULL, "new Logger()", 0, "Logger.java", NULL,
     "CALLS", 0, NULL, false, false,
     "Implicit constructor invocation occurs in a field initializer, with neither source-declared "
     "constructor target nor source-declared initializer callable owner. Needs stable semantic "
     "identities for both; do not attach to Service.run.",
     "Inspected exact source initializer and absent constructor declaration; no fabricated scope "
     "or Class target."},
    {"lrp_csharp_s2_method_dispatch", "Runner.cs", "Runner.Run", "new Counter()", 0, "Counter.cs",
     NULL, "CALLS", 0, NULL, false, false,
     "C# default constructor has no source declaration; needs materialized implicit target "
     "identity before an exact CALLS target can exist.",
     "Source fixture has no constructor declaration; do not invent a Class target or silently omit "
     "this semantic invocation from the denominator. A stable implicit declaration identity is "
     "needed before an exact graph suffix can be asserted."},
    {"lrp_csharp_s5_chained", "Main.cs", "Main.Run", "new Builder()", 0, "Builder.cs", NULL,
     "CALLS", 0, NULL, false, false,
     "C# default Builder constructor lacks an explicit source declaration and stable synthetic "
     "graph identity.",
     "Source fixture has no constructor declaration; do not invent a Class target or silently omit "
     "this semantic invocation from the denominator. A stable implicit declaration identity is "
     "needed before an exact graph suffix can be asserted."},
    {"lrp_csharp_s8_field_type_hint", "Service.cs", NULL, "new Logger()", 0, "Logger.cs", NULL,
     "CALLS", 0, NULL, false, false,
     "Implicit constructor call appears in a field initializer. Both initializer owner and default "
     "constructor target need stable semantic identities; it must not be attributed to "
     "Service.Run.",
     "Source fixture inspection, no guessed owner/target."},
    {"lrp_python_s1_crossfile_call", "main.py", "run", "triple(n)", 0, "util.py", "triple", "CALLS",
     1, "def triple(x):", true, false, NULL, NULL},
    {"lrp_python_s2_method_dispatch", "main.py", "run", "Counter()", 0, "counter.py", "Counter",
     "CALLS", 1, "class Counter:", true, false, NULL,
     "Exact imported Class-call identity with CLASS_CALLABLE and occurrence evidence is supported; "
     "retain this recovered constructor control independently of later member calls."},
    {"lrp_python_s2_method_dispatch", "main.py", "run", "c.inc()", 0, "counter.py", "Counter.inc",
     "CALLS", 5, "    def inc(self):", false, false,
     "Receiver construction is not established by the strict map; class mutation audit "
     "conservatively rejects attribute writes, including n.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s2_method_dispatch", "main.py", "run", "c.value()", 0, "counter.py",
     "Counter.value", "CALLS", 8, "    def value(self):", false, false,
     "Receiver construction is not established by the strict map; class mutation audit "
     "conservatively rejects attribute writes, including n.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s3_constructor", "main.py", "make", "Widget(name)", 0, "widget.py", "Widget",
     "CALLS", 1, "class Widget:", true, false, NULL,
     "Exact imported Class-call identity with CLASS_CALLABLE and occurrence evidence is supported; "
     "retain this recovered constructor control independently of later member calls."},
    {"lrp_python_s4_class_method", "main.py", "run", "Factory.version()", 0, "factory.py",
     "Factory.version", "CALLS", 7, "    def version():", false, false,
     "Decorated descriptor identity and class receiver binding are not established by the source "
     "proof adapter.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s5_chained", "main.py", "make", "Builder()", 0, "builder.py", "Builder", "CALLS",
     1, "class Builder:", true, false, NULL,
     "Exact imported Class-call identity with CLASS_CALLABLE and occurrence evidence is supported; "
     "retain this recovered constructor control independently of later member calls."},
    {"lrp_python_s5_chained", "main.py", "make", "Builder().add(1)", 0, "builder.py", "Builder.add",
     "CALLS", 5, "    def add(self, x):", false, false,
     "Constructor, field mutation effects, nonempty arguments and fluent return propagation lack "
     "complete source derivations.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s5_chained", "main.py", "make", "Builder().add(1).add(2)", 0, "builder.py",
     "Builder.add", "CALLS", 5, "    def add(self, x):", false, false,
     "Constructor, field mutation effects, nonempty arguments and fluent return propagation lack "
     "complete source derivations.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s5_chained", "main.py", "make", "Builder().add(1).add(2).build()", 0, "builder.py",
     "Builder.build", "CALLS", 9, "    def build(self):", false, false,
     "Constructor, field mutation effects, nonempty arguments and fluent return propagation lack "
     "complete source derivations.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s6b_annotated_param", "child.py", "run", "c.describe()", 0, "base.py",
     "Base.describe", "CALLS", 2, "    def describe(self):", false, false,
     "Inherited member lookup requires a complete MRO and receiver provenance; aggregate INHERITS "
     "edges are not proof inputs.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s6b_self_attr", "child.py", "Child.go", "self.describe()", 0, "base.py",
     "Base.describe", "CALLS", 2, "    def describe(self):", false, false,
     "Inherited member lookup requires a complete MRO and receiver provenance; aggregate INHERITS "
     "edges are not proof inputs.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s6b_constructor", "child.py", "run", "Child().describe()", 0, "base.py",
     "Base.describe", "CALLS", 2, "    def describe(self):", false, false,
     "Inherited member lookup requires a complete MRO and receiver provenance; aggregate INHERITS "
     "edges are not proof inputs.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s7_annotated_call", "service.py", "Service.get", "self.repo.find(id)", 0,
     "repo.py", "Repo.find", "CALLS", 2, "    def find(self, id: int) -> str:", false, false,
     "Parameter-to-property assignment and nested field receiver type are not represented by the "
     "strict map; method argument applicability is also unsupported.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_python_s8_field_type_hint", "service.py", "Service.__init__", "Logger()", 0, "logger.py",
     "Logger", "CALLS", 1, "class Logger:", true, false, NULL,
     "Exact imported Class-call identity with CLASS_CALLABLE and occurrence evidence is supported; "
     "retain this recovered constructor control independently of later member calls."},
    {"lrp_python_s8_field_type_hint", "service.py", "Service.run", "self.logger.log(msg)", 0,
     "logger.py", "Logger.log", "CALLS", 2, "    def log(self, msg: str) -> None:", false, false,
     "Annotated field provenance, nested receiver identity and nonempty argument applicability are "
     "not established.",
     "Reviewed exact fixture and proof guards: no intended admitted call; legacy name and "
     "confidence "
     "matches "
     "cannot supply the missing source facts."},
    {"lrp_c_s1_crossfile_call", "main.c", "run", "add(x, 1)", 0, "main.c", "add", "CALLS", 1,
     "int add(int a, int b);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s2_funcptr_in_struct", "main.c", "run", "make_ops()", 0, "main.c", "make_ops", "CALLS",
     2, "Ops make_ops(void);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s3_constructor", "main.c", "make_buf", "buf_new(n)", 0, "main.c", "buf_new", "CALLS", 2,
     "Buf *buf_new(int cap);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s4_static_local", "main.c", "run", "exported(n)", 0, "main.c", "exported", "CALLS", 1,
     "int exported(int x);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s5_chained_deref", "main.c", "run", "node_val(head->next)", 0, "main.c", "node_val",
     "CALLS", 2, "int node_val(const Node *n);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s6_base_method", "circle.c", "run", "shape_color(&c->base)", 0, "circle.c",
     "shape_color", "CALLS", 3, "int shape_color(const Shape *s);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s7_generic_callback", "sort.c", "my_sort", "int_cmp(&arr[0], &arr[1])", 0, "sort.c",
     "int_cmp", "CALLS", 2, "int int_cmp(const void *a, const void *b);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s8_field_call", "service.c", "run", "log_msg(&svc->logger, m)", 0, "service.c",
     "log_msg", "CALLS", 3, "void log_msg(const Logger *l, const char *msg);", false, false,
     "The visible prototype is a source declaration, but C extraction registers only function "
     "definitions; no graph node or exact registry declaration exists for the prototype. Cross-TU "
     "definition equivalence is a separate gap requiring an explicit linkage unit and compatible "
     "effective linkage/signature.",
     "Reviewed lang_specs.c c_func_types, extract_defs.c resolve_func_name_c_family, c_lsp.c "
     "c_proof_collect/c_proof_domain; Source audit reported binding_not_found. Existing "
     "resolver_c "
     "source-prototype unit controls inject declaration identities and therefore do not test "
     "materialization."},
    {"lrp_c_s2_funcptr_in_struct", "main.c", "run", "o.compute(n)", 0, "ops.c", "double_it",
     "CALLS", 3, "static int double_it(int x) { return x * 2; }", false, false,
     "Requires proving make_ops linkage to this implementation, its returned struct field "
     "assignment, and preservation of that callable value at the caller. A field type alone does "
     "not identify double_it.",
     "c_callable_proof accepts identifiers/qualified names and tracked local function pointers, "
     "not returned function-pointer fields. Source audit recorded o.compute "
     "binding_not_found; its factory "
     "call could mask this separate miss."},
    {"lrp_c_s4_static_local", "util.c", "exported", "helper(x)", 0, "util.c", "helper", "CALLS", 1,
     "static int helper(int x) { return x + 1; }", true, false, NULL,
     "Source-visible same-TU function and exact lexical domain are supported; this local edge "
     "explains the old aggregate floor passing."},
    {"lrp_c_s4_static_local", "main.c", "run", "exported(n)", 0, "util.c", "helper", "CALLS", 1,
     "static int helper(int x) { return x + 1; }", false, true,
     "Static helper belongs to util.c; no source binding from main.run selects it.",
     "Old test asserted only the total CALLS count and did not enforce this negative."},
    {"lrp_c_s7_generic_callback", "sort.c", "my_sort", "fn(&arr[0], &arr[1])", 0, "sort.c",
     "int_cmp", "CALLS", 2, "int int_cmp(const void *a, const void *b);", false, false,
     "The immutable local pointer assignment selects the visible int_cmp prototype, whose "
     "declaration is not materialized. The callback occurrence must be accounted independently of "
     "the direct call.",
     "Source audit persisted the direct int_cmp call and value occurrence but no fn CALLS "
     "row. "
     "c_callable_proof can track exact local pointer targets once their declarations exist."},
    {"lrp_cpp_s1_crossfile_call", "main.cpp", "run", "square(n)", 0, "main.cpp", "square", "CALLS",
     1, "int square(int x);", false, false,
     "C++ prototype wrapper has no extracted function name/node. Separately, c_proof_call_domain "
     "excludes identifier argument n even though it is declared int; the current no-ADL rule only "
     "accepts literals. Definition linkage remains unproved.",
     "Reviewed extract_defs.c resolve_func_name_c_family, c_lsp.c c_proof_call_domain and Prior "
     "source audit "
     "binding_not_found."},
    {"lrp_cpp_s2_method_dispatch", "main.cpp", "run", "c.inc()", 0, "counter.hpp", "Counter.inc",
     "CALLS", 5, "    void inc() { n++; }", true, false, NULL,
     "Corrected header fixture uses the existing direct zero-argument member subset; "
     "resolver_cpp_direct_member_requires_source_class_and_visibility and "
     "resolver_cpp_written_header_type_requires_complete_visible_domain are positive controls. "
     "The direct-member proof controls define the required supported subset."},
    {"lrp_cpp_s2_method_dispatch", "main.cpp", "run", "c.val()", 0, "counter.hpp", "Counter.val",
     "CALLS", 6, "    int val() const { return n; }", true, false, NULL,
     "Corrected header fixture uses the existing direct zero-argument member subset; "
     "resolver_cpp_direct_member_requires_source_class_and_visibility and "
     "resolver_cpp_written_header_type_requires_complete_visible_domain are positive controls. "
     "The direct-member proof controls define the required supported subset."},
    {"lrp_cpp_s2_method_dispatch", "main.cpp", "run", "Counter c;", 0, "counter.hpp",
     "Counter.Counter", "CALLS", 4, "    Counter() : n(0) {}", false, false,
     "Default-initialization invokes the explicit Counter constructor, but ordinary variable "
     "declaration constructor occurrences are not extracted/proven.",
     "Corrected incomplete-type fixture; audit found no constructor producer rule. inc/val must "
     "not satisfy this separate occurrence."},
    {"lrp_cpp_s3_constructor", "main.cpp", "run", "Widget w(n);", 0, "widget.hpp", "Widget.Widget",
     "CALLS", 4, "    Widget(int id) : id(id) {}", false, false,
     "Direct-initialization requires an extracted constructor occurrence and constructor "
     "overload/applicability proof.",
     "Source audit had no Widget constructor CALLS carrier; corrected one-header fixture "
     "retains the same "
     "direct-initialization syntax."},
    {"lrp_cpp_s3_constructor", "main.cpp", "run", "w.get_id()", 0, "widget.hpp", "Widget.get_id",
     "CALLS", 5, "    int get_id() const { return id; }", true, false, NULL,
     "Known declared local Widget receiver and public zero-argument header method match the "
     "existing proven direct-member subset; constructor absence cannot waive this getter."},
    {"lrp_cpp_s4_static_method", "main.cpp", "run", "Registry::count()", 0, "registry.hpp",
     "Registry.count", "CALLS", 3, "    static int count() { return 42; }", false, false,
     "Qualified static-member lookup needs class ownership/staticness proof. Free-function lookup "
     "excludes receiver methods; the direct member bridge only handles field-expression calls.",
     "Reviewed symbols.c function candidate receiver exclusion and c_lsp.c c_proof_direct_method; "
     "corrected the ODR-invalid fixture without changing the call."},
    {"lrp_cpp_s5_chained", "main.cpp", "run", "Builder()", 0, "builder.hpp", NULL, "CALLS", 0, NULL,
     false, false,
     "The implicit default constructor has no materialized callable declaration or constructor "
     "binding proof. A class node is not automatically a callable target.",
     "Header correction makes the class definition visible; current member bridge has no "
     "constructor contract."},
    {"lrp_cpp_s5_chained", "main.cpp", "run", "Builder().add(1)", 0, "builder.hpp", "Builder.add",
     "CALLS", 3, "    Builder& add(int x) { (void)x; return *this; }", false, false,
     "Constructor-result receiver provenance and one-argument member applicability are unsupported "
     "by the direct zero-argument identifier-receiver bridge.",
     "c_proof_direct_method requires identifier receiver and zero arguments; this preserved chain "
     "violates both requirements."},
    {"lrp_cpp_s5_chained", "main.cpp", "run", "Builder().add(1).build()", 0, "builder.hpp",
     "Builder.build", "CALLS", 4, "    int build() { return 1; }", false, false,
     "Requires the exact Builder& return fact from the selected add method and propagation through "
     "the chained receiver.",
     "c_proof_direct_method accepts only identifier receivers; old aggregate floor could be "
     "satisfied by another call."},
    {"lrp_cpp_s6_virtual_inherited", "circle.cpp", "Circle.run", "Shape::area()", 0, "shape.hpp",
     "Shape.area", "CALLS", 3, "    virtual int area() const { return 0; }", false, false,
     "Explicit base qualification selects Shape.area and suppresses virtual dispatch. "
     "Class-context qualified binding and base-declaration lookup lack proof.",
     "c_proof_name_lookup rejects enclosing_class_qn; c_proof_direct_class excludes inherited "
     "domains. Corrected header retains explicit Shape::area; Circle.area is a forbidden "
     "substitute."},
    {"lrp_cpp_s7_template", "main.cpp", "run", "clamp(n, 0, 100)", 0, "algo.hpp", "clamp", "CALLS",
     2, "T clamp(T v, T lo, T hi) {", false, false,
     "Template declaration identity, argument deduction/applicability, and builtin argument "
     "evidence to close ADL are missing.",
     "c_proof_collect skips template domains; c_proof_classify rejects type_param_names; "
     "c_proof_call_domain rejects identifier n. Template definition is now visible via header."},
    {"lrp_cpp_s8_field_call", "service.cpp", "Service.run", "logger.log(m)", 0, "logger.hpp",
     "Logger.log", "CALLS", 3, "    void log(const char *msg) { (void)msg; }", false, false,
     "Requires implicit logger field type provenance and nonzero-argument method applicability.",
     "c_proof_direct_method only supports zero-argument identifier receiver calls with an existing "
     "type-map fact; field initializer/inference cannot supply authority."},
    {"lrp_go_s1_crossfile_call", "main.go", "Run", "Double(n)", 0, "util.go", "Double", "CALLS", 3,
     "func Double(x int) int { return x * 2 }", true, false, NULL,
     "Exact same-package free-function binding and captured occurrence promotion are supported; "
     "Source audit scenario passed."},
    {"lrp_go_s2_method_dispatch", "runner.go", "Run", "c.Inc()", 0, "counter.go", "Inc", "CALLS", 5,
     "func (c *Counter) Inc() { c.n++ }", true, false, NULL,
     "Explicit *Counter parameter and direct method-set declaration identity are supported. Graph "
     "Go method QNs are flat; target file + method name identifies this fixture declaration."},
    {"lrp_go_s2_method_dispatch", "runner.go", "Run", "c.Value()", 0, "counter.go", "Value",
     "CALLS", 7, "func (c *Counter) Value() int { return c.n }", true, false, NULL,
     "Explicit *Counter parameter and direct method-set declaration identity are supported. Graph "
     "Go method QNs are flat; target file + method name identifies this fixture declaration."},
    {"lrp_go_s3_constructor", "main.go", "Make", "NewCounter(n)", 0, "counter.go", "NewCounter",
     "CALLS", 5, "func NewCounter(start int) *Counter {", true, false, NULL,
     "Ordinary same-package free-function call; Counter literal is a type/value use, not an "
     "additional CALLS edge."},
    {"lrp_go_s4_static_call", "main.go", "Run", "math.Square(n)", 0, "math/ops.go", "Square",
     "CALLS", 3, "func Square(x int) int { return x * x }", true, false, NULL,
     "Fixture now declares module example.com/probe and imports its exact math package path. Exact "
     "package binding is supported; Source audit still failed, so this remains an "
     "integration defect "
     "rather than an allowed miss."},
    {"lrp_go_stdlib_import_does_not_bind_local_directory", "main.go", "Run", "math.Square(n)", 0,
     "math/ops.go", "Square", "CALLS", 3, "func Square(x int) int { return x * x }", false, true,
     "Standard-library math does not contain Square and cannot bind the local project directory.",
     "Corrected positive has a declared module path; paired negative must retain zero exact local "
     "Square edges."},
    {"lrp_go_s5_chained_call", "main.go", "Make", "NewBuilder()", 0, "builder.go", "NewBuilder",
     "CALLS", 5, "func NewBuilder() *Builder { return &Builder{} }", true, false, NULL,
     "Same-package source function and declared *Builder return are supported."},
    {"lrp_go_s5_chained_call", "main.go", "Make", "NewBuilder().Add(\"x\")", 0, "builder.go", "Add",
     "CALLS", 7, "func (b *Builder) Add(s string) *Builder {", true, false, NULL,
     "go_return_fact can select free NewBuilder and prove its written *Builder return; direct Add "
     "method lookup is supported."},
    {"lrp_go_s5_chained_call", "main.go", "Make", "NewBuilder().Add(\"x\").Build()", 0,
     "builder.go", "Build", "CALLS", 12, "func (b *Builder) Build() []string { return b.items }",
     false, false,
     "Method-return provenance is missing: go_return_fact re-queries Add through the free-function "
     "resolver, which excludes receiver methods, so its *Builder return cannot establish the next "
     "receiver.",
     "Read go_return_fact and symbols function-candidate exclusion. Source audit scenario "
     "green only "
     "establishes its aggregate floor, not this outer Build occurrence."},
    {"lrp_go_s6_inherited_method", "dog.go", "Run", "d.Speak()", 0, "animal.go", "Speak", "CALLS",
     5, "func (a *Animal) Speak() string { return a.Name }", false, false,
     "Promoted embedded-method selection needs a complete depth/ambiguity lookup proof.",
     "go_callable_proof returns no proof when no direct method exists and embedded_types is "
     "nonempty. Legacy promotion remains diagnostic only; Source audit missed this call."},
    {"lrp_go_s7_generic_call", "main.go", "Double", "Map(xs, func(x int) int { return x * 2 })", 0,
     "util.go", "Map", "CALLS", 3, "func Map[T, U any](xs []T, fn func(T) U) []U {", true, false,
     NULL,
     "Named same-package generic function identity is supported without selecting an overload. The "
     "fn(x) inside Map is a dynamic callback and cannot substitute for this outer source "
     "occurrence."},
    {"lrp_go_s8_field_type_hint", "service.go", "Get", "s.repo.Find(id)", 0, "repo.go", "Find",
     "CALLS", 5, "func (r *Repo) Find(id int) string { return \"\" }", false, false,
     "The receiver is a selector expression requiring field-type provenance; go_receiver_fact does "
     "not support that shape.",
     "Reviewed go_receiver_fact_inner supported identifier/call/composite cases and existing "
     "field-chain quality accounting; Source audit missed s.repo.Find."},
    {"lrp_ts_s1_crossfile_call", "main.ts", "run", "format(s)", 0, "util.ts", "format", "CALLS", 1,
     "export function format(s: string): string { return s.trim(); }", true, false, NULL,
     "Explicit named import/export source binding is supported. util.format calling external "
     "s.trim cannot substitute."},
    {"lrp_ts_s2_method_dispatch", "main.ts", "run", "c.inc()", 0, "counter.ts", "Counter.inc",
     "CALLS", 3, "    inc(): void { this.n++; }", true, false, NULL,
     "TS receiver_type supports const identifier initialized by new imported Counter; no "
     "inheritance or competing declaration. Existing direct-method proof tests are positive "
     "controls."},
    {"lrp_ts_s2_method_dispatch", "main.ts", "run", "c.value()", 0, "counter.ts", "Counter.value",
     "CALLS", 4, "    value(): number { return this.n; }", true, false, NULL,
     "TS receiver_type supports const identifier initialized by new imported Counter; no "
     "inheritance or competing declaration. Existing direct-method proof tests are positive "
     "controls."},
    {"lrp_ts_s2_method_dispatch", "main.ts", "run", "new Counter()", 0, "counter.ts", NULL, "CALLS",
     0, NULL, false, false,
     "Implicit constructor has no materialized callable declaration/constructor proof. The "
     "new-expression type fact used by later member calls does not prove constructor invocation.",
     "TS proof bridge handles member calls and method references, not new-expression publication."},
    {"lrp_ts_s3_constructor", "main.ts", "make", "new Widget(name)", 0, "widget.ts",
     "Widget.constructor", "CALLS", 2, "    constructor(public name: string) {}", false, false,
     "new_expression has a call carrier but no constructor-target semantic proof.",
     "Source audit recorded callable/class target rejection; explicit Widget.constructor is "
     "present and "
     "cannot be replaced with unrelated Widget.label."},
    {"lrp_ts_s4_static_method", "main.ts", "run", "Factory.version()", 0, "factory.ts",
     "Factory.version", "CALLS", 2, "    static version(): string { return '1.0'; }", false, false,
     "Imported class value is not a proven instance receiver; static class-member selection needs "
     "an audited owner/staticness rule.",
     "TS receiver_type requires this or a lexical identifier with annotation/new-expression "
     "initializer; the import identifier Factory does not supply that binding."},
    {"lrp_ts_s4_static_method", "factory.ts", "Factory.create", "new Factory()", 0, "factory.ts",
     NULL, "CALLS", 0, NULL, false, false,
     "Implicit Factory constructor has no materialized callable declaration or constructor proof.",
     "The old global floor could be satisfied by this unrelated factory body call after a "
     "constructor implementation; it must never satisfy run->version."},
    {"lrp_ts_s5_chained", "main.ts", "run", "new Builder()", 0, "builder.ts", NULL, "CALLS", 0,
     NULL, false, false,
     "Implicit Builder constructor is not materialized/proven as a callable declaration.",
     "TS producer supports declared direct methods only; constructor binding remains separate."},
    {"lrp_ts_s5_chained", "main.ts", "run", "new Builder().add(1)", 0, "builder.ts", "Builder.add",
     "CALLS", 3, "    add(x: number): Builder { this.items.push(x); return this; }", false, false,
     "Chained receiver requires constructor/selected-method return facts; current receiver_type "
     "accepts only this or a lexical identifier, not call/new-expression receivers.",
     "Reviewed ts_proof.c receiver_type and cbm_ts_proof_method. Count each add occurrence "
     "independently even though graph edges may deduplicate by source/target."},
    {"lrp_ts_s5_chained", "main.ts", "run", "new Builder().add(1).add(2)", 0, "builder.ts",
     "Builder.add", "CALLS", 3, "    add(x: number): Builder { this.items.push(x); return this; }",
     false, false,
     "Chained receiver requires constructor/selected-method return facts; current receiver_type "
     "accepts only this or a lexical identifier, not call/new-expression receivers.",
     "Reviewed ts_proof.c receiver_type and cbm_ts_proof_method. Count each add occurrence "
     "independently even though graph edges may deduplicate by source/target."},
    {"lrp_ts_s5_chained", "main.ts", "run", "new Builder().add(1).add(2).build()", 0, "builder.ts",
     "Builder.build", "CALLS", 4, "    build(): number[] { return this.items; }", false, false,
     "Chained receiver requires constructor/selected-method return facts; current receiver_type "
     "accepts only this or a lexical identifier, not call/new-expression receivers.",
     "Reviewed ts_proof.c receiver_type and cbm_ts_proof_method. Count each add occurrence "
     "independently even though graph edges may deduplicate by source/target."},
    {"lrp_ts_s6_inherited_method", "child.ts", "run", "c.describe()", 0, "base.ts", "Base.describe",
     "CALLS", 2, "    describe(): string { return 'base'; }", false, false,
     "Complete inherited lookup proof is absent even though the inheritance edge and legacy lookup "
     "may exist.",
     "cbm_ts_proof_method explicitly rejects owners with embedded_types. The historical lsp_ts "
     "strategy assertion is obsolete after centralized projection and cannot establish proof."},
    {"lrp_ts_s7_generic", "main.ts", "run", "maxOf(a, b, (x, y) => x - y)", 0, "algo.ts", "maxOf",
     "CALLS", 1, "export function maxOf<T>(a: T, b: T, cmp: (x: T, y: T) => number): T {", true,
     false, NULL,
     "Explicit imported exported function binding is supported; the internal dynamic cmp(a,b) call "
     "is not this occurrence."},
    {"lrp_ts_s8_field_type_hint", "service.ts", "Service.run", "this.logger.log(msg)", 0,
     "logger.ts", "Logger.log", "CALLS", 2, "    log(msg: string): void { console.log(msg); }",
     false, false,
     "Nested field receiver needs proved declared field type and ownership; current receiver_type "
     "rejects member-expression receivers.",
     "Reviewed ts_proof.c receiver_type. Constructor and external console.log edges must not "
     "satisfy this occurrence."},
    {"lrp_ts_s8_field_type_hint", "service.ts", "Service.constructor", "new Logger()", 0,
     "logger.ts", NULL, "CALLS", 0, NULL, false, false,
     "Implicit Logger constructor is a genuine separate invocation, without materialized callable "
     "identity/constructor proof.",
     "Class type existence does not grant CLASS_CALLABLE authority for TypeScript."},
    {"lrp_kotlin_s1_crossfile_call", "Main.kt", "run", "double(n)", 0, "Util.kt", "double", "CALLS",
     1, "fun double(x: Int): Int = x * 2", true, false, NULL,
     "Same-package top-level identity and fixed positional builtin Int applicability are "
     "supported; Source audit passed."},
    {"lrp_kotlin_s2_method_dispatch", "Runner.kt", "run", "c.inc()", 0, "Counter.kt", "Counter.inc",
     "CALLS", 3, "    fun inc() { n++ }", false, false,
     "Counter contains property n. kt_proof_class_fact treats every non-function class-body "
     "declaration as an incomplete direct member domain; typed receiver alone is insufficient.",
     "Reviewed kt_proof_class_fact and direct parameter-type call path; zero-argument method-only "
     "classes have positive controls, but this property-bearing fixture does not."},
    {"lrp_kotlin_s2_method_dispatch", "Runner.kt", "run", "c.value()", 0, "Counter.kt",
     "Counter.value", "CALLS", 4, "    fun value(): Int = n", false, false,
     "Counter contains property n. kt_proof_class_fact treats every non-function class-body "
     "declaration as an incomplete direct member domain; typed receiver alone is insufficient.",
     "Reviewed kt_proof_class_fact and direct parameter-type call path; zero-argument method-only "
     "classes have positive controls, but this property-bearing fixture does not."},
    {"lrp_kotlin_s3_constructor", "Main.kt", "make", "Widget(name)", 0, "Widget.kt", NULL, "CALLS",
     0, NULL, false, false,
     "The primary constructor is the class header; no independent callable graph identity or "
     "Kotlin class-call contract is established. Constructor syntax cannot be promoted by class "
     "name alone.",
     "kt_proof_function_identity rejects names with type declarations; kt_proof_class_fact also "
     "leaves primary-constructor classes incomplete."},
    {"lrp_kotlin_s4_companion_static", "App.kt", "run", "Config.default()", 0, "Config.kt", NULL,
     "CALLS", 0, NULL, false, false,
     "Anonymous companion ownership/materialization and class-to-companion dispatch lack proof.",
     "kt_proof_call navigation requires a typed lexical receiver; kt_proof_walk does not walk "
     "companion-object declarations. No exact graph QN is invented."},
    {"lrp_kotlin_s4_companion_static", "Config.kt", "Config.Companion.default", "Config(false)", 0,
     "Config.kt", NULL, "CALLS", 0, NULL, false, false,
     "Primary constructor call within the companion is a separate genuine site; no callable "
     "materialization/constructor contract.",
     "No Kotlin class-call rule; absence of companion graph scope must remain counted."},
    {"lrp_kotlin_s5_chained", "Main.kt", "run", "Builder()", 0, "Builder.kt", NULL, "CALLS", 0,
     NULL, false, false, "Implicit primary constructor is not materialized/proven as a callable.",
     "Bare function resolution rejects class names; there is no separate constructor producer."},
    {"lrp_kotlin_s5_chained", "Main.kt", "run", "Builder().add(1)", 0, "Builder.kt", "Builder.add",
     "CALLS", 3, "    fun add(x: Int): Builder { items.add(x); return this }", false, false,
     "Requires chained constructor/return-type provenance. Navigation proof accepts a simple "
     "lexical receiver only; Builder property items also makes class domain incomplete.",
     "Reviewed kt_proof_call receiver shape and kt_proof_class_fact. Each add occurrence remains a "
     "separate denominator item."},
    {"lrp_kotlin_s5_chained", "Main.kt", "run", "Builder().add(1).add(2)", 0, "Builder.kt",
     "Builder.add", "CALLS", 3, "    fun add(x: Int): Builder { items.add(x); return this }", false,
     false,
     "Requires chained constructor/return-type provenance. Navigation proof accepts a simple "
     "lexical receiver only; Builder property items also makes class domain incomplete.",
     "Reviewed kt_proof_call receiver shape and kt_proof_class_fact. Each add occurrence remains a "
     "separate denominator item."},
    {"lrp_kotlin_s5_chained", "Main.kt", "run", "Builder().add(1).add(2).build()", 0, "Builder.kt",
     "Builder.build", "CALLS", 4, "    fun build(): List<Int> = items.toList()", false, false,
     "Requires chained constructor/return-type provenance. Navigation proof accepts a simple "
     "lexical receiver only; Builder property items also makes class domain incomplete.",
     "Reviewed kt_proof_call receiver shape and kt_proof_class_fact. Each add occurrence remains a "
     "separate denominator item."},
    {"lrp_kotlin_s6_inherited_method", "Child.kt", "Child.run", "describe()", 0, "Base.kt",
     "Base.describe", "CALLS", 2, "    open fun describe(): String = \"base\"", false, false,
     "Implicit-this inherited member lookup lacks proof; class-context bare calls are disabled and "
     "delegation/base domains remain incomplete.",
     "kt_proof_function sets bare_calls=false for members; kt_proof_class_fact rejects "
     "delegation_specifiers. The parent constructor call Base() cannot satisfy describe()."},
    {"lrp_kotlin_s6_inherited_method", "Child.kt", "Child", "Base()", 0, "Base.kt", NULL, "CALLS",
     0, NULL, false, false,
     "Implicit base-constructor invocation in the class header lacks callable materialization and "
     "an initializer scope contract.",
     "The proof walker processes function bodies, not constructor delegation as a CALLS producer."},
    {"lrp_kotlin_s7_generic", "Main.kt", "run", "maxOf(a, b)", 0, "Algo.kt", "maxOf", "CALLS", 1,
     "fun <T : Comparable<T>> maxOf(a: T, b: T): T = if (a > b) a else b", false, false,
     "Generic declaration/source-signature applicability is unsupported; candidate classifier "
     "rejects type_param_names.",
     "Reviewed kt_proof_candidate, kt_proof_function generic rejection and scalar-only signature "
     "bridge. Unique short name is not authority."},
    {"lrp_kotlin_s8_field_type_hint", "Service.kt", "Service.run", "logger.log(msg)", 0,
     "Logger.kt", "Logger.log", "CALLS", 2, "    fun log(msg: String): Unit { println(msg) }",
     false, false,
     "Property initializer Logger() does not donate a proven receiver identity to member body; "
     "current type map populates declared parameters and callable vals, not class field "
     "constructor inference.",
     "Reviewed kt_proof_function parameter map and kt_proof_call navigation; constructor and "
     "println calls cannot satisfy this method occurrence."},
    {"lrp_kotlin_s8_field_type_hint", "Service.kt", "Service", "Logger()", 0, "Logger.kt", NULL,
     "CALLS", 0, NULL, false, false,
     "Property-initializer constructor invocation lacks callable materialization and initializer "
     "scope contract.",
     "kt_proof_walk does not publish constructor calls from class property initializers; no Kotlin "
     "class-call authority."},
    {"lrp_python_s6b_constructor", "child.py", "run", "Child()", 0, "child.py", "Child", "CALLS", 4,
     "class Child(Base):", true, false, NULL,
     "Exact imported Class-call identity with CLASS_CALLABLE and occurrence evidence is supported; "
     "retain this recovered constructor control independently of later member calls."},
};

/* Callable signatures are normalized by the same QN format contract as nodes;
 * file and source declaration line disambiguate overloads independently. Go's
 * receiver owner is not a QN component: its flat method suffix is intentional. */
static bool lrp_qn_has_declaration(const char *actual, const char *suffix) {
    if (!actual || !suffix)
        return false;
    size_t length = cbm_qn_callable_base_len(actual), wanted = strlen(suffix);
    return (length == wanted && !memcmp(actual, suffix, wanted)) ||
           (length > wanted && actual[length - wanted - 1] == '.' &&
            !memcmp(actual + length - wanted, suffix, wanted));
}

static const char *lrp_fixture(const LRP_File *files, int count, const char *name) {
    for (int i = 0; i < count; i++)
        if (!strcmp(files[i].name, name))
            return files[i].content;
    return NULL;
}

static bool lrp_golden_site(const LRP_File *files, int count, const LRP_Golden *g, uint32_t *start,
                            uint32_t *end, int *line) {
    const char *source = lrp_fixture(files, count, g->file);
    if (!source || !g->occurrence || !g->occurrence[0])
        return false;
    const char *site = source;
    for (unsigned i = 0; i <= g->occurrence_index; i++) {
        site = strstr(site, g->occurrence);
        if (!site)
            return false;
        if (i != g->occurrence_index)
            site++;
    }
    *start = (uint32_t)(site - source);
    *end = *start + (uint32_t)strlen(g->occurrence);
    *line = 1;
    for (const char *p = source; p < site; p++)
        *line += *p == '\n';
    return true;
}

static bool lrp_declaration_source(const LRP_File *files, int count, const LRP_Golden *g) {
    if (!g->declaration_line || !g->declaration_text)
        return true; /* Explicit identity gap, or suffix-only source oracle. */
    const char *source = lrp_fixture(files, count, g->target_file);
    if (!source)
        return false;
    for (int line = 1; line < g->declaration_line; line++) {
        source = strchr(source, '\n');
        if (!source)
            return false;
        source++;
    }
    size_t length = strcspn(source, "\n");
    return strlen(g->declaration_text) == length && !memcmp(source, g->declaration_text, length);
}

static bool lrp_target_matches(const LRP_Golden *g, const char *target_file, const char *target,
                               int first, int last) {
    return g->target && target_file && !strcmp(target_file, g->target_file) &&
           lrp_qn_has_declaration(target, g->target) &&
           (!g->declaration_line || (first <= g->declaration_line && last >= g->declaration_line));
}

/* Rows are handwritten semantic expectations transcribed from fixture source,
 * not selected from resolver output. Missing extraction/identity still enters
 * the denominator. Every tolerated miss carries its own WHY and TRIED. */
enum { LRP_GOLDEN_COUNT = sizeof(LRP_GOLDEN) / sizeof(LRP_GOLDEN[0]) };
typedef struct {
    const char *scenario;
    bool recovered[LRP_GOLDEN_COUNT], pp_valid[LRP_GOLDEN_COUNT];
    uint32_t pp_starts[LRP_GOLDEN_COUNT], pp_ends[LRP_GOLDEN_COUNT];
    uint32_t starts[LRP_GOLDEN_COUNT], ends[LRP_GOLDEN_COUNT];
    int lines[LRP_GOLDEN_COUNT];
    int genuine, false_positives, projection_errors;
} LRP_Check;

static bool lrp_prepare_oracle(LRP_Check *check, const LRP_File *files, int nfiles) {
    bool ok = true;
    int selected = 0;
    for (size_t i = 0; i < LRP_GOLDEN_COUNT; i++) {
        const LRP_Golden *g = &LRP_GOLDEN[i];
        if (strcmp(g->scenario, check->scenario))
            continue;
        selected++;
        check->genuine += !g->forbidden;
        if (!lrp_golden_site(files, nfiles, g, &check->starts[i], &check->ends[i],
                             &check->lines[i]) ||
            !lrp_declaration_source(files, nfiles, g) ||
            (!g->required && (!g->quality_issue || !g->tried))) {
            fprintf(stderr, "    [LRP] %s invalid independent oracle at %s:%s\n", check->scenario,
                    g->file, g->occurrence);
            ok = false;
        }
    }
    return ok && selected;
}

/* The indexer's sequential and parallel extractors both pass the relative
 * filename and NULL defines/include paths. Use that same canonical transform,
 * not resolver output, to locate our independently labelled raw occurrence.
 * Snapshot equality makes a future extraction-config change fail explicitly. */
static bool lrp_source_snapshot(cbm_store_t *store, const char *project, const LRP_File *file,
                                const CBMPreprocessedSource *pp) {
    const char *sql = "SELECT raw_content_identity,raw_length,preprocessed_content_identity,"
                      "preprocessed_length FROM resolver_file_summaries "
                      "WHERE project=?1 AND file_path=?2";
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &stmt, NULL) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 2, file->name, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    char raw_hash[CBM_SHA256_HEX_LEN + 1], expanded_hash[CBM_SHA256_HEX_LEN + 1] = {0};
    size_t raw_length = strlen(file->content), expanded_length = pp ? strlen(pp->source) : 0;
    cbm_sha256_hex(file->content, raw_length, raw_hash);
    if (pp)
        cbm_sha256_hex(pp->source, expanded_length, expanded_hash);
    if (ok && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *raw = (const char *)sqlite3_column_text(stmt, 0);
        const char *expanded = (const char *)sqlite3_column_text(stmt, 2);
        ok = raw && expanded && !strcmp(raw, raw_hash) && !strcmp(expanded, expanded_hash) &&
             sqlite3_column_int64(stmt, 1) == (sqlite3_int64)raw_length &&
             sqlite3_column_int64(stmt, 3) == (sqlite3_int64)expanded_length &&
             sqlite3_step(stmt) == SQLITE_DONE;
    } else {
        ok = false;
    }
    sqlite3_finalize(stmt);
    if (!ok)
        fprintf(stderr, "    [LRP] ORACLE_CONFIG_MISMATCH %s raw/expanded snapshot differs\n",
                file->name);
    return ok;
}

static bool lrp_map_expanded(LRP_Check *check, size_t index, const CBMPreprocessedSource *pp) {
    const LRP_Golden *g = &LRP_GOLDEN[index];
    size_t length = strlen(g->occurrence);
    const char *site = pp->source;
    int matches = 0;
    while ((site = strstr(site, g->occurrence)) != NULL) {
        int first = 1, last = 1;
        for (const char *p = pp->source; p < site; p++)
            first += *p == '\n';
        last = first;
        for (size_t i = 0; i < length; i++)
            last += site[i] == '\n';
        int raw_last = check->lines[index] + last - first;
        bool owned = first <= pp->expanded_line_count && last <= pp->expanded_line_count;
        for (int line = first; owned && line <= last; line++)
            owned = pp->belongs_to_main_file[line] &&
                    pp->original_line_by_expanded_line[line] ==
                        (uint32_t)(check->lines[index] + line - first);
        if (owned && pp->original_line_by_expanded_line[last] == (uint32_t)raw_last) {
            check->pp_starts[index] = (uint32_t)(site - pp->source);
            check->pp_ends[index] = check->pp_starts[index] + (uint32_t)length;
            matches++;
        }
        site++;
    }
    if (matches != 1) {
        fprintf(stderr, "    [LRP] %s ORACLE_EXPANDED_SITE %s:%d text=%s matches=%d\n",
                check->scenario, g->file, check->lines[index], g->occurrence, matches);
        return false;
    }
    check->pp_valid[index] = true;
    return true;
}

static bool lrp_prepare_expanded(cbm_store_t *store, const char *project, LRP_Check *check,
                                 const LRP_File *files, int nfiles) {
    bool ok = true;
    for (int f = 0; f < nfiles; f++) {
        const char *ext = strrchr(files[f].name, '.');
        bool cpp = ext && (!strcmp(ext, ".cpp") || !strcmp(ext, ".hpp"));
        if (!cpp && (!ext || strcmp(ext, ".c")))
            continue;
        bool selected = false;
        for (size_t i = 0; i < LRP_GOLDEN_COUNT; i++)
            selected |= !strcmp(LRP_GOLDEN[i].scenario, check->scenario) &&
                        !strcmp(LRP_GOLDEN[i].file, files[f].name);
        if (!selected)
            continue;
        CBMPreprocessedSource *pp = cbm_preprocess_with_map(
            files[f].content, (int)strlen(files[f].content), files[f].name, NULL, NULL, cpp);
        bool snapshot_ok = lrp_source_snapshot(store, project, &files[f], pp);
        ok = snapshot_ok && ok;
        for (size_t i = 0; snapshot_ok && pp && i < LRP_GOLDEN_COUNT; i++)
            if (!strcmp(LRP_GOLDEN[i].scenario, check->scenario) &&
                !strcmp(LRP_GOLDEN[i].file, files[f].name))
                ok = lrp_map_expanded(check, i, pp) && ok;
        cbm_preprocessed_source_free(pp);
    }
    return ok;
}

static bool lrp_same_site(const LRP_Check *check, size_t i, uint32_t start, uint32_t end,
                          int origin) {
    if (origin == CBM_SOURCE_ORIGIN_RAW)
        return check->starts[i] == start && check->ends[i] == end;
    return origin == CBM_SOURCE_ORIGIN_PREPROCESSED && check->pp_valid[i] &&
           check->pp_starts[i] == start && check->pp_ends[i] == end;
}

/* Do not prefilter by the expected span, scope or role: a shifted/rebound
 * occurrence must fail rather than become a tolerated missing edge. */
static bool lrp_match_occurrence(LRP_Check *check, sqlite3_stmt *stmt) {
    const char *scenario = check->scenario;
    bool ok = true;
    const char *file = (const char *)sqlite3_column_text(stmt, 0);
    const char *scope = (const char *)sqlite3_column_text(stmt, 1);
    const char *relation = (const char *)sqlite3_column_text(stmt, 2);
    uint32_t start = (uint32_t)sqlite3_column_int64(stmt, 3);
    uint32_t end = (uint32_t)sqlite3_column_int64(stmt, 4);
    const char *target = (const char *)sqlite3_column_text(stmt, 6);
    const char *target_file = (const char *)sqlite3_column_text(stmt, 7);
    const char *source_file = (const char *)sqlite3_column_text(stmt, 10);
    bool covered = false, matched = false;
    for (size_t i = 0; i < LRP_GOLDEN_COUNT; i++) {
        const LRP_Golden *g = &LRP_GOLDEN[i];
        if (strcmp(g->scenario, scenario) || strcmp(g->file, file))
            continue;
        bool same_site = lrp_same_site(check, i, start, end, sqlite3_column_int(stmt, 5));
        if (!strcmp(g->relation, relation) || same_site ||
            (!strcmp(g->relation, "CALLS") && !strcmp(relation, "CALL_REFERENCE")))
            covered = true;
        if (!same_site || strcmp(g->relation, relation))
            continue;
        bool identity = source_file && !strcmp(source_file, g->file) &&
                        lrp_qn_has_declaration(scope, g->caller) &&
                        sqlite3_column_int(stmt, 11) <= check->lines[i] &&
                        sqlite3_column_int(stmt, 12) >= check->lines[i] &&
                        lrp_target_matches(g, target_file, target, sqlite3_column_int(stmt, 8),
                                           sqlite3_column_int(stmt, 9));
        if (identity && g->forbidden) {
            fprintf(stderr, "    [LRP] %s forbidden target %s -> %s\n", scenario, scope, target);
            ok = false;
        }
        if (identity && !g->forbidden && sqlite3_column_int(stmt, 13)) {
            check->recovered[i] = true;
            matched = true;
        }
    }
    if (covered && !matched) {
        check->false_positives++;
        fprintf(stderr, "    [LRP] %s FALSE_POSITIVE_OR_PROJECTION %s:%u-%u %s %s -> %s\n",
                scenario, file, start, end, relation, scope, target);
        ok = false;
    }
    return ok;
}

/* A deduplicated edge needs at least one exact admitted golden occurrence. */
static bool lrp_match_projection(LRP_Check *check, sqlite3_stmt *stmt) {
    const char *scenario = check->scenario;
    bool ok = true;
    const char *file = (const char *)sqlite3_column_text(stmt, 0);
    const char *scope = (const char *)sqlite3_column_text(stmt, 1);
    const char *target_file = (const char *)sqlite3_column_text(stmt, 2);
    const char *target = (const char *)sqlite3_column_text(stmt, 3);
    const char *relation = (const char *)sqlite3_column_text(stmt, 4);
    bool covered = false, matched = false;
    for (size_t i = 0; i < LRP_GOLDEN_COUNT; i++) {
        const LRP_Golden *g = &LRP_GOLDEN[i];
        if (strcmp(g->scenario, scenario) || strcmp(g->file, file))
            continue;
        if (!strcmp(g->relation, relation) ||
            (!strcmp(g->relation, "CALLS") && !strcmp(relation, "CALL_REFERENCE")))
            covered = true;
        if (strcmp(g->relation, relation))
            continue;
        bool identity = lrp_qn_has_declaration(scope, g->caller) &&
                        lrp_target_matches(g, target_file, target, sqlite3_column_int(stmt, 5),
                                           sqlite3_column_int(stmt, 6));
        if (identity && g->forbidden)
            ok = false;
        if (identity && !g->forbidden && check->recovered[i] && sqlite3_column_int(stmt, 7))
            matched = true;
    }
    if (covered && !matched) {
        check->projection_errors++;
        fprintf(stderr, "    [LRP] %s UNATTESTED_GRAPH_EDGE %s %s -> %s\n", scenario, relation,
                scope, target);
        ok = false;
    }
    return ok;
}

static bool lrp_query_oracle(cbm_store_t *store, const char *project, LRP_Check *check,
                             const char *sql, bool projection) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &stmt, NULL) != SQLITE_OK ||
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return false;
    }
    bool ok = true;
    int step;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        bool row_ok =
            projection ? lrp_match_projection(check, stmt) : lrp_match_occurrence(check, stmt);
        ok = row_ok && ok;
    }
    ok = ok && step == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return ok;
}

static bool lrp_report_oracle(const LRP_Check *check) {
    bool ok = true;
    int found = 0;
    for (size_t i = 0; i < LRP_GOLDEN_COUNT; i++) {
        const LRP_Golden *g = &LRP_GOLDEN[i];
        if (strcmp(g->scenario, check->scenario) || g->forbidden)
            continue;
        found += check->recovered[i];
        if (!check->recovered[i]) {
            fprintf(stderr, "    [LRP] %s QUALITY_MISS %s:%u-%u %s -> %s:%s; WHY=%s; TRIED=%s\n",
                    check->scenario, g->file, check->starts[i], check->ends[i], g->relation,
                    g->target_file, g->target ? g->target : "<unmaterialized identity>",
                    g->quality_issue ? g->quality_issue : "required supported relationship",
                    g->tried ? g->tried : "exact source occurrence and graph projection");
            if (g->required)
                ok = false;
        }
    }
    fprintf(stderr,
            "    [LRP] %s QUALITY genuine=%d admitted=%d missing=%d false_positive=%d "
            "projection_errors=%d\n",
            check->scenario, check->genuine, found, check->genuine - found, check->false_positives,
            check->projection_errors);
    return ok;
}

static bool lrp_check_oracle(cbm_store_t *store, const char *project, const LRP_File *files,
                             int nfiles, const char *scenario) {
    LRP_Check check = {.scenario = scenario};
    if (!store || !lrp_prepare_oracle(&check, files, nfiles) ||
        !lrp_prepare_expanded(store, project, &check, files, nfiles))
        return false;
    const char *occurrences =
        "SELECT r.file_path,r.scope,r.relation,r.start_byte,r.end_byte,r.source_origin,"
        "r.target,t.file_path,t.start_line,t.end_line,s.file_path,s.start_line,s.end_line,"
        "EXISTS(SELECT 1 FROM edges e WHERE e.project=r.project AND e.source_id=s.id "
        "AND e.target_id=t.id AND e.type=r.relation) "
        "FROM resolver_occurrences r LEFT JOIN nodes s ON s.project=r.project "
        "AND s.qualified_name=r.scope LEFT JOIN nodes t ON t.project=r.project "
        "AND t.qualified_name=r.target WHERE r.project=?1 AND r.emitted=1";
    const char *projections =
        "SELECT s.file_path,s.qualified_name,t.file_path,t.qualified_name,e.type,"
        "t.start_line,t.end_line,EXISTS(SELECT 1 FROM resolver_occurrences r "
        "WHERE r.project=e.project AND r.emitted=1 AND r.scope=s.qualified_name "
        "AND r.target=t.qualified_name AND r.relation=e.type) "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1";
    bool ok = lrp_query_oracle(store, project, &check, occurrences, false);
    ok = lrp_query_oracle(store, project, &check, projections, true) && ok;
    return lrp_report_oracle(&check) && ok;
}

static bool lrp_assert_golden(const LRP_File *files, int nfiles, const char *scenario) {
    LRP_Proj lp;
    cbm_store_t *store = lrp_index(&lp, files, nfiles);
    bool ok = lrp_check_oracle(store, lp.project, files, nfiles, scenario);
    if (!ok)
        lrp_diag(store, lp.project, scenario);
    lrp_cleanup(&lp, store);
    return ok;
}

static int lrp_exact_edge_by_qn_suffix(cbm_store_t *store, const char *project,
                                       const char *edge_type, const char *source_suffix,
                                       const char *target_suffix) {
    cbm_node_t *sources = NULL;
    cbm_node_t *targets = NULL;
    int source_count = 0;
    int target_count = 0;
    if (!store ||
        cbm_store_find_nodes_by_qn_suffix(store, project, source_suffix, &sources, &source_count) !=
            CBM_STORE_OK ||
        cbm_store_find_nodes_by_qn_suffix(store, project, target_suffix, &targets, &target_count) !=
            CBM_STORE_OK ||
        source_count != 1 || target_count != 1) {
        cbm_store_free_nodes(sources, source_count);
        cbm_store_free_nodes(targets, target_count);
        return -1;
    }
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    int matches = -1;
    if (cbm_store_find_edges_by_source_type(store, sources[0].id, edge_type, &edges, &edge_count) ==
        CBM_STORE_OK) {
        matches = 0;
        for (int i = 0; i < edge_count; i++) {
            if (edges[i].target_id == targets[0].id) {
                matches++;
            }
        }
    }
    cbm_store_free_edges(edges, edge_count);
    cbm_store_free_nodes(sources, source_count);
    cbm_store_free_nodes(targets, target_count);
    return matches;
}

/* Group 1: GO. Per-occurrence source oracle probes. */

/* S1 — Go cross-file plain function call. */
TEST(lrp_go_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.go", "package app\n\nfunc Double(x int) int { return x * 2 }\n"},
        {"main.go", "package app\n\nfunc Run(n int) int { return Double(n) }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s1_crossfile_call"));
    PASS();
}

/* S2 — Go method dispatch via receiver type (cross-file). */
TEST(lrp_go_s2_method_dispatch) {
    /* Counter is defined in counter.go; Run() in runner.go gets a Counter and
     * calls its Inc() method.  The lsp_cross pass sees Counter's type and
     * resolves the method call to Counter.Inc. */
    static const LRP_File f[] = {{"counter.go", "package app\n\ntype Counter struct{ n int }\n\n"
                                                "func (c *Counter) Inc() { c.n++ }\n\n"
                                                "func (c *Counter) Value() int { return c.n }\n"},
                                 {"runner.go", "package app\n\nfunc Run(c *Counter) int {\n"
                                               "    c.Inc()\n    return c.Value()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s2_method_dispatch"));
    PASS();
}

/* S3 — Go constructor / struct literal instantiation. */
TEST(lrp_go_s3_constructor) {
    /* NewCounter() is an ordinary function call. Its struct literal is a
     * separate type usage, covered by the dedicated usage probe. */
    static const LRP_File f[] = {
        {"counter.go",
         "package app\n\ntype Counter struct{ start int }\n\n"
         "func NewCounter(start int) *Counter {\n    return &Counter{start: start}\n}\n"},
        {"main.go", "package app\n\nfunc Make(n int) *Counter {\n    return NewCounter(n)\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s3_constructor"));
    PASS();
}

/* S4 — Go package-qualified call through the declared project module path. */
TEST(lrp_go_s4_static_call) {
    static const LRP_File f[] = {
        {"go.mod", "module example.com/probe\n\ngo 1.22\n"},
        {"math/ops.go", "package math\n\nfunc Square(x int) int { return x * x }\n"},
        {"main.go", "package main\n\nimport \"example.com/probe/math\"\n\n"
                    "func Run(n int) int { return math.Square(n) }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s4_static_call"));
    PASS();
}

/* An import of standard-library math cannot resolve to a same-named project
 * directory. Square is intentionally absent from the actual imported package. */
TEST(lrp_go_stdlib_import_does_not_bind_local_directory) {
    static const LRP_File f[] = {
        {"go.mod", "module example.com/probe\n\ngo 1.22\n"},
        {"math/ops.go", "package math\n\nfunc Square(x int) int { return x * x }\n"},
        {"main.go", "package main\n\nimport \"math\"\n\n"
                    "func Run(n int) int { return math.Square(n) }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])),
                                  "lrp_go_stdlib_import_does_not_bind_local_directory"));
    PASS();
}

/* S5 — Go chained call (a.b().c()). */
TEST(lrp_go_s5_chained_call) {
    /* builder.go defines a Builder with two chainable methods; main.go chains them.
     * The second call (.Build()) is resolved via the return type of .Add() → *Builder. */
    static const LRP_File f[] = {{"builder.go",
                                  "package app\n\ntype Builder struct{ items []string }\n\n"
                                  "func NewBuilder() *Builder { return &Builder{} }\n\n"
                                  "func (b *Builder) Add(s string) *Builder {\n    b.items = "
                                  "append(b.items, s)\n    return b\n}\n\n"
                                  "func (b *Builder) Build() []string { return b.items }\n"},
                                 {"main.go", "package app\n\nfunc Make() []string {\n"
                                             "    return NewBuilder().Add(\"x\").Build()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s5_chained_call"));
    PASS();
}

/* S6 — Go embedded-struct method call (Go's "inheritance"). */
TEST(lrp_go_s6_inherited_method) {
    /* Dog embeds Animal.  Calling dog.Speak() should resolve to Animal.Speak. */
    static const LRP_File f[] = {{"animal.go",
                                  "package zoo\n\ntype Animal struct{ Name string }\n\n"
                                  "func (a *Animal) Speak() string { return a.Name }\n"},
                                 {"dog.go", "package zoo\n\ntype Dog struct{ Animal }\n\n"
                                            "func Run(d *Dog) string { return d.Speak() }\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s6_inherited_method"));
    PASS();
}

/* S7 — Go generic function call (Go 1.18 generics). */
TEST(lrp_go_s7_generic_call) {
    /* Map is a generic function in util.go; called from main.go with a concrete type. */
    static const LRP_File f[] = {
        {"util.go",
         "package app\n\nfunc Map[T, U any](xs []T, fn func(T) U) []U {\n"
         "    out := make([]U, len(xs))\n"
         "    for i, x := range xs {\n        out[i] = fn(x)\n    }\n    return out\n}\n"},
        {"main.go", "package app\n\nfunc Double(xs []int) []int {\n"
                    "    return Map(xs, func(x int) int { return x * 2 })\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s7_generic_call"));
    PASS();
}

/* S8 — Go field-type-hint (this.field.method()). */
TEST(lrp_go_s8_field_type_hint) {
    /* Service has a field `repo *Repo`; its methods call repo.Find(). */
    static const LRP_File f[] = {
        {"repo.go", "package app\n\ntype Repo struct{}\n\n"
                    "func (r *Repo) Find(id int) string { return \"\" }\n"},
        {"service.go", "package app\n\ntype Service struct{ repo *Repo }\n\n"
                       "func (s *Service) Get(id int) string { return s.repo.Find(id) }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_s8_field_type_hint"));
    PASS();
}

/* Group 2: C. Per-occurrence source oracle probes. */

/* S1 — C cross-file plain function call. */
TEST(lrp_c_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.c", "int add(int a, int b) { return a + b; }\n"},
        {"main.c", "int add(int a, int b);\n\nint run(int x) { return add(x, 1); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s1_crossfile_call"));
    PASS();
}

/* S2 — C method-like call via function pointer in struct. */
TEST(lrp_c_s2_funcptr_in_struct) {
    /* The C lsp_cross should detect obj.method() where method is a function
     * pointer field with a known type.  This is the C approximation of S2. */
    static const LRP_File f[] = {
        {"ops.c",
         "typedef struct { int (*compute)(int); } Ops;\n\n"
         "static int double_it(int x) { return x * 2; }\n\n"
         "Ops make_ops(void) {\n    Ops o;\n    o.compute = double_it;\n    return o;\n}\n"},
        {"main.c", "typedef struct { int (*compute)(int); } Ops;\n"
                   "Ops make_ops(void);\n\n"
                   "int run(int n) {\n    Ops o = make_ops();\n    return o.compute(n);\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s2_funcptr_in_struct"));
    PASS();
}

/* S3 — C "constructor" pattern (factory function returns pointer). */
TEST(lrp_c_s3_constructor) {
    static const LRP_File f[] = {{"buf.c",
                                  "#include <stdlib.h>\n"
                                  "typedef struct { int cap; int *data; } Buf;\n\n"
                                  "Buf *buf_new(int cap) {\n    Buf *b = malloc(sizeof(Buf));\n"
                                  "    b->cap = cap;\n    return b;\n}\n"},
                                 {"main.c", "typedef struct { int cap; int *data; } Buf;\n"
                                            "Buf *buf_new(int cap);\n\n"
                                            "Buf *make_buf(int n) { return buf_new(n); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s3_constructor"));
    PASS();
}

/* S4 — C "static" pattern: file-scoped helper called across TUs. */
TEST(lrp_c_s4_static_local) {
    /* Note: `static` in C means file-scoped — a static helper in util.c
     * should NOT be resolvable from main.c.  This probes that the resolver
     * does NOT create a spurious cross-file CALLS edge for a static function.
     * Correct outcome: calls == 0 from main.c to helper (it's not exported). */
    static const LRP_File f[] = {
        {"util.c", "static int helper(int x) { return x + 1; }\n\n"
                   "int exported(int x) { return helper(x); }\n"},
        {"main.c", "int exported(int x);\n\nint run(int n) { return exported(n); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s4_static_local"));
    PASS();
}

/* S5 — C chained dereference call (ptr->field->method). */
TEST(lrp_c_s5_chained_deref) {
    static const LRP_File f[] = {
        {"node.c", "typedef struct Node { int val; struct Node *next; } Node;\n\n"
                   "int node_val(const Node *n) { return n->val; }\n"},
        {"main.c", "typedef struct Node { int val; struct Node *next; } Node;\n"
                   "int node_val(const Node *n);\n\n"
                   "int run(Node *head) { return node_val(head->next); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s5_chained_deref"));
    PASS();
}

/* S6 — C "inheritance" pattern: base struct embedded at start. */
TEST(lrp_c_s6_base_method) {
    static const LRP_File f[] = {
        {"shape.c", "typedef struct { int color; } Shape;\n\n"
                    "int shape_color(const Shape *s) { return s->color; }\n"},
        {"circle.c", "typedef struct { int color; } Shape;\n"
                     "typedef struct { Shape base; float radius; } Circle;\n"
                     "int shape_color(const Shape *s);\n\n"
                     "int run(Circle *c) { return shape_color(&c->base); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s6_base_method"));
    PASS();
}

/* S7 — C generic callback dispatch.
 * FIXTURE FIX: the previous fixture only ASSIGNED int_cmp to a function pointer
 * and then `(void)fn;` — it never invoked the callback, so there was no call to
 * resolve (only a USAGE for the value reference).  We now (a) invoke the callback
 * through the pointer AND (b) call int_cmp directly cross-file (the same plain
 * cross-file call shape that c/S1 resolves), so a CALLS edge is genuinely
 * exercised. */
TEST(lrp_c_s7_generic_callback) {
    static const LRP_File f[] = {{"compare.c", "int int_cmp(const void *a, const void *b) {\n"
                                               "    return *(const int*)a - *(const int*)b;\n}\n"},
                                 {"sort.c", "typedef int (*CmpFn)(const void *, const void *);\n"
                                            "int int_cmp(const void *a, const void *b);\n\n"
                                            "int my_sort(int *arr, int n) {\n"
                                            "    CmpFn fn = int_cmp;\n"
                                            "    int direct = int_cmp(&arr[0], &arr[1]);\n"
                                            "    return direct + fn(&arr[0], &arr[1]);\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s7_generic_callback"));
    PASS();
}

/* S8 — C field-type-hint: accessing a known struct member's address. */
TEST(lrp_c_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.c", "typedef struct { int level; } Logger;\n\n"
                     "void log_msg(const Logger *l, const char *msg) { (void)l; (void)msg; }\n"},
        {"service.c", "typedef struct { int level; } Logger;\n"
                      "typedef struct { Logger logger; int id; } Service;\n"
                      "void log_msg(const Logger *l, const char *msg);\n\n"
                      "void run(Service *svc, const char *m) { log_msg(&svc->logger, m); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_c_s8_field_call"));
    PASS();
}

/* Group 3: C++. Per-occurrence source oracle probes. */

/* S1 — C++ cross-file plain function call. */
TEST(lrp_cpp_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.cpp", "int square(int x) { return x * x; }\n"},
        {"main.cpp", "int square(int x);\n\nint run(int n) { return square(n); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s1_crossfile_call"));
    PASS();
}

/* S2 — C++ method dispatch on object (non-virtual). */
TEST(lrp_cpp_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.hpp", "class Counter {\npublic:\n"
                        "    int n;\n    Counter() : n(0) {}\n"
                        "    void inc() { n++; }\n    int val() const { return n; }\n};\n"},
        {"main.cpp", "#include \"counter.hpp\"\n\n"
                     "int run() {\n    Counter c;\n    c.inc();\n    return c.val();\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s2_method_dispatch"));
    PASS();
}

/* S3 — C++ constructor call. */
TEST(lrp_cpp_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.hpp", "class Widget {\npublic:\n    int id;\n"
                       "    Widget(int id) : id(id) {}\n"
                       "    int get_id() const { return id; }\n};\n"},
        {"main.cpp", "#include \"widget.hpp\"\n\n"
                     "int run(int n) {\n    Widget w(n);\n    return w.get_id();\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s3_constructor"));
    PASS();
}

/* S4 — C++ static method call (Class::method). */
TEST(lrp_cpp_s4_static_method) {
    static const LRP_File f[] = {{"registry.hpp", "class Registry {\npublic:\n"
                                                  "    static int count() { return 42; }\n};\n"},
                                 {"main.cpp", "#include \"registry.hpp\"\n\n"
                                              "int run() { return Registry::count(); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s4_static_method"));
    PASS();
}

/* S5 — C++ chained method call. */
TEST(lrp_cpp_s5_chained) {
    static const LRP_File f[] = {{"builder.hpp",
                                  "class Builder {\npublic:\n"
                                  "    Builder& add(int x) { (void)x; return *this; }\n"
                                  "    int build() { return 1; }\n};\n"},
                                 {"main.cpp", "#include \"builder.hpp\"\n\n"
                                              "int run() { return Builder().add(1).build(); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s5_chained"));
    PASS();
}

/* S6 — C++ virtual inherited method call. */
TEST(lrp_cpp_s6_virtual_inherited) {
    static const LRP_File f[] = {
        {"shape.hpp", "class Shape {\npublic:\n    virtual int area() const { return 0; }\n};\n"},
        {"circle.cpp", "#include \"shape.hpp\"\n\n"
                       "class Circle : public Shape {\npublic:\n"
                       "    int r;\n    Circle(int r) : r(r) {}\n"
                       "    int area() const override { return r * r * 3; }\n"
                       "    int run() const { return Shape::area(); }\n};\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s6_virtual_inherited"));
    PASS();
}

/* S7 — C++ template function call. */
TEST(lrp_cpp_s7_template) {
    static const LRP_File f[] = {
        {"algo.hpp", "template<typename T>\nT clamp(T v, T lo, T hi) {\n"
                     "    if (v < lo) return lo;\n    if (v > hi) return hi;\n    return v;\n}\n"},
        {"main.cpp", "#include \"algo.hpp\"\n\n"
                     "int run(int n) { return clamp(n, 0, 100); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s7_template"));
    PASS();
}

/* S8 — C++ member field method call. */
TEST(lrp_cpp_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.hpp",
         "class Logger {\npublic:\n    void log(const char *msg) { (void)msg; }\n};\n"},
        {"service.cpp", "#include \"logger.hpp\"\n\n"
                        "class Service {\n    Logger logger;\npublic:\n"
                        "    void run(const char *m) { logger.log(m); }\n};\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_cpp_s8_field_call"));
    PASS();
}

/* Group 4: RUST. Per-occurrence source oracle probes. */

/* S1 — Rust module-qualified source function call. */
TEST(lrp_rust_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"lib.rs", "pub fn square(x: i32) -> i32 { x * x }\n"},
        {"main.rs", "mod lib;\n\nfn run(n: i32) -> i32 { lib::square(n) }\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s1_crossfile_call"));
    PASS();
}

/* S2 — Rust method dispatch via impl block (receiver type). */
TEST(lrp_rust_s2_method_dispatch) {
    /* Counter is in counter.rs; run() in runner.rs calls c.inc() and c.value().
     * Without lsp_cross, the receiver type Counter is unknown in runner.rs,
     * so the method calls may not resolve to the correct impl methods. */
    static const LRP_File f[] = {{"counter.rs", "pub struct Counter { n: i32 }\n\n"
                                                "impl Counter {\n"
                                                "    pub fn new(n: i32) -> Self { Counter { n } }\n"
                                                "    pub fn inc(&mut self) { self.n += 1; }\n"
                                                "    pub fn value(&self) -> i32 { self.n }\n}\n"},
                                 {"runner.rs",
                                  "mod counter;\n\nfn run(c: &mut counter::Counter) -> i32 {\n"
                                  "    c.inc();\n    c.value()\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s2_method_dispatch"));
    PASS();
}

/* S3 — Rust constructor (associated fn new). */
TEST(lrp_rust_s3_constructor) {
    static const LRP_File f[] = {
        {"point.rs",
         "pub struct Point { pub x: f64, pub y: f64 }\n\n"
         "impl Point {\n    pub fn new(x: f64, y: f64) -> Self { Point { x, y } }\n"
         "    pub fn dist(&self) -> f64 { (self.x * self.x + self.y * self.y).sqrt() }\n}\n"},
        {"main.rs", "mod point;\n\nfn run() -> f64 {\n"
                    "    let p = point::Point::new(3.0, 4.0);\n    p.dist()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s3_constructor"));
    PASS();
}

/* S4 — Rust static / associated method call (Type::method). */
TEST(lrp_rust_s4_static_method) {
    static const LRP_File f[] = {
        {"config.rs", "pub struct Config { pub debug: bool }\n\n"
                      "impl Config {\n    pub fn default() -> Self { Config { debug: false } }\n"
                      "    pub fn is_debug(&self) -> bool { self.debug }\n}\n"},
        {"app.rs", "mod config;\n\nfn run() -> bool {\n"
                   "    let cfg = config::Config::default();\n    cfg.is_debug()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s4_static_method"));
    PASS();
}

/* S5 — Rust chained method call. */
TEST(lrp_rust_s5_chained) {
    static const LRP_File f[] = {
        {"builder.rs", "pub struct Builder { items: Vec<i32> }\n\n"
                       "impl Builder {\n"
                       "    pub fn new() -> Self { Builder { items: vec![] } }\n"
                       "    pub fn add(mut self, x: i32) -> Self { self.items.push(x); self }\n"
                       "    pub fn build(self) -> Vec<i32> { self.items }\n}\n"},
        {"main.rs", "mod builder;\n\nfn run() -> Vec<i32> {\n"
                    "    builder::Builder::new().add(1).add(2).build()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s5_chained"));
    PASS();
}

/* S6 — Rust trait method call on concrete type. */
TEST(lrp_rust_s6_trait_method) {
    /* Trait Display is defined in display.rs; Dog implements it in dog.rs.
     * run() in main.rs calls d.show() on a Dog where show() comes from the trait. */
    static const LRP_File f[] = {
        {"display.rs", "pub trait Display {\n    fn show(&self) -> String;\n}\n"},
        {"dog.rs", "pub struct Dog { pub name: String }\n\n"
                   "impl crate::display::Display for Dog {\n"
                   "    fn show(&self) -> String { self.name.clone() }\n}\n"},
        {"main.rs", "mod display;\nmod dog;\nuse crate::display::Display;\n\n"
                    "fn run(d: &dog::Dog) -> String {\n    d.show()\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s6_trait_method"));
    PASS();
}

/* S7 — Rust generic function call. */
TEST(lrp_rust_s7_generic) {
    static const LRP_File f[] = {
        {"algo.rs", "pub fn max_of<T: PartialOrd>(a: T, b: T) -> T {\n"
                    "    if a > b { a } else { b }\n}\n"},
        {"main.rs", "mod algo;\n\nfn run(x: i32, y: i32) -> i32 {\n    algo::max_of(x, y)\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s7_generic"));
    PASS();
}

/* S8 — Rust field method call. */
TEST(lrp_rust_s8_field_call) {
    static const LRP_File f[] = {
        {"logger.rs", "pub struct Logger;\n\nimpl Logger {\n"
                      "    pub fn log(&self, msg: &str) { let _ = msg; }\n}\n"},
        {"service.rs", "mod logger;\n\npub struct Service { pub logger: logger::Logger }\n\n"
                       "impl Service {\n"
                       "    pub fn run(&self, msg: &str) { self.logger.log(msg); }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_rust_s8_field_call"));
    PASS();
}

/* Cross-file call hidden inside format!'s token tree. The visible() control
 * proves the target and ordinary cross-file resolution are present; hidden()
 * requires the Rust LSP's semantic result to be paired with a synthetic
 * CBMCall carrier before pass_calls can materialize the exact graph edge. */
TEST(lrp_rust_crossfile_macro_hidden_call_carrier) {
    static const LRP_File f[] = {
        {"lib.rs", "pub fn render() -> &'static str { \"ok\" }\n"},
        {"main.rs", "mod lib;\n\n"
                    "pub fn visible() -> &'static str { lib::render() }\n\n"
                    "pub fn hidden() -> String { format!(\"{}\", lib::render()) }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])),
                                  "lrp_rust_crossfile_macro_hidden_call_carrier"));
    PASS();
}

/* Group 5: PYTHON. Per-occurrence source oracle probes. */

/* S1 — Python cross-file plain function call. */
TEST(lrp_python_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.py", "def triple(x):\n    return x * 3\n"},
        {"main.py", "from .util import triple\n\n\ndef run(n):\n    return triple(n)\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s1_crossfile_call"));
    PASS();
}

/* S2 — Python method dispatch via known class. */
TEST(lrp_python_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.py", "class Counter:\n"
                       "    def __init__(self):\n        self.n = 0\n\n"
                       "    def inc(self):\n        self.n += 1\n\n"
                       "    def value(self):\n        return self.n\n"},
        {"main.py", "from .counter import Counter\n\n\n"
                    "def run():\n    c = Counter()\n    c.inc()\n    return c.value()\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s2_method_dispatch"));
    PASS();
}

/* S3 — Python constructor call (class instantiation). */
TEST(lrp_python_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.py", "class Widget:\n    def __init__(self, name):\n        self.name = name\n\n"
                      "    def label(self):\n        return self.name\n"},
        {"main.py", "from .widget import Widget\n\n\ndef make(name):\n    return Widget(name)\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s3_constructor"));
    PASS();
}

/* S4 — Python class method call (classmethod / staticmethod). */
TEST(lrp_python_s4_class_method) {
    static const LRP_File f[] = {
        {"factory.py", "class Factory:\n"
                       "    @classmethod\n    def create(cls, n):\n        return cls()\n\n"
                       "    @staticmethod\n    def version():\n        return '1.0'\n"},
        {"main.py", "from .factory import Factory\n\n\ndef run():\n"
                    "    v = Factory.version()\n    return v\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s4_class_method"));
    PASS();
}

/* S5 — Python chained call. */
TEST(lrp_python_s5_chained) {
    static const LRP_File f[] = {
        {"builder.py",
         "class Builder:\n    def __init__(self):\n        self.items = []\n\n"
         "    def add(self, x):\n        self.items.append(x)\n        return self\n\n"
         "    def build(self):\n        return list(self.items)\n"},
        {"main.py", "from .builder import Builder\n\n\ndef make():\n"
                    "    return Builder().add(1).add(2).build()\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s5_chained"));
    PASS();
}

/* S6 — Python inherited method call. */
TEST(lrp_python_s6_inherited_method) {
    static const LRP_File f[] = {
        {"base.py", "class Base:\n    def describe(self):\n        return 'base'\n"},
        {"child.py", "from .base import Base\n\n\nclass Child(Base):\n"
                     "    def extra(self):\n        return 'extra'\n\n\n"
                     "def run(c):\n    return c.describe()\n"}};
    /* MEASURED on main before the guard: strategy=unique_name, cands=1,
     * conf=0.7500. "describe" is the only symbol of that name in this 2-file
     * fixture, so a weak short-name guess happened to be right. In a real repo
     * the same guess binds an arbitrary same-named method (the false edges
     * #1276 targets: accelerator.print() -> MockAccelerator.print), so the
     * Python weak-member guard suppresses member calls whose receiver is
     * unresolved -- here `c`, a bare parameter. c.describe() is the ONLY call
     * in the fixture, so a correctly-suppressed run yields exactly zero CALLS.
     *
     * This is NOT an inheritance case, despite the name, and it has no
     * flip-back condition: `c` is un-annotated, so no receiver type exists to
     * inherit THROUGH. Resolving cross-file base classes cannot recover this
     * edge, because there is nothing to say `c` is a Child. Cross-file
     * inheritance resolution is covered by S6b below, which supplies the
     * receiver type this fixture deliberately withholds.
     *
     * The base_classes extraction bug this comment used to cite is FIXED --
     * extraction_inheritance is green for Python, and base_classes holds clean
     * names with parens, kwargs and generic subscripts already stripped.
     *
     * Tripwire: assert the store opened AND calls == 0 exactly, so an infra/DB
     * failure cannot pass vacuously. */
    LRP_Proj lp;
    cbm_store_t *store = lrp_index(&lp, f, 2);
    ASSERT_NOT_NULL(store);
    int calls = cbm_store_count_edges_by_type(store, lp.project, "CALLS");
    lrp_cleanup(&lp, store);
    ASSERT_EQ(calls, 0);
    PASS();
}

/* S6b — Exact inherited calls for annotated, self and constructed receivers. */
TEST(lrp_python_s6b_inherited_method_typed_receiver) {
    static const LRP_File annotated[] = {
        {"base.py", "class Base:\n    def describe(self):\n        return 'base'\n"},
        {"child.py", "from .base import Base\n\n\nclass Child(Base):\n"
                     "    def extra(self):\n        return 'extra'\n\n\n"
                     "def run(c: Child):\n    return c.describe()\n"}};
    static const LRP_File self_attr[] = {
        {"base.py", "class Base:\n    def describe(self):\n        return 'base'\n"},
        {"child.py", "from .base import Base\n\n\nclass Child(Base):\n"
                     "    def go(self):\n        return self.describe()\n"}};
    static const LRP_File constructed[] = {
        {"base.py", "class Base:\n    def describe(self):\n        return 'base'\n"},
        {"child.py", "from .base import Base\n\n\nclass Child(Base):\n    pass\n\n\n"
                     "def run():\n    return Child().describe()\n"}};
    const LRP_File *shapes[3] = {annotated, self_attr, constructed};
    const char *names[3] = {"lrp_python_s6b_annotated_param", "lrp_python_s6b_self_attr",
                            "lrp_python_s6b_constructor"};
    for (int k = 0; k < 3; k++) {
        LRP_Proj lp;
        cbm_store_t *store = lrp_index(&lp, shapes[k], 2);
        ASSERT_NOT_NULL(store);
        int inherits = cbm_store_count_edges_by_type(store, lp.project, "INHERITS");
        bool ok = lrp_check_oracle(store, lp.project, shapes[k], 2, names[k]);
        if (!ok || inherits < 1)
            lrp_diag(store, lp.project, names[k]);
        lrp_cleanup(&lp, store);
        ASSERT_TRUE(inherits >= 1);
        ASSERT_TRUE(ok);
    }
    PASS();
}

/* S7 — Python type-annotated call (PEP 484 type hint). */
TEST(lrp_python_s7_annotated_call) {
    static const LRP_File f[] = {
        {"repo.py", "class Repo:\n    def find(self, id: int) -> str:\n        return ''\n"},
        {"service.py", "from .repo import Repo\n\n\nclass Service:\n"
                       "    def __init__(self, repo: Repo):\n        self.repo = repo\n\n"
                       "    def get(self, id: int) -> str:\n        return self.repo.find(id)\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s7_annotated_call"));
    PASS();
}

/* S8 — Python field-type-hint (self.field.method()). */
TEST(lrp_python_s8_field_type_hint) {
    /* Service stores a logger field and calls logger.log() in methods. */
    static const LRP_File f[] = {
        {"logger.py", "class Logger:\n    def log(self, msg: str) -> None:\n        pass\n"},
        {"service.py", "from .logger import Logger\n\n\nclass Service:\n"
                       "    def __init__(self):\n        self.logger: Logger = Logger()\n\n"
                       "    def run(self, msg: str):\n        self.logger.log(msg)\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_s8_field_type_hint"));
    PASS();
}

/* A cross-file dunder can resolve semantically only after the project-wide
 * Python registry is available. Because `left + right` is not a parser call,
 * the cross resolver must return an exact synthetic carrier as well as the
 * semantic record or pass_calls has nothing to materialize. */
TEST(lrp_python_crossfile_dunder_carrier) {
    static const LRP_File f[] = {{"alpha.py", "class Alpha:\n"
                                              "    def __add__(self, other):\n"
                                              "        return self\n"},
                                 {"main.py", "from .alpha import Alpha\n\n"
                                             "def combine(left: Alpha, right: Alpha):\n"
                                             "    return left + right\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])),
                                  "lrp_python_crossfile_dunder_carrier"));
    PASS();
}

/* Group 6: TYPESCRIPT. Per-occurrence source oracle probes. */

/* S1 — TypeScript cross-file plain function call. */
TEST(lrp_ts_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.ts", "export function format(s: string): string { return s.trim(); }\n"},
        {"main.ts", "import { format } from './util';\n\n"
                    "export function run(s: string): string { return format(s); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s1_crossfile_call"));
    PASS();
}

/* S2 — TypeScript method dispatch via typed variable. */
TEST(lrp_ts_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"counter.ts",
         "export class Counter {\n    private n = 0;\n"
         "    inc(): void { this.n++; }\n    value(): number { return this.n; }\n}\n"},
        {"main.ts", "import { Counter } from './counter';\n\n"
                    "export function run(): number {\n"
                    "    const c = new Counter();\n    c.inc();\n    return c.value();\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s2_method_dispatch"));
    PASS();
}

/* S3 — TypeScript constructor call. */
TEST(lrp_ts_s3_constructor) {
    static const LRP_File f[] = {
        {"widget.ts", "export class Widget {\n    constructor(public name: string) {}\n"
                      "    label(): string { return this.name; }\n}\n"},
        {"main.ts",
         "import { Widget } from './widget';\n\n"
         "export function make(name: string): Widget {\n    return new Widget(name);\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s3_constructor"));
    PASS();
}

/* S4 — TypeScript static method call. */
TEST(lrp_ts_s4_static_method) {
    static const LRP_File f[] = {
        {"factory.ts", "export class Factory {\n    static version(): string { return '1.0'; }\n"
                       "    static create(): Factory { return new Factory(); }\n}\n"},
        {"main.ts", "import { Factory } from './factory';\n\n"
                    "export function run(): string { return Factory.version(); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s4_static_method"));
    PASS();
}

/* S5 — TypeScript chained method call. */
TEST(lrp_ts_s5_chained) {
    static const LRP_File f[] = {
        {"builder.ts", "export class Builder {\n    private items: number[] = [];\n"
                       "    add(x: number): Builder { this.items.push(x); return this; }\n"
                       "    build(): number[] { return this.items; }\n}\n"},
        {"main.ts", "import { Builder } from './builder';\n\n"
                    "export function run(): number[] {\n"
                    "    return new Builder().add(1).add(2).build();\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s5_chained"));
    PASS();
}

/* S6 — TypeScript inherited method call. */
TEST(lrp_ts_s6_inherited_method) {
    static const LRP_File f[] = {
        {"base.ts", "export class Base {\n    describe(): string { return 'base'; }\n}\n"},
        {"child.ts",
         "import { Base } from './base';\n\n"
         "export class Child extends Base {\n    extra(): string { return 'child'; }\n}\n\n"
         "export function run(c: Child): string { return c.describe(); }\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s6_inherited_method"));
    PASS();
}

/* S7 — TypeScript generic function call. */
TEST(lrp_ts_s7_generic) {
    static const LRP_File f[] = {
        {"algo.ts", "export function maxOf<T>(a: T, b: T, cmp: (x: T, y: T) => number): T {\n"
                    "    return cmp(a, b) >= 0 ? a : b;\n}\n"},
        {"main.ts", "import { maxOf } from './algo';\n\n"
                    "export function run(a: number, b: number): number {\n"
                    "    return maxOf(a, b, (x, y) => x - y);\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s7_generic"));
    PASS();
}

/* S8 — TypeScript field-type-hint (this.field.method()). */
TEST(lrp_ts_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"logger.ts",
         "export class Logger {\n    log(msg: string): void { console.log(msg); }\n}\n"},
        {"service.ts", "import { Logger } from './logger';\n\n"
                       "export class Service {\n    private logger: Logger;\n"
                       "    constructor() { this.logger = new Logger(); }\n"
                       "    run(msg: string): void { this.logger.log(msg); }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_s8_field_type_hint"));
    PASS();
}

/* Group 7: JAVA. Per-occurrence source oracle probes. */

/* S1 — Java cross-file plain method call (same package). */
TEST(lrp_java_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"Util.java",
         "package app;\n\nclass Util {\n    static int square(int x) { return x * x; }\n}\n"},
        {"Main.java",
         "package app;\n\nclass Main {\n    int run(int n) { return Util.square(n); }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s1_crossfile_call"));
    PASS();
}

/* S2 — Java method dispatch on instance (receiver type from field). */
TEST(lrp_java_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.java",
         "package app;\n\nclass Counter {\n    private int n = 0;\n"
         "    public void inc() { n++; }\n    public int value() { return n; }\n}\n"},
        {"Runner.java", "package app;\n\nclass Runner {\n"
                        "    public int run() {\n        Counter c = new Counter();\n"
                        "        c.inc();\n        return c.value();\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s2_method_dispatch"));
    PASS();
}

/* S3 — Java constructor call (new T()). */
TEST(lrp_java_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.java", "package app;\n\nclass Widget {\n    private String name;\n"
                        "    public Widget(String name) { this.name = name; }\n"
                        "    public String label() { return name; }\n}\n"},
        {"Factory.java", "package app;\n\nclass Factory {\n"
                         "    public Widget make(String name) { return new Widget(name); }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s3_constructor"));
    PASS();
}

/* S4 — Java static method call (Class.staticMethod). */
TEST(lrp_java_s4_static_method) {
    static const LRP_File f[] = {
        {"MathUtil.java", "package app;\n\nclass MathUtil {\n"
                          "    public static int clamp(int v, int lo, int hi) {\n"
                          "        return v < lo ? lo : v > hi ? hi : v;\n    }\n}\n"},
        {"App.java", "package app;\n\nclass App {\n"
                     "    public int run(int n) { return MathUtil.clamp(n, 0, 100); }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s4_static_method"));
    PASS();
}

/* S5 — Java chained method call. */
TEST(lrp_java_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.java", "package app;\n\nimport java.util.ArrayList;\nimport java.util.List;\n\n"
                         "class Builder {\n    private List<Integer> items = new ArrayList<>();\n"
                         "    public Builder add(int x) { items.add(x); return this; }\n"
                         "    public List<Integer> build() { return items; }\n}\n"},
        {"Main.java", "package app;\n\nimport java.util.List;\n\n"
                      "class Main {\n    public List<Integer> run() {\n"
                      "        return new Builder().add(1).add(2).build();\n    }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s5_chained"));
    PASS();
}

/* S6 — Java inherited method call (subclass calls base method). */
TEST(lrp_java_s6_inherited_method) {
    static const LRP_File f[] = {{"Animal.java", "package zoo;\n\nclass Animal {\n    public "
                                                 "String describe() { return \"animal\"; }\n}\n"},
                                 {"Dog.java",
                                  "package zoo;\n\nclass Dog extends Animal {\n"
                                  "    public String run() { return describe(); }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s6_inherited_method"));
    PASS();
}

/* S7 — Java generic method call. */
TEST(lrp_java_s7_generic) {
    static const LRP_File f[] = {
        {"Util.java", "package app;\n\nimport java.util.List;\n\n"
                      "class Util {\n    public static <T> T first(List<T> list) {\n"
                      "        return list.isEmpty() ? null : list.get(0);\n    }\n}\n"},
        {"App.java", "package app;\n\nimport java.util.List;\nimport java.util.ArrayList;\n\n"
                     "class App {\n    public String run() {\n"
                     "        List<String> xs = new ArrayList<>();\n"
                     "        xs.add(\"hello\");\n        return Util.first(xs);\n    }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s7_generic"));
    PASS();
}

/* S8 — Java field-type-hint (this.field.method()). */
TEST(lrp_java_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.java", "package app;\n\nclass Logger {\n"
                        "    public void log(String msg) { System.out.println(msg); }\n}\n"},
        {"Service.java",
         "package app;\n\nclass Service {\n    private Logger logger = new Logger();\n"
         "    public void run(String msg) { logger.log(msg); }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_java_s8_field_type_hint"));
    PASS();
}

/* Group 8: KOTLIN. Per-occurrence source oracle probes. */

/* S1 — Kotlin cross-file plain function call.
 * REAL BUG (the known Kotlin cross-LSP gap — keep RED): even a bare top-level
 * `double(n)` does not resolve cross-file → calls=0 (diagnostics: only DEFINES=4).
 * Kotlin has no cbm_run_kotlin_lsp_cross and the generic name resolver does not
 * link this Kotlin cross-file call.  Fix = add Kotlin cross-file LSP
 * (internal/cbm/lsp/kotlin_lsp.c) and wire it in pass_lsp_cross.c. */
TEST(lrp_kotlin_s1_crossfile_call) {
    static const LRP_File f[] = {{"Util.kt", "fun double(x: Int): Int = x * 2\n"},
                                 {"Main.kt", "fun run(n: Int): Int = double(n)\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s1_crossfile_call"));
    PASS();
}

/* S2 — Kotlin method dispatch on typed receiver. */
TEST(lrp_kotlin_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.kt", "class Counter {\n    private var n = 0\n"
                       "    fun inc() { n++ }\n    fun value(): Int = n\n}\n"},
        {"Runner.kt", "fun run(c: Counter): Int {\n    c.inc()\n    return c.value()\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s2_method_dispatch"));
    PASS();
}

/* S3 — Kotlin constructor call. */
TEST(lrp_kotlin_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.kt", "class Widget(val name: String) {\n    fun label(): String = name\n}\n"},
        {"Main.kt", "fun make(name: String): Widget = Widget(name)\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s3_constructor"));
    PASS();
}

/* S4 — Kotlin companion-object static call. */
TEST(lrp_kotlin_s4_companion_static) {
    static const LRP_File f[] = {
        {"Config.kt",
         "class Config(val debug: Boolean) {\n"
         "    companion object {\n        fun default(): Config = Config(false)\n    }\n}\n"},
        {"App.kt", "fun run(): Config = Config.default()\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s4_companion_static"));
    PASS();
}

/* S5 — Kotlin chained method call. */
TEST(lrp_kotlin_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.kt", "class Builder {\n    private val items = mutableListOf<Int>()\n"
                       "    fun add(x: Int): Builder { items.add(x); return this }\n"
                       "    fun build(): List<Int> = items.toList()\n}\n"},
        {"Main.kt", "fun run(): List<Int> = Builder().add(1).add(2).build()\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s5_chained"));
    PASS();
}

/* S6 — Kotlin inherited method call (open class). */
TEST(lrp_kotlin_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.kt", "open class Base {\n    open fun describe(): String = \"base\"\n}\n"},
        {"Child.kt", "class Child : Base() {\n    fun run(): String = describe()\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s6_inherited_method"));
    PASS();
}

/* S7 — Kotlin generic function call. */
TEST(lrp_kotlin_s7_generic) {
    static const LRP_File f[] = {
        {"Algo.kt", "fun <T : Comparable<T>> maxOf(a: T, b: T): T = if (a > b) a else b\n"},
        {"Main.kt", "fun run(a: Int, b: Int): Int = maxOf(a, b)\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s7_generic"));
    PASS();
}

/* S8 — Kotlin field-type-hint (this.field.method()). */
TEST(lrp_kotlin_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.kt", "class Logger {\n    fun log(msg: String): Unit { println(msg) }\n}\n"},
        {"Service.kt", "class Service {\n    private val logger = Logger()\n"
                       "    fun run(msg: String) { logger.log(msg) }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_kotlin_s8_field_type_hint"));
    PASS();
}

/* Group 9: C#. Per-occurrence source oracle probes. */

/* S1 — C# cross-file plain static call. */
TEST(lrp_csharp_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"Util.cs", "namespace App {\n    class Util {\n"
                    "        public static int Square(int x) { return x * x; }\n    }\n}\n"},
        {"Main.cs", "namespace App {\n    class Main {\n"
                    "        public int Run(int n) { return Util.Square(n); }\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s1_crossfile_call"));
    PASS();
}

/* S2 — C# method dispatch on typed instance. */
TEST(lrp_csharp_s2_method_dispatch) {
    static const LRP_File f[] = {
        {"Counter.cs", "namespace App {\n    class Counter {\n        private int n = 0;\n"
                       "        public void Inc() { n++; }\n        public int Value() { return n; "
                       "}\n    }\n}\n"},
        {"Runner.cs",
         "namespace App {\n    class Runner {\n"
         "        public int Run() {\n            var c = new Counter();\n"
         "            c.Inc();\n            return c.Value();\n        }\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s2_method_dispatch"));
    PASS();
}

/* S3 — C# constructor call (new T()). */
TEST(lrp_csharp_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.cs", "namespace App {\n    class Widget {\n        public string Name;\n"
                      "        public Widget(string name) { Name = name; }\n"
                      "        public string Label() { return Name; }\n    }\n}\n"},
        {"Factory.cs",
         "namespace App {\n    class Factory {\n"
         "        public Widget Make(string name) { return new Widget(name); }\n    }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s3_constructor"));
    PASS();
}

/* S4 — C# static method call. */
TEST(lrp_csharp_s4_static_method) {
    static const LRP_File f[] = {
        {"MathHelper.cs",
         "namespace App {\n    class MathHelper {\n"
         "        public static int Clamp(int v, int lo, int hi) {\n"
         "            return v < lo ? lo : v > hi ? hi : v;\n        }\n    }\n}\n"},
        {"App.cs",
         "namespace App {\n    class AppEntry {\n"
         "        public int Run(int n) { return MathHelper.Clamp(n, 0, 100); }\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s4_static_method"));
    PASS();
}

/* S5 — C# chained method call. */
TEST(lrp_csharp_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.cs", "namespace App {\n    using System.Collections.Generic;\n"
                       "    class Builder {\n        private List<int> items = new List<int>();\n"
                       "        public Builder Add(int x) { items.Add(x); return this; }\n"
                       "        public List<int> Build() { return items; }\n    }\n}\n"},
        {"Main.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class Main {\n        public List<int> Run() {\n"
         "            return new Builder().Add(1).Add(2).Build();\n        }\n    }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s5_chained"));
    PASS();
}

/* S6 — C# inherited method call. */
TEST(lrp_csharp_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.cs", "namespace App {\n    class Base {\n"
                    "        public virtual string Describe() { return \"base\"; }\n    }\n}\n"},
        {"Child.cs", "namespace App {\n    class Child : Base {\n"
                     "        public string Run() { return Describe(); }\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s6_inherited_method"));
    PASS();
}

/* S7 — C# generic method call. */
TEST(lrp_csharp_s7_generic) {
    static const LRP_File f[] = {
        {"Container.cs",
         "namespace App {\n    using System.Collections.Generic;\n"
         "    class Container {\n"
         "        public static T First<T>(List<T> xs) {\n"
         "            return xs.Count > 0 ? xs[0] : default;\n        }\n    }\n}\n"},
        {"App.cs", "namespace App {\n    using System.Collections.Generic;\n"
                   "    class AppEntry {\n"
                   "        public string Run() {\n"
                   "            var xs = new List<string> { \"hello\" };\n"
                   "            return Container.First(xs);\n        }\n    }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s7_generic"));
    PASS();
}

/* S8 — C# field-type-hint (this.field.method()). */
TEST(lrp_csharp_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Logger.cs", "namespace App {\n    class Logger {\n"
                      "        public void Log(string msg) { }\n    }\n}\n"},
        {"Service.cs", "namespace App {\n    class Service {\n"
                       "        private Logger logger = new Logger();\n"
                       "        public void Run(string msg) { logger.Log(msg); }\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_csharp_s8_field_type_hint"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── BONUS: PHP (lsp_cross WIRED) ────────────────────────────────────
 * ══════════════════════════════════════════════════════════════════════
 * PHP has cbm_run_php_lsp_cross.  Key scenarios: $this->method(),
 * new T(), static T::method(), chaining.
 */

/* S1 — PHP cross-file function call. */
TEST(lrp_php_s1_crossfile_call) {
    static const LRP_File f[] = {
        {"util.php", "<?php\nfunction triple($x) { return $x * 3; }\n"},
        {"main.php", "<?php\nrequire_once 'util.php';\nfunction run($n) { return triple($n); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s1_crossfile_call"));
    PASS();
}

/* S2 — PHP method dispatch via $this (cross-file). */
TEST(lrp_php_s2_method_dispatch) {
    static const LRP_File f[] = {{"Counter.php",
                                  "<?php\nclass Counter {\n    private $n = 0;\n"
                                  "    public function inc() { $this->n++; }\n"
                                  "    public function value() { return $this->n; }\n}\n"},
                                 {"main.php", "<?php\nrequire_once 'Counter.php';\n"
                                              "function run() {\n    $c = new Counter();\n"
                                              "    $c->inc();\n    return $c->value();\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s2_method_dispatch"));
    PASS();
}

/* S3 — PHP constructor (new T()). */
TEST(lrp_php_s3_constructor) {
    static const LRP_File f[] = {
        {"Widget.php", "<?php\nclass Widget {\n    public $name;\n"
                       "    public function __construct($name) { $this->name = $name; }\n"
                       "    public function label() { return $this->name; }\n}\n"},
        {"factory.php", "<?php\nrequire_once 'Widget.php';\n"
                        "function make($name) { return new Widget($name); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s3_constructor"));
    PASS();
}

/* S4 — PHP static method call (Class::method). */
TEST(lrp_php_s4_static_method) {
    static const LRP_File f[] = {
        {"Config.php",
         "<?php\nclass Config {\n    public static function version() { return '1.0'; }\n}\n"},
        {"app.php", "<?php\nrequire_once 'Config.php';\n"
                    "function run() { return Config::version(); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s4_static_method"));
    PASS();
}

/* S5 — PHP chained method call. */
TEST(lrp_php_s5_chained) {
    static const LRP_File f[] = {
        {"Builder.php", "<?php\nclass Builder {\n    private $items = [];\n"
                        "    public function add($x) { $this->items[] = $x; return $this; }\n"
                        "    public function build() { return $this->items; }\n}\n"},
        {"main.php", "<?php\nrequire_once 'Builder.php';\n"
                     "function run() { return (new Builder())->add(1)->add(2)->build(); }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s5_chained"));
    PASS();
}

/* S6 — PHP inherited method call. */
TEST(lrp_php_s6_inherited_method) {
    static const LRP_File f[] = {
        {"Base.php", "<?php\nclass Base {\n    public function describe() { return 'base'; }\n}\n"},
        {"Child.php", "<?php\nrequire_once 'Base.php';\nclass Child extends Base {\n"
                      "    public function run() { return $this->describe(); }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s6_inherited_method"));
    PASS();
}

/* S7 — PHP interface method call (type-hinted parameter). */
TEST(lrp_php_s7_interface_call) {
    static const LRP_File f[] = {
        {"LoggerInterface.php",
         "<?php\ninterface LoggerInterface {\n    public function log($msg);\n}\n"},
        {"FileLogger.php",
         "<?php\nrequire_once 'LoggerInterface.php';\n"
         "class FileLogger implements LoggerInterface {\n"
         "    public function log($msg) { file_put_contents('/tmp/log', $msg); }\n}\n"},
        {"Service.php", "<?php\nrequire_once 'LoggerInterface.php';\n"
                        "class Service {\n    private $logger;\n"
                        "    public function __construct(LoggerInterface $logger) {\n"
                        "        $this->logger = $logger;\n    }\n"
                        "    public function run($msg) { $this->logger->log($msg); }\n}\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s7_interface_call"));
    PASS();
}

/* S8 — PHP field-type-hint (property with docblock type). */
TEST(lrp_php_s8_field_type_hint) {
    static const LRP_File f[] = {
        {"Mailer.php",
         "<?php\nclass Mailer {\n    public function send($to, $body) { return true; }\n}\n"},
        {"Notifier.php", "<?php\nrequire_once 'Mailer.php';\n"
                         "class Notifier {\n    /** @var Mailer */\n    private $mailer;\n"
                         "    public function __construct(Mailer $mailer) {\n"
                         "        $this->mailer = $mailer;\n    }\n"
                         "    public function notify($to, $msg) {\n"
                         "        $this->mailer->send($to, $msg);\n    }\n}\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_php_s8_field_type_hint"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * ─── BONUS: USAGE-EDGE PROBES (type references, not calls) ───────────
 * ══════════════════════════════════════════════════════════════════════
 * USAGE edges are created for non-call type references. Constructor syntax is
 * a CALLS relationship; typed-parameter scenarios remain USAGE.
 */

/* Go: struct literal usage of a cross-file type. */
TEST(lrp_go_usage_struct_literal) {
    static const LRP_File f[] = {
        {"types.go", "package app\n\ntype Config struct{ Port int }\n"},
        {"main.go", "package app\n\nfunc Make() Config { return Config{Port: 8080} }\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_go_usage_struct_literal"));
    PASS();
}

/* TypeScript: exact imported type reference as parameter annotation. */
TEST(lrp_ts_usage_type_param) {
    static const LRP_File f[] = {
        {"types.ts", "export interface Config { timeout: number; }\n"},
        {"main.ts", "import { Config } from './types';\n\n"
                    "export function run(cfg: Config): number { return cfg.timeout; }\n"}};
    ASSERT_TRUE(lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_ts_usage_type_param"));
    PASS();
}

static int lrp_ts_imported_type_with_decoy(const char *main_filename) {
    LRP_File files[] = {
        {"a/types.ts", "export interface Config { selected: number; }\n"},
        {"b/types.ts", "export interface Config { decoy: string; }\n"},
        {main_filename, "import type { Config } from './a/types';\n"
                        "export function choose(cfg: Config): number { return cfg.selected; }\n"},
    };
    LRP_Proj lp;
    cbm_store_t *store = lrp_index(&lp, files, 3);
    const char *scenario = !strcmp(main_filename, "main.tsx")
                               ? "lrp_tsx_usage_import_type_with_decoy"
                               : "lrp_ts_usage_import_type_with_decoy";
    bool ok = lrp_check_oracle(store, lp.project, files, 3, scenario);
    int decoy = store ? lrp_exact_edge_by_qn_suffix(store, lp.project, "USAGE", "main.choose",
                                                    "b.types.Config")
                      : -1;
    if (!ok || decoy != 0)
        lrp_diag(store, lp.project, main_filename);
    lrp_cleanup(&lp, store);
    return ok && decoy == 0;
}

/* A duplicate type name must not let unique-name fallback hide import identity. */
TEST(lrp_ts_usage_import_type_with_decoy) {
    ASSERT_TRUE(lrp_ts_imported_type_with_decoy("main.ts"));
    PASS();
}

TEST(lrp_tsx_usage_import_type_with_decoy) {
    ASSERT_TRUE(lrp_ts_imported_type_with_decoy("main.tsx"));
    PASS();
}

/* Python: constructor syntax is an exact call to the materialized Class node. */
TEST(lrp_python_usage_instantiation) {
    static const LRP_File f[] = {
        {"model.py", "class User:\n    def __init__(self, name):\n        self.name = name\n"},
        {"main.py", "from .model import User\n\n\ndef create(name):\n    return User(name)\n"}};
    ASSERT_TRUE(
        lrp_assert_golden(f, (int)(sizeof(f) / sizeof(f[0])), "lrp_python_usage_instantiation"));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════════ */

SUITE(lsp_resolution_probe) {

    /* Go source oracle probes. */
    RUN_TEST(lrp_go_s1_crossfile_call);
    RUN_TEST(lrp_go_s2_method_dispatch);
    RUN_TEST(lrp_go_s3_constructor);
    RUN_TEST(lrp_go_s4_static_call);
    RUN_TEST(lrp_go_stdlib_import_does_not_bind_local_directory);
    RUN_TEST(lrp_go_s5_chained_call);
    RUN_TEST(lrp_go_s6_inherited_method);
    RUN_TEST(lrp_go_s7_generic_call);
    RUN_TEST(lrp_go_s8_field_type_hint);

    /* C source oracle probes. */
    RUN_TEST(lrp_c_s1_crossfile_call);
    RUN_TEST(lrp_c_s2_funcptr_in_struct);
    RUN_TEST(lrp_c_s3_constructor);
    RUN_TEST(lrp_c_s4_static_local);
    RUN_TEST(lrp_c_s5_chained_deref);
    RUN_TEST(lrp_c_s6_base_method);
    RUN_TEST(lrp_c_s7_generic_callback);
    RUN_TEST(lrp_c_s8_field_call);

    /* C++ source oracle probes. */
    RUN_TEST(lrp_cpp_s1_crossfile_call);
    RUN_TEST(lrp_cpp_s2_method_dispatch);
    RUN_TEST(lrp_cpp_s3_constructor);
    RUN_TEST(lrp_cpp_s4_static_method);
    RUN_TEST(lrp_cpp_s5_chained);
    RUN_TEST(lrp_cpp_s6_virtual_inherited);
    RUN_TEST(lrp_cpp_s7_template);
    RUN_TEST(lrp_cpp_s8_field_call);

    /* Rust source oracle probes. */
    RUN_TEST(lrp_rust_s1_crossfile_call);
    RUN_TEST(lrp_rust_s2_method_dispatch);
    RUN_TEST(lrp_rust_s3_constructor);
    RUN_TEST(lrp_rust_s4_static_method);
    RUN_TEST(lrp_rust_s5_chained);
    RUN_TEST(lrp_rust_s6_trait_method);
    RUN_TEST(lrp_rust_s7_generic);
    RUN_TEST(lrp_rust_s8_field_call);
    RUN_TEST(lrp_rust_crossfile_macro_hidden_call_carrier);

    /* Python source oracle probes. */
    RUN_TEST(lrp_python_s1_crossfile_call);
    RUN_TEST(lrp_python_s2_method_dispatch);
    RUN_TEST(lrp_python_s3_constructor);
    RUN_TEST(lrp_python_s4_class_method);
    RUN_TEST(lrp_python_s5_chained);
    RUN_TEST(lrp_python_s6_inherited_method);
    RUN_TEST(lrp_python_s6b_inherited_method_typed_receiver);
    RUN_TEST(lrp_python_s7_annotated_call);
    RUN_TEST(lrp_python_s8_field_type_hint);
    RUN_TEST(lrp_python_crossfile_dunder_carrier);

    /* TypeScript source oracle probes. */
    RUN_TEST(lrp_ts_s1_crossfile_call);
    RUN_TEST(lrp_ts_s2_method_dispatch);
    RUN_TEST(lrp_ts_s3_constructor);
    RUN_TEST(lrp_ts_s4_static_method);
    RUN_TEST(lrp_ts_s5_chained);
    RUN_TEST(lrp_ts_s6_inherited_method);
    RUN_TEST(lrp_ts_s7_generic);
    RUN_TEST(lrp_ts_s8_field_type_hint);

    /* Java source oracle probes. */
    RUN_TEST(lrp_java_s1_crossfile_call);
    RUN_TEST(lrp_java_s2_method_dispatch);
    RUN_TEST(lrp_java_s3_constructor);
    RUN_TEST(lrp_java_s4_static_method);
    RUN_TEST(lrp_java_s5_chained);
    RUN_TEST(lrp_java_s6_inherited_method);
    RUN_TEST(lrp_java_s7_generic);
    RUN_TEST(lrp_java_s8_field_type_hint);

    /* Kotlin source oracle probes. */
    RUN_TEST(lrp_kotlin_s1_crossfile_call);
    RUN_TEST(lrp_kotlin_s2_method_dispatch);
    RUN_TEST(lrp_kotlin_s3_constructor);
    RUN_TEST(lrp_kotlin_s4_companion_static);
    RUN_TEST(lrp_kotlin_s5_chained);
    RUN_TEST(lrp_kotlin_s6_inherited_method);
    RUN_TEST(lrp_kotlin_s7_generic);
    RUN_TEST(lrp_kotlin_s8_field_type_hint);

    /* C# source oracle probes. */
    /* Per-occurrence required controls and audited quality gaps. */
    RUN_TEST(lrp_csharp_s1_crossfile_call);
    RUN_TEST(lrp_csharp_s2_method_dispatch);
    RUN_TEST(lrp_csharp_s3_constructor);
    RUN_TEST(lrp_csharp_s4_static_method);
    RUN_TEST(lrp_csharp_s5_chained);
    RUN_TEST(lrp_csharp_s6_inherited_method);
    RUN_TEST(lrp_csharp_s7_generic);
    RUN_TEST(lrp_csharp_s8_field_type_hint);

    /* PHP source oracle probes. */
    RUN_TEST(lrp_php_s1_crossfile_call);
    RUN_TEST(lrp_php_s2_method_dispatch);
    RUN_TEST(lrp_php_s3_constructor);
    RUN_TEST(lrp_php_s4_static_method);
    RUN_TEST(lrp_php_s5_chained);
    RUN_TEST(lrp_php_s6_inherited_method);
    RUN_TEST(lrp_php_s7_interface_call);
    RUN_TEST(lrp_php_s8_field_type_hint);

    /* ── USAGE-edge bonus probes ── */
    RUN_TEST(lrp_go_usage_struct_literal);
    RUN_TEST(lrp_ts_usage_type_param);
    RUN_TEST(lrp_ts_usage_import_type_with_decoy);
    RUN_TEST(lrp_tsx_usage_import_type_with_decoy);
    RUN_TEST(lrp_python_usage_instantiation);
}
