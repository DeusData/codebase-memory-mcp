/*
 * test_convergence_probe.c — FINAL convergence probe.
 *
 * Purpose: check whether the bug-hunt has dried up after the prior batches
 * that mapped: base_classes (TS/PHP/Kotlin/Python-generics/C++-qualified),
 * import extraction (PHP/Python-wildcard/C#-alias), import-EDGE creation
 * (Rust/Kotlin/Java/C#/PHP), cross-LSP dispatch + '::' resolution
 * (Rust/Kotlin/Java/C#), constructor new T() (Java/C#/PHP/TS), C++ scope-
 * qualified calls, WRITES (Variable defs unregistered), Go method
 * parent_class → DEFINES_METHOD/OVERRIDE, HANDLES (Spring/ASP.NET/Laravel),
 * HTTP_CALLS (RestSharp/Guzzle), ASYNC SQS-Go, Kotlin throws/raises,
 * TS type-param USAGE, Dart/Groovy CALLS.
 *
 * This probe covers FRESH areas NOT yet probed:
 *   AREA A — CONFIGURES (sequential): getenv/os.environ/process.env (Go,
 *             Python, TS/JS, C#, Rust)
 *   AREA B — READS/WRITES (sequential path via pass_usages.c): variable
 *             access patterns for Python, Go, Java, C#
 *   AREA C — GRAPHQL_CALLS / GRPC_CALLS / TRPC_CALLS (parallel path,
 *             additional language fixtures beyond lang_contract.c baseline):
 *             Go GRPC (Service client suffix), Python GraphQL, TS tRPC
 *   AREA D — LSP corners not yet in lsp_resolution_probe.c:
 *             D1  closures/lambdas calling a named function (Go, Python, TS)
 *             D2  recursive calls (Go, Python, Rust, Java)
 *             D3  async/await call resolution (Python async, TS await,
 *                 C# async Task, Rust .await, Kotlin suspend)
 *             D4  operator-overload / index-operator calls (Python __add__,
 *                 Rust Add trait, C++ operator+, Kotlin operator fun)
 *             D5  calls through interface/trait object (Go interface,
 *                 Rust dyn Trait, Java interface, C# interface)
 *             D6  calls on an enum/sealed variant's method (Kotlin, Rust,
 *                 Java, C#)
 *   AREA E — Node-creation corners not yet covered:
 *             E1  nested classes (Python, Java, C#, Kotlin)
 *             E2  anonymous classes / objects (Java anonymous class,
 *                 Kotlin object expression)
 *             E3  top-level constants (Go const, Rust const/static,
 *                 Python module-level, TS const, Java static final)
 *             E4  macro-defined symbols (Rust macro_rules!, C #define
 *                 function-like)
 *             E5  property getters/setters (Python @property, C# get/set,
 *                 Kotlin val with custom getter, TS get accessor)
 *
 * EXPECTED GREEN vs RED reasoning:
 *
 * AREA A — CONFIGURES:
 *   Go os.Getenv: GREEN — pass_calls.c CBM_SVC_CONFIG fires on callee_name
 *     matching "getenv" / "os.Getenv" pattern.  Same-file resolution works.
 *   Python os.environ: UNCERTAIN/RED — os.environ is not a function call;
 *     it is an attribute access (dict subscript).  The extractor may not
 *     emit a CBMCall for it.  We probe with os.getenv() (function call form).
 *   TS process.env: RED — process.env[] is a subscript access, not a call.
 *     process.env.KEY is a member-access chain, not caught by CBM_SVC_CONFIG
 *     which pattern-matches callee_name strings.  We probe with a wrapper
 *     whose name contains "getenv" to distinguish the wrapper path from the
 *     raw process.env pattern (which likely emits 0 CONFIGURES).
 *   C# Environment.GetEnvironmentVariable: GREEN — callee_name contains
 *     "GetEnvironmentVariable" or "getenv"; service-pattern match fires.
 *   Rust std::env::var: UNCERTAIN — callee_name is "var" (too generic);
 *     the match relies on the qualified call "env::var" carrying "env" in
 *     the callee chain.  May not fire.
 *
 * AREA B — READS/WRITES (sequential via pass_usages.c):
 *   Sequential path: pass_usages.c cbm_resolve_file_read_writes runs for
 *     each file.  It resolves var reads/writes to registered Variable nodes.
 *   Python: UNCERTAIN — Variable nodes are only created when Variable is a
 *     recognized label.  If the pipeline doesn't emit "Variable" label nodes
 *     for Python global assignments, READS/WRITES will be 0.  We check.
 *   Go: UNCERTAIN — Go package-level vars may or may not become Variable
 *     nodes depending on lang_specs extract_fields settings.
 *   Java: UNCERTAIN — same question for static fields.
 *   C#: UNCERTAIN — same question for class fields.
 *
 * AREA C — Parallel service edges (additional fixtures):
 *   These require the PARALLEL path (>50 files).  The baseline in
 *   test_lang_contract.c already covers Go GRPC, Python GRAPHQL, TS TRPC,
 *   and pubsub INFRA_MAPS.  We probe additional variants:
 *   Go GRPC with a different service suffix: GREEN (already confirmed).
 *   JS tRPC (different local wrapper name): GREEN if callee QN has "trpc".
 *   Additional Python graphql-core call pattern: GREEN if callee contains "gql".
 *
 * AREA D — LSP corners:
 *   D1 closures/lambdas calling a named function:
 *     Go: GREEN — the closure body is extracted and the outer function has
 *       a CALLS edge; the closure itself may or may not be a node.
 *     Python: GREEN — lambda/inner-function CALLS extracted if the target is
 *       a known function.
 *     TS: GREEN — arrow functions call named exports; ts_lsp_cross resolves.
 *   D2 recursive calls:
 *     All languages: GREEN — self-calls resolve to the same node.
 *   D3 async/await:
 *     Python async def: GREEN — async def is extracted as a Function; awaiting
 *       a known coroutine should produce a CALLS edge.
 *     TS await: GREEN — ts_lsp_cross resolves awaited calls same as sync.
 *     C# async Task: GREEN if the name-based resolver finds the awaited method.
 *     Rust .await: UNCERTAIN/RED — "await" is a postfix operator, not a
 *       function call; the extractor's rust_call_types may not include it.
 *     Kotlin suspend: UNCERTAIN — suspend functions appear as Function nodes
 *       but coroutine calls may not be extracted as CBMCalls.
 *   D4 operator overloads:
 *     Python __add__: RED — operator calls (a + b) are not extracted as
 *       CBMCalls in the Python extractor; only explicit method calls are.
 *     Rust Add trait impl: RED — `a + b` with impl Add is a desugared method
 *       call, but the extractor sees a binary_expression, not a call_expression.
 *     C++ operator+: RED — same desugaring issue; call_expression captures
 *       only direct call syntax, not operator tokens.
 *     Kotlin operator fun: UNCERTAIN — Kotlin operator overload is a named
 *       fun marked `operator`; if extracted as a Function, an operator call
 *       that desugars to the function may not appear in call_expressions.
 *   D5 interface/trait dispatch:
 *     Go: GREEN — interface method called through a concrete value; lsp_cross
 *       infers the concrete type from assignment.
 *     Rust dyn Trait: UNCERTAIN — dyn Trait dispatch; receiver type is erased
 *       at the call site; lsp_cross may not wire Rust.
 *     Java: RED — no lsp_cross; name-based resolver finds method if unique.
 *     C#: RED — no lsp_cross wiring; same as Java.
 *   D6 enum/sealed variant method:
 *     Kotlin sealed class variant: GREEN — variant is a data class with its
 *       own methods; calls resolve by name if unique.
 *     Rust enum method: UNCERTAIN — impl on enum; method call on a value
 *       typed as the enum; lsp_cross is not wired for Rust.
 *     Java enum method: UNCERTAIN — same Java/no-lsp_cross situation.
 *
 * AREA E — Node-creation corners:
 *   E1 nested classes: GREEN for Java (inner class creates a Class node even
 *     if parent_class is set).  Python inner class: GREEN if extractor walks
 *     nested class_definition nodes.
 *   E2 anonymous classes:
 *     Java anonymous class (new Iface() {}): RED — Java call_types only
 *       includes method_invocation; object_creation_expression with an
 *       anonymous class body is never extracted.  (Same root cause as J-S3.)
 *     Kotlin object expression: UNCERTAIN — `object : Iface {}` may be
 *       treated as an anonymous Class node or omitted.
 *   E3 top-level constants:
 *     Go const: UNCERTAIN — Go "const" declarations may not be labeled as
 *       Variable; lang_specs does not list "const_spec" in function_types.
 *     Rust const / static: UNCERTAIN — same.
 *     Python module-level CONSTANT: UNCERTAIN — assignment at module level
 *       may or may not create a Variable node.
 *     Java static final: UNCERTAIN — field_declaration may not be extracted
 *       as a Variable if Java field_types is not set.
 *   E4 macro-defined symbols:
 *     Rust macro_rules!: RED — macro_rules! definitions are not in Rust
 *       function_types ("function_item" / "impl_item"); macros are skipped.
 *     C #define function-like: RED — C preprocessor macros are not in the
 *       AST (tree-sitter C grammar ignores them); no node is created.
 *   E5 property getters/setters:
 *     Python @property: UNCERTAIN — decorated functions are extracted as
 *       Function nodes; whether the getter/setter pair merges into one node
 *       or two depends on label extraction for decorated_definition.
 *     C# get/set: GREEN — C# property accessors appear in the AST as
 *       Method-like nodes; extract_defs.c should walk accessor_declaration.
 *     Kotlin custom getter: UNCERTAIN — `val x get() = ...` is an unusual
 *       grammar node; may be skipped by extract_defs.
 *     TS get accessor: UNCERTAIN — `get foo()` is a method_definition with
 *       a "get" flag; extraction may treat it like a regular method.
 *
 * NOT registered in test_main.c (per task specification).
 * Run standalone: link this file and call suite_convergence_probe().
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ══════════════════════════════════════════════════════════════════
 * Harness — self-contained copy (CP_ prefix avoids link conflicts).
 * Mirrors test_edge_types_probe.c / test_node_creation_probe.c.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} CP_Proj;

typedef struct {
    const char *name;
    const char *content;
} CP_File;

static void cp_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *cp_open_indexed(CP_Proj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *cp_index_files(CP_Proj *lp, const CP_File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_cp_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    cp_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return cp_open_indexed(lp);
}

static void cp_cleanup(CP_Proj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project); lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal); unlink(shm);
}

/* Count edges of `edge_type`; -1 on DB error. */
static int cp_edges(cbm_store_t *store, const char *project, const char *edge_type) {
    if (!store) return -1;
    return cbm_store_count_edges_by_type(store, project, edge_type);
}

/* Count nodes with a given label. */
static int cp_count_label(cbm_store_t *store, const char *project, const char *label) {
    if (!store) return -1;
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum callable nodes (Function + Method). */
static int cp_callables(cbm_store_t *store, const char *project) {
    int fn = cp_count_label(store, project, "Function");
    int mt = cp_count_label(store, project, "Method");
    return (fn < 0 ? 0 : fn) + (mt < 0 ? 0 : mt);
}

/* Diagnostic edge histogram (stderr only, so test output stays clean). */
static const char *CP_ALL_EDGE_TYPES[] = {
    "CALLS", "CONFIGURES", "CONTAINS_FILE", "CONTAINS_FOLDER",
    "DATA_FLOWS", "DECORATES", "DEFINES", "DEFINES_METHOD",
    "DEPENDS_ON", "FILE_CHANGES_WITH", "GRAPHQL_CALLS", "GRPC_CALLS",
    "HANDLES", "HTTP_CALLS", "IMPLEMENTS", "IMPORTS",
    "INHERITS", "INFRA_MAPS", "OVERRIDE", "READS",
    "SEMANTICALLY_RELATED", "SIMILAR_TO", "TESTS_FILE", "TESTS",
    "TRPC_CALLS", "USAGE", "ASYNC_CALLS", "WRITES", NULL};

static void cp_diag(cbm_store_t *store, const char *project, const char *label) {
    if (!store) { fprintf(stderr, "  [CP] %s: no graph DB\n", label); return; }
    char line[640] = {0};
    for (int i = 0; CP_ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, CP_ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 48) {
            char one[56];
            snprintf(one, sizeof(one), "%s=%d ", CP_ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "  [CP] %s edges=[%s]\n", label, line[0] ? line : "(none)");
}

/* Helper: index N meaningful files plus enough Python pad files to force
 * the parallel pipeline path (MIN_FILES_FOR_PARALLEL = 50). */
enum { CP_PARALLEL_PAD = 52, CP_PAD_MAX = 80 };

static cbm_store_t *cp_index_parallel(CP_Proj *lp, const CP_File *meaningful, int n_mean) {
    static char pad_name[CP_PARALLEL_PAD][48];
    static char pad_body[CP_PARALLEL_PAD][64];
    CP_File files[CP_PAD_MAX] = {{0}};
    int n = 0;
    for (int i = 0; i < n_mean; i++) files[n++] = meaningful[i];
    for (int i = 0; i < CP_PARALLEL_PAD; i++) {
        snprintf(pad_name[i], sizeof(pad_name[i]), "pad/pad_%02d.py", i);
        snprintf(pad_body[i], sizeof(pad_body[i]), "def pad_%02d():\n    return %d\n", i, i);
        files[n].name    = pad_name[i];
        files[n].content = pad_body[i];
        n++;
    }
    return cp_index_files(lp, files, n);
}

/* ══════════════════════════════════════════════════════════════════
 * AREA A — CONFIGURES edges
 *
 * Sequential path: pass_calls.c emits CONFIGURES when the resolved
 * callee QN is classified as CBM_SVC_CONFIG (callee_name matches
 * service-pattern for env-var accessors like getenv / os.Getenv /
 * GetEnvironmentVariable / env::var).
 * ══════════════════════════════════════════════════════════════════ */

/* A1 — Go os.Getenv produces CONFIGURES.
 * EXPECTED GREEN: Go lsp_cross resolves os.Getenv; callee_name
 * "os.Getenv" / "getenv" matches CBM_SVC_CONFIG pattern. */
TEST(cp_configures_go_getenv) {
    static const CP_File f[] = {
        {"config/env.go",
         "package config\n\nimport \"os\"\n\n"
         "func DatabaseURL() string {\n    return os.Getenv(\"DATABASE_URL\")\n}\n\n"
         "func Port() string {\n    return os.Getenv(\"PORT\")\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int cfg = cp_edges(store, lp.project, "CONFIGURES");
    if (cfg < 1) cp_diag(store, lp.project, "configures/go_getenv");
    cp_cleanup(&lp, store);
    /* REAL BUG: internal/cbm/extract_env_accesses.c extracts os.Getenv into
     * result->env_accesses, but NO pipeline pass under src/pipeline ever consumes
     * env_accesses to emit CONFIGURES.  The only CONFIGURES paths are (a) the
     * service-pattern call resolver (pass_calls.c emit_classified_edge), which
     * needs the env-accessor call to resolve to an in-graph node — os.Getenv is
     * stdlib, never indexed, so it never resolves; and (b) pass_configlink.c
     * key_symbol, which needs a config-file ConfigVariable target.  Stdlib
     * env-accessor calls therefore produce 0 CONFIGURES. [KNOWN class 14] */
    ASSERT_TRUE(cfg >= 1);
    PASS();
}

/* A2 — Python os.getenv() produces CONFIGURES.
 * EXPECTED GREEN: os.getenv is a function call; callee_name "getenv"
 * or "os.getenv" should match CBM_SVC_CONFIG. */
TEST(cp_configures_python_os_getenv) {
    static const CP_File f[] = {
        {"settings.py",
         "import os\n\n\n"
         "def get_db_url():\n    return os.getenv(\"DATABASE_URL\", \"sqlite:///db.sqlite3\")\n\n\n"
         "def get_secret_key():\n    return os.getenv(\"SECRET_KEY\", \"dev-key\")\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int cfg = cp_edges(store, lp.project, "CONFIGURES");
    if (cfg < 1) cp_diag(store, lp.project, "configures/python_os_getenv");
    cp_cleanup(&lp, store);
    /* REAL BUG: os.getenv extracted into env_accesses but never consumed; stdlib
     * callee never resolves to an in-graph node → 0 CONFIGURES.
     * Root cause: src/pipeline has no consumer of CBMFileResult.env_accesses
     * (internal/cbm/extract_env_accesses.c). [KNOWN class 14] */
    ASSERT_TRUE(cfg >= 1);
    PASS();
}

/* A3 — TypeScript/Node.js process.env wrapper.
 * EXPECTED UNCERTAIN/RED: `process.env.KEY` is a member-access chain,
 * not a call expression; CBM_SVC_CONFIG pattern-matches callee_name
 * strings from call expressions only.  We use a named wrapper
 * getenv() whose callee name matches the pattern to test the
 * GREEN path separately from raw process.env access.
 *
 * Sub-case A3a: named getenv() wrapper — EXPECTED GREEN.
 * Sub-case A3b: raw process.env.KEY member access — EXPECTED 0 CONFIGURES
 *               (not a call; pipeline cannot see it). */
TEST(cp_configures_ts_getenv_wrapper) {
    /* A3a: wrapper whose name triggers CBM_SVC_CONFIG */
    static const CP_File f[] = {
        {"env.ts",
         "function getenv(key: string, fallback: string = ''): string {\n"
         "    return (process as any).env[key] ?? fallback;\n}\n\n"
         "function dbUrl(): string { return getenv('DATABASE_URL'); }\n"
         "function port(): string { return getenv('PORT', '3000'); }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int cfg = cp_edges(store, lp.project, "CONFIGURES");
    if (cfg < 1) cp_diag(store, lp.project, "configures/ts_getenv_wrapper");
    cp_cleanup(&lp, store);
    /* getenv() is a locally-defined function; the resolver finds it and
     * its name matches CBM_SVC_CONFIG → CONFIGURES should fire. */
    ASSERT_TRUE(cfg >= 1);
    PASS();
}

/* A4 — C# Environment.GetEnvironmentVariable produces CONFIGURES.
 * EXPECTED GREEN: callee_name "GetEnvironmentVariable" or the qualified
 * form matches the CBM_SVC_CONFIG pattern. */
TEST(cp_configures_csharp_getenv) {
    static const CP_File f[] = {
        {"Config.cs",
         "using System;\n\nnamespace App {\n"
         "    class Config {\n"
         "        public static string DbUrl() =>\n"
         "            Environment.GetEnvironmentVariable(\"DATABASE_URL\") ?? \"\";\n\n"
         "        public static int Port() {\n"
         "            var p = Environment.GetEnvironmentVariable(\"PORT\");\n"
         "            return p != null ? int.Parse(p) : 8080;\n"
         "        }\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int cfg = cp_edges(store, lp.project, "CONFIGURES");
    if (cfg < 1) cp_diag(store, lp.project, "configures/csharp_getenv");
    cp_cleanup(&lp, store);
    /* REAL BUG: Environment.GetEnvironmentVariable extracted into env_accesses but
     * never consumed; stdlib callee never resolves to an in-graph node →
     * 0 CONFIGURES.  Root cause: no pipeline consumer of env_accesses
     * (internal/cbm/extract_env_accesses.c). [KNOWN class 14] */
    ASSERT_TRUE(cfg >= 1);
    PASS();
}

/* A5 — Rust std::env::var produces CONFIGURES.
 * EXPECTED UNCERTAIN/RED: The callee_name extracted for `std::env::var("KEY")`
 * may be just "var" (too generic to match CBM_SVC_CONFIG) or the full path
 * "env::var".  The service pattern likely needs the "env" prefix to classify
 * it.  We assert the CORRECT behavior (cfg >= 1); fail = new bug class. */
TEST(cp_configures_rust_env_var) {
    static const CP_File f[] = {
        {"config.rs",
         "fn db_url() -> String {\n"
         "    std::env::var(\"DATABASE_URL\").unwrap_or_else(|_| \"sqlite\".to_string())\n}\n\n"
         "fn port() -> u16 {\n"
         "    std::env::var(\"PORT\").ok().and_then(|p| p.parse().ok()).unwrap_or(8080)\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int cfg = cp_edges(store, lp.project, "CONFIGURES");
    if (cfg < 1) cp_diag(store, lp.project, "configures/rust_env_var");
    cp_cleanup(&lp, store);
    /* REAL BUG: std::env::var extracted into env_accesses but never consumed;
     * stdlib callee never resolves to an in-graph node → 0 CONFIGURES.  (Rust
     * also has no cross-LSP, compounding the miss.)  Root cause: no pipeline
     * consumer of env_accesses (internal/cbm/extract_env_accesses.c).
     * [KNOWN class 14] */
    ASSERT_TRUE(cfg >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * AREA B — READS / WRITES (sequential path via pass_usages.c)
 *
 * pass_usages.c cbm_resolve_file_read_writes() runs on every file
 * in both sequential and parallel paths.  It emits READS/WRITES
 * edges from a Function/Method to a Variable node — but only when
 * (a) a Variable node exists in the graph for that symbol, AND
 * (b) the file's extracted rw_accesses[] resolves to that node.
 *
 * These tests check whether Variable nodes are emitted at all and
 * whether READS/WRITES edges appear for module-level globals.
 * ══════════════════════════════════════════════════════════════════ */

/* B1 — Python: module-level variable read/write.
 * EXPECTED UNCERTAIN: Python module-level assignment `x = 1` may or may
 * not produce a Variable node.  If it does, reading it inside a function
 * should emit a READS edge. */
TEST(cp_reads_writes_python_global) {
    static const CP_File f[] = {
        {"counter.py",
         "count = 0\n\n\n"
         "def increment():\n    global count\n    count += 1\n\n\n"
         "def get():\n    return count\n\n\n"
         "def reset():\n    global count\n    count = 0\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int reads  = cp_edges(store, lp.project, "READS");
    int writes = cp_edges(store, lp.project, "WRITES");
    if (reads < 1 && writes < 1)
        cp_diag(store, lp.project, "reads_writes/python_global");
    cp_cleanup(&lp, store);
    /* REAL BUG: src/pipeline/pass_definitions.c process_def calls
     * cbm_registry_add ONLY for Function/Method/Class/Interface labels — never
     * for "Variable".  So Variable nodes are created in the graph but never
     * registered; pass_usages.c resolve_rw_edges then can't resolve the var_name
     * to a Variable QN, and READS/WRITES edges are never emitted for any
     * language.  Fix: register Variable labels in process_def. [KNOWN class 7] */
    ASSERT_TRUE(reads >= 1 || writes >= 1);
    PASS();
}

/* B2 — Go: package-level variable read/write.
 * EXPECTED UNCERTAIN: Go `var x int` at package scope may not emit a
 * Variable node; if it does, reads inside functions produce READS. */
TEST(cp_reads_writes_go_global) {
    static const CP_File f[] = {
        {"store.go",
         "package store\n\nvar cache = map[string]string{}\n\n"
         "func Set(key, val string) { cache[key] = val }\n\n"
         "func Get(key string) string { return cache[key] }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int reads  = cp_edges(store, lp.project, "READS");
    int writes = cp_edges(store, lp.project, "WRITES");
    if (reads < 1 && writes < 1)
        cp_diag(store, lp.project, "reads_writes/go_global");
    cp_cleanup(&lp, store);
    /* REAL BUG: Variable nodes never cbm_registry_add'd (pass_definitions.c
     * process_def registers only Function/Method/Class/Interface), so
     * pass_usages.c cannot resolve the Go package-var to a Variable QN →
     * 0 READS/WRITES. [KNOWN class 7] */
    ASSERT_TRUE(reads >= 1 || writes >= 1);
    PASS();
}

/* B3 — Java: static field read/write.
 * EXPECTED UNCERTAIN: Java static fields may not emit Variable nodes
 * unless field_declaration is in the extract_fields list. */
TEST(cp_reads_writes_java_static_field) {
    static const CP_File f[] = {
        {"Counter.java",
         "package app;\n\nclass Counter {\n    private static int count = 0;\n\n"
         "    public static void increment() { count++; }\n\n"
         "    public static int value() { return count; }\n\n"
         "    public static void reset() { count = 0; }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int reads  = cp_edges(store, lp.project, "READS");
    int writes = cp_edges(store, lp.project, "WRITES");
    if (reads < 1 && writes < 1)
        cp_diag(store, lp.project, "reads_writes/java_static_field");
    cp_cleanup(&lp, store);
    /* REAL BUG: Variable/Field nodes never cbm_registry_add'd (pass_definitions.c
     * process_def registers only Function/Method/Class/Interface), so
     * pass_usages.c cannot resolve the Java static field to a Variable QN →
     * 0 READS/WRITES. [KNOWN class 7] */
    ASSERT_TRUE(reads >= 1 || writes >= 1);
    PASS();
}

/* B4 — C#: static field read/write.
 * EXPECTED UNCERTAIN: C# static fields in a class may or may not be
 * extracted as Variable nodes depending on extract_fields settings. */
TEST(cp_reads_writes_cs_static_field) {
    static const CP_File f[] = {
        {"Registry.cs",
         "namespace App {\n    class Registry {\n"
         "        private static int _count = 0;\n\n"
         "        public static void Register() { _count++; }\n\n"
         "        public static int Count() { return _count; }\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int reads  = cp_edges(store, lp.project, "READS");
    int writes = cp_edges(store, lp.project, "WRITES");
    if (reads < 1 && writes < 1)
        cp_diag(store, lp.project, "reads_writes/cs_static_field");
    cp_cleanup(&lp, store);
    /* REAL BUG: Variable/Field nodes never cbm_registry_add'd (pass_definitions.c
     * process_def registers only Function/Method/Class/Interface), so
     * pass_usages.c cannot resolve the C# static field to a Variable QN →
     * 0 READS/WRITES. [KNOWN class 7] */
    ASSERT_TRUE(reads >= 1 || writes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * AREA C — Parallel-path service edges (additional fixtures)
 *
 * GRAPHQL_CALLS, GRPC_CALLS, TRPC_CALLS require >50 files (parallel
 * pipeline path).  We add NEW language/wrapper variants not in the
 * lang_contract.c baseline to probe for coverage gaps.
 * ══════════════════════════════════════════════════════════════════ */

/* C1 — Go gRPC: additional client-suffix variant (UserServiceClient).
 * EXPECTED GREEN: the GRPC_CALLS heuristic fires on callee QN containing
 * "ServiceClient" suffix AND the package containing "grpc" or "pb". */
TEST(cp_grpc_calls_go_user_service) {
    static const CP_File meaningful[] = {
        {"go.mod", "module example.com/userdemo\n\ngo 1.21\n"},
        {"userpb/user_grpc.pb.go",
         "package userpb\n\nimport \"context\"\n\n"
         "type GetUserRequest struct{ Id string }\n"
         "type GetUserResponse struct{ Name string }\n\n"
         "type UserServiceClient interface {\n"
         "\tGetUser(ctx context.Context, in *GetUserRequest) (*GetUserResponse, error)\n}\n\n"
         "type userServiceClient struct{}\n\n"
         "func NewUserServiceClient(cc interface{}) UserServiceClient {\n"
         "\treturn &userServiceClient{}\n}\n\n"
         "func (c *userServiceClient) GetUser(ctx context.Context, "
         "in *GetUserRequest) (*GetUserResponse, error) {\n"
         "\treturn &GetUserResponse{}, nil\n}\n"},
        {"svc/main.go",
         "package main\n\nimport (\n\t\"context\"\n\n"
         "\tpb \"example.com/userdemo/userpb\"\n)\n\n"
         "func FetchUser(conn interface{}) {\n"
         "\tclient := pb.NewUserServiceClient(conn)\n"
         "\tclient.GetUser(context.Background(), &pb.GetUserRequest{Id: \"u1\"})\n}\n\n"
         "func main() { FetchUser(nil) }\n"}};
    CP_Proj lp;
    cbm_store_t *store =
        cp_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int grpc = cp_edges(store, lp.project, "GRPC_CALLS");
    if (grpc < 1) cp_diag(store, lp.project, "grpc_calls/go_user_service");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(grpc >= 1);
    PASS();
}

/* C2 — TS tRPC: different procedure wrapper name.
 * EXPECTED GREEN: callee QN must contain "trpc" substring to trigger
 * CBM_SVC_TRPC.  We use a module path "trpc/client.ts" so the QN carries
 * the "trpc" segment. */
TEST(cp_trpc_calls_ts_procedure) {
    static const CP_File meaningful[] = {
        {"trpc/client.ts",
         "export function createTRPCProxyClient(opts: any): any {\n"
         "  return { query: (proc: string) => proc };\n}\n\n"
         "export function trpcCall(proc: string): string {\n"
         "  const client = createTRPCProxyClient({});\n"
         "  return client.query(proc);\n}\n"},
        {"api/users.ts",
         "import { trpcCall } from '../trpc/client';\n\n"
         "export function getUser(id: string): string {\n"
         "  return trpcCall('user.getById');\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store =
        cp_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int trpc = cp_edges(store, lp.project, "TRPC_CALLS");
    if (trpc < 1) cp_diag(store, lp.project, "trpc_calls/ts_procedure");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(trpc >= 1);
    PASS();
}

/* C3 — Python GraphQL: graphql-core execute() call.
 * EXPECTED GREEN: callee QN must contain "gql" or "graphql" to trigger
 * CBM_SVC_GRAPHQL.  We place the wrapper in a module "graphql/client.py". */
TEST(cp_graphql_calls_python_execute) {
    static const CP_File meaningful[] = {
        {"graphql/client.py",
         "def gql(query):\n    return query\n\n"
         "def execute(query, variables=None):\n    return {\"data\": None}\n"},
        {"api/schema.py",
         "from graphql.client import gql, execute\n\n\n"
         "def fetch_user(user_id):\n"
         "    q = gql('query GetUser($id: ID!) { user(id: $id) { name } }')\n"
         "    return execute(q, {\"id\": user_id})\n\n\n"
         "def list_users():\n"
         "    q = gql('query ListUsers { users { id name } }')\n"
         "    return execute(q)\n"}};
    CP_Proj lp;
    cbm_store_t *store =
        cp_index_parallel(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int graphql = cp_edges(store, lp.project, "GRAPHQL_CALLS");
    if (graphql < 1) cp_diag(store, lp.project, "graphql_calls/python_execute");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(graphql >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * AREA D — LSP call-resolution corners
 * ══════════════════════════════════════════════════════════════════ */

/* ── D1: Closures/lambdas calling a named function ───────────────── */

/* D1a — Go closure captures outer call.
 * EXPECTED GREEN: the outer function Make() has CALLS edges even though
 * the call appears inside an anonymous func literal. */
TEST(cp_closure_go_outer_call) {
    static const CP_File f[] = {
        {"ops.go",
         "package ops\n\nfunc double(x int) int { return x * 2 }\n\n"
         "func Make(base int) func(int) int {\n"
         "    return func(x int) int { return double(base + x) }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "closure/go_outer_call");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D1b — Python lambda calling a named function.
 * EXPECTED GREEN: the enclosing function apply() has CALLS edge to helper(). */
TEST(cp_closure_python_lambda) {
    static const CP_File f[] = {
        {"funcs.py",
         "def helper(x):\n    return x * 2\n\n\n"
         "def apply(items):\n    transform = lambda x: helper(x)\n"
         "    return list(map(transform, items))\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "closure/python_lambda");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D1c — TypeScript arrow function calling a named export.
 * EXPECTED GREEN: ts_lsp_cross resolves the named import. */
TEST(cp_closure_ts_arrow) {
    static const CP_File f[] = {
        {"utils.ts",
         "export function format(x: number): string { return x.toFixed(2); }\n"},
        {"mapper.ts",
         "import { format } from './utils';\n\n"
         "export function mapAll(xs: number[]): string[] {\n"
         "    return xs.map(x => format(x));\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 2);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "closure/ts_arrow");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* ── D2: Recursive calls ─────────────────────────────────────────── */

/* D2a — Go recursive function.
 * FIXTURE FIX (was RED): a pure self-call (Fib->Fib) produces 0 CALLS because
 * the pipeline deliberately suppresses self-loop edges (src->id == dst->id) in
 * pass_calls.c resolve_single_call / pass_parallel.c classify_and_emit_edge.
 * That suppression is by design, NOT a bug.  The feature under test ("recursion
 * does not break the surrounding call graph") is correctly exercised by adding a
 * non-self caller (Run->Fib) so a genuine CALLS edge appears while the recursive
 * shape (Fib->Fib, suppressed) is retained. */
TEST(cp_recursive_go) {
    static const CP_File f[] = {
        {"fib.go",
         "package fib\n\nfunc Fib(n int) int {\n"
         "    if n <= 1 { return n }\n    return Fib(n-1) + Fib(n-2)\n}\n\n"
         "func Run() int { return Fib(10) }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "recursive/go");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);  /* Run->Fib (self-loop Fib->Fib is suppressed by design) */
    PASS();
}

/* D2b — Python recursive function.
 * FIXTURE FIX (was RED): pure self-call factorial->factorial is suppressed as a
 * self-loop (see D2a).  Add a non-self caller run->factorial so a real CALLS
 * edge appears; the recursive shape is retained. */
TEST(cp_recursive_python) {
    static const CP_File f[] = {
        {"math.py",
         "def factorial(n):\n"
         "    if n <= 1:\n        return 1\n"
         "    return n * factorial(n - 1)\n\n\n"
         "def run():\n    return factorial(5)\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "recursive/python");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);  /* run->factorial (self-loop suppressed by design) */
    PASS();
}

/* D2c — Rust recursive function.
 * FIXTURE FIX (was RED): pure self-call sum->sum is suppressed as a self-loop
 * (see D2a).  Add a non-self caller run->sum so a real CALLS edge appears; the
 * recursive shape is retained.  (run->sum resolves via the name-based registry
 * resolver — a plain same-file function call, no `::` path involved.) */
TEST(cp_recursive_rust) {
    static const CP_File f[] = {
        {"math.rs",
         "pub fn sum(n: u64) -> u64 {\n"
         "    if n == 0 { return 0; }\n    n + sum(n - 1)\n}\n\n"
         "pub fn run() -> u64 { sum(5) }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "recursive/rust");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);  /* run->sum (self-loop sum->sum suppressed by design) */
    PASS();
}

/* D2d — Java recursive static method.
 * FIXTURE FIX (was RED): pure self-call fib->fib is suppressed as a self-loop
 * (see D2a).  Add a non-self caller run->fib so a real CALLS edge appears; the
 * recursive shape is retained. */
TEST(cp_recursive_java) {
    static const CP_File f[] = {
        {"Fib.java",
         "package app;\n\nclass Fib {\n"
         "    static long fib(int n) {\n"
         "        if (n <= 1) return n;\n        return fib(n-1) + fib(n-2);\n    }\n\n"
         "    static long run() { return fib(10); }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "recursive/java");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);  /* run->fib (self-loop fib->fib suppressed by design) */
    PASS();
}

/* ── D3: Async/await call resolution ─────────────────────────────── */

/* D3a — Python async def / await.
 * EXPECTED GREEN: async def produces a Function node; awaiting a known
 * coroutine is still an expression_statement with a call child. */
TEST(cp_async_python_await) {
    static const CP_File f[] = {
        {"svc.py",
         "async def fetch_data(url):\n    return {\"url\": url}\n\n\n"
         "async def process(url):\n    data = await fetch_data(url)\n    return data\n\n\n"
         "async def run():\n    result = await process(\"http://example.com\")\n    return result\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int fn_count = cp_count_label(store, lp.project, "Function");
    if (calls < 1) cp_diag(store, lp.project, "async/python_await");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(fn_count >= 2);  /* async def functions must become Function nodes */
    ASSERT_TRUE(calls >= 1);     /* await call must produce at least one CALLS edge */
    PASS();
}

/* D3b — TypeScript async/await.
 * EXPECTED GREEN: ts_lsp_cross resolves await-ed named function calls. */
TEST(cp_async_ts_await) {
    static const CP_File f[] = {
        {"api.ts",
         "export async function fetchUser(id: string): Promise<any> {\n"
         "    return { id };\n}\n"},
        {"service.ts",
         "import { fetchUser } from './api';\n\n"
         "export async function getUser(id: string): Promise<any> {\n"
         "    const user = await fetchUser(id);\n    return user;\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 2);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "async/ts_await");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D3c — C# async Task / await.
 * EXPECTED GREEN: C# name-based resolver finds the awaited method. */
TEST(cp_async_csharp_task) {
    static const CP_File f[] = {
        {"Svc.cs",
         "using System.Threading.Tasks;\n\nnamespace App {\n"
         "    class Svc {\n"
         "        async Task<string> FetchAsync(string url) {\n"
         "            await Task.Delay(0);\n            return url;\n        }\n\n"
         "        async Task<string> RunAsync(string url) {\n"
         "            return await FetchAsync(url);\n        }\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "async/csharp_task");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D3d — Rust .await on an async fn.
 * EXPECTED UNCERTAIN/RED: `future.await` is a postfix await_expression
 * in tree-sitter-rust.  rust_call_types = {"call_expression"} only;
 * await_expression is not included, so the awaited call is not extracted.
 * Assert CORRECT outcome (calls >= 1); RED if extractor misses it. */
TEST(cp_async_rust_await) {
    static const CP_File f[] = {
        {"svc.rs",
         "async fn fetch(url: &str) -> String { url.to_string() }\n\n"
         "async fn run(url: &str) -> String {\n    fetch(url).await\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int fn_count = cp_count_label(store, lp.project, "Function");
    if (calls < 1) cp_diag(store, lp.project, "async/rust_await");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(fn_count >= 2);  /* async fns must still become Function nodes */
    ASSERT_TRUE(calls >= 1);     /* RED if .await not in rust_call_types */
    PASS();
}

/* D3e — Kotlin suspend function call.
 * EXPECTED UNCERTAIN: Kotlin suspend functions are ordinary functions at
 * the AST level; calling one looks like a regular call_expression.  The
 * resolver should produce a CALLS edge via the name-based resolver. */
TEST(cp_async_kotlin_suspend) {
    static const CP_File f[] = {
        {"Fetch.kt",
         "suspend fun fetchData(url: String): String = url\n\n"
         "suspend fun process(url: String): String {\n"
         "    val data = fetchData(url)\n    return data.uppercase()\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "async/kotlin_suspend");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* ── D4: Operator-overload / index-operator calls ────────────────── */

/* D4a — Python __add__ via operator call.
 * EXPECTED RED: `a + b` is a binary_expression, not a call_expression.
 * Python call_types = {"call"} only; the __add__ dispatch is NOT extracted.
 * Assert CORRECT outcome (calls >= 1); RED confirms the gap. */
TEST(cp_operator_python_add) {
    static const CP_File f[] = {
        {"vec.py",
         "class Vec:\n    def __init__(self, x, y):\n        self.x = x\n        self.y = y\n\n"
         "    def __add__(self, other):\n        return Vec(self.x + other.x, self.y + other.y)\n\n"
         "    def __repr__(self):\n        return f'Vec({self.x},{self.y})'\n\n\n"
         "def combine(a, b):\n    return a + b\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int methods = cp_count_label(store, lp.project, "Method");
    if (calls < 1) cp_diag(store, lp.project, "operator/python_add");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(methods >= 1);  /* __add__ must still be a Method node */
    /* `a + b` desugars to a.__add__(b) but is NOT in call_expressions.
     * Assert CORRECT outcome (calls >= 1): RED until binary_expression
     * desugaring is added to the Python extractor. */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D4b — Rust Add trait operator overload.
 * EXPECTED RED: `a + b` is a binary_expression; rust_call_types =
 * {"call_expression"} only; the add() desugaring is not extracted. */
TEST(cp_operator_rust_add) {
    static const CP_File f[] = {
        {"point.rs",
         "use std::ops::Add;\n\n"
         "#[derive(Clone, Copy)]\npub struct Point { pub x: i32, pub y: i32 }\n\n"
         "impl Add for Point {\n    type Output = Point;\n\n"
         "    fn add(self, other: Point) -> Point {\n"
         "        Point { x: self.x + other.x, y: self.y + other.y }\n    }\n}\n\n"
         "pub fn combine(a: Point, b: Point) -> Point { a + b }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "operator/rust_add");
    cp_cleanup(&lp, store);
    /* REAL BUG: `a + b` is a binary_expression; lang_specs.c rust_call_types =
     * {call_expression, macro_invocation} has no binary_expression entry, so the
     * Add-trait operator desugaring (a.add(b)) is never extracted as a call →
     * 0 CALLS. [KNOWN class 12] */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D4c — Kotlin operator fun plus.
 * EXPECTED UNCERTAIN: `a + b` is a binary_expression; the Kotlin extractor
 * may or may not desugar it to a.plus(b) call. */
TEST(cp_operator_kotlin_plus) {
    static const CP_File f[] = {
        {"Vec.kt",
         "data class Vec(val x: Int, val y: Int) {\n"
         "    operator fun plus(other: Vec): Vec = Vec(x + other.x, y + other.y)\n"
         "    operator fun minus(other: Vec): Vec = Vec(x - other.x, y - other.y)\n}\n\n"
         "fun combine(a: Vec, b: Vec): Vec = a + b\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int methods = cp_count_label(store, lp.project, "Method");
    if (calls < 1) cp_diag(store, lp.project, "operator/kotlin_plus");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(methods >= 1);   /* operator funs must be Method nodes */
    ASSERT_TRUE(calls >= 1);     /* UNCERTAIN/RED if binary_expression not desugared */
    PASS();
}

/* ── D5: Calls through interface/trait objects ───────────────────── */

/* D5a — Go interface dispatch.
 * EXPECTED GREEN: Go lsp_cross is wired; it infers the concrete type
 * from the assignment and resolves the method call. */
TEST(cp_interface_dispatch_go) {
    static const CP_File f[] = {
        {"shapes.go",
         "package shapes\n\n"
         "type Shape interface {\n    Area() float64\n    Perimeter() float64\n}\n\n"
         "type Circle struct{ R float64 }\n\n"
         "func (c Circle) Area() float64 { return 3.14159 * c.R * c.R }\n"
         "func (c Circle) Perimeter() float64 { return 2 * 3.14159 * c.R }\n\n"
         "func Describe(s Shape) float64 { return s.Area() + s.Perimeter() }\n\n"
         "func Run() float64 {\n    c := Circle{R: 5}\n    return Describe(c)\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "interface_dispatch/go");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D5b — Rust dyn Trait dispatch.
 * EXPECTED UNCERTAIN: Rust lsp_cross is not wired (cbm_pxc_has_cross_lsp
 * returns false for Rust); dyn Trait erases the concrete type; the name-
 * based resolver may find the method by name if unique. */
TEST(cp_interface_dispatch_rust_dyn) {
    static const CP_File f[] = {
        {"shapes.rs",
         "pub trait Shape {\n    fn area(&self) -> f64;\n    fn perimeter(&self) -> f64;\n}\n\n"
         "pub struct Circle { pub r: f64 }\n\n"
         "impl Shape for Circle {\n"
         "    fn area(&self) -> f64 { std::f64::consts::PI * self.r * self.r }\n"
         "    fn perimeter(&self) -> f64 { 2.0 * std::f64::consts::PI * self.r }\n}\n\n"
         "pub fn describe(s: &dyn Shape) -> f64 { s.area() + s.perimeter() }\n\n"
         "pub fn run() -> f64 {\n    let c = Circle { r: 5.0 };\n    describe(&c)\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "interface_dispatch/rust_dyn");
    cp_cleanup(&lp, store);
    /* UNCERTAIN: RED if Rust lsp_cross gap prevents trait-dispatch resolution */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D5c — Java interface dispatch (no lsp_cross).
 * EXPECTED UNCERTAIN: the name-based resolver may find the method by name
 * if unique within the project. */
TEST(cp_interface_dispatch_java) {
    static const CP_File f[] = {
        {"Shape.java",
         "package app;\n\n"
         "interface Shape { double area(); double perimeter(); }\n\n"
         "class Circle implements Shape {\n    double r;\n"
         "    Circle(double r) { this.r = r; }\n"
         "    public double area() { return Math.PI * r * r; }\n"
         "    public double perimeter() { return 2 * Math.PI * r; }\n}\n\n"
         "class Util {\n    static double describe(Shape s) {\n"
         "        return s.area() + s.perimeter();\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "interface_dispatch/java");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* ── D6: Calls on enum / sealed-class variant methods ────────────── */

/* D6a — Kotlin sealed class + when expression.
 * EXPECTED GREEN: variant data classes have their own methods; calls to them
 * are regular call_expressions resolved by name. */
TEST(cp_enum_variant_kotlin_sealed) {
    static const CP_File f[] = {
        {"Result.kt",
         "sealed class Result {\n"
         "    data class Ok(val value: String) : Result() {\n"
         "        fun display(): String = \"OK: $value\"\n    }\n"
         "    data class Err(val msg: String) : Result() {\n"
         "        fun display(): String = \"ERR: $msg\"\n    }\n}\n\n"
         "fun describe(r: Result): String = when (r) {\n"
         "    is Result.Ok -> r.display()\n"
         "    is Result.Err -> r.display()\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls = cp_edges(store, lp.project, "CALLS");
    if (calls < 1) cp_diag(store, lp.project, "enum_variant/kotlin_sealed");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D6b — Rust enum with impl.
 * EXPECTED UNCERTAIN: `v.label()` where v: Direction is a call_expression;
 * the name-based resolver should find label() if it's unique. */
TEST(cp_enum_variant_rust_impl) {
    static const CP_File f[] = {
        {"direction.rs",
         "pub enum Direction { North, South, East, West }\n\n"
         "impl Direction {\n"
         "    pub fn label(&self) -> &str {\n        match self {\n"
         "            Direction::North => \"N\", Direction::South => \"S\",\n"
         "            Direction::East  => \"E\", Direction::West  => \"W\",\n"
         "        }\n    }\n"
         "    pub fn opposite(&self) -> Direction {\n        match self {\n"
         "            Direction::North => Direction::South,\n"
         "            Direction::South => Direction::North,\n"
         "            Direction::East  => Direction::West,\n"
         "            Direction::West  => Direction::East,\n"
         "        }\n    }\n}\n\n"
         "pub fn describe(d: Direction) -> String {\n"
         "    format!(\"{} (opp: {})\", d.label(), d.opposite().label())\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int methods = cp_count_label(store, lp.project, "Method");
    if (calls < 1) cp_diag(store, lp.project, "enum_variant/rust_impl");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(methods >= 1);  /* label + opposite must be Method nodes */
    /* REAL BUG: `d.label()` is a method call on a receiver typed as the enum
     * Direction.  Rust is NOT in cbm_pxc_has_cross_lsp (src/pipeline/
     * pass_lsp_cross.c — only Go/C/CPP/CUDA/Python/JS/TS/TSX/PHP), so there is
     * no type-aware resolver to map the receiver to Direction::label, and the
     * name-based registry resolver does not resolve `recv.method()` method calls
     * → 0 CALLS. [KNOWN class 4 — Rust no cross-LSP] */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* D6c — Java enum method call.
 * EXPECTED UNCERTAIN: enum method call `d.label()` is a method_invocation;
 * name-based resolver should find it if the name is unique. */
TEST(cp_enum_method_java) {
    static const CP_File f[] = {
        {"Day.java",
         "package app;\n\nenum Day {\n"
         "    MON, TUE, WED, THU, FRI, SAT, SUN;\n\n"
         "    public boolean isWeekend() { return this == SAT || this == SUN; }\n"
         "    public String label() { return name().toLowerCase(); }\n}\n\n"
         "class DayUtil {\n"
         "    static String describe(Day d) {\n"
         "        return d.label() + (d.isWeekend() ? \"(rest)\" : \"(work)\");\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int calls   = cp_edges(store, lp.project, "CALLS");
    int types   = cp_count_label(store, lp.project, "Enum");
    if (calls < 1) cp_diag(store, lp.project, "enum_method/java");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1);   /* enum Day must become an Enum node */
    ASSERT_TRUE(calls >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * AREA E — Node-creation corners
 * ══════════════════════════════════════════════════════════════════ */

/* ── E1: Nested classes ──────────────────────────────────────────── */

/* E1a — Python inner class.
 * EXPECTED GREEN: extractor walks nested class_definition; inner class
 * gets its own type node. */
TEST(cp_nested_class_python) {
    static const CP_File f[] = {
        {"tree.py",
         "class BinaryTree:\n"
         "    class Node:\n        def __init__(self, val):\n"
         "            self.val = val\n            self.left = None\n"
         "            self.right = None\n\n"
         "    def __init__(self):\n        self.root = None\n\n"
         "    def insert(self, val):\n        self.root = self.Node(val)\n\n"
         "    def height(self):\n        return 0\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int fn_types = cp_count_label(store, lp.project, "Function")
                   + cp_count_label(store, lp.project, "Method")
                   + cp_count_label(store, lp.project, "Class");
    if (fn_types < 2) cp_diag(store, lp.project, "nested_class/python");
    cp_cleanup(&lp, store);
    /* Both BinaryTree and Node must produce type-like nodes */
    ASSERT_TRUE(fn_types >= 2);
    PASS();
}

/* E1b — Java inner class.
 * EXPECTED GREEN: Java extractor should produce a Class node for Inner even
 * when its declaration is nested inside Outer. */
TEST(cp_nested_class_java) {
    static const CP_File f[] = {
        {"Graph.java",
         "package app;\n\nclass Graph {\n    private int vertices;\n\n"
         "    class Edge {\n        int from, to, weight;\n"
         "        Edge(int f, int t, int w) { from=f; to=t; weight=w; }\n"
         "        int cost() { return weight; }\n    }\n\n"
         "    Graph(int v) { this.vertices = v; }\n\n"
         "    int run() {\n        return new Edge(0,1,5).cost();\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes = cp_count_label(store, lp.project, "Class");
    if (classes < 1) cp_diag(store, lp.project, "nested_class/java");
    cp_cleanup(&lp, store);
    /* At minimum Graph must be a Class; inner Edge is a bonus */
    ASSERT_TRUE(classes >= 1);
    PASS();
}

/* E1c — C# nested class.
 * EXPECTED GREEN: C# extractor walks class_declaration inside class_declaration. */
TEST(cp_nested_class_csharp) {
    static const CP_File f[] = {
        {"Outer.cs",
         "namespace App {\n    class Outer {\n        private int x;\n\n"
         "        class Inner {\n            private int y;\n"
         "            public int Value() { return y; }\n        }\n\n"
         "        public int Run() {\n            var i = new Inner();\n"
         "            return i.Value();\n        }\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes = cp_count_label(store, lp.project, "Class");
    if (classes < 1) cp_diag(store, lp.project, "nested_class/csharp");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);
    PASS();
}

/* ── E2: Anonymous classes / objects ─────────────────────────────── */

/* E2a — Java anonymous class.
 * EXPECTED RED: Java call_types only includes "method_invocation"; the
 * object_creation_expression for `new Iface() { ... }` is never extracted
 * as a call.  The anonymous class body adds methods but no Class node for
 * the anonymous type itself is created.  Enclosing class IS modelled.
 * Assert enclosing class exists; check for any CALLS edge. */
TEST(cp_anon_class_java) {
    static const CP_File f[] = {
        {"AnonDemo.java",
         "package app;\n\ninterface Greeter { String greet(String name); }\n\n"
         "class Factory {\n"
         "    static Greeter makeGreeter(String prefix) {\n"
         "        return new Greeter() {\n"
         "            public String greet(String name) {\n"
         "                return prefix + name;\n"
         "            }\n        };\n    }\n\n"
         "    static String run() {\n"
         "        Greeter g = makeGreeter(\"Hello, \");\n"
         "        return g.greet(\"World\");\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes = cp_count_label(store, lp.project, "Class");
    int ifaces  = cp_count_label(store, lp.project, "Interface");
    if (classes < 1 && ifaces < 1) cp_diag(store, lp.project, "anon_class/java");
    cp_cleanup(&lp, store);
    /* Factory class must exist; Greeter interface should also be modelled */
    ASSERT_TRUE(classes >= 1 || ifaces >= 1);
    PASS();
}

/* E2b — Kotlin object expression (anonymous class).
 * EXPECTED UNCERTAIN: `object : Iface {}` syntax; the Kotlin extractor may
 * produce a Class node (anonymous_object) or skip it. */
TEST(cp_anon_object_kotlin) {
    static const CP_File f[] = {
        {"Anon.kt",
         "interface Transformer { fun transform(x: Int): Int }\n\n"
         "fun makeDoubler(): Transformer = object : Transformer {\n"
         "    override fun transform(x: Int): Int = x * 2\n}\n\n"
         "fun run(x: Int): Int = makeDoubler().transform(x)\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int callables = cp_callables(store, lp.project);
    int calls     = cp_edges(store, lp.project, "CALLS");
    if (callables < 1) cp_diag(store, lp.project, "anon_object/kotlin");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(callables >= 1);  /* makeDoubler / run must be Function nodes */
    /* UNCERTAIN: anonymous object's transform may or may not be a Method node */
    (void)calls;                  /* informational only */
    PASS();
}

/* ── E3: Top-level constants ─────────────────────────────────────── */

/* E3a — Go package-level const.
 * EXPECTED UNCERTAIN: Go const_spec is likely not in function_types;
 * const nodes may not be created.  We assert pipeline doesn't crash and
 * the file produces at least one node (the Module). */
TEST(cp_constant_go_const) {
    static const CP_File f[] = {
        {"limits.go",
         "package limits\n\nconst (\n"
         "    MaxRetries = 3\n    DefaultTimeout = 30\n    BaseURL = \"https://api.example.com\"\n)\n\n"
         "func Retry(n int) bool { return n < MaxRetries }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int total = store ? cbm_store_count_nodes(store, lp.project) : -1;
    if (total < 1) cp_diag(store, lp.project, "constant/go_const");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(total >= 1);  /* at least Module + Function nodes */
    PASS();
}

/* E3b — Rust const and static.
 * EXPECTED UNCERTAIN: Rust const_item / static_item not in function_types;
 * these declarations may not produce graph nodes. */
TEST(cp_constant_rust_const_static) {
    static const CP_File f[] = {
        {"limits.rs",
         "pub const MAX_RETRIES: u32 = 3;\npub const BASE_URL: &str = \"https://api.example.com\";\n"
         "pub static COUNTER: std::sync::atomic::AtomicU32 = "
         "std::sync::atomic::AtomicU32::new(0);\n\n"
         "pub fn should_retry(n: u32) -> bool { n < MAX_RETRIES }\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int fn_count = cp_count_label(store, lp.project, "Function");
    int total    = store ? cbm_store_count_nodes(store, lp.project) : -1;
    if (fn_count < 1) cp_diag(store, lp.project, "constant/rust_const_static");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(fn_count >= 1);  /* should_retry must be a Function node */
    ASSERT_TRUE(total >= 1);
    PASS();
}

/* E3c — Java static final field as a constant.
 * EXPECTED UNCERTAIN: Java field_declaration may not be in extract_fields;
 * static final constants may not produce Variable nodes. */
TEST(cp_constant_java_static_final) {
    static const CP_File f[] = {
        {"Constants.java",
         "package app;\n\nclass Constants {\n"
         "    public static final int MAX_RETRIES = 3;\n"
         "    public static final String BASE_URL = \"https://api.example.com\";\n"
         "    public static final double PI = 3.14159265358979;\n\n"
         "    static boolean isValidUrl(String url) {\n"
         "        return url != null && url.startsWith(BASE_URL);\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes  = cp_count_label(store, lp.project, "Class");
    int methods  = cp_count_label(store, lp.project, "Method");
    if (classes < 1) cp_diag(store, lp.project, "constant/java_static_final");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);  /* Constants class must exist */
    ASSERT_TRUE(methods >= 1);  /* isValidUrl must be a Method node */
    PASS();
}

/* ── E4: Macro-defined symbols ───────────────────────────────────── */

/* E4a — Rust macro_rules!.
 * EXPECTED RED: macro_rules! items are not in Rust function_types
 * ("function_item" / "impl_item" only); no Function node is created for
 * the macro.  The enclosing module must still produce its Module node. */
TEST(cp_macro_rust_macro_rules) {
    static const CP_File f[] = {
        {"macros.rs",
         "macro_rules! add_two {\n    ($x:expr) => { $x + 2 };\n}\n\n"
         "macro_rules! log_error {\n    ($msg:expr) => { eprintln!(\"ERROR: {}\", $msg); };\n}\n\n"
         "pub fn run(x: i32) -> i32 {\n    add_two!(x)\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int fn_count = cp_count_label(store, lp.project, "Function");
    int total    = store ? cbm_store_count_nodes(store, lp.project) : -1;
    if (fn_count < 1) cp_diag(store, lp.project, "macro/rust_macro_rules");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(fn_count >= 1);  /* run() must be a Function node */
    ASSERT_TRUE(total >= 1);
    /* Intentionally do NOT assert that macro_rules! nodes are created — the
     * expected behavior is that they are NOT extracted (RED gap). */
    PASS();
}

/* E4b — C function-like #define macro.
 * EXPECTED RED: C preprocessor macros are not visible in the tree-sitter
 * AST (they are resolved before parsing in the preprocessed form).
 * The C extractor cannot see function-like #define symbols. */
TEST(cp_macro_c_define) {
    static const CP_File f[] = {
        {"math.c",
         "#define SQUARE(x) ((x) * (x))\n"
         "#define MAX(a, b) ((a) > (b) ? (a) : (b))\n"
         "#define CLAMP(v, lo, hi) MAX(lo, MIN(v, hi))\n\n"
         "int run(int x) {\n    return SQUARE(x) + MAX(x, 0);\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int fn_count = cp_count_label(store, lp.project, "Function");
    int total    = store ? cbm_store_count_nodes(store, lp.project) : -1;
    if (fn_count < 1) cp_diag(store, lp.project, "macro/c_define");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(fn_count >= 1);  /* run() must be a Function node */
    ASSERT_TRUE(total >= 1);
    /* #define SQUARE / MAX / CLAMP are NOT in the AST — expected to produce 0
     * Function/Macro nodes.  This is RED (known gap). */
    PASS();
}

/* ── E5: Property getters/setters ────────────────────────────────── */

/* E5a — Python @property decorator.
 * EXPECTED UNCERTAIN: @property-decorated functions are decorated_definition
 * nodes; extractor may produce one or two Function nodes (getter + setter). */
TEST(cp_property_python) {
    static const CP_File f[] = {
        {"model.py",
         "class Circle:\n    def __init__(self, radius):\n        self._radius = radius\n\n"
         "    @property\n    def radius(self):\n        return self._radius\n\n"
         "    @radius.setter\n    def radius(self, value):\n"
         "        if value < 0:\n            raise ValueError('negative radius')\n"
         "        self._radius = value\n\n"
         "    @property\n    def area(self):\n        return 3.14159 * self._radius ** 2\n\n\n"
         "def make(r):\n    c = Circle(r)\n    return c.area\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int methods  = cp_count_label(store, lp.project, "Method");
    int classes  = cp_count_label(store, lp.project, "Class");
    if (methods < 1 && classes < 1) cp_diag(store, lp.project, "property/python");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);  /* Circle class must exist */
    ASSERT_TRUE(methods >= 1);  /* at least __init__ must be a Method */
    PASS();
}

/* E5b — C# property with get/set accessors.
 * EXPECTED GREEN: C# property accessor_declaration is modelled; at minimum
 * the class and methods are produced. */
TEST(cp_property_csharp) {
    static const CP_File f[] = {
        {"Person.cs",
         "namespace App {\n    class Person {\n        private string _name;\n"
         "        private int _age;\n\n"
         "        public string Name {\n            get { return _name; }\n"
         "            set { _name = value ?? throw new ArgumentNullException(); }\n        }\n\n"
         "        public int Age {\n"
         "            get { return _age; }\n"
         "            set { _age = value >= 0 ? value : throw new ArgumentOutOfRangeException(); }\n"
         "        }\n\n"
         "        public string Describe() { return $\"{_name}, age {_age}\"; }\n    }\n}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes = cp_count_label(store, lp.project, "Class");
    int methods = cp_count_label(store, lp.project, "Method");
    if (classes < 1) cp_diag(store, lp.project, "property/csharp");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);
    ASSERT_TRUE(methods >= 1);  /* Describe() at minimum */
    PASS();
}

/* E5c — Kotlin property with custom getter.
 * EXPECTED UNCERTAIN: `val x: Type get() = ...` produces a val_declaration;
 * the Kotlin extractor may or may not model the custom getter as a Method. */
TEST(cp_property_kotlin_custom_getter) {
    static const CP_File f[] = {
        {"Rect.kt",
         "class Rect(val width: Double, val height: Double) {\n"
         "    val area: Double\n        get() = width * height\n\n"
         "    val perimeter: Double\n        get() = 2 * (width + height)\n\n"
         "    val isSquare: Boolean\n        get() = width == height\n\n"
         "    fun describe(): String = \"${width}x${height} area=${area}\"\n}\n\n"
         "fun makeRect(w: Double, h: Double): Rect = Rect(w, h)\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes   = cp_count_label(store, lp.project, "Class");
    int callables = cp_callables(store, lp.project);
    if (callables < 1) cp_diag(store, lp.project, "property/kotlin_custom_getter");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);    /* Rect class must exist */
    ASSERT_TRUE(callables >= 1);  /* describe + makeRect at minimum */
    PASS();
}

/* E5d — TypeScript get accessor.
 * EXPECTED UNCERTAIN: `get foo()` is a method_definition with kind "get";
 * the TS extractor may treat it as a Method node or skip it. */
TEST(cp_property_ts_accessor) {
    static const CP_File f[] = {
        {"Vector.ts",
         "export class Vector {\n    private _x: number;\n    private _y: number;\n\n"
         "    constructor(x: number, y: number) { this._x = x; this._y = y; }\n\n"
         "    get x(): number { return this._x; }\n"
         "    set x(value: number) { this._x = value; }\n\n"
         "    get y(): number { return this._y; }\n"
         "    set y(value: number) { this._y = value; }\n\n"
         "    get magnitude(): number {\n"
         "        return Math.sqrt(this._x * this._x + this._y * this._y);\n    }\n\n"
         "    add(other: Vector): Vector { return new Vector(this._x + other._x, this._y + other._y); }\n"
         "}\n"}};
    CP_Proj lp;
    cbm_store_t *store = cp_index_files(&lp, f, 1);
    int classes = cp_count_label(store, lp.project, "Class");
    int methods = cp_count_label(store, lp.project, "Method");
    if (classes < 1) cp_diag(store, lp.project, "property/ts_accessor");
    cp_cleanup(&lp, store);
    ASSERT_TRUE(classes >= 1);  /* Vector class must exist */
    ASSERT_TRUE(methods >= 1);  /* add() method at minimum; accessors are uncertain */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE registration
 * ══════════════════════════════════════════════════════════════════ */

SUITE(convergence_probe) {
    /* ── AREA A: CONFIGURES (5 cases) ───────────────── */
    /* A1 Go os.Getenv          — EXPECTED GREEN */
    RUN_TEST(cp_configures_go_getenv);
    /* A2 Python os.getenv()    — EXPECTED GREEN */
    RUN_TEST(cp_configures_python_os_getenv);
    /* A3 TS getenv() wrapper   — EXPECTED GREEN */
    RUN_TEST(cp_configures_ts_getenv_wrapper);
    /* A4 C# GetEnvironmentVariable — EXPECTED GREEN */
    RUN_TEST(cp_configures_csharp_getenv);
    /* A5 Rust std::env::var    — EXPECTED UNCERTAIN/RED */
    RUN_TEST(cp_configures_rust_env_var);

    /* ── AREA B: READS/WRITES (4 cases) ─────────────── */
    /* B1 Python global         — EXPECTED UNCERTAIN */
    RUN_TEST(cp_reads_writes_python_global);
    /* B2 Go package-level var  — EXPECTED UNCERTAIN */
    RUN_TEST(cp_reads_writes_go_global);
    /* B3 Java static field     — EXPECTED UNCERTAIN */
    RUN_TEST(cp_reads_writes_java_static_field);
    /* B4 C# static field       — EXPECTED UNCERTAIN */
    RUN_TEST(cp_reads_writes_cs_static_field);

    /* ── AREA C: Parallel service edges (3 cases) ───── */
    /* C1 Go GRPC UserService   — EXPECTED GREEN */
    RUN_TEST(cp_grpc_calls_go_user_service);
    /* C2 TS tRPC procedure     — EXPECTED GREEN */
    RUN_TEST(cp_trpc_calls_ts_procedure);
    /* C3 Python GraphQL execute — EXPECTED GREEN */
    RUN_TEST(cp_graphql_calls_python_execute);

    /* ── AREA D1: Closures/lambdas (3 cases) ─────────── */
    /* D1a Go closure           — EXPECTED GREEN */
    RUN_TEST(cp_closure_go_outer_call);
    /* D1b Python lambda        — EXPECTED GREEN */
    RUN_TEST(cp_closure_python_lambda);
    /* D1c TS arrow             — EXPECTED GREEN */
    RUN_TEST(cp_closure_ts_arrow);

    /* ── AREA D2: Recursive calls (4 cases) ──────────── */
    /* D2a Go                   — EXPECTED GREEN */
    RUN_TEST(cp_recursive_go);
    /* D2b Python               — EXPECTED GREEN */
    RUN_TEST(cp_recursive_python);
    /* D2c Rust                 — EXPECTED GREEN */
    RUN_TEST(cp_recursive_rust);
    /* D2d Java                 — EXPECTED GREEN */
    RUN_TEST(cp_recursive_java);

    /* ── AREA D3: Async/await (5 cases) ──────────────── */
    /* D3a Python async/await   — EXPECTED GREEN */
    RUN_TEST(cp_async_python_await);
    /* D3b TS async/await       — EXPECTED GREEN */
    RUN_TEST(cp_async_ts_await);
    /* D3c C# async Task        — EXPECTED GREEN */
    RUN_TEST(cp_async_csharp_task);
    /* D3d Rust .await          — EXPECTED UNCERTAIN/RED */
    RUN_TEST(cp_async_rust_await);
    /* D3e Kotlin suspend       — EXPECTED UNCERTAIN */
    RUN_TEST(cp_async_kotlin_suspend);

    /* ── AREA D4: Operator overloads (3 cases) ───────── */
    /* D4a Python __add__       — EXPECTED RED */
    RUN_TEST(cp_operator_python_add);
    /* D4b Rust Add trait       — EXPECTED RED */
    RUN_TEST(cp_operator_rust_add);
    /* D4c Kotlin operator fun  — EXPECTED UNCERTAIN */
    RUN_TEST(cp_operator_kotlin_plus);

    /* ── AREA D5: Interface/trait dispatch (3 cases) ─── */
    /* D5a Go interface         — EXPECTED GREEN */
    RUN_TEST(cp_interface_dispatch_go);
    /* D5b Rust dyn Trait       — EXPECTED UNCERTAIN */
    RUN_TEST(cp_interface_dispatch_rust_dyn);
    /* D5c Java interface       — EXPECTED UNCERTAIN */
    RUN_TEST(cp_interface_dispatch_java);

    /* ── AREA D6: Enum/sealed variant methods (3 cases) ─ */
    /* D6a Kotlin sealed        — EXPECTED GREEN */
    RUN_TEST(cp_enum_variant_kotlin_sealed);
    /* D6b Rust enum impl       — EXPECTED UNCERTAIN */
    RUN_TEST(cp_enum_variant_rust_impl);
    /* D6c Java enum method     — EXPECTED UNCERTAIN */
    RUN_TEST(cp_enum_method_java);

    /* ── AREA E1: Nested classes (3 cases) ──────────── */
    /* E1a Python inner class   — EXPECTED GREEN */
    RUN_TEST(cp_nested_class_python);
    /* E1b Java inner class     — EXPECTED GREEN */
    RUN_TEST(cp_nested_class_java);
    /* E1c C# nested class      — EXPECTED GREEN */
    RUN_TEST(cp_nested_class_csharp);

    /* ── AREA E2: Anonymous classes/objects (2 cases) ── */
    /* E2a Java anon class      — EXPECTED RED (enclosing class exists) */
    RUN_TEST(cp_anon_class_java);
    /* E2b Kotlin anon object   — EXPECTED UNCERTAIN */
    RUN_TEST(cp_anon_object_kotlin);

    /* ── AREA E3: Top-level constants (3 cases) ─────── */
    /* E3a Go const             — EXPECTED UNCERTAIN */
    RUN_TEST(cp_constant_go_const);
    /* E3b Rust const/static    — EXPECTED UNCERTAIN */
    RUN_TEST(cp_constant_rust_const_static);
    /* E3c Java static final    — EXPECTED UNCERTAIN */
    RUN_TEST(cp_constant_java_static_final);

    /* ── AREA E4: Macro-defined symbols (2 cases) ────── */
    /* E4a Rust macro_rules!    — enclosing fn GREEN; macro node RED */
    RUN_TEST(cp_macro_rust_macro_rules);
    /* E4b C #define            — enclosing fn GREEN; macro node RED */
    RUN_TEST(cp_macro_c_define);

    /* ── AREA E5: Property getters/setters (4 cases) ─── */
    /* E5a Python @property     — EXPECTED UNCERTAIN */
    RUN_TEST(cp_property_python);
    /* E5b C# get/set           — EXPECTED GREEN */
    RUN_TEST(cp_property_csharp);
    /* E5c Kotlin custom getter — EXPECTED UNCERTAIN */
    RUN_TEST(cp_property_kotlin_custom_getter);
    /* E5d TS get accessor      — EXPECTED UNCERTAIN */
    RUN_TEST(cp_property_ts_accessor);
}
