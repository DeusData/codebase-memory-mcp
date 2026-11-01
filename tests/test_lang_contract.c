/*
 * test_lang_contract.c — Per-language GRAPH-CONTRACT regression suite.
 *
 * Unlike the in-process unit tests (which exercise tiny inline snippets and
 * passed while real behavioral bugs shipped), this suite indexes a per-language
 * fixture through the FULL pipeline into a real graph DB and asserts INVARIANT
 * contracts on the result: expected node/edge TYPES are present, calls are
 * attributed to the calling Function (not the file/Module), resolution happens,
 * and extraction does not crash.
 *
 * Design notes:
 *   - INVARIANTS, not golden snapshots: edge counts are non-deterministic
 *     (parallel LSH/similarity/resolve), so we assert PRESENCE + floors, never
 *     exact counts. This survives grammar refreshes without churn.
 *   - Crashes are caught via a FORKED subprocess + exit-signal check: ASan does
 *     NOT intercept SIGBUS, so an in-process crash would kill the test runner.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "grammar_cases.h"
#include "resolver_quality.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>
#include <sqlite3.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <sys/wait.h> /* fork/waitpid crash isolation — POSIX only */
#endif

/* ── Shared harness: index one fixture file through the production pipeline ── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} LangProj;

typedef struct {
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} LangFile;

/* Given lp->tmpdir already populated with source files, run the production
 * index_repository flow and open the resulting graph DB (NULL on failure).
 * Split out of lang_index_files so a caller can interpose between writing files
 * and indexing (e.g. the FILE_CHANGES_WITH test git-inits + commits first). */

/* Normalize backslashes to forward slashes in place. cbm_mkdtemp on Windows
 * (msys2) yields a native path with backslashes (e.g. D:\a\_temp\...); those
 * break the JSON repo_path ("\a"/"\t" are invalid JSON escapes → index fails →
 * count=-1) and produce mixed-separator paths in git -C args. Forward slashes
 * are valid in JSON and accepted by Windows file APIs and git. */
static void lc_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
}

static cbm_store_t *lang_open_indexed(LangProj *lp) {
    /* Freed before reassigning: a fixture that indexes more than once would
     * otherwise drop the previous heap name on the floor. Teardown frees the
     * last one. */
    free(lp->project);
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) {
        return NULL;
    }
    char cache_dir[512];
    const char *configured_cache = getenv("CBM_CACHE_DIR");
    if (configured_cache && configured_cache[0]) {
        snprintf(cache_dir, sizeof(cache_dir), "%s", configured_cache);
    } else {
        const char *home = getenv("HOME");
        if (!home) {
            home = "/tmp";
        }
        snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    }
    cbm_mkdir_p(cache_dir, 0755);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) {
        return NULL;
    }
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) {
        free(resp);
    }
    return cbm_store_open_path(lp->dbpath);
}

/* Write each fixture file into a fresh temp project, index it via the MCP
 * production flow (discover -> extract -> registry -> resolve -> dump), and open
 * the resulting graph DB. Returns the store (NULL on any failure). */
static cbm_store_t *lang_index_files(LangProj *lp, const LangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_lc_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) {
        return NULL;
    }
    lc_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        /* Create parent directories if the fixture name embeds subdirs.
         * Use mkdir_p (not mkdir) so multi-level paths like
         * "pkg/validation/x.go" create every intermediate directory. */
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        /* Binary mode: keep fixture line endings exactly as written ("\n").
         * Windows text mode rewrites "\n"→"\r\n", which makes line-ending-
         * sensitive grammars under-extract (below_min / calls_breadth). */
        FILE *f = fopen(path, "wb");
        if (!f) {
            return NULL;
        }
        fputs(files[i].content, f);
        fclose(f);
    }

    return lang_open_indexed(lp);
}

/* Convenience: index a single fixture file. */
static cbm_store_t *lang_index(LangProj *lp, const char *filename, const char *content) {
    LangFile f = {filename, content};
    return lang_index_files(lp, &f, 1);
}

static void lang_cleanup(LangProj *lp, cbm_store_t *store) {
    if (store) {
        cbm_store_close(store);
    }
    if (lp->srv) {
        cbm_mcp_server_free(lp->srv);
        lp->srv = NULL;
    }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600];
    char shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Count nodes of `label` with >=1 outbound edge. Returns -1 on query error. */
static int label_with_outbound(cbm_store_t *store, const char *project, const char *label) {
    cbm_search_params_t p = {0};
    p.project = project;
    p.label = label;
    p.min_degree = 1;
    p.max_degree = -1;
    p.limit = 50;
    cbm_search_output_t out = {0};
    int n = -1;
    if (cbm_store_search(store, &p, &out) == CBM_STORE_OK) {
        n = out.count;
    }
    cbm_store_search_free(&out);
    return n;
}

/* Callables (Function OR Method) that have >=1 outbound edge — the cross-language
 * "calls are attributed to the calling routine, not lumped on the file/Module
 * node" invariant (a mis-attributed call would leave the callable at degree 0). */
static int callables_with_outbound(cbm_store_t *store, const char *project) {
    int fn = label_with_outbound(store, project, "Function");
    int mt = label_with_outbound(store, project, "Method");
    return (fn < 0 ? 0 : fn) + (mt < 0 ? 0 : mt);
}

/* Count nodes carrying `label`. Returns -1 on query error. */
static int count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK) {
        return -1;
    }
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Count "type-like" nodes across the labels different languages use for a
 * user-defined type (class/struct/interface/enum/trait/type alias). Lets a
 * contract assert "the type was modeled" without hard-coding one label. */
static int type_like_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = count_label(store, project, labels[i]);
        if (n > 0) {
            total += n;
        }
    }
    return total;
}

/* Run cbm_extract_file in a forked child; return true if the child died from a
 * signal (SIGBUS/SIGSEGV/...). ASan does not install a SIGBUS handler, so an
 * in-process crash would terminate the whole test runner — forking isolates it
 * and lets us assert "extraction must not crash" deterministically.
 *
 * Windows (msys2) has no fork()/waitpid(): run in-process. A genuine crash there
 * aborts the runner (a hard, visible failure), and the fork-isolated check still
 * runs on the POSIX CI legs, so coverage is preserved. */
static bool extract_crashes(const char *content, CBMLanguage lang, const char *relpath) {
#if defined(_WIN32)
    CBMFileResult *r =
        cbm_extract_file(content, (int)strlen(content), lang, "lc", relpath, 0, NULL, NULL);
    if (r) {
        cbm_free_result(r);
    }
    return false;
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        return false; /* cannot fork — do not flag a crash we can't observe */
    }
    if (pid == 0) {
        CBMFileResult *r =
            cbm_extract_file(content, (int)strlen(content), lang, "lc", relpath, 0, NULL, NULL);
        if (r) {
            cbm_free_result(r);
        }
        _exit(0);
    }
    int status = 0;
    (void)waitpid(pid, &status, 0);
    return WIFSIGNALED(status);
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  KNOWN-BUG CONTRACTS (must FAIL until fixed) — see P2
 * ══════════════════════════════════════════════════════════════════ */

/* Kotlin 0-IMPORTS bug — ROOT CAUSE: extraction layer. The Kotlin AST nests
 * imports as source_file -> import_list -> import_header*, but the extractor
 * matched the "import" keyword token as a direct child of root -> 0 imports.
 * This probe asserts cbm_extract_file captures them; it is the precise, reliable
 * reproduction + fix-verification (graph-layer IMPORTS-edge formation depends on
 * Maven/Gradle module resolution and is verified on the real ktor repo in the
 * P5 scale tier — a synthetic small fixture can't faithfully exercise it). */
static const char *KT_SRC = "import kotlin.io.path.Path\n"
                            "import java.util.ArrayList\n"
                            "\n"
                            "fun build(): ArrayList<String> {\n"
                            "    val xs = ArrayList<String>()\n"
                            "    xs.add(Path(\"/tmp/x\").toString())\n"
                            "    return xs\n"
                            "}\n";

TEST(contract_kotlin_imports_extracted) {
    CBMFileResult *r =
        cbm_extract_file(KT_SRC, (int)strlen(KT_SRC), CBM_LANG_KOTLIN, "lc", "a.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* 2 imports in the fixture */
    PASS();
}

/* C: a function that calls another must yield a CALLS edge attributed to the
 * CALLING FUNCTION (not lumped onto the file/Module node). */
static const char *C_SRC = "static int helper(int x) { return x + 1; }\n"
                           "int run(int y) {\n"
                           "    int a = helper(y);\n"
                           "    return helper(a);\n"
                           "}\n";

TEST(contract_c_calls_attributed_to_function) {
    LangProj lp;
    cbm_store_t *store = lang_index(&lp, "a.c", C_SRC);
    int calls = store ? cbm_store_count_edges_by_type(store, lp.project, "CALLS") : -1;
    int fn_callers = store ? callables_with_outbound(store, lp.project) : -1;
    lang_cleanup(&lp, store);
    ASSERT_TRUE(calls >= 1);      /* run() -> helper() */
    ASSERT_TRUE(fn_callers >= 1); /* the CALLS must hang off a Function, not Module */
    PASS();
}

/* Java: extraction must not crash on a real-world construct mix (enhanced-for +
 * method reference + method chain + pattern instanceof) — reproduces the SIGBUS. */
static const char *JAVA_SRC = "package zip;\n"
                              "import java.io.Closeable;\n"
                              "import java.lang.ref.Cleaner.Cleanable;\n"
                              "import java.util.ArrayList;\n"
                              "import java.util.List;\n"
                              "class Tracker {\n"
                              "    private final List<Cleanable> clean = new ArrayList<>();\n"
                              "    private final List<Closeable> close = new ArrayList<>();\n"
                              "    void assertAllClosed() throws Exception {\n"
                              "        for (Closeable closeable : this.close) {\n"
                              "            closeable.close();\n"
                              "        }\n"
                              "        this.clean.forEach(Cleanable::clean);\n"
                              "    }\n"
                              "    void added(Object obj, Cleanable cleanable) {\n"
                              "        if (obj instanceof Closeable closeable) {\n"
                              "            this.close.add(closeable);\n"
                              "        }\n"
                              "    }\n"
                              "}\n";

TEST(contract_java_extract_no_crash) {
    bool crashed = extract_crashes(JAVA_SRC, CBM_LANG_JAVA, "Tracker.java");
    ASSERT_TRUE(!crashed); /* CURRENTLY MAY FAIL: SIGBUS in the Java LSP */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  RICH PER-LANGUAGE CONTRACTS (P3) — invariants that must hold for each
 *  hybrid-LSP language on a clean same-file fixture: resolution produces
 *  CALLS, those CALLS are attributed to a callable (not the Module), and
 *  user-defined types are modeled. These guard against silent per-language
 *  regressions (a grammar/LSP change that stops resolving or mis-attributes).
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    int ok; /* graph DB opened */
    int calls;
    int callers; /* callables (Function/Method) with an outbound edge */
    int types;   /* type-like nodes */
    int imports;
} LangMetrics;

static LangMetrics lang_metrics(const LangFile *files, int nfiles) {
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, files, nfiles);
    LangMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.calls = cbm_store_count_edges_by_type(store, lp.project, "CALLS");
        m.callers = callables_with_outbound(store, lp.project);
        m.types = type_like_nodes(store, lp.project);
        m.imports = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
    }
    lang_cleanup(&lp, store);
    return m;
}

/* Go: same-package function call. */
TEST(contract_go_calls) {
    static const LangFile f[] = {{"svc.go",
                                  "package svc\n\n"
                                  "func helper(x int) int { return x + 1 }\n\n"
                                  "func run(y int) int {\n    return helper(helper(y))\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    PASS();
}

/* Rust: struct + impl methods + method call. */
TEST(contract_rust_methods) {
    static const LangFile f[] = {
        {"calc.rs", "struct Calc {\n    base: i32,\n}\n\n"
                    "impl Calc {\n"
                    "    fn helper(&self, x: i32) -> i32 { self.base + x }\n"
                    "    fn run(&self, y: i32) -> i32 { self.helper(y) }\n}\n\n"
                    "fn main() {\n    let c = Calc { base: 1 };\n    let _ = c.run(2);\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.types >= 1); /* struct Calc */
    PASS();
}

/* C#: class with methods + intra-class call. */
TEST(contract_csharp_methods) {
    static const LangFile f[] = {{"Calc.cs", "namespace App {\n"
                                             "    class Calc {\n"
                                             "        public int Helper(int x) { return x + 1; }\n"
                                             "        public int Run(int y) { return Helper(y); }\n"
                                             "    }\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.types >= 1); /* class Calc */
    PASS();
}

/* PHP: class with methods + $this-> call. */
TEST(contract_php_methods) {
    static const LangFile f[] = {
        {"Calc.php", "<?php\nclass Calc {\n"
                     "    public function helper($x) { return $x + 1; }\n"
                     "    public function run($y) { return $this->helper($y); }\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.types >= 1); /* class Calc */
    PASS();
}

/* Java: class with methods + intra-class call (single file — no crash risk). */
TEST(contract_java_methods) {
    static const LangFile f[] = {{"Calc.java", "package app;\n\nclass Calc {\n"
                                               "    int helper(int x) { return x + 1; }\n"
                                               "    int run(int y) { return helper(y); }\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.types >= 1); /* class Calc */
    PASS();
}

/* Kotlin: class with methods + intra-class call. */
TEST(contract_kotlin_methods) {
    static const LangFile f[] = {{"Calc.kt", "class Calc {\n"
                                             "    fun helper(x: Int): Int = x + 1\n"
                                             "    fun run(y: Int): Int = helper(y)\n}\n"}};
    LangMetrics m = lang_metrics(f, 1);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.types >= 1); /* class Calc */
    PASS();
}

/* Python: a RELATIVE import resolves to a sibling module — produces an IMPORTS
 * edge without needing a package manifest (resolve_relative_import), so this is
 * a reliable small-fixture graph-layer IMPORTS contract. */
TEST(contract_python_relative_import) {
    static const LangFile f[] = {
        {"util.py", "def helper(x):\n    return x + 1\n"},
        {"main.py", "from .util import helper\n\n\ndef run(y):\n    return helper(y)\n"}};
    LangMetrics m = lang_metrics(f, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.imports >= 1); /* IMPORTS edge for `from .util import helper` */
    PASS();
}

/* Python: relative aliased from-import must CALLS the real def (execute). */
TEST(contract_python_aliased_from_import_calls) {
    static const LangFile f[] = {{"gate.py", "def execute(x):\n    return x\n"},
                                 {"router.py",
                                  "from .gate import execute as bridge_execute\n\n\n"
                                  "def submit_task(y):\n    return bridge_execute(y)\n"}};
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, f, 2);
    ASSERT_TRUE(store != NULL);
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "submit_task", &nodes, &ncount),
              CBM_STORE_OK);
    ASSERT_TRUE(ncount >= 1);
    char **callers = NULL;
    char **callees = NULL;
    int n_callers = 0;
    int n_callees = 0;
    ASSERT_EQ(cbm_store_node_neighbor_names(store, nodes[0].id, 32, &callers, &n_callers, &callees,
                                            &n_callees),
              0);
    int saw_execute = 0;
    int saw_alias_ghost = 0;
    for (int i = 0; i < n_callees; i++) {
        if (callees[i] && strcmp(callees[i], "execute") == 0) {
            saw_execute = 1;
        }
        if (callees[i] && strcmp(callees[i], "bridge_execute") == 0) {
            saw_alias_ghost = 1;
        }
    }
    for (int i = 0; i < n_callers; i++) {
        free(callers[i]);
    }
    for (int i = 0; i < n_callees; i++) {
        free(callees[i]);
    }
    free(callers);
    free(callees);
    cbm_store_free_nodes(nodes, ncount);
    lang_cleanup(&lp, store);
    ASSERT_TRUE(saw_execute);
    ASSERT_FALSE(saw_alias_ghost);
    PASS();
}

/* Python: absolute aliased from-import must CALLS the real def (Yui router shape). */
TEST(contract_python_absolute_aliased_from_import_calls) {
    static const LangFile f[] = {
        {"services/satori_bridge/__init__.py", ""},
        {"services/satori_bridge/gate.py", "def execute(x):\n    return x\n"},
        {"services/yui_core/__init__.py", ""},
        {"services/yui_core/router.py",
         "from services.satori_bridge.gate import execute as bridge_execute\n\n\n"
         "def submit_task(y):\n    return bridge_execute(y)\n"},
        {"services/__init__.py", ""}};
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, f, 5);
    ASSERT_TRUE(store != NULL);
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "submit_task", &nodes, &ncount),
              CBM_STORE_OK);
    ASSERT_TRUE(ncount >= 1);
    char **callers = NULL;
    char **callees = NULL;
    int n_callers = 0;
    int n_callees = 0;
    ASSERT_EQ(cbm_store_node_neighbor_names(store, nodes[0].id, 32, &callers, &n_callers, &callees,
                                            &n_callees),
              0);
    int saw_execute = 0;
    int saw_alias_ghost = 0;
    for (int i = 0; i < n_callees; i++) {
        if (callees[i] && strcmp(callees[i], "execute") == 0) {
            saw_execute = 1;
        }
        if (callees[i] && strcmp(callees[i], "bridge_execute") == 0) {
            saw_alias_ghost = 1;
        }
    }
    for (int i = 0; i < n_callers; i++) {
        free(callers[i]);
    }
    for (int i = 0; i < n_callees; i++) {
        free(callees[i]);
    }
    free(callers);
    free(callees);
    cbm_store_free_nodes(nodes, ncount);
    lang_cleanup(&lp, store);
    ASSERT_TRUE(saw_execute);
    ASSERT_FALSE(saw_alias_ghost);
    PASS();
}

/* Python: unresolvable module + alias must emit no CALLS (no invented edge). */
TEST(contract_python_ghost_module_aliased_from_import_no_calls) {
    static const LangFile f[] = {{"router.py", "from ghost_module import nope as alias\n\n\n"
                                               "def submit_task(y):\n    return alias(y)\n"}};
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, f, 1);
    ASSERT_TRUE(store != NULL);
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    ASSERT_EQ(cbm_store_find_nodes_by_name(store, lp.project, "submit_task", &nodes, &ncount),
              CBM_STORE_OK);
    ASSERT_TRUE(ncount >= 1);
    char **callers = NULL;
    char **callees = NULL;
    int n_callers = 0;
    int n_callees = 0;
    ASSERT_EQ(cbm_store_node_neighbor_names(store, nodes[0].id, 32, &callers, &n_callers, &callees,
                                            &n_callees),
              0);
    int saw_any_callee = 0;
    for (int i = 0; i < n_callees; i++) {
        if (callees[i] && callees[i][0]) {
            saw_any_callee = 1;
        }
    }
    for (int i = 0; i < n_callers; i++) {
        free(callers[i]);
    }
    for (int i = 0; i < n_callees; i++) {
        free(callees[i]);
    }
    free(callers);
    free(callees);
    cbm_store_free_nodes(nodes, ncount);
    lang_cleanup(&lp, store);
    ASSERT_FALSE(saw_any_callee);
    PASS();
}

/* TypeScript: a relative import resolves to a sibling module → IMPORTS edge. */
TEST(contract_typescript_relative_import) {
    static const LangFile f[] = {
        {"util.ts", "export function helper(x: number): number {\n    return x + 1;\n}\n"},
        {"main.ts", "import { helper } from './util';\n\n"
                    "export function run(y: number): number {\n    return helper(y);\n}\n"}};
    LangMetrics m = lang_metrics(f, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.calls >= 1);
    ASSERT_TRUE(m.callers >= 1);
    ASSERT_TRUE(m.imports >= 1); /* IMPORTS edge for `import { helper } from './util'` */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  GRAPH-LEVEL BREADTH (P4) — every grammar's fixture, indexed through the
 *  full pipeline, must produce its defs as graph nodes. Indexes all fixtures
 *  in ONE pass (each in its own gNNN/ subdir to avoid filename collisions).
 *
 *  Discover maps files to a language by extension, so grammars whose extension
 *  collides with another's (e.g. .m objc/matlab, .yaml yaml/k8s) — or whose
 *  files discover intentionally skips — can't be isolated through index_repository.
 *  Those are allow-listed here and remain covered at the EXTRACTION level by
 *  test_grammar_regression.c (which uses an explicit language, bypassing discover).
 * ══════════════════════════════════════════════════════════════════ */

enum { GRAMMAR_BREADTH_MAX = 300, GRAMMAR_PATH_BUF = 96 };

/* Grammars not reachable as standalone indexed files via index_repository;
 * covered at the extraction level by test_grammar_regression.c instead. Reasons:
 *   nasm          — fixture ext .asm; discover maps .nasm (.asm is ambiguous across assemblers)
 *   dotenv        — discover does not route .env (often gitignored/secret)
 *   jsdoc, regex  — not standalone source files; the grammars parse content embedded in JS
 *   gitignore     — discover consumes .gitignore AS ignore rules, not as an indexed source
 *   gitattributes — same: consumed as a git metadata file
 *   sshconfig     — discover detects ssh_config / .ssh/config, not the generic name "config" */
static bool grammar_graph_allowlisted(const char *name) {
    static const char *allow[] = {"nasm",      "dotenv",        "jsdoc",     "regex",
                                  "gitignore", "gitattributes", "sshconfig", NULL};
    for (int i = 0; allow[i]; i++) {
        if (strcmp(allow[i], name) == 0) {
            return true;
        }
    }
    return false;
}

/* Non-structural node count for a file (excludes the File/Module/Folder/Package
 * wrappers the pipeline always creates) — i.e. how many defs reached the graph. */
static int def_nodes_for_file(cbm_store_t *store, const char *project, const char *rel) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_file(store, project, rel, &nodes, &count) != CBM_STORE_OK) {
        return -1;
    }
    int defs = 0;
    for (int i = 0; i < count; i++) {
        const char *l = nodes[i].label;
        if (l && strcmp(l, "File") != 0 && strcmp(l, "Module") != 0 && strcmp(l, "Folder") != 0 &&
            strcmp(l, "Package") != 0 && strcmp(l, "Directory") != 0) {
            defs++;
        }
    }
    cbm_store_free_nodes(nodes, count);
    return count == 0 ? -1 : defs; /* -1 signals "file not indexed at all" */
}

/* On a breadth failure, explain WHY: does direct extraction (no pipeline, no
 * timeout) yield defs, and what node labels did the pipeline actually create for
 * the file? Distinguishes "extraction itself broke" from "extraction works but
 * the def didn't reach the graph / got an unexpected label". */
static void breadth_diag(cbm_store_t *store, const char *project, const char *rel,
                         const GrammarCase *c) {
    CBMFileResult *dr =
        cbm_extract_file(c->src, (int)strlen(c->src), c->lang, "lc", c->path, 0, NULL, NULL);
    int direct = dr ? dr->defs.count : -1;
    const char *direct_label = (dr && dr->defs.count > 0) ? dr->defs.items[0].label : "-";
    char labels[256] = {0};
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_file(store, project, rel, &nodes, &count) == CBM_STORE_OK) {
        for (int i = 0; i < count && strlen(labels) < sizeof(labels) - 40; i++) {
            char one[48];
            snprintf(one, sizeof(one), "%s ", nodes[i].label ? nodes[i].label : "?");
            strncat(labels, one, sizeof(labels) - strlen(labels) - 1);
        }
    }
    cbm_store_free_nodes(nodes, count);
    fprintf(stderr, "      └─ direct_extract_defs=%d (label0=%s)  graph_labels=[%s]\n", direct,
            direct_label ? direct_label : "(null)", labels);
    if (dr) {
        cbm_free_result(dr);
    }
}

/* Preserve the Constant label contract and count exact cross-file references
 * that need a source-audited Chialisp include scope. */
TEST(contract_chialisp_constant_resolves_across_files) {
    static const LangFile f[] = {{"condition_codes.clib", "(\n  (defconstant CREATE_COIN 51)\n)\n"},
                                 {"puzzle.clsp", "(mod (ARG)\n"
                                                 "  (include condition_codes.clib)\n"
                                                 "  (defun run (amount)\n"
                                                 "    (list CREATE_COIN amount))\n"
                                                 ")\n"}};
    /* WHY: Constant remains a registry symbol, but registry membership cannot
     * establish include scope. Keep the exact missing reference in the quality
     * denominator until a source-audited Chialisp include/binding bridge exists.
     * Tried the original include and constant extraction; a same-named decoy
     * must never become the target, and the required ESM call must survive. */
    for (int parallel = 0; parallel < 2; parallel++) {
        enum { PADDING = 55 };
        LangFile all[4 + PADDING];
        all[0] = f[0];
        all[1] = f[1];
        all[2] = (LangFile){"decoy/condition_codes.clib", "(\n  (defconstant CREATE_COIN 99)\n)\n"};
        all[3] =
            (LangFile){"chialisp_control.mjs", "export function target() { return 1; }\n"
                                               "export function caller() { return target(); }\n"};
        char names[PADDING][64], texts[PADDING][64];
        for (int i = 0; parallel && i < PADDING; i++) {
            snprintf(names[i], sizeof(names[i]), "padding/pad_%02d.mjs", i);
            snprintf(texts[i], sizeof(texts[i]), "export const pad = %d;\n", i);
            all[4 + i] = (LangFile){names[i], texts[i]};
        }
        RQ_Source sources[4];
        for (int i = 0; i < 4; i++)
            sources[i] = (RQ_Source){.path = all[i].name, .text = all[i].content};
        static const RQ_Expected expected[] = {
            {.case_id = "included-chialisp-constant",
             .site = {.file = "puzzle.clsp", .caller = "puzzle.run", .needle = "CREATE_COIN"},
             .relation = "USAGE",
             .target = {.file = "condition_codes.clib",
                        .suffix = "CREATE_COIN",
                        .label = "Constant",
                        .declaration_line = 2,
                        .declaration_text = "  (defconstant CREATE_COIN 51)"},
             .mode = RQ_KNOWN_GAP,
             .gap_id = "chialisp-include-constant-scope",
             .why = "The Chialisp include binding has no audited source-proof bridge.",
             .tried = "Preserved include and Constant nodes; exact occurrence and foreign decoy "
                      "checked."},
            {.case_id = "required-control",
             .site = {.file = "chialisp_control.mjs",
                      .caller = "caller",
                      .needle = "target()",
                      .occurrence_index = 1},
             .relation = "CALLS",
             .target = {.file = "chialisp_control.mjs",
                        .suffix = "target",
                        .label = "Function",
                        .declaration_line = 1,
                        .declaration_text = "export function target() { return 1; }"},
             .mode = RQ_REQUIRED}};
        static const RQ_Forbidden forbidden = {
            .case_id = "unloaded-constant-is-not-a-binding",
            .site = {.file = "puzzle.clsp", .caller = "puzzle.run", .needle = "CREATE_COIN"},
            .relation = "USAGE",
            .target = {.file = "decoy/condition_codes.clib",
                       .suffix = "CREATE_COIN",
                       .label = "Constant",
                       .declaration_line = 2,
                       .declaration_text = "  (defconstant CREATE_COIN 99)"}};
        LangProj lp;
        cbm_store_t *store = lang_index_files(&lp, all, 4 + parallel * PADDING);
        RQ_Report report;
        bool ok = rq_check_project_declarations(store, lp.project, "chialisp-constant", sources, 4,
                                                expected, 2, &forbidden, 1, &report, stderr);
        cbm_node_t *constants = NULL;
        int constant_count = 0, exact = 0;
        if (store && cbm_store_find_nodes_by_name(store, lp.project, "CREATE_COIN", &constants,
                                                  &constant_count) == CBM_STORE_OK) {
            for (int i = 0; i < constant_count; i++)
                if (constants[i].file_path && constants[i].label &&
                    !strcmp(constants[i].file_path, "condition_codes.clib") &&
                    !strcmp(constants[i].label, "Constant") && constants[i].start_line == 2)
                    exact++;
        }
        cbm_store_free_nodes(constants, constant_count);
        lang_cleanup(&lp, store);
        ASSERT_TRUE(ok);
        ASSERT_EQ(exact, 1);
        ASSERT_EQ(report.genuine, 2);
        ASSERT_EQ(report.found, 1);
        ASSERT_EQ(report.missing, 1);
    }
    PASS();
}

TEST(contract_all_grammars_in_graph) {
    int n = (int)CBM_GRAMMAR_CASES_COUNT;
    if (n > GRAMMAR_BREADTH_MAX) {
        n = GRAMMAR_BREADTH_MAX;
    }
    static char names[GRAMMAR_BREADTH_MAX][GRAMMAR_PATH_BUF];
    LangFile files[GRAMMAR_BREADTH_MAX] = {0}; /* zero-init: GCC -Werror=maybe-uninitialized */
    for (int i = 0; i < n; i++) {
        snprintf(names[i], sizeof(names[i]), "g%03d/%s", i, CBM_GRAMMAR_CASES[i].path);
        files[i].name = names[i];
        files[i].content = CBM_GRAMMAR_CASES[i].src;
    }

    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, files, n);
    int total_nodes = store ? cbm_store_count_nodes(store, lp.project) : -1;

    int not_indexed = 0;
    int below_min = 0;
    int checked = 0;
    for (int i = 0; i < n; i++) {
        const GrammarCase *c = &CBM_GRAMMAR_CASES[i];
        if (grammar_graph_allowlisted(c->name)) {
            continue;
        }
        char rel[GRAMMAR_PATH_BUF];
        snprintf(rel, sizeof(rel), "g%03d/%s", i, c->path);
        int defs = store ? def_nodes_for_file(store, lp.project, rel) : -1;
        checked++;
        if (defs < 0) {
            fprintf(stderr, "  [GRAPH-BREADTH] %-14s NOT INDEXED (0 nodes for %s)\n", c->name, rel);
            breadth_diag(store, lp.project, rel, c);
            not_indexed++;
        } else if (defs < c->min_defs) {
            fprintf(stderr, "  [GRAPH-BREADTH] %-14s defs-in-graph=%d < min=%d\n", c->name, defs,
                    c->min_defs);
            breadth_diag(store, lp.project, rel, c);
            below_min++;
        }
    }
    fprintf(stderr,
            "  [GRAPH-BREADTH] checked=%d not_indexed=%d below_min=%d total_nodes=%d (of %d "
            "grammars)\n",
            checked, not_indexed, below_min, total_nodes, n);

    lang_cleanup(&lp, store);
    ASSERT_TRUE(total_nodes > 0);
    ASSERT_EQ(not_indexed, 0);
    ASSERT_EQ(below_min, 0);
    PASS();
}

/* CALLS breadth: preserve all 54 original source fixtures. The independent
 * oracle below counts 55 project run-to-helper sites, even when extraction
 * creates no row. Unrelated module-level run calls and helper-body calls are
 * outside that caller domain. External builtin sites within run are outside
 * the project-declaration target domain; a wrong external target at a golden
 * site still fails. Missing valid edges are quality debt, wrong edges bugs. */

typedef struct {
    const char *lang;
    const char *filename;
    const char *content;
} CallCase;

static const CallCase CALL_CASES[] = {
    {"ruby", "a.rb", "def helper\n  42\nend\n\ndef run\n  helper()\nend\n"},
    {"lua", "a.lua",
     "function helper(x)\n  return x + 1\nend\n\nfunction run()\n  return helper(41)\nend\n"},
    {"swift", "a.swift",
     "func helper() -> Int {\n    return 42\n}\n\nfunc run() {\n    let value = helper()\n    "
     "print(value)\n}\n"},
    {"dart", "a.dart", "void helper() {\n  print('helper');\n}\n\nvoid run() {\n  helper();\n}\n"},
    {"scala", "a.scala", "def helper(): Int =\n  21 + 21\n\ndef run(): Int =\n  helper() * 2\n"},
    {"bash", "a.sh", "helper() {\n  echo \"doing work\"\n}\n\nrun() {\n  helper\n}\n"},
    {"perl", "a.pl",
     "#!/usr/bin/perl\nuse strict;\nuse warnings;\n\nsub helper {\n    my ($name) = @_;\n    "
     "return \"Hello, $name\";\n}\n\nsub run {\n    my $greeting = helper(\"world\");\n    print "
     "\"$greeting\\n\";\n    return;\n}\n\nrun();\n"},
    {"r", "a.R", "helper <- function(x) {\n  x * 2\n}\n\nrun <- function() {\n  helper(21)\n}\n"},
    {"julia", "a.jl",
     "function helper(x)\n    return x + 1\nend\n\nfunction run(n)\n    return helper(n)\nend\n"},
    {"zig", "a.zig", "fn helper() void {}\n\npub fn run() void {\n    helper();\n}\n"},
    {"gdscript", "a.gd",
     "func helper() -> int:\n\treturn 42\n\n\nfunc run() -> void:\n\tvar value = "
     "helper()\n\tprint(value)\n"},
    {"groovy", "a.groovy",
     "def helper() {\n    println \"helping\"\n}\n\ndef run() {\n    helper()\n}\n"},
    {"elixir", "a.ex",
     "defmodule Sample do\n  def helper(x) do\n    x + 1\n  end\n\n  def run do\n    helper(41)\n  "
     "end\nend\n"},
    {"ocaml", "a.ml",
     "let helper x = x + 1\n\nlet run () =\n  let result = helper 41 in\n  print_int result\n"},
    {"gleam", "a.gleam",
     "pub fn helper(x: Int) -> Int {\n  x + 1\n}\n\npub fn run() -> Int {\n  helper(2)\n}\n"},
    {"crystal", "a.cr",
     "def helper(name : String) : String\n  \"hello, #{name}\"\nend\n\ndef run\n  message = "
     "helper(\"world\")\n  puts message\nend\n\nrun\n"},
    {"haskell", "a.hs", "helper :: Int -> Int\nhelper x = x + 1\n\nrun :: Int\nrun = helper 41\n"},
    {"fortran", "a.f90",
     "function helper(x) result(y)\n    integer, intent(in) :: x\n    integer :: y\n    y = x + "
     "1\nend function helper\n\nfunction run(n) result(total)\n    integer, intent(in) :: n\n    "
     "integer :: total\n    total = helper(n) + helper(n + 1)\nend function run\n"},
    {"pascal", "a.pas",
     "procedure Helper(x: Integer);\nbegin\n  WriteLn(x);\nend;\n\nprocedure Run;\nbegin\n  "
     "Helper(1);\nend;\n"},
    {"tcl", "a.tcl",
     "proc helper {x} {\n    return [expr {$x * 2}]\n}\n\nproc run {} {\n    set result [helper "
     "21]\n    puts $result\n}\n"},
    {"solidity", "a.sol",
     "// SPDX-License-Identifier: MIT\npragma solidity ^0.8.0;\n\nfunction helper(uint256 x) pure "
     "returns (uint256) {\n    return x + 1;\n}\n\nfunction run(uint256 y) pure returns (uint256) "
     "{\n    return helper(y);\n}\n"},
    {"commonlisp", "a.lisp", "(defun helper (x)\n  (* x 2))\n\n(defun run ()\n  (helper 21))\n"},
    {"powershell", "a.ps1",
     "function helper {\n    Write-Output 'hi'\n}\n\nfunction run {\n    helper\n}\n"},

    {"ada", "a.adb",
     "procedure Run is\n   procedure Helper is\n   begin\n      null;\n   end Helper;\nbegin\n   "
     "Helper;\nend Run;\n"},
    {"apex", "A.cls",
     "public class A {\n    static void helper() {\n        System.debug('helper');\n    }\n\n    "
     "static void run() {\n        helper();\n    }\n}\n"},
    {"awk", "a.awk",
     "function helper(x) {\n    return x + 1\n}\n\nfunction run() {\n    print helper(3)\n}\n"},
    {"cairo", "a.cairo",
     "fn helper(x: felt252) -> felt252 {\n    x + 1\n}\n\nfn run() -> felt252 {\n    "
     "helper(41)\n}\n"},
    {"chialisp", "a.clsp",
     "(mod ()\n  (defun helper (x)\n    (* x 2))\n  (defun run ()\n    (helper 21))\n)\n"},
    {"clojure", "a.clj", "(defn helper [] 42)\n\n(defn run [] (helper))\n"},
    {"cuda", "a.cu",
     "__device__ int helper(int x) {\n    return x * 2;\n}\n\n__global__ void run(int *out) {\n    "
     "out[0] = helper(21);\n}\n"},
    {"d", "a.d",
     "int helper(int x)\n{\n    return x + 1;\n}\n\nvoid run()\n{\n    int y = helper(41);\n}\n"},
    {"emacslisp", "a.el",
     "(defun helper (x)\n  \"Add one to X.\"\n  (+ x 1))\n\n(defun run ()\n  \"Call helper.\"\n  "
     "(helper 41))\n"},
    {"erlang", "a.erl",
     "-module(a).\n-export([run/0]).\n\nhelper(X) -> X + 1.\n\nrun() -> helper(41).\n"},
    {"fennel", "a.fnl", "(fn helper [x]\n  (+ x 1))\n\n(fn run [n]\n  (helper n))\n"},
    {"fish", "a.fish",
     "function helper\n    echo \"helping\"\nend\n\nfunction run\n    helper\nend\n"},
    {"fsharp", "a.fs", "let helper x = x + 1\n\nlet run () = helper 41\n"},
    {"glsl", "a.glsl",
     "float helper(float x) {\n    return x * 2.0;\n}\n\nvoid run() {\n    float y = "
     "helper(3.0);\n}\n"},
    {"hare", "a.ha", "fn helper() void = void;\n\nexport fn run() void = {\n\thelper();\n};\n"},
    {"hlsl", "a.hlsl",
     "float helper(float x)\n{\n    return x * 2.0;\n}\n\nfloat run(float v)\n{\n    return "
     "helper(v) + 1.0;\n}\n"},
    {"ispc", "a.ispc",
     "static inline uniform float helper(uniform float x) {\n    return x * 2.0f;\n}\n\nexport "
     "void run(uniform float in[], uniform float out[], uniform int n) {\n    foreach (i = 0 ... "
     "n) {\n        out[i] = helper(in[i]);\n    }\n}\n"},
    {"luau", "a.luau",
     "function helper(x)\n\treturn x + 1\nend\n\nfunction run(n)\n\treturn helper(n) * 2\nend\n"},
    {"matlab", "a.m",
     "function run()\n    helper();\nend\n\nfunction helper()\n    disp('hi');\nend\n"},
    {"odin", "a.odin",
     "package fixture\n\nhelper :: proc() -> int {\n\treturn 42\n}\n\nrun :: proc() {\n\tx := "
     "helper()\n\t_ = x\n}\n"},
    {"racket", "a.rkt",
     "#lang racket\n\n(define (helper x)\n  (+ x 1))\n\n(define (run)\n  (helper 41))\n"},
    {"rescript", "a.res", "let helper = (x) => x + 1\n\nlet run = () => helper(41)\n"},
    {"scheme", "a.scm", "(define (helper x)\n  (* x 2))\n\n(define (run)\n  (helper 21))\n"},
    {"slang", "a.slang",
     "void helper()\n{\n    int x = 1;\n}\n\nvoid run()\n{\n    helper();\n}\n"},
    {"squirrel", "a.nut",
     "function helper(x) {\n    return x + 1;\n}\n\nfunction run() {\n    return helper(41);\n}\n"},
    {"starlark", "a.bzl",
     "def helper(x):\n    return x + 1\n\ndef run():\n    return helper(41)\n"},
    {"sway", "a.sw", "fn helper() {}\n\nfn run() {\n    helper();\n}\n"},
    {"teal", "a.tl",
     "local function helper(x: number): number\n   return x + 1\nend\n\nlocal function run(): "
     "number\n   return helper(41)\nend\n"},
    {"vimscript", "a.vim",
     "function! Helper() abort\n  return 1\nendfunction\n\nfunction! Run() abort\n  call "
     "Helper()\nendfunction\n"},
    {"wgsl", "a.wgsl",
     "fn helper() -> f32 {\n  return 1.0;\n}\n\nfn run() -> f32 {\n  return helper();\n}\n"},
    {"zsh", "a.zsh", "function helper {\n  print \"helping\"\n}\n\nfunction run {\n  helper\n}\n"},
};

typedef struct {
    const char *language, *caller, *target;
    const char *sites[2];
    unsigned occurrence[2], site_count;
    int declaration_line;
    const char *declaration;
    RQ_Mode mode;
    const char *classification, *why, *tried;
} CallOracle;

/* Source-authored invocation sites and full declaration-line anchors. Binding
 * classifications identify an audited missing proof stage, not a claim that
 * extraction has no additional gap. Dart and CUDA are required capabilities. */
static const CallOracle CALL_ORACLES[] = {
    {"ruby",
     "run",
     "helper",
     {"helper()"},
     {0},
     1,
     1,
     "def helper",
     RQ_KNOWN_GAP,
     "binding",
     "Top-level def methods and implicit-receiver invocation have no audited Ruby method binding.",
     "The zero-argument helper() call is preserved; the generic language gate excludes Ruby."},
    {"lua",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "function helper(x)",
     RQ_KNOWN_GAP,
     "binding",
     "Global function declarations write _ENV; the Lua subset proves only direct local function "
     "bindings.",
     "The original global declarations remain unchanged; lua_local_function rejects both and "
     "records BINDING_BLOCKED."},
    {"swift",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "func helper() -> Int {",
     RQ_KNOWN_GAP,
     "binding",
     "Swift file membership in a build module and module-level function visibility are not proved.",
     "The original free helper() invocation remains; binding_build returns "
     "swift_build_module_membership_required."},
    {"dart",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "void helper() {",
     RQ_REQUIRED,
     "supported",
     NULL,
     NULL},
    {"scala",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "def helper(): Int =",
     RQ_KNOWN_GAP,
     "binding",
     "Scala top-level def visibility and invocation binding have no semantic proof bridge.",
     "Kept the Scala 3 expression-bodied helper and run definitions; only namespace imports are "
     "advertised."},
    {"bash",
     "run",
     "helper",
     {"helper"},
     {1},
     1,
     1,
     "helper() {",
     RQ_KNOWN_GAP,
     "binding",
     "Shell function lookup versus command execution has no audited Bash binding environment.",
     "Kept the bare helper command and preceding function declaration; the generic language gate "
     "excludes Bash."},
    {"perl",
     "run",
     "helper",
     {"helper(\"world\")"},
     {0},
     1,
     5,
     "sub helper {",
     RQ_KNOWN_GAP,
     "binding",
     "Perl package-sub invocation has no admitted semantic proof bridge from its legacy engine.",
     "Kept use strict/warnings and the direct main-package helper call; capability dispatch "
     "explicitly withholds that bridge."},
    {"r",
     "run",
     "helper",
     {"helper(21)"},
     {0},
     1,
     1,
     "helper <- function(x) {",
     RQ_KNOWN_GAP,
     "binding",
     "R function-valued assignment and lexical environment lookup are not represented by an "
     "audited binding adapter.",
     "Kept helper <- function and the direct call inside run; the generic language gate excludes "
     "R."},
    {"julia",
     "run",
     "helper",
     {"helper(n)"},
     {0},
     1,
     1,
     "function helper(x)",
     RQ_KNOWN_GAP,
     "binding",
     "Julia generic-function identity and method applicability have no audited invocation binding.",
     "Kept the original helper(n) call and function definition; no Julia semantic adapter enters "
     "binding_build."},
    {"zig",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "fn helper() void {}",
     RQ_KNOWN_GAP,
     "binding",
     "Zig container-level function declarations have no audited lexical invocation binding.",
     "Kept the private fn helper and pub fn run in one file; the generic language gate excludes "
     "Zig."},
    {"gdscript",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "func helper() -> int:",
     RQ_KNOWN_GAP,
     "binding",
     "GDScript script-member lookup for a bare function call has no audited owner binding.",
     "Kept the script-level func declarations and helper() call; GDScript has no semantic "
     "capability row."},
    {"groovy",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "def helper() {",
     RQ_KNOWN_GAP,
     "binding",
     "Groovy script methods and dynamic invocation have no audited receiver/name binding.",
     "Kept the def helper and def run source unchanged; the generic language gate excludes Groovy "
     "regardless of call extraction."},
    {"elixir",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     2,
     "  def helper(x) do",
     RQ_KNOWN_GAP,
     "binding",
     "Elixir local function name/arity lookup within defmodule has no audited module binding.",
     "Kept Sample.helper/1 and run/0 source declarations; no Elixir semantic proof adapter is "
     "selected."},
    {"ocaml",
     "run",
     "helper",
     {"helper 41"},
     {0},
     1,
     1,
     "let helper x = x + 1",
     RQ_KNOWN_GAP,
     "binding",
     "OCaml let-bound functions and curried application have no audited lexical binding adapter.",
     "Kept the original helper 41 application inside run; the generic language gate excludes "
     "OCaml."},
    {"gleam",
     "run",
     "helper",
     {"helper(2)"},
     {0},
     1,
     1,
     "pub fn helper(x: Int) -> Int {",
     RQ_KNOWN_GAP,
     "binding",
     "Gleam module function lookup and invocation applicability have no audited proof adapter.",
     "Kept the typed pub fn helper and direct helper(2) call; the generic language gate excludes "
     "Gleam."},
    {"crystal",
     "run",
     "helper",
     {"helper(\"world\")"},
     {0},
     1,
     1,
     "def helper(name : String) : String",
     RQ_KNOWN_GAP,
     "binding",
     "Crystal top-level method lookup and argument dispatch have no audited invocation binding.",
     "Kept the String helper and its literal argument inside run; no Crystal semantic capability "
     "row exists."},
    {"haskell",
     "run",
     "helper",
     {"helper 41"},
     {0},
     1,
     2,
     "helper x = x + 1",
     RQ_KNOWN_GAP,
     "binding",
     "Haskell equation bindings and application identity have no audited lexical proof adapter.",
     "Kept the separate helper type signature/equation and run value binding; the generic language "
     "gate excludes Haskell."},
    {"fortran",
     "run",
     "helper",
     {"helper(n)", "helper(n + 1)"},
     {0, 0},
     2,
     1,
     "function helper(x) result(y)",
     RQ_KNOWN_GAP,
     "binding",
     "Fortran external procedure identity and both function-reference sites have no audited "
     "binding bridge.",
     "Kept both helper(n) and helper(n + 1) references; the Fortran language gate supplies no "
     "semantic binding authority."},
    {"pascal",
     "Run",
     "Helper",
     {"Helper(1)"},
     {0},
     1,
     1,
     "procedure Helper(x: Integer);",
     RQ_KNOWN_GAP,
     "binding",
     "Pascal procedure visibility and case-insensitive name binding have no audited resolver "
     "adapter.",
     "Kept the preceding procedure Helper and Run invocation; the generic language gate excludes "
     "Pascal."},
    {"tcl",
     "run",
     "helper",
     {"helper 21"},
     {0},
     1,
     1,
     "proc helper {x} {",
     RQ_KNOWN_GAP,
     "binding",
     "Tcl proc registration and command-substitution lookup have no audited interpreter binding.",
     "Kept the nested [helper 21] command inside run; no Tcl semantic proof adapter is selected."},
    {"solidity",
     "run",
     "helper",
     {"helper(y)"},
     {0},
     1,
     4,
     "function helper(uint256 x) pure returns (uint256) {",
     RQ_KNOWN_GAP,
     "binding",
     "Solidity file-level functions and invocation applicability have no audited source binding "
     "bridge.",
     "Kept the pure helper(uint256) and direct helper(y) expression; Solidity is excluded by the "
     "generic binding gate."},
    {"commonlisp",
     "run",
     "helper",
     {"(helper 21)"},
     {0},
     1,
     1,
     "(defun helper (x)",
     RQ_KNOWN_GAP,
     "binding",
     "Common Lisp defun symbols and function-namespace lookup have no audited binding adapter.",
     "Kept the original list application and defun declaration; no Common Lisp semantic proof "
     "producer is advertised."},
    {"powershell",
     "run",
     "helper",
     {"helper"},
     {1},
     1,
     1,
     "function helper {",
     RQ_KNOWN_GAP,
     "binding",
     "PowerShell function/command precedence has no audited scope binding.",
     "Kept the bare helper command and function declaration; the generic language gate excludes "
     "PowerShell."},
    {"ada",
     "Run",
     "Helper",
     {"Helper;"},
     {1},
     1,
     2,
     "   procedure Helper is",
     RQ_KNOWN_GAP,
     "binding",
     "Ada nested subprogram visibility and procedure-call binding have no audited adapter.",
     "Kept Helper nested inside Run and its call statement; the generic language gate excludes "
     "Ada."},
    {"apex",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     2,
     "    static void helper() {",
     RQ_KNOWN_GAP,
     "binding",
     "Apex same-class static method lookup has no audited class/member proof bridge.",
     "Kept A.helper and A.run with the unqualified call; Apex does not enter the Java proof "
     "adapter."},
    {"awk",
     "run",
     "helper",
     {"helper(3)"},
     {0},
     1,
     1,
     "function helper(x) {",
     RQ_KNOWN_GAP,
     "binding",
     "Awk user-function declarations and call lookup have no audited binding adapter.",
     "Kept helper(3) within the print expression in run; the generic language gate excludes Awk."},
    {"cairo",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "fn helper(x: felt252) -> felt252 {",
     RQ_KNOWN_GAP,
     "binding",
     "Cairo module functions and felt252 invocation applicability have no audited proof bridge.",
     "Kept the typed helper and literal argument; no Cairo semantic binding producer is "
     "advertised."},
    {"chialisp",
     "run",
     "helper",
     {"(helper 21)"},
     {0},
     1,
     2,
     "  (defun helper (x)",
     RQ_KNOWN_GAP,
     "binding",
     "Chialisp mod-local defun binding has no audited lexical invocation adapter.",
     "Kept both definitions within the original mod form; the generic language gate excludes "
     "Chialisp."},
    {"clojure",
     "run",
     "helper",
     {"(helper)"},
     {0},
     1,
     1,
     "(defn helper [] 42)",
     RQ_KNOWN_GAP,
     "binding",
     "Clojure defn Vars and namespace lookup have no audited invocation binding.",
     "Kept the zero-argument helper list application; no Clojure semantic proof adapter is "
     "selected."},
    {"cuda",
     "run",
     "helper",
     {"helper(21)"},
     {0},
     1,
     1,
     "__device__ int helper(int x) {",
     RQ_REQUIRED,
     "supported",
     NULL,
     NULL},
    {"d",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "int helper(int x)",
     RQ_KNOWN_GAP,
     "binding",
     "D module function lookup and overload applicability have no audited semantic adapter.",
     "Kept the int helper and literal call in run; D does not enter the C-family proof bridge."},
    {"emacslisp",
     "run",
     "helper",
     {"(helper 41)"},
     {0},
     1,
     1,
     "(defun helper (x)",
     RQ_KNOWN_GAP,
     "binding",
     "Emacs Lisp defun function-cell lookup has no audited binding adapter.",
     "Kept the original defun bodies and helper list application; the generic language gate "
     "excludes Emacs Lisp."},
    {"erlang",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     4,
     "helper(X) -> X + 1.",
     RQ_KNOWN_GAP,
     "binding",
     "Erlang module-local function name/arity lookup has no audited semantic binding.",
     "Kept module a, export run/0 and local helper/1; the generic language gate excludes Erlang."},
    {"fennel",
     "run",
     "helper",
     {"(helper n)"},
     {0},
     1,
     1,
     "(fn helper [x]",
     RQ_KNOWN_GAP,
     "binding",
     "Fennel fn bindings and list application have no audited lexical proof adapter.",
     "Kept named fn helper and its argument application; Fennel does not enter the Lua "
     "local-function adapter."},
    {"fish",
     "run",
     "helper",
     {"helper"},
     {1},
     1,
     1,
     "function helper",
     RQ_KNOWN_GAP,
     "binding",
     "Fish function registration and command lookup have no audited environment binding.",
     "Kept the bare helper command within function run; the generic language gate excludes Fish."},
    {"fsharp",
     "run",
     "helper",
     {"helper 41"},
     {0},
     1,
     1,
     "let helper x = x + 1",
     RQ_KNOWN_GAP,
     "binding",
     "F# let-bound functions and application lookup have no audited lexical binding adapter.",
     "Kept the original let helper and let run definitions; no F# semantic proof producer is "
     "advertised."},
    {"glsl",
     "run",
     "helper",
     {"helper(3.0)"},
     {0},
     1,
     1,
     "float helper(float x) {",
     RQ_KNOWN_GAP,
     "binding",
     "GLSL function overload/type lookup has no audited shader-language binding bridge.",
     "Kept the float helper and floating argument; GLSL is not admitted through the C-family "
     "adapter."},
    {"hare",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "fn helper() void = void;",
     RQ_KNOWN_GAP,
     "binding",
     "Hare module function declarations and calls have no audited lexical proof adapter.",
     "Kept the void helper declaration and exported run body; the generic language gate excludes "
     "Hare."},
    {"hlsl",
     "run",
     "helper",
     {"helper(v)"},
     {0},
     1,
     1,
     "float helper(float x)",
     RQ_KNOWN_GAP,
     "binding",
     "HLSL function overload/type lookup has no audited shader-language binding bridge.",
     "Kept the float helper(v) expression; HLSL does not enter the C-family semantic adapter."},
    {"ispc",
     "run",
     "helper",
     {"helper(in[i])"},
     {0},
     1,
     1,
     "static inline uniform float helper(uniform float x) {",
     RQ_KNOWN_GAP,
     "binding",
     "ISPC function binding with uniform/varying types has no audited invocation adapter.",
     "Kept the uniform helper and foreach invocation; ISPC does not enter the C-family semantic "
     "bridge."},
    {"luau",
     "run",
     "helper",
     {"helper(n)"},
     {0},
     1,
     1,
     "function helper(x)",
     RQ_KNOWN_GAP,
     "binding",
     "Luau global function environment lookup has no audited language binding adapter.",
     "Kept the original global functions; CBM_LANG_LUAU neither selects the Lua adapter nor proves "
     "its _ENV-style lookup."},
    {"matlab",
     "run",
     "helper",
     {"helper()"},
     {0},
     1,
     5,
     "function helper()",
     RQ_KNOWN_GAP,
     "binding",
     "MATLAB file-local function lookup has no audited semantic binding adapter.",
     "Kept run before the file-local helper declaration and the original .m filename; MATLAB is "
     "excluded by binding_build."},
    {"odin",
     "run",
     "helper",
     {"helper()"},
     {0},
     1,
     3,
     "helper :: proc() -> int {",
     RQ_KNOWN_GAP,
     "binding",
     "Odin package procedure constants and invocation binding have no audited adapter.",
     "Kept helper :: proc and package fixture; the generic language gate excludes Odin."},
    {"racket",
     "run",
     "helper",
     {"(helper 41)"},
     {0},
     1,
     3,
     "(define (helper x)",
     RQ_KNOWN_GAP,
     "binding",
     "Racket module define bindings and list application have no audited lexical proof adapter.",
     "Kept #lang racket and function-shaped define forms; no Racket semantic proof producer is "
     "advertised."},
    {"rescript",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "let helper = (x) => x + 1",
     RQ_KNOWN_GAP,
     "binding",
     "ReScript let-bound callable values have no audited lexical invocation binding.",
     "Kept the arrow-function helper value and direct call; the generic language gate excludes "
     "ReScript."},
    {"scheme",
     "run",
     "helper",
     {"(helper 21)"},
     {0},
     1,
     1,
     "(define (helper x)",
     RQ_KNOWN_GAP,
     "binding",
     "Scheme define procedure bindings and application lookup have no audited lexical adapter.",
     "Kept the function-shaped define and helper application; the generic language gate excludes "
     "Scheme."},
    {"slang",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "void helper()",
     RQ_KNOWN_GAP,
     "binding",
     "Slang shader function visibility and invocation binding have no audited semantic adapter.",
     "Kept the void helper and direct call; Slang is not covered by the C-family proof bridge."},
    {"squirrel",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "function helper(x) {",
     RQ_KNOWN_GAP,
     "binding",
     "Squirrel function declarations and root-table lookup have no audited binding adapter.",
     "Kept the original helper declaration and literal call; the generic language gate excludes "
     "Squirrel."},
    {"starlark",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "def helper(x):",
     RQ_KNOWN_GAP,
     "binding",
     "Starlark module def bindings have no audited language-specific invocation adapter.",
     "Kept the .bzl definitions and direct helper call; Starlark is not implicitly treated as "
     "Python by binding_build."},
    {"sway",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "fn helper() {}",
     RQ_KNOWN_GAP,
     "binding",
     "Sway module function lookup has no audited lexical invocation binding.",
     "Kept fn helper and the direct call from run; the generic language gate excludes Sway."},
    {"teal",
     "run",
     "helper",
     {"helper(41)"},
     {0},
     1,
     1,
     "local function helper(x: number): number",
     RQ_KNOWN_GAP,
     "binding",
     "Teal typed local function binding has no audited dialect adapter.",
     "Kept both typed local declarations; CBM_LANG_TEAL does not select the Lua local-function "
     "proof path."},
    {"vimscript",
     "Run",
     "Helper",
     {"call Helper()"},
     {0},
     1,
     1,
     "function! Helper() abort",
     RQ_KNOWN_GAP,
     "binding",
     "Vimscript named-function lookup and call-statement binding have no audited adapter.",
     "Kept function! Helper and call Helper() with case preserved; the generic language gate "
     "excludes Vimscript."},
    {"wgsl",
     "run",
     "helper",
     {"helper()"},
     {1},
     1,
     1,
     "fn helper() -> f32 {",
     RQ_KNOWN_GAP,
     "binding",
     "WGSL function declarations and callable/type-constructor disambiguation have no audited "
     "binding adapter.",
     "Kept the f32 helper and direct return invocation; WGSL does not enter the C-family semantic "
     "bridge."},
    {"zsh",
     "run",
     "helper",
     {"helper"},
     {1},
     1,
     1,
     "function helper {",
     RQ_KNOWN_GAP,
     "binding",
     "Zsh shell-function lookup versus command dispatch has no audited binding environment.",
     "Kept the function helper declaration and bare command in run; the generic language gate "
     "excludes Zsh."},
};

/* Batch the unchanged fixtures so both drivers cross the parallel threshold.
 * Databases live outside the source tree and the caller's worker settings are
 * restored before any assertion can leave the test. */
static int call_breadth_index(const char *repo, const char *database, bool parallel) {
    const char *workers = getenv("CBM_WORKERS");
    const char *single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_workers = workers ? strdup(workers) : NULL;
    char *saved_single = single ? strdup(single) : NULL;
    if ((workers && !saved_workers) || (single && !saved_single)) {
        free(saved_workers);
        free(saved_single);
        return -1;
    }
    if (parallel) {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        cbm_setenv("CBM_WORKERS", "4", 1);
    } else {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
        cbm_setenv("CBM_WORKERS", "1", 1);
    }
    cbm_pipeline_t *pipeline = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
    if (pipeline)
        cbm_pipeline_set_persistence(pipeline, false);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "call-breadth")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    if (saved_workers)
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
    else
        cbm_unsetenv("CBM_WORKERS");
    if (saved_single)
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
    else
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    free(saved_workers);
    free(saved_single);
    return rc;
}

/* Compare logical emitted identities, never insertion order, proof node IDs,
 * or producer-specific explanations for unresolved occurrences. The independent
 * source oracle below validates correctness before this equivalence check. */
static char *call_breadth_outcomes(cbm_store_t *store) {
    const char *sql =
        "SELECT coalesce(group_concat(entry,char(10)),'') FROM ("
        "SELECT json_array(r.file_path,r.scope,r.relation,r.start_byte,r.end_byte,"
        "r.source_origin,r.target,t.file_path,t.label,t.start_line,t.end_line) AS entry "
        "FROM resolver_occurrences r LEFT JOIN nodes t ON t.project=r.project "
        "AND t.qualified_name=r.target WHERE r.project='call-breadth' "
        "AND r.emitted=1 AND r.relation='CALLS' AND r.file_path LIKE 'calls/%' ORDER BY entry)";
    sqlite3_stmt *stmt = NULL;
    char *result = NULL;
    if (sqlite3_prepare_v2(cbm_store_get_db(store), sql, -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW) {
        const char *text = (const char *)sqlite3_column_text(stmt, 0);
        if (text)
            result = strdup(text);
    }
    sqlite3_finalize(stmt);
    return result;
}

TEST(contract_calls_breadth) {
    enum { LANGUAGES = 54, SITES = 55, RUNS = 4 };
    ASSERT_EQ(sizeof(CALL_CASES) / sizeof(CALL_CASES[0]), LANGUAGES);
    ASSERT_EQ(sizeof(CALL_ORACLES) / sizeof(CALL_ORACLES[0]), LANGUAGES);
    const char *temporary = th_mktempdir("cbm_calls_breadth");
    ASSERT_NOT_NULL(temporary);
    char root[256], repo[512], databases[3][512];
    snprintf(root, sizeof(root), "%s", temporary);
    lc_to_fwd_slashes(root);
    snprintf(repo, sizeof(repo), "%s/repo", root);
    for (int i = 0; i < 3; i++)
        snprintf(databases[i], sizeof(databases[i]), "%s/result-%d.db", root, i);
    RQ_Source sources[LANGUAGES + 1] = {0};
    RQ_Expected expected[SITES] = {0};
    char paths[LANGUAGES][128], ids[SITES][64];
    size_t count = 0;
    bool prepared = true;
    for (size_t i = 0; i < LANGUAGES; i++) {
        const CallCase *fixture = CALL_CASES + i;
        const CallOracle *oracle = CALL_ORACLES + i;
        if (strcmp(fixture->lang, oracle->language) || !oracle->site_count ||
            oracle->site_count > 2 || count + oracle->site_count > SITES) {
            prepared = false;
            break;
        }
        snprintf(paths[i], sizeof(paths[i]), "calls/%s/%s", fixture->lang, fixture->filename);
        sources[i] = (RQ_Source){.path = paths[i], .text = fixture->content};
        char absolute[700];
        snprintf(absolute, sizeof(absolute), "%s/%s", repo, paths[i]);
        prepared = th_write_file(absolute, fixture->content) == 0 && prepared;
        fprintf(stderr, "[CALLS-BREADTH] case=%s classification=%s genuine=%u\n", fixture->lang,
                oracle->classification, oracle->site_count);
        for (unsigned j = 0; j < oracle->site_count; j++, count++) {
            if (oracle->site_count == 1)
                snprintf(ids[count], sizeof(ids[count]), "%s", fixture->lang);
            else
                snprintf(ids[count], sizeof(ids[count]), "%s.%u", fixture->lang, j + 1);
            expected[count] =
                (RQ_Expected){.case_id = ids[count],
                              .site = {.file = paths[i],
                                       .caller = oracle->caller,
                                       .needle = oracle->sites[j],
                                       .occurrence_index = oracle->occurrence[j]},
                              .relation = "CALLS",
                              .target = {.file = paths[i],
                                         .suffix = oracle->target,
                                         .label = oracle->mode == RQ_REQUIRED ? "Function" : NULL,
                                         .declaration_line = oracle->declaration_line,
                                         .declaration_text = oracle->declaration},
                              .mode = oracle->mode,
                              .gap_id = fixture->lang,
                              .why = oracle->why,
                              .tried = oracle->tried};
        }
    }
    prepared = count == SITES && prepared;
    const char *names[RUNS] = {"breadth-sequential", "breadth-parallel", "breadth-updated",
                               "breadth-fresh"};
    const int database_index[RUNS] = {0, 1, 0, 2};
    const bool parallel[RUNS] = {false, true, false, true};
    bool accepted[RUNS] = {0};
    RQ_Report reports[RUNS] = {0};
    char *outcomes[RUNS] = {0};
    for (int run = 0; run < RUNS && prepared; run++) {
        /* Exercise the existing-database update entry with changed source while
         * preserving every original fixture byte. Strict generation rebuilding
         * may deliberately route this update through a full rebuild. */
        if (run == 2) {
            char refresh[700];
            snprintf(refresh, sizeof(refresh), "%s/refresh.py", repo);
            sources[LANGUAGES] = (RQ_Source){.path = "refresh.py",
                                             .text = "def unrelated_refresh():\n    return 1\n"};
            prepared = th_write_file(refresh, sources[LANGUAGES].text) == 0;
            if (!prepared)
                break;
        }
        const char *database = databases[database_index[run]];
        if (call_breadth_index(repo, database, parallel[run]) != 0)
            continue;
        cbm_store_t *store = cbm_store_open_path_existing(database);
        if (store) {
            accepted[run] = rq_check_project_declarations(
                store, "call-breadth", names[run], sources, run < 2 ? LANGUAGES : LANGUAGES + 1,
                expected, count, NULL, 0, reports + run, stderr);
            outcomes[run] = call_breadth_outcomes(store);
            cbm_store_close(store);
        }
    }
    bool equivalent = outcomes[0] && outcomes[1] && outcomes[2] && outcomes[3] &&
                      !strcmp(outcomes[0], outcomes[1]) && !strcmp(outcomes[2], outcomes[3]) &&
                      !strcmp(outcomes[0], outcomes[2]);
    for (int run = 0; run < RUNS; run++)
        free(outcomes[run]);
    int cleaned = th_rmtree(root);
    ASSERT(prepared);
    ASSERT_EQ(cleaned, 0);
    for (int run = 0; run < RUNS; run++) {
        ASSERT(accepted[run]);
        ASSERT_EQ(reports[run].genuine, SITES);
        ASSERT_EQ(reports[run].found, 2);    /* independently required Dart and CUDA sites */
        ASSERT_EQ(reports[run].missing, 53); /* includes both original Fortran references */
    }
    ASSERT(equivalent);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  CROSS-CUTTING / SEMANTIC EDGE-TYPE PRESENCE CONTRACTS (P6)
 *
 *  CALLS (P3/P5) and IMPORTS (test_grammar_imports.c) cover the two
 *  highest-volume edge types. This section guards the REMAINING graph
 *  edge types the pipeline emits, each with the minimal fixture that
 *  triggers it through the SAME production index_repository flow
 *  (mode=full, so the similarity + semantic predump passes run):
 *
 *    structural        DEFINES, DEFINES_METHOD, CONTAINS_FILE
 *    type-relationship INHERITS, IMPLEMENTS, DECORATES
 *    test-mapping      TESTS_FILE
 *    service/dataflow  HANDLES, HTTP_CALLS, ASYNC_CALLS, DATA_FLOWS
 *    similarity        SIMILAR_TO, SEMANTICALLY_RELATED
 *
 *  We assert PRESENCE (count >= floor), never an exact count — edge
 *  counts are non-deterministic (parallel LSH / similarity / resolve).
 *  A regression that stops emitting any of these turns its test RED.
 *  Fixtures are reasoned from the producing pass (and SIMILAR_TO /
 *  SEMANTICALLY_RELATED were also verified end-to-end) so these are
 *  GREEN contracts: a failure here is a real edge-production regression,
 *  not a known gap.
 * ══════════════════════════════════════════════════════════════════ */

/* Every graph edge type the pipeline can emit — used to print a histogram
 * when an edge contract fails, so a regression shows exactly what WAS
 * produced instead of the missing type. */
static const char *ALL_EDGE_TYPES[] = {"CALLS",
                                       "CALL_REFERENCE",
                                       "CONFIGURES",
                                       "CONTAINS_FILE",
                                       "CONTAINS_FOLDER",
                                       "DATA_FLOWS",
                                       "DECORATES",
                                       "DEFINES",
                                       "DEFINES_METHOD",
                                       "DEPENDS_ON",
                                       "FILE_CHANGES_WITH",
                                       "GRAPHQL_CALLS",
                                       "GRPC_CALLS",
                                       "HANDLES",
                                       "HTTP_CALLS",
                                       "IMPLEMENTS",
                                       "IMPORTS",
                                       "INHERITS",
                                       "INFRA_MAPS",
                                       "MENTIONS",
                                       "OVERRIDE",
                                       "REFERENCES_FILE",
                                       "SEMANTICALLY_RELATED",
                                       "SIMILAR_TO",
                                       "SPAWNS",
                                       "TESTS_FILE",
                                       "TESTS",
                                       "TRPC_CALLS",
                                       "USAGE",
                                       "ASYNC_CALLS",
                                       NULL};

static void dump_edge_histogram(cbm_store_t *store, const char *project) {
    if (!store) {
        fprintf(stderr, "      └─ (no graph DB)\n");
        return;
    }
    char line[640] = {0};
    for (int i = 0; ALL_EDGE_TYPES[i]; i++) {
        int c = cbm_store_count_edges_by_type(store, project, ALL_EDGE_TYPES[i]);
        if (c > 0 && strlen(line) < sizeof(line) - 48) {
            char one[64];
            snprintf(one, sizeof(one), "%s=%d ", ALL_EDGE_TYPES[i], c);
            strncat(line, one, sizeof(line) - strlen(line) - 1);
        }
    }
    fprintf(stderr, "      └─ edges produced: [%s]\n", line[0] ? line : "(none)");
}

/* Index `files`, assert the named edge type appears at least `floor` times.
 * On failure, dump the full edge histogram so a regression is diagnosable. */
static int edge_present(const LangFile *files, int nfiles, const char *edge, int floor) {
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge) : -1;
    if (got < floor) {
        fprintf(stderr, "  [EDGE] FAIL %-20s count=%d expected>=%d\n", edge, got, floor);
        dump_edge_histogram(store, lp.project);
    }
    lang_cleanup(&lp, store);
    return got >= floor;
}

/* DEFINES — File -> definition, once per top-level def (purely structural). */
TEST(contract_edge_defines) {
    static const LangFile f[] = {{"main.py",
                                  "def greet(name):\n    return 'Hello ' + name\n\n\n"
                                  "def farewell(name):\n    return 'Goodbye ' + name\n\n\n"
                                  "def main():\n    msg = greet('World')\n"
                                  "    msg2 = farewell('World')\n    print(msg, msg2)\n"}};
    ASSERT_TRUE(edge_present(f, 1, "DEFINES", 3)); /* greet, farewell, main */
    PASS();
}

/* DEFINES_METHOD — Class -> Method when the method's parent_class resolves. */
TEST(contract_edge_defines_method) {
    static const LangFile f[] = {{"greeter.py",
                                  "class Greeter:\n    def hello(self):\n        return \"hi\"\n\n"
                                  "    def bye(self):\n        return \"bye\"\n\n\n"
                                  "def main():\n    g = Greeter()\n    return g.hello()\n"}};
    ASSERT_TRUE(edge_present(f, 1, "DEFINES_METHOD", 1)); /* Greeter.hello, Greeter.bye */
    PASS();
}

/* CONTAINS_FILE — Folder/Project -> File for each discovered file. */
TEST(contract_edge_contains_file) {
    static const LangFile f[] = {
        {"pkg/util.py", "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n"},
        {"pkg/helpers.py", "def greet(name):\n    return \"hello \" + name\n\n\n"
                           "class Greeter:\n    def __init__(self, prefix):\n"
                           "        self.prefix = prefix\n\n"
                           "    def say(self, name):\n        return self.prefix + name\n"}};
    ASSERT_TRUE(edge_present(f, 2, "CONTAINS_FILE", 1)); /* pkg/ -> util.py, helpers.py */
    PASS();
}

/* INHERITS — subclass -> base via base_classes resolving to a Class/Type node.
 * Uses JAVA `extends`: Java's base_classes extraction is verified by
 * test_extraction.c:java_class_extends_and_implements, and same-file classes
 * resolve same-module. (Python `class X(Base)` does NOT yet work in-unit — that
 * is a real extraction bug reproduced RED by
 * test_extraction.c:python_class_base_extracted_bare; do not use Python here.) */
TEST(contract_edge_inherits) {
    static const LangFile f[] = {
        {"Zoo.java", "class Base {\n    int base() { return 0; }\n}\n\n"
                     "class Animal extends Base {\n    int speak() { return 1; }\n}\n\n"
                     "class Dog extends Animal {\n    int speak() { return 2; }\n}\n"}};
    ASSERT_TRUE(edge_present(f, 1, "INHERITS", 1)); /* Animal->Base, Dog->Animal */
    PASS();
}

/* IMPLEMENTS — Rust `impl Trait for Struct` (Java/TS `implements` -> INHERITS). */
TEST(contract_edge_implements) {
    static const LangFile f[] = {{"shapes.rs",
                                  "trait Greet {\n    fn hello(&self) -> String;\n}\n\n"
                                  "trait Area {\n    fn area(&self) -> f64;\n}\n\n"
                                  "struct English;\n\nstruct Square {\n    side: f64,\n}\n\n"
                                  "impl Greet for English {\n    fn hello(&self) -> String {\n"
                                  "        String::from(\"hi\")\n    }\n}\n\n"
                                  "impl Area for Square {\n    fn area(&self) -> f64 {\n"
                                  "        self.side * self.side\n    }\n}\n\n"
                                  "impl Greet for Square {\n    fn hello(&self) -> String {\n"
                                  "        String::from(\"square\")\n    }\n}\n"}};
    ASSERT_TRUE(edge_present(f, 1, "IMPLEMENTS", 2)); /* 3 impl-for blocks */
    PASS();
}

/* DECORATES — decorated def -> locally-defined decorator function. */
TEST(contract_edge_decorates) {
    static const LangFile f[] = {{"audit.py",
                                  "def audit(func):\n    def wrapper(*args, **kwargs):\n"
                                  "        return func(*args, **kwargs)\n    return wrapper\n\n\n"
                                  "def trace(func):\n    def inner(*args, **kwargs):\n"
                                  "        return func(*args, **kwargs)\n    return inner\n\n\n"
                                  "@audit\ndef process_order(order_id):\n    return order_id\n\n\n"
                                  "@trace\ndef cancel_order(order_id):\n    return order_id\n\n\n"
                                  "@audit\ndef refund_order(order_id):\n    return order_id\n"}};
    ASSERT_TRUE(edge_present(f, 1, "DECORATES", 2)); /* 3 decorated defs */
    PASS();
}

/* TESTS_FILE — test file -> production file via path derivation (test_foo->foo). */
TEST(contract_edge_tests_file) {
    static const LangFile f[] = {
        {"foo.py", "def add(a, b):\n    return a + b\n\n\ndef multiply(a, b):\n    return a * b\n"},
        {"test_foo.py", "from foo import add, multiply\n\n\n"
                        "def test_add():\n    assert add(2, 3) == 5\n\n\n"
                        "def test_multiply():\n    assert multiply(2, 3) == 6\n"}};
    ASSERT_TRUE(edge_present(f, 2, "TESTS_FILE", 1));
    PASS();
}

/* HANDLES — Flask @app.route decorator handler -> Route node. */
TEST(contract_edge_handles) {
    static const LangFile f[] = {
        {"app.py",
         "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
         "@app.route(\"/users\")\ndef list_users():\n    return {\"users\": []}\n\n\n"
         "@app.route(\"/health\")\ndef health_check():\n    return {\"status\": \"ok\"}\n"}};
    ASSERT_TRUE(edge_present(f, 1, "HANDLES", 1)); /* 2 route handlers */
    PASS();
}

/* HTTP_CALLS — call resolving to a QN containing an HTTP-client lib id
 * ("requests") with a URL-shaped first arg -> Route node. Uses the SEQUENTIAL
 * pass path (small fixture): a local wrapper whose QN carries the lib substring. */
TEST(contract_edge_http_calls) {
    static const LangFile f[] = {
        {"service_b.py",
         "def requests_get(url, params=None):\n    return {\"url\": url, \"params\": params}\n\n\n"
         "def requests_post(url, body=None):\n    return {\"url\": url, \"body\": body}\n\n\n"
         "def service_a_fetch_order(order_id):\n"
         "    return requests_get(\"/api/orders/list\", params={\"id\": order_id})\n\n\n"
         "def service_a_create_order(payload):\n"
         "    return requests_post(\"/api/orders/create\", body=payload)\n"}};
    ASSERT_TRUE(edge_present(f, 1, "HTTP_CALLS", 1));
    PASS();
}

/* ASYNC_CALLS — call resolving to a QN containing an async-broker id (dir
 * "pubsub/" injects it via the FQN scheme) with a topic-shaped first arg. */
TEST(contract_edge_async_calls) {
    static const LangFile f[] = {
        {"pubsub/publisher.py",
         "def publish_event(topic, payload):\n    return (topic, payload)\n\n\n"
         "def enqueue_order(order):\n    return publish_event(\"order-events-topic\", order)\n\n\n"
         "def enqueue_shipment(shipment):\n"
         "    return publish_event(\"shipment-events-topic\", shipment)\n"}};
    ASSERT_TRUE(edge_present(f, 1, "ASYNC_CALLS", 1));
    PASS();
}

/* DATA_FLOWS — caller -> handler through a shared Route: a decorator handler
 * (HANDLES) and an HTTP call to the SAME ANY-method path (HTTP_CALLS) on a
 * Route whose QN matches (__route__ANY__/orders). create_data_flows then links
 * the HTTP caller to the route handler. Sequential-path-safe: the HTTP call
 * resolves to a local "requests"-substring wrapper, and method NULL -> "ANY". */
TEST(contract_edge_data_flows) {
    static const LangFile f[] = {
        {"app.py", "from flask import Flask\n\napp = Flask(__name__)\n\n\n"
                   "@app.route(\"/orders\")\ndef list_orders():\n    return {\"orders\": []}\n\n\n"
                   "def requests_get(url, params=None):\n    return {\"url\": url}\n\n\n"
                   "def client():\n    return requests_get(\"/orders\")\n"}};
    ASSERT_TRUE(edge_present(f, 1, "DATA_FLOWS", 1)); /* client -> list_orders via /orders */
    PASS();
}

/* SIMILAR_TO — structural near-clones (>=0.95 MinHash Jaccard), same extension.
 * This is the exact clone pair from tests/test_simhash.c. */
TEST(contract_edge_similar_to) {
    static const LangFile f[] = {
        {"pkg/validation/user_validator.go",
         "package validation\nimport \"errors\"\nimport \"strings\"\n"
         "func ValidateUser(u User) error {\n"
         "    if u.Name == \"\" { return errors.New(\"name required\") }\n"
         "    if len(u.Name) > 100 { return errors.New(\"name too long\") }\n"
         "    if u.Age < 0 { return errors.New(\"invalid age\") }\n"
         "    if u.Age > 200 { return errors.New(\"age too high\") }\n"
         "    if u.Email == \"\" { return errors.New(\"email required\") }\n"
         "    if !strings.Contains(u.Email, \"@\") { return errors.New(\"invalid email\") }\n"
         "    if u.Phone == \"\" { return errors.New(\"phone required\") }\n"
         "    if len(u.Phone) < 7 { return errors.New(\"phone too short\") }\n"
         "    if u.Country == \"\" { return errors.New(\"country required\") }\n"
         "    for _, c := range u.Tags {\n        if c == \"\" { return errors.New(\"empty tag\") "
         "}\n    }\n"
         "    return nil\n}\n"},
        {"pkg/validation/order_validator.go",
         "package validation\nimport \"errors\"\nimport \"strings\"\n"
         "func ValidateOrder(o Order) error {\n"
         "    if o.Title == \"\" { return errors.New(\"title required\") }\n"
         "    if len(o.Title) > 100 { return errors.New(\"title too long\") }\n"
         "    if o.Amount < 0 { return errors.New(\"invalid amount\") }\n"
         "    if o.Amount > 200 { return errors.New(\"amount too high\") }\n"
         "    if o.Status == \"\" { return errors.New(\"status required\") }\n"
         "    if !strings.Contains(o.Status, \"@\") { return errors.New(\"invalid status\") }\n"
         "    if o.Region == \"\" { return errors.New(\"region required\") }\n"
         "    if len(o.Region) < 7 { return errors.New(\"region too short\") }\n"
         "    if o.Vendor == \"\" { return errors.New(\"vendor required\") }\n"
         "    for _, c := range o.Items {\n        if c == \"\" { return errors.New(\"empty "
         "item\") }\n    }\n"
         "    return nil\n}\n"}};
    ASSERT_TRUE(edge_present(f, 2, "SIMILAR_TO", 1));
    PASS();
}

/* SEMANTICALLY_RELATED — near-identical functions sharing vocabulary/callees/
 * types but DIFFERENT control-flow shapes, so the semantic score clears 0.75
 * while the structural MinHash stays under the 0.95 SIMILAR_TO short-circuit. */
TEST(contract_edge_semantically_related) {
    static const LangFile f[] = {
        {"records.py",
         "def sanitize(value: str) -> str:\n    return value.strip().lower()\n\n\n"
         "def lookup(table: dict, key: str) -> str:\n    return table.get(key, \"\")\n\n\n"
         "def audit_log(message: str) -> None:\n    print(message)\n\n\n"
         "def normalize_user_record(record: dict, table: dict) -> dict:\n"
         "    \"\"\"Normalize a user record by sanitizing fields and looking up defaults.\"\"\"\n"
         "    result = {}\n    name = sanitize(record.get(\"name\", \"\"))\n"
         "    email = sanitize(record.get(\"email\", \"\"))\n    role = lookup(table, name)\n"
         "    if name and email:\n        result[\"name\"] = name\n        result[\"email\"] = "
         "email\n"
         "        result[\"role\"] = role\n        audit_log(\"normalized user record\")\n"
         "    return result\n\n\n"
         "def normalize_account_record(record: dict, table: dict) -> dict:\n"
         "    \"\"\"Normalize an account record by sanitizing fields and looking up "
         "defaults.\"\"\"\n"
         "    result = {}\n    name = sanitize(record.get(\"name\", \"\"))\n"
         "    email = sanitize(record.get(\"email\", \"\"))\n    role = lookup(table, name)\n"
         "    while name and email:\n        result[\"name\"] = name\n        result[\"email\"] = "
         "email\n"
         "        result[\"role\"] = role\n        audit_log(\"normalized account record\")\n"
         "        break\n    return result\n\n\n"
         "def normalize_member_record(record: dict, table: dict) -> dict:\n"
         "    \"\"\"Normalize a member record by sanitizing fields and looking up defaults.\"\"\"\n"
         "    result = {}\n    name = sanitize(record.get(\"name\", \"\"))\n"
         "    email = sanitize(record.get(\"email\", \"\"))\n    role = lookup(table, name)\n"
         "    for _ in range(1):\n        if not (name and email):\n            continue\n"
         "        result[\"name\"] = name\n        result[\"email\"] = email\n"
         "        result[\"role\"] = role\n        audit_log(\"normalized member record\")\n"
         "    return result\n\n\n"
         "def normalize_profile_record(record: dict, table: dict) -> dict:\n"
         "    \"\"\"Normalize a profile record by sanitizing fields and looking up "
         "defaults.\"\"\"\n"
         "    result = {}\n    name = sanitize(record.get(\"name\", \"\"))\n"
         "    email = sanitize(record.get(\"email\", \"\"))\n    role = lookup(table, name)\n"
         "    try:\n        assert name and email\n        result[\"name\"] = name\n"
         "        result[\"email\"] = email\n        result[\"role\"] = role\n"
         "        audit_log(\"normalized profile record\")\n"
         "    except AssertionError:\n        audit_log(\"skipped profile record\")\n"
         "    return result\n"}};
    ASSERT_TRUE(edge_present(f, 1, "SEMANTICALLY_RELATED", 1));
    PASS();
}

/* ── P6 (continued): the remaining pipeline edge types ─────────────────
 *  TESTS + DEPENDS_ON are produced in any pipeline path. The service-protocol
 *  edges (GRAPHQL_CALLS/GRPC_CALLS/TRPC_CALLS) and INFRA_MAPS are emitted ONLY
 *  in the PARALLEL path (file_count > 50), and FILE_CHANGES_WITH needs real git
 *  history — so those use a padded / git-initialised fixture. All fixtures here
 *  were empirically verified to produce their edge against the prod binary. */

/* TESTS — function-level test->tested-function (distinct from TESTS_FILE).
 * Requires a CROSS-FILE call: a Test* function in a *_test file calling a
 * production function in a NON-test file (the target must not be in a test
 * path). pass_tests runs regardless of file count. */
TEST(contract_edge_tests) {
    static const LangFile f[] = {
        {"calc.go", "package calc\n\nfunc Add(a int, b int) int {\n\treturn a + b\n}\n\n"
                    "func Mul(a int, b int) int {\n\treturn a * b\n}\n"},
        {"calc_test.go",
         "package calc\n\nfunc TestAdd(t *T) {\n\tgot := Add(2, 3)\n\t_ = got\n}\n\n"
         "func TestMul(t *T) {\n\tgot := Mul(2, 3)\n\t_ = got\n}\n"}};
    ASSERT_TRUE(edge_present(f, 2, "TESTS", 1)); /* TestAdd->Add, TestMul->Mul */
    PASS();
}
/* The required control uses the exact named-export lexical subset audited in
 * resolver_binding_js. It prevents missing resolver metadata or an empty graph
 * from making either known-gap contract pass. */
static const LangFile LANG_CONTRACT_ECMA_CONTROL = {
    "quality_control.js", "export function target() { return 1; }\n"
                          "export function caller() { return target(); }\n"};
static const RQ_Expected LANG_CONTRACT_ECMA_REQUIRED = {
    .case_id = "required-esm-local-call",
    .site = {.file = "quality_control.js",
             .caller = "quality_control.caller",
             .needle = "target()",
             .occurrence_index = 1},
    .relation = "CALLS",
    .target = {.file = "quality_control.js",
               .suffix = "quality_control.target",
               .label = "Function",
               .declaration_line = 1,
               .declaration_text = "export function target() { return 1; }"},
    .mode = RQ_REQUIRED};

/* #408: retain the intended workspace dependency and call as quality debt.
 * Matching names/versions do not certify which installation satisfies b's
 * dependency. The source manifests, exact entry, and both genuine occurrences
 * remain independently authored inputs; no emitted edge creates the oracle. */
TEST(contract_edge_workspaces_imports_issue408) {
    static const LangFile f[] = {
        {"package.json", "{\"name\":\"root\",\"private\":true,\"workspaces\":[\"packages/*\"]}\n"},
        {"packages/a/package.json",
         "{\"name\":\"@org/a\",\"version\":\"1.0.0\",\"main\":\"index.js\"}\n"},
        {"packages/a/index.js", "export function fromA() {\n  return 1;\n}\n"},
        {"packages/b/package.json",
         "{\"name\":\"@org/b\",\"version\":\"1.0.0\",\"main\":\"index.js\","
         "\"dependencies\":{\"@org/a\":\"1.0.0\"}}\n"},
        {"packages/b/index.js",
         "import { fromA } from '@org/a';\n\nexport function useA() {\n  return fromA();\n}\n"}};
    LangFile files[6];
    memcpy(files, f, sizeof(f));
    files[5] = LANG_CONTRACT_ECMA_CONTROL;
    /* All three manifests remain full indexing inputs. They configure lookup
     * domains but are not language source snapshots in resolver_file_summaries. */
    const RQ_Source sources[] = {{.path = files[2].name, .text = files[2].content},
                                 {.path = files[4].name, .text = files[4].content},
                                 {.path = files[5].name, .text = files[5].content}};
    const RQ_Expected expected[] = {
        {.case_id = "workspace-dependency-import",
         .site = {.file = "packages/b/index.js",
                  .caller = "packages.b.index.js.__file__",
                  .needle = "import { fromA } from '@org/a';"},
         .relation = "IMPORTS",
         .target = {.file = "packages/a/index.js",
                    .suffix = "packages.a.index.js.__file__",
                    .label = "File",
                    .declaration_line = 1,
                    .declaration_text = "export function fromA() {"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "node_workspace_dependency_binding_unproven",
         .why = "The nearer dependency needs an installation/workspace binding witness; "
                "co-located equal package names and versions are insufficient.",
         .tried = "Preserved all three package manifests and the exact entry file. "
                  "The audited dependency barrier abstains before enclosing-workspace lookup; "
                  "the source import span also remains required for future recovery."},
        {.case_id = "workspace-dependency-call",
         .site = {.file = "packages/b/index.js", .caller = "packages.b.useA", .needle = "fromA()"},
         .relation = "CALLS",
         .target = {.file = "packages/a/index.js",
                    .suffix = "packages.a.fromA",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "export function fromA() {"},
         .mode = RQ_KNOWN_GAP,
         .gap_id = "node_workspace_dependency_callable_binding_unproven",
         .why = "The genuine useA call depends on the same unproved package binding.",
         .tried = "Kept the named import and full fromA() occurrence. The unresolved import "
                  "blocks a same-name fallback rather than borrowing the indexed sibling."},
        LANG_CONTRACT_ECMA_REQUIRED};
    LangProj lp;
    cbm_store_t *store = lang_index_files(&lp, files, 6);
    ASSERT_NOT_NULL(store);
    RQ_Report report;
    bool accepted =
        rq_check_project_declarations(store, lp.project, "workspace-dependency-contract", sources,
                                      3, expected, 3, NULL, 0, &report, stderr);
    lang_cleanup(&lp, store);
    ASSERT(accepted);
    ASSERT_EQ(report.genuine, 3); /* import + original call + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 2);
    PASS();
}

/* #767: a wildcard tsconfig alias for the "@lib" prefix (mapped to ./src/lib)
 * shares that prefix with an unrelated scoped npm package ("@lib/external-pkg",
 * meant to resolve normally from node_modules). The engine has no such file
 * and must NOT invent an edge to the "src/lib" Folder node via a later
 * fallback strategy that re-tries the truncated "@lib" prefix against the
 * tsconfig's other, bare alias entry. Zero IMPORTS edges in the whole project
 * is the correct outcome: the same as any other unresolved external import. */
TEST(contract_edge_imports_alias_no_phantom_folder_edge_issue767) {
    LangProj lp;
    static const LangFile f[] = {
        {"tsconfig.json", "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                          "      \"@lib\": [\"./src/lib\"],\n"
                          "      \"@lib/*\": [\"./src/lib/*\"]\n    }\n  }\n}\n"},
        {"src/lib/thing.ts", "export const Thing = {};\n"},
        {"src/consumer.ts", "import { ClientC } from '@lib/external-pkg';\n\n"
                            "export function useClient() {\n  return new ClientC();\n}\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 3);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") : -1;
    if (got != 0) {
        fprintf(stderr, "  [EDGE] FAIL IMPORTS count=%d expected=0 (phantom Folder edge)\n", got);
    }
    ASSERT_EQ(got, 0);
    lang_cleanup(&lp, store);
    PASS();
}

/* #767 regression guard: a wildcard tsconfig alias resolving to a REAL,
 * indexed file must still produce its IMPORTS edge — the import-targetable
 * label filter must reject Folder/Project/etc. matches without rejecting
 * legitimate File/Module matches. */
TEST(contract_edge_imports_alias_resolves_real_file_issue767) {
    static const LangFile f[] = {
        {"tsconfig.json", "{\n  \"compilerOptions\": {\n    \"paths\": {\n"
                          "      \"@lib\": [\"./src/lib\"],\n"
                          "      \"@lib/*\": [\"./src/lib/*\"]\n    }\n  }\n}\n"},
        {"src/lib/thing.ts", "export const Thing = {};\n"},
        {"src/consumer.ts", "import { Thing } from '@lib/thing';\n\n"
                            "export function useThing() {\n  return Thing;\n}\n"}};
    ASSERT_TRUE(edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* True if a CALLS edge exists whose source QN ends with `src_suffix` and
 * target QN ends with `tgt_suffix`. */
static int calls_edge_between(cbm_store_t *store, const char *project, const char *src_suffix,
                              const char *tgt_suffix) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return 0;
    int found = 0;
    size_t ssl = strlen(src_suffix);
    size_t tsl = strlen(tgt_suffix);
    for (int i = 0; i < n && !found; i++) {
        cbm_node_t s, t;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &s) != CBM_STORE_OK)
            continue;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &t) != CBM_STORE_OK) {
            cbm_node_free_fields(&s);
            continue;
        }
        const char *sq = s.qualified_name;
        const char *tq = t.qualified_name;
        if (sq && tq) {
            size_t sql = strlen(sq);
            size_t tql = strlen(tq);
            if (sql >= ssl && strcmp(sq + sql - ssl, src_suffix) == 0 && tql >= tsl &&
                strcmp(tq + tql - tsl, tgt_suffix) == 0)
                found = 1;
        }
        cbm_node_free_fields(&s);
        cbm_node_free_fields(&t);
    }
    cbm_store_free_edges(edges, n);
    return found;
}

/* #999: URLs in CI/tooling configs (.pre-commit-config.yaml, .github/
 * workflows) are repository references for TOOLING, not endpoints this
 * service exposes. They must not mint __route__infra__ Route nodes — the
 * route matcher's root-service heuristic otherwise attaches every handler
 * of an ambiguous "/" route to each junk URL (HANDLES churn on plain
 * pallets/flask). */
static int count_infra_routes_matching(cbm_store_t *store, const char *project,
                                       const char *substr) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, "Route", &nodes, &count) != CBM_STORE_OK)
        return -1;
    int hits = 0;
    for (int i = 0; i < count; i++) {
        const char *qn = nodes[i].qualified_name;
        if (qn && strncmp(qn, "__route__infra__", (sizeof("__route__infra__") - 1)) == 0 &&
            strstr(qn, substr))
            hits++;
    }
    cbm_store_free_nodes(nodes, count);
    return hits;
}

TEST(contract_edge_no_infra_routes_from_ci_configs_issue999) {
    LangProj lp;
    static const LangFile f[] = {
        {"app.py", "from flask import Flask\n"
                   "app = Flask(__name__)\n"
                   "\n"
                   "@app.route(\"/\")\n"
                   "def index():\n"
                   "    return \"hi\"\n"
                   "\n"
                   "@app.route(\"/auth/login\")\n"
                   "def login():\n"
                   "    return \"login\"\n"},
        {".pre-commit-config.yaml", "repos:\n"
                                    "  - repo: https://github.com/astral-sh/ruff-pre-commit\n"
                                    "    rev: v0.1.0\n"
                                    "    hooks:\n"
                                    "      - id: ruff\n"
                                    "  - repo: https://github.com/pre-commit/pre-commit-hooks\n"
                                    "    rev: v4.5.0\n"
                                    "    hooks:\n"
                                    "      - id: trailing-whitespace\n"},
        {".github/workflows/ci.yml", "name: CI\n"
                                     "on: [push]\n"
                                     "jobs:\n"
                                     "  build:\n"
                                     "    runs-on: ubuntu-latest\n"
                                     "    steps:\n"
                                     "      - uses: actions/checkout@v4\n"
                                     "      - run: curl https://coverage.example.io/upload\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 3);
    ASSERT_NOT_NULL(store);
    /* No tooling URL may materialize as an infra Route node. */
    int gh_routes = count_infra_routes_matching(store, lp.project, "github.com");
    int wf_routes = count_infra_routes_matching(store, lp.project, "coverage.example.io");
    if (gh_routes != 0 || wf_routes != 0) {
        fprintf(stderr,
                "  [999] FAIL infra routes from CI configs: github.com=%d workflows=%d "
                "(expected 0/0)\n",
                gh_routes, wf_routes);
    }
    ASSERT_EQ(gh_routes, 0);
    ASSERT_EQ(wf_routes, 0);
    lang_cleanup(&lp, store);
    PASS();
}

/* #999 inverse guard: genuine infra endpoint URLs (Cloud Scheduler push
 * targets and similar deployment configs) must STILL mint infra Route
 * nodes — the CI/tooling deny must be file-scoped, not a blanket URL kill. */
TEST(contract_edge_infra_routes_from_deploy_configs_still_minted) {
    LangProj lp;
    static const LangFile f[] = {
        {"scheduler.yaml", "jobs:\n"
                           "  - name: nightly-sync\n"
                           "    schedule: \"0 3 * * *\"\n"
                           "    push_endpoint: https://sync.internal.example/api/v1/sync\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 1);
    ASSERT_NOT_NULL(store);
    int routes = count_infra_routes_matching(store, lp.project, "sync.internal.example");
    if (routes < 1) {
        fprintf(stderr,
                "  [999] FAIL deploy-config endpoint minted %d infra routes "
                "(expected >=1)\n",
                routes);
    }
    ASSERT_TRUE(routes >= 1);
    lang_cleanup(&lp, store);
    PASS();
}

/* #2388: the YAML key-path walk kept its pending mappings in a 256-frame stack
 * and stopped pushing once full. A `data:` payload block with more nested
 * entries than that lost the full `data.<entry>.webhook_url` path of every entry
 * past the cap; only the shorter per-mapping refs survived, the payload deny
 * never saw them, and each of those values minted an infra Route (44 of 300
 * here). Every payload value must stay unminted however wide the block is. */
enum { WIDE_PAYLOAD_ENTRIES = 300 };

TEST(contract_edge_no_infra_routes_from_wide_payload_block_issue2388) {
    size_t cap = (size_t)WIDE_PAYLOAD_ENTRIES * 64 + 16;
    char *src = malloc(cap);
    ASSERT_NOT_NULL(src);
    size_t pos = (size_t)snprintf(src, cap, "data:\n");
    for (int i = 0; i < WIDE_PAYLOAD_ENTRIES; i++) {
        pos +=
            (size_t)snprintf(src + pos, cap - pos,
                             "  svc%d:\n    webhook_url: https://h%d.payload.example/hook\n", i, i);
    }
    LangProj lp;
    LangFile f[] = {{"service.yaml", src}};
    cbm_store_t *store = lang_index_files(&lp, f, 1);
    ASSERT_NOT_NULL(store);
    int routes = count_infra_routes_matching(store, lp.project, "payload.example");
    if (routes != 0) {
        fprintf(stderr, "  [2388] FAIL wide payload block minted %d infra routes (expected 0)\n",
                routes);
    }
    ASSERT_EQ(routes, 0);
    lang_cleanup(&lp, store);
    free(src);
    PASS();
}

/* A config URL becomes an infra Route only under a key that names an endpoint
 * this service exposes. Reference links are not: across 17 infra repositories
 * the infra Routes were changelog `pull_request` links (deckhouse 6,240),
 * alert `runbook_url`s, chart `home`/`source`, kubeconfig `server`s, Argo
 * `repoURL`s and docs `link`s -- and one root handler took all of them. */
TEST(contract_edge_no_infra_routes_from_reference_links) {
    LangProj lp;
    static const LangFile f[] = {
        {"CHANGELOG/v1.29.yml", "candi:\n"
                                "  fixes:\n"
                                "    - summary: fix a race\n"
                                "      pull_request: https://github.com/org/repo/pull/1234\n"},
        {"modules/cni/oss.yaml", "- name: Cilium\n"
                                 "  link: https://github.com/cilium/cilium\n"},
        {"charts/app/alerts.yaml", "groups:\n"
                                   "  - name: app\n"
                                   "    rules:\n"
                                   "      - alert: Down\n"
                                   "        annotations:\n"
                                   "          runbook_url: https://runbooks.example.io/down\n"},
        {"apps/app.yaml", "spec:\n"
                          "  source:\n"
                          "    repoURL: https://charts.example.io/stable\n"
                          "  home: https://app.example.io\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 4);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(count_infra_routes_matching(store, lp.project, "https://"), 0);
    lang_cleanup(&lp, store);
    PASS();
}

/* The endpoint keys keep minting in their camelCase / _url spellings. */
TEST(contract_edge_infra_routes_from_endpoint_key_spellings) {
    LangProj lp;
    static const LangFile f[] = {
        {"pubsub.yaml", "subscriptions:\n"
                        "  - name: orders\n"
                        "    pushEndpoint: https://orders.internal.example/push\n"},
        {"hooks.yaml", "payments:\n"
                       "  callbackUrl: https://pay.internal.example/callback\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 2);
    ASSERT_NOT_NULL(store);
    ASSERT_TRUE(count_infra_routes_matching(store, lp.project, "orders.internal.example") >= 1);
    ASSERT_TRUE(count_infra_routes_matching(store, lp.project, "pay.internal.example") >= 1);
    lang_cleanup(&lp, store);
    PASS();
}

/* True if some CALLS edge's TARGET node carries `label` and a QN ending with
 * `qn_suffix` — i.e. the call resolved to that specific definition. */
static int calls_edge_targets(cbm_store_t *store, const char *project, const char *label,
                              const char *qn_suffix) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) != CBM_STORE_OK)
        return 0;
    int found = 0;
    size_t sl = strlen(qn_suffix);
    for (int i = 0; i < n && !found; i++) {
        cbm_node_t tgt;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
            continue;
        const char *qn = tgt.qualified_name;
        if (qn && tgt.label && strcmp(tgt.label, label) == 0) {
            size_t ql = strlen(qn);
            if (ql >= sl && strcmp(qn + ql - sl, qn_suffix) == 0)
                found = 1;
        }
        cbm_node_free_fields(&tgt);
    }
    cbm_store_free_edges(edges, n);
    return found;
}

/* #988: `from m import f as g; g()` produced NO CALLS edge — the Python LSP
 * bound the alias as MODULE("m.f") (the from-style heuristic keys on the
 * local name matching the module-path tail, which an alias never does), so
 * the call missed, and the registry fallback did not cover the minimal
 * two-file shape either. The alias must resolve exactly like the plain
 * import. The other import forms are pinned alongside as invariance guards
 * (all resolve on main today and must keep resolving). */
TEST(contract_edge_python_aliased_import_call_resolves_issue988) {
    LangProj lp;
    static const LangFile f[] = {{"m.py", "def f(x):\n    return x + 1\n"},
                                 {"pkg/__init__.py", ""},
                                 {"pkg/dm.py", "def h(x):\n    return x\n"},
                                 {"caller_alias.py", "from m import f as g\n"
                                                     "\n"
                                                     "def use_alias(x):\n"
                                                     "    return g(x)\n"},
                                 {"caller_plain.py", "from m import f\n"
                                                     "\n"
                                                     "def use_plain(x):\n"
                                                     "    return f(x)\n"},
                                 {"caller_modalias.py", "import m as mm\n"
                                                        "\n"
                                                        "def use_modalias(x):\n"
                                                        "    return mm.f(x)\n"},
                                 {"caller_dotalias.py", "import pkg.dm as dz\n"
                                                        "\n"
                                                        "def use_dotalias(x):\n"
                                                        "    return dz.h(x)\n"}};
    cbm_store_t *store = lang_index_files(&lp, f, 7);
    ASSERT_TRUE(store != NULL);
    int alias = calls_edge_between(store, lp.project, ".use_alias", ".m.f");
    int plain = calls_edge_between(store, lp.project, ".use_plain", ".m.f");
    int modalias = calls_edge_between(store, lp.project, ".use_modalias", ".m.f");
    int dotalias = calls_edge_between(store, lp.project, ".use_dotalias", ".pkg.dm.h");
    if (!alias || !plain || !modalias || !dotalias) {
        fprintf(stderr,
                "  [988] FAIL alias=%d plain=%d modalias=%d dotalias=%d (all import forms "
                "must produce the CALLS edge)\n",
                alias, plain, modalias, dotalias);
    }
    ASSERT_TRUE(alias); /* the #988 bug: aliased from-import call lost */
    ASSERT_TRUE(plain);
    ASSERT_TRUE(modalias);
    ASSERT_TRUE(dotalias);
    lang_cleanup(&lp, store);
    PASS();
}

/* #871: the original CommonJS exported-function call remains a genuine
 * missing edge. The shared source binder does not yet certify require-bound
 * callable values or module.exports; emitting the local alias Variable would
 * still be a bug. Keep the reporter's exact two-file source and alias guard. */
TEST(contract_edge_commonjs_require_call_resolves_issue871) {
    LangProj lp;
    static const LangFile f[] = {
        {"src/bs/doThing.js",
         "module.exports = async function doThing({ id }) {\n  return id;\n};\n"},
        {"src/mutations/doThing.js",
         "const doThing = require(\"../bs/doThing\");\n\n"
         "module.exports = async (parent, args) => {\n  return doThing({ id: args.id });\n};\n"}};
    LangFile files[3];
    memcpy(files, f, sizeof(f));
    files[2] = LANG_CONTRACT_ECMA_CONTROL;
    RQ_Source sources[3];
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    /* An assignment-owned anonymous arrow has no named callable declaration;
     * the extractor's canonical enclosing owner is this source module. */
    const RQ_Expected expected[] = {
        {.case_id = "commonjs-exported-function-call",
         .site = {.file = "src/mutations/doThing.js",
                  .caller = "src.mutations.doThing",
                  .needle = "doThing({ id: args.id })"},
         .relation = "CALLS",
         .target = {.file = "src/bs/doThing.js",
                    .suffix = "src.bs.doThing.doThing",
                    .label = "Function",
                    .declaration_line = 1,
                    .declaration_text = "module.exports = async function doThing({ id }) {"}},
        LANG_CONTRACT_ECMA_REQUIRED};
    const RQ_Forbidden forbidden[] = {
        {.case_id = "commonjs-import-alias-is-not-call-target",
         .site = {.file = "src/mutations/doThing.js",
                  .caller = "src.mutations.doThing",
                  .needle = "doThing({ id: args.id })"},
         .relation = "CALLS",
         .target = {.file = "src/mutations/doThing.js",
                    .suffix = "src.mutations.doThing.doThing",
                    .label = "Variable",
                    .declaration_line = 1,
                    .declaration_text = "const doThing = require(\"../bs/doThing\");"}}};
    cbm_store_t *store = lang_index_files(&lp, files, 3);
    ASSERT_NOT_NULL(store);
    RQ_Report report;
    bool accepted =
        rq_check_project_declarations(store, lp.project, "commonjs-callable-contract", sources, 3,
                                      expected, 2, forbidden, 1, &report, stderr);
    /* Preserve the original whole-project wrong-target guard as well as the
     * independently authored occurrence-level forbidden expectation. */
    int shadowed = calls_edge_targets(store, lp.project, "Variable", ".mutations.doThing.doThing");
    lang_cleanup(&lp, store);
    ASSERT(accepted);
    ASSERT_TRUE(!shadowed);
    ASSERT_EQ(report.genuine, 2); /* original exported-function call + required control */
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 1);
    PASS();
}

/* DEPENDS_ON — Helm Chart.yaml `dependencies:` -> per-dependency Chart node.
 * Basename must be exactly "Chart.yaml"; pass_k8s runs in both pipeline paths. */
TEST(contract_edge_depends_on) {
    static const LangFile f[] = {
        {"charts/myapp/Chart.yaml",
         "apiVersion: v2\nname: myapp\nversion: 1.0.0\ndescription: A test chart\n"
         "dependencies:\n  - name: postgresql\n    version: \"11.6.12\"\n"
         "    repository: \"https://charts.bitnami.com/bitnami\"\n"
         "  - name: redis\n    version: \"16.8.5\"\n"
         "    repository: \"https://charts.bitnami.com/bitnami\"\n"},
        {"main.py", "def hello():\n    return \"world\"\n"}};
    ASSERT_TRUE(edge_present(f, 2, "DEPENDS_ON", 1)); /* postgresql, redis */
    PASS();
}

/* Index `meaningful` files plus enough trivial pad files to cross the
 * MIN_FILES_FOR_PARALLEL (=50) threshold, forcing the PARALLEL pipeline path
 * (the only path that emits GRAPHQL_CALLS/GRPC_CALLS/TRPC_CALLS/INFRA_MAPS). */
enum { PARALLEL_PAD_FILES = 52 };

static cbm_store_t *index_parallel_fixture(LangProj *lp, const LangFile *meaningful, int n_mean) {
    static char pad_name[PARALLEL_PAD_FILES][40];
    static char pad_body[PARALLEL_PAD_FILES][64];
    LangFile files[PARALLEL_PAD_FILES + 16] = {0}; /* zero-init: GCC -Werror=maybe-uninitialized */
    int n = 0;
    for (int i = 0; i < n_mean; i++) {
        files[n++] = meaningful[i];
    }
    for (int i = 0; i < PARALLEL_PAD_FILES; i++) {
        snprintf(pad_name[i], sizeof(pad_name[i]), "pad/pad_%02d.py", i);
        snprintf(pad_body[i], sizeof(pad_body[i]), "def pad_%02d():\n    return %d\n", i, i);
        files[n].name = pad_name[i];
        files[n].content = pad_body[i];
        n++;
    }
    return lang_index_files(lp, files, n);
}

/* GRAPHQL_CALLS + GRPC_CALLS + TRPC_CALLS + INFRA_MAPS — all parallel-path-only,
 * so they share one padded (>50-file) index. Each edge is triggered by an
 * independent in-repo symbol whose resolved QN carries the relevant library
 * substring; combining them in one repo doesn't couple them. */
TEST(contract_edge_parallel_service_edges) {
    static const LangFile meaningful[] = {
        /* GRAPHQL_CALLS: local gql() — resolved callee QN contains "gql". */
        {"gql.py", "def gql(query_string):\n    return query_string\n"},
        {"client.py",
         "from gql import gql\n\n\ndef fetch_user():\n"
         /* The embedded double quote survives extraction from this single-quoted
          * Python string and breaks GRAPHQL_CALLS JSON unless operation is escaped. */
         "    return gql('query GetUser { user(name: \"quoted\") { id name } }')\n\n\n"
         "def create_user():\n"
         "    return gql(\"mutation CreateUser { addUser(name: \\\"x\\\") { id } }\")\n"},
        /* TRPC_CALLS: local createTRPCProxyClient (same-module resolution). */
        {"trpc_client.ts",
         "export function createTRPCProxyClient(opts: any): any {\n  return { invoke: () => opts "
         "};\n}\n\n"
         "export function loadUser(id: string) {\n  const client = createTRPCProxyClient({ id });\n"
         "  return client;\n}\n"},
        /* GRPC_CALLS: Go ServiceClient suffix heuristic (cross-package call). */
        {"go.mod", "module example.com/grpcdemo\n\ngo 1.21\n"},
        {"cartpb/cart_grpc.pb.go",
         "package cartpb\n\nimport \"context\"\n\ntype GetCartRequest struct{ UserId string }\n"
         "type GetCartResponse struct{ Items int }\n\ntype CartServiceClient interface {\n"
         "\tGetCart(ctx context.Context, in *GetCartRequest) (*GetCartResponse, error)\n}\n\n"
         "type cartServiceClient struct{}\n\nfunc NewCartServiceClient(cc interface{}) "
         "CartServiceClient {\n"
         "\treturn &cartServiceClient{}\n}\n\nfunc (c *cartServiceClient) GetCart(ctx "
         "context.Context, "
         "in *GetCartRequest) (*GetCartResponse, error) {\n\treturn &GetCartResponse{}, nil\n}\n"},
        {"client/main.go",
         "package main\n\nimport (\n\t\"context\"\n\n\tpb \"example.com/grpcdemo/cartpb\"\n)\n\n"
         "func FetchCart(conn interface{}) {\n\tclient := pb.NewCartServiceClient(conn)\n"
         "\tclient.GetCart(context.Background(), &pb.GetCartRequest{UserId: \"u1\"})\n}\n\n"
         "func main() {\n\tFetchCart(nil)\n}\n"},
        /* INFRA_MAPS: ASYNC_CALLS topic route (pubsub) matched to an infra YAML
         * subscription whose push_endpoint becomes the infra route. */
        {"app/pubsub.py", "def publish(topic, data):\n    return (topic, data)\n"},
        {"app/publisher.py", "import pubsub\n\n\ndef publish_order(data):\n"
                             "    pubsub.publish(\"order-events\", data)\n"},
        {"infra/pubsub/subscription.yaml",
         "subscriptions:\n  - name: order-worker-sub\n    topic: order-events\n"
         "    config:\n      push_endpoint: https://order-worker-abc123-uc.a.run.app/handle\n"}};

    LangProj lp;
    cbm_store_t *store =
        index_parallel_fixture(&lp, meaningful, (int)(sizeof(meaningful) / sizeof(meaningful[0])));
    int graphql = store ? cbm_store_count_edges_by_type(store, lp.project, "GRAPHQL_CALLS") : -1;
    int grpc = store ? cbm_store_count_edges_by_type(store, lp.project, "GRPC_CALLS") : -1;
    int trpc = store ? cbm_store_count_edges_by_type(store, lp.project, "TRPC_CALLS") : -1;
    int infra = store ? cbm_store_count_edges_by_type(store, lp.project, "INFRA_MAPS") : -1;
    int invalid_props = -1;
    if (store) {
        sqlite3_stmt *stmt = NULL;
        sqlite3 *db = cbm_store_get_db(store);
        if (db &&
            sqlite3_prepare_v2(db,
                               "SELECT count(*) FROM edges WHERE properties IS NOT NULL "
                               "AND properties != '' AND json_valid(properties)=0;",
                               -1, &stmt, NULL) == SQLITE_OK &&
            sqlite3_step(stmt) == SQLITE_ROW) {
            invalid_props = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    if (graphql < 1 || grpc < 1 || trpc < 1 || infra < 1) {
        fprintf(stderr,
                "  [EDGE] parallel-service: GRAPHQL_CALLS=%d GRPC_CALLS=%d TRPC_CALLS=%d "
                "INFRA_MAPS=%d\n",
                graphql, grpc, trpc, infra);
        dump_edge_histogram(store, lp.project);
    }
    lang_cleanup(&lp, store);
    ASSERT_TRUE(graphql >= 1);
    ASSERT_TRUE(grpc >= 1);
    ASSERT_TRUE(trpc >= 1);
    ASSERT_TRUE(infra >= 1);
    ASSERT_EQ(invalid_props, 0);
    PASS();
}

/* Run "git -C <dir> <args>"; direct git invocation (no shell builtins) so it
 * works under both POSIX shells and cmd.exe. Returns the command exit status. */
static int run_git(const char *dir, const char *args) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" %s", dir, args);
    return system(cmd);
}

/* FILE_CHANGES_WITH — git co-change coupling: two files that change together
 * across >=3 commits (coupling >= 0.3). The standard harness indexes a plain
 * non-git dir, so this test builds its own git repo (init + 4 commits touching
 * BOTH files) before indexing via lang_open_indexed. */
TEST(contract_edge_file_changes_with) {
    LangProj lp;
    memset(&lp, 0, sizeof(lp));
    snprintf(lp.tmpdir, sizeof(lp.tmpdir), "/tmp/cbm_fcw_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(lp.tmpdir));
    lc_to_fwd_slashes(lp.tmpdir);

    char a[700];
    char b[700];
    snprintf(a, sizeof(a), "%s/alpha.py", lp.tmpdir);
    snprintf(b, sizeof(b), "%s/beta.py", lp.tmpdir);
    th_write_file(a, "def alpha_v1():\n    return 1\n");
    th_write_file(b, "def beta_v1():\n    return 1\n");

    /* git must be present (this IS a git project) — a failed init means a broken
     * environment, which is a real failure, not a skip. git is installed on every
     * CI platform (incl. the Windows msys2 env), and run_git uses `git -C` with no
     * POSIX-shell syntax, so this runs everywhere. */
    ASSERT_EQ(run_git(lp.tmpdir, "init -q"), 0);
    run_git(lp.tmpdir, "config user.email t@t.io");
    run_git(lp.tmpdir, "config user.name t");
    run_git(lp.tmpdir, "add -A");
    run_git(lp.tmpdir, "commit -qm c1");
    for (int i = 2; i <= 4; i++) {
        th_append_file(a, "\n\ndef alpha_more():\n    return 9\n");
        th_append_file(b, "\n\ndef beta_more():\n    return 9\n");
        char msg[24];
        run_git(lp.tmpdir, "add -A");
        snprintf(msg, sizeof(msg), "commit -qm c%d", i);
        run_git(lp.tmpdir, msg);
    }

    cbm_store_t *store = lang_open_indexed(&lp);
    int fcw = store ? cbm_store_count_edges_by_type(store, lp.project, "FILE_CHANGES_WITH") : -1;
    if (fcw < 1) {
        dump_edge_histogram(store, lp.project);
    }
    lang_cleanup(&lp, store);
    ASSERT_TRUE(fcw >= 1); /* alpha.py <-> beta.py co-change across 4 commits */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════ */

SUITE(lang_contract) {
    /* The three known-bug reproductions. Kotlin reproduces in-fixture (an
     * extraction-layer bug, scale-independent). C call-attribution and the
     * Java/TS extraction crash only manifest at real-repo scale, so a small
     * fixture passes here — their reproductions live in the real-repo scale
     * tier; these fast contracts still guard against regressions. */
    RUN_TEST(contract_kotlin_imports_extracted);
    RUN_TEST(contract_c_calls_attributed_to_function);
    RUN_TEST(contract_java_extract_no_crash);

    /* Rich per-language invariants (P3). */
    RUN_TEST(contract_go_calls);
    RUN_TEST(contract_rust_methods);
    RUN_TEST(contract_csharp_methods);
    RUN_TEST(contract_php_methods);
    RUN_TEST(contract_java_methods);
    RUN_TEST(contract_kotlin_methods);
    RUN_TEST(contract_python_relative_import);
    RUN_TEST(contract_python_aliased_from_import_calls);
    RUN_TEST(contract_python_absolute_aliased_from_import_calls);
    RUN_TEST(contract_python_ghost_module_aliased_from_import_no_calls);
    RUN_TEST(contract_typescript_relative_import);

    /* Graph-level breadth across all grammars (P4). */
    RUN_TEST(contract_chialisp_constant_resolves_across_files);
    RUN_TEST(contract_all_grammars_in_graph);

    /* CALLS-edge breadth across non-LSP languages (P5). */
    RUN_TEST(contract_calls_breadth);

    /* Cross-cutting / semantic edge-type presence (P6). Each asserts the
     * pipeline still emits that edge type; a RED here is a real regression. */
    RUN_TEST(contract_edge_defines);
    RUN_TEST(contract_edge_defines_method);
    RUN_TEST(contract_edge_contains_file);
    RUN_TEST(contract_edge_inherits);
    RUN_TEST(contract_edge_implements);
    RUN_TEST(contract_edge_decorates);
    RUN_TEST(contract_edge_tests_file);
    RUN_TEST(contract_edge_handles);
    RUN_TEST(contract_edge_http_calls);
    RUN_TEST(contract_edge_async_calls);
    RUN_TEST(contract_edge_data_flows);
    RUN_TEST(contract_edge_similar_to);
    RUN_TEST(contract_edge_semantically_related);

    /* P6 (continued): remaining edge types — TESTS, DEPENDS_ON, the
     * parallel-path service edges (GRAPHQL/GRPC/TRPC_CALLS + INFRA_MAPS), and
     * FILE_CHANGES_WITH (git co-change). Completes 25-edge-type coverage. */
    RUN_TEST(contract_edge_tests);
    RUN_TEST(contract_edge_workspaces_imports_issue408);
    RUN_TEST(contract_edge_imports_alias_no_phantom_folder_edge_issue767);
    RUN_TEST(contract_edge_imports_alias_resolves_real_file_issue767);
    RUN_TEST(contract_edge_python_aliased_import_call_resolves_issue988);
    RUN_TEST(contract_edge_no_infra_routes_from_ci_configs_issue999);
    RUN_TEST(contract_edge_infra_routes_from_deploy_configs_still_minted);
    RUN_TEST(contract_edge_no_infra_routes_from_wide_payload_block_issue2388);
    RUN_TEST(contract_edge_no_infra_routes_from_reference_links);
    RUN_TEST(contract_edge_infra_routes_from_endpoint_key_spellings);
    RUN_TEST(contract_edge_commonjs_require_call_resolves_issue871);
    RUN_TEST(contract_edge_depends_on);
    RUN_TEST(contract_edge_parallel_service_edges);
    RUN_TEST(contract_edge_file_changes_with);
}
