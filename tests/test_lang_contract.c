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
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

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
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) {
        return NULL;
    }
    const char *home = getenv("HOME");
    if (!home) {
        home = "/tmp";
    }
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
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

/* ══════════════════════════════════════════════════════════════════
 *  CALLS-EDGE BREADTH (P5) — beyond the 9 hybrid-LSP languages, verify that a
 *  same-file caller->callee resolves to a CALLS edge through the generic
 *  name-based resolver (pass_calls.c). Languages where it does NOT are real
 *  edge gaps (existing bugs) — flagged here with expect_calls=false + a note,
 *  reproducibly, for the next (bugfixing) phase. Fixtures authored per-language
 *  to parse cleanly with the vendored grammar.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    const char *lang;
    const char *filename;
    const char *content;
    bool expect_calls;    /* true: must resolve a CALLS edge; false: known edge gap */
    const char *gap_note; /* why it doesn't resolve (for known gaps) */
} CallCase;

static const CallCase CALL_CASES[] = {
    {"ruby", "a.rb", "def helper\n  42\nend\n\ndef run\n  helper()\nend\n", true, NULL},
    {"lua", "a.lua",
     "function helper(x)\n  return x + 1\nend\n\nfunction run()\n  return helper(41)\nend\n", true,
     NULL},
    {"swift", "a.swift",
     "func helper() -> Int {\n    return 42\n}\n\nfunc run() {\n    let value = helper()\n    "
     "print(value)\n}\n",
     true, NULL},
    {"dart", "a.dart", "void helper() {\n  print('helper');\n}\n\nvoid run() {\n  helper();\n}\n",
     false, "selector call node carries no callee field; no dart branch in extract_calls.c"},
    {"scala", "a.scala", "def helper(): Int =\n  21 + 21\n\ndef run(): Int =\n  helper() * 2\n",
     true, NULL},
    {"bash", "a.sh", "helper() {\n  echo \"doing work\"\n}\n\nrun() {\n  helper\n}\n", true, NULL},
    {"perl", "a.pl",
     "#!/usr/bin/perl\nuse strict;\nuse warnings;\n\nsub helper {\n    my ($name) = @_;\n    "
     "return \"Hello, $name\";\n}\n\nsub run {\n    my $greeting = helper(\"world\");\n    print "
     "\"$greeting\\n\";\n    return;\n}\n\nrun();\n",
     true, NULL},
    {"r", "a.R", "helper <- function(x) {\n  x * 2\n}\n\nrun <- function() {\n  helper(21)\n}\n",
     true, NULL},
    {"julia", "a.jl",
     "function helper(x)\n    return x + 1\nend\n\nfunction run(n)\n    return helper(n)\nend\n",
     true, NULL},
    {"zig", "a.zig", "fn helper() void {}\n\npub fn run() void {\n    helper();\n}\n", true, NULL},
    {"gdscript", "a.gd",
     "func helper() -> int:\n\treturn 42\n\n\nfunc run() -> void:\n\tvar value = "
     "helper()\n\tprint(value)\n",
     true, NULL},
    {"groovy", "a.groovy",
     "def helper() {\n    println \"helping\"\n}\n\ndef run() {\n    helper()\n}\n", false,
     "function_call callee not on a function/name field and first child is not 'identifier'; no "
     "groovy branch in extract_calls.c"},
    {"elixir", "a.ex",
     "defmodule Sample do\n  def helper(x) do\n    x + 1\n  end\n\n  def run do\n    helper(41)\n  "
     "end\nend\n",
     true, NULL},
    {"ocaml", "a.ml",
     "let helper x = x + 1\n\nlet run () =\n  let result = helper 41 in\n  print_int result\n",
     true, NULL},
    {"gleam", "a.gleam",
     "pub fn helper(x: Int) -> Int {\n  x + 1\n}\n\npub fn run() -> Int {\n  helper(2)\n}\n", true,
     NULL},
    {"crystal", "a.cr",
     "def helper(name : String) : String\n  \"hello, #{name}\"\nend\n\ndef run\n  message = "
     "helper(\"world\")\n  puts message\nend\n\nrun\n",
     true, NULL},
    {"haskell", "a.hs", "helper :: Int -> Int\nhelper x = x + 1\n\nrun :: Int\nrun = helper 41\n",
     true, NULL},
    {"fortran", "a.f90",
     "function helper(x) result(y)\n    integer, intent(in) :: x\n    integer :: y\n    y = x + "
     "1\nend function helper\n\nfunction run(n) result(total)\n    integer, intent(in) :: n\n    "
     "integer :: total\n    total = helper(n) + helper(n + 1)\nend function run\n",
     true, NULL},
    {"pascal", "a.pas",
     "procedure Helper(x: Integer);\nbegin\n  WriteLn(x);\nend;\n\nprocedure Run;\nbegin\n  "
     "Helper(1);\nend;\n",
     true, NULL},
    {"tcl", "a.tcl",
     "proc helper {x} {\n    return [expr {$x * 2}]\n}\n\nproc run {} {\n    set result [helper "
     "21]\n    puts $result\n}\n",
     true, NULL},
    {"solidity", "a.sol",
     "// SPDX-License-Identifier: MIT\npragma solidity ^0.8.0;\n\nfunction helper(uint256 x) pure "
     "returns (uint256) {\n    return x + 1;\n}\n\nfunction run(uint256 y) pure returns (uint256) "
     "{\n    return helper(y);\n}\n",
     false,
     "call_expression callee not resolved by generic field/first-child extraction; no solidity "
     "branch in extract_calls.c"},
    {"commonlisp", "a.lisp", "(defun helper (x)\n  (* x 2))\n\n(defun run ()\n  (helper 21))\n",
     false,
     "list_lit call head is sym_lit not identifier; no commonlisp branch in extract_callee_name"},
    {"powershell", "a.ps1",
     "function helper {\n    Write-Output 'hi'\n}\n\nfunction run {\n    helper\n}\n", false,
     "command node child is command_name not identifier; extract_scripting_callee handles MATLAB "
     "not PowerShell"},

    /* The remaining code grammars (extends CALLS-edge breadth to ALL 66 code
     * grammars). expected_calls=false marks an already-root-caused gap. */
    {"ada", "a.adb",
     "procedure Run is\n   procedure Helper is\n   begin\n      null;\n   end Helper;\nbegin\n   "
     "Helper;\nend Run;\n",
     false,
     "procedure_call_statement callee did not resolve to a CALLS edge (empirically confirmed gap; "
     "no Ada branch in extract_calls.c)"},
    {"apex", "A.cls",
     "public class A {\n    static void helper() {\n        System.debug('helper');\n    }\n\n    "
     "static void run() {\n        helper();\n    }\n}\n",
     true, NULL},
    {"awk", "a.awk",
     "function helper(x) {\n    return x + 1\n}\n\nfunction run() {\n    print helper(3)\n}\n",
     true, NULL},
    {"cairo", "a.cairo",
     "fn helper(x: felt252) -> felt252 {\n    x + 1\n}\n\nfn run() -> felt252 {\n    "
     "helper(41)\n}\n",
     true, NULL},
    {"clojure", "a.clj", "(defn helper [] 42)\n\n(defn run [] (helper))\n", false,
     "lisp: call is a list_lit whose head is a sym_lit (not a field, not a first-child "
     "'identifier'); no lisp branch in extract_callee_name"},
    {"cuda", "a.cu",
     "__device__ int helper(int x) {\n    return x * 2;\n}\n\n__global__ void run(int *out) {\n    "
     "out[0] = helper(21);\n}\n",
     true, NULL},
    {"d", "a.d",
     "int helper(int x)\n{\n    return x + 1;\n}\n\nvoid run()\n{\n    int y = helper(41);\n}\n",
     true, NULL},
    {"emacslisp", "a.el",
     "(defun helper (x)\n  \"Add one to X.\"\n  (+ x 1))\n\n(defun run ()\n  \"Call helper.\"\n  "
     "(helper 41))\n",
     false,
     "lisp: call is a 'list' whose head is a 'symbol'; generic resolver wants a first-child "
     "'identifier'"},
    {"erlang", "a.erl",
     "-module(a).\n-export([run/0]).\n\nhelper(X) -> X + 1.\n\nrun() -> helper(41).\n", true, NULL},
    {"fennel", "a.fnl", "(fn helper [x]\n  (+ x 1))\n\n(fn run [n]\n  (helper n))\n", false,
     "lisp: call is a 'list' whose head is a 'symbol'; no fennel branch in extract_callee_name"},
    {"fish", "a.fish",
     "function helper\n    echo \"helping\"\nend\n\nfunction run\n    helper\nend\n", true, NULL},
    {"fsharp", "a.fs", "let helper x = x + 1\n\nlet run () = helper 41\n", false,
     "application_expression callee head is a 'long_identifier_or_op' wrapper, not a bare "
     "identifier/field; no fsharp callee branch"},
    {"glsl", "a.glsl",
     "float helper(float x) {\n    return x * 2.0;\n}\n\nvoid run() {\n    float y = "
     "helper(3.0);\n}\n",
     true, NULL},
    {"hare", "a.ha", "fn helper() void = void;\n\nexport fn run() void = {\n\thelper();\n};\n",
     true, NULL},
    {"hlsl", "a.hlsl",
     "float helper(float x)\n{\n    return x * 2.0;\n}\n\nfloat run(float v)\n{\n    return "
     "helper(v) + 1.0;\n}\n",
     true, NULL},
    {"ispc", "a.ispc",
     "static inline uniform float helper(uniform float x) {\n    return x * 2.0f;\n}\n\nexport "
     "void run(uniform float in[], uniform float out[], uniform int n) {\n    foreach (i = 0 ... "
     "n) {\n        out[i] = helper(in[i]);\n    }\n}\n",
     true, NULL},
    {"luau", "a.luau",
     "function helper(x)\n\treturn x + 1\nend\n\nfunction run(n)\n\treturn helper(n) * 2\nend\n",
     true, NULL},
    {"matlab", "a.m",
     "function run()\n    helper();\nend\n\nfunction helper()\n    disp('hi');\nend\n", true,
     "watch: .m extension collides with Objective-C under discover (content-sniffed)"},
    {"odin", "a.odin",
     "package fixture\n\nhelper :: proc() -> int {\n\treturn 42\n}\n\nrun :: proc() {\n\tx := "
     "helper()\n\t_ = x\n}\n",
     true, NULL},
    {"racket", "a.rkt",
     "#lang racket\n\n(define (helper x)\n  (+ x 1))\n\n(define (run)\n  (helper 41))\n", false,
     "lisp: call is a 'list' whose head is a 'symbol' (grammar has no 'identifier' node); no "
     "racket branch"},
    {"rescript", "a.res", "let helper = (x) => x + 1\n\nlet run = () => helper(41)\n", false,
     "call_expression 'function' field is a 'value_identifier' (not in extract_callee_from_fields' "
     "accepted type list)"},
    {"scheme", "a.scm", "(define (helper x)\n  (* x 2))\n\n(define (run)\n  (helper 21))\n", false,
     "lisp: call is a 'list' whose head is a 'symbol'; no scheme branch in extract_callee_name"},
    {"slang", "a.slang", "void helper()\n{\n    int x = 1;\n}\n\nvoid run()\n{\n    helper();\n}\n",
     true, NULL},
    {"squirrel", "a.nut",
     "function helper(x) {\n    return x + 1;\n}\n\nfunction run() {\n    return helper(41);\n}\n",
     true, NULL},
    {"starlark", "a.bzl", "def helper(x):\n    return x + 1\n\ndef run():\n    return helper(41)\n",
     true, NULL},
    {"sway", "a.sw", "fn helper() {}\n\nfn run() {\n    helper();\n}\n", true, NULL},
    {"teal", "a.tl",
     "local function helper(x: number): number\n   return x + 1\nend\n\nlocal function run(): "
     "number\n   return helper(41)\nend\n",
     true, NULL},
    {"vimscript", "a.vim",
     "function! Helper() abort\n  return 1\nendfunction\n\nfunction! Run() abort\n  call "
     "Helper()\nendfunction\n",
     true, NULL},
    {"wgsl", "a.wgsl",
     "fn helper() -> f32 {\n  return 1.0;\n}\n\nfn run() -> f32 {\n  return helper();\n}\n", false,
     "callee nested in type_constructor_or_function_call_expression -> type_declaration -> "
     "identifier; no field, first child is type_declaration"},
    {"zsh", "a.zsh", "function helper {\n  print \"helping\"\n}\n\nfunction run {\n  helper\n}\n",
     true, NULL},
};

TEST(contract_calls_breadth) {
    /* EVERY language must resolve a same-file caller->callee into a CALLS edge.
     * No skips/allowlists: a language that does not is a hard FAILURE that
     * reproduces the bug (the gap_note explains the known cause for the ones we
     * have already root-caused). These stay RED until fixed. */
    int n = (int)(sizeof(CALL_CASES) / sizeof(CALL_CASES[0]));
    int failures = 0;
    for (int i = 0; i < n; i++) {
        const CallCase *c = &CALL_CASES[i];
        const LangFile f = {c->filename, c->content};
        LangMetrics m = lang_metrics(&f, 1);
        if (!(m.ok && m.calls >= 1 && m.callers >= 1)) {
            fprintf(stderr, "  [CALLS-BREADTH] FAIL %-11s ok=%d calls=%d callers=%d%s%s\n", c->lang,
                    m.ok, m.calls, m.callers, c->gap_note ? " — " : "",
                    c->gap_note ? c->gap_note : "");
            failures++;
        }
    }
    fprintf(stderr,
            "  [CALLS-BREADTH] %d langs: %d FAILURES (each = a language that does not "
            "resolve a same-file CALLS edge)\n",
            n, failures);
    ASSERT_EQ(failures, 0);
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
static const char *ALL_EDGE_TYPES[] = {
    "CALLS",         "CONFIGURES", "CONTAINS_FILE",  "CONTAINS_FOLDER", "DATA_FLOWS",
    "DECORATES",     "DEFINES",    "DEFINES_METHOD", "DEPENDS_ON",      "FILE_CHANGES_WITH",
    "GRAPHQL_CALLS", "GRPC_CALLS", "HANDLES",        "HTTP_CALLS",      "IMPLEMENTS",
    "IMPORTS",       "INHERITS",   "INFRA_MAPS",     "OVERRIDE",        "SEMANTICALLY_RELATED",
    "SIMILAR_TO",    "TESTS_FILE", "TESTS",          "TRPC_CALLS",      "USAGE",
    "ASYNC_CALLS",   NULL};

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
/* #408: package.json `workspaces` cross-package IMPORTS produce zero edges
 * (v0.7.0 follow-up to #308, which fixed tsconfig `paths`). A Lerna/Yarn-style
 * monorepo where packages/b imports a sibling by its package name (@org/a) should
 * resolve via the sibling's package.json `name` -> dir (pass_pkgmap.c) into a
 * cross-package IMPORTS edge. Today bare-specifier workspace imports yield zero.
 * RED until workspace `@org/pkg` resolution produces the edge. */
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
    ASSERT_TRUE(edge_present(f, 5, "IMPORTS", 1));
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
        {"src/consumer.ts",
         "import { ClientC } from '@lib/external-pkg';\n\n"
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
        {"src/consumer.ts",
         "import { Thing } from '@lib/thing';\n\n"
         "export function useThing() {\n  return Thing;\n}\n"}};
    ASSERT_TRUE(edge_present(f, 3, "IMPORTS", 1));
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
         "    return gql(\"query GetUser { user { id name } }\")\n\n\n"
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
    RUN_TEST(contract_typescript_relative_import);

    /* Graph-level breadth across all grammars (P4). */
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
    RUN_TEST(contract_edge_depends_on);
    RUN_TEST(contract_edge_parallel_service_edges);
    RUN_TEST(contract_edge_file_changes_with);
}
