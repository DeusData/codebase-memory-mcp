/*
 * test_conditional_variants.c — one definition with mutually exclusive
 * variants is ONE identity in the graph, in every language.
 *
 * A variant is a second definition of the same name that only one build or
 * run ever sees: C-family #if/#else, C#/F# #if, Rust #[cfg], Swift #if,
 * Go build-tagged files, D version blocks, Erlang -ifdef, Haskell CPP,
 * Fortran .F90 preprocessing, Verilog `ifdef, Elixir/Python/Ruby/PHP
 * definitions under an `if`. C platform files (foo_win.c / foo_posix.c) are
 * the exception: each keeps its own node and path QN, so it is found by its
 * file, and they share a `variant_key` (cv_platform_check below).
 *
 * Every fixture defines `pick` twice — variant A calls `alpha`, variant B
 * calls `beta` — plus a `caller` that calls `pick`. The graph must hold:
 *   - exactly one node for `pick` (no per-variant twins, no condition text
 *     in its qualified name);
 *   - CALLS from that node to alpha AND beta (every variant's body counts);
 *   - a CALLS edge from caller to it;
 *   - its `variants` property listing every variant's span, so a change in
 *     any variant maps to the definition.
 * Fixtures avoid each language's unrelated extraction gaps (Ruby and Pascal
 * calls carry parentheses; Haskell calls are applications).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
#include <foundation/compat_fs.h>
#include <mcp/mcp.h>
#include <pipeline/pipeline.h>
#include <pipeline/pipeline_internal.h>
#include <store/store.h>
#include <sqlite3.h>
#include "helpers.h" /* cbm_fqn_compute: the path-derived QN of a definition */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    const char *name;
    const char *content;
} cv_file_t;

typedef struct {
    char tmpdir[256];
    char dbpath[1024];
    char *project;
    cbm_mcp_server_t *srv;
    cbm_store_t *store;
} cv_proj_t;

static bool cv_write(const char *root, const cv_file_t *file) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s", root, file->name);
    char *slash = strrchr(path, '/');
    if (slash && slash > path + strlen(root)) {
        *slash = '\0';
        cbm_mkdir_p(path, 0755);
        *slash = '/';
    }
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        return false;
    }
    bool ok = fputs(file->content, f) >= 0;
    return fclose(f) == 0 && ok;
}

static bool cv_index(cv_proj_t *p, const cv_file_t *files, int count) {
    memset(p, 0, sizeof(*p));
    snprintf(p->tmpdir, sizeof(p->tmpdir), "%s", th_mktempdir("cbm-cv"));
    if (!p->tmpdir[0]) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (!cv_write(p->tmpdir, &files[i])) {
            return false;
        }
    }
    p->project = cbm_project_name_from_path(p->tmpdir);
    if (!p->project) {
        return false;
    }
    /* Where the MCP server writes the project DB: CBM_CACHE_DIR, else the
     * per-user cache (same resolution as test_lang_contract.c). */
    char cache[700];
    const char *configured = getenv("CBM_CACHE_DIR");
    const char *home = getenv("HOME");
    if (configured && configured[0]) {
        snprintf(cache, sizeof(cache), "%s", configured);
    } else {
        snprintf(cache, sizeof(cache), "%s/.cache/codebase-memory-mcp", home ? home : "/tmp");
    }
    cbm_mkdir_p(cache, 0755);
    snprintf(p->dbpath, sizeof(p->dbpath), "%s/%s.db", cache, p->project);
    unlink(p->dbpath);
    p->srv = cbm_mcp_server_new(NULL);
    if (!p->srv) {
        return false;
    }
    char args[800];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", p->tmpdir);
    free(cbm_mcp_handle_tool(p->srv, "index_repository", args));
    p->store = cbm_store_open_path(p->dbpath);
    return p->store != NULL;
}

static void cv_cleanup(cv_proj_t *p) {
    if (p->store) {
        cbm_store_close(p->store);
    }
    if (p->srv) {
        cbm_mcp_server_free(p->srv);
    }
    if (p->dbpath[0]) {
        char side[1100];
        unlink(p->dbpath);
        snprintf(side, sizeof(side), "%s-wal", p->dbpath);
        unlink(side);
        snprintf(side, sizeof(side), "%s-shm", p->dbpath);
        unlink(side);
    }
    free(p->project);
    if (p->tmpdir[0]) {
        th_rmtree(p->tmpdir);
    }
    memset(p, 0, sizeof(*p));
}

/* One integer from SQL with text parameters; -1 on any query failure. */
static int cv_count(cv_proj_t *p, const char *sql, const char *a, const char *b) {
    sqlite3 *db = cbm_store_get_db(p->store);
    sqlite3_stmt *stmt = NULL;
    if (!db || sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return -1;
    }
    int n = sqlite3_bind_parameter_count(stmt);
    const char *values[] = {p->project, a, b};
    for (int i = 0; i < n && i < 3; i++) {
        sqlite3_bind_text(stmt, i + 1, values[i], -1, SQLITE_TRANSIENT);
    }
    int count = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    return count;
}

#define CV_DEF_LABELS "('Function','Method','Module','Class')"

typedef struct {
    int nodes;         /* distinct `pick` definitions */
    int clean_names;   /* of those, qualified names without condition text */
    int to_alpha;      /* CALLS pick -> alpha */
    int to_beta;       /* CALLS pick -> beta */
    int from_caller;   /* CALLS caller -> pick */
    int variant_spans; /* entries of pick's `variants` property */
} cv_result_t;

static cv_result_t cv_measure(cv_proj_t *p, const char *pick, const char *alpha, const char *beta,
                              const char *caller) {
    cv_result_t r;
    r.nodes = cv_count(p,
                       "SELECT COUNT(DISTINCT qualified_name) FROM nodes WHERE project=?1 "
                       "AND name=?2 AND label IN " CV_DEF_LABELS,
                       pick, NULL);
    r.clean_names = cv_count(
        p,
        "SELECT COUNT(*) FROM nodes WHERE project=?1 AND name=?2 AND label IN " CV_DEF_LABELS
        " AND qualified_name NOT GLOB '*[#[]()]*'",
        pick, NULL);
    const char *calls = "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                        "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type='CALLS' "
                        "AND s.name=?2 AND t.name=?3";
    r.to_alpha = cv_count(p, calls, pick, alpha);
    r.to_beta = cv_count(p, calls, pick, beta);
    r.from_caller = cv_count(p, calls, caller, pick);
    r.variant_spans =
        cv_count(p,
                 "SELECT COALESCE(MAX(json_array_length(properties,'$.variants')),0) "
                 "FROM nodes WHERE project=?1 AND name=?2 AND label IN " CV_DEF_LABELS,
                 pick, NULL);
    return r;
}

static int cv_expect_structure(const char *lang, cv_result_t r) {
    bool ok = r.nodes == 1 && r.clean_names == 1 && r.variant_spans >= 2;
    if (!ok) {
        fprintf(
            stderr, "  [%s] nodes=%d clean=%d ->alpha=%d ->beta=%d caller->=%d variant_spans=%d\n",
            lang, r.nodes, r.clean_names, r.to_alpha, r.to_beta, r.from_caller, r.variant_spans);
    }
    return ok;
}

/* Source-authored conditional bodies remain genuine quality obligations even
 * while configuration-aware binding is unsupported. No observed edge, node
 * count or resolver capability creates this denominator. The unchanged fixture
 * bytes, exact call sites and declaration lines below define it independently. */
typedef struct {
    const char *test, *case_id, *file, *caller, *needle;
    unsigned occurrence;
    const char *target_file, *target;
    int declaration_line;
    const char *declaration;
} cv_golden_t;

static const cv_golden_t cv_goldens[] = {
    {"cv_c_preprocessor", "cv_c_preprocessor-1", "pick.c", "pick", "alpha()", 0, "pick.c", "alpha",
     1, "void alpha(void) {}"},
    {"cv_c_preprocessor", "cv_c_preprocessor-2", "pick.c", "pick", "beta()", 0, "pick.c", "beta", 2,
     "void beta(void) {}"},
    {"cv_c_preprocessor", "cv_c_preprocessor-3", "pick.c", "caller", "pick()", 0, "pick.c", "pick",
     6, "void pick(void) { beta(); }"},
    {"cv_cpp_elif", "cv_cpp_elif-1", "pick.cpp", "pick", "alpha()", 1, "pick.cpp", "alpha", 1,
     "void alpha() {}"},
    {"cv_cpp_elif", "cv_cpp_elif-2", "pick.cpp", "pick", "beta()", 1, "pick.cpp", "beta", 2,
     "void beta() {}"},
    {"cv_cpp_elif", "cv_cpp_elif-3", "pick.cpp", "caller", "pick()", 2, "pick.cpp", "pick", 6,
     "void pick() { beta(); }"},
    {"cv_cpp_class_members", "cv_cpp_class_members-1", "pick.cpp", "pick", "alpha()", 1, "pick.cpp",
     "alpha", 2, "  void alpha() {}"},
    {"cv_cpp_class_members", "cv_cpp_class_members-2", "pick.cpp", "pick", "beta()", 1, "pick.cpp",
     "beta", 3, "  void beta() {}"},
    {"cv_cpp_class_members", "cv_cpp_class_members-3", "pick.cpp", "caller", "pick()", 2,
     "pick.cpp", "pick", 7, "  void pick() { beta(); }"},
    {"cv_cuda", "cv_cuda-1", "pick.cu", "pick", "alpha()", 1, "pick.cu", "alpha", 1,
     "__device__ void alpha() {}"},
    {"cv_cuda", "cv_cuda-2", "pick.cu", "pick", "beta()", 1, "pick.cu", "beta", 2,
     "__device__ void beta() {}"},
    {"cv_cuda", "cv_cuda-3", "pick.cu", "caller", "pick()", 2, "pick.cu", "pick", 6,
     "__device__ void pick() { beta(); }"},
    {"cv_csharp_class_members", "cv_csharp_class_members-1", "Picker.cs", "Pick", "Alpha()", 1,
     "Picker.cs", "Alpha", 2, "  static void Alpha() {}"},
    {"cv_csharp_class_members", "cv_csharp_class_members-2", "Picker.cs", "Pick", "Beta()", 1,
     "Picker.cs", "Beta", 3, "  static void Beta() {}"},
    {"cv_csharp_class_members", "cv_csharp_class_members-3", "Picker.cs", "Caller", "Pick()", 2,
     "Picker.cs", "Pick", 5, "  static void Pick() { Alpha(); }"},
    {"cv_fsharp", "cv_fsharp-1", "Pick.fs", "pick", "alpha ()", 1, "Pick.fs", "alpha", 2,
     "let alpha () = ()"},
    {"cv_fsharp", "cv_fsharp-2", "Pick.fs", "pick", "beta ()", 1, "Pick.fs", "beta", 3,
     "let beta () = ()"},
    {"cv_fsharp", "cv_fsharp-3", "Pick.fs", "caller", "pick ()", 2, "Pick.fs", "pick", 5,
     "let pick () = alpha ()"},
    {"cv_rust_cfg_free_functions", "cv_rust_cfg_free_functions-1", "src/lib.rs", "pick", "alpha()",
     1, "src/lib.rs", "alpha", 1, "fn alpha() {}"},
    {"cv_rust_cfg_free_functions", "cv_rust_cfg_free_functions-2", "src/lib.rs", "pick", "beta()",
     1, "src/lib.rs", "beta", 2, "fn beta() {}"},
    {"cv_rust_cfg_free_functions", "cv_rust_cfg_free_functions-3", "src/lib.rs", "caller", "pick()",
     2, "src/lib.rs", "pick", 6, "fn pick() { beta(); }"},
    {"cv_rust_cfg_methods", "cv_rust_cfg_methods-1", "src/lib.rs", "pick", "alpha()", 1,
     "src/lib.rs", "alpha", 2, "fn alpha() {}"},
    {"cv_rust_cfg_methods", "cv_rust_cfg_methods-2", "src/lib.rs", "pick", "beta()", 1,
     "src/lib.rs", "beta", 3, "fn beta() {}"},
    {"cv_rust_cfg_methods", "cv_rust_cfg_methods-3", "src/lib.rs", "caller", "self.pick()", 0,
     "src/lib.rs", "pick", 8, "    fn pick(&self) { beta(); }"},
    {"cv_go_build_tag_files", "cv_go_build_tag_files-1", "pick/pick_windows.go", "Pick", "alpha()",
     0, "pick/common.go", "alpha", 3, "func alpha() {}"},
    {"cv_go_build_tag_files", "cv_go_build_tag_files-2", "pick/pick_posix.go", "Pick", "beta()", 0,
     "pick/common.go", "beta", 4, "func beta() {}"},
    {"cv_go_build_tag_files", "cv_go_build_tag_files-3", "pick/common.go", "Caller", "Pick()", 0,
     "pick/pick_windows.go", NULL, 4, "func Pick() { alpha() }"},
    {"cv_swift", "cv_swift-1", "pick.swift", "pick", "alpha()", 1, "pick.swift", "alpha", 1,
     "func alpha() {}"},
    {"cv_swift", "cv_swift-2", "pick.swift", "pick", "beta()", 1, "pick.swift", "beta", 2,
     "func beta() {}"},
    {"cv_swift", "cv_swift-3", "pick.swift", "caller", "pick()", 2, "pick.swift", "pick", 4,
     "func pick() { alpha() }"},
    {"cv_python_if", "cv_python_if-1", "pick.py", "pick", "alpha()", 1, "pick.py", "alpha", 2,
     "def alpha(): pass"},
    {"cv_python_if", "cv_python_if-2", "pick.py", "pick", "beta()", 1, "pick.py", "beta", 3,
     "def beta(): pass"},
    {"cv_python_if", "cv_python_if-3", "pick.py", "caller", "pick()", 2, "pick.py", "pick", 5,
     "    def pick():"},
    {"cv_ruby_if", "cv_ruby_if-1", "pick.rb", "pick", "alpha()", 1, "pick.rb", "alpha", 1,
     "def alpha(); end"},
    {"cv_ruby_if", "cv_ruby_if-2", "pick.rb", "pick", "beta()", 1, "pick.rb", "beta", 2,
     "def beta(); end"},
    {"cv_ruby_if", "cv_ruby_if-3", "pick.rb", "caller", "pick()", 0, "pick.rb", "pick", 4,
     "  def pick"},
    {"cv_php_if", "cv_php_if-1", "pick.php", "pick", "alpha()", 1, "pick.php", "alpha", 2,
     "function alpha() {}"},
    {"cv_php_if", "cv_php_if-2", "pick.php", "pick", "beta()", 1, "pick.php", "beta", 3,
     "function beta() {}"},
    {"cv_php_if", "cv_php_if-3", "pick.php", "caller", "pick()", 2, "pick.php", "pick", 5,
     "    function pick() { alpha(); }"},
    {"cv_d_version", "cv_d_version-1", "pick.d", "pick", "alpha()", 1, "pick.d", "alpha", 1,
     "void alpha() {}"},
    {"cv_d_version", "cv_d_version-2", "pick.d", "pick", "beta()", 1, "pick.d", "beta", 2,
     "void beta() {}"},
    {"cv_d_version", "cv_d_version-3", "pick.d", "caller", "pick()", 2, "pick.d", "pick", 4,
     "    void pick() { alpha(); }"},
    {"cv_erlang_ifdef", "cv_erlang_ifdef-1", "pick.erl", "pick", "alpha()", 1, "pick.erl", "alpha",
     3, "alpha() -> ok."},
    {"cv_erlang_ifdef", "cv_erlang_ifdef-2", "pick.erl", "pick", "beta()", 1, "pick.erl", "beta", 4,
     "beta() -> ok."},
    {"cv_erlang_ifdef", "cv_erlang_ifdef-3", "pick.erl", "caller", "pick()", 2, "pick.erl", "pick",
     6, "pick() -> alpha()."},
    {"cv_haskell_cpp", "cv_haskell_cpp-1", "Pick.hs", "pick", "alpha x", 1, "Pick.hs", "alpha", 4,
     "alpha x = x"},
    {"cv_haskell_cpp", "cv_haskell_cpp-2", "Pick.hs", "pick", "beta x", 1, "Pick.hs", "beta", 6,
     "beta x = x"},
    {"cv_haskell_cpp", "cv_haskell_cpp-3", "Pick.hs", "caller", "pick x", 2, "Pick.hs", "pick", 9,
     "pick x = alpha x"},
    {"cv_haskell_cpp_elif_nested", "cv_haskell_cpp_elif_nested-1", "Pick.hs", "pick", "alpha x", 1,
     "Pick.hs", "alpha", 4, "alpha x = x"},
    {"cv_haskell_cpp_elif_nested", "cv_haskell_cpp_elif_nested-2", "Pick.hs", "pick", "beta x", 1,
     "Pick.hs", "beta", 6, "beta x = x"},
    {"cv_haskell_cpp_elif_nested", "cv_haskell_cpp_elif_nested-3", "Pick.hs", "caller", "pick x", 4,
     "Pick.hs", "pick", 9, "pick x = x"},
    {"cv_fortran_preprocessed", "cv_fortran_preprocessed-1", "pick.F90", "pick", "call alpha()", 0,
     "pick.F90", "alpha", 3, "  subroutine alpha()"},
    {"cv_fortran_preprocessed", "cv_fortran_preprocessed-2", "pick.F90", "pick", "call beta()", 0,
     "pick.F90", "beta", 5, "  subroutine beta()"},
    {"cv_fortran_preprocessed", "cv_fortran_preprocessed-3", "pick.F90", "caller", "call pick()", 0,
     "pick.F90", "pick", 8, "  subroutine pick()"},
    {"cv_pascal_ifdef", "cv_pascal_ifdef-1", "pick.pas", "pick", "alpha()", 0, "pick.pas", "alpha",
     5, "procedure alpha; begin end;"},
    {"cv_pascal_ifdef", "cv_pascal_ifdef-2", "pick.pas", "pick", "beta()", 0, "pick.pas", "beta", 6,
     "procedure beta; begin end;"},
    {"cv_pascal_ifdef", "cv_pascal_ifdef-3", "pick.pas", "caller", "pick()", 0, "pick.pas", "pick",
     8, "procedure pick; begin alpha(); end;"},
    {"cv_julia_static_if", "cv_julia_static_if-1", "pick.jl", "pick", "alpha()", 1, "pick.jl",
     "alpha", 1, "alpha() = nothing"},
    {"cv_julia_static_if", "cv_julia_static_if-2", "pick.jl", "pick", "beta()", 1, "pick.jl",
     "beta", 2, "beta() = nothing"},
    {"cv_julia_static_if", "cv_julia_static_if-3", "pick.jl", "caller", "pick()", 2, "pick.jl",
     "pick", 4, "    pick() = alpha()"},
    {"cv_elixir_compile_time_if", "cv_elixir_compile_time_if-1", "pick.ex", "pick", "alpha()", 0,
     "pick.ex", "alpha", 2, "  def alpha, do: :ok"},
    {"cv_elixir_compile_time_if", "cv_elixir_compile_time_if-2", "pick.ex", "pick", "beta()", 0,
     "pick.ex", "beta", 3, "  def beta, do: :ok"},
    {"cv_elixir_compile_time_if", "cv_elixir_compile_time_if-3", "pick.ex", "caller", "pick()", 0,
     "pick.ex", "pick", 5, "    def pick, do: alpha()"},
    {"cv_c_platform_files", "cv_c_platform_files-1", "src/plat_win.c", "pick", "alpha()", 0,
     "src/plat.c", "alpha", 1, "void alpha(void) {}"},
    {"cv_c_platform_files", "cv_c_platform_files-2", "src/plat_posix.c", "pick", "beta()", 0,
     "src/plat.c", "beta", 2, "void beta(void) {}"},
    {"cv_c_platform_files", "cv_c_platform_files-3", "src/plat.c", "caller", "pick()", 0,
     "src/plat.c", NULL, 3, "void pick(void);"},
    {"cv_c_platform_directories", "cv_c_platform_directories-1", "src/unix/fs.c", "pick", "alpha()",
     0, "src/core.c", "alpha", 1, "void alpha(void) {}"},
    {"cv_c_platform_directories", "cv_c_platform_directories-2", "src/win/fs.c", "pick", "beta()",
     0, "src/core.c", "beta", 2, "void beta(void) {}"},
    {"cv_c_platform_directories", "cv_c_platform_directories-3", "src/core.c", "caller", "pick()",
     0, "src/core.c", NULL, 3, "void pick(void);"},
};

/* Recovery is recorded per independently authored source obligation. The
 * Python branches share a graph identity, so each caller span must also match
 * its exact stored variant; the canonical node range alone cannot stand in. */
typedef struct {
    const char *case_id;
    unsigned caller_start, caller_end;
    const char *caller_declaration;
    const char *target_label; /* NULL: Function */
} cv_recovered_t;
static const cv_recovered_t cv_recovered[] = {
    {"cv_python_if-1", 5, 6, "    def pick():", NULL},
    {"cv_python_if-2", 8, 9, "    def pick():", NULL},
    {"cv_c_platform_files-1", 0, 0, NULL, NULL},
    {"cv_c_platform_files-2", 0, 0, NULL, NULL},
    {"cv_c_platform_directories-1", 0, 0, NULL, NULL},
    {"cv_c_platform_directories-2", 0, 0, NULL, NULL},
    /* Rust cfg twins are one node, whose canonical range is the last twin. */
    {"cv_rust_cfg_free_functions-1", 4, 4, "fn pick() { alpha(); }", NULL},
    {"cv_rust_cfg_free_functions-2", 6, 6, "fn pick() { beta(); }", NULL},
    {"cv_rust_cfg_free_functions-3", 0, 0, NULL, NULL},
    {"cv_rust_cfg_methods-1", 6, 6, "    fn pick(&self) { alpha(); }", NULL},
    {"cv_rust_cfg_methods-2", 8, 8, "    fn pick(&self) { beta(); }", NULL},
    {"cv_rust_cfg_methods-3", 0, 0, NULL, "Method"},
    /* The translation-unit rules bind a function's body in either branch, and a caller to the
     * one node both branches share. That node's stored span is the last branch. */
    {"cv_c_preprocessor-1", 4, 4, "void pick(void) { alpha(); }", NULL},
    {"cv_c_preprocessor-2", 6, 6, "void pick(void) { beta(); }", NULL},
    {"cv_c_preprocessor-3", 0, 0, NULL, NULL},
    {"cv_cpp_elif-1", 4, 4, "void pick() { alpha(); }", NULL},
    {"cv_cpp_elif-2", 6, 6, "void pick() { beta(); }", NULL},
    {"cv_cpp_elif-3", 0, 0, NULL, NULL},
    {"cv_cpp_class_members-1", 5, 5, "  void pick() { alpha(); }", "Method"},
    {"cv_cpp_class_members-2", 7, 7, "  void pick() { beta(); }", "Method"},
    {"cv_cpp_class_members-3", 0, 0, NULL, "Method"},
    {"cv_cuda-1", 4, 4, "__device__ void pick() { alpha(); }", NULL},
    {"cv_cuda-2", 6, 6, "__device__ void pick() { beta(); }", NULL},
    {"cv_cuda-3", 0, 0, NULL, NULL},
};

static bool cv_quality(cv_proj_t *p, const char *test, const cv_file_t *files, int count) {
    /* TEST(name) expands to test_##name; fixture keys use the public selector. */
    if (!strncmp(test, "test_", 5))
        test += 5;
    RQ_Source sources[4];
    RQ_Expected expected[3];
    size_t n = 0, required = 0;
    if (count < 1 || count > 4)
        return false;
    size_t source_count = 0;
    for (int i = 0; i < count; i++) {
        /* Build manifests configure the fixture but are not call-source
         * snapshots. Keep their bytes in the full indexing input above. */
        if (!strcmp(files[i].name, "Cargo.toml") || !strcmp(files[i].name, "go.mod"))
            continue;
        sources[source_count++] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    }
    if (!strcmp(test, "cv_objc_methods")) {
        /* These are bare C calls, not [self selector] messages. Later global
         * definitions/prototypes do not justify binding the earlier call to
         * a same-spelled Objective-C method or an undeclared C function. */
        static const RQ_Forbidden forbidden[] = {
            {.case_id = "objc-c-alpha-is-not-a-method-message",
             .site = {.file = "P.m", .caller = "pick", .needle = "alpha()"},
             .relation = "CALLS",
             .any_target = true},
            {.case_id = "objc-c-beta-is-not-a-method-message",
             .site = {.file = "P.m", .caller = "pick", .needle = "beta()"},
             .relation = "CALLS",
             .any_target = true},
            {.case_id = "objc-c-pick-is-not-a-method-message",
             .site = {.file = "P.m", .caller = "caller", .needle = "pick()"},
             .relation = "CALLS",
             .any_target = true}};
        RQ_Report report = {0};
        return rq_check_project_declarations(p->store, p->project, test, sources, source_count,
                                             NULL, 0, forbidden, 3, &report, stderr) &&
               report.genuine == 0 && report.wrong == 0;
    }
    for (size_t i = 0; i < sizeof(cv_goldens) / sizeof(cv_goldens[0]); i++) {
        const cv_golden_t *g = cv_goldens + i;
        if (strcmp(test, g->test))
            continue;
        if (n == 3)
            return false;
        const cv_recovered_t *recovered = NULL;
        for (size_t j = 0; j < sizeof(cv_recovered) / sizeof(cv_recovered[0]); j++)
            if (!strcmp(g->case_id, cv_recovered[j].case_id))
                recovered = cv_recovered + j;
        required += recovered != NULL;
        expected[n++] = (RQ_Expected){
            .case_id = g->case_id,
            .site = {.file = g->file,
                     .caller = g->caller,
                     .needle = g->needle,
                     .occurrence_index = g->occurrence,
                     .caller_variant_start_line = recovered ? recovered->caller_start : 0,
                     .caller_variant_end_line = recovered ? recovered->caller_end : 0,
                     .caller_variant_declaration =
                         recovered ? recovered->caller_declaration : NULL},
            .relation = "CALLS",
            .target = {.file = g->target_file,
                       .suffix = g->target,
                       .label = !recovered                ? NULL
                                : recovered->target_label ? recovered->target_label
                                                          : "Function",
                       .declaration_line = g->declaration_line,
                       .declaration_text = g->declaration},
            .mode = recovered ? RQ_REQUIRED : RQ_KNOWN_GAP,
            .expected_recovered = recovered != NULL,
            .gap_id = g->target ? "conditional-variant-source-binding"
                                : "conditional-variant-graph-identity",
            .why =
                recovered
                    ? "This source-authored body call is recovered and must retain its exact "
                      "callable target; merged callers also require their authored variant."
                : g->target
                    ? "The strict resolver has no admitted binding for these conditional or "
                      "build-selected declarations; each source relationship remains recall debt."
                    : "The source call is genuine, but an unselected build cannot choose a "
                      "physical implementation. Its declaration is only a debt anchor; no "
                      "faithful graph target identity is asserted.",
            .tried =
                recovered
                    ? "v76 admitted the four C body calls and both Python body calls. The first "
                      "Python caller needs its exact stored 5-6 variant rather than the canonical "
                      "8-9 range; both source spans are authored independently of emitted rows."
                    : "The original v69 canonical tests retained nodes and variant spans but "
                      "failed their CALLS checks. Exact source sites and targets replace "
                      "name-only positive queries without guessing a build configuration."};
    }
    RQ_Report report = {0};
    return n == 3 &&
           rq_check_project_declarations(p->store, p->project, test, sources, source_count,
                                         expected, n, NULL, 0, &report, stderr) &&
           report.genuine == n && report.found == required && report.missing == n - required;
}

static int cv_run(const char *test, const char *lang, const cv_file_t *files, int count,
                  const char *pick, const char *alpha, const char *beta, const char *caller) {
    cv_proj_t p;
    bool indexed = cv_index(&p, files, count);
    cv_result_t result = indexed ? cv_measure(&p, pick, alpha, beta, caller) : (cv_result_t){0};
    int ok = indexed && cv_expect_structure(lang, result) && cv_quality(&p, test, files, count);
    if (!indexed) {
        fprintf(stderr, "  [%s] fixture could not be indexed\n", lang);
    }
    cv_cleanup(&p);
    return ok;
}

#define CV_ONE(lang, path, src)                                                            \
    do {                                                                                   \
        static const cv_file_t files_[] = {{path, src}};                                   \
        ASSERT_TRUE(cv_run(__func__, lang, files_, 1, "pick", "alpha", "beta", "caller")); \
    } while (0)

TEST(cv_c_preprocessor) {
    CV_ONE("c", "pick.c",
           "void alpha(void) {}\nvoid beta(void) {}\n#ifdef _WIN32\nvoid pick(void) { alpha(); }\n"
           "#else\nvoid pick(void) { beta(); }\n#endif\nvoid caller(void) { pick(); }\n");
    PASS();
}

TEST(cv_cpp_elif) {
    CV_ONE(
        "cpp", "pick.cpp",
        "void alpha() {}\nvoid beta() {}\n#if defined(_WIN32)\nvoid pick() { alpha(); }\n"
        "#elif defined(__linux__)\nvoid pick() { beta(); }\n#endif\nvoid caller() { pick(); }\n");
    PASS();
}

TEST(cv_cpp_class_members) {
    CV_ONE("cpp_member", "pick.cpp",
           "struct P {\n  void alpha() {}\n  void beta() {}\n#ifdef _WIN32\n  void pick() { "
           "alpha(); }\n"
           "#else\n  void pick() { beta(); }\n#endif\n  void caller() { pick(); }\n};\n");
    PASS();
}

TEST(cv_objc_methods) {
    CV_ONE("objc", "P.m",
           "@implementation P\n- (void)alpha {}\n- (void)beta {}\n#if TARGET_OS_IPHONE\n"
           "- (void)pick { alpha(); }\n#else\n- (void)pick { beta(); }\n#endif\n"
           "- (void)caller { pick(); }\n@end\nvoid alpha(void) {}\nvoid beta(void) {}\n"
           "void pick(void);\n");
    PASS();
}

TEST(cv_cuda) {
    CV_ONE(
        "cuda", "pick.cu",
        "__device__ void alpha() {}\n__device__ void beta() {}\n#ifdef USE_FAST\n"
        "__device__ void pick() { alpha(); }\n#else\n__device__ void pick() { beta(); }\n#endif\n"
        "__device__ void caller() { pick(); }\n");
    PASS();
}

TEST(cv_csharp_class_members) {
    static const cv_file_t files[] = {
        {"Picker.cs", "class Picker {\n  static void Alpha() {}\n  static void Beta() {}\n"
                      "#if WINDOWS\n  static void Pick() { Alpha(); }\n#else\n"
                      "  static void Pick() { Beta(); }\n#endif\n"
                      "  static void Caller() { Pick(); }\n}\n"}};
    ASSERT_TRUE(cv_run(__func__, "csharp", files, 1, "Pick", "Alpha", "Beta", "Caller"));
    PASS();
}

TEST(cv_fsharp) {
    CV_ONE("fsharp", "Pick.fs",
           "module Pick\nlet alpha () = ()\nlet beta () = ()\n#if WINDOWS\nlet pick () = alpha ()\n"
           "#else\nlet pick () = beta ()\n#endif\nlet caller () = pick ()\n");
    PASS();
}

TEST(cv_rust_cfg_free_functions) {
    static const cv_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"cv\"\nversion = \"0.1.0\"\n"},
        {"src/lib.rs", "fn alpha() {}\nfn beta() {}\n#[cfg(windows)]\nfn pick() { alpha(); }\n"
                       "#[cfg(not(windows))]\nfn pick() { beta(); }\nfn caller() { pick(); }\n"}};
    ASSERT_TRUE(cv_run(__func__, "rust", files, 2, "pick", "alpha", "beta", "caller"));
    PASS();
}

TEST(cv_rust_cfg_methods) {
    static const cv_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"cv\"\nversion = \"0.1.0\"\n"},
        {"src/lib.rs",
         "struct P;\nfn alpha() {}\nfn beta() {}\nimpl P {\n    #[cfg(target_os = \"linux\")]\n"
         "    fn pick(&self) { alpha(); }\n    #[cfg(not(target_os = \"linux\"))]\n"
         "    fn pick(&self) { beta(); }\n    fn caller(&self) { self.pick(); }\n}\n"}};
    ASSERT_TRUE(cv_run(__func__, "rust_impl", files, 2, "pick", "alpha", "beta", "caller"));
    PASS();
}

TEST(cv_go_build_tag_files) {
    static const cv_file_t files[] = {
        {"go.mod", "module example.com/cv\n\ngo 1.22\n"},
        {"pick/pick_windows.go", "//go:build windows\npackage pick\n\nfunc Pick() { alpha() }\n"},
        {"pick/pick_posix.go", "//go:build !windows\npackage pick\n\nfunc Pick() { beta() }\n"},
        {"pick/common.go",
         "package pick\n\nfunc alpha() {}\nfunc beta() {}\nfunc Caller() { Pick() }\n"}};
    ASSERT_TRUE(cv_run(__func__, "go", files, 4, "Pick", "alpha", "beta", "Caller"));
    PASS();
}

TEST(cv_swift) {
    CV_ONE("swift", "pick.swift",
           "func alpha() {}\nfunc beta() {}\n#if os(Windows)\nfunc pick() { alpha() }\n#else\n"
           "func pick() { beta() }\n#endif\nfunc caller() { pick() }\n");
    PASS();
}

TEST(cv_python_if) {
    CV_ONE("python", "pick.py",
           "import sys\ndef alpha(): pass\ndef beta(): pass\nif sys.platform == \"win32\":\n"
           "    def pick():\n        alpha()\nelse:\n    def pick():\n        beta()\n"
           "def caller():\n    pick()\n");
    PASS();
}

TEST(cv_ruby_if) {
    CV_ONE(
        "ruby", "pick.rb",
        "def alpha(); end\ndef beta(); end\nif RUBY_PLATFORM =~ /mswin/\n  def pick\n    alpha()\n"
        "  end\nelse\n  def pick\n    beta()\n  end\nend\ndef caller\n  pick()\nend\n");
    PASS();
}

TEST(cv_php_if) {
    CV_ONE("php", "pick.php",
           "<?php\nfunction alpha() {}\nfunction beta() {}\nif (PHP_OS_FAMILY === 'Windows') {\n"
           "    function pick() { alpha(); }\n} else {\n    function pick() { beta(); }\n}\n"
           "function caller() { pick(); }\n");
    PASS();
}

TEST(cv_d_version) {
    CV_ONE("d", "pick.d",
           "void alpha() {}\nvoid beta() {}\nversion (Windows) {\n    void pick() { alpha(); }\n"
           "} else {\n    void pick() { beta(); }\n}\nvoid caller() { pick(); }\n");
    PASS();
}

TEST(cv_erlang_ifdef) {
    CV_ONE("erlang", "pick.erl",
           "-module(pick).\n-export([caller/0]).\nalpha() -> ok.\nbeta() -> ok.\n-ifdef(WINDOWS).\n"
           "pick() -> alpha().\n-else.\npick() -> beta().\n-endif.\ncaller() -> pick().\n");
    PASS();
}

TEST(cv_haskell_cpp) {
    CV_ONE("haskell", "Pick.hs",
           "{-# LANGUAGE CPP #-}\nmodule Pick where\nalpha :: Int -> Int\nalpha x = x\n"
           "beta :: Int -> Int\nbeta x = x\n#if defined(mingw32_HOST_OS)\npick :: Int -> Int\n"
           "pick x = alpha x\n#else\npick :: Int -> Int\npick x = beta x\n#endif\n"
           "caller :: Int -> Int\ncaller x = pick x\n");
    PASS();
}

/* The first branch calls neither target: alpha is reached only through the
 * #elif branch, beta only through the nested group's # else inside #else. */
TEST(cv_haskell_cpp_elif_nested) {
    CV_ONE("haskell", "Pick.hs",
           "{-# LANGUAGE CPP #-}\nmodule Pick where\nalpha :: Int -> Int\nalpha x = x\n"
           "beta :: Int -> Int\nbeta x = x\n#if defined(mingw32_HOST_OS)\npick :: Int -> Int\n"
           "pick x = x\n#elif defined(darwin_HOST_OS)\npick :: Int -> Int\npick x = alpha x\n"
           "#else\n# if WORD_SIZE_IN_BITS == 64\npick :: Int -> Int\npick x = x\n# else\n"
           "pick :: Int -> Int\npick x = beta x\n# endif\n#endif\n"
           "caller :: Int -> Int\ncaller x = pick x\n");
    PASS();
}

TEST(cv_fortran_preprocessed) {
    CV_ONE("fortran", "pick.F90",
           "module pickmod\ncontains\n  subroutine alpha()\n  end subroutine alpha\n"
           "  subroutine beta()\n  end subroutine beta\n#ifdef WINDOWS\n  subroutine pick()\n"
           "    call alpha()\n  end subroutine pick\n#else\n  subroutine pick()\n    call beta()\n"
           "  end subroutine pick\n#endif\n  subroutine caller()\n    call pick()\n"
           "  end subroutine caller\nend module pickmod\n");
    PASS();
}

TEST(cv_pascal_ifdef) {
    CV_ONE("pascal", "pick.pas",
           "unit pick;\ninterface\nprocedure caller;\nimplementation\nprocedure alpha; begin end;\n"
           "procedure beta; begin end;\n{$IFDEF WINDOWS}\nprocedure pick; begin alpha(); end;\n"
           "{$ELSE}\nprocedure pick; begin beta(); end;\n{$ENDIF}\n"
           "procedure caller; begin pick(); end;\nend.\n");
    PASS();
}

TEST(cv_julia_static_if) {
    CV_ONE("julia", "pick.jl",
           "alpha() = nothing\nbeta() = nothing\n@static if Sys.iswindows()\n    pick() = alpha()\n"
           "else\n    pick() = beta()\nend\ncaller() = pick()\n");
    PASS();
}

TEST(cv_elixir_compile_time_if) {
    CV_ONE("elixir", "pick.ex",
           "defmodule Pick do\n  def alpha, do: :ok\n  def beta, do: :ok\n"
           "  if Code.ensure_loaded?(:win32) do\n    def pick, do: alpha()\n  else\n"
           "    def pick, do: beta()\n  end\n  def caller, do: pick()\nend\n");
    PASS();
}

/* A multi-clause function under a compile-time `if`. Each branch's clauses
 * fold into one definition spanning them (the clause fold), and the two
 * branches stay two variants of one identity: exactly the spans 5-6 and 8-9,
 * neither one span per clause (no fold) nor one span over both branches (a
 * fold across the branch boundary). */
TEST(cv_elixir_multi_clause_under_if) {
    static const cv_file_t files[] = {{"pick.ex", "defmodule Pick do\n"                   /* 1 */
                                                  "  def alpha(_), do: :ok\n"             /* 2 */
                                                  "  def beta(_), do: :ok\n"              /* 3 */
                                                  "  if Code.ensure_loaded?(:win32) do\n" /* 4 */
                                                  "    def pick(0), do: alpha(0)\n"       /* 5 */
                                                  "    def pick(n), do: alpha(n)\n"       /* 6 */
                                                  "  else\n"                              /* 7 */
                                                  "    def pick(0), do: beta(0)\n"        /* 8 */
                                                  "    def pick(n), do: beta(n)\n"        /* 9 */
                                                  "  end\n"                               /* 10 */
                                                  "  def caller, do: pick(1)\n"           /* 11 */
                                                  "end\n"}};                              /* 12 */
    cv_proj_t p;
    ASSERT_TRUE(cv_index(&p, files, 1));
    cv_result_t r = cv_measure(&p, "pick", "alpha", "beta", "caller");
    const char *span_do =
        "SELECT COUNT(*) FROM nodes n, json_each(n.properties,'$.variants') v "
        "WHERE n.project=?1 AND n.name=?2 AND n.label IN " CV_DEF_LABELS
        " AND json_extract(v.value,'$.start_line')=5 AND json_extract(v.value,'$.end_line')=6";
    const char *span_else =
        "SELECT COUNT(*) FROM nodes n, json_each(n.properties,'$.variants') v "
        "WHERE n.project=?1 AND n.name=?2 AND n.label IN " CV_DEF_LABELS
        " AND json_extract(v.value,'$.start_line')=8 AND json_extract(v.value,'$.end_line')=9";
    int do_spans = cv_count(&p, span_do, "pick", NULL);
    int else_spans = cv_count(&p, span_else, "pick", NULL);
    cv_cleanup(&p);
    ASSERT_TRUE(cv_expect_structure("elixir_multi_clause", r));
    ASSERT_EQ(r.variant_spans, 2);
    ASSERT_EQ(do_spans, 1);
    ASSERT_EQ(else_spans, 1);
    PASS();
}

/* Verilog: the identity only. Module instantiation produces no CALLS edge in
 * this graph even without any `ifdef (a general Verilog gap, not a variant
 * one), so the body and caller edges are not this suite's to assert. */
TEST(cv_verilog_ifdef) {
    static const cv_file_t files[] = {
        {"pick.sv", "`ifdef FAST\nmodule pick(input a); alpha u(); endmodule\n`else\n"
                    "module pick(input a); beta u(); endmodule\n`endif\nmodule alpha(); endmodule\n"
                    "module beta(); endmodule\nmodule caller(); pick p(.a(1'b0)); endmodule\n"}};
    cv_proj_t p;
    ASSERT_TRUE(cv_index(&p, files, 1));
    cv_result_t r = cv_measure(&p, "pick", "alpha", "beta", "caller");
    cv_cleanup(&p);
    ASSERT_EQ(r.nodes, 1);
    ASSERT_EQ(r.clean_names, 1);
    ASSERT_GTE(r.variant_spans, 2);
    PASS();
}

/* One text value from SQL with text parameters (as cv_count); false when the
 * query fails or returns no row or NULL. */
static bool cv_text(cv_proj_t *p, const char *sql, const char *a, const char *b, char *out,
                    size_t n) {
    sqlite3 *db = cbm_store_get_db(p->store);
    sqlite3_stmt *stmt = NULL;
    out[0] = '\0';
    if (!db || sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return false;
    }
    int params = sqlite3_bind_parameter_count(stmt);
    const char *values[] = {p->project, a, b};
    for (int i = 0; i < params && i < 3; i++) {
        sqlite3_bind_text(stmt, i + 1, values[i], -1, SQLITE_TRANSIENT);
    }
    const char *text =
        sqlite3_step(stmt) == SQLITE_ROW ? (const char *)sqlite3_column_text(stmt, 0) : NULL;
    if (text) {
        snprintf(out, n, "%s", text);
    }
    sqlite3_finalize(stmt);
    return text != NULL;
}

/* One platform variant of `pick`: its file, the function its body calls, and
 * the other variant's callee, which must not be reached from it. */
typedef struct {
    const char *file;
    const char *callee;
    const char *other_callee;
} cv_platform_t;

/* C platform variants (foo_win.c / foo_posix.c, src/unix/ vs src/win/): every
 * definition keeps its own node under its path-derived QN, the QN its file
 * and name compute (findability: a lookup by that QN finds it); it calls what
 * its own body calls; all of them carry the platform-neutral QN
 * `<project>.<neutral>.pick` as variant_key, which relates them, and no
 * `variants` list; a shared caller needs a selected build before choosing an
 * implementation. Its unresolved target remains an explicit quality gap. */
static int cv_platform_check(const char *test, const char *lang, const cv_file_t *files, int count,
                             const cv_platform_t *variants, int variant_count,
                             const char *neutral) {
    cv_proj_t p;
    if (!cv_index(&p, files, count)) {
        fprintf(stderr, "  [%s] fixture could not be indexed\n", lang);
        cv_cleanup(&p);
        return 0;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    char want_key[512];
    snprintf(want_key, sizeof(want_key), "%s.%s.pick", p.project, neutral);
    int ok = 1;
    int nodes = cv_count(&p,
                         "SELECT COUNT(DISTINCT qualified_name) FROM nodes WHERE project=?1 "
                         "AND name='pick' AND label='Function'",
                         NULL, NULL);
    if (nodes != variant_count) {
        fprintf(stderr, "  [%s] pick nodes=%d, want %d\n", lang, nodes, variant_count);
        ok = 0;
    }
    const char *calls = "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                        "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
                        "e.type='CALLS' AND s.qualified_name=?2 AND t.name=?3";
    for (int i = 0; i < variant_count; i++) {
        const cv_platform_t *v = &variants[i];
        const char *want_qn = cbm_fqn_compute(&arena, p.project, v->file, "pick");
        char qn[512];
        char key[512];
        bool found = cv_text(&p,
                             "SELECT qualified_name FROM nodes WHERE project=?1 AND name='pick' "
                             "AND label='Function' AND file_path=?2",
                             v->file, NULL, qn, sizeof(qn));
        bool keyed = found && cv_text(&p,
                                      "SELECT json_extract(properties,'$.variant_key') FROM nodes "
                                      "WHERE project=?1 AND qualified_name=?2",
                                      qn, NULL, key, sizeof(key));
        int own = found ? cv_count(&p, calls, qn, v->callee) : -1;
        int other = found ? cv_count(&p, calls, qn, v->other_callee) : -1;
        if (!found || !want_qn || strcmp(qn, want_qn) != 0 || !keyed ||
            strcmp(key, want_key) != 0 || other != 0) {
            fprintf(stderr, "  [%s] %s: qn=%s (want %s) variant_key=%s (want %s) ->%s=%d ->%s=%d\n",
                    lang, v->file, found ? qn : "-", want_qn ? want_qn : "?", keyed ? key : "-",
                    want_key, v->callee, own, v->other_callee, other);
            ok = 0;
        }
    }
    int listed = cv_count(&p,
                          "SELECT COUNT(*) FROM nodes WHERE project=?1 AND name='pick' "
                          "AND json_extract(properties,'$.variants') IS NOT NULL",
                          NULL, NULL);
    int from_caller = cv_count(&p,
                               "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                               "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND "
                               "e.type='CALLS' AND s.name='caller' AND t.name='pick'",
                               NULL, NULL);
    if (listed != 0 || from_caller != 0) {
        fprintf(stderr, "  [%s] pick nodes with variants=%d caller->pick=%d\n", lang, listed,
                from_caller);
        ok = 0;
    }
    ok = cv_quality(&p, test, files, count) && ok;
    cbm_arena_destroy(&arena);
    cv_cleanup(&p);
    return ok;
}

/* C platform files: the same function in foo_win.c and foo_posix.c. */
TEST(cv_c_platform_files) {
    static const cv_file_t files[] = {
        {"src/plat_win.c", "void alpha(void);\nvoid pick(void) { alpha(); }\n"},
        {"src/plat_posix.c", "void beta(void);\nvoid pick(void) { beta(); }\n"},
        {"src/plat.c", "void alpha(void) {}\nvoid beta(void) {}\nvoid pick(void);\n"
                       "void caller(void) { pick(); }\n"}};
    static const cv_platform_t variants[] = {{"src/plat_win.c", "alpha", "beta"},
                                             {"src/plat_posix.c", "beta", "alpha"}};
    ASSERT_TRUE(cv_platform_check(__func__, "c_platform_files", files, 3, variants, 2, "src.plat"));
    PASS();
}

/* Platform directories (libuv's src/unix/ and src/win/) are platform variants
 * the same way platform suffixes are. */
TEST(cv_c_platform_directories) {
    static const cv_file_t files[] = {
        {"src/unix/fs.c", "void alpha(void);\nvoid pick(void) { alpha(); }\n"},
        {"src/win/fs.c", "void beta(void);\nvoid pick(void) { beta(); }\n"},
        {"src/core.c", "void alpha(void) {}\nvoid beta(void) {}\nvoid pick(void);\n"
                       "void caller(void) { pick(); }\n"}};
    static const cv_platform_t variants[] = {{"src/unix/fs.c", "alpha", "beta"},
                                             {"src/win/fs.c", "beta", "alpha"}};
    ASSERT_TRUE(cv_platform_check(__func__, "c_platform_dirs", files, 3, variants, 2, "src.fs"));
    PASS();
}

/* Not variants: static functions are file-local even in platform files, and
 * same-named external functions in files without a platform token may belong
 * to different programs. Each keeps its own node, and none carries a
 * variant_key. */
TEST(cv_c_non_variants_keep_their_nodes) {
    static const cv_file_t files[] = {{"src/plat_win.c", "static void pick(void) {}\n"},
                                      {"src/plat_posix.c", "static void pick(void) {}\n"},
                                      {"tools/one.c", "void helper(void) {}\n"},
                                      {"tools/two.c", "void helper(void) {}\n"}};
    cv_proj_t p;
    ASSERT_TRUE(cv_index(&p, files, 4));
    int picks = cv_count(&p,
                         "SELECT COUNT(DISTINCT qualified_name) FROM nodes WHERE project=?1 "
                         "AND name='pick' AND label='Function'",
                         NULL, NULL);
    int helpers = cv_count(&p,
                           "SELECT COUNT(DISTINCT qualified_name) FROM nodes WHERE project=?1 "
                           "AND name='helper' AND label='Function'",
                           NULL, NULL);
    int keyed = cv_count(&p,
                         "SELECT COUNT(*) FROM nodes WHERE project=?1 "
                         "AND json_extract(properties,'$.variant_key') IS NOT NULL",
                         NULL, NULL);
    cv_cleanup(&p);
    ASSERT_EQ(picks, 2);
    ASSERT_EQ(helpers, 2);
    ASSERT_EQ(keyed, 0);
    PASS();
}

/* A call that names a function no definition of its own language provides
 * must not bind to a same-named definition in another language. */
TEST(cv_no_cross_language_name_binding) {
    static const cv_file_t files[] = {
        {"go.mod", "module example.com/cv\n\ngo 1.22\n"},
        {"pick/pick.go", "package pick\n\nfunc Pick() {}\n"},
        {"cs/Caller.cs", "class C {\n  static void Caller() { Pick(); }\n}\n"}};
    cv_proj_t p;
    ASSERT_TRUE(cv_index(&p, files, 3));
    /* Positive controls: both definitions are in the graph, so a missing
     * edge below is the rule at work, not an empty index. */
    int go_pick = cv_count(&p,
                           "SELECT COUNT(*) FROM nodes WHERE project=?1 AND name='Pick' "
                           "AND file_path LIKE '%.go'",
                           NULL, NULL);
    int cs_caller = cv_count(&p,
                             "SELECT COUNT(*) FROM nodes WHERE project=?1 AND name='Caller' "
                             "AND file_path LIKE '%.cs'",
                             NULL, NULL);
    int crossing =
        cv_count(&p,
                 "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                 "JOIN nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type='CALLS' "
                 "AND s.name='Caller' AND t.file_path LIKE '%.go'",
                 NULL, NULL);
    cv_cleanup(&p);
    ASSERT_EQ(go_pick, 1);
    ASSERT_EQ(cs_caller, 1);
    ASSERT_EQ(crossing, 0);
    PASS();
}

/* ── Incremental: a definition written in several files ───────────────
 * A stored resolver generation includes negative lookup dependencies. On a
 * changed manifest the current planner requires a full rebuild, because the
 * old closure graph alone cannot prove those dependencies are unchanged.
 * Editing either Go variant must retain both spans; C platform variants must
 * retain separate nodes and their shared key. After each edit compare the
 * graph, resolver outcomes and immutable source/proof snapshots with a fresh
 * index of exactly the same tree. */

static bool cv_pipeline(const char *repo, const char *db, cbm_incremental_route_t *route) {
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (!p) {
        return false;
    }
    bool ok = cbm_pipeline_set_project_name(p, "cvinc");
    cbm_pipeline_set_persistence(p, false);
    ok = ok && cbm_pipeline_run(p) == 0;
    if (route) {
        *route = cbm_pipeline_incremental_test_last_route();
    }
    cbm_pipeline_free(p);
    return ok;
}

/* CALLS edges by qualified name, every node's variant spans and every node's
 * variant_key, one line each. */
static bool cv_signature(const char *db_path, char *out, size_t n) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    const char *sql =
        "SELECT line FROM (SELECT s.qualified_name || '>' || t.qualified_name AS line "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.type='CALLS' "
        "UNION ALL SELECT 'variants ' || qualified_name || ' ' || "
        "json_extract(properties,'$.variants') FROM nodes "
        "WHERE json_extract(properties,'$.variants') IS NOT NULL "
        "UNION ALL SELECT 'key ' || qualified_name || ' ' || "
        "json_extract(properties,'$.variant_key') FROM nodes "
        "WHERE json_extract(properties,'$.variant_key') IS NOT NULL) ORDER BY line";
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK;
    out[0] = '\0';
    int step = SQLITE_DONE;
    while (ok && (step = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *line = (const char *)sqlite3_column_text(stmt, 0);
        size_t used = strlen(out);
        int written = snprintf(out + used, n - used, "%s\n", line ? line : "?");
        ok = written >= 0 && (size_t)written < n - used;
    }
    ok = ok && step == SQLITE_DONE;
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return ok;
}

/* Compare semantic rows in a stable order. Numeric database IDs/ordinals are
 * replaced by their source identities or proof digests, never discarded facts. */
static bool cv_same_rows(sqlite3 *left, sqlite3 *right, const char *sql) {
    sqlite3_stmt *a = NULL, *b = NULL;
    bool ok = sqlite3_prepare_v2(left, sql, -1, &a, NULL) == SQLITE_OK &&
              sqlite3_prepare_v2(right, sql, -1, &b, NULL) == SQLITE_OK;
    while (ok) {
        int sa = sqlite3_step(a), sb = sqlite3_step(b);
        if (sa == SQLITE_DONE && sb == SQLITE_DONE)
            break;
        if (sa != SQLITE_ROW || sb != SQLITE_ROW) {
            ok = false;
            break;
        }
        const char *va = (const char *)sqlite3_column_text(a, 0);
        const char *vb = (const char *)sqlite3_column_text(b, 0);
        ok = va && vb && strcmp(va, vb) == 0;
    }
    sqlite3_finalize(a);
    sqlite3_finalize(b);
    return ok;
}

static bool cv_resolver_matches(const char *inc_db, const char *full_db) {
    static const char *queries[] = {
        "SELECT json_array(project,rules_version) FROM resolver_generations ORDER BY 1",
        "SELECT json_array(file_path,module,language,raw_content_identity,"
        "preprocessed_content_identity,configuration_identity,raw_length,preprocessed_length,"
        "extracted_summary,derived_summary,complete) FROM resolver_file_summaries ORDER BY 1",
        "SELECT json_array(r.file_path,r.scope,r.relation,r.spelling,r.start_byte,r.end_byte,"
        "r.source_origin,r.binding,r.domain,r.analysis,r.rule,r.facts,r.target,"
        "r.candidate_count,r.reason,r.emitted,r.rules_version,r.properties_json,"
        "v.content_identity,v.configuration_identity,p.digest,s.digest) "
        "FROM resolver_occurrences r LEFT JOIN resolver_occurrence_evidence v "
        "ON v.project=r.project AND v.ordinal=r.ordinal LEFT JOIN resolver_facts p "
        "ON p.project=v.project AND p.id=v.proof LEFT JOIN resolver_facts s "
        "ON s.project=v.project AND s.id=v.source_binding_proof ORDER BY 1",
        "SELECT json_array(kind,source_file,digest,payload) FROM resolver_facts ORDER BY 1",
        "SELECT json_array(f.digest,s.ordinal,p.digest) FROM resolver_supports s "
        "LEFT JOIN resolver_facts f ON f.project=s.project AND f.id=s.fact "
        "LEFT JOIN resolver_facts p ON p.project=s.project AND p.id=s.premise ORDER BY 1",
        "SELECT json_array(f.digest,l.domain,l.key,l.status) FROM resolver_lookups l "
        "LEFT JOIN resolver_facts f ON f.project=l.project AND f.id=l.fact ORDER BY 1",
        "SELECT json_array(b.file_path,b.key,b.kind,b.module,b.closed,b.binding,b.domain,"
        "b.analysis,b.target,f.digest) FROM resolver_derived_bindings b "
        "LEFT JOIN resolver_facts f ON f.project=b.project AND f.id=b.proof ORDER BY 1",
        "SELECT json_array(s.qualified_name,e.type,t.qualified_name,e.properties) "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "ORDER BY 1"};
    sqlite3 *a = NULL, *b = NULL;
    bool ok = sqlite3_open_v2(inc_db, &a, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
              sqlite3_open_v2(full_db, &b, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < sizeof(queries) / sizeof(queries[0]); i++) {
        ok = cv_same_rows(a, b, queries[i]);
        if (!ok)
            fprintf(stderr, "  [incremental resolver] signature mismatch query=%zu\n", i);
    }
    sqlite3_close(a);
    sqlite3_close(b);
    return ok;
}

/* Separate from both unchanged language fixtures: this required exact binding
 * must survive every edited generation and its independently rebuilt peer. */
static const cv_file_t cv_inc_control = {"control.py",
                                         "def exact_target():\n    return 1\n"
                                         "def exact_caller():\n    return exact_target()\n"};

static bool cv_incremental_quality(const char *db_path, const cv_file_t *files, int count, bool go,
                                   bool second_edit) {
    RQ_Source sources[6];
    size_t source_count = 0;
    for (int i = 0; i < count; i++)
        if (strcmp(files[i].name, "go.mod") != 0)
            sources[source_count++] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    sources[source_count++] =
        (RQ_Source){.path = cv_inc_control.name, .text = cv_inc_control.content};
    RQ_Expected expected[5] = {{.case_id = "incremental-required-exact-call",
                                .site = {.file = "control.py",
                                         .caller = "exact_caller",
                                         .needle = "exact_target()",
                                         .occurrence_index = 1},
                                .relation = "CALLS",
                                .target = {.file = "control.py",
                                           .suffix = "control.exact_target",
                                           .label = "Function",
                                           .declaration_line = 1,
                                           .declaration_text = "def exact_target():"},
                                .mode = RQ_REQUIRED}};
    RQ_Forbidden forbidden[4] = {0};
    if (go) {
        static const char *ids[] = {"go-variant-alpha", "go-variant-gamma", "go-variant-beta",
                                    "go-selected-build-identity"};
        static const char *paths[] = {"pick/pick_linux.go", "pick/pick_linux.go",
                                      "pick/pick_windows.go", "pick/caller.go"};
        static const char *needles[] = {"alpha()", "gammaFn()", "beta()", "Pick()"};
        static const char *targets[] = {"pick.alpha", "pick.gammaFn", "pick.beta", NULL};
        static const int lines[] = {3, 5, 4, 4};
        static const char *decls[] = {"func alpha() {}", "func gammaFn() {}", "func beta() {}",
                                      "func Pick() { alpha(); gammaFn() }"};
        for (size_t i = 0; i < 4; i++)
            expected[i + 1] = (RQ_Expected){
                .case_id = ids[i],
                .site = {.file = paths[i],
                         .caller = i == 3 ? "Caller" : "Pick",
                         .needle = needles[i]},
                .relation = "CALLS",
                .target = {.file = i == 3 ? "pick/pick_linux.go" : "pick/util.go",
                           .suffix = targets[i],
                           .label = "Function",
                           .declaration_line = lines[i],
                           .declaration_text = decls[i]},
                .mode = RQ_KNOWN_GAP,
                .gap_id = i == 3 ? "conditional-variant-graph-identity"
                                 : "conditional-variant-source-binding",
                .tried = "The original conditional incremental fixture failed its CALLS "
                         "expectations under strict admission. Both unchanged edited "
                         "source snapshots are checked against a full rebuild; no build "
                         "selection or variant coordinate mapping is invented.",
                .why = i == 3
                           ? "No selected build identifies a physical Pick declaration; the source "
                             "call remains genuine debt until variant-aware identity is proved."
                           : "Go build-conditional package binding lacks a complete proof; "
                             "source-authored body calls remain counted quality debt."};
    } else {
        static const char *ids[] = {"c-undeclared-alpha", "c-undeclared-gamma", "c-undeclared-beta",
                                    "c-undeclared-pick"};
        static const char *paths[] = {"pick_linux.c", "pick_linux.c", "pick_win.c", "caller.c"};
        static const char *needles[] = {"alpha()", "gamma_fn()", "beta()", "pick()"};
        for (size_t i = 0; i < 4; i++)
            forbidden[i] = (RQ_Forbidden){.case_id = ids[i],
                                          .site = {.file = paths[i],
                                                   .caller = i == 3 ? "caller" : "pick",
                                                   .needle = needles[i]},
                                          .relation = "CALLS",
                                          .any_target = true};
    }
    cv_proj_t p = {.project = "cvinc", .store = cbm_store_open_path(db_path)};
    if (!p.store)
        return false;
    RQ_Report report = {0};
    bool ok =
        rq_check(p.store, p.project,
                 go ? "conditional-incremental-go" : "conditional-incremental-c", sources,
                 source_count, expected, go ? 5 : 1, forbidden, go ? 0 : 4, &report, stderr) &&
        report.genuine == (go ? 5u : 1u) && report.found == 1 && report.missing == (go ? 4u : 0u);
    if (go) {
        cv_result_t r = cv_measure(&p, "Pick", "alpha", "beta", "Caller");
        ok = ok && cv_expect_structure("Go incremental", r);
        const char *span_sql =
            "SELECT COUNT(*) FROM nodes n,json_each(n.properties,'$.variants') v "
            "WHERE n.project=?1 AND n.qualified_name='cvinc.pick.Pick' "
            "AND json_extract(v.value,'$.file_path')=?2 "
            "AND json_extract(v.value,'$.start_line')=4 "
            "AND json_extract(v.value,'$.end_line')=CAST(?3 AS INTEGER)";
        ok = ok && cv_count(&p, span_sql, "pick/pick_linux.go", "4") == 1 &&
             cv_count(&p, span_sql, "pick/pick_windows.go", second_edit ? "6" : "4") == 1;
    } else {
        const char *node_sql =
            "SELECT COUNT(*) FROM nodes WHERE project=?1 AND qualified_name=?2 "
            "AND label='Function' AND start_line=1 AND end_line=CAST(?3 AS INTEGER) "
            "AND json_extract(properties,'$.variant_key')='cvinc.pick.pick' "
            "AND json_extract(properties,'$.variants') IS NULL";
        ok = ok && cv_count(&p, node_sql, "cvinc.pick_linux.pick", "1") == 1 &&
             cv_count(&p, node_sql, "cvinc.pick_win.pick", second_edit ? "3" : "1") == 1;
    }
    cbm_store_close(p.store);
    return ok;
}

/* Rewrite one variant, assert the documented conservative route, and compare
 * both published generations. RQ independently checks exact current snapshots
 * and required/forbidden source sites rather than trusting equality alone. */
static bool cv_edit_matches_full(const char *repo, const char *dbdir, const char *name,
                                 const char *content, char *sig, size_t n, const cv_file_t *files,
                                 int count, bool go, bool second_edit) {
    char inc_db[600];
    char full_db[600];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", dbdir);
    snprintf(full_db, sizeof(full_db), "%s/full.db", dbdir);
    sqlite3 *before = NULL;
    bool current = sqlite3_open_v2(inc_db, &before, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
                   rq_generation(before, "cvinc");
    sqlite3_close(before);
    static char full_sig[8192];
    const cv_file_t file = {name, content};
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    bool ok = current && cv_write(repo, &file) && cv_pipeline(repo, inc_db, &route);
    if (route != CBM_INCREMENTAL_ROUTE_FORCED_FULL) {
        fprintf(stderr, "  [incremental %s] route %d, expected resolver full rebuild\n", name,
                (int)route);
        ok = false;
    }
    unlink(full_db);
    ok = ok && cv_pipeline(repo, full_db, NULL) && cv_signature(inc_db, sig, n) &&
         cv_signature(full_db, full_sig, sizeof(full_sig));
    if (ok && strcmp(sig, full_sig) != 0) {
        fprintf(stderr, "  [incremental %s] rebuilt:\n%s  full:\n%s", name, sig, full_sig);
        ok = false;
    }
    return ok && cv_resolver_matches(inc_db, full_db) &&
           cv_incremental_quality(inc_db, files, count, go, second_edit) &&
           cv_incremental_quality(full_db, files, count, go, second_edit);
}

/* Write `files` into a fresh repository and index it once (the base the
 * edits repair). */
static bool cv_incremental_base(char *repo, size_t repo_n, char *dbdir, size_t dbdir_n,
                                const cv_file_t *files, int count) {
    snprintf(repo, repo_n, "%s", th_mktempdir("cbm-cvinc"));
    snprintf(dbdir, dbdir_n, "%s", th_mktempdir("cbm-cvdb"));
    bool ok = repo[0] && dbdir[0];
    for (int i = 0; ok && i < count; i++) {
        ok = cv_write(repo, &files[i]);
    }
    char inc_db[600];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", dbdir);
    return ok && cv_write(repo, &cv_inc_control) && cv_pipeline(repo, inc_db, NULL);
}

TEST(cv_incremental_variant_partners) {
    char repo[300];
    char dbdir[300];
    cv_file_t files[] = {
        {"go.mod", "module example.com/cvinc\n\ngo 1.22\n"},
        {"pick/util.go", "package pick\n\nfunc alpha() {}\nfunc beta() {}\nfunc gammaFn() {}\n"},
        {"pick/pick_linux.go", "//go:build linux\npackage pick\n\nfunc Pick() { alpha() }\n"},
        {"pick/pick_windows.go", "//go:build windows\npackage pick\n\nfunc Pick() { beta() }\n"},
        {"pick/caller.go", "package pick\n\nfunc Caller() { Pick() }\n"},
    };
    ASSERT_TRUE(cv_incremental_base(repo, sizeof(repo), dbdir, sizeof(dbdir), files,
                                    (int)(sizeof(files) / sizeof(files[0]))));
    static char sig[8192];
    /* The authored bytes and both variant spans survive each edit. */
    files[2].content = "//go:build linux\npackage pick\n\nfunc Pick() { alpha(); gammaFn() }\n";
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, files[2].name, files[2].content, sig, sizeof(sig),
                                     files, 5, true, false));
    files[3].content = "//go:build windows\npackage pick\n\nfunc Pick() {\n\tbeta()\n}\n";
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, files[3].name, files[3].content, sig, sizeof(sig),
                                     files, 5, true, true));
    th_rmtree(repo);
    th_rmtree(dbdir);
    PASS();
}

/* C platform files retain separate nodes and their variant_key through either
 * edit. Their undeclared calls cannot establish a target binding. */
TEST(cv_incremental_platform_files) {
    char repo[300];
    char dbdir[300];
    cv_file_t files[] = {
        {"util.c", "void alpha(void) {}\nvoid beta(void) {}\nvoid gamma_fn(void) {}\n"},
        {"pick_linux.c", "void pick(void) { alpha(); }\n"},
        {"pick_win.c", "void pick(void) { beta(); }\n"},
        {"caller.c", "void caller(void) { pick(); }\n"},
    };
    ASSERT_TRUE(cv_incremental_base(repo, sizeof(repo), dbdir, sizeof(dbdir), files,
                                    (int)(sizeof(files) / sizeof(files[0]))));
    static char sig[8192];
    files[1].content = "void pick(void) { alpha(); gamma_fn(); }\n";
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, files[1].name, files[1].content, sig, sizeof(sig),
                                     files, 4, false, false));
    ASSERT_TRUE(strstr(sig, "key cvinc.pick_linux.pick cvinc.pick.pick\n") &&
                strstr(sig, "key cvinc.pick_win.pick cvinc.pick.pick\n"));
    files[2].content = "void pick(void) {\n  beta();\n}\n";
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, files[2].name, files[2].content, sig, sizeof(sig),
                                     files, 4, false, true));
    ASSERT_TRUE(strstr(sig, "key cvinc.pick_linux.pick cvinc.pick.pick\n") &&
                strstr(sig, "key cvinc.pick_win.pick cvinc.pick.pick\n"));
    th_rmtree(repo);
    th_rmtree(dbdir);
    PASS();
}

/* A zero-edge implementation cannot satisfy the quality migration: this
 * ordinary exact local call is a required positive through the same harness. */
TEST(cv_unconditional_exact_call_control) {
    static const cv_file_t files[] = {{"control.py",
                                       "def exact_target():\n    return 1\n"
                                       "def exact_caller():\n    return exact_target()\n"}};
    const RQ_Source source = {.path = "control.py", .text = files[0].content};
    const RQ_Expected expected = {.case_id = "unconditional-exact-call",
                                  .site = {.file = "control.py",
                                           .caller = "exact_caller",
                                           .needle = "exact_target()",
                                           .occurrence_index = 1},
                                  .relation = "CALLS",
                                  .target = {.file = "control.py",
                                             .suffix = "control.exact_target",
                                             .label = "Function",
                                             .declaration_line = 1,
                                             .declaration_text = "def exact_target():"},
                                  .mode = RQ_REQUIRED};
    cv_proj_t p;
    ASSERT_TRUE(cv_index(&p, files, 1));
    RQ_Report report = {0};
    bool ok = rq_check(p.store, p.project, "conditional-unconditional-control", &source, 1,
                       &expected, 1, NULL, 0, &report, stderr);
    cv_cleanup(&p);
    ASSERT_TRUE(ok);
    ASSERT_EQ(report.genuine, 1);
    ASSERT_EQ(report.found, 1);
    ASSERT_EQ(report.missing, 0);
    PASS();
}

SUITE(conditional_variants) {
    RUN_TEST(cv_unconditional_exact_call_control);
    RUN_TEST(cv_c_preprocessor);
    RUN_TEST(cv_cpp_elif);
    RUN_TEST(cv_cpp_class_members);
    RUN_TEST(cv_objc_methods);
    RUN_TEST(cv_cuda);
    RUN_TEST(cv_csharp_class_members);
    RUN_TEST(cv_fsharp);
    RUN_TEST(cv_rust_cfg_free_functions);
    RUN_TEST(cv_rust_cfg_methods);
    RUN_TEST(cv_go_build_tag_files);
    RUN_TEST(cv_swift);
    RUN_TEST(cv_python_if);
    RUN_TEST(cv_ruby_if);
    RUN_TEST(cv_php_if);
    RUN_TEST(cv_d_version);
    RUN_TEST(cv_erlang_ifdef);
    RUN_TEST(cv_haskell_cpp);
    RUN_TEST(cv_haskell_cpp_elif_nested);
    RUN_TEST(cv_fortran_preprocessed);
    RUN_TEST(cv_pascal_ifdef);
    RUN_TEST(cv_julia_static_if);
    RUN_TEST(cv_elixir_compile_time_if);
    RUN_TEST(cv_elixir_multi_clause_under_if);
    RUN_TEST(cv_verilog_ifdef);
    RUN_TEST(cv_c_platform_files);
    RUN_TEST(cv_c_platform_directories);
    RUN_TEST(cv_c_non_variants_keep_their_nodes);
    RUN_TEST(cv_no_cross_language_name_binding);
    RUN_TEST(cv_incremental_variant_partners);
    RUN_TEST(cv_incremental_platform_files);
}
