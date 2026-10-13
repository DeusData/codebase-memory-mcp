/*
 * test_conditional_variants.c — one definition with mutually exclusive
 * variants is ONE identity in the graph, in every language.
 *
 * A variant is a second definition of the same name that only one build or
 * run ever sees: C-family #if/#else, C#/F# #if, Rust #[cfg], Swift #if,
 * Go build-tagged files, D version blocks, Erlang -ifdef, Haskell CPP,
 * Fortran .F90 preprocessing, Verilog `ifdef, Elixir/Python/Ruby/PHP
 * definitions under an `if`, and C platform files (foo_win.c / foo_posix.c).
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
#include <foundation/compat_fs.h>
#include <mcp/mcp.h>
#include <pipeline/pipeline.h>
#include <pipeline/pipeline_internal.h>
#include <store/store.h>
#include <sqlite3.h>

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

static int cv_expect(const char *lang, cv_result_t r) {
    bool ok = r.nodes == 1 && r.clean_names == 1 && r.to_alpha >= 1 && r.to_beta >= 1 &&
              r.from_caller >= 1 && r.variant_spans >= 2;
    if (!ok) {
        fprintf(
            stderr, "  [%s] nodes=%d clean=%d ->alpha=%d ->beta=%d caller->=%d variant_spans=%d\n",
            lang, r.nodes, r.clean_names, r.to_alpha, r.to_beta, r.from_caller, r.variant_spans);
    }
    return ok;
}

static int cv_run(const char *lang, const cv_file_t *files, int count, const char *pick,
                  const char *alpha, const char *beta, const char *caller) {
    cv_proj_t p;
    bool indexed = cv_index(&p, files, count);
    int ok = indexed && cv_expect(lang, cv_measure(&p, pick, alpha, beta, caller));
    if (!indexed) {
        fprintf(stderr, "  [%s] fixture could not be indexed\n", lang);
    }
    cv_cleanup(&p);
    return ok;
}

#define CV_ONE(lang, path, src)                                                  \
    do {                                                                         \
        static const cv_file_t files_[] = {{path, src}};                         \
        ASSERT_TRUE(cv_run(lang, files_, 1, "pick", "alpha", "beta", "caller")); \
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
    ASSERT_TRUE(cv_run("csharp", files, 1, "Pick", "Alpha", "Beta", "Caller"));
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
    ASSERT_TRUE(cv_run("rust", files, 2, "pick", "alpha", "beta", "caller"));
    PASS();
}

TEST(cv_rust_cfg_methods) {
    static const cv_file_t files[] = {
        {"Cargo.toml", "[package]\nname = \"cv\"\nversion = \"0.1.0\"\n"},
        {"src/lib.rs",
         "struct P;\nfn alpha() {}\nfn beta() {}\nimpl P {\n    #[cfg(target_os = \"linux\")]\n"
         "    fn pick(&self) { alpha(); }\n    #[cfg(not(target_os = \"linux\"))]\n"
         "    fn pick(&self) { beta(); }\n    fn caller(&self) { self.pick(); }\n}\n"}};
    ASSERT_TRUE(cv_run("rust_impl", files, 2, "pick", "alpha", "beta", "caller"));
    PASS();
}

TEST(cv_go_build_tag_files) {
    static const cv_file_t files[] = {
        {"go.mod", "module example.com/cv\n\ngo 1.22\n"},
        {"pick/pick_windows.go", "//go:build windows\npackage pick\n\nfunc Pick() { alpha() }\n"},
        {"pick/pick_posix.go", "//go:build !windows\npackage pick\n\nfunc Pick() { beta() }\n"},
        {"pick/common.go",
         "package pick\n\nfunc alpha() {}\nfunc beta() {}\nfunc Caller() { Pick() }\n"}};
    ASSERT_TRUE(cv_run("go", files, 4, "Pick", "alpha", "beta", "Caller"));
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

/* C platform files: the same function in foo_win.c and foo_posix.c is one
 * definition with two variants, and a caller in a shared file binds to it. */
TEST(cv_c_platform_files) {
    static const cv_file_t files[] = {
        {"src/plat_win.c", "void alpha(void);\nvoid pick(void) { alpha(); }\n"},
        {"src/plat_posix.c", "void beta(void);\nvoid pick(void) { beta(); }\n"},
        {"src/plat.c", "void alpha(void) {}\nvoid beta(void) {}\nvoid pick(void);\n"
                       "void caller(void) { pick(); }\n"}};
    ASSERT_TRUE(cv_run("c_platform_files", files, 3, "pick", "alpha", "beta", "caller"));
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
    ASSERT_TRUE(cv_run("c_platform_dirs", files, 3, "pick", "alpha", "beta", "caller"));
    PASS();
}

/* Not variants: static functions are file-local even in platform files, and
 * same-named external functions in files without a platform token may belong
 * to different programs. Each keeps its own node. */
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
    cv_cleanup(&p);
    ASSERT_EQ(picks, 2);
    ASSERT_EQ(helpers, 2);
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
 * Closure repair re-parses only the changed files and their recorded
 * dependents. `pick` written in two platform files is ONE node; editing
 * either file must re-parse the other too, or the repair drops the other
 * variant's span and calls (when the edited file holds the node) or keeps
 * the edited file's stale ones. After each edit the repaired graph must be
 * the graph a full index of the same tree builds. */

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

/* CALLS edges by name plus pick's variant spans, one line each. */
static bool cv_signature(const char *db_path, char *out, size_t n) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    const char *sql =
        "SELECT line FROM (SELECT s.name || '>' || t.name AS line FROM edges e "
        "JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.type='CALLS' "
        "UNION ALL SELECT 'variants ' || json_extract(properties,'$.variants') FROM nodes "
        "WHERE name='pick' AND label='Function') ORDER BY line";
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK;
    out[0] = '\0';
    while (ok && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *line = (const char *)sqlite3_column_text(stmt, 0);
        size_t used = strlen(out);
        snprintf(out + used, n - used, "%s\n", line ? line : "?");
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return ok;
}

/* Rewrite `name` in `repo`, repair the incremental DB, index the same tree
 * from scratch, and compare. */
static bool cv_edit_matches_full(const char *repo, const char *dbdir, const char *name,
                                 const char *content, char *sig, size_t n) {
    char inc_db[600];
    char full_db[600];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", dbdir);
    snprintf(full_db, sizeof(full_db), "%s/full.db", dbdir);
    static char full_sig[8192];
    const cv_file_t file = {name, content};
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    bool ok = cv_write(repo, &file) && cv_pipeline(repo, inc_db, &route);
    if (route != CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR) {
        fprintf(stderr, "  [incremental %s] route %d, not closure repair\n", name, (int)route);
        ok = false;
    }
    unlink(full_db);
    ok = ok && cv_pipeline(repo, full_db, NULL) && cv_signature(inc_db, sig, n) &&
         cv_signature(full_db, full_sig, sizeof(full_sig));
    if (ok && strcmp(sig, full_sig) != 0) {
        fprintf(stderr, "  [incremental %s] repaired:\n%s  full:\n%s", name, sig, full_sig);
        ok = false;
    }
    return ok;
}

TEST(cv_incremental_variant_partners) {
    char repo[300];
    char dbdir[300];
    snprintf(repo, sizeof(repo), "%s", th_mktempdir("cbm-cvinc"));
    snprintf(dbdir, sizeof(dbdir), "%s", th_mktempdir("cbm-cvdb"));
    ASSERT_TRUE(repo[0] && dbdir[0]);
    static const cv_file_t files[] = {
        {"util.c", "void alpha(void) {}\nvoid beta(void) {}\nvoid gamma_fn(void) {}\n"},
        {"pick_linux.c", "void pick(void) { alpha(); }\n"},
        {"pick_win.c", "void pick(void) { beta(); }\n"},
        {"caller.c", "void caller(void) { pick(); }\n"},
    };
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(cv_write(repo, &files[i]));
    }
    char inc_db[600];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", dbdir);
    ASSERT_TRUE(cv_pipeline(repo, inc_db, NULL));
    static char sig[8192];
    /* Edit each variant file in turn (one of them holds the node). */
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, "pick_linux.c",
                                     "void pick(void) { alpha(); gamma_fn(); }\n", sig,
                                     sizeof(sig)));
    ASSERT_TRUE(strstr(sig, "pick>beta\n") && strstr(sig, "pick>gamma_fn\n"));
    ASSERT_TRUE(cv_edit_matches_full(repo, dbdir, "pick_win.c", "void pick(void) {\n  beta();\n}\n",
                                     sig, sizeof(sig)));
    ASSERT_TRUE(strstr(sig, "pick>alpha\n") && strstr(sig, "pick>beta\n"));
    th_rmtree(repo);
    th_rmtree(dbdir);
    PASS();
}

SUITE(conditional_variants) {
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
    RUN_TEST(cv_verilog_ifdef);
    RUN_TEST(cv_c_platform_files);
    RUN_TEST(cv_c_platform_directories);
    RUN_TEST(cv_c_non_variants_keep_their_nodes);
    RUN_TEST(cv_no_cross_language_name_binding);
    RUN_TEST(cv_incremental_variant_partners);
}
