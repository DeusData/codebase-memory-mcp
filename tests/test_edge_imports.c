/*
 * test_edge_imports.c — Pipeline/edge-creation reproduction suite for IMPORTS
 * edges across all 9 hybrid-LSP languages.
 *
 * ── CONTEXT ─────────────────────────────────────────────────────────────────
 * This suite tests the GRAPH LEVEL (pipeline / edge-creation), NOT extraction.
 * A real-repo sanity check (2026-06) found IMPORTS edges ≈ 0 for several
 * languages even though CBMFileResult.imports IS populated at extraction time:
 *
 *   Language     import keyword   real-repo edges   status
 *   ----------   --------------   ---------------   --------
 *   Rust         use              2168 uses → 0      BUG (expected RED)
 *   Kotlin       import           6110 → ~0          BUG (expected RED)
 *   Java         import           many  → 0          BUG (expected RED)
 *   C#           using            many  → 0          BUG (expected RED)
 *   PHP          use              many  → ~0          BUG (expected RED)
 *   Python       import/from      working             OK  (expected GREEN)
 *   TypeScript   import           working             OK  (expected GREEN)
 *   Go           import           working             OK  (expected GREEN)
 *
 * ── WHAT THIS FILE TESTS ────────────────────────────────────────────────────
 * Each test indexes a small multi-file fixture through the FULL production
 * pipeline (index_repository → graph DB), then asserts:
 *   cbm_store_count_edges_by_type(store, project, "IMPORTS") >= N
 *
 * GREEN (guard) tests: Python, TypeScript, Go — these already produce IMPORTS
 * edges and MUST keep doing so. A RED here is a real regression.
 *
 * RED (bug reproduction) tests: Rust, Kotlin, Java, C#, PHP — the pipeline
 * does not yet turn extracted imports into resolved IMPORTS graph edges for
 * these languages. Each test should FAIL until the bug is fixed, at which
 * point it becomes a permanent regression guard.
 *
 * ── FIXTURE DESIGN ──────────────────────────────────────────────────────────
 * Every fixture uses two files in the same project: one defines a module/type,
 * the other imports it by the language's normal internal mechanism. Single-file
 * fixtures cannot produce inter-file IMPORTS edges; the import must cross files
 * so the resolver has a resolvable target in the same project graph.
 *
 * ── REGISTRATION ────────────────────────────────────────────────────────────
 * SUITE(edge_imports) is declared here. Do NOT register it in test_main.c
 * (another agent owns that file); the suite runs standalone via its own runner
 * when linked.
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

/* ── Harness (mirrors test_lang_contract.c) ─────────────────────────────── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} EILangProj;

typedef struct {
    const char *name;    /* relative filename, may include '/' for subdirs */
    const char *content;
} EILangFile;

static void ei_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

/* Write files, run index_repository, open graph DB.  Returns NULL on failure. */
static cbm_store_t *ei_index_files(EILangProj *lp, const EILangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_ei_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    ei_to_fwd_slashes(lp->tmpdir);

    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        /* Create intermediate directories for sub-path fixtures. */
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

static void ei_cleanup(EILangProj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Index `files`, check IMPORTS count >= `floor`.  Dumps a diagnostic on
 * failure so failures are self-diagnosable without re-running manually. */
static int ei_edge_present(const EILangFile *files, int nfiles, const char *edge_type, int floor) {
    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge_type) : -1;
    if (got < floor) {
        fprintf(stderr, "  [%s] FAIL count=%d expected>=%d\n", edge_type, got, floor);
    }
    ei_cleanup(&lp, store);
    return got >= floor;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GREEN GUARD — Python
 *
 * Python `from .mod import x` and `import mod` already resolve to IMPORTS
 * edges via the relative-import resolver.  These tests MUST stay GREEN.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Python: `from .util import helper` — canonical relative import. */
TEST(ei_python_relative_from_import) {
    static const EILangFile f[] = {
        {"util.py",  "def helper(x):\n    return x + 1\n"},
        {"main.py",  "from .util import helper\n\ndef run(y):\n    return helper(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: bare `import util` (absolute, same directory). */
TEST(ei_python_absolute_import) {
    static const EILangFile f[] = {
        {"util.py",  "def compute(x):\n    return x * 2\n"},
        {"main.py",  "import util\n\ndef run(y):\n    return util.compute(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: `from util import compute` — named absolute import. */
TEST(ei_python_from_absolute_import) {
    static const EILangFile f[] = {
        {"util.py",  "def compute(x):\n    return x * 2\n"},
        {"main.py",  "from util import compute\n\ndef run(y):\n    return compute(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: multiple names in one `from` statement. */
TEST(ei_python_from_multi_names) {
    static const EILangFile f[] = {
        {"ops.py",   "def add(a, b):\n    return a + b\n\ndef mul(a, b):\n    return a * b\n"},
        {"client.py","from ops import add, mul\n\ndef run(x, y):\n    return add(x, mul(x, y))\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: aliased import `import util as u`. */
TEST(ei_python_aliased_import) {
    static const EILangFile f[] = {
        {"util.py",  "def helper(x):\n    return x + 1\n"},
        {"main.py",  "import util as u\n\ndef run(y):\n    return u.helper(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: sub-package path `from pkg.util import fn`. */
TEST(ei_python_subpackage_import) {
    static const EILangFile f[] = {
        {"pkg/__init__.py", ""},
        {"pkg/util.py",     "def fn(x):\n    return x\n"},
        {"main.py",         "from pkg.util import fn\n\ndef run(y):\n    return fn(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Python: wildcard `from util import *`. */
TEST(ei_python_wildcard_import) {
    static const EILangFile f[] = {
        {"util.py",  "X = 42\n\ndef helper():\n    return X\n"},
        {"main.py",  "from util import *\n\ndef run():\n    return helper()\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: sibling relative `from .sibling import x` in a package. */
TEST(ei_python_package_sibling_import) {
    static const EILangFile f[] = {
        {"pkg/__init__.py", ""},
        {"pkg/a.py",        "def alpha():\n    return 1\n"},
        {"pkg/b.py",        "from .a import alpha\n\ndef beta():\n    return alpha() + 1\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GREEN GUARD — TypeScript
 *
 * TypeScript `import { x } from './mod'` already resolves via the relative-
 * import resolver.  These tests MUST stay GREEN.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* TypeScript: named relative import — the canonical GREEN guard. */
TEST(ei_typescript_named_relative_import) {
    static const EILangFile f[] = {
        {"util.ts",  "export function helper(x: number): number { return x + 1; }\n"},
        {"main.ts",  "import { helper } from './util';\n\n"
                     "export function run(y: number): number { return helper(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: default import `import helper from './util'`. */
TEST(ei_typescript_default_import) {
    static const EILangFile f[] = {
        {"util.ts",  "export default function helper(x: number): number { return x + 1; }\n"},
        {"main.ts",  "import helper from './util';\n\n"
                     "export function run(y: number): number { return helper(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: namespace import `import * as util from './util'`. */
TEST(ei_typescript_namespace_import) {
    static const EILangFile f[] = {
        {"util.ts",  "export const VALUE = 42;\n"
                     "export function compute(x: number): number { return x * VALUE; }\n"},
        {"main.ts",  "import * as util from './util';\n\n"
                     "export function run(): number { return util.compute(util.VALUE); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: aliased named import `import { helper as h } from './util'`. */
TEST(ei_typescript_aliased_import) {
    static const EILangFile f[] = {
        {"util.ts",  "export function helper(x: number): number { return x + 1; }\n"},
        {"main.ts",  "import { helper as h } from './util';\n\n"
                     "export function run(y: number): number { return h(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: multi-name import `import { a, b } from './ops'`. */
TEST(ei_typescript_multi_names_import) {
    static const EILangFile f[] = {
        {"ops.ts",   "export function add(a: number, b: number): number { return a + b; }\n"
                     "export function mul(a: number, b: number): number { return a * b; }\n"},
        {"client.ts","import { add, mul } from './ops';\n\n"
                     "export function run(x: number, y: number): number { return add(x, mul(x, y)); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: subdirectory import `import { fn } from './pkg/util'`. */
TEST(ei_typescript_subdir_import) {
    static const EILangFile f[] = {
        {"pkg/util.ts", "export function fn(x: number): number { return x; }\n"},
        {"main.ts",     "import { fn } from './pkg/util';\n\n"
                        "export function run(y: number): number { return fn(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: re-export `export { fn } from './util'`. */
TEST(ei_typescript_re_export) {
    static const EILangFile f[] = {
        {"util.ts",  "export function fn(x: number): number { return x; }\n"},
        {"index.ts", "export { fn } from './util';\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: `import type { T } from './types'` (type-only import). */
TEST(ei_typescript_type_import) {
    static const EILangFile f[] = {
        {"types.ts", "export interface Config { value: number; }\n"},
        {"main.ts",  "import type { Config } from './types';\n\n"
                     "export function run(c: Config): number { return c.value; }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * GREEN GUARD — Go
 *
 * Go `import "mod/pkg"` resolves via the Go module resolver.  These MUST
 * stay GREEN.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Go: simple same-module cross-package import. */
TEST(ei_go_same_module_import) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/demo\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go",      "package main\n\nimport \"example.com/demo/util\"\n\n"
                         "func main() { _ = util.Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: grouped import block `import ( "pkg1"; "pkg2" )`. */
TEST(ei_go_grouped_import_block) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/grp\n\ngo 1.21\n"},
        {"math/math.go", "package math\n\nfunc Add(a, b int) int { return a + b }\n"},
        {"strutil/str.go","package strutil\n\nfunc Join(a, b string) string { return a + b }\n"},
        {"main.go",      "package main\n\nimport (\n"
                         "\t\"example.com/grp/math\"\n"
                         "\t\"example.com/grp/strutil\"\n)\n\n"
                         "func main() {\n"
                         "\t_ = math.Add(1, 2)\n"
                         "\t_ = strutil.Join(\"a\", \"b\")\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 4, "IMPORTS", 1));
    PASS();
}

/* Go: aliased import `import util "example.com/demo/util"`. */
TEST(ei_go_aliased_import) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/alias\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go",      "package main\n\nimport u \"example.com/alias/util\"\n\n"
                         "func main() { _ = u.Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: dot import `import . "example.com/demo/util"` (members into current ns). */
TEST(ei_go_dot_import) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/dot\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go",      "package main\n\nimport . \"example.com/dot/util\"\n\n"
                         "func main() { _ = Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: sub-package path import (multi-level directory). */
TEST(ei_go_subpackage_import) {
    static const EILangFile f[] = {
        {"go.mod",               "module example.com/sub\n\ngo 1.21\n"},
        {"pkg/math/ops.go",      "package math\n\nfunc Mul(a, b int) int { return a * b }\n"},
        {"main.go",              "package main\n\nimport \"example.com/sub/pkg/math\"\n\n"
                                 "func main() { _ = math.Mul(2, 3) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: blank import `import _ "example.com/demo/util"` (side-effects only). */
TEST(ei_go_blank_import) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/blank\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc init() {}\n"},
        {"main.go",      "package main\n\nimport _ \"example.com/blank/util\"\n\nfunc main() {}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: two files importing the same internal package (both should yield edges). */
TEST(ei_go_two_consumers_same_package) {
    static const EILangFile f[] = {
        {"go.mod",       "module example.com/two\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"a/a.go",       "package a\n\nimport \"example.com/two/util\"\n\n"
                         "func Run() int { return util.Helper(1) }\n"},
        {"b/b.go",       "package b\n\nimport \"example.com/two/util\"\n\n"
                         "func Run() int { return util.Helper(2) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 4, "IMPORTS", 2));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * RED REPRODUCTION — Rust
 *
 * The pipeline does NOT create IMPORTS graph edges for Rust `use` declarations
 * even though extraction captures them.  Each test below should FAIL (count=0)
 * until the edge-creation pipeline is fixed.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Rust: `mod a;` file inclusion + `use crate::a::f` cross-module use. */
TEST(ei_rust_mod_plus_use) {
    static const EILangFile f[] = {
        {"a.rs",    "pub fn f(x: i32) -> i32 { x + 1 }\n"},
        {"main.rs", "mod a;\nuse crate::a::f;\n\nfn main() { let _ = f(1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: `use crate::util::helper` — top-level function in a sibling module. */
TEST(ei_rust_use_crate_path) {
    static const EILangFile f[] = {
        {"util.rs", "pub fn helper(x: i32) -> i32 { x * 2 }\n"},
        {"main.rs", "mod util;\nuse crate::util::helper;\n\nfn main() { let _ = helper(3); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: grouped use `use crate::ops::{add, mul}`. */
TEST(ei_rust_grouped_use) {
    static const EILangFile f[] = {
        {"ops.rs",  "pub fn add(a: i32, b: i32) -> i32 { a + b }\n"
                    "pub fn mul(a: i32, b: i32) -> i32 { a * b }\n"},
        {"main.rs", "mod ops;\nuse crate::ops::{add, mul};\n\n"
                    "fn main() { let _ = add(mul(2, 3), 1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: aliased use `use crate::util::helper as h`. */
TEST(ei_rust_aliased_use) {
    static const EILangFile f[] = {
        {"util.rs", "pub fn helper(x: i32) -> i32 { x + 1 }\n"},
        {"main.rs", "mod util;\nuse crate::util::helper as h;\n\nfn run() -> i32 { h(5) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: pub re-export `pub use crate::util::helper`. */
TEST(ei_rust_pub_re_export) {
    static const EILangFile f[] = {
        {"util.rs", "pub fn helper(x: i32) -> i32 { x + 1 }\n"},
        {"lib.rs",  "mod util;\npub use crate::util::helper;\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: struct import `use crate::models::Config`. */
TEST(ei_rust_struct_use) {
    static const EILangFile f[] = {
        {"models.rs", "pub struct Config { pub value: i32 }\n"},
        {"main.rs",   "mod models;\nuse crate::models::Config;\n\n"
                      "fn make() -> Config { Config { value: 1 } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: glob use `use crate::ops::*`. */
TEST(ei_rust_glob_use) {
    static const EILangFile f[] = {
        {"ops.rs",  "pub fn add(a: i32, b: i32) -> i32 { a + b }\n"
                    "pub fn sub(a: i32, b: i32) -> i32 { a - b }\n"},
        {"main.rs", "mod ops;\nuse crate::ops::*;\n\nfn run() -> i32 { add(sub(5, 1), 2) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: trait import `use crate::traits::Compute`. */
TEST(ei_rust_trait_use) {
    static const EILangFile f[] = {
        {"traits.rs", "pub trait Compute { fn run(&self) -> i32; }\n"},
        {"main.rs",   "mod traits;\nuse crate::traits::Compute;\n\n"
                      "struct Impl;\nimpl Compute for Impl { fn run(&self) -> i32 { 42 } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * RED REPRODUCTION — Kotlin
 *
 * The pipeline does NOT create IMPORTS graph edges for Kotlin `import`
 * statements even though extraction captures them.  Expected RED.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Kotlin: `import com.example.Util` — basic cross-file class import. */
TEST(ei_kotlin_basic_class_import) {
    static const EILangFile f[] = {
        {"Util.kt",  "package com.example\n\nclass Util {\n    fun greet() = \"hello\"\n}\n"},
        {"Main.kt",  "package com.example\n\nimport com.example.Util\n\n"
                     "fun main() { val u = Util(); println(u.greet()) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: `import com.example.fn` — top-level function import. */
TEST(ei_kotlin_toplevel_function_import) {
    static const EILangFile f[] = {
        {"ops.kt",  "package com.example\n\nfun add(a: Int, b: Int): Int = a + b\n"},
        {"main.kt", "package com.example\n\nimport com.example.add\n\n"
                    "fun run(): Int = add(1, 2)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: aliased import `import com.example.Util as U`. */
TEST(ei_kotlin_aliased_import) {
    static const EILangFile f[] = {
        {"Util.kt",  "package com.example\n\nclass Util {\n    fun compute(x: Int) = x + 1\n}\n"},
        {"Main.kt",  "package com.example\n\nimport com.example.Util as U\n\n"
                     "fun run(): Int { val u = U(); return u.compute(5) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: wildcard import `import com.example.*`. */
TEST(ei_kotlin_wildcard_import) {
    static const EILangFile f[] = {
        {"ops.kt",  "package com.example\n\nfun add(a: Int, b: Int) = a + b\n"
                    "fun mul(a: Int, b: Int) = a * b\n"},
        {"main.kt", "package com.example\n\nimport com.example.*\n\n"
                    "fun run() = add(1, mul(2, 3))\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: multiple imports in one file. */
TEST(ei_kotlin_multiple_imports) {
    static const EILangFile f[] = {
        {"A.kt",    "package com.x\n\nclass A { fun a() = 1 }\n"},
        {"B.kt",    "package com.x\n\nclass B { fun b() = 2 }\n"},
        {"Main.kt", "package com.x\n\nimport com.x.A\nimport com.x.B\n\n"
                    "fun run(): Int { return A().a() + B().b() }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Kotlin: object/companion import `import com.example.Config.DEFAULT`. */
TEST(ei_kotlin_object_member_import) {
    static const EILangFile f[] = {
        {"Config.kt", "package com.example\n\nobject Config {\n    const val DEFAULT = 42\n}\n"},
        {"Main.kt",   "package com.example\n\nimport com.example.Config.DEFAULT\n\n"
                      "fun run() = DEFAULT\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: data class import across packages. */
TEST(ei_kotlin_data_class_import) {
    static const EILangFile f[] = {
        {"model/User.kt",  "package com.example.model\n\ndata class User(val name: String)\n"},
        {"service/Svc.kt", "package com.example.service\n\nimport com.example.model.User\n\n"
                           "fun greet(u: User) = \"Hello \" + u.name\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * RED REPRODUCTION — Java
 *
 * The pipeline does NOT create IMPORTS graph edges for Java `import`
 * statements.  Expected RED.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Java: `import com.example.Util` — basic class import. */
TEST(ei_java_basic_class_import) {
    static const EILangFile f[] = {
        {"Util.java",  "package com.example;\npublic class Util {\n"
                       "    public int compute(int x) { return x + 1; }\n}\n"},
        {"Main.java",  "package com.example;\nimport com.example.Util;\n"
                       "public class Main {\n"
                       "    public static void main(String[] args) {\n"
                       "        Util u = new Util();\n        System.out.println(u.compute(1));\n"
                       "    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: `import com.example.util.MathOps` — utility class in sub-package. */
TEST(ei_java_subpackage_import) {
    static const EILangFile f[] = {
        {"util/MathOps.java", "package com.example.util;\n"
                              "public class MathOps {\n"
                              "    public static int add(int a, int b) { return a + b; }\n}\n"},
        {"Main.java",         "package com.example;\nimport com.example.util.MathOps;\n"
                              "public class Main {\n"
                              "    void run() { int x = MathOps.add(1, 2); }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: wildcard import `import com.example.util.*`. */
TEST(ei_java_wildcard_import) {
    static const EILangFile f[] = {
        {"util/Ops.java",  "package com.example.util;\n"
                           "public class Ops { public static int add(int a,int b){return a+b;} }\n"},
        {"Main.java",      "package com.example;\nimport com.example.util.*;\n"
                           "public class Main { void run() { int x = Ops.add(1, 2); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: static import `import static com.example.MathOps.add`. */
TEST(ei_java_static_import) {
    static const EILangFile f[] = {
        {"MathOps.java", "package com.example;\n"
                         "public class MathOps { public static int add(int a,int b){return a+b;} }\n"},
        {"Main.java",    "package com.example;\nimport static com.example.MathOps.add;\n"
                         "public class Main { void run() { int x = add(1, 2); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: multiple imports in one file. */
TEST(ei_java_multiple_imports) {
    static const EILangFile f[] = {
        {"A.java",    "package com.x;\npublic class A { public int a() { return 1; } }\n"},
        {"B.java",    "package com.x;\npublic class B { public int b() { return 2; } }\n"},
        {"Main.java", "package com.x;\nimport com.x.A;\nimport com.x.B;\n"
                      "public class Main { void run() { new A().a(); new B().b(); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Java: interface import across files. */
TEST(ei_java_interface_import) {
    static const EILangFile f[] = {
        {"Compute.java", "package com.example;\npublic interface Compute { int run(int x); }\n"},
        {"Impl.java",    "package com.example;\nimport com.example.Compute;\n"
                         "public class Impl implements Compute {\n"
                         "    public int run(int x) { return x + 1; }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: static wildcard import `import static com.example.Constants.*`. */
TEST(ei_java_static_wildcard_import) {
    static const EILangFile f[] = {
        {"Constants.java", "package com.example;\n"
                           "public class Constants { public static final int MAX = 100; }\n"},
        {"Main.java",      "package com.example;\nimport static com.example.Constants.*;\n"
                           "public class Main { void check() { int x = MAX; } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * RED REPRODUCTION — C#
 *
 * The pipeline does NOT create IMPORTS graph edges for C# `using` directives.
 * Expected RED.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* C#: `using App.Utils` — basic namespace import. */
TEST(ei_csharp_basic_using) {
    static const EILangFile f[] = {
        {"Utils.cs", "namespace App.Utils {\n"
                     "    public class Helper {\n"
                     "        public int Compute(int x) { return x + 1; }\n    }\n}\n"},
        {"Main.cs",  "using App.Utils;\nnamespace App {\n"
                     "    class Main {\n"
                     "        void Run() { var h = new Helper(); _ = h.Compute(1); }\n    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: aliased using `using H = App.Utils.Helper`. */
TEST(ei_csharp_aliased_using) {
    static const EILangFile f[] = {
        {"Utils.cs", "namespace App.Utils {\n"
                     "    public class Helper { public int Compute(int x) { return x + 1; } }\n}\n"},
        {"Main.cs",  "using H = App.Utils.Helper;\nnamespace App {\n"
                     "    class Main { void Run() { var h = new H(); } }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: `using static App.MathOps` — static member access. */
TEST(ei_csharp_using_static) {
    static const EILangFile f[] = {
        {"MathOps.cs", "namespace App {\n"
                       "    public static class MathOps {\n"
                       "        public static int Add(int a, int b) { return a + b; }\n    }\n}\n"},
        {"Main.cs",    "using static App.MathOps;\nnamespace App {\n"
                       "    class Main { void Run() { int x = Add(1, 2); } }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: multiple using directives in one file. */
TEST(ei_csharp_multiple_usings) {
    static const EILangFile f[] = {
        {"A.cs",    "namespace Com.X { public class A { public int a() { return 1; } } }\n"},
        {"B.cs",    "namespace Com.X { public class B { public int b() { return 2; } } }\n"},
        {"Main.cs", "using Com.X;\nnamespace Com.X {\n"
                    "    class Main { void Run() { new A().a(); new B().b(); } }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* C#: interface in a separate namespace imported via using. */
TEST(ei_csharp_interface_using) {
    static const EILangFile f[] = {
        {"Interfaces.cs", "namespace App.Contracts {\n"
                          "    public interface ICompute { int Run(int x); }\n}\n"},
        {"Impl.cs",       "using App.Contracts;\nnamespace App {\n"
                          "    public class Impl : ICompute {\n"
                          "        public int Run(int x) { return x + 1; }\n    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: sub-namespace import `using App.Models.Domain`. */
TEST(ei_csharp_subnamespace_using) {
    static const EILangFile f[] = {
        {"models/User.cs",  "namespace App.Models.Domain {\n"
                            "    public class User { public string Name { get; set; } }\n}\n"},
        {"service/Svc.cs",  "using App.Models.Domain;\nnamespace App.Service {\n"
                            "    public class UserService {\n"
                            "        public string Greet(User u) { return \"Hello \" + u.Name; }\n"
                            "    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: file-scoped namespace + using (C# 10 style). */
TEST(ei_csharp_file_scoped_namespace) {
    static const EILangFile f[] = {
        {"Ops.cs",  "namespace App.Ops;\npublic static class Ops {\n"
                    "    public static int Add(int a, int b) => a + b;\n}\n"},
        {"Main.cs", "using App.Ops;\nnamespace App.Main;\npublic class Main {\n"
                    "    public void Run() { int x = Ops.Add(1, 2); }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * RED REPRODUCTION — PHP
 *
 * The pipeline does NOT create IMPORTS graph edges for PHP `use` statements.
 * Expected RED.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* PHP: `use App\Utils\Helper` — basic namespace import. */
TEST(ei_php_basic_use) {
    static const EILangFile f[] = {
        {"Utils/Helper.php", "<?php\nnamespace App\\Utils;\nclass Helper {\n"
                             "    public function compute(int $x): int { return $x + 1; }\n}\n"},
        {"main.php",         "<?php\nuse App\\Utils\\Helper;\n"
                             "function run(): int { $h = new Helper(); return $h->compute(1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: aliased use `use App\Utils\Helper as H`. */
TEST(ei_php_aliased_use) {
    static const EILangFile f[] = {
        {"Utils/Helper.php", "<?php\nnamespace App\\Utils;\nclass Helper {\n"
                             "    public function compute(int $x): int { return $x + 1; }\n}\n"},
        {"main.php",         "<?php\nuse App\\Utils\\Helper as H;\n"
                             "function run(): int { $h = new H(); return $h->compute(1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: grouped use `use App\Utils\{A, B}`. */
TEST(ei_php_grouped_use) {
    static const EILangFile f[] = {
        {"Utils/A.php", "<?php\nnamespace App\\Utils;\nclass A { public function a(): int { return 1; } }\n"},
        {"Utils/B.php", "<?php\nnamespace App\\Utils;\nclass B { public function b(): int { return 2; } }\n"},
        {"main.php",    "<?php\nuse App\\Utils\\{A, B};\n"
                        "function run(): int { $a = new A(); $b = new B(); return $a->a() + $b->b(); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* PHP: `use function App\Utils\compute` — function import. */
TEST(ei_php_function_use) {
    static const EILangFile f[] = {
        {"Utils/funcs.php", "<?php\nnamespace App\\Utils;\nfunction compute(int $x): int { return $x * 2; }\n"},
        {"main.php",        "<?php\nuse function App\\Utils\\compute;\n"
                            "function run(): int { return compute(5); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: `use const App\Utils\MAX_VALUE` — constant import. */
TEST(ei_php_const_use) {
    static const EILangFile f[] = {
        {"Utils/consts.php", "<?php\nnamespace App\\Utils;\nconst MAX_VALUE = 100;\n"},
        {"main.php",         "<?php\nuse const App\\Utils\\MAX_VALUE;\n"
                             "function check(int $x): bool { return $x < MAX_VALUE; }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: multiple use statements in one file. */
TEST(ei_php_multiple_use_statements) {
    static const EILangFile f[] = {
        {"A.php",    "<?php\nnamespace Com\\X;\nclass A { public function a(): int { return 1; } }\n"},
        {"B.php",    "<?php\nnamespace Com\\X;\nclass B { public function b(): int { return 2; } }\n"},
        {"main.php", "<?php\nuse Com\\X\\A;\nuse Com\\X\\B;\n"
                     "function run(): int { $a = new A(); $b = new B(); return $a->a() + $b->b(); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* PHP: interface use across files. */
TEST(ei_php_interface_use) {
    static const EILangFile f[] = {
        {"Contracts/Computable.php", "<?php\nnamespace App\\Contracts;\n"
                                     "interface Computable { public function run(int $x): int; }\n"},
        {"Impl.php",                 "<?php\nuse App\\Contracts\\Computable;\n"
                                     "class Impl implements Computable {\n"
                                     "    public function run(int $x): int { return $x + 1; }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SUITE registration
 * ═══════════════════════════════════════════════════════════════════════════ */

SUITE(edge_imports) {
    /* ── GREEN GUARDS — Python (must stay passing) ── */
    RUN_TEST(ei_python_relative_from_import);
    RUN_TEST(ei_python_absolute_import);
    RUN_TEST(ei_python_from_absolute_import);
    RUN_TEST(ei_python_from_multi_names);
    RUN_TEST(ei_python_aliased_import);
    RUN_TEST(ei_python_subpackage_import);
    RUN_TEST(ei_python_wildcard_import);
    RUN_TEST(ei_python_package_sibling_import);

    /* ── GREEN GUARDS — TypeScript (must stay passing) ── */
    RUN_TEST(ei_typescript_named_relative_import);
    RUN_TEST(ei_typescript_default_import);
    RUN_TEST(ei_typescript_namespace_import);
    RUN_TEST(ei_typescript_aliased_import);
    RUN_TEST(ei_typescript_multi_names_import);
    RUN_TEST(ei_typescript_subdir_import);
    RUN_TEST(ei_typescript_re_export);
    RUN_TEST(ei_typescript_type_import);

    /* ── GREEN GUARDS — Go (must stay passing) ── */
    RUN_TEST(ei_go_same_module_import);
    RUN_TEST(ei_go_grouped_import_block);
    RUN_TEST(ei_go_aliased_import);
    RUN_TEST(ei_go_dot_import);
    RUN_TEST(ei_go_subpackage_import);
    RUN_TEST(ei_go_blank_import);
    RUN_TEST(ei_go_two_consumers_same_package);

    /* ── RED REPRODUCTIONS — Rust (expected to FAIL until pipeline fixed) ── */
    RUN_TEST(ei_rust_mod_plus_use);
    RUN_TEST(ei_rust_use_crate_path);
    RUN_TEST(ei_rust_grouped_use);
    RUN_TEST(ei_rust_aliased_use);
    RUN_TEST(ei_rust_pub_re_export);
    RUN_TEST(ei_rust_struct_use);
    RUN_TEST(ei_rust_glob_use);
    RUN_TEST(ei_rust_trait_use);

    /* ── RED REPRODUCTIONS — Kotlin (expected to FAIL until pipeline fixed) ── */
    RUN_TEST(ei_kotlin_basic_class_import);
    RUN_TEST(ei_kotlin_toplevel_function_import);
    RUN_TEST(ei_kotlin_aliased_import);
    RUN_TEST(ei_kotlin_wildcard_import);
    RUN_TEST(ei_kotlin_multiple_imports);
    RUN_TEST(ei_kotlin_object_member_import);
    RUN_TEST(ei_kotlin_data_class_import);

    /* ── RED REPRODUCTIONS — Java (expected to FAIL until pipeline fixed) ── */
    RUN_TEST(ei_java_basic_class_import);
    RUN_TEST(ei_java_subpackage_import);
    RUN_TEST(ei_java_wildcard_import);
    RUN_TEST(ei_java_static_import);
    RUN_TEST(ei_java_multiple_imports);
    RUN_TEST(ei_java_interface_import);
    RUN_TEST(ei_java_static_wildcard_import);

    /* ── RED REPRODUCTIONS — C# (expected to FAIL until pipeline fixed) ── */
    RUN_TEST(ei_csharp_basic_using);
    RUN_TEST(ei_csharp_aliased_using);
    RUN_TEST(ei_csharp_using_static);
    RUN_TEST(ei_csharp_multiple_usings);
    RUN_TEST(ei_csharp_interface_using);
    RUN_TEST(ei_csharp_subnamespace_using);
    RUN_TEST(ei_csharp_file_scoped_namespace);

    /* ── RED REPRODUCTIONS — PHP (expected to FAIL until pipeline fixed) ── */
    RUN_TEST(ei_php_basic_use);
    RUN_TEST(ei_php_aliased_use);
    RUN_TEST(ei_php_grouped_use);
    RUN_TEST(ei_php_function_use);
    RUN_TEST(ei_php_const_use);
    RUN_TEST(ei_php_multiple_use_statements);
    RUN_TEST(ei_php_interface_use);
}
