/*
 * test_grammar_probe_c.c — Grammar-only node / import / inheritance probe.
 *
 * Covers 11 grammar-only code languages:
 *   scheme, slang, solidity, squirrel, starlark, sway, tcl, teal,
 *   vimscript, wgsl, zsh
 *
 * Scope: NODE creation + IMPORTS + INHERITANCE (where OOP applies).
 * CALLS edges are already covered by the P5 contract_calls_breadth suite and
 * are NOT re-probed here.
 *
 * Convention:
 *   green = guard (must stay green; a regression turns it red)
 *   red   = known bug (stays red until fixed; brief comment names the root cause)
 *
 * This file is NOT registered in test_main.c — it is a standalone probe that
 * can be run via SUITE(grammar_probe_c) once linked.
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
#if !defined(_WIN32)
#include <sys/wait.h>
#endif

/* ── Harness (mirrors test_lang_contract.c) ─────────────────────────────── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GP_Proj;

typedef struct {
    const char *name;
    const char *content;
} GP_File;

static void gp_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *gp_open_indexed(GP_Proj *lp) {
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

static cbm_store_t *gp_index_files(GP_Proj *lp, const GP_File *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpc_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    gp_to_fwd_slashes(lp->tmpdir);
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
    return gp_open_indexed(lp);
}

static cbm_store_t *gp_index(GP_Proj *lp, const char *filename, const char *content) {
    GP_File f = {filename, content};
    return gp_index_files(lp, &f, 1);
}

static void gp_cleanup(GP_Proj *lp, cbm_store_t *store) {
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

/* Count graph nodes by label. Returns -1 on query error. */
static int gp_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Count type-like nodes (Class/Struct/Interface/Enum/Trait/Type). */
static int gp_type_like(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = gp_count_label(store, project, labels[i]);
        if (n > 0) total += n;
    }
    return total;
}

/* Extraction-level: count imports via cbm_extract_file (bypasses graph). */
static int gp_extract_imports(const char *src, CBMLanguage lang, const char *path) {
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), lang, "gpc", path, 0, NULL, NULL);
    if (!r) return -1;
    int n = r->imports.count;
    cbm_free_result(r);
    return n;
}

/* Extraction-level: count defs with given label. */
static int gp_extract_label(const char *src, CBMLanguage lang, const char *path,
                             const char *label) {
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), lang, "gpc", path, 0, NULL, NULL);
    if (!r) return -1;
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].label, label) == 0) n++;
    }
    cbm_free_result(r);
    return n;
}

/* Extraction-level: check any def has at least one non-NULL base_class. */
static int gp_extract_has_base_class(const char *src, CBMLanguage lang, const char *path) {
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), lang, "gpc", path, 0, NULL, NULL);
    if (!r) return 0;
    int found = 0;
    for (int i = 0; i < r->defs.count && !found; i++) {
        const char **bc = r->defs.items[i].base_classes;
        if (bc && bc[0]) found = 1;
    }
    cbm_free_result(r);
    return found;
}

/* ══════════════════════════════════════════════════════════════════
 *  SCHEME (.scm) — functional, no OOP, no import extraction support
 * ══════════════════════════════════════════════════════════════════ */

/* Scheme: two (define ...) forms → two Function nodes in the graph. */
TEST(scheme_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "a.scm",
        "(define (compute x)\n"
        "  (* x x))\n"
        "\n"
        "(define (display-result n)\n"
        "  (display n)\n"
        "  (newline))\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: both defines become Function nodes */
    PASS();
}

/* Scheme: (import ...) / (require ...) — no import_types configured in lang_specs;
 * extraction yields 0 imports. This is a known gap (missing scheme branch). */
TEST(scheme_import_not_extracted) {
    /* red: scheme_import_types = empty_types in lang_specs; import forms silently dropped */
    int n = gp_extract_imports(
        "(import (rnrs io ports))\n"
        "(import (srfi srfi-1))\n"
        "\n"
        "(define (run) (display \"hi\"))\n",
        CBM_LANG_SCHEME, "a.scm");
    ASSERT_TRUE(n >= 1); /* CURRENTLY FAILS: scheme has no import_types; n == 0 */
    PASS();
}

/* Scheme: (require ...) form (Racket-style; also common in R7RS implementations). */
TEST(scheme_require_not_extracted) {
    /* red: same root cause — scheme import_types = empty_types */
    int n = gp_extract_imports(
        "(require 'srfi/1)\n"
        "(require 'json)\n"
        "\n"
        "(define (run) (display 1))\n",
        CBM_LANG_SCHEME, "a.scm");
    ASSERT_TRUE(n >= 1); /* CURRENTLY FAILS: n == 0 */
    PASS();
}

/* Scheme: Module node always emitted as the file wrapper. */
TEST(scheme_module_node) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "calc.scm",
        "(define (add a b) (+ a b))\n");
    int mods = store ? gp_count_label(store, lp.project, "Module") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(mods >= 1); /* green: Module wrapper always created */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SLANG (.slang) — GPU shader language (HLSL-derivative)
 * ══════════════════════════════════════════════════════════════════ */

/* Slang: plain functions become Function nodes. */
TEST(slang_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "a.slang",
        "float helper(float x)\n"
        "{\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "float compute(float v)\n"
        "{\n"
        "    return helper(v) + 1.0;\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: both function_definitions become Function nodes */
    PASS();
}

/* Slang: struct → Class node (slang_class_types includes class_specifier). */
TEST(slang_struct_node) {
    int cls = gp_extract_label(
        "struct Vertex {\n"
        "    float3 position;\n"
        "    float2 uv;\n"
        "};\n"
        "\n"
        "struct Material {\n"
        "    float4 color;\n"
        "};\n",
        CBM_LANG_SLANG, "types.slang", "Class");
    /* green if class_specifier is covered — red if struct maps to an uncovered node kind */
    ASSERT_TRUE(cls >= 1); /* EXPECTED GREEN: slang_class_types includes class_specifier */
    PASS();
}

/* Slang: shader entry point annotation ([shader("vertex")]) — the annotated
 * function is still a function_definition; should produce a Function node. */
TEST(slang_shader_entry_point) {
    int fns = gp_extract_label(
        "[shader(\"vertex\")]\n"
        "VertexOutput vertMain(VertexInput input)\n"
        "{\n"
        "    VertexOutput o;\n"
        "    o.position = float4(input.position, 1.0);\n"
        "    return o;\n"
        "}\n"
        "\n"
        "[shader(\"fragment\")]\n"
        "float4 fragMain(VertexOutput input) : SV_Target\n"
        "{\n"
        "    return float4(1.0, 0.0, 0.0, 1.0);\n"
        "}\n",
        CBM_LANG_SLANG, "shader.slang", "Function");
    ASSERT_TRUE(fns >= 2); /* green: annotated functions are still function_definitions */
    PASS();
}

/* Slang: #include → extracted import (slang_import_types includes preproc_include). */
TEST(slang_include_import) {
    int n = gp_extract_imports(
        "#include \"math_utils.slang\"\n"
        "#include \"common.slang\"\n"
        "\n"
        "float run(float x) { return x; }\n",
        CBM_LANG_SLANG, "main.slang");
    /* REAL BUG (class 2): SLANG absent from cbm_extract_imports() dispatch switch
     * in extract_imports.c — slang_import_types is configured but never consumed;
     * imports always 0. Fix: add CBM_LANG_SLANG case to the dispatch. */
    ASSERT_TRUE(n >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SOLIDITY (.sol) — EVM smart contract language
 * ══════════════════════════════════════════════════════════════════ */

/* Solidity: contract → Class node (solidity_class_types = contract_declaration). */
TEST(solidity_contract_node) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "token.sol",
        "// SPDX-License-Identifier: MIT\n"
        "pragma solidity ^0.8.0;\n"
        "\n"
        "contract Token {\n"
        "    mapping(address => uint256) public balances;\n"
        "\n"
        "    function mint(address to, uint256 amount) public {\n"
        "        balances[to] += amount;\n"
        "    }\n"
        "}\n");
    int cls = store ? gp_type_like(store, lp.project) : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(cls >= 1); /* green: contract_declaration → Class node */
    PASS();
}

/* Solidity: interface → Interface node. */
TEST(solidity_interface_node) {
    int cls = gp_extract_label(
        "// SPDX-License-Identifier: MIT\n"
        "pragma solidity ^0.8.0;\n"
        "\n"
        "interface IERC20 {\n"
        "    function totalSupply() external view returns (uint256);\n"
        "    function transfer(address to, uint256 amount) external returns (bool);\n"
        "}\n",
        CBM_LANG_SOLIDITY, "IERC20.sol", "Interface");
    /* green: interface_declaration in solidity_class_types maps to Interface label */
    ASSERT_TRUE(cls >= 1);
    PASS();
}

/* Solidity: contract methods → Method nodes inside the contract Class. */
TEST(solidity_method_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "vault.sol",
        "// SPDX-License-Identifier: MIT\n"
        "pragma solidity ^0.8.0;\n"
        "\n"
        "contract Vault {\n"
        "    uint256 private balance;\n"
        "\n"
        "    function deposit(uint256 amount) public {\n"
        "        balance += amount;\n"
        "    }\n"
        "\n"
        "    function withdraw(uint256 amount) public {\n"
        "        require(balance >= amount);\n"
        "        balance -= amount;\n"
        "    }\n"
        "}\n");
    int methods = store ? gp_count_label(store, lp.project, "Method") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(methods >= 2); /* green: contract functions with parent class → Method */
    PASS();
}

/* Solidity: import directive extracted at extraction level. */
TEST(solidity_import_extracted) {
    int n = gp_extract_imports(
        "// SPDX-License-Identifier: MIT\n"
        "pragma solidity ^0.8.0;\n"
        "\n"
        "import \"@openzeppelin/contracts/token/ERC20/ERC20.sol\";\n"
        "import {Ownable} from \"@openzeppelin/contracts/access/Ownable.sol\";\n"
        "\n"
        "contract MyToken {\n"
        "}\n",
        CBM_LANG_SOLIDITY, "a.sol");
    /* REAL BUG (class 2): SOLIDITY absent from cbm_extract_imports() dispatch in
     * extract_imports.c — solidity_import_types configured but never consumed. */
    ASSERT_TRUE(n >= 2);
    PASS();
}

/* Solidity: contract inheritance (`is` clause) → base_classes populated.
 * The Solidity grammar uses "inheritance_specifier" child nodes under
 * contract_declaration, which the fallback in extract_base_classes covers. */
TEST(solidity_contract_inheritance) {
    int has_base = gp_extract_has_base_class(
        "// SPDX-License-Identifier: MIT\n"
        "pragma solidity ^0.8.0;\n"
        "\n"
        "contract Base {\n"
        "    function baseFunc() public virtual {}\n"
        "}\n"
        "\n"
        "contract Child is Base {\n"
        "    function childFunc() public {}\n"
        "}\n",
        CBM_LANG_SOLIDITY, "inherit.sol");
    /* UNCERTAIN/RED: extract_base_classes looks for "inheritance_specifier" in
     * the fallback list; if tree-sitter-solidity uses a different field name
     * (e.g. "heritage") the base_classes array will be empty → INHERITS edge lost. */
    ASSERT_TRUE(has_base >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SQUIRREL (.nut) — scripting language (game engines, Squirrel VM)
 * ══════════════════════════════════════════════════════════════════ */

/* Squirrel: top-level functions become Function nodes. */
TEST(squirrel_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "util.nut",
        "function greet(name) {\n"
        "    return \"Hello, \" + name\n"
        "}\n"
        "\n"
        "function farewell(name) {\n"
        "    return \"Goodbye, \" + name\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: function_declaration → Function nodes */
    PASS();
}

/* Squirrel: class declaration → Class node.
 * squirrel_class_types includes "class_declaration". */
TEST(squirrel_class_node) {
    int cls = gp_extract_label(
        "class Animal {\n"
        "    name = null\n"
        "    constructor(n) { name = n }\n"
        "    function speak() { return name + \" speaks\" }\n"
        "}\n",
        CBM_LANG_SQUIRREL, "animal.nut", "Class");
    /* REAL BUG (NEW-16, node-extraction incompleteness): squirrel_class_types lists
     * "class_declaration" and the vendored squirrel grammar emits that exact node,
     * yet extract_defs produces 0 Class nodes. Squirrel class defs are not reaching
     * extract_class_def (lang_specs/extract_defs gap for Squirrel). */
    ASSERT_TRUE(cls >= 1);
    PASS();
}

/* Squirrel: class methods inside a class body → Method nodes. */
TEST(squirrel_class_methods) {
    int methods = gp_extract_label(
        "class Calculator {\n"
        "    function add(a, b) { return a + b }\n"
        "    function sub(a, b) { return a - b }\n"
        "}\n",
        CBM_LANG_SQUIRREL, "calc.nut", "Method");
    /* REAL BUG (NEW-16): consequence of squirrel class defs not extracted — with no
     * Class node, in-body functions never get parent_class set, so 0 Method nodes. */
    ASSERT_TRUE(methods >= 2);
    PASS();
}

/* Squirrel: class inheritance via `extends`.
 * The Squirrel grammar may use "base_class" or an inline expression;
 * extract_base_classes' field scan covers "superclass" — may not match. */
TEST(squirrel_class_inheritance) {
    int has_base = gp_extract_has_base_class(
        "class Animal {\n"
        "    function speak() { return \"...\" }\n"
        "}\n"
        "\n"
        "class Dog extends Animal {\n"
        "    function speak() { return \"Woof\" }\n"
        "}\n",
        CBM_LANG_SQUIRREL, "dog.nut");
    /* UNCERTAIN/RED: Squirrel `extends` field name in tree-sitter-squirrel may
     * differ from the field names searched by extract_base_classes; no explicit
     * Squirrel branch exists → base_classes likely empty → INHERITS edge lost. */
    ASSERT_TRUE(has_base >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  STARLARK (.bzl / .star) — Bazel/Buck build language
 * ══════════════════════════════════════════════════════════════════ */

/* Starlark: def statements → Function nodes. */
TEST(starlark_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "rules.bzl",
        "def cc_library_rule(name, srcs, deps = []):\n"
        "    native.cc_library(\n"
        "        name = name,\n"
        "        srcs = srcs,\n"
        "        deps = deps,\n"
        "    )\n"
        "\n"
        "def cc_binary_rule(name, srcs):\n"
        "    native.cc_binary(name = name, srcs = srcs)\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: def_statement → Function nodes */
    PASS();
}

/* Starlark: load() call — starlark_import_types uses "with_clause" which is
 * the Python-style 'with' node, NOT "load_statement". The Starlark grammar
 * uses "load_statement" for load(); "with_clause" is a grammar mismatch. */
TEST(starlark_load_not_extracted) {
    /* red: starlark_import_types = "with_clause" but load() is a "load_statement";
     * grammar type mismatch → imports always 0 for Starlark load() calls. */
    int n = gp_extract_imports(
        "load(\"@rules_cc//cc:defs.bzl\", \"cc_library\")\n"
        "load(\"//tools:build_rules.bzl\", \"my_rule\")\n"
        "\n"
        "def my_target(name):\n"
        "    cc_library(name = name)\n",
        CBM_LANG_STARLARK, "BUILD.bzl");
    ASSERT_TRUE(n >= 2); /* CURRENTLY FAILS: with_clause ≠ load_statement → n == 0 */
    PASS();
}

/* Starlark: .star extension also routes to Starlark grammar. */
TEST(starlark_star_extension_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "macros.star",
        "def format_label(name, pkg = None):\n"
        "    if pkg:\n"
        "        return \"//{}/:{}\".format(pkg, name)\n"
        "    return \":{}\".format(name)\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* green: .star → CBM_LANG_STARLARK → Function node */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SWAY (.sw) — Fuel VM smart contract language (Rust-inspired)
 * ══════════════════════════════════════════════════════════════════ */

/* Sway: fn items → Function nodes. */
TEST(sway_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "contract.sw",
        "fn compute(x: u64) -> u64 {\n"
        "    x * x\n"
        "}\n"
        "\n"
        "fn validate(v: u64) -> bool {\n"
        "    v > 0\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: function_item → Function nodes */
    PASS();
}

/* Sway: struct → type-like node (Struct label). */
TEST(sway_struct_node) {
    int structs = gp_extract_label(
        "struct Point {\n"
        "    x: u64,\n"
        "    y: u64,\n"
        "}\n"
        "\n"
        "struct Rectangle {\n"
        "    origin: Point,\n"
        "    width: u64,\n"
        "    height: u64,\n"
        "}\n",
        CBM_LANG_SWAY, "geom.sw", "Struct");
    /* REAL BUG (NEW-16): sway_class_types lists "struct_item" and the vendored sway
     * grammar emits that node, yet 0 Struct nodes are produced — Sway type defs are
     * not reaching extract_class_def. */
    ASSERT_TRUE(structs >= 2);
    PASS();
}

/* Sway: `use` declaration → extracted import (sway_import_types = use_declaration). */
TEST(sway_use_import_extracted) {
    int n = gp_extract_imports(
        "use fuel_std::address::Address;\n"
        "use fuel_std::token::transfer;\n"
        "\n"
        "fn run() {}\n",
        CBM_LANG_SWAY, "a.sw");
    /* REAL BUG (class 2): SWAY absent from cbm_extract_imports() dispatch in
     * extract_imports.c — sway_import_types configured but never consumed. */
    ASSERT_TRUE(n >= 2);
    PASS();
}

/* Sway: `abi` declaration → type-like node (abi_item in sway_class_types). */
TEST(sway_abi_node) {
    int types = gp_extract_label(
        "abi MyContract {\n"
        "    fn total_supply() -> u64;\n"
        "    fn balance_of(addr: b256) -> u64;\n"
        "}\n",
        CBM_LANG_SWAY, "abi.sw", "Interface");
    /* REAL BUG (NEW-16): abi_item is in sway_class_types and the grammar emits it,
     * but 0 Interface nodes result (Sway type defs not extracted). Note also
     * class_label_for_kind has no "abi_item" case, so even if extracted it would
     * fall to the default "Class" label, not "Interface". */
    ASSERT_TRUE(types >= 1);
    PASS();
}

/* Sway: trait implementation (impl_item in sway_class_types). */
TEST(sway_impl_node) {
    int types = gp_extract_label(
        "struct Counter { value: u64 }\n"
        "\n"
        "impl Counter {\n"
        "    fn new() -> Self { Counter { value: 0 } }\n"
        "    fn increment(ref mut self) { self.value += 1; }\n"
        "}\n",
        CBM_LANG_SWAY, "counter.sw", "Class");
    /* green: impl_item in sway_class_types → Class label */
    ASSERT_TRUE(types >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TCL (.tcl) — Tool Command Language
 * ══════════════════════════════════════════════════════════════════ */

/* Tcl: proc declarations → Function nodes. */
TEST(tcl_proc_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "utils.tcl",
        "proc greet {name} {\n"
        "    return \"Hello, $name\"\n"
        "}\n"
        "\n"
        "proc farewell {name} {\n"
        "    return \"Goodbye, $name\"\n"
        "}\n"
        "\n"
        "proc run {} {\n"
        "    puts [greet \"world\"]\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 3); /* green: proc → Function nodes */
    PASS();
}

/* Tcl: namespace → type-like (tcl_class_types = namespace → Class label). */
TEST(tcl_namespace_node) {
    int cls = gp_extract_label(
        "namespace eval MathUtils {\n"
        "    proc add {a b} { expr {$a + $b} }\n"
        "    proc mul {a b} { expr {$a * $b} }\n"
        "}\n",
        CBM_LANG_TCL, "math.tcl", "Class");
    /* REAL BUG (NEW-16): tcl_class_types lists "namespace" and the grammar emits a
     * "namespace" node, yet 0 Class nodes are produced — Tcl namespace defs are not
     * reaching extract_class_def. */
    ASSERT_TRUE(cls >= 1);
    PASS();
}

/* Tcl: `source` command — tcl import_types = empty_types; no extraction support.
 * A Tcl script can source other files but the extractor ignores these commands. */
TEST(tcl_source_not_extracted) {
    /* red: tcl_import_types = empty_types; "source" and "package require" are
     * plain commands, not tagged with a distinct AST node type. */
    int n = gp_extract_imports(
        "source lib/utils.tcl\n"
        "source lib/config.tcl\n"
        "package require http\n"
        "\n"
        "proc run {} { puts \"running\" }\n",
        CBM_LANG_TCL, "main.tcl");
    ASSERT_TRUE(n >= 2); /* CURRENTLY FAILS: n == 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TEAL (.tl) — Typed Lua (Teal language)
 * ══════════════════════════════════════════════════════════════════ */

/* Teal: local function declarations → Function nodes. */
TEST(teal_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "math.tl",
        "local function add(a: number, b: number): number\n"
        "   return a + b\n"
        "end\n"
        "\n"
        "local function multiply(a: number, b: number): number\n"
        "   return a * b\n"
        "end\n"
        "\n"
        "local function square(x: number): number\n"
        "   return multiply(x, x)\n"
        "end\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 3); /* green: function_statement → Function nodes */
    PASS();
}

/* Teal: record declaration → type-like node (teal_class_types includes record_declaration). */
TEST(teal_record_node) {
    int types = gp_extract_label(
        "local record Point\n"
        "   x: number\n"
        "   y: number\n"
        "end\n"
        "\n"
        "local record Color\n"
        "   r: number\n"
        "   g: number\n"
        "   b: number\n"
        "end\n",
        CBM_LANG_TEAL, "types.tl", "Class");
    /* green: record_declaration in teal_class_types → Class label */
    ASSERT_TRUE(types >= 2);
    PASS();
}

/* Teal: `require` — teal_import_types = empty_types; no extraction support.
 * Teal uses Lua-style require() but the extractor has no import_types configured. */
TEST(teal_require_not_extracted) {
    /* red: teal_import_types = empty_types; require() is a plain call_expression
     * and is not recognized as an import statement. */
    int n = gp_extract_imports(
        "local json = require(\"dkjson\")\n"
        "local socket = require(\"socket\")\n"
        "\n"
        "local function run()\n"
        "   return json.encode({x = 1})\n"
        "end\n",
        CBM_LANG_TEAL, "main.tl");
    ASSERT_TRUE(n >= 2); /* CURRENTLY FAILS: n == 0 */
    PASS();
}

/* Teal: interface declaration → Interface node. */
TEST(teal_interface_node) {
    int ifaces = gp_extract_label(
        "local interface Drawable\n"
        "   draw: function(self: Drawable)\n"
        "end\n",
        CBM_LANG_TEAL, "drawable.tl", "Interface");
    /* green: interface_declaration in teal_class_types → Interface label */
    ASSERT_TRUE(ifaces >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  VIMSCRIPT (.vim) — Vim configuration/plugin language
 * ══════════════════════════════════════════════════════════════════ */

/* VimScript: function! declarations → Function nodes. */
TEST(vimscript_function_bang_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "plugin.vim",
        "function! FormatBuffer() abort\n"
        "  %s/\\s\\+$//e\n"
        "endfunction\n"
        "\n"
        "function! RunTests() abort\n"
        "  call FormatBuffer()\n"
        "  echom 'tests done'\n"
        "endfunction\n"
        "\n"
        "function! SetupKeyMaps() abort\n"
        "  nnoremap <F5> :call RunTests()<CR>\n"
        "endfunction\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    /* green: function! declarations are extracted as Function nodes */
    ASSERT_TRUE(fns >= 2); /* grammar_labels shows Function:1 for single function fixture;
                            * multiple functions → multiple nodes */
    PASS();
}

/* VimScript: function without bang. */
TEST(vimscript_function_no_bang) {
    int fns = gp_extract_label(
        "function MyPlugin#Init()\n"
        "  let g:myplugin_loaded = 1\n"
        "endfunction\n"
        "\n"
        "function MyPlugin#Enable()\n"
        "  call MyPlugin#Init()\n"
        "endfunction\n",
        CBM_LANG_VIMSCRIPT, "myplugin.vim", "Function");
    /* green: non-bang function declarations also extracted */
    ASSERT_TRUE(fns >= 2);
    PASS();
}

/* VimScript: autoload namespace (Plugin#Function convention) — still Function nodes. */
TEST(vimscript_autoload_function) {
    int fns = gp_extract_label(
        "function! airline#statusline#update()\n"
        "  let s:parts = []\n"
        "endfunction\n",
        CBM_LANG_VIMSCRIPT, "statusline.vim", "Function");
    ASSERT_TRUE(fns >= 1); /* green: autoload-namespaced function → Function node */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  WGSL (.wgsl) — WebGPU Shading Language
 * ══════════════════════════════════════════════════════════════════ */

/* WGSL: plain fn items → Function nodes. */
TEST(wgsl_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "compute.wgsl",
        "fn dot2(v: vec2<f32>) -> f32 {\n"
        "  return v.x * v.x + v.y * v.y;\n"
        "}\n"
        "\n"
        "fn normalize2(v: vec2<f32>) -> vec2<f32> {\n"
        "  let len = sqrt(dot2(v));\n"
        "  return vec2<f32>(v.x / len, v.y / len);\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 2); /* green: function_declaration → Function nodes */
    PASS();
}

/* WGSL: @vertex / @fragment / @compute entry point attributes — the decorated
 * function is still a function_declaration; should produce Function nodes. */
TEST(wgsl_entry_point_functions) {
    int fns = gp_extract_label(
        "struct VertexOutput {\n"
        "  @builtin(position) position: vec4<f32>,\n"
        "  @location(0) color: vec4<f32>,\n"
        "}\n"
        "\n"
        "@vertex\n"
        "fn vs_main(@location(0) pos: vec2<f32>) -> VertexOutput {\n"
        "  var out: VertexOutput;\n"
        "  out.position = vec4<f32>(pos, 0.0, 1.0);\n"
        "  out.color = vec4<f32>(1.0, 0.0, 0.0, 1.0);\n"
        "  return out;\n"
        "}\n"
        "\n"
        "@fragment\n"
        "fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {\n"
        "  return in.color;\n"
        "}\n"
        "\n"
        "@compute @workgroup_size(64)\n"
        "fn cs_main(@builtin(global_invocation_id) id: vec3<u32>) {}\n",
        CBM_LANG_WGSL, "pipeline.wgsl", "Function");
    ASSERT_TRUE(fns >= 3); /* green: all three entry points are function_declarations */
    PASS();
}

/* WGSL: struct → type-like node (wgsl_class_types). */
TEST(wgsl_struct_node) {
    int types = gp_extract_label(
        "struct Uniforms {\n"
        "  mvp: mat4x4<f32>,\n"
        "  time: f32,\n"
        "}\n"
        "\n"
        "struct LightData {\n"
        "  position: vec3<f32>,\n"
        "  intensity: f32,\n"
        "}\n",
        CBM_LANG_WGSL, "uniforms.wgsl", "Struct");
    /* REAL BUG (NEW-16): wgsl_class_types lists "struct_declaration" and the vendored
     * wgsl grammar emits that node, yet 0 Struct nodes result — WGSL struct defs are
     * not reaching extract_class_def. */
    ASSERT_TRUE(types >= 1);
    PASS();
}

/* WGSL: `enable` directive — wgsl_import_types includes "enable_directive". */
TEST(wgsl_enable_directive_extracted) {
    int n = gp_extract_imports(
        "enable f16;\n"
        "enable dual_source_blending;\n"
        "\n"
        "fn run() -> f16 { return f16(1.0); }\n",
        CBM_LANG_WGSL, "ext.wgsl");
    /* REAL BUG (class 2): WGSL absent from cbm_extract_imports() dispatch in
     * extract_imports.c — wgsl_import_types ("enable_directive") never consumed. */
    ASSERT_TRUE(n >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ZSH (.zsh) — Z shell scripting language
 * ══════════════════════════════════════════════════════════════════ */

/* Zsh: function declarations → Function nodes. */
TEST(zsh_function_nodes) {
    GP_Proj lp;
    cbm_store_t *store = gp_index(&lp, "deploy.zsh",
        "function check_deps {\n"
        "  command -v docker || return 1\n"
        "  command -v kubectl || return 1\n"
        "}\n"
        "\n"
        "function build_image {\n"
        "  docker build -t myapp:latest .\n"
        "}\n"
        "\n"
        "function deploy {\n"
        "  check_deps && build_image\n"
        "  kubectl apply -f k8s/\n"
        "}\n");
    int fns = store ? gp_count_label(store, lp.project, "Function") : -1;
    gp_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 3); /* green: function {} → Function nodes */
    PASS();
}

/* Zsh: function keyword shorthand. */
TEST(zsh_function_shorthand) {
    int fns = gp_extract_label(
        "setup() {\n"
        "  echo \"setting up\"\n"
        "}\n"
        "\n"
        "teardown() {\n"
        "  echo \"tearing down\"\n"
        "}\n",
        CBM_LANG_ZSH, "test_hooks.zsh", "Function");
    /* green: both POSIX-style `name()` and `function name` forms should be extracted */
    ASSERT_TRUE(fns >= 2);
    PASS();
}

/* Zsh: `source` command — zsh_import_types = empty_types; no extraction.
 * Shell `source` / `. file` is a command invocation, not a tagged import node. */
TEST(zsh_source_not_extracted) {
    /* red: zsh_import_types = empty_types; source/. are plain command nodes,
     * not import_statement or similar tagged nodes. */
    int n = gp_extract_imports(
        "source ~/.zshrc\n"
        "source lib/utils.zsh\n"
        ". lib/helpers.zsh\n"
        "\n"
        "function run {\n"
        "  echo \"running\"\n"
        "}\n",
        CBM_LANG_ZSH, "main.zsh");
    ASSERT_TRUE(n >= 2); /* CURRENTLY FAILS: n == 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE DECLARATION
 * ══════════════════════════════════════════════════════════════════ */

void suite_grammar_probe_c(void) {
    /* scheme */
    RUN_TEST(scheme_function_nodes);
    RUN_TEST(scheme_import_not_extracted);
    RUN_TEST(scheme_require_not_extracted);
    RUN_TEST(scheme_module_node);

    /* slang */
    RUN_TEST(slang_function_nodes);
    RUN_TEST(slang_struct_node);
    RUN_TEST(slang_shader_entry_point);
    RUN_TEST(slang_include_import);

    /* solidity */
    RUN_TEST(solidity_contract_node);
    RUN_TEST(solidity_interface_node);
    RUN_TEST(solidity_method_nodes);
    RUN_TEST(solidity_import_extracted);
    RUN_TEST(solidity_contract_inheritance);

    /* squirrel */
    RUN_TEST(squirrel_function_nodes);
    RUN_TEST(squirrel_class_node);
    RUN_TEST(squirrel_class_methods);
    RUN_TEST(squirrel_class_inheritance);

    /* starlark */
    RUN_TEST(starlark_function_nodes);
    RUN_TEST(starlark_load_not_extracted);
    RUN_TEST(starlark_star_extension_function_nodes);

    /* sway */
    RUN_TEST(sway_function_nodes);
    RUN_TEST(sway_struct_node);
    RUN_TEST(sway_use_import_extracted);
    RUN_TEST(sway_abi_node);
    RUN_TEST(sway_impl_node);

    /* tcl */
    RUN_TEST(tcl_proc_nodes);
    RUN_TEST(tcl_namespace_node);
    RUN_TEST(tcl_source_not_extracted);

    /* teal */
    RUN_TEST(teal_function_nodes);
    RUN_TEST(teal_record_node);
    RUN_TEST(teal_require_not_extracted);
    RUN_TEST(teal_interface_node);

    /* vimscript */
    RUN_TEST(vimscript_function_bang_nodes);
    RUN_TEST(vimscript_function_no_bang);
    RUN_TEST(vimscript_autoload_function);

    /* wgsl */
    RUN_TEST(wgsl_function_nodes);
    RUN_TEST(wgsl_entry_point_functions);
    RUN_TEST(wgsl_struct_node);
    RUN_TEST(wgsl_enable_directive_extracted);

    /* zsh */
    RUN_TEST(zsh_function_nodes);
    RUN_TEST(zsh_function_shorthand);
    RUN_TEST(zsh_source_not_extracted);
}
