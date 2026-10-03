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
#include <pipeline/pipeline_internal.h>
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
    const char *name; /* relative filename, may include '/' for subdirs */
    const char *content;
} EILangFile;

typedef struct {
    const char *name; /* fixture filename relative to a checked-in fixture root */
} EILangFixtureFile;

static void ei_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\')
            *p = '/';
    }
}

/* Write files, run index_repository, open graph DB.  Returns NULL on failure. */
static cbm_store_t *ei_index_files(EILangProj *lp, const EILangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_ei_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir))
        return NULL;
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
        if (!f)
            return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }

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

static char *ei_slurp_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (size < 0) {
        fclose(f);
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    char *buf = (char *)calloc((size_t)size + 1, 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    fclose(f);
    buf[nread] = '\0';
    return buf;
}

static cbm_store_t *ei_index_fixture_files(EILangProj *lp, const char *fixture_root,
                                           const EILangFixtureFile *files, int nfiles) {
    EILangFile *loaded = (EILangFile *)calloc((size_t)nfiles, sizeof(EILangFile));
    char **contents = (char **)calloc((size_t)nfiles, sizeof(char *));
    if (!loaded || !contents) {
        free(loaded);
        free(contents);
        return NULL;
    }

    cbm_store_t *store = NULL;
    for (int i = 0; i < nfiles; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", fixture_root, files[i].name);
        contents[i] = ei_slurp_file(path);
        if (!contents[i]) {
            goto done;
        }
        loaded[i].name = files[i].name;
        loaded[i].content = contents[i];
    }

    store = ei_index_files(lp, loaded, nfiles);

done:
    for (int i = 0; i < nfiles; i++) {
        free(contents[i]);
    }
    free(contents);
    free(loaded);
    return store;
}

static int64_t ei_node_id_for_file_label(cbm_store_t *store, const char *project,
                                         const char *file_path, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_file(store, project, file_path, &nodes, &count) != CBM_STORE_OK) {
        return 0;
    }
    int64_t id = 0;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, label) == 0) {
            id = nodes[i].id;
            break;
        }
    }
    if (id == 0 && count > 0) {
        id = nodes[0].id;
    }
    cbm_store_free_nodes(nodes, count);
    return id;
}

static void ei_cleanup(EILangProj *lp, cbm_store_t *store) {
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
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* Index `files`, check IMPORTS count >= `floor`.  Dumps a diagnostic on
 * failure so failures are self-diagnosable without re-running manually. */
/* Exact-count variant of ei_edge_present: a fabricated EXTRA edge must fail
 * the probe, so a floor is not enough (#1932's negative-assertion gap). */
static int ei_edge_count_is(const EILangFile *files, int nfiles, const char *edge_type,
                            int expected) {
    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, files, nfiles);
    int got = store ? cbm_store_count_edges_by_type(store, lp.project, edge_type) : -1;
    if (got != expected) {
        fprintf(stderr, "  [%s] FAIL count=%d expected==%d\n", edge_type, got, expected);
    }
    ei_cleanup(&lp, store);
    return got == expected;
}

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
        {"util.py", "def helper(x):\n    return x + 1\n"},
        {"main.py", "from .util import helper\n\ndef run(y):\n    return helper(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: bare `import util` (absolute, same directory). */
TEST(ei_python_absolute_import) {
    static const EILangFile f[] = {
        {"util.py", "def compute(x):\n    return x * 2\n"},
        {"main.py", "import util\n\ndef run(y):\n    return util.compute(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: `from util import compute` — named absolute import. */
TEST(ei_python_from_absolute_import) {
    static const EILangFile f[] = {
        {"util.py", "def compute(x):\n    return x * 2\n"},
        {"main.py", "from util import compute\n\ndef run(y):\n    return compute(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: multiple names in one `from` statement. */
TEST(ei_python_from_multi_names) {
    static const EILangFile f[] = {
        {"ops.py", "def add(a, b):\n    return a + b\n\ndef mul(a, b):\n    return a * b\n"},
        {"client.py",
         "from ops import add, mul\n\ndef run(x, y):\n    return add(x, mul(x, y))\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: aliased import `import util as u`. */
TEST(ei_python_aliased_import) {
    static const EILangFile f[] = {
        {"util.py", "def helper(x):\n    return x + 1\n"},
        {"main.py", "import util as u\n\ndef run(y):\n    return u.helper(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: sub-package path `from pkg.util import fn`. */
TEST(ei_python_subpackage_import) {
    static const EILangFile f[] = {
        {"pkg/__init__.py", ""},
        {"pkg/util.py", "def fn(x):\n    return x\n"},
        {"main.py", "from pkg.util import fn\n\ndef run(y):\n    return fn(y)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Python: wildcard `from util import *`. */
TEST(ei_python_wildcard_import) {
    static const EILangFile f[] = {
        {"util.py", "X = 42\n\ndef helper():\n    return X\n"},
        {"main.py", "from util import *\n\ndef run():\n    return helper()\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Python: sibling relative `from .sibling import x` in a package. */
TEST(ei_python_package_sibling_import) {
    static const EILangFile f[] = {
        {"pkg/__init__.py", ""},
        {"pkg/a.py", "def alpha():\n    return 1\n"},
        {"pkg/b.py", "from .a import alpha\n\ndef beta():\n    return alpha() + 1\n"}};
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
        {"util.ts", "export function helper(x: number): number { return x + 1; }\n"},
        {"main.ts", "import { helper } from './util';\n\n"
                    "export function run(y: number): number { return helper(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* #1682: extensionless dotted basenames are part of the module name.  The
 * resolver used to strip `.engine`, miss the module, and bind both imports to
 * the same-named fixture Function in the sibling spec file. */
TEST(ei_typescript_dotted_relative_import_targets_source_module_issue1682) {
    static const char *engine_path = "packages/api/src/modules/featureX/featureX.engine.ts";
    static const char *consumer_path = "packages/api/src/modules/consumer/consumer.service.ts";
    static const EILangFile f[] = {
        {"packages/api/src/modules/featureX/featureX.engine.ts",
         "export interface SomeType { id: string; qty: number; }\n"
         "export interface Evaluation { rateByItem: Record<string, number>; }\n"
         "export function helperB(configs: SomeType[], lines: SomeType[]): Evaluation {\n"
         "  return { rateByItem: { [lines[0].id]: lines[0].qty + configs.length } };\n"
         "}\n"},
        {"packages/api/src/modules/featureX/featureX.service.ts",
         "import { SomeType, Evaluation, helperB } from './featureX.engine';\n"
         "export class FeatureXService {\n"
         "  evaluate(configs: SomeType[], lines: SomeType[]): Evaluation {\n"
         "    return helperB(configs, lines);\n"
         "  }\n"
         "}\n"},
        {"packages/api/src/modules/featureX/featureX.service.spec.ts",
         "import { SomeType, helperB } from './featureX.engine';\n"
         "function featureX(overrides: Partial<SomeType>): SomeType {\n"
         "  return { id: 'x', qty: 1, ...overrides };\n"
         "}\n"
         "export function exerciseFixture(): number {\n"
         "  return helperB([featureX({})], [featureX({ qty: 2 })]).rateByItem.x;\n"
         "}\n"},
        {"packages/api/src/modules/consumer/consumer.service.ts",
         "import { helperB, type SomeType } from '../featureX/featureX.engine';\n"
         "export class ConsumerService {\n"
         "  callerMethod(items: SomeType[]): number {\n"
         "    return helperB(items, [{ id: 'p1', qty: 1 }]).rateByItem.p1;\n"
         "  }\n"
         "}\n"},
        {"packages/mobile/src/api.ts",
         "export function helperB(token: string): Promise<unknown> {\n"
         "  return fetch('/api/x', { method: 'POST', body: token });\n"
         "}\n"},
    };

    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, f, (int)(sizeof(f) / sizeof(f[0])));
    ASSERT_NOT_NULL(store);

    int64_t consumer_id = ei_node_id_for_file_label(store, lp.project, consumer_path, "File");
    ASSERT_GT(consumer_id, 0);

    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    ASSERT_EQ(
        cbm_store_find_edges_by_source_type(store, consumer_id, "IMPORTS", &edges, &edge_count),
        CBM_STORE_OK);

    bool saw_helper = false;
    bool saw_type = false;
    bool helper_target_ok = false;
    bool type_target_ok = false;
    for (int i = 0; i < edge_count; i++) {
        const char *props = edges[i].properties_json ? edges[i].properties_json : "";
        bool is_helper = strstr(props, "\"local_name\":\"helperB\"") != NULL;
        bool is_type = strstr(props, "\"local_name\":\"SomeType\"") != NULL;
        if (!is_helper && !is_type) {
            continue;
        }

        cbm_node_t *target = (cbm_node_t *)calloc(1, sizeof(cbm_node_t));
        ASSERT_NOT_NULL(target);
        ASSERT_EQ(cbm_store_find_node_by_id(store, edges[i].target_id, target), CBM_STORE_OK);
        bool target_ok = target->file_path && strcmp(target->file_path, engine_path) == 0;
        if (is_helper) {
            saw_helper = true;
            helper_target_ok = target_ok;
        }
        if (is_type) {
            saw_type = true;
            type_target_ok = target_ok;
        }
        cbm_store_free_nodes(target, 1);
    }
    cbm_store_free_edges(edges, edge_count);
    ei_cleanup(&lp, store);

    ASSERT_TRUE(saw_helper);
    ASSERT_TRUE(saw_type);
    ASSERT_TRUE(helper_target_ok);
    ASSERT_TRUE(type_target_ok);
    PASS();
}

/* TypeScript: default import `import helper from './util'`. */
TEST(ei_typescript_default_import) {
    static const EILangFile f[] = {
        {"util.ts", "export default function helper(x: number): number { return x + 1; }\n"},
        {"main.ts", "import helper from './util';\n\n"
                    "export function run(y: number): number { return helper(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: namespace import `import * as util from './util'`. */
TEST(ei_typescript_namespace_import) {
    static const EILangFile f[] = {
        {"util.ts", "export const VALUE = 42;\n"
                    "export function compute(x: number): number { return x * VALUE; }\n"},
        {"main.ts", "import * as util from './util';\n\n"
                    "export function run(): number { return util.compute(util.VALUE); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: aliased named import `import { helper as h } from './util'`. */
TEST(ei_typescript_aliased_import) {
    static const EILangFile f[] = {
        {"util.ts", "export function helper(x: number): number { return x + 1; }\n"},
        {"main.ts", "import { helper as h } from './util';\n\n"
                    "export function run(y: number): number { return h(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: multi-name import `import { a, b } from './ops'`. */
TEST(ei_typescript_multi_names_import) {
    static const EILangFile f[] = {
        {"ops.ts", "export function add(a: number, b: number): number { return a + b; }\n"
                   "export function mul(a: number, b: number): number { return a * b; }\n"},
        {"client.ts",
         "import { add, mul } from './ops';\n\n"
         "export function run(x: number, y: number): number { return add(x, mul(x, y)); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: subdirectory import `import { fn } from './pkg/util'`. */
TEST(ei_typescript_subdir_import) {
    static const EILangFile f[] = {
        {"pkg/util.ts", "export function fn(x: number): number { return x; }\n"},
        {"main.ts", "import { fn } from './pkg/util';\n\n"
                    "export function run(y: number): number { return fn(y); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: re-export `export { fn } from './util'`. */
TEST(ei_typescript_re_export) {
    static const EILangFile f[] = {
        {"util.ts", "export function fn(x: number): number { return x; }\n"},
        {"index.ts", "export { fn } from './util';\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* TypeScript: `import type { T } from './types'` (type-only import). */
TEST(ei_typescript_type_import) {
    static const EILangFile f[] = {
        {"types.ts", "export interface Config { value: number; }\n"},
        {"main.ts", "import type { Config } from './types';\n\n"
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
        {"go.mod", "module example.com/demo\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go", "package main\n\nimport \"example.com/demo/util\"\n\n"
                    "func main() { _ = util.Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: grouped import block `import ( "pkg1"; "pkg2" )`. */
TEST(ei_go_grouped_import_block) {
    static const EILangFile f[] = {
        {"go.mod", "module example.com/grp\n\ngo 1.21\n"},
        {"math/math.go", "package math\n\nfunc Add(a, b int) int { return a + b }\n"},
        {"strutil/str.go", "package strutil\n\nfunc Join(a, b string) string { return a + b }\n"},
        {"main.go", "package main\n\nimport (\n"
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
        {"go.mod", "module example.com/alias\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go", "package main\n\nimport u \"example.com/alias/util\"\n\n"
                    "func main() { _ = u.Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: dot import `import . "example.com/demo/util"` (members into current ns). */
TEST(ei_go_dot_import) {
    static const EILangFile f[] = {
        {"go.mod", "module example.com/dot\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"main.go", "package main\n\nimport . \"example.com/dot/util\"\n\n"
                    "func main() { _ = Helper(1) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: sub-package path import (multi-level directory). */
TEST(ei_go_subpackage_import) {
    static const EILangFile f[] = {
        {"go.mod", "module example.com/sub\n\ngo 1.21\n"},
        {"pkg/math/ops.go", "package math\n\nfunc Mul(a, b int) int { return a * b }\n"},
        {"main.go", "package main\n\nimport \"example.com/sub/pkg/math\"\n\n"
                    "func main() { _ = math.Mul(2, 3) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: blank import `import _ "example.com/demo/util"` (side-effects only). */
TEST(ei_go_blank_import) {
    static const EILangFile f[] = {
        {"go.mod", "module example.com/blank\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc init() {}\n"},
        {"main.go", "package main\n\nimport _ \"example.com/blank/util\"\n\nfunc main() {}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Go: two files importing the same internal package (both should yield edges). */
TEST(ei_go_two_consumers_same_package) {
    static const EILangFile f[] = {
        {"go.mod", "module example.com/two\n\ngo 1.21\n"},
        {"util/util.go", "package util\n\nfunc Helper(x int) int { return x + 1 }\n"},
        {"a/a.go", "package a\n\nimport \"example.com/two/util\"\n\n"
                   "func Run() int { return util.Helper(1) }\n"},
        {"b/b.go", "package b\n\nimport \"example.com/two/util\"\n\n"
                   "func Run() int { return util.Helper(2) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 4, "IMPORTS", 2));
    PASS();
}

TEST(ei_go_import_never_binds_symbol) {
    /* #1934: a Go import path names a package, never a symbol. `os/exec` is
     * external (not in the graph), so the ONLY correct outcome is no edge —
     * but Strategy 3's symbol-name fallback matched the path's last segment
     * against any project definition named `exec` and bound the import to a
     * test harness's method. Exact count: the internal `util` import (edge 1,
     * via Strategy 1 → the package Folder) must be the whole IMPORTS
     * relation; the fallback edge onto harness.exec (reproduce-first RED:
     * count 2) must not exist. */
    static const EILangFile f[] = {
        {"go.mod", "module example.com/fxi\n\ngo 1.22\n"},
        {"util/util.go", "package util\n\nfunc Tag() string { return \"t\" }\n"},
        {"helper/harness.go", "package helper\n\ntype harness struct{ n int }\n\n"
                              "func (h *harness) exec(cmd string) error { return nil }\n"},
        /* Same-package decoy: the field-measured survivor bound os/exec to a
         * method in the IMPORTER'S OWN package (Strategy 1b's sibling-file
         * resolution accepts symbol labels too), so the fixture needs the
         * collision both cross-package and same-package. */
        {"app/aux.go", "package app\n\ntype runner struct{ n int }\n\n"
                       "func (r *runner) exec(cmd string) error { return nil }\n"},
        {"app/run.go", "package app\n\nimport (\n\t\"os/exec\"\n\n"
                       "\t\"example.com/fxi/util\"\n)\n\n"
                       "func Run() error {\n\t_ = util.Tag()\n"
                       "\treturn exec.Command(\"true\").Run()\n}\n"}};
    ASSERT_TRUE(ei_edge_count_is(f, 5, "IMPORTS", 1));
    PASS();
}

/* #2127 helper: inbound edges of `edge_type` onto the (single) node named
 * `name` with label `label`; -1 when that node is missing. */
static int ei_inbound_edges_on(cbm_store_t *store, const char *project, const char *name,
                               const char *label, const char *edge_type) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_name(store, project, name, &nodes, &count) != CBM_STORE_OK) {
        return -1;
    }
    int64_t id = 0;
    for (int i = 0; i < count; i++) {
        if (nodes[i].label && strcmp(nodes[i].label, label) == 0) {
            id = nodes[i].id;
        }
    }
    cbm_store_free_nodes(nodes, count);
    if (id == 0) {
        return -1;
    }
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_target_type(store, id, edge_type, &edges, &n) != CBM_STORE_OK) {
        return -1;
    }
    cbm_store_free_edges(edges, n);
    return n;
}

/* #2127: `from unittest.mock import patch` names an EXTERNAL module. Strategy
 * 1 cannot resolve it, and Strategy 3's symbol-name fallback bound the import
 * to the only project definition named `patch` — an unrelated REST view's
 * HTTP handler — so every patch(...) call became an import_map CALLS edge at
 * confidence 0.95 (and, once the import edge is gone, a unique_name edge; the
 * member form `mock.patch(...)` a suffix_match edge). A Python import path
 * names its module chain, so a symbol hit whose QN does not contain the chain
 * of an EXTERNAL import is not the imported thing. The true project import
 * (`from app.util import helper`) must keep both its IMPORTS and its CALLS
 * edge. `pad` > MIN_FILES_FOR_PARALLEL(50) runs the
 * same fixture through the parallel pipeline, so both drivers are covered. */
static int ei_py_external_import_case(int pad) {
    enum { EI_2127_BASE = 5, EI_2127_MAX = EI_2127_BASE + 64 };
    static char names[EI_2127_MAX][32];
    EILangFile f[EI_2127_MAX];
    int n = 0;
    f[n++] = (EILangFile){"app/views.py", "class PkgConfigView:\n"
                                          "    def get(self, request):\n        return 1\n\n"
                                          "    def patch(self, request):\n        return 2\n\n"
                                          "    def copy(self):\n        return 3\n"};
    f[n++] = (EILangFile){"app/util.py", "def helper():\n    return 1\n"};
    /* Recall pin: a PROJECT module re-exporting a name defined elsewhere
     * (`app.base` re-exports `app.errors.BoomError`) is an internal import;
     * its weak resolution is never judged by the #2127 guard. */
    f[n++] = (EILangFile){"app/errors.py", "class BoomError(Exception):\n    pass\n"};
    f[n++] = (EILangFile){"app/base.py", "from app.errors import BoomError\n"};
    f[n++] = (EILangFile){"tests/test_views.py", "import copy\n"
                                                 "from unittest import mock\n"
                                                 "from unittest.mock import patch\n"
                                                 "from app.base import BoomError\n"
                                                 "from app.util import helper\n\n\n"
                                                 "def test_something():\n"
                                                 "    mock.patch(\"app.views.other\")\n"
                                                 "    copy.copy(helper)\n"
                                                 "    if helper() > 1:\n"
                                                 "        raise BoomError()\n"
                                                 "    with patch(\"app.views.thing\"):\n"
                                                 "        return helper()\n"};
    for (int i = 0; i < pad && n < EI_2127_MAX; i++) {
        snprintf(names[n], sizeof(names[n]), "pad/mod_%02d.py", i);
        f[n] = (EILangFile){names[n], "def filler():\n    return 0\n"};
        n++;
    }
    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, f, n);
    int bad_imports =
        store ? ei_inbound_edges_on(store, lp.project, "patch", "Method", "IMPORTS") : -1;
    int bad_calls = store ? ei_inbound_edges_on(store, lp.project, "patch", "Method", "CALLS") : -1;
    /* `import copy` (a plain module import of stdlib `copy`) is no project
     * method: neither the import nor `copy.copy(...)` may bind it. */
    bad_imports += store ? ei_inbound_edges_on(store, lp.project, "copy", "Method", "IMPORTS") : 0;
    bad_calls += store ? ei_inbound_edges_on(store, lp.project, "copy", "Method", "CALLS") : 0;
    int good_imports =
        store ? ei_inbound_edges_on(store, lp.project, "helper", "Function", "IMPORTS") : -1;
    int good_calls =
        store ? ei_inbound_edges_on(store, lp.project, "helper", "Function", "CALLS") : -1;
    int reexport_calls =
        store ? ei_inbound_edges_on(store, lp.project, "BoomError", "Class", "CALLS") : -1;
    int ok = bad_imports == 0 && bad_calls == 0 && good_imports >= 1 && good_calls >= 1 &&
             reexport_calls >= 1;
    if (!ok) {
        fprintf(stderr,
                "  [#2127 pad=%d] PkgConfigView.patch+copy IMPORTS=%d CALLS=%d (want 0/0); "
                "helper IMPORTS=%d CALLS=%d (want >=1/>=1); BoomError CALLS=%d (want >=1)\n",
                pad, bad_imports, bad_calls, good_imports, good_calls, reexport_calls);
    }
    ei_cleanup(&lp, store);
    return ok;
}

TEST(ei_py_external_import_never_binds_project_symbol) {
    /* Both legs run before asserting so a failure diagnoses both drivers. */
    int sequential_ok = ei_py_external_import_case(0);
    int parallel_ok = ei_py_external_import_case(60);
    ASSERT_TRUE(sequential_ok);
    ASSERT_TRUE(parallel_ok);
    PASS();
}

/* C++: header include should resolve to the header file node, not the same-stem
 * source node. Also exercises angle-bracket include resolution. */
TEST(ei_cpp_header_include_targets_header_file) {
    static const EILangFixtureFile fixture_files[] = {
        {"main.cpp"},           {"NodeController.h"},     {"NodeController.cpp"},
        {"SystemController.h"}, {"SystemController.cpp"},
    };

    EILangProj lp;
    cbm_store_t *store =
        ei_index_fixture_files(&lp, "tests/fixtures/cpp_include", fixture_files,
                               (int)(sizeof(fixture_files) / sizeof(fixture_files[0])));
    ASSERT_NOT_NULL(store);

    int64_t main_id = ei_node_id_for_file_label(store, lp.project, "main.cpp", "File");
    int64_t node_source_id =
        ei_node_id_for_file_label(store, lp.project, "NodeController.cpp", "File");
    int64_t system_source_id =
        ei_node_id_for_file_label(store, lp.project, "SystemController.cpp", "File");

    ASSERT_GT(main_id, 0);
    ASSERT_GT(node_source_id, 0);
    ASSERT_GT(system_source_id, 0);

    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    int rc = cbm_store_find_edges_by_source_type(store, main_id, "IMPORTS", &edges, &edge_count);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_TRUE(edge_count >= 2);

    bool saw_node_header = false;
    bool saw_system_header = false;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t *target = (cbm_node_t *)calloc(1, sizeof(cbm_node_t));

        /* Pass target directly (no &) because it is already a pointer */
        ASSERT_EQ(cbm_store_find_node_by_id(store, edges[i].target_id, target), CBM_STORE_OK);
        ASSERT_EQ(edges[i].source_id, main_id);
        ASSERT_NEQ(edges[i].target_id, node_source_id);
        ASSERT_NEQ(edges[i].target_id, system_source_id);

        /* Use -> instead of . to access fields on a pointer */
        if (target->file_path && strcmp(target->file_path, "NodeController.h") == 0) {
            saw_node_header = true;
        }
        if (target->file_path && strcmp(target->file_path, "SystemController.h") == 0) {
            saw_system_header = true;
        }

        /* Free the node inside the loop */
        cbm_store_free_nodes(target, 1);
    }
    cbm_store_free_edges(edges, edge_count);

    ASSERT_TRUE(saw_node_header);
    ASSERT_TRUE(saw_system_header);

    ei_cleanup(&lp, store);
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
        {"a.rs", "pub fn f(x: i32) -> i32 { x + 1 }\n"},
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
    static const EILangFile f[] = {{"ops.rs", "pub fn add(a: i32, b: i32) -> i32 { a + b }\n"
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
    static const EILangFile f[] = {{"util.rs", "pub fn helper(x: i32) -> i32 { x + 1 }\n"},
                                   {"lib.rs", "mod util;\npub use crate::util::helper;\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: struct import `use crate::models::Config`. */
TEST(ei_rust_struct_use) {
    static const EILangFile f[] = {{"models.rs", "pub struct Config { pub value: i32 }\n"},
                                   {"main.rs", "mod models;\nuse crate::models::Config;\n\n"
                                               "fn make() -> Config { Config { value: 1 } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: glob use `use crate::ops::*`. */
TEST(ei_rust_glob_use) {
    static const EILangFile f[] = {
        {"ops.rs", "pub fn add(a: i32, b: i32) -> i32 { a + b }\n"
                   "pub fn sub(a: i32, b: i32) -> i32 { a - b }\n"},
        {"main.rs", "mod ops;\nuse crate::ops::*;\n\nfn run() -> i32 { add(sub(5, 1), 2) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Rust: trait import `use crate::traits::Compute`. */
TEST(ei_rust_trait_use) {
    static const EILangFile f[] = {
        {"traits.rs", "pub trait Compute { fn run(&self) -> i32; }\n"},
        {"main.rs", "mod traits;\nuse crate::traits::Compute;\n\n"
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
        {"Util.kt", "package com.example\n\nclass Util {\n    fun greet() = \"hello\"\n}\n"},
        {"Main.kt", "package com.example\n\nimport com.example.Util\n\n"
                    "fun main() { val u = Util(); println(u.greet()) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: `import com.example.fn` — top-level function import. */
TEST(ei_kotlin_toplevel_function_import) {
    static const EILangFile f[] = {
        {"ops.kt", "package com.example\n\nfun add(a: Int, b: Int): Int = a + b\n"},
        {"main.kt", "package com.example\n\nimport com.example.add\n\n"
                    "fun run(): Int = add(1, 2)\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: aliased import `import com.example.Util as U`. */
TEST(ei_kotlin_aliased_import) {
    static const EILangFile f[] = {
        {"Util.kt", "package com.example\n\nclass Util {\n    fun compute(x: Int) = x + 1\n}\n"},
        {"Main.kt", "package com.example\n\nimport com.example.Util as U\n\n"
                    "fun run(): Int { val u = U(); return u.compute(5) }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: wildcard import `import com.example.*`. */
TEST(ei_kotlin_wildcard_import) {
    static const EILangFile f[] = {{"ops.kt",
                                    "package com.example\n\nfun add(a: Int, b: Int) = a + b\n"
                                    "fun mul(a: Int, b: Int) = a * b\n"},
                                   {"main.kt", "package com.example\n\nimport com.example.*\n\n"
                                               "fun run() = add(1, mul(2, 3))\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: multiple imports in one file. */
TEST(ei_kotlin_multiple_imports) {
    static const EILangFile f[] = {{"A.kt", "package com.x\n\nclass A { fun a() = 1 }\n"},
                                   {"B.kt", "package com.x\n\nclass B { fun b() = 2 }\n"},
                                   {"Main.kt", "package com.x\n\nimport com.x.A\nimport com.x.B\n\n"
                                               "fun run(): Int { return A().a() + B().b() }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Kotlin: object/companion import `import com.example.Config.DEFAULT`. */
TEST(ei_kotlin_object_member_import) {
    static const EILangFile f[] = {
        {"Config.kt", "package com.example\n\nobject Config {\n    const val DEFAULT = 42\n}\n"},
        {"Main.kt", "package com.example\n\nimport com.example.Config.DEFAULT\n\n"
                    "fun run() = DEFAULT\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Kotlin: data class import across packages. */
TEST(ei_kotlin_data_class_import) {
    static const EILangFile f[] = {
        {"model/User.kt", "package com.example.model\n\ndata class User(val name: String)\n"},
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
        {"Util.java", "package com.example;\npublic class Util {\n"
                      "    public int compute(int x) { return x + 1; }\n}\n"},
        {"Main.java", "package com.example;\nimport com.example.Util;\n"
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
        {"Main.java", "package com.example;\nimport com.example.util.MathOps;\n"
                      "public class Main {\n"
                      "    void run() { int x = MathOps.add(1, 2); }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: wildcard import `import com.example.util.*`. */
TEST(ei_java_wildcard_import) {
    static const EILangFile f[] = {
        {"util/Ops.java", "package com.example.util;\n"
                          "public class Ops { public static int add(int a,int b){return a+b;} }\n"},
        {"Main.java", "package com.example;\nimport com.example.util.*;\n"
                      "public class Main { void run() { int x = Ops.add(1, 2); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: static import `import static com.example.MathOps.add`. */
TEST(ei_java_static_import) {
    static const EILangFile f[] = {
        {"MathOps.java",
         "package com.example;\n"
         "public class MathOps { public static int add(int a,int b){return a+b;} }\n"},
        {"Main.java", "package com.example;\nimport static com.example.MathOps.add;\n"
                      "public class Main { void run() { int x = add(1, 2); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* Java: multiple imports in one file. */
TEST(ei_java_multiple_imports) {
    static const EILangFile f[] = {
        {"A.java", "package com.x;\npublic class A { public int a() { return 1; } }\n"},
        {"B.java", "package com.x;\npublic class B { public int b() { return 2; } }\n"},
        {"Main.java", "package com.x;\nimport com.x.A;\nimport com.x.B;\n"
                      "public class Main { void run() { new A().a(); new B().b(); } }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* Java: interface import across files. */
TEST(ei_java_interface_import) {
    static const EILangFile f[] = {
        {"Compute.java", "package com.example;\npublic interface Compute { int run(int x); }\n"},
        {"Impl.java", "package com.example;\nimport com.example.Compute;\n"
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
        {"Main.java", "package com.example;\nimport static com.example.Constants.*;\n"
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
        {"Main.cs", "using App.Utils;\nnamespace App {\n"
                    "    class Main {\n"
                    "        void Run() { var h = new Helper(); _ = h.Compute(1); }\n    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: aliased using `using H = App.Utils.Helper`. */
TEST(ei_csharp_aliased_using) {
    static const EILangFile f[] = {
        {"Utils.cs",
         "namespace App.Utils {\n"
         "    public class Helper { public int Compute(int x) { return x + 1; } }\n}\n"},
        {"Main.cs", "using H = App.Utils.Helper;\nnamespace App {\n"
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
        {"Main.cs", "using static App.MathOps;\nnamespace App {\n"
                    "    class Main { void Run() { int x = Add(1, 2); } }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: multiple using directives in one file. */
TEST(ei_csharp_multiple_usings) {
    static const EILangFile f[] = {
        {"A.cs", "namespace Com.X { public class A { public int a() { return 1; } } }\n"},
        {"B.cs", "namespace Com.X { public class B { public int b() { return 2; } } }\n"},
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
        {"Impl.cs", "using App.Contracts;\nnamespace App {\n"
                    "    public class Impl : ICompute {\n"
                    "        public int Run(int x) { return x + 1; }\n    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: sub-namespace import `using App.Models.Domain`. */
TEST(ei_csharp_subnamespace_using) {
    static const EILangFile f[] = {
        {"models/User.cs", "namespace App.Models.Domain {\n"
                           "    public class User { public string Name { get; set; } }\n}\n"},
        {"service/Svc.cs", "using App.Models.Domain;\nnamespace App.Service {\n"
                           "    public class UserService {\n"
                           "        public string Greet(User u) { return \"Hello \" + u.Name; }\n"
                           "    }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* C#: file-scoped namespace + using (C# 10 style). */
TEST(ei_csharp_file_scoped_namespace) {
    static const EILangFile f[] = {
        {"Ops.cs", "namespace App.Ops;\npublic static class Ops {\n"
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
        {"main.php", "<?php\nuse App\\Utils\\Helper;\n"
                     "function run(): int { $h = new Helper(); return $h->compute(1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: aliased use `use App\Utils\Helper as H`. */
TEST(ei_php_aliased_use) {
    static const EILangFile f[] = {
        {"Utils/Helper.php", "<?php\nnamespace App\\Utils;\nclass Helper {\n"
                             "    public function compute(int $x): int { return $x + 1; }\n}\n"},
        {"main.php", "<?php\nuse App\\Utils\\Helper as H;\n"
                     "function run(): int { $h = new H(); return $h->compute(1); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: grouped use `use App\Utils\{A, B}`. */
TEST(ei_php_grouped_use) {
    static const EILangFile f[] = {
        {"Utils/A.php",
         "<?php\nnamespace App\\Utils;\nclass A { public function a(): int { return 1; } }\n"},
        {"Utils/B.php",
         "<?php\nnamespace App\\Utils;\nclass B { public function b(): int { return 2; } }\n"},
        {"main.php",
         "<?php\nuse App\\Utils\\{A, B};\n"
         "function run(): int { $a = new A(); $b = new B(); return $a->a() + $b->b(); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* PHP: `use function App\Utils\compute` — function import. */
TEST(ei_php_function_use) {
    static const EILangFile f[] = {
        {"Utils/funcs.php",
         "<?php\nnamespace App\\Utils;\nfunction compute(int $x): int { return $x * 2; }\n"},
        {"main.php", "<?php\nuse function App\\Utils\\compute;\n"
                     "function run(): int { return compute(5); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: `use const App\Utils\MAX_VALUE` — constant import. */
TEST(ei_php_const_use) {
    static const EILangFile f[] = {
        {"Utils/consts.php", "<?php\nnamespace App\\Utils;\nconst MAX_VALUE = 100;\n"},
        {"main.php", "<?php\nuse const App\\Utils\\MAX_VALUE;\n"
                     "function check(int $x): bool { return $x < MAX_VALUE; }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* PHP: multiple use statements in one file. */
TEST(ei_php_multiple_use_statements) {
    static const EILangFile f[] = {
        {"A.php", "<?php\nnamespace Com\\X;\nclass A { public function a(): int { return 1; } }\n"},
        {"B.php", "<?php\nnamespace Com\\X;\nclass B { public function b(): int { return 2; } }\n"},
        {"main.php",
         "<?php\nuse Com\\X\\A;\nuse Com\\X\\B;\n"
         "function run(): int { $a = new A(); $b = new B(); return $a->a() + $b->b(); }\n"}};
    ASSERT_TRUE(ei_edge_present(f, 3, "IMPORTS", 1));
    PASS();
}

/* PHP: interface use across files. */
TEST(ei_php_interface_use) {
    static const EILangFile f[] = {
        {"Contracts/Computable.php",
         "<?php\nnamespace App\\Contracts;\n"
         "interface Computable { public function run(int $x): int; }\n"},
        {"Impl.php", "<?php\nuse App\\Contracts\\Computable;\n"
                     "class Impl implements Computable {\n"
                     "    public function run(int $x): int { return $x + 1; }\n}\n"}};
    ASSERT_TRUE(ei_edge_present(f, 2, "IMPORTS", 1));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PHP PSR-4 — #1186
 *
 * With composer.json `autoload.psr-4`, `use App\Models\Agency;` names exactly
 * one file: <mapped-dir>/Models/Agency.php. The resolver instead fell through
 * to the namespace bucket and bound every class import of App\Models to the
 * FIRST file declaring that namespace (User.php), fabricating a hub. These
 * tests pin the exact target file per local name; a missing class file must
 * leave the import unresolved rather than land on a sibling.
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    const char *local_name; /* IMPORTS edge local_name */
    const char *want_path;  /* expected target file_path; NULL = no edge */
} EIPhpImportExpect;

/* Return the number of matching IMPORTS edges, or -1 on a setup/query error.
 * A failed query or missing importer must never look like a valid no-edge result. */
static int ei_import_target_path(cbm_store_t *store, const char *project, const char *importer,
                                 const char *local, char *out, size_t outsz) {
    out[0] = '\0';
    cbm_node_t *sources = NULL;
    int source_count = 0;
    if (cbm_store_find_nodes_by_file(store, project, importer, &sources, &source_count) !=
        CBM_STORE_OK) {
        return -1;
    }
    int64_t src_id = 0;
    for (int i = 0; i < source_count; i++) {
        if (sources[i].label && strcmp(sources[i].label, "File") == 0) {
            src_id = sources[i].id;
            break;
        }
    }
    cbm_store_free_nodes(sources, source_count);
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (src_id <= 0 ||
        cbm_store_find_edges_by_source_type(store, src_id, "IMPORTS", &edges, &n) != CBM_STORE_OK) {
        return -1;
    }
    int matches = 0;
    char needle[256];
    snprintf(needle, sizeof(needle), "\"local_name\":\"%s\"", local);
    for (int i = 0; i < n; i++) {
        if (!edges[i].properties_json || !strstr(edges[i].properties_json, needle)) {
            continue;
        }
        cbm_node_t target;
        memset(&target, 0, sizeof(target));
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &target) != CBM_STORE_OK) {
            matches = -1;
            break;
        }
        snprintf(out, outsz, "%s", target.file_path ? target.file_path : "?");
        cbm_node_free_fields(&target);
        matches++;
    }
    cbm_store_free_edges(edges, n);
    return matches;
}

/* Index `files` and check every expectation for `importer`. Returns 1 when
 * all hold; prints each mismatch so a RED names the fabricated target. */
static int ei_php_imports_match(const EILangFile *files, int nfiles, const char *importer,
                                const EIPhpImportExpect *want, int nwant) {
    const char *source = NULL;
    for (int i = 0; i < nfiles; i++) {
        if (strcmp(files[i].name, importer) == 0) {
            source = files[i].content;
            break;
        }
    }
    CBMFileResult *extracted = source ? cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP,
                                                         "php_import_test", importer, 0, NULL, NULL)
                                      : NULL;
    if (!extracted) {
        return 0;
    }
    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, files, nfiles);
    if (!store) {
        cbm_free_result(extracted);
        ei_cleanup(&lp, store);
        return 0;
    }
    int ok = 1;
    for (int i = 0; i < nwant; i++) {
        int import_count = 0;
        for (int j = 0; j < extracted->imports.count; j++) {
            const CBMImport *imp = &extracted->imports.items[j];
            if (imp->local_name && strcmp(imp->local_name, want[i].local_name) == 0 &&
                imp->module_path && imp->module_path[0] && !imp->is_default) {
                import_count++;
            }
        }
        char got[512];
        int edge_count = ei_import_target_path(store, lp.project, importer, want[i].local_name, got,
                                               sizeof(got));
        const char *expect = want[i].want_path ? want[i].want_path : "";
        if (import_count != 1 || edge_count != (want[i].want_path ? 1 : 0) ||
            strcmp(got, expect) != 0) {
            fprintf(stderr, "  [IMPORTS %s] %s -> got \"%s\", want \"%s\"\n", importer,
                    want[i].local_name, got, expect);
            fprintf(stderr, "  extracted=%d, matching edges=%d\n", import_count, edge_count);
            ok = 0;
        }
    }
    cbm_free_result(extracted);
    ei_cleanup(&lp, store);
    return ok;
}

#define EI_PHP_CLASS(ns, cls) "<?php\nnamespace " ns ";\n\nclass " cls " {\n}\n"

/* Several classes in one namespace directory: each import binds its own file,
 * not the first file of App\Models. */
TEST(ei_php_psr4_class_per_file_issue1186) {
    static const EILangFile f[] = {
        {"composer.json",
         "{\"name\":\"acme/app\",\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Models/Agency.php", EI_PHP_CLASS("App\\Models", "Agency")},
        {"app/Models/Client.php", EI_PHP_CLASS("App\\Models", "Client")},
        {"app/Models/Property.php", EI_PHP_CLASS("App\\Models", "Property")},
        {"app/Models/User.php", EI_PHP_CLASS("App\\Models", "User")},
        {"app/Http/Controller.php", "<?php\nnamespace App\\Http;\n\n"
                                    "use App\\Models\\Property;\nuse App\\Models\\User;\n"
                                    "use App\\Models\\Client as C;\n\n"
                                    "class Controller {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Property", "app/Models/Property.php"},
        {"User", "app/Models/User.php"},
        {"C", "app/Models/Client.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 6, "app/Http/Controller.php", want, 3));
    PASS();
}

/* Sub-namespaces map to subdirectories of the PSR-4 root, at any depth. */
TEST(ei_php_psr4_nested_subnamespace_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Models/Billing/Account.php", EI_PHP_CLASS("App\\Models\\Billing", "Account")},
        {"app/Models/Billing/Invoice.php", EI_PHP_CLASS("App\\Models\\Billing", "Invoice")},
        {"app/Models/Billing/Tax/Exempt.php", EI_PHP_CLASS("App\\Models\\Billing\\Tax", "Exempt")},
        {"app/Models/Billing/Tax/Rate.php", EI_PHP_CLASS("App\\Models\\Billing\\Tax", "Rate")},
        {"app/Jobs/Bill.php", "<?php\nnamespace App\\Jobs;\n\n"
                              "use App\\Models\\Billing\\Invoice;\n"
                              "use App\\Models\\Billing\\Tax\\Rate;\n\n"
                              "class Bill {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Invoice", "app/Models/Billing/Invoice.php"},
        {"Rate", "app/Models/Billing/Tax/Rate.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 6, "app/Jobs/Bill.php", want, 2));
    PASS();
}

/* Several psr-4 roots, across two composer.json files and both autoload
 * sections: the longest matching prefix wins (App\Domain\ -> src/Domain/ over
 * App\ -> app/), a package's own root maps its namespace, and autoload-dev
 * maps Tests\. The decoy app/Domain/Order.php declares the same namespace and
 * sorts first. */
TEST(ei_php_psr4_multiple_roots_longest_prefix_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\","
                          "\"App\\\\Domain\\\\\":\"src/Domain/\"}},"
                          "\"autoload-dev\":{\"psr-4\":{\"Tests\\\\\":\"tests/\"}}}\n"},
        {"tests/Support/Assert.php", EI_PHP_CLASS("Tests\\Support", "Assert")},
        {"tests/Support/Factory.php", EI_PHP_CLASS("Tests\\Support", "Factory")},
        {"packages/billing/composer.json",
         "{\"name\":\"acme/billing\",\"autoload\":{\"psr-4\":{\"Billing\\\\\":\"src/\"}}}\n"},
        {"app/Domain/Order.php", EI_PHP_CLASS("App\\Domain", "Order")},
        {"src/Domain/Customer.php", EI_PHP_CLASS("App\\Domain", "Customer")},
        {"src/Domain/Order.php", EI_PHP_CLASS("App\\Domain", "Order")},
        {"packages/billing/src/Account.php", EI_PHP_CLASS("Billing", "Account")},
        {"packages/billing/src/Ledger.php", EI_PHP_CLASS("Billing", "Ledger")},
        {"app/Http/Checkout.php", "<?php\nnamespace App\\Http;\n\n"
                                  "use App\\Domain\\Order;\nuse Billing\\Ledger;\n"
                                  "use Tests\\Support\\Assert;\n\n"
                                  "class Checkout {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Order", "src/Domain/Order.php"},
        {"Ledger", "packages/billing/src/Ledger.php"},
        {"Assert", "tests/Support/Assert.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 10, "app/Http/Checkout.php", want, 3));
    PASS();
}

/* Control: `use function` / `use const` name a namespace member, not a class
 * file, so they must not go through PSR-4 class-file mapping (there is no
 * app/Helpers/format_money.php); they keep resolving to the declaring file. */
TEST(ei_php_psr4_use_function_not_class_mapped_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Helpers/money.php", "<?php\nnamespace App\\Helpers;\n\n"
                                  "const CURRENCY = 'EUR';\n\n"
                                  "function format_money($x) { return $x; }\n"},
        {"app/Http/Shop.php", "<?php\nnamespace App\\Http;\n\n"
                              "use function App\\Helpers\\format_money;\n"
                              "use const App\\Helpers\\CURRENCY;\n\n"
                              "class Shop {\n"
                              "    public function show() { return format_money(CURRENCY); }\n"
                              "}\n"}};
    static const EIPhpImportExpect want[] = {
        {"format_money", "app/Helpers/money.php"},
        {"CURRENCY", "app/Helpers/money.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 3, "app/Http/Shop.php", want, 2));
    PASS();
}

/* Control: a PSR-4 class whose file does not exist stays unresolved — it must
 * never fall back to the first file of the namespace directory. The present
 * sibling import still resolves. */
TEST(ei_php_psr4_missing_class_file_unresolved_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Models/User.php", EI_PHP_CLASS("App\\Models", "User")},
        {"app/Http/Guard.php", "<?php\nnamespace App\\Http;\n\n"
                               "use App\\Models\\Ghost;\nuse App\\Models\\User;\n\n"
                               "class Guard {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Ghost", NULL},
        {"User", "app/Models/User.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 3, "app/Http/Guard.php", want, 2));
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PHP vendor / unmapped namespaces — #1186 follow-up
 *
 * A `use` of a namespace that no composer psr-4 prefix covers and no project
 * file declares (Illuminate\, Spatie\, ...) names a class outside the project.
 * The name-guess fallbacks bound it to any same-named project symbol, even in
 * another language (`use Illuminate\Http\JsonResponse` -> a TypeScript class
 * Http). Such an import must produce no edge. Namespaces that project files
 * do declare keep resolving, to the file that declares the imported class.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* A vendor class import whose short name matches a project class binds to
 * nothing; the project's own PSR-4 import of that class still resolves. */
TEST(ei_php_vendor_use_same_name_no_edge_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Enums/Acl/Role.php", "<?php\nnamespace App\\Enums\\Acl;\n\nenum Role: string {\n"
                                   "    case ADMIN = 'admin';\n}\n"},
        {"app/Models/Permission.php", EI_PHP_CLASS("App\\Models", "Permission")},
        {"app/Models/User.php", "<?php\nnamespace App\\Models;\n\n"
                                "use Spatie\\Permission\\Models\\Role;\n"
                                "use Spatie\\Permission\\Models\\Permission as SpatiePermission;\n"
                                "use App\\Enums\\Acl\\Role as AclRole;\n\n"
                                "class User {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Role", NULL},
        {"SpatiePermission", NULL},
        {"AclRole", "app/Enums/Acl/Role.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 4, "app/Models/User.php", want, 3));
    PASS();
}

/* A vendor class import never binds to a non-PHP file: neither through a
 * namespace segment (`Http` -> a TS class Http) nor through the class name
 * (`Request` -> a TS class Request). Nor does a global-namespace `use Closure;`
 * (-> a TS type Closure). */
TEST(ei_php_vendor_use_cross_language_no_edge_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"resources/js/services/http.ts", "export class Http {\n  get(url: string) {}\n}\n"},
        {"resources/js/services/request.ts", "export class Request {\n  send() {}\n}\n"},
        {"resources/js/types.d.ts", "type Closure = () => void\n"},
        {"app/Http/Controllers/SongController.php",
         "<?php\nnamespace App\\Http\\Controllers;\n\n"
         "use Closure;\nuse Illuminate\\Http\\JsonResponse;\nuse Illuminate\\Http\\Request;\n\n"
         "class SongController {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"JsonResponse", NULL},
        {"Request", NULL},
        {"Closure", NULL},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 5, "app/Http/Controllers/SongController.php", want, 3));
    PASS();
}

/* Without composer.json the rule is the same: a namespace no project file
 * declares binds to nothing, while a declared one still resolves. */
TEST(ei_php_undeclared_namespace_no_composer_no_edge_issue1186) {
    static const EILangFile f[] = {
        {"lib/Util/Helper.php", EI_PHP_CLASS("Lib\\Util", "Helper")},
        {"main.php", "<?php\nnamespace Main;\n\n"
                     "use Vendor\\Pkg\\Helper as VendorHelper;\nuse Lib\\Util\\Helper;\n\n"
                     "class Main {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"VendorHelper", NULL},
        {"Helper", "lib/Util/Helper.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 2, "main.php", want, 2));
    PASS();
}

/* Control (no composer.json): a namespace declared by several project files
 * resolves each class import to the file that declares that class, not to the
 * first file of the namespace. */
TEST(ei_php_declared_namespace_per_class_file_issue1186) {
    static const EILangFile f[] = {
        {"lib/Util/Aaa.php", EI_PHP_CLASS("Lib\\Util", "Aaa")},
        {"lib/Util/Helper.php", EI_PHP_CLASS("Lib\\Util", "Helper")},
        {"lib/Util/Zed.php", EI_PHP_CLASS("Lib\\Util", "Zed")},
        {"main.php", "<?php\nnamespace Main;\n\n"
                     "use Lib\\Util\\Helper;\nuse Lib\\Util\\Zed as Z;\nuse Lib\\Util\\Aaa;\n\n"
                     "class Main {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Helper", "lib/Util/Helper.php"},
        {"Z", "lib/Util/Zed.php"},
        {"Aaa", "lib/Util/Aaa.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 4, "main.php", want, 3));
    PASS();
}

/* No namespace declarations anywhere: an empty map must still block vendor
 * name guessing, while an imported global project class remains valid. */
TEST(ei_php_vendor_use_empty_namespace_map_issue1186) {
    static const EILangFile f[] = {{"Helper.php", "<?php\nclass Helper {\n}\n"},
                                   {"Legacy.php", "<?php\nclass LocalHelper {\n}\n"},
                                   {"main.php",
                                    "<?php\nuse Vendor\\Pkg\\Helper;\nuse LocalHelper as Local;\n"
                                    "function run() {}\n"}};
    static const EIPhpImportExpect want[] = {
        {"Helper", NULL},
        {"Local", "Legacy.php"},
    };
    ASSERT_TRUE(ei_php_imports_match(f, 3, "main.php", want, 2));
    PASS();
}

/* Filtering one generic winner would discard Helper when a.ts sorts first.
 * Filtering candidates must instead preserve the valid global PHP class. */
TEST(ei_php_global_use_keeps_php_candidate_issue1186) {
    static const EILangFile f[] = {
        {"a.ts", "export class Helper {\n}\n"},
        {"z.php", "<?php\nclass Helper {\n}\n"},
        {"main.php", "<?php\nnamespace Client;\nuse Helper;\nclass Main {\n}\n"}};
    static const EIPhpImportExpect want[] = {{"Helper", "z.php"}};
    ASSERT_TRUE(ei_php_imports_match(f, 3, "main.php", want, 1));
    PASS();
}

/* Unmapped project namespaces retain exact member-file targets, mixed grouped
 * use kinds, aliases, fully qualified names, and imports of a namespace itself. */
TEST(ei_php_declared_functions_and_alias_issue1186) {
    static const EILangFile f[] = {
        {"lib/Util/Aaa.php", EI_PHP_CLASS("Lib\\Util", "Aaa")},
        {"lib/Util/render.php", "<?php\nnamespace Lib\\Util;\nfunction render() {}\n"},
        {"lib/Util/encode.php", "<?php\nnamespace Lib\\Util;\nfunction encode() {}\n"},
        {"lib/Util/Helper.php", EI_PHP_CLASS("Lib\\Util", "Helper")},
        {"lib/Extra/Extra.php", EI_PHP_CLASS("Lib\\Extra", "Extra")},
        {"lib/Other/Other.php", EI_PHP_CLASS("Lib\\Other", "Other")},
        {"main.php",
         "<?php\nnamespace Main;\n"
         "use Lib\\Util\\{function render as fmt, function encode as enc, Helper as H};\n"
         "use Lib\\Extra as Tools;\nuse \\Lib\\Other\\Other as FullyQualified;\n"
         "class Main {\n}\n"}};
    static const EIPhpImportExpect want[] = {
        {"fmt", "lib/Util/render.php"},
        {"enc", "lib/Util/encode.php"},
        {"H", "lib/Util/Helper.php"},
        {"Tools", "lib/Extra/Extra.php"},
        {"FullyQualified", "lib/Other/Other.php"},
    };
    CBMFileResult *r = cbm_extract_file(f[6].content, (int)strlen(f[6].content), CBM_LANG_PHP,
                                        "php_import_test", "main.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int kinds_ok = 0;
    for (int i = 0; i < r->imports.count; i++) {
        const CBMImport *imp = &r->imports.items[i];
        if (!imp->local_name || !imp->module_path || imp->is_default) {
            continue;
        }
        if ((strcmp(imp->local_name, "fmt") == 0 &&
             strcmp(imp->module_path, "Lib\\Util\\render") == 0 &&
             imp->kind == CBM_IMPORT_KIND_FUNCTION) ||
            (strcmp(imp->local_name, "enc") == 0 &&
             strcmp(imp->module_path, "Lib\\Util\\encode") == 0 &&
             imp->kind == CBM_IMPORT_KIND_FUNCTION) ||
            (strcmp(imp->local_name, "H") == 0 &&
             strcmp(imp->module_path, "Lib\\Util\\Helper") == 0 &&
             imp->kind == CBM_IMPORT_KIND_DEFAULT)) {
            kinds_ok++;
        }
    }
    cbm_free_result(r);
    ASSERT_EQ(kinds_ok, 3);
    ASSERT_TRUE(ei_php_imports_match(f, 7, "main.php", want, 5));
    PASS();
}

/* Exercise the NULL namespace-map contract used by CALLS directly. Try both
 * a direct module-path collision and an earlier-sorting symbol-name collision. */
static int ei_php_null_map_candidate(bool direct_collision) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new("phpimports", "/tmp");
    if (!gbuf) {
        return 0;
    }
    int64_t decoy = cbm_gbuf_upsert_node(
        gbuf, "Function", "render",
        direct_collision ? "phpimports.Lib.render" : "phpimports.a.render", "a.ts", 1, 1, "{}");
    int64_t valid = cbm_gbuf_upsert_node(gbuf, "Function", "render", "phpimports.z.render", "z.php",
                                         1, 1, "{}");
    cbm_pipeline_ctx_t ctx = {.project_name = "phpimports", .gbuf = gbuf};
    CBMImport imp = {
        .local_name = "fmt", .module_path = "Lib\\render", .kind = CBM_IMPORT_KIND_FUNCTION};
    CBMHashTable *saved_pkgmap = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(NULL);
    const cbm_gbuf_node_t *target =
        cbm_pipeline_resolve_import_node(&ctx, "main.php", "phpimports.main.__file__", &imp, NULL);
    int ok = decoy > 0 && valid > 0 && target && target->id == valid;
    cbm_pipeline_set_pkgmap(saved_pkgmap);
    cbm_gbuf_free(gbuf);
    return ok;
}

TEST(ei_php_null_namespace_map_keeps_php_candidate_issue1186) {
    ASSERT_TRUE(ei_php_null_map_candidate(false));
    ASSERT_TRUE(ei_php_null_map_candidate(true));
    PASS();
}

/* A require/include path is not a namespace use. Even with an empty namespace
 * map it retains generic path resolution to the named file. */
TEST(ei_php_require_path_keeps_generic_resolution_issue1186) {
    cbm_gbuf_t *gbuf = cbm_gbuf_new("phpimports", "/tmp");
    ASSERT_NOT_NULL(gbuf);
    int64_t valid = cbm_gbuf_upsert_node(gbuf, "File", "helper.php", "phpimports.lib.helper",
                                         "lib/helper.php", 1, 1, "{}");
    cbm_pipeline_ctx_t ctx = {.project_name = "phpimports", .gbuf = gbuf};
    CBMImport imp = {.local_name = "helper", .module_path = "lib/helper.php"};
    CBMHashTable *map = cbm_pipeline_namespace_map_build_names("phpimports", NULL, NULL, 0);
    CBMHashTable *saved_pkgmap = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(NULL);
    const cbm_gbuf_node_t *target =
        cbm_pipeline_resolve_import_node(&ctx, "main.php", "phpimports.main.__file__", &imp, map);
    int ok = map && valid > 0 && target && target->id == valid;
    cbm_pipeline_set_pkgmap(saved_pkgmap);
    cbm_pipeline_namespace_map_free(map);
    cbm_gbuf_free(gbuf);
    ASSERT_TRUE(ok);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * PHP vendor-imported types — #1186 follow-up (CALLS / INHERITS / USAGE)
 *
 * With the vendor `use` unresolved, the edges that NAME the imported type fell
 * back to name guessing: `extends Request` bound to the project's own Request
 * class, calls on a Request-typed receiver to its same-named methods (or to a
 * TypeScript function), and the type references to that class. A name bound
 * by a vendor `use` must take the LSP result or nothing.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Missing source/target nodes or any failed query are fixture failures, not
 * evidence that a forbidden edge is absent. */
static int ei_php_checked_file_edges(cbm_store_t *store, const char *project, const char *source,
                                     const char *type, const char *target) {
    cbm_node_t *src = NULL, *dst = NULL;
    int nsrc = 0, ndst = 0;
    if (cbm_store_find_nodes_by_file(store, project, source, &src, &nsrc) != CBM_STORE_OK ||
        cbm_store_find_nodes_by_file(store, project, target, &dst, &ndst) != CBM_STORE_OK) {
        cbm_store_free_nodes(src, nsrc);
        cbm_store_free_nodes(dst, ndst);
        return -1;
    }
    int source_files = 0, target_files = 0, source_defs = 0, target_defs = 0;
    for (int i = 0; i < nsrc; i++) {
        source_files += src[i].label && strcmp(src[i].label, "File") == 0;
        source_defs += src[i].label &&
                       (strcmp(src[i].label, "Class") == 0 || strcmp(src[i].label, "Method") == 0);
    }
    for (int i = 0; i < ndst; i++) {
        target_files += dst[i].label && strcmp(dst[i].label, "File") == 0;
        target_defs += dst[i].label && (strcmp(dst[i].label, "Class") == 0 ||
                                        strcmp(dst[i].label, "Function") == 0 ||
                                        strcmp(dst[i].label, "Method") == 0);
    }
    int hits =
        source_files == 1 && target_files == 1 && source_defs > 0 && target_defs > 0 ? 0 : -1;
    for (int i = 0; hits >= 0 && i < nsrc; i++) {
        cbm_edge_t *edges = NULL;
        int count = 0;
        if (cbm_store_find_edges_by_source_type(store, src[i].id, type, &edges, &count) !=
            CBM_STORE_OK) {
            hits = -1;
        }
        for (int j = 0; hits >= 0 && j < count; j++) {
            cbm_node_t node = {0};
            if (cbm_store_find_node_by_id(store, edges[j].target_id, &node) != CBM_STORE_OK) {
                hits = -1;
            } else {
                hits += node.file_path && strcmp(node.file_path, target) == 0;
                cbm_node_free_fields(&node);
            }
        }
        cbm_store_free_edges(edges, count);
    }
    cbm_store_free_nodes(src, nsrc);
    cbm_store_free_nodes(dst, ndst);
    return hits;
}

static bool ei_php_vendor_sites_present(const char *source, const char *path) {
    CBMFileResult *r = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "vendor_types",
                                        path, 0, NULL, NULL);
    if (!r)
        return false;
    const char *names[] = {"validated", "makeIt", "go", "build", "get", "sendAsync"};
    bool ok = r->defs.count > 0 && r->imports.count == 2;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        int found = 0;
        for (int j = 0; j < r->calls.count; j++) {
            const CBMCall *call = &r->calls.items[j];
            if (call->callee_name && strstr(call->callee_name, names[i]) &&
                call->enclosing_func_qn && strstr(call->enclosing_func_qn, "Foo.run") &&
                call->site_end_byte > call->site_start_byte &&
                call->site_end_byte <= strlen(source))
                found++;
        }
        ok = found == 1 && ok;
    }
    int request_uses = 0;
    for (int i = 0; i < r->usages.count; i++) {
        request_uses +=
            r->usages.items[i].ref_name && strcmp(r->usages.items[i].ref_name, "Request") == 0;
    }
    ok = request_uses > 0 && ok;
    cbm_free_result(r);
    return ok;
}

TEST(ei_php_vendor_types_never_bind_by_name_issue1186) {
    static const EILangFile f[] = {
        {"composer.json", "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\"}}}\n"},
        {"app/Http/Requests/Request.php",
         "<?php\nnamespace App\\Http\\Requests;\n\nclass Request {\n"
         "    public function validated() { return 1; }\n"
         "    public static function makeIt() { return 2; }\n"
         "    public function get($k) { return $k; }\n}\n"},
        {"app/Services/Svc.php", "<?php\nnamespace App\\Services;\n\nclass Svc {\n"
                                 "    public function go() { return 3; }\n"
                                 "    public static function build() { return 4; }\n}\n"},
        {"resources/js/api.ts", "export function sendAsync(x: number) { return x; }\n"},
        {"app/Http/Integrations/Foo.php", "<?php\nnamespace App\\Http\\Integrations;\n\n"
                                          "use Saloon\\Http\\Request;\nuse App\\Services\\Svc;\n\n"
                                          "class Foo extends Request {\n"
                                          "    public function run(Request $r, Svc $s) {\n"
                                          "        $r->validated();\n"
                                          "        Request::makeIt();\n"
                                          "        $made = new Request();\n"
                                          "        $s->go();\n"
                                          "        Svc::build();\n"
                                          "        $r->get('id');\n"
                                          "        return $r->sendAsync(1);\n"
                                          "    }\n}\n\n"
                                          "class Bar extends Svc {\n}\n"}};
    static const char *const foo = "app/Http/Integrations/Foo.php";
    static const char *const req = "app/Http/Requests/Request.php";
    static const char *const svc = "app/Services/Svc.php";
    static const struct {
        const char *type;
        const char *tgt;
        int want_min;
        int want_max;
    } checks[] = {
        {"INHERITS", req, 0, 0}, {"CALLS", req, 0, 0},
        {"USAGE", req, 0, 0},    {"CALLS", "resources/js/api.ts", 0, 0},
        {"INHERITS", svc, 1, 1}, {"CALLS", svc, 2, 2},
        {"CALLS", foo, 0, 0},    {"USAGE", svc, 1, 1 << 20},
    };
    ASSERT_TRUE(ei_php_vendor_sites_present(f[4].content, foo));
    EILangProj lp;
    cbm_store_t *store = ei_index_files(&lp, f, 5);
    int ok = store != NULL;
    for (size_t i = 0; store && i < sizeof(checks) / sizeof(checks[0]); i++) {
        int got = ei_php_checked_file_edges(store, lp.project, foo, checks[i].type, checks[i].tgt);
        if (got < checks[i].want_min || got > checks[i].want_max) {
            fprintf(stderr, "  [%s] Foo.php -> %s: got %d, want %d..%d\n", checks[i].type,
                    checks[i].tgt, got, checks[i].want_min, checks[i].want_max);
            ok = 0;
        }
    }
    ei_cleanup(&lp, store);
    ASSERT_TRUE(ok);
    PASS();
}

/* Build the metadata through the production writer, then challenge its
 * readback. A File node or an empty edge query alone is never completeness. */
static bool ei_php_seed_result(cbm_gbuf_t *gb, const char *project, const char *rel,
                               const CBMFileResult *r) {
    char *qn = cbm_pipeline_fqn_compute(project, rel, "__file__");
    if (!qn)
        return false;
    int64_t id = cbm_gbuf_upsert_node(gb, "File", rel, qn, rel, 0, 0, "{}");
    free(qn);
    if (id <= 0 || !r || r->defs.count == 0)
        return false;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (!d->label || !d->name || !d->qualified_name ||
            cbm_gbuf_upsert_node(gb, d->label, d->name, d->qualified_name, rel, d->start_line,
                                 d->end_line, "{}") <= 0)
            return false;
    }
    return true;
}

TEST(ei_php_binding_snapshot_requires_current_complete_evidence) {
    const char *source = "<?php\nnamespace Local;\nuse Vendor\\Http\\Request;\n"
                         "class Main { function run(Request $r) { $r->send(); } }\n";
    CBMFileResult *r =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "p", "main.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->imports.count, 1);
    ASSERT_GT(r->calls.count, 0);
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp");
    ASSERT_NOT_NULL(gb);
    cbm_php_vendor_names_t names = {0};
    cbm_pipeline_php_vendor_names_build(NULL, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets); /* no File node */
    ASSERT_TRUE(ei_php_seed_result(gb, "p", "main.php", r));
    char *file_qn = cbm_pipeline_fqn_compute("p", "main.php", "__file__");
    ASSERT_NOT_NULL(file_qn);
    const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(gb, file_qn);
    ASSERT_NOT_NULL(file);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets); /* legacy File properties */
    const char *rels[] = {"main.php"};
    CBMFileResult *results[] = {r};
    CBMHashTable *ns = cbm_pipeline_namespace_map_build("p", results, rels, 1);
    ASSERT_NOT_NULL(ns);
    cbm_pipeline_ctx_t ctx = {.project_name = "p", .gbuf = gb};
    CBMHashTable *saved = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, r, "main.php", file_qn, file, ns);
    cbm_pipeline_set_pkgmap(saved);
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = -1;
    ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(gb, file->id, "IMPORTS", &edges, &edge_count), 0);
    ASSERT_EQ(edge_count, 0);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NOT_NULL(names.targets);
    ASSERT_NULL(names.targets[0]);
    ASSERT_TRUE(cbm_pipeline_php_vendor_bound(&names, "Request"));
    cbm_pipeline_php_vendor_names_free(&names);

    /* A failed/unknown resolver state cannot reuse the preceding proof. */
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, r, "main.php", file_qn, file, NULL);
    cbm_pipeline_set_pkgmap(saved);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets);
    char overlong[4096];
    memset(overlong, 'X', sizeof(overlong));
    memcpy(overlong + sizeof(overlong) - 9, "\\Request", 9);
    CBMImport oversized = r->imports.items[0];
    oversized.module_path = overlong;
    CBMFileResult oversized_result = *r;
    oversized_result.imports.items = &oversized;
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, &oversized_result, "main.php", file_qn, file, ns);
    cbm_pipeline_set_pkgmap(saved);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", &oversized_result, &names);
    ASSERT_NULL(names.targets);
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, r, "main.php", file_qn, file, ns);
    cbm_pipeline_set_pkgmap(saved);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NOT_NULL(names.targets);
    cbm_pipeline_php_vendor_names_free(&names);

    CBMImport changed = r->imports.items[0];
    changed.module_path = "Different\\Http\\Request";
    CBMFileResult stale = *r;
    stale.imports.items = &changed;
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", &stale, &names);
    ASSERT_NULL(names.targets); /* old marker does not cover new extraction */
    stale.imports.count = 0;
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", &stale, &names);
    ASSERT_NULL(names.targets); /* even removal invalidates the full manifest */

    ASSERT_EQ(
        cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, "{\"php_imports_complete\":1}"),
        0);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets);
    ASSERT_EQ(cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, "{invalid"), 0);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets);
    cbm_pipeline_namespace_map_free(ns);
    free(file_qn);
    cbm_gbuf_free(gb);
    cbm_free_result(r);
    PASS();
}

TEST(ei_php_binding_snapshot_groups_kinds_and_invalidates_failed_rebuild) {
    const char *source =
        "<?php\nnamespace Local;\n"
        "use Lib\\Pkg\\{Thing as Same, function render as Same, const VALUE as Same};\n"
        "class Main { function run(Same $x) { $x->go(); } }\n";
    const char *library =
        "<?php\nnamespace Lib\\Pkg;\n"
        "class Thing { function go() {} }\nfunction render() {}\nconst VALUE = 1;\n";
    CBMFileResult *r =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "p", "main.php", 0, NULL, NULL);
    CBMFileResult *lib = cbm_extract_file(library, (int)strlen(library), CBM_LANG_PHP, "p",
                                          "lib/Thing.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(lib);
    ASSERT_EQ(r->imports.count, 3);
    int class_kind = 0, function_kind = 0, const_kind = 0;
    for (int i = 0; i < r->imports.count; i++) {
        ASSERT_STR_EQ(r->imports.items[i].local_name, "Same");
        class_kind += r->imports.items[i].kind == CBM_IMPORT_KIND_DEFAULT;
        function_kind += r->imports.items[i].kind == CBM_IMPORT_KIND_FUNCTION;
        const_kind += r->imports.items[i].kind == CBM_IMPORT_KIND_CONST;
    }
    ASSERT_EQ(class_kind, 1);
    ASSERT_EQ(function_kind, 1);
    ASSERT_EQ(const_kind, 1);
    cbm_gbuf_t *gb = cbm_gbuf_new("p", "/tmp");
    ASSERT_NOT_NULL(gb);
    ASSERT_TRUE(ei_php_seed_result(gb, "p", "main.php", r));
    ASSERT_TRUE(ei_php_seed_result(gb, "p", "lib/Thing.php", lib));
    char *file_qn = cbm_pipeline_fqn_compute("p", "main.php", "__file__");
    char *target_qn = cbm_pipeline_fqn_compute("p", "lib/Thing.php", "__file__");
    ASSERT_NOT_NULL(file_qn);
    ASSERT_NOT_NULL(target_qn);
    const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(gb, file_qn);
    ASSERT_NOT_NULL(file);
    CBMFileResult *results[] = {r, lib};
    const char *rels[] = {"main.php", "lib/Thing.php"};
    CBMHashTable *ns = cbm_pipeline_namespace_map_build("p", results, rels, 2);
    ASSERT_NOT_NULL(ns);
    cbm_pipeline_ctx_t ctx = {.project_name = "p", .gbuf = gb};
    CBMHashTable *saved = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, r, "main.php", file_qn, file, ns);
    cbm_pipeline_set_pkgmap(saved);
    cbm_php_vendor_names_t names = {0};
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NOT_NULL(names.targets);
    for (int i = 0; i < r->imports.count; i++)
        ASSERT_STR_EQ(names.targets[i], target_qn);
    ASSERT_TRUE(!cbm_pipeline_php_vendor_bound(&names, "Same"));
    cbm_pipeline_php_vendor_names_free(&names);
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = -1;
    ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(gb, file->id, "IMPORTS", &edges, &edge_count), 0);
    ASSERT_EQ(edge_count, 1); /* same target and alias, three represented kinds */
    ASSERT_NOT_NULL(strstr(edges[0]->properties_json, "php_import_bindings"));

    /* A copied File marker cannot certify legacy, malformed or dangling
     * IMPORTS rows. The query succeeds and returns the deliberately bad row. */
    const char *bad_properties[] = {
        "{\"local_name\":\"Same\"}",
        "{\"local_name\":\"Same\",\"php_import_bindings\":[{\"kind\":0}]}",
        edges[0]->properties_json,
    };
    for (int bad = 0; bad < 3; bad++) {
        cbm_gbuf_t *legacy = cbm_gbuf_new("p", "/tmp");
        ASSERT_NOT_NULL(legacy);
        ASSERT_TRUE(ei_php_seed_result(legacy, "p", "main.php", r));
        ASSERT_TRUE(ei_php_seed_result(legacy, "p", "lib/Thing.php", lib));
        const cbm_gbuf_node_t *legacy_file = cbm_gbuf_find_by_qn(legacy, file_qn);
        const cbm_gbuf_node_t *legacy_target = cbm_gbuf_find_by_qn(legacy, target_qn);
        ASSERT_NOT_NULL(legacy_file);
        ASSERT_NOT_NULL(legacy_target);
        ASSERT_EQ(cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)legacy_file,
                                                    file->properties_json),
                  0);
        cbm_gbuf_insert_edge(legacy, legacy_file->id,
                             bad == 2 ? legacy_target->id + 10000 : legacy_target->id, "IMPORTS",
                             bad_properties[bad]);
        const cbm_gbuf_edge_t **bad_edges = NULL;
        int bad_count = 0;
        ASSERT_EQ(cbm_gbuf_find_edges_by_source_type(legacy, legacy_file->id, "IMPORTS", &bad_edges,
                                                     &bad_count),
                  0);
        ASSERT_EQ(bad_count, 1);
        cbm_pipeline_php_vendor_names_build(legacy, "p", "main.php", r, &names);
        ASSERT_NULL(names.targets);
        cbm_gbuf_free(legacy);
    }

    CBMImport duplicates[] = {r->imports.items[0], r->imports.items[0]};
    CBMFileResult malformed = *r;
    malformed.imports.items = duplicates;
    malformed.imports.count = 2;
    cbm_pipeline_set_pkgmap(NULL);
    cbm_pipeline_php_create_import_edges(&ctx, &malformed, "main.php", file_qn, file, ns);
    cbm_pipeline_set_pkgmap(saved);
    ASSERT_NOT_NULL(strstr(file->properties_json, "\"php_imports_complete\":0"));
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", &malformed, &names);
    ASSERT_NULL(names.targets);
    cbm_pipeline_php_vendor_names_build(gb, "p", "main.php", r, &names);
    ASSERT_NULL(names.targets); /* failed rebuild cannot revive old proof */
    cbm_pipeline_namespace_map_free(ns);
    free(file_qn);
    free(target_qn);
    cbm_gbuf_free(gb);
    cbm_free_result(r);
    cbm_free_result(lib);
    PASS();
}

TEST(ei_php_psr4_missing_class_leaves_type_evidence_unknown) {
    EILangFile files[] = {
        {"composer.json",
         "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\",\"Vendor\\\\\":\"missing/\"}}}\n"},
        {"app/Svc.php", "<?php\nnamespace App;\nclass Svc { function go() {} }\n"},
        {"app/Main.php", "<?php\nnamespace App;\nuse Vendor\\Request;\nuse App\\Svc;\n"
                         "class Main { function run(Request $r, Svc $s) { $s->go(); } }\n"},
    };
    CBMFileResult *r = cbm_extract_file(files[2].content, (int)strlen(files[2].content),
                                        CBM_LANG_PHP, "p", files[2].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->imports.count, 2);
    int sites = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *c = &r->calls.items[i];
        sites += c->callee_name && strstr(c->callee_name, "go") && c->enclosing_func_qn &&
                 strstr(c->enclosing_func_qn, "Main.run") && c->site_end_byte > c->site_start_byte;
    }
    cbm_free_result(r);
    ASSERT_EQ(sites, 1);
    const char *covered_missing = files[0].content;
    const char *empty_prefix =
        "{\"autoload\":{\"psr-4\":{\"App\\\\\":\"app/\",\"\":\"missing/\"}}}\n";
    /* The baseline resolver also cannot certify a Composer fallback prefix. */
    for (int scenario = 0; scenario < 2; scenario++) {
        files[0].content = scenario == 0 ? covered_missing : empty_prefix;
        EILangProj lp;
        cbm_store_t *store = ei_index_files(&lp, files, 3);
        bool ok = store != NULL;
        cbm_node_t *nodes = NULL;
        int count = 0, file_count = 0;
        if (!store || cbm_store_find_nodes_by_file(store, lp.project, "app/Main.php", &nodes,
                                                   &count) != CBM_STORE_OK) {
            ok = false;
        }
        for (int i = 0; i < count; i++) {
            if (nodes[i].label && strcmp(nodes[i].label, "File") == 0) {
                file_count++;
                ok = nodes[i].properties_json &&
                     !strstr(nodes[i].properties_json, "\"php_imports_complete\":1") && ok;
            }
        }
        cbm_store_free_nodes(nodes, count);
        char target[512];
        if (store) {
            ok = ei_import_target_path(store, lp.project, "app/Main.php", "Request", target,
                                       sizeof(target)) == 0 &&
                 ok;
            ok = ei_import_target_path(store, lp.project, "app/Main.php", "Svc", target,
                                       sizeof(target)) == 1 &&
                 strcmp(target, "app/Svc.php") == 0 && ok;
            ok = ei_php_checked_file_edges(store, lp.project, "app/Main.php", "CALLS",
                                           "app/Svc.php") == 1 &&
                 ok;
        }
        ei_cleanup(&lp, store);
        ASSERT_EQ(file_count, 1);
        ASSERT_TRUE(ok);
    }
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
    RUN_TEST(ei_typescript_dotted_relative_import_targets_source_module_issue1682);
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
    RUN_TEST(ei_go_import_never_binds_symbol);
    RUN_TEST(ei_py_external_import_never_binds_project_symbol);
    RUN_TEST(ei_cpp_header_include_targets_header_file);

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
    RUN_TEST(ei_php_psr4_class_per_file_issue1186);
    RUN_TEST(ei_php_psr4_nested_subnamespace_issue1186);
    RUN_TEST(ei_php_psr4_multiple_roots_longest_prefix_issue1186);
    RUN_TEST(ei_php_psr4_use_function_not_class_mapped_issue1186);
    RUN_TEST(ei_php_psr4_missing_class_file_unresolved_issue1186);
    RUN_TEST(ei_php_vendor_use_same_name_no_edge_issue1186);
    RUN_TEST(ei_php_vendor_use_cross_language_no_edge_issue1186);
    RUN_TEST(ei_php_undeclared_namespace_no_composer_no_edge_issue1186);
    RUN_TEST(ei_php_declared_namespace_per_class_file_issue1186);
    RUN_TEST(ei_php_vendor_use_empty_namespace_map_issue1186);
    RUN_TEST(ei_php_global_use_keeps_php_candidate_issue1186);
    RUN_TEST(ei_php_declared_functions_and_alias_issue1186);
    RUN_TEST(ei_php_null_namespace_map_keeps_php_candidate_issue1186);
    RUN_TEST(ei_php_require_path_keeps_generic_resolution_issue1186);
    RUN_TEST(ei_php_vendor_types_never_bind_by_name_issue1186);
    RUN_TEST(ei_php_binding_snapshot_requires_current_complete_evidence);
    RUN_TEST(ei_php_binding_snapshot_groups_kinds_and_invalidates_failed_rebuild);
    RUN_TEST(ei_php_psr4_missing_class_leaves_type_evidence_unknown);
}
