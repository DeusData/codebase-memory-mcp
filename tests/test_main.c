/*
 * test_main.c — Test runner entry point for pure C rewrite.
 *
 * Includes all test suites and runs them sequentially.
 */
/* Global test counters (declared extern in test_framework.h) */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;
int tf_filter_count = 0;

#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/constants.h"
#include "foundation/profile.h"
#include "foundation/compat.h"    /* cbm_setenv — #845 supervisor kill switch */
#include "foundation/compat_fs.h" /* cbm_fopen — worker response file */
#include "foundation/mem.h"       /* cbm_mem_init — worker budget */
#include "foundation/platform.h"  /* system RAM-aware worker budget */
#include "mcp/index_supervisor.h" /* cbm_index_set_worker_role */
#include "mcp/mcp.h"              /* cbm_mcp_handle_tool — act as a real worker */
#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <winsock2.h> /* #798 follow-up: socket-isolation re-exec probe */
#endif

/* #832 guard support: when the index supervisor spawns THIS binary as
 * `<self> cli --index-worker index_repository <args_json> --response-out <file>`
 * (exactly the argv cbm_index_spawn_worker builds), act as a faithful in-process
 * index worker instead of re-running the test suites. This lets the deterministic
 * gating guard (test_mcp.c) spawn a REAL worker child that indexes the fixture and
 * writes its response back, using only public APIs — no production test seam.
 * Returns an exit code (>=0) when it handled a worker invocation, else -1. */
static int tf_maybe_run_index_worker(int argc, char **argv) {
    if (argc < 2 || strcmp(argv[1], "cli") != 0) {
        return -1;
    }
    bool is_worker = false;
    const char *tool = NULL;
    const char *args_json = "{}";
    const char *response_out = NULL;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--index-worker") == 0) {
            is_worker = true;
        } else if (strcmp(argv[i], "--response-out") == 0) {
            if (i + 1 < argc) {
                response_out = argv[++i];
            }
        } else if (argv[i][0] == '{') {
            args_json = argv[i];
        } else if (argv[i][0] != '-' && !tool) {
            tool = argv[i];
        }
    }
    if (!is_worker) {
        return -1;
    }
    if (!tool) {
        tool = "index_repository";
    }

    cbm_mem_init(cbm_mem_ram_fraction_for_total(cbm_system_info().total_ram));
    cbm_index_set_worker_role(true, response_out); /* worker role → index in-process */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return 1;
    }
    char *result = cbm_mcp_handle_tool(srv, tool, args_json);
    if (result) {
        const char *ro = cbm_index_worker_response_out();
        if (ro) {
            FILE *rf = cbm_fopen(ro, "wb");
            if (rf) {
                (void)fputs(result, rf);
                (void)fclose(rf);
            }
        }
    }
    /* Faithful worker exit: mirror run_cli's supervised-worker fast path.
     * The worker-role pipeline deliberately skips its teardown (the OS
     * reclaims everything wholesale on process death), so a normal return
     * through main() lets LeakSanitizer run at exit, report the
     * intentionally-unfreed pipeline, and force exit code 1 — the
     * supervisor then reads a HEALTHY index as worker_failed (the
     * Linux-only IDX832 red: LSan is active in Linux gcc ASan builds,
     * absent on macOS/Windows). _Exit skips atexit/LSan by design,
     * exactly like the production worker in run_cli. */
    fflush(NULL);
    _Exit(result ? 0 : 1);
}

/* #798 follow-up: socket-isolation probe. The parent test
 * (popen_isolates_listening_socket, test_security.c) spawns THIS binary through
 * cbm_popen — the same cmd.exe-grandchild path git takes — passing the numeric
 * value of an inheritable listening-socket handle. If cbm_popen correctly
 * isolates handles, that socket is NOT present in this child and getsockopt
 * fails; a regression to raw _popen leaks it (bInheritHandles=TRUE propagates it
 * transitively through cmd.exe) and getsockopt succeeds. We report via exit code
 * so the verdict survives `cmd.exe /c` (proven by popen_isolated_propagates_exit_code).
 * Returns an exit code (>=0) when it handled a probe invocation, else -1. */
static int tf_maybe_run_socket_probe(int argc, char **argv) {
#ifdef _WIN32
    if (argc < 3 || strcmp(argv[1], "__cbm_sockprobe") != 0) {
        return -1;
    }
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        return 0; /* no winsock in child ⇒ cannot observe a socket ⇒ not leaked */
    }
    unsigned long long hv = strtoull(argv[2], NULL, 10);
    SOCKET s = (SOCKET)(uintptr_t)hv;
    int type = 0;
    int len = (int)sizeof(type);
    int rc = getsockopt(s, SOL_SOCKET, SO_TYPE, (char *)&type, &len);
    /* rc==0 ⇒ the handle is a live socket in THIS child ⇒ it was inherited. */
    return rc == 0 ? 42 : 0;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static int g_suite_argc = 0;
static char **g_suite_argv = NULL;

static bool suite_requested(const char *name) {
    if (g_suite_argc <= 1) {
        return true;
    }
    for (int i = 1; i < g_suite_argc; i++) {
        if (strcmp(g_suite_argv[i], name) == 0) {
            return true;
        }
    }
    return false;
}

#define RUN_SELECTED_SUITE(name)      \
    do {                              \
        if (suite_requested(#name)) { \
            RUN_SUITE(name);          \
        }                             \
    } while (0)

/* Forward declarations of suite functions */
extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_platform(void);
extern void suite_subprocess(void);
extern void suite_extraction(void);
extern void suite_extraction_inheritance(void);
extern void suite_extraction_imports(void);
extern void suite_parse_coverage(void);
extern void suite_grammar_regression(void);
extern void suite_grammar_labels(void);
extern void suite_grammar_imports(void);
extern void suite_ac(void);
extern void suite_store_nodes(void);
extern void suite_store_edges(void);
extern void suite_store_search(void);
extern void suite_store_bulk(void);
extern void suite_cypher(void);
extern void suite_mcp(void);
extern void suite_language(void);
extern void suite_userconfig(void);
extern void suite_gitignore(void);
extern void suite_git_context(void);
extern void suite_discover(void);
extern void suite_graph_buffer(void);
extern void suite_registry(void);
extern void suite_pipeline(void);
extern void suite_index_resilience(void);
extern void suite_fqn(void);
extern void suite_route_canon(void);
extern void suite_path_alias(void);
extern void suite_watcher(void);
extern void suite_lz4(void);
extern void suite_zstd(void);
extern void suite_artifact(void);
extern void suite_sqlite_writer(void);
extern void suite_go_lsp(void);
extern void suite_c_lsp(void);
extern void suite_php_lsp(void);
extern void suite_cs_lsp(void);
extern void suite_cs_lsp_bench(void);
extern void suite_perl_lsp(void);
extern void suite_scope(void);
extern void suite_type_rep(void);
extern void suite_py_lsp(void);
extern void suite_py_lsp_bench(void);
extern void suite_py_lsp_stress(void);
extern void suite_py_lsp_scale(void);
extern void suite_ts_lsp(void);
extern void suite_java_lsp(void);
extern void suite_java_lsp_coverage(void);
extern void suite_kotlin_lsp(void);
extern void suite_rust_lsp(void);
extern void suite_store_arch(void);
extern void suite_httplink(void);
extern void suite_store_pragmas(void);
extern void suite_store_checkpoint(void);
extern void suite_traces(void);
extern void suite_configlink(void);
extern void suite_infrascan(void);
extern void suite_cli(void);
extern void suite_system_info(void);
extern void suite_worker_pool(void);
extern void suite_parallel(void);
extern void suite_mem(void);
extern void suite_ui(void);
extern void suite_token_reduction(void);
extern void suite_depindex(void);
extern void suite_pagerank(void);
extern void suite_tool_consolidation(void);
extern void suite_input_validation(void);
extern void suite_httpd(void);
extern void suite_security(void);
extern void suite_yaml(void);
extern void suite_integration(void);
extern void suite_lang_contract(void);
extern void suite_edge_imports(void);
extern void suite_edge_structural(void);
extern void suite_lsp_resolution_probe(void);
extern void suite_node_creation_probe(void);
extern void suite_edge_types_probe(void);
extern void suite_convergence_probe(void);
extern void suite_matrix_known_classes(void);
extern void suite_matrix_new_constructs(void);
extern void suite_grammar_probe_a(void);
extern void suite_grammar_probe_b(void);
extern void suite_grammar_probe_c(void);
extern void suite_grammar_probe_d(void);
extern void suite_grammar_probe_e(void);
extern void suite_grammar_probe_f(void);
extern void suite_grammar_probe_g(void);
extern void suite_incremental(void);
extern void suite_semantic(void);
extern void suite_ast_profile(void);
extern void suite_slab_alloc(void);
extern void suite_simhash(void);
extern void suite_stack_overflow(void);
extern void suite_dump_verify(void);
extern void suite_dump_verify_io(void);

/* Free the main thread's thread-local node-type bitset cache before exit so
 * LeakSanitizer (Linux x64) doesn't report it. Worker threads free their own
 * caches at thread teardown (pass_parallel.c). */
extern void cbm_kind_in_set_free_cache(void);

/* Capacity for the per-run isolated cache dir path. */
#define TEST_CACHE_DIR_CAP CBM_PATH_MAX
/* cbm_setenv() overwrite flag: nonzero = replace an existing value. */
#define ENV_OVERWRITE 1
/* Test-only injection used to prove cleanup failures make the runner red. */
#define TEST_CACHE_CLEANUP_FAIL_ENV "CBM_TEST_FAIL_CACHE_CLEANUP"
/* Existing integration-test artifact root: setting it opts failed runs into
 * retaining their isolated cache alongside other diagnostic evidence. */
#define TEST_ARTIFACT_DIR_ENV "CBM_TEST_ARTIFACT_DIR"

static char test_cache_dir[TEST_CACHE_DIR_CAP];

static int cleanup_test_cache(void) {
    if (!test_cache_dir[0]) {
        return 0;
    }
    /* Retention is explicit. Ordinary red and green runs both clean the exact
     * runner-owned root; CBM_TEST_ARTIFACT_DIR opts a failed run into keeping
     * its cache for debugging. Forked children use _exit() and cannot run this
     * inherited atexit handler. */
    const char *artifact_dir = getenv(TEST_ARTIFACT_DIR_ENV);
    if (tf_fail_count != 0 && artifact_dir && artifact_dir[0] != '\0') {
        fprintf(stderr, "retained failed test cache: %s\n", test_cache_dir);
        return 0;
    }
    if (getenv(TEST_CACHE_CLEANUP_FAIL_ENV)) {
        return -1;
    }
    if (th_rmtree(test_cache_dir) != 0) {
        return -1;
    }
    test_cache_dir[0] = '\0';
    return 0;
}

static void cleanup_test_cache_at_exit(void) {
    if (cleanup_test_cache() != 0) {
        fprintf(stderr, "warning: failed to remove test cache: %s\n", test_cache_dir);
    }
}

static void require_test_cache_cleanup(void) {
    if (cleanup_test_cache() != 0) {
        fprintf(stderr, "failed to remove test cache: %s\n", test_cache_dir);
        tf_fail_count++;
    }
}

int main(int argc, char **argv) {
    cbm_profile_init();
    /* #798 follow-up: if spawned as the socket-isolation probe, report whether an
     * inheritable socket handle crossed into this child and exit before any suite. */
    int probe_rc = tf_maybe_run_socket_probe(argc, argv);
    if (probe_rc >= 0) {
        return probe_rc;
    }

    /* #832: if spawned as a supervised index worker, do the real work and exit
     * before any suite runs (see tf_maybe_run_index_worker). */
    int worker_rc = tf_maybe_run_index_worker(argc, argv);
    if (worker_rc >= 0) {
        return worker_rc;
    }

    /* #845 belt-and-suspenders: this binary EMBEDS cbm_mcp_handle_tool. The
     * supervisor gate already ignores unmarked hosts, but pin the kill switch
     * too so even a future supervisor-marked test host can never resolve THIS
     * binary as `<self> cli --index-worker …` and recursively re-run suites.
     * A test that exercises the supervisor must explicitly re-enable it. */
    cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);

    g_suite_argc = argc;
    g_suite_argv = argv;
    printf("\n  codebase-memory-mcp  C test suite\n");

    /* DEFAULT-ON store isolation: redirect every test index into a per-run
     * temp dir so the suite never pollutes the user's real
     * ~/.cache/codebase-memory-mcp. Opt out with CBM_TEST_NO_ISOLATE=1 (e.g.
     * to debug against the real store).
     *
     * Works because every test helper now builds its db path via
     * cbm_resolve_cache_dir() (honors CBM_CACHE_DIR), matching the pipeline
     * write path. Earlier these helpers hardcoded ~/.cache and mismatched the
     * CBM_CACHE_DIR-honoring write → 815 empty-store failures. The production
     * path (pipeline.c + mcp.c) honors CBM_CACHE_DIR regardless. */
    const char *no_iso = getenv("CBM_TEST_NO_ISOLATE");
    if (!no_iso || no_iso[0] == '\0') {
        const char *artifact_dir = getenv(TEST_ARTIFACT_DIR_ENV);
        const char *cache_parent =
            artifact_dir && artifact_dir[0] != '\0' ? artifact_dir : cbm_tmpdir();
        if (artifact_dir && artifact_dir[0] != '\0' && !cbm_mkdir_p(artifact_dir, 0755)) {
            fprintf(stderr, "failed to create test artifact directory: %s\n", artifact_dir);
            return 1;
        }
        int n = snprintf(test_cache_dir, sizeof(test_cache_dir), "%s/cbm-test-cache-XXXXXX",
                         cache_parent);
        if (n < 0 || (size_t)n >= sizeof(test_cache_dir) || !cbm_mkdtemp(test_cache_dir)) {
            fprintf(stderr, "failed to create isolated test cache\n");
            return 1;
        }
        if (cbm_setenv("CBM_CACHE_DIR", test_cache_dir, ENV_OVERWRITE) != 0 ||
            atexit(cleanup_test_cache_at_exit) != 0) {
            fprintf(stderr, "failed to initialize isolated test cache\n");
            th_cleanup(test_cache_dir);
            return 1;
        }
    }

    /* Optional focused runs:
     *   CBM_ONLY_SUITE=<substring> selects matching suites.
     *   CBM_ONLY_TEST=<substring> selects matching tests after suite setup.
     * Leave both unset to run the complete test suite. */
    const char *only_suite = getenv("CBM_ONLY_SUITE");
    if (only_suite && only_suite[0]) {
        if (strstr("arena", only_suite)) RUN_SUITE(arena);
        if (strstr("hash_table", only_suite)) RUN_SUITE(hash_table);
        if (strstr("dyn_array", only_suite)) RUN_SUITE(dyn_array);
        if (strstr("str_intern", only_suite)) RUN_SUITE(str_intern);
        if (strstr("log", only_suite)) RUN_SUITE(log);
        if (strstr("str_util", only_suite)) RUN_SUITE(str_util);
        if (strstr("platform", only_suite)) RUN_SUITE(platform);
        if (strstr("subprocess", only_suite)) RUN_SUITE(subprocess);
        if (strstr("dump_verify", only_suite)) RUN_SUITE(dump_verify);
        if (strstr("ac", only_suite)) RUN_SUITE(ac);
        if (strstr("extraction", only_suite)) RUN_SUITE(extraction);
        if (strstr("extraction_inheritance", only_suite)) RUN_SUITE(extraction_inheritance);
        if (strstr("extraction_imports", only_suite)) RUN_SUITE(extraction_imports);
        if (strstr("grammar_regression", only_suite)) RUN_SUITE(grammar_regression);
        if (strstr("grammar_labels", only_suite)) RUN_SUITE(grammar_labels);
        if (strstr("grammar_imports", only_suite)) RUN_SUITE(grammar_imports);
        if (strstr("store_nodes", only_suite)) RUN_SUITE(store_nodes);
        if (strstr("store_edges", only_suite)) RUN_SUITE(store_edges);
        if (strstr("store_search", only_suite)) RUN_SUITE(store_search);
        if (strstr("store_bulk", only_suite)) RUN_SUITE(store_bulk);
        if (strstr("store_pragmas", only_suite)) RUN_SUITE(store_pragmas);
        if (strstr("store_checkpoint", only_suite)) RUN_SUITE(store_checkpoint);
        if (strstr("dump_verify_io", only_suite)) RUN_SUITE(dump_verify_io);
        if (strstr("cypher", only_suite)) RUN_SUITE(cypher);
        if (strstr("mcp", only_suite)) RUN_SUITE(mcp);
        if (strstr("language", only_suite)) RUN_SUITE(language);
        if (strstr("userconfig", only_suite)) RUN_SUITE(userconfig);
        if (strstr("gitignore", only_suite)) RUN_SUITE(gitignore);
        if (strstr("git_context", only_suite)) RUN_SUITE(git_context);
        if (strstr("discover", only_suite)) RUN_SUITE(discover);
        if (strstr("graph_buffer", only_suite)) RUN_SUITE(graph_buffer);
        if (strstr("registry", only_suite)) RUN_SUITE(registry);
        if (strstr("pipeline", only_suite)) RUN_SUITE(pipeline);
        if (strstr("fqn", only_suite)) RUN_SUITE(fqn);
        if (strstr("route_canon", only_suite)) RUN_SUITE(route_canon);
        if (strstr("path_alias", only_suite)) RUN_SUITE(path_alias);
        if (strstr("watcher", only_suite)) RUN_SUITE(watcher);
        if (strstr("lz4", only_suite)) RUN_SUITE(lz4);
        if (strstr("zstd", only_suite)) RUN_SUITE(zstd);
        if (strstr("sqlite_writer", only_suite)) RUN_SUITE(sqlite_writer);
        if (strstr("artifact", only_suite)) RUN_SUITE(artifact);
        if (strstr("scope", only_suite)) RUN_SUITE(scope);
        if (strstr("type_rep", only_suite)) RUN_SUITE(type_rep);
        if (strstr("go_lsp", only_suite)) RUN_SUITE(go_lsp);
        if (strstr("c_lsp", only_suite)) RUN_SUITE(c_lsp);
        if (strstr("php_lsp", only_suite)) RUN_SUITE(php_lsp);
        if (strstr("cs_lsp", only_suite)) RUN_SUITE(cs_lsp);
        if (strstr("cs_lsp_bench", only_suite)) RUN_SUITE(cs_lsp_bench);
        if (strstr("py_lsp", only_suite)) RUN_SUITE(py_lsp);
        if (strstr("kotlin_lsp", only_suite)) RUN_SUITE(kotlin_lsp);
        if (strstr("rust_lsp", only_suite)) RUN_SUITE(rust_lsp);
        if (strstr("py_lsp_bench", only_suite)) RUN_SUITE(py_lsp_bench);
        if (strstr("py_lsp_stress", only_suite)) RUN_SUITE(py_lsp_stress);
        if (strstr("py_lsp_scale", only_suite)) RUN_SUITE(py_lsp_scale);
        if (strstr("ts_lsp", only_suite)) RUN_SUITE(ts_lsp);
        if (strstr("java_lsp", only_suite)) RUN_SUITE(java_lsp);
        if (strstr("java_lsp_coverage", only_suite)) RUN_SUITE(java_lsp_coverage);
        if (strstr("store_arch", only_suite)) RUN_SUITE(store_arch);
        if (strstr("httplink", only_suite)) RUN_SUITE(httplink);
        if (strstr("traces", only_suite)) RUN_SUITE(traces);
        if (strstr("configlink", only_suite)) RUN_SUITE(configlink);
        if (strstr("infrascan", only_suite)) RUN_SUITE(infrascan);
        if (strstr("cli", only_suite)) RUN_SUITE(cli);
        if (strstr("system_info", only_suite)) RUN_SUITE(system_info);
        if (strstr("worker_pool", only_suite)) RUN_SUITE(worker_pool);
        if (strstr("parallel", only_suite)) RUN_SUITE(parallel);
        if (strstr("mem", only_suite)) RUN_SUITE(mem);
        if (strstr("ui", only_suite)) RUN_SUITE(ui);
        if (strstr("token_reduction", only_suite)) RUN_SUITE(token_reduction);
        if (strstr("depindex", only_suite)) RUN_SUITE(depindex);
        if (strstr("pagerank", only_suite)) RUN_SUITE(pagerank);
        if (strstr("tool_consolidation", only_suite)) RUN_SUITE(tool_consolidation);
        if (strstr("input_validation", only_suite)) RUN_SUITE(input_validation);
        if (strstr("httpd", only_suite)) RUN_SUITE(httpd);
        if (strstr("security", only_suite)) RUN_SUITE(security);
        if (strstr("yaml", only_suite)) RUN_SUITE(yaml);
        if (strstr("semantic", only_suite)) RUN_SUITE(semantic);
        if (strstr("ast_profile", only_suite)) RUN_SUITE(ast_profile);
        if (strstr("simhash", only_suite)) RUN_SUITE(simhash);
        if (strstr("stack_overflow", only_suite)) RUN_SUITE(stack_overflow);
        if (strstr("integration", only_suite)) RUN_SUITE(integration);
        if (strstr("lang_contract", only_suite)) RUN_SUITE(lang_contract);
        if (strstr("edge_imports", only_suite)) RUN_SUITE(edge_imports);
        if (strstr("edge_structural", only_suite)) RUN_SUITE(edge_structural);
        if (strstr("lsp_resolution_probe", only_suite)) RUN_SUITE(lsp_resolution_probe);
        if (strstr("node_creation_probe", only_suite)) RUN_SUITE(node_creation_probe);
        if (strstr("edge_types_probe", only_suite)) RUN_SUITE(edge_types_probe);
        if (strstr("convergence_probe", only_suite)) RUN_SUITE(convergence_probe);
        if (strstr("matrix_known_classes", only_suite)) RUN_SUITE(matrix_known_classes);
        if (strstr("matrix_new_constructs", only_suite)) RUN_SUITE(matrix_new_constructs);
        if (strstr("grammar_probe_a", only_suite)) RUN_SUITE(grammar_probe_a);
        if (strstr("grammar_probe_b", only_suite)) RUN_SUITE(grammar_probe_b);
        if (strstr("grammar_probe_c", only_suite)) RUN_SUITE(grammar_probe_c);
        if (strstr("grammar_probe_d", only_suite)) RUN_SUITE(grammar_probe_d);
        if (strstr("grammar_probe_e", only_suite)) RUN_SUITE(grammar_probe_e);
        if (strstr("grammar_probe_f", only_suite)) RUN_SUITE(grammar_probe_f);
        if (strstr("grammar_probe_g", only_suite)) RUN_SUITE(grammar_probe_g);
        if (strstr("incremental", only_suite)) RUN_SUITE(incremental);
        /* Match the full-run exit path so focused sanitizer/leak runs do not
         * report process-lifetime caches as suite-owned allocations. */
        cbm_kind_in_set_free_cache();
        sqlite3_shutdown();
        require_test_cache_cleanup();
        TEST_SUMMARY();
        return 0;
    }

    /* Foundation */
    RUN_SELECTED_SUITE(arena);
    RUN_SELECTED_SUITE(hash_table);
    RUN_SELECTED_SUITE(dyn_array);
    RUN_SELECTED_SUITE(str_intern);
    RUN_SELECTED_SUITE(log);
    RUN_SELECTED_SUITE(str_util);
    RUN_SELECTED_SUITE(platform);
    RUN_SELECTED_SUITE(subprocess);
    RUN_SELECTED_SUITE(dump_verify);

    /* Existing C code regression tests */
    RUN_SELECTED_SUITE(ac);
    RUN_SELECTED_SUITE(extraction);
    RUN_SELECTED_SUITE(extraction_inheritance);
    RUN_SELECTED_SUITE(extraction_imports);
    RUN_SELECTED_SUITE(parse_coverage);
    RUN_SELECTED_SUITE(grammar_regression);
    RUN_SELECTED_SUITE(grammar_labels);
    RUN_SELECTED_SUITE(grammar_imports);

    /* Store (M5) */
    RUN_SELECTED_SUITE(store_nodes);
    RUN_SELECTED_SUITE(store_edges);
    RUN_SELECTED_SUITE(store_search);
    RUN_SELECTED_SUITE(store_bulk);
    RUN_SELECTED_SUITE(store_pragmas);
    RUN_SELECTED_SUITE(store_checkpoint);
    RUN_SELECTED_SUITE(dump_verify_io);

    /* Cypher (M6) */
    RUN_SELECTED_SUITE(cypher);

    /* MCP Server (M9) */
    RUN_SELECTED_SUITE(mcp);

    /* Discover (M2) */
    RUN_SELECTED_SUITE(language);
    RUN_SELECTED_SUITE(userconfig);
    RUN_SELECTED_SUITE(gitignore);
    RUN_SELECTED_SUITE(git_context);
    RUN_SELECTED_SUITE(discover);

    /* Graph Buffer (M7) */
    RUN_SELECTED_SUITE(graph_buffer);

    /* Pipeline (M8) */
    RUN_SELECTED_SUITE(registry);
    RUN_SELECTED_SUITE(pipeline);
    RUN_SELECTED_SUITE(index_resilience);
    RUN_SELECTED_SUITE(fqn);
    RUN_SELECTED_SUITE(route_canon);
    RUN_SELECTED_SUITE(path_alias);

    /* Watcher (M10) */
    RUN_SELECTED_SUITE(watcher);

    /* LZ4 + zstd + SQLite writer */
    RUN_SELECTED_SUITE(lz4);
    RUN_SELECTED_SUITE(zstd);
    RUN_SELECTED_SUITE(sqlite_writer);

    /* Persistent artifact export/import */
    RUN_SELECTED_SUITE(artifact);

    /* LSP resolvers */
    RUN_SELECTED_SUITE(scope);
    RUN_SELECTED_SUITE(type_rep);
    RUN_SELECTED_SUITE(go_lsp);
    RUN_SELECTED_SUITE(c_lsp);
    RUN_SELECTED_SUITE(php_lsp);
    RUN_SELECTED_SUITE(cs_lsp);
    RUN_SELECTED_SUITE(cs_lsp_bench);
    RUN_SELECTED_SUITE(perl_lsp);
    RUN_SELECTED_SUITE(py_lsp);
    RUN_SELECTED_SUITE(kotlin_lsp);
    RUN_SELECTED_SUITE(rust_lsp);
    RUN_SELECTED_SUITE(py_lsp_bench);
    RUN_SELECTED_SUITE(py_lsp_stress);
    RUN_SELECTED_SUITE(py_lsp_scale);
    RUN_SELECTED_SUITE(ts_lsp);
    RUN_SELECTED_SUITE(java_lsp);
    RUN_SELECTED_SUITE(java_lsp_coverage);

    /* Architecture + ADR + Louvain */
    RUN_SELECTED_SUITE(store_arch);

    /* HTTP link */
    RUN_SUITE(httplink);

    /* Traces helpers */
    RUN_SELECTED_SUITE(traces);

    /* Config link */
    RUN_SELECTED_SUITE(configlink);

    /* Infrastructure scanning */
    RUN_SELECTED_SUITE(infrascan);

    /* CLI (install, update, config) */
    RUN_SELECTED_SUITE(cli);

    /* System info + worker pool (parallelism) */
    RUN_SELECTED_SUITE(system_info);
    RUN_SELECTED_SUITE(worker_pool);

    /* Parallel pipeline */
    RUN_SELECTED_SUITE(parallel);

    /* mem + arena + slab integration */
    RUN_SELECTED_SUITE(slab_alloc);
    RUN_SELECTED_SUITE(mem);

    /* UI (config, embedded assets, layout) */
    RUN_SELECTED_SUITE(ui);

    /* Token reduction */
    RUN_SUITE(token_reduction);

    /* Dependency indexing */
    RUN_SUITE(depindex);

    /* PageRank (node + edge ranking) */
    RUN_SUITE(pagerank);

    /* Tool consolidation (Phase 9) */
    RUN_SUITE(tool_consolidation);

    /* Input validation (fuzz-derived) */
    RUN_SUITE(input_validation);

    /* UI HTTP server (transport + routing) */
    RUN_SELECTED_SUITE(httpd);

    /* Security defenses */
    RUN_SELECTED_SUITE(security);

    /* YAML parser */
    RUN_SELECTED_SUITE(yaml);

    /* SimHash / SIMILAR_TO */
    RUN_SELECTED_SUITE(semantic);
    RUN_SELECTED_SUITE(ast_profile);
    RUN_SELECTED_SUITE(simhash);

    /* Stack overflow regression (GitHub #199) */
    RUN_SELECTED_SUITE(stack_overflow);

    /* Integration (end-to-end) */
    RUN_SELECTED_SUITE(integration);

    /* Per-language graph contracts (node/edge types, attribution, no-crash) */
    RUN_SELECTED_SUITE(lang_contract);
    RUN_SELECTED_SUITE(edge_imports);
    RUN_SELECTED_SUITE(edge_structural);
    RUN_SELECTED_SUITE(lsp_resolution_probe);
    RUN_SELECTED_SUITE(node_creation_probe);
    RUN_SELECTED_SUITE(edge_types_probe);
    RUN_SELECTED_SUITE(convergence_probe);
    RUN_SELECTED_SUITE(matrix_known_classes);
    RUN_SELECTED_SUITE(matrix_new_constructs);
    RUN_SELECTED_SUITE(grammar_probe_a);
    RUN_SELECTED_SUITE(grammar_probe_b);
    RUN_SELECTED_SUITE(grammar_probe_c);
    RUN_SELECTED_SUITE(grammar_probe_d);
    RUN_SELECTED_SUITE(grammar_probe_e);
    RUN_SELECTED_SUITE(grammar_probe_f);
    RUN_SELECTED_SUITE(grammar_probe_g);

    RUN_SELECTED_SUITE(incremental);

    /* Release process-lifetime caches so LeakSanitizer reports no leaks. */
    cbm_kind_in_set_free_cache();
    sqlite3_shutdown();
    require_test_cache_cleanup();
    TEST_SUMMARY();
}
