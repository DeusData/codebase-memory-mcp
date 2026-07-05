/*
 * test_main.c — Test runner entry point for pure C rewrite.
 *
 * Includes all test suites and runs them sequentially.
 */
/* Global test counters (declared extern in test_framework.h) */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;

#include "test_framework.h"
#include "foundation/compat.h"    /* cbm_setenv — #845 supervisor kill switch */
#include "foundation/compat_fs.h" /* cbm_fopen — worker response file */
#include "foundation/mem.h"       /* cbm_mem_init — worker budget */
#include "mcp/index_supervisor.h" /* cbm_index_set_worker_role */
#include "mcp/mcp.h"              /* cbm_mcp_handle_tool — act as a real worker */
#include <sqlite3.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

    cbm_mem_init(0.5);
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
        free(result);
    }
    cbm_mcp_server_free(srv);
    return 0;
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
extern void suite_grammar_regression(void);
extern void suite_grammar_labels(void);
extern void suite_grammar_imports(void);
extern void suite_ac(void);
extern void suite_store_nodes(void);
extern void suite_store_edges(void);
extern void suite_store_search(void);
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
extern void suite_store_bulk(void);
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

int main(int argc, char **argv) {
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
    TEST_SUMMARY();
}
