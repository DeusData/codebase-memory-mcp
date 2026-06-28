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
#include <sqlite3.h>

/* Forward declarations of suite functions */
extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_platform(void);
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
extern void suite_store_bulk(void);
extern void suite_cypher(void);
extern void suite_mcp(void);
extern void suite_language(void);
extern void suite_userconfig(void);
extern void suite_gitignore(void);
extern void suite_discover(void);
extern void suite_graph_buffer(void);
extern void suite_registry(void);
extern void suite_pipeline(void);
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
extern void suite_simhash(void);
extern void suite_stack_overflow(void);
extern void suite_dump_verify(void);
extern void suite_dump_verify_io(void);

/* Free the main thread's thread-local node-type bitset cache before exit so
 * LeakSanitizer (Linux x64) doesn't report it. Worker threads free their own
 * caches at thread teardown (pass_parallel.c). */
extern void cbm_kind_in_set_free_cache(void);

/* Capacity for the per-run isolated cache dir path. comfortably fits
 * "/tmp/cbm-test-cache-" + the 6 mkdtemp placeholder chars + headroom. */
#define TEST_CACHE_DIR_CAP 512
/* setenv() overwrite flag: nonzero = replace an existing value. */
#define ENV_OVERWRITE 1

int main(void) {
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
        static char test_cache_dir[TEST_CACHE_DIR_CAP];
        snprintf(test_cache_dir, sizeof(test_cache_dir), "/tmp/cbm-test-cache-XXXXXX");
        if (mkdtemp(test_cache_dir)) {
            setenv("CBM_CACHE_DIR", test_cache_dir, ENV_OVERWRITE);
        }
    }

    const char *only_suite = getenv("CBM_ONLY_SUITE");
    if (only_suite && only_suite[0]) {
        if (strstr("incremental", only_suite)) RUN_SUITE(incremental);
        if (strstr("mcp", only_suite)) RUN_SUITE(mcp);
        if (strstr("tool_consolidation", only_suite)) RUN_SUITE(tool_consolidation);
        if (strstr("cli", only_suite)) RUN_SUITE(cli);
        if (strstr("pipeline", only_suite)) RUN_SUITE(pipeline);
        if (strstr("httplink", only_suite)) RUN_SUITE(httplink);
        if (strstr("parallel", only_suite)) RUN_SUITE(parallel);
        if (strstr("store_nodes", only_suite)) RUN_SUITE(store_nodes);
        if (strstr("store_search", only_suite)) RUN_SUITE(store_search);
        if (strstr("store_bulk", only_suite)) RUN_SUITE(store_bulk);
        if (strstr("store_pragmas", only_suite)) RUN_SUITE(store_pragmas);
        if (strstr("store_checkpoint", only_suite)) RUN_SUITE(store_checkpoint);
        if (strstr("sqlite_writer", only_suite)) RUN_SUITE(sqlite_writer);
        if (strstr("graph_buffer", only_suite)) RUN_SUITE(graph_buffer);
        if (strstr("pagerank", only_suite)) RUN_SUITE(pagerank);
        if (strstr("depindex", only_suite)) RUN_SUITE(depindex);
        if (strstr("token_reduction", only_suite)) RUN_SUITE(token_reduction);
        if (strstr("input_validation", only_suite)) RUN_SUITE(input_validation);
        if (strstr("store_arch", only_suite)) RUN_SUITE(store_arch);
        if (strstr("infrascan", only_suite)) RUN_SUITE(infrascan);
        if (strstr("watcher", only_suite)) RUN_SUITE(watcher);
        if (strstr("security", only_suite)) RUN_SUITE(security);
        if (strstr("artifact", only_suite)) RUN_SUITE(artifact);
        TEST_SUMMARY();
        return 0;
    }

    /* Foundation */
    RUN_SUITE(arena);
    RUN_SUITE(hash_table);
    RUN_SUITE(dyn_array);
    RUN_SUITE(str_intern);
    RUN_SUITE(log);
    RUN_SUITE(str_util);
    RUN_SUITE(platform);
    RUN_SUITE(dump_verify);

    /* Existing C code regression tests */
    RUN_SUITE(ac);
    RUN_SUITE(extraction);
    RUN_SUITE(extraction_inheritance);
    RUN_SUITE(extraction_imports);
    RUN_SUITE(grammar_regression);
    RUN_SUITE(grammar_labels);
    RUN_SUITE(grammar_imports);

    /* Store (M5) */
    RUN_SUITE(store_nodes);
    RUN_SUITE(store_edges);
    RUN_SUITE(store_search);
    RUN_SUITE(store_bulk);
    RUN_SUITE(store_pragmas);
    RUN_SUITE(store_checkpoint);
    RUN_SUITE(dump_verify_io);

    /* Cypher (M6) */
    RUN_SUITE(cypher);

    /* MCP Server (M9) */
    RUN_SUITE(mcp);

    /* Discover (M2) */
    RUN_SUITE(language);
    RUN_SUITE(userconfig);
    RUN_SUITE(gitignore);
    RUN_SUITE(discover);
    RUN_SUITE(userconfig);

    /* Graph Buffer (M7) */
    RUN_SUITE(graph_buffer);

    /* Pipeline (M8) */
    RUN_SUITE(registry);
    RUN_SUITE(pipeline);
    RUN_SUITE(fqn);
    RUN_SUITE(route_canon);
    RUN_SUITE(path_alias);

    /* Watcher (M10) */
    RUN_SUITE(watcher);

    /* LZ4 + zstd + SQLite writer */
    RUN_SUITE(lz4);
    RUN_SUITE(zstd);
    RUN_SUITE(sqlite_writer);

    /* Persistent artifact export/import */
    RUN_SUITE(artifact);

    /* LSP resolvers */
    RUN_SUITE(scope);
    RUN_SUITE(type_rep);
    RUN_SUITE(go_lsp);
    RUN_SUITE(c_lsp);
    RUN_SUITE(php_lsp);
    RUN_SUITE(cs_lsp);
    RUN_SUITE(cs_lsp_bench);
    RUN_SUITE(py_lsp);
    RUN_SUITE(kotlin_lsp);
    RUN_SUITE(rust_lsp);
    RUN_SUITE(py_lsp_bench);
    RUN_SUITE(py_lsp_stress);
    RUN_SUITE(py_lsp_scale);
    RUN_SUITE(ts_lsp);
    RUN_SUITE(java_lsp);
    RUN_SUITE(java_lsp_coverage);

    /* Architecture + ADR + Louvain */
    RUN_SUITE(store_arch);

    /* HTTP link */
    RUN_SUITE(httplink);

    /* Traces helpers */
    RUN_SUITE(traces);

    /* Config link */
    RUN_SUITE(configlink);

    /* Infrastructure scanning */
    RUN_SUITE(infrascan);

    /* CLI (install, update, config) */
    RUN_SUITE(cli);

    /* System info + worker pool (parallelism) */
    RUN_SUITE(system_info);
    RUN_SUITE(worker_pool);

    /* Parallel pipeline */
    RUN_SUITE(parallel);

    /* mem + arena + slab integration */
    RUN_SUITE(mem);

    /* UI (config, embedded assets, layout) */
    RUN_SUITE(ui);

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
    RUN_SUITE(httpd);

    /* Security defenses */
    RUN_SUITE(security);

    /* YAML parser */
    RUN_SUITE(yaml);

    /* SimHash / SIMILAR_TO */
    RUN_SUITE(simhash);

    /* Stack overflow regression (GitHub #199) */
    RUN_SUITE(stack_overflow);

    /* Integration (end-to-end) */
    RUN_SUITE(integration);

    /* Per-language graph contracts (node/edge types, attribution, no-crash) */
    RUN_SUITE(lang_contract);
    RUN_SUITE(edge_imports);
    RUN_SUITE(edge_structural);
    RUN_SUITE(lsp_resolution_probe);
    RUN_SUITE(node_creation_probe);
    RUN_SUITE(edge_types_probe);
    RUN_SUITE(convergence_probe);
    RUN_SUITE(matrix_known_classes);
    RUN_SUITE(matrix_new_constructs);
    RUN_SUITE(grammar_probe_a);
    RUN_SUITE(grammar_probe_b);
    RUN_SUITE(grammar_probe_c);
    RUN_SUITE(grammar_probe_d);
    RUN_SUITE(grammar_probe_e);
    RUN_SUITE(grammar_probe_f);
    RUN_SUITE(grammar_probe_g);

    RUN_SUITE(incremental);

    /* Release process-lifetime caches so LeakSanitizer reports no leaks. */
    cbm_kind_in_set_free_cache();
    sqlite3_shutdown();
    TEST_SUMMARY();
}
