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

/* Forward declarations of suite functions */
extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_platform(void);
extern void suite_extraction(void);
extern void suite_ac(void);
extern void suite_store_nodes(void);
extern void suite_store_edges(void);
extern void suite_store_search(void);
extern void suite_cypher(void);
extern void suite_mcp(void);
extern void suite_language(void);
extern void suite_userconfig(void);
extern void suite_gitignore(void);
extern void suite_discover(void);
extern void suite_graph_buffer(void);
extern void suite_registry(void);
extern void suite_pipeline(void);
extern void suite_watcher(void);
extern void suite_lz4(void);
extern void suite_sqlite_writer(void);
extern void suite_go_lsp(void);
extern void suite_c_lsp(void);
extern void suite_store_arch(void);
extern void suite_store_bulk(void);
extern void suite_httplink(void);
extern void suite_traces(void);
extern void suite_configlink(void);
extern void suite_infrascan(void);
extern void suite_cli(void);
extern void suite_system_info(void);
extern void suite_worker_pool(void);
extern void suite_parallel(void);
extern void suite_mem(void);
extern void suite_ui(void);
extern void suite_security(void);
extern void suite_integration(void);

int main(void) {
    printf("\n  codebase-memory-mcp  C test suite\n");

    /* Foundation */
    RUN_SUITE(arena);
    RUN_SUITE(hash_table);
    RUN_SUITE(dyn_array);
    RUN_SUITE(str_intern);
    RUN_SUITE(log);
    RUN_SUITE(str_util);
    RUN_SUITE(platform);

    /* Existing C code regression tests */
    RUN_SUITE(ac);
    RUN_SUITE(extraction);

    /* Store (M5) */
    RUN_SUITE(store_nodes);
    RUN_SUITE(store_edges);
    RUN_SUITE(store_search);
    RUN_SUITE(store_bulk);

    /* Cypher (M6) */
    RUN_SUITE(cypher);

    /* MCP Server (M9) */
    RUN_SUITE(mcp);

    /* Discover (M2) */
    RUN_SUITE(language);
    RUN_SUITE(userconfig);
    RUN_SUITE(gitignore);
    RUN_SUITE(discover);

    /* Graph Buffer (M7) */
    RUN_SUITE(graph_buffer);

    /* Pipeline (M8) */
    RUN_SUITE(registry);
    RUN_SUITE(pipeline);

    /* Watcher (M10) */
    RUN_SUITE(watcher);

    /* LZ4 + SQLite writer */
    RUN_SUITE(lz4);
    RUN_SUITE(sqlite_writer);

    /* LSP resolvers */
    RUN_SUITE(go_lsp);
    RUN_SUITE(c_lsp);

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

    /* Security defenses */
    RUN_SUITE(security);

    /* Integration (end-to-end) */
    RUN_SUITE(integration);

    TEST_SUMMARY();
}
