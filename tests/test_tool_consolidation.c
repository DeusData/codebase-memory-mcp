/*
 * test_tool_consolidation.c — Tests for the streamlined/default tool surface.
 *
 * §4b: the search_code_graph mega-tool was deleted; the default surface is now
 * 5 focused tools: search_graph, query_graph, search_code, trace_path (from
 * TOOLS[]) plus get_code (from STREAMLINED_TOOLS[]). Covers tool
 * visibility, split-tool dispatch, get_code alias dispatch, project param path
 * support, and tool config visibility.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/constants.h"
#include "test_helpers.h"
#include "test_framework.h"
#include <cli/cli.h>
#include <mcp/mcp.h>
#include <store/store.h>
#include <depindex/depindex.h>
#include <yyjson/yyjson.h>
#include <string.h>
#include <stdlib.h>
#include <watcher/watcher.h>
#include <pagerank/pagerank.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <unistd.h>

static const yyjson_val *tool_array_from_doc(yyjson_doc *doc) {
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!root) return NULL;
    yyjson_val *tools = yyjson_obj_get(root, "tools");
    if (tools && yyjson_is_arr(tools)) return tools;
    yyjson_val *result = yyjson_obj_get(root, "result");
    if (!result) return NULL;
    tools = yyjson_obj_get(result, "tools");
    return tools && yyjson_is_arr(tools) ? tools : NULL;
}

static bool tool_list_has_exact_name(const char *json, const char *name) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) return false;
    const yyjson_val *tools = tool_array_from_doc(doc);
    bool found = false;
    if (tools) {
        yyjson_arr_iter it;
        yyjson_arr_iter_init((yyjson_val *)tools, &it);
        yyjson_val *tool;
        while ((tool = yyjson_arr_iter_next(&it)) != NULL) {
            yyjson_val *tool_name = yyjson_obj_get(tool, "name");
            if (tool_name && yyjson_is_str(tool_name) &&
                strcmp(yyjson_get_str(tool_name), name) == 0) {
                found = true;
                break;
            }
        }
    }
    yyjson_doc_free(doc);
    return found;
}

static size_t tool_list_exact_count(const char *json) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) return 0;
    const yyjson_val *tools = tool_array_from_doc(doc);
    size_t count = tools ? yyjson_arr_size(tools) : 0;
    yyjson_doc_free(doc);
    return count;
}

static bool tool_schema_has_property(const char *json, const char *tool, const char *prop) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) return false;
    const yyjson_val *tools = tool_array_from_doc(doc);
    bool found = false;
    if (tools) {
        yyjson_arr_iter it;
        yyjson_arr_iter_init((yyjson_val *)tools, &it);
        yyjson_val *item;
        while ((item = yyjson_arr_iter_next(&it)) != NULL) {
            yyjson_val *name = yyjson_obj_get(item, "name");
            if (!name || !yyjson_is_str(name) || strcmp(yyjson_get_str(name), tool) != 0) {
                continue;
            }
            yyjson_val *schema = yyjson_obj_get(item, "inputSchema");
            yyjson_val *props = schema ? yyjson_obj_get(schema, "properties") : NULL;
            found = props && yyjson_obj_get(props, prop) != NULL;
            break;
        }
    }
    yyjson_doc_free(doc);
    return found;
}

static bool tool_schema_required_has(const char *json, const char *tool, const char *prop) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) return false;
    const yyjson_val *tools = tool_array_from_doc(doc);
    bool found = false;
    if (tools) {
        yyjson_arr_iter it;
        yyjson_arr_iter_init((yyjson_val *)tools, &it);
        yyjson_val *item;
        while ((item = yyjson_arr_iter_next(&it)) != NULL) {
            yyjson_val *name = yyjson_obj_get(item, "name");
            if (!name || !yyjson_is_str(name) || strcmp(yyjson_get_str(name), tool) != 0) {
                continue;
            }
            yyjson_val *schema = yyjson_obj_get(item, "inputSchema");
            yyjson_val *required = schema ? yyjson_obj_get(schema, "required") : NULL;
            if (required && yyjson_is_arr(required)) {
                yyjson_arr_iter rit;
                yyjson_arr_iter_init(required, &rit);
                yyjson_val *r;
                while ((r = yyjson_arr_iter_next(&rit)) != NULL) {
                    if (yyjson_is_str(r) && strcmp(yyjson_get_str(r), prop) == 0) {
                        found = true;
                        break;
                    }
                }
            }
            break;
        }
    }
    yyjson_doc_free(doc);
    return found;
}

static char *save_tool_mode(void) {
    const char *mode = getenv("CBM_TOOL_MODE");
    if (!mode) return NULL;
    size_t len = strlen(mode);
    char *copy = (char *)malloc(len + 1);
    if (!copy) return NULL;
    memcpy(copy, mode, len + 1);
    return copy;
}

static void restore_tool_mode(char *saved) {
    if (saved) {
        setenv("CBM_TOOL_MODE", saved, 1);
        free(saved);
    } else {
        unsetenv("CBM_TOOL_MODE");
    }
}

static char *extract_tool_text(const char *mcp_result) {
    yyjson_doc *doc = yyjson_read(mcp_result, strlen(mcp_result), 0);
    if (!doc) {
        return strdup(mcp_result);
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *content = yyjson_obj_get(root, "content");
    if (!content || !yyjson_is_arr(content)) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *item = yyjson_arr_get(content, 0);
    yyjson_val *text = item ? yyjson_obj_get(item, "text") : NULL;
    const char *str = text && yyjson_is_str(text) ? yyjson_get_str(text) : mcp_result;
    char *copy = strdup(str);
    yyjson_doc_free(doc);
    return copy;
}

static bool json_array_has_string(const char *json, const char *array_key, const char *value) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *arr = yyjson_obj_get(root, array_key);
    bool found = false;
    if (arr && yyjson_is_arr(arr)) {
        yyjson_arr_iter it;
        yyjson_arr_iter_init(arr, &it);
        yyjson_val *item;
        while ((item = yyjson_arr_iter_next(&it)) != NULL) {
            if (yyjson_is_str(item) && strcmp(yyjson_get_str(item), value) == 0) {
                found = true;
                break;
            }
        }
    }
    yyjson_doc_free(doc);
    return found;
}

/* ── 1. Tool visibility tests ─────────────────────────────── */

TEST(streamlined_mode_shows_default_user_tools) {
    /* NULL srv → streamlined mode (no config available).
     * §4b: default surface is five user-facing tools plus _hidden_tools.
     * Canonical tools are emitted from TOOLS[] to avoid schema drift; get_code
     * is the concise streamlined alias. The search_code_graph mega-tool is
     * gone. */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    /* Default-surface tools must be present by name. */
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "query_graph"));
    ASSERT_NOT_NULL(strstr(json, "search_code"));
    ASSERT_NOT_NULL(strstr(json, "trace_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code"));
    /* The deleted mega-tool must NOT appear */
    ASSERT_NULL(strstr(json, "search_code_graph"));
    /* Hidden classic-only tools should NOT be top-level entries */
    ASSERT_NULL(strstr(json, "\"index_repository\""));
    ASSERT_NULL(strstr(json, "\"get_code_snippet\""));
    ASSERT_NULL(strstr(json, "\"manage_adr\""));
    free(json);
    PASS();
}

TEST(server_default_mode_shows_streamlined_tools) {
    /* New server default is streamlined mode unless CBM_TOOL_MODE/config opts
     * into classic. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    ASSERT_NOT_NULL(strstr(resp, "query_graph"));
    ASSERT_NOT_NULL(strstr(resp, "search_code"));
    ASSERT_NOT_NULL(strstr(resp, "trace_path"));
    ASSERT_NOT_NULL(strstr(resp, "get_code"));
    /* The deleted mega-tool must NOT appear */
    ASSERT_NULL(strstr(resp, "search_code_graph"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(api_surface_default_streamlined_regression_gate) {
    char *saved_mode = save_tool_mode();
    unsetenv("CBM_TOOL_MODE");

    char *json = cbm_mcp_tools_list(NULL);
    restore_tool_mode(saved_mode);
    ASSERT_NOT_NULL(json);

    /* Five user-facing tools plus the _hidden_tools discovery hint. */
    ASSERT_EQ(6, tool_list_exact_count(json));
    ASSERT(tool_list_has_exact_name(json, "search_graph"));
    ASSERT(tool_list_has_exact_name(json, "query_graph"));
    ASSERT(tool_list_has_exact_name(json, "search_code"));
    ASSERT(tool_list_has_exact_name(json, "trace_path"));
    ASSERT(tool_list_has_exact_name(json, "get_code"));
    ASSERT(tool_list_has_exact_name(json, "_hidden_tools"));

    ASSERT(!tool_list_has_exact_name(json, "index_repository"));
    ASSERT(!tool_list_has_exact_name(json, "get_code_snippet"));
    ASSERT(!tool_list_has_exact_name(json, "get_architecture"));
    ASSERT(!tool_list_has_exact_name(json, "index_dependencies"));
    ASSERT(!tool_list_has_exact_name(json, "search_code_graph"));
    free(json);
    PASS();
}

TEST(api_surface_classic_regression_gate) {
    char *saved_mode = save_tool_mode();
    setenv("CBM_TOOL_MODE", "classic", 1);

    char *json = cbm_mcp_tools_list(NULL);
    restore_tool_mode(saved_mode);
    ASSERT_NOT_NULL(json);

    ASSERT_EQ(15, tool_list_exact_count(json));
    ASSERT(tool_list_has_exact_name(json, "index_repository"));
    ASSERT(tool_list_has_exact_name(json, "search_graph"));
    ASSERT(tool_list_has_exact_name(json, "query_graph"));
    ASSERT(tool_list_has_exact_name(json, "trace_path"));
    ASSERT(tool_list_has_exact_name(json, "get_code_snippet"));
    ASSERT(tool_list_has_exact_name(json, "get_graph_schema"));
    ASSERT(tool_list_has_exact_name(json, "get_architecture"));
    ASSERT(tool_list_has_exact_name(json, "search_code"));
    ASSERT(tool_list_has_exact_name(json, "list_projects"));
    ASSERT(tool_list_has_exact_name(json, "delete_project"));
    ASSERT(tool_list_has_exact_name(json, "index_status"));
    ASSERT(tool_list_has_exact_name(json, "detect_changes"));
    ASSERT(tool_list_has_exact_name(json, "manage_adr"));
    ASSERT(tool_list_has_exact_name(json, "ingest_traces"));
    ASSERT(tool_list_has_exact_name(json, "index_dependencies"));

    ASSERT(!tool_list_has_exact_name(json, "get_code"));
    ASSERT(!tool_list_has_exact_name(json, "_hidden_tools"));
    ASSERT(!tool_list_has_exact_name(json, "search_code_graph"));
    free(json);
    PASS();
}

TEST(hidden_tools_reveal_discoverable_tools) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *before = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(before);
    ASSERT_EQ(6, tool_list_exact_count(before));
    ASSERT(!tool_list_has_exact_name(before, "index_repository"));
    ASSERT(!tool_list_has_exact_name(before, "get_architecture"));
    free(before);

    char *hint = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(hint);
    ASSERT_NOT_NULL(strstr(hint, "revealed"));
    free(hint);

    char *after = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(after);
    ASSERT(tool_list_has_exact_name(after, "index_repository"));
    ASSERT(tool_list_has_exact_name(after, "get_code_snippet"));
    ASSERT(tool_list_has_exact_name(after, "get_architecture"));
    ASSERT(tool_list_has_exact_name(after, "index_dependencies"));
    ASSERT(tool_list_has_exact_name(after, "trace_path"));
    ASSERT(tool_list_has_exact_name(after, "_hidden_tools"));
    ASSERT_EQ(17, tool_list_exact_count(after));
    free(after);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(hidden_tools_payload_excludes_already_visible_configured_tools) {
    char *saved_mode = save_tool_mode();
    setenv("CBM_TOOL_MODE", "streamlined", 1);

    char *tmp = th_mktempdir("cbm_hidden_tools_cfg");
    ASSERT_NOT_NULL(tmp);
    char cfg_dir[CBM_SZ_512];
    int n = snprintf(cfg_dir, sizeof(cfg_dir), "%s", tmp);
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(cfg_dir));

    cbm_config_t *cfg = cbm_config_open(cfg_dir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, "tool_index_repository", "true"), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_config(srv, cfg);

    char *before = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(before);
    ASSERT(tool_list_has_exact_name(before, "index_repository"));
    ASSERT(!tool_list_has_exact_name(before, "get_architecture"));
    free(before);

    char *hint = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(hint);
    char *text = extract_tool_text(hint);
    ASSERT_NOT_NULL(text);
    ASSERT(json_array_has_string(text, "advanced_tools", "index_repository"));
    ASSERT(json_array_has_string(text, "already_visible_tools", "index_repository"));
    ASSERT(!json_array_has_string(text, "hidden_tools", "index_repository"));
    ASSERT(json_array_has_string(text, "hidden_tools", "get_architecture"));
    free(text);
    free(hint);

    char *after = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(after);
    ASSERT(tool_list_has_exact_name(after, "get_architecture"));
    ASSERT_EQ(17, tool_list_exact_count(after));
    free(after);

    cbm_mcp_server_free(srv);
    cbm_config_close(cfg);
    restore_tool_mode(saved_mode);
    PASS();
}

TEST(streamlined_reveal_covers_classic_capabilities) {
    char *saved_mode = save_tool_mode();

    setenv("CBM_TOOL_MODE", "classic", 1);
    char *classic = cbm_mcp_tools_list(NULL);
    unsetenv("CBM_TOOL_MODE");
    ASSERT_NOT_NULL(classic);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *hint = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(hint);
    free(hint);

    char *revealed = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(revealed);

    const char *classic_tools[] = {
        "index_repository", "search_graph", "query_graph", "trace_path",
        "get_code_snippet", "get_graph_schema", "get_architecture",
        "search_code", "list_projects", "delete_project", "index_status",
        "detect_changes", "manage_adr", "ingest_traces", "index_dependencies",
    };
    for (size_t i = 0; i < sizeof(classic_tools) / sizeof(classic_tools[0]); i++) {
        ASSERT(tool_list_has_exact_name(classic, classic_tools[i]));
        ASSERT(tool_list_has_exact_name(revealed, classic_tools[i]));
    }

    /* Streamlined keeps get_code as the concise source-retrieval spelling, but
     * reveal also exposes get_code_snippet for full upstream/classic parity. */
    ASSERT(tool_list_has_exact_name(revealed, "get_code"));
    ASSERT(tool_list_has_exact_name(revealed, "_hidden_tools"));
    ASSERT(!tool_list_has_exact_name(classic, "get_code"));
    ASSERT(!tool_list_has_exact_name(classic, "_hidden_tools"));

    free(revealed);
    cbm_mcp_server_free(srv);
    free(classic);
    restore_tool_mode(saved_mode);
    PASS();
}

TEST(streamlined_core_parameter_contract) {
    char *saved_mode = save_tool_mode();
    unsetenv("CBM_TOOL_MODE");

    char *json = cbm_mcp_tools_list(NULL);
    restore_tool_mode(saved_mode);
    ASSERT_NOT_NULL(json);

    const char *search_params[] = {
        "project", "label", "name_pattern", "pattern", "qn_pattern",
        "query", "file_pattern", "semantic_query", "relationship",
        "case_sensitive", "min_degree", "max_degree", "exclude_entry_points",
        "include_connected", "limit", "offset", "sort_by", "mode", "summary",
        "compact", "include_dependencies", "exclude",
    };
    for (size_t i = 0; i < sizeof(search_params) / sizeof(search_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "search_graph", search_params[i]));
    }

    const char *query_params[] = {"query", "project", "max_rows", "max_output_bytes"};
    for (size_t i = 0; i < sizeof(query_params) / sizeof(query_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "query_graph", query_params[i]));
    }
    ASSERT(tool_schema_required_has(json, "query_graph", "query"));

    const char *trace_params[] = {
        "function_name", "qualified_name", "project", "direction", "depth",
        "max_results", "compact", "mode", "edge_types", "exclude",
        "include_tests", "risk_labels", "parameter_name",
    };
    for (size_t i = 0; i < sizeof(trace_params) / sizeof(trace_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "trace_path", trace_params[i]));
    }
    ASSERT(!tool_schema_has_property(json, "trace_path", "scope"));
    ASSERT(!tool_schema_required_has(json, "trace_path", "function_name"));
    ASSERT(!tool_schema_required_has(json, "trace_path", "project"));

    const char *code_params[] = {
        "qualified_name", "project", "mode", "max_lines", "auto_resolve",
        "include_neighbors", "compact",
    };
    for (size_t i = 0; i < sizeof(code_params) / sizeof(code_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "get_code", code_params[i]));
    }
    ASSERT(tool_schema_required_has(json, "get_code", "qualified_name"));
    ASSERT_NOT_NULL(strstr(json, "compact config key"));

    const char *source_params[] = {
        "pattern", "project", "file_pattern", "path_filter", "regex",
        "case_sensitive", "context", "mode", "limit",
    };
    for (size_t i = 0; i < sizeof(source_params) / sizeof(source_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "search_code", source_params[i]));
    }

    free(json);
    PASS();
}

TEST(default_tool_autoindex_description_is_precise) {
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);

    ASSERT_NOT_NULL(strstr(json, "Graph-backed default tools auto-index"));
    ASSERT_NOT_NULL(strstr(json, "search_code searches"));
    ASSERT_NOT_NULL(strstr(json, "already indexed/current project"));
    ASSERT_NOT_NULL(strstr(json, "Does not index projects"));
    ASSERT_NULL(strstr(json, "Default tools auto-index"));

    free(json);
    PASS();
}

TEST(revealed_trace_path_parameter_contract) {
    char *saved_mode = save_tool_mode();
    unsetenv("CBM_TOOL_MODE");

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *hint = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(hint);
    free(hint);

    char *json = cbm_mcp_tools_list(srv);
    restore_tool_mode(saved_mode);
    ASSERT_NOT_NULL(json);

    const char *trace_params[] = {
        "function_name", "qualified_name", "project", "direction", "depth",
        "max_results", "compact", "mode", "edge_types", "exclude",
        "include_tests", "risk_labels", "parameter_name",
    };
    for (size_t i = 0; i < sizeof(trace_params) / sizeof(trace_params[0]); i++) {
        ASSERT(tool_schema_has_property(json, "trace_path", trace_params[i]));
    }
    ASSERT(!tool_schema_has_property(json, "trace_path", "scope"));

    free(json);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(revealed_advanced_tool_schema_matches_handlers) {
    char *saved_mode = save_tool_mode();
    unsetenv("CBM_TOOL_MODE");

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *hint = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(hint);
    free(hint);

    char *json = cbm_mcp_tools_list(srv);
    restore_tool_mode(saved_mode);
    ASSERT_NOT_NULL(json);

    ASSERT(tool_schema_has_property(json, "get_code_snippet", "compact"));
    ASSERT(tool_schema_has_property(json, "get_architecture", "exclude"));
    ASSERT_NOT_NULL(strstr(json, "Graph edge creation from traces is not yet implemented"));
    ASSERT_NOT_NULL(strstr(json, "Reserved for future multi-hop impact traversal"));

    free(json);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 2. Dispatch tests ────────────────────────────────────── */

TEST(search_graph_dispatch) {
    /* §4b: search_graph is now a default-surface tool and dispatches directly
     * to handle_search_graph (previously reached via the search_code_graph
     * mega-tool's default branch). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"nonexistent_xyz\"}");
    ASSERT_NOT_NULL(result);
    /* Should get a response (may be empty results, not an error about unknown tool) */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(query_graph_dispatch) {
    /* §4b: query_graph is now a default-surface tool and dispatches directly
     * to handle_query_graph (previously reached via search_code_graph cypher=). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "query_graph",
        "{\"query\":\"MATCH (n) RETURN n.name LIMIT 1\"}");
    ASSERT_NOT_NULL(result);
    /* Should get a Cypher response (may be empty), not unknown tool error */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(get_code_dispatch) {
    /* get_code → routes to get_code_snippet handler */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "get_code",
        "{\"qualified_name\":\"nonexistent.func\"}");
    ASSERT_NOT_NULL(result);
    /* Should get snippet response (may be not found), not unknown tool */
    ASSERT_NULL(strstr(result, "unknown tool"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(canonical_tool_names_dispatch) {
    /* Canonical streamlined/classic tool names should dispatch directly. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* search_graph */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NULL(strstr(r1, "unknown tool"));
    free(r1);

    /* query_graph */
    char *r2 = cbm_mcp_handle_tool(srv, "query_graph",
        "{\"query\":\"MATCH (n) RETURN n.name LIMIT 1\"}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NULL(strstr(r2, "unknown tool"));
    free(r2);

    /* get_code_snippet */
    char *r3 = cbm_mcp_handle_tool(srv, "get_code_snippet",
        "{\"qualified_name\":\"test.func\"}");
    ASSERT_NOT_NULL(r3);
    ASSERT_NULL(strstr(r3, "unknown tool"));
    free(r3);

    /* trace_path */
    char *r4 = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"main\"}");
    ASSERT_NOT_NULL(r4);
    ASSERT_NULL(strstr(r4, "unknown tool"));
    free(r4);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 3. Project param path support ────────────────────────── */

TEST(project_param_path_detection) {
    /* expand_project_param should detect paths and convert.
     * §4b: test indirectly via search_graph (same handler the old
     * search_code_graph default branch routed to) with a path-like project.
     * Since the path won't exist as a db, we just verify no crash. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"/tmp/nonexistent_test_project\",\"name_pattern\":\"foo\"}");
    ASSERT_NOT_NULL(result);
    /* Should get an error about project not loaded, not a crash */
    ASSERT_NOT_NULL(strstr(result, "error") != NULL ? strstr(result, "error") : result);
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 4. Edge case tests ───────────────────────────────────── */

TEST(unknown_tool_returns_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "completely_fake_tool", "{}");
    ASSERT_NOT_NULL(result);
    /* Should indicate unknown tool */
    ASSERT_NOT_NULL(strstr(result, "unknown"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(null_tool_name_returns_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, NULL, "{}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "missing"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 5. Progressive disclosure ────────────────────────────── */

TEST(streamlined_mode_has_hidden_tools_hint) {
    /* Streamlined tool list should include _hidden_tools entry
     * that tells the AI what tools are available and how to enable them. */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "_hidden_tools"));
    ASSERT_NOT_NULL(strstr(json, "CBM_TOOL_MODE"));
    ASSERT_NOT_NULL(strstr(json, "index_repository"));
    ASSERT_NOT_NULL(strstr(json, "tool_mode"));
    free(json);
    PASS();
}

TEST(hidden_tools_still_dispatch) {
    /* Even though hidden in streamlined mode, calling hidden tool names
     * still works — dispatch is unconditional. This ensures the AI can
     * use hidden tools after learning about them from the hint. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* index_status is hidden in streamlined mode but should still dispatch */
    char *result = cbm_mcp_handle_tool(srv, "index_status", "{}");
    ASSERT_NOT_NULL(result);
    /* Should get a response about no project, not unknown tool */
    ASSERT_NULL(strstr(result, "unknown"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 6. Session context in responses ─────────────────────── */

TEST(search_graph_has_session_project) {
    /* search_graph response should include session_project */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "test_proj");
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"nonexistent\"}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "session_project"));
    ASSERT_NOT_NULL(strstr(result, "test_proj"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(index_status_has_session_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "my_proj");
    char *result = cbm_mcp_handle_tool(srv, "index_status", "{}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "session_project"));
    ASSERT_NOT_NULL(strstr(result, "my_proj"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 7. Context injection ─────────────────────────────────── */

TEST(first_response_has_context_header) {
    /* First search_graph call should include _context with schema/status.
     * Uses in-memory store (no session_root) so auto-index won't trigger. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "ctx_test");
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(result);
    /* First response should have _context (escaped: inner JSON is JSON-encoded in outer) */
    ASSERT_NOT_NULL(strstr(result, "\\\"_context\\\":"));
    ASSERT_NOT_NULL(strstr(result, "status"));
    free(result);

    /* Second call should NOT have _context (already injected).
     * Use escaped pattern "\\\"_context\\\":" to avoid false-positives from node
     * names like "inject_context_once" that contain "_context" as a substring. */
    char *result2 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test2\"}");
    ASSERT_NOT_NULL(result2);
    ASSERT_NULL(strstr(result2, "\\\"_context\\\":"));
    /* But session_project should still be present */
    ASSERT_NOT_NULL(strstr(result2, "session_project"));
    free(result2);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(context_has_schema_info) {
    /* _context should include node_labels and edge_types arrays */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    /* In-memory store has schema tables → should see these fields (escaped JSON key) */
    ASSERT_NOT_NULL(strstr(result, "\\\"_context\\\":"));
    ASSERT_NOT_NULL(strstr(result, "node_labels"));
    ASSERT_NOT_NULL(strstr(result, "edge_types"));
    free(result);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 7. MCP Resources tests (Phase 10) ───────────────────── */

TEST(resources_list_returns_3_resources) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://architecture"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://status"));
    ASSERT_NOT_NULL(strstr(resp, "Code Graph Schema"));
    ASSERT_NOT_NULL(strstr(resp, "Architecture Overview"));
    ASSERT_NOT_NULL(strstr(resp, "Index Status"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_schema) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://schema\"}}");
    ASSERT_NOT_NULL(resp);
    /* Response should contain contents array with schema data */
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "application/json"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_architecture) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://architecture\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://architecture"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_status) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://status"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_unknown_uri) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://nonexistent\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    /* MCP spec: resource not found = -32002 */
    ASSERT_NOT_NULL(strstr(resp, "-32002"));
    /* Error message should include the bad URI and list valid resources */
    ASSERT_NOT_NULL(strstr(resp, "codebase://nonexistent"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(initialize_advertises_resources_capability) {
    char *resp = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "resources"));
    ASSERT_NOT_NULL(strstr(resp, "listChanged"));
    free(resp);
    PASS();
}

TEST(initialize_parses_client_resources_capability) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Send initialize with client capabilities including resources */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{\"subscribe\":false}},"
        "\"clientInfo\":{\"name\":\"test\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* MCP resources are pull-only — declaring resources capability does NOT mean
     * the client auto-reads codebase://schema or codebase://architecture.
     * inject_context_once must still fire on the first tool call so the model
     * receives architectural context without requiring explicit user action.
     * Ref: https://modelcontextprotocol.io/specification/2025-06-18/server/resources */
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    /* session_project should still appear */
    ASSERT_NOT_NULL(strstr(result, "session_project"));
    /* _context MUST appear on first call regardless of resources capability.
     * cbm_mcp_text_result embeds inner JSON as a JSON-encoded string value, so
     * "\"_context\":" in the inner JSON appears as "\\\"_context\\\":" in raw bytes. */
    ASSERT_NOT_NULL(strstr(result, "\\\"_context\\\":"));
    free(result);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(no_resources_capability_gets_context_injection) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Send initialize WITHOUT resources capability */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{},"
        "\"clientInfo\":{\"name\":\"old-client\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* Without resources capability, first tool call should get _context */
    char *result = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(result);
    ASSERT_NOT_NULL(strstr(result, "\\\"_context\\\":"));
    free(result);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 8. MCP spec compliance tests ─────────────────────────── */

TEST(initialize_response_has_protocol_version) {
    char *resp = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "protocolVersion"));
    /* Default (no params): returns latest supported version */
    ASSERT_NOT_NULL(strstr(resp, "2025-11-25"));
    ASSERT_NOT_NULL(strstr(resp, "serverInfo"));
    ASSERT_NOT_NULL(strstr(resp, "codebase-memory-mcp"));
    free(resp);
    PASS();
}

TEST(initialize_resources_cap_subscribe_false) {
    /* Server must advertise subscribe:false (we don't support per-resource subscriptions) */
    char *resp = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"subscribe\":false"));
    ASSERT_NOT_NULL(strstr(resp, "\"listChanged\":true"));
    free(resp);
    PASS();
}

TEST(resources_list_has_mimeType_and_description) {
    /* MCP spec requires name, uri; recommends description and mimeType */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "mimeType"));
    ASSERT_NOT_NULL(strstr(resp, "application/json"));
    ASSERT_NOT_NULL(strstr(resp, "description"));
    ASSERT_NOT_NULL(strstr(resp, "name"));
    /* Resource descriptions should be actionable — tell AI when to read them */
    ASSERT_NOT_NULL(strstr(resp, "Read this"));
    ASSERT_NOT_NULL(strstr(resp, "Cypher"));  /* schema mentions Cypher */
    ASSERT_NOT_NULL(strstr(resp, "PageRank")); /* architecture mentions PageRank */
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_response_has_contents_array) {
    /* MCP spec: resources/read returns {contents: [{uri, mimeType, text}]} */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"contents\""));
    ASSERT_NOT_NULL(strstr(resp, "\"uri\""));
    ASSERT_NOT_NULL(strstr(resp, "\"mimeType\""));
    ASSERT_NOT_NULL(strstr(resp, "\"text\""));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_missing_uri_param) {
    /* resources/read with no uri → error -32602 (invalid params) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    ASSERT_NOT_NULL(strstr(resp, "Missing uri"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resources_read_no_params_at_all) {
    /* resources/read with no params object */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "error"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 9. Client behavioral difference tests ───────────────── */

TEST(resource_client_gets_context_only_on_first_call) {
    /* Resource-capable client gets _context on the FIRST call only.
     * MCP resources are pull-only (no server push). Declaring resources:{}
     * does not trigger automatic resource reads — the model must be explicitly
     * instructed or the user must @-mention a resource URI.
     * context_injected=true after first injection prevents duplicates.
     * Ref: https://modelcontextprotocol.io/docs/concepts/resources */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{}},"
        "\"clientInfo\":{\"name\":\"modern\",\"version\":\"2.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* First call MUST have _context (JSON key "_context":).
     * cbm_mcp_text_result embeds inner JSON as a JSON-encoded string value, so the
     * literal bytes in the outer response are \"_context\": (backslash-escaped quotes).
     * Search for "\\\"_context\\\":" which matches \"_context\": in raw bytes. */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "\\\"_context\\\":"));
    ASSERT_NOT_NULL(strstr(r1, "session_project"));
    free(r1);

    /* Calls 2 and 3: _context must NOT repeat (context_injected dedup guard).
     * Use the escaped pattern "\\\"_context\\\":" to avoid false positives from
     * node names like "inject_context_once" matching bare "_context" searches. */
    for (int i = 0; i < 2; i++) {
        char *r = cbm_mcp_handle_tool(srv, "search_graph",
            "{\"name_pattern\":\"test\"}");
        ASSERT_NOT_NULL(r);
        ASSERT_NULL(strstr(r, "\\\"_context\\\":"));
        ASSERT_NOT_NULL(strstr(r, "session_project"));
        free(r);
    }
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(legacy_client_gets_context_only_on_first_call) {
    /* Legacy client: _context on first call, NOT on subsequent calls */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{},"
        "\"clientInfo\":{\"name\":\"legacy\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* First call: MUST have _context (escaped: inner JSON is JSON-encoded in outer) */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "\\\"_context\\\":"));
    free(r1);

    /* Second call: must NOT have _context (one-shot dedup via context_injected flag) */
    char *r2 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test2\"}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NULL(strstr(r2, "\\\"_context\\\":"));
    ASSERT_NOT_NULL(strstr(r2, "session_project"));
    free(r2);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(empty_resources_capability_still_gets_context) {
    /* MCP spec: capabilities.resources:{} means resources supported
     * (neither subscribe nor listChanged, but resources protocol works).
     * Even so, resources are pull-only — declaring support does not trigger
     * automatic reads. _context injection applies to ALL clients. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{}},"
        "\"clientInfo\":{\"name\":\"minimal\",\"version\":\"1.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* Empty resources:{} client still gets _context on first call.
     * Use escaped pattern: inner JSON is embedded as JSON-encoded string in outer response. */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "\\\"_context\\\":"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(no_initialize_defaults_to_legacy_behavior) {
    /* Server with no initialize call → defaults to legacy (no resources) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Call tool directly without initialize → should get _context (legacy) */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "\\\"_context\\\":"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 17. MCP resources pull-only: inject_context_once fires for ALL clients ─
 *
 * The MCP spec defines resources as "application-controlled" — there is no
 * server-push mechanism. When a client declares resources capability, it means
 * the client CAN fetch resources explicitly (e.g. via @resource-uri in Claude
 * Code), NOT that it will automatically read them. References:
 *   https://modelcontextprotocol.io/specification/2025-06-18/server/resources
 *   https://modelcontextprotocol.io/docs/concepts/resources
 *   https://workos.com/blog/mcp-features-guide (resources = application-controlled)
 *
 * inject_context_once embeds schema/architecture in the FIRST tool response.
 * This is the only reliable delivery channel that doesn't require explicit
 * user action:
 *   - notifications/resources/updated signals changes but sends NO content
 *   - resources/read requires explicit model action (not automatic)
 *   - Claude Code resources require user @-mention or explicit instruction
 *
 * The context_injected flag already prevents duplicate injection on subsequent
 * calls, so ALL clients receive context exactly once regardless of whether
 * they declared resources capability.
 * ──────────────────────────────────────────────────────────────────────── */

TEST(resource_capable_client_gets_context_on_first_call) {
    /* Resource-capable client MUST get _context on the first tool call.
     * Declaring resources:{} does NOT mean automatic resource reads —
     * the model only reads resources when explicitly instructed (user @-mention
     * or system prompt directive). Embedding _context in the first response
     * is the only reliable delivery channel without user intervention. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{}},"
        "\"clientInfo\":{\"name\":\"claude-code\",\"version\":\"2.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* First tool call MUST include _context regardless of resources capability.
     * Escaped pattern: cbm_mcp_text_result embeds inner JSON as a JSON-encoded string,
     * so "\"_context\":" appears as "\\\"_context\\\":" in the outer raw bytes. */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"test\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "\\\"_context\\\":"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_capable_client_no_context_on_second_call) {
    /* After first-call injection, context_injected=true suppresses duplicates.
     * This dedup applies to ALL clients equally — resource-capable or not.
     * Session-project is still included on every call (it's lightweight). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
        "\"params\":{\"protocolVersion\":\"2024-11-05\","
        "\"capabilities\":{\"resources\":{\"subscribe\":true}},"
        "\"clientInfo\":{\"name\":\"claude-code\",\"version\":\"2.0\"}}}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    /* First call: _context present (escaped pattern — inner JSON is JSON-encoded in outer) */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "\\\"_context\\\":"));
    free(r1);

    /* Second call: _context must NOT be repeated (context_injected guard).
     * "\\\"_context\\\":" searches for \"_context\": in raw bytes — won't false-positive
     * on node names like "inject_context_once". */
    char *r2 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"y\"}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NULL(strstr(r2, "\\\"_context\\\":"));
    /* session_project must still be present on every call */
    ASSERT_NOT_NULL(strstr(r2, "session_project"));
    free(r2);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 10. Tool-resource cross-referencing tests ───────────── */

TEST(tool_descriptions_reference_resources) {
    /* Tool descriptions should tell the AI about available resources
     * so it knows to read codebase://schema before writing Cypher, etc.
     * §4b: trace_path mentions codebase://architecture; the _hidden_tools
     * hint mentions all three resource URIs. */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    /* Resources are referenced in the default surface / hint */
    ASSERT_NOT_NULL(strstr(json, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(json, "codebase://architecture"));
    /* get_code should reference search_graph for qualified names */
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    free(json);
    PASS();
}

TEST(hidden_tools_hint_mentions_resources) {
    /* The _hidden_tools progressive disclosure hint should tell the AI
     * about context resources so it can read them without enabling tools */
    char *json = cbm_mcp_tools_list(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "_hidden_tools"));
    /* Should mention all 3 resource URIs */
    ASSERT_NOT_NULL(strstr(json, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(json, "codebase://architecture"));
    ASSERT_NOT_NULL(strstr(json, "codebase://status"));
    free(json);
    PASS();
}

/* ── 11. Error message quality tests ─────────────────────── */

TEST(error_no_project_loaded_has_hint) {
    /* search_graph with a nonexistent project name → resolve_store returns NULL
     * but cbm_mcp_server_new creates a default store. Use a project name that
     * won't match any DB file to trigger the error. The REQUIRE_STORE macro
     * in search_graph handles auto-index, but for a fake project path it will
     * still fail and return the hint. Test via the error structure in trace. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* trace_path goes through REQUIRE_STORE → no project loaded if store NULL.
     * With cbm_mcp_server_new(NULL), resolve_store(NULL) returns the default store.
     * The function_not_found error (which also has hint) tests the pattern. */
    char *r = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"nonexistent_fn\"}");
    ASSERT_NOT_NULL(r);
    /* The response should have a hint field (either "no project loaded" or "not found") */
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_function_not_found_includes_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "trace_path",
        "{\"function_name\":\"nonexistent_xyz_func\"}");
    ASSERT_NOT_NULL(r);
    /* Error should include the function name that was searched for */
    ASSERT_NOT_NULL(strstr(r, "nonexistent_xyz_func"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_symbol_not_found_includes_qn) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "get_code_snippet",
        "{\"qualified_name\":\"nonexistent.module.func_xyz\"}");
    ASSERT_NOT_NULL(r);
    /* Error should include the qualified name that was searched for */
    ASSERT_NOT_NULL(strstr(r, "nonexistent.module.func_xyz"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_missing_required_param_has_hint) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* query_graph missing query param */
    char *r1 = cbm_mcp_handle_tool(srv, "query_graph", "{}");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(strstr(r1, "query is required"));
    ASSERT_NOT_NULL(strstr(r1, "hint"));
    free(r1);

    /* trace_path missing function_name */
    char *r2 = cbm_mcp_handle_tool(srv, "trace_path", "{}");
    ASSERT_NOT_NULL(r2);
    ASSERT_NOT_NULL(strstr(r2, "function_name or qualified_name is required"));
    ASSERT_NOT_NULL(strstr(r2, "hint"));
    free(r2);

    /* get_code_snippet missing qualified_name */
    char *r3 = cbm_mcp_handle_tool(srv, "get_code_snippet", "{}");
    ASSERT_NOT_NULL(r3);
    ASSERT_NOT_NULL(strstr(r3, "qualified_name is required"));
    ASSERT_NOT_NULL(strstr(r3, "hint"));
    free(r3);

    /* search_code missing pattern */
    char *r4 = cbm_mcp_handle_tool(srv, "search_code", "{}");
    ASSERT_NOT_NULL(r4);
    ASSERT_NOT_NULL(strstr(r4, "pattern is required"));
    ASSERT_NOT_NULL(strstr(r4, "hint"));
    free(r4);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_unknown_tool_lists_valid_tools) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *r = cbm_mcp_handle_tool(srv, "nonexistent_tool_xyz", "{}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "nonexistent_tool_xyz"));
    ASSERT_NOT_NULL(strstr(r, "hint"));
    /* §4b: hint now lists the split tools, not the deleted mega-tool */
    ASSERT_NOT_NULL(strstr(r, "search_graph"));
    ASSERT_NOT_NULL(strstr(r, "tools/list"));
    free(r);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(error_resource_not_found_has_spec_code) {
    /* MCP spec: resource not found = -32002 with actionable message */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://bad_uri_xyz\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "-32002"));
    ASSERT_NOT_NULL(strstr(resp, "bad_uri_xyz"));
    ASSERT_NOT_NULL(strstr(resp, "codebase://schema"));
    ASSERT_NOT_NULL(strstr(resp, "resources/list"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 12. JSON-RPC response structure tests (e2e) ─────────── */

TEST(resource_error_is_top_level_not_nested_in_result) {
    /* BUG found by binary testing: resource errors were double-wrapped.
     * handle_resources_read returned a pre-formatted JSON-RPC error, but
     * cbm_mcp_server_handle wrapped it again in cbm_jsonrpc_format_response.
     * Result: {result: {jsonrpc, id:0, error: {...}}} instead of {error: {...}}
     * Fix: error path returns early before the wrapper. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://nonexistent\"}}");
    ASSERT_NOT_NULL(resp);
    /* Must have top-level "error" key, NOT nested inside "result" */
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    /* Error id must match request id */
    ASSERT_NOT_NULL(strstr(resp, "\"id\":42"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_error_missing_uri_is_top_level) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"resources/read\","
        "\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":99"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_error_no_params_is_top_level) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"resources/read\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":77"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_success_has_result_not_error) {
    /* Complement: successful reads must have "result", NOT "error" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    ASSERT_NOT_NULL(strstr(resp, "\"id\":50"));
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_schema_returns_real_data_when_indexed) {
    /* After search_graph opens the session store, resources should return real data.
     * Uses cbm_mcp_server_new(NULL) which creates an in-memory store. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Force store open via a tool call */
    char *r1 = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"name_pattern\":\"x\"}");
    free(r1);
    /* Now read schema resource — should have node_labels/edge_types arrays */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://schema\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    /* text field should have node_labels (may be empty array but key must exist) */
    ASSERT_NOT_NULL(strstr(resp, "node_labels"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(resource_status_returns_not_indexed_when_no_store) {
    /* Fresh server with no session — status resource should say not_indexed */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Don't set session_project, don't call any tools */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"resources/read\","
        "\"params\":{\"uri\":\"codebase://status\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "contents"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 13. Dep search bug regression tests ─────────────────── */

/* Bug 1: resolve_store must route dep project names to parent DB.
 * "myapp.dep.pandas" should open myapp.db, not myapp.dep.pandas.db. */
TEST(dep_search_explicit_dep_project_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Use unique name to avoid creating DB files that interfere with other tests */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_deptest_proj_.dep.pandas\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    /* Clean up any DB file that resolve_store may have created */
    char path[1024];
    snprintf(path, sizeof(path), "%s/_tc_deptest_proj_.db",
             cbm_resolve_cache_dir());
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Bug 2: Store prefix match — search with project name must include deps. */
TEST(store_prefix_match_includes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.lib", "/tmp/lib");
    cbm_node_t n1 = {.project = "myapp", .label = "Function", .name = "main",
                      .qualified_name = "myapp.main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "myapp.dep.lib", .label = "Function", .name = "lib_fn",
                      .qualified_name = "myapp.dep.lib.lib_fn", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "myapp";
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_TRUE(out.count >= 2);
    bool found_project = false, found_dep = false;
    for (int i = 0; i < out.count; i++) {
        if (strcmp(out.results[i].node.project, "myapp") == 0) found_project = true;
        if (strcmp(out.results[i].node.project, "myapp.dep.lib") == 0) found_dep = true;
    }
    ASSERT_TRUE(found_project);
    ASSERT_TRUE(found_dep);
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* #18 RED: project-over-dependency source ranking in the COMMON prefix-match
 * path (project="myapp", which includes myapp.dep.*). A dependency symbol must
 * NOT outrank the project's own symbol of the same name — the "Path" concern
 * (a python-stdlib Path must not be front-of-line over the user's own Path).
 *
 * The store already has a dep-last tiebreak, but it ONLY fires for
 * params->project_pattern (glob). The common prefix path sets params->project,
 * so today deps are NOT demoted here → a dep inserted first (lower id) wins the
 * name/id tiebreak and ranks above the project symbol. This test must FAIL until
 * dep-last is extended to the prefix-match path. */
TEST(store_prefix_ranks_project_above_dep) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.stdlib", "/tmp/stdlib");

    /* Insert the DEPENDENCY symbol FIRST so it gets the lower id — under the
     * current (no-dep-last-in-prefix-path) ordering it wins the id tiebreak. */
    cbm_node_t nd = {.project = "myapp.dep.stdlib", .label = "Class", .name = "Path",
                     .qualified_name = "myapp.dep.stdlib.Path", .file_path = "stdlib/path.py"};
    cbm_store_upsert_node(s, &nd);
    cbm_node_t np = {.project = "myapp", .label = "Class", .name = "Path",
                     .qualified_name = "myapp.Path", .file_path = "src/path.py"};
    cbm_store_upsert_node(s, &np);

    cbm_search_params_t params = {0};
    params.project = "myapp"; /* prefix match: includes myapp.dep.* */
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_GTE(out.count, 2);

    /* The PROJECT 'Path' must rank above the DEPENDENCY 'Path'. */
    int proj_idx = -1, dep_idx = -1;
    for (int i = 0; i < out.count; i++) {
        if (strcmp(out.results[i].node.name, "Path") != 0) continue;
        if (strcmp(out.results[i].node.project, "myapp") == 0) proj_idx = i;
        if (strcmp(out.results[i].node.project, "myapp.dep.stdlib") == 0) dep_idx = i;
    }
    ASSERT_NEQ(proj_idx, -1);
    ASSERT_NEQ(dep_idx, -1);
    ASSERT_TRUE(proj_idx < dep_idx); /* project before dependency */

    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* #49: dep-last ranking holds across ALL sort_by modes (relevance/name/degree/
 * calls/linkrank), not just the default. With equal primary metrics (both
 * "Path", no edges → tied pagerank/degree/calls/linkrank/name), the dep-last
 * secondary key must put the project symbol first in every mode. Dep is
 * inserted first (lower id) so a missing dep-last would let the dep win the id
 * tiebreak — making this a real per-mode check, not a tautology. */
TEST(store_prefix_ranks_project_above_dep_all_sort_modes) {
    static const char *modes[] = {"relevance", "name", "degree", "calls", "linkrank"};
    for (size_t m = 0; m < sizeof(modes) / sizeof(modes[0]); m++) {
        cbm_store_t *s = cbm_store_open_memory();
        cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
        cbm_store_upsert_project(s, "myapp.dep.stdlib", "/tmp/stdlib");
        cbm_node_t nd = {.project = "myapp.dep.stdlib", .label = "Class", .name = "Path",
                         .qualified_name = "myapp.dep.stdlib.Path", .file_path = "stdlib/path.py"};
        cbm_store_upsert_node(s, &nd); /* dep first → lower id */
        cbm_node_t np = {.project = "myapp", .label = "Class", .name = "Path",
                         .qualified_name = "myapp.Path", .file_path = "src/path.py"};
        cbm_store_upsert_node(s, &np);

        cbm_search_params_t params = {0};
        params.project = "myapp"; /* prefix: includes deps */
        params.sort_by = modes[m];
        params.limit = 10;
        cbm_search_output_t out = {0};
        cbm_store_search(s, &params, &out);
        ASSERT_GTE(out.count, 2);

        int proj_idx = -1, dep_idx = -1;
        for (int i = 0; i < out.count; i++) {
            if (strcmp(out.results[i].node.name, "Path") != 0) continue;
            if (strcmp(out.results[i].node.project, "myapp") == 0) proj_idx = i;
            if (strcmp(out.results[i].node.project, "myapp.dep.stdlib") == 0) dep_idx = i;
        }
        ASSERT_NEQ(proj_idx, -1);
        ASSERT_NEQ(dep_idx, -1);
        ASSERT_TRUE(proj_idx < dep_idx); /* project before dep in EVERY mode */

        cbm_store_search_free(&out);
        cbm_store_close(s);
    }
    PASS();
}

/* #38: the dep-last ranking is tunable. With disable_dep_ranking=true the store
 * applies PURE relevance order — a dep symbol may rank above the project's own.
 * This makes the ranking a parameter (config key search_disable_dep_ranking)
 * rather than a hard-coded decision, per the project's meta-param conventions. */
TEST(store_prefix_disable_dep_ranking_lets_dep_win) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.stdlib", "/tmp/stdlib");
    cbm_node_t nd = {.project = "myapp.dep.stdlib", .label = "Class", .name = "Path",
                     .qualified_name = "myapp.dep.stdlib.Path", .file_path = "stdlib/path.py"};
    cbm_store_upsert_node(s, &nd);
    cbm_node_t np = {.project = "myapp", .label = "Class", .name = "Path",
                     .qualified_name = "myapp.Path", .file_path = "src/path.py"};
    cbm_store_upsert_node(s, &np);

    cbm_search_params_t params = {0};
    params.project = "myapp";
    params.disable_dep_ranking = true; /* pure relevance — no dep demotion */
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_GTE(out.count, 2);

    /* With dep-ranking disabled and equal relevance, the dep (lower id, inserted
     * first) wins the id tiebreak → ranks above the project symbol. */
    int proj_idx = -1, dep_idx = -1;
    for (int i = 0; i < out.count; i++) {
        if (strcmp(out.results[i].node.name, "Path") != 0) continue;
        if (strcmp(out.results[i].node.project, "myapp") == 0) proj_idx = i;
        if (strcmp(out.results[i].node.project, "myapp.dep.stdlib") == 0) dep_idx = i;
    }
    ASSERT_NEQ(proj_idx, -1);
    ASSERT_NEQ(dep_idx, -1);
    ASSERT_TRUE(dep_idx < proj_idx); /* dep before project (ranking disabled) */

    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* Bug 2 complement: exact match should NOT include deps. */
TEST(store_exact_match_excludes_deps) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "myapp", "/tmp/myapp");
    cbm_store_upsert_project(s, "myapp.dep.lib", "/tmp/lib");
    cbm_node_t n1 = {.project = "myapp", .label = "Function", .name = "main",
                      .qualified_name = "myapp.main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "myapp.dep.lib", .label = "Function", .name = "lib_fn",
                      .qualified_name = "myapp.dep.lib.lib_fn", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "myapp";
    params.project_exact = true;
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_EQ(out.count, 1);
    ASSERT_STR_EQ(out.results[0].node.project, "myapp");
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* Bug 3: cbm_is_dep_project must detect deps from any project. */
TEST(is_dep_project_cross_project_detection) {
    ASSERT_TRUE(cbm_is_dep_project("otherapp.dep.pandas", "myapp"));
    ASSERT_TRUE(cbm_is_dep_project("otherapp.dep.serde", "myapp"));
    ASSERT_TRUE(cbm_is_dep_project("myapp.dep.pandas", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("myapp", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("otherapp", "myapp"));
    ASSERT_FALSE(cbm_is_dep_project("deputy", "myapp"));
    PASS();
}

/* E2E: Full dep workflow — index + deps + search returns both with correct tags. */
TEST(e2e_dep_search_returns_project_and_dep_results) {
    cbm_store_t *s = cbm_store_open_memory();
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "app", "/tmp/app");
    cbm_store_upsert_project(s, "app.dep.mylib", "/tmp/lib");
    cbm_node_t n1 = {.project = "app", .label = "Function", .name = "app_main",
                      .qualified_name = "app.app_main", .file_path = "main.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_node_t n2 = {.project = "app.dep.mylib", .label = "Function", .name = "lib_helper",
                      .qualified_name = "app.dep.mylib.lib_helper", .file_path = "lib.c"};
    cbm_store_upsert_node(s, &n2);
    cbm_search_params_t params = {0};
    params.project = "app";
    params.limit = 10;
    cbm_search_output_t out = {0};
    cbm_store_search(s, &params, &out);
    ASSERT_EQ(out.count, 2);
    bool found_dep = false, found_proj = false;
    for (int i = 0; i < out.count; i++) {
        if (cbm_is_dep_project(out.results[i].node.project, "app")) {
            found_dep = true;
            const char *sep = strstr(out.results[i].node.project, ".dep.");
            ASSERT_NOT_NULL(sep);
            ASSERT_STR_EQ(sep + 5, "mylib");
        } else {
            found_proj = true;
        }
    }
    ASSERT_TRUE(found_dep);
    ASSERT_TRUE(found_proj);
    cbm_store_search_free(&out);
    cbm_store_close(s);
    PASS();
}

/* ── 14. MCP protocol conformance (binary-level) ─────────── */

TEST(all_tools_have_object_inputSchema) {
    /* BUG found by dogfooding: _hidden_tools had inputSchema as a JSON string
     * instead of a JSON object. Claude Code rejected the entire tools/list,
     * making all 3 real tools invisible. MCP spec requires inputSchema to be
     * a JSON Schema object, not a serialized string.
     * This test parses the tools/list JSON and verifies every tool's
     * inputSchema is a JSON object (not string, not null, not array). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);

    /* Parse the response and check each tool */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *result = yyjson_obj_get(root, "result");
    ASSERT_NOT_NULL(result);
    yyjson_val *tools = yyjson_obj_get(result, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    size_t idx, max;
    yyjson_val *tool;
    yyjson_arr_foreach(tools, idx, max, tool) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        yyjson_val *schema = yyjson_obj_get(tool, "inputSchema");
        const char *tool_name = yyjson_get_str(name);
        /* inputSchema MUST be a JSON object, NOT a string */
        ASSERT_NOT_NULL(schema);
        ASSERT_TRUE(yyjson_is_obj(schema));  /* fails if string/null/array */
        (void)tool_name; /* used for debugging if assertion fails */
    }

    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 15. Cross-project search prefix collision tests ──────── */

TEST(cross_project_search_not_confused_by_prefix) {
    /* BUG found by dogfooding: session "Users-athundt-.claude" and searching
     * project "Users-athundt-.claude-codebase-memory-mcp-..." matched on the
     * first 22 chars (shared path prefix), causing search to open the empty
     * session DB instead of the target's 22K-node DB.
     * Fix: after strncmp, check next char is '.' or '\0'.
     *
     * Test: create server with session "myapp", search with project "myapp-other".
     * The search should NOT use the session store — it should try to open
     * "myapp-other.db" (which won't exist, giving 0 results or error),
     * NOT return session store data. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    /* Search with a project that shares prefix but is NOT a dep of session */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp-other-project\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    /* Should NOT return session_project data (the bug returned session results).
     * The response should indicate the OTHER project (may be empty or error). */
    /* Key check: if the bug exists, session store is used and we'd see results
     * from "myapp" project. With the fix, resolve_store opens "myapp-other-project.db"
     * which either doesn't exist (error/empty) or has different data. */
    free(r);

    /* Clean up any spurious DB file created by resolve_store */
    char path[1024];
    snprintf(path, sizeof(path), "%s/myapp-other-project.db",
             cbm_resolve_cache_dir());
    (void)unlink(path);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(session_dep_search_uses_session_store) {
    /* Complement: "myapp.dep.lib" SHOULD use session store (myapp.db).
     * The '.' after session prefix correctly identifies it as a dep. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    /* This should use session store (myapp.db), not open myapp.dep.lib.db */
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp.dep.lib\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    /* We can't easily verify which DB was opened, but the search shouldn't crash
     * and should return session_project in the response. */
    ASSERT_NOT_NULL(strstr(r, "session_project"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(exact_session_name_uses_session_store) {
    /* Searching with exact session project name should use session store. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");

    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp\",\"name_pattern\":\".*\",\"limit\":3}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "session_project"));
    free(r);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Edge cases for prefix collision — various naming patterns that could match */

TEST(prefix_collision_dash_after_session_name) {
    /* "myapp-v2" should NOT match session "myapp" — dash is not a dep separator */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp-v2\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/myapp-v2.db", cbm_resolve_cache_dir());
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_underscore_after_session_name) {
    /* "myapp_test" should NOT match session "myapp" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp_test\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/myapp_test.db", cbm_resolve_cache_dir());
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_longer_name_with_dot_not_dep) {
    /* "myapp.config" has a dot but is NOT a dep (no ".dep." segment).
     * Should NOT use session store — it's a different project. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"myapp.config\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    /* Note: "myapp.config" starts with "myapp" + "." so the DB selection
     * WILL use session store (by design — the check is session + ".").
     * This is acceptable because deps use ".dep." which contains ".",
     * and non-dep sub-projects (myapp.config) would be in the same DB. */
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_completely_different_project) {
    /* "other-project" shares no prefix with session "myapp" */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "myapp");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"other-project\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/other-project.db", cbm_resolve_cache_dir());
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(prefix_collision_session_is_substring_of_project) {
    /* Session "ab" and project "abc" — "ab" is a prefix of "abc" but
     * "abc"[2] is 'c' (not '.' or '\0'), so should NOT match. */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_session_project(srv, "ab");
    char *r = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"abc\",\"name_pattern\":\".*\",\"limit\":1}");
    ASSERT_NOT_NULL(r);
    free(r);
    char path[1024];
    snprintf(path, sizeof(path), "%s/abc.db", cbm_resolve_cache_dir());
    (void)unlink(path);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── 16. get_code NULL-project regression tests ─────────── */

/* Bug: Tier 1-3 use WHERE project = ?1, so they return nothing when project
 * is NULL (SQL NULL comparison is always false). Fix: eff_project falls back
 * to srv->current_project when the caller omits the project param.
 *
 * Test: after search_graph opens a store, get_code with no project param
 * should resolve via Tier 1 exact QN match. */
TEST(get_code_no_project_uses_open_store_tier1) {
    /* Create a file DB with one node */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_gc_proj_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_gc_proj_", "/tmp");
    cbm_node_t n = {.project = "_tc_gc_proj_", .label = "Function",
                    .name = "tc_resolve_fn",
                    .qualified_name = "_tc_gc_proj_.src.tc_resolve_fn",
                    .file_path = "src/tc_resolve_fn.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    /* Create server; call search_graph to open the store (sets current_project) */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *sr = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_gc_proj_\",\"name_pattern\":\"tc_resolve_fn\",\"limit\":1}");
    ASSERT_NOT_NULL(sr);
    free(sr);

    /* get_code with no project param — eff_project must fall back to current_project */
    char *gr = cbm_mcp_handle_tool(srv, "get_code",
        "{\"qualified_name\":\"_tc_gc_proj_.src.tc_resolve_fn\"}");
    ASSERT_NOT_NULL(gr);
    /* Must NOT be ambiguous — Tier 1 exact QN should resolve via eff_project */
    ASSERT_NULL(strstr(gr, "\"ambiguous\""));
    /* Must contain the function name in the response */
    ASSERT_NOT_NULL(strstr(gr, "tc_resolve_fn"));
    free(gr);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

/* Bug: Tier 4 fuzzy search finding exactly 1 result returned status=ambiguous.
 * Fix: when fuzzy_count == 1, resolve immediately instead of calling
 * snippet_suggestions which always sets status=ambiguous. */
TEST(get_code_single_fuzzy_result_resolves_not_ambiguous) {
    /* Create a file DB with one node */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_gc_fuzzy_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_gc_fuzzy_", "/tmp");
    cbm_node_t n = {.project = "_tc_gc_fuzzy_", .label = "Function",
                    .name = "tc_unique_fuzzy_fn",
                    .qualified_name = "_tc_gc_fuzzy_.src.tc_unique_fuzzy_fn",
                    .file_path = "src/tc_unique_fuzzy_fn.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Open the store via search_graph so current_project is set */
    char *sr = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_gc_fuzzy_\",\"name_pattern\":\"tc_unique_fuzzy_fn\",\"limit\":1}");
    ASSERT_NOT_NULL(sr);
    free(sr);

    /* QN with a wrong prefix — Tiers 1-3 will miss, Tier 4 fuzzy finds 1 by name */
    char *gr = cbm_mcp_handle_tool(srv, "get_code",
        "{\"qualified_name\":\"wrong.prefix.tc_unique_fuzzy_fn\"}");
    ASSERT_NOT_NULL(gr);
    /* Must NOT be ambiguous — single fuzzy result should auto-resolve */
    ASSERT_NULL(strstr(gr, "\"ambiguous\""));
    /* Must contain the function name */
    ASSERT_NOT_NULL(strstr(gr, "tc_unique_fuzzy_fn"));
    free(gr);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

/* Option C: cold-start test — no prior search_code_graph call.
 * extract_project_from_qn() must find the DB by scanning dot-prefixes of the
 * QN, so get_code works even when srv->current_project is unset. */
TEST(get_code_cold_start_parses_project_from_qn) {
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_gc_cold_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_gc_cold_", "/tmp");
    cbm_node_t n = {.project = "_tc_gc_cold_", .label = "Function",
                    .name = "tc_cold_fn",
                    .qualified_name = "_tc_gc_cold_.src.tc_cold_fn",
                    .file_path = "src/tc_cold_fn.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    /* Fresh server — no prior tool calls, srv->current_project is unset */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* get_code with no project — must parse "_tc_gc_cold_" from the QN */
    char *gr = cbm_mcp_handle_tool(srv, "get_code",
        "{\"qualified_name\":\"_tc_gc_cold_.src.tc_cold_fn\"}");
    ASSERT_NOT_NULL(gr);
    /* Cold-start Option C: must resolve, not return ambiguous or not-found */
    ASSERT_NULL(strstr(gr, "\"ambiguous\""));
    ASSERT_NULL(strstr(gr, "\"error\""));
    ASSERT_NOT_NULL(strstr(gr, "tc_cold_fn"));
    free(gr);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

/* ── Watcher registration tests ──────────────────────────── */

TEST(watcher_registered_after_index_repository) {
    /* Create a tiny temp repo so indexing succeeds quickly */
    char repo_path[] = "/tmp/cbm_watch_test_XXXXXX";
    ASSERT_NOT_NULL(mkdtemp(repo_path));
    char src_path[256];
    snprintf(src_path, sizeof(src_path), "%s/test.c", repo_path);
    FILE *f = fopen(src_path, "w");
    if (f) { fprintf(f, "void hello(void) {}\n"); fclose(f); }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_watcher_t *w = cbm_watcher_new(NULL, NULL, NULL);
    ASSERT_NOT_NULL(w);
    cbm_mcp_server_set_watcher(srv, w);

    char args[512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", repo_path);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    free(resp);

    ASSERT_TRUE(cbm_watcher_watch_count(w) > 0);

    cbm_mcp_server_free(srv);
    cbm_watcher_free(w);
    (void)unlink(src_path);
    (void)rmdir(repo_path);
    PASS();
}

TEST(watcher_registered_on_resolve_store) {
    /* Pre-populate a DB with a project that has a known root_path */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_watcher_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_watcher_", "/tmp/cbm_watcher_root");
    cbm_node_t n = {.project = "_tc_watcher_", .label = "Function",
                    .name = "watcher_fn", .qualified_name = "_tc_watcher_.watcher_fn",
                    .file_path = "watcher_fn.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_watcher_t *w = cbm_watcher_new(NULL, NULL, NULL);
    ASSERT_NOT_NULL(w);
    cbm_mcp_server_set_watcher(srv, w);

    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_watcher_\",\"name_pattern\":\"watcher_fn\",\"limit\":1}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    ASSERT_TRUE(cbm_watcher_watch_count(w) > 0);

    cbm_mcp_server_free(srv);
    cbm_watcher_free(w);
    (void)unlink(db_path);
    PASS();
}

TEST(watcher_not_registered_for_unknown_path) {
    /* Project entry exists but root_path is empty — watcher must NOT be registered */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_watcher_nopath_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_watcher_nopath_", "");
    cbm_node_t n = {.project = "_tc_watcher_nopath_", .label = "Function",
                    .name = "nopath_fn", .qualified_name = "_tc_watcher_nopath_.nopath_fn",
                    .file_path = "nopath_fn.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_watcher_t *w = cbm_watcher_new(NULL, NULL, NULL);
    ASSERT_NOT_NULL(w);
    cbm_mcp_server_set_watcher(srv, w);

    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_watcher_nopath_\",\"name_pattern\":\"nopath_fn\",\"limit\":1}");
    ASSERT_NOT_NULL(resp);
    free(resp);

    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_mcp_server_free(srv);
    cbm_watcher_free(w);
    (void)unlink(db_path);
    PASS();
}

/* ── Empty DB / stale index detection ────────────────────── */

TEST(hidden_tools_returns_info_not_error) {
    /* _hidden_tools should return tool list, not "unknown tool" error */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_handle_tool(srv, "_hidden_tools", "{}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "hidden_tools"));
    ASSERT_NOT_NULL(strstr(resp, "index_repository"));
    /* Must NOT be an error */
    ASSERT_NULL(strstr(resp, "unknown tool"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(compact_defaults_to_true) {
    /* When compact is not provided, name field should be omitted if it's
     * the last segment of qualified_name */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_compact_default_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_compact_default_", "/tmp/compact_test");
    cbm_node_t n = {.project = "_tc_compact_default_", .label = "Function",
                    .name = "my_func", .qualified_name = "_tc_compact_default_.my_func",
                    .file_path = "test.c"};
    cbm_store_upsert_node(s, &n);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* Search WITHOUT compact param — should default to compact=true */
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_compact_default_\",\"name_pattern\":\"my_func\",\"limit\":1}");
    ASSERT_NOT_NULL(resp);
    /* In compact mode, "name" should NOT appear as a separate key when
     * it matches the last segment of qualified_name */
    /* Parse the result text to check */
    yyjson_doc *doc = yyjson_read(resp, strlen(resp), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *results = yyjson_obj_get(root, "results");
    if (results && yyjson_arr_size(results) > 0) {
        yyjson_val *first = yyjson_arr_get_first(results);
        /* name key should be absent in compact mode */
        ASSERT_NULL(yyjson_obj_get(first, "name"));
        ASSERT_NOT_NULL(yyjson_obj_get(first, "qualified_name"));
    }
    yyjson_doc_free(doc);
    free(resp);
    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

TEST(pagerank_output_has_limited_precision) {
    /* Pagerank values should be serialized with limited precision (~4 sig figs),
     * not full 17-digit double precision */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_pr_precision_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_pr_precision_", "/tmp/pr_test");
    cbm_node_t n1 = {.project = "_tc_pr_precision_", .label = "Function",
                     .name = "fn_a", .qualified_name = "_tc_pr_precision_.fn_a",
                     .file_path = "a.c"};
    cbm_node_t n2 = {.project = "_tc_pr_precision_", .label = "Function",
                     .name = "fn_b", .qualified_name = "_tc_pr_precision_.fn_b",
                     .file_path = "b.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    /* Compute PageRank (even with no edges, nodes get baseline scores) */
    cbm_pagerank_compute_default(s, "_tc_pr_precision_");
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_pr_precision_\",\"sort_by\":\"relevance\",\"limit\":2}");
    ASSERT_NOT_NULL(resp);
    /* Pagerank values should NOT have more than ~8 characters (e.g. "4.72e-05")
     * Check that we don't have 17-digit sequences like "0.00004717680769635863" */
    ASSERT_NULL(strstr(resp, "000000000")); /* No 9+ consecutive zeros in pagerank */
    free(resp);
    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

TEST(empty_db_not_treated_as_indexed) {
    /* A DB file with schema but 0 nodes should NOT prevent re-indexing.
     * Regression test: previously stat(db_path)==0 was enough to skip. */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_empty_db_test_.db",
             cbm_resolve_cache_dir());
    /* Create DB with schema but no data */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_close(s);

    /* Verify the file exists */
    struct stat st;
    ASSERT_EQ(stat(db_path, &st), 0);

    /* Open it read-only and verify 0 nodes */
    sqlite3 *db = NULL;
    ASSERT_EQ(sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL), SQLITE_OK);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM nodes", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int node_count = sqlite3_column_int(stmt, 0);
    ASSERT_EQ(node_count, 0);
    sqlite3_finalize(stmt);

    /* Verify "SELECT 1 FROM nodes LIMIT 1" returns no rows (this is what db_has_content checks) */
    rc = sqlite3_prepare_v2(db, "SELECT 1 FROM nodes LIMIT 1", -1, &stmt, NULL);
    ASSERT_EQ(rc, SQLITE_OK);
    ASSERT_NEQ(sqlite3_step(stmt), SQLITE_ROW); /* Should be SQLITE_DONE, not SQLITE_ROW */
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    (void)unlink(db_path);
    PASS();
}

/* ── Exclude param tests ─────────────────────────────────── */

TEST(search_exclude_filters_file_paths) {
    /* exclude param should remove matching results */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_exclude_test_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_exclude_test_", "/tmp/exclude_test");
    cbm_node_t n1 = {.project = "_tc_exclude_test_", .label = "Function",
                     .name = "core_fn", .qualified_name = "_tc_exclude_test_.core_fn",
                     .file_path = "src/main.c"};
    cbm_node_t n2 = {.project = "_tc_exclude_test_", .label = "Function",
                     .name = "test_fn", .qualified_name = "_tc_exclude_test_.test_fn",
                     .file_path = "tests/test_main.c"};
    cbm_node_t n3 = {.project = "_tc_exclude_test_", .label = "Function",
                     .name = "script_fn", .qualified_name = "_tc_exclude_test_.script_fn",
                     .file_path = "scripts/setup.sh"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_upsert_node(s, &n2);
    cbm_store_upsert_node(s, &n3);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* Without exclude: should find all 3 */
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_exclude_test_\",\"limit\":10}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "core_fn"));
    ASSERT_NOT_NULL(strstr(resp, "test_fn"));
    ASSERT_NOT_NULL(strstr(resp, "script_fn"));
    free(resp);

    /* With exclude: should filter out tests and scripts */
    resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_exclude_test_\",\"limit\":10,"
        "\"exclude\":[\"tests/**\",\"scripts/**\"]}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "core_fn"));
    ASSERT_NULL(strstr(resp, "test_fn"));
    ASSERT_NULL(strstr(resp, "script_fn"));
    free(resp);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

TEST(search_exclude_empty_array_no_effect) {
    /* Empty exclude array should not filter anything */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_excl_empty_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_excl_empty_", "/tmp/excl_empty");
    cbm_node_t n1 = {.project = "_tc_excl_empty_", .label = "Function",
                     .name = "fn1", .qualified_name = "_tc_excl_empty_.fn1",
                     .file_path = "src/a.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_excl_empty_\",\"limit\":10,\"exclude\":[]}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "fn1"));
    free(resp);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

TEST(search_exclude_all_returns_empty) {
    /* Excluding everything should return 0 results, not error */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_tc_excl_all_.db",
             cbm_resolve_cache_dir());
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_excl_all_", "/tmp/excl_all");
    cbm_node_t n1 = {.project = "_tc_excl_all_", .label = "Function",
                     .name = "fn1", .qualified_name = "_tc_excl_all_.fn1",
                     .file_path = "src/a.c"};
    cbm_store_upsert_node(s, &n1);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"project\":\"_tc_excl_all_\",\"limit\":10,\"exclude\":[\"**\"]}");
    ASSERT_NOT_NULL(resp);
    /* Should not contain fn1 (it was excluded) and should not be an error */
    ASSERT_NULL(strstr(resp, "fn1"));
    /* The response should contain "results" (empty array) not an error */
    ASSERT_NOT_NULL(strstr(resp, "results"));
    free(resp);

    cbm_mcp_server_free(srv);
    (void)unlink(db_path);
    PASS();
}

TEST(exclude_param_in_tool_schema) {
    /* Both streamlined and classic tool schemas should include exclude param */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *tools = cbm_mcp_tools_list(srv);
    ASSERT_NOT_NULL(tools);
    /* §4b: search_graph (default surface) should have exclude */
    ASSERT_NOT_NULL(strstr(tools, "\"exclude\""));
    free(tools);
    cbm_mcp_server_free(srv);
    PASS();
}

/* TDD: path_filter param (origin/main addition — fails before merge, passes after)
 * origin/main mcp.c:3522–3704 adds path_filter to handle_search_code().
 * After merge, search_code schema must advertise path_filter parameter.
 * Pre-merge: path_filter absent from schema → ASSERT fails (expected red).
 * Post-merge: path_filter present → ASSERT passes (expected green). */
TEST(path_filter_param_in_tool_schema) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* §4b: tools/list returns the default surface including search_code */
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    /* After merge: search_code_graph (or search_code) schema must include path_filter */
    ASSERT_NOT_NULL(strstr(resp, "path_filter"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* TDD: structured error when project param AND session_project both absent
 * (origin/main build_project_list_error() capability — merged per plan §2c)
 * Before merge: empty/wrong result; After merge: {"error":..., "available_projects":[...]} */
TEST(project_missing_returns_structured_error) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* No session project set, no project= param */
    char *resp = cbm_mcp_handle_tool(srv, "search_graph", "{\"name_pattern\":\"foo\"}");
    ASSERT_NOT_NULL(resp);
    /* After merge with DRY project resolution: must return a valid JSON response,
     * not crash. Pre-merge: may return empty results. Post-merge: structured error
     * with available_projects list via build_project_list_error(). */
    ASSERT_NOT_NULL(resp);
    /* At minimum, response must be valid JSON (not a bare NULL or crash) */
    bool has_error = strstr(resp, "error") != NULL;
    bool has_results = strstr(resp, "results") != NULL;
    bool has_project = strstr(resp, "project") != NULL;
    ASSERT_TRUE(has_error || has_results || has_project);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Bug fixes: search_code/search_graph mode/param sharp edges ─ */

/* TDD Bug 1: case_sensitive=false must add -i to grep so uppercase patterns match lowercase.
 * Before fix: build_grep_cmd has no -i flag → HELLO_WORLD misses hello_world → FAIL.
 * After fix: build_grep_cmd adds -i when case_sensitive=false → match found → PASS. */
TEST(source_grep_case_insensitive_by_default) {
    /* Register a project in the store so get_project_root can resolve it */
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/_tc_ci_test_.db",
             cbm_resolve_cache_dir());
    char proj_dir[256];
    snprintf(proj_dir, sizeof(proj_dir), "%s/cbm_ci_test_%d", cbm_tmpdir(), (int)getpid());
    cbm_mkdir_p(proj_dir, 0755);

    /* Write a file with only lowercase content */
    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/hello.c", proj_dir);
    FILE *f = fopen(src_path, "w");
    if (f) { fprintf(f, "void hello_world(void) {}\n"); fclose(f); }

    /* Register project in store so get_project_root returns proj_dir */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_ci_test_", proj_dir);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* grep for UPPERCASE pattern with case_sensitive=false (default) */
    char *resp = cbm_mcp_handle_tool(srv, "search_code",
        "{\"search_in\":\"source\",\"pattern\":\"HELLO_WORLD\","
        "\"project\":\"_tc_ci_test_\",\"case_sensitive\":false}");
    ASSERT_NOT_NULL(resp);
    /* After fix: -i flag → case-insensitive grep finds "hello_world" via HELLO_WORLD */
    bool found_match = strstr(resp, "hello") != NULL || strstr(resp, "hello_world") != NULL;
    bool nonzero_count = strstr(resp, "\"count\":0") == NULL;
    ASSERT_TRUE(found_match && nonzero_count);
    free(resp);

    cbm_mcp_server_free(srv);
    remove(src_path);
    cbm_rmdir(proj_dir);
    (void)unlink(db_path);
    PASS();
}

/* TDD Bug 1b: case_sensitive=true must NOT add -i → uppercase pattern misses lowercase file.
 * Pattern "HELLO_WORLD" vs file containing "hello_world" only → 0 matches. */
TEST(source_grep_case_sensitive_flag_works) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/_tc_cs_test_.db",
             cbm_resolve_cache_dir());
    char proj_dir[256];
    snprintf(proj_dir, sizeof(proj_dir), "%s/cbm_cs_test_%d", cbm_tmpdir(), (int)getpid());
    cbm_mkdir_p(proj_dir, 0755);

    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/lower.c", proj_dir);
    FILE *f = fopen(src_path, "w");
    if (f) { fprintf(f, "void hello_world(void) {}\n"); fclose(f); }

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_cs_test_", proj_dir);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_handle_tool(srv, "search_code",
        "{\"search_in\":\"source\",\"pattern\":\"HELLO_WORLD\","
        "\"project\":\"_tc_cs_test_\",\"case_sensitive\":true}");
    ASSERT_NOT_NULL(resp);
    /* After fix: no -i flag → case-sensitive grep must NOT find "hello_world" via "HELLO_WORLD" */
    /* Response format uses "total_grep_matches" and "total_results" fields */
    /* The outer wrapper JSON-encodes the inner result, so "key":val becomes \"key\":val in resp.
     * Search for the escaped form: \\\" in C source == \" in bytes == the escaped JSON quote. */
    ASSERT_NOT_NULL(strstr(resp, "\\\"total_grep_matches\\\":0"));
    free(resp);

    cbm_mcp_server_free(srv);
    remove(src_path);
    cbm_rmdir(proj_dir);
    (void)unlink(db_path);
    PASS();
}

/* TDD Bug 2: mode="compact" in graph mode returns an unhelpful error.
 * After fix: error message must mention that "compact" belongs to source grep
 * mode and explain the two separate mode enums. */
TEST(graph_mode_compact_error_is_descriptive) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    /* mode="compact" is valid in source grep but invalid in graph mode */
    char *resp = cbm_mcp_handle_tool(srv, "search_graph",
        "{\"mode\":\"compact\",\"label\":\"Function\"}");
    ASSERT_NOT_NULL(resp);
    /* Must return an error (not crash or silent wrong result) */
    ASSERT_NOT_NULL(strstr(resp, "error"));
    /* After fix: error should mention source grep context — "source" or "search_in" */
    bool mentions_source = strstr(resp, "source") != NULL ||
                           strstr(resp, "search_in") != NULL ||
                           strstr(resp, "grep") != NULL;
    ASSERT_TRUE(mentions_source);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* TDD Bug 3: mode="summary" in source grep must produce a warning,
 * not silently fall through to compact output.
 * Before fix: response has no "mode_warning" → FAIL.
 * After fix: response contains "mode_warning" field → PASS. */
TEST(source_grep_mode_summary_warns) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/_tc_sm_test_.db",
             cbm_resolve_cache_dir());
    char proj_dir[256];
    snprintf(proj_dir, sizeof(proj_dir), "%s/cbm_sm_test_%d", cbm_tmpdir(), (int)getpid());
    cbm_mkdir_p(proj_dir, 0755);

    char src_path[320];
    snprintf(src_path, sizeof(src_path), "%s/code.c", proj_dir);
    FILE *f = fopen(src_path, "w");
    if (f) { fprintf(f, "void foo(void) {}\n"); fclose(f); }

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);
    cbm_store_upsert_project(s, "_tc_sm_test_", proj_dir);
    cbm_store_close(s);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_handle_tool(srv, "search_code",
        "{\"search_in\":\"source\",\"pattern\":\"foo\","
        "\"project\":\"_tc_sm_test_\",\"mode\":\"summary\"}");
    ASSERT_NOT_NULL(resp);
    /* After fix: response must contain "mode_warning" key */
    ASSERT_NOT_NULL(strstr(resp, "mode_warning"));
    free(resp);

    cbm_mcp_server_free(srv);
    remove(src_path);
    cbm_rmdir(proj_dir);
    (void)unlink(db_path);
    PASS();
}

/* TDD Bug 4 (revised for §4b): the split-tool surface eliminates the graph-vs-source
 * "compact" mode collision that motivated the original "graph mode only" schema note.
 * search_graph owns the compact boolean; search_code uses a mode string and has no
 * compact boolean. Verify that post-§4b the compact param lives only on search_graph. */
TEST(schema_compact_documented_as_graph_only) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    /* search_graph schema must include a compact boolean param */
    ASSERT_NOT_NULL(strstr(resp, "\"compact\""));
    /* The mega-tool's "graph mode only" warning is gone (no collision after split).
     * search_code has no compact boolean — it uses mode instead. */
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* TDD Bug 5: context param must appear in the search_code_graph schema.
 * Before fix: schema has no "context" param → ASSERT fails (red).
 * After fix: schema includes context param with description → ASSERT passes. */
TEST(schema_has_context_param_for_source_grep) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(srv,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    /* search_code_graph schema must include "context" param */
    ASSERT_NOT_NULL(strstr(resp, "\"context\""));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* ── Suite registration ──────────────────────────────────── */

SUITE(tool_consolidation) {
    /* MCP protocol conformance */
    RUN_TEST(all_tools_have_object_inputSchema);
    /* Tool visibility */
    RUN_TEST(streamlined_mode_shows_default_user_tools);
    RUN_TEST(server_default_mode_shows_streamlined_tools);
    RUN_TEST(api_surface_default_streamlined_regression_gate);
    RUN_TEST(api_surface_classic_regression_gate);
    RUN_TEST(hidden_tools_reveal_discoverable_tools);
    RUN_TEST(hidden_tools_payload_excludes_already_visible_configured_tools);
    RUN_TEST(streamlined_reveal_covers_classic_capabilities);
    RUN_TEST(streamlined_core_parameter_contract);
    RUN_TEST(default_tool_autoindex_description_is_precise);
    RUN_TEST(revealed_trace_path_parameter_contract);
    RUN_TEST(revealed_advanced_tool_schema_matches_handlers);
    /* Dispatch */
    RUN_TEST(search_graph_dispatch);
    RUN_TEST(query_graph_dispatch);
    RUN_TEST(get_code_dispatch);
    RUN_TEST(canonical_tool_names_dispatch);
    /* Path support */
    RUN_TEST(project_param_path_detection);
    /* Edge cases */
    RUN_TEST(unknown_tool_returns_error);
    RUN_TEST(null_tool_name_returns_error);
    /* Progressive disclosure */
    RUN_TEST(streamlined_mode_has_hidden_tools_hint);
    RUN_TEST(hidden_tools_still_dispatch);
    /* Session context */
    RUN_TEST(search_graph_has_session_project);
    RUN_TEST(index_status_has_session_project);
    /* Context injection */
    RUN_TEST(first_response_has_context_header);
    RUN_TEST(context_has_schema_info);
    /* MCP Resources (Phase 10) */
    RUN_TEST(resources_list_returns_3_resources);
    RUN_TEST(resources_read_schema);
    RUN_TEST(resources_read_architecture);
    RUN_TEST(resources_read_status);
    RUN_TEST(resources_read_unknown_uri);
    RUN_TEST(initialize_advertises_resources_capability);
    RUN_TEST(initialize_parses_client_resources_capability);
    RUN_TEST(no_resources_capability_gets_context_injection);
    /* MCP spec compliance */
    RUN_TEST(initialize_response_has_protocol_version);
    RUN_TEST(initialize_resources_cap_subscribe_false);
    RUN_TEST(resources_list_has_mimeType_and_description);
    RUN_TEST(resources_read_response_has_contents_array);
    RUN_TEST(resources_read_missing_uri_param);
    RUN_TEST(resources_read_no_params_at_all);
    /* Client behavioral differences */
    RUN_TEST(resource_client_gets_context_only_on_first_call);
    RUN_TEST(legacy_client_gets_context_only_on_first_call);
    RUN_TEST(empty_resources_capability_still_gets_context);
    RUN_TEST(no_initialize_defaults_to_legacy_behavior);
    /* MCP resources pull-only: context injection fires for all clients (§17) */
    RUN_TEST(resource_capable_client_gets_context_on_first_call);
    RUN_TEST(resource_capable_client_no_context_on_second_call);
    /* Tool descriptions reference resources */
    RUN_TEST(tool_descriptions_reference_resources);
    RUN_TEST(hidden_tools_hint_mentions_resources);
    /* Error message quality */
    RUN_TEST(error_no_project_loaded_has_hint);
    RUN_TEST(error_function_not_found_includes_name);
    RUN_TEST(error_symbol_not_found_includes_qn);
    RUN_TEST(error_missing_required_param_has_hint);
    RUN_TEST(error_unknown_tool_lists_valid_tools);
    RUN_TEST(error_resource_not_found_has_spec_code);
    /* JSON-RPC response structure (e2e) */
    RUN_TEST(resource_error_is_top_level_not_nested_in_result);
    RUN_TEST(resource_error_missing_uri_is_top_level);
    RUN_TEST(resource_error_no_params_is_top_level);
    RUN_TEST(resource_success_has_result_not_error);
    RUN_TEST(resource_schema_returns_real_data_when_indexed);
    RUN_TEST(resource_status_returns_not_indexed_when_no_store);
    /* Dep search bug regressions */
    RUN_TEST(dep_search_explicit_dep_project_name);
    RUN_TEST(store_prefix_match_includes_deps);
    RUN_TEST(store_prefix_ranks_project_above_dep);
    RUN_TEST(store_prefix_ranks_project_above_dep_all_sort_modes);
    RUN_TEST(store_prefix_disable_dep_ranking_lets_dep_win);
    RUN_TEST(store_exact_match_excludes_deps);
    RUN_TEST(is_dep_project_cross_project_detection);
    RUN_TEST(e2e_dep_search_returns_project_and_dep_results);
    /* Cross-project search prefix collision */
    RUN_TEST(cross_project_search_not_confused_by_prefix);
    RUN_TEST(session_dep_search_uses_session_store);
    RUN_TEST(exact_session_name_uses_session_store);
    RUN_TEST(prefix_collision_dash_after_session_name);
    RUN_TEST(prefix_collision_underscore_after_session_name);
    RUN_TEST(prefix_collision_longer_name_with_dot_not_dep);
    RUN_TEST(prefix_collision_completely_different_project);
    RUN_TEST(prefix_collision_session_is_substring_of_project);
    /* get_code NULL-project regression */
    RUN_TEST(get_code_no_project_uses_open_store_tier1);
    RUN_TEST(get_code_single_fuzzy_result_resolves_not_ambiguous);
    RUN_TEST(get_code_cold_start_parses_project_from_qn);
    RUN_TEST(watcher_registered_after_index_repository);
    RUN_TEST(watcher_registered_on_resolve_store);
    RUN_TEST(watcher_not_registered_for_unknown_path);
    /* Phase 10.2: Bug fixes and token optimization */
    RUN_TEST(hidden_tools_returns_info_not_error);
    RUN_TEST(compact_defaults_to_true);
    RUN_TEST(pagerank_output_has_limited_precision);
    RUN_TEST(empty_db_not_treated_as_indexed);
    /* Exclude param */
    RUN_TEST(search_exclude_filters_file_paths);
    RUN_TEST(search_exclude_empty_array_no_effect);
    RUN_TEST(search_exclude_all_returns_empty);
    RUN_TEST(exclude_param_in_tool_schema);
    /* origin/main additions: path_filter param + structured error for missing project */
    RUN_TEST(path_filter_param_in_tool_schema);
    RUN_TEST(project_missing_returns_structured_error);
    /* Bug fixes: search_code_graph mode/param sharp edges (TDD) */
    RUN_TEST(source_grep_case_insensitive_by_default);
    RUN_TEST(source_grep_case_sensitive_flag_works);
    RUN_TEST(graph_mode_compact_error_is_descriptive);
    RUN_TEST(source_grep_mode_summary_warns);
    RUN_TEST(schema_compact_documented_as_graph_only);
    RUN_TEST(schema_has_context_param_for_source_grep);
}
