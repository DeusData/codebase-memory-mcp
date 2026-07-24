/*
 * test_mcp.c — Tests for the MCP server module.
 *
 * Covers: JSON-RPC parsing, MCP protocol, tool dispatch, tool handlers.
 */
#include "../src/foundation/compat.h"
#include <sqlite3.h>
#include "../src/foundation/compat_fs.h" /* cbm_unlink / cbm_rmdir */
#include "../src/foundation/constants.h"
#include "../src/foundation/log.h"
#include "../src/foundation/platform.h" /* cbm_file_size */
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <mcp/index_supervisor.h> /* spawn-count hook — #845 in-process guard */
#include <mcp/mcp.h>
#include <mcp/mcp_internal.h>
#include <pipeline/pipeline.h>
#include <store/store.h>
#include <watcher/watcher.h>
#include <yyjson/yyjson.h>
#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h> /* chmod / stat for read-only query reproductions */
#ifdef _WIN32
#include <direct.h>
#define cbm_chdir _chdir
#define cbm_getcwd _getcwd
#else
#ifdef __APPLE__
#include <libproc.h>
#endif
#include <spawn.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#define cbm_chdir chdir
#define cbm_getcwd getcwd
extern char **environ;
#endif

static bool mcp_response_has_exact_tool(const char *response, const char *expected_name) {
    yyjson_doc *doc = response ? yyjson_read(response, strlen(response), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *result = root ? yyjson_obj_get(root, "result") : NULL;
    yyjson_val *tools = result ? yyjson_obj_get(result, "tools") : NULL;
    bool found = false;
    if (tools && yyjson_is_arr(tools)) {
        size_t index, max;
        yyjson_val *tool;
        yyjson_arr_foreach(tools, index, max, tool) {
            yyjson_val *name = yyjson_obj_get(tool, "name");
            if (name && yyjson_is_str(name) && strcmp(yyjson_get_str(name), expected_name) == 0) {
                found = true;
                break;
            }
        }
    }
    yyjson_doc_free(doc);
    return found;
}

static size_t mcp_response_tool_count(const char *response) {
    yyjson_doc *doc = response ? yyjson_read(response, strlen(response), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *result = root ? yyjson_obj_get(root, "result") : NULL;
    yyjson_val *tools = result ? yyjson_obj_get(result, "tools") : NULL;
    size_t count = tools && yyjson_is_arr(tools) ? yyjson_arr_size(tools) : 0U;
    yyjson_doc_free(doc);
    return count;
}

static char mcp_log_buf[4096];
static bool mcp_saw_autoindex_log;

static void mcp_capture_log(const char *line) {
    snprintf(mcp_log_buf, sizeof(mcp_log_buf), "%s", line ? line : "");
    if (line && strstr(line, "msg=autoindex.")) {
        mcp_saw_autoindex_log = true;
    }
}

static bool response_contains_json_fragment(const char *response, const char *fragment) {
    if (!response || !fragment) {
        return false;
    }
    if (strstr(response, fragment)) {
        return true;
    }

    char escaped[512];
    size_t out = 0;
    for (size_t i = 0; fragment[i] && out + 2 < sizeof(escaped); i++) {
        if (fragment[i] == '"') {
            escaped[out++] = '\\';
        }
        escaped[out++] = fragment[i];
    }
    escaped[out] = '\0';
    return strstr(response, escaped) != NULL;
}

static void restore_cache_dir(const char *saved_copy) {
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
}

static void cleanup_project_db(const char *cache, const char *project) {
    if (!cache || !project) {
        return;
    }

    char path[CBM_SZ_4K];
    snprintf(path, sizeof(path), "%s/%s.db", cache, project);
    cbm_unlink(path);
    snprintf(path, sizeof(path), "%s/%s.db-wal", cache, project);
    cbm_unlink(path);
    snprintf(path, sizeof(path), "%s/%s.db-shm", cache, project);
    cbm_unlink(path);
}

#define MCP_MUTATION_GUARD_MAX_EVENTS 16

typedef struct {
    int deny_begin_call;      /* one-based; zero allows every acquisition */
    int cancel_on_begin_call; /* one-based; zero never requests cancellation */
    int begin_count;
    int end_count;
    cbm_mcp_server_t *cancel_server;
    bool cancel_attempted;
    bool cancel_accepted;
    const char *observed_db_path;
    const char *observed_backup_path;
    bool db_exists_at_begin;
    bool backup_exists_at_begin;
    bool db_exists_at_end;
    bool backup_exists_at_end;
    char begin_projects[MCP_MUTATION_GUARD_MAX_EVENTS][CBM_SZ_256];
    char end_projects[MCP_MUTATION_GUARD_MAX_EVENTS][CBM_SZ_256];
} mcp_mutation_guard_probe_t;

typedef struct {
    const char *deny_step;
    int call_count;
    char steps[4][64];
} mcp_quarantine_hook_probe_t;

typedef struct {
    bool reject_merge_base;
    int diff_calls;
    int merge_base_calls;
} mcp_command_hook_probe_t;

static bool mcp_quarantine_hook_probe(void *context, const char *step) {
    mcp_quarantine_hook_probe_t *probe = context;
    if (!probe || !step) {
        return false;
    }
    int event = probe->call_count++;
    if (event >= 0 && event < 4) {
        snprintf(probe->steps[event], sizeof(probe->steps[event]), "%s", step);
    }
    return !probe->deny_step || strcmp(probe->deny_step, step) != 0;
}

static bool mcp_command_hook_probe(void *context, const char *command) {
    mcp_command_hook_probe_t *probe = context;
    if (!probe || !command) {
        return false;
    }
    if (strstr(command, "merge-base")) {
        probe->merge_base_calls++;
        return !probe->reject_merge_base;
    }
    probe->diff_calls++;
    return true;
}

static bool mcp_mutation_guard_probe_begin(void *context, const char *project) {
    mcp_mutation_guard_probe_t *probe = context;
    if (!probe) {
        return false;
    }

    int event = probe->begin_count++;
    if (event < MCP_MUTATION_GUARD_MAX_EVENTS) {
        snprintf(probe->begin_projects[event], sizeof(probe->begin_projects[event]), "%s",
                 project ? project : "");
    }
    if (probe->cancel_on_begin_call > 0 && probe->begin_count == probe->cancel_on_begin_call) {
        probe->cancel_attempted = true;
        probe->cancel_accepted = cbm_mcp_server_cancel_active(probe->cancel_server);
    }
    if (probe->observed_db_path) {
        probe->db_exists_at_begin = cbm_file_exists(probe->observed_db_path);
    }
    if (probe->observed_backup_path) {
        probe->backup_exists_at_begin = cbm_file_exists(probe->observed_backup_path);
    }
    return probe->deny_begin_call == 0 || probe->begin_count != probe->deny_begin_call;
}

static void mcp_mutation_guard_probe_end(void *context, const char *project) {
    mcp_mutation_guard_probe_t *probe = context;
    if (!probe) {
        return;
    }

    int event = probe->end_count++;
    if (event < MCP_MUTATION_GUARD_MAX_EVENTS) {
        snprintf(probe->end_projects[event], sizeof(probe->end_projects[event]), "%s",
                 project ? project : "");
    }
    if (probe->observed_db_path) {
        probe->db_exists_at_end = cbm_file_exists(probe->observed_db_path);
    }
    if (probe->observed_backup_path) {
        probe->backup_exists_at_end = cbm_file_exists(probe->observed_backup_path);
    }
}

static bool mcp_make_corrupt_project_store(const char *cache, const char *project) {
    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return false;
    }

    /* Numeric root paths are the deterministic corruption trigger used by
     * cbm_store_check_integrity() and the issue #557 reproduction. */
    bool created = cbm_store_upsert_project(store, project, "826") == CBM_STORE_OK;
    cbm_store_close(store);
    return created;
}

/* Keep a writer open so the fixture has a real, committed WAL generation.
 * Query-only opens must not alter either file when quarantine is denied or
 * cannot be published safely. The caller owns the returned store. */
static cbm_store_t *mcp_open_corrupt_project_store_with_wal(const char *cache,
                                                            const char *project) {
    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return NULL;
    }

    bool ready =
        cbm_store_exec(store, "PRAGMA wal_autocheckpoint=0;") == CBM_STORE_OK &&
        cbm_store_upsert_project(store, project, "826") == CBM_STORE_OK &&
        cbm_store_exec(store, "CREATE TABLE IF NOT EXISTS guard_wal_sentinel(value TEXT);"
                              "INSERT INTO guard_wal_sentinel(value) VALUES('committed');") ==
            CBM_STORE_OK;
    if (!ready) {
        cbm_store_close(store);
        return NULL;
    }
    return store;
}

static bool mcp_make_valid_project_store_at(const char *path, const char *project,
                                            const char *root_path) {
    cbm_store_t *store = cbm_store_open_path(path);
    if (!store) {
        return false;
    }
    bool ready = cbm_store_upsert_project(store, project, root_path) == CBM_STORE_OK &&
                 cbm_store_prepare_for_publish(store) == CBM_STORE_OK;
    cbm_store_close(store);
    return ready;
}

static unsigned char *mcp_read_file_bytes(const char *path, long *out_len) {
    if (!out_len) {
        return NULL;
    }
    *out_len = 0;
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp) {
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long size = ftell(fp);
    if (size < 0 || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    unsigned char *bytes = malloc(size > 0 ? (size_t)size : 1);
    if (!bytes) {
        fclose(fp);
        return NULL;
    }
    size_t read_count = fread(bytes, 1, (size_t)size, fp);
    fclose(fp);
    if (read_count != (size_t)size) {
        free(bytes);
        return NULL;
    }
    *out_len = size;
    return bytes;
}

static bool mcp_file_matches_snapshot(const char *path, const unsigned char *expected,
                                      long expected_len) {
    long actual_len = 0;
    unsigned char *actual = mcp_read_file_bytes(path, &actual_len);
    bool matches = actual && expected && actual_len == expected_len &&
                   memcmp(actual, expected, (size_t)actual_len) == 0;
    free(actual);
    return matches;
}

/* Return the number of quarantine files for a project and, when present, the
 * first path whose name is distinct from the legacy fixed `.corrupt` name. */
static bool mcp_is_corrupt_backup_main_name(const char *name, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    if (strcmp(name, prefix) == 0) {
        return true;
    }
    const char *suffix = name + prefix_len;
    if (strncmp(name, prefix, prefix_len) != 0 || suffix[0] != '.' || strlen(suffix + 1) != 16) {
        return false;
    }
    for (const char *cursor = suffix + 1; *cursor; cursor++) {
        if (!isxdigit((unsigned char)*cursor)) {
            return false;
        }
    }
    return true;
}

static int mcp_find_corrupt_backups(const char *cache, const char *project, char *unique_path,
                                    size_t unique_path_size) {
    if (unique_path && unique_path_size > 0) {
        unique_path[0] = '\0';
    }
    char prefix[CBM_DIRENT_NAME_MAX];
    snprintf(prefix, sizeof(prefix), "%s.db.corrupt", project);
    int count = 0;
    cbm_dir_t *dir = cbm_opendir(cache);
    if (!dir) {
        return 0;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (!mcp_is_corrupt_backup_main_name(entry->name, prefix)) {
            continue;
        }
        char path[CBM_SZ_1K];
        snprintf(path, sizeof(path), "%s/%s", cache, entry->name);
        if (!cbm_file_exists(path)) {
            continue;
        }
        count++;
        if (unique_path && unique_path_size > 0 && unique_path[0] == '\0' &&
            strcmp(entry->name, prefix) != 0) {
            snprintf(unique_path, unique_path_size, "%s", path);
        }
    }
    cbm_closedir(dir);
    return count;
}

static int mcp_count_corrupt_artifacts(const char *cache, const char *project) {
    char prefix[CBM_DIRENT_NAME_MAX];
    snprintf(prefix, sizeof(prefix), "%s.db.corrupt", project);
    size_t prefix_len = strlen(prefix);
    int count = 0;
    cbm_dir_t *dir = cbm_opendir(cache);
    if (!dir) {
        return 0;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (strncmp(entry->name, prefix, prefix_len) == 0) {
            count++;
        }
    }
    cbm_closedir(dir);
    return count;
}

static int mcp_count_directory_entries_with_prefix(const char *directory, const char *prefix) {
    cbm_dir_t *dir = cbm_opendir(directory);
    if (!dir) {
        return -1;
    }
    size_t prefix_length = strlen(prefix);
    int count = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (strncmp(entry->name, prefix, prefix_length) == 0) {
            count++;
        }
    }
    cbm_closedir(dir);
    return count;
}

static void mcp_cleanup_corrupt_backups(const char *cache, const char *project) {
    char prefix[CBM_DIRENT_NAME_MAX];
    snprintf(prefix, sizeof(prefix), "%s.db.corrupt", project);
    size_t prefix_len = strlen(prefix);
    cbm_dir_t *dir = cbm_opendir(cache);
    if (!dir) {
        return;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (strncmp(entry->name, prefix, prefix_len) == 0) {
            char path[CBM_SZ_1K];
            snprintf(path, sizeof(path), "%s/%s", cache, entry->name);
            cbm_unlink(path);
        }
    }
    cbm_closedir(dir);
}

typedef struct {
    mcp_mutation_guard_probe_t guard;
    const char *replacement_path;
    const char *live_path;
    bool replacement_attempted;
    bool replacement_succeeded;
} mcp_replacing_mutation_guard_t;

static bool mcp_replacing_mutation_guard_begin(void *context, const char *project) {
    mcp_replacing_mutation_guard_t *replacement = context;
    if (!replacement || !mcp_mutation_guard_probe_begin(&replacement->guard, project)) {
        return false;
    }
    replacement->replacement_attempted = true;
    bool sidecars_removed = cbm_remove_db_sidecars(replacement->live_path) == 0;
    replacement->replacement_succeeded =
        sidecars_removed &&
        cbm_rename_replace(replacement->replacement_path, replacement->live_path) == 0;
    return true;
}

static void mcp_replacing_mutation_guard_end(void *context, const char *project) {
    mcp_replacing_mutation_guard_t *replacement = context;
    if (replacement) {
        mcp_mutation_guard_probe_end(&replacement->guard, project);
    }
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_parse_request) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"capabilities\":{}}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.jsonrpc, "2.0");
    ASSERT_STR_EQ(req.method, "initialize");
    ASSERT_EQ(req.id, 1);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.params_raw);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_notification) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "notifications/initialized");
    ASSERT_FALSE(req.has_id);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_invalid) {
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("not json", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_tools_call) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"search_graph\","
                       "\"arguments\":{\"label\":\"Function\",\"limit\":5}}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "tools/call");
    ASSERT_EQ(req.id, 42);
    ASSERT_NOT_NULL(req.params_raw);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

/* issue #253: JSON-RPC 2.0 §4 permits string ids (Claude Desktop sends them
 * for "initialize"). Previously strtol-coerced to 0; must be preserved. */
TEST(jsonrpc_parse_string_id_issue253) {
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":\"init-abc\",\"method\":\"initialize\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.id_str);
    ASSERT_STR_EQ(req.id_str, "init-abc");
    cbm_jsonrpc_request_free(&req);

    /* A purely non-numeric string would have become 0 under strtol. */
    const char *line2 = "{\"jsonrpc\":\"2.0\",\"id\":\"xyz\",\"method\":\"ping\"}";
    cbm_jsonrpc_request_t req2 = {0};
    ASSERT_EQ(cbm_jsonrpc_parse(line2, &req2), 0);
    ASSERT_NOT_NULL(req2.id_str);
    ASSERT_STR_EQ(req2.id_str, "xyz");
    cbm_jsonrpc_request_free(&req2);
    PASS();
}

/* issue #253: the response must echo the string id verbatim, not as a number. */
TEST(jsonrpc_format_response_string_id_issue253) {
    cbm_jsonrpc_response_t resp = {
        .id_str = "init-abc",
        .result_json = "{\"ok\":true}",
    };
    char *json = cbm_jsonrpc_format_response(&resp);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"id\":\"init-abc\""));
    /* Must NOT have coerced to a numeric id. */
    ASSERT_NULL(strstr(json, "\"id\":0"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC FORMATTING
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_format_response) {
    cbm_jsonrpc_response_t resp = {
        .id = 1,
        .result_json = "{\"name\":\"codebase-memory-mcp\"}",
    };
    char *json = cbm_jsonrpc_format_response(&resp);
    ASSERT_NOT_NULL(json);
    /* Should contain jsonrpc, id, and result */
    ASSERT_NOT_NULL(strstr(json, "\"jsonrpc\":\"2.0\""));
    ASSERT_NOT_NULL(strstr(json, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(json, "\"result\""));
    free(json);
    PASS();
}

TEST(jsonrpc_format_error) {
    char *json = cbm_jsonrpc_format_error(5, -32600, "Invalid Request");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"id\":5"));
    ASSERT_NOT_NULL(strstr(json, "\"error\""));
    ASSERT_NOT_NULL(strstr(json, "-32600"));
    ASSERT_NOT_NULL(strstr(json, "Invalid Request"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_initialize_response) {
    cbm_cli_set_version("9.8.7-test");

    /* Default (no params): returns latest supported version */
    char *json = cbm_mcp_initialize_response(NULL);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(json, "\"version\":\"9.8.7-test\""));
    ASSERT_NOT_NULL(strstr(json, "capabilities"));
    ASSERT_NOT_NULL(strstr(json, "tools"));
    ASSERT_NOT_NULL(strstr(json, "\"listChanged\":false"));
    ASSERT_NOT_NULL(strstr(json, "\"prompts\":{\"listChanged\":false}"));
    ASSERT_NOT_NULL(strstr(json, "\"instructions\":"));
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "auto-refresh"));
    ASSERT_NOT_NULL(strstr(json, "2025-11-25"));
    free(json);

    /* Client requests a supported version: server echoes it */
    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"2024-11-05\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2024-11-05"));
    free(json);

    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"2025-06-18\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2025-06-18"));
    free(json);

    /* Client requests unknown version: server returns its latest */
    json = cbm_mcp_initialize_response("{\"protocolVersion\":\"9999-01-01\"}");
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "2025-11-25"));
    free(json);
    cbm_cli_set_version("dev");
    PASS();
}

TEST(mcp_tools_list) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    /* Should contain all tools, including the targeted coverage gate. */
    ASSERT_NOT_NULL(strstr(json, "index_repository"));
    ASSERT_NOT_NULL(strstr(json, "search_graph"));
    ASSERT_NOT_NULL(strstr(json, "query_graph"));
    ASSERT_NOT_NULL(strstr(json, "trace_path"));
    ASSERT_NOT_NULL(strstr(json, "get_code_snippet"));
    ASSERT_NOT_NULL(strstr(json, "get_graph_schema"));
    ASSERT_NOT_NULL(strstr(json, "get_architecture"));
    ASSERT_NOT_NULL(strstr(json, "search_code"));
    ASSERT_NOT_NULL(strstr(json, "list_projects"));
    ASSERT_NOT_NULL(strstr(json, "delete_project"));
    ASSERT_NOT_NULL(strstr(json, "index_status"));
    ASSERT_NOT_NULL(strstr(json, "check_index_coverage"));
    ASSERT_NOT_NULL(strstr(json, "detect_changes"));
    ASSERT_NOT_NULL(strstr(json, "manage_adr"));
    ASSERT_NOT_NULL(strstr(json, "ingest_traces"));
    free(json);
    PASS();
}

TEST(mcp_tools_list_latest_metadata) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"title\":\"Search graph\""));
    ASSERT_NOT_NULL(strstr(json, "\"title\":\"Index repository\""));
    ASSERT_NOT_NULL(strstr(json, "\"title\":\"Check index coverage\""));
    ASSERT_NOT_NULL(strstr(json, "\"outputSchema\":{\"type\":\"object\""));
    ASSERT_NOT_NULL(strstr(json, "\"additionalProperties\":true"));
    free(json);
    PASS();
}

TEST(mcp_tools_have_behavior_annotations) {
    struct {
        const char *name;
        bool read_only;
        bool destructive;
        bool idempotent;
        bool open_world;
    } expected[] = {
        {"index_repository", false, false, true, false},
        /* These query tools can reach resolve_store(), whose corrupt-store
         * recovery quarantines/removes database files. Keep the annotations
         * conservative until query resolution is strictly non-mutating. */
        {"search_graph", false, true, true, false},
        {"query_graph", false, true, true, false},
        {"trace_path", false, true, true, false},
        {"get_code_snippet", false, true, true, false},
        {"get_graph_schema", false, true, true, false},
        {"get_architecture", false, true, true, false},
        {"search_code", false, true, true, false},
        {"list_projects", true, false, true, false},
        {"delete_project", false, true, true, false},
        {"index_status", false, true, true, false},
        {"check_index_coverage", false, true, true, false},
        {"detect_changes", false, true, true, false},
        {"manage_adr", false, true, false, false},
        {"ingest_traces", false, false, false, false},
    };

    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *tools = yyjson_obj_get(yyjson_doc_get_root(doc), "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_EQ(yyjson_arr_size(tools), sizeof(expected) / sizeof(expected[0]));

    size_t matched = 0;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tools, &iter);
    yyjson_val *tool;
    while ((tool = yyjson_arr_iter_next(&iter)) != NULL) {
        yyjson_val *name_val = yyjson_obj_get(tool, "name");
        yyjson_val *annotations = yyjson_obj_get(tool, "annotations");
        ASSERT_NOT_NULL(name_val);
        ASSERT_NOT_NULL(annotations);
        ASSERT_TRUE(yyjson_is_obj(annotations));

        const char *name = yyjson_get_str(name_val);
        bool found = false;
        for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
            if (strcmp(name, expected[i].name) != 0) {
                continue;
            }
            yyjson_val *read_only = yyjson_obj_get(annotations, "readOnlyHint");
            yyjson_val *destructive = yyjson_obj_get(annotations, "destructiveHint");
            yyjson_val *idempotent = yyjson_obj_get(annotations, "idempotentHint");
            yyjson_val *open_world = yyjson_obj_get(annotations, "openWorldHint");
            ASSERT_TRUE(yyjson_is_bool(read_only));
            ASSERT_TRUE(yyjson_is_bool(destructive));
            ASSERT_TRUE(yyjson_is_bool(idempotent));
            ASSERT_TRUE(yyjson_is_bool(open_world));
            ASSERT_EQ(yyjson_get_bool(read_only), expected[i].read_only);
            ASSERT_EQ(yyjson_get_bool(destructive), expected[i].destructive);
            ASSERT_EQ(yyjson_get_bool(idempotent), expected[i].idempotent);
            ASSERT_EQ(yyjson_get_bool(open_world), expected[i].open_world);
            found = true;
            matched++;
            break;
        }
        ASSERT_TRUE(found);
    }

    ASSERT_EQ(matched, sizeof(expected) / sizeof(expected[0]));
    yyjson_doc_free(doc);
    free(json);
    PASS();
}

TEST(mcp_index_repository_declares_name_override_issue571) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"index_repository\""));
    ASSERT_NOT_NULL(strstr(json, "\"name\":{\"type\":\"string\""));
    ASSERT_NOT_NULL(strstr(json, "Non-ASCII bytes are encoded"));
    free(json);
    PASS();
}

TEST(mcp_tools_array_schemas_have_items) {
    /* VS Code 1.112+ rejects array schemas without "items" (see
     * https://github.com/microsoft/vscode/issues/248810).
     * Walk every tool's inputSchema and verify that every "type":"array"
     * property also contains "items". */
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    /* Scan for all occurrences of "type":"array" — each must be followed
     * by "items" before the next closing brace of that property. */
    const char *p = json;
    while ((p = strstr(p, "\"type\":\"array\"")) != NULL) {
        /* Find the enclosing '}' for this property object */
        const char *end = strchr(p, '}');
        ASSERT_NOT_NULL(end);
        /* "items" must appear between p and end */
        size_t span = (size_t)(end - p);
        char *segment = malloc(span + 1);
        memcpy(segment, p, span);
        segment[span] = '\0';
        ASSERT_NOT_NULL(strstr(segment, "\"items\"")); /* array missing items */
        free(segment);
        p = end;
    }

    free(json);
    PASS();
}

TEST(mcp_ingest_traces_items_disallow_additional_properties_issue731) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);
    yyjson_val *tools = yyjson_obj_get(root, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    yyjson_val *tool;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tools, &iter);
    yyjson_val *ingest_traces = NULL;
    while ((tool = yyjson_arr_iter_next(&iter)) != NULL) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        if (name && yyjson_is_str(name) && strcmp(yyjson_get_str(name), "ingest_traces") == 0) {
            ingest_traces = tool;
            break;
        }
    }
    ASSERT_NOT_NULL(ingest_traces);

    yyjson_val *input_schema = yyjson_obj_get(ingest_traces, "inputSchema");
    ASSERT_NOT_NULL(input_schema);
    yyjson_val *properties = yyjson_obj_get(input_schema, "properties");
    ASSERT_NOT_NULL(properties);
    yyjson_val *traces = yyjson_obj_get(properties, "traces");
    ASSERT_NOT_NULL(traces);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(traces, "type")), "array");
    yyjson_val *items = yyjson_obj_get(traces, "items");
    ASSERT_NOT_NULL(items);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(items, "type")), "object");
    yyjson_val *item_properties = yyjson_obj_get(items, "properties");
    ASSERT_NOT_NULL(item_properties);
    yyjson_val *caller = yyjson_obj_get(item_properties, "caller");
    ASSERT_NOT_NULL(caller);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(caller, "type")), "string");
    yyjson_val *callee = yyjson_obj_get(item_properties, "callee");
    ASSERT_NOT_NULL(callee);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(callee, "type")), "string");
    yyjson_val *count = yyjson_obj_get(item_properties, "count");
    ASSERT_NOT_NULL(count);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(count, "type")), "integer");
    yyjson_val *additional_properties = yyjson_obj_get(items, "additionalProperties");
    ASSERT_NOT_NULL(additional_properties);
    ASSERT_TRUE(yyjson_is_bool(additional_properties));
    ASSERT_FALSE(yyjson_get_bool(additional_properties));

    yyjson_doc_free(doc);
    free(json);
    PASS();
}

/* Guard for PR #560 (schema enum): the get_architecture aspects items schema
 * must carry an enum of the valid tokens — including the new "overview" —
 * mirroring VALID_ASPECTS in mcp.c. Parsed structurally like
 * mcp_ingest_traces_items_disallow_additional_properties_issue731. */
TEST(mcp_get_architecture_aspects_schema_enum_pr560) {
    char *json = cbm_mcp_tools_list();
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_NOT_NULL(root);
    yyjson_val *tools = yyjson_obj_get(root, "tools");
    ASSERT_NOT_NULL(tools);
    ASSERT_TRUE(yyjson_is_arr(tools));

    yyjson_val *tool;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(tools, &iter);
    yyjson_val *get_arch = NULL;
    while ((tool = yyjson_arr_iter_next(&iter)) != NULL) {
        yyjson_val *name = yyjson_obj_get(tool, "name");
        if (name && yyjson_is_str(name) && strcmp(yyjson_get_str(name), "get_architecture") == 0) {
            get_arch = tool;
            break;
        }
    }
    ASSERT_NOT_NULL(get_arch);

    yyjson_val *input_schema = yyjson_obj_get(get_arch, "inputSchema");
    ASSERT_NOT_NULL(input_schema);
    yyjson_val *properties = yyjson_obj_get(input_schema, "properties");
    ASSERT_NOT_NULL(properties);
    yyjson_val *aspects = yyjson_obj_get(properties, "aspects");
    ASSERT_NOT_NULL(aspects);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(aspects, "type")), "array");
    yyjson_val *items = yyjson_obj_get(aspects, "items");
    ASSERT_NOT_NULL(items);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(items, "type")), "string");
    yyjson_val *enum_arr = yyjson_obj_get(items, "enum");
    ASSERT_NOT_NULL(enum_arr);
    ASSERT_TRUE(yyjson_is_arr(enum_arr));

    /* The enum must be exactly the valid-token set — no more, no less. */
    static const char *expected[] = {"all",      "overview",   "structure", "dependencies",
                                     "routes",   "languages",  "packages",  "entry_points",
                                     "hotspots", "boundaries", "layers",    "file_tree",
                                     "clusters", "cycles"};
    size_t expected_count = sizeof(expected) / sizeof(expected[0]);
    ASSERT_EQ(yyjson_arr_size(enum_arr), expected_count);
    for (size_t i = 0; i < expected_count; i++) {
        bool found = false;
        yyjson_val *ev;
        yyjson_arr_iter eiter;
        yyjson_arr_iter_init(enum_arr, &eiter);
        while ((ev = yyjson_arr_iter_next(&eiter)) != NULL) {
            if (yyjson_is_str(ev) && strcmp(yyjson_get_str(ev), expected[i]) == 0) {
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found);
    }

    yyjson_doc_free(doc);
    free(json);
    PASS();
}

TEST(mcp_text_result) {
    char *json = cbm_mcp_text_result("{\"total\":5}", false);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"type\":\"text\""));
    /* The text value is JSON-escaped inside the "text" field */
    ASSERT_NOT_NULL(strstr(json, "total"));
    ASSERT_NOT_NULL(strstr(json, "\"structuredContent\":{\"total\":5}"));
    ASSERT_NOT_NULL(strstr(json, "\"isError\":false"));
    ASSERT_NULL(strstr(json, "\"isError\":true"));
    free(json);
    PASS();
}

TEST(mcp_text_result_wraps_plain_text_as_structured_content) {
    char *json = cbm_mcp_text_result("plain text", false);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"structuredContent\":{\"text\":\"plain text\"}"));
    ASSERT_NOT_NULL(strstr(json, "\"isError\":false"));
    free(json);
    PASS();
}

TEST(mcp_cancel_matches_request_id) {
    ASSERT_TRUE(cbm_mcp_cancel_request_matches("{\"requestId\":7}", 7, NULL));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":8}", 7, NULL));
    ASSERT_TRUE(cbm_mcp_cancel_request_matches("{\"requestId\":\"call-1\"}", -1, "call-1"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":\"call-2\"}", -1, "call-1"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{\"requestId\":7}", -1, "7"));
    ASSERT_FALSE(cbm_mcp_cancel_request_matches("{}", 7, NULL));
    PASS();
}

TEST(mcp_text_result_error) {
    char *json = cbm_mcp_text_result("something failed", true);
    ASSERT_NOT_NULL(json);
    ASSERT_NOT_NULL(strstr(json, "\"structuredContent\":{\"error\":\"something failed\"}"));
    ASSERT_NOT_NULL(strstr(json, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(json, "something failed"));
    free(json);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_get_tool_name) {
    const char *params = "{\"name\":\"search_graph\",\"arguments\":{\"label\":\"Function\"}}";
    char *name = cbm_mcp_get_tool_name(params);
    ASSERT_NOT_NULL(name);
    ASSERT_STR_EQ(name, "search_graph");
    free(name);
    PASS();
}

TEST(mcp_get_arguments) {
    const char *params =
        "{\"name\":\"search_graph\",\"arguments\":{\"label\":\"Function\",\"limit\":5}}";
    char *args = cbm_mcp_get_arguments(params);
    ASSERT_NOT_NULL(args);
    ASSERT_NOT_NULL(strstr(args, "\"label\":\"Function\""));
    ASSERT_NOT_NULL(strstr(args, "\"limit\":5"));
    free(args);
    PASS();
}

TEST(mcp_get_string_arg) {
    const char *args = "{\"label\":\"Function\",\"name_pattern\":\".*Order.*\"}";
    char *val = cbm_mcp_get_string_arg(args, "label");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, "Function");
    free(val);

    val = cbm_mcp_get_string_arg(args, "name_pattern");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, ".*Order.*");
    free(val);

    val = cbm_mcp_get_string_arg(args, "nonexistent");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_int_arg) {
    const char *args = "{\"limit\":10,\"offset\":5}";
    int val = cbm_mcp_get_int_arg(args, "limit", 0);
    ASSERT_EQ(val, 10);
    val = cbm_mcp_get_int_arg(args, "offset", 0);
    ASSERT_EQ(val, 5);
    val = cbm_mcp_get_int_arg(args, "missing", 42);
    ASSERT_EQ(val, 42);
    PASS();
}

TEST(mcp_get_bool_arg) {
    const char *args = "{\"include_connected\":true,\"regex\":false}";
    bool val = cbm_mcp_get_bool_arg(args, "include_connected");
    ASSERT_TRUE(val);
    val = cbm_mcp_get_bool_arg(args, "regex");
    ASSERT_FALSE(val);
    val = cbm_mcp_get_bool_arg(args, "missing");
    ASSERT_FALSE(val);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SERVER HANDLE — PROTOCOL FLOW
 * ══════════════════════════════════════════════════════════════════ */

TEST(server_handle_initialize) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                                   "\"params\":{\"capabilities\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(resp, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(resp, "capabilities"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_initialized_notification) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* Notification has no id → no response */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
    ASSERT_NULL(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_tools_list) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":2"));
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    ASSERT_NOT_NULL(strstr(resp, "query_graph"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_tools_list_defaults_to_all_tools_and_accepts_cursor) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":200,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":200"));
    ASSERT_NULL(strstr(resp, "\"nextCursor\""));
    ASSERT_NOT_NULL(strstr(resp, "index_repository"));
    ASSERT_NOT_NULL(strstr(resp, "manage_adr"));
    ASSERT_NOT_NULL(strstr(resp, "ingest_traces"));
    free(resp);

    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":202,\"method\":\"tools/list\",\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":202"));
    ASSERT_NULL(strstr(resp, "\"nextCursor\""));
    ASSERT_NOT_NULL(strstr(resp, "manage_adr"));
    ASSERT_NOT_NULL(strstr(resp, "ingest_traces"));
    free(resp);

    resp = cbm_mcp_server_handle(
        srv,
        "{\"jsonrpc\":\"2.0\",\"id\":201,\"method\":\"tools/list\",\"params\":{\"cursor\":\"8\"}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":201"));
    ASSERT_NULL(strstr(resp, "\"nextCursor\""));
    ASSERT_NOT_NULL(strstr(resp, "manage_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_analysis_profile_filters_and_rejects_mutators) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_tool_profile(srv, CBM_MCP_TOOL_PROFILE_ANALYSIS);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":219,\"method\":\"initialize\",\"params\":{}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "analysis tool profile"));
    ASSERT_NOT_NULL(strstr(resp, "check_index_coverage"));
    ASSERT_NULL(strstr(resp, "index_repository"));
    free(resp);

    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":220,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    static const char *const analysis_tools[] = {
        "search_graph",     "query_graph",          "trace_path",     "get_code_snippet",
        "get_graph_schema", "get_architecture",     "search_code",    "list_projects",
        "index_status",     "check_index_coverage", "detect_changes",
    };
    ASSERT_EQ(mcp_response_tool_count(resp), sizeof(analysis_tools) / sizeof(analysis_tools[0]));
    for (size_t i = 0U; i < sizeof(analysis_tools) / sizeof(analysis_tools[0]); i++) {
        ASSERT_TRUE(mcp_response_has_exact_tool(resp, analysis_tools[i]));
    }
    free(resp);

    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":221,\"method\":\"tools/call\","
                                      "\"params\":{\"name\":\"delete_project\","
                                      "\"arguments\":{\"project\":\"anything\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not available in the analysis tool profile"));
    ASSERT_NOT_NULL(strstr(resp, "isError"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_scout_profile_exposes_only_the_fast_tier) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_mcp_server_set_tool_profile(srv, CBM_MCP_TOOL_PROFILE_SCOUT);
    mcp_saw_autoindex_log = false;
    cbm_log_set_sink_ex(mcp_capture_log, CBM_LOG_SINK_REPLACE);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":222,\"method\":\"initialize\",\"params\":{}}");
    cbm_log_set_sink(NULL);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "scout tool profile"));
    ASSERT_NOT_NULL(strstr(resp, "check_index_coverage"));
    ASSERT_NULL(strstr(resp, "index_repository"));
    ASSERT_FALSE(mcp_saw_autoindex_log);
    free(resp);

    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":223,\"method\":\"tools/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_EQ(mcp_response_tool_count(resp), 7U);
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "search_graph"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "trace_path"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "get_code_snippet"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "get_architecture"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "list_projects"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "index_status"));
    ASSERT_TRUE(mcp_response_has_exact_tool(resp, "check_index_coverage"));
    ASSERT_FALSE(mcp_response_has_exact_tool(resp, "query_graph"));
    ASSERT_FALSE(mcp_response_has_exact_tool(resp, "search_code"));
    ASSERT_FALSE(mcp_response_has_exact_tool(resp, "get_graph_schema"));
    ASSERT_FALSE(mcp_response_has_exact_tool(resp, "detect_changes"));
    ASSERT_FALSE(mcp_response_has_exact_tool(resp, "index_repository"));
    free(resp);

    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":224,\"method\":\"tools/call\","
                                      "\"params\":{\"name\":\"query_graph\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not available in the scout tool profile"));
    ASSERT_NOT_NULL(strstr(resp, "isError"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(analysis_profile_arguments_fail_closed_and_disable_http) {
    cbm_mcp_tool_profile_t profile = CBM_MCP_TOOL_PROFILE_ALL;
    const char *no_profile[] = {"codebase-memory-mcp"};
    const char *analysis_equals[] = {"codebase-memory-mcp", "--tool-profile=analysis"};
    const char *analysis_pair[] = {"codebase-memory-mcp", "--tool-profile", "analysis"};
    const char *scout_equals[] = {"codebase-memory-mcp", "--tool-profile=scout"};
    const char *unknown_equals[] = {"codebase-memory-mcp", "--tool-profile=analaysis"};
    const char *unknown_pair[] = {"codebase-memory-mcp", "--tool-profile", "all"};
    const char *missing_value[] = {"codebase-memory-mcp", "--tool-profile"};

    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(1, no_profile, &profile), 0);
    ASSERT_EQ(profile, CBM_MCP_TOOL_PROFILE_ALL);
    ASSERT_TRUE(cbm_mcp_tool_profile_allows_http(profile));

    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(2, analysis_equals, &profile), 0);
    ASSERT_EQ(profile, CBM_MCP_TOOL_PROFILE_ANALYSIS);
    ASSERT_FALSE(cbm_mcp_tool_profile_allows_http(profile));

    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(3, analysis_pair, &profile), 0);
    ASSERT_EQ(profile, CBM_MCP_TOOL_PROFILE_ANALYSIS);
    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(2, scout_equals, &profile), 0);
    ASSERT_EQ(profile, CBM_MCP_TOOL_PROFILE_SCOUT);
    ASSERT_FALSE(cbm_mcp_tool_profile_allows_http(profile));
    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(2, unknown_equals, &profile), -1);
    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(3, unknown_pair, &profile), -1);
    ASSERT_EQ(cbm_mcp_parse_tool_profile_args(2, missing_value, &profile), -1);
    PASS();
}

TEST(hook_windows_path_containment_is_case_insensitive_and_segment_safe) {
    ASSERT_TRUE(cbm_hook_path_contains_for_testing("C:/Repo", "c:/repo/src/main.c", true));
    ASSERT_FALSE(cbm_hook_path_contains_for_testing("C:/Repo", "c:/repository/src/main.c", true));
    ASSERT_FALSE(cbm_hook_path_contains_for_testing("C:/Repo", "c:/repo/src/main.c", false));
    PASS();
}

TEST(server_handle_prompts_list_workflows) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":203,\"method\":\"prompts/list\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":203"));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"explore_codebase\""));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"review_change_impact\""));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"project\""));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"question\""));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"change\""));
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"base_branch\""));
    ASSERT_NOT_NULL(strstr(resp, "\"required\":true"));
    ASSERT_NULL(strstr(resp, "\"nextCursor\""));

    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_prompts_get_workflows) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":204,\"method\":\"prompts/get\","
             "\"params\":{\"name\":\"explore_codebase\",\"arguments\":{"
             "\"project\":\"payments\",\"question\":\"How are refunds routed?\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"role\":\"user\""));
    ASSERT_NOT_NULL(strstr(resp, "\"type\":\"text\""));
    ASSERT_NOT_NULL(strstr(resp, "payments"));
    ASSERT_NOT_NULL(strstr(resp, "How are refunds routed?"));
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    ASSERT_NOT_NULL(strstr(resp, "trace_path"));
    ASSERT_NOT_NULL(strstr(resp, "get_code_snippet"));
    free(resp);

    resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":205,\"method\":\"prompts/get\","
                                   "\"params\":{\"name\":\"review_change_impact\",\"arguments\":{"
                                   "\"project\":\"payments\",\"change\":\"refund retry policy\","
                                   "\"base_branch\":\"develop\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "refund retry policy"));
    ASSERT_NOT_NULL(strstr(resp, "develop"));
    ASSERT_NOT_NULL(strstr(resp, "detect_changes"));
    ASSERT_NOT_NULL(strstr(resp, "trace_path"));
    ASSERT_NOT_NULL(strstr(resp, "include_tests"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_prompts_get_validates_arguments) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":206,\"method\":\"prompts/get\","
                                   "\"params\":{\"name\":\"unknown\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"code\":-32602"));
    ASSERT_NOT_NULL(strstr(resp, "Invalid prompt name"));
    free(resp);

    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":207,\"method\":\"prompts/get\","
                                      "\"params\":{\"name\":\"explore_codebase\",\"arguments\":{"
                                      "\"project\":\"payments\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"code\":-32602"));
    ASSERT_NOT_NULL(strstr(resp, "Missing required prompt arguments"));
    free(resp);

    /* Optional means it may be omitted, not that an explicitly invalid value
     * may be silently substituted. */
    resp = cbm_mcp_server_handle(srv,
                                 "{\"jsonrpc\":\"2.0\",\"id\":208,\"method\":\"prompts/get\","
                                 "\"params\":{\"name\":\"review_change_impact\",\"arguments\":{"
                                 "\"project\":\"payments\",\"change\":\"refund retry policy\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "base_branch \\\"main\\\""));
    free(resp);

    resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":209,\"method\":\"prompts/get\","
                                   "\"params\":{\"name\":\"review_change_impact\",\"arguments\":{"
                                   "\"project\":\"payments\",\"change\":\"refund retry policy\","
                                   "\"base_branch\":\"\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"code\":-32602"));
    ASSERT_NOT_NULL(strstr(resp, "Invalid prompt arguments"));
    free(resp);

    resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":210,\"method\":\"prompts/get\","
                                   "\"params\":{\"name\":\"review_change_impact\",\"arguments\":{"
                                   "\"project\":\"payments\",\"change\":\"refund retry policy\","
                                   "\"base_branch\":17}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"code\":-32602"));
    ASSERT_NOT_NULL(strstr(resp, "Invalid prompt arguments"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_logs_request_without_params) {
    mcp_log_buf[0] = '\0';
    CBMLogLevel prev_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_format(CBM_LOG_FORMAT_TEXT);
    cbm_log_set_sink_ex(mcp_capture_log, CBM_LOG_SINK_REPLACE);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":210,\"method\":\"tools/list\","
                                   "\"params\":{\"token\":\"secret\"}}");
    ASSERT_NOT_NULL(resp);
    free(resp);
    cbm_mcp_server_free(srv);

    cbm_log_set_sink(NULL);
    cbm_log_set_level(prev_level);

    ASSERT_NOT_NULL(strstr(mcp_log_buf, "msg=mcp.request"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "protocol=jsonrpc"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "method=tools/list"));
    ASSERT_NOT_NULL(strstr(mcp_log_buf, "status=ok"));
    ASSERT_NULL(strstr(mcp_log_buf, "token"));
    ASSERT_NULL(strstr(mcp_log_buf, "secret"));
    PASS();
}

TEST(server_handle_unknown_method) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"unknown/method\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "-32601")); /* Method not found */
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TOOL HANDLERS (via server_handle)
 * ══════════════════════════════════════════════════════════════════ */

/* Helper: create a server with an in-memory store populated with test data */
static cbm_mcp_server_t *setup_mcp_with_data(void) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL); /* NULL = in-memory */
    return srv;
}

TEST(tool_list_projects_empty) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":10,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"id\":10"));
    /* Should return a result (possibly empty list) */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_graph_schema_empty) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_graph_schema\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_unknown_tool) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":12,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"nonexistent_tool\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return result with isError */
    ASSERT_NOT_NULL(strstr(resp, "isError"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_graph_basic) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    /* search_graph with no project → should work on empty store */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":13,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_graph\","
                                   "\"arguments\":{\"label\":\"Function\",\"limit\":10}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Forward declarations for helpers defined later in this file */
static cbm_mcp_server_t *setup_snippet_server(char *tmp_dir, size_t tmp_sz);
static void cleanup_snippet_dir(const char *tmp_dir);
static char *extract_text_content(const char *mcp_result);

/* callers_total/callees_total must count what the caller can enumerate: with
 * include_tests=false (default) test-file rows are hidden from the table, so
 * the totals must apply the same filter — a raw visited_count overstated the
 * set (field-eval agent read callers_total=175 against 2 visible rows and
 * distrusted the tool). */
TEST(tool_trace_totals_respect_test_filter) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "totproj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/tot");

    cbm_node_t tgt = {.project = proj,
                      .label = "Function",
                      .name = "tgt",
                      .qualified_name = "totproj.a.tgt",
                      .file_path = "a.c",
                      .start_line = 1,
                      .end_line = 5};
    int64_t tid = cbm_store_upsert_node(st, &tgt);
    ASSERT_GT(tid, 0);
    cbm_node_t prod = {.project = proj,
                       .label = "Function",
                       .name = "prod_caller",
                       .qualified_name = "totproj.a.prod_caller",
                       .file_path = "a.c",
                       .start_line = 10,
                       .end_line = 15};
    int64_t pid = cbm_store_upsert_node(st, &prod);
    ASSERT_GT(pid, 0);
    cbm_node_t tst = {.project = proj,
                      .label = "Function",
                      .name = "test_caller",
                      .qualified_name = "totproj.t.test_caller",
                      .file_path = "tests/test_x.c",
                      .start_line = 1,
                      .end_line = 5};
    int64_t xid = cbm_store_upsert_node(st, &tst);
    ASSERT_GT(xid, 0);
    cbm_edge_t e1 = {.project = proj, .source_id = pid, .target_id = tid, .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(st, &e1), 0);
    cbm_edge_t e2 = {.project = proj, .source_id = xid, .target_id = tid, .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(st, &e2), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":90,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"totproj\","
             "\"function_name\":\"tgt\",\"direction\":\"inbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    free(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "callers_total: 1")); /* test row filtered */
    free(inner);

    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":91,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"totproj\","
             "\"function_name\":\"tgt\",\"direction\":\"inbound\",\"include_tests\":true}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    free(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "callers_total: 2")); /* both visible now */
    free(inner);
    cbm_mcp_server_free(srv);
    PASS();
}

/* SCC condensation (get_architecture aspect "cycles"): a 3-function CALLS
 * cycle A->B->C->A must be reported as one circular dependency of size 3 with
 * all three members; a separate acyclic chain (D->E) must NOT appear. The
 * aspect is opt-in — a default get_architecture call must NOT compute it. */
TEST(tool_get_architecture_cycles_detects_scc) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "cycproj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/cyc");

    const char *names[5] = {"A", "B", "C", "D", "E"};
    int64_t id[5];
    for (int i = 0; i < 5; i++) {
        char qn[32];
        snprintf(qn, sizeof(qn), "cycproj.m.%s", names[i]);
        cbm_node_t n = {.project = proj,
                        .label = "Function",
                        .name = names[i],
                        .qualified_name = qn,
                        .file_path = "m.c",
                        .start_line = i + 1,
                        .end_line = i + 2};
        id[i] = cbm_store_upsert_node(st, &n);
        ASSERT_GT(id[i], 0);
    }
    /* cycle A->B->C->A, plus acyclic D->E */
    struct {
        int f;
        int t;
    } e[] = {{0, 1}, {1, 2}, {2, 0}, {3, 4}};
    for (size_t i = 0; i < sizeof(e) / sizeof(e[0]); i++) {
        cbm_edge_t ed = {
            .project = proj, .source_id = id[e[i].f], .target_id = id[e[i].t], .type = "CALLS"};
        ASSERT_GT(cbm_store_insert_edge(st, &ed), 0);
    }

    /* opt-in cycles aspect */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":71,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"get_architecture\",\"arguments\":{\"project\":\"cycproj\","
             "\"aspects\":[\"cycles\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "cycles: 1")); /* exactly one SCC of size>1 */
    ASSERT_NOT_NULL(strstr(inner, "cycproj.m.A"));
    ASSERT_NOT_NULL(strstr(inner, "cycproj.m.B"));
    ASSERT_NOT_NULL(strstr(inner, "cycproj.m.C"));
    ASSERT_NULL(strstr(inner, "cycproj.m.D")); /* acyclic node not in any cycle */
    free(inner);
    free(resp);

    /* default call (no aspects) must NOT run the scan. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":72,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"get_architecture\",\"arguments\":{\"project\":\"cycproj\"}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "cycles:"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Context-bomb guard: get_code_snippet on a whole-file node (a Module/File
 * span) used to read the ENTIRE file into one response — a field-eval agent
 * that fell back to a Module snippet pulled ~400KB in a single call. The read
 * must clip at MCP_SNIPPET_MAX_LINES and flag source_clipped, while the exact
 * start/end range stays in the response for a targeted re-read. */
TEST(tool_get_code_snippet_clips_whole_file_node) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_snipcap_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char proj_dir[512];
    snprintf(proj_dir, sizeof(proj_dir), "%s/project", tmp);
    cbm_mkdir(proj_dir);
    char src_path[600];
    snprintf(src_path, sizeof(src_path), "%s/big.py", proj_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    enum { BIG_LINES = 2000 };
    for (int i = 0; i < BIG_LINES; i++) {
        fprintf(fp, "line_%04d = %d  # padding to blow up an unclipped read\n", i, i);
    }
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "test-project";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, proj_dir);

    cbm_node_t mod = {0};
    mod.project = proj;
    mod.label = "Module";
    mod.name = "big";
    mod.qualified_name = "test-project.big";
    mod.file_path = "big.py";
    mod.start_line = 1;
    mod.end_line = BIG_LINES;
    ASSERT_GT(cbm_store_upsert_node(st, &mod), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":70,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"get_code_snippet\",\"arguments\":{\"project\":\"test-project\","
             "\"qualified_name\":\"test-project.big\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"source_clipped\":true"));
    /* The whole 2000-line file (~100KB) must NOT be in the response. */
    ASSERT_TRUE(strlen(inner) < 60000);
    /* The last line must be absent (clipped), the first present. */
    ASSERT_NOT_NULL(strstr(inner, "line_0000"));
    ASSERT_NULL(strstr(inner, "line_1999"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    th_rmtree(tmp);
    PASS();
}

TEST(tool_search_graph_includes_node_properties) {
    /* Node properties are OPT-IN columns in the default TOON output: the
     * default row is qn/label/file/lines/degrees only, `fields` adds the
     * requested property columns, and format:"json" restores the legacy
     * verbose objects with the full property blob. The setup_snippet_server
     * inserts HandleRequest with a signature/return_type/is_exported blob. */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Default TOON: compact table, no property spill. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"label\":\"Function\","
             "\"name_pattern\":\"HandleRequest\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"structuredContent\":{\"text\":"));
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "results:")); /* TOON table header */
    ASSERT_NOT_NULL(strstr(inner, "(rows: name label lines in out;"));
    ASSERT_NOT_NULL(strstr(inner, "HandleRequest"));
    ASSERT_NULL(strstr(inner, "func HandleRequest")); /* signature not spilled */
    ASSERT_NULL(strstr(inner, "is_exported"));
    free(inner);
    free(resp);

    /* fields:["signature"] adds the column + values. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":43,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"label\":\"Function\","
             "\"name_pattern\":\"HandleRequest\",\"fields\":[\"signature\"],\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "(rows: name label lines in out signature;"));
    /* values with spaces are QUOTED so column positions survive */
    ASSERT_NOT_NULL(strstr(inner, "\"func HandleRequest() error\""));
    ASSERT_NOT_NULL(strstr(inner, "func HandleRequest"));
    free(inner);
    free(resp);

    /* format:"json" = json-stringified tree: same grouped model, column-
     * ordered row arrays — never per-row key envelopes or property blobs.
     * fields adds columns there too. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":44,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"label\":\"Function\","
             "\"name_pattern\":\"HandleRequest\",\"format\":\"json\","
             "\"fields\":[\"signature\"],\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"qn_prefix\"")); /* grouped tree model */
    ASSERT_NOT_NULL(strstr(inner, "\"cols\""));
    ASSERT_NOT_NULL(strstr(inner, "\"rows\""));
    ASSERT_NOT_NULL(strstr(inner, "\"signature\""));      /* requested column */
    ASSERT_NOT_NULL(strstr(inner, "func HandleRequest")); /* its value */
    ASSERT_NULL(strstr(inner, "is_exported"));            /* blob never spills */
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_output_byte_budgets) {
    /* GUARD: absolute byte ceilings on default tool outputs. Re-bloat (e.g.
     * a property blob sneaking back into row emission — the fp field alone
     * is ~450B/hit) blows these ceilings immediately. The numbers are
     * generous vs the measured compact outputs (search hit rows ≈ 90B) but
     * far below the legacy verbose sizes (≈1.5KB/hit). */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* search_graph: 1-hit search must stay under 600B. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":46,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"label\":\"Function\","
             "\"name_pattern\":\"HandleRequest\",\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "HandleRequest")); /* non-vacuous: hit present */
    ASSERT_LT((int)strlen(inner), 600);
    free(inner);
    free(resp);

    /* trace_path: single-hop trace on the fixture must stay under 800B. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":47,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\","
             "\"arguments\":{\"project\":\"test-project\",\"function_name\":\"HandleRequest\","
             "\"direction\":\"both\",\"depth\":2}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "callees:"));
    ASSERT_LT((int)strlen(inner), 800);
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_search_graph_toon_never_leaks_internal_fields) {
    /* The similarity/semantic pipeline intermediates (fp minhash hex, sp
     * structural profile, bt body-token bag) dominated the legacy payload
     * (~45%) and carry zero agent value. GUARD: they never appear in TOON
     * output — not by default and not even when explicitly requested via
     * fields (blocklist). */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    /* A node whose properties carry the internal fields with sentinels. */
    cbm_node_t n = {0};
    n.project = "test-project";
    n.label = "Function";
    n.name = "fpCarrier";
    n.qualified_name = "test-project.src.fpCarrier";
    n.file_path = "src/fp.go";
    n.start_line = 1;
    n.end_line = 2;
    n.properties_json = "{\"fp\":\"FPSENTINEL00\",\"sp\":\"SPSENTINEL00\","
                        "\"bt\":\"BTSENTINEL00\",\"complexity\":7}";
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":45,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project\":\"test-project\",\"name_pattern\":\"fpCarrier\","
             "\"fields\":[\"fp\",\"sp\",\"bt\",\"complexity\"],\"limit\":5}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "fpCarrier"));
    ASSERT_NULL(strstr(inner, "FPSENTINEL00"));
    ASSERT_NULL(strstr(inner, "SPSENTINEL00"));
    ASSERT_NULL(strstr(inner, "BTSENTINEL00"));
    /* Non-blocked requested field still comes through. */
    ASSERT_NOT_NULL(strstr(inner, "complexity"));
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_lean_defaults_schema_and_status) {
    /* GUARDS for the lean-default contract (TOON round 2):
     * 1. get_graph_schema must not advertise the blocked internal fields
     *    (fp/sp/bt) — the server refuses to emit them, so listing them in the
     *    schema invited agents to request fields they can never get.
     * 2. index_status omits the git context block unless verbose:true — the
     *    worktree/shadow path variants only matter when debugging where an
     *    index lives. */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    cbm_node_t n = {0};
    n.project = "test-project";
    n.label = "Function";
    n.name = "schemaCarrier";
    n.qualified_name = "test-project.src.schemaCarrier";
    n.file_path = "src/sc.go";
    n.start_line = 1;
    n.end_line = 2;
    n.properties_json = "{\"fp\":\"x\",\"sp\":\"y\",\"bt\":\"z\",\"complexity\":3}";
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":48,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_graph_schema\","
                                   "\"arguments\":{\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "Function"));   /* non-vacuous: label present */
    ASSERT_NOT_NULL(strstr(inner, "complexity")); /* obtainable property listed */
    ASSERT_NULL(strstr(inner, "\"fp\""));         /* blocked fields not advertised */
    ASSERT_NULL(strstr(inner, "\"sp\""));
    ASSERT_NULL(strstr(inner, "\"bt\""));
    free(inner);
    free(resp);

    /* index_status: no git block by default... */
    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":49,\"method\":\"tools/call\","
                                      "\"params\":{\"name\":\"index_status\","
                                      "\"arguments\":{\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"status\""));
    ASSERT_NULL(strstr(inner, "\"git\""));
    free(inner);
    free(resp);

    /* ...and present with verbose:true. */
    resp = cbm_mcp_server_handle(srv,
                                 "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"tools/call\","
                                 "\"params\":{\"name\":\"index_status\","
                                 "\"arguments\":{\"project\":\"test-project\",\"verbose\":true}}}");
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"git\""));
    free(inner);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── Tool-output regression suite (gating) ──────────────────────────
 * Context-explosion detector: flags the measured smells that re-introduce
 * token bloat into default outputs, independent of any specific tool:
 *   1. blocked internal fields (fp/sp/bt) appearing anywhere;
 *   2. repeated-key JSON envelopes — the same key emitted per row instead of
 *      a header-once table (the un-TOONed enumeration smell; detect_changes
 *      shipped 4,787x3 of these = 416KB);
 *   3. embedded prose notes/hints beyond one line (~220 chars) — long prose
 *      belongs in tool descriptions or docs, not repeated per response.
 * Returns NULL when clean, else a static description of the violation. */
static const char *output_explosion_smell(const char *inner) {
    static const char *row_keys[] = {
        "\"name\":", "\"label\":", "\"file\":", "\"path\":", "\"qualified_name\":", "\"qn\":"};
    if (strstr(inner, "\"fp\":") || strstr(inner, "\"sp\":") || strstr(inner, "\"bt\":")) {
        return "blocked internal field (fp/sp/bt) leaked into output";
    }
    for (size_t k = 0; k < sizeof(row_keys) / sizeof(row_keys[0]); k++) {
        int n = 0;
        for (const char *p = strstr(inner, row_keys[k]); p && n <= 32;
             p = strstr(p + 1, row_keys[k])) {
            n++;
        }
        if (n > 32) {
            return "repeated-key envelope (>32x same JSON key) — emit a header-once table";
        }
    }
    for (const char *p = strstr(inner, "\"note\":\""); p; p = strstr(p + 1, "\"note\":\"")) {
        const char *end = strchr(p + 9, '"');
        while (end && end[-1] == '\\') {
            end = strchr(end + 1, '"');
        }
        if (end && end - (p + 9) > 220) {
            return "embedded note exceeds one line (~220 chars)";
        }
    }
    return NULL;
}

/* Run one tool call on the fixture server, apply the explosion detector and
 * an absolute byte ceiling, and require a semantic-floor marker so trimming
 * can never hollow the response out either. */
static const char *check_tool_output(cbm_mcp_server_t *srv, const char *req, int ceiling,
                                     const char *floor_marker) {
    char *resp = cbm_mcp_server_handle(srv, req);
    if (!resp) {
        return "no response";
    }
    char *inner = extract_text_content(resp);
    free(resp);
    if (!inner) {
        return "no text content";
    }
    static char why[256];
    const char *smell = output_explosion_smell(inner);
    if (smell) {
        snprintf(why, sizeof(why), "%s", smell);
        free(inner);
        return why;
    }
    if ((int)strlen(inner) >= ceiling) {
        snprintf(why, sizeof(why), "output %d B >= ceiling %d B", (int)strlen(inner), ceiling);
        free(inner);
        return why;
    }
    if (floor_marker && !strstr(inner, floor_marker)) {
        snprintf(why, sizeof(why), "semantic floor missing: %s", floor_marker);
        free(inner);
        return why;
    }
    free(inner);
    return NULL;
}

TEST(tool_output_regression_gate) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    struct {
        const char *req;
        int ceiling;
        const char *floor;
    } cases[] = {
        {"{\"jsonrpc\":\"2.0\",\"id\":70,\"method\":\"tools/call\",\"params\":{"
         "\"name\":\"search_graph\",\"arguments\":{\"project\":\"test-project\","
         "\"name_pattern\":\".*\",\"limit\":50}}}",
         6000, "results:"},
        {"{\"jsonrpc\":\"2.0\",\"id\":71,\"method\":\"tools/call\",\"params\":{"
         "\"name\":\"get_graph_schema\",\"arguments\":{\"project\":\"test-project\"}}}",
         6000, "node_labels"},
        {"{\"jsonrpc\":\"2.0\",\"id\":72,\"method\":\"tools/call\",\"params\":{"
         "\"name\":\"index_status\",\"arguments\":{\"project\":\"test-project\"}}}",
         7000, "\"status\""},
        {"{\"jsonrpc\":\"2.0\",\"id\":73,\"method\":\"tools/call\",\"params\":{"
         "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"test-project\","
         "\"function_name\":\"HandleRequest\",\"direction\":\"both\"}}}",
         1500, "callees:"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *why = check_tool_output(srv, cases[i].req, cases[i].ceiling, cases[i].floor);
        if (why) {
            char msg[320];
            snprintf(msg, sizeof(msg), "case %d: %s", (int)i, why);
            FAIL(msg);
        }
    }

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_search_graph_query_honors_file_pattern_issue552) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "issue-552";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/issue-552");

    cbm_node_t lib_status = {0};
    lib_status.project = proj;
    lib_status.label = "Function";
    lib_status.name = "status";
    lib_status.qualified_name = "issue-552.src.lib.status";
    lib_status.file_path = "src/lib/status.c";
    lib_status.start_line = 1;
    lib_status.end_line = 3;
    ASSERT_GT(cbm_store_upsert_node(st, &lib_status), 0);

    cbm_node_t component_status = {0};
    component_status.project = proj;
    component_status.label = "Function";
    component_status.name = "status";
    component_status.qualified_name = "issue-552.src.components.status";
    component_status.file_path = "src/components/status.c";
    component_status.start_line = 1;
    component_status.end_line = 3;
    ASSERT_GT(cbm_store_upsert_node(st, &component_status), 0);

    cbm_store_exec(st, "INSERT INTO nodes_fts(nodes_fts) VALUES('delete-all');");
    ASSERT_EQ(cbm_store_exec(st,
                             "INSERT INTO nodes_fts(rowid, name, qualified_name, label, "
                             "file_path) "
                             "SELECT id, cbm_camel_split(name), qualified_name, label, file_path "
                             "FROM nodes;"),
              CBM_STORE_OK);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":552,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_graph\","
                                   "\"arguments\":{\"project\":\"issue-552\",\"query\":\"status\","
                                   "\"file_pattern\":\"src/lib/*\",\"limit\":10}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "search_mode: bm25"));
    ASSERT_NOT_NULL(strstr(inner, "src/lib/status.c"));
    ASSERT_NULL(strstr(inner, "src/components/status.c"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Resource discovery methods this server doesn't populate must return EMPTY
 * lists, not -32601 Method-not-found: clients like Cline probe them on connect
 * and surface the errors as a failed connection (#958). */
TEST(mcp_resource_discovery_methods_return_empty_lists) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    struct {
        const char *method;
        const char *want;
    } cases[] = {
        {"resources/list", "\"resources\":[]"},
        {"resources/templates/list", "\"resourceTemplates\":[]"},
    };
    for (int i = 0; i < 2; i++) {
        char reqbuf[256];
        snprintf(reqbuf, sizeof(reqbuf), "{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"%s\"}",
                 100 + i, cases[i].method);
        char *resp = cbm_mcp_server_handle(srv, reqbuf);
        ASSERT_NOT_NULL(resp);
        ASSERT_NULL(strstr(resp, "Method not found"));
        ASSERT_NOT_NULL(strstr(resp, cases[i].want));
        free(resp);
    }

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_basic) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":14,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"query_graph\","
             "\"arguments\":{\"query\":\"MATCH (f:Function) RETURN f.name\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_index_status_no_project) {
    cbm_mcp_server_t *srv = setup_mcp_with_data();

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":15,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"index_status\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error or empty status */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce the exact-file false negative in the current Read hook: index_status
 * intentionally caps each coverage category at 500 entries, so a later path is
 * absent even though the authoritative index_coverage table contains it.  The
 * targeted coverage tool must query that table rather than scan the capped
 * presentation response. */
TEST(tool_check_index_coverage_finds_path_beyond_status_cap) {
    enum { ROW_COUNT = 502 };
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *project = "coverage-cap-regression";
    ASSERT_EQ(cbm_store_upsert_project(st, project, "/tmp/coverage-cap-regression"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, project);

    char (*paths)[64] = calloc(ROW_COUNT, sizeof(*paths));
    cbm_coverage_row_t *rows = calloc(ROW_COUNT, sizeof(*rows));
    ASSERT_NOT_NULL(paths);
    ASSERT_NOT_NULL(rows);
    for (int i = 0; i < ROW_COUNT; i++) {
        snprintf(paths[i], sizeof(paths[i]), "src/partial-%04d.c", i);
        rows[i].rel_path = paths[i];
        rows[i].kind = "parse_partial";
        rows[i].detail = i == ROW_COUNT - 1 ? "777-790" : "1-2";
        ASSERT_EQ(cbm_store_upsert_file_hash(st, project, paths[i], "fixture", i + 1, 10),
                  CBM_STORE_OK);
    }
    ASSERT_EQ(cbm_store_coverage_replace(st, project, rows, ROW_COUNT), CBM_STORE_OK);

    char *status =
        cbm_mcp_handle_tool(srv, "index_status", "{\"project\":\"coverage-cap-regression\"}");
    ASSERT_NOT_NULL(status);
    char *status_inner = extract_text_content(status);
    ASSERT_NOT_NULL(status_inner);
    ASSERT_NOT_NULL(strstr(status_inner, "\"truncated\":true"));
    ASSERT_NULL(strstr(status_inner, "src/partial-0501.c"));
    free(status_inner);
    free(status);

    char *coverage = cbm_mcp_handle_tool(
        srv, "check_index_coverage",
        "{\"project\":\"coverage-cap-regression\",\"paths\":[\"src/partial-0501.c\"]}");
    ASSERT_NOT_NULL(coverage);
    char *coverage_inner = extract_text_content(coverage);
    ASSERT_NOT_NULL(coverage_inner);
    ASSERT_NOT_NULL(strstr(coverage_inner, "src/partial-0501.c"));
    ASSERT_NOT_NULL(strstr(coverage_inner, "\"status\":\"partial\""));
    ASSERT_NOT_NULL(strstr(coverage_inner, "777-790"));

    free(coverage_inner);
    free(coverage);
    free(rows);
    free(paths);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_check_index_coverage_reports_paths_scopes_and_ranges) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    ASSERT_EQ(cbm_store_upsert_file_hash(st, "test-project", "main.go", "", 0, 0), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_upsert_file_hash(st, "test-project", "src/skip.c", "", 0, 0), CBM_STORE_OK);
    cbm_coverage_row_t rows[] = {
        {.rel_path = "main.go", .kind = "parse_partial", .detail = "3-4,9"},
        {.rel_path = "generated", .kind = "not_indexed_dir", .detail = "excluded subtree"},
        {.rel_path = "src/skip.c", .kind = "oversized", .detail = "file exceeds cap"},
    };
    ASSERT_EQ(cbm_store_coverage_replace(st, "test-project", rows, 3), CBM_STORE_OK);

    char *coverage =
        cbm_mcp_handle_tool(srv, "check_index_coverage",
                            "{\"project\":\"test-project\","
                            "\"paths\":[\"main.go\",\"generated/pkg/a.c\",\"../escape.c\"],"
                            "\"scopes\":[\".\"]}");
    ASSERT_NOT_NULL(coverage);
    char *inner = extract_text_content(coverage);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"path\":\"main.go\""));
    ASSERT_NOT_NULL(strstr(inner, "\"status\":\"partial\""));
    ASSERT_NOT_NULL(strstr(inner, "\"start\":3"));
    ASSERT_NOT_NULL(strstr(inner, "\"end\":4"));
    ASSERT_NOT_NULL(strstr(inner, "\"start\":9"));
    ASSERT_NOT_NULL(strstr(inner, "generated/pkg/a.c"));
    ASSERT_NOT_NULL(strstr(inner, "not_indexed_dir"));
    ASSERT_NOT_NULL(strstr(inner, "outside_project"));
    ASSERT_NOT_NULL(strstr(inner, "src/skip.c"));
    ASSERT_NOT_NULL(strstr(inner, "file exceeds cap"));
    ASSERT_NOT_NULL(strstr(inner, "best_effort"));

    free(inner);
    free(coverage);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

static int write_coverage_meta(cbm_store_t *store, const char *generation,
                               const char *recording_status) {
    cbm_coverage_meta_t meta = {
        .generation = generation,
        .index_mode = "fast",
        .recorded_at = "2026-07-12T00:00:00Z",
        .recording_status = recording_status,
        .ignored_files_stored = 0,
        .ignored_files_total = 0,
        .coverage_version = 1,
        .hash_records_complete = true,
    };
    return cbm_store_coverage_replace_ex(store, "test-project", NULL, 0, &meta);
}

TEST(tool_check_index_coverage_rejects_stale_generation) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(write_coverage_meta(store, "stale-generation", "complete"), CBM_STORE_OK);

    char *response = cbm_mcp_handle_tool(srv, "check_index_coverage",
                                         "{\"project\":\"test-project\",\"paths\":[\"main.go\"]}");
    ASSERT_NOT_NULL(response);
    char *inner = extract_text_content(response);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"generation_matches\":false"));
    ASSERT_NOT_NULL(strstr(inner, "\"status\":\"coverage_unavailable\""));
    ASSERT_NOT_NULL(strstr(inner, "\"recommended_action\":\"read_source_and_reindex\""));

    free(inner);
    free(response);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_check_index_coverage_requires_source_when_file_metadata_changed) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    cbm_project_t project = {0};
    ASSERT_EQ(cbm_store_get_project(store, "test-project", &project), CBM_STORE_OK);
    ASSERT_EQ(write_coverage_meta(store, project.indexed_at, "complete"), CBM_STORE_OK);
    cbm_project_free_fields(&project);
    ASSERT_EQ(cbm_store_upsert_file_hash(store, "test-project", "main.go", "fixture", 0, 0),
              CBM_STORE_OK);

    char *response = cbm_mcp_handle_tool(srv, "check_index_coverage",
                                         "{\"project\":\"test-project\",\"paths\":[\"main.go\"]}");
    ASSERT_NOT_NULL(response);
    char *inner = extract_text_content(response);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"generation_matches\":true"));
    ASSERT_NOT_NULL(strstr(inner, "\"freshness\":\"metadata_changed\""));
    ASSERT_NOT_NULL(strstr(inner, "\"recommended_action\":\"read_source_and_reindex\""));

    free(inner);
    free(response);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_check_index_coverage_surfaces_lookup_errors) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    cbm_project_t project = {0};
    ASSERT_EQ(cbm_store_get_project(store, "test-project", &project), CBM_STORE_OK);
    ASSERT_EQ(write_coverage_meta(store, project.indexed_at, "complete"), CBM_STORE_OK);
    cbm_project_free_fields(&project);
    ASSERT_EQ(
        cbm_store_exec(store, "ALTER TABLE index_coverage RENAME COLUMN detail TO broken_detail;"),
        CBM_STORE_OK);

    char *response = cbm_mcp_handle_tool(
        srv, "check_index_coverage",
        "{\"project\":\"test-project\",\"paths\":[\"main.go\"],\"scopes\":[\".\"]}");
    ASSERT_NOT_NULL(response);
    char *inner = extract_text_content(response);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"coverage_lookup\":\"error\""));
    ASSERT_NOT_NULL(strstr(inner, "\"status\":\"coverage_unavailable\""));
    ASSERT_NULL(strstr(inner, "\"status\":\"no_recorded_issue\""));

    free(inner);
    free(response);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

TEST(tool_index_status_includes_git_metadata) {
    /* The git context block moved behind verbose:true (lean-default contract,
     * TOON round 2) — this test pins the verbose path's content; the default-
     * omission guard lives in tool_lean_defaults_schema_and_status. */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":16,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"index_status\","
             "\"arguments\":{\"project\":\"test-project\",\"verbose\":true}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "\"root_path\""));
    ASSERT_NOT_NULL(strstr(inner, "\"git\""));
    ASSERT_NOT_NULL(strstr(inner, "\"is_git\":false"));
    ASSERT_NOT_NULL(strstr(inner, "\"root_exists\":true"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  TOOL HANDLERS WITH DATA
 * ══════════════════════════════════════════════════════════════════ */

TEST(tool_trace_call_path_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":20,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_call_path\","
                                   "\"arguments\":{\"function_name\":\"NonExistent\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about project not found */
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_trace_missing_function_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":21,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"trace_call_path\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression: two same-named definitions with equal rank must be reported
 * ambiguous, not silently traced (trace_path previously took nodes[0]). */
TEST(tool_trace_call_path_ambiguous) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "amb-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/amb");
    cbm_node_t a = {.project = proj,
                    .label = "Function",
                    .name = "amb",
                    .qualified_name = "amb-proj.a.amb",
                    .file_path = "a.c",
                    .start_line = 10,
                    .end_line = 20};
    cbm_node_t b = {.project = proj,
                    .label = "Function",
                    .name = "amb",
                    .qualified_name = "amb-proj.b.amb",
                    .file_path = "b.c",
                    .start_line = 10,
                    .end_line = 20}; /* equal span -> genuine tie */
    ASSERT_GT(cbm_store_upsert_node(st, &a), 0);
    ASSERT_GT(cbm_store_upsert_node(st, &b), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":61,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\","
             "\"arguments\":{\"function_name\":\"amb\",\"project\":\"amb-proj\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "ambiguous"));
    ASSERT_NOT_NULL(strstr(inner, "suggestions"));
    ASSERT_NULL(strstr(inner, "\"callees\""));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Multi-seed union hop semantics: bfs_union_same_name deduped visited nodes
 * keep-FIRST-seen, so a node reached at hop 2 from the first seed kept hop 2
 * even when the second seed reaches it at hop 1. hop feeds risk_labels and
 * (soon) pagination watermarks — it must be the MINIMUM across seeds, matching
 * the single-BFS MIN(hop) semantics (#797). */
TEST(tool_trace_union_records_min_hop_across_seeds) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "dualproj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/dual");

    /* One real definition + one body-less stub (start==end) — the #546/#650
     * shape pick_resolved_node resolves WITHOUT ambiguity while
     * bfs_union_same_name still traverses both. Seed A (real def, lower id,
     * traversed first) reaches tgt only via mid (hop 2); the stub seed B
     * reaches tgt directly (hop 1). */
    cbm_node_t sa = {.project = proj,
                     .label = "Function",
                     .name = "dual",
                     .qualified_name = "dualproj.a.dual",
                     .file_path = "a.c",
                     .start_line = 1,
                     .end_line = 50};
    cbm_node_t sb = {.project = proj,
                     .label = "Function",
                     .name = "dual",
                     .qualified_name = "dualproj.b.dual",
                     .file_path = "b.d.ts",
                     .start_line = 1,
                     .end_line = 1};
    cbm_node_t mid = {.project = proj,
                      .label = "Function",
                      .name = "mid",
                      .qualified_name = "dualproj.c.mid",
                      .file_path = "c.c",
                      .start_line = 1,
                      .end_line = 5};
    cbm_node_t tgt = {.project = proj,
                      .label = "Function",
                      .name = "tgt",
                      .qualified_name = "dualproj.c.tgt",
                      .file_path = "c.c",
                      .start_line = 10,
                      .end_line = 15};
    int64_t ida = cbm_store_upsert_node(st, &sa);
    int64_t idb = cbm_store_upsert_node(st, &sb);
    int64_t idm = cbm_store_upsert_node(st, &mid);
    int64_t idt = cbm_store_upsert_node(st, &tgt);
    ASSERT_GT(ida, 0);
    ASSERT_GT(idb, 0);
    ASSERT_GT(idm, 0);
    ASSERT_GT(idt, 0);
    cbm_edge_t e1 = {.project = proj, .source_id = ida, .target_id = idm, .type = "CALLS"};
    cbm_edge_t e2 = {.project = proj, .source_id = idm, .target_id = idt, .type = "CALLS"};
    cbm_edge_t e3 = {.project = proj, .source_id = idb, .target_id = idt, .type = "CALLS"};
    ASSERT_GT(cbm_store_insert_edge(st, &e1), 0);
    ASSERT_GT(cbm_store_insert_edge(st, &e2), 0);
    ASSERT_GT(cbm_store_insert_edge(st, &e3), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":62,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\","
             "\"arguments\":{\"function_name\":\"dual\",\"project\":\"dualproj\","
             "\"direction\":\"outbound\",\"depth\":3}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    /* tgt is one hop from seed B — the union must record hop 1, not seed A's 2. */
    ASSERT_NOT_NULL(strstr(inner, "  tgt 1"));
    ASSERT_NULL(strstr(inner, "  tgt 2"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Exactly-once trace pagination: 12 callees paged at limit=5 must yield
 * 5+5+2 rows with every callee appearing on exactly one page, exact totals
 * on every page, and a final page without a cursor. Stale and mismatched
 * cursors must fail with teaching errors, never silently restart. */
TEST(tool_trace_pagination_exactly_once) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "pageproj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/page");

    cbm_node_t hub = {.project = proj,
                      .label = "Function",
                      .name = "hub",
                      .qualified_name = "pageproj.h.hub",
                      .file_path = "h.c",
                      .start_line = 1,
                      .end_line = 9};
    int64_t hid = cbm_store_upsert_node(st, &hub);
    ASSERT_GT(hid, 0);
    enum { CALLEES = 12 };
    for (int i = 0; i < CALLEES; i++) {
        char nm[16];
        char qn[48];
        snprintf(nm, sizeof(nm), "c%02d", i);
        snprintf(qn, sizeof(qn), "pageproj.m.c%02d", i);
        cbm_node_t n = {.project = proj,
                        .label = "Function",
                        .name = nm,
                        .qualified_name = qn,
                        .file_path = "m.c",
                        .start_line = 1,
                        .end_line = 3};
        int64_t nid = cbm_store_upsert_node(st, &n);
        ASSERT_GT(nid, 0);
        cbm_edge_t e = {.project = proj, .source_id = hid, .target_id = nid, .type = "CALLS"};
        ASSERT_GT(cbm_store_insert_edge(st, &e), 0);
    }

    char pages[3][4096];
    char tok[192] = "";
    int npages = 0;
    for (; npages < 3; npages++) {
        char req[640];
        if (tok[0]) {
            snprintf(req, sizeof(req),
                     "{\"jsonrpc\":\"2.0\",\"id\":80,\"method\":\"tools/call\",\"params\":{"
                     "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"pageproj\","
                     "\"function_name\":\"hub\",\"direction\":\"outbound\",\"limit\":5,"
                     "\"cursor\":\"%s\"}}}",
                     tok);
        } else {
            snprintf(req, sizeof(req),
                     "{\"jsonrpc\":\"2.0\",\"id\":80,\"method\":\"tools/call\",\"params\":{"
                     "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"pageproj\","
                     "\"function_name\":\"hub\",\"direction\":\"outbound\",\"limit\":5}}}");
        }
        char *resp = cbm_mcp_server_handle(srv, req);
        ASSERT_NOT_NULL(resp);
        char *inner = extract_text_content(resp);
        free(resp);
        ASSERT_NOT_NULL(inner);
        snprintf(pages[npages], sizeof(pages[npages]), "%s", inner);
        ASSERT_NOT_NULL(strstr(inner, "callees_total: 12")); /* exact total, every page */
        const char *nx = strstr(inner, "next: ");
        if (nx) {
            const char *e = strchr(nx + 6, '\n');
            size_t tl = e ? (size_t)(e - (nx + 6)) : strlen(nx + 6);
            ASSERT_TRUE(tl < sizeof(tok));
            memcpy(tok, nx + 6, tl);
            tok[tl] = '\0';
        } else {
            tok[0] = '\0';
        }
        free(inner);
        if (!tok[0]) {
            npages++;
            break;
        }
    }
    ASSERT_EQ(npages, 3); /* 5 + 5 + 2 */
    /* Exactly-once: every callee appears on exactly ONE page. */
    for (int i = 0; i < CALLEES; i++) {
        char qn[48];
        snprintf(qn, sizeof(qn), "  c%02d 1\n", i);
        int seen = 0;
        for (int p = 0; p < 3; p++) {
            if (strstr(pages[p], qn)) {
                seen++;
            }
        }
        ASSERT_EQ(seen, 1);
    }
    /* Final page carries no cursor. */
    ASSERT_NULL(strstr(pages[2], "next: "));

    /* Params mismatch: replay a page-2-era cursor with a different depth. */
    const char *nx1 = strstr(pages[0], "next: ");
    ASSERT_NOT_NULL(nx1);
    char tok1[192];
    const char *e1 = strchr(nx1 + 6, '\n');
    size_t tl1 = e1 ? (size_t)(e1 - (nx1 + 6)) : strlen(nx1 + 6);
    memcpy(tok1, nx1 + 6, tl1);
    tok1[tl1] = '\0';
    char req2[640];
    snprintf(req2, sizeof(req2),
             "{\"jsonrpc\":\"2.0\",\"id\":81,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"pageproj\","
             "\"function_name\":\"hub\",\"direction\":\"outbound\",\"limit\":5,\"depth\":2,"
             "\"cursor\":\"%s\"}}}",
             tok1);
    char *resp = cbm_mcp_server_handle(srv, req2);
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    free(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "cursor_params_mismatch"));
    free(inner);

    /* Stale: an index run (upsert_project bumps the generation) invalidates
     * outstanding cursors with a loud, actionable error. */
    cbm_store_upsert_project(st, proj, "/tmp/page");
    snprintf(req2, sizeof(req2),
             "{\"jsonrpc\":\"2.0\",\"id\":82,\"method\":\"tools/call\",\"params\":{"
             "\"name\":\"trace_call_path\",\"arguments\":{\"project\":\"pageproj\","
             "\"function_name\":\"hub\",\"direction\":\"outbound\",\"limit\":5,"
             "\"cursor\":\"%s\"}}}",
             tok1);
    resp = cbm_mcp_server_handle(srv, req2);
    ASSERT_NOT_NULL(resp);
    inner = extract_text_content(resp);
    free(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "stale_cursor"));
    free(inner);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression: when same-named nodes differ in rank, trace must pick the real
 * definition (callable, larger body) — NOT nodes[0]. The Module is inserted
 * first; if trace took nodes[0] the outbound trace would be empty. */
TEST(tool_trace_call_path_prefers_definition) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "pref-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/pref");
    /* nodes[0]: the WRONG match (a Module, tiny span), inserted first. */
    cbm_node_t wrong = {.project = proj,
                        .label = "Module",
                        .name = "dup",
                        .qualified_name = "pref-proj.dup",
                        .file_path = "dup.x",
                        .start_line = 1,
                        .end_line = 1};
    /* the real definition: a Function with a body. */
    cbm_node_t def = {.project = proj,
                      .label = "Function",
                      .name = "dup",
                      .qualified_name = "pref-proj.src.dup",
                      .file_path = "src/dup.c",
                      .start_line = 10,
                      .end_line = 50};
    cbm_node_t callee = {.project = proj,
                         .label = "Function",
                         .name = "callee",
                         .qualified_name = "pref-proj.src.callee",
                         .file_path = "src/dup.c",
                         .start_line = 60,
                         .end_line = 70};
    ASSERT_GT(cbm_store_upsert_node(st, &wrong), 0);
    int64_t id_def = cbm_store_upsert_node(st, &def);
    int64_t id_callee = cbm_store_upsert_node(st, &callee);
    ASSERT_GT(id_def, 0);
    ASSERT_GT(id_callee, 0);
    cbm_edge_t e = {.project = proj, .source_id = id_def, .target_id = id_callee, .type = "CALLS"};
    cbm_store_insert_edge(st, &e);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":62,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"dup\","
             "\"project\":\"pref-proj\",\"direction\":\"outbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "ambiguous"));
    /* picked the Function definition -> its outbound CALLS edge to "callee" shows */
    ASSERT_NOT_NULL(strstr(inner, "callee"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first (#887): the client-supplied `depth` on trace_call_path must be
 * clamped to the MCP ceiling (cbm_mcp_max_depth(), default 15). On origin/main
 * an MCP_MAX_DEPTH=15 constant was defined but never applied — `depth` flowed
 * straight into bfs_union_same_name, so an unbounded value drives the shared
 * cbm_store_bfs to arbitrary depth. Over an 18-node call chain, depth=1000
 * reaches n16/n17 (RED); with the clamp the walk stops at hop 15, so n15 is
 * reached but n16 is not (GREEN). Quoted tokens ("n15"/"n16") match only the
 * node-name field, never the qualified_name (preceded by '.'), so the boundary
 * check is exact. */
TEST(tool_trace_call_path_depth_clamped) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "depth-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/depth");

    /* Linear call chain n00 -CALLS-> n01 -> ... -> n17 (18 nodes). */
    int64_t ids[18];
    for (int i = 0; i < 18; i++) {
        char name[8];
        char qn[32];
        snprintf(name, sizeof(name), "n%02d", i);
        snprintf(qn, sizeof(qn), "depth-proj.n%02d", i);
        cbm_node_t n = {.project = proj,
                        .label = "Function",
                        .name = name,
                        .qualified_name = qn,
                        .file_path = "chain.c",
                        .start_line = 1,
                        .end_line = 2};
        ids[i] = cbm_store_upsert_node(st, &n);
    }
    for (int i = 0; i < 17; i++) {
        cbm_edge_t e = {
            .project = proj, .source_id = ids[i], .target_id = ids[i + 1], .type = "CALLS"};
        cbm_store_insert_edge(st, &e);
    }

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":71,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"n00\","
             "\"project\":\"depth-proj\",\"direction\":\"outbound\",\"depth\":1000}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* Reached within the ceiling (proves the traversal ran) but clamped at 15.
     * TOON rows carry bare QNs, so match the names unquoted. */
    ASSERT_NOT_NULL(strstr(inner, "n15"));
    ASSERT_NULL(strstr(inner, "n16"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first (#650, distilled): two GENUINELY-DIFFERENT same-named functions
 * whose bodies differ in length score differently, so the old exact-tie check did
 * not flag them ambiguous — and bfs_union_same_name (#546) then merged the caller
 * sets of both into one confidently-conflated answer (the mirror of #546's under-
 * report). The fix: 2+ real callable defs => ambiguous (disambiguate), never union
 * distinct symbols. RED before the pick_resolved_node real_def_count rule (response
 * merged callerA+callerB), GREEN after (response is ambiguous, no "callers"). */
TEST(tool_trace_call_path_distinct_defs_not_over_unioned) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "ou-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/ou");
    /* two unrelated real definitions of "dupreal", DIFFERENT body spans */
    cbm_node_t da = {.project = proj,
                     .label = "Function",
                     .name = "dupreal",
                     .qualified_name = "ou-proj.a.dupreal",
                     .file_path = "a.c",
                     .start_line = 10,
                     .end_line = 20}; /* span 10 */
    cbm_node_t db = {.project = proj,
                     .label = "Function",
                     .name = "dupreal",
                     .qualified_name = "ou-proj.b.dupreal",
                     .file_path = "b.c",
                     .start_line = 10,
                     .end_line = 40}; /* span 30 (no tie) */
    cbm_node_t ca = {.project = proj,
                     .label = "Function",
                     .name = "callerA",
                     .qualified_name = "ou-proj.a.callerA",
                     .file_path = "a.c",
                     .start_line = 30,
                     .end_line = 40};
    cbm_node_t cb = {.project = proj,
                     .label = "Function",
                     .name = "callerB",
                     .qualified_name = "ou-proj.b.callerB",
                     .file_path = "b.c",
                     .start_line = 50,
                     .end_line = 60};
    int64_t id_da = cbm_store_upsert_node(st, &da);
    int64_t id_db = cbm_store_upsert_node(st, &db);
    int64_t id_ca = cbm_store_upsert_node(st, &ca);
    int64_t id_cb = cbm_store_upsert_node(st, &cb);
    ASSERT_GT(id_da, 0);
    ASSERT_GT(id_db, 0);
    ASSERT_GT(id_ca, 0);
    ASSERT_GT(id_cb, 0);
    cbm_edge_t ea = {.project = proj, .source_id = id_ca, .target_id = id_da, .type = "CALLS"};
    cbm_edge_t eb = {.project = proj, .source_id = id_cb, .target_id = id_db, .type = "CALLS"};
    cbm_store_insert_edge(st, &ea);
    cbm_store_insert_edge(st, &eb);

    char *resp = cbm_mcp_server_handle(
        srv,
        "{\"jsonrpc\":\"2.0\",\"id\":63,\"method\":\"tools/call\","
        "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"dupreal\","
        "\"project\":\"ou-proj\",\"direction\":\"inbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    /* distinct symbols must be disambiguated, not merged into one caller set */
    ASSERT_NOT_NULL(strstr(inner, "ambiguous"));
    ASSERT_NOT_NULL(strstr(inner, "suggestions"));
    ASSERT_NULL(strstr(inner, "\"callers\""));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Guard that the ambiguity gate does NOT regress the #546 fix: a real .ts
 * implementation plus a body-less ambient .d.ts stub is ONE logical symbol
 * (one real callable def + a fragment), so it must stay non-ambiguous and the
 * caller sets from both nodes must be unioned. */
TEST(tool_trace_call_path_dts_stub_unions_with_impl) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "dts-proj";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/dts");
    cbm_node_t impl = {.project = proj,
                       .label = "Function",
                       .name = "sym546",
                       .qualified_name = "dts-proj.impl.sym546",
                       .file_path = "src/sym.ts",
                       .start_line = 10,
                       .end_line = 30}; /* real body */
    cbm_node_t stub = {.project = proj,
                       .label = "Function",
                       .name = "sym546",
                       .qualified_name = "dts-proj.stub.sym546",
                       .file_path = "types/sym.d.ts",
                       .start_line = 5,
                       .end_line = 5}; /* body-less ambient decl */
    cbm_node_t crel = {.project = proj,
                       .label = "Function",
                       .name = "callerRel",
                       .qualified_name = "dts-proj.callerRel",
                       .file_path = "src/rel.ts",
                       .start_line = 1,
                       .end_line = 8};
    cbm_node_t cali = {.project = proj,
                       .label = "Function",
                       .name = "callerAlias",
                       .qualified_name = "dts-proj.callerAlias",
                       .file_path = "src/ali.ts",
                       .start_line = 1,
                       .end_line = 8};
    int64_t id_impl = cbm_store_upsert_node(st, &impl);
    int64_t id_stub = cbm_store_upsert_node(st, &stub);
    int64_t id_crel = cbm_store_upsert_node(st, &crel);
    int64_t id_cali = cbm_store_upsert_node(st, &cali);
    ASSERT_GT(id_impl, 0);
    ASSERT_GT(id_stub, 0);
    ASSERT_GT(id_crel, 0);
    ASSERT_GT(id_cali, 0);
    /* callers split by import style: relative -> impl, path-alias -> stub */
    cbm_edge_t er = {.project = proj, .source_id = id_crel, .target_id = id_impl, .type = "CALLS"};
    cbm_edge_t el = {.project = proj, .source_id = id_cali, .target_id = id_stub, .type = "CALLS"};
    cbm_store_insert_edge(st, &er);
    cbm_store_insert_edge(st, &el);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":64,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"trace_call_path\",\"arguments\":{\"function_name\":\"sym546\","
             "\"project\":\"dts-proj\",\"direction\":\"inbound\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NULL(strstr(inner, "ambiguous"));
    /* union across impl + stub: BOTH callers appear (this is the #546 fix) */
    ASSERT_NOT_NULL(strstr(inner, "callerRel"));
    ASSERT_NOT_NULL(strstr(inner, "callerAlias"));
    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_delete_project_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":22,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"delete_project\","
                                   "\"arguments\":{\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not_found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_delete_project_mutation_guard_blocks_then_releases) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-mcp-delete-guard-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-delete-project";
    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, project, "/tmp/guard-delete-project"), CBM_STORE_OK);
    cbm_store_close(setup);
    ASSERT_TRUE(cbm_file_exists(db_path));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t probe = {.deny_begin_call = 1};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char *resp =
        cbm_mcp_handle_tool(srv, "delete_project", "{\"project\":\"guard-delete-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "blocked"));
    ASSERT_EQ(probe.begin_count, 1);
    ASSERT_EQ(probe.end_count, 0);
    ASSERT_STR_EQ(probe.begin_projects[0], project);
    ASSERT_TRUE(cbm_file_exists(db_path));
    free(resp);

    probe.deny_begin_call = 0;
    resp = cbm_mcp_handle_tool(srv, "delete_project", "{\"project\":\"guard-delete-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "deleted"));
    ASSERT_EQ(probe.begin_count, 2);
    ASSERT_EQ(probe.end_count, 1);
    ASSERT_STR_EQ(probe.begin_projects[1], project);
    ASSERT_STR_EQ(probe.end_projects[0], project);
    ASSERT_FALSE(cbm_file_exists(db_path));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    cbm_rmdir(cache);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    PASS();
}

TEST(tool_index_repository_mutation_guard_blocks_before_local_worker) {
    char root[CBM_SZ_1K];
    (void)snprintf(root, sizeof(root), "%s/cbm-index-guard-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(root));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t probe = {.deny_begin_call = 1};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char args[CBM_SZ_2K];
    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"GuardedIndex\","
                   "\"mode\":\"fast\"}",
                   root);
    int spawn_before = cbm_index_supervisor_spawn_count();
    char *response = cbm_mcp_handle_tool(srv, "index_repository", args);
    int spawn_after = cbm_index_supervisor_spawn_count();

    ASSERT_NOT_NULL(response);
    ASSERT_NOT_NULL(strstr(response, "blocked"));
    ASSERT_EQ(probe.begin_count, 1);
    ASSERT_EQ(probe.end_count, 0);
    ASSERT_STR_EQ(probe.begin_projects[0], "GuardedIndex");
    ASSERT_EQ(spawn_after, spawn_before);

    free(response);
    cbm_mcp_server_free(srv);
    (void)th_rmtree(root);
    PASS();
}

TEST(tool_get_architecture_empty) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":24,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_architecture\","
                                   "\"arguments\":{\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* No store for nonexistent project — should return project error */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression for #281: handle_get_architecture must actually call
 * cbm_store_get_architecture and surface its sections. Before the fix
 * only label/edge histograms were emitted regardless of which aspects
 * were requested. The store-side arch_entry_points query reads
 * properties.is_entry_point on Function nodes, so we tag one node and
 * assert the resulting JSON surfaces an "entry_points" array containing
 * the tagged function — which is impossible without the wiring. */
TEST(tool_get_architecture_emits_populated_sections) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch-test";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch-test");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "arch-test.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":91,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-test\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* The handler always emits node/edge counts and schema histograms;
     * those existed before #281. The "entry_points" array only appears
     * when cbm_store_get_architecture is actually called and its result
     * is serialized — which is exactly what #281 wires up. */
    ASSERT_NOT_NULL(strstr(inner, "entry_points:"));
    ASSERT_NOT_NULL(strstr(inner, "main"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Distills PR #560 (overview subset): "overview" must expand to a compact
 * subset — every aspect EXCEPT file_tree. Before the fix, "overview" was not
 * registered in either aspect gate (want_aspect in store.c, aspect_wanted in
 * mcp.c), so aspects=["overview"] silently degraded to just
 * {total_nodes,total_edges}. RED on unfixed code: no "entry_points" key. */
TEST(tool_get_architecture_overview_compact_subset_pr560) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch560";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch560");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "arch560.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    /* A File node so the file_tree aspect has real content — makes the
     * "overview drops file_tree" assertion below non-vacuous. */
    cbm_node_t file_node = {.project = proj,
                            .label = "File",
                            .name = "main.go",
                            .qualified_name = "arch560.cmd.main.go",
                            .file_path = "cmd/main.go"};
    ASSERT_GT(cbm_store_upsert_node(st, &file_node), 0);

    /* Sanity: with "all", both entry_points and file_tree surface. */
    char *resp_all = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":560,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp_all);
    char *inner_all = extract_text_content(resp_all);
    ASSERT_NOT_NULL(inner_all);
    ASSERT_NOT_NULL(strstr(inner_all, "entry_points:"));
    ASSERT_NOT_NULL(strstr(inner_all, "file_tree:"));
    free(inner_all);
    free(resp_all);

    /* "overview": substantive content (entry_points, node_labels) but NO
     * file_tree section. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":561,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560\",\"aspects\":[\"overview\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(strstr(inner, "entry_points:"));
    ASSERT_NOT_NULL(strstr(inner, "node_labels:"));
    ASSERT_NULL(strstr(inner, "file_tree:"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Distills PR #560 (server-side validation): unknown aspect tokens must be
 * rejected with an isError result listing the valid values. Before the fix
 * the JSON-Schema accepted any string and both aspect gates simply never
 * matched, so a typo like "bogus_aspect" produced a silent near-empty payload
 * with isError:false. RED on unfixed code: no isError, no "Unknown aspect". */
TEST(tool_get_architecture_rejects_unknown_aspect_pr560) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch560v";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch560v");

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":562,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch560v\",\"aspects\":[\"bogus_aspect\"]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(resp, "Unknown aspect 'bogus_aspect'"));
    /* The error must teach the valid vocabulary, including the new token. */
    ASSERT_NOT_NULL(strstr(resp, "overview"));
    ASSERT_NOT_NULL(strstr(resp, "file_tree"));

    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first for #640: query handlers must accept the `project_name`
 * alias, not only the canonical `project` key. list_projects surfaces the field
 * as "name" and the error hint says "pass the project name", so a caller
 * naturally passes `project_name`. With no alias, the handler reads key
 * "project" -> NULL -> resolve_store bails before opening any .db -> "project
 * not found or not indexed" even though the project is indexed. Mirrors
 * tool_get_architecture_emits_populated_sections but with the alias key. */
TEST(tool_get_architecture_accepts_project_name_alias_issue640) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "alias640";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/alias640");

    cbm_node_t main_fn = {0};
    main_fn.project = proj;
    main_fn.label = "Function";
    main_fn.name = "main";
    main_fn.qualified_name = "alias640.cmd.main";
    main_fn.file_path = "cmd/main.go";
    main_fn.start_line = 1;
    main_fn.end_line = 3;
    main_fn.properties_json = "{\"is_entry_point\":true}";
    ASSERT_GT(cbm_store_upsert_node(st, &main_fn), 0);

    /* Caller passes `project_name` (the natural guess) instead of `project`. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":640,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project_name\":\"alias640\",\"aspects\":[\"all\"]}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* RED before the alias: inner is the "project not found" error.
     * GREEN after: the alias resolves and architecture sections surface. */
    ASSERT_NULL(strstr(inner, "project not found"));
    ASSERT_NOT_NULL(strstr(inner, "entry_points:"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first for #640: the alias must apply across query handlers, not
 * just get_architecture. search_graph with `project_name` must resolve too. */
TEST(tool_search_graph_accepts_project_name_alias_issue640) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "alias640b";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/alias640b");

    cbm_node_t fn = {0};
    fn.project = proj;
    fn.label = "Function";
    fn.name = "WidgetHandler";
    fn.qualified_name = "alias640b.svc.WidgetHandler";
    fn.file_path = "svc/widget.go";
    fn.start_line = 1;
    fn.end_line = 2;
    ASSERT_GT(cbm_store_upsert_node(st, &fn), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":641,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\","
             "\"arguments\":{\"project_name\":\"alias640b\",\"name_pattern\":\"Widget.*\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    ASSERT_NULL(strstr(inner, "project not found"));
    ASSERT_NOT_NULL(strstr(inner, "WidgetHandler"));

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* #1025: agents pass the repo FOLDER name ("codebase-memory-mcp"), but
 * indexed project names derive from the full path
 * (E:\project\graph\x -> "E-project-graph-x"), so exact lookup fails with
 * "project not found" while list_projects clearly shows the project. A
 * passed name that matches exactly ONE indexed project as a segment-aligned
 * tail ("-<name>" suffix) must resolve to it; zero or several matches keep
 * the existing error. Runs against real cache-dir .db files (the resolution
 * scans filenames), so this test indexes real fixtures under an overridden
 * CBM_CACHE_DIR. */
static void i1025_write_repo(const char *dir, const char *fn_name) {
    char path[CBM_SZ_512];
    snprintf(path, sizeof(path), "%s/mod.py", dir);
    FILE *f = fopen(path, "w");
    if (!f)
        return;
    fprintf(f, "def %s(x):\n    return x + 1\n", fn_name);
    fclose(f);
}

TEST(tool_project_arg_resolves_unique_tail_issue1025) {
    char repo_a[CBM_SZ_256];
    char repo_b[CBM_SZ_256];
    char repo_c[CBM_SZ_256];
    char cache[CBM_SZ_256];
    snprintf(repo_a, sizeof(repo_a), "/tmp/cbm-i1025a-XXXXXX");
    snprintf(repo_b, sizeof(repo_b), "/tmp/cbm-i1025b-XXXXXX");
    snprintf(repo_c, sizeof(repo_c), "/tmp/cbm-i1025c-XXXXXX");
    snprintf(cache, sizeof(cache), "/tmp/cbm-i1025d-XXXXXX");
    if (!cbm_mkdtemp(repo_a) || !cbm_mkdtemp(repo_b) || !cbm_mkdtemp(repo_c) ||
        !cbm_mkdtemp(cache)) {
        FAIL("mkdtemp failed");
    }
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? cbm_strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);

    i1025_write_repo(repo_a, "unique_tail_target");
    i1025_write_repo(repo_b, "amb_one");
    i1025_write_repo(repo_c, "amb_two");

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[CBM_SZ_1K];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"name\":\"E-project-graph-suffix1025\"}",
             repo_a);
    char *r = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(r);
    free(r);
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"name\":\"F-alpha-amb1025\"}", repo_b);
    r = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(r);
    free(r);
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"name\":\"G-beta-amb1025\"}", repo_c);
    r = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(r);
    free(r);

    /* 1. Unique tail resolves (RED today: "project not found"). */
    r = cbm_mcp_handle_tool(srv, "search_graph",
                            "{\"project\":\"suffix1025\",\"name_pattern\":\".*target.*\"}");
    ASSERT_NOT_NULL(r);
    if (strstr(r, "project not found")) {
        fprintf(stderr, "  [1025] FAIL unique tail did not resolve: %.200s\n", r);
    }
    ASSERT_NULL(strstr(r, "project not found"));
    ASSERT_NOT_NULL(strstr(r, "unique_tail_target"));
    free(r);

    /* 2. Ambiguous tail stays an error (never guess between projects). */
    r = cbm_mcp_handle_tool(srv, "search_graph",
                            "{\"project\":\"amb1025\",\"name_pattern\":\".*\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(strstr(r, "project not found"));
    free(r);

    /* 3. Exact full name keeps working unchanged. */
    r = cbm_mcp_handle_tool(srv, "search_graph",
                            "{\"project\":\"E-project-graph-suffix1025\","
                            "\"name_pattern\":\".*target.*\"}");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(strstr(r, "project not found"));
    free(r);

    cbm_mcp_server_free(srv);
    if (saved_cache_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
        free(saved_cache_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_rmtree(repo_a);
    th_rmtree(repo_b);
    th_rmtree(repo_c);
    th_rmtree(cache);
    PASS();
}

/* Regression for #604: path scopes architecture totals and content. */
TEST(tool_get_architecture_path_scoping) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);

    const char *proj = "arch-path";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, "/tmp/arch-path");

    cbm_node_t pkg_global = {.project = proj,
                             .label = "Package",
                             .name = "Django",
                             .qualified_name = "arch-path.Django",
                             .file_path = "vendor/django/__init__.py"};
    cbm_store_upsert_node(st, &pkg_global);

    cbm_node_t pkg_local = {.project = proj,
                            .label = "Package",
                            .name = "hoa",
                            .qualified_name = "arch-path.hoa",
                            .file_path = "apps/hoa/main.go"};
    cbm_store_upsert_node(st, &pkg_local);

    cbm_node_t f_hoa = {.project = proj,
                        .label = "File",
                        .name = "main.go",
                        .qualified_name = "arch-path.apps.hoa.main.go",
                        .file_path = "apps/hoa/main.go"};
    cbm_store_upsert_node(st, &f_hoa);

    cbm_node_t f_other = {.project = proj,
                          .label = "File",
                          .name = "other.go",
                          .qualified_name = "arch-path.other.go",
                          .file_path = "lib/other.go"};
    cbm_store_upsert_node(st, &f_other);

    char *resp_root = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":92,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"get_architecture\","
             "\"arguments\":{\"project\":\"arch-path\",\"aspects\":[\"packages\"]}}}");
    ASSERT_NOT_NULL(resp_root);
    char *inner_root = extract_text_content(resp_root);
    ASSERT_NOT_NULL(inner_root);
    ASSERT_NOT_NULL(strstr(inner_root, "Django"));

    char *resp_scoped =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":93,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_architecture\","
                                   "\"arguments\":{\"project\":\"arch-path\",\"path\":\"apps/hoa\","
                                   "\"aspects\":[\"packages\"]}}}");
    ASSERT_NOT_NULL(resp_scoped);
    char *inner_scoped = extract_text_content(resp_scoped);
    ASSERT_NOT_NULL(inner_scoped);

    ASSERT_NOT_NULL(strstr(inner_scoped, "root_total_nodes"));
    ASSERT_NOT_NULL(strstr(inner_scoped, "scoped_total_nodes"));
    ASSERT_NOT_NULL(strstr(inner_scoped, "path: "));
    ASSERT_NOT_NULL(strstr(inner_scoped, "hoa"));
    ASSERT_NULL(strstr(inner_scoped, "Django"));

    int root_nodes = 0;
    int scoped_nodes = 0;
    /* TOON scalar form (`key: N`) with JSON fallback for format:"json". */
    const char *rt = strstr(inner_scoped, "root_total_nodes: ");
    const char *stn = strstr(inner_scoped, "scoped_total_nodes: ");
    if (rt) {
        sscanf(rt, "root_total_nodes: %d", &root_nodes);
    } else if ((rt = strstr(inner_scoped, "\"root_total_nodes\":")) != NULL) {
        sscanf(rt, "\"root_total_nodes\":%d", &root_nodes);
    }
    if (stn) {
        sscanf(stn, "scoped_total_nodes: %d", &scoped_nodes);
    } else if ((stn = strstr(inner_scoped, "\"scoped_total_nodes\":")) != NULL) {
        sscanf(stn, "\"scoped_total_nodes\":%d", &scoped_nodes);
    }
    ASSERT_TRUE(root_nodes > scoped_nodes);
    ASSERT_TRUE(scoped_nodes > 0);

    free(inner_scoped);
    free(resp_scoped);
    free(inner_root);
    free(resp_root);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_query_graph_missing_query) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":23,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"query_graph\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about missing query */
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PIPELINE-DEPENDENT TOOL HANDLERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(tool_index_repository_missing_path) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":30,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"index_repository\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_code_snippet_missing_qn) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":31,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_code_snippet\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_get_code_snippet_not_found) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":32,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"get_code_snippet\","
                                   "\"arguments\":{\"qualified_name\":\"nonexistent.func\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_code_missing_pattern) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":33,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_search_code_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":34,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"func main\","
                                   "\"project\":\"nonexistent\"}}}");
    ASSERT_NOT_NULL(resp);
    /* No project indexed → error */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed") ||
                strstr(resp, "required"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(search_code_multi_word) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Multi-word query "HandleRequest error" — should find the line
     * "func HandleRequest() error {" via regex conversion. */
    char req[512];
    snprintf(req, sizeof(req),
             "{\"jsonrpc\":\"2.0\",\"id\":90,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest error\","
             "\"project\":\"test-project\"}}}");

    char *resp = cbm_mcp_server_handle(srv, req);
    ASSERT_NOT_NULL(resp);
    /* Should find at least one result (not zero) */
    ASSERT_TRUE(strstr(resp, "HandleRequest") != NULL);
    /* Should NOT contain an error about "not found" */
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* Reproduce-first (#687): scoped content search over a repo whose ROOT PATH
 * contains a space. write_scoped_filelist emits "<root>/<file>" records that the
 * Unix pipeline pipes to grep via xargs. With plain `xargs` (newline-split) the
 * space splits one path into several bogus args -> grep finds nothing ->
 * total_grep_matches == 0 (RED on the unfixed code). The fix writes NUL-separated
 * records + uses `xargs -0`, so the path stays a single argument -> match found
 * (GREEN). On Windows the scoped path uses PowerShell Get-Content -LiteralPath,
 * which already handles spaces, so this asserts correct behavior there too. */
TEST(search_code_scoped_path_with_spaces_issue687) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_srch_space_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("cbm_mkdtemp failed");
    }

    /* Project root deliberately contains a space. */
    char proj_dir[640];
    snprintf(proj_dir, sizeof(proj_dir), "%s/my project", tmp);
    cbm_mkdir(proj_dir);

    char src_path[768];
    snprintf(src_path, sizeof(src_path), "%s/main.go", proj_dir);
    FILE *fp = fopen(src_path, "w");
    if (!fp) {
        rmdir(proj_dir);
        rmdir(tmp);
        FAIL("cannot write source file under spaced path");
    }
    fprintf(fp, "package main\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "space-search";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, proj_dir);

    /* A node so the file is "indexed" (cbm_store_list_files -> scoped grep path)
     * and the grep hit classifies to a result. */
    cbm_node_t n = {.project = proj,
                    .label = "Function",
                    .name = "HandleRequest",
                    .qualified_name = "space-search.main.HandleRequest",
                    .file_path = "main.go",
                    .start_line = 3,
                    .end_line = 5};
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":94,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"space-search\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* grep must have found the match despite the space in the root path. */
    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    } else if ((g = strstr(inner, "total_grep_matches: ")) != NULL) {
        /* TOON scalar form — the search_code compact default. */
        sscanf(g, "total_grep_matches: %d", &grep_matches);
    }
    ASSERT_TRUE(grep_matches > 0);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    unlink(src_path);
    rmdir(proj_dir);
    rmdir(tmp);
    PASS();
}

#ifdef _WIN32
/* Issue #903 follow-up: scoped search_code on Windows writes a UTF-8 filelist
 * containing absolute source paths, then reads it back through PowerShell.
 * Windows PowerShell 5.1 treats UTF-8 without BOM as ANSI unless told
 * otherwise, so a non-ASCII project root can be mojibaked before
 * Select-String sees the LiteralPath. */
TEST(search_code_scoped_path_with_cjk_root_issue903) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s/cbm_srch_cjk_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(tmp)) {
        FAIL("cbm_mkdtemp failed");
    }

    char proj_dir[640];
    snprintf(proj_dir, sizeof(proj_dir), "%s/%s", tmp,
             "\xE4\xB8\xAD\xE6\x96\x87\xE9\xA1\xB9\xE7\x9B\xAE");
    if (!cbm_mkdir_p(proj_dir, 0755)) {
        cbm_rmdir(tmp);
        FAIL("cannot create CJK project dir");
    }

    char src_path[768];
    snprintf(src_path, sizeof(src_path), "%s/main.go", proj_dir);
    FILE *fp = cbm_fopen(src_path, "wb");
    if (!fp) {
        cbm_rmdir(proj_dir);
        cbm_rmdir(tmp);
        FAIL("cannot write source file under CJK path");
    }
    fprintf(fp, "package main\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    const char *proj = "cjk-search";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, proj_dir);

    cbm_node_t n = {.project = proj,
                    .label = "Function",
                    .name = "HandleRequest",
                    .qualified_name = "cjk-search.main.HandleRequest",
                    .file_path = "main.go",
                    .start_line = 3,
                    .end_line = 5};
    ASSERT_GT(cbm_store_upsert_node(st, &n), 0);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":903,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"cjk-search\"}}}");
    ASSERT_NOT_NULL(resp);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    } else if ((g = strstr(inner, "total_grep_matches: ")) != NULL) {
        /* TOON scalar form — the search_code compact default. */
        sscanf(g, "total_grep_matches: %d", &grep_matches);
    }
    ASSERT_TRUE(grep_matches > 0);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cbm_unlink(src_path);
    cbm_rmdir(proj_dir);
    cbm_rmdir(tmp);
    PASS();
}
#endif

/* Shared fixture for the path_filter prefilter tests (PR #756 distilled):
 * a project with two indexed files that both contain the search pattern —
 * src/handler.go (inside the filter) and vendor/other.go (outside it). */
static cbm_mcp_server_t *setup_prefilter_server(char *tmp, size_t tmp_sz, char *src_path,
                                                size_t src_sz, char *vendor_path,
                                                size_t vendor_sz) {
    snprintf(tmp, tmp_sz, "/tmp/cbm_srch_pref_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        return NULL;
    }
    char dir[640];
    snprintf(dir, sizeof(dir), "%s/src", tmp);
    cbm_mkdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor", tmp);
    cbm_mkdir(dir);

    snprintf(src_path, src_sz, "%s/src/handler.go", tmp);
    snprintf(vendor_path, vendor_sz, "%s/vendor/other.go", tmp);
    FILE *fp = fopen(src_path, "w");
    if (!fp) {
        return NULL;
    }
    fprintf(fp, "package main\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);
    fp = fopen(vendor_path, "w");
    if (!fp) {
        return NULL;
    }
    fprintf(fp, "package vendored\n\nfunc HandleRequest() error {\n\treturn nil\n}\n");
    fclose(fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }
    cbm_store_t *st = cbm_mcp_server_store(srv);
    const char *proj = "prefilter-search";
    cbm_mcp_server_set_project(srv, proj);
    cbm_store_upsert_project(st, proj, tmp);

    cbm_node_t n1 = {.project = proj,
                     .label = "Function",
                     .name = "HandleRequest",
                     .qualified_name = "prefilter-search.main.HandleRequest",
                     .file_path = "src/handler.go",
                     .start_line = 3,
                     .end_line = 5};
    cbm_node_t n2 = {.project = proj,
                     .label = "Function",
                     .name = "HandleRequest",
                     .qualified_name = "prefilter-search.vendored.HandleRequest",
                     .file_path = "vendor/other.go",
                     .start_line = 3,
                     .end_line = 5};
    if (cbm_store_upsert_node(st, &n1) <= 0 || cbm_store_upsert_node(st, &n2) <= 0) {
        cbm_mcp_server_free(srv);
        return NULL;
    }
    return srv;
}

static void cleanup_prefilter_dir(const char *tmp, const char *src_path, const char *vendor_path) {
    char dir[640];
    unlink(src_path);
    unlink(vendor_path);
    snprintf(dir, sizeof(dir), "%s/src", tmp);
    rmdir(dir);
    snprintf(dir, sizeof(dir), "%s/vendor", tmp);
    rmdir(dir);
    rmdir(tmp);
}

/* PR #756 (distilled): scoped search_code prefilters the indexed filelist by
 * path_filter before grep runs. POSITIVE invariant guard: a path_filter that
 * matches the file containing the hit must still return that hit (guards
 * against over-filtering — the prefilter predicate must stay IDENTICAL to the
 * post-grep filter in collect_grep_matches), and files outside the filter
 * stay excluded. Green on pre-prefilter main too (the post-grep filter alone
 * produced the same results): the change is results-preserving perf-only. */
TEST(search_code_path_filter_prefilter_keeps_matches) {
    char tmp[512], src_path[768], vendor_path[768];
    cbm_mcp_server_t *srv = setup_prefilter_server(tmp, sizeof(tmp), src_path, sizeof(src_path),
                                                   vendor_path, sizeof(vendor_path));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":95,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"prefilter-search\","
             "\"path_filter\":\"^src/\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    /* The in-filter hit is returned; the out-of-filter file is not. */
    ASSERT_NOT_NULL(strstr(inner, "src/handler.go"));
    ASSERT_TRUE(strstr(inner, "vendor/other.go") == NULL);

    /* Exactly the one in-filter grep match survives (same count before and
     * after the prefilter — predicate identity). */
    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    } else if ((g = strstr(inner, "total_grep_matches: ")) != NULL) {
        /* TOON scalar form — the search_code compact default. */
        sscanf(g, "total_grep_matches: %d", &grep_matches);
    }
    ASSERT_EQ(grep_matches, 1);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_prefilter_dir(tmp, src_path, vendor_path);
    PASS();
}

/* PR #756 (distilled): path_filter matching ZERO indexed files. With the
 * prefilter the scoped filelist has 0 records, and handle_search_code now
 * skips the grep subprocess entirely (xargs on an empty filelist is
 * platform-dependent: GNU execs grep once with no operands, BSD skips) and
 * returns the empty result directly. Must be a clean zero-result response —
 * no error. Green on pre-prefilter main too (there the full filelist is
 * grepped and the post-grep filter drops every hit — an empty filelist is
 * unreachable on main): guards the edge the prefilter introduces. */
TEST(search_code_path_filter_matches_nothing) {
    char tmp[512], src_path[768], vendor_path[768];
    cbm_mcp_server_t *srv = setup_prefilter_server(tmp, sizeof(tmp), src_path, sizeof(src_path),
                                                   vendor_path, sizeof(vendor_path));
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":96,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_code\","
             "\"arguments\":{\"pattern\":\"HandleRequest\",\"project\":\"prefilter-search\","
             "\"path_filter\":\"^no_such_dir/\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "\"isError\":true") == NULL);
    char *inner = extract_text_content(resp);
    ASSERT_NOT_NULL(inner);

    int grep_matches = -1;
    const char *g = strstr(inner, "\"total_grep_matches\":");
    if (g) {
        sscanf(g, "\"total_grep_matches\":%d", &grep_matches);
    } else if ((g = strstr(inner, "total_grep_matches: ")) != NULL) {
        /* TOON scalar form — the search_code compact default. */
        sscanf(g, "total_grep_matches: %d", &grep_matches);
    }
    ASSERT_EQ(grep_matches, 0);
    int results = -1;
    const char *r = strstr(inner, "\"total_results\":");
    if (r) {
        sscanf(r, "\"total_results\":%d", &results);
    } else if ((r = strstr(inner, "total_results: ")) != NULL) {
        sscanf(r, "total_results: %d", &results);
    }
    ASSERT_EQ(results, 0);
    ASSERT_TRUE(strstr(inner, "handler.go") == NULL);
    ASSERT_TRUE(strstr(inner, "other.go") == NULL);

    free(inner);
    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_prefilter_dir(tmp, src_path, vendor_path);
    PASS();
}

/* issue #283: search_code with regex=true and a syntactically invalid pattern
 * must return an explicit error, not an empty result indistinguishable from a
 * legitimate no-match. */
TEST(search_code_invalid_regex_errors_issue283) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* Unclosed group under regex=true → must be flagged as an error. */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":91,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"func(\",\"regex\":true,"
                                   "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"isError\":true"));
    ASSERT_NOT_NULL(strstr(resp, "invalid regex"));
    free(resp);

    /* Same pattern as a literal (regex=false) must NOT error. */
    resp = cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":92,\"method\":\"tools/call\","
                                      "\"params\":{\"name\":\"search_code\","
                                      "\"arguments\":{\"pattern\":\"func(\",\"regex\":false,"
                                      "\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "invalid regex") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* issue #282: a literal '|' under regex=false is a silent 0-match trap. It must
 * now be surfaced as a warning (and the result carries elapsed_ms). */
TEST(search_code_literal_pipe_warns_issue282) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":93,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"HandleRequest|Nope\","
                                   "\"regex\":false,\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "warning"));    /* surfaced, not silent */
    ASSERT_NOT_NULL(strstr(resp, "regex=true")); /* the hint names the fix */
    ASSERT_NOT_NULL(strstr(resp, "elapsed_ms")); /* timing is reported */
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

/* issue #272: '&' in a path / file_pattern is neutralised by the command's
 * quoting and must no longer be rejected as "invalid characters". */
TEST(search_code_ampersand_accepted_issue272) {
    char tmp[512];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":94,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"search_code\","
                                   "\"arguments\":{\"pattern\":\"HandleRequest\","
                                   "\"file_pattern\":\"*R&D*.go\",\"project\":\"test-project\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "invalid characters") == NULL);
    free(resp);

    cleanup_snippet_dir(tmp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_detect_changes_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":35,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"detect_changes\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "missing required argument: project"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_manage_adr_no_project) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":36,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"manage_adr\","
                                   "\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "missing required argument: project"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* Regression test for use-after-free in handle_manage_adr (get path).
 * MUST FAIL before fix: free(buf) is called before yy_doc_to_str serializes doc,
 * so result field is missing or contains garbage. MUST PASS after fix. */
TEST(tool_manage_adr_get_with_existing_adr) {
    /* Create a temp directory with .codebase-memory/adr.md */
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS(); /* skip if mkdtemp fails */
    }

    char adr_dir[512];
    snprintf(adr_dir, sizeof(adr_dir), "%s/.codebase-memory", tmp_dir);
    cbm_mkdir(adr_dir);

    char adr_path[512];
    snprintf(adr_path, sizeof(adr_path), "%s/adr.md", adr_dir);
    FILE *fp = fopen(adr_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("## PURPOSE\nTest ADR content for regression test.\n\n"
          "## STACK\nC, SQLite.\n\n"
          "## ARCHITECTURE\nMCP server.\n",
          fp);
    fclose(fp);

    /* Create server and register the project */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    cbm_store_upsert_project(st, "test-adr-uaf", tmp_dir);
    cbm_mcp_server_set_project(srv, "test-adr-uaf");

    /* Call manage_adr via full JSON-RPC path to exercise cbm_jsonrpc_format_response.
     * The bug: free(buf) before yy_doc_to_str causes garbage JSON; format_response
     * then fails to parse the result and omits the "result" field entirely. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\","
             "\"arguments\":{\"project\":\"test-adr-uaf\",\"mode\":\"get\"}}}");
    ASSERT_NOT_NULL(resp);
    /* JSON-RPC response must include a "result" field (absent when use-after-free) */
    ASSERT_NOT_NULL(strstr(resp, "\"result\""));
    /* ADR content must appear in response */
    ASSERT_NOT_NULL(strstr(resp, "PURPOSE"));
    /* Must not be an error */
    ASSERT_NULL(strstr(resp, "\"isError\":true"));
    free(resp);

    /* Clean up */
    cbm_mcp_server_free(srv);
    remove(adr_path);
    rmdir(adr_dir);
    rmdir(tmp_dir);
    PASS();
}

/* issue #256: manage_adr (MCP) and the UI /api/adr endpoints must share ONE
 * backend. A manage_adr(update) write must be readable via cbm_store_adr_get
 * (the exact API the UI's /api/adr GET uses). */
TEST(tool_manage_adr_unified_backend_issue256) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *st = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(st);
    cbm_store_upsert_project(st, "adr-unify", "/tmp/adr-unify");
    cbm_mcp_server_set_project(srv, "adr-unify");

    /* Write via the MCP tool. */
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":120,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\",\"arguments\":{\"project\":\"adr-unify\","
             "\"mode\":\"update\",\"content\":\"## PURPOSE\\nUnified ADR backend.\\n\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    /* Read DIRECTLY via the store API the UI /api/adr uses — must see it. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    ASSERT_EQ(cbm_store_adr_get(st, "adr-unify", &adr), CBM_STORE_OK);
    ASSERT_NOT_NULL(adr.content);
    ASSERT_NOT_NULL(strstr(adr.content, "Unified ADR backend."));
    cbm_store_adr_free(&adr);

    /* And manage_adr(get) round-trips the same content. */
    resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":121,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"manage_adr\",\"arguments\":{\"project\":\"adr-unify\","
             "\"mode\":\"get\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Unified ADR backend."));
    ASSERT_NULL(strstr(resp, "\"isError\":true"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_manage_adr_mutation_guard_balances_success) {
    const char *project = "guard-adr-success";
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, project, "/tmp/guard-adr-success"), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, project);

    mcp_mutation_guard_probe_t probe = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char *resp = cbm_mcp_handle_tool(srv, "manage_adr",
                                     "{\"project\":\"guard-adr-success\",\"mode\":\"update\","
                                     "\"content\":\"## PURPOSE\\nGuarded ADR.\\n\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    ASSERT_EQ(probe.begin_count, 1);
    ASSERT_EQ(probe.end_count, 1);
    ASSERT_STR_EQ(probe.begin_projects[0], project);
    ASSERT_STR_EQ(probe.end_projects[0], project);
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_manage_adr_mutation_guard_releases_on_missing_store) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-mcp-adr-guard-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-adr-missing";
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t probe = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char *resp = cbm_mcp_handle_tool(srv, "manage_adr",
                                     "{\"project\":\"guard-adr-missing\",\"mode\":\"get\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "not indexed"));
    ASSERT_EQ(probe.begin_count, 1);
    ASSERT_EQ(probe.end_count, 1);
    ASSERT_STR_EQ(probe.begin_projects[0], project);
    ASSERT_STR_EQ(probe.end_projects[0], project);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    cbm_rmdir(cache);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    PASS();
}

/* A raw cbm_mcp_handle_tool() call is still one request lifetime. Cancellation
 * published from inside a non-pipeline handler must therefore be accepted,
 * observed before the write, and retired at completion so the next raw request
 * on the same server is not poisoned. */
TEST(tool_raw_dispatch_cancel_is_scoped_non_mutating_and_next_request_clean) {
    const char *project = "raw-cancel-adr";
    char root[256];
    snprintf(root, sizeof(root), "%s/cbm-mcp-raw-adr-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(root));
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, project, root), CBM_STORE_OK);
    cbm_mcp_server_set_project(srv, project);

    mcp_mutation_guard_probe_t probe = {
        .cancel_on_begin_call = 1,
        .cancel_server = srv,
    };
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char *cancelled_response =
        cbm_mcp_handle_tool(srv, "manage_adr",
                            "{\"project\":\"raw-cancel-adr\",\"mode\":\"update\","
                            "\"content\":\"## PURPOSE\\nMUST NOT COMMIT.\\n\"}");
    bool cancellation_reported = cancelled_response && strstr(cancelled_response, "cancelled") &&
                                 strstr(cancelled_response, "\"isError\":true");

    cbm_adr_t cancelled_adr = {0};
    int cancelled_lookup = cbm_store_adr_get(store, project, &cancelled_adr);
    if (cancelled_lookup == CBM_STORE_OK) {
        cbm_store_adr_free(&cancelled_adr);
    }

    char *next_response =
        cbm_mcp_handle_tool(srv, "manage_adr",
                            "{\"project\":\"raw-cancel-adr\",\"mode\":\"update\","
                            "\"content\":\"## PURPOSE\\nClean next request.\\n\"}");
    bool next_response_clean = next_response && strstr(next_response, "updated") &&
                               !strstr(next_response, "cancelled") &&
                               !strstr(next_response, "\"isError\":true");
    cbm_adr_t next_adr = {0};
    int next_lookup = cbm_store_adr_get(store, project, &next_adr);
    bool next_write_committed = next_lookup == CBM_STORE_OK && next_adr.content &&
                                strstr(next_adr.content, "Clean next request") &&
                                !strstr(next_adr.content, "MUST NOT COMMIT");
    if (next_lookup == CBM_STORE_OK) {
        cbm_store_adr_free(&next_adr);
    }

    free(cancelled_response);
    free(next_response);
    cbm_mcp_server_free(srv);
    (void)cbm_rmdir(root);

    ASSERT_TRUE(probe.cancel_attempted);
    ASSERT_TRUE(probe.cancel_accepted);
    ASSERT_TRUE(cancellation_reported);
    ASSERT_EQ(cancelled_lookup, CBM_STORE_NOT_FOUND);
    ASSERT_TRUE(next_response_clean);
    ASSERT_TRUE(next_write_committed);
    ASSERT_EQ(probe.begin_count, 2);
    ASSERT_EQ(probe.end_count, 2);
    ASSERT_STR_EQ(probe.begin_projects[0], project);
    ASSERT_STR_EQ(probe.end_projects[0], project);
    ASSERT_STR_EQ(probe.begin_projects[1], project);
    ASSERT_STR_EQ(probe.end_projects[1], project);
    PASS();
}

/* The daemon publishes its transport request before entering MCP dispatch. A
 * disconnect in that narrow interval must remain latched through the nested
 * raw tool scope instead of being erased at dispatch entry. */
TEST(tool_outer_request_scope_preserves_predispatch_cancel) {
    const char *project = "outer-scope-cancel-adr";
    char root[256];
    (void)snprintf(root, sizeof(root), "%s/cbm-mcp-outer-cancel-XXXXXX", cbm_tmpdir());
    bool root_created = cbm_mkdtemp(root) != NULL;
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    bool project_ready =
        root_created && store && cbm_store_upsert_project(store, project, root) == CBM_STORE_OK;
    cbm_mcp_server_set_project(srv, project);
    bool outer_scope = project_ready && cbm_mcp_server_request_scope_begin(srv);
    bool cancel_accepted = outer_scope && cbm_mcp_server_cancel_active(srv);
    char *cancelled_response =
        cancel_accepted
            ? cbm_mcp_handle_tool(srv, "manage_adr",
                                  "{\"project\":\"outer-scope-cancel-adr\","
                                  "\"mode\":\"update\",\"content\":\"MUST NOT COMMIT\"}")
            : NULL;
    bool cancellation_reported = cancelled_response && strstr(cancelled_response, "cancelled") &&
                                 strstr(cancelled_response, "\"isError\":true");
    cbm_mcp_server_request_scope_end(srv);

    char *next_response = srv ? cbm_mcp_handle_tool(srv, "ingest_traces", "{\"traces\":[]}") : NULL;
    bool next_response_clean = next_response && strstr(next_response, "accepted") &&
                               !strstr(next_response, "cancelled") &&
                               !strstr(next_response, "\"isError\":true");

    free(cancelled_response);
    free(next_response);
    cbm_mcp_server_free(srv);
    (void)cbm_rmdir(root);

    ASSERT_TRUE(root_created);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(project_ready);
    ASSERT_TRUE(outer_scope);
    ASSERT_TRUE(cancel_accepted);
    ASSERT_TRUE(cancellation_reported);
    ASSERT_TRUE(next_response_clean);
    PASS();
}

/* Publish cancellation from the local index mutation guard: the request scope
 * must already be active, and the cancellation must either stop before
 * pipeline admission or remain set through pipeline binding. No project DB may
 * be published, and the following request must start with a clean token. */
TEST(tool_index_repository_early_raw_cancel_survives_index_entry) {
    char cache[256];
    char repo[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-raw-index-cache-XXXXXX", cbm_tmpdir());
    snprintf(repo, sizeof(repo), "%s/cbm-mcp-raw-index-repo-XXXXXX", cbm_tmpdir());
    bool cache_created = cbm_mkdtemp(cache) != NULL;
    bool repo_created = cbm_mkdtemp(repo) != NULL;

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    if (cache_created) {
        cbm_setenv("CBM_CACHE_DIR", cache, 1);
    }

    char *project = repo_created ? cbm_project_name_from_path(repo) : NULL;
    cbm_mcp_server_t *srv =
        cache_created && repo_created && project ? cbm_mcp_server_new(NULL) : NULL;
    mcp_mutation_guard_probe_t probe = {
        .cancel_on_begin_call = 1,
        .cancel_server = srv,
    };
    if (srv) {
        cbm_mcp_server_set_background_tasks(srv, false);
        cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                                  mcp_mutation_guard_probe_end, &probe);
    }

    char args[CBM_SZ_1K];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", repo);
    char *cancelled_response = srv ? cbm_mcp_handle_tool(srv, "index_repository", args) : NULL;
    bool cancellation_reported = cancelled_response && strstr(cancelled_response, "cancelled") &&
                                 strstr(cancelled_response, "\"isError\":true");

    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project ? project : "missing-project");
    bool no_project_published = !cbm_file_exists(db_path);

    char *next_response = srv ? cbm_mcp_handle_tool(srv, "ingest_traces", "{\"traces\":[]}") : NULL;
    bool next_response_clean = next_response && strstr(next_response, "accepted") &&
                               !strstr(next_response, "cancelled") &&
                               !strstr(next_response, "\"isError\":true");

    free(cancelled_response);
    free(next_response);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    if (cache_created) {
        (void)cbm_rmdir(cache);
    }
    if (repo_created) {
        (void)cbm_rmdir(repo);
    }
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    free(project);

    ASSERT_TRUE(cache_created);
    ASSERT_TRUE(repo_created);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(probe.cancel_attempted);
    ASSERT_TRUE(probe.cancel_accepted);
    ASSERT_TRUE(cancellation_reported);
    ASSERT_EQ(probe.begin_count, 1);
    ASSERT_EQ(probe.end_count, 1);
    ASSERT_TRUE(no_project_published);
    ASSERT_TRUE(next_response_clean);
    PASS();
}

static bool mcp_cross_repo_create_project_store(const char *cache, const char *project,
                                                const char *root_path) {
    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return false;
    }
    bool created = cbm_store_upsert_project(store, project, root_path) == CBM_STORE_OK;
    cbm_store_close(store);
    return created;
}

/* Seed exactly one HTTP route match without invoking the indexing pipeline.
 * This keeps the duplicate-target regression fast and makes a doubled result
 * count observable instead of relying on an empty (zero-edge) scan. */
static bool mcp_cross_repo_seed_http_match(const char *cache, const char *source_project,
                                           const char *target_project, const char *root_path) {
    char source_path[CBM_SZ_1K];
    char target_path[CBM_SZ_1K];
    snprintf(source_path, sizeof(source_path), "%s/%s.db", cache, source_project);
    snprintf(target_path, sizeof(target_path), "%s/%s.db", cache, target_project);

    cbm_store_t *source = cbm_store_open_path(source_path);
    cbm_store_t *target = cbm_store_open_path(target_path);
    if (!source || !target) {
        cbm_store_close(source);
        cbm_store_close(target);
        return false;
    }

    bool ok = cbm_store_upsert_project(source, source_project, root_path) == CBM_STORE_OK &&
              cbm_store_upsert_project(target, target_project, root_path) == CBM_STORE_OK;

    cbm_node_t caller = {.project = source_project,
                         .label = "Function",
                         .name = "call_once",
                         .qualified_name = "cross.source.call_once",
                         .file_path = "client.c",
                         .start_line = 1,
                         .end_line = 2};
    cbm_node_t local_route = {.project = source_project,
                              .label = "Route",
                              .name = "GET /dedupe",
                              .qualified_name = "__route__GET__/dedupe",
                              .file_path = "client.c",
                              .start_line = 3,
                              .end_line = 3};
    int64_t caller_id = ok ? cbm_store_upsert_node(source, &caller) : 0;
    int64_t local_route_id = ok ? cbm_store_upsert_node(source, &local_route) : 0;
    cbm_edge_t http_call = {.project = source_project,
                            .source_id = caller_id,
                            .target_id = local_route_id,
                            .type = "HTTP_CALLS",
                            .properties_json = "{\"url_path\":\"/dedupe\",\"method\":\"GET\"}"};
    ok = ok && caller_id > 0 && local_route_id > 0 && cbm_store_insert_edge(source, &http_call) > 0;

    cbm_node_t target_route = {.project = target_project,
                               .label = "Route",
                               .name = "GET /dedupe",
                               .qualified_name = "__route__GET__/dedupe",
                               .file_path = "server.c",
                               .start_line = 3,
                               .end_line = 3};
    cbm_node_t handler = {.project = target_project,
                          .label = "Function",
                          .name = "handle_once",
                          .qualified_name = "cross.target.handle_once",
                          .file_path = "server.c",
                          .start_line = 1,
                          .end_line = 2};
    int64_t target_route_id = ok ? cbm_store_upsert_node(target, &target_route) : 0;
    int64_t handler_id = ok ? cbm_store_upsert_node(target, &handler) : 0;
    cbm_edge_t handles = {.project = target_project,
                          .source_id = handler_id,
                          .target_id = target_route_id,
                          .type = "HANDLES"};
    ok = ok && target_route_id > 0 && handler_id > 0 && cbm_store_insert_edge(target, &handles) > 0;

    cbm_store_close(source);
    cbm_store_close(target);
    return ok;
}

TEST(tool_cross_repo_mutation_guard_sorts_dedupes_and_unwinds) {
    char repo[256];
    snprintf(repo, sizeof(repo), "/tmp/cbm-mcp-cross-guard-XXXXXX");
    if (!cbm_mkdtemp(repo)) {
        PASS();
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, repo, NULL));

    mcp_mutation_guard_probe_t probe = {.deny_begin_call = 3};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"zzz-target\",\"000-target\",\"zzz-target\"]}",
             repo);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "blocked"));

    /* The source plus two unique targets are acquired in lexical order. The
     * third acquisition is denied, so only the first two are unwound. */
    ASSERT_EQ(probe.begin_count, 3);
    ASSERT_TRUE(strcmp(probe.begin_projects[0], probe.begin_projects[1]) < 0);
    ASSERT_TRUE(strcmp(probe.begin_projects[1], probe.begin_projects[2]) < 0);
    int low_target_count = 0;
    int high_target_count = 0;
    for (int i = 0; i < probe.begin_count; i++) {
        low_target_count += strcmp(probe.begin_projects[i], "000-target") == 0;
        high_target_count += strcmp(probe.begin_projects[i], "zzz-target") == 0;
    }
    ASSERT_EQ(low_target_count, 1);
    ASSERT_EQ(high_target_count, 1);
    ASSERT_EQ(probe.end_count, 2);
    ASSERT_STR_EQ(probe.end_projects[0], probe.begin_projects[1]);
    ASSERT_STR_EQ(probe.end_projects[1], probe.begin_projects[0]);
    free(resp);

    cbm_mcp_server_free(srv);
    cbm_rmdir(repo);
    PASS();
}

static unsigned char mcp_test_ascii_casefold(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? (unsigned char)(ch + ('a' - 'A')) : ch;
}

static bool mcp_test_project_keys_equivalent(const char *left, const char *right) {
    if (!left || !right) {
        return left == right;
    }
    while (*left && *right) {
        if (mcp_test_ascii_casefold((unsigned char)*left) !=
            mcp_test_ascii_casefold((unsigned char)*right)) {
            return false;
        }
        left++;
        right++;
    }
    return *left == *right;
}

/* Project-lock keys ASCII-fold A-Z, so case aliases must be one lease here too.
 * Otherwise Foo + foo self-deadlocks, and two requests whose raw strcmp order
 * differs can acquire the same OS locks in opposite (ABBA) order. Keep the
 * original spellings: folding is only the comparison key, not a lookup value. */
TEST(tool_cross_repo_mutation_guard_casefolds_aliases_and_order) {
    char repo[256];
    snprintf(repo, sizeof(repo), "/tmp/cbm-mcp-cross-case-guard-XXXXXX");
    if (!cbm_mkdtemp(repo)) {
        PASS();
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, repo, NULL));

    mcp_mutation_guard_probe_t first = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &first);
    char first_args[CBM_SZ_2K];
    snprintf(first_args, sizeof(first_args),
             "{\"repo_path\":\"%s\",\"name\":\"Zulu\","
             "\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"Foo\",\"foo\",\"Alpha\"]}",
             repo);
    char *first_resp = cbm_mcp_handle_tool(srv, "index_repository", first_args);
    ASSERT_NOT_NULL(first_resp);
    free(first_resp);

    mcp_mutation_guard_probe_t second = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &second);
    char second_args[CBM_SZ_2K];
    snprintf(second_args, sizeof(second_args),
             "{\"repo_path\":\"%s\",\"name\":\"zULU\","
             "\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"foo\",\"ALPHA\",\"FOO\"]}",
             repo);
    char *second_resp = cbm_mcp_handle_tool(srv, "index_repository", second_args);
    ASSERT_NOT_NULL(second_resp);
    free(second_resp);

    ASSERT_EQ(first.begin_count, 3);
    ASSERT_EQ(first.end_count, 3);
    ASSERT_EQ(second.begin_count, 3);
    ASSERT_EQ(second.end_count, 3);
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(
            mcp_test_project_keys_equivalent(first.begin_projects[i], second.begin_projects[i]));
        ASSERT_TRUE(
            mcp_test_project_keys_equivalent(first.end_projects[i], first.begin_projects[2 - i]));
        ASSERT_TRUE(
            mcp_test_project_keys_equivalent(second.end_projects[i], second.begin_projects[2 - i]));
    }
    ASSERT_STR_EQ(first.begin_projects[0], "Alpha");
    ASSERT_STR_EQ(first.begin_projects[1], "Foo");
    ASSERT_STR_EQ(first.begin_projects[2], "Zulu");
    ASSERT_STR_EQ(second.begin_projects[0], "ALPHA");
    ASSERT_STR_EQ(second.begin_projects[1], "FOO");
    ASSERT_STR_EQ(second.begin_projects[2], "zULU");

    cbm_mcp_server_free(srv);
    cbm_rmdir(repo);
    PASS();
}

/* A wildcard means "all projects" and therefore cannot be combined with a
 * named target. Accepting the mixed form both obscures caller intent and lets
 * the cross-repo pass create/use a literal "*.db" target on POSIX. Validation
 * must happen before any project mutation lease is acquired. */
TEST(tool_cross_repo_rejects_wildcard_mixed_with_named_targets) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-cross-wildcard-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char *project = cbm_project_name_from_path(cache);
    ASSERT_NOT_NULL(project);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, cache, NULL));

    mcp_mutation_guard_probe_t probe = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"*\",\"named-target\"]}",
             cache);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool rejected = resp && strstr(resp, "\"isError\":true") != NULL;
    bool explained = resp && strstr(resp, "target_projects") && strstr(resp, "*") &&
                     (strstr(resp, "only") || strstr(resp, "combin"));
    int begin_count = probe.begin_count;
    int end_count = probe.end_count;

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    cleanup_project_db(cache, "*");
    cleanup_project_db(cache, "named-target");
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(rejected);
    ASSERT_TRUE(explained);
    ASSERT_EQ(begin_count, 0);
    ASSERT_EQ(end_count, 0);
    PASS();
}

/* Cancellation can arrive while the final mutation lease is being acquired.
 * The cross-repo operation must advertise itself through cancel_active(),
 * observe the pending cancellation before doing cross-project writes, and
 * unwind every lease it acquired. */
TEST(tool_cross_repo_checks_cancellation_after_acquiring_leases) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-cross-cancel-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char *project = cbm_project_name_from_path(cache);
    ASSERT_NOT_NULL(project);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, cache, NULL));

    mcp_mutation_guard_probe_t probe = {
        .cancel_on_begin_call = 3,
        .cancel_server = srv,
    };
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);

    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"000-cancel-target\",\"zzz-cancel-target\"]}",
             cache);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool response_cancelled = resp && strstr(resp, "cancelled") != NULL;
    bool cancel_attempted = probe.cancel_attempted;
    bool cancel_accepted = probe.cancel_accepted;
    int begin_count = probe.begin_count;
    int end_count = probe.end_count;
    bool reverse_unwind = begin_count == 3 && end_count == 3 &&
                          strcmp(probe.end_projects[0], probe.begin_projects[2]) == 0 &&
                          strcmp(probe.end_projects[1], probe.begin_projects[1]) == 0 &&
                          strcmp(probe.end_projects[2], probe.begin_projects[0]) == 0;

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    cleanup_project_db(cache, "000-cancel-target");
    cleanup_project_db(cache, "zzz-cancel-target");
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(cancel_attempted);
    ASSERT_TRUE(cancel_accepted);
    ASSERT_TRUE(response_cancelled);
    ASSERT_EQ(begin_count, 3);
    ASSERT_EQ(end_count, 3);
    ASSERT_TRUE(reverse_unwind);
    PASS();
}

/* cbm_store_open_path() creates its path. Cross-repo validation must therefore
 * reject an absent source or named target before the matcher opens either one;
 * otherwise a typo silently becomes a valid-looking empty project database. */
TEST(tool_cross_repo_missing_inputs_fail_without_creating_ghost_databases) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-cross-missing-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char *source_project = cbm_project_name_from_path(cache);
    ASSERT_NOT_NULL(source_project);
    const char *existing_target = "existing-cross-target";
    const char *missing_target = "missing-cross-target";
    ASSERT_TRUE(mcp_cross_repo_create_project_store(cache, existing_target, cache));

    char source_db_path[CBM_SZ_1K];
    char missing_target_db_path[CBM_SZ_1K];
    snprintf(source_db_path, sizeof(source_db_path), "%s/%s.db", cache, source_project);
    snprintf(missing_target_db_path, sizeof(missing_target_db_path), "%s/%s.db", cache,
             missing_target);
    ASSERT_FALSE(cbm_file_exists(source_db_path));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, cache, NULL));

    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"%s\"]}",
             cache, existing_target);
    char *source_resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool source_failed = source_resp && strstr(source_resp, "\"isError\":true");
    bool source_reported =
        source_resp && (strstr(source_resp, "not indexed") || strstr(source_resp, "not found") ||
                        strstr(source_resp, "missing"));
    bool source_ghost_created = cbm_file_exists(source_db_path);
    free(source_resp);

    cleanup_project_db(cache, source_project);
    ASSERT_TRUE(mcp_cross_repo_create_project_store(cache, source_project, cache));
    ASSERT_FALSE(cbm_file_exists(missing_target_db_path));

    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"%s\"]}",
             cache, missing_target);
    char *target_resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool target_failed = target_resp && strstr(target_resp, "\"isError\":true");
    bool target_reported =
        target_resp && (strstr(target_resp, "not indexed") || strstr(target_resp, "not found") ||
                        strstr(target_resp, "missing"));
    bool target_ghost_created = cbm_file_exists(missing_target_db_path);
    free(target_resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, source_project);
    cleanup_project_db(cache, existing_target);
    cleanup_project_db(cache, missing_target);
    free(source_project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(source_failed);
    ASSERT_TRUE(source_reported);
    ASSERT_FALSE(source_ghost_created);
    ASSERT_TRUE(target_failed);
    ASSERT_TRUE(target_reported);
    ASSERT_FALSE(target_ghost_created);
    PASS();
}

/* Named targets are a set, not a work list. A duplicate must be leased,
 * scanned, and counted once; the fixture provides one real edge so the result
 * counters cannot pass vacuously at zero. */
TEST(tool_cross_repo_dedupes_targets_before_scanning_and_counting) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-cross-dedupe-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char *source_project = cbm_project_name_from_path(cache);
    ASSERT_NOT_NULL(source_project);
    const char *target_project = "cross-dedupe-target";
    ASSERT_TRUE(mcp_cross_repo_seed_http_match(cache, source_project, target_project, cache));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, cache, NULL));

    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"%s\",\"%s\"]}",
             cache, target_project, target_project);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool succeeded = resp && strstr(resp, "\"isError\":true") == NULL;
    bool scanned_once = response_contains_json_fragment(resp, "\"projects_scanned\":1");
    bool counted_once = response_contains_json_fragment(resp, "\"cross_http_calls\":1") &&
                        response_contains_json_fragment(resp, "\"total_cross_edges\":1");

    char source_db_path[CBM_SZ_1K];
    char target_db_path[CBM_SZ_1K];
    snprintf(source_db_path, sizeof(source_db_path), "%s/%s.db", cache, source_project);
    snprintf(target_db_path, sizeof(target_db_path), "%s/%s.db", cache, target_project);
    cbm_store_t *source = cbm_store_open_path_query(source_db_path);
    cbm_store_t *target = cbm_store_open_path_query(target_db_path);
    int source_cross_edges =
        source ? cbm_store_count_edges_by_type(source, source_project, "CROSS_HTTP_CALLS") : -1;
    int target_cross_edges =
        target ? cbm_store_count_edges_by_type(target, target_project, "CROSS_HTTP_CALLS") : -1;
    cbm_store_close(source);
    cbm_store_close(target);

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, source_project);
    cleanup_project_db(cache, target_project);
    free(source_project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(succeeded);
    ASSERT_TRUE(scanned_once);
    ASSERT_TRUE(counted_once);
    ASSERT_EQ(source_cross_edges, 1);
    ASSERT_EQ(target_cross_edges, 1);
    PASS();
}

/* `name` is the documented index project-name override and must identify the
 * cross-repo source too. Deriving from repo_path here makes custom-named
 * projects impossible to rescan even though ordinary indexing created them. */
TEST(tool_cross_repo_honors_source_name_override) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-cross-name-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *source_project = "cross-custom-source";
    const char *target_project = "cross-custom-target";
    ASSERT_TRUE(mcp_cross_repo_seed_http_match(cache, source_project, target_project, cache));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    ASSERT_TRUE(cbm_mcp_server_set_session_context(srv, cache, NULL));
    char args[CBM_SZ_2K];
    snprintf(args, sizeof(args),
             "{\"repo_path\":\"%s\",\"name\":\"%s\","
             "\"mode\":\"cross-repo-intelligence\","
             "\"target_projects\":[\"%s\"]}",
             cache, source_project, target_project);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool succeeded = resp && !response_contains_json_fragment(resp, "\"isError\":true") &&
                     response_contains_json_fragment(resp, "\"cross_http_calls\":1");

    free(resp);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, source_project);
    cleanup_project_db(cache, target_project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(succeeded);
    PASS();
}

/* Corrupt-store quarantine renames/unlinks the project DB and sidecars, so it
 * is a mutation even when resolve_store() was reached by a query tool. The
 * query path needs one balanced lease; manage_adr already owns that project
 * lease and must not acquire a nested second lease during the same cleanup. */
TEST(tool_corrupt_store_cleanup_guard_is_balanced_and_not_nested) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-guard-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-project";
    char db_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);

    ASSERT_TRUE(mcp_make_corrupt_project_store(cache, project));
    cbm_mcp_server_t *query_srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(query_srv);
    mcp_mutation_guard_probe_t query_probe = {
        .observed_db_path = db_path,
    };
    cbm_mcp_server_set_project_mutation_guard(query_srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &query_probe);

    char *resp =
        cbm_mcp_handle_tool(query_srv, "search_graph",
                            "{\"project\":\"guard-corrupt-project\",\"name_pattern\":\".*\"}");
    free(resp);
    cbm_mcp_server_free(query_srv);
    char query_backup_path[CBM_SZ_1K];
    int query_backup_count =
        mcp_find_corrupt_backups(cache, project, query_backup_path, sizeof(query_backup_path));
    bool query_quarantined =
        !cbm_file_exists(db_path) && query_backup_count == 1 && query_backup_path[0] != '\0';

    /* Replant the same deterministic corruption to exercise manage_adr's
     * already-held lease independently from the query server above. */
    mcp_cleanup_corrupt_backups(cache, project);
    ASSERT_TRUE(mcp_make_corrupt_project_store(cache, project));
    cbm_mcp_server_t *adr_srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(adr_srv);
    mcp_mutation_guard_probe_t adr_probe = {
        .observed_db_path = db_path,
    };
    cbm_mcp_server_set_project_mutation_guard(adr_srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &adr_probe);
    resp = cbm_mcp_handle_tool(adr_srv, "manage_adr",
                               "{\"project\":\"guard-corrupt-project\",\"mode\":\"get\"}");
    free(resp);
    cbm_mcp_server_free(adr_srv);
    char adr_backup_path[CBM_SZ_1K];
    int adr_backup_count =
        mcp_find_corrupt_backups(cache, project, adr_backup_path, sizeof(adr_backup_path));
    bool adr_quarantined =
        !cbm_file_exists(db_path) && adr_backup_count == 1 && adr_backup_path[0] != '\0';

    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(query_quarantined);
    ASSERT_EQ(query_probe.begin_count, 1);
    ASSERT_EQ(query_probe.end_count, 1);
    ASSERT_STR_EQ(query_probe.begin_projects[0], project);
    ASSERT_STR_EQ(query_probe.end_projects[0], project);
    ASSERT_TRUE(query_probe.db_exists_at_begin);
    ASSERT_FALSE(query_probe.db_exists_at_end);
    ASSERT_TRUE(adr_quarantined);
    ASSERT_EQ(adr_probe.begin_count, 1);
    ASSERT_EQ(adr_probe.end_count, 1);
    ASSERT_STR_EQ(adr_probe.begin_projects[0], project);
    ASSERT_STR_EQ(adr_probe.end_projects[0], project);
    ASSERT_TRUE(adr_probe.db_exists_at_begin);
    ASSERT_FALSE(adr_probe.db_exists_at_end);
    PASS();
}

/* Integrity is checked before the lease is requested, but quarantine itself
 * must fail closed when that lease is denied. In particular, a rejected query
 * may not remove either a recoverable DB generation or its committed WAL. */
TEST(tool_corrupt_store_cleanup_guard_denial_preserves_db_and_wal) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-denied-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-denied";
    char db_path[CBM_SZ_1K];
    char wal_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    cbm_store_t *writer = mcp_open_corrupt_project_store_with_wal(cache, project);
    ASSERT_NOT_NULL(writer);
    ASSERT_TRUE(cbm_file_exists(db_path));
    ASSERT_TRUE(cbm_file_exists(wal_path));

    long db_len = 0;
    long wal_len = 0;
    unsigned char *db_before = mcp_read_file_bytes(db_path, &db_len);
    unsigned char *wal_before = mcp_read_file_bytes(wal_path, &wal_len);
    ASSERT_NOT_NULL(db_before);
    ASSERT_NOT_NULL(wal_before);
    ASSERT_TRUE(db_len > 0);
    ASSERT_TRUE(wal_len > 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t probe = {.deny_begin_call = 1};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);
    char *resp = cbm_mcp_handle_tool(
        srv, "search_graph", "{\"project\":\"guard-corrupt-denied\",\"name_pattern\":\".*\"}");

    bool db_unchanged = mcp_file_matches_snapshot(db_path, db_before, db_len);
    bool wal_unchanged = mcp_file_matches_snapshot(wal_path, wal_before, wal_len);
    char unexpected_backup[CBM_SZ_1K];
    int backup_count =
        mcp_find_corrupt_backups(cache, project, unexpected_backup, sizeof(unexpected_backup));
    int artifact_count = mcp_count_corrupt_artifacts(cache, project);
    int begin_count = probe.begin_count;
    int end_count = probe.end_count;
    bool guarded_project = begin_count == 1 && strcmp(probe.begin_projects[0], project) == 0;

    free(resp);
    cbm_mcp_server_free(srv);
    free(db_before);
    free(wal_before);
    cbm_store_close(writer);
    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_EQ(begin_count, 1);
    ASSERT_EQ(end_count, 0);
    ASSERT_TRUE(guarded_project);
    ASSERT_TRUE(db_unchanged);
    ASSERT_TRUE(wal_unchanged);
    ASSERT_EQ(backup_count, 0);
    ASSERT_EQ(artifact_count, 0);
    PASS();
}

/* Another session may publish a good generation while this query waits for
 * the mutation lease. Cleanup must re-open and re-check the path after lease
 * acquisition; quarantining based on the stale pre-wait handle loses the new
 * generation and returns a false "not indexed" result. */
TEST(tool_corrupt_store_cleanup_rechecks_generation_after_guard_wait) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-recheck-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-recheck";
    const char *replacement_root = "/tmp/guard-corrupt-replacement";
    char db_path[CBM_SZ_1K];
    char replacement_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    snprintf(replacement_path, sizeof(replacement_path), "%s/%s.replacement.db", cache, project);
    ASSERT_TRUE(mcp_make_corrupt_project_store(cache, project));
    ASSERT_TRUE(mcp_make_valid_project_store_at(replacement_path, project, replacement_root));

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_replacing_mutation_guard_t replacement = {
        .replacement_path = replacement_path,
        .live_path = db_path,
    };
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_replacing_mutation_guard_begin,
                                              mcp_replacing_mutation_guard_end, &replacement);
    char *resp = cbm_mcp_handle_tool(
        srv, "search_graph", "{\"project\":\"guard-corrupt-recheck\",\"name_pattern\":\".*\"}");
    bool response_used_replacement =
        resp && !response_contains_json_fragment(resp, "\"isError\":true");
    free(resp);
    cbm_mcp_server_free(srv);

    cbm_store_t *check = cbm_store_open_path_query(db_path);
    bool valid_generation = check && cbm_store_check_integrity(check);
    cbm_project_t stored_project = {0};
    bool replacement_root_visible =
        check && cbm_store_get_project(check, project, &stored_project) == CBM_STORE_OK &&
        stored_project.root_path && strcmp(stored_project.root_path, replacement_root) == 0;
    cbm_project_free_fields(&stored_project);
    cbm_store_close(check);
    char unexpected_backup[CBM_SZ_1K];
    int backup_count =
        mcp_find_corrupt_backups(cache, project, unexpected_backup, sizeof(unexpected_backup));
    bool live_exists = cbm_file_exists(db_path);
    bool replacement_consumed = !cbm_file_exists(replacement_path);
    int begin_count = replacement.guard.begin_count;
    int end_count = replacement.guard.end_count;
    bool guarded_project = begin_count == 1 && end_count == 1 &&
                           strcmp(replacement.guard.begin_projects[0], project) == 0 &&
                           strcmp(replacement.guard.end_projects[0], project) == 0;
    bool replacement_attempted = replacement.replacement_attempted;
    bool replacement_succeeded = replacement.replacement_succeeded;

    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    cbm_unlink(replacement_path);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(replacement_attempted);
    ASSERT_TRUE(replacement_succeeded);
    ASSERT_TRUE(guarded_project);
    ASSERT_TRUE(response_used_replacement);
    ASSERT_TRUE(live_exists);
    ASSERT_TRUE(replacement_consumed);
    ASSERT_TRUE(valid_generation);
    ASSERT_TRUE(replacement_root_visible);
    ASSERT_EQ(backup_count, 0);
    PASS();
}

/* A fixed `.corrupt` destination is itself user recovery data. A later
 * quarantine must retain it byte-for-byte and choose a distinct backup name
 * rather than unlinking the previous incident before rename. */
TEST(tool_corrupt_store_cleanup_preserves_existing_backup_and_uses_unique_name) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-unique-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-unique";
    char db_path[CBM_SZ_1K];
    char existing_backup_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    snprintf(existing_backup_path, sizeof(existing_backup_path), "%s.corrupt", db_path);
    ASSERT_TRUE(mcp_make_corrupt_project_store(cache, project));
    ASSERT_EQ(th_write_file(existing_backup_path, "previous-backup-must-survive\n"), 0);

    long existing_len = 0;
    unsigned char *existing_before = mcp_read_file_bytes(existing_backup_path, &existing_len);
    ASSERT_NOT_NULL(existing_before);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t probe = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &probe);
    char *resp = cbm_mcp_handle_tool(
        srv, "search_graph", "{\"project\":\"guard-corrupt-unique\",\"name_pattern\":\".*\"}");
    free(resp);
    cbm_mcp_server_free(srv);

    bool existing_unchanged =
        mcp_file_matches_snapshot(existing_backup_path, existing_before, existing_len);
    free(existing_before);
    char unique_backup_path[CBM_SZ_1K];
    int backup_count =
        mcp_find_corrupt_backups(cache, project, unique_backup_path, sizeof(unique_backup_path));
    cbm_store_t *quarantined =
        unique_backup_path[0] ? cbm_store_open_path_query(unique_backup_path) : NULL;
    bool unique_backup_is_corrupt = quarantined && !cbm_store_check_integrity(quarantined);
    cbm_store_close(quarantined);
    bool live_removed = !cbm_file_exists(db_path);
    int begin_count = probe.begin_count;
    int end_count = probe.end_count;
    bool guarded_project = begin_count == 1 && end_count == 1 &&
                           strcmp(probe.begin_projects[0], project) == 0 &&
                           strcmp(probe.end_projects[0], project) == 0;

    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(guarded_project);
    ASSERT_TRUE(existing_unchanged);
    ASSERT_EQ(backup_count, 2);
    ASSERT_TRUE(unique_backup_path[0] != '\0');
    ASSERT_TRUE(unique_backup_is_corrupt);
    ASSERT_TRUE(live_removed);
    PASS();
}

/* Deterministically fail immediately before atomic snapshot publication on
 * every platform. The incomplete pending copy must be removed while the live
 * DB and its committed WAL remain byte-for-byte untouched. */
TEST(tool_corrupt_store_cleanup_publish_failure_preserves_db_and_wal) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-publish-fail-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-publish-fail";
    char db_path[CBM_SZ_1K];
    char wal_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    cbm_store_t *writer = mcp_open_corrupt_project_store_with_wal(cache, project);
    ASSERT_NOT_NULL(writer);
    ASSERT_TRUE(cbm_file_exists(wal_path));

    long db_len = 0;
    long wal_len = 0;
    unsigned char *db_before = mcp_read_file_bytes(db_path, &db_len);
    unsigned char *wal_before = mcp_read_file_bytes(wal_path, &wal_len);
    ASSERT_NOT_NULL(db_before);
    ASSERT_NOT_NULL(wal_before);
    ASSERT_TRUE(db_len > 0);
    ASSERT_TRUE(wal_len > 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t guard = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &guard);
    mcp_quarantine_hook_probe_t hook = {.deny_step = "before_snapshot_publish"};
    cbm_mcp_server_set_quarantine_test_hook(srv, mcp_quarantine_hook_probe, &hook);
    char *resp =
        cbm_mcp_handle_tool(srv, "search_graph",
                            "{\"project\":\"guard-corrupt-publish-fail\",\"name_pattern\":\".*\"}");

    bool db_unchanged = mcp_file_matches_snapshot(db_path, db_before, db_len);
    bool wal_unchanged = mcp_file_matches_snapshot(wal_path, wal_before, wal_len);
    char unexpected_backup[CBM_SZ_1K];
    int backup_count =
        mcp_find_corrupt_backups(cache, project, unexpected_backup, sizeof(unexpected_backup));
    int artifact_count = mcp_count_corrupt_artifacts(cache, project);
    int begin_count = guard.begin_count;
    int end_count = guard.end_count;
    bool guarded_project = begin_count == 1 && end_count == 1 &&
                           strcmp(guard.begin_projects[0], project) == 0 &&
                           strcmp(guard.end_projects[0], project) == 0;
    bool failed_at_publish =
        hook.call_count == 1 && strcmp(hook.steps[0], "before_snapshot_publish") == 0;

    free(resp);
    cbm_mcp_server_free(srv);
    free(db_before);
    free(wal_before);
    cbm_store_close(writer);
    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(failed_at_publish);
    ASSERT_TRUE(guarded_project);
    ASSERT_TRUE(db_unchanged);
    ASSERT_TRUE(wal_unchanged);
    ASSERT_EQ(backup_count, 0);
    ASSERT_EQ(artifact_count, 0);
    PASS();
}

/* Once the recovery snapshot is atomically visible, a crash/failure before
 * deleting the live generation may leave both copies. The live DB/WAL must be
 * unchanged, and the published backup must already contain committed WAL data
 * as one self-contained SQLite database. */
TEST(tool_corrupt_store_cleanup_publishes_complete_wal_snapshot_before_delete) {
    char cache[256];
    snprintf(cache, sizeof(cache), "%s/cbm-mcp-corrupt-after-publish-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    const char *project = "guard-corrupt-after-publish";
    char db_path[CBM_SZ_1K];
    char wal_path[CBM_SZ_1K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    cbm_store_t *writer = mcp_open_corrupt_project_store_with_wal(cache, project);
    ASSERT_NOT_NULL(writer);
    ASSERT_TRUE(cbm_file_exists(wal_path));

    long db_len = 0;
    long wal_len = 0;
    unsigned char *db_before = mcp_read_file_bytes(db_path, &db_len);
    unsigned char *wal_before = mcp_read_file_bytes(wal_path, &wal_len);
    ASSERT_NOT_NULL(db_before);
    ASSERT_NOT_NULL(wal_before);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    mcp_mutation_guard_probe_t guard = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &guard);
    mcp_quarantine_hook_probe_t hook = {.deny_step = "after_snapshot_publish"};
    cbm_mcp_server_set_quarantine_test_hook(srv, mcp_quarantine_hook_probe, &hook);
    char *resp = cbm_mcp_handle_tool(
        srv, "search_graph",
        "{\"project\":\"guard-corrupt-after-publish\",\"name_pattern\":\".*\"}");

    bool db_unchanged = mcp_file_matches_snapshot(db_path, db_before, db_len);
    bool wal_unchanged = mcp_file_matches_snapshot(wal_path, wal_before, wal_len);
    char backup_path[CBM_SZ_1K];
    int backup_count = mcp_find_corrupt_backups(cache, project, backup_path, sizeof(backup_path));
    int artifact_count = mcp_count_corrupt_artifacts(cache, project);
    cbm_store_t *snapshot = backup_path[0] ? cbm_store_open_path_query(backup_path) : NULL;
    cbm_project_t recovered = {0};
    bool recovered_wal_project =
        snapshot && cbm_store_get_project(snapshot, project, &recovered) == CBM_STORE_OK &&
        recovered.root_path && strcmp(recovered.root_path, "826") == 0;
    cbm_project_free_fields(&recovered);
    cbm_store_close(snapshot);
    char backup_wal[CBM_SZ_2K];
    char backup_shm[CBM_SZ_2K];
    snprintf(backup_wal, sizeof(backup_wal), "%s-wal", backup_path);
    snprintf(backup_shm, sizeof(backup_shm), "%s-shm", backup_path);
    bool snapshot_self_contained = !cbm_file_exists(backup_wal) && !cbm_file_exists(backup_shm);
    bool hook_order = hook.call_count == 2 &&
                      strcmp(hook.steps[0], "before_snapshot_publish") == 0 &&
                      strcmp(hook.steps[1], "after_snapshot_publish") == 0;
    bool guard_balanced = guard.begin_count == 1 && guard.end_count == 1 &&
                          strcmp(guard.begin_projects[0], project) == 0 &&
                          strcmp(guard.end_projects[0], project) == 0;

    free(resp);
    cbm_mcp_server_free(srv);
    free(db_before);
    free(wal_before);
    cbm_store_close(writer);
    mcp_cleanup_corrupt_backups(cache, project);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    cbm_rmdir(cache);

    ASSERT_TRUE(hook_order);
    ASSERT_TRUE(guard_balanced);
    ASSERT_TRUE(db_unchanged);
    ASSERT_TRUE(wal_unchanged);
    ASSERT_EQ(backup_count, 1);
    ASSERT_EQ(artifact_count, 1);
    ASSERT_TRUE(recovered_wal_project);
    ASSERT_TRUE(snapshot_self_contained);
    PASS();
}

TEST(tool_index_repository_reports_store_backed_adr) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-index-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-index-adr-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", tmp_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Store-backed ADR metadata.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT(response_contains_json_fragment(resp, "\"adr_present\":true"));
    ASSERT_NULL(strstr(resp, "adr_hint"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Store-backed ADR metadata."));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_index_repository_dot_uses_absolute_project_key_and_preserves_adr) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-index-dot-adr-test-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-index-dot-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return helper()\n\ndef helper():\n    return 1\n", fp);
    fclose(fp);

    char old_cwd[CBM_SZ_4K];
    ASSERT_NOT_NULL(cbm_getcwd(old_cwd, sizeof(old_cwd)));

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    ASSERT_EQ(cbm_chdir(tmp_dir), 0);
    char *resp =
        cbm_mcp_handle_tool(srv, "index_repository", "{\"repo_path\":\".\",\"mode\":\"fast\"}");
    ASSERT_EQ(cbm_chdir(old_cwd), 0);
    ASSERT_NOT_NULL(resp);
    if (!response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        free(resp);
        cbm_mcp_server_free(srv);
        cleanup_project_db(cache, project);
        restore_cache_dir(saved_copy);
        free(saved_copy);
        free(project);
        remove(src_path);
        cbm_rmdir(cache);
        cbm_rmdir(tmp_dir);
        PASS();
    }
    ASSERT_NOT_NULL(strstr(resp, project));
    ASSERT(!response_contains_json_fragment(resp, "\"project\":\"root\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Dot-path ADR marker.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    ASSERT_EQ(cbm_chdir(tmp_dir), 0);
    resp = cbm_mcp_handle_tool(srv, "index_repository", "{\"repo_path\":\".\",\"mode\":\"fast\"}");
    ASSERT_EQ(cbm_chdir(old_cwd), 0);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT_NOT_NULL(strstr(resp, project));
    ASSERT(response_contains_json_fragment(resp, "\"adr_present\":true"));
    ASSERT(!response_contains_json_fragment(resp, "\"project\":\"root\""));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Dot-path ADR marker."));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_manage_adr_not_found_rich_error) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-missing-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp = cbm_mcp_handle_tool(srv, "manage_adr",
                                     "{\"project\":\"cbm-no-such-project-zzz\",\"mode\":\"get\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NOT_NULL(strstr(resp, "hint"));
    free(resp);

    cbm_mcp_server_free(srv);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    cbm_rmdir(cache);
    PASS();
}

TEST(tool_manage_adr_get_accepts_abs_path) {
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-abspath-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-abspath-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", tmp_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Abs-path normalization test.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", tmp_dir);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Abs-path normalization test."));
    ASSERT_NULL(strstr(resp, "or not indexed"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
}

TEST(tool_manage_adr_get_accepts_symlink_path) {
#ifdef _WIN32
    PASS();
#else
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-adr-realpath-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-adr-realpath-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    char link_path[320];
    snprintf(link_path, sizeof(link_path), "%s-link", tmp_dir);
    (void)unlink(link_path);
    if (symlink(tmp_dir, link_path) != 0) {
        cbm_rmdir(cache);
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    char *project = cbm_project_name_from_path(tmp_dir);
    ASSERT_NOT_NULL(project);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", link_path);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT(response_contains_json_fragment(resp, "\"status\":\"indexed\""));
    ASSERT_NOT_NULL(strstr(resp, project));
    free(resp);

    char update_args[2048];
    snprintf(update_args, sizeof(update_args),
             "{\"project\":\"%s\",\"mode\":\"update\",\"content\":\"## PURPOSE\\n"
             "Symlink-path normalization test.\\n\"}",
             project);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", update_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "updated"));
    free(resp);

    char get_args[512];
    snprintf(get_args, sizeof(get_args), "{\"project\":\"%s\",\"mode\":\"get\"}", link_path);
    resp = cbm_mcp_handle_tool(srv, "manage_adr", get_args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "Symlink-path normalization test."));
    ASSERT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NULL(strstr(resp, "no_adr"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    free(project);
    remove(src_path);
    unlink(link_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);
    PASS();
#endif
}

TEST(tool_detect_changes_not_found_rich_error) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-detect-missing-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS();
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    char *resp =
        cbm_mcp_handle_tool(srv, "detect_changes", "{\"project\":\"cbm-no-such-project-zzz\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "or not indexed"));
    ASSERT_NOT_NULL(strstr(resp, "hint"));
    free(resp);

    cbm_mcp_server_free(srv);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    cbm_rmdir(cache);
    PASS();
}

/* detect_changes owns shell output through regular temporary files. An error
 * after opening that file must use fclose + unlink. The command hook then
 * rejects merge-base only when it reaches the contained subprocess helper, so
 * a raw popen regression bypasses the hook and fails this test. */
TEST(tool_detect_changes_contained_commands_clean_up_error_and_success) {
    char cache[512];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-detect-contained-XXXXXX", cbm_tmpdir());
    bool cache_created = cbm_mkdtemp(cache) != NULL;
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    bool environment_ready = cache_created && cbm_setenv("CBM_CACHE_DIR", cache, 1) == 0;

    char root[CBM_SZ_4K] = {0};
    bool root_ready = cbm_getcwd(root, sizeof(root)) != NULL;
    const char *project = "detect-contained-project";
    cbm_mcp_server_t *srv = environment_ready && root_ready ? cbm_mcp_server_new(NULL) : NULL;
    bool server_ready = srv != NULL;
    cbm_store_t *store = srv ? cbm_mcp_server_store(srv) : NULL;
    bool project_ready = store && cbm_store_upsert_project(store, project, root) == CBM_STORE_OK;
    mcp_command_hook_probe_t command_probe = {.reject_merge_base = true};
    if (project_ready) {
        cbm_mcp_server_set_project(srv, project);
        cbm_mcp_server_set_command_test_hook(srv, mcp_command_hook_probe, &command_probe);
    }

    char *invalid_response =
        project_ready ? cbm_mcp_handle_tool(srv, "detect_changes",
                                            "{\"project\":\"detect-contained-project\","
                                            "\"base_branch\":\"HEAD\",\"scope\":\"files\","
                                            "\"direction\":\"sideways\"}")
                      : NULL;
    bool invalid_rejected = invalid_response && strstr(invalid_response, "invalid direction");
    char logs[640];
    (void)snprintf(logs, sizeof(logs), "%s/logs", cache);
    int artifacts_after_error =
        invalid_response ? mcp_count_directory_entries_with_prefix(logs, ".mcp-command-") : -1;

    char *rejected_response =
        project_ready ? cbm_mcp_handle_tool(srv, "detect_changes",
                                            "{\"project\":\"detect-contained-project\","
                                            "\"base_branch\":\"HEAD\",\"scope\":\"files\"}")
                      : NULL;
    bool containment_rejected =
        rejected_response && strstr(rejected_response, "contained command could not complete");
    int artifacts_after_rejection =
        rejected_response ? mcp_count_directory_entries_with_prefix(logs, ".mcp-command-") : -1;

    command_probe.reject_merge_base = false;
    char *success_response =
        project_ready ? cbm_mcp_handle_tool(srv, "detect_changes",
                                            "{\"project\":\"detect-contained-project\","
                                            "\"base_branch\":\"HEAD\",\"scope\":\"files\"}")
                      : NULL;
    bool merge_base_reported = success_response && strstr(success_response, "merge_base");
    int artifacts_after_success =
        success_response ? mcp_count_directory_entries_with_prefix(logs, ".mcp-command-") : -1;

    free(invalid_response);
    free(rejected_response);
    free(success_response);
    cbm_mcp_server_free(srv);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    bool cleaned = !cache_created || th_rmtree(cache) == 0;

    ASSERT_TRUE(cache_created);
    ASSERT_TRUE(environment_ready);
    ASSERT_TRUE(root_ready);
    ASSERT_TRUE(server_ready);
    ASSERT_TRUE(project_ready);
    ASSERT_TRUE(invalid_rejected);
    ASSERT_EQ(artifacts_after_error, 0);
    ASSERT_TRUE(containment_rejected);
    ASSERT_EQ(artifacts_after_rejection, 0);
    ASSERT_TRUE(merge_base_reported);
    ASSERT_EQ(artifacts_after_success, 0);
    ASSERT_EQ(command_probe.diff_calls, 3);
    ASSERT_EQ(command_probe.merge_base_calls, 2);
    ASSERT_TRUE(cleaned);
    PASS();
}

TEST(tool_ingest_traces_basic) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":37,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"ingest_traces\","
             "\"arguments\":{\"traces\":[{\"caller\":\"a\",\"callee\":\"b\"}]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "accepted"));
    ASSERT_NOT_NULL(strstr(resp, "traces_received"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(tool_ingest_traces_empty) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":38,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"ingest_traces\","
                                   "\"arguments\":{\"traces\":[]}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "accepted"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  IDLE STORE EVICTION
 * ══════════════════════════════════════════════════════════════════ */

TEST(store_idle_eviction) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    /* Trigger resolve_store via a tool call to set store_last_used */
    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* Evict with 0s timeout → should evict immediately */
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_FALSE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_no_eviction_within_timeout) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* Evict with large timeout → should NOT evict */
    cbm_mcp_server_evict_idle(srv, 99999);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_evict_protects_initial_store) {
    /* Evicting with NULL server should not crash */
    cbm_mcp_server_evict_idle(NULL, 0);

    /* Evicting server whose store was never accessed via a named project
     * should NOT evict the initial in-memory store (store_last_used == 0). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(store_idle_evict_access_resets_timer) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    cbm_mcp_server_set_project(srv, "test-evict");

    /* First access */
    char *resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    /* Second access (resets timer) */
    resp = cbm_mcp_handle_tool(srv, "get_graph_schema", "{\"project\":\"test-evict\"}");
    free(resp);

    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* With large timeout, store should survive */
    cbm_mcp_server_evict_idle(srv, 99999);
    ASSERT_TRUE(cbm_mcp_server_has_cached_store(srv));

    /* With 0 timeout, store should be evicted */
    cbm_mcp_server_evict_idle(srv, 0);
    ASSERT_FALSE(cbm_mcp_server_has_cached_store(srv));

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  URI HELPERS
 * ══════════════════════════════════════════════════════════════════ */

TEST(parse_file_uri_unix) {
    char path[256];
    ASSERT_TRUE(cbm_parse_file_uri("file:///home/user/project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/home/user/project");

    ASSERT_TRUE(cbm_parse_file_uri("file:///tmp/test", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/tmp/test");

    ASSERT_TRUE(cbm_parse_file_uri("file:///", path, sizeof(path)));
    ASSERT_STR_EQ(path, "/");
    PASS();
}

TEST(parse_file_uri_windows) {
    char path[256];
    /* Windows drive letter — leading / stripped */
    ASSERT_TRUE(cbm_parse_file_uri("file:///C:/Users/project", path, sizeof(path)));
    ASSERT_STR_EQ(path, "C:/Users/project");

    ASSERT_TRUE(cbm_parse_file_uri("file:///D:/Projects/myapp", path, sizeof(path)));
    ASSERT_STR_EQ(path, "D:/Projects/myapp");
    PASS();
}

TEST(parse_file_uri_invalid) {
    char path[256];
    /* Non-file URI */
    ASSERT_FALSE(cbm_parse_file_uri("https://example.com", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");

    /* Empty string */
    ASSERT_FALSE(cbm_parse_file_uri("", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");

    /* NULL */
    ASSERT_FALSE(cbm_parse_file_uri(NULL, path, sizeof(path)));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SNIPPET TESTS — Port of internal/tools/snippet_test.go
 * ══════════════════════════════════════════════════════════════════ */

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* Create an MCP server pre-populated with nodes/edges matching Go testSnippetServer.
 * Writes a source file to tmp_dir/project/main.go.
 * Caller must free the server with cbm_mcp_server_free and
 * unlink the source file + rmdir manually. */
static cbm_mcp_server_t *setup_snippet_server(char *tmp_dir, size_t tmp_sz) {
    /* Create temp dir */
    snprintf(tmp_dir, tmp_sz, "/tmp/cbm_snippet_test_XXXXXX");
    if (!cbm_mkdtemp(tmp_dir))
        return NULL;

    char proj_dir[512];
    snprintf(proj_dir, sizeof(proj_dir), "%s/project", tmp_dir);
    cbm_mkdir(proj_dir);

    /* Write sample source file */
    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.go", proj_dir);
    FILE *fp = fopen(src_path, "w");
    if (!fp)
        return NULL;
    fprintf(fp, "package main\n"
                "\n"
                "func HandleRequest() error {\n"
                "\treturn nil\n"
                "}\n"
                "\n"
                "func ProcessOrder(id int) {\n"
                "\t// process\n"
                "}\n"
                "\n"
                "func Run() {\n"
                "\t// server\n"
                "}\n");
    fclose(fp);

    /* Create server with in-memory store */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv)
        return NULL;

    cbm_store_t *st = cbm_mcp_server_store(srv);
    if (!st) {
        cbm_mcp_server_free(srv);
        return NULL;
    }

    const char *proj_name = "test-project";
    cbm_mcp_server_set_project(srv, proj_name);
    cbm_store_upsert_project(st, proj_name, proj_dir);

    /* Create nodes */
    cbm_node_t n_hr = {0};
    n_hr.project = proj_name;
    n_hr.label = "Function";
    n_hr.name = "HandleRequest";
    n_hr.qualified_name = "test-project.cmd.server.main.HandleRequest";
    n_hr.file_path = "main.go";
    n_hr.start_line = 3;
    n_hr.end_line = 5;
    n_hr.properties_json = "{\"signature\":\"func HandleRequest() error\","
                           "\"return_type\":\"error\","
                           "\"is_exported\":true}";
    int64_t id_hr = cbm_store_upsert_node(st, &n_hr);

    cbm_node_t n_po = {0};
    n_po.project = proj_name;
    n_po.label = "Function";
    n_po.name = "ProcessOrder";
    n_po.qualified_name = "test-project.cmd.server.main.ProcessOrder";
    n_po.file_path = "main.go";
    n_po.start_line = 7;
    n_po.end_line = 9;
    n_po.properties_json = "{\"signature\":\"func ProcessOrder(id int)\"}";
    int64_t id_po = cbm_store_upsert_node(st, &n_po);

    cbm_node_t n_run1 = {0};
    n_run1.project = proj_name;
    n_run1.label = "Function";
    n_run1.name = "Run";
    n_run1.qualified_name = "test-project.cmd.server.Run";
    n_run1.file_path = "main.go";
    n_run1.start_line = 11;
    n_run1.end_line = 13;
    int64_t id_run1 = cbm_store_upsert_node(st, &n_run1);

    cbm_node_t n_run2 = {0};
    n_run2.project = proj_name;
    n_run2.label = "Function";
    n_run2.name = "Run";
    n_run2.qualified_name = "test-project.cmd.worker.Run";
    n_run2.file_path = "main.go";
    n_run2.start_line = 11;
    n_run2.end_line = 13;
    cbm_store_upsert_node(st, &n_run2);

    /* Create edges: HandleRequest -> ProcessOrder, HandleRequest -> Run1 */
    cbm_edge_t e1 = {.project = proj_name, .source_id = id_hr, .target_id = id_po, .type = "CALLS"};
    cbm_store_insert_edge(st, &e1);

    cbm_edge_t e2 = {
        .project = proj_name, .source_id = id_hr, .target_id = id_run1, .type = "CALLS"};
    cbm_store_insert_edge(st, &e2);
    (void)id_run1; /* run1 used for edge above */

    return srv;
}

/* Cleanup temp files created by setup_snippet_server */
static void cleanup_snippet_dir(const char *tmp_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/project/main.go", tmp_dir);
    unlink(path);
    snprintf(path, sizeof(path), "%s/project", tmp_dir);
    rmdir(path);
    rmdir(tmp_dir);
}

/* Extract the inner "text" value from an MCP tool result JSON.
 * The MCP envelope is: {"content":[{"type":"text","text":"<inner json>"}]}
 * This returns the unescaped inner JSON. Caller must free. */
static char *extract_text_content(const char *mcp_result) {
    if (!mcp_result)
        return NULL;
    yyjson_doc *doc = yyjson_read(mcp_result, strlen(mcp_result), 0);
    if (!doc)
        return strdup(mcp_result); /* fallback */
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *content = yyjson_obj_get(root, "content");
    if (!content) {
        /* Handle JSON-RPC wrapper: {"jsonrpc":...,"result":{"content":[...]}} */
        yyjson_val *rpc_result = yyjson_obj_get(root, "result");
        if (rpc_result) {
            content = yyjson_obj_get(rpc_result, "content");
        }
    }
    if (!content || !yyjson_is_arr(content)) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *item = yyjson_arr_get(content, 0);
    if (!item) {
        yyjson_doc_free(doc);
        return strdup(mcp_result);
    }
    yyjson_val *text = yyjson_obj_get(item, "text");
    const char *str = yyjson_get_str(text);
    char *result = str ? strdup(str) : strdup(mcp_result);
    yyjson_doc_free(doc);
    return result;
}

/* Call get_code_snippet and extract inner text content.
 * Caller must free returned string. */
static char *call_snippet(cbm_mcp_server_t *srv, const char *args_json) {
    char *raw = cbm_mcp_handle_tool(srv, "get_code_snippet", args_json);
    char *text = extract_text_content(raw);
    free(raw);
    return text;
}

static bool is_valid_json_response(const char *json) {
    if (!json) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_doc_free(doc);
    return true;
}

static bool snippet_source_has_replacement(const char *json) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *source = yyjson_obj_get(root, "source");
    const char *source_str = yyjson_get_str(source);
    bool found = source_str && strstr(source_str, "\xEF\xBF\xBD");
    yyjson_doc_free(doc);
    return found;
}

/* ── TestSnippet_ExactQN ──────────────────────────────────────── */

TEST(snippet_exact_qn) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    /* Exact match should NOT have match_method */
    ASSERT_NULL(strstr(resp, "\"match_method\""));
    /* No property-blob spill: the source IS the payload (signature and
     * docstring are literally in it); metrics live behind search_graph
     * fields=[...]. */
    ASSERT_NULL(strstr(resp, "\"signature\""));
    ASSERT_NULL(strstr(resp, "\"return_type\""));
    /* Caller/callee counts: 0 callers, 2 callees */
    ASSERT_NOT_NULL(strstr(resp, "\"callers\":0"));
    ASSERT_NOT_NULL(strstr(resp, "\"callees\":2"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_QNSuffix ─────────────────────────────────────── */

TEST(snippet_qn_suffix) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = call_snippet(srv, "{\"qualified_name\":\"main.HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_UniqueShortName ──────────────────────────────── */

TEST(snippet_unique_short_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "ProcessOrder" is unique — suffix tier matches (QN ends with .ProcessOrder) */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"ProcessOrder\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"ProcessOrder\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_NameTier ─────────────────────────────────────── */

TEST(snippet_name_tier) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "HandleRequest" — suffix tier finds it (QN ends with .HandleRequest) */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"name\":\"HandleRequest\""));
    ASSERT_NOT_NULL(strstr(resp, "\"match_method\":\"suffix\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AmbiguousShortName ───────────────────────────── */

TEST(snippet_ambiguous_short_name) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" matches 2 nodes — should return suggestions */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"status\":\"ambiguous\""));
    ASSERT_NOT_NULL(strstr(resp, "\"message\""));
    ASSERT_NOT_NULL(strstr(resp, "\"suggestions\""));
    /* Must NOT have "error" key */
    ASSERT_NULL(strstr(resp, "\"error\""));
    /* Must NOT have "source" */
    ASSERT_NULL(strstr(resp, "\"source\""));
    /* Should have at least 2 suggestions with qualified_name */
    ASSERT_NOT_NULL(strstr(resp, "test-project.cmd.server.Run"));
    ASSERT_NOT_NULL(strstr(resp, "test-project.cmd.worker.Run"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_NotFound ─────────────────────────────────────── */

TEST(snippet_not_found) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp = call_snippet(srv, "{\"qualified_name\":\"CompletelyNonexistentFunctionXYZ123\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should return error or suggestions */
    ASSERT_TRUE(strstr(resp, "not found") || strstr(resp, "suggestions"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_FuzzySuggestions ─────────────────────────────── */

TEST(snippet_fuzzy_suggestions) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Handle" is not an exact QN or suffix — should get not-found guidance */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Handle\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should guide user to search_graph */
    ASSERT_NOT_NULL(strstr(resp, "search_graph"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_EnrichedProperties ───────────────────────────── */

TEST(snippet_enriched_properties) {
    /* GUARD (inverted since the compact-output change): the snippet response
     * carries the verbatim source plus location/degree/coverage metadata and
     * NOTHING from the node's property blob — no signature/return_type/
     * is_exported duplication, and never the fp/sp/bt similarity internals
     * (41% of the legacy response). */
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    ASSERT_NULL(strstr(resp, "\"signature\""));
    ASSERT_NULL(strstr(resp, "\"return_type\""));
    ASSERT_NULL(strstr(resp, "\"is_exported\""));
    ASSERT_NULL(strstr(resp, "\"fp\""));
    ASSERT_NULL(strstr(resp, "\"bt\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_FuzzyLastSegment ─────────────────────────────── */

TEST(snippet_fuzzy_last_segment) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "auth.handlers.HandleRequest" — suffix match should find HandleRequest */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"auth.handlers.HandleRequest\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Should either find it via suffix or guide to search_graph */
    ASSERT_TRUE(strstr(resp, "HandleRequest") != NULL || strstr(resp, "search_graph") != NULL);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AutoResolve_Default ──────────────────────────── */

TEST(snippet_auto_resolve_default) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" is ambiguous (2 candidates). Without auto_resolve → suggestions */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"status\":\"ambiguous\""));
    ASSERT_NULL(strstr(resp, "\"source\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_AutoResolve_Enabled ──────────────────────────── */

TEST(snippet_auto_resolve_enabled) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    /* "Run" — suffix match should find candidates or guide to search */
    char *resp = call_snippet(srv, "{\"qualified_name\":\"Run\","
                                   "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* "Run" matches multiple nodes via suffix → should get suggestions or source */
    ASSERT_TRUE(strstr(resp, "Run") != NULL);
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_IncludeNeighbors_Default ─────────────────────── */

TEST(snippet_include_neighbors_default) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    /* Without include_neighbors → NO caller_names/callee_names */
    ASSERT_NULL(strstr(resp, "\"caller_names\""));
    ASSERT_NULL(strstr(resp, "\"callee_names\""));
    /* But should still have counts */
    ASSERT_NOT_NULL(strstr(resp, "\"callers\""));
    ASSERT_NOT_NULL(strstr(resp, "\"callees\""));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_IncludeNeighbors_Enabled ─────────────────────── */

TEST(snippet_include_neighbors_enabled) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char *resp =
        call_snippet(srv, "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                          "\"include_neighbors\":true,\"project\":\"test-project\"}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"source\""));
    /* HandleRequest has 0 callers → no caller_names array */
    ASSERT_NULL(strstr(resp, "\"caller_names\""));
    /* HandleRequest has 2 callees: ProcessOrder and Run */
    ASSERT_NOT_NULL(strstr(resp, "\"callee_names\""));
    ASSERT_NOT_NULL(strstr(resp, "ProcessOrder"));
    ASSERT_NOT_NULL(strstr(resp, "Run"));
    free(resp);

    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ── TestSnippet_SourceInvalidUtf8 ────────────────────────────── */

TEST(snippet_source_invalid_utf8) {
    char tmp[256];
    cbm_mcp_server_t *srv = setup_snippet_server(tmp, sizeof(tmp));
    ASSERT_NOT_NULL(srv);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/project/main.go", tmp);
    FILE *fp = fopen(src_path, "wb");
    ASSERT_NOT_NULL(fp);
    const unsigned char source[] = {
        'p',  'a',  'c', 'k', 'a', 'g',  'e',  ' ',  'm',  'a',  'i',  'n', '\n', '\n',
        'f',  'u',  'n', 'c', ' ', 'H',  'a',  'n',  'd',  'l',  'e',  'R', 'e',  'q',
        'u',  'e',  's', 't', '(', ')',  ' ',  'e',  'r',  'r',  'o',  'r', ' ',  '{',
        '\n', '\t', '/', '/', ' ', 0xC0, 0xD4, 0xB7, 0xC2, '\n', '\t', 'r', 'e',  't',
        'u',  'r',  'n', ' ', 'n', 'i',  'l',  '\n', '}',  '\n'};
    ASSERT_EQ(fwrite(source, 1, sizeof(source), fp), sizeof(source));
    ASSERT_EQ(fclose(fp), 0);

    char *raw =
        cbm_mcp_handle_tool(srv, "get_code_snippet",
                            "{\"qualified_name\":\"test-project.cmd.server.main.HandleRequest\","
                            "\"project\":\"test-project\"}");
    ASSERT_TRUE(is_valid_json_response(raw));
    char *resp = extract_text_content(raw);
    ASSERT_NOT_NULL(resp);
    ASSERT_TRUE(is_valid_json_response(resp));
    ASSERT_NULL(strstr(resp, "\xC0\xD4"));
    ASSERT_NOT_NULL(strstr(resp, "HandleRequest"));
    ASSERT_NOT_NULL(strstr(resp, "return nil"));
    ASSERT_TRUE(snippet_source_has_replacement(resp));

    free(resp);
    free(raw);
    cbm_mcp_server_free(srv);
    cleanup_snippet_dir(tmp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(jsonrpc_parse_empty_string) {
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_missing_jsonrpc_field) {
    /* jsonrpc field absent — parser defaults to "2.0" if method present */
    const char *line = "{\"id\":1,\"method\":\"initialize\",\"params\":{}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.jsonrpc, "2.0");
    ASSERT_STR_EQ(req.method, "initialize");
    ASSERT_TRUE(req.has_id);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_missing_method) {
    /* method is required — should fail */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":1,\"params\":{}}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_string_id) {
    /* JSON-RPC §4: string and numeric ids are distinct. A string id is
     * preserved verbatim (issue #253), never coerced to a number. */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":\"99\",\"method\":\"tools/list\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(req.has_id);
    ASSERT_NOT_NULL(req.id_str);
    ASSERT_STR_EQ(req.id_str, "99");
    ASSERT_STR_EQ(req.method, "tools/list");
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_no_params) {
    /* Request with no params field — params_raw should be NULL */
    const char *line = "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/list\"}";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_NULL(req.params_raw);
    ASSERT_EQ(req.id, 5);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_extra_whitespace) {
    /* Leading/trailing whitespace and internal spacing in JSON */
    const char *line = "  { \"jsonrpc\" : \"2.0\" , \"id\" : 7 , \"method\" : \"ping\" }  ";
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse(line, &req);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(req.id, 7);
    ASSERT_STR_EQ(req.method, "ping");
    cbm_jsonrpc_request_free(&req);
    PASS();
}

TEST(jsonrpc_parse_array_not_object) {
    /* JSON array at root — not a valid JSON-RPC request */
    cbm_jsonrpc_request_t req = {0};
    int rc = cbm_jsonrpc_parse("[1,2,3]", &req);
    ASSERT_EQ(rc, -1);
    cbm_jsonrpc_request_free(&req);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(mcp_get_string_arg_empty_json) {
    /* Empty JSON string — yyjson_read fails → NULL */
    char *val = cbm_mcp_get_string_arg("", "key");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_string_arg_empty_object) {
    /* Valid JSON with no keys → NULL for any key */
    char *val = cbm_mcp_get_string_arg("{}", "key");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_string_arg_nested_value) {
    /* Value is an object, not a string → should return NULL */
    const char *args = "{\"config\":{\"nested\":true},\"name\":\"hello\"}";
    char *val = cbm_mcp_get_string_arg(args, "config");
    ASSERT_NULL(val); /* not a string type */
    val = cbm_mcp_get_string_arg(args, "name");
    ASSERT_NOT_NULL(val);
    ASSERT_STR_EQ(val, "hello");
    free(val);
    PASS();
}

TEST(mcp_get_string_arg_int_value) {
    /* Value is an integer, not a string → NULL */
    char *val = cbm_mcp_get_string_arg("{\"count\":42}", "count");
    ASSERT_NULL(val);
    PASS();
}

TEST(mcp_get_int_arg_empty_json) {
    int val = cbm_mcp_get_int_arg("", "key", 99);
    ASSERT_EQ(val, 99);
    PASS();
}

TEST(mcp_get_int_arg_string_value) {
    /* Value is a string, not int → should return default */
    int val = cbm_mcp_get_int_arg("{\"limit\":\"ten\"}", "limit", 5);
    ASSERT_EQ(val, 5);
    PASS();
}

TEST(mcp_get_int_arg_bool_value) {
    /* Value is a bool, not int → default */
    int val = cbm_mcp_get_int_arg("{\"flag\":true}", "flag", -1);
    ASSERT_EQ(val, -1);
    PASS();
}

TEST(mcp_get_bool_arg_empty_json) {
    bool val = cbm_mcp_get_bool_arg("", "key");
    ASSERT_FALSE(val);
    PASS();
}

TEST(mcp_get_bool_arg_int_value) {
    /* Value is int 1, not bool → should return false */
    bool val = cbm_mcp_get_bool_arg("{\"flag\":1}", "flag");
    ASSERT_FALSE(val);
    PASS();
}

TEST(mcp_get_tool_name_empty_json) {
    char *name = cbm_mcp_get_tool_name("");
    ASSERT_NULL(name);
    PASS();
}

TEST(mcp_get_tool_name_missing_name) {
    char *name = cbm_mcp_get_tool_name("{\"arguments\":{}}");
    ASSERT_NULL(name);
    PASS();
}

TEST(mcp_get_arguments_empty_json) {
    char *args = cbm_mcp_get_arguments("");
    ASSERT_NULL(args);
    PASS();
}

TEST(mcp_get_arguments_no_arguments_key) {
    /* No "arguments" key → returns "{}" */
    char *args = cbm_mcp_get_arguments("{\"name\":\"tool\"}");
    ASSERT_NOT_NULL(args);
    ASSERT_STR_EQ(args, "{}");
    free(args);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  FILE URI PARSING — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(parse_file_uri_http_scheme) {
    char path[256];
    ASSERT_FALSE(cbm_parse_file_uri("http://example.com/path", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");
    PASS();
}

TEST(parse_file_uri_ftp_scheme) {
    char path[256];
    ASSERT_FALSE(cbm_parse_file_uri("ftp://server/file.txt", path, sizeof(path)));
    ASSERT_STR_EQ(path, "");
    PASS();
}

TEST(parse_file_uri_buffer_too_small) {
    char path[5]; /* only 5 bytes — path gets truncated */
    ASSERT_TRUE(cbm_parse_file_uri("file:///usr/local/bin", path, sizeof(path)));
    /* snprintf truncates to 4 chars + NUL */
    ASSERT_EQ(strlen(path), 4);
    ASSERT_STR_EQ(path, "/usr");
    PASS();
}

TEST(parse_file_uri_spaces_in_path) {
    char path[256];
    ASSERT_TRUE(cbm_parse_file_uri("file:///home/user/my%20project", path, sizeof(path)));
    /* Raw percent-encoding is preserved (not decoded) */
    ASSERT_STR_EQ(path, "/home/user/my%20project");
    PASS();
}

TEST(parse_file_uri_null_out_path) {
    /* NULL out_path — should not crash */
    ASSERT_FALSE(cbm_parse_file_uri("file:///tmp", NULL, 256));
    PASS();
}

TEST(parse_file_uri_zero_size) {
    char path[256] = "garbage";
    /* out_size=0 → should fail safely */
    ASSERT_FALSE(cbm_parse_file_uri("file:///tmp", path, 0));
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SERVER HANDLE — EDGE CASES
 * ══════════════════════════════════════════════════════════════════ */

TEST(server_handle_invalid_json) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    char *resp = cbm_mcp_server_handle(srv, "this is not json at all");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    ASSERT_NOT_NULL(strstr(resp, "-32700")); /* Parse error */
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_empty_object) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* Valid JSON but no method field → parse error */
    char *resp = cbm_mcp_server_handle(srv, "{}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "\"error\""));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

TEST(server_handle_tools_call_missing_name) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);

    /* tools/call with no tool name in params */
    char *resp =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":50,\"method\":\"tools/call\","
                                   "\"params\":{\"arguments\":{}}}");
    ASSERT_NOT_NULL(resp);
    /* Should return error about unknown/missing tool */
    ASSERT_NOT_NULL(strstr(resp, "\"id\":50"));
    ASSERT_TRUE(strstr(resp, "error") || strstr(resp, "isError") || strstr(resp, "unknown"));
    free(resp);

    cbm_mcp_server_free(srv);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  POLL/GETLINE FILE* BUFFERING FIX
 * ══════════════════════════════════════════════════════════════════ */

#ifndef _WIN32
#include <unistd.h>
#include <signal.h>

/* Signal handler used by alarm() to abort the test if it hangs */
static void alarm_handler(int sig) {
    (void)sig;
    /* Writing to stderr is async-signal-safe */
    const char msg[] = "FAIL: mcp_server_run_rapid_messages timed out (>5s)\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
}

TEST(mcp_server_run_rapid_messages) {
    /* Simulate a client sending initialize + notifications/initialized +
     * tools/list all at once (no delays), which exercises the FILE*
     * buffering fix: the first getline() over-reads kernel data into the
     * libc buffer; without the fix, subsequent poll() calls block for 60s.
     *
     * We use alarm(5) to abort the test process if the server hangs. */
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    /* Write all 3 messages to the write end in one shot */
    const char *msgs = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{}}}\n"
                       "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n"
                       "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n";
    ssize_t written = write(fds[1], msgs, strlen(msgs));
    ASSERT_TRUE(written > 0);
    close(fds[1]); /* EOF signals end of input to the server */

    FILE *in_fp = fdopen(fds[0], "r");
    ASSERT_NOT_NULL(in_fp);

    FILE *out_fp = tmpfile();
    ASSERT_NOT_NULL(out_fp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* Install alarm to fail the test if cbm_mcp_server_run blocks */
    signal(SIGALRM, alarm_handler);
    alarm(5);

    int rc = cbm_mcp_server_run(srv, in_fp, out_fp);

    alarm(0); /* cancel alarm */
    signal(SIGALRM, SIG_DFL);

    ASSERT_EQ(rc, 0);

    /* Verify both responses are present:
     *   id:1 — initialize response
     *   id:2 — tools/list response (notifications/initialized produces none)
     * and that the tools list payload is included. */
    rewind(out_fp);
    char buf[4096] = {0};
    size_t nread = fread(buf, 1, sizeof(buf) - 1, out_fp);
    ASSERT_TRUE(nread > 0);
    ASSERT_NOT_NULL(strstr(buf, "\"id\":1"));
    ASSERT_NOT_NULL(strstr(buf, "\"id\":2"));
    ASSERT_NOT_NULL(strstr(buf, "tools"));

    cbm_mcp_server_free(srv);
    fclose(out_fp);
    /* in_fp already EOF; fclose cleans up */
    fclose(in_fp);
    PASS();
}
#endif /* !_WIN32 */

/* Issue #235: passing an unrecognised project name to a tool crashed the
 * binary with a buffer overflow while building the "available_projects"
 * error list — collect_db_project_names overflowed projects[CBM_SZ_4K] via
 * an unsigned underflow on (out_sz - offset) once the listed names exceeded
 * the buffer. Fill a temp cache dir with enough long-named .db files to
 * exceed 4 KB, then hit the bad-project path. Under ASan a regression aborts
 * here; the fixed bounds-check keeps it clean and returns a normal error. */
#define ISSUE235_DBNAME(buf, dir, i)                                                         \
    snprintf((buf), sizeof(buf),                                                             \
             "%s/proj_%02d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.db",                      \
             (dir), (i))
TEST(tool_bad_project_name_no_overflow_issue235) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-badproj-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* 40 * ~120-char names overflows the 4 KB available-projects buffer.
     * collect_db_project_names advertises each db's INTERNAL project name
     * (#704), so the fixture must hold valid dbs with long internal names —
     * not stub files — for the bounds-check path to actually be exercised. */
    enum { ISSUE235_N = 40 };
    for (int i = 0; i < ISSUE235_N; i++) {
        char name[512];
        ISSUE235_DBNAME(name, cache, i);
        char iname[256];
        snprintf(iname, sizeof(iname),
                 "proj_%02d_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                 "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 i);
        cbm_store_t *st = cbm_store_open_path(name);
        if (st) {
            cbm_store_upsert_project(st, iname, cache);
            cbm_store_close(st);
        }
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":"
             "\"search_graph\",\"arguments\":{\"label\":\"Function\","
             "\"project\":\"definitely-not-a-real-project-xyz\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));
    free(resp);
    cbm_mcp_server_free(srv);

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    for (int i = 0; i < ISSUE235_N; i++) {
        char name[512];
        ISSUE235_DBNAME(name, cache, i);
        cbm_unlink(name);
        char side[540];
        snprintf(side, sizeof(side), "%s-wal", name);
        cbm_unlink(side);
        snprintf(side, sizeof(side), "%s-shm", name);
        cbm_unlink(side);
    }
    cbm_rmdir(cache);
    PASS();
}
#undef ISSUE235_DBNAME

/* Issue #235 (follow-up): with many long-named projects indexed,
 * collect_db_project_names overflowed projects[CBM_SZ_4K] and truncated the
 * LAST name MID-TOKEN, then clamped offset to out_sz-1 — emitting malformed,
 * unterminated JSON like
 *   ...,"available_projects":["a",...,"vjson_49_bbb],"count":50}
 * (unclosed string + unclosed array). build_project_list_error wrapped that
 * invalid body into the tool error, so a "project not found" reply was NOT
 * valid JSON once enough projects were indexed.
 *
 * Reproduce-first: fill an isolated cache dir with enough long INTERNAL-named
 * dbs to overflow the 4 KB buffer, hit the bad-project path, then assert the
 * ERROR BODY (the inner MCP text content) parses as valid JSON and that
 * available_projects is a JSON array whose length == count. RED on the
 * truncating code (yyjson_read returns NULL on the mid-token cut); GREEN after
 * the element-boundary fix, which only ever writes whole "name" tokens. */
#define BADPROJ_JSON_DBNAME(buf, dir, i)                                                      \
    snprintf((buf), sizeof(buf),                                                              \
             "%s/vjson_%02d_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.db",                       \
             (dir), (i))
TEST(tool_bad_project_error_valid_json_issue235) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-badproj-vjson-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* 50 * ~120-char INTERNAL names >> 4 KB → the available_projects buffer
     * overflows and the last name is cut mid-token on the unfixed code. */
    enum { BADPROJ_N = 50 };
    for (int i = 0; i < BADPROJ_N; i++) {
        char name[512];
        BADPROJ_JSON_DBNAME(name, cache, i);
        char iname[256];
        snprintf(iname, sizeof(iname),
                 "vjson_%02d_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                 "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 i);
        cbm_store_t *st = cbm_store_open_path(name);
        if (st) {
            cbm_store_upsert_project(st, iname, cache);
            cbm_store_close(st);
        }
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":"
             "\"search_graph\",\"arguments\":{\"label\":\"Function\","
             "\"project\":\"definitely-not-a-real-project-xyz\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "not found"));

    /* The inner MCP text content is the error body built by
     * build_project_list_error. Capture its validity BEFORE cleanup so a RED
     * failure still restores the environment. */
    char *body = extract_text_content(resp);
    bool body_valid = false;
    bool aps_ok = false; /* available_projects is an array whose len == count */
    if (body) {
        yyjson_doc *bdoc = yyjson_read(body, strlen(body), 0);
        if (bdoc) {
            body_valid = true;
            yyjson_val *broot = yyjson_doc_get_root(bdoc);
            yyjson_val *aps = yyjson_obj_get(broot, "available_projects");
            yyjson_val *cnt = yyjson_obj_get(broot, "count");
            if (aps && yyjson_is_arr(aps) && cnt && yyjson_is_int(cnt)) {
                aps_ok = (yyjson_arr_size(aps) == (size_t)yyjson_get_int(cnt));
            }
            yyjson_doc_free(bdoc);
        }
    }
    free(body);
    free(resp);
    cbm_mcp_server_free(srv);

    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    for (int i = 0; i < BADPROJ_N; i++) {
        char name[512];
        BADPROJ_JSON_DBNAME(name, cache, i);
        cbm_unlink(name);
        char side[540];
        snprintf(side, sizeof(side), "%s-wal", name);
        cbm_unlink(side);
        snprintf(side, sizeof(side), "%s-shm", name);
        cbm_unlink(side);
    }
    cbm_rmdir(cache);

    /* RED on the unfixed code: mid-token truncation → invalid JSON body. */
    ASSERT_TRUE(body_valid);
    ASSERT_TRUE(aps_ok);
    PASS();
}
#undef BADPROJ_JSON_DBNAME

/* ── #704: project resolution must key on the db's INTERNAL project name ──
 *
 * Issue #704: project resolution is registry-less and filename-addressed.
 * resolve_store() opens <cache>/<passed>.db and then requires the internal
 * `projects.name` row to equal the passed name; list_projects /
 * collect_db_project_names derive the advertised name from the .db FILENAME.
 * When a db's filename != its internal name (a legacy '.'-vs-'-' username
 * twin, or a copied/renamed file) it shows up in list_projects under the
 * filename, but every query returns "project not found" — node rows are
 * tagged with the INTERNAL name, so neither the filename nor the resolve
 * path lines up. The fix makes list + resolve both key on the INTERNAL name.
 *
 * Reproduce-first fixture in an isolated CBM_CACHE_DIR:
 *   - alpha704.db  : filename == internal name "alpha704"   (control / fast path)
 *   - gamma704.db  : internal name "beta704"                (DRIFT: built as
 *                    beta704.db then renamed → filename != internal name)
 *   - ghost704.db  : 0-byte file                            (ghost / unresolvable)
 *
 * RED on buggy code / GREEN on the fix:
 *   A. list_projects advertises "beta704" (internal), NOT "gamma704" (filename),
 *      and NOT "ghost704" (0-byte filtered).
 *   B. search_graph(project="beta704") resolves via the cache-dir scan and
 *      returns the node — not the "project not found" error.
 *   C. control project "alpha704" still resolves on the fast path.
 *   D. the 0-byte ghost is not resolvable.
 *   E. addressing the drifted db by its FILENAME ("gamma704") stays not-found
 *      (we key on the internal name, never the file on disk).
 */

/* Create a file-backed project db at <dir>/<filename> whose INTERNAL project
 * name is `internal` (which may differ from the filename), holding one
 * Function node named `fn`. Returns true on success. */
static bool issue704_make_db(const char *dir, const char *filename, const char *internal,
                             const char *fn) {
    char path[700];
    snprintf(path, sizeof(path), "%s/%s", dir, filename);
    cbm_store_t *st = cbm_store_open_path(path);
    if (!st) {
        return false;
    }
    bool ok = (cbm_store_upsert_project(st, internal, dir) == CBM_STORE_OK);
    if (ok) {
        char qn[256];
        snprintf(qn, sizeof(qn), "%s.%s", internal, fn);
        cbm_node_t n = {0};
        n.project = internal;
        n.label = "Function";
        n.name = fn;
        n.qualified_name = qn;
        n.file_path = "main.go";
        n.start_line = 1;
        n.end_line = 2;
        ok = (cbm_store_upsert_node(st, &n) > 0);
    }
    cbm_store_close(st);
    return ok;
}

TEST(tool_resolve_store_by_internal_name_issue704) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-issue704-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails — not a #704 signal */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* (1) control: filename == internal name */
    ASSERT_TRUE(issue704_make_db(cache, "alpha704.db", "alpha704", "alphaFunc704"));

    /* (2) DRIFT: build beta704.db (internal "beta704") then rename the file to
     *     gamma704.db, so filename "gamma704" != internal "beta704". */
    ASSERT_TRUE(issue704_make_db(cache, "beta704.db", "beta704", "betaFunc704"));
    char beta_path[700];
    char gamma_path[700];
    snprintf(beta_path, sizeof(beta_path), "%s/beta704.db", cache);
    snprintf(gamma_path, sizeof(gamma_path), "%s/gamma704.db", cache);
    ASSERT_EQ(rename(beta_path, gamma_path), 0);

    /* (3) ghost: 0-byte db file */
    char ghost_path[700];
    snprintf(ghost_path, sizeof(ghost_path), "%s/ghost704.db", cache);
    FILE *gp = fopen(ghost_path, "w");
    ASSERT_NOT_NULL(gp);
    fclose(gp);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* ── A: list_projects reports INTERNAL names; filters the ghost ── */
    char *list =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(list);
    ASSERT_NOT_NULL(strstr(list, "alpha704")); /* control */
    ASSERT_NOT_NULL(strstr(list, "beta704"));  /* internal name of drifted db (RED before) */
    ASSERT_NULL(strstr(list, "gamma704"));     /* filename must NOT be advertised (RED before) */
    ASSERT_NULL(strstr(list, "ghost704"));     /* 0-byte ghost filtered (RED before) */
    free(list);

    /* ── B: the drifted project resolves by its INTERNAL name ──────── */
    char *q_beta = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"beta704\",\"name_pattern\":\"betaFunc704\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_beta);
    ASSERT_NOT_NULL(strstr(q_beta, "betaFunc704")); /* resolved + returned node (RED before) */
    ASSERT_NULL(strstr(q_beta, "not found"));       /* not the not-found error */
    free(q_beta);

    /* ── C: control project still resolves on the fast path ────────── */
    char *q_alpha = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"alpha704\",\"name_pattern\":\"alphaFunc704\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_alpha);
    ASSERT_NOT_NULL(strstr(q_alpha, "alphaFunc704"));
    free(q_alpha);

    /* ── D: the 0-byte ghost is NOT resolvable ─────────────────────── */
    char *q_ghost = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"ghost704\",\"name_pattern\":\".*\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_ghost);
    ASSERT_NOT_NULL(strstr(q_ghost, "not found"));
    free(q_ghost);

    /* ── E: addressing the drifted db by its FILENAME stays not-found ── */
    char *q_gamma = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"gamma704\",\"name_pattern\":\".*\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q_gamma);
    ASSERT_NOT_NULL(strstr(q_gamma, "not found"));
    free(q_gamma);

    cbm_mcp_server_free(srv);

    /* ── cleanup ───────────────────────────────────────────────────── */
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    char a_path[700];
    snprintf(a_path, sizeof(a_path), "%s/alpha704.db", cache);
    cbm_unlink(a_path);
    cbm_unlink(gamma_path);
    cbm_unlink(ghost_path);
    mcp_cleanup_corrupt_backups(cache, "ghost704");
    char side[740];
    snprintf(side, sizeof(side), "%s-wal", a_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-shm", a_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-wal", gamma_path);
    cbm_unlink(side);
    snprintf(side, sizeof(side), "%s-shm", gamma_path);
    cbm_unlink(side);
    cbm_rmdir(cache);
    PASS();
}

/* ── #1044: a "<name>::missed" shadow row must not hide the project ──
 *
 * The miss-graph pass inserts a second `projects` row ("<name>::missed") so
 * its nodes satisfy the FK on nodes.project. db_internal_project_name
 * required the projects table to hold EXACTLY ONE row, so any project with
 * a miss graph vanished from list_projects and the graph UI, and the
 * fallback-scan resolve path failed.
 *
 * RED on buggy code / GREEN on the fix:
 *   A. list_projects still advertises "delta1044" while the shadow row exists.
 *   B. the shadow name itself is never advertised.
 *   C. search_graph(project="delta1044") still resolves and returns the node.
 */
TEST(tool_list_projects_ignores_missed_shadow_issue1044) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-issue1044-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        PASS(); /* skip if mkdtemp fails — not a #1044 signal */
    }

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    ASSERT_TRUE(issue704_make_db(cache, "delta1044.db", "delta1044", "deltaFunc1044"));

    /* Add the shadow row exactly the way the miss-graph pass does. */
    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/delta1044.db", cache);
    cbm_store_t *st = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(st);
    ASSERT_EQ(cbm_store_upsert_project(st, "delta1044::missed", ""), CBM_STORE_OK);
    cbm_store_close(st);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);

    /* ── A + B: primary advertised, shadow hidden ─────────────────── */
    char *list =
        cbm_mcp_server_handle(srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                                   "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}");
    ASSERT_NOT_NULL(list);
    ASSERT_NOT_NULL(strstr(list, "delta1044")); /* RED before: db skipped as ghost */
    ASSERT_NULL(strstr(list, "::missed"));      /* shadow never advertised */
    free(list);

    /* ── C: the project still resolves and returns its node ───────── */
    char *q = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"search_graph\",\"arguments\":{"
             "\"project\":\"delta1044\",\"name_pattern\":\"deltaFunc1044\",\"limit\":5}}}");
    ASSERT_NOT_NULL(q);
    ASSERT_NOT_NULL(strstr(q, "deltaFunc1044"));
    ASSERT_NULL(strstr(q, "not found"));
    free(q);

    cbm_mcp_server_free(srv);

    /* ── cleanup ───────────────────────────────────────────────────── */
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    cbm_unlink(db_path);
    char side1044[740];
    snprintf(side1044, sizeof(side1044), "%s-wal", db_path);
    cbm_unlink(side1044);
    snprintf(side1044, sizeof(side1044), "%s-shm", db_path);
    cbm_unlink(side1044);
    cbm_rmdir(cache);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  QUERY STORE COHERENCE + READ-ONLY  (data-integrity reproductions)
 *
 *  Bug: query tools resolve the project store via resolve_store() ->
 *  cbm_store_open_path_query(), which opens the DB SQLITE_OPEN_READWRITE
 *  and runs configure_pragmas() with the WRITE pragmas
 *  (journal_mode=WAL + wal_checkpoint + synchronous). Two consequences:
 *    (a) read-only query tools MUTATE the on-disk DB (write pragmas), and
 *    (b) query tools FAIL outright on a read-only DB file / filesystem
 *        (the READWRITE open returns CANTOPEN -> resolve_store NULL ->
 *        "project not found").
 *  Both read-only tests below are written reproduce-first and are RED on the
 *  unfixed code, GREEN once query opens are READONLY with read-only
 *  pragmas.
 * ══════════════════════════════════════════════════════════════════ */

/* Reproduce-first: one MCP session caches a query connection to generation A,
 * then the fixture models an independent writer publishing generation B by
 * atomically replacing the project DB at the same cache path. Because
 * resolve_store() keys its cache only by project name, the next query can reuse
 * stale generation A. It must instead return generation B. */
TEST(query_store_reopens_after_database_replacement) {
    static const char project[] = "cbm-store-generation-refresh";
    static const char active_filename[] = "cbm-store-generation-refresh.db";
    static const char staged_filename[] = "cbm-store-generation-next.db";

    char cache[512];
    snprintf(cache, sizeof(cache), "%s/cbm-store-generation-XXXXXX", cbm_tmpdir());
    bool cache_ready = cbm_mkdtemp(cache) != NULL;
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    if (cache_ready) {
        cbm_setenv("CBM_CACHE_DIR", cache, 1);
    }

    bool generation_a_ready =
        cache_ready && issue704_make_db(cache, active_filename, project, "GenerationA");
    cbm_mcp_server_t *srv = generation_a_ready ? cbm_mcp_server_new(NULL) : NULL;
    bool server_ready = srv != NULL;

    char args[512];
    snprintf(args, sizeof(args),
             "{\"project\":\"%s\",\"name_pattern\":\".*Generation.*\",\"limit\":10}", project);
    char *before = srv ? cbm_mcp_handle_tool(srv, "search_graph", args) : NULL;
    bool saw_generation_a = before && strstr(before, "GenerationA") != NULL;

    bool generation_b_ready =
        cache_ready && issue704_make_db(cache, staged_filename, project, "GenerationB");
    char active_path[700];
    char staged_path[700];
    snprintf(active_path, sizeof(active_path), "%s/%s", cache, active_filename);
    snprintf(staged_path, sizeof(staged_path), "%s/%s", cache, staged_filename);
    bool replaced = generation_b_ready && cbm_rename_replace(staged_path, active_path) == 0;

    char *after = (srv && replaced) ? cbm_mcp_handle_tool(srv, "search_graph", args) : NULL;
    bool saw_generation_b = after && strstr(after, "GenerationB") != NULL;
    bool retained_generation_a = after && strstr(after, "GenerationA") != NULL;

    free(before);
    free(after);
    if (srv) {
        cbm_mcp_server_free(srv);
    }
    if (cache_ready) {
        cleanup_project_db(cache, project);
        cleanup_project_db(cache, "cbm-store-generation-next");
        cbm_rmdir(cache);
    }
    restore_cache_dir(saved_copy);
    free(saved_copy);

    ASSERT_TRUE(cache_ready);
    ASSERT_TRUE(generation_a_ready);
    ASSERT_TRUE(server_ready);
    ASSERT_TRUE(saw_generation_a);
    ASSERT_TRUE(generation_b_ready);
    ASSERT_TRUE(replaced);
    ASSERT_TRUE(saw_generation_b);
    ASSERT_FALSE(retained_generation_a);
    PASS();
}

#define ROQ_PROJECT "cbm-roq-test"

/* Whole-file byte snapshot. Returns malloc'd buffer (caller frees) and
 * writes the length to *out_len. Returns NULL on failure. */
static unsigned char *roq_read_file_bytes(const char *path, long *out_len) {
    *out_len = 0;
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long sz = ftell(fp);
    if (sz < 0) {
        fclose(fp);
        return NULL;
    }
    rewind(fp);
    unsigned char *buf = malloc((size_t)sz > 0 ? (size_t)sz : 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)sz, fp);
    fclose(fp);
    if (got != (size_t)sz) {
        free(buf);
        return NULL;
    }
    *out_len = sz;
    return buf;
}

static int roq_file_exists(const char *path) {
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

/* ── (a) NO-MUTATION ──────────────────────────────────────────────────
 *
 * readonly_query_does_not_mutate_db
 *
 * Create a real project DB, convert it to rollback (DELETE) journal mode
 * on disk, snapshot its exact bytes, run search_graph through the server,
 * then re-snapshot. The buggy query path runs `PRAGMA journal_mode=WAL`,
 * which rewrites the file header (1,1 -> 2,2) and spawns a -wal sidecar —
 * so the snapshots differ. The fixed READONLY path runs no write pragma,
 * so the file is byte-identical.
 *
 * The DELETE-mode fixture is what makes the mutation OBSERVABLE: on an
 * already-WAL file `journal_mode=WAL` is a silent no-op, so we deliberately
 * stage the DB in rollback mode (the same technique repro_issue557 uses to
 * plant a deterministic trigger).
 *
 * WHY RED on unfixed code:
 *   journal_mode=WAL rewrites the header -> memcmp(before, after) != 0 and
 *   a -wal file is created while the cached store is open. Both assertions
 *   that demand "unchanged" fire.
 * ─────────────────────────────────────────────────────────────────── */
TEST(readonly_query_does_not_mutate_db) {
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "%s/cbm_roq_a_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL); /* setup failure */
    }
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", tmp_cache, 1);

    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", tmp_cache, ROQ_PROJECT);
    char wal_path[730];
    char shm_path[730];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);

    /* Build the DB and flip it to rollback journal mode on disk. */
    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, ROQ_PROJECT, "/tmp/roq"), CBM_STORE_OK);
    cbm_node_t node = {.project = ROQ_PROJECT,
                       .label = "Function",
                       .name = "ReadOnlyProbe",
                       .qualified_name = "roq.mod.ReadOnlyProbe",
                       .file_path = "mod.c"};
    ASSERT_TRUE(cbm_store_upsert_node(setup, &node) > 0);
    ASSERT_EQ(cbm_store_exec(setup, "PRAGMA journal_mode=DELETE;"), 0);
    cbm_store_close(setup);

    /* Snapshot BEFORE any query. */
    long before_len = 0;
    unsigned char *before = roq_read_file_bytes(db_path, &before_len);
    ASSERT_NOT_NULL(before);

    /* Run a query tool through the server (the resolve_store path). */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[512];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"name_pattern\":\".*ReadOnlyProbe.*\"}",
             ROQ_PROJECT);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph", args);

    /* Capture sidecar state WHILE the cached store is still open (the buggy
     * RW+WAL open creates -wal here; on close it would be removed again). */
    int wal_while_open = roq_file_exists(wal_path);
    int query_ok = (resp && strstr(resp, "ReadOnlyProbe") != NULL);
    int query_failed = (resp && (strstr(resp, "not found") || strstr(resp, "not indexed")));

    cbm_mcp_server_free(srv); /* closes the store; header change is persisted */

    long after_len = 0;
    unsigned char *after = roq_read_file_bytes(db_path, &after_len);

    int identical = (before && after && before_len == after_len &&
                     memcmp(before, after, (size_t)before_len) == 0);

    if (resp) {
        free(resp);
    }
    free(before);
    free(after);
    cbm_unlink(db_path);
    cbm_unlink(wal_path);
    cbm_unlink(shm_path);
    cbm_rmdir(tmp_cache);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }

    ASSERT_TRUE(query_ok);        /* read path ran and returned the node */
    ASSERT_FALSE(query_failed);   /* not the "project not found" path */
    ASSERT_TRUE(identical);       /* RED on buggy code: WAL pragma rewrote header */
    ASSERT_FALSE(wal_while_open); /* RED on buggy code: RW+WAL open spawned -wal */
    PASS();
}

/* ── (b) READ-ONLY FILESYSTEM ─────────────────────────────────────────
 *
 * readonly_query_succeeds_on_readonly_fs
 *
 * Create a real project DB (left in WAL journal mode, as the indexer
 * writes it), then chmod the CONTAINING DIRECTORY to 0555 (read-only) to
 * simulate a read-only mount / immutable media, then run search_graph.
 *
 * Note on why the directory (not just the file) must be read-only: SQLite's
 * unix VFS auto-downgrades a failed O_RDWR main-db open to O_RDONLY, so a
 * 0444 *file* alone does NOT surface the bug — the connection silently
 * becomes read-only and, with a writable dir, still creates the WAL -shm
 * and reads. The genuine read-only-FS symptom is the WAL write-pragma
 * (journal_mode=WAL) being unable to create the -shm/-wal sidecars in a
 * read-only directory.
 *
 * WHY RED on unfixed code:
 *   cbm_store_open_path_query() runs configure_pragmas(.., false) which
 *   executes `PRAGMA journal_mode = WAL`. In a read-only directory the WAL
 *   wal-index (-shm) cannot be created, so the pragma errors ->
 *   configure_pragmas fails -> the open returns NULL -> resolve_store()
 *   returns NULL -> the handler emits "project not found or not indexed".
 *
 * GREEN on fixed code:
 *   the READONLY open skips the WAL write-pragma; the plain READONLY open
 *   of a WAL-mode DB in a read-only dir still needs -shm, so it fails and
 *   the immutable-URI fallback (file:..?immutable=1) reads the main DB
 *   file directly and the query returns the node. (This is the test that
 *   exercises the immutable fallback path.)
 * ─────────────────────────────────────────────────────────────────── */
TEST(readonly_query_succeeds_on_readonly_fs) {
    char tmp_cache[512];
    snprintf(tmp_cache, sizeof(tmp_cache), "%s/cbm_roq_b_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(tmp_cache)) {
        ASSERT_NOT_NULL(NULL); /* setup failure */
    }
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", tmp_cache, 1);

    char db_path[700];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", tmp_cache, ROQ_PROJECT);
    char wal_path[730];
    char shm_path[730];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);

    /* Build the DB in its natural WAL journal mode and ensure it is cleanly
     * checkpointed (no -wal frames) so the immutable fallback can read all
     * data from the main file. */
    cbm_store_t *setup = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(setup);
    ASSERT_EQ(cbm_store_upsert_project(setup, ROQ_PROJECT, "/tmp/roq"), CBM_STORE_OK);
    cbm_node_t node = {.project = ROQ_PROJECT,
                       .label = "Function",
                       .name = "ReadOnlyProbe",
                       .qualified_name = "roq.mod.ReadOnlyProbe",
                       .file_path = "mod.c"};
    ASSERT_TRUE(cbm_store_upsert_node(setup, &node) > 0);
    (void)cbm_store_checkpoint(setup); /* fold WAL frames into the main file */
    cbm_store_close(setup);            /* clean close removes -wal/-shm */

    /* Make the containing directory read-only (simulate a read-only mount).
     * SQLite can still traverse + read files, but cannot create -shm/-wal. */
    ASSERT_EQ(chmod(tmp_cache, 0555), 0);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[512];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"name_pattern\":\".*ReadOnlyProbe.*\"}",
             ROQ_PROJECT);
    char *resp = cbm_mcp_handle_tool(srv, "search_graph", args);

    int query_ok = (resp && strstr(resp, "ReadOnlyProbe") != NULL);
    int query_failed = (resp && (strstr(resp, "not found") || strstr(resp, "not indexed")));

    if (resp) {
        free(resp);
    }
    cbm_mcp_server_free(srv);

    /* Restore write permission on the dir BEFORE unlink (cannot remove dir
     * entries while the directory is read-only). */
    chmod(tmp_cache, 0755);
    cbm_unlink(db_path);
    cbm_unlink(wal_path);
    cbm_unlink(shm_path);
    cbm_rmdir(tmp_cache);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }

    ASSERT_FALSE(query_failed); /* RED on buggy code: WAL pragma fails on RO dir */
    ASSERT_TRUE(query_ok);      /* RED on buggy code: no node returned */
    PASS();
}

#undef ROQ_PROJECT

/* ══════════════════════════════════════════════════════════════════
 *  #823 — CLI/supervised index_repository must preserve name override
 * ══════════════════════════════════════════════════════════════════ */

enum {
    IDX823_OK = 0,
    IDX823_NO_SERVER = 61,
    IDX823_NO_RESULT = 62,
    IDX823_NOT_INDEXED = 63,
    IDX823_RESPONSE_NAME_MISSING = 64,
    IDX823_LIST_NAME_MISSING = 65,
    IDX823_SEARCH_FAILED = 66,
    IDX823_PARENT_GUARD_USED = 67,
};

#ifndef _WIN32 /* helper used only by the POSIX fork harness below */
static int idx823_supervised_name_override_check(const char *repo_dir, const char *custom_name) {
    /* Match the real CLI/MCP server state: a marked host with the supervisor
     * enabled. The worker receives the same args JSON the CLI forwards. */
    cbm_index_supervisor_mark_host();
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    cbm_setenv("CBM_INDEX_MAX_RESTARTS", "1", 1);
    cbm_setenv("CBM_INDEX_WORKER_TIMEOUT_S", "30", 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return IDX823_NO_SERVER;
    }
    /* A supervised local index transfers project-lock ownership to the worker.
     * Denying the parent guard is therefore harmless and proves the parent did
     * not acquire a lease before spawning. RED on the former ordering, which
     * returned "blocked" without ever starting the worker. */
    mcp_mutation_guard_probe_t parent_guard = {.deny_begin_call = 1};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &parent_guard);

    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\",\"name\":\"%s\"}",
             repo_dir, custom_name);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    int code = IDX823_OK;
    if (parent_guard.begin_count != 0 || parent_guard.end_count != 0) {
        code = IDX823_PARENT_GUARD_USED;
    } else if (!resp) {
        code = IDX823_NO_RESULT;
    } else if (!response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        code = IDX823_NOT_INDEXED;
    } else {
        char expected[256];
        snprintf(expected, sizeof(expected), "\"project\":\"%s\"", custom_name);
        if (!response_contains_json_fragment(resp, expected)) {
            code = IDX823_RESPONSE_NAME_MISSING;
        }
    }
    free(resp);

    if (code == IDX823_OK) {
        char *projects = cbm_mcp_handle_tool(srv, "list_projects", "{}");
        char expected[256];
        snprintf(expected, sizeof(expected), "\"name\":\"%s\"", custom_name);
        if (!projects || !response_contains_json_fragment(projects, expected)) {
            code = IDX823_LIST_NAME_MISSING;
        }
        free(projects);
    }

    if (code == IDX823_OK) {
        char q[512];
        snprintf(q, sizeof(q),
                 "{\"project\":\"%s\",\"name_pattern\":\"idx823_fn\",\"label\":\"Function\"}",
                 custom_name);
        char *sr = cbm_mcp_handle_tool(srv, "search_graph", q);
        if (!sr || !strstr(sr, "idx823_fn")) {
            code = IDX823_SEARCH_FAILED;
        }
        free(sr);
    }

    cbm_mcp_server_free(srv);
    return code;
}
#endif

TEST(index_repository_cli_name_override_issue823) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX fork harness required to isolate supervisor host mark");
#else
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idx823-repo-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        FAIL("cbm_mkdtemp repo failed");
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx823-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        th_rmtree(tmp_dir);
        FAIL("cbm_mkdtemp cache failed");
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    ASSERT_EQ(th_write_file(src_path, "def idx823_fn():\n    return 823\n"), 0);

    const char *custom_name = "issue823-custom-project";
    int code = -1;
    bool signalled = false;
    int sig = 0;

    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60);
        _exit(idx823_supervised_name_override_check(tmp_dir, custom_name));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    char *path_project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, custom_name);
    cleanup_project_db(cache, path_project);
    free(path_project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    th_rmtree(cache);
    th_rmtree(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => worker hang)\n", sig);
    } else if (code != IDX823_OK) {
        printf("    child exit code %d (64=response name, 65=list name, "
               "66=search, 67=parent guard used)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX823_OK);
    PASS();
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  #845 — supervisor gate must not wrap embedders of cbm_mcp_handle_tool
 * ══════════════════════════════════════════════════════════════════ */

TEST(index_supervisor_unsafe_clean_is_never_fallback_or_recovery) {
    char response[] = "{\"status\":\"indexed\"}";
    cbm_index_worker_result_t result = {
        .outcome = CBM_PROC_CLEAN,
        .exit_code = 0,
        .tree_quiesced = true,
        .response = response,
    };
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(0, &result), CBM_MCP_SUPERVISED_RESULT_SUCCESS);

    result.cancellation_requested = true;
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(0, &result),
              CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL);
    result.cancellation_requested = false;
    result.tree_quiesced = false;
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(0, &result),
              CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL);
    result.tree_quiesced = true;
    result.supervision_failed = true;
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(0, &result),
              CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL);

    result.supervision_failed = false;
    result.outcome = CBM_PROC_CRASH;
    result.response = NULL;
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(0, &result),
              CBM_MCP_SUPERVISED_RESULT_CONTAINED_FAILURE);
    ASSERT_EQ(cbm_mcp_supervised_result_disposition(-1, &result),
              CBM_MCP_SUPERVISED_RESULT_FALLBACK);
    PASS();
}

/* Child-side check: index a tiny fixture and verify it ran IN-PROCESS.
 * Distinct exit codes so the parent can report the exact failure mode. */
enum {
    IDX845_OK = 0,
    IDX845_SPAWNED = 41,     /* a worker subprocess was spawned — the #845 bug */
    IDX845_NO_RESULT = 42,   /* handle_tool returned NULL */
    IDX845_NOT_INDEXED = 43, /* response lacks status=indexed */
};

static int idx845_index_inprocess_check(const char *repo_dir) {
    int spawns_before = cbm_index_supervisor_spawn_count();

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return IDX845_NO_RESULT;
    }
    char args[1024];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", repo_dir);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);

    int code = IDX845_OK;
    if (cbm_index_supervisor_spawn_count() != spawns_before) {
        code = IDX845_SPAWNED;
    } else if (!resp) {
        code = IDX845_NO_RESULT;
    } else if (!response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        code = IDX845_NOT_INDEXED;
    }
    free(resp);
    cbm_mcp_server_free(srv);
    return code;
}

TEST(index_supervisor_gate_requires_marked_host_issue845) {
    /* #845: index_repository via cbm_mcp_handle_tool from an EMBEDDER (this test
     * binary) must index IN-PROCESS even with CBM_INDEX_SUPERVISOR unset. The
     * supervisor gate may only wrap a process that called
     * cbm_index_supervisor_mark_host() — i.e. the real binary's main(). Before
     * the fix, should_wrap() was true for ANY embedder: the gate resolved the
     * CURRENT binary (this test runner!) and spawned
     * '<test-runner> cli --index-worker --index-worker-build …', which a test binary
     * interprets as suite-filter args → it re-runs test suites in the child →
     * recursive spawn chains (observed 11-min hangs; kernel VM-map load during
     * the 2026-07-04 host panics).
     *
     * POSIX: run the call in a forked child under alarm(20) so the pre-fix
     * recursive behaviour cannot hang the runner; the child reports via exit
     * code. Windows: no fork — run in-process (safe once the gate is fixed; the
     * pre-fix redness is demonstrated on POSIX). */
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idx845-repo-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx845-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* The point of the guard: NO kill switch. The gate itself must keep an
     * unmarked host in-process. Save + restore the ambient value. */
    const char *saved_sv = getenv("CBM_INDEX_SUPERVISOR");
    char *saved_sv_copy = saved_sv ? strdup(saved_sv) : NULL;
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def main():\n    return 'ok'\n", fp);
    fclose(fp);

    int code = -1;
    bool signalled = false;
    int sig = 0;
#ifdef _WIN32
    code = idx845_index_inprocess_check(tmp_dir);
#else
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(20); /* pre-fix spawn chain must die here, not hang the runner */
        _exit(idx845_index_inprocess_check(tmp_dir));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }
#endif

    /* Restore env BEFORE asserting so a red run doesn't leak state. */
    if (saved_sv_copy) {
        cbm_setenv("CBM_INDEX_SUPERVISOR", saved_sv_copy, 1);
        free(saved_sv_copy);
    } else {
        cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    }
    char *project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => recursive spawn chain hang)\n", sig);
    } else if (code != IDX845_OK) {
        printf("    child exit code %d (41=worker spawned, 42=no result, 43=not indexed)\n", code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX845_OK);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  Mandatory supervision must fail closed in real CBM hosts
 * ══════════════════════════════════════════════════════════════════ */

/* A real CBM host must never turn a supervisor refusal into permission to run
 * the native index pipeline in its own long-lived process. The legacy
 * CBM_INDEX_SUPERVISOR=0 switch is a deterministic start-failure seam here: on
 * the buggy path should_wrap() returned false, the parent mutation guard ran,
 * and the project DB was written in-process. The fixed path keeps supervision
 * mandatory, returns an error, and leaves both the guard and filesystem
 * untouched. Host marking is process-lifetime state, so isolate it in a clean
 * re-exec. posix_spawn stays reliable after earlier tests created threads,
 * whereas a late raw fork can fail transiently under sanitizers on macOS. */
enum {
    IDXFAILCLOSED_OK = 0,
    IDXFAILCLOSED_NO_SERVER = 81,
    IDXFAILCLOSED_PARENT_MUTATED = 82,
    IDXFAILCLOSED_NO_RESPONSE = 83,
    IDXFAILCLOSED_INDEXED = 84,
    IDXFAILCLOSED_NOT_ERROR = 85,
};

#ifndef _WIN32
int mcp_test_idxfailclosed_supervisor_start_check(const char *repo_dir, const char *cache_dir) {
    (void)cbm_setenv("CBM_CACHE_DIR", cache_dir, 1);
    cbm_index_supervisor_mark_host();
    (void)cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return IDXFAILCLOSED_NO_SERVER;
    }
    mcp_mutation_guard_probe_t parent_guard = {0};
    cbm_mcp_server_set_project_mutation_guard(srv, mcp_mutation_guard_probe_begin,
                                              mcp_mutation_guard_probe_end, &parent_guard);

    char args[CBM_SZ_4K];
    (void)snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", repo_dir);
    char *response = cbm_mcp_handle_tool(srv, "index_repository", args);

    int result = IDXFAILCLOSED_OK;
    if (parent_guard.begin_count != 0 || parent_guard.end_count != 0) {
        result = IDXFAILCLOSED_PARENT_MUTATED;
    } else if (!response) {
        result = IDXFAILCLOSED_NO_RESPONSE;
    } else if (response_contains_json_fragment(response, "\"status\":\"indexed\"")) {
        result = IDXFAILCLOSED_INDEXED;
    } else if (!response_contains_json_fragment(response, "\"status\":\"error\"") ||
               !response_contains_json_fragment(response, "\"outcome\":\"spawn_failed\"")) {
        result = IDXFAILCLOSED_NOT_ERROR;
    }

    free(response);
    cbm_mcp_server_free(srv);
    return result;
}

static bool idxfailclosed_self_path(char out[CBM_SZ_4K]) {
#ifdef __APPLE__
    int length = proc_pidpath(getpid(), out, CBM_SZ_4K);
    bool resolved = length > 0 && length < CBM_SZ_4K;
    if (resolved) {
        out[length] = '\0';
    }
    return resolved;
#elif defined(__linux__)
    ssize_t length = readlink("/proc/self/exe", out, CBM_SZ_4K - 1);
    bool resolved = length > 0 && length < (ssize_t)CBM_SZ_4K - 1;
    if (resolved) {
        out[length] = '\0';
    }
    return resolved;
#else
    (void)out;
    return false;
#endif
}
#endif

TEST(index_supervisor_start_failure_is_fail_closed_in_real_host) {
#ifdef _WIN32
    SKIP_PLATFORM("immutable host mark needs fork isolation (POSIX-only)");
#else
    char repo_dir[CBM_SZ_1K];
    char cache_dir[CBM_SZ_1K];
    (void)snprintf(repo_dir, sizeof(repo_dir), "%s/cbm-idx-failclosed-repo-XXXXXX", cbm_tmpdir());
    (void)snprintf(cache_dir, sizeof(cache_dir), "%s/cbm-idx-failclosed-cache-XXXXXX",
                   cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(repo_dir));
    ASSERT_NOT_NULL(cbm_mkdtemp(cache_dir));

    char source_path[CBM_SZ_4K];
    (void)snprintf(source_path, sizeof(source_path), "%s/should_not_index.py", repo_dir);
    FILE *source = cbm_fopen(source_path, "wb");
    ASSERT_NOT_NULL(source);
    ASSERT_TRUE(fputs("def should_not_index():\n    return True\n", source) >= 0);
    ASSERT_EQ(fclose(source), 0);

    char *project = cbm_project_name_from_path(repo_dir);
    ASSERT_NOT_NULL(project);
    char db_path[CBM_SZ_4K];
    (void)snprintf(db_path, sizeof(db_path), "%s/%s.db", cache_dir, project);

    char self_path[CBM_SZ_4K] = {0};
    ASSERT_TRUE(idxfailclosed_self_path(self_path));
    char *const child_argv[] = {
        self_path, "__cbm_mcp_idxfailclosed_probe", repo_dir, cache_dir, NULL,
    };
    (void)fflush(NULL);
    pid_t child = -1;
    ASSERT_EQ(posix_spawn(&child, self_path, NULL, NULL, child_argv, environ), 0);
    ASSERT_TRUE(child > 0);
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    bool exited = WIFEXITED(status);
    int child_result = exited ? WEXITSTATUS(status) : -1;
    bool database_absent = !cbm_file_exists(db_path);

    cleanup_project_db(cache_dir, project);
    free(project);
    (void)cbm_unlink(source_path);
    (void)th_rmtree(repo_dir);
    (void)th_rmtree(cache_dir);

    ASSERT_TRUE(exited);
    ASSERT_EQ(child_result, IDXFAILCLOSED_OK);
    ASSERT_TRUE(database_absent);
    PASS();
#endif
}

/* ═══════════════════════════════════════════════════════════
 *  #832 — background auto-index + watcher re-index must run in the
 *         supervised worker SUBPROCESS (RSS isolation)
 * ══════════════════════════════════════════════════════════ */

/* The long-lived server ran the full index pipeline in-process on two background
 * paths (session auto-index in mcp.c, watcher re-index in main.c). Worker-thread
 * mimalloc heaps abandon pages at thread exit and mimalloc v3
 * (page_reclaim_on_free=0) does not reclaim them when the main thread later frees
 * their blocks, so RSS ratchets across re-index cycles (#832). The fix routes both
 * paths through cbm_mcp_index_run_supervised_path() — the SAME supervised worker
 * subprocess the index_repository tool uses — so the child hands 100%% of its RSS
 * back to the OS on exit.
 *
 * This guard proves the ROUTING: on a supervisor-marked host with the kill switch
 * OFF, the shared entry the watcher/auto-index now call must (a) spawn a worker
 * child (cbm_index_supervisor_spawn_count() increases) and (b) actually index the
 * fixture (the worker child writes the Function node). RED on the unfixed
 * in-process routing: it calls cbm_pipeline_run directly, so spawn_count is
 * unchanged → IDX832_NO_SPAWN. */
enum {
    IDX832_OK = 0,
    IDX832_NO_SPAWN = 51,    /* spawn_count unchanged — routed in-process (RED) */
    IDX832_NULL_RESP = 52,   /* supervised entry degraded to NULL */
    IDX832_NOT_INDEXED = 53, /* response/store lacks the indexed Function node */
    IDX832_SERVER_FAIL = 54,
};

#ifndef _WIN32 /* helper used only by the POSIX fork harness below */
static int idx832_supervised_route_check(const char *repo_dir) {
    /* Become a supervisor host with the kill switch OFF — exactly the real MCP
     * server's state. Done in the FORKED CHILD only (see the harness) so the
     * parent test-runner's process-wide host mark stays clear and the #845
     * unmarked-embedder guard is unaffected. Bound the recovery loop + worker
     * quiet-timeout so a stuck child cannot run long under the fork+alarm net. */
    cbm_index_supervisor_mark_host();
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    cbm_setenv("CBM_INDEX_MAX_RESTARTS", "1", 1);
    cbm_setenv("CBM_INDEX_WORKER_TIMEOUT_S", "30", 1);

    int spawns_before = cbm_index_supervisor_spawn_count();
    char *resp = cbm_mcp_index_run_supervised_path(repo_dir);
    int spawns_after = cbm_index_supervisor_spawn_count();

    if (spawns_after == spawns_before) {
        free(resp);
        return IDX832_NO_SPAWN; /* the discriminating assertion: RED in-process */
    }
    if (!resp) {
        return IDX832_NULL_RESP;
    }
    bool indexed = response_contains_json_fragment(resp, "\"status\":\"indexed\"");
    free(resp);
    if (!indexed) {
        return IDX832_NOT_INDEXED;
    }

    /* Store-level proof the worker child did real work: the Function node it wrote
     * must be queryable from a fresh server reading the DB the child produced. */
    char *project = cbm_project_name_from_path(repo_dir);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        free(project);
        return IDX832_SERVER_FAIL;
    }
    int code = IDX832_OK;
    if (project) {
        char q[512];
        snprintf(q, sizeof(q),
                 "{\"project\":\"%s\",\"name_pattern\":\"idx832_fn\",\"label\":\"Function\"}",
                 project);
        char *sr = cbm_mcp_handle_tool(srv, "search_graph", q);
        if (!sr || !strstr(sr, "idx832_fn")) {
            code = IDX832_NOT_INDEXED;
        }
        free(sr);
    }
    cbm_mcp_server_free(srv);
    free(project);
    return code;
}
#endif /* !_WIN32 */

TEST(index_bg_paths_route_through_supervisor_issue832) {
#ifdef _WIN32
    /* The guard marks the process as a supervisor host, which cannot be undone.
     * POSIX isolates that in a forked child; without fork we would pollute the
     * shared test-runner (breaking the #845 unmarked-embedder guard). The routing
     * logic is platform-independent and covered on POSIX CI; Windows containment
     * is covered by the end-to-end crash-containment test. */
    SKIP_PLATFORM("supervisor-host guard needs fork isolation (POSIX-only)");
#else
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idx832-repo-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx832-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1); /* inherited by the worker child */

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def idx832_fn():\n    return 'ok'\n", fp);
    fclose(fp);

    int code = -1;
    bool signalled = false;
    int sig = 0;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60); /* a stuck worker dies here instead of hanging the runner */
        _exit(idx832_supervised_route_check(tmp_dir));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    char *project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => worker hang)\n", sig);
    } else if (code != IDX832_OK) {
        printf("    child exit code %d (51=no spawn/in-process=RED, 52=null resp, "
               "53=not indexed, 54=server fail)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX832_OK);
    PASS();
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  Parallel-only crash recovery (ms-typescript cascade fix)
 * ══════════════════════════════════════════════════════════════════ */

/* The old recovery loop re-ran the worker SINGLE-THREADED to keep one exact
 * crash marker. At scale that fell into the sequential crawl, was killed as
 * a hang mid-pass, and the stale marker quarantined FOUR innocent
 * ms-typescript fixtures, one 15-minute retry at a time. The reworked loop
 * re-runs PARALLEL with a marker journal; a file is quarantined only when
 * it is in-flight across two consecutive failed runs.
 *
 * This guard proves the CONTRACT: with an injected crasher among good
 * files, the supervised index must (a) never spawn a single-threaded worker
 * (cbm_index_supervisor_spawn_st_count stays 0 — RED on the old loop),
 * (b) quarantine exactly the crasher, (c) leave the innocents indexed and
 * NOT quarantined. */
enum {
    IDXPAR_OK = 0,
    IDXPAR_ST_SPAWN = 61,      /* single-threaded recovery spawn happened (RED) */
    IDXPAR_NULL_RESP = 62,     /* supervised entry degraded to NULL */
    IDXPAR_NOT_INDEXED = 63,   /* response lacks status indexed */
    IDXPAR_NO_QUARANTINE = 64, /* crasher missing from skipped[] */
    IDXPAR_INNOCENT_HIT = 65,  /* a good file was quarantined/skipped */
    IDXPAR_GOOD_MISSING = 66,  /* good file's Function absent from the store */
};

#ifndef _WIN32
static int idxpar_recovery_check(const char *repo_dir) {
    cbm_index_supervisor_mark_host();
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    /* Rounds needed: fail+record, fail+quarantine, clean. Generous cap. */
    cbm_setenv("CBM_INDEX_MAX_RESTARTS", "5", 1);
    cbm_setenv("CBM_INDEX_WORKER_TIMEOUT_S", "30", 1);
    cbm_setenv("CBM_TEST_CRASH_ON", "idxpar_crasher", 1);

    int st_before = cbm_index_supervisor_spawn_st_count();
    char *resp = cbm_mcp_index_run_supervised_path(repo_dir);
    int st_after = cbm_index_supervisor_spawn_st_count();
    cbm_unsetenv("CBM_TEST_CRASH_ON");

    if (st_after != st_before) {
        free(resp);
        return IDXPAR_ST_SPAWN; /* discriminating assertion: RED on the old loop */
    }
    if (!resp) {
        return IDXPAR_NULL_RESP;
    }
    bool indexed = response_contains_json_fragment(resp, "\"status\":\"indexed\"");
    bool crasher_skipped = strstr(resp, "idxpar_crasher.py") != NULL;
    bool innocent_hit =
        strstr(resp, "idxpar_good_a.py") != NULL || strstr(resp, "idxpar_good_b.py") != NULL;
    free(resp);
    if (!indexed) {
        return IDXPAR_NOT_INDEXED;
    }
    if (!crasher_skipped) {
        return IDXPAR_NO_QUARANTINE;
    }
    if (innocent_hit) {
        return IDXPAR_INNOCENT_HIT;
    }

    /* Store proof: an innocent's Function node exists. */
    char *project = cbm_project_name_from_path(repo_dir);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    int code = IDXPAR_OK;
    if (srv && project) {
        char q[512];
        snprintf(q, sizeof(q),
                 "{\"project\":\"%s\",\"name_pattern\":\"idxpar_good_fn\",\"label\":\"Function\"}",
                 project);
        char *sr = cbm_mcp_handle_tool(srv, "search_graph", q);
        if (!sr || !strstr(sr, "idxpar_good_fn")) {
            code = IDXPAR_GOOD_MISSING;
        }
        free(sr);
    }
    if (srv) {
        cbm_mcp_server_free(srv);
    }
    free(project);
    return code;
}
#endif /* !_WIN32 */

/* #773: SIGABRT (invalid free in ts_stack_delete via
 * cbm_destroy_thread_parser) on the SECOND index_repository in one server
 * process, once both repos take the PARALLEL path (~30+ files). The
 * supervisor masks this on the default MCP path (fresh worker process per
 * index); the in-process pipeline — CBM_INDEX_SUPERVISOR=0, and every
 * embedded/test consumer — dies. Forked child so the abort cannot kill the
 * runner; ASan legs print the exact bad free. */
enum {
    IDX773_OK = 0,
    IDX773_FIRST_FAILED = 71,  /* first index didn't return indexed */
    IDX773_SECOND_FAILED = 72, /* second index didn't return indexed */
};

#ifndef _WIN32
static void idx773_write_py_repo(const char *dir, int files, int variant) {
    for (int i = 0; i < files; i++) {
        char path[CBM_SZ_512];
        snprintf(path, sizeof(path), "%s/mod_%d_%03d.py", dir, variant, i);
        FILE *f = fopen(path, "w");
        if (!f) {
            continue;
        }
        fprintf(f,
                "class Handler%d:\n"
                "    def run(self, x):\n"
                "        return self.helper(x) + %d\n"
                "    def helper(self, x):\n"
                "        for i in range(10):\n"
                "            x += i\n"
                "        return x\n"
                "\n"
                "def main_%d(x):\n"
                "    return Handler%d().run(x)\n",
                i, i, i, i);
        fclose(f);
    }
}

static int idx773_double_index_check(const char *dir_a, const char *dir_b) {
    cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return IDX773_FIRST_FAILED;
    }
    char args[CBM_SZ_512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"full\"}", dir_a);
    char *r1 = cbm_mcp_handle_tool(srv, "index_repository", args);
    bool ok1 = r1 && strstr(r1, "indexed") != NULL;
    free(r1);
    if (!ok1) {
        cbm_mcp_server_free(srv);
        return IDX773_FIRST_FAILED;
    }
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"full\"}", dir_b);
    char *r2 = cbm_mcp_handle_tool(srv, "index_repository", args); /* SIGABRT here (RED) */
    bool ok2 = r2 && strstr(r2, "indexed") != NULL;
    free(r2);
    cbm_mcp_server_free(srv);
    return ok2 ? IDX773_OK : IDX773_SECOND_FAILED;
}
#endif /* !_WIN32 */

/* #898: the SEQUENTIAL pipeline emitted malformed JSON for brokered
 * ASYNC_CALLS edges ("broker":"bullmq} — missing closing quote) and stored
 * the RAW broker/method string as the synthesized Route node's properties
 * (literally `bullmq` instead of {"broker":"bullmq"}). json_extract over
 * those rows errors, generated-column indexes fail, and PRAGMA quick_check
 * aborts with "malformed JSON" — which since the artifact deep-integrity
 * check also means such caches are refused at import. The parallel path
 * was correct; both pipelines must emit identical, valid JSON. */
TEST(sequential_service_edge_props_are_valid_json_issue898) {
    char tmp[CBM_SZ_256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_seq898_XXXXXX");
    if (!cbm_mkdtemp(tmp)) {
        FAIL("mkdtemp failed");
    }
    char cache[CBM_SZ_256];
    snprintf(cache, sizeof(cache), "/tmp/cbm_seq898_cache_XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp);
        FAIL("cache mkdtemp failed");
    }
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? cbm_strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char src_path[CBM_SZ_512];
    snprintf(src_path, sizeof(src_path), "%s/queue.py", tmp);
    FILE *f = fopen(src_path, "w");
    ASSERT_NOT_NULL(f);
    /* celery.Celery("tasks") resolves through the import map to a QN the
     * service-pattern table classifies as ASYNC with broker "celery". */
    fputs("import celery\n"
          "\n"
          "def enqueue():\n"
          "    celery.Celery(\"tasks\")\n",
          f);
    fclose(f);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    char args[CBM_SZ_512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", tmp);
    char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "indexed"));
    free(resp);

    /* File-backed MCP stores are deliberately request-scoped so a sibling
     * process can atomically replace the DB generation (and so Windows does
     * not retain a replacement-blocking handle). Inspect the published DB
     * through an independent query handle instead of relying on srv->store. */
    char *project = cbm_project_name_from_path(tmp);
    ASSERT_NOT_NULL(project);
    char db_path[CBM_SZ_512];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    ASSERT_NOT_NULL(store);
    struct sqlite3 *db = cbm_store_get_db(store);
    ASSERT_NOT_NULL(db);

    /* Non-vacuous: the fixture must actually produce a brokered edge. */
    sqlite3_stmt *stmt = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(db, "SELECT count(*) FROM edges WHERE type='ASYNC_CALLS';", -1,
                                 &stmt, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int async_edges = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_TRUE(async_edges >= 1);

    /* THE BUG: malformed properties on edges (broker quote) and Route nodes
     * (raw string). Every properties blob must be valid JSON. */
    ASSERT_EQ(sqlite3_prepare_v2(db,
                                 "SELECT count(*) FROM edges WHERE properties IS NOT NULL "
                                 "AND properties != '' AND json_valid(properties)=0;",
                                 -1, &stmt, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int bad_edges = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(bad_edges, 0);

    ASSERT_EQ(sqlite3_prepare_v2(db,
                                 "SELECT count(*) FROM nodes WHERE properties IS NOT NULL "
                                 "AND properties != '' AND json_valid(properties)=0;",
                                 -1, &stmt, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int bad_nodes = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_EQ(bad_nodes, 0);

    /* Pipeline parity: the broker must be extractable exactly like the
     * parallel path emits it. */
    ASSERT_EQ(sqlite3_prepare_v2(db,
                                 "SELECT count(*) FROM edges WHERE type='ASYNC_CALLS' AND "
                                 "json_extract(properties,'$.broker')='celery';",
                                 -1, &stmt, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    int brokered = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    ASSERT_TRUE(brokered >= 1);

    cbm_store_close(store);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    free(project);
    th_rmtree(cache);
    cbm_unlink(src_path);
    cbm_rmdir(tmp);
    PASS();
}

TEST(index_second_inprocess_run_survives_issue773) {
#ifdef _WIN32
    SKIP_PLATFORM("fork-isolated crash guard (POSIX-only)");
#else
    char dir_a[CBM_SZ_256];
    char dir_b[CBM_SZ_256];
    char cache[CBM_SZ_256];
    snprintf(dir_a, sizeof(dir_a), "/tmp/cbm-idx773a-XXXXXX");
    snprintf(dir_b, sizeof(dir_b), "/tmp/cbm-idx773b-XXXXXX");
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx773c-XXXXXX");
    if (!cbm_mkdtemp(dir_a) || !cbm_mkdtemp(dir_b) || !cbm_mkdtemp(cache)) {
        FAIL("mkdtemp failed");
    }
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? cbm_strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    /* Trigger shape: run 1 small enough for the SEQUENTIAL path (parses on
     * the calling thread, mimalloc epoch), run 2 large enough for the
     * PARALLEL path (switches the global ts allocator to the slab). */
    idx773_write_py_repo(dir_a, 5, 0);
    idx773_write_py_repo(dir_b, 60, 1);

    int code = -1;
    bool signalled = false;
    int sig = 0;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(180); /* generous: two full parallel indexes */
        _exit(idx773_double_index_check(dir_a, dir_b));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);

    if (signalled) {
        printf("    child killed by signal %d (SIGABRT = the #773 invalid free)\n", sig);
    } else if (code != IDX773_OK) {
        printf("    child exit code %d (71=first index failed, 72=second failed)\n", code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX773_OK);
    PASS();
#endif
}

TEST(index_recovery_parallel_quarantines_crasher) {
#ifdef _WIN32
    SKIP_PLATFORM("parallel-recovery guard needs fork isolation (POSIX-only)");
#else
    char tmp_dir[CBM_SZ_256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idxpar-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        FAIL("mkdtemp failed");
    }
    char cache[CBM_SZ_256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idxpar-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        FAIL("mkdtemp cache failed");
    }
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? cbm_strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char p1[CBM_SZ_512];
    char p2[CBM_SZ_512];
    char pc[CBM_SZ_512];
    snprintf(p1, sizeof(p1), "%s/idxpar_good_a.py", tmp_dir);
    snprintf(p2, sizeof(p2), "%s/idxpar_good_b.py", tmp_dir);
    snprintf(pc, sizeof(pc), "%s/idxpar_crasher.py", tmp_dir);
    FILE *f = fopen(p1, "w");
    ASSERT_NOT_NULL(f);
    fputs("def idxpar_good_fn():\n    return 'ok'\n", f);
    fclose(f);
    f = fopen(p2, "w");
    ASSERT_NOT_NULL(f);
    fputs("def idxpar_good_fn_b():\n    return 'ok'\n", f);
    fclose(f);
    f = fopen(pc, "w");
    ASSERT_NOT_NULL(f);
    fputs("def idxpar_crash_fn():\n    return 'boom'\n", f);
    fclose(f);

    int code = -1;
    bool signalled = false;
    int sig = 0;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(120); /* generous: three supervised rounds + clean run */
        _exit(idxpar_recovery_check(tmp_dir));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    char *project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    remove(p1);
    remove(p2);
    remove(pc);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => recovery loop hang)\n", sig);
    } else if (code != IDXPAR_OK) {
        printf("    child exit code %d (61=ST spawn/RED, 62=null resp, 63=not indexed, "
               "64=no quarantine, 65=innocent hit, 66=good missing)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDXPAR_OK);
    PASS();
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  AUTO_WATCH GATE  (distilled from PR #625)
 *
 *  Background watcher registration on session connect is gated by the
 *  `auto_watch` config key (default TRUE = existing behavior).
 * ══════════════════════════════════════════════════════════════════ */

/* Drive the already-indexed connect path (initialize → maybe_auto_index →
 * watcher registration) and return the resulting watch count.
 * auto_watch_value: NULL leaves the key unset (exercises the default),
 * otherwise the key is set to that value before initialize.
 * Returns a negative code on fixture setup failure. */
static int auto_watch_connect_watch_count(const char *auto_watch_value) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-autowatch-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        return -1;
    }

    char repodir[512];
    snprintf(repodir, sizeof(repodir), "%s/repo", cache);
    if (th_mkdir_p(repodir) != 0) {
        th_rmtree(cache);
        return -2;
    }

    /* Same derivation detect_session uses on the cwd — realpath-based, so
     * the name matches even where /tmp is a symlink (macOS). */
    char *project = cbm_project_name_from_path(repodir);
    if (!project) {
        th_rmtree(cache);
        return -3;
    }

    /* Pre-create <cache>/<project>.db so maybe_auto_index takes the
     * "already indexed" branch — the watcher-registration site under test. */
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    if (th_write_file(db_path, "") != 0) {
        free(project);
        th_rmtree(cache);
        return -4;
    }
    free(project);

    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char old_cwd[1024];
    if (!cbm_getcwd(old_cwd, sizeof(old_cwd)) || cbm_chdir(repodir) != 0) {
        restore_cache_dir(saved_copy);
        free(saved_copy);
        th_rmtree(cache);
        return -5;
    }

    int count = -6;
    cbm_config_t *cfg = cbm_config_open(cache);
    cbm_store_t *wstore = cbm_store_open_memory();
    cbm_watcher_t *watcher = wstore ? cbm_watcher_new(wstore, NULL, NULL) : NULL;
    if (cfg && watcher) {
        if (auto_watch_value) {
            cbm_config_set(cfg, CBM_CONFIG_AUTO_WATCH, auto_watch_value);
        }

        cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
        if (srv) {
            cbm_mcp_server_set_watcher(srv, watcher);
            cbm_mcp_server_set_config(srv, cfg);
            char *resp = cbm_mcp_server_handle(
                srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}");
            free(resp);
            count = cbm_watcher_watch_count(watcher);
            cbm_mcp_server_free(srv);
        }
    }

    if (watcher) {
        cbm_watcher_free(watcher);
    }
    if (wstore) {
        cbm_store_close(wstore);
    }
    if (cfg) {
        cbm_config_close(cfg);
    }

    (void)cbm_chdir(old_cwd);
    restore_cache_dir(saved_copy);
    free(saved_copy);
    th_rmtree(cache);
    return count;
}

/* Default (key unset) → watcher registered on connect. Guards the
 * no-behavior-change promise of the auto_watch gate: existing users keep
 * background auto-sync without touching config. */
TEST(mcp_auto_watch_default_registers_watcher_on_connect) {
    int count = auto_watch_connect_watch_count(NULL);
    if (count < 0) {
        PASS(); /* fixture setup failed (tmpdir/cwd unavailable) — skip */
    }
    ASSERT_EQ(count, 1);
    PASS();
}

/* auto_watch=false → NO watcher registered on connect. RED on pre-gate code
 * (registration was unconditional and the key did not exist). */
TEST(mcp_auto_watch_false_skips_watcher_on_connect) {
    int count = auto_watch_connect_watch_count("false");
    if (count < 0) {
        PASS(); /* fixture setup failed (tmpdir/cwd unavailable) — skip */
    }
    ASSERT_EQ(count, 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  #853 — auto_watch=false must ALSO gate the SUPERVISED fresh-index
 *          watcher registration (keystone × #849 merge interaction)
 * ══════════════════════════════════════════════════════════════════ */

/* #849 routed ALL watcher registration through register_watcher_if_enabled()
 * (auto_watch gate). The #832 keystone then added a SECOND registration site in
 * autoindex_thread's supervised-success branch, but wired it as a DIRECT
 * cbm_watcher_watch() guarded only by `if (srv->watcher)` — srv->watcher is set
 * unconditionally, so that guard does NOT honour `config set auto_watch false`.
 * The above tests only cover the already-indexed on-connect path
 * (register_watcher_if_enabled); this guard covers the fresh-index SUPERVISED
 * autoindex_thread branch that #832 introduced.
 *
 * Drive the real public entry initialize → maybe_auto_index → autoindex_thread on
 * a supervisor-marked host (kill switch off) with a FRESH project (no prior .db)
 * and auto_watch=false. cbm_mcp_server_free() joins the autoindex thread, so the
 * (buggy or gated) registration decision has run before we read the watch count.
 *
 * RED on the unfixed ungated block: the supervised success branch calls
 * cbm_watcher_watch() unconditionally → watch_count == 1 → IDX853_WATCHER_REGISTERED.
 * GREEN once it calls register_watcher_if_enabled() → auto_watch_off skip → 0.
 * spawn_count is asserted to have advanced so the assertion cannot pass vacuously
 * (i.e. green only because the supervised branch was never entered). */
enum {
    IDX853_OK = 0,                  /* watch_count==0, supervised branch ran → GREEN */
    IDX853_WATCHER_REGISTERED = 61, /* watch_count==1 → RED: ungated cbm_watcher_watch */
    IDX853_NO_SPAWN = 62,           /* spawn_count unchanged → supervised path not exercised */
    IDX853_SETUP_FAIL = 63,         /* config/watcher/server/cwd setup failed */
    IDX853_BAD_COUNT = 64,          /* unexpected watch_count (<0 or >1) */
};

#ifndef _WIN32 /* helper used only by the POSIX fork harness below */
static int idx853_supervised_autowatch_check(const char *repo_dir, const char *cache_dir) {
    /* Become a supervisor host with the kill switch OFF — the real prod MCP
     * server's state. Done in the FORKED CHILD only (see harness) so the parent
     * test-runner's process-wide host mark stays clear (#845 invariant). Bound the
     * worker so a stuck spawn cannot run long under the fork+alarm net. */
    cbm_index_supervisor_mark_host();
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    cbm_setenv("CBM_INDEX_MAX_RESTARTS", "1", 1);
    cbm_setenv("CBM_INDEX_WORKER_TIMEOUT_S", "30", 1);

    cbm_config_t *cfg = cbm_config_open(cache_dir);
    cbm_store_t *wstore = cbm_store_open_memory();
    cbm_watcher_t *watcher = wstore ? cbm_watcher_new(wstore, NULL, NULL) : NULL;
    if (!cfg || !watcher) {
        if (watcher) {
            cbm_watcher_free(watcher);
        }
        if (wstore) {
            cbm_store_close(wstore);
        }
        if (cfg) {
            cbm_config_close(cfg);
        }
        return IDX853_SETUP_FAIL;
    }
    /* auto_index=true → maybe_auto_index launches autoindex_thread for the fresh
     * project; auto_watch=false → the gate this guard exercises. */
    cbm_config_set(cfg, CBM_CONFIG_AUTO_INDEX, "true");
    cbm_config_set(cfg, CBM_CONFIG_AUTO_WATCH, "false");

    /* detect_session derives session_root/session_project from the cwd. */
    char old_cwd[1024];
    if (!cbm_getcwd(old_cwd, sizeof(old_cwd)) || cbm_chdir(repo_dir) != 0) {
        cbm_watcher_free(watcher);
        cbm_store_close(wstore);
        cbm_config_close(cfg);
        return IDX853_SETUP_FAIL;
    }

    int spawns_before = cbm_index_supervisor_spawn_count();
    int code = IDX853_SETUP_FAIL;

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (srv) {
        cbm_mcp_server_set_watcher(srv, watcher);
        cbm_mcp_server_set_config(srv, cfg);
        char *resp = cbm_mcp_server_handle(
            srv, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}");
        free(resp);
        /* free() joins the autoindex thread → the supervised worker has finished
         * and the registration decision (buggy or gated) has executed. */
        cbm_mcp_server_free(srv);

        int spawns_after = cbm_index_supervisor_spawn_count();
        int watch_count = cbm_watcher_watch_count(watcher);

        if (spawns_after == spawns_before) {
            code = IDX853_NO_SPAWN; /* supervised branch never ran — not a valid probe */
        } else if (watch_count == 1) {
            code = IDX853_WATCHER_REGISTERED; /* the discriminating RED assertion */
        } else if (watch_count == 0) {
            code = IDX853_OK;
        } else {
            code = IDX853_BAD_COUNT;
        }
    }

    (void)cbm_chdir(old_cwd);
    cbm_watcher_free(watcher);
    cbm_store_close(wstore);
    cbm_config_close(cfg);
    return code;
}
#endif /* !_WIN32 */

TEST(mcp_auto_watch_false_skips_supervised_autoindex_issue853) {
#ifdef _WIN32
    /* Marks the process as a supervisor host (irreversible); POSIX isolates that
     * in a forked child. The gate logic is platform-independent and covered on
     * POSIX CI. */
    SKIP_PLATFORM("supervisor-host guard needs fork isolation (POSIX-only)");
#else
    char tmp_dir[256];
    snprintf(tmp_dir, sizeof(tmp_dir), "/tmp/cbm-idx853-repo-XXXXXX");
    if (!cbm_mkdtemp(tmp_dir)) {
        PASS();
    }
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-idx853-cache-XXXXXX");
    if (!cbm_mkdtemp(cache)) {
        cbm_rmdir(tmp_dir);
        PASS();
    }

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1); /* inherited by the worker child */

    char src_path[512];
    snprintf(src_path, sizeof(src_path), "%s/main.py", tmp_dir);
    FILE *fp = fopen(src_path, "w");
    ASSERT_NOT_NULL(fp);
    fputs("def idx853_fn():\n    return 'ok'\n", fp);
    fclose(fp);

    int code = -1;
    bool signalled = false;
    int sig = 0;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60); /* a stuck worker dies here instead of hanging the runner */
        _exit(idx853_supervised_autowatch_check(tmp_dir, cache));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    char *project = cbm_project_name_from_path(tmp_dir);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    remove(src_path);
    cbm_rmdir(cache);
    cbm_rmdir(tmp_dir);

    if (signalled) {
        printf("    child killed by signal %d (alarm => worker hang)\n", sig);
    } else if (code != IDX853_OK) {
        printf("    child exit code %d (61=watcher registered under auto_watch=false=RED, "
               "62=no spawn, 63=setup fail, 64=bad count)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDX853_OK);
    PASS();
#endif
}

/* The containment guard both MCP file-read sinks route through
 * (resolve_snippet_source for get_code_snippet, attach_result_source for
 * search_code). A result path that resolves outside the indexed project root
 * — via a `..` segment or a followed symlink/junction — must be rejected so
 * its contents never reach a tool response. */
extern bool cbm_path_within_root(const char *root_path, const char *abs_path);

TEST(mcp_path_within_root_rejects_escape) {
#ifdef _WIN32
    char root[512];
    char outside[512];
    snprintf(root, sizeof(root), "%s/cbm_pwr_root_XXXXXX", cbm_tmpdir());
    snprintf(outside, sizeof(outside), "%s/cbm_pwr_outside_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(root));
    ASSERT_NOT_NULL(cbm_mkdtemp(outside));

    char inside[700];
    char target[700];
    char junction[700];
    char linked_target[900];
    snprintf(inside, sizeof(inside), "%s/inside.c", root);
    snprintf(target, sizeof(target), "%s/outside.c", outside);
    snprintf(junction, sizeof(junction), "%s/escape", root);
    snprintf(linked_target, sizeof(linked_target), "%s/outside.c", junction);
    FILE *fp = cbm_fopen(inside, "w");
    ASSERT_NOT_NULL(fp);
    fputs("int inside;\n", fp);
    fclose(fp);
    fp = cbm_fopen(target, "w");
    ASSERT_NOT_NULL(fp);
    fputs("int outside;\n", fp);
    fclose(fp);

    /* cbm_tmpdir() can expose the MSYS spelling C:/msys64/...; cmd's mklink
     * builtin treats the slash before "msys64" as another option delimiter.
     * Native backslashes are required only at this cmd.exe fixture boundary. */
    char junction_native[sizeof(junction)];
    char outside_native[sizeof(outside)];
    snprintf(junction_native, sizeof(junction_native), "%s", junction);
    snprintf(outside_native, sizeof(outside_native), "%s", outside);
    for (char *cursor = junction_native; *cursor; cursor++) {
        if (*cursor == '/') {
            *cursor = '\\';
        }
    }
    for (char *cursor = outside_native; *cursor; cursor++) {
        if (*cursor == '/') {
            *cursor = '\\';
        }
    }
    const char *junction_argv[] = {"cmd.exe",       "/d",           "/c", "mklink", "/J",
                                   junction_native, outside_native, NULL};
    bool linked = cbm_exec_no_shell(junction_argv) == 0;

    ASSERT_TRUE(linked);
    ASSERT_TRUE(cbm_path_within_root(root, inside));
    ASSERT_FALSE(cbm_path_within_root(root, target));
    ASSERT_FALSE(cbm_path_within_root(root, linked_target));

    char case_alias[sizeof(root)];
    snprintf(case_alias, sizeof(case_alias), "%s", root);
    char *leaf = strrchr(case_alias, '/');
    char *backslash_leaf = strrchr(case_alias, '\\');
    if (!leaf || (backslash_leaf && backslash_leaf > leaf)) {
        leaf = backslash_leaf;
    }
    leaf = leaf ? leaf + 1 : case_alias;
    if (*leaf >= 'a' && *leaf <= 'z') {
        *leaf = (char)(*leaf - 'a' + 'A');
    } else if (*leaf >= 'A' && *leaf <= 'Z') {
        *leaf = (char)(*leaf - 'A' + 'a');
    }
    ASSERT_TRUE(cbm_path_within_root(case_alias, inside));

    char drive_root[] = {root[0], ':', '\\', '\0'};
    ASSERT_TRUE(((root[0] >= 'A' && root[0] <= 'Z') || (root[0] >= 'a' && root[0] <= 'z')) &&
                root[1] == ':');
    ASSERT_TRUE(cbm_path_within_root(drive_root, inside));

    cbm_rmdir(junction);
    cbm_unlink(inside);
    cbm_unlink(target);
    cbm_rmdir(root);
    cbm_rmdir(outside);
    PASS();
#else
    char root[512];
    snprintf(root, sizeof(root), "%s/cbm_pwr_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(root)) {
        FAIL("cbm_mkdtemp failed");
    }
    char inside[700];
    snprintf(inside, sizeof(inside), "%s/inside.c", root);
    FILE *fp = fopen(inside, "w");
    ASSERT_NOT_NULL(fp);
    fputs("int x;\n", fp);
    fclose(fp);

    /* The abs_path a sink builds for an in-root result stays contained; a `..`
     * escape to an existing outside file (/etc/hosts) resolves out and must be
     * rejected. */
    char escape[900];
    snprintf(escape, sizeof(escape), "%s/../../../../etc/hosts", root);
    ASSERT_TRUE(cbm_path_within_root(root, inside));
    ASSERT_FALSE(cbm_path_within_root(root, escape));
    ASSERT_FALSE(cbm_path_within_root(root, "/etc/hosts"));
    ASSERT_TRUE(cbm_path_within_root("/", "/etc/hosts"));

    remove(inside);
    cbm_rmdir(root);
    PASS();
#endif
}

/* base_branch is spliced into a `git diff --name-only "<base>"...HEAD` command;
 * a value starting with '-' would be taken by git as an option (e.g.
 * --output=<path> writes the diff to an arbitrary file) rather than a ref. It
 * must be rejected up front, alongside the shell-metacharacter check. */
TEST(detect_changes_rejects_option_like_base_branch) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char *resp = cbm_mcp_server_handle(
        srv, "{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"detect_changes\","
             "\"arguments\":{\"project\":\"p\",\"base_branch\":\"--output=/tmp/cbm_pwn\"}}}");
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "invalid characters"));
    free(resp);
    cbm_mcp_server_free(srv);
    PASS();
}

TEST(detect_changes_rejects_windows_cmd_metacharacters_in_base_branch) {
#ifdef _WIN32
    const char *const branches[] = {"topic%PATH%", "topic!name!", "topic^name"};
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    for (size_t i = 0; i < sizeof(branches) / sizeof(branches[0]); i++) {
        char request[512];
        snprintf(request, sizeof(request),
                 "{\"jsonrpc\":\"2.0\",\"id\":78,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"detect_changes\","
                 "\"arguments\":{\"project\":\"p\",\"base_branch\":\"%s\"}}}",
                 branches[i]);
        char *response = cbm_mcp_server_handle(srv, request);
        ASSERT_NOT_NULL(response);
        ASSERT_NOT_NULL(strstr(response, "base_branch contains invalid characters"));
        free(response);
    }
    cbm_mcp_server_free(srv);
    PASS();
#else
    SKIP_PLATFORM("cmd.exe interpolation validation runs on Windows");
#endif
}

TEST(detect_changes_rejects_windows_cmd_metacharacters_in_project_root) {
#ifdef _WIN32
    const char *const roots[] = {"C:\\cbm-root-%PATH%", "C:\\cbm-root-!name!",
                                 "C:\\cbm-root-^name"};
    const char *project = "windows-cmd-root-validation";
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    ASSERT_NOT_NULL(srv);
    cbm_store_t *store = cbm_mcp_server_store(srv);
    ASSERT_NOT_NULL(store);
    cbm_mcp_server_set_project(srv, project);
    mcp_command_hook_probe_t command_probe = {0};
    cbm_mcp_server_set_command_test_hook(srv, mcp_command_hook_probe, &command_probe);

    for (size_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
        ASSERT_EQ(cbm_store_upsert_project(store, project, roots[i]), CBM_STORE_OK);
        char *response = cbm_mcp_handle_tool(
            srv, "detect_changes",
            "{\"project\":\"windows-cmd-root-validation\",\"base_branch\":\"main\"}");
        ASSERT_NOT_NULL(response);
        ASSERT_NOT_NULL(strstr(response, "project path contains invalid characters"));
        free(response);
    }
    ASSERT_EQ(command_probe.diff_calls, 0);
    ASSERT_EQ(command_probe.merge_base_calls, 0);
    cbm_mcp_server_free(srv);
    PASS();
#else
    SKIP_PLATFORM("cmd.exe interpolation validation runs on Windows");
#endif
}

/* Opt-in workspace boundary: when CBM_ALLOWED_ROOT is set, index_repository
 * must refuse a repo_path that resolves outside it. Unset (the default) imposes
 * no restriction. */
TEST(index_repository_honors_allowed_root) {
    char allowed[512];
    snprintf(allowed, sizeof(allowed), "%s/cbm_allowed_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(allowed)) {
        FAIL("cbm_mkdtemp failed");
    }
    cbm_setenv("CBM_ALLOWED_ROOT", allowed, 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    char args[1024];
    snprintf(args, sizeof(args),
             "{\"jsonrpc\":\"2.0\",\"id\":88,\"method\":\"tools/call\","
             "\"params\":{\"name\":\"index_repository\","
             "\"arguments\":{\"repo_path\":\"%s/../..\"}}}",
             allowed); /* resolves to a parent, outside the allowed root */
    char *resp = cbm_mcp_server_handle(srv, args);
    ASSERT_NOT_NULL(resp);
    ASSERT_NOT_NULL(strstr(resp, "outside the allowed root"));
    free(resp);

    cbm_unsetenv("CBM_ALLOWED_ROOT");
    cbm_mcp_server_free(srv);
    cbm_rmdir(allowed);
    PASS();
}

TEST(index_repository_relative_path_uses_explicit_session_root) {
    char session_root[512];
    char cache[512];
    snprintf(session_root, sizeof(session_root), "%s/cbm_daemon_session_XXXXXX", cbm_tmpdir());
    snprintf(cache, sizeof(cache), "%s/cbm_daemon_cache_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(session_root) || !cbm_mkdtemp(cache)) {
        th_rmtree(session_root);
        th_rmtree(cache);
        FAIL("cbm_mkdtemp failed");
    }

    char repo[1024];
    char source[1200];
    snprintf(repo, sizeof(repo), "%s/repo", session_root);
    snprintf(source, sizeof(source), "%s/main.py", repo);
    ASSERT_EQ(th_write_file(source, "def main():\n    return 1\n"), 0);

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    const char *saved_supervisor = getenv("CBM_INDEX_SUPERVISOR");
    char *saved_supervisor_copy = saved_supervisor ? strdup(saved_supervisor) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_setenv("CBM_INDEX_SUPERVISOR", "0", 1);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    bool context_set = srv && cbm_mcp_server_set_session_context(srv, session_root, session_root);
    const char request[] = "{\"jsonrpc\":\"2.0\",\"id\":89,\"method\":\"tools/call\","
                           "\"params\":{\"name\":\"index_repository\","
                           "\"arguments\":{\"repo_path\":\"repo\",\"mode\":\"fast\"}}}";
    char *response = context_set ? cbm_mcp_server_handle(srv, request) : NULL;
    bool accepted = response && strstr(response, "outside the allowed root") == NULL &&
                    strstr(response, "\"isError\":true") == NULL;

    char *project = cbm_project_name_from_path(repo);
    char db_path[CBM_SZ_4K];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project ? project : "missing");
    bool indexed_session_repo = project && cbm_file_size(db_path) >= 0;

    free(response);
    cbm_mcp_server_free(srv);
    cleanup_project_db(cache, project);
    free(project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    if (saved_supervisor_copy) {
        cbm_setenv("CBM_INDEX_SUPERVISOR", saved_supervisor_copy, 1);
    } else {
        cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    }
    free(saved_supervisor_copy);
    th_rmtree(session_root);
    th_rmtree(cache);

    ASSERT_TRUE(context_set);
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(indexed_session_repo);
    PASS();
}

/* A daemon-backed session validates repo_path against its own session root, but
 * the supervised worker is a fresh process that inherits the daemon's cwd. A
 * relative path must therefore be resolved once by the session and forwarded to
 * the worker as that same canonical absolute path. The decoy repo makes an
 * unsanitized handoff observable: forwarding the original "repo" indexes the
 * cwd-relative decoy instead of the validated session repo. */
enum {
    IDXCANON_OK = 0,
    IDXCANON_GETCWD_FAILED = 71,
    IDXCANON_CHDIR_FAILED = 72,
    IDXCANON_NO_SERVER = 73,
    IDXCANON_CONTEXT_FAILED = 74,
    IDXCANON_NO_SPAWN = 75,
    IDXCANON_NO_RESULT = 76,
    IDXCANON_NOT_INDEXED = 77,
    IDXCANON_WRONG_PROJECT = 78,
    IDXCANON_DECOY_INDEXED = 79,
    IDXCANON_TARGET_MISSING = 80,
    IDXCANON_CWD_RESTORE_FAILED = 81,
};

#ifndef _WIN32
static int idxcanon_supervised_session_path_check(const char *session_root, const char *decoy_cwd) {
    char saved_cwd[CBM_SZ_4K];
    if (!cbm_getcwd(saved_cwd, sizeof(saved_cwd))) {
        return IDXCANON_GETCWD_FAILED;
    }
    if (cbm_chdir(decoy_cwd) != 0) {
        return IDXCANON_CHDIR_FAILED;
    }

    /* Match a real supervisor host. Environment changes are isolated to this
     * forked child and inherited by its worker; the parent test process keeps
     * its supervisor kill switch and allowed-root environment untouched. */
    cbm_index_supervisor_mark_host();
    cbm_unsetenv("CBM_INDEX_SUPERVISOR");
    cbm_unsetenv("CBM_ALLOWED_ROOT");
    cbm_setenv("CBM_INDEX_MAX_RESTARTS", "1", 1);
    cbm_setenv("CBM_INDEX_WORKER_TIMEOUT_S", "30", 1);

    char session_repo[CBM_SZ_4K];
    char decoy_repo[CBM_SZ_4K];
    snprintf(session_repo, sizeof(session_repo), "%s/repo", session_root);
    snprintf(decoy_repo, sizeof(decoy_repo), "%s/repo", decoy_cwd);
    char *session_project = cbm_project_name_from_path(session_repo);
    char *decoy_project = cbm_project_name_from_path(decoy_repo);

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    int code = IDXCANON_OK;
    if (!srv) {
        code = IDXCANON_NO_SERVER;
    } else if (!cbm_mcp_server_set_session_context(srv, session_root, session_root)) {
        code = IDXCANON_CONTEXT_FAILED;
    }

    int spawns_before = cbm_index_supervisor_spawn_count();
    char *resp = code == IDXCANON_OK
                     ? cbm_mcp_handle_tool(srv, "index_repository",
                                           "{\"repo_path\":\"repo\",\"mode\":\"fast\"}")
                     : NULL;
    int spawns_after = cbm_index_supervisor_spawn_count();
    if (code == IDXCANON_OK && spawns_after == spawns_before) {
        code = IDXCANON_NO_SPAWN;
    } else if (code == IDXCANON_OK && !resp) {
        code = IDXCANON_NO_RESULT;
    } else if (code == IDXCANON_OK &&
               !response_contains_json_fragment(resp, "\"status\":\"indexed\"")) {
        code = IDXCANON_NOT_INDEXED;
    }

    if (code == IDXCANON_OK) {
        char expected[CBM_SZ_4K];
        snprintf(expected, sizeof(expected), "\"project\":\"%s\"",
                 session_project ? session_project : "");
        if (!session_project || !response_contains_json_fragment(resp, expected)) {
            code = IDXCANON_WRONG_PROJECT;
        }
    }
    free(resp);

    /* A raw "repo" handoff is interpreted relative to decoy_cwd by the worker
     * and creates this project DB. Its absence proves the original JSON did not
     * substitute a different path after the parent validated session_repo. */
    if (code == IDXCANON_OK) {
        const char *cache = getenv("CBM_CACHE_DIR");
        char decoy_db[CBM_SZ_4K];
        snprintf(decoy_db, sizeof(decoy_db), "%s/%s.db", cache ? cache : "",
                 decoy_project ? decoy_project : "");
        if (!cache || !decoy_project || cbm_file_size(decoy_db) >= 0) {
            code = IDXCANON_DECOY_INDEXED;
        }
    }

    if (code == IDXCANON_OK) {
        char query[CBM_SZ_4K];
        snprintf(query, sizeof(query),
                 "{\"project\":\"%s\",\"name_pattern\":\"canonical_target_fn\","
                 "\"label\":\"Function\"}",
                 session_project ? session_project : "");
        char *search = cbm_mcp_handle_tool(srv, "search_graph", query);
        if (!session_project || !search || !strstr(search, "canonical_target_fn")) {
            code = IDXCANON_TARGET_MISSING;
        }
        free(search);
    }

    cbm_mcp_server_free(srv);
    free(session_project);
    free(decoy_project);
    if (cbm_chdir(saved_cwd) != 0 && code == IDXCANON_OK) {
        code = IDXCANON_CWD_RESTORE_FAILED;
    }
    return code;
}
#endif

TEST(index_repository_supervisor_uses_canonical_session_path) {
#ifdef _WIN32
    SKIP_PLATFORM("supervisor-host guard needs fork isolation (POSIX-only)");
#else
    char session_root[512];
    char decoy_cwd[512];
    char cache[512];
    snprintf(session_root, sizeof(session_root), "%s/cbm_canonical_session_XXXXXX", cbm_tmpdir());
    snprintf(decoy_cwd, sizeof(decoy_cwd), "%s/cbm_canonical_decoy_XXXXXX", cbm_tmpdir());
    snprintf(cache, sizeof(cache), "%s/cbm_canonical_cache_XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(session_root) || !cbm_mkdtemp(decoy_cwd) || !cbm_mkdtemp(cache)) {
        th_rmtree(session_root);
        th_rmtree(decoy_cwd);
        th_rmtree(cache);
        FAIL("cbm_mkdtemp failed");
    }

    char session_source[CBM_SZ_4K];
    char decoy_source[CBM_SZ_4K];
    snprintf(session_source, sizeof(session_source), "%s/repo/main.py", session_root);
    snprintf(decoy_source, sizeof(decoy_source), "%s/repo/main.py", decoy_cwd);
    ASSERT_EQ(th_write_file(session_source, "def canonical_target_fn():\n    return 1\n"), 0);
    ASSERT_EQ(th_write_file(decoy_source, "def decoy_fn():\n    return 2\n"), 0);

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);

    int code = -1;
    bool signalled = false;
    int sig = 0;
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60);
        _exit(idxcanon_supervised_session_path_check(session_root, decoy_cwd));
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        signalled = true;
        sig = WTERMSIG(status);
    }

    char session_repo[CBM_SZ_4K];
    char decoy_repo[CBM_SZ_4K];
    snprintf(session_repo, sizeof(session_repo), "%s/repo", session_root);
    snprintf(decoy_repo, sizeof(decoy_repo), "%s/repo", decoy_cwd);
    char *session_project = cbm_project_name_from_path(session_repo);
    char *decoy_project = cbm_project_name_from_path(decoy_repo);
    cleanup_project_db(cache, session_project);
    cleanup_project_db(cache, decoy_project);
    free(session_project);
    free(decoy_project);
    restore_cache_dir(saved_cache_copy);
    free(saved_cache_copy);
    th_rmtree(session_root);
    th_rmtree(decoy_cwd);
    th_rmtree(cache);

    if (signalled) {
        printf("    child killed by signal %d (alarm => worker hang)\n", sig);
    } else if (code != IDXCANON_OK) {
        printf("    child exit code %d (75=no spawn, 77=not indexed, 78=wrong project, "
               "79=decoy indexed, 80=target missing)\n",
               code);
    }
    ASSERT_FALSE(signalled);
    ASSERT_EQ(code, IDXCANON_OK);
    PASS();
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(mcp) {
    RUN_TEST(mcp_path_within_root_rejects_escape);
    RUN_TEST(detect_changes_rejects_option_like_base_branch);
    RUN_TEST(detect_changes_rejects_windows_cmd_metacharacters_in_base_branch);
    RUN_TEST(detect_changes_rejects_windows_cmd_metacharacters_in_project_root);
    RUN_TEST(index_repository_honors_allowed_root);
    /* JSON-RPC parsing */
    RUN_TEST(jsonrpc_parse_request);
    RUN_TEST(jsonrpc_parse_notification);
    RUN_TEST(jsonrpc_parse_invalid);
    RUN_TEST(jsonrpc_parse_tools_call);
    RUN_TEST(jsonrpc_parse_string_id_issue253);
    RUN_TEST(jsonrpc_format_response_string_id_issue253);

    /* JSON-RPC parsing — edge cases */
    RUN_TEST(jsonrpc_parse_empty_string);
    RUN_TEST(jsonrpc_parse_missing_jsonrpc_field);
    RUN_TEST(jsonrpc_parse_missing_method);
    RUN_TEST(jsonrpc_parse_string_id);
    RUN_TEST(jsonrpc_parse_no_params);
    RUN_TEST(jsonrpc_parse_extra_whitespace);
    RUN_TEST(jsonrpc_parse_array_not_object);

    /* JSON-RPC formatting */
    RUN_TEST(jsonrpc_format_response);
    RUN_TEST(jsonrpc_format_error);

    /* MCP protocol helpers */
    RUN_TEST(mcp_initialize_response);
    RUN_TEST(mcp_tools_list);
    RUN_TEST(mcp_tools_list_latest_metadata);
    RUN_TEST(mcp_tools_have_behavior_annotations);
    RUN_TEST(mcp_index_repository_declares_name_override_issue571);
    RUN_TEST(mcp_tools_array_schemas_have_items);
    RUN_TEST(mcp_ingest_traces_items_disallow_additional_properties_issue731);
    RUN_TEST(mcp_get_architecture_aspects_schema_enum_pr560);
    RUN_TEST(mcp_text_result);
    RUN_TEST(mcp_text_result_wraps_plain_text_as_structured_content);
    RUN_TEST(mcp_cancel_matches_request_id);
    RUN_TEST(mcp_text_result_error);

    /* Argument extraction */
    RUN_TEST(mcp_get_tool_name);
    RUN_TEST(mcp_get_arguments);
    RUN_TEST(mcp_get_string_arg);
    RUN_TEST(mcp_get_int_arg);
    RUN_TEST(mcp_get_bool_arg);

    /* Argument extraction — edge cases */
    RUN_TEST(mcp_get_string_arg_empty_json);
    RUN_TEST(mcp_get_string_arg_empty_object);
    RUN_TEST(mcp_get_string_arg_nested_value);
    RUN_TEST(mcp_get_string_arg_int_value);
    RUN_TEST(mcp_get_int_arg_empty_json);
    RUN_TEST(mcp_get_int_arg_string_value);
    RUN_TEST(mcp_get_int_arg_bool_value);
    RUN_TEST(mcp_get_bool_arg_empty_json);
    RUN_TEST(mcp_get_bool_arg_int_value);
    RUN_TEST(mcp_get_tool_name_empty_json);
    RUN_TEST(mcp_get_tool_name_missing_name);
    RUN_TEST(mcp_get_arguments_empty_json);
    RUN_TEST(mcp_get_arguments_no_arguments_key);

    /* Server protocol handling */
    RUN_TEST(server_handle_initialize);
    RUN_TEST(server_handle_initialized_notification);
    RUN_TEST(server_handle_tools_list);
    RUN_TEST(server_handle_tools_list_defaults_to_all_tools_and_accepts_cursor);
    RUN_TEST(server_handle_analysis_profile_filters_and_rejects_mutators);
    RUN_TEST(server_handle_scout_profile_exposes_only_the_fast_tier);
    RUN_TEST(analysis_profile_arguments_fail_closed_and_disable_http);
    RUN_TEST(hook_windows_path_containment_is_case_insensitive_and_segment_safe);
    RUN_TEST(server_handle_prompts_list_workflows);
    RUN_TEST(server_handle_prompts_get_workflows);
    RUN_TEST(server_handle_prompts_get_validates_arguments);
    RUN_TEST(server_handle_logs_request_without_params);
    RUN_TEST(server_handle_unknown_method);

    /* Server handle — edge cases */
    RUN_TEST(server_handle_invalid_json);
    RUN_TEST(server_handle_empty_object);
    RUN_TEST(server_handle_tools_call_missing_name);

    /* Tool handlers */
    RUN_TEST(tool_list_projects_empty);
    RUN_TEST(tool_get_graph_schema_empty);
    RUN_TEST(tool_unknown_tool);
    RUN_TEST(tool_search_graph_basic);
    RUN_TEST(tool_trace_totals_respect_test_filter);
    RUN_TEST(tool_get_architecture_cycles_detects_scc);
    RUN_TEST(tool_get_code_snippet_clips_whole_file_node);
    RUN_TEST(tool_search_graph_includes_node_properties);
    RUN_TEST(tool_search_graph_toon_never_leaks_internal_fields);
    RUN_TEST(tool_lean_defaults_schema_and_status);
    RUN_TEST(tool_output_regression_gate);
    RUN_TEST(tool_output_byte_budgets);
    RUN_TEST(tool_search_graph_query_honors_file_pattern_issue552);
    RUN_TEST(mcp_resource_discovery_methods_return_empty_lists);
    RUN_TEST(tool_query_graph_basic);
    RUN_TEST(tool_index_status_no_project);
    RUN_TEST(tool_check_index_coverage_finds_path_beyond_status_cap);
    RUN_TEST(tool_check_index_coverage_reports_paths_scopes_and_ranges);
    RUN_TEST(tool_check_index_coverage_rejects_stale_generation);
    RUN_TEST(tool_check_index_coverage_requires_source_when_file_metadata_changed);
    RUN_TEST(tool_check_index_coverage_surfaces_lookup_errors);
    RUN_TEST(tool_index_status_includes_git_metadata);

    /* Tool handlers with validation */
    RUN_TEST(tool_trace_call_path_not_found);
    RUN_TEST(tool_trace_missing_function_name);
    RUN_TEST(tool_trace_call_path_ambiguous);
    RUN_TEST(tool_trace_union_records_min_hop_across_seeds);
    RUN_TEST(tool_trace_pagination_exactly_once);
    RUN_TEST(tool_trace_call_path_prefers_definition);
    RUN_TEST(tool_trace_call_path_depth_clamped);
    RUN_TEST(tool_trace_call_path_distinct_defs_not_over_unioned);
    RUN_TEST(tool_trace_call_path_dts_stub_unions_with_impl);
    RUN_TEST(tool_delete_project_not_found);
    RUN_TEST(tool_get_architecture_empty);
    RUN_TEST(tool_get_architecture_emits_populated_sections);
    RUN_TEST(tool_get_architecture_overview_compact_subset_pr560);
    RUN_TEST(tool_get_architecture_rejects_unknown_aspect_pr560);
    RUN_TEST(tool_get_architecture_accepts_project_name_alias_issue640);
    RUN_TEST(tool_search_graph_accepts_project_name_alias_issue640);
    RUN_TEST(tool_project_arg_resolves_unique_tail_issue1025);
    RUN_TEST(tool_get_architecture_path_scoping);
    RUN_TEST(tool_query_graph_missing_query);

    /* Pipeline-dependent tool handlers */
    RUN_TEST(tool_index_repository_missing_path);
    RUN_TEST(tool_get_code_snippet_missing_qn);
    RUN_TEST(tool_get_code_snippet_not_found);
    RUN_TEST(tool_search_code_missing_pattern);
    RUN_TEST(tool_search_code_no_project);
    RUN_TEST(search_code_multi_word);
    RUN_TEST(search_code_scoped_path_with_spaces_issue687);
#ifdef _WIN32
    RUN_TEST(search_code_scoped_path_with_cjk_root_issue903);
#endif
    RUN_TEST(search_code_path_filter_prefilter_keeps_matches);
    RUN_TEST(search_code_path_filter_matches_nothing);
    RUN_TEST(search_code_invalid_regex_errors_issue283);
    RUN_TEST(search_code_literal_pipe_warns_issue282);
    RUN_TEST(search_code_ampersand_accepted_issue272);
    RUN_TEST(tool_detect_changes_no_project);
    RUN_TEST(tool_manage_adr_no_project);
    RUN_TEST(tool_manage_adr_get_with_existing_adr);
    RUN_TEST(tool_manage_adr_unified_backend_issue256);
    RUN_TEST(tool_index_repository_reports_store_backed_adr);
    RUN_TEST(tool_index_repository_dot_uses_absolute_project_key_and_preserves_adr);
    RUN_TEST(index_repository_relative_path_uses_explicit_session_root);
    RUN_TEST(index_repository_supervisor_uses_canonical_session_path);
    RUN_TEST(index_repository_cli_name_override_issue823);
    RUN_TEST(index_supervisor_unsafe_clean_is_never_fallback_or_recovery);
    RUN_TEST(index_supervisor_gate_requires_marked_host_issue845);
    RUN_TEST(index_supervisor_start_failure_is_fail_closed_in_real_host);
    RUN_TEST(index_bg_paths_route_through_supervisor_issue832);
    RUN_TEST(sequential_service_edge_props_are_valid_json_issue898);
    RUN_TEST(index_second_inprocess_run_survives_issue773);
    RUN_TEST(index_recovery_parallel_quarantines_crasher);
    RUN_TEST(tool_manage_adr_not_found_rich_error);
    RUN_TEST(tool_manage_adr_get_accepts_abs_path);
    RUN_TEST(tool_manage_adr_get_accepts_symlink_path);
    RUN_TEST(tool_detect_changes_not_found_rich_error);
    RUN_TEST(tool_detect_changes_contained_commands_clean_up_error_and_success);
    RUN_TEST(tool_ingest_traces_basic);
    RUN_TEST(tool_ingest_traces_empty);

    /* Query store generation freshness */
    RUN_TEST(query_store_reopens_after_database_replacement);
    /* Query store read-only (data integrity) */
    RUN_TEST(readonly_query_does_not_mutate_db);
    RUN_TEST(readonly_query_succeeds_on_readonly_fs);

    /* Idle store eviction */
    RUN_TEST(store_idle_eviction);
    RUN_TEST(store_idle_no_eviction_within_timeout);
    RUN_TEST(store_idle_evict_protects_initial_store);
    RUN_TEST(store_idle_evict_access_resets_timer);

    /* URI helpers */
    RUN_TEST(parse_file_uri_unix);
    RUN_TEST(parse_file_uri_windows);
    RUN_TEST(parse_file_uri_invalid);

    /* URI helpers — edge cases */
    RUN_TEST(parse_file_uri_http_scheme);
    RUN_TEST(parse_file_uri_ftp_scheme);
    RUN_TEST(parse_file_uri_buffer_too_small);
    RUN_TEST(parse_file_uri_spaces_in_path);
    RUN_TEST(parse_file_uri_null_out_path);
    RUN_TEST(parse_file_uri_zero_size);

    /* Poll/getline FILE* buffering fix */
#ifndef _WIN32
    RUN_TEST(mcp_server_run_rapid_messages);
#endif

    /* Snippet resolution (port of snippet_test.go) */
    RUN_TEST(snippet_exact_qn);
    RUN_TEST(snippet_qn_suffix);
    RUN_TEST(snippet_unique_short_name);
    RUN_TEST(snippet_name_tier);
    RUN_TEST(snippet_ambiguous_short_name);
    RUN_TEST(snippet_not_found);
    RUN_TEST(snippet_fuzzy_suggestions);
    RUN_TEST(snippet_enriched_properties);
    RUN_TEST(snippet_fuzzy_last_segment);
    RUN_TEST(snippet_auto_resolve_default);
    RUN_TEST(snippet_auto_resolve_enabled);
    RUN_TEST(snippet_include_neighbors_default);
    RUN_TEST(snippet_include_neighbors_enabled);
    RUN_TEST(snippet_source_invalid_utf8);
    RUN_TEST(tool_bad_project_name_no_overflow_issue235);
    RUN_TEST(tool_bad_project_error_valid_json_issue235);
    RUN_TEST(tool_resolve_store_by_internal_name_issue704);
    RUN_TEST(tool_list_projects_ignores_missed_shadow_issue1044);

    /* auto_watch gate (distilled from PR #625) */
    RUN_TEST(mcp_auto_watch_default_registers_watcher_on_connect);
    RUN_TEST(mcp_auto_watch_false_skips_watcher_on_connect);
    RUN_TEST(mcp_auto_watch_false_skips_supervised_autoindex_issue853);
}

/* Kept separate so daemon-coordination regressions can be iterated without
 * running the much larger MCP behavior suite. */
SUITE(mcp_mutation_guard) {
    RUN_TEST(tool_delete_project_mutation_guard_blocks_then_releases);
    RUN_TEST(tool_index_repository_mutation_guard_blocks_before_local_worker);
    RUN_TEST(tool_manage_adr_mutation_guard_balances_success);
    RUN_TEST(tool_manage_adr_mutation_guard_releases_on_missing_store);
    RUN_TEST(tool_raw_dispatch_cancel_is_scoped_non_mutating_and_next_request_clean);
    RUN_TEST(tool_outer_request_scope_preserves_predispatch_cancel);
    RUN_TEST(tool_index_repository_early_raw_cancel_survives_index_entry);
    RUN_TEST(tool_cross_repo_mutation_guard_sorts_dedupes_and_unwinds);
    RUN_TEST(tool_cross_repo_mutation_guard_casefolds_aliases_and_order);
    RUN_TEST(tool_cross_repo_rejects_wildcard_mixed_with_named_targets);
    RUN_TEST(tool_cross_repo_checks_cancellation_after_acquiring_leases);
    RUN_TEST(tool_cross_repo_missing_inputs_fail_without_creating_ghost_databases);
    RUN_TEST(tool_cross_repo_dedupes_targets_before_scanning_and_counting);
    RUN_TEST(tool_cross_repo_honors_source_name_override);
    RUN_TEST(tool_corrupt_store_cleanup_guard_is_balanced_and_not_nested);
    RUN_TEST(tool_corrupt_store_cleanup_guard_denial_preserves_db_and_wal);
    RUN_TEST(tool_corrupt_store_cleanup_rechecks_generation_after_guard_wait);
    RUN_TEST(tool_corrupt_store_cleanup_preserves_existing_backup_and_uses_unique_name);
    RUN_TEST(tool_corrupt_store_cleanup_publish_failure_preserves_db_and_wal);
    RUN_TEST(tool_corrupt_store_cleanup_publishes_complete_wal_snapshot_before_delete);
}
