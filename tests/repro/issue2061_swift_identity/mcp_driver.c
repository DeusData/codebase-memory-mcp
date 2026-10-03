/* Read the reviewed A graph COPY through production MCP only. No indexing,
 * store queries, graph traversal, resolver replacement, or fixture creation.
 * Exit 0: all response checks pass; 1: mismatch; 2: setup/envelope error.
 */
#include "mcp/mcp.h"
#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int passed, failed, errors;

static void check(const char *name, bool ok) {
    printf("OBS %s %s\n", name, ok ? "PASS" : "FAIL");
    if (ok)
        passed++;
    else
        failed++;
}

static bool number(yyjson_val *obj, const char *key, int expected) {
    yyjson_val *v = yyjson_obj_get(obj, key);
    return yyjson_is_int(v) && yyjson_get_int(v) == expected;
}

static bool complete(yyjson_val *obj) {
    const char *flags[] = {"truncated", "has_more", "engine_saturated",
                           "output_budget_floor_exceeded"};
    const char *cursors[] = {"next", "next_cursor", "next_offset", "cursor"};
    for (size_t i = 0; i < sizeof(flags) / sizeof(flags[0]); i++) {
        yyjson_val *v = yyjson_obj_get(obj, flags[i]);
        if (v && !yyjson_is_false(v))
            return false;
    }
    for (size_t i = 0; i < sizeof(cursors) / sizeof(cursors[0]); i++) {
        if (yyjson_obj_get(obj, cursors[i]))
            return false;
    }
    return true;
}

/* Decode returned presentation rows, never graph data. Every row must match;
 * no filtering can turn an unwanted row into a successful absence check. */
static bool rows(yyjson_val *obj, const char *project, bool search, const char *suffix,
                 int expected) {
    yyjson_val *cols = yyjson_obj_get(obj, "cols");
    yyjson_val *groups = yyjson_obj_get(obj, "groups");
    if (!yyjson_is_arr(cols) || !yyjson_is_arr(groups) ||
        !yyjson_equals_str(yyjson_arr_get(cols, 0), "name") ||
        !yyjson_equals_str(yyjson_arr_get(cols, 1), search ? "label" : "hop") ||
        yyjson_arr_size(cols) != (search ? 5U : 2U))
        return false;
    int count = 0, flag = 0, name = 0;
    size_t gi, gm, ri, rm;
    yyjson_val *group, *row;
    yyjson_arr_foreach(groups, gi, gm, group) {
        const char *prefix = yyjson_get_str(yyjson_obj_get(group, "qn_prefix"));
        yyjson_val *values = yyjson_obj_get(group, "rows");
        if (!prefix || !yyjson_is_arr(values))
            return false;
        yyjson_arr_foreach(values, ri, rm, row) {
            const char *leaf = yyjson_get_str(yyjson_arr_get(row, 0));
            char qn[2048], wanted[2048];
            if (!leaf || !yyjson_is_arr(row) || yyjson_arr_size(row) != yyjson_arr_size(cols) ||
                snprintf(qn, sizeof(qn), "%s%s%s", prefix, *prefix ? "." : "", leaf) >=
                    (int)sizeof(qn))
                return false;
            count++;
            if (search) {
                snprintf(wanted, sizeof(wanted), "%s.Sources.Service.Service.work(flag:Bool)",
                         project);
                if (strcmp(qn, wanted) == 0 &&
                    yyjson_equals_str(yyjson_arr_get(row, 2), "5-7"))
                    flag++;
                else {
                    snprintf(wanted, sizeof(wanted), "%s.Sources.Service.Service.work(name:String)",
                             project);
                    if (strcmp(qn, wanted) != 0 ||
                        !yyjson_equals_str(yyjson_arr_get(row, 2), "10-12"))
                        return false;
                    name++;
                }
                if (!yyjson_equals_str(yyjson_obj_get(group, "file"), "Sources/Service.swift") ||
                    !yyjson_equals_str(yyjson_arr_get(row, 1), "Method"))
                    return false;
            } else {
                snprintf(wanted, sizeof(wanted), "%s.%s", project, suffix);
                yyjson_val *hop = yyjson_arr_get(row, 1);
                if (strcmp(qn, wanted) != 0 || !yyjson_is_int(hop) || yyjson_get_int(hop) != 1)
                    return false;
            }
        }
    }
    return count == expected && (!search || (flag == 1 && name == 1));
}

static void request(cbm_mcp_server_t *srv, const char *project, int id, const char *tool,
                    const char *args, bool search, bool tree, const char *suffix, int expected) {
    char input[4096], label[64];
    snprintf(label, sizeof(label), "request_%d", id);
    int n = snprintf(input, sizeof(input),
                     "{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"tools/call\","
                     "\"params\":{\"name\":\"%s\",\"arguments\":%s}}", id, tool, args);
    if (n < 0 || n >= (int)sizeof(input)) {
        errors++;
        return;
    }
    printf("REQUEST %d %s\n", id, input);
    char *raw = cbm_mcp_server_handle(srv, input);
    printf("RAW %d %s\n", id, raw ? raw : "null");
    yyjson_doc *doc = raw ? yyjson_read(raw, strlen(raw), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *result = yyjson_obj_get(root, "result");
    yyjson_val *content = yyjson_obj_get(result, "content");
    yyjson_val *item = yyjson_arr_get(content, 0);
    const char *text = yyjson_get_str(yyjson_obj_get(item, "text"));
    yyjson_val *structured = yyjson_obj_get(result, "structuredContent");
    bool envelope = yyjson_equals_str(yyjson_obj_get(root, "jsonrpc"), "2.0") &&
                    number(root, "id", id) && !yyjson_obj_get(root, "error") &&
                    yyjson_is_obj(result) && yyjson_is_false(yyjson_obj_get(result, "isError")) &&
                    yyjson_is_arr(content) && yyjson_arr_size(content) == 1 &&
                    yyjson_equals_str(yyjson_obj_get(item, "type"), "text") && text;
    if (!envelope) {
        fprintf(stderr, "ERROR request_%d invalid/error envelope\n", id);
        errors++;
    } else if (tree) {
        /* This singleton fixture selects the smaller direct table. Exact text
         * checks total, relation, one identity/hop, and absence of continuation
         * or extra rows; an encoding change is a reviewable mismatch. */
        char wanted[2048];
        snprintf(wanted, sizeof(wanted),
                 "function: target\ndirection: inbound\ncallers_total: 1\n"
                 "callers_total_relation: eq\ncallers: 1  (cols: qn hop)\n"
                 "  %s.Sources.Service.Service.work(flag:Bool) 1\n", project);
        check(label, !structured && strcmp(text, wanted) == 0);
    } else {
        yyjson_doc *payload = yyjson_read(text, strlen(text), 0);
        yyjson_val *body = payload ? yyjson_doc_get_root(payload) : NULL;
        bool ok = yyjson_is_obj(structured) && yyjson_is_obj(body) &&
                  yyjson_equals(structured, body) && complete(body);
        if (search) {
            ok = ok && number(body, "total", 2) && number(body, "returned", 2) &&
                 rows(body, project, true, NULL, 2);
        } else {
            const char *leg = id == 2 ? "callers" : "callees";
            const char *total = id == 2 ? "callers_total" : "callees_total";
            const char *relation = id == 2 ? "callers_total_relation" : "callees_total_relation";
            ok = ok && number(body, total, expected) &&
                 yyjson_equals_str(yyjson_obj_get(body, relation), "eq") &&
                 rows(yyjson_obj_get(body, leg), project, false, suffix, expected);
        }
        check(label, ok);
        yyjson_doc_free(payload);
    }
    yyjson_doc_free(doc);
    free(raw);
}

int main(int argc, char **argv) {
    if (argc != 2 || strcmp(argv[1], "issue2061-swift-identity") != 0 ||
        !getenv("CBM_CACHE_DIR") || !getenv("CBM_ALLOWED_ROOT")) {
        fprintf(stderr, "usage: mcp-driver issue2061-swift-identity; set isolated "
                        "CBM_CACHE_DIR and CBM_ALLOWED_ROOT\n");
        return 2;
    }
    const char *project = argv[1];
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv)
        return 2;
    cbm_mcp_server_set_background_tasks(srv, false);
    cbm_mcp_server_set_tool_profile(srv, CBM_MCP_TOOL_PROFILE_ANALYSIS);
    if (!cbm_mcp_server_set_session_context(srv, getenv("CBM_ALLOWED_ROOT"),
                                           getenv("CBM_ALLOWED_ROOT"))) {
        cbm_mcp_server_free(srv);
        return 2;
    }
    char args[2048];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"name_pattern\":\"^work$\","
             "\"format\":\"json\",\"limit\":20}", project);
    request(srv, project, 1, "search_graph", args, true, false, NULL, 2);
    for (int id = 2; id <= 5; id++) {
        const char *function = id <= 3 ? "target" : id == 4 ? "onlyCallsOverloadB" :
            "issue2061-swift-identity.Sources.Service.Service.work(name:String)";
        snprintf(args, sizeof(args), "{\"project\":\"%s\",\"function_name\":\"%s\","
                 "\"direction\":\"%s\",\"depth\":%d,\"limit\":100,"
                 "\"max_output_tokens\":3200%s}", project, function,
                 id <= 3 ? "inbound" : "outbound", id <= 3 ? 3 : 1,
                 id == 3 ? "" : ",\"format\":\"json\"");
        request(srv, project, id, "trace_path", args, false, id == 3,
                id == 2 ? "Sources.Service.Service.work(flag:Bool)" :
                          "Sources.Service.Service.work(name:String)", id == 5 ? 0 : 1);
    }
    cbm_mcp_server_free(srv);
    int rc = errors ? 2 : failed ? 1 : 0;
    printf("SUMMARY passed=%d failed=%d errors=%d exit=%d\n", passed, failed, errors, rc);
    return rc;
}
