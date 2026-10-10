/* Standalone original #2061 fixture; real pipeline/store, no resolver substitute.
 * Exit 0: all observations pass; 1: semantic mismatch; 2: setup/API error.
 * Retain the temporary repository and DB as evidence; see README.md.
 */
#include "pipeline/pipeline.h"
#include "store/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int passed, failed, errors;

static void observe(const char *name, int actual, int expected) {
    printf("OBS %s actual=%d expected=%d %s\n", name, actual, expected,
           actual == expected ? "PASS" : "FAIL");
    if (actual == expected)
        passed++;
    else
        failed++;
}

static int api_ok(const char *name, int rc) {
    if (rc == CBM_STORE_OK)
        return 1;
    fprintf(stderr, "ERROR %s rc=%d\n", name, rc);
    errors++;
    return 0;
}

static int write_source(const char *root, const char *name, const char *source) {
    char path[1024];
    if (snprintf(path, sizeof(path), "%s/repo/Sources/%s", root, name) >= (int)sizeof(path))
        return 0;
    FILE *f = fopen(path, "wx");
    if (!f)
        return 0;
    int ok = fputs(source, f) >= 0;
    if (fclose(f) != 0)
        ok = 0;
    return ok;
}

/* File/line identity works on P's unsuffixed and A's suffixed QNs alike. */
static int64_t find_node(cbm_store_t *s, const char *project, const char *name, const char *file,
                         int first, int last, int *total) {
    cbm_node_t *nodes = NULL;
    int count = 0, matches = 0;
    int64_t id = 0;
    if (!api_ok("find_nodes_by_name",
                cbm_store_find_nodes_by_name(s, project, name, &nodes, &count)))
        return 0;
    if (total)
        *total = count;
    for (int i = 0; i < count; i++) {
        const cbm_node_t *n = &nodes[i];
        printf("NODE name=%s id=%lld file=%s lines=%d-%d qn=%s\n", name, (long long)n->id,
               n->file_path ? n->file_path : "", n->start_line, n->end_line,
               n->qualified_name ? n->qualified_name : "");
        if (n->file_path && strcmp(n->file_path, file) == 0 && n->start_line == first &&
            n->end_line == last) {
            id = n->id;
            matches++;
        }
    }
    cbm_store_free_nodes(nodes, count);
    return matches == 1 ? id : 0;
}

/* Missing endpoints are unavailable (-1), never evidence that an edge is absent. */
static int calls(cbm_store_t *s, int64_t source, int64_t target) {
    if (!source || !target)
        return -1;
    cbm_edge_t *edges = NULL;
    int count = 0, found = 0;
    if (!api_ok("find_edges_by_source_type",
                cbm_store_find_edges_by_source_type(s, source, "CALLS", &edges, &count)))
        return -1;
    for (int i = 0; i < count; i++)
        if (edges[i].target_id == target)
            found = 1;
    cbm_store_free_edges(edges, count);
    return found;
}

/* Traverse stored CALLS only, for the issue's inbound depth=3. Depth bounds
 * cycles; no candidate cap can silently discard the offending caller.
 * This checks graph reachability, not the trace_path presentation layer.
 */
static int inbound(cbm_store_t *s, int64_t target, int64_t caller, int depth) {
    if (!target || !caller)
        return -1;
    if (!depth)
        return 0;
    cbm_edge_t *edges = NULL;
    int count = 0, found = 0;
    if (!api_ok("find_edges_by_target_type",
                cbm_store_find_edges_by_target_type(s, target, "CALLS", &edges, &count)))
        return -1;
    for (int i = 0; i < count; i++) {
        printf("INBOUND remaining_depth=%d source=%lld target=%lld\n", depth,
               (long long)edges[i].source_id, (long long)target);
        if (edges[i].source_id == caller)
            found = 1;
        int nested = inbound(s, edges[i].source_id, caller, depth - 1);
        if (nested < 0)
            found = -1;
        else if (nested && found >= 0)
            found = 1;
    }
    cbm_store_free_edges(edges, count);
    return found;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s /absolute/fresh-evidence-directory\n", argv[0]);
        return 2;
    }
    char repo[1024], sources[1024], db[1024];
    if (argv[1][0] != '/' ||
        snprintf(repo, sizeof(repo), "%s/repo", argv[1]) >= (int)sizeof(repo) ||
        snprintf(sources, sizeof(sources), "%s/repo/Sources", argv[1]) >= (int)sizeof(sources) ||
        snprintf(db, sizeof(db), "%s/graph.db", argv[1]) >= (int)sizeof(db) ||
        mkdir(argv[1], 0700) != 0 || mkdir(repo, 0700) != 0 || mkdir(sources, 0700) != 0) {
        perror("fresh evidence directories");
        return 2;
    }
    /* Verbatim Swift code blocks from github.com/DeusData/codebase-memory-mcp/issues/2061. */
    if (!write_source(argv[1], "Sink.swift", "class Sink {\n\tfunc target() {}\n}\n") ||
        !write_source(argv[1], "Service.swift",
                      "class Service {\n"
                      "\tlet sink = Sink()\n\n"
                      "\t// Overload A: DOES call target()\n"
                      "\tfunc work(flag: Bool) {\n"
                      "\t\tself.sink.target()\n\t}\n\n"
                      "\t// Overload B: does NOT call target()\n"
                      "\tfunc work(name: String) {\n"
                      "\t\tprint(name)\n\t}\n}\n") ||
        !write_source(argv[1], "Caller.swift",
                      "class Caller {\n"
                      "\tlet service = Service()\n\n"
                      "\t// Calls ONLY overload B, which never reaches target()\n"
                      "\tfunc onlyCallsOverloadB() {\n"
                      "\t\tself.service.work(name: \"x\")\n\t}\n}\n")) {
        perror("write fixture");
        return 2;
    }
    printf("EVIDENCE root=%s mode=fast\n", argv[1]);
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FAST);
    if (!p) {
        fprintf(stderr, "ERROR pipeline_new\n");
        return 2;
    }
    /* Stable project name avoids differing path-derived graph identities. */
    if (!cbm_pipeline_set_project_name(p, "issue2061-swift-identity")) {
        fprintf(stderr, "ERROR set_project_name\n");
        cbm_pipeline_free(p);
        return 2;
    }
    int rc = cbm_pipeline_run(p);
    if (rc != 0) {
        fprintf(stderr, "ERROR pipeline_run rc=%d (not semantic RED)\n", rc);
        cbm_pipeline_free(p);
        return 2;
    }
    cbm_store_t *s = cbm_store_open_path(db);
    if (!s) {
        fprintf(stderr, "ERROR store_open\n");
        cbm_pipeline_free(p);
        return 2;
    }
    const char *project = cbm_pipeline_project_name(p);
    int work_count = -1;
    int64_t flag = find_node(s, project, "work", "Sources/Service.swift", 5, 7, &work_count);
    int64_t name = find_node(s, project, "work", "Sources/Service.swift", 10, 12, NULL);
    int64_t target = find_node(s, project, "target", "Sources/Sink.swift", 2, 2, NULL);
    int64_t caller =
        find_node(s, project, "onlyCallsOverloadB", "Sources/Caller.swift", 5, 7, NULL);
    observe("two_work_nodes", work_count, 2);
    observe("flag_node_at_5_7", flag != 0, 1);
    observe("name_node_at_10_12", name != 0, 1);
    observe("target_node", target != 0, 1);
    observe("caller_node", caller != 0, 1);
    observe("flag_calls_target", calls(s, flag, target), 1);
    observe("name_does_not_call_target", calls(s, name, target), 0);
    observe("caller_calls_name", calls(s, caller, name), 1);
    observe("caller_does_not_call_flag", calls(s, caller, flag), 0);
    observe("target_inbound_depth3_has_no_false_caller", inbound(s, target, caller, 3), 0);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    printf("SUMMARY passed=%d failed=%d errors=%d exit=%d\n", passed, failed, errors,
           errors   ? 2
           : failed ? 1
                    : 0);
    return errors ? 2 : failed ? 1 : 0;
}
