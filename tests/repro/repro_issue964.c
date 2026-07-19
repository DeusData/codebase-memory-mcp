/*
 * repro_issue964.c — Reproduce-first case for OPEN bug #964.
 *
 * Issue: #964 — "C/C++ #include not mapped as IMPORTS edges — expected or a
 * bug?" (triaged as a bug: header files look disconnected; the reporter's
 * zero-inbound query returns 194 of 214 files on a PlatformIO project).
 *
 * Root cause (deeper than the report guesses):
 *   cbm_pipeline_fqn_compute (src/pipeline/fqn.c) strips the file extension,
 *   so a header and its same-stem source collide on BOTH derived QNs:
 *     NodeController.h   -> module "proj.NodeController",
 *                           file   "proj.NodeController.__file__"
 *     NodeController.cpp -> module "proj.NodeController",
 *                           file   "proj.NodeController.__file__"
 *   Node upserts match by QN, so the header's File node is merged into the
 *   source's — the header has NO node of its own in the graph at all. The
 *   `#include "NodeController.h"` import then resolves to the shared module
 *   (named after the .cpp), and the header can never receive an inbound
 *   IMPORTS edge. Headers without a same-stem sibling keep a node but the
 *   include-edge targeting still lands on the module, not the header file.
 *
 *   NOTE: the module-QN unification itself is (at least partly) load-bearing
 *   for C/C++ — header declarations and source definitions sharing a module
 *   QN is what lets cross-file call resolution join them. The fix must give
 *   headers their own FILE identity + point include edges at it WITHOUT
 *   splitting the shared module QN. Both File-node creation sites (pipeline.c
 *   full build; pipeline_incremental.c changed-file re-creation) must agree,
 *   and the incremental site is concurrently being fixed by PR #995 (#994) —
 *   implement this fix on top of that landing, never in parallel with it.
 *
 * Expected (correct) behaviour:
 *   1. NodeController.h has its own File node (distinct from the .cpp's).
 *   2. main.cpp's `#include "NodeController.h"` produces an IMPORTS edge
 *      whose target is the HEADER's node, so the header is not disconnected.
 *
 * Why RED on current code: the header's File node does not exist (merged by
 * QN collision), so both assertions fail.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* True if a File-labelled node with exactly `name` exists. */
static int r964_file_node_exists(cbm_store_t *store, const char *project, const char *name) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, "File", &nodes, &count) != CBM_STORE_OK)
        return 0;
    int found = 0;
    for (int i = 0; i < count; i++) {
        if (nodes[i].name && strcmp(nodes[i].name, name) == 0)
            found = 1;
    }
    cbm_store_free_nodes(nodes, count);
    return found;
}

/* Count INBOUND edges (any of the reporter's connection types) landing on the
 * File node named `name` — the exact disconnection metric from the issue's
 * query `NOT EXISTS { (f)<-[:IMPORTS|CALLS|CONTAINS_FILE|DEFINES|USAGE]-() }`.
 * A header that used to have no node at all (extension-stripped QN collision)
 * now has its own File node AND is connected (CONTAINS_FILE from its folder,
 * plus DEFINES of the class it declares); `#include` itself resolves to the
 * declared Class node via #983, not to the header File. */
static int r964_file_inbound(cbm_store_t *store, const char *project, const char *name) {
    static const char *const types[] = {"IMPORTS", "CALLS", "CONTAINS_FILE",
                                         "DEFINES", "USAGE",  NULL};
    int total = 0;
    for (int t = 0; types[t]; t++) {
        cbm_edge_t *edges = NULL;
        int n = 0;
        if (cbm_store_find_edges_by_type(store, project, types[t], &edges, &n) != CBM_STORE_OK)
            continue;
        for (int i = 0; i < n; i++) {
            cbm_node_t tgt;
            if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
                continue;
            if (tgt.name && strcmp(tgt.name, name) == 0)
                total++;
            cbm_node_free_fields(&tgt);
        }
        cbm_store_free_edges(edges, n);
    }
    return total;
}

TEST(repro_issue964_header_has_node_and_is_connected) {
    static const RFile files[] = {
        {"NodeController.h", "#pragma once\n"
                             "class NodeController {\n"
                             "public:\n"
                             "    void run();\n"
                             "};\n"},
        {"NodeController.cpp", "#include \"NodeController.h\"\n"
                               "void NodeController::run() {}\n"},
        {"main.cpp", "#include \"NodeController.h\"\n"
                     "#include <vector>\n"
                     "\n"
                     "int main() {\n"
                     "    NodeController c;\n"
                     "    c.run();\n"
                     "    return 0;\n"
                     "}\n"}};

    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, 3);
    ASSERT_NOT_NULL(store);

    int header_node = r964_file_node_exists(store, lp.project, "NodeController.h");
    int header_inbound = r964_file_inbound(store, lp.project, "NodeController.h");
    if (!header_node || header_inbound < 1) {
        fprintf(stderr,
                "  [964] FAIL header_file_node=%d inbound_edges=%d (header merged into "
                "same-stem .cpp by extension-stripped QN collision, or left disconnected)\n",
                header_node, header_inbound);
    }
    ASSERT_TRUE(header_node);         /* header keeps its own File node (not merged) */
    ASSERT_TRUE(header_inbound >= 1); /* and is connected (not the reporter's zero-inbound) */

    rh_cleanup(&lp, store);
    PASS();
}

SUITE(repro_issue964) {
    RUN_TEST(repro_issue964_header_has_node_and_is_connected);
}
