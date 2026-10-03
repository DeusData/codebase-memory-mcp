/*
 * query_flow.c — Data flow extraction: Ingress -> Handlers -> Storage -> Egress.
 */
#include "diagram/diagram.h"
#include "store/store.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cbm_diagram_query_flow(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                           cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                           char *errbuf, size_t errbuf_cap) {
    if (!store || !opts || !graph) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Invalid arguments to query_flow");
        return -1;
    }

    sqlite3 *db = (sqlite3 *)cbm_store_get_db(store);
    if (!db) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Failed to get database handle");
        return -1;
    }

    const char *project = (opts->project && opts->project[0] != '\0') ? opts->project : "";
    const char *entry_point = (opts->entry_point && opts->entry_point[0] != '\0') ? opts->entry_point : NULL;

    /* Add the four standard stages as groups */
    cbm_diag_graph_add_group(graph, "Ingress", "Ingress & API Endpoints");
    cbm_diag_graph_add_group(graph, "Handlers", "Processing & Business Logic");
    cbm_diag_graph_add_group(graph, "Storage", "Persistent Storage & Data Access");
    cbm_diag_graph_add_group(graph, "Egress", "Egress & External Services");

    int node_count = 0;
    int edge_count = 0;

    /* 1. Find Ingress Nodes */
    sqlite3_stmt *stmt_ingress = NULL;
    const char *sql_ingress;
    if (entry_point) {
        sql_ingress =
            "SELECT id, name, label, file_path FROM nodes "
            "WHERE (project = ?1 OR ?1 = '') AND (name = ?2 OR qualified_name = ?2 OR label = 'Route') "
            "LIMIT 10;";
    } else {
        sql_ingress =
            "SELECT id, name, label, file_path FROM nodes "
            "WHERE (project = ?1 OR ?1 = '') AND (label = 'Route' OR name LIKE 'POST %' OR name LIKE 'GET %') "
            "LIMIT 10;";
    }

    if (sqlite3_prepare_v2(db, sql_ingress, -1, &stmt_ingress, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt_ingress, 1, project, -1, SQLITE_STATIC);
        if (entry_point) {
            sqlite3_bind_text(stmt_ingress, 2, entry_point, -1, SQLITE_STATIC);
        }

        typedef struct {
            int64_t id;
            char name[128];
            char sid[128];
        } ingress_item_t;

        ingress_item_t ingress_items[16];
        int ingress_count = 0;

        while (sqlite3_step(stmt_ingress) == SQLITE_ROW && ingress_count < 16) {
            int64_t id = sqlite3_column_int64(stmt_ingress, 0);
            const char *name = (const char *)sqlite3_column_text(stmt_ingress, 1);
            if (!name) name = "Endpoint";

            ingress_items[ingress_count].id = id;
            snprintf(ingress_items[ingress_count].name, sizeof(ingress_items[ingress_count].name), "%s", name);
            cbm_diagram_sanitize_id(name, ingress_items[ingress_count].sid, sizeof(ingress_items[ingress_count].sid));

            cbm_diag_graph_add_node(graph, ingress_items[ingress_count].sid, name, "Ingress", "box", false);
            ingress_count++;
            node_count++;
        }
        sqlite3_finalize(stmt_ingress);

        /* 2. For each ingress item, find handlers via HANDLES or CALLS */
        const char *sql_handlers =
            "SELECT n.id, n.name, n.label, n.file_path, e.type "
            "FROM edges e "
            "JOIN nodes n ON (e.source_id = n.id OR e.target_id = n.id) "
            "WHERE (e.source_id = ?1 OR e.target_id = ?1) AND e.type IN ('HANDLES', 'CALLS') AND n.id != ?1 "
            "LIMIT 10;";

        sqlite3_stmt *stmt_h = NULL;
        if (sqlite3_prepare_v2(db, sql_handlers, -1, &stmt_h, NULL) == SQLITE_OK) {
            for (int i = 0; i < ingress_count; i++) {
                sqlite3_reset(stmt_h);
                sqlite3_clear_bindings(stmt_h);
                sqlite3_bind_int64(stmt_h, 1, ingress_items[i].id);

                int64_t handler_ids[8];
                char handler_sids[8][128];
                int h_count = 0;

                while (sqlite3_step(stmt_h) == SQLITE_ROW && h_count < 8) {
                    int64_t hid = sqlite3_column_int64(stmt_h, 0);
                    const char *hname = (const char *)sqlite3_column_text(stmt_h, 1);
                    if (!hname) hname = "handler";

                    handler_ids[h_count] = hid;
                    cbm_diagram_sanitize_id(hname, handler_sids[h_count], sizeof(handler_sids[h_count]));

                    cbm_diag_graph_add_node(graph, handler_sids[h_count], hname, "Handlers", "rounded", false);
                    cbm_diag_graph_add_edge(graph, ingress_items[i].sid, handler_sids[h_count], "handles", "solid", 1);
                    edge_count++;
                    node_count++;
                    h_count++;
                }

                /* 3. For each handler, query READS and WRITES */
                const char *sql_rw =
                    "SELECT n.name, e.type "
                    "FROM edges e "
                    "JOIN nodes n ON e.target_id = n.id "
                    "WHERE e.source_id = ?1 AND e.type IN ('READS', 'WRITES') "
                    "LIMIT 10;";

                sqlite3_stmt *stmt_rw = NULL;
                if (sqlite3_prepare_v2(db, sql_rw, -1, &stmt_rw, NULL) == SQLITE_OK) {
                    for (int h = 0; h < h_count; h++) {
                        sqlite3_reset(stmt_rw);
                        sqlite3_clear_bindings(stmt_rw);
                        sqlite3_bind_int64(stmt_rw, 1, handler_ids[h]);

                        while (sqlite3_step(stmt_rw) == SQLITE_ROW) {
                            const char *store_name = (const char *)sqlite3_column_text(stmt_rw, 0);
                            const char *edge_type = (const char *)sqlite3_column_text(stmt_rw, 1);
                            if (!store_name) store_name = "database";

                            char store_sid[128];
                            cbm_diagram_sanitize_id(store_name, store_sid, sizeof(store_sid));

                            cbm_diag_graph_add_node(graph, store_sid, store_name, "Storage", "cylinder", false);
                            cbm_diag_graph_add_edge(graph, handler_sids[h], store_sid,
                                                    edge_type ? edge_type : "ACCESS", "solid", 1);
                            edge_count++;
                            node_count++;
                        }
                    }
                    sqlite3_finalize(stmt_rw);
                }
            }
            sqlite3_finalize(stmt_h);
        }
    }

    /* Fallback if no routes found: find any top entry point functions */
    if (node_count == 0) {
        const char *sql_fallback =
            "SELECT n.id, n.name, n.file_path "
            "FROM nodes n "
            "WHERE (n.project = ?1 OR ?1 = '') AND n.label IN ('Function', 'Method') "
            "ORDER BY n.id ASC LIMIT 5;";

        sqlite3_stmt *stmt_fb = NULL;
        if (sqlite3_prepare_v2(db, sql_fallback, -1, &stmt_fb, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt_fb, 1, project, -1, SQLITE_STATIC);
            while (sqlite3_step(stmt_fb) == SQLITE_ROW) {
                const char *fname = (const char *)sqlite3_column_text(stmt_fb, 1);
                if (!fname) fname = "entry";

                char sid[128];
                cbm_diagram_sanitize_id(fname, sid, sizeof(sid));
                cbm_diag_graph_add_node(graph, sid, fname, "Ingress", "rounded", false);
                node_count++;
            }
            sqlite3_finalize(stmt_fb);
        }
    }

    if (nodes_analyzed) *nodes_analyzed = node_count;
    if (edges_traversed) *edges_traversed = edge_count;

    return 0;
}
