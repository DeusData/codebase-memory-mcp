/*
 * query_dependencies.c — Package and module dependency DAG extraction.
 */
#include "diagram/diagram.h"
#include "store/store.h"
#include "foundation/str_util.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cbm_diagram_query_dependencies(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                                   cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                                   char *errbuf, size_t errbuf_cap) {
    if (!store || !opts || !graph) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Invalid arguments to query_dependencies");
        return -1;
    }

    sqlite3 *db = (sqlite3 *)cbm_store_get_db(store);
    if (!db) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Failed to get database handle");
        return -1;
    }

    const char *project = (opts->project && opts->project[0] != '\0') ? opts->project : "";
    const char *scope_path = (opts->scope_path && opts->scope_path[0] != '\0') ? opts->scope_path : NULL;

    int node_count = 0;
    int edge_count = 0;

    /* 1. If packages exist, query package-level IMPORTS */
    const char *sql_pkg_imports =
        "SELECT n1.name, n2.name, count(*) "
        "FROM edges e "
        "JOIN nodes n1 ON e.source_id = n1.id "
        "JOIN nodes n2 ON e.target_id = n2.id "
        "WHERE (e.project = ?1 OR ?1 = '') AND e.type = 'IMPORTS' "
        "  AND n1.label = 'Package' AND n2.label = 'Package' "
        "GROUP BY n1.name, n2.name "
        "LIMIT 100;";

    sqlite3_stmt *stmt = NULL;
    bool found_package_deps = false;

    if (sqlite3_prepare_v2(db, sql_pkg_imports, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_STATIC);

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *from_name = (const char *)sqlite3_column_text(stmt, 0);
            const char *to_name = (const char *)sqlite3_column_text(stmt, 1);
            int count = sqlite3_column_int(stmt, 2);
            if (!from_name || !to_name) continue;

            char from_id[128], to_id[128];
            cbm_diagram_sanitize_id(from_name, from_id, sizeof(from_id));
            cbm_diagram_sanitize_id(to_name, to_id, sizeof(to_id));

            cbm_diag_graph_add_node(graph, from_id, from_name, "Packages", "rounded", false);
            cbm_diag_graph_add_node(graph, to_id, to_name, "Packages", "rounded", false);

            char edge_lbl[64] = "";
            if (count > 1) {
                snprintf(edge_lbl, sizeof(edge_lbl), "%d", count);
            }
            cbm_diag_graph_add_edge(graph, from_id, to_id, edge_lbl, "solid", count);

            node_count += 2;
            edge_count++;
            found_package_deps = true;
        }
        sqlite3_finalize(stmt);
    }

    /* 2. If no package nodes or if scoped, aggregate by file/folder directory prefixes */
    if (!found_package_deps) {
        const char *sql_files =
            "SELECT n1.file_path, n2.file_path, count(*) "
            "FROM edges e "
            "JOIN nodes n1 ON e.source_id = n1.id "
            "JOIN nodes n2 ON e.target_id = n2.id "
            "WHERE (e.project = ?1 OR ?1 = '') AND e.type = 'IMPORTS' "
            "  AND n1.file_path != '' AND n2.file_path != '' "
            "  AND n1.file_path != n2.file_path "
            "  AND (?2 IS NULL OR n1.file_path LIKE ?2 || '%' OR n2.file_path LIKE ?2 || '%') "
            "GROUP BY n1.file_path, n2.file_path "
            "LIMIT 50;";

        if (sqlite3_prepare_v2(db, sql_files, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, project, -1, SQLITE_STATIC);
            if (scope_path) {
                sqlite3_bind_text(stmt, 2, scope_path, -1, SQLITE_STATIC);
            } else {
                sqlite3_bind_null(stmt, 2);
            }

            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *f1 = (const char *)sqlite3_column_text(stmt, 0);
                const char *f2 = (const char *)sqlite3_column_text(stmt, 1);
                int count = sqlite3_column_int(stmt, 2);
                if (!f1 || !f2) continue;

                const char *base1 = cbm_path_base(f1);
                const char *base2 = cbm_path_base(f2);

                char id1[128], id2[128];
                cbm_diagram_sanitize_id(base1, id1, sizeof(id1));
                cbm_diagram_sanitize_id(base2, id2, sizeof(id2));

                cbm_diag_graph_add_node(graph, id1, base1, "Modules", "box", false);
                cbm_diag_graph_add_node(graph, id2, base2, "Modules", "box", false);

                char edge_lbl[64] = "";
                if (count > 1) {
                    snprintf(edge_lbl, sizeof(edge_lbl), "%d", count);
                }
                cbm_diag_graph_add_edge(graph, id1, id2, edge_lbl, "solid", count);

                node_count += 2;
                edge_count++;
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Fallback if no imports recorded: query cross-package boundaries from architecture */
    if (edge_count == 0) {
        cbm_diagram_opts_t arch_opts = *opts;
        arch_opts.type = CBM_DIAGRAM_ARCHITECTURE;
        return cbm_diagram_query_arch(store, &arch_opts, graph, nodes_analyzed, edges_traversed,
                                      errbuf, errbuf_cap);
    }

    if (nodes_analyzed) *nodes_analyzed = node_count;
    if (edges_traversed) *edges_traversed = edge_count;

    return 0;
}
