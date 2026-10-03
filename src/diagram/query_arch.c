/*
 * query_arch.c — Subsystem and package architecture extraction.
 */
#include "diagram/diagram.h"
#include "store/store.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cbm_diagram_query_arch(cbm_store_t *store, const cbm_diagram_opts_t *opts,
                           cbm_diag_graph_t *graph, int *nodes_analyzed, int *edges_traversed,
                           char *errbuf, size_t errbuf_cap) {
    if (!store || !opts || !graph) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Invalid arguments to query_arch");
        return -1;
    }

    const char *project = (opts->project && opts->project[0] != '\0') ? opts->project : "";
    const char *scope_path = (opts->scope_path && opts->scope_path[0] != '\0') ? opts->scope_path : NULL;

    const char *aspects[] = {"packages", "layers", "boundaries", "services"};
    int aspect_count = 4;

    cbm_architecture_info_t arch;
    memset(&arch, 0, sizeof(arch));

    int rc = cbm_store_get_architecture(store, project, scope_path, aspects, aspect_count, &arch);
    if (rc != CBM_STORE_OK) {
        if (errbuf) snprintf(errbuf, errbuf_cap, "Failed to retrieve architecture info from store");
        return -1;
    }

    /* 1. Register layers as groups if present */
    for (int i = 0; i < arch.layer_count; i++) {
        if (arch.layers[i].layer && arch.layers[i].layer[0] != '\0') {
            cbm_diag_graph_add_group(graph, arch.layers[i].layer, arch.layers[i].layer);
        }
    }

    /* Helper lambda/lookup for a package's layer */
    #define GET_PACKAGE_LAYER(pkg_name) ({ \
        const char *l = "Packages"; \
        for (int k = 0; k < arch.layer_count; k++) { \
            if (arch.layers[k].name && strcmp(arch.layers[k].name, pkg_name) == 0) { \
                l = arch.layers[k].layer ? arch.layers[k].layer : "Packages"; \
                break; \
            } \
        } \
        l; \
    })

    /* 2. Add package nodes */
    int node_count = 0;
    for (int i = 0; i < arch.package_count; i++) {
        const char *name = arch.packages[i].name;
        if (!name || name[0] == '\0') continue;

        char sanitized_id[128];
        cbm_diagram_sanitize_id(name, sanitized_id, sizeof(sanitized_id));

        char label[256];
        if (arch.packages[i].node_count > 0) {
            snprintf(label, sizeof(label), "%s (%d symbols)", name, arch.packages[i].node_count);
        } else {
            snprintf(label, sizeof(label), "%s", name);
        }

        const char *layer_name = GET_PACKAGE_LAYER(name);
        cbm_diag_graph_add_group(graph, layer_name, layer_name);

        cbm_diag_graph_add_node(graph, sanitized_id, label, layer_name, "rounded", false);
        node_count++;
    }

    /* 3. Add boundary edges (cross-package calls) */
    int edge_count = 0;
    for (int i = 0; i < arch.boundary_count; i++) {
        const char *from = arch.boundaries[i].from;
        const char *to = arch.boundaries[i].to;
        if (!from || !to || from[0] == '\0' || to[0] == '\0') continue;

        char from_id[128], to_id[128];
        cbm_diagram_sanitize_id(from, from_id, sizeof(from_id));
        cbm_diagram_sanitize_id(to, to_id, sizeof(to_id));

        /* Detect circular dependency */
        bool is_cycle = false;
        for (int j = 0; j < i; j++) {
            if (arch.boundaries[j].from && arch.boundaries[j].to &&
                strcmp(arch.boundaries[j].from, to) == 0 &&
                strcmp(arch.boundaries[j].to, from) == 0) {
                is_cycle = true;
                break;
            }
        }

        char edge_label[128];
        if (arch.boundaries[i].call_count > 0) {
            snprintf(edge_label, sizeof(edge_label), "calls: %d", arch.boundaries[i].call_count);
        } else {
            edge_label[0] = '\0';
        }

        cbm_diag_graph_add_edge(graph, from_id, to_id, edge_label,
                                is_cycle ? "error" : "solid",
                                arch.boundaries[i].call_count);
        edge_count++;
    }

    /* 4. Add service links */
    for (int i = 0; i < arch.service_count; i++) {
        const char *from = arch.services[i].from;
        const char *to = arch.services[i].to;
        if (!from || !to || from[0] == '\0' || to[0] == '\0') continue;

        char from_id[128], to_id[128];
        cbm_diagram_sanitize_id(from, from_id, sizeof(from_id));
        cbm_diagram_sanitize_id(to, to_id, sizeof(to_id));

        char edge_label[128];
        snprintf(edge_label, sizeof(edge_label), "%s (%d)",
                 arch.services[i].type ? arch.services[i].type : "service",
                 arch.services[i].count);

        cbm_diag_graph_add_edge(graph, from_id, to_id, edge_label, "dashed", arch.services[i].count);
        edge_count++;
    }

    /* 4. Add fallback package-level imports if boundary call count is 0 */
    if (edge_count == 0) {
        sqlite3 *db = (sqlite3 *)cbm_store_get_db(store);
        if (db) {
            const char *sql_pkg_imports =
                "SELECT n1.name, n2.name, count(*) "
                "FROM edges e "
                "JOIN nodes n1 ON e.source_id = n1.id "
                "JOIN nodes n2 ON e.target_id = n2.id "
                "WHERE (e.project = ?1 OR ?1 = '') AND (e.type = 'IMPORTS' OR e.type = 'CALLS') "
                "  AND n1.label = 'Package' AND n2.label = 'Package' "
                "GROUP BY n1.name, n2.name "
                "LIMIT 100;";
            sqlite3_stmt *stmt = NULL;
            if (sqlite3_prepare_v2(db, sql_pkg_imports, -1, &stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_text(stmt, 1, project, -1, SQLITE_STATIC);
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    const char *from_name = (const char *)sqlite3_column_text(stmt, 0);
                    const char *to_name = (const char *)sqlite3_column_text(stmt, 1);
                    int count = sqlite3_column_int(stmt, 2);
                    if (from_name && to_name && strcmp(from_name, to_name) != 0) {
                        char from_id[128], to_id[128];
                        cbm_diagram_sanitize_id(from_name, from_id, sizeof(from_id));
                        cbm_diagram_sanitize_id(to_name, to_id, sizeof(to_id));
                        cbm_diag_graph_add_edge(graph, from_id, to_id, "imports", "solid", count);
                        edge_count++;
                    }
                }
                sqlite3_finalize(stmt);
            }
        }
    }

    cbm_store_architecture_free(&arch);

    if (nodes_analyzed) *nodes_analyzed = node_count;
    if (edges_traversed) *edges_traversed = edge_count;

    return 0;
}
