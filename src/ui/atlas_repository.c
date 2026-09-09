/* Semantic repository map, independent of the rendering/layout node budget. */
#include "ui/atlas.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define MAP_LABELS \
    "('File','Module','Function','Method','Class','Interface','Struct','Enum','Route')"
#define MAP_RELATIONS "('CALLS','IMPORTS','USAGE','INHERITS','IMPLEMENTS','DATA_FLOWS')"
#define MAP_SELECTED                                                 \
    "SELECT id FROM nodes WHERE project=?1 AND label IN " MAP_LABELS \
    " ORDER BY file_path,id LIMIT 20000"

static void map_text(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key, sqlite3_stmt *st,
                     int col) {
    const char *value = (const char *)sqlite3_column_text(st, col);
    if (value && *value)
        yyjson_mut_obj_add_strcpy(doc, obj, key, value);
}

char *cbm_atlas_repository_json(cbm_store_t *store, const char *project) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db || !project)
        return NULL;
    /* Counts, edges, source locations and generation describe one WAL snapshot. */
    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK)
        return NULL;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_val *nodes = yyjson_mut_arr(doc), *edges = yyjson_mut_arr(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_val(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_val(doc, root, "edges", edges);
    yyjson_mut_obj_add_str(doc, root, "node_scope",
                           "files, modules, callables, types and routes; variables omitted");
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(
            db,
            "SELECT indexed_at,(SELECT count(*) FROM nodes WHERE project=?1 AND label "
            "IN " MAP_LABELS "),"
            "(SELECT count(*) FROM edges WHERE project=?1 AND type IN " MAP_RELATIONS "),"
            "(SELECT count(*) FROM nodes WHERE project=?1) FROM projects WHERE name=?1",
            -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW)
        goto fail;
    map_text(doc, root, "indexed_at", st, 0);
    int64_t total_nodes = sqlite3_column_int64(st, 1), total_edges = sqlite3_column_int64(st, 2);
    yyjson_mut_obj_add_int(doc, root, "total_nodes", total_nodes);
    yyjson_mut_obj_add_int(doc, root, "total_edges", total_edges);
    yyjson_mut_obj_add_int(doc, root, "indexed_total_nodes", sqlite3_column_int64(st, 3));
    sqlite3_finalize(st);
    st = NULL;
    char generation[128] = {0};
    cbm_store_generation(store, generation, sizeof(generation));
    yyjson_mut_obj_add_strcpy(doc, root, "generation", generation);
    if (sqlite3_prepare_v2(db,
                           "SELECT id,label,name,qualified_name,file_path,start_line,end_line,"
                           "substr(json_extract(properties,'$.docstring'),1,280),"
                           "coalesce(json_extract(properties,'$.is_entry_point'),json_extract("
                           "properties,'$.is_entry'),0),"
                           "coalesce(json_extract(properties,'$.is_test'),0),"
                           "coalesce(json_extract(properties,'$.is_exported'),0) "
                           "FROM nodes WHERE project=?1 AND label IN " MAP_LABELS
                           " ORDER BY file_path,id LIMIT 20000",
                           -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        yyjson_mut_val *node = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, node, "id", sqlite3_column_int64(st, 0));
        map_text(doc, node, "label", st, 1);
        map_text(doc, node, "name", st, 2);
        map_text(doc, node, "qualified_name", st, 3);
        const char *qualified_name = (const char *)sqlite3_column_text(st, 3);
        if (qualified_name)
            yyjson_mut_obj_add_strcpy(doc, node, "package_name", cbm_qn_to_package(qualified_name));
        map_text(doc, node, "file_path", st, 4);
        yyjson_mut_obj_add_int(doc, node, "start_line", sqlite3_column_int64(st, 5));
        yyjson_mut_obj_add_int(doc, node, "end_line", sqlite3_column_int64(st, 6));
        map_text(doc, node, "docstring", st, 7);
        yyjson_mut_obj_add_bool(doc, node, "is_entry", sqlite3_column_int(st, 8) != 0);
        yyjson_mut_obj_add_bool(doc, node, "is_test", sqlite3_column_int(st, 9) != 0);
        yyjson_mut_obj_add_bool(doc, node, "is_exported", sqlite3_column_int(st, 10) != 0);
        yyjson_mut_arr_add_val(nodes, node);
    }
    if (rc != SQLITE_DONE)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    if (sqlite3_prepare_v2(
            db,
            "WITH selected AS (" MAP_SELECTED ") "
            "SELECT id,source_id,target_id,type,json_extract(properties,'$.line'),"
            "CASE WHEN json_type(properties,'$.strategy')='text' THEN "
            "json_extract(properties,'$.strategy') END,"
            "CASE WHEN json_type(properties,'$.confidence') IN ('integer','real') THEN "
            "json_extract(properties,'$.confidence') END FROM edges "
            "WHERE project=?1 AND type IN " MAP_RELATIONS
            " AND source_id IN selected AND target_id IN selected "
            "ORDER BY id LIMIT 100001",
            -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    int count = 0;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        if (++count > 100000)
            break;
        yyjson_mut_val *edge = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, edge, "id", sqlite3_column_int64(st, 0));
        yyjson_mut_obj_add_int(doc, edge, "source", sqlite3_column_int64(st, 1));
        yyjson_mut_obj_add_int(doc, edge, "target", sqlite3_column_int64(st, 2));
        map_text(doc, edge, "type", st, 3);
        if (sqlite3_column_type(st, 4) != SQLITE_NULL)
            yyjson_mut_obj_add_int(doc, edge, "line", sqlite3_column_int64(st, 4));
        map_text(doc, edge, "strategy", st, 5);
        if (sqlite3_column_type(st, 6) != SQLITE_NULL && isfinite(sqlite3_column_double(st, 6)))
            yyjson_mut_obj_add_real(doc, edge, "confidence", sqlite3_column_double(st, 6));
        yyjson_mut_arr_add_val(edges, edge);
    }
    if (rc != SQLITE_DONE && count <= 100000)
        goto fail;
    sqlite3_finalize(st);
    st = NULL;
    yyjson_mut_obj_add_bool(doc, root, "nodes_truncated", total_nodes > 20000);
    yyjson_mut_obj_add_bool(doc, root, "edges_truncated",
                            count > 100000 || total_edges > (int64_t)yyjson_mut_arr_size(edges));
    yyjson_mut_obj_add_str(
        doc, root, "edge_scope",
        "recorded relation types between retained semantic nodes; other endpoints omitted");
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK)
        goto fail;
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
fail:
    sqlite3_finalize(st);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    yyjson_mut_doc_free(doc);
    return NULL;
}
