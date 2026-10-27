/*
 * neo4j_sync.c — Mirror a published project database into Neo4j.
 */
#include "neo4j/neo4j_sync.h"

#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "store/store.h"

#include "sqlite3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N4_SYNC_BATCH_ROWS 2000
#define N4_DELETE_BATCH 10000
#define N4_SIDECAR_SUFFIX ".graph.json"
#define N4_SIDECAR_MAX (64 * 1024)
#define N4_GEN_MAX 128

/* Columns written explicitly; a flattened property never overwrites them. */
static const char *const k_reserved_node_keys[] = {
    "db",         "gen",      "project",         "id",        "label",
    "name",       "qualified_name", "file_path", "start_line", "end_line",
    "properties_json", "prop_keys", NULL};
static const char *const k_reserved_edge_keys[] = {"id", "type", "properties_json", "prop_keys",
                                                   NULL};

/* Layout version of a synced graph (CbmProject.sync_format). Bump when the
 * stored shape changes; an older graph is re-uploaded on the next sync even
 * if the SQLite generation is unchanged. 2: prop_keys on nodes and edges. */
#define N4_SYNC_FORMAT 2

static void sync_err(char *err, size_t err_sz, const char *what, const char *detail) {
    if (err && err_sz) {
        (void)snprintf(err, err_sz, "%s%s%s", what, detail ? ": " : "", detail ? detail : "");
    }
}

/* ── Sidecar marker ─────────────────────────────────────────────── */

int cbm_neo4j_sidecar_path(const char *db_path, char *out, size_t out_sz) {
    if (!db_path || !*db_path) {
        return -1;
    }
    int n = snprintf(out, out_sz, "%s%s", db_path, N4_SIDECAR_SUFFIX);
    return (n > 0 && (size_t)n < out_sz) ? 0 : -1;
}

int cbm_neo4j_sidecar_read(const char *db_path, cbm_neo4j_config_t *cfg, char *synced_generation,
                           size_t synced_sz, char *err, size_t err_sz) {
    char path[4096];
    if (synced_generation && synced_sz) {
        synced_generation[0] = '\0';
    }
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return 1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return 1;
    }
    char *buf = malloc(N4_SIDECAR_MAX + 1);
    if (!buf) {
        (void)fclose(f);
        return -1;
    }
    size_t n = fread(buf, 1, N4_SIDECAR_MAX, f);
    (void)fclose(f);
    buf[n] = '\0';
    int rc = cbm_neo4j_config_from_meta(buf, cfg, err, err_sz);
    if (rc == 0 && synced_generation && synced_sz) {
        yyjson_doc *d = yyjson_read(buf, n, 0);
        const char *g = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(d), "synced_generation"));
        (void)snprintf(synced_generation, synced_sz, "%s", g ? g : "");
        yyjson_doc_free(d);
    }
    free(buf);
    return rc == 0 ? 0 : -1;
}

int cbm_neo4j_sidecar_locate(const char *db_path, const char *project, char *out,
                             size_t out_sz) {
    char path[4096];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) == 0) {
        FILE *f = cbm_fopen(path, "rb");
        if (f) {
            (void)fclose(f);
            (void)snprintf(out, out_sz, "%s", db_path);
            return 0;
        }
    }
    if (!project || !project[0]) {
        return 1;
    }
    /* The database was found under another file name (the #704 internal-name
     * scan): its marker still carries the project's canonical name. */
    size_t dir_len = 0;
    for (const char *p = db_path; *p; p++) {
        if (*p == '/' || *p == '\\') {
            dir_len = (size_t)(p - db_path) + 1;
        }
    }
    char canonical[4096];
    int n = snprintf(canonical, sizeof(canonical), "%.*s%s.db", (int)dir_len, db_path, project);
    if (n <= 0 || (size_t)n >= sizeof(canonical) ||
        cbm_neo4j_sidecar_path(canonical, path, sizeof(path)) != 0) {
        return 1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return 1;
    }
    (void)fclose(f);
    (void)snprintf(out, out_sz, "%s", canonical);
    return 0;
}

int cbm_neo4j_sidecar_field(const char *db_path, const char *key, char *out, size_t out_sz) {
    char path[4096];
    out[0] = '\0';
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return -1;
    }
    char *buf = malloc(N4_SIDECAR_MAX + 1);
    if (!buf) {
        (void)fclose(f);
        return -1;
    }
    size_t n = fread(buf, 1, N4_SIDECAR_MAX, f);
    (void)fclose(f);
    yyjson_doc *d = yyjson_read(buf, n, 0);
    free(buf);
    const char *v = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(d), key));
    (void)snprintf(out, out_sz, "%s", v ? v : "");
    yyjson_doc_free(d);
    return 0;
}

int cbm_neo4j_sidecar_write(const char *db_path, const cbm_neo4j_config_t *cfg,
                            const char *synced_generation) {
    return cbm_neo4j_sidecar_write_status(db_path, cfg, synced_generation, NULL);
}

int cbm_neo4j_sidecar_write_status(const char *db_path, const cbm_neo4j_config_t *cfg,
                                   const char *synced_generation, const char *last_sync_error) {
    char path[4096];
    char tmp[4096 + 8];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    (void)snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    char *meta = cbm_neo4j_config_to_meta(cfg);
    if (!meta) {
        return -1;
    }
    yyjson_doc *src = yyjson_read(meta, strlen(meta), 0);
    free(meta);
    yyjson_mut_doc *d = yyjson_doc_mut_copy(src, NULL);
    yyjson_doc_free(src);
    if (!d) {
        return -1;
    }
    yyjson_mut_obj_add_strcpy(d, yyjson_mut_doc_get_root(d), "synced_generation",
                              synced_generation ? synced_generation : "");
    if (last_sync_error && last_sync_error[0]) {
        yyjson_mut_obj_add_strcpy(d, yyjson_mut_doc_get_root(d), "last_sync_error",
                                  last_sync_error);
    }
    size_t len = 0;
    char *json = yyjson_mut_write(d, YYJSON_WRITE_PRETTY, &len);
    yyjson_mut_doc_free(d);
    if (!json) {
        return -1;
    }
    FILE *f = cbm_fopen(tmp, "wb");
    if (!f) {
        free(json);
        return -1;
    }
    bool ok = fwrite(json, 1, len, f) == len;
    ok = (fclose(f) == 0) && ok;
    free(json);
    if (!ok || cbm_rename_replace(tmp, path) != 0) {
        (void)cbm_unlink(tmp);
        return -1;
    }
    return 0;
}

int cbm_neo4j_sidecar_remove(const char *db_path) {
    char path[4096];
    if (cbm_neo4j_sidecar_path(db_path, path, sizeof(path)) != 0) {
        return -1;
    }
    (void)cbm_unlink(path);
    return 0;
}

/* ── Statement helpers ──────────────────────────────────────────── */

static int run_simple(cbm_neo4j_t *c, const char *cypher, const char *db, int64_t gen,
                      cbm_neo4j_result_t *out) {
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        return -1;
    }
    yyjson_mut_val *params = cbm_neo4j_batch_add(&b, cypher);
    if (db) {
        yyjson_mut_obj_add_strcpy(b.doc, params, "db", db);
    }
    yyjson_mut_obj_add_int(b.doc, params, "gen", gen);
    int rc = cbm_neo4j_batch_run(c, &b, out);
    cbm_neo4j_batch_free(&b);
    return rc;
}

int cbm_neo4j_ensure_schema(cbm_neo4j_t *c) {
    static const char *const stmts[] = {
        "CREATE CONSTRAINT cbm_project_name IF NOT EXISTS FOR (p:CbmProject) REQUIRE p.name IS "
        "UNIQUE",
        "CREATE INDEX cbm_node_db IF NOT EXISTS FOR (n:CbmNode) ON (n.db)",
        "CREATE INDEX cbm_node_id IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.id)",
        "CREATE INDEX cbm_node_qn IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.qualified_name)",
        "CREATE INDEX cbm_node_name IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.name)",
        "CREATE INDEX cbm_node_file IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.file_path)",
        "CREATE INDEX cbm_node_label IF NOT EXISTS FOR (n:CbmNode) ON (n.db, n.gen, n.label)",
        NULL};
    /* Schema statements cannot share a transaction with each other in every
     * Neo4j version, so send them one per request. */
    for (int i = 0; stmts[i]; i++) {
        cbm_neo4j_batch_t b;
        if (cbm_neo4j_batch_init(&b) != 0) {
            return -1;
        }
        (void)cbm_neo4j_batch_add(&b, stmts[i]);
        int rc = cbm_neo4j_batch_run(c, &b, NULL);
        cbm_neo4j_batch_free(&b);
        if (rc != 0) {
            return -1;
        }
    }
    /* Best effort: wait for freshly created indexes before bulk MATCHes. */
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) == 0) {
        (void)cbm_neo4j_batch_add(&b, "CALL db.awaitIndexes(300)");
        (void)cbm_neo4j_batch_run(c, &b, NULL);
        cbm_neo4j_batch_free(&b);
    }
    return 0;
}

static bool key_reserved(const char *k, const char *const *reserved) {
    for (int i = 0; reserved[i]; i++) {
        if (strcmp(k, reserved[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Copy top-level scalar members of properties_json into obj. Arrays of
 * scalars are kept too (Neo4j stores homogeneous lists); nested objects stay
 * only inside properties_json. prop_keys lists every top-level key -- the
 * set SQLite's json_each reports for get_graph_schema -- flattened or not. */
static void flatten_props(yyjson_mut_doc *d, yyjson_mut_val *obj, const char *json,
                          const char *const *reserved) {
    yyjson_mut_val *prop_keys = yyjson_mut_arr(d);
    yyjson_mut_obj_add_val(d, obj, "prop_keys", prop_keys);
    if (!json || !*json || strcmp(json, "{}") == 0) {
        return;
    }
    yyjson_doc *pd = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = pd ? yyjson_doc_get_root(pd) : NULL;
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(pd);
        return;
    }
    size_t idx;
    size_t max;
    yyjson_val *k;
    yyjson_val *v;
    yyjson_obj_foreach(root, idx, max, k, v) {
        const char *key = yyjson_get_str(k);
        if (key) {
            yyjson_mut_arr_add_strcpy(d, prop_keys, key);
        }
        if (!key || key_reserved(key, reserved)) {
            continue;
        }
        if (yyjson_is_str(v) || yyjson_is_num(v) || yyjson_is_bool(v)) {
            yyjson_mut_obj_add(obj, yyjson_mut_strcpy(d, key), yyjson_val_mut_copy(d, v));
        } else if (yyjson_is_arr(v) && yyjson_arr_size(v) > 0) {
            /* Only homogeneous string or number lists are valid Neo4j values. */
            yyjson_val *first = yyjson_arr_get_first(v);
            bool all_str = true;
            bool all_num = true;
            size_t ai;
            size_t amax;
            yyjson_val *av;
            yyjson_arr_foreach(v, ai, amax, av) {
                all_str = all_str && yyjson_is_str(av);
                all_num = all_num && yyjson_is_num(av);
            }
            if ((all_str || all_num) && first) {
                yyjson_mut_obj_add(obj, yyjson_mut_strcpy(d, key), yyjson_val_mut_copy(d, v));
            }
        }
    }
    yyjson_doc_free(pd);
}

static const char *col_text(sqlite3_stmt *st, int i) {
    const char *s = (const char *)sqlite3_column_text(st, i);
    return s ? s : "";
}

/* ── Node / edge streaming ──────────────────────────────────────── */

typedef struct {
    cbm_neo4j_t *c;
    const char *db;
    int64_t gen;
    cbm_neo4j_batch_t batch;
    yyjson_mut_val *rows;
    int row_count;
    char group[256]; /* label or edge type of the open batch */
    bool is_edge;
    int64_t total;
} n4_stream_t;

static int stream_open_batch(n4_stream_t *s, const char *group) {
    if (cbm_neo4j_batch_init(&s->batch) != 0) {
        return -1;
    }
    (void)snprintf(s->group, sizeof(s->group), "%s", group);
    char cypher[768];
    bool ident = cbm_neo4j_ident_ok(group);
    if (!s->is_edge) {
        if (ident) {
            (void)snprintf(cypher, sizeof(cypher),
                           "UNWIND $rows AS r CREATE (n:CbmNode:`%s`) SET n = r", group);
        } else {
            (void)snprintf(cypher, sizeof(cypher),
                           "UNWIND $rows AS r CREATE (n:CbmNode) SET n = r");
        }
    } else {
        /* Types that are not plain identifiers land on CBM_EDGE with the real
         * type kept in the `type` property. */
        (void)snprintf(cypher, sizeof(cypher),
                       "UNWIND $rows AS r "
                       "MATCH (a:CbmNode {db: $db, gen: $gen, id: r.s}) "
                       "MATCH (b:CbmNode {db: $db, gen: $gen, id: r.t}) "
                       "CREATE (a)-[e:`%s`]->(b) SET e = r.p",
                       ident ? group : "CBM_EDGE");
    }
    yyjson_mut_val *params = cbm_neo4j_batch_add(&s->batch, cypher);
    yyjson_mut_obj_add_strcpy(s->batch.doc, params, "db", s->db);
    yyjson_mut_obj_add_int(s->batch.doc, params, "gen", s->gen);
    s->rows = yyjson_mut_arr(s->batch.doc);
    yyjson_mut_obj_add_val(s->batch.doc, params, "rows", s->rows);
    s->row_count = 0;
    return 0;
}

static int stream_flush(n4_stream_t *s) {
    if (!s->batch.doc) {
        return 0;
    }
    int rc = s->row_count > 0 ? cbm_neo4j_batch_run(s->c, &s->batch, NULL) : 0;
    s->total += s->row_count;
    cbm_neo4j_batch_free(&s->batch);
    s->rows = NULL;
    s->row_count = 0;
    s->group[0] = '\0';
    return rc;
}

/* Returns the row object to fill, flushing/reopening on group change or when
 * the batch is full. NULL on failure. */
static yyjson_mut_val *stream_row(n4_stream_t *s, const char *group) {
    if (s->batch.doc && (strcmp(s->group, group) != 0 || s->row_count >= N4_SYNC_BATCH_ROWS)) {
        if (stream_flush(s) != 0) {
            return NULL;
        }
    }
    if (!s->batch.doc && stream_open_batch(s, group) != 0) {
        return NULL;
    }
    yyjson_mut_val *row = yyjson_mut_obj(s->batch.doc);
    yyjson_mut_arr_append(s->rows, row);
    s->row_count++;
    return row;
}

static int sync_nodes(n4_stream_t *s, sqlite3 *db, char *err, size_t err_sz) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(
            db,
            "SELECT id, project, label, name, qualified_name, file_path, start_line, "
            "end_line, properties FROM nodes ORDER BY label, id",
            -1, &st, NULL) != SQLITE_OK) {
        sync_err(err, err_sz, "read nodes", sqlite3_errmsg(db));
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *label = col_text(st, 2);
        yyjson_mut_val *row = stream_row(s, label);
        if (!row) {
            sync_err(err, err_sz, "write nodes to Neo4j", cbm_neo4j_error(s->c));
            rc = -1;
            break;
        }
        yyjson_mut_doc *d = s->batch.doc;
        const char *props = col_text(st, 8);
        /* Flattened properties first so explicit columns win on collision. */
        flatten_props(d, row, props, k_reserved_node_keys);
        yyjson_mut_obj_add_strcpy(d, row, "db", s->db);
        yyjson_mut_obj_add_int(d, row, "gen", s->gen);
        yyjson_mut_obj_add_int(d, row, "id", sqlite3_column_int64(st, 0));
        yyjson_mut_obj_add_strcpy(d, row, "project", col_text(st, 1));
        yyjson_mut_obj_add_strcpy(d, row, "label", label);
        yyjson_mut_obj_add_strcpy(d, row, "name", col_text(st, 3));
        yyjson_mut_obj_add_strcpy(d, row, "qualified_name", col_text(st, 4));
        yyjson_mut_obj_add_strcpy(d, row, "file_path", col_text(st, 5));
        yyjson_mut_obj_add_int(d, row, "start_line", sqlite3_column_int(st, 6));
        yyjson_mut_obj_add_int(d, row, "end_line", sqlite3_column_int(st, 7));
        yyjson_mut_obj_add_strcpy(d, row, "properties_json", props[0] ? props : "{}");
    }
    sqlite3_finalize(st);
    if (rc == 0 && stream_flush(s) != 0) {
        sync_err(err, err_sz, "write nodes to Neo4j", cbm_neo4j_error(s->c));
        rc = -1;
    }
    cbm_neo4j_batch_free(&s->batch);
    return rc;
}

static int sync_edges(n4_stream_t *s, sqlite3 *db, char *err, size_t err_sz) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT id, source_id, target_id, type, properties FROM edges "
                           "ORDER BY type, id",
                           -1, &st, NULL) != SQLITE_OK) {
        sync_err(err, err_sz, "read edges", sqlite3_errmsg(db));
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *type = col_text(st, 3);
        yyjson_mut_val *row = stream_row(s, type);
        if (!row) {
            sync_err(err, err_sz, "write edges to Neo4j", cbm_neo4j_error(s->c));
            rc = -1;
            break;
        }
        yyjson_mut_doc *d = s->batch.doc;
        yyjson_mut_obj_add_int(d, row, "s", sqlite3_column_int64(st, 1));
        yyjson_mut_obj_add_int(d, row, "t", sqlite3_column_int64(st, 2));
        yyjson_mut_val *p = yyjson_mut_obj(d);
        const char *props = col_text(st, 4);
        flatten_props(d, p, props, k_reserved_edge_keys);
        yyjson_mut_obj_add_int(d, p, "id", sqlite3_column_int64(st, 0));
        yyjson_mut_obj_add_strcpy(d, p, "type", type);
        yyjson_mut_obj_add_strcpy(d, p, "properties_json", props[0] ? props : "{}");
        yyjson_mut_obj_add_val(d, row, "p", p);
    }
    sqlite3_finalize(st);
    if (rc == 0 && stream_flush(s) != 0) {
        sync_err(err, err_sz, "write edges to Neo4j", cbm_neo4j_error(s->c));
        rc = -1;
    }
    cbm_neo4j_batch_free(&s->batch);
    return rc;
}

/* Delete nodes of db matching `where` (a predicate on n using $gen) in
 * bounded transactions. */
static int delete_loop(cbm_neo4j_t *c, const char *db, const char *where, int64_t gen) {
    char cypher[512];
    (void)snprintf(cypher, sizeof(cypher),
                   "MATCH (n:CbmNode {db: $db}) WHERE %s WITH n LIMIT %d DETACH DELETE n "
                   "RETURN count(*)",
                   where, N4_DELETE_BATCH);
    for (;;) {
        cbm_neo4j_result_t r;
        if (run_simple(c, cypher, db, gen, &r) != 0) {
            return -1;
        }
        yyjson_val *data = cbm_neo4j_result_data(&r, 0);
        int64_t n = cbm_neo4j_val_int(cbm_neo4j_row_col(yyjson_arr_get_first(data), 0), 0);
        cbm_neo4j_result_free(&r);
        if (n <= 0) {
            return 0;
        }
    }
}

int cbm_neo4j_sync_db(cbm_neo4j_t *c, const char *db_path, const char *project,
                      char *generation_out, size_t generation_sz, char *err, size_t err_sz) {
    if (cbm_neo4j_ensure_schema(c) != 0) {
        sync_err(err, err_sz, "create Neo4j indexes", cbm_neo4j_error(c));
        return -1;
    }
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        sync_err(err, err_sz, "open project database", db_path);
        return -1;
    }
    char generation[N4_GEN_MAX] = "";
    (void)cbm_store_generation(store, generation, sizeof(generation));
    cbm_project_t proj = {0};
    bool have_proj = cbm_store_get_project(store, project, &proj) == CBM_STORE_OK;

    /* Current generation; a CbmProject is created on first sync. */
    cbm_neo4j_result_t r;
    if (run_simple(c, "MERGE (p:CbmProject {name: $db}) RETURN coalesce(p.gen, 0)", project, 0,
                   &r) != 0) {
        sync_err(err, err_sz, "read Neo4j project state", cbm_neo4j_error(c));
        goto fail;
    }
    /* The cleanup below deletes every generation except old_gen, so old_gen
     * must be the published one: exactly one integer row, never a default. */
    yyjson_val *gen_rows = cbm_neo4j_result_data(&r, 0);
    yyjson_val *gen_val = cbm_neo4j_row_col(yyjson_arr_get_first(gen_rows), 0);
    if (yyjson_arr_size(gen_rows) != 1 || !yyjson_is_int(gen_val)) {
        cbm_neo4j_result_free(&r);
        sync_err(err, err_sz, "read Neo4j project state", "unexpected generation reply");
        goto fail;
    }
    int64_t old_gen = yyjson_get_sint(gen_val);
    cbm_neo4j_result_free(&r);
    int64_t new_gen = old_gen + 1;

    /* Leftovers of an interrupted earlier sync. */
    if (delete_loop(c, project, "n.gen <> $gen", old_gen) != 0) {
        sync_err(err, err_sz, "clean up stale Neo4j generation", cbm_neo4j_error(c));
        goto fail;
    }

    n4_stream_t s;
    memset(&s, 0, sizeof(s));
    s.c = c;
    s.db = project;
    s.gen = new_gen;
    sqlite3 *db = cbm_store_get_db(store);
    if (sync_nodes(&s, db, err, err_sz) != 0) {
        goto fail_partial;
    }
    int64_t node_total = s.total;
    s.total = 0;
    s.is_edge = true;
    if (sync_edges(&s, db, err, err_sz) != 0) {
        goto fail_partial;
    }
    int64_t edge_total = s.total;

    /* Flip readers to the new generation. */
    cbm_neo4j_batch_t b;
    if (cbm_neo4j_batch_init(&b) != 0) {
        goto fail_partial;
    }
    yyjson_mut_val *params =
        cbm_neo4j_batch_add(&b, "MATCH (p:CbmProject {name: $db}) SET p.gen = $gen, "
                                "p.generation = $generation, p.root_path = $root, "
                                "p.indexed_at = $indexed_at, p.node_count = $nodes, "
                                "p.edge_count = $edges, p.sync_format = $format");
    yyjson_mut_obj_add_int(b.doc, params, "format", N4_SYNC_FORMAT);
    yyjson_mut_obj_add_strcpy(b.doc, params, "db", project);
    yyjson_mut_obj_add_int(b.doc, params, "gen", new_gen);
    yyjson_mut_obj_add_strcpy(b.doc, params, "generation", generation);
    yyjson_mut_obj_add_strcpy(b.doc, params, "root",
                              have_proj && proj.root_path ? proj.root_path : "");
    yyjson_mut_obj_add_strcpy(b.doc, params, "indexed_at",
                              have_proj && proj.indexed_at ? proj.indexed_at : "");
    yyjson_mut_obj_add_int(b.doc, params, "nodes", node_total);
    yyjson_mut_obj_add_int(b.doc, params, "edges", edge_total);
    int flip_rc = cbm_neo4j_batch_run(c, &b, NULL);
    cbm_neo4j_batch_free(&b);
    if (flip_rc != 0) {
        sync_err(err, err_sz, "publish Neo4j generation", cbm_neo4j_error(c));
        goto fail_partial;
    }

    /* Old generation is now unreachable for readers; reclaim it. A failure
     * here leaves garbage that the next sync removes. */
    if (delete_loop(c, project, "n.gen <> $gen", new_gen) != 0) {
        cbm_log_warn("neo4j.sync", "phase", "delete_old_generation", "err", cbm_neo4j_error(c));
    }
    cbm_log_info("neo4j.sync", "project", project, "uri", cbm_neo4j_uri(c), "status", "ok");
    if (generation_out && generation_sz) {
        (void)snprintf(generation_out, generation_sz, "%s", generation);
    }
    if (have_proj) {
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(store);
    return 0;

fail_partial:
    /* Drop what was written for the new generation; readers never saw it. */
    (void)delete_loop(c, project, "n.gen = $gen", new_gen);
fail:
    if (have_proj) {
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(store);
    return -1;
}

/* The SQLite generation Neo4j currently publishes for project ("" when the
 * project is absent, or was synced in an older layout and must be re-sent).
 * 0, or -1 on a transport/Cypher failure. */
static int remote_generation(cbm_neo4j_t *c, const char *project, char *out, size_t out_sz) {
    out[0] = '\0';
    cbm_neo4j_result_t r;
    if (run_simple(c,
                   "MATCH (p:CbmProject {name: $db}) RETURN p.generation, coalesce(p.sync_format, 1)",
                   project, 0, &r) != 0) {
        return -1;
    }
    yyjson_val *row = yyjson_arr_get_first(cbm_neo4j_result_data(&r, 0));
    yyjson_val *v = cbm_neo4j_row_col(row, 0);
    bool current_format = cbm_neo4j_val_int(cbm_neo4j_row_col(row, 1), 1) >= N4_SYNC_FORMAT;
    (void)snprintf(out, out_sz, "%s", current_format && yyjson_is_str(v) ? yyjson_get_str(v) : "");
    cbm_neo4j_result_free(&r);
    return 0;
}

int cbm_neo4j_drop_project(cbm_neo4j_t *c, const char *project, char *err, size_t err_sz) {
    if (delete_loop(c, project, "true", 0) != 0 ||
        run_simple(c, "MATCH (p:CbmProject {name: $db}) DELETE p", project, 0, NULL) != 0) {
        sync_err(err, err_sz, "delete project from Neo4j", cbm_neo4j_error(c));
        return -1;
    }
    return 0;
}

int cbm_neo4j_after_publish(const char *db_path, const char *project,
                            const cbm_neo4j_config_t *explicit_cfg, bool disable, char *err,
                            size_t err_sz) {
    if (err && err_sz) {
        err[0] = '\0';
    }
    if (disable) {
        (void)cbm_neo4j_sidecar_remove(db_path);
        return 0;
    }
    cbm_neo4j_config_t cfg;
    char synced[N4_GEN_MAX] = "";
    bool explicit_req = explicit_cfg != NULL;
    /* True when a marker for this same target already exists: a failed sync
     * is then recorded in it, so it stays visible after this run. */
    bool marked_target = !explicit_req;
    if (explicit_req) {
        cfg = *explicit_cfg;
        /* Same target as last time: keep the recorded generation so an
         * unchanged re-index skips the upload. */
        cbm_neo4j_config_t prev;
        char perr[256];
        if (cbm_neo4j_sidecar_read(db_path, &prev, synced, sizeof(synced), perr, sizeof(perr)) !=
                0 ||
            strcmp(prev.uri, cfg.uri) != 0 || strcmp(prev.database, cfg.database) != 0) {
            synced[0] = '\0';
        } else {
            marked_target = true;
        }
        cbm_neo4j_config_wipe(&prev);
    } else {
        int rc = cbm_neo4j_sidecar_read(db_path, &cfg, synced, sizeof(synced), err, err_sz);
        if (rc == 1) {
            return 0; /* plain SQLite project */
        }
        if (rc != 0) {
            cbm_log_error("neo4j.sync", "project", project, "err", err);
            return -1;
        }
    }

    /* Skip the upload when Neo4j already holds this exact generation. */
    char current[N4_GEN_MAX] = "";
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (store) {
        (void)cbm_store_generation(store, current, sizeof(current));
        cbm_store_close(store);
    }

    int result = 0;
    cbm_neo4j_t *c = cbm_neo4j_connect(&cfg, err, err_sz);
    if (!c) {
        result = -1;
    } else {
        /* The marker only records what was last sent. Skip the upload only
         * when Neo4j itself still publishes this exact generation: the graph
         * may have been deleted or overwritten there since (a cleared
         * database must be refilled by the next run, not reported current).
         * The lookup also proves reachability and credentials. */
        bool up_to_date = false;
        if (current[0] && strcmp(current, synced) == 0) {
            char remote[N4_GEN_MAX] = "";
            if (remote_generation(c, project, remote, sizeof(remote)) != 0) {
                sync_err(err, err_sz, "connect to Neo4j", cbm_neo4j_error(c));
                result = -1;
            } else {
                up_to_date = strcmp(remote, current) == 0;
            }
        }
        if (result == 0 && !up_to_date) {
            char done_gen[N4_GEN_MAX] = "";
            if (cbm_neo4j_sync_db(c, db_path, project, done_gen, sizeof(done_gen), err, err_sz) !=
                0) {
                result = -1;
            } else {
                (void)snprintf(synced, sizeof(synced), "%s", done_gen);
            }
        }
    }
    cbm_neo4j_close(c);

    if (result == 0) {
        if (explicit_req) {
            cbm_neo4j_config_remember(&cfg);
        }
        if (cbm_neo4j_sidecar_write(db_path, &cfg, synced) != 0) {
            sync_err(err, err_sz, "write graph backend marker", db_path);
            result = -1;
        }
    } else {
        cbm_log_error("neo4j.sync", "project", project, "err", err ? err : "");
        /* Keep the last good synced_generation and record why this sync
         * failed; the next successful sync rewrites the marker without it. */
        if (marked_target) {
            (void)cbm_neo4j_sidecar_write_status(db_path, &cfg, synced,
                                                 err && err[0] ? err : "Neo4j sync failed");
        }
    }
    cbm_neo4j_config_wipe(&cfg);
    /* A Neo4j-backed project is indexed into Neo4j or not at all: an inherited
     * (watcher / plain re-index) sync failure fails the run like an explicit
     * one. Reads stay refused until a sync succeeds (no local fallback). */
    return result;
}
