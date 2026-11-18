/*
 * neo4j_sync.h — Mirror a published project database into Neo4j and record
 * which projects are Neo4j-backed.
 *
 * Neo4j model (one "graph" per project database file, keyed by db = the
 * project name the .db belongs to):
 *   (:CbmProject {name, gen, generation, root_path, indexed_at, node_count, edge_count})
 *   (:CbmNode:<Label> {db, gen, project, id, label, name, qualified_name, file_path,
 *                      start_line, end_line, properties_json, ...top-level scalar props})
 *   (a)-[:<TYPE> {id, properties_json, ...top-level scalar props}]->(b)
 *
 * A full sync writes generation gen+1, flips CbmProject.gen, then deletes the
 * old generation, so readers that filter on CbmProject.gen never observe a
 * half-written graph. A delta sync (the usual re-index) sends only the nodes
 * and edges that changed since the last sync, in place, in one Neo4j
 * transaction -- equally all-or-nothing for readers. It relies on the local
 * record "<db_path>.neo4j-state.db" of what Neo4j holds, and falls back to a
 * full sync whenever that record cannot be trusted or the change is too large
 * for one transaction (CBM_NEO4J_DELTA_MAX_ROWS, default 50000; 0 = never).
 */
#ifndef CBM_NEO4J_SYNC_H
#define CBM_NEO4J_SYNC_H

#include "neo4j/neo4j_client.h"

#include <stddef.h>
#include <stdint.h>

/* "<db_path>.graph.json": the project's graph-backend marker. */
int cbm_neo4j_sidecar_path(const char *db_path, char *out, size_t out_sz);

/* "<db_path>.neo4j-state.db": the delta sync's record of what Neo4j holds
 * (one id + row hash per node and edge). Safe to delete: the next sync is
 * then a full one. */
int cbm_neo4j_state_path(const char *db_path, char *out, size_t out_sz);

/* What a sync did. */
enum {
    CBM_NEO4J_SYNC_NONE = 0,    /* no sync ran (plain SQLite project, or it failed) */
    CBM_NEO4J_SYNC_SKIPPED = 1, /* Neo4j already held this generation */
    CBM_NEO4J_SYNC_FULL = 2,    /* every node and edge was uploaded */
    CBM_NEO4J_SYNC_DELTA = 3,   /* only the changes were sent */
};
typedef struct {
    int mode; /* CBM_NEO4J_SYNC_* */
    /* DELTA: rows sent per kind. FULL: *_added = every row uploaded. */
    int64_t nodes_added;
    int64_t nodes_changed;
    int64_t nodes_removed;
    int64_t edges_added;
    int64_t edges_changed;
    int64_t edges_removed;
    int64_t nodes_total; /* the graph's size after the sync */
    int64_t edges_total;
    char detail[256]; /* FULL: why the delta could not be used */
} cbm_neo4j_sync_stats_t;

/* What the calling thread's last cbm_neo4j_after_publish / cbm_neo4j_sync_db
 * did (zeroed at the start of each). */
void cbm_neo4j_last_sync_stats(cbm_neo4j_sync_stats_t *out);
/* One-line summary of st, as recorded in the marker's "last_sync" field. */
int cbm_neo4j_sync_stats_format(const cbm_neo4j_sync_stats_t *st, char *out, size_t out_sz);

/* Read the marker: 0 + cfg filled (password resolved from cache/env), 1 when
 * the project has no marker (plain SQLite), -1 on a malformed marker.
 * synced_generation (optional) receives the store generation last synced. */
int cbm_neo4j_sidecar_read(const char *db_path, cbm_neo4j_config_t *cfg, char *synced_generation,
                           size_t synced_sz, char *err, size_t err_sz);
int cbm_neo4j_sidecar_write(const char *db_path, const cbm_neo4j_config_t *cfg,
                            const char *synced_generation);
/* As above, also recording why the last sync failed (NULL/"" = it succeeded),
 * so a failed background sync stays visible through index_status. */
int cbm_neo4j_sidecar_write_status(const char *db_path, const cbm_neo4j_config_t *cfg,
                                   const char *synced_generation, const char *last_sync_error);
/* The path whose marker governs the database at db_path: db_path itself when
 * "<db_path>.graph.json" exists, else "<same dir>/<project>.db" when that
 * project's marker exists (a database found under another file name). 0 and
 * out filled, or 1 when the project is not Neo4j-backed. */
int cbm_neo4j_sidecar_locate(const char *db_path, const char *project, char *out, size_t out_sz);

/* One string field of the marker ("" when absent). 0, or -1 without marker. */
int cbm_neo4j_sidecar_field(const char *db_path, const char *key, char *out, size_t out_sz);
int cbm_neo4j_sidecar_remove(const char *db_path);

/* Create the indexes/constraints the sync and the read path rely on.
 * Idempotent. */
int cbm_neo4j_ensure_schema(cbm_neo4j_t *c);

/* Bring Neo4j's copy of the database at db_path (db = project) up to date:
 * a delta when the local record allows it, otherwise every node and edge.
 * On success, *generation_out (optional) receives the SQLite store
 * generation that was synced. Returns 0 or -1 with err. */
int cbm_neo4j_sync_db(cbm_neo4j_t *c, const char *db_path, const char *project,
                      char *generation_out, size_t generation_sz, char *err, size_t err_sz);

/* Remove a project's graph (all generations) and its CbmProject node. */
int cbm_neo4j_drop_project(cbm_neo4j_t *c, const char *project, char *err, size_t err_sz);

/* Post-publish hook used by the pipeline. explicit_cfg != NULL means the
 * caller asked for Neo4j on this run (index_repository graph_db); NULL means
 * inherit the project's marker, if any. disable == true removes the marker
 * (graph_db "sqlite"/"none"). Returns 0, or -1 with err when the graph could
 * not be synced -- explicit or inherited alike (a malformed marker too). */
int cbm_neo4j_after_publish(const char *db_path, const char *project,
                            const cbm_neo4j_config_t *explicit_cfg, bool disable, char *err,
                            size_t err_sz);

#ifdef CBM_ENABLE_TEST_SEAMS
/* Tests: called (on the calling thread) after a delta's checks and right
 * before its transaction is sent, to act as a concurrent writer. NULL clears. */
void cbm_neo4j_test_set_delta_hook(void (*fn)(void *ctx), void *ctx);
#endif

#endif /* CBM_NEO4J_SYNC_H */
