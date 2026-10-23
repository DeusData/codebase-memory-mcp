/*
 * neo4j_store.h — Answer a store's graph reads from Neo4j.
 */
#ifndef CBM_NEO4J_STORE_H
#define CBM_NEO4J_STORE_H

#include "cypher/cypher.h"
#include "store/store.h"

#include <stddef.h>

typedef enum {
    CBM_NEO4J_ATTACH_NONE = 0, /* project is plain SQLite */
    CBM_NEO4J_ATTACHED = 1,    /* graph reads now go to Neo4j */
    CBM_NEO4J_ATTACH_FALLBACK =
        2, /* Neo4j-backed, but unusable now: SQLite serves (err says why) */
} cbm_neo4j_attach_t;

/* If the project database at db_path is marked Neo4j-backed, connect and
 * route s's graph reads to the Neo4j generation that mirrors exactly this
 * SQLite generation. When Neo4j is unreachable, rejects the credentials, or
 * holds a different generation (a failed sync), the store keeps reading the
 * identical local copy and err explains the fallback. */
cbm_neo4j_attach_t cbm_neo4j_store_attach(cbm_store_t *s, const char *db_path, const char *project,
                                          char *err, size_t err_sz);

/* Run a Cypher query natively in Neo4j for a Neo4j-backed project, with
 * $project bound to the project name and $db / $gen bound to its current
 * graph. Results are converted to strings like cbm_cypher_execute's.
 * Returns 0, or -1 with out->error set. */
int cbm_neo4j_native_query(const char *db_path, const char *project, const char *query,
                           int max_rows, cbm_cypher_result_t *out);

#endif /* CBM_NEO4J_STORE_H */
