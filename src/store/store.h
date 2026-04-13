/*
 * store.h — Opaque SQLite graph store for code knowledge graphs.
 *
 * All functions are prefixed cbm_store_*. The store handle is opaque —
 * callers never touch SQLite internals directly.
 *
 * Thread safety: a single store handle must not be used concurrently.
 * Use one store per thread or external synchronization.
 */
#ifndef CBM_STORE_H
#define CBM_STORE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ── Opaque handle ──────────────────────────────────────────────── */

typedef struct cbm_store cbm_store_t;

/* ── Result codes ───────────────────────────────────────────────── */

#define CBM_STORE_OK 0
#define CBM_STORE_ERR (-1)
#define CBM_STORE_NOT_FOUND (-2)

/* ── Data structures ────────────────────────────────────────────── */

typedef struct {
    int64_t id;
    const char *project;
    const char *label;          /* Function, Class, Method, Module, File, ... */
    const char *name;           /* short name */
    const char *qualified_name; /* full dotted path */
    const char *file_path;      /* relative file path */
    int start_line;
    int end_line;
    const char *properties_json; /* JSON string, NULL → "{}" */
} cbm_node_t;

typedef struct {
    int64_t id;
    const char *project;
    int64_t source_id;
    int64_t target_id;
    const char *type;            /* CALLS, HTTP_CALLS, IMPORTS, ... */
    const char *properties_json; /* JSON string, NULL → "{}" */
} cbm_edge_t;

typedef struct {
    const char *name;
    const char *indexed_at; /* ISO 8601 */
    const char *root_path;
} cbm_project_t;

typedef struct {
    const char *project;
    const char *rel_path;
    const char *sha256;
    int64_t mtime_ns;
    int64_t size;
} cbm_file_hash_t;

/* Find nodes overlapping a line range in a file (excludes Module/Package). */
int cbm_store_find_nodes_by_file_overlap(cbm_store_t *s, const char *project, const char *file_path,
                                         int start_line, int end_line, cbm_node_t **out,
                                         int *count);

/* Find nodes whose qualified_name ends with the given suffix (dot-boundary). */
int cbm_store_find_nodes_by_qn_suffix(cbm_store_t *s, const char *project, const char *suffix,
                                      cbm_node_t **out, int *count);

/* Get CALLS degree of a node (inbound and outbound). */
void cbm_store_node_degree(cbm_store_t *s, int64_t node_id, int *in_deg, int *out_deg);

/* Get distinct file paths for a project. Caller must free each out[i] and out itself.
 * Returns CBM_STORE_OK or CBM_STORE_ERR. */
int cbm_store_list_files(cbm_store_t *s, const char *project, char ***out, int *count);

/* Get caller/callee names for a node (CALLS/HTTP_CALLS/ASYNC_CALLS edges).
 * Returns 0 on success. Caller must free each out_callers[i]/out_callees[i]
 * and the arrays themselves. */
int cbm_store_node_neighbor_names(cbm_store_t *s, int64_t node_id, int limit, char ***out_callers,
                                  int *caller_count, char ***out_callees, int *callee_count);

/* Batch count in/out degree for multiple nodes.
 * edge_type: filter by edge type (e.g. "CALLS"), or NULL/"" for all types.
 * out_in[i] and out_out[i] receive the in/out degree for node_ids[i].
 * Returns CBM_STORE_OK or CBM_STORE_ERR. */
int cbm_store_batch_count_degrees(cbm_store_t *s, const int64_t *node_ids, int id_count,
                                  const char *edge_type, int *out_in, int *out_out);

/* Upsert file hashes in batch. */
int cbm_store_upsert_file_hash_batch(cbm_store_t *s, const cbm_file_hash_t *hashes, int count);

/* Find edges whose properties contain a url_path matching the keyword. */
int cbm_store_find_edges_by_url_path(cbm_store_t *s, const char *project, const char *keyword,
                                     cbm_edge_t **out, int *count);

/* Restore database from another store (backup API). */
int cbm_store_restore_from(cbm_store_t *dst, cbm_store_t *src);

/* ── Search ─────────────────────────────────────────────────────── */

typedef struct {
    const char *project;
    const char *label;        /* NULL = any label */
    const char *name_pattern; /* regex on name, NULL = any */
    const char *qn_pattern;   /* regex on qualified_name, NULL = any */
    const char *file_pattern; /* glob on file_path, NULL = any */
    const char *relationship; /* edge type filter, NULL = any */
    const char *direction;    /* "inbound" / "outbound" / "any", NULL = any */
    int min_degree;           /* -1 = no filter (default), 0+ = minimum */
    int max_degree;           /* -1 = no filter (default), 0+ = maximum */
    int limit;                /* 0 = default (10) */
    int offset;
    bool exclude_entry_points;
    bool include_connected;
    const char *sort_by; /* "relevance" / "name" / "degree", NULL = relevance */
    bool case_sensitive;
    const char **exclude_labels; /* NULL-terminated array, or NULL */
} cbm_search_params_t;

typedef struct {
    cbm_node_t node;
    int in_degree;
    int out_degree;
    /* connected_names: allocated array of strings, count in connected_count */
    const char **connected_names;
    int connected_count;
} cbm_search_result_t;

typedef struct {
    cbm_search_result_t *results;
    int count;
    int total; /* total before pagination */
} cbm_search_output_t;

/* ── Traversal ──────────────────────────────────────────────────── */

typedef struct {
    cbm_node_t node;
    int hop; /* BFS depth from root */
} cbm_node_hop_t;

typedef struct {
    const char *from_name;
    const char *to_name;
    const char *type;
    double confidence;
} cbm_edge_info_t;

typedef struct {
    cbm_node_t root;
    cbm_node_hop_t *visited;
    int visited_count;
    cbm_edge_info_t *edges;
    int edge_count;
} cbm_traverse_result_t;

/* ── Schema introspection ───────────────────────────────────────── */

typedef struct {
    const char *label;
    int count;
} cbm_label_count_t;

typedef struct {
    const char *type;
    int count;
} cbm_type_count_t;

typedef struct {
    cbm_label_count_t *node_labels;
    int node_label_count;
    cbm_type_count_t *edge_types;
    int edge_type_count;
    /* relationship patterns like "(Function)-[CALLS]->(Function) [123x]" */
    const char **rel_patterns;
    int rel_pattern_count;
    const char **sample_func_names;
    int sample_func_count;
    const char **sample_class_names;
    int sample_class_count;
    const char **sample_qns;
    int sample_qn_count;
} cbm_schema_info_t;

/* ── Lifecycle ──────────────────────────────────────────────────── */

/* Open an in-memory database (for testing). */
cbm_store_t *cbm_store_open_memory(void);

/* Open a file-backed database at the given path. Creates if needed. */
cbm_store_t *cbm_store_open_path(const char *db_path);

/* Open an existing file-backed database for querying only (no SQLITE_OPEN_CREATE).
 * Returns NULL if the file does not exist — never creates a new .db file. */
cbm_store_t *cbm_store_open_path_query(const char *db_path);

/* Check database integrity. Returns true if the DB passes basic sanity checks
 * (projects table has correct types, no corruption indicators).
 * Returns false if corruption is detected — caller should delete and re-index. */
bool cbm_store_check_integrity(cbm_store_t *s);

/* Open database for a named project in the default cache dir. */
cbm_store_t *cbm_store_open(const char *project);

/* Close the store and free all resources. NULL-safe. */
void cbm_store_close(cbm_store_t *s);

/* Get the underlying sqlite3 handle (for testing only). */
struct sqlite3 *cbm_store_get_db(cbm_store_t *s);

/* Get the last error message (static string, valid until next call). */
const char *cbm_store_error(cbm_store_t *s);

/* ── Transaction ────────────────────────────────────────────────── */

/* Begin a transaction. Returns CBM_STORE_OK on success. */
int cbm_store_begin(cbm_store_t *s);

/* Commit the current transaction. */
int cbm_store_commit(cbm_store_t *s);

/* Rollback the current transaction. */
int cbm_store_rollback(cbm_store_t *s);

/* ── Bulk write optimization ────────────────────────────────────── */

/* Tune pragmas for bulk write throughput (synchronous=OFF, large cache).
 * WAL journal mode is preserved throughout for crash safety. */
int cbm_store_begin_bulk(cbm_store_t *s);

/* Restore normal pragmas (synchronous=NORMAL, default cache) after bulk writes. */
int cbm_store_end_bulk(cbm_store_t *s);

/* Drop user indexes for faster bulk inserts. */
int cbm_store_drop_indexes(cbm_store_t *s);

/* Recreate user indexes after bulk inserts. */
int cbm_store_create_indexes(cbm_store_t *s);

/* ── WAL / Checkpoint ───────────────────────────────────────────── */

/* Force WAL checkpoint + PRAGMA optimize. */
int cbm_store_checkpoint(cbm_store_t *s);

/* ── Dump / Restore ─────────────────────────────────────────────── */

/* Dump in-memory database to a file. */
int cbm_store_dump_to_file(cbm_store_t *s, const char *dest_path);

/* ── Project CRUD ───────────────────────────────────────────────── */

int cbm_store_upsert_project(cbm_store_t *s, const char *name, const char *root_path);
int cbm_store_get_project(cbm_store_t *s, const char *name, cbm_project_t *out);
int cbm_store_list_projects(cbm_store_t *s, cbm_project_t **out, int *count);
int cbm_store_delete_project(cbm_store_t *s, const char *name);

/* ── Node CRUD ──────────────────────────────────────────────────── */

/* Upsert a single node. Returns node ID (>0) or CBM_STORE_ERR. */
int64_t cbm_store_upsert_node(cbm_store_t *s, const cbm_node_t *n);

/* Upsert nodes in batch. out_ids must have room for count entries. */
int cbm_store_upsert_node_batch(cbm_store_t *s, const cbm_node_t *nodes, int count,
                                int64_t *out_ids);

/* Find node by primary key. Returns CBM_STORE_OK or CBM_STORE_NOT_FOUND. */
int cbm_store_find_node_by_id(cbm_store_t *s, int64_t id, cbm_node_t *out);

/* Find node by project + qualified_name. */
int cbm_store_find_node_by_qn(cbm_store_t *s, const char *project, const char *qn, cbm_node_t *out);

/* Find node by qualified_name only (no project filter — QNs are globally unique). */
int cbm_store_find_node_by_qn_any(cbm_store_t *s, const char *qn, cbm_node_t *out);

/* Find nodes by name (exact match). Returns allocated array, caller frees. */
int cbm_store_find_nodes_by_name(cbm_store_t *s, const char *project, const char *name,
                                 cbm_node_t **out, int *count);

/* Find nodes by name across all projects. Returns allocated array, caller frees. */
int cbm_store_find_nodes_by_name_any(cbm_store_t *s, const char *name, cbm_node_t **out,
                                     int *count);

/* Find nodes by label. */
int cbm_store_find_nodes_by_label(cbm_store_t *s, const char *project, const char *label,
                                  cbm_node_t **out, int *count);

/* Find nodes by file path. */
int cbm_store_find_nodes_by_file(cbm_store_t *s, const char *project, const char *file_path,
                                 cbm_node_t **out, int *count);

/* Batch lookup: map qualified names → node IDs.
 * qns[i] is resolved; out_ids[i] receives the ID or 0 if not found.
 * Returns number of QNs actually found, or CBM_STORE_ERR. */
int cbm_store_find_node_ids_by_qns(cbm_store_t *s, const char *project, const char **qns,
                                   int qn_count, int64_t *out_ids);

/* Count nodes in project. Returns count or CBM_STORE_ERR. */
int cbm_store_count_nodes(cbm_store_t *s, const char *project);

/* Delete all nodes for a project (cascade deletes edges). */
int cbm_store_delete_nodes_by_project(cbm_store_t *s, const char *project);

/* Delete nodes by file path. */
int cbm_store_delete_nodes_by_file(cbm_store_t *s, const char *project, const char *file_path);

/* Delete nodes by label. */
int cbm_store_delete_nodes_by_label(cbm_store_t *s, const char *project, const char *label);

/* ── Edge CRUD ──────────────────────────────────────────────────── */

/* Insert or update edge. Returns edge ID (>0) or CBM_STORE_ERR. */
int64_t cbm_store_insert_edge(cbm_store_t *s, const cbm_edge_t *e);

/* Insert edges in batch. */
int cbm_store_insert_edge_batch(cbm_store_t *s, const cbm_edge_t *edges, int count);

/* Find edges by source node. */
int cbm_store_find_edges_by_source(cbm_store_t *s, int64_t source_id, cbm_edge_t **out, int *count);

/* Find edges by target node. */
int cbm_store_find_edges_by_target(cbm_store_t *s, int64_t target_id, cbm_edge_t **out, int *count);

/* Find edges by source + type. */
int cbm_store_find_edges_by_source_type(cbm_store_t *s, int64_t source_id, const char *type,
                                        cbm_edge_t **out, int *count);

/* Find edges by target + type. */
int cbm_store_find_edges_by_target_type(cbm_store_t *s, int64_t target_id, const char *type,
                                        cbm_edge_t **out, int *count);

/* Find all edges of a type in project. */
int cbm_store_find_edges_by_type(cbm_store_t *s, const char *project, const char *type,
                                 cbm_edge_t **out, int *count);

/* Count all edges in project. */
int cbm_store_count_edges(cbm_store_t *s, const char *project);

/* Count edges of given type. */
int cbm_store_count_edges_by_type(cbm_store_t *s, const char *project, const char *type);

/* Delete all edges for a project. */
int cbm_store_delete_edges_by_project(cbm_store_t *s, const char *project);

/* Delete edges by type. */
int cbm_store_delete_edges_by_type(cbm_store_t *s, const char *project, const char *type);

/* ── File hash CRUD ─────────────────────────────────────────────── */

int cbm_store_upsert_file_hash(cbm_store_t *s, const char *project, const char *rel_path,
                               const char *sha256, int64_t mtime_ns, int64_t size);

int cbm_store_get_file_hashes(cbm_store_t *s, const char *project, cbm_file_hash_t **out,
                              int *count);

int cbm_store_delete_file_hash(cbm_store_t *s, const char *project, const char *rel_path);

int cbm_store_delete_file_hashes(cbm_store_t *s, const char *project);

/* ── Search ─────────────────────────────────────────────────────── */

int cbm_store_search(cbm_store_t *s, const cbm_search_params_t *params, cbm_search_output_t *out);

/* Free a search output's allocated memory. */
void cbm_store_search_free(cbm_search_output_t *out);

/* ── Traversal ──────────────────────────────────────────────────── */

int cbm_store_bfs(cbm_store_t *s, int64_t start_id, const char *direction, const char **edge_types,
                  int edge_type_count, int max_depth, int max_results, cbm_traverse_result_t *out);

/* Free a traverse result's allocated memory. */
void cbm_store_traverse_free(cbm_traverse_result_t *out);

/* ── Impact analysis ────────────────────────────────────────────── */

typedef enum {
    CBM_RISK_CRITICAL = 0,
    CBM_RISK_HIGH = 1,
    CBM_RISK_MEDIUM = 2,
    CBM_RISK_LOW = 3,
} cbm_risk_level_t;

/* Map BFS hop depth to risk level. */
cbm_risk_level_t cbm_hop_to_risk(int hop);

/* String representation of risk level. */
const char *cbm_risk_label(cbm_risk_level_t level);

typedef struct {
    int critical;
    int high;
    int medium;
    int low;
    int total;
    bool has_cross_service;
} cbm_impact_summary_t;

/* Build impact summary from visited hops and edges. */
cbm_impact_summary_t cbm_build_impact_summary(const cbm_node_hop_t *hops, int hop_count,
                                              const cbm_edge_info_t *edges, int edge_count);

/* Deduplicate BFS hops, keeping minimum hop per node ID.
 * Returns allocated array and count via out params. Caller frees result. */
int cbm_deduplicate_hops(const cbm_node_hop_t *hops, int hop_count, cbm_node_hop_t **out,
                         int *out_count);

/* ── Schema ─────────────────────────────────────────────────────── */

int cbm_store_get_schema(cbm_store_t *s, const char *project, cbm_schema_info_t *out);

/* Free a schema info's allocated memory. */
void cbm_store_schema_free(cbm_schema_info_t *out);

/* ── Architecture ───────────────────────────────────────────────── */

typedef struct {
    const char *language;
    int file_count;
} cbm_language_count_t;

typedef struct {
    const char *name;
    int node_count;
    int fan_in;
    int fan_out;
} cbm_package_summary_t;

typedef struct {
    const char *name;
    const char *qualified_name;
    const char *file;
} cbm_entry_point_t;

typedef struct {
    const char *method;
    const char *path;
    const char *handler;
} cbm_route_info_t;

typedef struct {
    const char *name;
    const char *qualified_name;
    int fan_in;
} cbm_hotspot_t;

typedef struct {
    const char *from;
    const char *to;
    int call_count;
} cbm_cross_pkg_boundary_t;

typedef struct {
    const char *from;
    const char *to;
    const char *type;
    int count;
} cbm_service_link_t;

typedef struct {
    const char *name;
    const char *layer;
    const char *reason;
} cbm_package_layer_t;

typedef struct {
    int id;
    const char *label;
    int members;
    double cohesion;
    const char **top_nodes;
    int top_node_count;
    const char **packages;
    int package_count;
    const char **edge_types;
    int edge_type_count;
} cbm_cluster_info_t;

typedef struct {
    const char *path;
    const char *type; /* "dir" or "file" */
    int children;
} cbm_file_tree_entry_t;

typedef struct {
    /* Pointers first to minimize padding */
    cbm_language_count_t *languages;
    cbm_package_summary_t *packages;
    cbm_entry_point_t *entry_points;
    cbm_route_info_t *routes;
    cbm_hotspot_t *hotspots;
    cbm_cross_pkg_boundary_t *boundaries;
    cbm_service_link_t *services;
    cbm_package_layer_t *layers;
    cbm_cluster_info_t *clusters;
    cbm_file_tree_entry_t *file_tree;
    /* Counts after pointers */
    int language_count;
    int package_count;
    int entry_point_count;
    int route_count;
    int hotspot_count;
    int boundary_count;
    int service_count;
    int layer_count;
    int cluster_count;
    int file_tree_count;
} cbm_architecture_info_t;

int cbm_store_get_architecture(cbm_store_t *s, const char *project, const char **aspects,
                               int aspect_count, cbm_architecture_info_t *out);
void cbm_store_architecture_free(cbm_architecture_info_t *out);

/* ── ADR (Architecture Decision Record) ────────────────────────── */

#define CBM_ADR_MAX_LENGTH 8000

typedef struct {
    const char *project;
    const char *content;
    const char *created_at;
    const char *updated_at;
} cbm_adr_t;

int cbm_store_adr_store(cbm_store_t *s, const char *project, const char *content);
int cbm_store_adr_get(cbm_store_t *s, const char *project, cbm_adr_t *out);
int cbm_store_adr_delete(cbm_store_t *s, const char *project);
int cbm_store_adr_update_sections(cbm_store_t *s, const char *project, const char **keys,
                                  const char **values, int count, cbm_adr_t *out);
void cbm_store_adr_free(cbm_adr_t *adr);

/* ADR section parsing/rendering (pure functions, no store needed) */

enum { PROPS_MAX = 16 };

typedef struct {
    char *keys[PROPS_MAX];
    char *values[PROPS_MAX];
    int count;
} cbm_adr_sections_t;

cbm_adr_sections_t cbm_adr_parse_sections(const char *content);
char *cbm_adr_render(const cbm_adr_sections_t *sections);
int cbm_adr_validate_content(const char *content, char *errbuf, int errbuf_size);
int cbm_adr_validate_section_keys(const char **keys, int count, char *errbuf, int errbuf_size);
void cbm_adr_sections_free(cbm_adr_sections_t *s);

/* ── Search helpers (exposed for testing) ───────────────────────── */

/* Convert a glob pattern to SQL LIKE pattern. Caller must free result. */
char *cbm_glob_to_like(const char *pattern);

/* Extract literal substrings (>= 3 chars) from a regex pattern for LIKE pre-filtering.
 * Bails on alternation (|). Returns count of hints written to out[].
 * Each out[i] is malloc'd — caller must free each string. */
int cbm_extract_like_hints(const char *pattern, char **out, int max_out);

/* Prepend (?i) to a regex pattern if not already present.
 * Returns a static buffer — do NOT free. */
const char *cbm_ensure_case_insensitive(const char *pattern);

/* Strip leading (?i) from a regex pattern.
 * Returns a static buffer — do NOT free. */
const char *cbm_strip_case_flag(const char *pattern);

/* ── Architecture helpers (exposed for testing) ────────────────── */

const char *cbm_qn_to_package(const char *qn);
const char *cbm_qn_to_top_package(const char *qn);
bool cbm_is_test_file_path(const char *fp);
int cbm_store_find_architecture_docs(cbm_store_t *s, const char *project, char ***out, int *count);

/* ── Louvain algorithm ─────────────────────────────────────────── */

typedef struct {
    int64_t src;
    int64_t dst;
} cbm_louvain_edge_t;

typedef struct {
    int64_t node_id;
    int community;
} cbm_louvain_result_t;

int cbm_louvain(const int64_t *nodes, int node_count, const cbm_louvain_edge_t *edges,
                int edge_count, cbm_louvain_result_t **out, int *out_count);

/* ── Memory management helpers ──────────────────────────────────── */

/* Free heap-allocated strings in a stack-allocated node (does NOT free the node itself). */
void cbm_node_free_fields(cbm_node_t *n);

/* Free heap-allocated strings in a stack-allocated project (does NOT free the project itself). */
void cbm_project_free_fields(cbm_project_t *p);

/* Free an array of nodes returned by find_nodes_by_* functions. */
void cbm_store_free_nodes(cbm_node_t *nodes, int count);

/* Free an array of edges returned by find_edges_by_* functions. */
void cbm_store_free_edges(cbm_edge_t *edges, int count);

/* Free an array of projects. */
void cbm_store_free_projects(cbm_project_t *projects, int count);

/* Free an array of file hashes. */
void cbm_store_free_file_hashes(cbm_file_hash_t *hashes, int count);

/* ── Vector search ───────────────────────────────────────────────── */

/* Result from vector similarity search. */
typedef struct {
    int64_t node_id;
    char *name;
    char *qualified_name;
    char *file_path;
    char *label;
    double score;
} cbm_vector_result_t;

/* Search for nodes similar to the given query keywords using stored RI vectors.
 * Builds a merged query vector from the keywords, then does cosine scan via
 * the cbm_cosine_i8 SQL function joined with the nodes table.
 * Returns results sorted by score DESC. Caller must free with cbm_store_free_vector_results. */
int cbm_store_vector_search(cbm_store_t *s, const char *project, const char **keywords,
                            int keyword_count, int limit, cbm_vector_result_t **out,
                            int *out_count);

/* Free vector search results. */
void cbm_store_free_vector_results(cbm_vector_result_t *results, int count);

/* Count vectors for a project. */
int cbm_store_count_vectors(cbm_store_t *s, const char *project);

/* Execute an arbitrary SQL statement (pragmas, FTS5 maintenance, etc).
 * Returns CBM_STORE_OK on success. */
int cbm_store_exec(cbm_store_t *s, const char *sql);

#endif /* CBM_STORE_H */
