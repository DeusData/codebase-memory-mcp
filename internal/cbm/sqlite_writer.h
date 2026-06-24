#ifndef CBM_SQLITE_WRITER_H
#define CBM_SQLITE_WRITER_H

#include <stdint.h>

// --- Input structs (flat, borrowed strings) ---

typedef struct {
    int64_t id; // sequential ID (1..N), assigned by Go
    const char *project;
    const char *label;
    const char *name;
    const char *qualified_name;
    const char *file_path;
    int start_line;
    int end_line;
    const char *properties; // JSON string
} CBMDumpNode;

typedef struct {
    int64_t id; // sequential ID (1..M), assigned by Go
    const char *project;
    int64_t source_id; // final sequential ID (1..N)
    int64_t target_id; // final sequential ID (1..N)
    const char *type;
    const char *properties; // JSON string
    const char *url_path;   // extracted from properties by Go (for idx_edges_url_path)
} CBMDumpEdge;

typedef struct {
    int64_t node_id; // final sequential ID (matches nodes.id)
    const char *project;
    const uint8_t *vector; // int8-quantized vector blob
    int vector_len;        // length in bytes (e.g. 256 for d=256)
} CBMDumpVector;

typedef struct {
    int64_t id; // sequential ID (1..T)
    const char *project;
    const char *token;     // the token string
    const uint8_t *vector; // int8-quantized enriched RI vector blob
    int vector_len;        // length in bytes (e.g. 256 for d=256)
    float idf;             // inverse document frequency weight
} CBMDumpTokenVec;

// --- Public API ---

// Write a complete SQLite .db file from sorted in-memory data.
// Constructs B-tree pages directly — no SQL parser, no INSERTs.
// Returns 0 on success, non-zero on error.
// vectors/vector_count and token_vecs/token_vec_count may be NULL/0.
int cbm_write_db(const char *path, const char *project, const char *root_path,
                 const char *indexed_at, CBMDumpNode *nodes, int node_count, CBMDumpEdge *edges,
                 int edge_count, CBMDumpVector *vectors, int vector_count,
                 CBMDumpTokenVec *token_vecs, int token_vec_count);

// --- Streaming writer: incremental bulk node-table append ---
//
// Lets the indexer flush node rows (including heavy `properties`) to the DB in
// batches, mid-pipeline, freeing heavy memory — while preserving the direct-page
// bulk write (no per-row INSERTs). The nodes table is built across append calls
// via a persistent page builder; everything else (edges, vectors, metadata,
// indexes, sqlite_master) is written at finalize. cbm_write_db() above is a
// one-shot wrapper over this API (open -> append all nodes -> finalize) and
// produces byte-identical output.
//
// Usage: w = cbm_writer_open(path);
//        cbm_writer_append_nodes(w, batch, n) x N  (ascending, contiguous ids);
//        cbm_writer_finalize(w, ...);   // consumes + frees w, closes the file.
typedef struct cbm_db_writer cbm_db_writer_t;

cbm_db_writer_t *cbm_writer_open(const char *path);

// Append a batch of node records. Heavy `properties` are consumed here, so the
// caller may free them after this returns. Node ids must be ascending and
// contiguous across the whole sequence of append calls. Returns 0 on success.
int cbm_writer_append_nodes(cbm_db_writer_t *w, const CBMDumpNode *nodes, int count);

// Finalize: build the nodes-table interior, write edges/vectors/token_vectors,
// metadata, all indexes, and sqlite_master + header. The node/edge/vector arrays
// supply the (light) columns the index builders sort on; node `properties` are
// NOT read here (already written during append). Frees w and closes the file.
int cbm_writer_finalize(cbm_db_writer_t *w, const char *project, const char *root_path,
                        const char *indexed_at, CBMDumpNode *nodes, int node_count,
                        CBMDumpEdge *edges, int edge_count, CBMDumpVector *vectors,
                        int vector_count, CBMDumpTokenVec *token_vecs, int token_vec_count);

#endif // CBM_SQLITE_WRITER_H
