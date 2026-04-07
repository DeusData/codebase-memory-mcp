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

#endif // CBM_SQLITE_WRITER_H
