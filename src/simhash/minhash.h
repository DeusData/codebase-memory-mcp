/*
 * minhash.h — MinHash fingerprinting + LSH for near-clone detection.
 *
 * Computes K=64 MinHash signatures from AST node-type trigrams using
 * xxHash with distinct seeds.  No external dependencies — xxHash is
 * vendored.  Pure functions — thread-safe, no shared state.
 *
 * Workflow:
 *   1. cbm_minhash_compute()  — during AST extraction (per function)
 *   2. cbm_minhash_jaccard()  — pairwise similarity (K=64 agreement)
 *   3. cbm_lsh_*              — locality-sensitive hashing for O(n)
 *                               candidate generation
 */
#ifndef CBM_MINHASH_H
#define CBM_MINHASH_H

#include <stdint.h>
#include <stdbool.h>

/* Number of hash permutations (seeds).  Larger K = more accurate Jaccard
 * estimate, but more memory per function.  64 gives ±0.12 standard
 * error on Jaccard — sufficient for a 0.95 threshold. */
#define CBM_MINHASH_K 64

/* Minimum number of leaf AST tokens required to compute a fingerprint.
 * Leaf-only counting is language-agnostic: leaf nodes correspond to
 * actual source tokens (identifiers, literals, keywords, operators),
 * not grammar-internal structure that varies across parsers.
 * 30 leaf tokens ≈ BigCloneBench standard of 50 raw source tokens. */
#define CBM_MINHASH_MIN_NODES 30

/* Default Jaccard threshold for SIMILAR_TO edge emission. */
#define CBM_MINHASH_JACCARD_THRESHOLD 0.95

/* Maximum SIMILAR_TO edges per node (prevents utility function explosion). */
#define CBM_MINHASH_MAX_EDGES_PER_NODE 10

/* LSH parameters: b bands × r rows.  Threshold ≈ (1/b)^(1/r). */
#define CBM_LSH_BANDS 32
#define CBM_LSH_ROWS 2

/* ── MinHash fingerprint ─────────────────────────────────────────── */

/* A MinHash signature: K minimum hash values, one per seed. */
typedef struct {
    uint32_t values[CBM_MINHASH_K];
} cbm_minhash_t;

/* Opaque tree-sitter node — forward declared to avoid pulling in
 * tree_sitter/api.h from every consumer. */
typedef struct TSNode TSNode;

/* Compute MinHash fingerprint for a function body's AST.
 *
 * Walks the subtree rooted at `func_body`, normalises leaf node types
 * (identifiers → "I", strings → "S", numbers → "N", type annotations
 * → "T"), builds trigrams, and hashes each trigram with K seeds.
 *
 * Returns true and fills `out` on success.
 * Returns false if the body has fewer than CBM_MINHASH_MIN_NODES
 * normalised tokens (fingerprint not meaningful). */
bool cbm_minhash_compute(TSNode func_body, const char *source, int language, cbm_minhash_t *out);

/* Compute exact Jaccard similarity between two MinHash signatures.
 * Returns value in [0.0, 1.0]. */
double cbm_minhash_jaccard(const cbm_minhash_t *a, const cbm_minhash_t *b);

/* Hex encoding: K uint32 values × 8 hex chars each = 512 chars. */
enum { CBM_MINHASH_HEX_LEN = 512 };

/* Buffer size for hex-encoded fingerprint including NUL. */
enum { CBM_MINHASH_HEX_BUF = 513 };

/* JSON overhead for ,"fp":"..." wrapper (key + quotes + comma + colon). */
enum { CBM_MINHASH_JSON_OVERHEAD = 10 };
void cbm_minhash_to_hex(const cbm_minhash_t *fp, char *buf, int bufsize);

/* Decode a hex string back to a MinHash signature.
 * Returns true on success. */
bool cbm_minhash_from_hex(const char *hex, cbm_minhash_t *out);

/* ── LSH index ───────────────────────────────────────────────────── */

/* Opaque LSH index handle. */
typedef struct cbm_lsh_index cbm_lsh_index_t;

/* Entry stored in the LSH index. */
typedef struct {
    int64_t node_id;
    const cbm_minhash_t *fingerprint;
    const char *file_path; /* for same-file tagging */
    const char *file_ext;  /* for same-language filtering */
} cbm_lsh_entry_t;

/* Create a new LSH index. */
cbm_lsh_index_t *cbm_lsh_new(void);

/* Insert an entry into the LSH index. */
void cbm_lsh_insert(cbm_lsh_index_t *idx, const cbm_lsh_entry_t *entry);

/* Query candidates similar to the given fingerprint.
 * Returns candidate entries via `out` (caller does NOT free the array
 * — it is owned by the index).  Sets `count`. */
void cbm_lsh_query(const cbm_lsh_index_t *idx, const cbm_minhash_t *fp,
                   const cbm_lsh_entry_t ***out, int *count);

/* Free the LSH index and all internal storage. */
void cbm_lsh_free(cbm_lsh_index_t *idx);

#endif /* CBM_MINHASH_H */
