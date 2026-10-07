/* Versioned content binding for one stable store read snapshot. */
#ifndef CBM_STORE_GRAPH_DIGEST_H
#define CBM_STORE_GRAPH_DIGEST_H

#include "store/store.h"

#include <stddef.h>
#include <stdint.h>

#define CBM_STORE_GRAPH_DIGEST_VERSION 1u
#define CBM_STORE_GRAPH_DIGEST_BYTES 32u

typedef enum {
    CBM_STORE_GRAPH_DIGEST_OK = 0,
    CBM_STORE_GRAPH_DIGEST_INVALID,
    CBM_STORE_GRAPH_DIGEST_SCHEMA,
    CBM_STORE_GRAPH_DIGEST_PROJECT_MISSING,
    CBM_STORE_GRAPH_DIGEST_LIMIT,
    CBM_STORE_GRAPH_DIGEST_CANCELLED,
    CBM_STORE_GRAPH_DIGEST_ERROR
} cbm_store_graph_digest_status_t;

typedef struct {
    uint64_t max_rows;
    uint64_t max_framed_bytes;
} cbm_store_graph_digest_limits_t;

typedef struct {
    uint32_t version;
    unsigned char sha256[CBM_STORE_GRAPH_DIGEST_BYTES];
    uint64_t rows;
    uint64_t framed_bytes;
} cbm_store_graph_digest_t;

/* Borrow a live, exclusive D5 scope. Hash the version-1 canonical stream of
 * all nine known tables, including their exact CREATE SQL and explicit project
 * identity. The database must use UTF-8. No new handler or transaction is used.
 * Independent scopes/connections may run concurrently; no mutable state is
 * shared. The callback/connection restrictions of D5 continue to apply.
 *
 * All arguments are required. project is nonempty, length <= INT_MAX, and has
 * no embedded NUL; other database values are length-aware. Limits are positive,
 * with max_framed_bytes <= UINT64_MAX/8. Bytes count the entire framed input;
 * rows count data rows only. Limits do not cap SQLite's internal sort/value
 * memory. Large values are hashed with scope checks at most 65536 bytes apart.
 *
 * Clear out before validation. Any failure leaves it zero and invalidates a
 * supplied live scope. LIMIT latches D5 ERR; cancellation latches CANCELLED.
 * Statements are always finalized. There is no result owner to free. The
 * caller closes the scope and must reject the digest and discard the connection
 * if closing returns SCOPE_DISCARD. A digest is content binding only, never
 * authenticity, source identity, extraction completeness or permission to
 * narrow selection. Do not reuse it as certification for another read scope. */
cbm_store_graph_digest_status_t cbm_store_graph_digest(
    cbm_store_read_scope_t *scope, const unsigned char *project, size_t project_len,
    const cbm_store_graph_digest_limits_t *limits, cbm_store_graph_digest_t *out);

#endif /* CBM_STORE_GRAPH_DIGEST_H */
