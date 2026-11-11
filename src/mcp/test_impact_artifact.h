/*
 * test_impact_artifact.h — The team artifact bundle of one commit.
 *
 * A bundle travels from the main-branch producer to PR selection (never
 * committed). Layout under its directory:
 *
 *   receipt.json                    this module; the bundle's authority
 *   .codebase-memory/graph.db.zst   the commit's frozen graph (artifact.h)
 *   .codebase-memory/artifact.json
 *   coverage/functions.tsv          optional coverage map, format 1
 *   coverage/tests.tsv
 *   coverage/meta.json
 *
 * The receipt binds every part by SHA-256: the graph by the digest of the
 * graph as an importer sees it (cbm_store_graph_digest of the imported
 * database), the map by the exact bytes of its three files. A digest is
 * content binding only; where a bundle came from is established by whoever
 * fetched it, never by anything inside it.
 */
#ifndef CBM_TEST_IMPACT_ARTIFACT_H
#define CBM_TEST_IMPACT_ARTIFACT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CBM_TI_RECEIPT_FILE "receipt.json"
#define CBM_TI_RECEIPT_SCHEMA "cbm.test_impact.artifact.v1"
#define CBM_TI_COVERAGE_DIR "coverage"

typedef struct {
    char commit[65];       /* 40 or 64 lowercase hex */
    char graph_sha256[65]; /* 64 lowercase hex */
    /* cbm_store_graph_content_digest of the same graph: equal for equal graphs
     * however they were built (the weekly incremental-equals-full check). */
    char graph_content_sha256[65];
    /* cbm_store_graph_topology_digest: what selection reads. The weekly check
     * fails when an incremental graph's topology differs from the full one. */
    char graph_topology_sha256[65];
    bool has_coverage;
    char functions_sha256[65];
    char tests_sha256[65];
    char metadata_sha256[65];
    char compatibility_sha256[65];
    /* Oldest retained coverage observation (seconds since the epoch). A full
     * refresh sets it; an incremental publish carries it forward unchanged. */
    int64_t oldest_observation_at;
    char platform[128]; /* producer's platform/toolchain label, informational */
} cbm_ti_receipt_t;

typedef enum {
    CBM_TI_RECEIPT_OK = 0,
    CBM_TI_RECEIPT_ABSENT,  /* no receipt.json */
    CBM_TI_RECEIPT_INVALID, /* unreadable shape, unknown schema, a malformed field */
    CBM_TI_RECEIPT_IO,
} cbm_ti_receipt_status_t;

/* Strict: every field present and well formed, no unknown schema. Clears *out
 * first; on any non-OK status *out stays zero. */
cbm_ti_receipt_status_t cbm_ti_receipt_read(const char *bundle_dir, cbm_ti_receipt_t *out);

/* Writes receipt.json atomically (temporary file + rename). */
bool cbm_ti_receipt_write(const char *bundle_dir, const cbm_ti_receipt_t *receipt);

/* Lowercase SHA-256 hex of a file's exact bytes. false when unreadable. */
bool cbm_ti_sha256_file(const char *path, char hex[65]);

/* Versioned digest of what coverage depends on beyond the code it measures:
 * the platform/toolchain label and the exact bytes of each listed path under
 * root (an absent path is encoded as absent, so adding one changes it).
 * Producer and consumer call this same function; equality is the check. */
bool cbm_ti_compatibility_digest(const char *root, const char *const *paths, int path_count,
                                 const char *platform, char hex[65]);

#endif /* CBM_TEST_IMPACT_ARTIFACT_H */
