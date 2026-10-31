/* Pure internal whole-file origin join; the adapter supplies authenticated evidence. */
#ifndef CBM_TEST_IMPACT_ORIGINS_H
#define CBM_TEST_IMPACT_ORIGINS_H

#include "mcp/test_impact.h"
#include "mcp/test_impact_changes.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CBM_COVERAGE_ORIGIN_FORMAT_VERSION 2u
#define CBM_COVERAGE_ORIGIN_SHA256_BYTES 32u

typedef struct {
    const unsigned char *data;
    size_t length;
} cbm_coverage_origin_bytes_t;

typedef enum {
    CBM_COVERAGE_ORIGIN_GIT_SHA1 = 1,
    CBM_COVERAGE_ORIGIN_GIT_SHA256 = 2
} cbm_coverage_origin_object_format_t;

typedef struct {
    unsigned char bytes[32]; /* SHA-1 uses first 20; remaining 12 MUST be zero. */
} cbm_coverage_origin_oid_t;

typedef struct {
    /* Provider-namespaced nonempty opaque identity; never infer from root/URL. */
    cbm_coverage_origin_bytes_t repository_key;
    cbm_coverage_origin_object_format_t object_format;
    cbm_coverage_origin_oid_t artifact;   /* A */
    cbm_coverage_origin_oid_t merge_base; /* unique actual M */
    cbm_coverage_origin_oid_t head;       /* pinned H */
    /* Historical field/wire slot: exact identity-table byte digest for this map.
     * Format 1: original functions.tsv. Format 2: original profiles.tsv, including
     * its wire-version/image/count header and raw-name/hash/counter-count rows.
     * The digest binds bytes; it does not authenticate the map or its producer. */
    unsigned char functions_sha256[32];
    uint32_t manifest_version;
    unsigned char manifest_sha256[32];
    unsigned char compatibility_sha256[32];
    unsigned char producer_profile_sha256[32];
} cbm_coverage_origin_binding_t;

typedef struct {
    /* Trusted adapter facts, NEVER copied from parsed manifest declarations. */
    /* Independently established, format-appropriate admission for this exact
     * map and its test observations. A v1 receipt cannot admit a v2 map;
     * successful parsing supplies no admission evidence. */
    bool artifact_admitted;
    bool origin_source_verified;       /* authenticated repository/A/table/manifest tuple */
    bool origin_attestations_verified; /* supported producer claim/proof semantics */
    bool comparisons_verified;         /* complete same-owner A ancestor of actual M, pinned H */
    cbm_coverage_origin_binding_t binding;
    unsigned char artifact_to_head_sha256[32];
    unsigned char merge_base_to_head_sha256[32];
} cbm_coverage_origin_context_t;

typedef struct {
    const cbm_coverage_map_t *coverage;
    const cbm_changes_t *request_changes; /* D4a inventory/state; no hunk use */
    cbm_coverage_origin_bytes_t manifest;
    cbm_coverage_origin_bytes_t artifact_to_head;
    cbm_coverage_origin_bytes_t merge_base_to_head;
    const cbm_coverage_origin_context_t *context;
} cbm_coverage_origin_input_t;

typedef struct {
    uint64_t max_input_bytes; /* manifest + AH + MH + context key; <= UINT64_MAX/8 */
    uint64_t max_items;       /* decoded records, borrowed map checks and ID visits; see spec */
    size_t max_alloc_bytes;   /* cumulative logical arena allocation requests */
    size_t max_result_ids;    /* distinct final union cardinality; positive, <= INT_MAX */
} cbm_coverage_origin_limits_t;

typedef bool (*cbm_coverage_origin_cancel_fn)(void *context);

typedef enum {
    CBM_COVERAGE_ORIGIN_OK = 0,
    CBM_COVERAGE_ORIGIN_INVALID,
    CBM_COVERAGE_ORIGIN_FORMAT,
    CBM_COVERAGE_ORIGIN_BINDING,
    CBM_COVERAGE_ORIGIN_UNVERIFIED,
    CBM_COVERAGE_ORIGIN_LIMIT,
    CBM_COVERAGE_ORIGIN_CANCELLED,
    CBM_COVERAGE_ORIGIN_OOM
} cbm_coverage_origin_status_t;

typedef enum {
    CBM_COVERAGE_ORIGIN_UNKNOWN = 0,
    CBM_COVERAGE_ORIGIN_COMPLETE_IDS = 1,
    CBM_COVERAGE_ORIGIN_ALL = 2
} cbm_coverage_origin_disposition_t;

enum {
    CBM_COVERAGE_ORIGIN_EDIT_A = 1u << 0,
    CBM_COVERAGE_ORIGIN_EDIT_M = 1u << 1,
    CBM_COVERAGE_ORIGIN_EDIT_D = 1u << 2,
    CBM_COVERAGE_ORIGIN_EDIT_T = 1u << 3
};

enum { CBM_COVERAGE_ORIGIN_FROM_ARTIFACT = 1u << 0, CBM_COVERAGE_ORIGIN_FROM_MERGE_BASE = 1u << 1 };

enum {
    CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN = 1u << 0,
    CBM_COVERAGE_ORIGIN_PATH_MISSING = 1u << 1,
    CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN = 1u << 2,
    CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN = 1u << 3,
    CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN = 1u << 4,
    CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED = 1u << 5,
    CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN = 1u << 6
};

typedef struct {
    cbm_coverage_origin_bytes_t path; /* owned NUL-terminated; length authoritative */
    unsigned comparisons;
    char artifact_status; /* A/M/D/T or zero when absent from that comparison */
    char merge_base_status;
    cbm_coverage_origin_disposition_t disposition; /* UNKNOWN for missing row */
    unsigned supported_edits;                      /* zero for missing row */
    unsigned reasons;        /* local gaps only; global gaps affect overall completeness */
    const int *function_ids; /* owned ascending unique positive evidence */
    size_t function_count;
} cbm_coverage_origin_path_t;

typedef struct cbm_coverage_origin_join cbm_coverage_origin_join_t;

/* Clear *out before all other validation. Borrowed spans, map, D4a owner and
 * context stay immutable during this synchronous call. Callback is prompt,
 * non-reentrant and cannot mutate/free inputs. All dynamic storage belongs to
 * one result-owned CBMArena; mutable state is invocation-local.
 *
 * OK returns a fully evaluated owner, possibly incomplete and/or requiring
 * broad fallback. All other statuses return *out=NULL and release call-owned
 * storage. No partial prefix, retained borrowed pointer, or mutation of another
 * owner. Missing evidence is OK/incomplete; malformed input, wrong binding,
 * missing admission, cancel, limit and OOM are errors. No inferred identities.
 * The adapter MUST use can_narrow, never complete alone. */
cbm_coverage_origin_status_t cbm_coverage_origin_join(const cbm_coverage_origin_input_t *input,
                                                      const cbm_coverage_origin_limits_t *limits,
                                                      cbm_coverage_origin_cancel_fn cancelled,
                                                      void *cancel_context,
                                                      cbm_coverage_origin_join_t **out);
void cbm_coverage_origin_join_free(cbm_coverage_origin_join_t *join);

/* Views are immutable until join_free; path views may share one owned ALL ID
 * array. NULL join: pointer=NULL/count=0, complete=false, broad=false,
 * can_narrow=false, reasons=REQUEST_UNKNOWN, request_state=UNKNOWN. */
const cbm_coverage_origin_binding_t *cbm_coverage_origin_join_binding(
    const cbm_coverage_origin_join_t *join);
const int *cbm_coverage_origin_join_ids(const cbm_coverage_origin_join_t *join, int *count);
const cbm_coverage_origin_path_t *cbm_coverage_origin_join_paths(
    const cbm_coverage_origin_join_t *join, size_t *count);
bool cbm_coverage_origin_join_complete(const cbm_coverage_origin_join_t *join);
bool cbm_coverage_origin_join_broad_fallback_required(const cbm_coverage_origin_join_t *join);
/* Exactly complete(join) && !broad_fallback_required(join). Necessary evidence
 * only: it cannot upgrade other independent selector completeness flags. */
bool cbm_coverage_origin_join_can_narrow(const cbm_coverage_origin_join_t *join);
unsigned cbm_coverage_origin_join_reasons(const cbm_coverage_origin_join_t *join);
/* Preserve cross-checked D4a M->H state. Never derive EMPTY from AH or ID count. */
cbm_changes_state_t cbm_coverage_origin_join_request_state(const cbm_coverage_origin_join_t *join);

#endif
