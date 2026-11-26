/* Pure test-impact lane projection and result codec.
 * Provider evidence remains the caller's responsibility.
 */
#ifndef CBM_MCP_TEST_IMPACT_RESULT_H
#define CBM_MCP_TEST_IMPACT_RESULT_H

#include "mcp/test_impact.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    const unsigned char *data;
    size_t length;
} cbm_test_result_bytes_t;

typedef enum {
    CBM_TEST_RESULT_OK = 0,
    CBM_TEST_RESULT_INVALID,
    CBM_TEST_RESULT_UNSUPPORTED,
    CBM_TEST_RESULT_LIMIT,
    CBM_TEST_RESULT_CANCELLED,
    CBM_TEST_RESULT_OOM,
    /* Existing policy matcher returned false without a classified cause. */
    CBM_TEST_RESULT_MATCH_FAILED
} cbm_test_result_status_t;

/* Ordinals are internal, not wire values. These supplement the existing
 * CBM_TEST_SELECT_* reason mask and have fixed uppercase wire spellings.
 */
typedef enum {
    CBM_TEST_RESULT_FALLBACK_POLICY_UNAVAILABLE = 0,
    CBM_TEST_RESULT_FALLBACK_ACTIVATION_UNKNOWN,
    CBM_TEST_RESULT_FALLBACK_RULE_TARGET_UNKNOWN,
    CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
    CBM_TEST_RESULT_FALLBACK_GRAPH_REJECTED,
    CBM_TEST_RESULT_FALLBACK_CAPABILITY_MISSING,
    CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE,
    CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
    CBM_TEST_RESULT_FALLBACK_ENGINE_SATURATED,
    CBM_TEST_RESULT_FALLBACK_UNMAPPED_FILE,
    CBM_TEST_RESULT_FALLBACK_UNRESOLVED_CHANGED_TEST,
    CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE,
    CBM_TEST_RESULT_FALLBACK_RULE_RUN_ALL,
    CBM_TEST_RESULT_FALLBACK_POLICY_EVALUATION_FAILED,
    CBM_TEST_RESULT_FALLBACK_TRAVERSAL_POLICY_UNKNOWN,
    /* The walk reached the test runner's entry point: every suite may run
     * changed code at start-up (smart-ci-design review M-3). */
    CBM_TEST_RESULT_FALLBACK_RUNNER_REACHED,
    CBM_TEST_RESULT_FALLBACK_COUNT
} cbm_test_result_fallback_t;

typedef enum {
    CBM_TEST_RESULT_COMPARISON_EMPTY = 0,
    CBM_TEST_RESULT_COMPARISON_CHANGED,
    CBM_TEST_RESULT_COMPARISON_UNAVAILABLE
} cbm_test_result_comparison_t;

typedef struct {
    int rule_index; /* zero-based index in the supplied policy */
    /* One aggregate per causally matched rule. True means EVERY represented
     * target was resolved. Required true for run_all/ignore (no target).
     * First-match classification and path completeness remain upstream facts.
     */
    bool target_resolved;
} cbm_test_result_rule_match_t;

typedef enum {
    CBM_TEST_RESULT_GRAPH_UNAVAILABLE = 0,
    CBM_TEST_RESULT_GRAPH_REJECTED,
    CBM_TEST_RESULT_GRAPH_CERTIFIED
} cbm_test_result_graph_state_t;

typedef enum {
    CBM_TEST_RESULT_COVERAGE_UNAVAILABLE = 0,
    CBM_TEST_RESULT_COVERAGE_REJECTED,
    CBM_TEST_RESULT_COVERAGE_ADMITTED
} cbm_test_result_coverage_state_t;

typedef enum {
    CBM_TEST_RESULT_OBJECT_UNKNOWN = 0,
    CBM_TEST_RESULT_OBJECT_SHA1,
    CBM_TEST_RESULT_OBJECT_SHA256
} cbm_test_result_object_format_t;

/* UNKNOWN is absent and requires all zero bytes. SHA1 uses bytes[0..19]
 * with bytes[20..31] zero. SHA256 uses all bytes. No textual abbreviation.
 */
typedef struct {
    cbm_test_result_object_format_t format;
    unsigned char bytes[32];
} cbm_test_result_oid_t;

typedef enum {
    CBM_TEST_RESULT_COVERAGE_FORMAT_UNKNOWN = 0,
    CBM_TEST_RESULT_COVERAGE_FORMAT_FUNCTIONS_V1,
    CBM_TEST_RESULT_COVERAGE_FORMAT_PROFILES_V2
} cbm_test_result_coverage_format_t;

/* Reporting codes, not evidence-verification operations. */
typedef enum {
    CBM_TEST_RESULT_EVIDENCE_PROVIDER_UNAVAILABLE = 0,
    CBM_TEST_RESULT_EVIDENCE_SOURCE_UNVERIFIED,
    CBM_TEST_RESULT_EVIDENCE_IDENTITY_MISMATCH,
    CBM_TEST_RESULT_EVIDENCE_CONTENT_MISMATCH,
    CBM_TEST_RESULT_EVIDENCE_COMMIT_MISMATCH,
    CBM_TEST_RESULT_EVIDENCE_ANCESTRY_UNPROVED,
    CBM_TEST_RESULT_EVIDENCE_TOO_OLD,
    CBM_TEST_RESULT_EVIDENCE_COMPATIBILITY_MISMATCH,
    CBM_TEST_RESULT_EVIDENCE_METADATA_INVALID,
    CBM_TEST_RESULT_EVIDENCE_FORMAT_UNSUPPORTED,
    CBM_TEST_RESULT_EVIDENCE_INVENTORY_INCOMPLETE,
    CBM_TEST_RESULT_EVIDENCE_DIAGNOSTICS_INCOMPLETE,
    CBM_TEST_RESULT_EVIDENCE_CAPABILITY_MISSING,
    CBM_TEST_RESULT_EVIDENCE_SEMANTIC_INPUTS_UNPROVED,
    CBM_TEST_RESULT_EVIDENCE_APPLICABILITY_UNPROVED,
    CBM_TEST_RESULT_EVIDENCE_OBSERVATIONS_INCOMPLETE,
    CBM_TEST_RESULT_EVIDENCE_IMAGE_UNPROVED,
    CBM_TEST_RESULT_EVIDENCE_COUNT
} cbm_test_result_evidence_reason_t;

typedef struct {
    const cbm_test_result_evidence_reason_t *values;
    size_t count;
} cbm_test_result_evidence_reasons_t;

typedef enum {
    CBM_TEST_RESULT_APPLICABILITY_UNAVAILABLE = 0,
    CBM_TEST_RESULT_APPLICABILITY_REJECTED,
    CBM_TEST_RESULT_APPLICABILITY_ESTABLISHED
} cbm_test_result_applicability_state_t;

typedef enum {
    CBM_TEST_RESULT_APPLICABILITY_KIND_UNKNOWN = 0,
    CBM_TEST_RESULT_APPLICABILITY_ALL_MODIFICATIONS,
    CBM_TEST_RESULT_APPLICABILITY_EXACT_QUERY
} cbm_test_result_applicability_kind_t;

typedef struct {
    bool present;
    unsigned char bytes[32];
} cbm_test_result_digest_t;

typedef struct {
    bool present;
    int64_t value;
} cbm_test_result_timestamp_t;

typedef struct {
    cbm_test_result_graph_state_t state;
    cbm_test_result_oid_t commit;
    cbm_test_result_bytes_t generation;
    cbm_test_result_digest_t sha256;
    cbm_test_result_evidence_reasons_t rejection_reasons;
} cbm_test_result_graph_receipt_t;

typedef struct {
    cbm_test_result_applicability_state_t state;
    cbm_test_result_applicability_kind_t kind;
    uint32_t version;                /* zero = unknown; established all-modifications = 2 */
    cbm_test_result_bytes_t profile; /* empty = unknown; exact-query needs one */
    cbm_test_result_digest_t certificate_sha256;
    cbm_test_result_oid_t artifact_commit;
    cbm_test_result_oid_t merge_base;
    cbm_test_result_oid_t head;
    cbm_test_result_evidence_reasons_t rejection_reasons;
} cbm_test_result_applicability_receipt_t;

typedef struct {
    cbm_test_result_coverage_state_t state;
    cbm_test_result_coverage_format_t format;
    cbm_test_result_bytes_t artifact;
    cbm_test_result_oid_t commit;
    cbm_test_result_digest_t image_sha256;
    cbm_test_result_digest_t identities_sha256;
    cbm_test_result_digest_t tests_sha256;
    cbm_test_result_digest_t metadata_sha256;
    cbm_test_result_digest_t graph_sha256;
    cbm_test_result_digest_t compatibility_sha256;
    cbm_test_result_timestamp_t oldest_observation_at;
    cbm_test_result_evidence_reasons_t rejection_reasons;
    cbm_test_result_applicability_receipt_t applicability;
} cbm_test_result_coverage_receipt_t;

typedef struct {
    cbm_test_result_bytes_t language;
    cbm_test_result_bytes_t edge;
} cbm_test_result_extension_t;

typedef enum {
    CBM_TEST_RESULT_ROUTES_NOT_USED = 0,
    CBM_TEST_RESULT_ROUTES_FOLLOWED,
    CBM_TEST_RESULT_ROUTES_UNKNOWN
} cbm_test_result_route_state_t;

typedef struct {
    cbm_test_result_object_format_t object_format;
    cbm_test_result_oid_t base;
    cbm_test_result_oid_t head;
    cbm_test_result_oid_t merge_base;
    cbm_test_result_digest_t diff_sha256;
    cbm_test_result_digest_t name_status_sha256;
    cbm_test_result_digest_t config_sha256;
    cbm_test_result_digest_t policy_sha256;
    cbm_test_result_graph_receipt_t graph;
    cbm_test_result_coverage_receipt_t coverage;
    const cbm_test_result_extension_t *extensions;
    size_t extension_count;
    cbm_test_result_route_state_t routes;
} cbm_test_result_receipt_t;

typedef enum {
    CBM_TEST_RESULT_WARNING_TEST_NOT_REGISTERED = 0,
    CBM_TEST_RESULT_WARNING_SOURCE_MAPPING_UNKNOWN,
    CBM_TEST_RESULT_WARNING_DIAGNOSTICS_INCOMPLETE,
    CBM_TEST_RESULT_WARNING_UNSUPPORTED_MAPPING,
    CBM_TEST_RESULT_WARNING_COUNT
} cbm_test_result_warning_code_t;

typedef struct {
    cbm_test_result_warning_code_t code;
    cbm_test_result_bytes_t file; /* raw path bytes; empty = unavailable */
    uint64_t line;                /* zero = unavailable */
} cbm_test_result_warning_t;

typedef struct {
    cbm_test_result_comparison_t comparison;
    const cbm_test_selection_t *selection;
    const cbm_test_model_t *model;
    const cbm_test_policy_t *policy;
    bool inventory_complete;  /* external runner inventory, not model parsing */
    bool activation_complete; /* complete upstream first-match/activation facts */
    const cbm_test_result_rule_match_t *matched_rules;
    size_t matched_rule_count;
    const cbm_test_result_fallback_t *fallbacks;
    size_t fallback_count;
    const cbm_test_result_warning_t *warnings;
    size_t warning_count;
    const cbm_test_result_receipt_t *receipt;
} cbm_test_result_input_t;

typedef struct {
    uint64_t max_input_bytes;
    uint64_t max_items;
    size_t max_alloc_bytes;
    size_t max_output_bytes;
} cbm_test_result_limits_t;

typedef bool (*cbm_test_result_cancel_fn)(void *context);
typedef struct cbm_test_result cbm_test_result_t;

/* Pure synchronous build; borrows immutable input only for this call.
 * Clear *out before validation. All limits positive. Non-OK: *out=NULL.
 * Does not certify provider facts, resolve rules, read files or execute tests.
 * Lane precedence: (1) nonempty global set -> fallback, including EMPTY;
 * (2) empty global set plus EMPTY -> shortcut; (3) remaining CHANGED ->
 * ordinary projection. UNAVAILABLE necessarily has a global reason.
 * Opaque owners must be valid, immutable, and model finished. Receipt is
 * required, including when it contains only unavailable evidence.
 * Existing policy matcher calls retain their separately stated work bound
 * and allocation allowance; cancellation applies to owned loops only.
 * max_output_bytes covers raw codec JSON, excluding its storage NUL. An outer
 * MCP duplication/escaping gate is separate and mandatory for transport use.
 */
cbm_test_result_status_t cbm_test_result_build(const cbm_test_result_input_t *input,
                                               const cbm_test_result_limits_t *limits,
                                               cbm_test_result_cancel_fn cancelled,
                                               void *cancel_context, cbm_test_result_t **out);

/* Borrowed UTF-8 NUL-terminated wire (length excludes NUL), or NULL/zero.
 * The digest is lowercase 64-hex with NUL. Both views last until free.
 */
const char *cbm_test_result_json(const cbm_test_result_t *result, size_t *length);
const char *cbm_test_result_decision_sha256(const cbm_test_result_t *result);
void cbm_test_result_free(cbm_test_result_t *result);

#endif
