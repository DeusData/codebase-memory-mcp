#ifndef CBM_RESOLVE_RECORD_H
#define CBM_RESOLVE_RECORD_H

#include "model.h"
#include <stdbool.h>
#include <stddef.h>

/* One extracted occurrence, including unsuccessful lookups. Graph edges are
 * an aggregate projection; this record must not be deduplicated by endpoints.
 * source_origin distinguishes raw and preprocessed coordinates. A missing
 * span is explicit and does not establish occurrence identity. */
typedef struct {
    const char *file_path;
    const char *scope;
    const char *relation;
    const char *spelling;
    const char *properties_json;
    uint32_t start_byte;
    uint32_t end_byte;
    uint32_t source_origin;
    cbm_resolve_result_t result;
    const cbm_resolve_proof_t *binding_proof;
    const char *content_identity;
    const char *configuration_identity;
    /* Transient ownership only: shared immutable solver roots are imported
     * once by the coordinator before their shared arena is destroyed. */
    bool proof_borrowed;
    bool emitted;
} cbm_resolve_record_t;

/* Captured extraction summary, independent of derived lookup outcomes. */
typedef struct {
    const char *file_path;
    const char *module;
    const char *raw_content_identity;
    const char *preprocessed_content_identity;
    const char *configuration_identity;
    const char *extracted_summary;
    uint64_t raw_length, preprocessed_length;
    uint32_t language;
    bool complete;
} cbm_resolve_file_t;

/* A stabilized exported binding/type state, including unresolved states. */
typedef struct {
    const char *file_path;
    const char *key;
    unsigned kind;
    bool module, closed, proof_borrowed;
    const char *payload_json; /* Structured type/version and named dependency set. */
    cbm_resolve_result_t result;
} cbm_resolve_summary_t;

/* The denominator is extracted occurrences, not all genuine references.
 * Recall must be measured independently against source-labelled fixtures,
 * including references extraction missed. */
typedef struct {
    size_t occurrences;
    size_t emitted;
    size_t ambiguous;
    size_t unresolved;
    size_t unsupported;
    size_t limited;
    size_t failed;
} cbm_resolve_counts_t;

#endif
