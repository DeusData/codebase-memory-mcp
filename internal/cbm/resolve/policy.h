#ifndef CBM_RESOLVE_POLICY_H
#define CBM_RESOLVE_POLICY_H

#include "record.h"
#include "proof.h"
#include <stdbool.h>

/* Pure common admission, used after the adapter has validated the occurrence,
 * binding domain and graph identity. Does not infer proof from strategy names
 * or numeric confidence, and never turns an incomplete lookup into a fact. */
bool cbm_resolve_admit(const cbm_resolve_result_t *result);

/* Provisional producer check. A coordinator must bind the exact occurrence
 * and own all support roots before final admission/publication. */
bool cbm_resolve_record_candidate(const cbm_resolve_record_t *record);

/* A producer's occurrence attestation binds semantic authority to one exact
 * source site and relation. Reusable declaration/export roots do not qualify. */
bool cbm_resolve_record_site_proof(const cbm_resolve_record_t *record,
                                   const cbm_resolve_proof_t *proof, const char *project);

/* Edge publication additionally requires an exact source occurrence. IMPORTS
 * denotes a file dependency and does not yet carry a token span. */
bool cbm_resolve_record_admit(const cbm_resolve_record_t *record);

/* An occurrence's properties minus what its own columns already say, so calls
 * that differ only by position share one string. The result is what follows
 * pass_resolver's head, with char(1) in place of the line. *line is that line,
 * 0 when there is none, or -1 when the record has any other shape and the result
 * is its whole text. JSON cannot carry a raw control byte, and one that arrived
 * anyway forces the whole form, so the split always reverses exactly. The caller
 * frees the result with cbm_free(CBM_MEM_CLASS_RESOLVE, ...). */
char *cbm_resolve_properties_tail(const cbm_resolve_record_t *record, int32_t *line);

/* The inputs of a row's occurrence fact: premises[0] is the row's proof, and a
 * binding proof when there is one follows it. */
void cbm_resolve_record_envelope(const cbm_resolve_record_t *record, const char *project,
                                 cbm_resolve_proof_source_t *source, cbm_resolve_proof_key_t *key,
                                 cbm_resolve_result_t *outcome,
                                 const cbm_resolve_proof_t *premises[2], size_t *premise_count);

/* Zero denotes an unsupported rule. Exported for diagnostics and adapters;
 * setting these bits without performing the lookup is not valid evidence. */
uint32_t cbm_resolve_rule_required_facts(cbm_resolve_rule_t rule);

#endif
