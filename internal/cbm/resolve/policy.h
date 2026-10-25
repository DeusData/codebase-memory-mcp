#ifndef CBM_RESOLVE_POLICY_H
#define CBM_RESOLVE_POLICY_H

#include "record.h"
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

/* Zero denotes an unsupported rule. Exported for diagnostics and adapters;
 * setting these bits without performing the lookup is not valid evidence. */
uint32_t cbm_resolve_rule_required_facts(cbm_resolve_rule_t rule);

#endif
