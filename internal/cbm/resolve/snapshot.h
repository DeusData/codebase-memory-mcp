#ifndef CBM_RESOLVE_SNAPSHOT_H
#define CBM_RESOLVE_SNAPSHOT_H
#include "../cbm.h"
#include "record.h"
#include "proof.h"
#include "../../../src/foundation/sha256.h"
cbm_resolve_proof_source_t cbm_result_proof_source(const CBMFileResult *result,
                                                   CBMSourceOrigin origin, const char *scope);
/* Length-delimited canonical fields; no host endianness or pointer identity. */
void cbm_resolve_hash_text(cbm_sha256_ctx *hash, const char *value);
void cbm_resolve_hash_number(cbm_sha256_ctx *hash, uint64_t value);
void cbm_resolve_hash_finish(cbm_sha256_ctx *hash, char output[65]);
/* Summary of extracted declarations/imports/type assignments, separately from
 * stabilized exported bindings and type derivations. Borrows result strings. */
bool cbm_resolve_snapshot(const CBMFileResult *result, const char *path, CBMLanguage language,
                          cbm_resolve_file_t *out, char extracted_summary[65]);
#endif
