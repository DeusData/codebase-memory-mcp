#ifndef CBM_LSP_TS_PROOF_H
#define CBM_LSP_TS_PROOF_H

#include "ts_lsp.h"
#include "../resolve/symbols.h"

void cbm_ts_proof_prepare(TSLSPContext *ctx, TSNode root);
cbm_resolve_method_result_t cbm_ts_proof_method(TSLSPContext *ctx, TSNode receiver,
                                                const char *name, TSNode occurrence);

#endif
