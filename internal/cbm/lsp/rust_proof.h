#ifndef CBM_LSP_RUST_PROOF_H
#define CBM_LSP_RUST_PROOF_H
#include "go_lsp.h"
bool cbm_rust_collect_source_facts(CBMArena *, const char *, int, const char *, CBMLSPDef *, int);
void cbm_rust_proof_resolve(CBMArena *, const char *, int, const CBMTypeRegistry *,
                            const CBMFileResult *, const char **, const char **, int,
                            CBMResolvedCallArray *);
#endif
