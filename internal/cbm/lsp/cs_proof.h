#ifndef CBM_LSP_CS_PROOF_H
#define CBM_LSP_CS_PROOF_H
#include "cs_lsp.h"
bool cbm_cs_collect_source_facts(CBMArena *arena, const char *source, int length,
                                 const char *module, CBMLSPDef *defs, int count);
void cbm_cs_proof_resolve(CBMArena *arena, const char *source, int length, TSNode root,
                          const CBMTypeRegistry *registry, const CBMFileResult *file,
                          CBMResolvedCallArray *out);
#endif
