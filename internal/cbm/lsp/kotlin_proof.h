#ifndef CBM_LSP_KOTLIN_PROOF_H
#define CBM_LSP_KOTLIN_PROOF_H
#include "kotlin_lsp.h"
bool cbm_kotlin_collect_source_facts(CBMArena *arena, const char *source, int length,
                                     const char *module, CBMLSPDef *defs, int count);
void cbm_kotlin_proof_resolve(CBMArena *arena, const char *source, int length, TSNode root,
                              const CBMTypeRegistry *registry, const CBMFileResult *file,
                              CBMResolvedCallArray *out);
#endif
