#ifndef CBM_LSP_PY_PROOF_H
#define CBM_LSP_PY_PROOF_H
#include "py_lsp.h"
bool cbm_py_collect_source_facts(CBMArena *arena, const char *source, int length,
                                 const char *module, CBMLSPDef *defs, int count);
void cbm_py_proof_resolve(CBMArena *arena, const char *source, int length,
                          const CBMTypeRegistry *registry, const CBMFileResult *file,
                          const char **import_names, const char **import_qns, int import_count,
                          CBMResolvedCallArray *out);
#endif
