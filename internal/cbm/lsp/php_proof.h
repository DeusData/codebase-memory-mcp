#ifndef CBM_LSP_PHP_PROOF_H
#define CBM_LSP_PHP_PROOF_H
#include "php_lsp.h"

bool cbm_php_collect_source_facts(CBMArena *arena, const char *source, int source_len,
                                  const char *module_qn, CBMLSPDef *defs, int count);
/* Independent source proof; legacy inferred types never establish these facts. */
void cbm_php_proof_resolve(CBMArena *arena, const char *source, int source_len,
                           const char *module_qn, TSNode root, const CBMTypeRegistry *registry,
                           const CBMPHPImportBindings *bindings, const CBMLSPDef *defs, int count,
                           CBMResolvedCallArray *out);
void cbm_php_proof_resolve_with_source(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, TSNode root,
                                       const CBMTypeRegistry *registry,
                                       const CBMPHPImportBindings *bindings, const CBMLSPDef *defs,
                                       int count, CBMResolvedCallArray *out,
                                       const cbm_resolve_proof_source_t *source_context);
#endif
