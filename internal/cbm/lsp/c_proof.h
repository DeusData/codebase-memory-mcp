#ifndef CBM_LSP_C_PROOF_H
#define CBM_LSP_C_PROOF_H

#include "go_lsp.h"

/* Source modifiers and complete direct class domains. No inherited, template,
 * macro-generated or inferred member becomes authoritative through this audit. */
bool cbm_c_collect_source_facts(CBMArena *arena, const char *source, int length, const char *module,
                                CBMLSPDef *defs, int count);

#endif
