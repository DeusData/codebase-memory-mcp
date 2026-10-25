#ifndef CBM_RESOLVE_CAPABILITIES_H
#define CBM_RESOLVE_CAPABILITIES_H
#include "../cbm.h"
#include <stdbool.h>

/* A supported subset is an implementation capability, never evidence for an
 * occurrence. The default for a new language is explicit unavailability. */
enum {
    CBM_RESOLVE_CAP_SCOPE = 1u << 0,
    CBM_RESOLVE_CAP_IMPORT_EXPORT = 1u << 1,
    CBM_RESOLVE_CAP_DECLARED_TYPE = 1u << 2,
    CBM_RESOLVE_CAP_MEMBER = 1u << 3,
    CBM_RESOLVE_CAP_TYPE_FLOW = 1u << 4,
    CBM_RESOLVE_CAP_CALLABLE = 1u << 5
};
typedef struct {
    CBMLanguage language;
    bool syntax_declarations;
    bool syntax_calls;
    bool syntax_imports;
    bool syntax_writes;
    uint32_t proof_subsets;
    const char *adapter;
    const char *limitation;
} cbm_resolve_capabilities_t;

/* Returns false only for an invalid enum. Syntax fields describe configured
 * extraction surfaces, not completeness; independent goldens measure recall. */
bool cbm_resolve_capabilities(CBMLanguage language, cbm_resolve_capabilities_t *out);
#endif
