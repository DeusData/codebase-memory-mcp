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
typedef enum {
    CBM_RESOLVE_EDGE_IMPORTS,
    CBM_RESOLVE_EDGE_CALLS,
    CBM_RESOLVE_EDGE_CALL_REFERENCE,
    CBM_RESOLVE_EDGE_USAGE,
    CBM_RESOLVE_EDGE_READS,
    CBM_RESOLVE_EDGE_WRITES,
    CBM_RESOLVE_EDGE_USES_TYPE,
    CBM_RESOLVE_EDGE_COUNT
} cbm_resolve_edge_kind_t;

typedef enum {
    CBM_RESOLVE_EDGE_UNSUPPORTED,
    CBM_RESOLVE_EDGE_SUPPORTED_SUBSET
} cbm_resolve_edge_availability_t;

typedef struct {
    const char *relation;
    cbm_resolve_edge_availability_t availability;
    const char *limitation;
} cbm_resolve_edge_capability_t;

typedef struct {
    CBMLanguage language;
    bool syntax_declarations;
    bool syntax_calls;
    bool syntax_imports;
    bool syntax_writes;
    uint32_t proof_subsets;
    const char *adapter;
    const char *limitation;
    /* Every valid language has all seven entries. Syntax and legacy proof
     * categories do not enable an edge; each supported subset needs a producer. */
    cbm_resolve_edge_capability_t edges[CBM_RESOLVE_EDGE_COUNT];
} cbm_resolve_capabilities_t;

/* Returns false only for an invalid enum. Syntax fields describe configured
 * extraction surfaces, not completeness; independent goldens measure recall. */
bool cbm_resolve_capabilities(CBMLanguage language, cbm_resolve_capabilities_t *out);
/* Exact, case-sensitive graph relation names. False means invalid input or
 * an unknown relation, not an unsupported language/relation pair. Returned
 * strings are static. Unsupported entries remain explicit quality gaps. */
bool cbm_resolve_edge_capability(CBMLanguage language, const char *relation,
                                 cbm_resolve_edge_capability_t *out);
#endif
