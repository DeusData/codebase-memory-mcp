#ifndef CBM_LANG_SPECS_H
#define CBM_LANG_SPECS_H

#include "cbm.h"

// CBMLangSpec mirrors Go's lang.LanguageSpec with NULL-terminated string arrays.
typedef struct {
    CBMLanguage language;
    const char **function_node_types;
    const char **class_node_types;
    const char **field_node_types;
    const char **module_node_types;
    const char **call_node_types;
    const char **import_node_types;
    const char **import_from_types;
    const char **branching_node_types;
    const char **variable_node_types;
    const char **assignment_node_types;
    const char **throw_node_types;
    const char *throws_clause_field; // NULL if none
    const char **decorator_node_types;
    const char **env_access_functions;       // NULL-terminated (NULL if none)
    const char **env_access_member_patterns; // NULL-terminated (NULL if none)
    const TSLanguage *(*ts_factory)(void);   // Tree-sitter grammar factory (NULL if shared)
} CBMLangSpec;

// Get the language spec for a given language. Returns NULL for unsupported.
const CBMLangSpec *cbm_lang_spec(CBMLanguage lang);

// Get the TSLanguage* for a given language. Returns NULL for unsupported.
// These resolve at link time to grammar symbols from Go tree-sitter modules.
const TSLanguage *cbm_ts_language(CBMLanguage lang);

#endif // CBM_LANG_SPECS_H
