#ifndef CBM_LANG_SPECS_H
#define CBM_LANG_SPECS_H

#include "cbm.h"

// CBMEmbeddedLangSpec describes a sub-language embedded inside a host AST.
// Used by host grammars whose tree-sitter parser does not recurse into the
// embedded content (e.g. Svelte, Vue, Astro: script_element -> raw_text holds
// JavaScript verbatim that the host grammar leaves unparsed). The generic
// embedded-imports walker locates each script_node_type in the host AST,
// finds its content_node_type child, and re-parses that slice with the
// embedded_language's grammar so existing extractors run on the inner AST.
typedef struct {
    const char *script_node_type;  // e.g. "script_element"
    const char *content_node_type; // e.g. "raw_text"
    CBMLanguage embedded_language; // grammar used to re-parse the content slice
} CBMEmbeddedLangSpec;

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
    const char **section_node_types;         // B11: config/markup containers (→ Section label, NOT Class)
    const TSLanguage *(*ts_factory)(void);   // Tree-sitter grammar factory (NULL if shared)
    // NULL-terminated list of embedded sub-languages (NULL if host grammar has
    // no embedded content to re-parse). The terminator is an entry whose
    // script_node_type is NULL.
    const CBMEmbeddedLangSpec *embedded_imports;
} CBMLangSpec;

// Returns a NULL-terminated list of callee-name suffixes that indicate a
// string-dispatch call for a given language (e.g. ".classMethodValue" for
// Python/IRIS), or NULL if the language has no such dispatch pattern.
// Kept out of CBMLangSpec to avoid -Wmissing-field-initializers across 155
// language rows; the table lives in extract_calls.c next to the dispatch code.
const char **cbm_string_dispatch_suffixes(CBMLanguage lang);

// Get the language spec for a given language. Returns NULL for unsupported.
const CBMLangSpec *cbm_lang_spec(CBMLanguage lang);

// Get the TSLanguage* for a given language. Returns NULL for unsupported.
// These resolve at link time to grammar symbols from Go tree-sitter modules.
const TSLanguage *cbm_ts_language(CBMLanguage lang);

#endif // CBM_LANG_SPECS_H
