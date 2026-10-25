#include "capabilities.h"
#include "../lang_specs.h"
#include <string.h>

static bool syntax(const char *const *kinds) {
    return kinds && kinds[0];
}

bool cbm_resolve_capabilities(CBMLanguage language, cbm_resolve_capabilities_t *out) {
    if (!out || language < 0 || language >= CBM_LANG_COUNT)
        return false;
    *out = (cbm_resolve_capabilities_t){
        .language = language,
        .adapter = "dependency_only",
        .limitation = "No semantic binding proof adapter; semantic occurrences abstain."};
    const CBMLangSpec *spec = cbm_lang_spec(language);
    if (spec) {
        out->syntax_declarations =
            syntax(spec->function_node_types) || syntax(spec->class_node_types);
        out->syntax_calls = syntax(spec->call_node_types);
        out->syntax_imports = syntax(spec->import_node_types) || syntax(spec->import_from_types) ||
                              spec->embedded_imports;
        out->syntax_writes = syntax(spec->assignment_node_types);
    }
    switch (language) {
    case CBM_LANG_GO:
        out->adapter = "go";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Exact declarations and typed receivers; unknown owners, effects and "
                          "competing targets abstain.";
        break;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
        out->adapter = "c_family";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Exact visibility and owner proofs; unsupported overload, template, ADL "
                          "and callable-field selection abstain.";
        break;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        out->adapter = "typescript";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Source-audited bindings, annotations and constructions; unproved "
                          "inherited or dynamic lookup abstains.";
        break;
    case CBM_LANG_PYTHON:
        out->adapter = "python";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Explicit imports and bounded reexports; ordinary declared receivers and "
                          "aliases; dynamic/decorated/inherited lookup abstains.";
        break;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_CSHARP:
    case CBM_LANG_PHP:
        out->adapter = language == CBM_LANG_JAVA     ? "java"
                       : language == CBM_LANG_KOTLIN ? "kotlin"
                       : language == CBM_LANG_CSHARP ? "csharp"
                                                     : "php";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW;
        out->limitation = "Source-audited namespaces and direct declared members; unsupported "
                          "inheritance, extensions, overloads and effects abstain.";
        break;
    case CBM_LANG_RUST:
        out->adapter = "rust";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW;
        out->limitation =
            "Declared modules and exact free functions; direct inherent &self methods on &T; "
            "macros, attributes, traits and richer receiver search abstain.";
        break;
    case CBM_LANG_DART:
        out->adapter = "dart_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation = "Direct public source bindings with URI, prefix and show/hide rules; "
                          "parts, reexports and type propagation unimplemented.";
        break;
    case CBM_LANG_LUA:
        out->adapter = "lua_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE;
        out->limitation = "Direct local function bindings; runtime require/export and receiver "
                          "type proofs unimplemented.";
        break;
    case CBM_LANG_ARKTS:
        out->adapter = "javascript_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation = "Supported source bindings only; no typed cross-file dispatch for the "
                          "containing language.";
        break;
    case CBM_LANG_SWIFT:
        out->limitation = "Swift build-module membership and binding proofs unimplemented.";
        break;
    case CBM_LANG_PERL:
        out->limitation =
            "Single-file legacy engine has no admitted cross-file semantic proof bridge.";
        break;
    default:
        break;
    }
    return true;
}
