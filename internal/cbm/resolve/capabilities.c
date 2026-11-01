#include "capabilities.h"
#include "../lang_specs.h"
#include <string.h>

static bool syntax(const char *const *kinds) {
    return kinds && kinds[0];
}

/* Positive rows are audited producer subsets, independent of parser syntax.
 * Unlisted languages and relations keep the explicit unsupported default. */
#define EDGE(name) (1u << CBM_RESOLVE_EDGE_##name)
#define CALLABLE_EDGES (EDGE(CALLS) | EDGE(CALL_REFERENCE))
#define C_EDGES (EDGE(IMPORTS) | CALLABLE_EDGES | EDGE(USAGE) | EDGE(READS) | EDGE(WRITES))
#define SOURCE_EDGES (EDGE(IMPORTS) | CALLABLE_EDGES | EDGE(USAGE))
#define TYPED_EDGES (EDGE(IMPORTS) | EDGE(CALLS) | EDGE(USES_TYPE))
#define OO_EDGES \
    (EDGE(IMPORTS) | CALLABLE_EDGES | EDGE(USAGE) | EDGE(READS) | EDGE(WRITES) | EDGE(USES_TYPE))
typedef struct {
    CBMLanguage language;
    unsigned supported;
    const char *imports;
} edge_row_t;
static const edge_row_t edge_rows[] = {
    {CBM_LANG_GO, SOURCE_EDGES,
     "Exact project package paths with configured roots; unresolved external packages abstain."},
    {CBM_LANG_C, C_EDGES,
     "Literal include paths and configured search roots only; unproved preprocessing abstains."},
    {CBM_LANG_CPP, C_EDGES,
     "Literal include paths and configured search roots only; unproved preprocessing abstains."},
    {CBM_LANG_CUDA, C_EDGES,
     "Literal include paths and configured search roots only; unproved preprocessing abstains."},
    {CBM_LANG_OBJC, EDGE(IMPORTS),
     "Literal include paths and configured search roots only; no semantic proof bridge "
     "advertised."},
    {CBM_LANG_JAVASCRIPT, SOURCE_EDGES | EDGE(WRITES),
     "Exact relative modules, directory entries and configured package roots, ES and CommonJS "
     "export surfaces; runtime module paths and classic browser scripts abstain."},
    {CBM_LANG_TYPESCRIPT, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Exact relative modules, directory entries and configured package roots, ES and CommonJS "
     "export surfaces; runtime module paths abstain."},
    {CBM_LANG_TSX, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Exact relative modules, directory entries and configured package roots, ES and CommonJS "
     "export surfaces; runtime module paths abstain."},
    {CBM_LANG_ARKTS, SOURCE_EDGES,
     "Source JavaScript-style imports only; containing-language typed dispatch is unsupported."},
    {CBM_LANG_VUE, CALLABLE_EDGES,
     "One inline script with audited lexical bindings and an inert template; multiple script "
     "contexts, template effects and external script sources abstain."},
    {CBM_LANG_SVELTE, CALLABLE_EDGES,
     "One inline script with audited lexical bindings; only static markup and bare-identifier "
     "event references are supported. Multiple contexts and template effects abstain."},
    {CBM_LANG_PYTHON, SOURCE_EDGES | EDGE(USES_TYPE),
     "Static project imports and bounded reexports; dynamic imports and unknown package roots "
     "abstain."},
    {CBM_LANG_JAVA, OO_EDGES,
     "Exact declared package members; incomplete or competing namespace candidates abstain."},
    {CBM_LANG_KOTLIN, OO_EDGES,
     "Exact declared packages and aliases; incomplete or competing namespace candidates abstain."},
    {CBM_LANG_CSHARP, OO_EDGES,
     "Exact declared namespace members; unproved aliases and competing candidates abstain."},
    {CBM_LANG_PHP, TYPED_EDGES,
     "Declared namespaces, Composer PSR-4 and literal includes; dynamic paths abstain."},
    {CBM_LANG_RUST, TYPED_EDGES,
     "Explicit crate/module trees and use paths; macros, attributes and unknown roots abstain."},
    {CBM_LANG_DART, EDGE(IMPORTS) | EDGE(CALLS),
     "Direct URI imports with prefix and show/hide bindings; parts and reexports are unsupported."},
    {CBM_LANG_LUA, EDGE(CALLS), NULL},
    {CBM_LANG_CSS, EDGE(IMPORTS),
     "Top-level quoted literal imports before style rules, relative to the source stylesheet; "
     "URL functions, escaping, layer ordering and remote paths abstain."},
    {CBM_LANG_BICEP, EDGE(IMPORTS),
     "Complete literal local module paths relative to the declaring file; interpolation, "
     "registries and template specifications abstain."},
    {CBM_LANG_CAPNP, EDGE(IMPORTS),
     "Complete literal relative import paths; absolute search paths, escaped and concatenated "
     "strings abstain."},
    {CBM_LANG_JUST, EDGE(IMPORTS),
     "Complete literal imports relative to the declaring file; home expansion, shell expansion, "
     "escaping and computed paths abstain."},
    {CBM_LANG_FUNC, EDGE(IMPORTS),
     "Complete literal #include paths relative to the including source file; escaped strings, "
     "computed paths and custom source-provider mappings abstain."},
    {CBM_LANG_SCSS, EDGE(IMPORTS),
     "Complete initial literal @use/@forward URLs with unique relative file/partial candidates; "
     "@import, configured importers, load paths, index directories and effects abstain."},
    {CBM_LANG_MESON, EDGE(IMPORTS),
     "Complete literal subdir calls in an unconditional initial statement prefix; parent "
     "traversal, keyword arguments, conditions and prior unknown effects abstain."},
    {CBM_LANG_SCALA, EDGE(IMPORTS),
     "Exact declared namespace members only; no semantic binding proof bridge advertised."},
    {CBM_LANG_HCL, EDGE(IMPORTS) | EDGE(USAGE),
     "Terraform/OpenTofu literal local module sources and Terragrunt literal paths with audited "
     "parent-directory evaluation; remote, dynamic and ambiguous inputs abstain."},
    {CBM_LANG_YAML, EDGE(IMPORTS) | EDGE(CALLS) | EDGE(USAGE),
     "Ansible literal task/playbook paths and repository role paths only; dynamic directives "
     "and unsupported YAML values abstain."},
    {CBM_LANG_K8S, EDGE(USAGE), NULL},
    {CBM_LANG_KUSTOMIZE, EDGE(IMPORTS),
     "Literal relative resource paths only; transformed or generated resources are unsupported."},
};
#undef OO_EDGES
#undef TYPED_EDGES
#undef SOURCE_EDGES
#undef C_EDGES
#undef CALLABLE_EDGES
#undef EDGE

static void edge_capabilities(cbm_resolve_capabilities_t *out) {
    static const char *const names[CBM_RESOLVE_EDGE_COUNT] = {
        "IMPORTS", "CALLS", "CALL_REFERENCE", "USAGE", "READS", "WRITES", "USES_TYPE"};
    static const char *const unavailable[CBM_RESOLVE_EDGE_COUNT] = {
        "No audited dependency-path subset is advertised; missing imports are a resolver quality "
        "gap.",
        "No admitted invocation subset is advertised; missing calls are a resolver quality gap.",
        "No admitted callable-reference subset is advertised; syntax alone cannot prove a "
        "callable.",
        "No admitted declaration-usage subset is advertised; matching spelling cannot prove "
        "identity.",
        "No admitted read-target subset is advertised; read syntax cannot prove declaration "
        "identity.",
        "No admitted write-target subset is advertised; assignment syntax cannot prove declaration "
        "identity.",
        "No admitted type-reference subset is advertised; type spelling cannot prove declaration "
        "identity."};
    for (int i = 0; i < CBM_RESOLVE_EDGE_COUNT; i++)
        out->edges[i] =
            (cbm_resolve_edge_capability_t){.relation = names[i],
                                            .availability = CBM_RESOLVE_EDGE_UNSUPPORTED,
                                            .limitation = unavailable[i]};
    for (size_t row = 0; row < sizeof(edge_rows) / sizeof(edge_rows[0]); row++) {
        const edge_row_t *entry = &edge_rows[row];
        if (entry->language != out->language)
            continue;
        for (int i = 0; i < CBM_RESOLVE_EDGE_COUNT; i++) {
            if (!(entry->supported & (1u << i)))
                continue;
            out->edges[i].availability = CBM_RESOLVE_EDGE_SUPPORTED_SUBSET;
            out->edges[i].limitation =
                i == CBM_RESOLVE_EDGE_IMPORTS ? entry->imports : out->limitation;
        }
        break;
    }
    if (out->language == CBM_LANG_C || out->language == CBM_LANG_CPP ||
        out->language == CBM_LANG_CUDA) {
        const char *fields =
            "Exact field identifier with a proved owner; unknown receiver, ambiguous "
            "inheritance and unmodelled effects abstain.";
        out->edges[CBM_RESOLVE_EDGE_READS].limitation = fields;
        out->edges[CBM_RESOLVE_EDGE_WRITES].limitation = fields;
    }
    if (out->language == CBM_LANG_HCL)
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Terraform/OpenTofu exact declarations within a module directory and declared outputs "
            "of a literal local module source; duplicate declarations and unsupported inputs "
            "abstain.";
    if (out->language == CBM_LANG_KOTLIN)
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Exact source-bound value references, including composite callable-reference "
            "arguments; unproved receiver identity and dynamic values abstain.";
    if (out->language == CBM_LANG_YAML) {
        out->edges[CBM_RESOLVE_EDGE_CALLS].limitation =
            "Ansible exact handlers in the current play or default role handlers/main.yml or "
            "handlers/main.yaml, and literal listener-topic membership; duplicate named handlers "
            "and dynamic names abstain.";
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Kubernetes raw literal image repositories and core Secret, ConfigMap, ServiceAccount "
            "and PersistentVolumeClaim references in explicit flat Kustomize resource lists; exact "
            "namespaces and unique raw declarations are required. Aliases, duplicate keys, nested "
            "builds and transformed manifests abstain. Compose image repository references "
            "require exact source spans and a captured environment input; unsupported images "
            "abstain.";
    }
    if (out->language == CBM_LANG_K8S)
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Kubernetes raw literal image repositories and core Secret, ConfigMap, ServiceAccount "
            "and PersistentVolumeClaim references in explicit flat Kustomize resource lists; "
            "unique declarations and exact namespaces are required. Aliases, duplicate keys, "
            "nested builds and transformed manifests abstain.";
    if (out->language == CBM_LANG_GO)
        out->edges[CBM_RESOLVE_EDGE_WRITES].limitation =
            "No admitted Go field/write bridge; assignment extraction remains a resolver quality "
            "gap.";
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
    case CBM_LANG_HCL:
        out->adapter = "hcl_infrastructure";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation = "Audited Terraform and Terragrunt occurrences only; dynamic evaluation "
                          "and unsupported or ambiguous declarations abstain.";
        break;
    case CBM_LANG_YAML:
        out->adapter = "yaml_infrastructure";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation = "Audited infrastructure occurrences only; dynamic values and "
                          "unsupported source mappings abstain.";
        break;
    case CBM_LANG_K8S:
        out->adapter = "kubernetes_infrastructure";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE;
        out->limitation = "Audited Kubernetes image and object references only; transformed "
                          "manifests, dynamic values and ambiguous resource names abstain.";
        break;
    case CBM_LANG_VUE:
    case CBM_LANG_SVELTE:
        out->adapter = "embedded_javascript_lexical";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = language == CBM_LANG_VUE
                              ? "One inline script with audited lexical bindings and an inert "
                                "template; multiple scripts, template effects and external script "
                                "sources abstain."
                              : "One inline script with audited lexical bindings; static markup "
                                "and bare-identifier event references only. Multiple script "
                                "contexts and template effects abstain.";
        break;
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
        out->limitation = "Source-audited lexical scopes, annotations, constructions and declared "
                          "fields; unproved inherited or dynamic lookup and classic browser "
                          "scripts abstain.";
        break;
    case CBM_LANG_PYTHON:
        out->adapter = "python";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation =
            "Explicit imports and bounded reexports; ordinary declared receivers and "
            "aliases; dynamic/inherited lookup and decorated callable identity abstain.";
        break;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_CSHARP:
        out->adapter = language == CBM_LANG_JAVA     ? "java"
                       : language == CBM_LANG_KOTLIN ? "kotlin"
                                                     : "csharp";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation =
            "The declaration static rules select: lexical scopes, import precedence, inherited "
            "members over resolved supertypes, arity, known argument types and exact overload "
            "matches. Competing overloads on different nodes, lambdas with an unreadable "
            "receiver, members hidden by an unindexed supertype and unmodelled inference abstain. "
            "A type-shaped name is assumed not to be a field of an unindexed supertype. Implicit "
            "iterator and componentN calls share one span and are not emitted. A bare call inside "
            "an extension on an external type abstains, because that type's members are unlisted. "
            "A C# file the grammar cannot read is read without its later conditional branches.";
        break;
    case CBM_LANG_PHP:
        out->adapter = "php";
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
    edge_capabilities(out);
    return true;
}

bool cbm_resolve_edge_capability(CBMLanguage language, const char *relation,
                                 cbm_resolve_edge_capability_t *out) {
    cbm_resolve_capabilities_t capabilities;
    if (!out || !relation || !cbm_resolve_capabilities(language, &capabilities))
        return false;
    for (int i = 0; i < CBM_RESOLVE_EDGE_COUNT; i++) {
        if (strcmp(relation, capabilities.edges[i].relation) == 0) {
            *out = capabilities.edges[i];
            return true;
        }
    }
    return false;
}
