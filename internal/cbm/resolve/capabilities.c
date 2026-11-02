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
    {CBM_LANG_GO, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Exact project package paths from the source's module, its go.mod replace directives and "
     "the enclosing go.work, also when another go.mod declares the same module path; every "
     "other package abstains."},
    {CBM_LANG_C, C_EDGES,
     "Literal include paths, configured search roots, and a spelling only one project "
     "file can satisfy; translation-unit visibility through includes; unproved "
     "preprocessing abstains."},
    {CBM_LANG_CPP, C_EDGES,
     "Literal include paths, configured search roots, and a spelling only one project "
     "file can satisfy; translation-unit visibility through includes; unproved "
     "preprocessing abstains."},
    {CBM_LANG_CUDA, C_EDGES,
     "Literal include paths, configured search roots, and a spelling only one project "
     "file can satisfy; translation-unit visibility through includes; unproved "
     "preprocessing abstains."},
    {CBM_LANG_OBJC, EDGE(IMPORTS),
     "Literal include paths and configured search roots only; no semantic proof bridge "
     "advertised."},
    {CBM_LANG_JAVASCRIPT, SOURCE_EDGES | EDGE(WRITES),
     "Exact relative modules, directory entries, workspace manifests and their exports, ES and "
     "CommonJS export surfaces; runtime module paths and classic browser scripts abstain."},
    {CBM_LANG_TYPESCRIPT, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Exact relative modules, directory entries, tsconfig paths, workspace manifests and their "
     "exports, ES and CommonJS export surfaces; runtime module paths and built-only package "
     "entries abstain."},
    {CBM_LANG_TSX, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Exact relative modules, directory entries, tsconfig paths, workspace manifests and their "
     "exports, ES and CommonJS export surfaces; runtime module paths and built-only package "
     "entries abstain."},
    {CBM_LANG_ARKTS, SOURCE_EDGES,
     "Source JavaScript-style imports only; containing-language typed dispatch is unsupported."},
    {CBM_LANG_VUE, CALLABLE_EDGES,
     "One inline script with audited lexical bindings and an inert template; multiple script "
     "contexts, template effects and external script sources abstain."},
    {CBM_LANG_SVELTE, CALLABLE_EDGES,
     "One inline script with audited lexical bindings; only static markup and bare-identifier "
     "event references are supported. Multiple contexts and template effects abstain."},
    {CBM_LANG_PYTHON, SOURCE_EDGES | EDGE(WRITES) | EDGE(USES_TYPE),
     "Static project imports from the file's own search path, reexports and closed star imports; "
     "dynamic imports, unknown star sources and competing search-path candidates abstain."},
    {CBM_LANG_JAVA, OO_EDGES,
     "Exact declared package members; incomplete or competing namespace candidates abstain."},
    {CBM_LANG_KOTLIN, OO_EDGES,
     "Exact declared packages and aliases; incomplete or competing namespace candidates abstain."},
    {CBM_LANG_CSHARP, OO_EDGES,
     "Exact declared namespace members; unproved aliases and competing candidates abstain."},
    {CBM_LANG_PHP, TYPED_EDGES | EDGE(USAGE),
     "Declared namespaces, Composer PSR-4 and literal includes; dynamic paths abstain."},
    {CBM_LANG_ELIXIR, EDGE(IMPORTS) | EDGE(CALLS),
     "alias, import, require and use of a module declared once in the project; a module declared "
     "twice or outside the project abstains."},
    {CBM_LANG_ERLANG, EDGE(IMPORTS) | EDGE(CALLS),
     "-import and -behaviour of the module whose file bears its name, and -include next to the "
     "source; a name borne by two files, -include_lib and macro paths abstain."},
    {CBM_LANG_ZIG, EDGE(IMPORTS) | EDGE(CALLS) | EDGE(USAGE) | EDGE(USES_TYPE),
     "@import of a literal .zig path relative to the importing file; names mapped by the build "
     "abstain."},
    {CBM_LANG_PERL, EDGE(IMPORTS) | EDGE(CALLS),
     "use of a module found under the lib directory of an ancestor of the importing file; other "
     "@INC entries are not known and abstain."},
    {CBM_LANG_HASKELL, EDGE(CALLS) | EDGE(USAGE), NULL},
    {CBM_LANG_OCAML, EDGE(CALLS) | EDGE(USAGE), NULL},
    {CBM_LANG_RUBY, EDGE(IMPORTS) | EDGE(CALLS),
     "require_relative of a literal path, and require of a path under the lib directory of an "
     "ancestor of the importing file; other load paths and interpolation abstain."},
    {CBM_LANG_RUST, TYPED_EDGES,
     "Cargo crates, workspace [patch.crates-io] directories, module trees, use trees, re-exports "
     "and globs; items a macro's definition passes through count as items. Other macro-generated "
     "items and unmodeled receiver types abstain."},
    {CBM_LANG_SWIFT, EDGE(CALLS) | EDGE(USAGE) | EDGE(USES_TYPE),
     "Modules from SwiftPM targets; lexical scope, extensions, inheritance and argument labels; "
     "declared and constructed receiver types. Xcode module membership abstains."},
    {CBM_LANG_DART, EDGE(IMPORTS) | EDGE(CALLS) | EDGE(USAGE),
     "Lexical scope, parts, exports, show/hide and prefixes; package: URIs through the pubspec "
     "name; declared and constructed receiver types along class, mixin and superclass chains. "
     "Collection element types, conditional imports and part-of by library name abstain."},
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
    {CBM_LANG_SCALA, EDGE(IMPORTS) | EDGE(CALLS) | EDGE(USAGE) | EDGE(USES_TYPE),
     "Packages, imports by precedence, templates and their linearized parents; declared receiver "
     "types and `new`. Inferred return types, infix calls and Java classes abstain."},
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
    if (out->language == CBM_LANG_PHP)
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Class-like names through use aliases and the current namespace, at the exact name "
            "token; imported and absent names outside the project abstain.";
    if (out->language == CBM_LANG_K8S)
        out->edges[CBM_RESOLVE_EDGE_USAGE].limitation =
            "Kubernetes raw literal image repositories and core Secret, ConfigMap, ServiceAccount "
            "and PersistentVolumeClaim references in explicit flat Kustomize resource lists; "
            "unique declarations and exact namespaces are required. Aliases, duplicate keys, "
            "nested builds and transformed manifests abstain.";
    if (out->language == CBM_LANG_GO)
        out->edges[CBM_RESOLVE_EDGE_WRITES].limitation =
            "Package variables and fields selected through a declared static type. Writes through "
            "an unknown operand and keys of a literal whose element type is elided abstain.";
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
        out->limitation = "Lexical scope, declared static types including anonymous structs and "
                          "types local to a function, function values by result position, a "
                          "const list's implicit repetition, and Go's selector rule for "
                          "embedding. The standard library's errors.AsType, slices.Clone, "
                          "slices.All, slices.Backward and maps.All are read through. A "
                          "conversion T(x) uses the type and calls nothing. Exported names "
                          "behind an embedded type outside the project, a map's key type, a "
                          "type parameter's constraint methods, methods declared through an "
                          "alias and competing targets abstain.";
        break;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
        out->adapter = "c_family";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Exact visibility and owner proofs; overloads of one parameter count "
                          "with separate nodes, templates, ADL with a second visible function "
                          "and callable-field selection abstain.";
        break;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        out->adapter = "typescript";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation = "Source-audited lexical scopes, annotations, casts, constructions, "
                          "declared members, declared return types and type aliases that select "
                          "one declaration; receivers with two declarations, narrowed receivers, "
                          "inferred returns, callback parameters, direct eval and classic browser "
                          "scripts abstain.";
        break;
    case CBM_LANG_PYTHON:
        out->adapter = "python";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW | CBM_RESOLVE_CAP_CALLABLE;
        out->limitation =
            "Imports, reexports and closed star imports, through source roots a manifest states; "
            "self, cls and super through C3 order; receivers typed by construction, annotation, "
            "isinstance, with or except; annotated containers; a decorator keeps the declaration "
            "unless its source discards the function. Untyped receivers, computed bases, Enum "
            "and NamedTuple members and classes reached by computed writes abstain.";
        break;
    case CBM_LANG_PHP:
        out->adapter = "php_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW;
        out->limitation =
            "Namespace, use and global-fallback name resolution; class chains through declared "
            "parameter, property and return types and consistent local assignment. A chain "
            "leaving the project, magic members, PHPDoc types and reference effects abstain.";
        break;
    case CBM_LANG_ELIXIR:
        out->adapter = "elixir_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation =
            "Module functions by name and arity through defmodule, alias and import; quote, "
            "protocols, computed modules and a graph node shared by two modules abstain.";
        break;
    case CBM_LANG_ERLANG:
        out->adapter = "erlang_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation = "Functions by name and arity, -import and exported m:f and ?MODULE:f; a "
                          "variable module or function and macros abstain.";
        break;
    case CBM_LANG_ZIG:
        out->adapter = "zig_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW;
        out->limitation =
            "Container declarations, relative @import and members through declared types and "
            "return types; build-mapped imports, comptime types, captures and a graph node shared "
            "by two structs of a file abstain.";
        break;
    case CBM_LANG_PERL:
        out->adapter = "perl_source";
        out->proof_subsets =
            CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT | CBM_RESOLVE_CAP_MEMBER;
        out->limitation =
            "Package subs, qualified subs, Exporter imports and Class->m or $self->m through @ISA; "
            "builtins, computed names and a graph node shared by two packages abstain. A glob "
            "assigned through a computed name is unbounded and is not modelled.";
        break;
    case CBM_LANG_RUBY:
        out->adapter = "ruby_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_MEMBER;
        out->limitation =
            "Implicit self, self and Const receivers on the lexical class's ancestor chain; blocks "
            "that can rebind self, metaprogrammed names, unknown ancestors and reopened duplicates "
            "abstain. Constants inherited from an ancestor are not modelled.";
        break;
    case CBM_LANG_HASKELL:
        out->adapter = "haskell_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation =
            "Local bindings, top-level declarations, import lists and qualified imports against "
            "the export list; re-exports, type-class dispatch and two providers abstain.";
        break;
    case CBM_LANG_OCAML:
        out->adapter = "ocaml_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT;
        out->limitation =
            "Sequential let scope, local shadowing, module aliases and compilation units in the "
            "same directory; an open or include of an unknown module and functors abstain.";
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
    case CBM_LANG_RUST:
        out->adapter = "rust";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER |
                             CBM_RESOLVE_CAP_TYPE_FLOW;
        out->limitation =
            "Cargo crates and module trees, use trees, re-exports, globs, same-module items, "
            "inherent and in-scope trait methods with autoderef, tuple struct constructors, "
            "project "
            "traits on external and primitive types, associated consts, raw pointer dereference, "
            "and "
            "declared receiver types. A macro_rules! whose definition passes its items through or "
            "evaluates its expression arguments is read through, and so is pin_project_lite's "
            "pin_project!. An inherent impl proves a type only a macro of its module can have "
            "declared. Other items a macro generates, variants as call targets, fields sharing a "
            "method's name, a name bound differently under two cfg configurations, and external "
            "return types abstain.";
        break;
    case CBM_LANG_DART:
        out->adapter = "dart_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER;
        out->limitation = "Lexical scope, parts, exports and prefixes; declared and constructed "
                          "receiver types; collection element types and conditional imports "
                          "abstain.";
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
    case CBM_LANG_SCALA:
        out->adapter = "scala_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER;
        out->limitation = "Packages, imports by precedence, linearized parents; declared receiver "
                          "types; inferred return types and infix calls abstain.";
        break;
    case CBM_LANG_SWIFT:
        out->adapter = "swift_source";
        out->proof_subsets = CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_IMPORT_EXPORT |
                             CBM_RESOLVE_CAP_DECLARED_TYPE | CBM_RESOLVE_CAP_MEMBER;
        out->limitation = "Modules from SwiftPM targets; outside those, a file decides only for "
                          "itself. Implicit member expressions and collection element types "
                          "abstain.";
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
