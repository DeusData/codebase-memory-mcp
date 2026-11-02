#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/json_writer.h"
#include "helpers.h"
#include "lang_specs.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/import_resolve.h"
#include "pipeline/resolver_ecma.h"
#include "pipeline/shared_resolver.h"
#include "resolve/binding.h"
#include "resolve/lookup.h"
#include "resolve/policy.h"
#include "resolve/py_members.h"

#include "resolve/rust_names.h"
#include "resolve/snapshot.h"
#include "resolve/solver.h"
#include "resolve/source_adapter.h"
#include "lsp/go_stabilize.h"
#include "lsp/oo_strict.h"
#include "result_spill.h"
#include "service_patterns.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yyjson/yyjson.h>

/* Export identity and its declaring source must both agree with the graph.
 * Two files collapsed to one QN are ambiguous, even if graph upsert chose one.
 */
typedef struct export_support {
    const cbm_resolve_proof_t *audit;
    struct export_support *next;
} export_support_t;

typedef struct {
    const char *file, *key, *solver_key;
    bool supported;
    bool ambiguous;
    unsigned kind;
    const char *dependency;
    export_support_t *supports;
    size_t support_count;
} export_proof_t;

typedef struct {
    const char *slot;
    bool ambiguous;
} python_import_t;

typedef struct {
    const char *key, *import_key, *suffix;
    cbm_resolve_solver_result_t result;
} python_query_t;

/* Conditional equivalence is separate from target selection. A two-branch
 * Python owner is proved from the raw AST, never inferred from same-QN graph
 * collisions. Incoming references still require an unambiguous declaration. */
typedef struct {
    uint32_t start_line, end_line, start_byte, end_byte, body_start, body_end;
    uint32_t group_start, group_end;
    unsigned branch;
} owner_variant_t;

typedef struct {
    owner_variant_t members[2];
    const char *content, *configuration;
    bool invalid;
} conditional_owner_t;

typedef struct target_file {
    const char *file;
    struct target_file *next;
} target_file_t;

typedef struct {
    const char *file;
    const char *label;
    uint32_t start_line, end_line, second_start_line, second_end_line;
    unsigned declarations;
    conditional_owner_t *conditional;
    /* Other files declaring this QN (Go build-tag twins, C declaration headers). */
    target_file_t *other_files;
    bool ambiguous;
    /* Java/Kotlin/C# overloads of one owner share a qualified name, and the
     * graph keeps them as one node. That node faithfully names all of them. */
    bool overload_set;
    /* TypeScript's separate type space: one graph node carries a value and a
     * same-named type, so either declaration may be the materialized one. */
    const char *merged_label;
    uint32_t merged_start_line, merged_end_line;
    /* Only overload signatures so far; the implementation takes over. */
    bool signature_only;
    /* A same-QN declaration lives in another file, so no span can settle it. */
    bool cross_file;
    /* A different kind sharing the QN (C: struct and function) is no variant. */
    bool mixed_kinds, rebound;
    /* cfg twins and one type's repeated impl methods are one graph identity;
     * the Rust adapter separately rejects a QN two scopes of a file claim. */
    bool rust;
    /* A C file's #if branches define one name more than once. The graph holds one
     * node for them, with each branch's span as a variant, so an edge to or from
     * that node is true of the configurations it was found in. */
    bool c_variants;
    bool one_name;
} target_proof_t;

typedef struct {
    const char *module; /* exact project module, NULL when not one */
    uint32_t at;
} python_star_source_t;

typedef struct {
    const char *module_qn;
    cbm_resolve_proof_source_t source;
    bool complete, ambiguous, python_mutated;
    bool python_audited;       /* every statement was seen, whatever __getattr__ answers besides */
    bool python_star_chosen;   /* a condition chooses among its stars: the binder stopped at one */
    const char **python_names; /* module-level binders, sorted, parallel to python_name_at */
    uint32_t *python_name_at;
    size_t python_name_count;
    python_star_source_t *python_stars; /* source order */
    size_t python_star_count;
    /* Names before this byte are unknown: a star of an unknown module follows. */
    uint32_t python_open_barrier;
    /* A literal __all__ is the star-import interface; otherwise the public names. */
    const char **python_all;
    size_t python_all_count;
    bool python_all_literal, python_all_unknown;
    /* Memo for that interface: state 0 unvisited, 1 in progress, 2 done. */
    const char **python_visible;
    size_t python_visible_count;
    unsigned char python_visible_state;
    bool python_visible_open;
} namespace_proof_t;

typedef struct {
    const char *local, *path, *key;
    CBMImportKind kind;
} python_nested_entry_t;

typedef struct {
    python_nested_entry_t *items;
    size_t count, capacity;
} python_nested_list_t;

typedef struct python_pending_import {
    const char *key, *slot, *parent_file, *child_file, *member, *write_root, *write_slot;
    const cbm_resolve_proof_t *audit;
    const char *loaded_module;
    const cbm_resolve_proof_t *load_audit;
    const char *source_file;
    const char **writes; /* dotted paths stored through this import, NULL for any name */
    size_t write_count;
    bool module, audited, write_module, in_parent;
    struct python_pending_import *next;
} python_pending_import_t;

typedef struct {
    const cbm_resolve_source_import_t *source;
    const namespace_proof_t *namespace;
    const char *file;
    const cbm_resolve_proof_t *path_proof;
    cbm_resolve_lookup_context_t namespace_context;
    bool system;
} source_import_t;

typedef struct http_candidate_import {
    const char *key, *dependency;
    struct http_candidate_import *next;
} http_candidate_import_t;

typedef struct {
    const char *target, *module, *file, *base;
    bool ambiguous;
} http_default_t;

typedef struct {
    const char *file, *dependency, *scope, *callee, *url, *content, *configuration;
    uint32_t start, end;
    bool ambiguous;
} http_import_site_t;

struct cbm_shared_resolver {
    CBMArena arena;
    CBMHashTable *exports;
    CBMHashTable *targets;
    CBMHashTable *namespaces;
    CBMHashTable *python_queries;
    python_pending_import_t *python_imports;
    CBMHashTable *python_namespaces;
    bool python_unknown_namespace_escape;
    cbm_ecma_table_t *ecma;
    CBMHashTable *python_star_provided; /* "<module>.<name>" a closed star import binds */
    CBMHashTable *python_written;       /* "<module>.<path>" a statement stores or deletes */
    CBMHashTable *python_parent_loaded; /* a submodule its package's own __init__ loads */
    CBMHashTable *python_nested;        /* file -> python_nested_list_t */
    cbm_py_members_t *python_members;
    CBMHashTable *http_defaults, *http_sites, *http_unsafe;
    CBMHashTable *http_candidates, *http_veto;
    http_candidate_import_t *http_candidate_imports;
    bool http_domain_complete;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_solver_t *solver;
    cbm_go_stabilization_t *go_types;
    cbm_source_table_t *source_table;
    cbm_shared_resolver_export_summary_t *summary;
    size_t summary_count;
    const char *project;
    bool complete;
};

typedef struct {
    uint32_t start, end;
    CBMSourceOrigin origin;
    CBMResolvedKind kind;
    const char *caller;
    const CBMResolvedCall *row;
    bool ambiguous;
} semantic_site_t;

/* A declaration of one ECMAScript file that its graph node really shows. */
typedef struct {
    int start_line, end_line;
    int parent; /* The nearest shown declaration around this one, or -1. */
    const cbm_gbuf_node_t *node;
} ecma_owner_t;

/* Lines and shown declarations of one file, built at its first same-named twin. Lookups are
 * a binary search plus the nesting depth, so a file full of twins stays near linear. */
typedef struct {
    uint32_t *line_starts;
    size_t line_count;
    ecma_owner_t *owners;
    int owner_count;
    bool built;
} ecma_spans_t;

typedef struct {
    const cbm_shared_resolver_t *shared;
    const cbm_resolver_file_t *file;
    cbm_resolve_binding_index_t *bindings;
    CBMArena *arena;
    source_import_t *imports;
    size_t import_count;
    uint64_t import_work;
    CBMHashTable *import_names;
    CBMHashTable *python_imports;
    CBMHashTable *ecma_dependencies;
    semantic_site_t *sites;
    size_t site_count;
    const cbm_gbuf_node_t *file_node;
    bool source_matches;
    const char *source;
    size_t source_length;
    ecma_spans_t *ecma_spans;
    const cbm_rust_file_analysis_t *rust;
    /* One node per distinct site fact, so its size is the file's own: only the
     * memory core can refuse it, never a cap that would drop edges from a big file. */
    cbm_resolve_proof_pool_t rust_proofs;
    const cbm_source_unit_t *source_unit;
    /* Set when the language also has older binding rules: the adapter's sites are
     * attested here, so they need not claim that older index is complete. */
    cbm_resolve_binding_index_t *source_bindings;
} file_resolver_t;

static bool ecma_language(CBMLanguage language) {
    return language == CBM_LANG_JAVASCRIPT || language == CBM_LANG_TYPESCRIPT ||
           language == CBM_LANG_TSX || language == CBM_LANG_ARKTS;
}

static char *load_source(const char *path, size_t *length) {
    *length = 0;
    FILE *file = path ? cbm_fopen(path, "rb") : NULL;
    if (!file)
        return NULL;
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }
    long size = ftell(file);
    if (size < 0 || size > cbm_max_file_bytes() || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    char *source = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)size + 1);
    if (!source) {
        fclose(file);
        return NULL;
    }
    size_t got = fread(source, 1, (size_t)size, file);
    bool failed = ferror(file) || got != (size_t)size;
    fclose(file);
    if (failed) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
        return NULL;
    }
    source[got] = '\0';
    *length = got;
    return source;
}

char *cbm_shared_resolver_load_source(const char *path, size_t *length) {
    return load_source(path, length);
}

static char *file_source(const cbm_resolver_file_t *file, size_t *length) {
    const char *root = file->ctx ? file->ctx->repo_path : NULL;
    if (!root || !file->rel_path)
        return NULL;
    size_t root_len = strlen(root), rel_len = strlen(file->rel_path);
    if (root_len > SIZE_MAX - rel_len - 2)
        return NULL;
    char *path = cbm_alloc(CBM_MEM_CLASS_EXTRACT, root_len + rel_len + 2);
    if (!path)
        return NULL;
    memcpy(path, root, root_len);
    path[root_len] = '/';
    memcpy(path + root_len + 1, file->rel_path, rel_len + 1);
    char *source = load_source(path, length);
    cbm_free(CBM_MEM_CLASS_EXTRACT, path);
    return source;
}

/* Parallel extraction and spill release the cached syntax tree. Rebuild the
 * Python owner audit from the exact raw snapshot, and let the binding index
 * borrow the same tree so a released tree is parsed only once per phase.
 * The view borrows every other result field and is never freed as a result. */
static const CBMFileResult *owner_source_view(const CBMFileResult *result, CBMLanguage language,
                                              const char *source, size_t length,
                                              CBMFileResult *view, TSTree **owned) {
    *owned = NULL;
    if (language != CBM_LANG_PYTHON || result->cached_tree || !source || length > UINT32_MAX ||
        !cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW))
        return result;
    TSParser *parser = ts_parser_new();
    if (parser && ts_parser_set_language(parser, cbm_ts_language(language)))
        *owned = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    if (parser)
        ts_parser_delete(parser);
    if (!*owned)
        return result;
    *view = *result;
    view->cached_tree = *owned;
    view->cached_lang = language;
    return view;
}

static bool ecma_type_only_label(const char *label) {
    return !strcmp(label, "Type") || !strcmp(label, "Interface");
}

/* TypeScript facts about two same-QN declarations of one file. Returns true when
 * they are one declared entity, which then needs no ambiguity verdict. */
static bool ecma_merge_target(CBMArena *arena, target_proof_t *prior,
                              const CBMDefinition *definition, const char *file) {
    if (strcmp(prior->file, file))
        return false;
    bool same_label = !strcmp(prior->label, definition->label);
    if (same_label && !strcmp(definition->label, "Function")) {
        /* `function f(a: string): void;` above `function f(a: any) {}` */
        if (definition->is_abstract)
            return true;
        if (prior->signature_only) {
            prior->signature_only = false;
            prior->start_line = definition->start_line;
            prior->end_line = definition->end_line;
            return true;
        }
    }
    if (!same_label && !prior->merged_label &&
        ecma_type_only_label(prior->label) != ecma_type_only_label(definition->label)) {
        prior->merged_label = cbm_arena_strdup(arena, definition->label);
        prior->merged_start_line = definition->start_line;
        prior->merged_end_line = definition->end_line;
        return true;
    }
    return false;
}

static bool target_declared_in(const target_proof_t *target, const char *file) {
    if (!strcmp(target->file, file))
        return true;
    for (const target_file_t *other = target->other_files; other; other = other->next)
        if (!strcmp(other->file, file))
            return true;
    return false;
}

/* Overloads, trait impls and build-tag twins all stay live, so one node is exact for any of them.
 * Elsewhere a later declaration of a name rebinds it, as a second Python def does, and the bodies
 * are not equivalent: selecting that node would claim edges from a body nothing may reach. */
static bool declarations_coexist(CBMLanguage language) {
    switch (language) {
    case CBM_LANG_GO:
    case CBM_LANG_RUST:
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
    case CBM_LANG_CSHARP:
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_SWIFT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        return true;
    default:
        return false;
    }
}

/* A typing.overload stub states a signature for the def that follows; with it, one declaration.
 * The binding audit proves the decorator is the real one before anything resolves to the name. */
static bool python_overload_stub_definition(const CBMDefinition *definition) {
    for (const char *const *decorator = definition->decorators; decorator && *decorator;
         decorator++) {
        const char *dot = strrchr(*decorator, '.');
        const char *name = dot ? dot + 1 : *decorator;
        if (*name == '@')
            name++;
        if (!strcmp(name, "overload"))
            return true;
    }
    return false;
}

/* `@x.setter def x`: the same property as the def it follows, and its one graph node keeps
 * this span. */
static bool python_accessor_definition(const CBMDefinition *definition) {
    size_t length = definition->name ? strlen(definition->name) : 0;
    for (const char *const *decorator = definition->decorators; length && decorator && *decorator;
         decorator++) {
        const char *text = **decorator == '@' ? *decorator + 1 : *decorator;
        if (strncmp(text, definition->name, length) || text[length] != '.')
            continue;
        const char *member = text + length + 1;
        if (!strcmp(member, "setter") || !strcmp(member, "getter") || !strcmp(member, "deleter"))
            return true;
    }
    return false;
}

static bool add_target(cbm_shared_resolver_t *resolver, const CBMDefinition *definition,
                       const char *file, CBMLanguage language) {
    if (!definition->qualified_name || !definition->label || !file)
        return true;
    if (language == CBM_LANG_PYTHON && python_overload_stub_definition(definition))
        return true;
    if (language == CBM_LANG_PYTHON && python_accessor_definition(definition)) {
        target_proof_t *property = cbm_ht_get(resolver->targets, definition->qualified_name);
        if (property && !strcmp(property->file, file) &&
            !strcmp(property->label, definition->label)) {
            property->start_line = definition->start_line;
            property->end_line = definition->end_line;
            return true;
        }
    }
    target_proof_t *prior = cbm_ht_get(resolver->targets, definition->qualified_name);
    if (prior && prior->rust && language == CBM_LANG_RUST) {
        prior->ambiguous |= strcmp(prior->file, file) != 0;
        return true;
    }
    if (prior) {
        if ((language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX ||
             language == CBM_LANG_ARKTS) &&
            ecma_merge_target(&resolver->arena, prior, definition, file))
            return true;
        bool same_file = !strcmp(prior->file, file);
        bool same_owner = same_file && !strcmp(prior->label, definition->label);
        bool same_span =
            prior->start_line == definition->start_line && prior->end_line == definition->end_line;
        prior->cross_file |= !same_file;
        if (same_owner &&
            (language == CBM_LANG_C || language == CBM_LANG_CPP || language == CBM_LANG_CUDA)) {
            prior->c_variants = true;
            return true;
        }
        /* Clauses of one function are one declaration wherever the QN carries
         * no arity. The adapter separately refuses a QN shared by two modules.
         * A Swift struct and its extension in one file are one type's node too,
         * under whichever label the graph kept. */
        bool one_type = same_file && cbm_label_is_type_like(prior->label) &&
                        cbm_label_is_type_like(definition->label);
        bool one_name = (same_owner || one_type) && cbm_source_adapter_one_name_one_node(language);
        if (same_owner && !same_span && cbm_oo_language(language) &&
            (strcmp(definition->label, "Class") != 0 || language == CBM_LANG_CSHARP))
            prior->overload_set = true;
        else
            prior->ambiguous |=
                !one_name && (!same_owner || !same_span || language == CBM_LANG_PYTHON);
        prior->one_name |= one_name;
        prior->mixed_kinds |= strcmp(prior->label, definition->label) != 0;
        prior->rebound |= !declarations_coexist(language);
        if (!target_declared_in(prior, file)) {
            target_file_t *other = cbm_arena_calloc(&resolver->arena, sizeof(*other));
            const char *copy = cbm_arena_strdup(&resolver->arena, file);
            if (!other || !copy)
                return false;
            *other = (target_file_t){.file = copy, .next = prior->other_files};
            prior->other_files = other;
        }
        if (prior->declarations == 1 && same_owner) {
            prior->second_start_line = definition->start_line;
            prior->second_end_line = definition->end_line;
        }
        if (prior->declarations < 3)
            prior->declarations++;
        return true;
    }
    target_proof_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    const char *key = cbm_arena_strdup(&resolver->arena, definition->qualified_name);
    if (!entry || !key)
        return false;
    entry->file = cbm_arena_strdup(&resolver->arena, file);
    entry->label = cbm_arena_strdup(&resolver->arena, definition->label);
    entry->start_line = definition->start_line;
    entry->end_line = definition->end_line;
    entry->declarations = 1;
    entry->signature_only = definition->is_abstract;
    entry->rebound = !declarations_coexist(language);
    entry->rust = language == CBM_LANG_RUST;
    if (!entry->file || !entry->label)
        return false;
    cbm_ht_set(resolver->targets, key, entry);
    return cbm_ht_get(resolver->targets, key) == entry;
}

static TSNode owner_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static bool owner_kind(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}

static bool note_owner_branch(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                              const char *file, const char *source, size_t length,
                              const char *scope, TSNode group, TSNode block, unsigned branch) {
    TSTreeCursor cursor = ts_tree_cursor_new(block);
    bool ok = true;
    for (bool more = ts_tree_cursor_goto_first_child(&cursor); ok && more;
         more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode statement = ts_tree_cursor_current_node(&cursor);
        TSNode function = owner_kind(statement, "decorated_definition")
                              ? owner_field(statement, "definition")
                              : statement;
        TSNode name = owner_field(function, "name"), body = owner_field(function, "body");
        if (!owner_kind(function, "function_definition") || !owner_kind(name, "identifier") ||
            !owner_kind(body, "block"))
            continue;
        uint32_t start = ts_node_start_byte(name), end = ts_node_end_byte(name);
        if (end <= start || end > length || end - start > INT_MAX)
            continue;
        const char *qn = cbm_arena_sprintf(&resolver->arena, "%s.%.*s", scope, (int)(end - start),
                                           source + start);
        if (!qn) {
            ok = false;
            break;
        }
        target_proof_t *target = cbm_ht_get(resolver->targets, qn);
        if (!target || !target->ambiguous || target->declarations != 2 ||
            strcmp(target->file, file) ||
            (strcmp(target->label, "Function") && strcmp(target->label, "Method")) ||
            target->start_line == target->second_start_line)
            continue;
        uint32_t first = ts_node_start_point(function).row + 1;
        uint32_t outer = ts_node_start_point(statement).row + 1;
        uint32_t last = ts_node_end_point(function).row + 1;
        unsigned member;
        if ((first == target->start_line || outer == target->start_line) &&
            last == target->end_line)
            member = 0;
        else if ((first == target->second_start_line || outer == target->second_start_line) &&
                 last == target->second_end_line)
            member = 1;
        else
            continue;
        if (!target->conditional) {
            conditional_owner_t *owner = cbm_arena_calloc(&resolver->arena, sizeof(*owner));
            if (!owner) {
                ok = false;
                break;
            }
            owner->content = cbm_arena_strdup(&resolver->arena,
                                              result->source_content_id[CBM_SOURCE_ORIGIN_RAW]);
            owner->configuration =
                cbm_arena_strdup(&resolver->arena, result->source_configuration_id);
            if (!owner->content || !owner->configuration) {
                ok = false;
                break;
            }
            target->conditional = owner;
        }
        conditional_owner_t *owner = target->conditional;
        owner->invalid |= owner->members[member].branch != 0;
        owner->members[member] = (owner_variant_t){
            .start_line =
                first == target->start_line || first == target->second_start_line ? first : outer,
            .end_line = last,
            .start_byte = ts_node_start_byte(function),
            .end_byte = ts_node_end_byte(function),
            .body_start = ts_node_start_byte(body),
            .body_end = ts_node_end_byte(body),
            .group_start = ts_node_start_byte(group),
            .group_end = ts_node_end_byte(group),
            .branch = branch};
    }
    ts_tree_cursor_delete(&cursor);
    return ok;
}

/* An if/else whose two arms each declare a name: one declaration of it, whichever arm runs. */
static bool note_owner_group(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                             const char *file, const char *source, size_t length, const char *scope,
                             TSNode group) {
    TSNode first = owner_field(group, "consequence");
    TSNode alternative = owner_field(group, "alternative");
    TSNode second = owner_field(alternative, "body");
    if (!owner_kind(first, "block") || !owner_kind(alternative, "else_clause") ||
        !owner_kind(second, "block"))
        return true;
    return note_owner_branch(resolver, result, file, source, length, scope, group, first, 1) &&
           note_owner_branch(resolver, result, file, source, length, scope, group, second, 2);
}

/* Audit one raw syntax tree in linear time: each if/else directly in a scope's body, where the
 * scope is the module or a def or class reached through bodies. elif, nested groups, other
 * languages and cross-file build variants remain quality gaps. */
static bool note_conditional_owners_in(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                                       const char *file, const char *source, size_t length,
                                       const char *scope, TSNode body, unsigned depth) {
    /* A cursor, not an index loop: a fetch by index walks from the first child. */
    bool ok = depth < 32;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    for (bool more = ok && ts_tree_cursor_goto_first_child(&cursor); ok && more;
         more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode statement = ts_tree_cursor_current_node(&cursor);
        if (owner_kind(statement, "if_statement")) {
            ok = note_owner_group(resolver, result, file, source, length, scope, statement);
            continue;
        }
        TSNode definition = owner_kind(statement, "decorated_definition")
                                ? owner_field(statement, "definition")
                                : statement;
        if (!owner_kind(definition, "function_definition") &&
            !owner_kind(definition, "class_definition"))
            continue;
        TSNode name = owner_field(definition, "name"), inner = owner_field(definition, "body");
        if (!owner_kind(name, "identifier") || !owner_kind(inner, "block"))
            continue;
        uint32_t start = ts_node_start_byte(name), end = ts_node_end_byte(name);
        if (end <= start || end > length)
            continue;
        const char *child = cbm_arena_sprintf(&resolver->arena, "%s.%.*s", scope,
                                              (int)(end - start), source + start);
        ok = child && note_conditional_owners_in(resolver, result, file, source, length, child,
                                                 inner, depth + 1);
    }
    ts_tree_cursor_delete(&cursor);
    return ok;
}

static bool note_conditional_owners(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                                    const char *file, CBMLanguage language, const char *source,
                                    size_t length) {
    if (language != CBM_LANG_PYTHON || !result->cached_tree || !source ||
        result->cached_lang != language || !result->module_qn ||
        !cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW))
        return true;
    TSNode root = ts_tree_root_node(result->cached_tree);
    if (!owner_kind(root, "module") || ts_node_has_error(root))
        return true;
    return note_conditional_owners_in(resolver, result, file, source, length, result->module_qn,
                                      root, 0);
}

static bool target_materialized(const file_resolver_t *resolver, const cbm_gbuf_node_t *node) {
    /* Standalone single-file callers verify their own syntax. A frozen
     * project resolver additionally excludes graph upserts that collapsed
     * distinct declarations to the same qualified name. */
    if (!resolver->shared)
        return true;
    if (!resolver->shared->complete || !node || !node->qualified_name || !node->file_path ||
        !node->label)
        return false;
    const target_proof_t *proof = cbm_ht_get(resolver->shared->targets, node->qualified_name);
    if (!proof)
        return false;
    if (proof->rust)
        return !proof->ambiguous && !strcmp(proof->file, node->file_path);
    /* Several declarations of one kind sharing a QN are one identity in this graph: one
     * node whose `variants` keep every span and whose edges are the union of every body
     * (graph_buffer.c, "Definition variants"). Rust `impl From<A> for E` beside
     * `impl From<B> for E`, overloads and build-tag twins all land here, and any member
     * reaches the same node, so selecting that node is exact whichever member a site
     * means. The node must still be one the census recorded.
     * TypeScript merges its overloads and value + type pairs into one proof, so an
     * ambiguous proof there is two distinct functions that extraction named flat. A
     * source adapter states its own rule for what a shared name means. */
    if (proof->ambiguous)
        return !cbm_source_adapter_supported(resolver->file->language) &&
               !ecma_language(resolver->file->language) && !proof->mixed_kinds && !proof->rebound &&
               !strcmp(proof->label, node->label) && target_declared_in(proof, node->file_path);
    if (strcmp(proof->file, node->file_path) || node->start_line < 0 || node->end_line < 0)
        return false;
    /* The upsert keeps one clause's span and label; all agreed on the file. */
    if (proof->one_name)
        return true;
    if (proof->merged_label && !strcmp(proof->merged_label, node->label) &&
        proof->merged_start_line == (uint32_t)node->start_line &&
        proof->merged_end_line == (uint32_t)node->end_line)
        return true;
    if (strcmp(proof->label, node->label))
        return false;
    return proof->overload_set || proof->c_variants ||
           (proof->start_line == (uint32_t)node->start_line &&
            proof->end_line == (uint32_t)node->end_line);
}

static int ecma_owner_compare(const void *left, const void *right) {
    const ecma_owner_t *a = left, *b = right;
    if (a->start_line != b->start_line)
        return a->start_line < b->start_line ? -1 : 1;
    return a->end_line == b->end_line ? 0 : (a->end_line > b->end_line ? -1 : 1);
}

/* One pass over the bytes and one over the definitions, with a stack of the open spans. */
static bool ecma_spans_build(const file_resolver_t *resolver) {
    ecma_spans_t *spans = resolver->ecma_spans;
    if (spans->built)
        return true;
    size_t lines = 1;
    for (size_t at = 0; at < resolver->source_length; at++)
        lines += resolver->source[at] == '\n';
    const CBMFileResult *result = resolver->file->result;
    size_t capacity = result->defs.count > 0 ? (size_t)result->defs.count : 0;
    spans->line_starts = cbm_arena_alloc(resolver->arena, lines * sizeof(*spans->line_starts));
    spans->owners =
        capacity ? cbm_arena_alloc(resolver->arena, capacity * sizeof(*spans->owners)) : NULL;
    int *open = capacity ? cbm_arena_alloc(resolver->arena, capacity * sizeof(*open)) : NULL;
    if (!spans->line_starts || (capacity && (!spans->owners || !open)))
        return false;
    spans->line_starts[spans->line_count++] = 0;
    for (size_t at = 0; at < resolver->source_length; at++)
        if (resolver->source[at] == '\n')
            spans->line_starts[spans->line_count++] = (uint32_t)at + 1;
    for (int d = 0; d < result->defs.count; d++) {
        const CBMDefinition *def = &result->defs.items[d];
        if (!def->label || (strcmp(def->label, "Function") && strcmp(def->label, "Method")))
            continue;
        const cbm_gbuf_node_t *node =
            cbm_gbuf_find_by_qn(resolver->file->graph, def->qualified_name);
        if (!node || !node->file_path || strcmp(node->file_path, resolver->file->rel_path) ||
            node->start_line != (int)def->start_line || node->end_line != (int)def->end_line)
            continue;
        spans->owners[spans->owner_count++] = (ecma_owner_t){
            .start_line = node->start_line, .end_line = node->end_line, .node = node};
    }
    if (spans->owner_count > 1)
        qsort(spans->owners, (size_t)spans->owner_count, sizeof(*spans->owners),
              ecma_owner_compare);
    int depth = 0;
    for (int i = 0; i < spans->owner_count; i++) {
        ecma_owner_t *owner = &spans->owners[i];
        while (depth && spans->owners[open[depth - 1]].end_line < owner->end_line)
            depth--;
        owner->parent = depth ? open[depth - 1] : -1;
        open[depth++] = i;
    }
    spans->built = true;
    return true;
}

static int ecma_line_at(const ecma_spans_t *spans, uint32_t start) {
    size_t low = 0, high = spans->line_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (spans->line_starts[mid] <= start)
            low = mid + 1;
        else
            high = mid;
    }
    return (int)low;
}

/* JavaScript nests functions freely and extraction names them flat, so two
 * `const handle` of one file share a QN and one graph node, showing the last. That
 * node is still exactly right for an occurrence inside its own span. */
static bool ecma_source_span_settles(const file_resolver_t *resolver, const cbm_gbuf_node_t *node,
                                     uint32_t start) {
    const target_proof_t *proof =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, node->qualified_name) : NULL;
    if (!proof || !proof->ambiguous || proof->cross_file || !resolver->source ||
        start > resolver->source_length || node->start_line <= 0 || !resolver->ecma_spans ||
        !ecma_spans_build(resolver))
        return false;
    int line = ecma_line_at(resolver->ecma_spans, start);
    return line >= node->start_line && line <= node->end_line;
}

/* An occurrence in a twin the node does not show. That twin has no node of its own, so it
 * is as nameless to the graph as an anonymous callable, and its body belongs to the nearest
 * declaration around it that the graph does show, else the file. A line only proves it is
 * inside a span it falls strictly within; an outer owner is coarser and still true. */
static const cbm_gbuf_node_t *ecma_shown_owner(const file_resolver_t *resolver,
                                               const cbm_gbuf_node_t *twin, uint32_t start) {
    const target_proof_t *proof =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, twin->qualified_name) : NULL;
    if (!proof || !proof->ambiguous || proof->cross_file || !resolver->source ||
        start > resolver->source_length || !resolver->ecma_spans || !resolver->file_node ||
        !ecma_spans_build(resolver))
        return NULL;
    const ecma_spans_t *spans = resolver->ecma_spans;
    int line = ecma_line_at(spans, start);
    int low = 0, high = spans->owner_count;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (spans->owners[mid].start_line <= line)
            low = mid + 1;
        else
            high = mid;
    }
    for (int at = low - 1; at >= 0; at = spans->owners[at].parent) {
        const ecma_owner_t *owner = &spans->owners[at];
        if (owner->node != twin && owner->start_line < line && line < owner->end_line)
            return owner->node;
    }
    /* The node a module-level occurrence of this file uses. */
    const char *module_qn = resolver->file->result->module_qn;
    const cbm_gbuf_node_t *module =
        module_qn ? cbm_gbuf_find_by_qn(resolver->file->graph, module_qn) : NULL;
    /* `message-part.tsx` and `message-part.css` share a module QN; its node may show the other. */
    return module && module->label && !strcmp(module->label, "Module") && module->file_path &&
                   !strcmp(module->file_path, resolver->file->rel_path)
               ? module
               : resolver->file_node;
}

/* The lexical declaration this occurrence names is the one the node shows. */
static bool ecma_target_span_settles(const file_resolver_t *resolver, const cbm_gbuf_node_t *node,
                                     const char *name, uint32_t start, uint32_t end,
                                     CBMSourceOrigin origin) {
    const target_proof_t *proof =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, node->qualified_name) : NULL;
    uint32_t first = 0, last = 0;
    return proof && proof->ambiguous && !proof->cross_file && node->start_line > 0 &&
           ecma_language(resolver->file->language) &&
           cbm_resolve_binding_js_declaration_lines(resolver->bindings, name, start, end, origin,
                                                    &first, &last) &&
           first == (uint32_t)node->start_line && last == (uint32_t)node->end_line;
}

/* A merged value + type answers a type reference even when the value is the
 * declaration the single graph node happens to show. */
static bool target_declares_type(const file_resolver_t *resolver, const cbm_gbuf_node_t *node) {
    if (cbm_label_is_type_like(node->label))
        return true;
    const target_proof_t *proof =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, node->qualified_name) : NULL;
    return proof && proof->merged_label &&
           (cbm_label_is_type_like(proof->merged_label) || cbm_label_is_type_like(proof->label));
}

/* The occurrence's enclosing declaration must be the source node, and that declaration
 * must live in this file. For a variant set the winning span may be another member's, even
 * in another file (Go build tags), so ownership is the census' list of declaring files. */
static bool source_identity_holds(const file_resolver_t *resolver, const cbm_gbuf_node_t *source,
                                  bool conditional) {
    const char *file = resolver->file->rel_path;
    if (!source->file_path)
        return false;
    const target_proof_t *proof =
        resolver->shared && resolver->shared->complete && source->qualified_name
            ? cbm_ht_get(resolver->shared->targets, source->qualified_name)
            : NULL;
    if (proof && proof->ambiguous && !proof->mixed_kinds && !proof->rebound &&
        source != resolver->file_node)
        return target_materialized(resolver, source) && target_declared_in(proof, file);
    if (strcmp(source->file_path, file))
        return false;
    return source == resolver->file_node || (source->label && !strcmp(source->label, "Module")) ||
           target_materialized(resolver, source) || conditional;
}

typedef struct {
    const target_proof_t *target;
    unsigned seen;
    bool invalid;
} owner_span_audit_t;

static void owner_span(const char *file, int start, int end, void *user) {
    owner_span_audit_t *audit = user;
    const target_proof_t *target = audit->target;
    if (!file || strcmp(file, target->file) || start <= 0 || end < start) {
        audit->invalid = true;
        return;
    }
    if ((uint32_t)start == target->start_line && (uint32_t)end == target->end_line)
        audit->seen |= 1;
    else if ((uint32_t)start == target->second_start_line &&
             (uint32_t)end == target->second_end_line)
        audit->seen |= 2;
    else
        audit->invalid = true;
}

/* foreach_span can read unsettled worker spans, but deliberately tolerates
 * malformed JSON entries. Projection authority cannot inherit that tolerance. */
static bool owner_properties_valid(const cbm_gbuf_node_t *node, const target_proof_t *target) {
    const char *text = node->properties_json;
    if (!text)
        return false;
    yyjson_doc *doc = yyjson_read(text, strlen(text), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *variants = yyjson_obj_get(root, "variants");
    bool valid = yyjson_is_obj(root) && (!variants || yyjson_is_arr(variants));
    owner_span_audit_t audit = {.target = target};
    if (valid && variants) {
        size_t index, count;
        yyjson_val *entry;
        yyjson_arr_foreach(variants, index, count, entry) {
            yyjson_val *first = yyjson_obj_get(entry, "start_line");
            yyjson_val *last = yyjson_obj_get(entry, "end_line");
            int64_t start = yyjson_get_sint(first), end = yyjson_get_sint(last);
            if (!yyjson_is_int(first) || !yyjson_is_int(last) || start <= 0 || end < start ||
                start > INT_MAX || end > INT_MAX) {
                audit.invalid = true;
                break;
            }
            owner_span(yyjson_get_str(yyjson_obj_get(entry, "file_path")), (int)start, (int)end,
                       &audit);
        }
        valid = !audit.invalid && audit.seen == 3;
    }
    yyjson_doc_free(doc);
    return valid;
}

static const owner_variant_t *conditional_source_owner(const file_resolver_t *resolver,
                                                       const cbm_gbuf_node_t *node, uint32_t start,
                                                       uint32_t end, CBMSourceOrigin origin) {
    const CBMFileResult *result = resolver->file->result;
    if (!resolver->shared || !resolver->shared->complete || !resolver->source_matches || !node ||
        !node->qualified_name || !node->file_path || !node->label ||
        origin != CBM_SOURCE_ORIGIN_RAW || end <= start || !result->cached_tree ||
        resolver->file->language != CBM_LANG_PYTHON || result->cached_lang != CBM_LANG_PYTHON)
        return NULL;
    const target_proof_t *target = cbm_ht_get(resolver->shared->targets, node->qualified_name);
    const conditional_owner_t *owner = target ? target->conditional : NULL;
    if (!target || !owner || !target->ambiguous || target->declarations != 2 || owner->invalid ||
        strcmp(target->file, resolver->file->rel_path) || strcmp(target->file, node->file_path) ||
        strcmp(target->label, node->label) ||
        strcmp(owner->content, result->source_content_id[CBM_SOURCE_ORIGIN_RAW]) ||
        strcmp(owner->configuration, result->source_configuration_id))
        return NULL;
    const owner_variant_t *a = &owner->members[0], *b = &owner->members[1];
    if (!a->branch || !b->branch || a->branch == b->branch || a->group_start != b->group_start ||
        a->group_end != b->group_end || !owner_properties_valid(node, target))
        return NULL;
    owner_span_audit_t spans = {.target = target};
    owner_span(node->file_path, node->start_line, node->end_line, &spans);
    if (spans.invalid)
        return NULL;
    spans.seen = 0;
    cbm_gbuf_foreach_span(resolver->file->graph, node, owner_span, &spans);
    if (spans.invalid || spans.seen != 3)
        return NULL;
    const owner_variant_t *selected = start >= a->body_start && end <= a->body_end   ? a
                                      : start >= b->body_start && end <= b->body_end ? b
                                                                                     : NULL;
    if (!selected)
        return NULL;
    /* Exact containment alone could lend an outer declaration to a nested
     * function/lambda. Confirm the nearest callable syntax owner as well. */
    TSNode syntax =
        ts_node_named_descendant_for_byte_range(ts_tree_root_node(result->cached_tree), start, end);
    for (unsigned depth = 0; !ts_node_is_null(syntax) && depth < 64; depth++) {
        if (owner_kind(syntax, "lambda") || owner_kind(syntax, "class_definition"))
            return NULL;
        if (owner_kind(syntax, "function_definition"))
            return ts_node_start_byte(syntax) == selected->start_byte &&
                           ts_node_end_byte(syntax) == selected->end_byte
                       ? selected
                       : NULL;
        syntax = ts_node_parent(syntax);
    }
    return NULL;
}

static const cbm_resolve_proof_t *conditional_owner_proof(const file_resolver_t *resolver,
                                                          const cbm_gbuf_node_t *node,
                                                          const owner_variant_t *owner,
                                                          const cbm_resolve_proof_t *existing) {
    const cbm_resolve_proof_view_t *semantic = cbm_resolve_proof_view(existing);
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(resolver->bindings);
    if (!semantic || !context)
        return NULL;
    cbm_resolve_proof_source_t source = cbm_result_proof_source(
        resolver->file->result, CBM_SOURCE_ORIGIN_RAW, node->qualified_name);
    cbm_resolve_proof_key_t key = {.lookup_domain = resolver->file->module_qn,
                                   .lookup_key = node->name,
                                   .binding_identity = node->qualified_name,
                                   .declaration_identity = node->qualified_name,
                                   .start_byte = owner->start_byte,
                                   .end_byte = owner->end_byte,
                                   .origin = CBM_SOURCE_ORIGIN_RAW,
                                   .flags = owner->branch,
                                   .detail = "raw_python_if_else_canonical_owner"};
    cbm_resolve_result_t identity = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                     .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                     .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                     .target = node->qualified_name,
                                     .candidate_count = 1,
                                     .facts = CBM_RESOLVE_FACT_EXACT_TARGET,
                                     .reason = "raw_python_if_else_canonical_owner"};
    const cbm_resolve_proof_t *premises[] = {
        existing,
        cbm_resolve_proof_leaf(context->pool, CBM_PROOF_SOURCE_FACT, &source, &key, &identity)};
    return cbm_resolve_proof_derive(context->pool, CBM_PROOF_NAMED_JOIN, &semantic->source,
                                    &semantic->key, &semantic->outcome, premises, 2, true);
}

static const char *export_solver_key(CBMArena *arena, const char *qn) {
    return qn ? cbm_arena_sprintf(arena, "export:%zu:%s", strlen(qn), qn) : NULL;
}

static const char *python_import_key(CBMArena *arena, const char *file, int index) {
    return cbm_arena_sprintf(arena, "python_import:%zu:%s:%d", strlen(file), file, index);
}

static const cbm_resolve_proof_t *export_audit(cbm_shared_resolver_t *resolver,
                                               const CBMFileResult *result, const char *file,
                                               const char *key, const char *target, unsigned kind,
                                               bool supported, const char *detail,
                                               const CBMImport *import) {
    cbm_resolve_proof_source_t source = {.project = resolver->project,
                                         .file = file,
                                         .module = result->module_qn,
                                         .scope = result->module_qn,
                                         .content_identity =
                                             result->source_content_id[CBM_SOURCE_ORIGIN_RAW],
                                         .configuration_identity = result->source_configuration_id,
                                         .source_origin = CBM_SOURCE_ORIGIN_RAW};
    cbm_resolve_proof_key_t lookup = {.lookup_domain =
                                          import ? import->module_path : result->module_qn,
                                      .lookup_key = import ? import->local_name : key,
                                      .binding_identity = key,
                                      .declaration_identity = target,
                                      .flags = kind,
                                      .detail = detail};
    cbm_resolve_result_t outcome = {.binding = supported && target ? CBM_RESOLVE_BINDING_UNIQUE
                                                                   : CBM_RESOLVE_BINDING_UNRESOLVED,
                                    .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                    .analysis = supported ? CBM_RESOLVE_ANALYSIS_COMPLETE
                                                          : CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                    .target = supported ? target : NULL,
                                    .candidate_count = supported && target ? 1 : 0,
                                    .reason = detail};
    return cbm_resolve_proof_leaf(&resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN, &source, &lookup,
                                  &outcome);
}

static bool add_export(cbm_shared_resolver_t *resolver, const char *qn, const char *file,
                       unsigned kind, bool supported, const char *dependency,
                       const cbm_resolve_proof_t *audit) {
    if (!audit)
        return false;
    export_proof_t *entry = cbm_ht_get(resolver->exports, qn);
    if (!entry) {
        entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
        const char *key = cbm_arena_strdup(&resolver->arena, qn);
        const char *path = cbm_arena_strdup(&resolver->arena, file);
        const char *solver_key = export_solver_key(&resolver->arena, qn);
        if (!entry || !key || !path || !solver_key)
            return false;
        *entry = (export_proof_t){.file = path,
                                  .key = key,
                                  .solver_key = solver_key,
                                  .supported = supported,
                                  .kind = kind,
                                  .dependency = dependency};
        cbm_ht_set(resolver->exports, key, entry);
        if (cbm_ht_get(resolver->exports, key) != entry)
            return false;
    } else {
        /* Multiple registrations never choose an arbitrary defining source.
         * Keep every audited support; the conflicting binding stays unknown. */
        entry->ambiguous = true;
        entry->supported &= supported;
        if (strcmp(file, entry->file) < 0)
            entry->file = cbm_arena_strdup(&resolver->arena, file);
        if (!entry->file)
            return false;
    }
    export_support_t *support = cbm_arena_calloc(&resolver->arena, sizeof(*support));
    if (!support)
        return false;
    *support = (export_support_t){.audit = audit, .next = entry->supports};
    entry->supports = support;
    entry->support_count++;
    return true;
}

static bool export_language(CBMLanguage language) {
    return language == CBM_LANG_PYTHON || language == CBM_LANG_JAVASCRIPT ||
           language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX ||
           language == CBM_LANG_ARKTS || language == CBM_LANG_DART || language == CBM_LANG_RUST;
}

/* A star import takes __all__ when it is a static literal. Anything else that
 * binds __all__ leaves the interface unknown, never the public-name fallback. */
static bool capture_python_all(cbm_shared_resolver_t *resolver, namespace_proof_t *entry,
                               const CBMFileResult *result) {
    const CBMPyNamespaceFacts *facts = &result->py_namespace;
    if (facts->status != CBM_PY_NS_COMPLETE) {
        size_t low = 0, high = entry->python_name_count;
        while (low < high) {
            size_t middle = low + (high - low) / 2;
            if (strcmp(entry->python_names[middle], "__all__") < 0)
                low = middle + 1;
            else
                high = middle;
        }
        entry->python_all_unknown =
            low < entry->python_name_count && !strcmp(entry->python_names[low], "__all__");
        return true;
    }
    size_t total = 0;
    for (int i = 0; i < facts->count; i++)
        total += facts->items[i].name_count > 0 ? (size_t)facts->items[i].name_count : 0;
    const char **names = total ? cbm_arena_alloc(&resolver->arena, total * sizeof(*names)) : NULL;
    if (total && !names)
        return false;
    for (int i = 0; i < facts->count; i++) {
        const CBMPyNamespaceFact *fact = &facts->items[i];
        if (fact->kind == CBM_PY_NS_ALL_UNKNOWN) {
            entry->python_all_unknown = true;
            continue;
        }
        if (fact->kind == CBM_PY_NS_ALL_DELETE || fact->kind == CBM_PY_NS_ALL_SET) {
            entry->python_all_count = 0;
            entry->python_all_literal = fact->kind == CBM_PY_NS_ALL_SET;
            entry->python_all_unknown = false;
        }
        if (fact->kind != CBM_PY_NS_ALL_SET && fact->kind != CBM_PY_NS_ALL_APPEND)
            continue;
        entry->python_all_literal = true;
        for (int n = 0; n < fact->name_count; n++) {
            names[entry->python_all_count] = cbm_arena_strdup(&resolver->arena, fact->names[n]);
            if (!names[entry->python_all_count])
                return false;
            entry->python_all_count++;
        }
    }
    entry->python_all = names;
    return true;
}

static bool capture_python_stars(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                 namespace_proof_t *entry, const char *file,
                                 const cbm_resolve_binding_index_t *index) {
    size_t count = cbm_resolve_binding_python_star_count(index);
    if (!count)
        return true;
    entry->python_stars = cbm_arena_calloc(&resolver->arena, count * sizeof(*entry->python_stars));
    if (!entry->python_stars)
        return false;
    size_t kept = 0;
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_python_star_t *star = cbm_resolve_binding_python_star_at(index, i);
        /* Under TYPE_CHECKING it binds nothing at run time, and only some modules have another
         * way to have those names. */
        if (star->annotation_only && !cbm_resolve_binding_python_names_come_at_run_time(index))
            continue;
        CBMImport path = {.local_name = star->module_path,
                          .module_path = star->module_path,
                          .kind = CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS};
        bool module = false;
        char *slot =
            cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, &path, &module);
        const char *copy = slot && module ? cbm_arena_strdup(&resolver->arena, slot) : NULL;
        bool lost = slot && module && !copy;
        cbm_free(CBM_MEM_CLASS_SEMANTIC, slot);
        if (lost)
            return false;
        entry->python_stars[kept++] = (python_star_source_t){.module = copy, .at = star->at};
    }
    entry->python_star_count = kept;
    return true;
}

static bool capture_python_namespace(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                     const CBMFileResult *result, const char *file,
                                     const cbm_resolve_binding_index_t *index) {
    namespace_proof_t *prior = cbm_ht_get(resolver->namespaces, file);
    if (prior) {
        prior->ambiguous = true;
        return true;
    }
    namespace_proof_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    const char *path = cbm_arena_strdup(&resolver->arena, file);
    size_t length = result->module_qn ? cbm_fqn_symbol_scope_len(result->module_qn) : 0;
    const char *module =
        length ? cbm_arena_strndup(&resolver->arena, result->module_qn, length) : NULL;
    if (!entry || !path || !module)
        return false;
    cbm_resolve_lookup_context_t context;
    cbm_resolve_proof_source_t source =
        cbm_result_proof_source(result, CBM_SOURCE_ORIGIN_RAW, result->module_qn);
    source.project = resolver->project;
    source.file = path;
    if (!cbm_resolve_lookup_context_init(&context, &resolver->proofs, source))
        return false;
    entry->source = context.source;
    entry->module_qn = module;
    entry->complete = cbm_resolve_binding_python_namespace_closed(index);
    entry->python_audited =
        cbm_resolve_binding_status(index) &&
        !strcmp(cbm_resolve_binding_status(index), "binding_complete_supported_subset");
    entry->python_star_chosen =
        cbm_resolve_binding_status(index) &&
        !strcmp(cbm_resolve_binding_status(index), "conditional_wildcard_import");
    size_t count = cbm_resolve_binding_python_namespace_size(index);
    if (count > SIZE_MAX / sizeof(*entry->python_names))
        return false;
    entry->python_names =
        count ? cbm_arena_alloc(&resolver->arena, count * sizeof(*entry->python_names)) : NULL;
    entry->python_name_at =
        count ? cbm_arena_alloc(&resolver->arena, count * sizeof(*entry->python_name_at)) : NULL;
    if (count && (!entry->python_names || !entry->python_name_at))
        return false;
    for (size_t i = 0; i < count; i++) {
        uint32_t at = 0;
        const char *name = cbm_resolve_binding_python_namespace_name(index, i, &at);
        if (!name)
            continue;
        const char *copy = cbm_arena_strdup(&resolver->arena, name);
        if (!copy)
            return false;
        entry->python_name_at[entry->python_name_count] = at;
        entry->python_names[entry->python_name_count++] = copy;
    }
    if (!capture_python_all(resolver, entry, result) ||
        !capture_python_stars(resolver, ctx, entry, file, index))
        return false;
    cbm_ht_set(resolver->namespaces, path, entry);
    namespace_proof_t *same_module = cbm_ht_get(resolver->python_namespaces, module);
    if (same_module) {
        same_module->ambiguous = true;
        entry->ambiguous = true;
    } else {
        cbm_ht_set(resolver->python_namespaces, module, entry);
    }
    return cbm_ht_get(resolver->namespaces, path) == entry &&
           (same_module || cbm_ht_get(resolver->python_namespaces, module) == entry);
}

/* Import execution loads the named module even when its local value binding
 * is outside the positive subset. A from-import loads its parent module first.
 * Resolve that exact configured file path; never infer loading from an indexed
 * Module definition or a matching leaf name. */
static bool python_loaded_module(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                 const char *file, const CBMImport *import, const char **out) {
    *out = NULL;
    if (!import->module_path || !import->module_path[0])
        return true;
    CBMImport path = *import;
    if (import->kind == CBM_IMPORT_KIND_PYTHON_FROM) {
        const char *last = strrchr(import->module_path, '.');
        if (!last || !last[1])
            return true;
        size_t length = (size_t)(last - import->module_path);
        if (length < strspn(import->module_path, "."))
            length++;
        path.module_path = cbm_arena_strndup(&resolver->arena, import->module_path, length);
        if (!path.module_path)
            return false;
    } else if (import->kind != CBM_IMPORT_KIND_PYTHON_MODULE_ROOT &&
               import->kind != CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS) {
        return true;
    }
    /* This query asks only for the loaded file's namespace. Its local spelling
     * is unused, and may be '*' on the original from-import. */
    path.kind = CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS;
    path.local_name = path.module_path;
    bool module = false;
    char *loaded =
        cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, &path, &module);
    if (!loaded || !module) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, loaded);
        return true;
    }
    *out = cbm_arena_strdup(&resolver->arena, loaded);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, loaded);
    return *out != NULL;
}

/* What this file writes through the module the import bound. */
static bool python_note_import_writes(cbm_shared_resolver_t *resolver,
                                      const cbm_pipeline_ctx_t *ctx,
                                      const cbm_resolve_binding_index_t *index, const char *file,
                                      const CBMImport *import, python_pending_import_t *entry) {
    const char *member = NULL;
    while (cbm_resolve_binding_python_import_write(index, import->local_name, entry->write_count,
                                                   &member))
        entry->write_count++;
    if (!entry->write_count)
        return true;
    entry->writes = cbm_arena_alloc(&resolver->arena, entry->write_count * sizeof(*entry->writes));
    if (!entry->writes)
        return false;
    for (size_t i = 0; i < entry->write_count; i++) {
        cbm_resolve_binding_python_import_write(index, import->local_name, i, &member);
        entry->writes[i] = member ? cbm_arena_strdup(&resolver->arena, member) : NULL;
        if (member && !entry->writes[i])
            return false;
    }
    char *effect = cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, import,
                                                   &entry->write_module);
    if (effect) {
        entry->write_slot = cbm_arena_strdup(&resolver->arena, effect);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, effect);
        if (!entry->write_slot)
            return false;
    }
    if (import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && import->local_name) {
        CBMImport root = *import;
        root.module_path = import->local_name;
        root.kind = CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS;
        bool is_module = false;
        char *bound =
            cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, &root, &is_module);
        if (bound) {
            entry->write_root = cbm_arena_strdup(&resolver->arena, bound);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, bound);
            if (!entry->write_root)
                return false;
        }
    }
    return true;
}

static bool defer_python_import(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                const cbm_resolve_binding_index_t *index, const char *file,
                                const CBMImport *import, const char *key, const char *slot,
                                bool module, bool audited, bool runs,
                                const cbm_resolve_proof_t *audit, const char *loaded_module,
                                const cbm_resolve_proof_t *load_audit) {
    python_pending_import_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    if (!entry)
        return false;
    entry->key = cbm_arena_strdup(&resolver->arena, key);
    entry->slot = slot ? cbm_arena_strdup(&resolver->arena, slot) : NULL;
    entry->module = module;
    entry->audited = audited;
    entry->audit = audit;
    entry->loaded_module = loaded_module;
    entry->load_audit = load_audit;
    entry->source_file = cbm_arena_strdup(&resolver->arena, file);
    if (!entry->key || (slot && !entry->slot) || !entry->source_file)
        return false;
    if (runs && !python_note_import_writes(resolver, ctx, index, file, import, entry))
        return false;
    const cbm_gbuf_node_t *parent, *child;
    if (audited && !module &&
        cbm_pipeline_python_submodule_candidate(ctx->gbuf, ctx->project_name, file, import, &parent,
                                                &child)) {
        entry->parent_file = cbm_arena_strdup(&resolver->arena, parent->file_path);
        entry->child_file = cbm_arena_strdup(&resolver->arena, child->file_path);
        entry->member = cbm_arena_strdup(&resolver->arena, strrchr(import->module_path, '.') + 1);
        entry->in_parent = !strcmp(parent->file_path, file) && import->local_name &&
                           !strcmp(import->local_name, entry->member);
        if (!entry->parent_file || !entry->child_file || !entry->member)
            return false;
    }
    entry->next = resolver->python_imports;
    resolver->python_imports = entry;
    return true;
}

static bool http_default_capture(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                 const cbm_resolve_binding_index_t *index,
                                 const CBMFileResult *result, const char *file) {
    const char *target = NULL, *base = NULL;
    if (!cbm_resolve_binding_http_default(index, &target, &base))
        return true;
    const cbm_path_alias_map_t *aliases =
        ctx->path_aliases ? cbm_path_alias_find_for_file(ctx->path_aliases, file) : NULL;
    char project_path[CBM_PATH_ALIAS_RESULT_MAX];
    bool aliased = aliases && cbm_path_alias_resolve_into(aliases, "axios", project_path,
                                                          sizeof(project_path));
    CBMHashTable *packages = cbm_pipeline_get_pkgmap();
    /* NULL denotes an incomplete configuration domain, not external origin. */
    bool project_origin = !packages || aliased || cbm_ht_get(packages, "axios");
    if (project_origin)
        return true;
    /* The extracted default annotation must identify this same source module;
     * a private/named client Variable alone is not a default-value fact. */
    const CBMDefinition *module = NULL;
    for (int d = 0; d < result->defs.count; d++) {
        const CBMDefinition *definition = &result->defs.items[d];
        if (definition->label && !strcmp(definition->label, "Module") &&
            definition->qualified_name && !strcmp(definition->qualified_name, result->module_qn)) {
            if (module)
                return true;
            module = definition;
        }
    }
    const cbm_gbuf_node_t *graph = cbm_gbuf_find_by_qn(ctx->gbuf, result->module_qn);
    if (!module || !module->http_client || strcmp(module->http_client, "axios") || !graph ||
        !graph->label || strcmp(graph->label, "Module") || !graph->file_path ||
        strcmp(graph->file_path, file) ||
        ((base || module->http_base_url) &&
         (!base || !module->http_base_url || strcmp(base, module->http_base_url))))
        return true;
    http_default_t *prior = cbm_ht_get(resolver->http_defaults, file);
    if (prior) {
        prior->ambiguous = true;
        return true;
    }
    http_default_t *value = cbm_arena_calloc(&resolver->arena, sizeof(*value));
    if (!value)
        return false;
    *value = (http_default_t){.target = cbm_arena_strdup(&resolver->arena, target),
                              .module = cbm_arena_strdup(&resolver->arena, result->module_qn),
                              .file = cbm_arena_strdup(&resolver->arena, file),
                              .base = base ? cbm_arena_strdup(&resolver->arena, base) : NULL};
    if (!value->target || !value->module || !value->file || (base && !value->base))
        return false;
    cbm_ht_set(resolver->http_defaults, value->file, value);
    return cbm_ht_get(resolver->http_defaults, value->file) == value;
}

static bool http_site_capture(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                              const char *file, const CBMCall *call, const char *dependency) {
    char *key = cbm_arena_sprintf(&resolver->arena, "%zu:%s:%u:%u:%s", strlen(file), file,
                                  call->site_start_byte, call->site_end_byte, call->callee_name);
    if (!key)
        return false;
    http_import_site_t *prior = cbm_ht_get(resolver->http_sites, key);
    if (prior) {
        prior->ambiguous |= strcmp(prior->scope, call->enclosing_func_qn) != 0 ||
                            strcmp(prior->dependency, dependency) != 0 ||
                            strcmp(prior->url, call->first_string_arg) != 0;
        return true;
    }
    http_import_site_t *value = cbm_arena_calloc(&resolver->arena, sizeof(*value));
    if (!value)
        return false;
    *value = (http_import_site_t){
        .file = cbm_arena_strdup(&resolver->arena, file),
        .dependency = cbm_arena_strdup(&resolver->arena, dependency),
        .scope = cbm_arena_strdup(&resolver->arena, call->enclosing_func_qn),
        .callee = cbm_arena_strdup(&resolver->arena, call->callee_name),
        .url = cbm_arena_strdup(&resolver->arena, call->first_string_arg),
        .content =
            cbm_arena_strdup(&resolver->arena, result->source_content_id[CBM_SOURCE_ORIGIN_RAW]),
        .configuration = cbm_arena_strdup(&resolver->arena, result->source_configuration_id),
        .start = call->site_start_byte,
        .end = call->site_end_byte};
    if (!value->file || !value->dependency || !value->scope || !value->callee || !value->content ||
        !value->configuration || !value->url)
        return false;
    cbm_ht_set(resolver->http_sites, key, value);
    return cbm_ht_get(resolver->http_sites, key) == value;
}

static bool http_project_path(const cbm_pipeline_ctx_t *ctx, const char *file, const char *path) {
    if (!path || !*path)
        return false;
    if (path[0] == '.' || path[0] == '/')
        return true;
    const cbm_path_alias_map_t *aliases =
        ctx->path_aliases ? cbm_path_alias_find_for_file(ctx->path_aliases, file) : NULL;
    char aliased[CBM_PATH_ALIAS_RESULT_MAX];
    bool configured =
        aliases && cbm_path_alias_resolve_into(aliases, path, aliased, sizeof(aliased));
    CBMHashTable *packages = cbm_pipeline_get_pkgmap();
    if (!packages)
        return true; /* Unknown package membership closes the HTTP use domain. */
    char *prefix = cbm_mem_strdup(CBM_MEM_CLASS_EXTRACT, path);
    if (!prefix)
        return true;
    configured |= cbm_ht_get(packages, prefix) != NULL;
    for (size_t i = strlen(prefix); !configured && i > 0; i--)
        if (prefix[i - 1] == '/' || prefix[i - 1] == '.' || prefix[i - 1] == '\\') {
            prefix[i - 1] = '\0';
            configured = cbm_ht_get(packages, prefix) != NULL;
        }
    cbm_free(CBM_MEM_CLASS_EXTRACT, prefix);
    return configured;
}

/* Bind all source-enumerated accesses before any HTTP lookup. Unknown
 * dynamic/project paths close the domain globally. Known unsafe accesses
 * poison their exact dependency, independent of source-file ordering. */
static bool http_domain_capture(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                const cbm_resolve_binding_index_t *index, const char *file) {
    if (!cbm_resolve_binding_http_domain_complete(index)) {
        resolver->http_domain_complete = false;
        return true;
    }
    size_t count = cbm_resolve_binding_http_dependency_count(index);
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_http_dependency_t *access =
            cbm_resolve_binding_http_dependency_at(index, i);
        CBMImport import = {.module_path = access->path};
        const cbm_gbuf_node_t *dependency = NULL;
        cbm_resolve_result_t path =
            cbm_pipeline_resolve_import_result(ctx, file, NULL, &import, NULL, &dependency);
        if (!cbm_resolve_admit(&path) || !dependency || !dependency->file_path) {
            bool project_path = http_project_path(ctx, file, access->path) ||
                                path.binding == CBM_RESOLVE_BINDING_AMBIGUOUS;
            if (project_path ||
                (!strcmp(access->path, "axios") && !access->side_effect_only &&
                 !cbm_resolve_binding_http_import_safe(index, access->local_name, true)))
                resolver->http_domain_complete = false;
            continue;
        }
        if (access->side_effect_only ||
            (access->local_name &&
             cbm_resolve_binding_http_import_safe(index, access->local_name, false)))
            continue;
        char *key = cbm_arena_strdup(&resolver->arena, dependency->file_path);
        if (!key)
            return false;
        cbm_ht_set(resolver->http_unsafe, key, resolver);
        if (cbm_ht_get(resolver->http_unsafe, key) != resolver)
            return false;
    }
    return true;
}

/* Annotation candidates only veto fallback; they never authorize an edge.
 * This covers default/namespace/re-export aliases even when the source proof
 * is incomplete, and is finalized after every target file has been visited. */
static bool http_candidate_imports(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                   const CBMFileResult *result, const char *file) {
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name || !cbm_import_binds_runtime_name(import))
            continue;
        const cbm_gbuf_node_t *dependency = NULL;
        (void)cbm_pipeline_resolve_import_result(ctx, file, NULL, import, NULL, &dependency);
        if ((!dependency || !dependency->file_path) &&
            !http_project_path(ctx, file, import->module_path))
            continue;
        http_candidate_import_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
        if (!entry)
            return false;
        entry->key = cbm_arena_sprintf(&resolver->arena, "%zu:%s:%s", strlen(file), file,
                                       import->local_name);
        entry->dependency = dependency && dependency->file_path
                                ? cbm_arena_strdup(&resolver->arena, dependency->file_path)
                                : NULL;
        if (!entry->key || (dependency && dependency->file_path && !entry->dependency))
            return false;
        entry->next = resolver->http_candidate_imports;
        resolver->http_candidate_imports = entry;
    }
    return true;
}

static bool http_candidate_definition(cbm_shared_resolver_t *resolver,
                                      const CBMDefinition *definition, const char *file) {
    if (!definition->label || !definition->http_client || strcmp(definition->http_client, "axios"))
        return true;
    if (!strcmp(definition->label, "Module")) {
        const char *key = cbm_arena_strdup(&resolver->arena, file);
        if (!key)
            return false;
        cbm_ht_set(resolver->http_candidates, key, resolver);
        return cbm_ht_get(resolver->http_candidates, key) == resolver;
    }
    if (strcmp(definition->label, "Variable") || !definition->name)
        return true;
    http_candidate_import_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    if (!entry)
        return false;
    entry->key =
        cbm_arena_sprintf(&resolver->arena, "%zu:%s:%s", strlen(file), file, definition->name);
    entry->dependency = cbm_arena_strdup(&resolver->arena, file);
    if (!entry->key || !entry->dependency)
        return false;
    entry->next = resolver->http_candidate_imports;
    resolver->http_candidate_imports = entry;
    return true;
}

static bool http_finalize_veto(cbm_shared_resolver_t *resolver) {
    for (http_candidate_import_t *entry = resolver->http_candidate_imports; entry;
         entry = entry->next) {
        if (entry->dependency && !cbm_ht_get(resolver->http_candidates, entry->dependency))
            continue;
        /* Unknown project bindings veto only HTTP spelling fallbacks. An
         * observed Axios-default candidate also vetoes other weak classifiers. */
        void *value = entry->dependency ? (void *)resolver : (void *)resolver->http_veto;
        if (cbm_ht_get(resolver->http_veto, entry->key) == resolver)
            continue;
        cbm_ht_set(resolver->http_veto, entry->key, value);
        if (cbm_ht_get(resolver->http_veto, entry->key) != value)
            return false;
    }
    return true;
}

/* Reuse the one source binding audit already built for exports. Import path
 * queries run once per import; each call contributes one exact site lookup.
 * No source/tree/binding-index pointer survives this temporary file arena. */
static bool http_file_capture(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                              CBMArena *scratch, const cbm_resolve_binding_index_t *index,
                              const CBMFileResult *result, const char *file) {
    if (!http_domain_capture(resolver, ctx, index, file) ||
        !http_default_capture(resolver, ctx, index, result, file))
        return false;
    CBMHashTable *imports = cbm_ht_create(16);
    if (!imports)
        return false;
    bool ok = true;
    for (int i = 0; ok && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->is_default || !import->local_name || !import->module_path ||
            !cbm_import_binds_runtime_name(import))
            continue;
        const char *source_path = cbm_resolve_binding_http_import_path(index, import->local_name);
        if (!source_path || strcmp(source_path, import->module_path))
            continue;
        const cbm_gbuf_node_t *dependency = NULL;
        cbm_resolve_result_t path =
            cbm_pipeline_resolve_import_result(ctx, file, NULL, import, NULL, &dependency);
        if (!cbm_resolve_admit(&path) || !dependency || !dependency->file_path ||
            !dependency->label ||
            (strcmp(dependency->label, "File") && strcmp(dependency->label, "Module")))
            continue;
        cbm_ht_set(imports, import->local_name, (void *)dependency->file_path);
        ok = cbm_ht_get(imports, import->local_name) == dependency->file_path;
    }
    for (int c = 0; ok && c < result->calls.count; c++) {
        const CBMCall *call = &result->calls.items[c];
        const char *dot = call->callee_name ? strchr(call->callee_name, '.') : NULL;
        if (!dot || strchr(dot + 1, '.') || !call->enclosing_func_qn ||
            call->source_origin != CBM_SOURCE_ORIGIN_RAW)
            continue;
        const char *name =
            cbm_arena_strndup(scratch, call->callee_name, (size_t)(dot - call->callee_name));
        if (!name) {
            ok = false;
            break;
        }
        const char *dependency = cbm_ht_get(imports, name);
        if (dependency && cbm_resolve_binding_import_occurrence(index, call, name))
            ok = http_site_capture(resolver, result, file, call, dependency);
    }
    cbm_ht_free(imports);
    return ok;
}

/* A .js file's own manifest can declare it a module whatever its syntax. */
static cbm_resolve_binding_index_t *binding_build_for(CBMArena *arena, const CBMFileResult *result,
                                                      CBMLanguage language, const char *rel_path,
                                                      const char *source, size_t length) {
    bool declared = language == CBM_LANG_JAVASCRIPT && cbm_pipeline_node_declared_module(rel_path);
    return cbm_resolve_binding_build_declared_module(arena, result, language, source, length,
                                                     declared);
}

/* `import a.b` binds the package a, and a.c reads through it. A dotted root import
 * therefore also needs a fact for a itself, once per file and only when no plain
 * `import a` already provides one. */
static bool python_root_fact_needed(const CBMFileResult *result, int position) {
    const CBMImport *import = &result->imports.items[position];
    if (import->kind != CBM_IMPORT_KIND_PYTHON_MODULE_ROOT || !import->module_path ||
        !import->local_name || !strchr(import->module_path, '.') ||
        !cbm_import_binds_runtime_name(import))
        return false;
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *other = &result->imports.items[i];
        if (other->kind != CBM_IMPORT_KIND_PYTHON_MODULE_ROOT || !other->local_name ||
            !other->module_path || strcmp(other->local_name, import->local_name) ||
            !cbm_import_binds_runtime_name(other))
            continue;
        if (!strchr(other->module_path, '.') || i < position)
            return false;
    }
    return true;
}

static const char *python_root_key(CBMArena *arena, const char *file, const char *local) {
    return cbm_arena_sprintf(arena, "python_root:%zu:%s:%s", strlen(file), file, local);
}

static const char *python_nested_key(CBMArena *arena, const char *file, uint32_t at,
                                     const char *local) {
    return cbm_arena_sprintf(arena, "python_nested:%zu:%s:%u:%s", strlen(file), file, at, local);
}

static bool python_note_nested(cbm_shared_resolver_t *resolver, const char *file,
                               const CBMImport *import, const char *key) {
    python_nested_list_t *list = cbm_ht_get(resolver->python_nested, file);
    if (!list) {
        list = cbm_arena_calloc(&resolver->arena, sizeof(*list));
        const char *path = cbm_arena_strdup(&resolver->arena, file);
        if (!list || !path)
            return false;
        cbm_ht_set(resolver->python_nested, path, list);
        if (cbm_ht_get(resolver->python_nested, path) != list)
            return false;
    }
    if (list->count == list->capacity) {
        size_t capacity = list->capacity ? list->capacity * 2 : 8;
        python_nested_entry_t *items = cbm_arena_alloc(&resolver->arena, capacity * sizeof(*items));
        if (!items)
            return false;
        if (list->count)
            memcpy(items, list->items, list->count * sizeof(*items));
        list->items = items;
        list->capacity = capacity;
    }
    python_nested_entry_t entry = {.local = cbm_arena_strdup(&resolver->arena, import->local_name),
                                   .path = cbm_arena_strdup(&resolver->arena, import->module_path),
                                   .key = key,
                                   .kind = import->kind};
    if (!entry.local || !entry.path)
        return false;
    list->items[list->count++] = entry;
    return true;
}

/* The part of a dotted use below an import's binding: `import a.b` binds the
 * package root, so only `a.b` and what follows it reach that module's fact. */
static bool python_nested_suffix(const char *local, const char *path, CBMImportKind kind,
                                 const char *name, const char **suffix) {
    const char *base = kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT ? path : local;
    size_t length = strlen(base);
    if (strncmp(name, base, length) || (name[length] && name[length] != '.'))
        return false;
    *suffix = name[length] ? name + length + 1 : NULL;
    return true;
}

/* Every source-audited import gets its own immutable fact. `exported` is false
 * for a function-scoped import: it binds no module attribute. */
static bool python_export_import(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                 const CBMFileResult *result, const char *file, CBMArena *arena,
                                 const cbm_resolve_binding_index_t *index, const CBMImport *import,
                                 const char *fact_key, bool audited, bool exported, bool runs) {
    bool module = false;
    char *slot = audited ? cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file,
                                                           import, &module)
                         : NULL;
    const cbm_resolve_proof_t *audit =
        fact_key ? export_audit(resolver, result, file, fact_key, slot, (unsigned)import->kind,
                                audited, "source_python_import_audit", import)
                 : NULL;
    const char *loaded = NULL;
    /* A TYPE_CHECKING import never runs, so it loads nothing and writes nothing. */
    bool ok =
        fact_key && audit && (!runs || python_loaded_module(resolver, ctx, file, import, &loaded));
    const cbm_resolve_proof_t *load_audit =
        ok && loaded
            ? export_audit(resolver, result, file, fact_key, loaded, (unsigned)import->kind, false,
                           "python_explicit_module_load_barrier", import)
            : NULL;
    ok = ok && (!loaded || load_audit) &&
         defer_python_import(resolver, ctx, index, file, import, fact_key, slot, module, audited,
                             runs, audit, loaded, load_audit);
    /* A plain dotted import binds only a package root, not the loaded leaf
     * under that root's exported name. Exact module-path uses still retain
     * the private import fact above. */
    bool dotted_root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && import->module_path &&
                       strchr(import->module_path, '.');
    const char *key = cbm_fqn_compute(arena, ctx->project_name, file, import->local_name);
    if (ok && key && !dotted_root && exported) {
        const cbm_resolve_proof_t *binding = export_audit(
            resolver, result, file, key, NULL, 0, audited, "source_python_export_audit", NULL);
        ok = add_export(resolver, key, file, 0, audited, fact_key, binding);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, slot);
    return ok;
}

static CBMHashTable *python_import_map(CBMArena *arena, const char *file,
                                       const CBMFileResult *result);

typedef struct {
    const char *file, *module;
    const CBMHashTable *imports;
    const CBMHashTable *star_provided;
} python_member_imports_t;

/* The import fact a dotted chain's root reaches, and what remains of the chain. */
static const char *python_member_import_key(void *context, CBMArena *arena,
                                            const cbm_resolve_python_nested_import_t *nested,
                                            const char *chain, const char **rest) {
    python_member_imports_t *lookup = context;
    size_t dotted = strcspn(chain, "([@");
    if (nested) {
        const char *base = nested->import.kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT
                               ? nested->import.module_path
                               : nested->import.local_name;
        size_t length = strlen(base);
        if (length > dotted || strncmp(chain, base, length) ||
            (chain[length] && !strchr(".([@", chain[length])))
            return NULL;
        *rest = chain + length;
        return python_nested_key(arena, lookup->file, nested->at, nested->import.local_name);
    }
    char *prefix = cbm_arena_strndup(arena, chain, dotted);
    size_t length = dotted;
    while (prefix && length) {
        prefix[length] = '\0';
        const python_import_t *import = cbm_ht_get(lookup->imports, prefix);
        if (import) {
            *rest = chain + length;
            return import->ambiguous ? NULL : import->slot;
        }
        while (length && chain[--length] != '.') {}
    }
    return NULL;
}

static const char *python_member_star_key(void *context, CBMArena *arena, const char *name) {
    python_member_imports_t *lookup = context;
    if (!lookup->star_provided || !lookup->module)
        return NULL;
    const char *key = cbm_arena_sprintf(
        arena, "%.*s.%s", (int)cbm_fqn_symbol_scope_len(lookup->module), lookup->module, name);
    return key && cbm_ht_get(lookup->star_provided, key) ? export_solver_key(arena, key) : NULL;
}

static bool collect_source_adapter(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                                   const cbm_file_info_t *file) {
    if (!cbm_source_adapter_supported(file->language))
        return true;
    size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
    char *owned = result->source ? NULL : load_source(file->path, &length);
    const char *source = result->source ? result->source : owned;
    bool matches = cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW);
    bool ok = cbm_source_collect(resolver->source_table, result, file->language, file->rel_path,
                                 matches ? source : NULL, length);
    cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
    return ok;
}

static bool export_file(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                        const CBMFileResult *result, const cbm_file_info_t *file) {
    if (!export_language(file->language))
        return true;
    if ((file->language == CBM_LANG_JAVASCRIPT || file->language == CBM_LANG_TYPESCRIPT ||
         file->language == CBM_LANG_TSX || file->language == CBM_LANG_ARKTS) &&
        !http_candidate_imports(resolver, ctx, result, file->rel_path))
        return false;
    CBMArena arena;
    cbm_arena_init(&arena);
    size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
    char *owned = result->source ? NULL : load_source(file->path, &length);
    const char *source = result->source ? result->source : owned;
    if (!cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW)) {
        resolver->http_domain_complete = false;
        resolver->complete = false;
        cbm_arena_destroy(&arena);
        cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
        return true;
    }
    CBMFileResult source_view;
    TSTree *owner_tree = NULL;
    result = owner_source_view(result, file->language, source, length, &source_view, &owner_tree);
    cbm_resolve_binding_index_t *index =
        binding_build_for(&arena, result, file->language, file->rel_path, source, length);
    /* Whether a star can rebind an earlier name is judged across modules. */
    cbm_resolve_binding_python_set_star_barrier(index, 0);
    /* Sequential extraction releases raw buffers. Audit the same verified
     * bytes used for exports instead of requiring parallel source retention.
     * The owner summary copies identities and stores spans, never this buffer. */
    bool ok = index && note_conditional_owners(resolver, result, file->rel_path, file->language,
                                               source, length);
    if (ok && file->language == CBM_LANG_PYTHON)
        ok = capture_python_namespace(resolver, ctx, result, file->rel_path, index);
    if (ok && ecma_language(file->language)) {
        const char *key =
            cbm_arena_sprintf(&arena, "ecma_module:%zu:%s", strlen(file->rel_path), file->rel_path);
        const cbm_resolve_proof_t *audit =
            key ? export_audit(resolver, result, file->rel_path, key, result->module_qn, 0,
                               cbm_resolve_binding_js_exports_complete(index) ||
                                   cbm_resolve_binding_js_export_count(index) > 0,
                               "source_ecma_module_audit", NULL)
                : NULL;
        ok = audit && cbm_ecma_table_add(resolver->ecma, ctx, file->rel_path, result->module_qn,
                                         index, audit);
    }
    if (ok && (file->language == CBM_LANG_JAVASCRIPT || file->language == CBM_LANG_TYPESCRIPT ||
               file->language == CBM_LANG_TSX || file->language == CBM_LANG_ARKTS))
        ok = http_file_capture(resolver, ctx, &arena, index, result, file->rel_path);
    if (ok && file->language == CBM_LANG_DART) {
        namespace_proof_t *proof = cbm_ht_get(resolver->namespaces, file->rel_path);
        if (proof) {
            proof->ambiguous = true;
        } else {
            proof = cbm_arena_calloc(&resolver->arena, sizeof(*proof));
            char *key = cbm_arena_strdup(&resolver->arena, file->rel_path);
            const char *module =
                result->module_qn ? cbm_arena_strdup(&resolver->arena, result->module_qn) : NULL;
            ok = proof && key && module;
            if (ok) {
                cbm_resolve_lookup_context_t captured;
                cbm_resolve_proof_source_t snapshot = {
                    .project = resolver->project,
                    .file = key,
                    .module = module,
                    .scope = module,
                    .content_identity = result->source_content_id[CBM_SOURCE_ORIGIN_RAW],
                    .configuration_identity = result->source_configuration_id,
                    .source_origin = CBM_SOURCE_ORIGIN_RAW};
                ok = cbm_resolve_lookup_context_init(&captured, &resolver->proofs, snapshot);
                if (ok) {
                    /* The interned source fact owns every captured string after
                     * this extraction result and temporary binding index leave. */
                    *proof = (namespace_proof_t){.module_qn = module,
                                                 .source = captured.source,
                                                 .complete =
                                                     cbm_resolve_binding_namespace_complete(index)};
                    cbm_ht_set(resolver->namespaces, key, proof);
                    ok = cbm_ht_get(resolver->namespaces, key) == proof;
                }
            }
        }
        size_t count = cbm_resolve_binding_namespace_size(index);
        for (size_t n = 0; ok && n < count; n++) {
            unsigned kind = 0;
            const char *qn = cbm_resolve_binding_namespace_member(index, n, &kind);
            if (qn) {
                bool supported = (kind & CBM_RESOLVE_EXPORT_VALUE) != 0;
                const cbm_resolve_proof_t *audit =
                    export_audit(resolver, result, file->rel_path, qn, qn, kind, supported,
                                 "source_namespace_export_audit", NULL);
                ok = add_export(resolver, qn, file->rel_path, kind, supported, NULL, audit);
            }
        }
    }
    for (int d = 0; ok && file->language != CBM_LANG_DART && d < result->defs.count; d++) {
        const CBMDefinition *definition = &result->defs.items[d];
        /* Indexing child.py does not execute it or overwrite an explicit child
         * value in __init__.py. Actual loads are separate effect observations. */
        if (file->language == CBM_LANG_PYTHON &&
            ((definition->label && !strcmp(definition->label, "Module")) ||
             python_overload_stub_definition(definition) || python_accessor_definition(definition)))
            continue;
        const char *qn = definition->qualified_name;
        if (qn && qn[0]) {
            unsigned kind = cbm_resolve_binding_export_kind(index, qn);
            bool supported = (kind & CBM_RESOLVE_EXPORT_VALUE) != 0;
            const cbm_resolve_proof_t *audit =
                export_audit(resolver, result, file->rel_path, qn, qn, kind, supported,
                             "source_declaration_export_audit", NULL);
            ok = add_export(resolver, qn, file->rel_path, kind, supported, NULL, audit);
        }
    }
    /* Every source-audited import gets its own immutable fact, including the
     * importing site's hop. A named import depends on the complete exported
     * binding; a module import uses the adapter's exact resolved namespace. */
    for (int i = 0; ok && file->language == CBM_LANG_PYTHON && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name || !cbm_import_binds_runtime_name(import))
            continue;
        ok =
            python_export_import(resolver, ctx, result, file->rel_path, &arena, index, import,
                                 python_import_key(&resolver->arena, file->rel_path, i),
                                 cbm_resolve_binding_import(index, import->local_name), true, true);
        if (ok && python_root_fact_needed(result, i)) {
            CBMImport root = *import;
            root.module_path = import->local_name;
            ok = python_export_import(
                resolver, ctx, result, file->rel_path, &arena, index, &root,
                python_root_key(&resolver->arena, file->rel_path, import->local_name),
                cbm_resolve_binding_import(index, import->local_name), false, false);
        }
    }
    size_t nested = ok && file->language == CBM_LANG_PYTHON
                        ? cbm_resolve_binding_python_nested_count(index)
                        : 0;
    for (size_t i = 0; ok && i < nested; i++) {
        const cbm_resolve_python_nested_import_t *item =
            cbm_resolve_binding_python_nested_at(index, i);
        const char *key =
            python_nested_key(&resolver->arena, file->rel_path, item->at, item->import.local_name);
        ok = key && python_note_nested(resolver, file->rel_path, &item->import, key) &&
             python_export_import(
                 resolver, ctx, result, file->rel_path, &arena, index, &item->import, key,
                 cbm_resolve_binding_python_nested_audited(index, item),
                 /* A TYPE_CHECKING import runs nothing. In a module whose names can come at
                  * run time it is what the module states that name to be: celery and every
                  * PEP 562 lazy package rely on it. Any other module has no such name then. */
                 item->scope == 0 && (!item->annotation_only ||
                                      cbm_resolve_binding_python_names_come_at_run_time(index)),
                 !item->annotation_only);
    }
    if (ok && file->language == CBM_LANG_PYTHON && cbm_resolve_binding_status(index) &&
        !strcmp(cbm_resolve_binding_status(index), "binding_complete_supported_subset")) {
        CBMHashTable *imports = python_import_map(&arena, file->rel_path, result);
        python_member_imports_t lookup = {.file = file->rel_path,
                                          .module = result->module_qn,
                                          .imports = imports,
                                          .star_provided = resolver->python_star_provided};
        /* Star sources settle later, so a star-only base name is unknown here. */
        cbm_py_import_lookup_t table = {&lookup, python_member_import_key, NULL};
        ok = imports && cbm_py_members_add_file(resolver->python_members, index, &table);
        cbm_ht_free(imports);
    }
    if (owner_tree)
        ts_tree_delete(owner_tree);
    cbm_arena_destroy(&arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
    return ok;
}

typedef struct {
    export_proof_t **items;
    size_t count;
} export_list_t;

static void collect_export(const char *key, void *value, void *context) {
    (void)key;
    export_list_t *list = context;
    list->items[list->count++] = value;
}

static int compare_export(const void *left, const void *right) {
    return strcmp((*(export_proof_t *const *)left)->key, (*(export_proof_t *const *)right)->key);
}

/* `from pkg import child` finds the submodule when pkg's namespace has no such name. In
 * pkg's own __init__ the statement is itself that name's binder, and it runs before the
 * name exists, so a binder there of its own does not count. */
static bool python_namespace_absent(cbm_shared_resolver_t *resolver,
                                    const namespace_proof_t *parent, const char *name,
                                    bool in_parent) {
    if (!parent || parent->ambiguous || parent->python_mutated || !name ||
        parent->python_open_barrier)
        return false;
    /* A closed star import binds this name as a value, not the submodule. */
    const char *qualified = cbm_arena_sprintf(&resolver->arena, "%s.%s", parent->module_qn, name);
    if (!qualified || cbm_ht_get(resolver->python_star_provided, qualified) ||
        cbm_ht_get(resolver->python_written, qualified))
        return false;
    /* __getattr__ answers only for a name the module lacks. A child its own __init__ loads is
     * set on it by that load, so it never gets to answer for this one. */
    if (!parent->complete &&
        !(parent->python_audited && cbm_ht_get(resolver->python_parent_loaded, qualified)))
        return false;
    size_t low = 0, high = parent->python_name_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (strcmp(parent->python_names[middle], name) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    size_t binders = 0;
    while (low + binders < parent->python_name_count &&
           !strcmp(parent->python_names[low + binders], name))
        binders++;
    return binders == 0 || (in_parent && binders == 1);
}

static const cbm_resolve_proof_t *python_submodule_proof(cbm_shared_resolver_t *resolver,
                                                         const python_pending_import_t *import,
                                                         const namespace_proof_t *parent,
                                                         const namespace_proof_t *child) {
    cbm_resolve_proof_key_t key = {.lookup_domain = parent->module_qn,
                                   .lookup_key = import->member,
                                   .lookup_closed = true,
                                   .detail = "closed_python_parent_attribute_absent"};
    cbm_resolve_result_t absent = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE, .reason = key.detail};
    const cbm_resolve_proof_t *absence = cbm_resolve_proof_leaf(
        &resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN, &parent->source, &key, &absent);
    key.lookup_domain = child->source.file;
    key.binding_identity = child->module_qn;
    key.detail = "exact_configured_python_submodule_path";
    cbm_resolve_result_t selected = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                     .binding = CBM_RESOLVE_BINDING_UNIQUE,
                                     .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                     .target = child->module_qn,
                                     .candidate_count = 1,
                                     .reason = key.detail};
    const cbm_resolve_proof_t *path = cbm_resolve_proof_leaf(
        &resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN, &child->source, &key, &selected);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(import->audit);
    if (!absence || !path || !view)
        return NULL;
    const cbm_resolve_proof_t *premises[] = {import->audit, absence, path};
    key.lookup_domain = parent->module_qn;
    key.binding_identity = import->key;
    key.detail = "python_from_submodule_after_absent_attribute";
    return cbm_resolve_proof_derive(&resolver->proofs, CBM_PROOF_IMPORT_BINDING, &view->source,
                                    &key, &selected, premises, 3, true);
}

static void invalidate_python_namespace(cbm_shared_resolver_t *resolver, const char *module) {
    namespace_proof_t *entry = module ? cbm_ht_get(resolver->python_namespaces, module) : NULL;
    if (entry)
        entry->python_mutated = true;
}
/* A loaded child is installed on its parent's namespace. Conservatively
 * retain this barrier across all analyzed files and execution orders; later
 * source-order analysis may recover bindings captured before that load. Each
 * prefix must itself be an exact captured module namespace, so unrelated
 * same-leaf files never invalidate one another. Work follows imported
 * namespace prefixes, using hash lookups rather than a files-by-import scan. */
static bool collect_python_module_loads(cbm_shared_resolver_t *resolver) {
    /* `from . import child` in the parent's own __init__ binds the very module a load installs
     * there, so that load competes with nothing. */
    CBMHashTable *own_children = cbm_ht_create(32);
    if (!own_children)
        return false;
    for (const python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        /* A load by the parent's own __init__: a from-import of the child, or of something in it.
         */
        const namespace_proof_t *source = cbm_ht_get(resolver->namespaces, import->source_file);
        const char *dot = import->loaded_module ? strrchr(import->loaded_module, '.') : NULL;
        if (source && dot && strlen(source->module_qn) == (size_t)(dot - import->loaded_module) &&
            !strncmp(source->module_qn, import->loaded_module,
                     (size_t)(dot - import->loaded_module)))
            cbm_ht_set(resolver->python_parent_loaded, import->loaded_module, resolver);
        const char *key = import->in_parent && import->slot ? import->slot : NULL;
        if (key) {
            cbm_ht_set(own_children, key, resolver);
            cbm_ht_set(resolver->python_parent_loaded, key, resolver);
        }
    }
    for (const python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        if (!import->loaded_module)
            continue;
        char *module = cbm_arena_strdup(&resolver->arena, import->loaded_module);
        if (!module || !import->load_audit) {
            cbm_ht_free(own_children);
            return false;
        }
        for (;;) {
            const namespace_proof_t *loaded = cbm_ht_get(resolver->python_namespaces, module);
            target_proof_t *declaration = cbm_ht_get(resolver->targets, module);
            /* Local lexical proofs also use this census. They cannot retain
             * callable identity after a module load replaces that binding. */
            if (loaded && declaration)
                declaration->ambiguous = true;
            /* Keep actual load support beside any value it can overwrite. A
             * source namespace alone is never a competing value declaration. */
            if (loaded && cbm_ht_get(resolver->exports, module) &&
                !cbm_ht_get(own_children, module) &&
                !add_export(resolver, module, loaded->source.file, 0, false, NULL,
                            import->load_audit)) {
                cbm_ht_free(own_children);
                return false;
            }
            /* The load installs this one child on its parent: the same module
             * identity a submodule import names, so it closes no other name. */
            char *dot = strrchr(module, '.');
            if (!dot)
                break;
            *dot = '\0';
        }
    }
    cbm_ht_free(own_children);
    return true;
}

/* A statement that stores through an imported module changes the name it spells, and
 * no other: `django.__file__ = None` in a test says nothing about django.forms. Only a
 * write that can reach any name takes the module's whole absence proof. */
static bool collect_python_namespace_effects(cbm_shared_resolver_t *resolver) {
    for (const python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        const char *base = import->write_root ? import->write_root : import->write_slot;
        for (size_t i = 0; i < import->write_count; i++) {
            if (!import->writes[i]) {
                invalidate_python_namespace(resolver, import->write_root);
                if (import->write_module)
                    invalidate_python_namespace(resolver, import->write_slot);
                continue;
            }
            if (!base)
                continue;
            const char *key = cbm_arena_sprintf(&resolver->arena, "%s.%s", base, import->writes[i]);
            if (!key)
                return false;
            cbm_ht_set(resolver->python_written, key, resolver);
            if (cbm_ht_get(resolver->python_written, key) != resolver)
                return false;
        }
    }
    return true;
}

/* The names `from module import *` binds. Unknown when __all__ is not a static
 * literal, when the module was not fully audited, or through an unknown star. */
static void python_star_interface(cbm_shared_resolver_t *resolver, namespace_proof_t *module) {
    if (module->python_visible_state == 2)
        return;
    /* A computed __all__ can only narrow what the module binds, and a file that asks a star for
     * a name relies on it being there. So the public names stand in for it. */
    if (module->python_visible_state == 1 || !module->complete || module->ambiguous) {
        module->python_visible_open = true;
        if (module->python_visible_state == 0)
            module->python_visible_state = 2;
        return;
    }
    module->python_visible_state = 1;
    if (module->python_all_literal && !module->python_all_unknown) {
        module->python_visible = module->python_all;
        module->python_visible_count = module->python_all_count;
        module->python_visible_state = 2;
        return;
    }
    size_t total = module->python_name_count;
    for (size_t s = 0; s < module->python_star_count; s++) {
        namespace_proof_t *source =
            module->python_stars[s].module
                ? cbm_ht_get(resolver->python_namespaces, module->python_stars[s].module)
                : NULL;
        if (!source) {
            module->python_visible_open = true;
            continue;
        }
        python_star_interface(resolver, source);
        module->python_visible_open |= source->python_visible_open;
        total += source->python_visible_count;
    }
    const char **names = total ? cbm_arena_alloc(&resolver->arena, total * sizeof(*names)) : NULL;
    if (total && !names) {
        module->python_visible_open = true;
        module->python_visible_state = 2;
        return;
    }
    size_t count = 0;
    for (size_t n = 0; n < module->python_name_count; n++)
        if (module->python_names[n][0] != '_')
            names[count++] = module->python_names[n];
    for (size_t s = 0; s < module->python_star_count; s++) {
        namespace_proof_t *source =
            module->python_stars[s].module
                ? cbm_ht_get(resolver->python_namespaces, module->python_stars[s].module)
                : NULL;
        for (size_t n = 0; source && n < source->python_visible_count; n++)
            names[count++] = source->python_visible[n];
    }
    module->python_visible = names;
    module->python_visible_count = count;
    module->python_visible_state = 2;
}

static bool python_explicit_after(const namespace_proof_t *module, const char *name, uint32_t at) {
    size_t low = 0, high = module->python_name_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (strcmp(module->python_names[middle], name) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    for (; low < module->python_name_count && !strcmp(module->python_names[low], name); low++)
        if (module->python_name_at[low] >= at)
            return true;
    return false;
}

/* Last star wins; a later explicit binder wins over any star. An unknown star
 * can rebind whatever precedes it, so that stops the walk, and so does a star under a condition:
 * the binder stops at the first of `if c: from .a import *  else: from .b import *` and never
 * sees the other. */
static bool python_expand_module_stars(cbm_shared_resolver_t *resolver, namespace_proof_t *module) {
    for (size_t s = module->python_star_count; s-- > 0;) {
        const python_star_source_t *star = &module->python_stars[s];
        namespace_proof_t *source =
            star->module ? cbm_ht_get(resolver->python_namespaces, star->module) : NULL;
        if (source)
            python_star_interface(resolver, source);
        if (!source || source->python_visible_open || (module->python_star_chosen)) {
            module->python_open_barrier = star->at;
            for (size_t n = 0; n < module->python_name_count; n++) {
                if (module->python_name_at[n] >= star->at)
                    continue;
                const char *key = cbm_arena_sprintf(&resolver->arena, "%s.%s", module->module_qn,
                                                    module->python_names[n]);
                export_proof_t *entry = key ? cbm_ht_get(resolver->exports, key) : NULL;
                if (!key)
                    return false;
                if (entry) {
                    entry->supported = false;
                    entry->dependency = NULL;
                }
            }
            return true;
        }
        for (size_t n = 0; n < source->python_visible_count; n++) {
            const char *name = source->python_visible[n];
            const char *key = cbm_arena_sprintf(&resolver->arena, "%s.%s", module->module_qn, name);
            const char *origin =
                cbm_arena_sprintf(&resolver->arena, "%s.%s", source->module_qn, name);
            if (!key || !origin)
                return false;
            if (cbm_ht_get(resolver->python_star_provided, key) ||
                python_explicit_after(module, name, star->at))
                continue;
            cbm_resolve_proof_key_t lookup = {.lookup_domain = source->module_qn,
                                              .lookup_key = name,
                                              .binding_identity = key,
                                              .declaration_identity = origin,
                                              .detail = "python_closed_star_import"};
            cbm_resolve_result_t outcome = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                            .binding = CBM_RESOLVE_BINDING_UNIQUE,
                                            .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                            .target = origin,
                                            .candidate_count = 1,
                                            .reason = lookup.detail};
            const cbm_resolve_proof_t *audit = cbm_resolve_proof_leaf(
                &resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN, &module->source, &lookup, &outcome);
            const char *dependency = export_solver_key(&resolver->arena, origin);
            if (!audit || !dependency ||
                !add_export(resolver, key, module->source.file, 0, true, dependency, audit))
                return false;
            cbm_ht_set(resolver->python_star_provided, key, source);
            if (cbm_ht_get(resolver->python_star_provided, key) != source)
                return false;
        }
    }
    return true;
}

typedef struct {
    namespace_proof_t **items;
    size_t count;
} python_namespace_list_t;

static void collect_python_namespace(const char *key, void *value, void *context) {
    (void)key;
    python_namespace_list_t *list = context;
    list->items[list->count++] = value;
}

static int compare_python_namespace(const void *left, const void *right) {
    return strcmp((*(namespace_proof_t *const *)left)->module_qn,
                  (*(namespace_proof_t *const *)right)->module_qn);
}

static bool expand_python_stars(cbm_shared_resolver_t *resolver) {
    size_t count = cbm_ht_count(resolver->python_namespaces);
    if (!count)
        return true;
    namespace_proof_t **items = cbm_arena_alloc(&resolver->arena, count * sizeof(*items));
    if (!items)
        return false;
    python_namespace_list_t list = {.items = items};
    cbm_ht_foreach(resolver->python_namespaces, collect_python_namespace, &list);
    qsort(items, list.count, sizeof(*items), compare_python_namespace);
    for (size_t i = 0; i < list.count; i++)
        if (items[i]->python_star_count && !python_expand_module_stars(resolver, items[i]))
            return false;
    return true;
}

/* Decorators must be judged before exports stabilize, because a kept declaration
 * is an export. These follow the same import facts one hop at a time. */
typedef struct {
    cbm_shared_resolver_t *resolver;
    CBMHashTable *by_key; /* fact key -> python_pending_import_t */
} python_presolve_t;

static bool python_presolve_follow(const python_presolve_t *presolve, const char *key,
                                   const char **target, bool *module, bool *external) {
    const cbm_shared_resolver_t *resolver = presolve->resolver;
    for (unsigned hop = 0; hop < 32; hop++) {
        const python_pending_import_t *import = cbm_ht_get(presolve->by_key, key);
        if (import) {
            if (!import->audited)
                return false;
            if (!import->slot) {
                *external = true;
                return false;
            }
            if (import->module) {
                *target = import->slot;
                *module = true;
                return true;
            }
            key = import->slot;
        }
        const export_proof_t *entry = cbm_ht_get(resolver->exports, key);
        if (!entry || entry->ambiguous)
            return false;
        if (!entry->dependency) {
            *target = entry->key;
            return true;
        }
        key = entry->dependency;
        /* A star dependency is keyed as a solver export. */
        if (!strncmp(key, "export:", 7)) {
            const char *colon = strchr(key + 7, ':');
            if (!colon)
                return false;
            key = colon + 1;
        }
    }
    return false;
}

static bool python_presolve_import_value(const void *context, const char *key, const char **target,
                                         bool *module, bool *external) {
    return python_presolve_follow(context, key, target, module, external);
}

static bool python_presolve_module_member(const void *context, CBMArena *arena, const char *module,
                                          const char *name, const char **target, bool *is_module) {
    const python_presolve_t *presolve = context;
    const char *member = cbm_arena_sprintf(arena, "%s.%s", module, name);
    bool external = false;
    if (!member)
        return false;
    if (python_presolve_follow(presolve, member, target, is_module, &external))
        return true;
    const namespace_proof_t *child = cbm_ht_get(presolve->resolver->python_namespaces, member);
    if (!child || child->ambiguous)
        return false;
    *target = child->module_qn;
    *is_module = true;
    return true;
}

static const char *python_presolve_label(const void *context, const char *qualified_name) {
    const python_presolve_t *presolve = context;
    const target_proof_t *target = cbm_ht_get(presolve->resolver->targets, qualified_name);
    return target && !target->ambiguous ? target->label : NULL;
}

/* A kept declaration exports as if undecorated. */
static bool python_publish_kept(void *context, const char *target, bool is_class) {
    cbm_shared_resolver_t *resolver = context;
    export_proof_t *entry = cbm_ht_get(resolver->exports, target);
    if (!entry || entry->ambiguous || entry->dependency)
        return true;
    const namespace_proof_t *module = NULL;
    const char *dot = strrchr(target, '.');
    if (dot) {
        const char *scope = cbm_arena_strndup(&resolver->arena, target, (size_t)(dot - target));
        module = scope ? cbm_ht_get(resolver->python_namespaces, scope) : NULL;
    }
    if (!module)
        return true;
    cbm_resolve_proof_key_t lookup = {.lookup_domain = module->module_qn,
                                      .lookup_key = target,
                                      .binding_identity = target,
                                      .declaration_identity = target,
                                      .detail = "python_decorators_keep_declaration"};
    cbm_resolve_result_t outcome = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                    .binding = CBM_RESOLVE_BINDING_UNIQUE,
                                    .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                    .target = target,
                                    .candidate_count = 1,
                                    .reason = lookup.detail};
    const cbm_resolve_proof_t *audit = cbm_resolve_proof_leaf(
        &resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN, &module->source, &lookup, &outcome);
    export_support_t *support = cbm_arena_calloc(&resolver->arena, sizeof(*support));
    if (!audit || !support)
        return false;
    support->audit = audit;
    entry->supports = support;
    entry->support_count = 1;
    entry->supported = true;
    entry->kind = CBM_RESOLVE_EXPORT_VALUE | (is_class ? CBM_RESOLVE_EXPORT_TYPE : 0);
    return true;
}

static bool decide_python_decorated(cbm_shared_resolver_t *resolver) {
    CBMHashTable *by_key = cbm_ht_create(256);
    if (!by_key)
        return false;
    for (python_pending_import_t *import = resolver->python_imports; import; import = import->next)
        cbm_ht_set(by_key, import->key, import);
    python_presolve_t presolve = {.resolver = resolver, .by_key = by_key};
    cbm_py_members_env_t env = {.context = &presolve,
                                .import_value = python_presolve_import_value,
                                .module_member = python_presolve_module_member,
                                .label = python_presolve_label};
    bool ok = cbm_py_members_decide_decorated(resolver->python_members, &env, python_publish_kept,
                                              resolver);
    cbm_ht_free(by_key);
    return ok;
}

static bool settle_python_imports(cbm_shared_resolver_t *resolver) {
    if (!collect_python_module_loads(resolver))
        return false;
    if (!collect_python_namespace_effects(resolver) || !expand_python_stars(resolver))
        return false;
    for (python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        const namespace_proof_t *parent =
            import->parent_file ? cbm_ht_get(resolver->namespaces, import->parent_file) : NULL;
        const namespace_proof_t *child =
            import->child_file ? cbm_ht_get(resolver->namespaces, import->child_file) : NULL;
        if (child && !child->ambiguous &&
            python_namespace_absent(resolver, parent, import->member, import->in_parent)) {
            const cbm_resolve_proof_t *proof =
                python_submodule_proof(resolver, import, parent, child);
            if (!proof)
                return false;
            import->slot = child->module_qn;
            import->module = true;
            import->audit = proof;
        }
        bool ok;
        if (import->slot && !import->module) {
            const char *dependency = export_solver_key(&resolver->arena, import->slot);
            ok = dependency &&
                 cbm_resolve_solver_copy(resolver->solver, import->key, dependency, import->audit);
        } else {
            ok = cbm_resolve_solver_leaf(
                resolver->solver, import->key,
                !import->audited ? CBM_SOLVER_UNSUPPORTED
                : import->slot   ? CBM_SOLVER_KNOWN
                                 : CBM_SOLVER_UNKNOWN,
                (cbm_resolve_solver_value_t){.target = import->slot, .module = import->module},
                import->audit);
        }
        if (!ok)
            return false;
    }
    /* Submodule proofs above decided which imports are modules. */
    return decide_python_decorated(resolver);
}

static bool stabilize_exports(cbm_shared_resolver_t *resolver) {
    size_t count = cbm_ht_count(resolver->exports);
    if (count > SIZE_MAX / sizeof(export_proof_t *) ||
        count > SIZE_MAX / sizeof(*resolver->summary))
        return false;
    export_proof_t **items =
        count ? cbm_arena_alloc(&resolver->arena, count * sizeof(*items)) : NULL;
    resolver->summary =
        count ? cbm_arena_calloc(&resolver->arena, count * sizeof(*resolver->summary)) : NULL;
    if (count && (!items || !resolver->summary))
        return false;
    export_list_t list = {.items = items};
    cbm_ht_foreach(resolver->exports, collect_export, &list);
    if (count > 1)
        qsort(items, count, sizeof(*items), compare_export);
    for (size_t i = 0; i < count; i++) {
        const export_proof_t *entry = items[i];
        const cbm_resolve_proof_t *audit = entry->supports->audit;
        if (entry->support_count > 1) {
            if (entry->support_count > SIZE_MAX / sizeof(audit))
                return false;
            const cbm_resolve_proof_t **premises =
                cbm_arena_alloc(&resolver->arena, entry->support_count * sizeof(*premises));
            if (!premises)
                return false;
            size_t n = 0;
            for (const export_support_t *support = entry->supports; support;
                 support = support->next)
                premises[n++] = support->audit;
            cbm_resolve_proof_source_t source = {.project = resolver->project, .scope = entry->key};
            cbm_resolve_proof_key_t key = {.lookup_domain = "shared_exports",
                                           .lookup_key = entry->key,
                                           .binding_identity = entry->key,
                                           .detail = "conflicting_export_registrations"};
            cbm_resolve_result_t conflict = {.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                             .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                             .candidate_count = 2};
            audit = cbm_resolve_proof_derive(&resolver->proofs, CBM_PROOF_EXPORT_BINDING, &source,
                                             &key, &conflict, premises, n, false);
            if (!audit)
                return false;
        }
        bool ok;
        if (entry->dependency && !entry->ambiguous) {
            ok = cbm_resolve_solver_copy(resolver->solver, entry->solver_key, entry->dependency,
                                         audit);
        } else {
            cbm_resolve_solver_status_t status = entry->ambiguous   ? CBM_SOLVER_AMBIGUOUS
                                                 : entry->supported ? CBM_SOLVER_KNOWN
                                                                    : CBM_SOLVER_UNSUPPORTED;
            ok = cbm_resolve_solver_leaf(
                resolver->solver, entry->solver_key, status,
                (cbm_resolve_solver_value_t){.target = entry->key, .kind = entry->kind}, audit);
        }
        if (!ok)
            return false;
    }
    if (!cbm_resolve_solver_solve(resolver->solver))
        return false;
    resolver->complete &= cbm_resolve_solver_complete(resolver->solver);
    for (size_t i = 0; i < count; i++) {
        resolver->summary[i].file = items[i]->file;
        cbm_resolve_solver_get(resolver->solver, items[i]->solver_key,
                               &resolver->summary[i].binding);
        resolver->summary[i].binding.key = items[i]->key;
    }
    resolver->summary_count = count;
    return true;
}

static CBMHashTable *python_import_map(CBMArena *arena, const char *file,
                                       const CBMFileResult *result) {
    CBMHashTable *map = cbm_ht_create(32);
    if (!map)
        return NULL;
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name || !cbm_import_binds_runtime_name(import))
            continue;
        bool root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && import->module_path &&
                    strchr(import->module_path, '.');
        const char *name = root ? import->module_path : import->local_name;
        python_import_t *prior = cbm_ht_get(map, name);
        if (prior) {
            prior->ambiguous = true;
            continue;
        }
        python_import_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
        const char *key = cbm_arena_strdup(arena, name);
        const char *slot = python_import_key(arena, file, i);
        if (!entry || !key || !slot) {
            cbm_ht_free(map);
            return NULL;
        }
        entry->slot = slot;
        cbm_ht_set(map, key, entry);
        if (cbm_ht_get(map, key) != entry) {
            cbm_ht_free(map);
            return NULL;
        }
    }
    for (int i = 0; i < result->imports.count; i++) {
        if (!python_root_fact_needed(result, i))
            continue;
        const char *local = result->imports.items[i].local_name;
        python_import_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
        const char *key = cbm_arena_strdup(arena, local);
        const char *slot = python_root_key(arena, file, local);
        if (!entry || !key || !slot) {
            cbm_ht_free(map);
            return NULL;
        }
        entry->slot = slot;
        cbm_ht_set(map, key, entry);
    }
    return map;
}

static const char *python_query_key(CBMArena *arena, const char *import_key, const char *suffix) {
    return cbm_arena_sprintf(arena, "python_member:%zu:%s:%zu:%s", strlen(import_key), import_key,
                             strlen(suffix), suffix);
}

static bool collect_python_member_query(cbm_shared_resolver_t *resolver, CBMArena *arena,
                                        const char *slot, const char *suffix) {
    if (!suffix || !suffix[0] || strchr(suffix, '.'))
        return true;
    cbm_resolve_solver_result_t binding;
    cbm_resolve_solver_get(resolver->solver, slot, &binding);
    if (binding.status != CBM_SOLVER_KNOWN || !binding.value.module)
        return true;
    const char *key = python_query_key(arena, slot, suffix);
    if (!key)
        return false;
    if (cbm_ht_get(resolver->python_queries, key))
        return true;
    if (cbm_ht_count(resolver->python_queries) >= 1000000) {
        resolver->complete = false;
        return true;
    }
    python_query_t *query = cbm_arena_calloc(&resolver->arena, sizeof(*query));
    const char *saved_key = cbm_arena_strdup(&resolver->arena, key);
    if (!query || !saved_key)
        return false;
    *query = (python_query_t){.key = saved_key,
                              .import_key = cbm_arena_strdup(&resolver->arena, slot),
                              .suffix = cbm_arena_strdup(&resolver->arena, suffix)};
    if (!query->import_key || !query->suffix)
        return false;
    cbm_ht_set(resolver->python_queries, saved_key, query);
    return cbm_ht_get(resolver->python_queries, saved_key) == query;
}

static bool collect_python_query(cbm_shared_resolver_t *resolver, CBMArena *arena,
                                 const CBMHashTable *imports, const python_nested_list_t *nested,
                                 const char *name) {
    if (!name || !name[0])
        return true;
    for (size_t i = 0; nested && i < nested->count; i++) {
        const python_nested_entry_t *entry = &nested->items[i];
        const char *suffix = NULL;
        if (python_nested_suffix(entry->local, entry->path, entry->kind, name, &suffix) &&
            !collect_python_member_query(resolver, arena, entry->key, suffix))
            return false;
    }
    char *prefix = cbm_arena_strdup(arena, name);
    if (!prefix)
        return false;
    size_t length = strlen(name);
    while (length) {
        prefix[length] = '\0';
        const python_import_t *import = cbm_ht_get(imports, prefix);
        if (import) {
            const char *suffix = name[length] ? name + length + 1 : NULL;
            return import->ambiguous ||
                   collect_python_member_query(resolver, arena, import->slot, suffix);
        }
        while (length && name[--length] != '.') {}
    }
    return true;
}

static cbm_resolve_result_t python_binding_outcome(const cbm_resolve_solver_result_t *binding) {
    cbm_resolve_result_t outcome = {.reason = "python_stabilized_export_binding",
                                    .proof = binding->proof};
    switch (binding->status) {
    case CBM_SOLVER_KNOWN:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        if (!binding->value.module) {
            outcome.binding = CBM_RESOLVE_BINDING_UNIQUE;
            outcome.domain = CBM_RESOLVE_DOMAIN_PROJECT;
            outcome.target = binding->value.target;
            outcome.candidate_count = 1;
        }
        break;
    case CBM_SOLVER_AMBIGUOUS:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        outcome.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        outcome.candidate_count = 2;
        break;
    case CBM_SOLVER_PENDING:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_PENDING;
        break;
    case CBM_SOLVER_UNKNOWN:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN;
        break;
    case CBM_SOLVER_UNSUPPORTED:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        break;
    case CBM_SOLVER_LIMITED:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
        break;
    case CBM_SOLVER_FAILED:
        outcome.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        break;
    }
    return outcome;
}

typedef struct {
    python_query_t **items;
    size_t count;
} python_query_list_t;

static void collect_query_row(const char *key, void *value, void *context) {
    (void)key;
    python_query_list_t *list = context;
    list->items[list->count++] = value;
}

static int compare_query(const void *left, const void *right) {
    return strcmp((*(python_query_t *const *)left)->key, (*(python_query_t *const *)right)->key);
}

static bool settle_python_query(cbm_shared_resolver_t *resolver, python_query_t *query) {
    cbm_resolve_solver_result_t base;
    cbm_resolve_solver_get(resolver->solver, query->import_key, &base);
    if (base.status != CBM_SOLVER_KNOWN || !base.value.module || !base.proof)
        return false;
    const char *member =
        cbm_arena_sprintf(&resolver->arena, "%s.%s", base.value.target, query->suffix);
    const char *key = export_solver_key(&resolver->arena, member);
    if (!member || !key)
        return false;
    cbm_resolve_solver_get(resolver->solver, key, &query->result);
    const cbm_resolve_proof_view_t *input = cbm_resolve_proof_view(base.proof);
    cbm_resolve_proof_key_t lookup = {.lookup_domain = base.value.target,
                                      .lookup_key = query->suffix,
                                      .binding_identity = query->key,
                                      .detail = "exact_module_export_lookup"};
    const cbm_resolve_proof_t *member_proof = query->result.proof;
    if (!member_proof) {
        lookup.lookup_closed = cbm_resolve_solver_complete(resolver->solver);
        cbm_resolve_result_t missing = {.analysis = lookup.lookup_closed
                                                        ? CBM_RESOLVE_ANALYSIS_COMPLETE
                                                        : CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                        .reason = "missing_module_export"};
        member_proof = cbm_resolve_proof_leaf(&resolver->proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                              &input->source, &lookup, &missing);
    }
    if (!member_proof)
        return false;
    const cbm_resolve_proof_t *premises[] = {base.proof, member_proof};
    cbm_resolve_result_t outcome = python_binding_outcome(&query->result);
    query->result.key = query->key;
    query->result.predecessors_closed =
        base.predecessors_closed &&
        (query->result.proof ? query->result.predecessors_closed : true);
    query->result.proof =
        cbm_resolve_proof_derive(&resolver->proofs, CBM_PROOF_REGISTRY_LOOKUP, &input->source,
                                 &lookup, &outcome, premises, 2, query->result.predecessors_closed);
    return query->result.proof != NULL;
}

static bool python_env_import_value(const void *context, const char *key, const char **target,
                                    bool *module, bool *external) {
    const cbm_shared_resolver_t *resolver = context;
    cbm_resolve_solver_result_t binding;
    cbm_resolve_solver_get(resolver->solver, key, &binding);
    if (binding.status == CBM_SOLVER_KNOWN) {
        *target = binding.value.target;
        *module = binding.value.module;
        return *target != NULL;
    }
    /* A leaf import with no project slot: the path names no project module. */
    *external =
        binding.status == CBM_SOLVER_UNKNOWN && binding.proof && binding.dependency_count == 0;
    return false;
}

static bool python_env_module_member(const void *context, CBMArena *arena, const char *module,
                                     const char *name, const char **target, bool *is_module) {
    const cbm_shared_resolver_t *resolver = context;
    const char *member = cbm_arena_sprintf(arena, "%s.%s", module, name);
    const char *key = member ? export_solver_key(arena, member) : NULL;
    if (!key)
        return false;
    cbm_resolve_solver_result_t binding;
    cbm_resolve_solver_get(resolver->solver, key, &binding);
    if (binding.status == CBM_SOLVER_KNOWN) {
        *target = binding.value.target;
        *is_module = binding.value.module;
        return *target != NULL;
    }
    /* An attribute no module-level binder provides reads the loaded submodule. */
    namespace_proof_t *parent = cbm_ht_get(resolver->python_namespaces, module);
    namespace_proof_t *child = cbm_ht_get(resolver->python_namespaces, member);
    if (binding.proof || !child || child->ambiguous || !parent || parent->ambiguous ||
        parent->python_open_barrier ||
        (!parent->complete &&
         !(parent->python_audited && cbm_ht_get(resolver->python_parent_loaded, member))))
        return false;
    *target = child->module_qn;
    *is_module = true;
    return true;
}

static const char *python_env_label(const void *context, const char *qualified_name) {
    const cbm_shared_resolver_t *resolver = context;
    const target_proof_t *target = cbm_ht_get(resolver->targets, qualified_name);
    return target && !target->ambiguous ? target->label : NULL;
}

static cbm_py_members_env_t python_members_env(const cbm_shared_resolver_t *resolver) {
    return (cbm_py_members_env_t){.context = resolver,
                                  .import_value = python_env_import_value,
                                  .module_member = python_env_module_member,
                                  .label = python_env_label};
}

static bool prepare_python_queries(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                   const cbm_file_info_t *files, int file_count,
                                   CBMFileResult **cache) {
    if (!resolver->complete)
        return true;
    /* One demand pass after stabilization. Each requested module member is
     * derived once in the shared pool, so workers never copy whole chains. */
    for (int f = 0; f < file_count && resolver->complete; f++) {
        if (files[f].language != CBM_LANG_PYTHON)
            continue;
        bool loaded = false;
        CBMFileResult *result = cbm_pipeline_result_acquire(ctx, cache, f, NULL, &loaded);
        if (!result) {
            resolver->http_domain_complete = false;
            resolver->complete = false;
            break;
        }
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMHashTable *imports = python_import_map(&arena, files[f].rel_path, result);
        const python_nested_list_t *nested = cbm_ht_get(resolver->python_nested, files[f].rel_path);
        bool ok = imports != NULL;
        for (int i = 0; ok && i < result->calls.count; i++)
            ok = collect_python_query(resolver, &arena, imports, nested,
                                      result->calls.items[i].callee_name);
        for (int i = 0; ok && i < result->usages.count; i++)
            ok = collect_python_query(resolver, &arena, imports, nested,
                                      result->usages.items[i].ref_name);
        for (int i = 0; ok && i < result->rw.count; i++)
            ok = collect_python_query(resolver, &arena, imports, nested,
                                      result->rw.items[i].var_name);
        for (int i = 0; ok && i < result->type_refs.count; i++)
            ok = collect_python_query(resolver, &arena, imports, nested,
                                      result->type_refs.items[i].type_name);
        cbm_ht_free(imports);
        cbm_arena_destroy(&arena);
        cbm_pipeline_result_release(result, loaded);
        if (!ok)
            return false;
    }
    if (!resolver->complete)
        return true;
    size_t count = cbm_ht_count(resolver->python_queries);
    python_query_t **items =
        count ? cbm_arena_alloc(&resolver->arena, count * sizeof(*items)) : NULL;
    if (count && !items)
        return false;
    python_query_list_t list = {.items = items};
    cbm_ht_foreach(resolver->python_queries, collect_query_row, &list);
    if (count > 1)
        qsort(items, count, sizeof(*items), compare_query);
    for (size_t i = 0; i < count; i++)
        if (!settle_python_query(resolver, items[i]))
            return false;
    return true;
}

/* Imports bind by declared package name, which every importer needs before any file's
 * imports are collected. An unreadable file simply declares nothing here. */
static bool note_go_packages(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                             const cbm_file_info_t *files, int file_count) {
    for (int f = 0; f < file_count; f++) {
        if (files[f].language != CBM_LANG_GO)
            continue;
        size_t length = 0;
        char *source = load_source(files[f].path, &length);
        if (!source)
            continue;
        CBMArena arena;
        cbm_arena_init(&arena);
        char *path = cbm_arena_strdup(&arena, files[f].rel_path);
        if (path)
            cbm_normalize_path_sep(path);
        const char *module =
            path ? cbm_fqn_module_source_lang(&arena, ctx->project_name, path, CBM_LANG_GO) : NULL;
        bool ok =
            module && cbm_go_stabilization_note_package(resolver->go_types, module, source, length);
        cbm_arena_destroy(&arena);
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
        if (!ok)
            return false;
    }
    return true;
}

static bool collect_go_types(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                             const CBMFileResult *result, const cbm_file_info_t *file) {
    if (file->language != CBM_LANG_GO)
        return true;
    size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
    char *owned = result->source ? NULL : load_source(file->path, &length);
    const char *source = result->source ? result->source : owned;
    if (!cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW)) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
        resolver->complete = false;
        return true;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    char *path = cbm_arena_strdup(&arena, file->rel_path);
    if (path)
        cbm_normalize_path_sep(path);
    const char *module =
        path ? cbm_fqn_module_source_lang(&arena, ctx->project_name, path, CBM_LANG_GO) : NULL;
    bool ok = module && cbm_pipeline_capture_go_imports(ctx, file->rel_path, result);
    cbm_resolve_proof_source_t snapshot = {.project = resolver->project,
                                           .file = file->rel_path,
                                           .module = module,
                                           .scope = module,
                                           .content_identity = result->source_content_id[0],
                                           .configuration_identity =
                                               result->source_configuration_id};
    if (ok)
        ok = cbm_go_stabilization_collect(resolver->go_types, result, source, length, &snapshot,
                                          module, cbm_gbuf_imports(ctx->gbuf, file->rel_path));
    cbm_arena_destroy(&arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
    return ok;
}

static char *solver_summary_payload(const cbm_resolve_solver_result_t *binding) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc)
        return NULL;
    yyjson_mut_val *object = yyjson_mut_obj(doc), *dependencies = yyjson_mut_arr(doc);
    yyjson_mut_doc_set_root(doc, object);
    bool ok =
        object && dependencies &&
        yyjson_mut_obj_add_uint(doc, object, "fact_kind", binding->value.fact_kind) &&
        yyjson_mut_obj_add_uint(doc, object, "binding_version", binding->value.binding_version) &&
        yyjson_mut_obj_add_str(doc, object, "type_identity",
                               binding->value.type_identity ? binding->value.type_identity : "") &&
        yyjson_mut_obj_add_str(
            doc, object, "declaration_identity",
            binding->value.declaration_identity ? binding->value.declaration_identity : "") &&
        yyjson_mut_obj_add_val(doc, object, "dependencies", dependencies);
    for (size_t i = 0; ok && i < binding->dependency_count; i++)
        ok = yyjson_mut_arr_add_str(doc, dependencies, binding->dependencies[i]);
    char *json = ok ? cbm_json_write_text(doc, cbm_json_writer_allocator(), NULL) : NULL;
    yyjson_mut_doc_free(doc);
    return json;
}

cbm_shared_resolver_t *cbm_shared_resolver_build(const cbm_pipeline_ctx_t *ctx,
                                                 const cbm_file_info_t *files, int file_count,
                                                 CBMFileResult **cache) {
    if (!cbm_shared_resolver_capture_sources(ctx, files, file_count, cache))
        return NULL;
    cbm_shared_resolver_t *resolver = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*resolver));
    if (!resolver)
        return NULL;
    cbm_arena_init(&resolver->arena);
    resolver->exports = cbm_ht_create(128);
    resolver->targets = cbm_ht_create(128);
    resolver->namespaces = cbm_ht_create(128);
    resolver->python_queries = cbm_ht_create(128);
    resolver->python_namespaces = cbm_ht_create(128);
    resolver->python_star_provided = cbm_ht_create(128);
    resolver->python_written = cbm_ht_create(128);
    resolver->python_parent_loaded = cbm_ht_create(128);
    resolver->python_nested = cbm_ht_create(128);
    resolver->python_members = cbm_py_members_new();
    resolver->http_defaults = cbm_ht_create(128);
    resolver->http_sites = cbm_ht_create(128);
    resolver->http_unsafe = cbm_ht_create(128);
    resolver->http_candidates = cbm_ht_create(128);
    resolver->http_veto = cbm_ht_create(128);
    resolver->http_domain_complete = true;
    resolver->project = cbm_arena_strdup(&resolver->arena, ctx->project_name);
    cbm_resolve_proof_pool_init_project(&resolver->proofs, &resolver->arena);
    resolver->solver = cbm_resolve_solver_new(&resolver->arena, &resolver->proofs,
                                              (cbm_resolve_solver_limits_t){0});
    resolver->go_types =
        cbm_go_stabilization_new(&resolver->arena, &resolver->proofs, resolver->solver);
    resolver->ecma = cbm_ecma_table_new(&resolver->arena);
    resolver->source_table = cbm_source_table_new();
    cbm_source_table_set_packages(resolver->source_table, cbm_pipeline_get_pkgmap());
    resolver->complete = true;
    if (!resolver->exports || !resolver->targets || !resolver->namespaces ||
        !resolver->python_queries || !resolver->python_namespaces ||
        !resolver->python_star_provided || !resolver->python_written ||
        !resolver->python_parent_loaded || !resolver->python_nested || !resolver->python_members ||
        !resolver->http_defaults || !resolver->http_sites || !resolver->http_unsafe ||
        !resolver->http_candidates || !resolver->http_veto || !resolver->project ||
        !resolver->solver || !resolver->go_types || !resolver->ecma || !resolver->source_table) {
        cbm_shared_resolver_free(resolver);
        return NULL;
    }
    if (!note_go_packages(resolver, ctx, files, file_count)) {
        cbm_shared_resolver_free(resolver);
        return NULL;
    }
    for (int f = 0; f < file_count; f++) {
        /* Embedded ECMAScript surfaces need their own complete use audit.
         * Their absence from this adapter cannot certify non-mutation. */
        if (files[f].language == CBM_LANG_HTML || files[f].language == CBM_LANG_VUE ||
            files[f].language == CBM_LANG_SVELTE || files[f].language == CBM_LANG_ASTRO)
            resolver->http_domain_complete = false;
        bool loaded = false;
        CBMFileResult *result = cbm_pipeline_result_acquire(ctx, cache, f, NULL, &loaded);
        if (!result) {
            resolver->http_domain_complete = false;
            resolver->complete = false;
            continue;
        }
        bool ok = true;
        for (int d = 0; ok && d < result->defs.count; d++) {
            const CBMDefinition *definition = &result->defs.items[d];
            /* Synthetic namespaces have their own file-keyed census; their
             * QNs may equal an initializer's genuine callable declaration. */
            bool namespace = files[f].language == CBM_LANG_PYTHON && definition->label &&
                             !strcmp(definition->label, "Module");
            ok =
                namespace || add_target(resolver, definition, files[f].rel_path, files[f].language);
            if (ok)
                ok = http_candidate_definition(resolver, definition, files[f].rel_path);
        }
        if (ok)
            ok = export_file(resolver, ctx, result, &files[f]);
        if (ok)
            ok = collect_go_types(resolver, ctx, result, &files[f]);
        if (ok)
            ok = collect_source_adapter(resolver, result, &files[f]);
        cbm_pipeline_result_release(result, loaded);
        if (!ok) {
            cbm_log_error("resolver.build.error", "file", files[f].rel_path, "reason",
                          "file_facts_failed");
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    if (!settle_python_imports(resolver) || !http_finalize_veto(resolver) ||
        !cbm_go_stabilization_register(resolver->go_types) || !stabilize_exports(resolver) ||
        !cbm_go_stabilization_seal(resolver->go_types) ||
        !prepare_python_queries(resolver, ctx, files, file_count, cache)) {
        cbm_log_error("resolver.build.error", "reason", "settle_failed");
        cbm_shared_resolver_free(resolver);
        return NULL;
    }
    cbm_py_members_env_t python_env = python_members_env(resolver);
    if (!cbm_py_members_seal(resolver->python_members, &python_env)) {
        cbm_shared_resolver_free(resolver);
        return NULL;
    }
    for (size_t i = 0; i < resolver->summary_count; i++) {
        const cbm_shared_resolver_export_summary_t *item = &resolver->summary[i];
        cbm_resolve_summary_t summary = {.file_path = item->file,
                                         .key = item->binding.key,
                                         .kind = item->binding.value.kind,
                                         .module = item->binding.value.module,
                                         .closed = item->binding.predecessors_closed,
                                         .proof_borrowed = item->binding.proof != NULL,
                                         .result = python_binding_outcome(&item->binding)};
        char *payload = solver_summary_payload(&item->binding);
        summary.payload_json = payload;
        bool written = payload && cbm_gbuf_record_resolver_summary(ctx->gbuf, &summary);
        cbm_free(CBM_MEM_CLASS_OTHER, payload);
        if (!written) {
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    for (size_t i = 0; i < cbm_resolve_solver_count(resolver->solver); i++) {
        const cbm_resolve_solver_result_t *binding = cbm_resolve_solver_at(resolver->solver, i);
        if (!cbm_go_stabilization_summary_key(binding->key))
            continue;
        const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(binding->proof);
        char *payload = solver_summary_payload(binding);
        cbm_resolve_summary_t summary = {.file_path = proof ? proof->source.file : "",
                                         .key = binding->key,
                                         .kind = binding->value.kind,
                                         .module = binding->value.module,
                                         .closed = binding->predecessors_closed,
                                         .proof_borrowed = binding->proof != NULL,
                                         .payload_json = payload,
                                         .result = python_binding_outcome(binding)};
        bool written = payload && cbm_gbuf_record_resolver_summary(ctx->gbuf, &summary);
        cbm_free(CBM_MEM_CLASS_OTHER, payload);
        if (!written) {
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    return resolver;
}

bool cbm_shared_resolver_http_candidate_receiver(const cbm_shared_resolver_t *resolver,
                                                 const char *file, const CBMCall *call) {
    if (!resolver || !file || !call || !call->callee_name)
        return false;
    const char *dot = strchr(call->callee_name, '.');
    size_t root_length = dot ? (size_t)(dot - call->callee_name) : strlen(call->callee_name);
    size_t file_length = strlen(file);
    if (root_length > INT_MAX || file_length > SIZE_MAX - root_length - 40)
        return true;
    size_t length = file_length + root_length + 40;
    char *key = cbm_alloc(CBM_MEM_CLASS_EXTRACT, length);
    if (!key)
        return true;
    snprintf(key, length, "%zu:%s:%.*s", file_length, file, (int)root_length, call->callee_name);
    const void *value = cbm_ht_get(resolver->http_veto, key);
    cbm_free(CBM_MEM_CLASS_EXTRACT, key);
    return value == resolver ||
           (value && cbm_service_pattern_match(call->callee_name) == CBM_SVC_HTTP);
}

bool cbm_shared_resolver_http_base(const cbm_shared_resolver_t *resolver, const char *file,
                                   const CBMFileResult *result, const CBMCall *call,
                                   const char **base) {
    *base = NULL;
    if (!resolver || !resolver->http_domain_complete || !file || !result || !call ||
        !call->callee_name || !call->first_string_arg || !call->enclosing_func_qn ||
        call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
        call->site_end_byte <= call->site_start_byte ||
        !result->source_content_id[CBM_SOURCE_ORIGIN_RAW][0] || !result->source_configuration_id[0])
        return false;
    size_t file_length = strlen(file), callee_length = strlen(call->callee_name);
    if (file_length > SIZE_MAX - callee_length - 80)
        return false;
    size_t length = file_length + callee_length + 80;
    char *key = cbm_alloc(CBM_MEM_CLASS_EXTRACT, length);
    if (!key)
        return false;
    snprintf(key, length, "%zu:%s:%u:%u:%s", strlen(file), file, call->site_start_byte,
             call->site_end_byte, call->callee_name);
    const http_import_site_t *site = cbm_ht_get(resolver->http_sites, key);
    cbm_free(CBM_MEM_CLASS_EXTRACT, key);
    if (!site || site->ambiguous || strcmp(site->file, file) ||
        strcmp(site->scope, call->enclosing_func_qn) || strcmp(site->callee, call->callee_name) ||
        strcmp(site->url, call->first_string_arg) || site->start != call->site_start_byte ||
        site->end != call->site_end_byte ||
        strcmp(site->content, result->source_content_id[CBM_SOURCE_ORIGIN_RAW]) ||
        strcmp(site->configuration, result->source_configuration_id))
        return false;
    const target_proof_t *caller = cbm_ht_get(resolver->targets, site->scope);
    if (!caller || caller->ambiguous || strcmp(caller->file, file) ||
        (strcmp(caller->label, "Function") && strcmp(caller->label, "Module")))
        return false;
    const http_default_t *value = cbm_ht_get(resolver->http_defaults, site->dependency);
    if (!value || value->ambiguous || cbm_ht_get(resolver->http_unsafe, value->file))
        return false;
    const target_proof_t *target = cbm_ht_get(resolver->targets, value->target);
    const target_proof_t *module = cbm_ht_get(resolver->targets, value->module);
    if (!target || !module || target->ambiguous || module->ambiguous ||
        strcmp(target->file, value->file) || strcmp(module->file, value->file) ||
        strcmp(target->label, "Variable") || strcmp(module->label, "Module"))
        return false;
    *base = value->base;
    return true;
}

size_t cbm_shared_resolver_export_count(const cbm_shared_resolver_t *resolver) {
    return resolver && resolver->complete ? resolver->summary_count : 0;
}

const cbm_shared_resolver_export_summary_t *cbm_shared_resolver_export_at(
    const cbm_shared_resolver_t *resolver, size_t index) {
    return index < cbm_shared_resolver_export_count(resolver) ? &resolver->summary[index] : NULL;
}

bool cbm_shared_resolver_exports_complete(const cbm_shared_resolver_t *resolver) {
    return resolver && resolver->complete && cbm_resolve_solver_complete(resolver->solver);
}

void cbm_shared_resolver_fact(const cbm_shared_resolver_t *resolver, const char *key,
                              cbm_resolve_solver_result_t *out) {
    cbm_resolve_solver_get(resolver ? resolver->solver : NULL, key, out);
}

const char *cbm_shared_resolver_python_base(const cbm_shared_resolver_t *resolver,
                                            const char *qualified_name, size_t offset) {
    return resolver ? cbm_py_members_base(resolver->python_members, qualified_name, offset) : NULL;
}

bool cbm_shared_resolver_owns_proof(const cbm_shared_resolver_t *resolver,
                                    const cbm_resolve_proof_t *proof) {
    return resolver && cbm_resolve_proof_pool_owns(&resolver->proofs, proof);
}

void cbm_shared_resolver_free(cbm_shared_resolver_t *resolver) {
    if (!resolver)
        return;
    cbm_ht_free(resolver->exports);
    cbm_ht_free(resolver->targets);
    cbm_ht_free(resolver->namespaces);
    cbm_ht_free(resolver->python_queries);
    cbm_ht_free(resolver->python_namespaces);
    cbm_ht_free(resolver->python_star_provided);
    cbm_ht_free(resolver->python_nested);
    cbm_py_members_free(resolver->python_members);
    cbm_ht_free(resolver->http_defaults);
    cbm_ht_free(resolver->http_sites);
    cbm_ht_free(resolver->http_unsafe);
    cbm_ht_free(resolver->http_candidates);
    cbm_ht_free(resolver->http_veto);
    cbm_go_stabilization_dispose(resolver->go_types);
    cbm_ecma_table_free(resolver->ecma);
    cbm_source_table_free(resolver->source_table);
    cbm_arena_destroy(&resolver->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, resolver);
}

static int compare_site(const void *left, const void *right) {
    const semantic_site_t *a = left, *b = right;
#define COMPARE_FIELD(field)  \
    if (a->field != b->field) \
    return a->field < b->field ? -1 : 1
    COMPARE_FIELD(start);
    COMPARE_FIELD(end);
    COMPARE_FIELD(origin);
    COMPARE_FIELD(kind);
#undef COMPARE_FIELD
    return strcmp(a->caller ? a->caller : "", b->caller ? b->caller : "");
}

static bool same_target(const CBMResolvedCall *a, const CBMResolvedCall *b) {
    return a->callee_qn && b->callee_qn && strcmp(a->callee_qn, b->callee_qn) == 0 &&
           strcmp(a->callee_sig ? a->callee_sig : "", b->callee_sig ? b->callee_sig : "") == 0;
}

static bool index_semantics(file_resolver_t *resolver) {
    const CBMResolvedCallArray *rows = &resolver->file->result->resolved_calls;
    if (rows->count <= 0)
        return true;
    if ((size_t)rows->count > SIZE_MAX / sizeof(*resolver->sites))
        return false;
    resolver->sites =
        cbm_calloc(CBM_MEM_CLASS_EXTRACT, (size_t)rows->count * sizeof(*resolver->sites));
    if (!resolver->sites)
        return false;
    for (int r = 0; r < rows->count; r++) {
        const CBMResolvedCall *row = &rows->items[r];
        /* Legacy strategy/confidence rows are not semantic authority. A
         * supported generic adapter may independently prove the occurrence. */
        uint32_t required = cbm_resolve_rule_required_facts(row->evidence.rule);
        if (row->site_end_byte <= row->site_start_byte ||
            row->source_origin > CBM_SOURCE_ORIGIN_PREPROCESSED || !row->callee_qn ||
            !row->callee_qn[0] || !required || !row->evidence.proof ||
            row->evidence.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            (row->evidence.facts & (required | CBM_RESOLVE_FACT_OCCURRENCE_EXACT)) !=
                (required | CBM_RESOLVE_FACT_OCCURRENCE_EXACT))
            continue;
        resolver->sites[resolver->site_count++] = (semantic_site_t){.start = row->site_start_byte,
                                                                    .end = row->site_end_byte,
                                                                    .origin = row->source_origin,
                                                                    .kind = row->kind,
                                                                    .caller = row->caller_qn,
                                                                    .row = row};
    }
    qsort(resolver->sites, resolver->site_count, sizeof(*resolver->sites), compare_site);
    size_t unique = 0;
    for (size_t r = 0; r < resolver->site_count; r++) {
        semantic_site_t *site = &resolver->sites[r];
        if (unique && compare_site(site, &resolver->sites[unique - 1]) == 0) {
            semantic_site_t *prior = &resolver->sites[unique - 1];
            prior->ambiguous |= !same_target(prior->row, site->row);
            /* Both rows now carry independently admissible evidence. Choose
             * a stable representation of an equal target, not the last row
             * left by an unstable sort. Preserve proved class-callability,
             * which projection needs in addition to the rule's base facts.
             * Conflicting targets still abstain. */
            uint32_t prior_class = prior->row->evidence.facts & CBM_RESOLVE_FACT_CLASS_CALLABLE;
            uint32_t next_class = site->row->evidence.facts & CBM_RESOLVE_FACT_CLASS_CALLABLE;
            if (!prior->ambiguous && (next_class > prior_class ||
                                      (next_class == prior_class &&
                                       (site->row->evidence.rule < prior->row->evidence.rule ||
                                        (site->row->evidence.rule == prior->row->evidence.rule &&
                                         site->row->evidence.facts < prior->row->evidence.facts)))))
                prior->row = site->row;
        } else {
            resolver->sites[unique++] = *site;
        }
    }
    resolver->site_count = unique;
    return true;
}

static const semantic_site_t *find_semantic(const file_resolver_t *resolver, const char *scope,
                                            uint32_t start, uint32_t end, CBMSourceOrigin origin,
                                            CBMResolvedKind kind) {
    if (end <= start || !resolver->site_count)
        return NULL;
    semantic_site_t key = {
        .start = start, .end = end, .origin = origin, .kind = kind, .caller = scope};
    return bsearch(&key, resolver->sites, resolver->site_count, sizeof(*resolver->sites),
                   compare_site);
}

static cbm_resolve_result_t unresolved(const char *reason) {
    return (cbm_resolve_result_t){.reason = reason};
}

static cbm_resolve_result_t semantic_result(const file_resolver_t *resolver,
                                            const semantic_site_t *site, CBMArena *arena) {
    if (resolver->file->result->lsp_failed)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED,
                                      .reason = "lsp_analysis_failed_prior_proof_untrusted"};
    if (site->ambiguous)
        return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .candidate_count = 2,
                                      .reason = "conflicting_semantic_targets"};
    const CBMResolvedCall *row = site->row;
    const char *target = row->callee_qn;
    if (target && row->callee_sig && row->callee_sig[0]) {
        size_t base = strlen(target), suffix = strlen(row->callee_sig);
        if (base > SIZE_MAX - suffix - 1)
            return unresolved("target_identity_too_long");
        char *joined = cbm_arena_alloc(arena, base + suffix + 1);
        if (!joined)
            return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED,
                                          .reason = "target_identity_allocation_failed"};
        memcpy(joined, target, base);
        memcpy(joined + base, row->callee_sig, suffix + 1);
        target = joined;
    }
    cbm_resolve_result_t result = {.binding = target ? CBM_RESOLVE_BINDING_UNIQUE
                                                     : CBM_RESOLVE_BINDING_UNRESOLVED,
                                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = row->evidence.analysis,
                                   .rule = row->evidence.rule,
                                   .facts = row->evidence.facts,
                                   .target = target,
                                   .candidate_count = target ? 1 : 0,
                                   .reason = "semantic_binding_evidence",
                                   .proof = row->evidence.proof};
    if (!target || !cbm_gbuf_find_by_qn(resolver->file->graph, target)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "semantic_target_not_materialized";
    }
    return result;
}

static bool export_complete(const file_resolver_t *resolver, const char *target) {
    if (!resolver->shared || !resolver->shared->complete || !target)
        return false;
    const export_proof_t *proof = cbm_ht_get(resolver->shared->exports, target);
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, target);
    return proof && proof->supported && !proof->ambiguous && node && node->file_path &&
           strcmp(proof->file, node->file_path) == 0;
}

static bool prepare_python_imports(file_resolver_t *resolver) {
    resolver->python_imports =
        python_import_map(resolver->arena, resolver->file->rel_path, resolver->file->result);
    return resolver->python_imports != NULL;
}

static cbm_resolve_result_t python_export_result(file_resolver_t *resolver, const char *slot,
                                                 const char *suffix) {
    if (!slot || !resolver->shared || !resolver->shared->complete)
        return unresolved("python_import_namespace_unavailable");
    cbm_resolve_solver_result_t binding;
    cbm_resolve_solver_get(resolver->shared->solver, slot, &binding);
    if (binding.status == CBM_SOLVER_KNOWN && binding.value.module) {
        if (!suffix || !suffix[0] || strchr(suffix, '.'))
            return unresolved("python_module_member_path_unsupported");
        const char *key = python_query_key(resolver->arena, slot, suffix);
        const python_query_t *query =
            key ? cbm_ht_get(resolver->shared->python_queries, key) : NULL;
        if (!query)
            return unresolved("python_module_member_query_unavailable");
        binding = query->result;
    } else if (suffix && suffix[0]) {
        return unresolved("python_export_member_dispatch_unproven");
    }
    cbm_resolve_result_t outcome = python_binding_outcome(&binding);
    if (binding.status != CBM_SOLVER_KNOWN || binding.value.module)
        return outcome;
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, binding.value.target);
    if (!node || !target_materialized(resolver, node))
        return unresolved("python_export_declaration_not_materialized");
    outcome.rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
    outcome.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                    CBM_RESOLVE_FACT_IMPORT_BOUND;
    if ((binding.value.kind & CBM_RESOLVE_EXPORT_TYPE) && node->label &&
        !strcmp(node->label, "Class"))
        outcome.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
    return outcome;
}

/* A name that no scope binds, provided by a closed star import of this module. */
static bool python_star_result(file_resolver_t *resolver, const char *name,
                               cbm_resolve_result_t *result) {
    if (!resolver->shared || !resolver->shared->python_star_provided ||
        !resolver->file->result->module_qn || strchr(name, '.'))
        return false;
    const char *module = resolver->file->result->module_qn;
    const char *key = cbm_arena_sprintf(resolver->arena, "%.*s.%s",
                                        (int)cbm_fqn_symbol_scope_len(module), module, name);
    if (!key || !cbm_ht_get(resolver->shared->python_star_provided, key))
        return false;
    *result = python_export_result(resolver, export_solver_key(resolver->arena, key), NULL);
    return true;
}

static bool python_import_result(file_resolver_t *resolver, const char *name, uint32_t start,
                                 uint32_t end, CBMSourceOrigin origin,
                                 cbm_resolve_result_t *result) {
    if (!resolver->python_imports)
        return false;
    const cbm_resolve_python_nested_import_t *nested =
        cbm_resolve_binding_python_nested_site(resolver->bindings, name, start, end, origin);
    if (nested) {
        const char *suffix = NULL;
        *result =
            python_nested_suffix(nested->import.local_name, nested->import.module_path,
                                 nested->import.kind, name, &suffix)
                ? python_export_result(resolver,
                                       python_nested_key(resolver->arena, resolver->file->rel_path,
                                                         nested->at, nested->import.local_name),
                                       suffix)
                : unresolved("python_nested_import_path_mismatch");
        return true;
    }
    size_t length = strlen(name);
    while (length) {
        const char *prefix = cbm_arena_strndup(resolver->arena, name, length);
        if (!prefix) {
            *result = (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED,
                                             .reason = "python_import_key_allocation"};
            return true;
        }
        const python_import_t *import = cbm_ht_get(resolver->python_imports, prefix);
        if (import) {
            *result = import->ambiguous
                          ? unresolved("python_import_binding_conflict")
                          : python_export_result(resolver, import->slot,
                                                 name[length] ? name + length + 1 : NULL);
            return true;
        }
        while (length && name[--length] != '.') {}
    }
    return false;
}

/* An explicit module binder a later closed star overrides: only a binder after
 * the star stays explicit, see python_expand_module_stars. */
static bool python_star_rebinds(const file_resolver_t *resolver, const char *name) {
    const char *module = resolver->file->result->module_qn;
    if (!resolver->shared || !module || strchr(name, '.'))
        return false;
    const char *key = cbm_arena_sprintf(resolver->arena, "%.*s.%s",
                                        (int)cbm_fqn_symbol_scope_len(module), module, name);
    return key && cbm_ht_get(resolver->shared->python_star_provided, key);
}

static bool prepare_source_imports(file_resolver_t *resolver) {
    if (resolver->file->language == CBM_LANG_PYTHON)
        return prepare_python_imports(resolver);
    if (resolver->file->language != CBM_LANG_DART)
        return true;
    resolver->import_names = cbm_ht_create(32);
    if (!resolver->import_names)
        return false;
    resolver->import_count = cbm_resolve_binding_import_count(resolver->bindings);
    if (!resolver->import_count)
        return true;
    resolver->imports =
        cbm_arena_calloc(resolver->arena, resolver->import_count * sizeof(*resolver->imports));
    if (!resolver->imports)
        return false;
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(resolver->bindings);
    if (!context)
        return false;
    const CBMImportArray *extracted = &resolver->file->result->imports;
    size_t extracted_at = 0;
    for (size_t i = 0; i < resolver->import_count; i++) {
        source_import_t *import = &resolver->imports[i];
        import->source = cbm_resolve_binding_import_at(resolver->bindings, i);
        const char *uri = import->source ? import->source->uri : NULL;
        cbm_resolve_lookup_t query;
        cbm_resolve_lookup_begin(&query, context, "dart.import_path", uri ? uri : "missing_uri", 0,
                                 0, CBM_SOURCE_ORIGIN_RAW);
        cbm_resolve_result_t outcome = unresolved("dart_import_uri_unavailable");
        outcome.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN;
        import->system = uri && strncmp(uri, "dart:", 5) == 0;
        if (!uri || import->system || strchr(uri, ':') || uri[0] == '/') {
            outcome.analysis =
                import->system ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
            outcome.reason =
                import->system ? "dart_system_import_classified" : "dart_import_uri_unsupported";
            (void)cbm_resolve_lookup_observe(
                &query, "dart.import_uri_kind", uri ? uri : "missing_uri", uri,
                uri ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
                uri ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN, 0, uri != NULL,
                outcome.reason);
        } else {
            const char *path =
                uri[0] == '.' ? uri : cbm_arena_sprintf(resolver->arena, "./%s", uri);
            if (!path)
                return false;
            /* Both source walks are ordered. Match this exact token once;
             * another valid import of the same URI cannot lend its authority
             * to a conditional, deferred or escaped occurrence. */
            while (extracted_at < (size_t)extracted->count &&
                   extracted->items[extracted_at].site_start_byte < import->source->uri_start_byte)
                extracted_at++;
            const CBMImport *captured =
                extracted_at < (size_t)extracted->count ? &extracted->items[extracted_at] : NULL;
            bool audited = captured &&
                           captured->site_start_byte == import->source->uri_start_byte &&
                           captured->site_end_byte == import->source->uri_end_byte &&
                           captured->site_end_byte > captured->site_start_byte &&
                           captured->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                           captured->kind == CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH &&
                           captured->module_path && !strcmp(captured->module_path, uri);
            CBMImport request = {.module_path = path,
                                 .kind = audited ? CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH
                                                 : CBM_IMPORT_KIND_DEFAULT};
            if (captured && captured->site_start_byte == import->source->uri_start_byte)
                extracted_at++;
            const cbm_gbuf_node_t *target = NULL;
            outcome = cbm_pipeline_resolve_import_result(
                resolver->file->ctx, resolver->file->rel_path,
                resolver->file_node ? resolver->file_node->qualified_name : NULL, &request, NULL,
                &target);
            /* Observe the path adapter's result before the admission check.
             * Failed/unsupported paths remain open competitors for names the
             * importing source exposes; no directory membership is inferred. */
            bool closed = outcome.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
                          (outcome.facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE);
            (void)cbm_resolve_lookup_observe(&query, "dart.import_paths", path, outcome.target,
                                             outcome.binding, outcome.analysis, outcome.facts,
                                             closed, outcome.reason);
            if (cbm_resolve_admit(&outcome) && target && target->file_path && resolver->shared) {
                import->file = target->file_path;
                import->namespace = cbm_ht_get(resolver->shared->namespaces, import->file);
                if (import->namespace &&
                    !cbm_resolve_lookup_context_init(&import->namespace_context, context->pool,
                                                     import->namespace->source))
                    return false;
            }
        }
        import->path_proof = cbm_resolve_lookup_finish(&query, CBM_PROOF_IMPORT_BINDING, outcome);
        if (!import->path_proof)
            return false;
    }
    return true;
}

static bool import_exposes(const cbm_resolve_source_import_t *import, const char *name) {
    if (!import || !name || name[0] == '_')
        return false;
    bool shown = !import->has_show;
    for (size_t i = 0; i < import->show_count; i++)
        if (strcmp(import->show[i], name) == 0)
            shown = true;
    for (size_t i = 0; i < import->hide_count; i++)
        if (strcmp(import->hide[i], name) == 0)
            return false;
    return shown;
}

static cbm_resolve_result_t dart_query_finish(cbm_resolve_lookup_t *query,
                                              cbm_resolve_result_t result,
                                              cbm_resolve_result_t observed) {
    result.proof = cbm_resolve_lookup_finish(query, CBM_PROOF_REGISTRY_LOOKUP, observed);
    if (!result.proof) {
        result.analysis =
            query->context && query->context->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                ? CBM_RESOLVE_ANALYSIS_LIMITED
                : CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "dart_lookup_proof_unavailable";
    } else {
        cbm_resolve_analysis_t analysis = cbm_resolve_proof_view(result.proof)->outcome.analysis;
        if (analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
            result.analysis = analysis;
            result.facts &= ~CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
        }
    }
    return result;
}

/* A bounded namespace lookup carries its declaring file's captured source.
 * These roots contain only source/audit leaves, never shared export chains. */
static const cbm_resolve_proof_t *dart_namespace_member(source_import_t *import, const char *name,
                                                        const char *qn,
                                                        const export_proof_t *export,
                                                        bool materialized) {
    cbm_resolve_lookup_t query;
    cbm_resolve_lookup_begin(&query, &import->namespace_context, import->namespace->module_qn, name,
                             0, 0, CBM_SOURCE_ORIGIN_RAW);
    bool complete = import->namespace->complete && !import->namespace->ambiguous;
    (void)cbm_resolve_lookup_observe(
        &query, "dart.namespace_audit", import->file, import->namespace->module_qn,
        import->namespace->ambiguous ? CBM_RESOLVE_BINDING_AMBIGUOUS : CBM_RESOLVE_BINDING_UNIQUE,
        complete ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN, 0, complete,
        "captured_library_namespace_audit");
    cbm_resolve_result_t observed = {.analysis = complete ? CBM_RESOLVE_ANALYSIS_COMPLETE
                                                          : CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                     .reason = "dart_namespace_member_absent"};
    if (complete) {
        bool identity =
            export && !export->ambiguous && import->file && strcmp(export->file, import->file) == 0;
        (void)cbm_resolve_lookup_observe(
            &query, import->namespace->module_qn, name, export ? qn : NULL,
            export && export->ambiguous ? CBM_RESOLVE_BINDING_AMBIGUOUS
            : export                    ? CBM_RESOLVE_BINDING_UNIQUE
                                        : CBM_RESOLVE_BINDING_UNRESOLVED,
            CBM_RESOLVE_ANALYSIS_COMPLETE, export ? export->kind : 0, true,
            export ? "dart_namespace_member_observed" : "dart_namespace_member_absent");
        if (export) {
            bool supported = identity && export->supported && materialized;
            (void)cbm_resolve_lookup_observe(
                &query, "dart.export_identity_audit", qn,
                supported ? "supported_materialized_export" : "export_identity_unproven",
                CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, export->kind, true,
                supported ? "source_export_matches_graph" : "source_export_not_admissible");
            observed =
                (cbm_resolve_result_t){.binding = export->ambiguous ? CBM_RESOLVE_BINDING_AMBIGUOUS
                                                                    : CBM_RESOLVE_BINDING_UNIQUE,
                                       .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                       .analysis = supported ? CBM_RESOLVE_ANALYSIS_COMPLETE
                                                             : CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                       .target = qn,
                                       .candidate_count = export->ambiguous ? 2 : 1,
                                       .reason = supported ? "dart_namespace_member_supported"
                                                           : "dart_namespace_member_unsupported"};
        }
    }
    return cbm_resolve_lookup_finish(&query, CBM_PROOF_REGISTRY_LOOKUP, observed);
}

static cbm_resolve_result_t dart_imported_name(file_resolver_t *resolver, const char *name,
                                               cbm_resolve_lookup_t *query) {
    cbm_resolve_result_t result = unresolved("dart_import_binding_missing");
    result.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    const char *target = NULL;
    bool incomplete = !resolver->shared || !resolver->shared->complete;
    bool unsupported = false;
    uint32_t candidates = 0;
    (void)cbm_resolve_lookup_observe(
        query, "dart.shared_exports", name, incomplete ? NULL : "source_exports_available",
        incomplete ? CBM_RESOLVE_BINDING_UNRESOLVED : CBM_RESOLVE_BINDING_UNIQUE,
        incomplete ? CBM_RESOLVE_ANALYSIS_UNKNOWN : CBM_RESOLVE_ANALYSIS_COMPLETE, 0, !incomplete,
        "shared_export_snapshot_status");
    /* This subset resolves unprefixed library names. Qualified selectors need
     * their own occurrence extraction and do not become lexical identifiers. */
    if (strchr(name, '.')) {
        result = unresolved("dart_prefix_occurrence_required");
        return dart_query_finish(query, result, result);
    }
    for (size_t i = 0; i < resolver->import_count; i++) {
        if (++resolver->import_work > 65536) {
            query->closed = false;
            result.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
            result.reason = "dart_import_domain_work_limit";
            return dart_query_finish(query, result, result);
        }
        source_import_t *import = &resolver->imports[i];
        const cbm_resolve_source_import_t *source = import->source;
        bool exposed = source && !source->prefix && import_exposes(source, name);
        const char *key =
            cbm_arena_sprintf(resolver->arena, "%zu:%s:%s", i,
                              source && source->uri ? source->uri : "missing_uri", name);
        if (!key) {
            query->failed = true;
            return dart_query_finish(query, result, result);
        }
        const char *visibility = !source          ? "dart_import_source_missing"
                                 : source->prefix ? "dart_prefix_excludes_unqualified_name"
                                 : !exposed       ? "dart_source_combinators_exclude_name"
                                 : import->system ? "dart_system_name_shadowed_by_project"
                                                  : "dart_unprefixed_name_exposed";
        uint32_t flags = source ? (source->prefix ? 1u : 0u) | (source->has_show ? 2u : 0u) |
                                      (source->hide_count ? 4u : 0u) | (import->system ? 8u : 0u)
                                : 0;
        (void)cbm_resolve_lookup_observe(
            query, "dart.import_visibility", key, source ? source->uri : NULL,
            source ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
            source ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN, flags,
            source != NULL, visibility);
        if (!source) {
            incomplete = true;
            continue;
        }
        /* Exclusions are proved from importing source; unrelated failed path
         * lookups cannot enter this name's applicable dependency set. Dart
         * non-system declarations shadow system declarations (§19.4). */
        if (!exposed || import->system)
            continue;
        (void)cbm_resolve_lookup_support(query, import->path_proof);
        if (!import->namespace) {
            (void)cbm_resolve_lookup_observe(
                query, "dart.source_namespaces", import->file ? import->file : source->uri, NULL,
                CBM_RESOLVE_BINDING_UNRESOLVED, CBM_RESOLVE_ANALYSIS_UNKNOWN, 0, false,
                "dart_imported_source_namespace_unavailable");
            incomplete = true;
            continue;
        }
        const char *qn =
            cbm_arena_sprintf(resolver->arena, "%s.%s", import->namespace->module_qn, name);
        if (!qn) {
            query->failed = true;
            return dart_query_finish(query, result, result);
        }
        bool complete = import->namespace->complete && !import->namespace->ambiguous;
        const export_proof_t *export = complete ? cbm_ht_get(resolver->shared->exports, qn) : NULL;
        bool materialized = export && export_complete(resolver, qn);
        (void)cbm_resolve_lookup_support(
            query, dart_namespace_member(import, name, qn, export, materialized));
        if (!complete) {
            incomplete = true;
            continue;
        }
        if (!export)
            continue;
        if (export->ambiguous || !import->file || strcmp(export->file, import->file) != 0) {
            incomplete = true;
            continue;
        }
        if (!target || strcmp(target, qn) != 0) {
            candidates++;
            target = qn;
        }
        unsupported |= !export->supported || !materialized;
    }
    if (candidates > 1) {
        result.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        result.candidate_count = candidates;
        result.reason = "dart_import_name_ambiguous";
    } else if (incomplete || unsupported) {
        result.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        result.reason =
            incomplete ? "dart_import_namespace_incomplete" : "dart_import_member_unsupported";
        result.candidate_count = candidates;
    } else if (target) {
        result = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                        .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                        .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                                 CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                                 CBM_RESOLVE_FACT_IMPORT_BOUND,
                                        .target = target,
                                        .candidate_count = 1,
                                        .reason = "dart_complete_import_namespace"};
    }
    return dart_query_finish(query, result, result);
}

static cbm_resolve_result_t dart_name_result(file_resolver_t *resolver, const char *name) {
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(resolver->bindings);
    cbm_resolve_lookup_t query;
    cbm_resolve_lookup_begin(&query, context, "dart.source_name", name, 0, 0,
                             CBM_SOURCE_ORIGIN_RAW);
    bool complete = cbm_resolve_binding_namespace_complete(resolver->bindings);
    (void)cbm_resolve_lookup_observe(
        &query, "dart.local_namespace_audit", name, complete ? "source_namespace_complete" : NULL,
        complete ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        complete ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN, 0, complete,
        cbm_resolve_binding_status(resolver->bindings));
    if (!complete) {
        cbm_resolve_result_t result = unresolved(cbm_resolve_binding_status(resolver->bindings));
        return dart_query_finish(&query, result, result);
    }
    /* Any local spelling, including an unsupported binder, takes precedence.
     * A missing/ambiguous local candidate must not fall through to imports. */
    bool present = cbm_resolve_binding_name_present(resolver->bindings, name);
    (void)cbm_resolve_lookup_observe(
        &query, resolver->file->module_qn, name, present ? name : NULL,
        present ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
        present ? "dart_local_spelling_present" : "dart_local_spelling_absent");
    if (present) {
        cbm_resolution_t local =
            cbm_registry_resolve_observed(resolver->file->registry, name, resolver->file->module_qn,
                                          NULL, NULL, 0, CBM_LANG_DART, NULL, false, context);
        (void)cbm_resolve_lookup_support(&query, local.proof.proof);
        const cbm_resolve_proof_view_t *observed = cbm_resolve_proof_view(local.proof.proof);
        return dart_query_finish(&query, local.proof, observed ? observed->outcome : local.proof);
    }
    cbm_resolve_result_t *cached = cbm_ht_get(resolver->import_names, name);
    if (cached)
        return *cached;
    cached = cbm_arena_alloc(resolver->arena, sizeof(*cached));
    char *key = cbm_arena_strdup(resolver->arena, name);
    if (!cached || !key) {
        query.failed = true;
        cbm_resolve_result_t result = unresolved("dart_import_cache_allocation");
        return dart_query_finish(&query, result, result);
    }
    *cached = dart_imported_name(resolver, name, &query);
    cbm_ht_set(resolver->import_names, key, cached);
    if (cbm_ht_get(resolver->import_names, key) != cached) {
        query.failed = true;
        return dart_query_finish(&query, *cached, *cached);
    }
    return *cached;
}

/* The repository file a specifier names, once per specifier per file. */
static const char *ecma_dependency(file_resolver_t *resolver, const char *source) {
    if (!resolver->ecma_dependencies) {
        resolver->ecma_dependencies = cbm_ht_create(16);
        if (!resolver->ecma_dependencies)
            return NULL;
    }
    static const char none[] = "";
    const char *known = cbm_ht_get(resolver->ecma_dependencies, source);
    if (known)
        return known == none ? NULL : known;
    const char *file =
        cbm_ecma_dependency(resolver->file->ctx, resolver->file->rel_path, source, resolver->arena);
    char *key = cbm_arena_strdup(resolver->arena, source);
    if (key)
        cbm_ht_set(resolver->ecma_dependencies, key, (void *)(file ? file : none));
    return file;
}

/* The value a module binding holds, by the exporting modules' own surfaces. */
static cbm_ecma_value_t ecma_import_value(const file_resolver_t *resolver,
                                          const cbm_resolve_js_import_t *import, const char *file,
                                          bool type_space, cbm_resolve_lookup_t *query) {
    const cbm_ecma_table_t *table = resolver->shared->ecma;
    if (import->is_default)
        return cbm_ecma_default(table, file, query);
    if (import->member && type_space)
        return cbm_ecma_lookup_type(table, file, import->member, query);
    if (import->member)
        return cbm_ecma_lookup(table, file, import->member, query);
    if (import->is_require)
        return cbm_ecma_whole(table, file, query);
    return (cbm_ecma_value_t){.status = CBM_ECMA_NAMESPACE, .file = file};
}

/* Follow `a.b.c` from the imported root: a namespace answers by its exports, a
 * declaration only by an exact member declared in the same file. */
static const char *ecma_member_target(file_resolver_t *resolver, const char *owner,
                                      const char *member) {
    const char *qn = cbm_arena_sprintf(resolver->arena, "%s.%s", owner, member);
    const cbm_gbuf_node_t *node = qn ? cbm_gbuf_find_by_qn(resolver->file->graph, qn) : NULL;
    const cbm_gbuf_node_t *base = cbm_gbuf_find_by_qn(resolver->file->graph, owner);
    return node && base && node->file_path && base->file_path &&
                   !strcmp(node->file_path, base->file_path)
               ? qn
               : NULL;
}

static cbm_resolve_result_t ecma_finish(cbm_resolve_lookup_t *query, cbm_resolve_result_t result) {
    cbm_resolve_result_t symbolic = result;
    result.proof = cbm_resolve_lookup_finish(query, CBM_PROOF_REGISTRY_LOOKUP, symbolic);
    if (query->context && !result.proof) {
        result.analysis = query->context->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                              ? CBM_RESOLVE_ANALYSIS_LIMITED
                              : CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "ecma_import_proof_unavailable";
    }
    return result;
}

static cbm_resolve_result_t ecma_import_result(file_resolver_t *resolver,
                                               const cbm_resolve_js_import_t *import,
                                               const char *name, bool invocation,
                                               bool type_position) {
    cbm_resolve_lookup_t query;
    cbm_resolve_lookup_begin(&query, cbm_resolve_binding_lookup_context(resolver->bindings),
                             "ecma.import_binding", name, 0, 0, CBM_SOURCE_ORIGIN_RAW);
    cbm_resolve_result_t result = unresolved("import_target_unresolved");
    const char *file = import->source && resolver->shared && resolver->shared->complete
                           ? ecma_dependency(resolver, import->source)
                           : NULL;
    (void)cbm_resolve_lookup_observe(
        &query, "ecma.import_paths", import->source ? import->source : "unknown_specifier", file,
        file ? CBM_RESOLVE_BINDING_UNIQUE : CBM_RESOLVE_BINDING_UNRESOLVED,
        file ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN, 0, file != NULL,
        file ? "ecma_import_path_observed" : "ecma_import_path_open");
    if (!file)
        return ecma_finish(&query, result);
    const char *rest = strchr(name, '.');
    cbm_ecma_value_t value =
        ecma_import_value(resolver, import, file, type_position && !rest, &query);
    const char *target = value.status == CBM_ECMA_DECLARATION ? value.target : NULL;
    if (target && value.member)
        target = ecma_member_target(resolver, target, value.member);
    /* `exports = module.exports = createApp; exports.named = named`: the required value
     * is one object, so the module's own names answer before createApp's members. */
    if (target && rest && import->is_require && !import->member) {
        const char *segment = rest + 1;
        rest = strchr(segment, '.');
        const char *member = cbm_arena_strndup(resolver->arena, segment,
                                               rest ? (size_t)(rest - segment) : strlen(segment));
        if (!member) {
            query.failed = true;
            return ecma_finish(&query, result);
        }
        value = cbm_ecma_lookup(resolver->shared->ecma, file, member, &query);
        target = value.status == CBM_ECMA_DECLARATION ? value.target : NULL;
        if (target && value.member)
            target = ecma_member_target(resolver, target, value.member);
    }
    while (rest && (value.status == CBM_ECMA_NAMESPACE || target)) {
        const char *segment = rest + 1;
        rest = strchr(segment, '.');
        if (target) {
            /* A class or namespace declares the rest of the path, or nothing does. */
            target = ecma_member_target(resolver, target, segment);
            if (target && rest) {
                target = ecma_member_target(resolver, target, rest + 1);
                rest = NULL;
            }
            rest = NULL;
            break;
        }
        const char *member = cbm_arena_strndup(resolver->arena, segment,
                                               rest ? (size_t)(rest - segment) : strlen(segment));
        if (!member) {
            query.failed = true;
            return ecma_finish(&query, result);
        }
        value = type_position && !rest
                    ? cbm_ecma_lookup_type(resolver->shared->ecma, value.file, member, &query)
                    : cbm_ecma_lookup(resolver->shared->ecma, value.file, member, &query);
        target = value.status == CBM_ECMA_DECLARATION ? value.target : NULL;
        if (target && value.member)
            target = ecma_member_target(resolver, target, value.member);
    }
    if (value.status == CBM_ECMA_AMBIGUOUS) {
        result = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                        .candidate_count = 2,
                                        .reason = "ecma_export_ambiguous"};
    } else if (target && !rest) {
        const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, target);
        uint32_t facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                         CBM_RESOLVE_FACT_IMPORT_BOUND;
        if (invocation && node && node->label && !strcmp(node->label, "Class")) {
            /* `new X()` runs X's own constructor when X declares one. */
            const char *constructor = ecma_member_target(resolver, target, "constructor");
            if (constructor)
                target = constructor;
            else
                facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
        }
        result = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                        .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                        .facts = facts,
                                        .target = target,
                                        .candidate_count = 1,
                                        .reason = "ecma_export_binding"};
    } else if (value.status == CBM_ECMA_DECLARATION || value.status == CBM_ECMA_ABSENT) {
        result.reason = "import_member_unresolved";
    } else if (value.status == CBM_ECMA_NAMESPACE) {
        result.reason = "ecma_namespace_value";
    } else {
        result.reason = "ecma_export_unproven";
    }
    return ecma_finish(&query, result);
}

static bool python_member_result(file_resolver_t *resolver, const char *name, uint32_t start,
                                 uint32_t end, CBMSourceOrigin origin, bool invocation,
                                 cbm_resolve_result_t *out) {
    const cbm_shared_resolver_t *shared = resolver->shared;
    if (!shared || !shared->complete || !shared->python_members || !resolver->python_imports)
        return false;
    python_member_imports_t lookup = {.file = resolver->file->rel_path,
                                      .module = resolver->file->result->module_qn,
                                      .imports = resolver->python_imports,
                                      .star_provided = shared->python_star_provided};
    cbm_py_import_lookup_t table = {&lookup, python_member_import_key, python_member_star_key};
    cbm_py_members_env_t env = python_members_env(shared);
    return cbm_py_members_resolve(shared->python_members, &env, &table, resolver->bindings,
                                  resolver->arena, name, start, end, origin, invocation, out);
}

static bool python_target_is_variable(const file_resolver_t *resolver, const char *qualified_name) {
    const target_proof_t *target =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, qualified_name) : NULL;
    return target && target->label && !strcmp(target->label, "Variable");
}

static bool callable_node(const cbm_gbuf_node_t *node);

/* An adapter's decision for this exact occurrence, never its spelling. */
static cbm_resolve_result_t source_adapter_result(file_resolver_t *resolver, const char *name,
                                                  uint32_t start, uint32_t end,
                                                  CBMSourceOrigin origin, bool invocation) {
    CBMLanguage language = resolver->file->language;
    if (!invocation && !cbm_source_adapter_decides_references(language))
        return unresolved("source_adapter_reference_unsupported");
    cbm_source_outcome_t outcome;
    cbm_source_unit_site(resolver->source_unit, start, end,
                         invocation ? CBM_SOURCE_SITE_CALL : CBM_SOURCE_SITE_REFERENCE, &outcome);
    if (!outcome.target)
        return unresolved(outcome.reason);
    return cbm_resolve_binding_source_site(
        resolver->source_bindings ? resolver->source_bindings : resolver->bindings, name, start,
        end, origin, outcome.domain, outcome.target, outcome.rule, outcome.facts, outcome.reason);
}

static cbm_resolve_result_t generic_result(file_resolver_t *resolver, const char *name,
                                           const char *scope, uint32_t start, uint32_t end,
                                           CBMSourceOrigin origin, bool invocation) {
    const cbm_resolver_file_t *file = resolver->file;
    if (!name || !name[0])
        return unresolved("reference_spelling_missing");
    if (ecma_language(file->language) || file->language == CBM_LANG_VUE ||
        file->language == CBM_LANG_SVELTE) {
        cbm_resolve_result_t local =
            cbm_resolve_binding_local(resolver->bindings, name, start, end, origin);
        if (cbm_resolve_admit(&local))
            return local;
        const char *path = name;
        bool type_position = false;
        const cbm_resolve_js_import_t *import = cbm_resolve_binding_js_import(
            resolver->bindings, name, start, end, origin, &path, &type_position);
        cbm_resolve_result_t member =
            cbm_resolve_binding_js_member(resolver->bindings, name, start, end, origin);
        if (cbm_resolve_admit(&member))
            return member;
        if (import) {
            cbm_resolve_result_t proof =
                ecma_import_result(resolver, import, path, invocation, type_position);
            return proof.target ? cbm_resolve_binding_validate(resolver->bindings, name, start, end,
                                                               origin, proof)
                                : proof;
        }
    }
    /* The C adapter's semantic rows are the whole translation-unit lookup. With no
     * row there is no second, name-based way to arrive at a target. */
    if (file->language == CBM_LANG_C || file->language == CBM_LANG_CPP ||
        file->language == CBM_LANG_CUDA)
        return unresolved("c_translation_unit_binding_unproven");
    if (cbm_source_adapter_supported(file->language) &&
        !cbm_source_adapter_has_binding_rules(file->language))
        return source_adapter_result(resolver, name, start, end, origin, invocation);
    if (file->language == CBM_LANG_DART) {
        cbm_resolve_result_t proof = dart_name_result(resolver, name);
        proof = cbm_resolve_binding_validate(resolver->bindings, name, start, end, origin, proof);
        /* The older rules keep what they prove, unless that is a class they cannot
         * call, which is the adapter's constructor rule to decide. */
        const cbm_gbuf_node_t *selected =
            proof.target ? cbm_gbuf_find_by_qn(file->graph, proof.target) : NULL;
        if (cbm_resolve_admit(&proof) && (!invocation || callable_node(selected)))
            return proof;
        cbm_resolve_result_t decided =
            source_adapter_result(resolver, name, start, end, origin, invocation);
        if (decided.target)
            return decided;
        /* Both abstained: the occurrence's own reason says more than the file's. */
        if (invocation)
            proof.reason = decided.reason;
        return proof;
    }
    if (file->language == CBM_LANG_PYTHON) {
        cbm_resolve_result_t proof =
            cbm_resolve_binding_local(resolver->bindings, name, start, end, origin);
        if (cbm_resolve_admit(&proof)) {
            cbm_resolve_result_t held;
            if (invocation && python_target_is_variable(resolver, proof.target) &&
                python_member_result(resolver, name, start, end, origin, invocation, &held))
                return held;
            return python_star_rebinds(resolver, name) ? unresolved("python_star_rebinding")
                                                       : proof;
        }
        if (python_import_result(resolver, name, start, end, origin, &proof) ||
            python_star_result(resolver, name, &proof)) {
            cbm_resolve_result_t bound =
                proof.target ? cbm_resolve_binding_validate(resolver->bindings, name, start, end,
                                                            origin, proof)
                             : proof;
            /* Calling a variable calls what it holds, when that has a fact. */
            bool value_call = invocation && proof.target && cbm_resolve_admit(&bound) &&
                              python_target_is_variable(resolver, proof.target);
            cbm_resolve_result_t member;
            return (!cbm_resolve_admit(&bound) || value_call) &&
                           python_member_result(resolver, name, start, end, origin, invocation,
                                                &member)
                       ? member
                       : bound;
        }
        cbm_resolve_result_t member;
        if (python_member_result(resolver, name, start, end, origin, invocation, &member))
            return member;
    }
    cbm_resolution_t candidate = cbm_registry_resolve_observed(
        file->registry, name, file->module_qn, file->import_keys, file->import_values,
        file->import_count, file->language, scope, invocation,
        cbm_resolve_binding_lookup_context(resolver->bindings));
    if (!candidate.qualified_name)
        return candidate.proof;
    if (!candidate.proof.target || strcmp(candidate.qualified_name, candidate.proof.target) != 0)
        return unresolved("candidate_identity_mismatch");
    if (file->language == CBM_LANG_PYTHON &&
        candidate.proof.rule == CBM_RESOLVE_RULE_IMPORT_BINDING)
        return unresolved("python_import_source_binding_unproven");
    if (candidate.proof.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
        export_complete(resolver, candidate.proof.target))
        candidate.proof.facts |= CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    return cbm_resolve_binding_validate(resolver->bindings, name, start, end, origin,
                                        candidate.proof);
}

/* One site fact from the Rust adapter, sealed with the proof of this occurrence:
 * its enclosing scope, spelling, span and relation. */
static cbm_resolve_result_t rust_result(file_resolver_t *resolver, cbm_rust_site_kind_t kind,
                                        const char *name, const char *scope, uint32_t start,
                                        uint32_t end, CBMSourceOrigin origin,
                                        const char *relation) {
    const cbm_rust_site_t *site = origin == CBM_SOURCE_ORIGIN_RAW
                                      ? cbm_rust_site_find(resolver->rust, kind, start, end)
                                      : NULL;
    if (!site)
        return unresolved(resolver->rust ? "rust_no_resolved_site" : "rust_project_model_missing");
    cbm_resolve_result_t result = {.binding = site->binding,
                                   .domain = site->domain,
                                   .analysis = site->analysis,
                                   .rule = site->rule,
                                   .facts = site->facts,
                                   .target = site->target,
                                   .candidate_count =
                                       site->binding == CBM_RESOLVE_BINDING_UNIQUE      ? 1
                                       : site->binding == CBM_RESOLVE_BINDING_AMBIGUOUS ? 2
                                                                                        : 0,
                                   .reason = site->reason};
    if (!cbm_resolve_admit(&result))
        return result;
    /* A graph node of another kind under this QN is not the declaration selected:
     * a field sharing its name with a method keeps only the method's node. */
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, site->target);
    if (node && site->label && node->label && strcmp(node->label, site->label)) {
        result.facts = 0;
        result.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
        result.candidate_count = 0;
        result.target = NULL;
        result.reason = "rust_target_label_conflict";
        return result;
    }
    cbm_resolve_lookup_context_t *shared = cbm_resolve_binding_lookup_context(resolver->bindings);
    if (!shared)
        return unresolved("rust_proof_context_missing");
    cbm_resolve_proof_source_t source = shared->source;
    source.scope = scope;
    cbm_resolve_lookup_context_t context;
    cbm_resolve_lookup_t query;
    if (!cbm_resolve_lookup_context_init(&context, &resolver->rust_proofs, source)) {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "rust_proof_unavailable";
        return result;
    }
    cbm_resolve_lookup_begin(&query, &context, "rust.name_resolution", name, start, end,
                             CBM_SOURCE_ORIGIN_RAW);
    (void)cbm_resolve_lookup_observe(&query, "rust.name_resolution", name, site->target,
                                     CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE,
                                     result.facts, true, site->reason);
    query.key.binding_identity = site->target;
    query.key.detail = relation;
    result.proof = cbm_resolve_lookup_finish(&query, CBM_PROOF_REGISTRY_LOOKUP, result);
    if (!result.proof) {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "rust_proof_unavailable";
    }
    return result;
}

static const cbm_gbuf_node_t *source_node(const file_resolver_t *resolver, const char *scope) {
    if (!scope || !scope[0])
        return resolver->file_node;
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, scope);
    /* A package-level reference in a directory-module language carries the DIRECTORY
     * module QN, which is the Folder shared by every file of the package. An anonymous
     * callable's scope names no node at all. Either way the occurrence is in this file, the
     * attribution every ordinary resolver uses (#787). */
    return !node || cbm_pipeline_node_is_dir_container(node) ? resolver->file_node : node;
}

static bool callable_node(const cbm_gbuf_node_t *node) {
    return node && node->label &&
           (strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0 ||
            strcmp(node->label, "Constructor") == 0 || strcmp(node->label, "Macro") == 0);
}

/* Called only by the typed extraction-carrier loops below. Those carriers
 * retain the actual AST role and enclosing declaration; the lexical auditor
 * independently checked this exact source site and its selected target. This
 * adds that role to local authority without cloning shared export ancestors. */
static const cbm_resolve_proof_t *local_occurrence_proof(const file_resolver_t *resolver,
                                                         const char *scope, const char *name,
                                                         const char *relation, uint32_t start,
                                                         uint32_t end, CBMSourceOrigin origin,
                                                         const cbm_resolve_result_t *result) {
    const cbm_resolve_binding_index_t *authority = resolver->bindings;
    const cbm_resolve_proof_t *local =
        resolver->source_bindings
            ? cbm_resolve_binding_site_proof(resolver->source_bindings, name, start, end, origin)
            : NULL;
    if (local)
        authority = resolver->source_bindings;
    else
        local = cbm_resolve_binding_site_proof(resolver->bindings, name, start, end, origin);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(local);
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(authority);
    if (!view || !context || !scope || !scope[0])
        return local;
    /* Incomplete or contradictory local authority must reach the sealer as
     * such; an extraction carrier cannot repair a lexical resolution failure. */
    if (view->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !view->predecessors_closed ||
        view->outcome.binding != CBM_RESOLVE_BINDING_UNIQUE ||
        view->outcome.domain != CBM_RESOLVE_DOMAIN_PROJECT || view->outcome.candidate_count != 1 ||
        view->outcome.rule != result->rule || !view->outcome.target || !result->target ||
        strcmp(view->outcome.target, result->target) ||
        !(view->outcome.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT) ||
        (result->facts & ~view->outcome.facts) || view->key.start_byte != start ||
        view->key.end_byte != end || view->key.origin != (uint32_t)origin ||
        !view->key.lookup_key || strcmp(view->key.lookup_key, name))
        return local;
    cbm_resolve_proof_source_t source = view->source;
    source.scope = scope;
    cbm_resolve_proof_key_t key = {.lookup_domain = scope,
                                   .lookup_key = name,
                                   .binding_identity = result->target,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .origin = (uint32_t)origin,
                                   .detail = relation};
    cbm_resolve_result_t role = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                 .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                 .reason = "typed_extraction_carrier_role"};
    const cbm_resolve_proof_t *premises[] = {
        local, cbm_resolve_proof_leaf(context->pool, CBM_PROOF_SOURCE_FACT, &source, &key, &role)};
    return cbm_resolve_proof_derive(context->pool, CBM_PROOF_NAMED_JOIN, &source, &key, result,
                                    premises, 2, true);
}

/* The extraction carrier read this token in this role. A Go reference's proof attests the
 * declaration, shared by every site that selects it. A package-level occurrence's proof
 * names the module scope, which in a directory-module language is the shared Folder, while
 * the edge starts at this file's node. Either way this ties that proof to the exact site. */
static bool carrier_needs_site_proof(const file_resolver_t *resolver, const cbm_gbuf_node_t *source,
                                     const char *scope, const cbm_resolve_result_t *result) {
    if (!result->proof || !cbm_resolve_admit(result))
        return false;
    if (source == resolver->file_node && scope && scope[0] && strcmp(scope, source->qualified_name))
        return true;
    return resolver->file->language == CBM_LANG_GO &&
           cbm_resolve_proof_view(result->proof)->key.start_byte == 0;
}

static const cbm_resolve_proof_t *carrier_occurrence_proof(const file_resolver_t *resolver,
                                                           const char *source_qn, const char *name,
                                                           const char *relation, uint32_t start,
                                                           uint32_t end, CBMSourceOrigin origin,
                                                           const cbm_resolve_result_t *result) {
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(resolver->bindings);
    if (!context || !source_qn || !name)
        return NULL;
    cbm_resolve_proof_source_t source =
        cbm_result_proof_source(resolver->file->result, origin, source_qn);
    source.file = resolver->file->rel_path;
    cbm_resolve_proof_key_t key = {.lookup_domain = source_qn,
                                   .lookup_key = name,
                                   .binding_identity = result->target,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .origin = (uint32_t)origin,
                                   .detail = relation};
    cbm_resolve_result_t site = *result;
    site.proof = NULL;
    site.reason = "extraction_carrier_occurrence";
    return cbm_resolve_proof_leaf(context->pool, CBM_PROOF_SOURCE_FACT, &source, &key, &site);
}

/* Projection retains the complete source spelling and argument metadata.
 * Fixed-size JSON buffers can silently merge long imported/reference names. */
static char *record_properties(const char *name, const char *relation, uint32_t start, uint32_t end,
                               CBMSourceOrigin origin, const cbm_resolve_result_t *result,
                               const CBMCall *call) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    bool ok = root && yyjson_mut_obj_add_str(doc, root, "strategy", "shared_resolver") &&
              yyjson_mut_obj_add_int(doc, root, "resolver_rule", result->rule) &&
              yyjson_mut_obj_add_uint(doc, root, "start_byte", start) &&
              yyjson_mut_obj_add_uint(doc, root, "end_byte", end) &&
              yyjson_mut_obj_add_uint(doc, root, "source_origin", origin);
    if (ok && (!strcmp(relation, "CALLS") || !strcmp(relation, "CALL_REFERENCE")))
        ok = yyjson_mut_obj_add_strcpy(doc, root, "callee", name ? name : "");
    if (ok && call && call->start_line > 0)
        ok = yyjson_mut_obj_add_int(doc, root, "line", call->start_line);
    if (ok && call && call->arg_count > 0) {
        yyjson_mut_val *args = yyjson_mut_arr(doc);
        ok = args && call->args && call->arg_count <= CBM_MAX_CALL_ARGS &&
             yyjson_mut_obj_add_val(doc, root, "args", args);
        for (int i = 0; ok && i < call->arg_count; i++) {
            const CBMCallArg *arg = &call->args[i];
            yyjson_mut_val *entry = yyjson_mut_obj(doc);
            ok = entry && yyjson_mut_obj_add_int(doc, entry, "i", arg->index) &&
                 yyjson_mut_obj_add_strcpy(doc, entry, "e", arg->expr ? arg->expr : "");
            if (ok && arg->keyword)
                ok = yyjson_mut_obj_add_strcpy(doc, entry, "k", arg->keyword);
            if (ok && arg->value)
                ok = yyjson_mut_obj_add_strcpy(doc, entry, "v", arg->value);
            if (ok)
                ok = yyjson_mut_arr_add_val(args, entry);
        }
    }
    if (ok)
        yyjson_mut_doc_set_root(doc, root);
    char *properties = ok ? cbm_json_write_text(doc, cbm_json_writer_allocator(), NULL) : NULL;
    yyjson_mut_doc_free(doc);
    return properties;
}

static bool record_result(const file_resolver_t *resolver, const char *scope, const char *name,
                          const char *relation, uint32_t start, uint32_t end,
                          CBMSourceOrigin origin, cbm_resolve_result_t result,
                          const CBMCall *call) {
    const cbm_gbuf_node_t *source = source_node(resolver, scope);
    const cbm_gbuf_node_t *target =
        result.target ? cbm_gbuf_find_by_qn(resolver->file->graph, result.target) : NULL;
    if (!source || !target) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        if (result.binding == CBM_RESOLVE_BINDING_UNIQUE)
            result.reason = source ? "target_not_materialized" : "source_not_materialized";
    }
    const owner_variant_t *conditional =
        conditional_source_owner(resolver, source, start, end, origin);
    bool moved = false;
    if (source && !source_identity_holds(resolver, source, conditional != NULL)) {
        bool here = source->file_path && !strcmp(source->file_path, resolver->file->rel_path);
        if (!here || !ecma_source_span_settles(resolver, source, start)) {
            const cbm_gbuf_node_t *owner = here ? ecma_shown_owner(resolver, source, start) : NULL;
            if (owner) {
                source = owner;
                moved = true;
            } else {
                result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
                if (result.binding == CBM_RESOLVE_BINDING_UNIQUE)
                    result.reason = "source_declaration_identity_conflict";
            }
        }
    }
    if (target && !target_materialized(resolver, target) &&
        !ecma_target_span_settles(resolver, target, name, start, end, origin)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "target_declaration_identity_conflict";
    }
    if (target && (strcmp(relation, "CALL_REFERENCE") == 0 || strcmp(relation, "CALLS") == 0) &&
        !callable_node(target) &&
        !((((resolver->file->language == CBM_LANG_PYTHON ||
             cbm_oo_language(resolver->file->language)) &&
            target->label && cbm_label_is_type_like(target->label)) ||
           ((ecma_language(resolver->file->language) || resolver->file->language == CBM_LANG_VUE ||
             resolver->file->language == CBM_LANG_SVELTE) &&
            target->label && !strcmp(target->label, "Class")) ||
           ((resolver->file->language == CBM_LANG_DART ||
             resolver->file->language == CBM_LANG_SWIFT ||
             resolver->file->language == CBM_LANG_SCALA) &&
            target->label &&
            (!strcmp(target->label, "Class") ||
             (resolver->file->language == CBM_LANG_SWIFT && !strcmp(target->label, "Struct")))) ||
           (resolver->file->language == CBM_LANG_RUST && target->label &&
            cbm_label_is_type_like(target->label))) &&
          (result.facts & CBM_RESOLVE_FACT_CLASS_CALLABLE))) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "callable_target_not_callable";
    }
    if (target && strcmp(relation, "USES_TYPE") == 0 && !target_declares_type(resolver, target)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "type_target_not_type";
    }
    if (!resolver->source_matches) {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "source_snapshot_mismatch";
    }
    const cbm_resolve_proof_t *binding =
        local_occurrence_proof(resolver, source ? source->qualified_name : scope, name, relation,
                               start, end, origin, &result);
    if (!binding && source &&
        ((moved && result.proof && cbm_resolve_admit(&result)) ||
         carrier_needs_site_proof(resolver, source, scope, &result)))
        binding = carrier_occurrence_proof(resolver, source->qualified_name, name, relation, start,
                                           end, origin, &result);
    if (conditional && result.proof && cbm_resolve_admit(&result)) {
        /* Extend local authority rather than cloning shared export ancestors
         * into every worker. An export summary cannot attest a local site. */
        if (!binding && cbm_shared_resolver_owns_proof(resolver->shared, result.proof)) {
            result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
            result.reason = "conditional_owner_occurrence_proof_missing";
        } else {
            const cbm_resolve_proof_t *joined = conditional_owner_proof(
                resolver, source, conditional, binding ? binding : result.proof);
            if (!joined)
                return false;
            if (binding)
                binding = joined;
            else
                result.proof = joined;
        }
    }
    char *properties = record_properties(name, relation, start, end, origin, &result, call);
    if (!properties)
        return false;
    cbm_resolve_record_t record = {
        .file_path = resolver->file->rel_path,
        .scope = source ? source->qualified_name : scope,
        .relation = relation,
        .spelling = name,
        .start_byte = start,
        .end_byte = end,
        .source_origin = origin,
        .result = result,
        .binding_proof = binding,
        .content_identity =
            origin >= CBM_SOURCE_ORIGIN_RAW && origin <= CBM_SOURCE_ORIGIN_PREPROCESSED
                ? resolver->file->result->source_content_id[origin]
                : "",
        .configuration_identity = resolver->file->result->source_configuration_id,
        .proof_borrowed = cbm_shared_resolver_owns_proof(resolver->shared, result.proof),
        .properties_json = properties};
    record.emitted = cbm_resolve_record_candidate(&record);
    bool ok = !record.emitted || cbm_gbuf_insert_edge(resolver->file->output, source->id,
                                                      target->id, relation, properties);
    if (ok)
        ok = cbm_gbuf_record_resolution(resolver->file->output, &record);
    cbm_free(CBM_MEM_CLASS_OTHER, properties);
    return ok;
}

static bool resolve_calls(file_resolver_t *resolver, CBMArena *arena) {
    const CBMFileResult *result = resolver->file->result;
    bool go = resolver->file->language == CBM_LANG_GO;
    for (int c = 0; c < result->calls.count; c++) {
        const CBMCall *call = &result->calls.items[c];
        /* The shared fixed point decides a Go site it can answer, including ambiguity. A site
         * it leaves unknown keeps whatever the hybrid engine proved on its own: an unknown
         * operand here is missing knowledge, not a contradiction of that proof. Go's lookup
         * goes through neither the registry nor a lexical adapter, so nothing else can answer. */
        cbm_resolve_result_t stabilized = unresolved("go_callee_unresolved");
        bool go_site =
            go && resolver->source_matches && call->source_origin == CBM_SOURCE_ORIGIN_RAW &&
            cbm_go_stabilization_call(resolver->shared->go_types, resolver->file->rel_path,
                                      call->enclosing_func_qn, call->site_start_byte,
                                      call->site_end_byte, &stabilized);
        cbm_resolve_result_t outcome;
        if (go_site && stabilized.binding != CBM_RESOLVE_BINDING_UNRESOLVED) {
            outcome = stabilized;
        } else {
            const semantic_site_t *site =
                find_semantic(resolver, call->enclosing_func_qn, call->site_start_byte,
                              call->site_end_byte, call->source_origin, CBM_RESOLVED_INVOCATION);
            if (go) {
                outcome = site ? semantic_result(resolver, site, arena) : stabilized;
                if (!cbm_resolve_admit(&outcome))
                    outcome = stabilized;
            } else {
                /* Exact simple JSX tags can obtain an independent lexical proof. An
                 * existing semantic result, including ambiguity, remains authoritative. */
                bool lexical_jsx = call->requires_lsp_resolution &&
                                   cbm_resolve_binding_jsx_occurrence(
                                       resolver->bindings, call->callee_name, call->site_start_byte,
                                       call->site_end_byte, call->source_origin);
                outcome = site ? semantic_result(resolver, site, arena)
                          : resolver->file->language == CBM_LANG_RUST
                              ? rust_result(resolver, CBM_RUST_SITE_CALL, call->callee_name,
                                            call->enclosing_func_qn, call->site_start_byte,
                                            call->site_end_byte, call->source_origin, "CALLS")
                          : call->requires_lsp_resolution && !lexical_jsx
                              ? unresolved("semantic_resolution_required")
                              : generic_result(resolver, call->callee_name, call->enclosing_func_qn,
                                               call->site_start_byte, call->site_end_byte,
                                               call->source_origin, true);
            }
        }
        /* Go's T(x) converts. No function runs, and the site uses the type. */
        const char *relation =
            go && outcome.reason == cbm_go_type_conversion ? "USES_TYPE" : "CALLS";
        if (!record_result(resolver, call->enclosing_func_qn, call->callee_name, relation,
                           call->site_start_byte, call->site_end_byte, call->source_origin, outcome,
                           call))
            return false;
    }
    size_t extra_count = 0;
    const cbm_source_extra_call_t *extras =
        cbm_source_unit_extra_calls(resolver->source_unit, &extra_count);
    for (size_t e = 0; e < extra_count; e++) {
        const cbm_source_extra_call_t *extra = &extras[e];
        cbm_resolve_result_t outcome = source_adapter_result(
            resolver, extra->name, extra->start, extra->end, CBM_SOURCE_ORIGIN_RAW, true);
        if (!record_result(resolver, extra->scope, extra->name, "CALLS", extra->start, extra->end,
                           CBM_SOURCE_ORIGIN_RAW, outcome, NULL))
            return false;
    }
    return true;
}

/* Go's lexical and selector rules decide a reference token the fixed point analysed. Go
 * has no other authority, so an unanalysed token skips the registry's lookup and its proof. */
static bool go_reference(const file_resolver_t *resolver, uint32_t start, uint32_t end,
                         CBMSourceOrigin origin, bool member, cbm_resolve_result_t *out) {
    if (resolver->file->language != CBM_LANG_GO)
        return false;
    if (resolver->source_matches && origin == CBM_SOURCE_ORIGIN_RAW &&
        cbm_go_stabilization_reference(resolver->shared->go_types, resolver->file->rel_path, start,
                                       end, out))
        return true;
    *out = unresolved(member ? "receiver_type_required" : "go_reference_unresolved");
    return true;
}

static bool resolve_usages(file_resolver_t *resolver, CBMArena *arena) {
    const CBMFileResult *result = resolver->file->result;
    for (int u = 0; u < result->usages.count; u++) {
        const CBMUsage *usage = &result->usages.items[u];
        const char *relation = usage->kind == CBM_USAGE_CALL_REFERENCE ? "CALL_REFERENCE" : "USAGE";
        /* USAGE means a value. A type annotation's name is USES_TYPE, from type_refs. */
        if (usage->kind != CBM_USAGE_CALL_REFERENCE &&
            cbm_resolve_binding_js_type_occurrence(resolver->bindings, usage->ref_name,
                                                   usage->site_start_byte, usage->site_end_byte,
                                                   usage->source_origin)) {
            if (!record_result(resolver, usage->enclosing_func_qn, usage->ref_name, relation,
                               usage->site_start_byte, usage->site_end_byte, usage->source_origin,
                               unresolved("usage_names_a_type"), NULL))
                return false;
            continue;
        }
        const semantic_site_t *site =
            find_semantic(resolver, usage->enclosing_func_qn, usage->site_start_byte,
                          usage->site_end_byte, usage->source_origin, CBM_RESOLVED_CALL_REFERENCE);
        if (site && usage->may_be_call_reference)
            relation = "CALL_REFERENCE";
        if (!site) {
            site = find_semantic(resolver, usage->enclosing_func_qn, usage->site_start_byte,
                                 usage->site_end_byte, usage->source_origin,
                                 CBM_RESOLVED_VALUE_REFERENCE);
            if (site)
                relation = "USAGE";
        }
        if (!site)
            site = find_semantic(resolver, usage->enclosing_func_qn, usage->site_start_byte,
                                 usage->site_end_byte, usage->source_origin,
                                 CBM_RESOLVED_TYPE_REFERENCE);
        if (!site && usage->is_member_access)
            site = find_semantic(resolver, usage->enclosing_func_qn, usage->site_start_byte,
                                 usage->site_end_byte, usage->source_origin,
                                 CBM_RESOLVED_FIELD_REFERENCE);
        /* A direct callable argument is a call reference, decided before the
         * occurrence proof records its relation. */
        if (!site && resolver->file->language == CBM_LANG_RUST && usage->may_be_call_reference) {
            const cbm_rust_site_t *fact = cbm_rust_site_find(
                resolver->rust, CBM_RUST_SITE_REF, usage->site_start_byte, usage->site_end_byte);
            if (fact && fact->target &&
                callable_node(cbm_gbuf_find_by_qn(resolver->file->graph, fact->target)))
                relation = "CALL_REFERENCE";
        }
        cbm_resolve_result_t outcome;
        if (site)
            outcome = semantic_result(resolver, site, arena);
        else if (resolver->file->language == CBM_LANG_RUST)
            outcome = rust_result(resolver, CBM_RUST_SITE_REF, usage->ref_name,
                                  usage->enclosing_func_qn, usage->site_start_byte,
                                  usage->site_end_byte, usage->source_origin, relation);
        else if (!go_reference(resolver, usage->site_start_byte, usage->site_end_byte,
                               usage->source_origin, usage->is_member_access, &outcome))
            outcome = usage->is_member_access
                          ? unresolved("receiver_type_required")
                          : generic_result(resolver, usage->ref_name, usage->enclosing_func_qn,
                                           usage->site_start_byte, usage->site_end_byte,
                                           usage->source_origin, false);
        /* Source binding can establish a direct callable argument without a
         * hybrid row. Composite expressions remain USAGE: the extraction
         * carrier deliberately leaves may_be_call_reference false for them. */
        if (!site && usage->may_be_call_reference && cbm_resolve_admit(&outcome) &&
            callable_node(cbm_gbuf_find_by_qn(resolver->file->graph, outcome.target)))
            relation = "CALL_REFERENCE";
        /* Reading a property hands over its result, not the function. */
        if (!site && outcome.reason && !strcmp(outcome.reason, CBM_RESOLVE_PYTHON_PROPERTY_READ))
            relation = "USAGE";
        if (!record_result(resolver, usage->enclosing_func_qn, usage->ref_name, relation,
                           usage->site_start_byte, usage->site_end_byte, usage->source_origin,
                           outcome, NULL))
            return false;
    }
    return true;
}

static bool resolve_writes(file_resolver_t *resolver, CBMArena *arena) {
    const CBMFileResult *result = resolver->file->result;
    for (int r = 0; r < result->rw.count; r++) {
        const CBMReadWrite *rw = &result->rw.items[r];
        const semantic_site_t *site =
            find_semantic(resolver, rw->enclosing_func_qn, rw->site_start_byte, rw->site_end_byte,
                          rw->source_origin, CBM_RESOLVED_FIELD_REFERENCE);
        if (!site && !rw->is_member_access &&
            (resolver->file->language == CBM_LANG_C || resolver->file->language == CBM_LANG_CPP ||
             resolver->file->language == CBM_LANG_CUDA))
            site =
                find_semantic(resolver, rw->enclosing_func_qn, rw->site_start_byte,
                              rw->site_end_byte, rw->source_origin, CBM_RESOLVED_VALUE_REFERENCE);
        cbm_resolve_result_t outcome;
        if (site)
            outcome = semantic_result(resolver, site, arena);
        else if (resolver->file->language == CBM_LANG_RUST)
            outcome = rust_result(resolver, CBM_RUST_SITE_REF, rw->var_name, rw->enclosing_func_qn,
                                  rw->site_start_byte, rw->site_end_byte, rw->source_origin,
                                  rw->is_write ? "WRITES" : "READS");
        else if (!go_reference(resolver, rw->site_start_byte, rw->site_end_byte, rw->source_origin,
                               rw->is_member_access, &outcome))
            outcome = rw->is_member_access
                          ? unresolved("receiver_type_required")
                          : generic_result(resolver, rw->var_name, rw->enclosing_func_qn,
                                           rw->site_start_byte, rw->site_end_byte,
                                           rw->source_origin, false);
        if (!record_result(resolver, rw->enclosing_func_qn, rw->var_name,
                           rw->is_write ? "WRITES" : "READS", rw->site_start_byte,
                           rw->site_end_byte, rw->source_origin, outcome, NULL))
            return false;
    }
    return true;
}

static bool resolve_types(file_resolver_t *resolver) {
    const CBMFileResult *result = resolver->file->result;
    for (int t = 0; t < result->type_refs.count; t++) {
        const CBMTypeRef *type = &result->type_refs.items[t];
        const semantic_site_t *site =
            type->site_is_exact ? find_semantic(resolver, type->enclosing_func_qn,
                                                type->site_start_byte, type->site_end_byte,
                                                type->source_origin, CBM_RESOLVED_TYPE_REFERENCE)
                                : NULL;
        cbm_resolve_result_t outcome;
        if (site)
            outcome = semantic_result(resolver, site, resolver->arena);
        else if (!type->site_is_exact)
            outcome = unresolved("type_occurrence_identity_missing");
        else if (resolver->file->language == CBM_LANG_RUST)
            outcome = rust_result(resolver, CBM_RUST_SITE_REF, type->type_name,
                                  type->enclosing_func_qn, type->site_start_byte,
                                  type->site_end_byte, type->source_origin, "USES_TYPE");
        else if (!go_reference(resolver, type->site_start_byte, type->site_end_byte,
                               type->source_origin, false, &outcome))
            outcome = generic_result(resolver, type->type_name, type->enclosing_func_qn,
                                     type->site_start_byte, type->site_end_byte,
                                     type->source_origin, false);
        if (!record_result(resolver, type->enclosing_func_qn, type->type_name, "USES_TYPE",
                           type->site_start_byte, type->site_end_byte, type->source_origin, outcome,
                           NULL))
            return false;
    }
    return true;
}

bool cbm_shared_resolver_file(const cbm_shared_resolver_t *shared,
                              const cbm_resolver_file_t *file) {
    if (!file || !file->ctx || !file->result || !file->graph || !file->output || !file->registry)
        return false;
    CBMArena arena;
    cbm_arena_init(&arena);
    size_t length = file->source                   ? file->source_len
                    : file->result->source_len > 0 ? (size_t)file->result->source_len
                                                   : 0;
    const char *source = file->source ? file->source : file->result->source;
    char *owned = source ? NULL : file_source(file, &length);
    if (!source)
        source = owned;
    CBMFileResult source_view;
    TSTree *owner_tree = NULL;
    cbm_resolver_file_t source_file = *file;
    source_file.result =
        owner_source_view(file->result, file->language, source, length, &source_view, &owner_tree);
    file = &source_file;
    ecma_spans_t ecma_spans = {0};
    file_resolver_t resolver = {.shared = shared,
                                .file = file,
                                .arena = &arena,
                                .source = source,
                                .source_length = length,
                                .ecma_spans = ecma_language(file->language) ? &ecma_spans : NULL,
                                .source_matches = cbm_result_source_matches(
                                    file->result, source, length, CBM_SOURCE_ORIGIN_RAW)};
    resolver.file_node =
        cbm_pipeline_file_node(file->graph, file->ctx->project_name, file->rel_path);
    resolver.bindings =
        binding_build_for(&arena, file->result, file->language, file->rel_path, source, length);
    if (file->language == CBM_LANG_PYTHON && shared && file->result->module_qn) {
        const char *module = file->result->module_qn;
        const char *scope = cbm_arena_strndup(&arena, module, cbm_fqn_symbol_scope_len(module));
        const namespace_proof_t *namespace =
            scope ? cbm_ht_get(shared->python_namespaces, scope) : NULL;
        if (namespace)
            cbm_resolve_binding_python_set_star_barrier(resolver.bindings,
                                                        namespace->python_open_barrier);
        cbm_py_members_apply_decorated(shared->python_members, resolver.bindings);
    }
    TSTree *rust_tree = NULL;
    bool rust_ok = true;
    if (file->language == CBM_LANG_RUST && file->ctx->rust_project && source &&
        resolver.source_matches && length <= UINT32_MAX) {
        const TSTree *tree = file->result->cached_tree;
        if (!tree) {
            TSParser *parser = ts_parser_new();
            if (parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_RUST)))
                rust_tree = cbm_parse_rust_file(parser, file->rel_path, source, (uint32_t)length,
                                                (TSParseOptions){0});
            if (parser)
                ts_parser_delete(parser);
            tree = rust_tree;
        }
        /* A partial parse leaves no faithful scopes: every site stays unresolved. */
        if (tree && !ts_node_has_error(ts_tree_root_node(tree))) {
            resolver.rust = cbm_rust_analyze_file(file->ctx->rust_project, &arena, file->rel_path,
                                                  source, length, ts_tree_root_node(tree));
            rust_ok = resolver.rust != NULL;
            cbm_resolve_proof_pool_init_project(&resolver.rust_proofs, &arena);
        }
    }
    /* A standalone caller's project is this one file. */
    cbm_source_table_t *standalone = NULL;
    if (cbm_source_adapter_supported(file->language)) {
        const cbm_source_table_t *table = shared ? shared->source_table : NULL;
        bool matches = source && cbm_result_source_matches(file->result, source, length,
                                                           CBM_SOURCE_ORIGIN_RAW);
        if (!table) {
            standalone = cbm_source_table_new();
            if (standalone)
                (void)cbm_source_collect(standalone, file->result, file->language, file->rel_path,
                                         matches ? source : NULL, length);
            table = standalone;
        }
        resolver.source_unit =
            cbm_source_unit_build(&arena, table, file->result, file->language, file->rel_path,
                                  matches ? source : NULL, length);
        if (cbm_source_adapter_has_binding_rules(file->language))
            resolver.source_bindings = cbm_resolve_binding_source_index(
                &arena, file->result, file->language, source, length);
    }
    bool begun = cbm_gbuf_resolution_begin(file->output);
    bool ok = rust_ok && begun && resolver.bindings &&
              cbm_resolve_binding_set_identity(resolver.bindings, file->ctx->project_name,
                                               file->rel_path) &&
              (!resolver.source_bindings ||
               cbm_resolve_binding_set_identity(resolver.source_bindings, file->ctx->project_name,
                                                file->rel_path)) &&
              prepare_source_imports(&resolver) && index_semantics(&resolver) &&
              resolve_calls(&resolver, &arena) && resolve_usages(&resolver, &arena) &&
              resolve_writes(&resolver, &arena) && resolve_types(&resolver);
    if (begun)
        cbm_gbuf_resolution_end(file->output);
    cbm_ht_free(resolver.import_names);
    cbm_ht_free(resolver.python_imports);
    cbm_ht_free(resolver.ecma_dependencies);
    cbm_free(CBM_MEM_CLASS_EXTRACT, resolver.sites);
    cbm_source_table_free(standalone);
    if (owner_tree)
        ts_tree_delete(owner_tree);
    if (rust_tree)
        ts_tree_delete(rust_tree);
    cbm_arena_destroy(&arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
    return ok;
}

static bool controlled_relation(const char *relation) {
    static const char *const labels[] = {"CALLS",  "USAGE",     "CALL_REFERENCE", "READS",
                                         "WRITES", "USES_TYPE", "IMPORTS"};
    if (!relation)
        return false;
    for (size_t i = 0; i < sizeof(labels) / sizeof(labels[0]); i++) {
        if (strcmp(relation, labels[i]) == 0)
            return true;
    }
    return false;
}

static int compare_record(const void *left, const void *right) {
    const cbm_resolve_record_t *a = *(const cbm_resolve_record_t *const *)left;
    const cbm_resolve_record_t *b = *(const cbm_resolve_record_t *const *)right;
    int order = strcmp(a->scope, b->scope);
    if (!order)
        order = strcmp(a->result.target, b->result.target);
    if (!order)
        order = strcmp(a->relation, b->relation);
    if (!order)
        order = strcmp(a->file_path, b->file_path);
    if (!order && a->source_origin != b->source_origin)
        order = a->source_origin < b->source_origin ? -1 : 1;
    if (!order && a->start_byte != b->start_byte)
        order = a->start_byte < b->start_byte ? -1 : 1;
    if (!order && a->end_byte != b->end_byte)
        order = a->end_byte < b->end_byte ? -1 : 1;
    if (!order)
        order = strcmp(a->properties_json ? a->properties_json : "{}",
                       b->properties_json ? b->properties_json : "{}");
    return order;
}

bool cbm_shared_resolver_project(cbm_gbuf_t *graph) {
    if (!graph || !cbm_gbuf_resolutions_complete(graph))
        return false;
    size_t count = 0, admitted = 0;
    const cbm_resolve_record_t *records = cbm_gbuf_resolutions(graph, &count);
    for (size_t r = 0; r < count; r++) {
        const cbm_resolve_record_t *record = &records[r];
        if (!record->emitted || !controlled_relation(record->relation))
            continue;
        if (!cbm_resolve_record_admit(record) || !record->scope ||
            !cbm_gbuf_find_by_qn(graph, record->scope) ||
            !cbm_gbuf_find_by_qn(graph, record->result.target))
            return false;
        admitted++;
    }
    if (admitted > SIZE_MAX / sizeof(const cbm_resolve_record_t *))
        return false;
    const cbm_resolve_record_t **ordered =
        admitted ? cbm_alloc(CBM_MEM_CLASS_EXTRACT, admitted * sizeof(*ordered)) : NULL;
    if (admitted && !ordered)
        return false;
    size_t next = 0;
    for (size_t r = 0; r < count; r++) {
        if (records[r].emitted && controlled_relation(records[r].relation))
            ordered[next++] = &records[r];
    }
    if (admitted > 1)
        qsort(ordered, admitted, sizeof(*ordered), compare_record);
    static const char *const labels[] = {"CALLS",  "USAGE",     "CALL_REFERENCE", "READS",
                                         "WRITES", "USES_TYPE", "IMPORTS"};
    for (size_t i = 0; i < sizeof(labels) / sizeof(labels[0]); i++) {
        if (cbm_gbuf_delete_edges_by_type(graph, labels[i]) < 0) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, ordered);
            return false;
        }
    }
    bool ok = true;
    for (size_t r = 0; ok && r < admitted; r++) {
        const cbm_resolve_record_t *record = ordered[r];
        const cbm_gbuf_node_t *source = cbm_gbuf_find_by_qn(graph, record->scope);
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(graph, record->result.target);
        if (!source || !target) {
            ok = false;
            break;
        }
        ok = cbm_gbuf_insert_edge(graph, source->id, target->id, record->relation,
                                  record->properties_json ? record->properties_json : "{}") != 0;
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, ordered);
    return ok;
}

/* Adapter bridges share source capture and occurrence sealing; the adapter
 * retains responsibility for its language's closed lookup semantics. */
typedef struct {
    cbm_resolve_file_t file;
    const char *captured_bytes;
    size_t captured_length;
    bool captured;
} adapter_source_t;

static bool adapter_fail(cbm_shared_resolver_adapter_t *adapter) {
    if (adapter && adapter->ctx)
        atomic_store(&adapter->ctx->resolver_failed, true);
    return false;
}

static bool adapter_text_equal(const char *left, const char *right) {
    return strcmp(left ? left : "", right ? right : "") == 0;
}

static bool adapter_snapshot_equal(const cbm_resolve_file_t *left,
                                   const cbm_resolve_file_t *right) {
    return left->raw_length == right->raw_length &&
           left->preprocessed_length == right->preprocessed_length &&
           left->language == right->language && left->complete == right->complete &&
           adapter_text_equal(left->file_path, right->file_path) &&
           adapter_text_equal(left->module, right->module) &&
           adapter_text_equal(left->raw_content_identity, right->raw_content_identity) &&
           adapter_text_equal(left->preprocessed_content_identity,
                              right->preprocessed_content_identity) &&
           adapter_text_equal(left->configuration_identity, right->configuration_identity) &&
           adapter_text_equal(left->extracted_summary, right->extracted_summary);
}

/* Copy the row because recording another file may relocate the gbuf array.
 * Its interned strings remain owned by the generation. */
static bool adapter_index_snapshot(CBMHashTable *index, CBMArena *arena,
                                   const cbm_resolve_file_t *file) {
    if (!file->file_path || !file->file_path[0])
        return false;
    cbm_resolve_file_t *old = cbm_ht_get(index, file->file_path);
    if (old)
        return adapter_snapshot_equal(old, file);
    cbm_resolve_file_t *copy = cbm_arena_alloc(arena, sizeof(*copy));
    if (!copy)
        return false;
    *copy = *file;
    cbm_ht_set(index, file->file_path, copy);
    return cbm_ht_get(index, file->file_path) == copy;
}

bool cbm_shared_resolver_capture_sources(const cbm_pipeline_ctx_t *ctx,
                                         const cbm_file_info_t *files, int file_count,
                                         CBMFileResult **cache) {
    if (!ctx || !ctx->gbuf || file_count < 0 || (file_count && (!files || !cache)))
        return false;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    CBMHashTable *index = cbm_ht_create(0);
    bool ok = index != NULL;
    size_t count = 0;
    const cbm_resolve_file_t *recorded = cbm_gbuf_resolver_files(ctx->gbuf, &count);
    for (size_t i = 0; ok && i < count; i++) {
        ok = adapter_index_snapshot(index, &arena, &recorded[i]);
        if (!ok)
            cbm_log_error("resolver.sources.error", "file", recorded[i].file_path, "reason",
                          "recorded_snapshots_conflict");
    }
    for (int i = 0; ok && i < file_count; i++) {
        bool loaded = false;
        bool parked = ctx->spill && cbm_result_spill_has(ctx->spill, i);
        CBMFileResult *result;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        /* Deterministic acquisition failure at a real populated spill slot. */
        bool force_missing = parked && !cache[i] &&
                             (ctx->test_fault_mask & CBM_PIPELINE_TEST_FAULT_RESOLVER_SPILL_NULL);
        result = force_missing ? NULL : cbm_pipeline_result_acquire(ctx, cache, i, NULL, &loaded);
#else
        result = cbm_pipeline_result_acquire(ctx, cache, i, NULL, &loaded);
#endif
        if (!result) {
            if (parked) {
                cbm_log_error("resolver.sources.error", "file", files[i].rel_path, "reason",
                              "spill_load_failed");
                ok = false;
            }
            continue; /* An intentional extraction skip owns no spill slot. */
        }
        char extracted_summary[65];
        cbm_resolve_file_t snapshot;
        ok = cbm_resolve_capture_extraction_state(result) &&
             cbm_resolve_snapshot(result, files[i].rel_path, files[i].language, &snapshot,
                                  extracted_summary);
        if (!ok) /* The whole publication stops here: it must say why. */
            cbm_log_error("resolver.sources.error", "file", files[i].rel_path, "reason",
                          "extraction_state_inconsistent");
        const cbm_resolve_file_t *old =
            ok && snapshot.file_path ? cbm_ht_get(index, snapshot.file_path) : NULL;
        if (ok && old) {
            ok = adapter_snapshot_equal(old, &snapshot);
            if (!ok) {
                cbm_log_error("resolver.sources.error", "file", files[i].rel_path, "reason",
                              "snapshot_differs_from_adapter_capture");
                cbm_log_error("resolver.sources.detail", "old_module", old->module, "new_module",
                              snapshot.module, "old_summary", old->extracted_summary, "new_summary",
                              snapshot.extracted_summary, "old_raw", old->raw_content_identity,
                              "new_raw", snapshot.raw_content_identity);
            }
        } else if (ok) {
            ok = cbm_gbuf_record_resolver_file(ctx->gbuf, &snapshot);
            if (ok) {
                recorded = cbm_gbuf_resolver_files(ctx->gbuf, &count);
                ok = count && adapter_index_snapshot(index, &arena, &recorded[count - 1]);
            }
        }
        cbm_pipeline_result_release(result, loaded);
    }
    if (index)
        cbm_ht_free(index);
    cbm_arena_destroy(&arena);
    return ok;
}

bool cbm_shared_resolver_adapter_init(cbm_shared_resolver_adapter_t *adapter,
                                      cbm_pipeline_ctx_t *ctx, CBMArena *arena) {
    if (!adapter)
        return false;
    *adapter = (cbm_shared_resolver_adapter_t){.ctx = ctx, .arena = arena};
    if (!ctx || !ctx->gbuf || !arena)
        return adapter_fail(adapter);
    adapter->source_files = cbm_ht_create(0);
    if (!adapter->source_files)
        return adapter_fail(adapter);
    /* One adapter serves every infrastructure input in the project. */
    cbm_resolve_proof_pool_init_project(&adapter->proofs, arena);
    size_t count = 0;
    const cbm_resolve_file_t *files = cbm_gbuf_resolver_files(ctx->gbuf, &count);
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_file_t *file = &files[i];
        if (!file->file_path || !file->file_path[0])
            return adapter_fail(adapter);
        adapter_source_t *old = cbm_ht_get(adapter->source_files, file->file_path);
        if (old) {
            if (old->file.raw_length != file->raw_length || old->file.language != file->language ||
                !adapter_text_equal(old->file.module, file->module) ||
                !adapter_text_equal(old->file.raw_content_identity, file->raw_content_identity) ||
                !adapter_text_equal(old->file.configuration_identity, file->configuration_identity))
                return adapter_fail(adapter);
            continue;
        }
        adapter_source_t *entry = cbm_arena_alloc(arena, sizeof(*entry));
        if (!entry)
            return adapter_fail(adapter);
        *entry = (adapter_source_t){.file = *file};
        cbm_ht_set(adapter->source_files, file->file_path, entry);
        if (cbm_ht_get(adapter->source_files, file->file_path) != entry)
            return adapter_fail(adapter);
    }
    return true;
}

void cbm_shared_resolver_adapter_dispose(cbm_shared_resolver_adapter_t *adapter) {
    if (adapter && adapter->source_files) {
        cbm_ht_free(adapter->source_files);
        adapter->source_files = NULL;
    }
}

bool cbm_shared_resolver_adapter_capture(cbm_shared_resolver_adapter_t *adapter, const char *file,
                                         CBMLanguage language, const char *bytes, size_t length,
                                         cbm_resolve_proof_source_t *source) {
    adapter_source_t *entry =
        adapter && adapter->source_files && file ? cbm_ht_get(adapter->source_files, file) : NULL;
    if (!entry || !source || !bytes || entry->file.raw_length != length ||
        entry->file.language != (uint32_t)language || !entry->file.raw_content_identity ||
        !entry->file.raw_content_identity[0] || !entry->file.configuration_identity ||
        !entry->file.configuration_identity[0] || memchr(bytes, '\0', length))
        return adapter_fail(adapter);
    char identity[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(bytes, length, identity);
    if (strcmp(identity, entry->file.raw_content_identity) ||
        !cbm_gbuf_capture_input(adapter->ctx->gbuf, file, true, length, identity))
        return adapter_fail(adapter);
    entry->captured = true;
    entry->captured_bytes = bytes;
    entry->captured_length = length;
    *source =
        (cbm_resolve_proof_source_t){.project = adapter->ctx->project_name,
                                     .file = entry->file.file_path,
                                     .module = entry->file.module,
                                     .content_identity = entry->file.raw_content_identity,
                                     .configuration_identity = entry->file.configuration_identity,
                                     .source_origin = CBM_SOURCE_ORIGIN_RAW};
    return true;
}

static bool adapter_source_matches(cbm_shared_resolver_adapter_t *adapter,
                                   const cbm_resolve_proof_source_t *source) {
    adapter_source_t *entry = adapter && adapter->source_files && source && source->file
                                  ? cbm_ht_get(adapter->source_files, source->file)
                                  : NULL;
    return entry && entry->captured && source->source_origin == CBM_SOURCE_ORIGIN_RAW &&
           source->scope && source->scope[0] &&
           adapter_text_equal(source->project, adapter->ctx->project_name) &&
           adapter_text_equal(source->module, entry->file.module) &&
           adapter_text_equal(source->content_identity, entry->file.raw_content_identity) &&
           adapter_text_equal(source->configuration_identity, entry->file.configuration_identity);
}

const cbm_resolve_proof_t *cbm_shared_resolver_adapter_occurrence(
    cbm_shared_resolver_adapter_t *adapter, const cbm_resolve_proof_source_t *source,
    const char *bytes, size_t length, uint32_t start, uint32_t end, const char *spelling,
    const char *relation) {
    adapter_source_t *entry = adapter && adapter->source_files && source && source->file
                                  ? cbm_ht_get(adapter->source_files, source->file)
                                  : NULL;
    if (!adapter_source_matches(adapter, source) || !entry || !bytes ||
        entry->captured_bytes != bytes || entry->captured_length != length || end <= start ||
        end > length || !spelling || strlen(spelling) != end - start ||
        memcmp(bytes + start, spelling, end - start) || !controlled_relation(relation)) {
        (void)adapter_fail(adapter);
        return NULL;
    }
    cbm_resolve_proof_key_t key = {.lookup_domain = source->scope,
                                   .lookup_key = spelling,
                                   .detail = relation,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .origin = source->source_origin};
    cbm_resolve_result_t parsed = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                   .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                   .reason = "adapter_original_source_occurrence"};
    const cbm_resolve_proof_t *proof =
        cbm_resolve_proof_leaf(&adapter->proofs, CBM_PROOF_SOURCE_FACT, source, &key, &parsed);
    if (!proof)
        (void)adapter_fail(adapter);
    return proof;
}

static bool adapter_semantic_matches(const cbm_resolve_proof_view_t *site,
                                     const cbm_resolve_proof_view_t *semantic) {
    return semantic && semantic->kind != CBM_PROOF_SOURCE_FACT &&
           semantic->kind != CBM_PROOF_OCCURRENCE_BINDING &&
           adapter_text_equal(site->source.project, semantic->source.project) &&
           adapter_text_equal(site->source.file, semantic->source.file) &&
           adapter_text_equal(site->source.module, semantic->source.module) &&
           adapter_text_equal(site->source.scope, semantic->source.scope) &&
           adapter_text_equal(site->source.content_identity, semantic->source.content_identity) &&
           adapter_text_equal(site->source.configuration_identity,
                              semantic->source.configuration_identity) &&
           site->source.source_origin == semantic->source.source_origin &&
           site->key.start_byte == semantic->key.start_byte &&
           site->key.end_byte == semantic->key.end_byte &&
           site->key.origin == semantic->key.origin &&
           adapter_text_equal(site->key.lookup_key, semantic->key.lookup_key) &&
           adapter_text_equal(site->key.detail, semantic->key.detail) &&
           (semantic->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            adapter_text_equal(semantic->key.binding_identity, semantic->outcome.target));
}

bool cbm_shared_resolver_adapter_record_proofs(cbm_shared_resolver_adapter_t *adapter,
                                               const cbm_resolve_proof_t *occurrence,
                                               int64_t owner_id, int64_t target_id,
                                               const char *properties,
                                               const cbm_resolve_proof_t *semantic) {
    const cbm_resolve_proof_view_t *site = cbm_resolve_proof_view(occurrence);
    const cbm_resolve_proof_view_t *lookup = cbm_resolve_proof_view(semantic);
    if (!adapter || !site || site->kind != CBM_PROOF_SOURCE_FACT ||
        !cbm_resolve_proof_pool_owns(&adapter->proofs, occurrence) ||
        !adapter_source_matches(adapter, &site->source) ||
        !adapter_semantic_matches(site, lookup) || !controlled_relation(site->key.detail))
        return adapter_fail(adapter);
    const cbm_gbuf_node_t *owner = cbm_gbuf_find_by_id(adapter->ctx->gbuf, owner_id);
    const cbm_gbuf_node_t *target =
        target_id > 0 ? cbm_gbuf_find_by_id(adapter->ctx->gbuf, target_id) : NULL;
    cbm_resolve_result_t outcome = lookup->outcome;
    bool exact = cbm_resolve_admit(&outcome);
    if (!owner || !adapter_text_equal(owner->qualified_name, site->source.scope) ||
        !adapter_text_equal(owner->file_path, site->source.file) ||
        (exact && (!lookup->key.lookup_closed || !lookup->predecessors_closed || !target ||
                   !adapter_text_equal(target->qualified_name, outcome.target))) ||
        (!exact && target_id > 0))
        return adapter_fail(adapter);
    if (exact &&
        (!strcmp(site->key.detail, "CALLS") || !strcmp(site->key.detail, "CALL_REFERENCE")) &&
        !callable_node(target))
        return adapter_fail(adapter);
    if (exact && !strcmp(site->key.detail, "USES_TYPE") && !cbm_label_is_type_like(target->label))
        return adapter_fail(adapter);
    cbm_resolve_proof_key_t key = site->key;
    key.binding_identity = outcome.target;
    key.lookup_closed = lookup->key.lookup_closed;
    if (exact)
        outcome.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    const cbm_resolve_proof_t *premises[] = {occurrence, semantic};
    const cbm_resolve_proof_t *joined = cbm_resolve_proof_derive(
        &adapter->proofs, CBM_PROOF_NAMED_JOIN, &site->source, &key, &outcome, premises, 2, true);
    outcome.proof = joined
                        ? cbm_resolve_proof_derive(&adapter->proofs, CBM_PROOF_OCCURRENCE_BINDING,
                                                   &site->source, &key, &outcome, &joined, 1, true)
                        : NULL;
    if (!outcome.proof)
        return adapter_fail(adapter);
    const cbm_resolve_proof_t *root = outcome.proof;
    outcome = root->value.outcome;
    outcome.proof = root;
    cbm_resolve_record_t record = {.file_path = site->source.file,
                                   .scope = site->source.scope,
                                   .relation = site->key.detail,
                                   .spelling = site->key.lookup_key,
                                   .properties_json = properties,
                                   .start_byte = site->key.start_byte,
                                   .end_byte = site->key.end_byte,
                                   .source_origin = site->key.origin,
                                   .result = outcome,
                                   .content_identity = site->source.content_identity,
                                   .configuration_identity = site->source.configuration_identity};
    record.emitted = cbm_resolve_record_admit(&record);
    if ((exact && !record.emitted) || !cbm_gbuf_record_resolution(adapter->ctx->gbuf, &record) ||
        (record.emitted && !cbm_gbuf_insert_edge(adapter->ctx->gbuf, owner_id, target_id,
                                                 record.relation, properties)))
        return adapter_fail(adapter);
    return true;
}

bool cbm_shared_resolver_adapter_record(cbm_shared_resolver_adapter_t *adapter,
                                        const cbm_resolve_proof_source_t *source, const char *bytes,
                                        size_t length, uint32_t start, uint32_t end,
                                        const char *spelling, int64_t owner, int64_t target,
                                        const char *relation, const char *properties,
                                        const cbm_resolve_proof_t *semantic) {
    const cbm_resolve_proof_t *site = cbm_shared_resolver_adapter_occurrence(
        adapter, source, bytes, length, start, end, spelling, relation);
    return site && cbm_shared_resolver_adapter_record_proofs(adapter, site, owner, target,
                                                             properties, semantic);
}
