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
#include "resolve/snapshot.h"
#include "resolve/solver.h"
#include "lsp/go_stabilize.h"
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

typedef struct {
    const char *file;
    const char *label;
    uint32_t start_line, end_line, second_start_line, second_end_line;
    unsigned declarations;
    conditional_owner_t *conditional;
    bool ambiguous;
    /* TypeScript's separate type space: one graph node carries a value and a
     * same-named type, so either declaration may be the materialized one. */
    const char *merged_label;
    uint32_t merged_start_line, merged_end_line;
    /* Only overload signatures so far; the implementation takes over. */
    bool signature_only;
    /* A same-QN declaration lives in another file, so no span can settle it. */
    bool cross_file;
} target_proof_t;

typedef struct {
    const char *module_qn;
    cbm_resolve_proof_source_t source;
    bool complete, ambiguous, python_mutated;
    const char **python_names;
    size_t python_name_count;
} namespace_proof_t;

typedef struct python_pending_import {
    const char *key, *slot, *parent_file, *child_file, *member, *escape_root, *escape_slot;
    const cbm_resolve_proof_t *audit;
    const char *loaded_module;
    const cbm_resolve_proof_t *load_audit;
    bool module, audited, escaped, escape_module;
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
    CBMHashTable *http_defaults, *http_sites, *http_unsafe;
    CBMHashTable *http_candidates, *http_veto;
    http_candidate_import_t *http_candidate_imports;
    bool http_domain_complete;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_solver_t *solver;
    cbm_go_stabilization_t *go_types;
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
static bool ecma_merge_target(target_proof_t *prior, const CBMDefinition *definition,
                              const char *file) {
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
        prior->merged_label = definition->label;
        prior->merged_start_line = definition->start_line;
        prior->merged_end_line = definition->end_line;
        return true;
    }
    return false;
}

static bool add_target(cbm_shared_resolver_t *resolver, const CBMDefinition *definition,
                       const char *file, CBMLanguage language) {
    if (!definition->qualified_name || !definition->label || !file)
        return true;
    target_proof_t *prior = cbm_ht_get(resolver->targets, definition->qualified_name);
    if (prior) {
        if ((language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX ||
             language == CBM_LANG_ARKTS) &&
            ecma_merge_target(prior, definition, file))
            return true;
        bool same_owner = !strcmp(prior->file, file) && !strcmp(prior->label, definition->label);
        prior->cross_file |= strcmp(prior->file, file) != 0;
        prior->ambiguous |= !same_owner || prior->start_line != definition->start_line ||
                            prior->end_line != definition->end_line || language == CBM_LANG_PYTHON;
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
                              const char *file, const char *source, size_t length, TSNode group,
                              TSNode block, unsigned branch) {
    uint32_t count = ts_node_named_child_count(block);
    for (uint32_t i = 0; i < count; i++) {
        TSNode function = ts_node_named_child(block, i);
        TSNode name = owner_field(function, "name"), body = owner_field(function, "body");
        if (!owner_kind(function, "function_definition") || !owner_kind(name, "identifier") ||
            !owner_kind(body, "block"))
            continue;
        uint32_t start = ts_node_start_byte(name), end = ts_node_end_byte(name);
        if (end <= start || end > length || end - start > INT_MAX)
            continue;
        const char *qn = cbm_arena_sprintf(&resolver->arena, "%s.%.*s", result->module_qn,
                                           (int)(end - start), source + start);
        if (!qn)
            return false;
        target_proof_t *target = cbm_ht_get(resolver->targets, qn);
        if (!target || !target->ambiguous || target->declarations != 2 ||
            strcmp(target->file, file) || strcmp(target->label, "Function") ||
            target->start_line == target->second_start_line)
            continue;
        uint32_t first = ts_node_start_point(function).row + 1;
        uint32_t last = ts_node_end_point(function).row + 1;
        unsigned member;
        if (first == target->start_line && last == target->end_line)
            member = 0;
        else if (first == target->second_start_line && last == target->second_end_line)
            member = 1;
        else
            continue;
        if (!target->conditional) {
            conditional_owner_t *owner = cbm_arena_calloc(&resolver->arena, sizeof(*owner));
            if (!owner)
                return false;
            owner->content = cbm_arena_strdup(&resolver->arena,
                                              result->source_content_id[CBM_SOURCE_ORIGIN_RAW]);
            owner->configuration =
                cbm_arena_strdup(&resolver->arena, result->source_configuration_id);
            if (!owner->content || !owner->configuration)
                return false;
            target->conditional = owner;
        }
        conditional_owner_t *owner = target->conditional;
        owner->invalid |= owner->members[member].branch != 0;
        owner->members[member] = (owner_variant_t){.start_line = first,
                                                   .end_line = last,
                                                   .start_byte = ts_node_start_byte(function),
                                                   .end_byte = ts_node_end_byte(function),
                                                   .body_start = ts_node_start_byte(body),
                                                   .body_end = ts_node_end_byte(body),
                                                   .group_start = ts_node_start_byte(group),
                                                   .group_end = ts_node_end_byte(group),
                                                   .branch = branch};
    }
    return true;
}

/* Audit one raw syntax tree in linear time. This deliberately supports only
 * direct module-level if/else bodies. elif, nested groups, decorated functions,
 * other languages and cross-file build variants remain quality gaps. */
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
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode group = ts_node_named_child(root, i);
        if (!owner_kind(group, "if_statement"))
            continue;
        TSNode first = owner_field(group, "consequence");
        TSNode alternative = owner_field(group, "alternative");
        TSNode second = owner_field(alternative, "body");
        if (!owner_kind(first, "block") || !owner_kind(alternative, "else_clause") ||
            !owner_kind(second, "block"))
            continue;
        if (!note_owner_branch(resolver, result, file, source, length, group, first, 1) ||
            !note_owner_branch(resolver, result, file, source, length, group, second, 2))
            return false;
    }
    return true;
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
    if (!proof || proof->ambiguous || strcmp(proof->file, node->file_path) ||
        node->start_line < 0 || node->end_line < 0)
        return false;
    if (proof->merged_label && !strcmp(proof->merged_label, node->label) &&
        proof->merged_start_line == (uint32_t)node->start_line &&
        proof->merged_end_line == (uint32_t)node->end_line)
        return true;
    return !strcmp(proof->label, node->label) && proof->start_line == (uint32_t)node->start_line &&
           proof->end_line == (uint32_t)node->end_line;
}

/* JavaScript nests functions freely and extraction names them flat, so two
 * `const handle` of one file share a QN and one graph node, showing the last. That
 * node is still exactly right for an occurrence inside its own span. */
static bool ecma_source_span_settles(const file_resolver_t *resolver, const cbm_gbuf_node_t *node,
                                     uint32_t start, const char *source, size_t length) {
    const target_proof_t *proof =
        resolver->shared ? cbm_ht_get(resolver->shared->targets, node->qualified_name) : NULL;
    if (!proof || !proof->ambiguous || proof->cross_file || !source || start > length ||
        node->start_line <= 0 || !ecma_language(resolver->file->language))
        return false;
    int line = 1;
    for (uint32_t at = 0; at < start; at++)
        line += source[at] == '\n';
    return line >= node->start_line && line <= node->end_line;
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

static bool capture_python_namespace(cbm_shared_resolver_t *resolver, const CBMFileResult *result,
                                     const char *file, const cbm_resolve_binding_index_t *index) {
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
    if (!cbm_resolve_binding_python_effects_complete(index))
        resolver->python_unknown_namespace_escape = true;
    size_t count = cbm_resolve_binding_python_namespace_size(index);
    if (count > SIZE_MAX / sizeof(*entry->python_names))
        return false;
    entry->python_names =
        count ? cbm_arena_alloc(&resolver->arena, count * sizeof(*entry->python_names)) : NULL;
    if (count && !entry->python_names)
        return false;
    for (size_t i = 0; i < count; i++) {
        const char *name = cbm_resolve_binding_python_namespace_name(index, i);
        if (!name)
            continue;
        const char *copy = cbm_arena_strdup(&resolver->arena, name);
        if (!copy)
            return false;
        entry->python_names[entry->python_name_count++] = copy;
    }
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

static bool defer_python_import(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                                const char *file, const CBMImport *import, const char *key,
                                const char *slot, bool module, bool audited, bool escaped,
                                const cbm_resolve_proof_t *audit, const char *loaded_module,
                                const cbm_resolve_proof_t *load_audit) {
    python_pending_import_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    if (!entry)
        return false;
    entry->key = cbm_arena_strdup(&resolver->arena, key);
    entry->slot = slot ? cbm_arena_strdup(&resolver->arena, slot) : NULL;
    entry->module = module;
    entry->audited = audited;
    entry->escaped = escaped;
    entry->audit = audit;
    entry->loaded_module = loaded_module;
    entry->load_audit = load_audit;
    if (!entry->key || (slot && !entry->slot))
        return false;
    if (escaped) {
        char *effect = cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, import,
                                                       &entry->escape_module);
        if (effect) {
            entry->escape_slot = cbm_arena_strdup(&resolver->arena, effect);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, effect);
            if (!entry->escape_slot)
                return false;
        }
    }
    if (escaped && import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && import->local_name) {
        CBMImport root = *import;
        root.module_path = import->local_name;
        root.kind = CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS;
        bool is_module = false;
        char *bound =
            cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file, &root, &is_module);
        if (bound) {
            entry->escape_root = cbm_arena_strdup(&resolver->arena, bound);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, bound);
            if (!entry->escape_root)
                return false;
        }
    }
    const cbm_gbuf_node_t *parent, *child;
    if (audited && !module &&
        cbm_pipeline_python_submodule_candidate(ctx->gbuf, ctx->project_name, file, import, &parent,
                                                &child)) {
        entry->parent_file = cbm_arena_strdup(&resolver->arena, parent->file_path);
        entry->child_file = cbm_arena_strdup(&resolver->arena, child->file_path);
        entry->member = cbm_arena_strdup(&resolver->arena, strrchr(import->module_path, '.') + 1);
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
    /* Sequential extraction releases raw buffers. Audit the same verified
     * bytes used for exports instead of requiring parallel source retention.
     * The owner summary copies identities and stores spans, never this buffer. */
    bool ok = index && note_conditional_owners(resolver, result, file->rel_path, file->language,
                                               source, length);
    if (ok && file->language == CBM_LANG_PYTHON)
        ok = capture_python_namespace(resolver, result, file->rel_path, index);
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
        if (file->language == CBM_LANG_PYTHON && definition->label &&
            !strcmp(definition->label, "Module"))
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
        bool audited = cbm_resolve_binding_import(index, import->local_name);
        bool module = false;
        char *slot = audited ? cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name,
                                                               file->rel_path, import, &module)
                             : NULL;
        const char *fact_key = python_import_key(&resolver->arena, file->rel_path, i);
        const cbm_resolve_proof_t *audit =
            fact_key ? export_audit(resolver, result, file->rel_path, fact_key, slot,
                                    (unsigned)import->kind, audited, "source_python_import_audit",
                                    import)
                     : NULL;
        if (!fact_key || !audit) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, slot);
            ok = false;
            break;
        }
        bool escaped = cbm_resolve_binding_python_import_escaped(index, import->local_name);
        const char *loaded = NULL;
        if (!python_loaded_module(resolver, ctx, file->rel_path, import, &loaded)) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, slot);
            ok = false;
            break;
        }
        const cbm_resolve_proof_t *load_audit =
            loaded ? export_audit(resolver, result, file->rel_path, fact_key, loaded,
                                  (unsigned)import->kind, false,
                                  "python_explicit_module_load_barrier", import)
                   : NULL;
        ok = (!loaded || load_audit) &&
             defer_python_import(resolver, ctx, file->rel_path, import, fact_key, slot, module,
                                 audited, escaped, audit, loaded, load_audit);
        /* A plain dotted import binds only a package root, not the loaded leaf
         * under that root's exported name. Exact module-path uses still retain
         * the private import fact above. */
        bool dotted_root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT &&
                           import->module_path && strchr(import->module_path, '.');
        const char *key =
            cbm_fqn_compute(&arena, ctx->project_name, file->rel_path, import->local_name);
        if (ok && key && !dotted_root) {
            const cbm_resolve_proof_t *exported =
                export_audit(resolver, result, file->rel_path, key, NULL, 0, audited,
                             "source_python_export_audit", NULL);
            ok = add_export(resolver, key, file->rel_path, 0, audited, fact_key, exported);
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, slot);
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

static bool python_namespace_absent(const namespace_proof_t *parent, const char *name) {
    if (!parent || !parent->complete || parent->ambiguous || parent->python_mutated || !name)
        return false;
    size_t low = 0, high = parent->python_name_count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (strcmp(parent->python_names[middle], name) < 0)
            low = middle + 1;
        else
            high = middle;
    }
    return low == parent->python_name_count || strcmp(parent->python_names[low], name) != 0;
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
    for (const python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        if (!import->loaded_module)
            continue;
        char *module = cbm_arena_strdup(&resolver->arena, import->loaded_module);
        if (!module || !import->load_audit)
            return false;
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
                !add_export(resolver, module, loaded->source.file, 0, false, NULL,
                            import->load_audit))
                return false;
            char *dot = strrchr(module, '.');
            if (!dot)
                break;
            *dot = '\0';
            if (loaded)
                invalidate_python_namespace(resolver, module);
        }
    }
    return true;
}

static void collect_python_namespace_effects(cbm_shared_resolver_t *resolver) {
    for (const python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        if (!import->escaped)
            continue;
        invalidate_python_namespace(resolver, import->escape_root);
        if (import->escape_module) {
            invalidate_python_namespace(resolver, import->escape_slot);
            continue;
        }
        /* Only a source-audited direct callable declaration excludes a module
         * alias here. A graph Variable alone proves no value identity. Pending
         * re-exports stay unknown, independent of file/import iteration order. */
        const target_proof_t *target =
            import->escape_slot ? cbm_ht_get(resolver->targets, import->escape_slot) : NULL;
        const export_proof_t *declaration =
            import->escape_slot ? cbm_ht_get(resolver->exports, import->escape_slot) : NULL;
        bool callable = target && !target->ambiguous && target->label &&
                        (!strcmp(target->label, "Function") || !strcmp(target->label, "Class")) &&
                        declaration && declaration->supported && !declaration->ambiguous &&
                        !declaration->dependency;
        if (import->escape_slot && !callable)
            resolver->python_unknown_namespace_escape = true;
    }
}

static bool settle_python_imports(cbm_shared_resolver_t *resolver) {
    if (!collect_python_module_loads(resolver))
        return false;
    collect_python_namespace_effects(resolver);
    for (python_pending_import_t *import = resolver->python_imports; import;
         import = import->next) {
        const namespace_proof_t *parent =
            import->parent_file ? cbm_ht_get(resolver->namespaces, import->parent_file) : NULL;
        const namespace_proof_t *child =
            import->child_file ? cbm_ht_get(resolver->namespaces, import->child_file) : NULL;
        if (!resolver->python_unknown_namespace_escape && child && !child->ambiguous &&
            !child->python_mutated && python_namespace_absent(parent, import->member)) {
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
    return true;
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
    return map;
}

static const char *python_query_key(CBMArena *arena, const char *import_key, const char *suffix) {
    return cbm_arena_sprintf(arena, "python_member:%zu:%s:%zu:%s", strlen(import_key), import_key,
                             strlen(suffix), suffix);
}

static bool collect_python_query(cbm_shared_resolver_t *resolver, CBMArena *arena,
                                 const CBMHashTable *imports, const char *name) {
    if (!name || !name[0])
        return true;
    char *prefix = cbm_arena_strdup(arena, name);
    if (!prefix)
        return false;
    size_t length = strlen(name);
    while (length) {
        prefix[length] = '\0';
        const python_import_t *import = cbm_ht_get(imports, prefix);
        if (import) {
            const char *suffix = name[length] ? name + length + 1 : NULL;
            if (import->ambiguous || !suffix || !suffix[0] || strchr(suffix, '.'))
                return true;
            cbm_resolve_solver_result_t binding;
            cbm_resolve_solver_get(resolver->solver, import->slot, &binding);
            if (binding.status != CBM_SOLVER_KNOWN || !binding.value.module)
                return true;
            const char *key = python_query_key(arena, import->slot, suffix);
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
            *query =
                (python_query_t){.key = saved_key,
                                 .import_key = cbm_arena_strdup(&resolver->arena, import->slot),
                                 .suffix = cbm_arena_strdup(&resolver->arena, suffix)};
            if (!query->import_key || !query->suffix)
                return false;
            cbm_ht_set(resolver->python_queries, saved_key, query);
            return cbm_ht_get(resolver->python_queries, saved_key) == query;
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
        bool ok = imports != NULL;
        for (int i = 0; ok && i < result->calls.count; i++)
            ok =
                collect_python_query(resolver, &arena, imports, result->calls.items[i].callee_name);
        for (int i = 0; ok && i < result->usages.count; i++)
            ok = collect_python_query(resolver, &arena, imports, result->usages.items[i].ref_name);
        for (int i = 0; ok && i < result->rw.count; i++)
            ok = collect_python_query(resolver, &arena, imports, result->rw.items[i].var_name);
        for (int i = 0; ok && i < result->type_refs.count; i++)
            ok = collect_python_query(resolver, &arena, imports,
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
    resolver->complete = true;
    if (!resolver->exports || !resolver->targets || !resolver->namespaces ||
        !resolver->python_queries || !resolver->python_namespaces || !resolver->http_defaults ||
        !resolver->http_sites || !resolver->http_unsafe || !resolver->http_candidates ||
        !resolver->http_veto || !resolver->project || !resolver->solver || !resolver->go_types ||
        !resolver->ecma) {
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
        cbm_pipeline_result_release(result, loaded);
        if (!ok) {
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    if (!settle_python_imports(resolver) || !http_finalize_veto(resolver) ||
        !cbm_go_stabilization_register(resolver->go_types) || !stabilize_exports(resolver) ||
        !cbm_go_stabilization_seal(resolver->go_types) ||
        !prepare_python_queries(resolver, ctx, files, file_count, cache)) {
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
        if (strncmp(binding->key, "go/", 3))
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
    cbm_ht_free(resolver->http_defaults);
    cbm_ht_free(resolver->http_sites);
    cbm_ht_free(resolver->http_unsafe);
    cbm_ht_free(resolver->http_candidates);
    cbm_ht_free(resolver->http_veto);
    cbm_go_stabilization_dispose(resolver->go_types);
    cbm_ecma_table_free(resolver->ecma);
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

static bool python_import_result(file_resolver_t *resolver, const char *name,
                                 cbm_resolve_result_t *result) {
    if (!resolver->python_imports)
        return false;
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
        if (invocation && node && node->label && !strcmp(node->label, "Class"))
            facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
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
    if (file->language == CBM_LANG_DART) {
        cbm_resolve_result_t proof = dart_name_result(resolver, name);
        return cbm_resolve_binding_validate(resolver->bindings, name, start, end, origin, proof);
    }
    if (file->language == CBM_LANG_PYTHON) {
        cbm_resolve_result_t proof =
            cbm_resolve_binding_local(resolver->bindings, name, start, end, origin);
        if (cbm_resolve_admit(&proof))
            return proof;
        if (python_import_result(resolver, name, &proof))
            return proof.target ? cbm_resolve_binding_validate(resolver->bindings, name, start, end,
                                                               origin, proof)
                                : proof;
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

static const cbm_gbuf_node_t *source_node(const file_resolver_t *resolver, const char *scope) {
    if (!scope || !scope[0])
        return resolver->file_node;
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, scope);
    return node && !cbm_pipeline_node_is_dir_container(node) ? node : NULL;
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
    const cbm_resolve_proof_t *local =
        cbm_resolve_binding_site_proof(resolver->bindings, name, start, end, origin);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(local);
    cbm_resolve_lookup_context_t *context = cbm_resolve_binding_lookup_context(resolver->bindings);
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
    if (source &&
        (!source->file_path || strcmp(source->file_path, resolver->file->rel_path) ||
         (source != resolver->file_node && (!source->label || strcmp(source->label, "Module")) &&
          !target_materialized(resolver, source) && !conditional &&
          !ecma_source_span_settles(resolver, source, start, resolver->source,
                                    resolver->source_length)))) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "source_declaration_identity_conflict";
    }
    if (target && !target_materialized(resolver, target) &&
        !ecma_target_span_settles(resolver, target, name, start, end, origin)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "target_declaration_identity_conflict";
    }
    if (target && (strcmp(relation, "CALL_REFERENCE") == 0 || strcmp(relation, "CALLS") == 0) &&
        !callable_node(target) &&
        !((resolver->file->language == CBM_LANG_PYTHON || ecma_language(resolver->file->language) ||
           resolver->file->language == CBM_LANG_VUE ||
           resolver->file->language == CBM_LANG_SVELTE) &&
          target->label && !strcmp(target->label, "Class") &&
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
    for (int c = 0; c < result->calls.count; c++) {
        const CBMCall *call = &result->calls.items[c];
        const semantic_site_t *site =
            find_semantic(resolver, call->enclosing_func_qn, call->site_start_byte,
                          call->site_end_byte, call->source_origin, CBM_RESOLVED_INVOCATION);
        /* Exact simple JSX tags can obtain an independent lexical proof. An
         * existing semantic result, including ambiguity, remains authoritative. */
        bool lexical_jsx = call->requires_lsp_resolution &&
                           cbm_resolve_binding_jsx_occurrence(
                               resolver->bindings, call->callee_name, call->site_start_byte,
                               call->site_end_byte, call->source_origin);
        cbm_resolve_result_t outcome =
            site ? semantic_result(resolver, site, arena)
            : call->requires_lsp_resolution && !lexical_jsx
                ? unresolved("semantic_resolution_required")
                : generic_result(resolver, call->callee_name, call->enclosing_func_qn,
                                 call->site_start_byte, call->site_end_byte, call->source_origin,
                                 true);
        if (resolver->file->language == CBM_LANG_GO && resolver->source_matches &&
            call->source_origin == CBM_SOURCE_ORIGIN_RAW)
            (void)cbm_go_stabilization_call(resolver->shared->go_types, resolver->file->rel_path,
                                            call->enclosing_func_qn, call->site_start_byte,
                                            call->site_end_byte, &outcome);
        if (!record_result(resolver, call->enclosing_func_qn, call->callee_name, "CALLS",
                           call->site_start_byte, call->site_end_byte, call->source_origin, outcome,
                           call))
            return false;
    }
    return true;
}

static bool resolve_usages(file_resolver_t *resolver, CBMArena *arena) {
    const CBMFileResult *result = resolver->file->result;
    for (int u = 0; u < result->usages.count; u++) {
        const CBMUsage *usage = &result->usages.items[u];
        const char *relation = usage->kind == CBM_USAGE_CALL_REFERENCE ? "CALL_REFERENCE" : "USAGE";
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
        cbm_resolve_result_t outcome =
            site ? semantic_result(resolver, site, arena)
            : usage->is_member_access
                ? unresolved("receiver_type_required")
                : generic_result(resolver, usage->ref_name, usage->enclosing_func_qn,
                                 usage->site_start_byte, usage->site_end_byte, usage->source_origin,
                                 false);
        /* Source binding can establish a direct callable argument without a
         * hybrid row. Composite expressions remain USAGE: the extraction
         * carrier deliberately leaves may_be_call_reference false for them. */
        if (!site && usage->may_be_call_reference && cbm_resolve_admit(&outcome) &&
            callable_node(cbm_gbuf_find_by_qn(resolver->file->graph, outcome.target)))
            relation = "CALL_REFERENCE";
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
        cbm_resolve_result_t outcome =
            site ? semantic_result(resolver, site, arena)
            : rw->is_member_access
                ? unresolved("receiver_type_required")
                : generic_result(resolver, rw->var_name, rw->enclosing_func_qn, rw->site_start_byte,
                                 rw->site_end_byte, rw->source_origin, false);
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
        cbm_resolve_result_t outcome =
            site ? semantic_result(resolver, site, resolver->arena)
            : !type->site_is_exact
                ? unresolved("type_occurrence_identity_missing")
                : generic_result(resolver, type->type_name, type->enclosing_func_qn,
                                 type->site_start_byte, type->site_end_byte, type->source_origin,
                                 false);
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
    file_resolver_t resolver = {.shared = shared,
                                .file = file,
                                .arena = &arena,
                                .source = source,
                                .source_length = length,
                                .source_matches = cbm_result_source_matches(
                                    file->result, source, length, CBM_SOURCE_ORIGIN_RAW)};
    resolver.file_node =
        cbm_pipeline_file_node(file->graph, file->ctx->project_name, file->rel_path);
    resolver.bindings =
        binding_build_for(&arena, file->result, file->language, file->rel_path, source, length);
    bool begun = cbm_gbuf_resolution_begin(file->output);
    bool ok = begun && resolver.bindings &&
              cbm_resolve_binding_set_identity(resolver.bindings, file->ctx->project_name,
                                               file->rel_path) &&
              prepare_source_imports(&resolver) && index_semantics(&resolver) &&
              resolve_calls(&resolver, &arena) && resolve_usages(&resolver, &arena) &&
              resolve_writes(&resolver, &arena) && resolve_types(&resolver);
    if (begun)
        cbm_gbuf_resolution_end(file->output);
    cbm_ht_free(resolver.import_names);
    cbm_ht_free(resolver.python_imports);
    cbm_ht_free(resolver.ecma_dependencies);
    cbm_free(CBM_MEM_CLASS_EXTRACT, resolver.sites);
    if (owner_tree)
        ts_tree_delete(owner_tree);
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
    for (size_t i = 0; ok && i < count; i++)
        ok = adapter_index_snapshot(index, &arena, &recorded[i]);
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
        const cbm_resolve_file_t *old =
            ok && snapshot.file_path ? cbm_ht_get(index, snapshot.file_path) : NULL;
        if (ok && old) {
            ok = adapter_snapshot_equal(old, &snapshot);
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
