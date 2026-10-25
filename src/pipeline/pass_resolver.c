#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/import_resolve.h"
#include "pipeline/shared_resolver.h"
#include "resolve/binding.h"
#include "resolve/lookup.h"
#include "resolve/policy.h"
#include "resolve/snapshot.h"
#include "resolve/solver.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct {
    const char *file;
    const char *label;
    uint32_t start_line, end_line;
    bool ambiguous;
} target_proof_t;

typedef struct {
    const char *module_qn;
    cbm_resolve_proof_source_t source;
    bool complete, ambiguous;
} namespace_proof_t;

typedef struct {
    const cbm_resolve_source_import_t *source;
    const namespace_proof_t *namespace;
    const char *file;
    const cbm_resolve_proof_t *path_proof;
    cbm_resolve_lookup_context_t namespace_context;
    bool system;
} source_import_t;

struct cbm_shared_resolver {
    CBMArena arena;
    CBMHashTable *exports;
    CBMHashTable *targets;
    CBMHashTable *namespaces;
    CBMHashTable *python_queries;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_solver_t *solver;
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
    semantic_site_t *sites;
    size_t site_count;
    const cbm_gbuf_node_t *file_node;
    bool source_matches;
} file_resolver_t;

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
    char *source = malloc((size_t)size + 1);
    if (!source) {
        fclose(file);
        return NULL;
    }
    size_t got = fread(source, 1, (size_t)size, file);
    bool failed = ferror(file) || got != (size_t)size;
    fclose(file);
    if (failed) {
        free(source);
        return NULL;
    }
    source[got] = '\0';
    *length = got;
    return source;
}

static char *file_source(const cbm_resolver_file_t *file, size_t *length) {
    const char *root = file->ctx ? file->ctx->repo_path : NULL;
    if (!root || !file->rel_path)
        return NULL;
    size_t root_len = strlen(root), rel_len = strlen(file->rel_path);
    if (root_len > SIZE_MAX - rel_len - 2)
        return NULL;
    char *path = malloc(root_len + rel_len + 2);
    if (!path)
        return NULL;
    memcpy(path, root, root_len);
    path[root_len] = '/';
    memcpy(path + root_len + 1, file->rel_path, rel_len + 1);
    char *source = load_source(path, length);
    free(path);
    return source;
}

static bool add_target(cbm_shared_resolver_t *resolver, const CBMDefinition *definition,
                       const char *file) {
    if (!definition->qualified_name || !definition->label || !file)
        return true;
    target_proof_t *prior = cbm_ht_get(resolver->targets, definition->qualified_name);
    if (prior) {
        prior->ambiguous |=
            strcmp(prior->file, file) != 0 || strcmp(prior->label, definition->label) != 0 ||
            prior->start_line != definition->start_line || prior->end_line != definition->end_line;
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
    if (!entry->file || !entry->label)
        return false;
    cbm_ht_set(resolver->targets, key, entry);
    return cbm_ht_get(resolver->targets, key) == entry;
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
    return proof && !proof->ambiguous && !strcmp(proof->file, node->file_path) &&
           !strcmp(proof->label, node->label) && node->start_line >= 0 && node->end_line >= 0 &&
           proof->start_line == (uint32_t)node->start_line &&
           proof->end_line == (uint32_t)node->end_line;
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

static bool export_file(cbm_shared_resolver_t *resolver, const cbm_pipeline_ctx_t *ctx,
                        const CBMFileResult *result, const cbm_file_info_t *file) {
    if (!export_language(file->language))
        return true;
    CBMArena arena;
    cbm_arena_init(&arena);
    size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
    char *owned = result->source ? NULL : load_source(file->path, &length);
    const char *source = result->source ? result->source : owned;
    if (!cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW)) {
        resolver->complete = false;
        cbm_arena_destroy(&arena);
        free(owned);
        return true;
    }
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, file->language, source, length);
    bool ok = index != NULL;
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
        const char *qn = result->defs.items[d].qualified_name;
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
            free(slot);
            ok = false;
            break;
        }
        if (slot && !module) {
            const char *dependency = export_solver_key(&resolver->arena, slot);
            ok = dependency &&
                 cbm_resolve_solver_copy(resolver->solver, fact_key, dependency, audit);
        } else {
            ok = cbm_resolve_solver_leaf(
                resolver->solver, fact_key,
                !audited ? CBM_SOLVER_UNSUPPORTED
                : slot   ? CBM_SOLVER_KNOWN
                         : CBM_SOLVER_UNKNOWN,
                (cbm_resolve_solver_value_t){.target = slot, .module = module}, audit);
        }
        /* A plain dotted import binds only a package root, not the loaded leaf
         * under that root's exported name. Exact module-path uses still retain
         * the private import fact above. */
        bool dotted_root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT &&
                           import->module_path && strchr(import->module_path, '.');
        char *key = cbm_pipeline_fqn_compute(ctx->project_name, file->rel_path, import->local_name);
        if (ok && key && !dotted_root) {
            const cbm_resolve_proof_t *exported =
                export_audit(resolver, result, file->rel_path, key, NULL, 0, audited,
                             "source_python_export_audit", NULL);
            ok = add_export(resolver, key, file->rel_path, 0, audited, fact_key, exported);
        }
        free(slot);
        free(key);
    }
    cbm_arena_destroy(&arena);
    free(owned);
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

cbm_shared_resolver_t *cbm_shared_resolver_build(const cbm_pipeline_ctx_t *ctx,
                                                 const cbm_file_info_t *files, int file_count,
                                                 CBMFileResult **cache) {
    if (!ctx || file_count < 0 || (file_count && (!files || !cache)))
        return NULL;
    cbm_shared_resolver_t *resolver = calloc(1, sizeof(*resolver));
    if (!resolver)
        return NULL;
    cbm_arena_init(&resolver->arena);
    resolver->exports = cbm_ht_create(128);
    resolver->targets = cbm_ht_create(128);
    resolver->namespaces = cbm_ht_create(128);
    resolver->python_queries = cbm_ht_create(128);
    resolver->project = cbm_arena_strdup(&resolver->arena, ctx->project_name);
    cbm_resolve_proof_pool_init(&resolver->proofs, &resolver->arena, 4000000, 4000000,
                                UINT64_C(64000000));
    resolver->solver = cbm_resolve_solver_new(&resolver->arena, &resolver->proofs,
                                              (cbm_resolve_solver_limits_t){0});
    resolver->complete = true;
    if (!resolver->exports || !resolver->targets || !resolver->namespaces ||
        !resolver->python_queries || !resolver->project || !resolver->solver) {
        cbm_shared_resolver_free(resolver);
        return NULL;
    }
    for (int f = 0; f < file_count; f++) {
        bool loaded = false;
        CBMFileResult *result = cbm_pipeline_result_acquire(ctx, cache, f, NULL, &loaded);
        if (!result) {
            resolver->complete = false;
            continue;
        }
        char extracted_summary[65];
        cbm_resolve_file_t snapshot;
        bool ok = cbm_resolve_snapshot(result, files[f].rel_path, files[f].language, &snapshot,
                                       extracted_summary) &&
                  cbm_gbuf_record_resolver_file(ctx->gbuf, &snapshot);
        for (int d = 0; ok && d < result->defs.count; d++)
            ok = add_target(resolver, &result->defs.items[d], files[f].rel_path);
        if (ok)
            ok = export_file(resolver, ctx, result, &files[f]);
        cbm_pipeline_result_release(result, loaded);
        if (!ok) {
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    if (!stabilize_exports(resolver) ||
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
        if (!cbm_gbuf_record_resolver_summary(ctx->gbuf, &summary)) {
            cbm_shared_resolver_free(resolver);
            return NULL;
        }
    }
    return resolver;
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
    cbm_arena_destroy(&resolver->arena);
    free(resolver);
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
    resolver->sites = calloc((size_t)rows->count, sizeof(*resolver->sites));
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
            CBMImport request = {.module_path = path};
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

static cbm_resolve_result_t generic_result(file_resolver_t *resolver, const char *name,
                                           const char *scope, uint32_t start, uint32_t end,
                                           CBMSourceOrigin origin, bool invocation) {
    const cbm_resolver_file_t *file = resolver->file;
    if (!name || !name[0])
        return unresolved("reference_spelling_missing");
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
    if (source &&
        (!source->file_path || strcmp(source->file_path, resolver->file->rel_path) ||
         (source != resolver->file_node && (!source->label || strcmp(source->label, "Module")) &&
          !target_materialized(resolver, source)))) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "source_declaration_identity_conflict";
    }
    if (target && !target_materialized(resolver, target)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "target_declaration_identity_conflict";
    }
    if (target && (strcmp(relation, "CALL_REFERENCE") == 0 || strcmp(relation, "CALLS") == 0) &&
        !callable_node(target) &&
        !(resolver->file->language == CBM_LANG_PYTHON && target->label &&
          !strcmp(target->label, "Class") && (result.facts & CBM_RESOLVE_FACT_CLASS_CALLABLE))) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "callable_target_not_callable";
    }
    if (target && strcmp(relation, "USES_TYPE") == 0 && !cbm_label_is_type_like(target->label)) {
        result.facts &= ~CBM_RESOLVE_FACT_EXACT_TARGET;
        result.reason = "type_target_not_type";
    }
    if (!resolver->source_matches) {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.reason = "source_snapshot_mismatch";
    }
    char properties[CBM_SZ_2K];
    int n = snprintf(properties, sizeof(properties),
                     "{\"strategy\":\"shared_resolver\",\"resolver_rule\":%d,"
                     "\"start_byte\":%u,\"end_byte\":%u,\"source_origin\":%u",
                     result.rule, start, end, (unsigned)origin);
    if (call)
        cbm_pipeline_close_call_props(properties, n, call, true);
    else if (n > 0 && (size_t)n + 1 < sizeof(properties)) {
        properties[n] = '}';
        properties[n + 1] = '\0';
    } else {
        return false;
    }
    cbm_resolve_record_t record = {
        .file_path = resolver->file->rel_path,
        .scope = source ? source->qualified_name : scope,
        .relation = relation,
        .spelling = name,
        .start_byte = start,
        .end_byte = end,
        .source_origin = origin,
        .result = result,
        .binding_proof = local_occurrence_proof(resolver, source ? source->qualified_name : scope,
                                                name, relation, start, end, origin, &result),
        .content_identity =
            origin >= CBM_SOURCE_ORIGIN_RAW && origin <= CBM_SOURCE_ORIGIN_PREPROCESSED
                ? resolver->file->result->source_content_id[origin]
                : "",
        .configuration_identity = resolver->file->result->source_configuration_id,
        .proof_borrowed = cbm_shared_resolver_owns_proof(resolver->shared, result.proof),
        .properties_json = properties};
    record.emitted = cbm_resolve_record_candidate(&record);
    if (record.emitted &&
        !cbm_gbuf_insert_edge(resolver->file->output, source->id, target->id, relation, properties))
        return false;
    return cbm_gbuf_record_resolution(resolver->file->output, &record);
}

static bool resolve_calls(file_resolver_t *resolver, CBMArena *arena) {
    const CBMFileResult *result = resolver->file->result;
    for (int c = 0; c < result->calls.count; c++) {
        const CBMCall *call = &result->calls.items[c];
        const semantic_site_t *site =
            find_semantic(resolver, call->enclosing_func_qn, call->site_start_byte,
                          call->site_end_byte, call->source_origin, CBM_RESOLVED_INVOCATION);
        cbm_resolve_result_t outcome =
            site ? semantic_result(resolver, site, arena)
            : call->requires_lsp_resolution
                ? unresolved("semantic_resolution_required")
                : generic_result(resolver, call->callee_name, call->enclosing_func_qn,
                                 call->site_start_byte, call->site_end_byte, call->source_origin,
                                 true);
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
    file_resolver_t resolver = {.shared = shared,
                                .file = file,
                                .arena = &arena,
                                .source_matches = cbm_result_source_matches(
                                    file->result, source, length, CBM_SOURCE_ORIGIN_RAW)};
    resolver.file_node =
        cbm_pipeline_file_node(file->graph, file->ctx->project_name, file->rel_path);
    resolver.bindings =
        cbm_resolve_binding_build(&arena, file->result, file->language, source, length);
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
    free(resolver.sites);
    cbm_arena_destroy(&arena);
    free(owned);
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
    const cbm_resolve_record_t **ordered = admitted ? malloc(admitted * sizeof(*ordered)) : NULL;
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
            free(ordered);
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
    free(ordered);
    return ok;
}
