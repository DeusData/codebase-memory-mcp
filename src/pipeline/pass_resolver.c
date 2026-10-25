#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/import_resolve.h"
#include "pipeline/shared_resolver.h"
#include "resolve/binding.h"
#include "resolve/policy.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Export identity and its declaring source must both agree with the graph.
 * Two files collapsed to one QN are ambiguous, even if graph upsert chose one.
 */
typedef struct {
    const char *file;
    bool supported;
    bool ambiguous;
    unsigned kind;
    const char *redirect;
    bool module;
} export_proof_t;

typedef struct {
    const char *slot;
    bool module, ambiguous;
} python_import_t;

typedef struct {
    const char *file;
    const char *label;
    uint32_t start_line, end_line;
    bool ambiguous;
} target_proof_t;

typedef struct {
    const char *module_qn;
    bool complete, ambiguous;
} namespace_proof_t;

typedef struct {
    const cbm_resolve_source_import_t *source;
    const namespace_proof_t *namespace;
    const char *file;
    bool system;
} source_import_t;

struct cbm_shared_resolver {
    CBMArena arena;
    CBMHashTable *exports;
    CBMHashTable *targets;
    CBMHashTable *namespaces;
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

static bool add_export(cbm_shared_resolver_t *resolver, const char *qn, const char *file,
                       bool supported) {
    export_proof_t *old = cbm_ht_get(resolver->exports, qn);
    if (old) {
        old->ambiguous |= strcmp(old->file, file) != 0;
        old->supported &= supported;
        return true;
    }
    export_proof_t *entry = cbm_arena_calloc(&resolver->arena, sizeof(*entry));
    char *key = cbm_arena_strdup(&resolver->arena, qn);
    char *path = cbm_arena_strdup(&resolver->arena, file);
    if (!entry || !key || !path)
        return false;
    *entry = (export_proof_t){.file = path, .supported = supported};
    cbm_ht_set(resolver->exports, key, entry);
    return cbm_ht_get(resolver->exports, key) == entry;
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
                *proof = (namespace_proof_t){
                    .module_qn = module, .complete = cbm_resolve_binding_namespace_complete(index)};
                cbm_ht_set(resolver->namespaces, key, proof);
                ok = cbm_ht_get(resolver->namespaces, key) == proof;
            }
        }
        size_t count = cbm_resolve_binding_namespace_size(index);
        for (size_t n = 0; ok && n < count; n++) {
            unsigned kind = 0;
            const char *qn = cbm_resolve_binding_namespace_member(index, n, &kind);
            if (qn)
                ok = add_export(resolver, qn, file->rel_path,
                                (kind & CBM_RESOLVE_EXPORT_VALUE) != 0);
        }
    }
    for (int d = 0; ok && file->language != CBM_LANG_DART && d < result->defs.count; d++) {
        const char *qn = result->defs.items[d].qualified_name;
        if (qn && qn[0]) {
            unsigned kind = cbm_resolve_binding_export_kind(index, qn);
            ok = add_export(resolver, qn, file->rel_path, (kind & CBM_RESOLVE_EXPORT_VALUE) != 0);
            export_proof_t *proof = cbm_ht_get(resolver->exports, qn);
            if (proof)
                proof->kind = kind;
        }
    }
    /* Source-audited namespace links, separate from graph declarations. A
     * re-export can be proved without inventing a node in the exporting file. */
    for (int i = 0; ok && file->language == CBM_LANG_PYTHON && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!cbm_resolve_binding_import(index, import->local_name))
            continue;
        bool module = false;
        char *slot = cbm_pipeline_python_import_slot(ctx->gbuf, ctx->project_name, file->rel_path,
                                                     import, &module);
        char *key = cbm_pipeline_fqn_compute(ctx->project_name, file->rel_path, import->local_name);
        /* Plain dotted imports bind only a package root; they cannot export
         * the loaded leaf module under that root's name. */
        bool dotted_root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT &&
                           import->module_path && strchr(import->module_path, '.');
        if (slot && key && !dotted_root) {
            ok = add_export(resolver, key, file->rel_path, true);
            export_proof_t *proof = cbm_ht_get(resolver->exports, key);
            if (proof) {
                proof->redirect = cbm_arena_strdup(&resolver->arena, slot);
                proof->module = module;
                ok = proof->redirect != NULL;
            }
        }
        free(slot);
        free(key);
    }
    cbm_arena_destroy(&arena);
    free(owned);
    return ok;
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
    resolver->complete = true;
    if (!resolver->exports || !resolver->targets || !resolver->namespaces) {
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
        bool ok = true;
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
    return resolver;
}

void cbm_shared_resolver_free(cbm_shared_resolver_t *resolver) {
    if (!resolver)
        return;
    cbm_ht_free(resolver->exports);
    cbm_ht_free(resolver->targets);
    cbm_ht_free(resolver->namespaces);
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
        if (row->site_end_byte <= row->site_start_byte ||
            row->evidence.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            row->evidence.rule == CBM_RESOLVE_RULE_NONE ||
            !(row->evidence.facts & CBM_RESOLVE_FACT_LOOKUP_COMPLETE))
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
            if (site->row->evidence.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE)
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
                                   .reason = "semantic_binding_evidence"};
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
    resolver->python_imports = cbm_ht_create(32);
    if (!resolver->python_imports)
        return false;
    const cbm_resolver_file_t *file = resolver->file;
    for (int i = 0; i < file->result->imports.count; i++) {
        const CBMImport *import = &file->result->imports.items[i];
        if (!import->local_name || !cbm_import_binds_runtime_name(import))
            continue;
        bool root = import->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && import->module_path &&
                    strchr(import->module_path, '.');
        const char *name = root ? import->module_path : import->local_name;
        python_import_t *prior = cbm_ht_get(resolver->python_imports, name);
        if (prior) {
            prior->ambiguous = true;
            continue;
        }
        python_import_t *entry = cbm_arena_calloc(resolver->arena, sizeof(*entry));
        const char *key = cbm_arena_strdup(resolver->arena, name);
        if (!entry || !key)
            return false;
        char *slot = cbm_pipeline_python_import_slot(file->graph, file->ctx->project_name,
                                                     file->rel_path, import, &entry->module);
        if (slot) {
            entry->slot = cbm_arena_strdup(resolver->arena, slot);
            free(slot);
            if (!entry->slot)
                return false;
        }
        cbm_ht_set(resolver->python_imports, key, entry);
        if (cbm_ht_get(resolver->python_imports, key) != entry)
            return false;
    }
    return true;
}

static cbm_resolve_result_t python_export_result(file_resolver_t *resolver, const char *slot,
                                                 const char *suffix, bool module) {
    if (!slot)
        return unresolved("import_target_unresolved");
    if (!resolver->shared || !resolver->shared->complete)
        return unresolved("python_import_namespace_unavailable");
    for (unsigned depth = 0; depth < 32; depth++) {
        if (module) {
            if (!suffix || !suffix[0] || strchr(suffix, '.'))
                return unresolved("python_module_member_path_unsupported");
            slot = cbm_arena_sprintf(resolver->arena, "%s.%s", slot, suffix);
            suffix = NULL;
        }
        const export_proof_t *proof = slot ? cbm_ht_get(resolver->shared->exports, slot) : NULL;
        if (!proof || !proof->supported || proof->ambiguous)
            return unresolved("python_export_unproven");
        if (proof->redirect) {
            slot = proof->redirect;
            module = proof->module;
            continue;
        }
        if (suffix && suffix[0])
            return unresolved("python_export_member_dispatch_unproven");
        const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(resolver->file->graph, slot);
        if (!node || !node->file_path || strcmp(node->file_path, proof->file))
            return unresolved("python_export_declaration_not_materialized");
        return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                               CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                               CBM_RESOLVE_FACT_IMPORT_BOUND |
                                               ((proof->kind & CBM_RESOLVE_EXPORT_TYPE) &&
                                                        node->label && !strcmp(node->label, "Class")
                                                    ? CBM_RESOLVE_FACT_CLASS_CALLABLE
                                                    : 0),
                                      .target = slot,
                                      .candidate_count = 1,
                                      .reason = "python_source_export_chain"};
    }
    return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_LIMITED,
                                  .reason = "python_export_chain_budget_or_cycle"};
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
            *result =
                import->ambiguous
                    ? unresolved("python_import_binding_conflict")
                    : python_export_result(resolver, import->slot,
                                           name[length] ? name + length + 1 : NULL, import->module);
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
    for (size_t i = 0; i < resolver->import_count; i++) {
        source_import_t *import = &resolver->imports[i];
        import->source = cbm_resolve_binding_import_at(resolver->bindings, i);
        const char *uri = import->source ? import->source->uri : NULL;
        if (!uri)
            continue;
        import->system = strncmp(uri, "dart:", 5) == 0;
        if (import->system || strchr(uri, ':') || uri[0] == '/')
            continue;
        const char *path = uri[0] == '.' ? uri : cbm_arena_sprintf(resolver->arena, "./%s", uri);
        if (!path)
            return false;
        CBMImport request = {.module_path = path};
        const cbm_gbuf_node_t *target = NULL;
        cbm_resolve_result_t proof = cbm_pipeline_resolve_import_result(
            resolver->file->ctx, resolver->file->rel_path,
            resolver->file_node ? resolver->file_node->qualified_name : NULL, &request, NULL,
            &target);
        if (cbm_resolve_admit(&proof) && target && target->file_path && resolver->shared) {
            import->file = target->file_path;
            import->namespace = cbm_ht_get(resolver->shared->namespaces, import->file);
        }
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

static cbm_resolve_result_t dart_imported_name(file_resolver_t *resolver, const char *name) {
    cbm_resolve_result_t result = unresolved("dart_import_binding_missing");
    result.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
    const char *target = NULL;
    bool incomplete = !resolver->shared || !resolver->shared->complete;
    bool unsupported = false;
    uint32_t candidates = 0;
    /* This subset resolves unprefixed library names. Qualified selectors need
     * their own occurrence extraction and do not become lexical identifiers. */
    if (strchr(name, '.'))
        return unresolved("dart_prefix_occurrence_required");
    for (size_t i = 0; i < resolver->import_count; i++) {
        if (++resolver->import_work > 65536) {
            result.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
            result.reason = "dart_import_domain_work_limit";
            return result;
        }
        const source_import_t *import = &resolver->imports[i];
        if (!import->source || import->source->prefix || !import_exposes(import->source, name))
            continue;
        /* Dart non-system declarations shadow same-name system declarations
         * (§19.4). System names alone never establish a project target. */
        if (import->system)
            continue;
        if (!import->namespace || !import->namespace->complete || import->namespace->ambiguous) {
            incomplete = true;
            continue;
        }
        const char *qn =
            cbm_arena_sprintf(resolver->arena, "%s.%s", import->namespace->module_qn, name);
        if (!qn)
            return unresolved("dart_import_name_allocation");
        const export_proof_t *export = cbm_ht_get(resolver->shared->exports, qn);
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
        unsupported |= !export->supported || !export_complete(resolver, qn);
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
    return result;
}

static cbm_resolve_result_t dart_name_result(file_resolver_t *resolver, const char *name) {
    if (!cbm_resolve_binding_namespace_complete(resolver->bindings))
        return unresolved(cbm_resolve_binding_status(resolver->bindings));
    /* Any local spelling, including an unsupported binder, takes precedence.
     * A missing/ambiguous local candidate must not fall through to imports. */
    if (cbm_resolve_binding_name_present(resolver->bindings, name)) {
        cbm_resolution_t local =
            cbm_registry_resolve_lang(resolver->file->registry, name, resolver->file->module_qn,
                                      NULL, NULL, 0, CBM_LANG_DART);
        return local.proof;
    }
    cbm_resolve_result_t *cached = cbm_ht_get(resolver->import_names, name);
    if (cached)
        return *cached;
    cached = cbm_arena_alloc(resolver->arena, sizeof(*cached));
    char *key = cbm_arena_strdup(resolver->arena, name);
    if (!cached || !key)
        return unresolved("dart_import_cache_allocation");
    *cached = dart_imported_name(resolver, name);
    cbm_ht_set(resolver->import_names, key, cached);
    if (cbm_ht_get(resolver->import_names, key) != cached)
        return unresolved("dart_import_cache_allocation");
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
    cbm_resolution_t candidate =
        invocation
            ? cbm_registry_resolve_call_scoped(file->registry, name, file->module_qn,
                                               file->import_keys, file->import_values,
                                               file->import_count, file->language, scope)
            : cbm_registry_resolve_lang(file->registry, name, file->module_qn, file->import_keys,
                                        file->import_values, file->import_count, file->language);
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
    cbm_resolve_record_t record = {.file_path = resolver->file->rel_path,
                                   .scope = source ? source->qualified_name : scope,
                                   .relation = relation,
                                   .spelling = name,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .source_origin = origin,
                                   .result = result,
                                   .properties_json = properties};
    record.emitted = cbm_resolve_record_admit(&record);
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
    file_resolver_t resolver = {.shared = shared, .file = file, .arena = &arena};
    resolver.file_node =
        cbm_pipeline_file_node(file->graph, file->ctx->project_name, file->rel_path);
    resolver.bindings =
        cbm_resolve_binding_build(&arena, file->result, file->language, source, length);
    bool ok = resolver.bindings && prepare_source_imports(&resolver) &&
              index_semantics(&resolver) && resolve_calls(&resolver, &arena) &&
              resolve_usages(&resolver, &arena) && resolve_writes(&resolver, &arena) &&
              resolve_types(&resolver);
    cbm_ht_free(resolver.import_names);
    cbm_ht_free(resolver.python_imports);
    free(resolver.sites);
    cbm_arena_destroy(&arena);
    free(owned);
    return ok;
}

static bool controlled_relation(const char *relation) {
    static const char *const labels[] = {"CALLS", "USAGE",  "CALL_REFERENCE",
                                         "READS", "WRITES", "USES_TYPE"};
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
    static const char *const labels[] = {"CALLS", "USAGE",  "CALL_REFERENCE",
                                         "READS", "WRITES", "USES_TYPE"};
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
