#include "pipeline/resolver_ecma.h"

#include "foundation/hash_table.h"
#include "pipeline/import_resolve.h"
#include "pipeline/shared_resolver.h"
#include "resolve/policy.h"

#include <string.h>

typedef struct ecma_entry {
    const char *target;
    const char *dependency;
    const char *member;
    bool known;
    bool has_source;
    bool is_object;
    bool default_import;
    bool conflict;
} ecma_entry_t;

typedef struct ecma_star {
    const char *dependency;
    struct ecma_star *next;
} ecma_star_t;

typedef struct {
    const char *file;
    const char *module_qn;
    const cbm_resolve_proof_t *audit;
    CBMHashTable *exports;
    CBMHashTable *type_exports;
    ecma_entry_t *whole;
    ecma_star_t *stars;
    bool complete;
    bool commonjs;
    bool analyzed;
} ecma_module_t;

struct cbm_ecma_table {
    CBMArena *arena;
    CBMHashTable *modules;
    CBMArena owned_arena;
    bool owns_arena;
};

typedef struct {
    const cbm_ecma_table_t *table;
    cbm_resolve_lookup_t *query;
    CBMHashTable *visited;
    CBMArena arena;
    bool arena_ready;
    bool type_space;
} ecma_walk_t;

static cbm_ecma_value_t ecma_name(ecma_walk_t *walk, const char *file, const char *name);
static cbm_ecma_value_t ecma_module_whole(ecma_walk_t *walk, const char *file);

static void ecma_free_module(const char *key, void *value, void *context) {
    (void)key;
    (void)context;
    ecma_module_t *module = value;
    cbm_ht_free(module->exports);
    cbm_ht_free(module->type_exports);
}

cbm_ecma_table_t *cbm_ecma_table_new(CBMArena *arena) {
    cbm_ecma_table_t *table = arena ? cbm_arena_calloc(arena, sizeof(*table)) : NULL;
    if (!table)
        return NULL;
    table->arena = arena;
    table->modules = cbm_ht_create(128);
    return table->modules ? table : NULL;
}

void cbm_ecma_table_free(cbm_ecma_table_t *table) {
    if (!table || !table->modules)
        return;
    cbm_ht_foreach(table->modules, ecma_free_module, NULL);
    cbm_ht_free(table->modules);
    table->modules = NULL;
}

const char *cbm_ecma_dependency(const cbm_pipeline_ctx_t *ctx, const char *file, const char *source,
                                CBMArena *arena) {
    if (!ctx || !file || !source || !source[0])
        return NULL;
    CBMImport request = {.module_path = source};
    const cbm_gbuf_node_t *dependency = NULL;
    cbm_resolve_result_t proof =
        cbm_pipeline_resolve_import_result(ctx, file, NULL, &request, NULL, &dependency);
    if (!cbm_resolve_admit(&proof) || !dependency || !dependency->file_path || !dependency->label ||
        (strcmp(dependency->label, "File") && strcmp(dependency->label, "Module")))
        return NULL;
    return cbm_arena_strdup(arena, dependency->file_path);
}

static const char *ecma_copy(CBMArena *arena, const char *text) {
    return text ? cbm_arena_strdup(arena, text) : NULL;
}

static bool ecma_same_value(const ecma_entry_t *a, const cbm_resolve_js_export_t *b,
                            const char *dependency) {
    const char *member = b->member;
    if (a->known != b->known || a->default_import != b->default_import ||
        a->is_object != b->is_object)
        return false;
    if ((a->target != NULL) != (b->target != NULL) || (a->target && strcmp(a->target, b->target)))
        return false;
    if ((a->dependency != NULL) != (dependency != NULL) ||
        (a->dependency && strcmp(a->dependency, dependency)))
        return false;
    if (a->is_object && b->is_object)
        return false; /* a second `module.exports = {}` replaces the first */
    return (a->member != NULL) == (member != NULL) && (!member || !strcmp(a->member, member));
}

bool cbm_ecma_table_add(cbm_ecma_table_t *table, const cbm_pipeline_ctx_t *ctx, const char *file,
                        const char *module_qn, const cbm_resolve_binding_index_t *index,
                        const cbm_resolve_proof_t *audit) {
    if (!table || !file)
        return false;
    /* Two extraction results for one path cannot pick a surface by order. */
    ecma_module_t *prior = cbm_ht_get(table->modules, file);
    if (prior) {
        prior->analyzed = false;
        return true;
    }
    ecma_module_t *module = cbm_arena_calloc(table->arena, sizeof(*module));
    const char *key = cbm_arena_strdup(table->arena, file);
    if (!module || !key)
        return false;
    *module = (ecma_module_t){.file = key,
                              .module_qn = ecma_copy(table->arena, module_qn),
                              .audit = audit,
                              .exports = cbm_ht_create(16),
                              .type_exports = cbm_ht_create(8),
                              .commonjs = cbm_resolve_binding_js_commonjs(index),
                              .complete = cbm_resolve_binding_js_exports_complete(index),
                              .analyzed = cbm_resolve_binding_js_export_count(index) > 0 ||
                                          cbm_resolve_binding_js_exports_complete(index)};
    if (!module->exports || !module->type_exports) {
        cbm_ht_free(module->exports);
        cbm_ht_free(module->type_exports);
        return false;
    }
    cbm_ht_set(table->modules, key, module);
    if (cbm_ht_get(table->modules, key) != module) {
        cbm_ht_free(module->exports);
        cbm_ht_free(module->type_exports);
        return false;
    }
    size_t count = cbm_resolve_binding_js_export_count(index);
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_js_export_t *item = cbm_resolve_binding_js_export_at(index, i);
        const char *dependency =
            item->source ? cbm_ecma_dependency(ctx, file, item->source, table->arena) : NULL;
        if (!item->name && !item->whole) {
            ecma_star_t *star = cbm_arena_calloc(table->arena, sizeof(*star));
            if (!star)
                return false;
            *star = (ecma_star_t){.dependency = dependency, .next = module->stars};
            module->stars = star;
            continue;
        }
        CBMHashTable *space =
            item->kind == CBM_RESOLVE_EXPORT_TYPE ? module->type_exports : module->exports;
        ecma_entry_t *existing = item->whole ? module->whole : cbm_ht_get(space, item->name);
        if (existing) {
            existing->conflict |= !ecma_same_value(existing, item, dependency);
            /* Which `module.exports =` ran last decides every name of the module. */
            if (item->whole && existing->conflict)
                module->analyzed = false;
            continue;
        }
        ecma_entry_t *entry = cbm_arena_calloc(table->arena, sizeof(*entry));
        const char *name = item->name ? cbm_arena_strdup(table->arena, item->name) : NULL;
        if (!entry || (item->name && !name))
            return false;
        *entry = (ecma_entry_t){.target = ecma_copy(table->arena, item->target),
                                .dependency = dependency,
                                .member = ecma_copy(table->arena, item->member),
                                .known = item->known,
                                .has_source = item->source != NULL,
                                .is_object = item->is_object,
                                .default_import = item->default_import};
        if (item->whole) {
            module->whole = entry;
        } else {
            cbm_ht_set(space, name, entry);
            if (cbm_ht_get(space, name) != entry)
                return false;
        }
    }
    return true;
}

static bool ecma_source_language(CBMLanguage language) {
    return language == CBM_LANG_JAVASCRIPT || language == CBM_LANG_TYPESCRIPT ||
           language == CBM_LANG_TSX || language == CBM_LANG_ARKTS;
}

cbm_ecma_table_t *cbm_ecma_surfaces_build(const cbm_pipeline_ctx_t *ctx,
                                          const cbm_file_info_t *files, int file_count,
                                          CBMFileResult **cache) {
    cbm_ecma_table_t *table = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*table));
    if (!table)
        return NULL;
    *table = (cbm_ecma_table_t){.owns_arena = true};
    cbm_arena_init(&table->owned_arena);
    table->arena = &table->owned_arena;
    table->modules = cbm_ht_create(128);
    if (!table->modules) {
        cbm_ecma_surfaces_free(table);
        return NULL;
    }
    for (int f = 0; f < file_count; f++) {
        if (!ecma_source_language(files[f].language))
            continue;
        bool loaded = false;
        CBMFileResult *result = cbm_pipeline_result_acquire(ctx, cache, f, NULL, &loaded);
        if (!result)
            continue;
        size_t length = result->source_len > 0 ? (size_t)result->source_len : 0;
        char *owned =
            result->source ? NULL : cbm_shared_resolver_load_source(files[f].path, &length);
        const char *source = result->source ? result->source : owned;
        CBMArena arena;
        cbm_arena_init(&arena);
        bool declared = files[f].language == CBM_LANG_JAVASCRIPT &&
                        cbm_pipeline_node_declared_module(files[f].rel_path);
        const cbm_resolve_binding_index_t *index =
            source ? cbm_resolve_binding_build_declared_module(&arena, result, files[f].language,
                                                               source, length, declared)
                   : NULL;
        bool ok = !index ||
                  cbm_ecma_table_add(table, ctx, files[f].rel_path, result->module_qn, index, NULL);
        cbm_arena_destroy(&arena);
        cbm_free(CBM_MEM_CLASS_EXTRACT, owned);
        cbm_pipeline_result_release(result, loaded);
        if (!ok) {
            cbm_ecma_surfaces_free(table);
            return NULL;
        }
    }
    return table;
}

void cbm_ecma_surfaces_free(cbm_ecma_table_t *surfaces) {
    if (!surfaces)
        return;
    cbm_ecma_table_free(surfaces);
    cbm_arena_destroy(&surfaces->owned_arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, surfaces);
}

static void ecma_hop(ecma_walk_t *walk, const ecma_module_t *module, const char *name,
                     const cbm_ecma_value_t *value) {
    if (!walk->query || !walk->query->context)
        return;
    if (module->audit)
        (void)cbm_resolve_lookup_support(walk->query, module->audit);
    (void)cbm_resolve_lookup_observe(
        walk->query, module->module_qn ? module->module_qn : module->file, name,
        value->status == CBM_ECMA_DECLARATION ? value->target : value->file,
        CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, 0, true,
        "ecma_exported_name_observed");
}

static const ecma_module_t *ecma_module(const ecma_walk_t *walk, const char *file) {
    const ecma_module_t *module = file ? cbm_ht_get(walk->table->modules, file) : NULL;
    return module && module->analyzed ? module : NULL;
}

static cbm_ecma_value_t ecma_entry_value(ecma_walk_t *walk, const ecma_entry_t *entry) {
    if (entry->conflict)
        return (cbm_ecma_value_t){.status = CBM_ECMA_AMBIGUOUS};
    if (!entry->known)
        return (cbm_ecma_value_t){.status = CBM_ECMA_UNKNOWN};
    if (entry->target)
        return (cbm_ecma_value_t){.status = CBM_ECMA_DECLARATION, .target = entry->target};
    if (entry->has_source) {
        if (!entry->dependency)
            return (cbm_ecma_value_t){.status = CBM_ECMA_UNKNOWN};
        if (entry->default_import)
            return ecma_name(walk, entry->dependency, "default");
        return entry->member ? ecma_name(walk, entry->dependency, entry->member)
                             : ecma_module_whole(walk, entry->dependency);
    }
    return (cbm_ecma_value_t){.status = CBM_ECMA_UNKNOWN};
}

/* A diamond of re-exports reaches one module twice, and a cycle reaches it again
 * while it is still being answered. ES resolves a revisit as no answer. */
static bool ecma_first_visit(ecma_walk_t *walk, const char *file, const char *name) {
    if (!walk->arena_ready) {
        cbm_arena_init(&walk->arena);
        walk->visited = cbm_ht_create(32);
        walk->arena_ready = true;
    }
    if (!walk->visited)
        return false;
    const char *key = cbm_arena_sprintf(&walk->arena, "%s\n%s", file, name);
    if (!key || cbm_ht_get(walk->visited, key))
        return false;
    cbm_ht_set(walk->visited, key, (void *)key);
    return cbm_ht_get(walk->visited, key) != NULL;
}

static bool ecma_values_agree(const cbm_ecma_value_t *a, const cbm_ecma_value_t *b) {
    if (a->status != b->status)
        return false;
    const char *left = a->status == CBM_ECMA_DECLARATION ? a->target : a->file;
    const char *right = b->status == CBM_ECMA_DECLARATION ? b->target : b->file;
    if (!left || !right || strcmp(left, right))
        return false;
    return (a->member != NULL) == (b->member != NULL) &&
           (!a->member || !strcmp(a->member, b->member));
}

static cbm_ecma_value_t ecma_name(ecma_walk_t *walk, const char *file, const char *name) {
    const ecma_module_t *module = ecma_module(walk, file);
    if (!module)
        return (cbm_ecma_value_t){.status = CBM_ECMA_UNKNOWN};
    /* Each space answers first for itself and then for the other: an import names
     * both, and an unused half of a same-named pair is no competitor. */
    const ecma_entry_t *entry =
        cbm_ht_get(walk->type_space ? module->type_exports : module->exports, name);
    if (!entry)
        entry = cbm_ht_get(walk->type_space ? module->exports : module->type_exports, name);
    bool direct = entry && entry->target && entry->known && !entry->conflict;
    if (!direct && !ecma_first_visit(walk, file, name))
        return (cbm_ecma_value_t){.status = CBM_ECMA_ABSENT};
    if (entry) {
        cbm_ecma_value_t value = ecma_entry_value(walk, entry);
        if (value.status == CBM_ECMA_DECLARATION || value.status == CBM_ECMA_NAMESPACE)
            ecma_hop(walk, module, name, &value);
        return value;
    }
    if (!strcmp(name, "default")) {
        /* Interop: a CommonJS module's default is its module.exports. */
        if (module->whole || module->commonjs) {
            cbm_ecma_value_t value = ecma_module_whole(walk, file);
            if (value.status == CBM_ECMA_DECLARATION || value.status == CBM_ECMA_NAMESPACE)
                ecma_hop(walk, module, name, &value);
            return value;
        }
        return (cbm_ecma_value_t){.status = module->complete ? CBM_ECMA_ABSENT : CBM_ECMA_UNKNOWN};
    }
    /* `module.exports = createApp` has createApp's own members. */
    if (module->whole && !module->whole->is_object) {
        cbm_ecma_value_t whole = ecma_entry_value(walk, module->whole);
        if (whole.status == CBM_ECMA_DECLARATION && !whole.member)
            whole.member = name;
        else if (whole.status == CBM_ECMA_NAMESPACE)
            whole = ecma_name(walk, whole.file, name);
        if (whole.status == CBM_ECMA_DECLARATION || whole.status == CBM_ECMA_NAMESPACE)
            ecma_hop(walk, module, name, &whole);
        return whole;
    }
    /* Ambiguous stars are excluded by ES and an explicit import of one is a
     * link error, so one answering star is the answer even beside an open one. */
    cbm_ecma_value_t found = {.status = CBM_ECMA_ABSENT};
    bool open = !module->complete;
    for (const ecma_star_t *star = module->stars; star; star = star->next) {
        if (!star->dependency) {
            open = true;
            continue;
        }
        cbm_ecma_value_t value = ecma_name(walk, star->dependency, name);
        if (value.status == CBM_ECMA_AMBIGUOUS)
            return value;
        if (value.status == CBM_ECMA_UNKNOWN) {
            open = true;
            continue;
        }
        if (value.status == CBM_ECMA_ABSENT)
            continue;
        if (found.status != CBM_ECMA_ABSENT && !ecma_values_agree(&found, &value))
            return (cbm_ecma_value_t){.status = CBM_ECMA_AMBIGUOUS};
        found = value;
    }
    if (found.status != CBM_ECMA_ABSENT) {
        ecma_hop(walk, module, name, &found);
        return found;
    }
    return (cbm_ecma_value_t){.status = open ? CBM_ECMA_UNKNOWN : CBM_ECMA_ABSENT};
}

static cbm_ecma_value_t ecma_module_whole(ecma_walk_t *walk, const char *file) {
    const ecma_module_t *module = ecma_module(walk, file);
    if (!module)
        return (cbm_ecma_value_t){.status = CBM_ECMA_UNKNOWN};
    if (module->whole && !module->whole->is_object) {
        if (!ecma_first_visit(walk, file, "module.exports"))
            return (cbm_ecma_value_t){.status = CBM_ECMA_ABSENT};
        return ecma_entry_value(walk, module->whole);
    }
    return (cbm_ecma_value_t){.status = CBM_ECMA_NAMESPACE, .file = module->file};
}

static void ecma_walk_end(ecma_walk_t *walk) {
    if (!walk->arena_ready)
        return;
    cbm_ht_free(walk->visited);
    cbm_arena_destroy(&walk->arena);
}

cbm_ecma_value_t cbm_ecma_lookup(const cbm_ecma_table_t *table, const char *file, const char *name,
                                 cbm_resolve_lookup_t *query) {
    ecma_walk_t walk = {.table = table, .query = query};
    cbm_ecma_value_t value = ecma_name(&walk, file, name);
    ecma_walk_end(&walk);
    return value;
}

cbm_ecma_value_t cbm_ecma_lookup_type(const cbm_ecma_table_t *table, const char *file,
                                      const char *name, cbm_resolve_lookup_t *query) {
    ecma_walk_t walk = {.table = table, .query = query, .type_space = true};
    cbm_ecma_value_t value = ecma_name(&walk, file, name);
    ecma_walk_end(&walk);
    return value;
}

cbm_ecma_value_t cbm_ecma_default(const cbm_ecma_table_t *table, const char *file,
                                  cbm_resolve_lookup_t *query) {
    return cbm_ecma_lookup(table, file, "default", query);
}

cbm_ecma_value_t cbm_ecma_whole(const cbm_ecma_table_t *table, const char *file,
                                cbm_resolve_lookup_t *query) {
    ecma_walk_t walk = {.table = table, .query = query};
    cbm_ecma_value_t value = ecma_module_whole(&walk, file);
    ecma_walk_end(&walk);
    return value;
}
