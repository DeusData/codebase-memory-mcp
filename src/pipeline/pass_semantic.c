#include "resolve/stats.h"
#include "pipeline/import_resolve.h"
#include "pipeline/pass_lsp_cross.h"
/*
 * pass_semantic.c — Semantic edge passes: INHERITS, DECORATES, IMPLEMENTS.
 *
 * Operates on already-extracted nodes in the graph buffer:
 *   - INHERITS: Class/Interface → base class (from base_classes in extraction)
 *   - DECORATES: Function/Method → decorator function (from decorators in
 * extraction)
 *   - IMPLEMENTS: Struct/Class → Interface (Go implicit + explicit base_classes
 * + Rust impl)
 *
 * These passes re-extract files to access base_classes, decorators, and
 * impl_traits data since that information is in CBMDefinition fields, not
 * stored in node properties JSON.
 *
 * Depends on: pass_definitions having populated the registry and graph buffer
 */
#include "callable_sig.h"
#include "helpers.h"
#include "lang_specs.h"
#include "lsp/c_proof.h"
#include "lsp/cs_proof.h"
#include "lsp/rust_proof.h"
#include "lsp/kotlin_proof.h"
#include "lsp/php_proof.h"
#include "resolve/binding.h"
#include "cbm.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h" // cbm_json_escape
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/shared_resolver.h"
#include <stdint.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* True for languages whose module QN derives from the CONTAINING DIRECTORY
 * (Java/Go package). MUST match cbm_lang_module_is_dir()
 * (internal/cbm/helpers.c) so base-class / same-module resolution keys against
 * the directory-based def-node QNs. */
static bool ps_module_is_dir(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA || lang == CBM_LANG_GO;
}

static char *read_file(const char *path, int *out_len) {
    *out_len = -1; /* unknown/read failure; zero is a proven empty file */
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > cbm_max_file_bytes()) { /* generous, env-configurable cap (B4) */
        if (size == 0) {
            *out_len = 0;
        }
        (void)fclose(f);
        return NULL;
    }
    /* +pad: tree-sitter lexer lookahead reads past EOF; keep it in-bounds */
    enum { CBM_TS_LOOKAHEAD_PAD = 16 };
    char *buf = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)size + CBM_TS_LOOKAHEAD_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, size, f);
    (void)fclose(f);
    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    memset(buf + nread, 0, CBM_TS_LOOKAHEAD_PAD);
    *out_len = (int)nread;
    return buf;
}

/* Structural class identity is separate from callable/member dispatch. Build
 * these source facts once, then share immutable indexes with resolve workers.
 * Unknown package membership never becomes absence evidence. */
typedef struct {
    CBMLanguage language;
    const char *package;
    CBMHashTable *imports;
    bool has_imports;
    CBMHashTable *php_names;
    const char *php_include;
    uint32_t php_include_end;
    bool php_complete, php_inert, php_use_imports;
    CBMHashTable *type_names, *rust_impls;
    const char *include_module;
    uint32_t include_end;
    bool typed_complete;
} semantic_source_t;

typedef struct {
    const cbm_gbuf_node_t *node;
    semantic_source_t *source;
    const char *base;
    const char *package_base;
    bool public_class;
    bool ambiguous;
    uint32_t php_start;
    bool php_standalone;
    uint32_t declaration_start;
} semantic_class_t;

struct cbm_semantic_bindings {
    CBMArena arena;
    CBMHashTable *files, *classes, *packages;
    bool kotlin_complete, csharp_complete;
};

static bool semantic_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}

/* ts_node_named_child(node, i) walks from the first child on every call, so an
 * index loop is quadratic in the sibling count; TypeScript's declaration files
 * held this pass for over 11 minutes on one root. One cursor pass snapshots the
 * children, and the loops keep their early returns. */
static bool semantic_named_list(CBMArena *arena, TSNode node, TSNode **out, uint32_t *count) {
    uint32_t expected = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    *out = NULL;
    *count = 0;
    if (!expected)
        return true;
    TSNode *children = cbm_arena_alloc(arena, (size_t)expected * sizeof(*children));
    if (!children)
        return false;
    uint32_t found = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child) && found < expected)
                children[found++] = child;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS, found);
    *out = children;
    *count = found;
    return found == expected;
}

static TSNode semantic_child(TSNode node, const char *kind) {
    TSNode found = {0};
    if (ts_node_is_null(node))
        return found;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child) && semantic_is(child, kind)) {
                found = child;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return found;
}

static bool semantic_ecma(CBMLanguage language) {
    return language == CBM_LANG_TYPESCRIPT || language == CBM_LANG_TSX;
}

static bool semantic_simple_name(const char *name) {
    if (!name || !name[0])
        return false;
    for (size_t i = 0; name[i]; i++) {
        unsigned char c = (unsigned char)name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
              (i && c >= '0' && c <= '9')))
            return false;
    }
    return true;
}

static bool semantic_put(CBMHashTable *table, const char *key, void *value) {
    if (!key || !value)
        return false;
    cbm_ht_set(table, key, value);
    return cbm_ht_get(table, key) == value;
}

static void semantic_source_free(const char *key, void *value, void *ud) {
    (void)key;
    (void)ud;
    cbm_ht_free(((semantic_source_t *)value)->imports);
    cbm_ht_free(((semantic_source_t *)value)->php_names);
    cbm_ht_free(((semantic_source_t *)value)->type_names);
    cbm_ht_free(((semantic_source_t *)value)->rust_impls);
}

void cbm_semantic_bindings_free(cbm_semantic_bindings_t *bindings) {
    if (!bindings)
        return;
    if (bindings->files)
        cbm_ht_foreach(bindings->files, semantic_source_free, NULL);
    cbm_ht_free(bindings->files);
    cbm_ht_free(bindings->classes);
    cbm_ht_free(bindings->packages);
    cbm_arena_destroy(&bindings->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, bindings);
}

/* Keep exact AST heritage spelling; extractor names alone can strip generic
 * or qualified syntax. Only a single bare superclass is admitted here. */
static const char *semantic_bare_base(CBMArena *arena, TSNode node, const char *source,
                                      CBMLanguage language) {
    if (!ts_node_is_null(semantic_child(node, "type_parameters")))
        return NULL;
    TSNode base = {0};
    if (semantic_ecma(language)) {
        TSNode heritage = semantic_child(node, "class_heritage");
        TSNode clause = semantic_child(heritage, "extends_clause");
        if (ts_node_is_null(clause))
            return NULL;
        base = ts_node_child_by_field_name(clause, "value", 5);
        if (!semantic_is(base, "identifier"))
            return NULL;
    } else {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
            TSNode child = ts_node_named_child(node, i);
            if (!semantic_is(child, "delegation_specifier"))
                continue;
            if (!ts_node_is_null(base) || ts_node_named_child_count(child) != 1)
                return NULL;
            TSNode type = ts_node_named_child(child, 0);
            if (semantic_is(type, "constructor_invocation"))
                type = ts_node_named_child(type, 0);
            if (!semantic_is(type, "user_type") || ts_node_named_child_count(type) != 1)
                return NULL;
            base = ts_node_named_child(type, 0);
            if (!semantic_is(base, "type_identifier"))
                return NULL;
        }
    }
    if (ts_node_is_null(base))
        return NULL;
    const char *name = cbm_node_text(arena, base, source);
    return semantic_simple_name(name) ? name : NULL;
}

static bool semantic_package_name(cbm_semantic_bindings_t *bindings, const char *package,
                                  const char *name, semantic_class_t *member) {
    const char *key = cbm_arena_sprintf(&bindings->arena, "%s:%s", package, name);
    if (!key)
        return false;
    semantic_class_t *prior = cbm_ht_get(bindings->packages, key);
    if (prior) {
        prior->ambiguous = true;
        return true;
    }
    return semantic_put(bindings->packages, key, member);
}

static bool semantic_imports(cbm_semantic_bindings_t *bindings, cbm_pipeline_ctx_t *ctx,
                             semantic_source_t *file, const cbm_file_info_t *info,
                             const CBMFileResult *result,
                             const cbm_resolve_binding_index_t *index) {
    const char **keys = NULL, **values = NULL;
    int count = 0;
    if (cbm_pipeline_ecma_import_map(ctx, info->rel_path, result, false, &keys, &values, &count))
        return false;
    bool ok = true;
    for (int i = 0; ok && i < count; i++) {
        const char *key = cbm_arena_strdup(&bindings->arena, keys[i]);
        const char *audited =
            values[i] && cbm_resolve_binding_import(index, keys[i]) ? values[i] : "";
        char *value = cbm_arena_strdup(&bindings->arena, audited);
        ok = semantic_put(file->imports, key, value);
    }
    cbm_pxc_free_import_map(keys, values, count);
    return ok;
}

static bool semantic_class_facts(cbm_semantic_bindings_t *bindings, cbm_pipeline_ctx_t *ctx,
                                 semantic_source_t *file, const cbm_file_info_t *info,
                                 const CBMFileResult *result, const char *source, int length,
                                 TSNode root, const cbm_resolve_binding_index_t *index,
                                 CBMArena *temporary) {
    TSNode *roots;
    uint32_t children;
    if (!semantic_named_list(temporary, root, &roots, &children) || children > INT32_MAX)
        return false;
    CBMLSPDef *facts = cbm_arena_calloc(temporary, (size_t)children * sizeof(*facts));
    semantic_class_t **classes = cbm_arena_calloc(temporary, (size_t)children * sizeof(*classes));
    if (children && (!facts || !classes))
        return false;
    int count = 0;
    for (uint32_t i = 0; i < children; i++) {
        TSNode node = roots[i];
        if (semantic_ecma(info->language) && semantic_is(node, "export_statement"))
            node = ts_node_child_by_field_name(node, "declaration", 11);
        bool class_node =
            semantic_is(node, "class_declaration") ||
            (semantic_ecma(info->language) && semantic_is(node, "abstract_class_declaration"));
        if (!class_node) {
            if (info->language != CBM_LANG_KOTLIN)
                continue;
            if (semantic_is(node, "package_header") || semantic_is(node, "import_list") ||
                semantic_is(node, "line_comment") || semantic_is(node, "multiline_comment"))
                continue;
            /* Functions are harmless to the census only when their exact
             * spelling is retained as a competing package binding. Other
             * declaration forms keep the package domain open for now. */
            if (!semantic_is(node, "function_declaration")) {
                bindings->kotlin_complete = false;
                continue;
            }
            TSNode name_node = semantic_child(node, "simple_identifier");
            const char *name = ts_node_is_null(name_node)
                                   ? NULL
                                   : cbm_node_text(&bindings->arena, name_node, source);
            semantic_class_t *blocked = cbm_arena_calloc(&bindings->arena, sizeof(*blocked));
            if (!name || !blocked)
                return false;
            if (!semantic_simple_name(name))
                bindings->kotlin_complete = false;
            if (!semantic_package_name(bindings, file->package, name, blocked))
                return false;
            continue;
        }
        TSNode name_node = info->language == CBM_LANG_KOTLIN
                               ? semantic_child(node, "type_identifier")
                               : ts_node_child_by_field_name(node, "name", 4);
        const char *name =
            ts_node_is_null(name_node) ? NULL : cbm_node_text(&bindings->arena, name_node, source);
        if (!name)
            return false;
        const char *qn = cbm_fqn_compute(&bindings->arena, ctx->project_name, info->rel_path, name);
        semantic_class_t *entry = cbm_arena_calloc(&bindings->arena, sizeof(*entry));
        if (!qn || !entry)
            return false;
        entry->source = file;
        entry->base = semantic_bare_base(&bindings->arena, node, source, info->language);
        const cbm_gbuf_node_t *graph = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
        uint32_t line = ts_node_start_point(node).row + 1;
        if (graph && graph->label && !strcmp(graph->label, "Class") && graph->file_path &&
            !strcmp(graph->file_path, info->rel_path) && graph->start_line == (int)line)
            entry->node = graph;
        semantic_class_t *prior = cbm_ht_get(bindings->classes, qn);
        if (prior)
            prior->ambiguous = true;
        else if (!semantic_put(bindings->classes, qn, entry))
            return false;
        if (info->language == CBM_LANG_KOTLIN) {
            if (!semantic_simple_name(name))
                bindings->kotlin_complete = false;
            if (entry->base) {
                entry->package_base =
                    cbm_arena_sprintf(&bindings->arena, "%s:%s", file->package, entry->base);
                if (!entry->package_base)
                    return false;
            }
            if (!semantic_package_name(bindings, file->package, name, entry))
                return false;
            const char *prefix = file->package[0] ? file->package : result->module_qn;
            facts[count] = (CBMLSPDef){
                .qualified_name = cbm_arena_sprintf(&bindings->arena, "%s.%s", prefix, name),
                .short_name = name,
                .source_start_line = line};
            if (!facts[count].qualified_name)
                return false;
            classes[count++] = entry;
        } else {
            unsigned kind = cbm_resolve_binding_export_kind(index, qn);
            entry->public_class = (kind & (CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE)) ==
                                  (CBM_RESOLVE_EXPORT_VALUE | CBM_RESOLVE_EXPORT_TYPE);
        }
    }
    if (info->language != CBM_LANG_KOTLIN)
        return true;
    if (!cbm_kotlin_collect_source_facts(temporary, source, length, result->module_qn, facts,
                                         count)) {
        bindings->kotlin_complete = false;
        return true;
    }
    for (int i = 0; i < count; i++) {
        uint32_t required =
            CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_TOP_LEVEL;
        classes[i]->public_class = (facts[i].source_flags & required) == required &&
                                   facts[i].namespace_name &&
                                   !strcmp(facts[i].namespace_name, file->package);
    }
    return true;
}

static const char *semantic_php_lower(CBMArena *arena, const char *name) {
    if (!semantic_simple_name(name))
        return NULL;
    char *lower = cbm_arena_strdup(arena, name);
    if (lower)
        for (char *p = lower; *p; p++)
            if (*p >= 'A' && *p <= 'Z')
                *p = (char)(*p - 'A' + 'a');
    return lower;
}

/* This first include bridge deliberately admits one direct, unconditional
 * include. Its target must contain only global class declarations. It does
 * not evaluate PHP, compose include chains, or assume runtime include_path. */
static bool semantic_php_facts(cbm_semantic_bindings_t *bindings, cbm_pipeline_ctx_t *ctx,
                               semantic_source_t *file, const cbm_file_info_t *info,
                               const CBMFileResult *result, const char *source, int length,
                               TSNode root, CBMArena *temporary) {
    TSNode *roots;
    uint32_t children;
    if (!semantic_named_list(temporary, root, &roots, &children) || children > INT32_MAX)
        return false;
    CBMLSPDef *facts = cbm_arena_calloc(temporary, (size_t)children * sizeof(*facts));
    semantic_class_t **classes = cbm_arena_calloc(temporary, (size_t)children * sizeof(*classes));
    if (children && (!facts || !classes))
        return false;
    file->php_names = cbm_ht_create(0);
    if (!file->php_names)
        return false;
    file->php_complete = true;
    file->php_inert = true;
    int count = 0, includes = 0;
    for (uint32_t i = 0; i < children; i++) {
        TSNode node = roots[i];
        if (semantic_is(node, "php_tag") || semantic_is(node, "comment"))
            continue;
        if (semantic_is(node, "expression_statement")) {
            file->php_inert = false;
            TSNode value = ts_node_named_child(node, 0);
            if (ts_node_named_child_count(node) != 1 ||
                !semantic_is(value, "require_once_expression"))
                file->php_complete = false;
            includes++;
            file->php_include_end = ts_node_end_byte(node);
            continue;
        }
        if (semantic_is(node, "namespace_use_declaration"))
            file->php_use_imports = true;
        if (!semantic_is(node, "class_declaration")) {
            file->php_complete = file->php_inert = false;
            continue;
        }
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        const char *name =
            ts_node_is_null(name_node) ? NULL : cbm_node_text(&bindings->arena, name_node, source);
        const char *lower = semantic_php_lower(&bindings->arena, name);
        if (!lower) {
            file->php_complete = false;
            continue;
        }
        const char *qn = cbm_fqn_compute(&bindings->arena, ctx->project_name, info->rel_path, name);
        semantic_class_t *entry = cbm_arena_calloc(&bindings->arena, sizeof(*entry));
        if (!qn || !entry)
            return false;
        entry->source = file;
        entry->php_start = ts_node_start_byte(node);
        TSNode base = semantic_child(node, "base_clause");
        TSNode base_name = ts_node_is_null(base) ? (TSNode){0} : ts_node_named_child(base, 0);
        if (!ts_node_is_null(base) && ts_node_named_child_count(base) == 1 &&
            semantic_is(base_name, "name")) {
            entry->base = cbm_node_text(&bindings->arena, base_name, source);
            entry->package_base = semantic_php_lower(&bindings->arena, entry->base);
        }
        entry->php_standalone = ts_node_is_null(base);
        for (uint32_t j = 0; j < ts_node_named_child_count(node); j++) {
            TSNode child = ts_node_named_child(node, j);
            if (!semantic_is(child, "name") && !semantic_is(child, "declaration_list") &&
                !semantic_is(child, "base_clause"))
                entry->php_standalone = false;
        }
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        TSNode *members;
        uint32_t member_count;
        if (!semantic_named_list(temporary, body, &members, &member_count))
            return false;
        for (uint32_t j = 0; j < member_count; j++) {
            TSNode member = members[j];
            if (semantic_is(member, "comment"))
                continue;
            if (!semantic_is(member, "method_declaration") ||
                ts_node_is_null(ts_node_child_by_field_name(member, "body", 4)))
                entry->php_standalone = false;
        }
        if (!entry->php_standalone)
            file->php_inert = false;
        const cbm_gbuf_node_t *graph = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
        uint32_t line = ts_node_start_point(node).row + 1;
        if (graph && graph->label && !strcmp(graph->label, "Class") && graph->file_path &&
            !strcmp(graph->file_path, info->rel_path) && graph->start_line == (int)line)
            entry->node = graph;
        semantic_class_t *prior = cbm_ht_get(bindings->classes, qn);
        if (prior)
            prior->ambiguous = true;
        else if (!semantic_put(bindings->classes, qn, entry))
            return false;
        prior = cbm_ht_get(file->php_names, lower);
        if (prior)
            prior->ambiguous = true;
        else if (!semantic_put(file->php_names, lower, entry))
            return false;
        facts[count] = (CBMLSPDef){.qualified_name = qn,
                                   .graph_qualified_name = qn,
                                   .short_name = name,
                                   .source_start_line = line};
        classes[count++] = entry;
    }
    if (includes > 1)
        file->php_complete = false;
    if (includes == 1) {
        if (result->imports.count != 1 ||
            result->imports.items[0].kind != CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH) {
            file->php_complete = false;
        } else {
            cbm_php_vendor_names_t imported = {0};
            cbm_pipeline_php_vendor_names_build(ctx->gbuf, ctx->project_name, info->rel_path,
                                                result, &imported);
            const char *const *targets =
                imported.targets ? imported.targets : imported.proven_targets;
            const cbm_gbuf_node_t *target =
                targets && targets[0] ? cbm_gbuf_find_by_qn(ctx->gbuf, targets[0]) : NULL;
            /* A saved graph import is a consistency check, never the path
             * authority: derive the same target from the admitted AST anchor. */
            const char *slash = strrchr(info->rel_path, '/');
            const char *directory = slash ? cbm_arena_strndup(temporary, info->rel_path,
                                                              (size_t)(slash - info->rel_path))
                                          : "";
            enum { PHP_SOURCE_PATH_CAPACITY = 4096 };
            char path[PHP_SOURCE_PATH_CAPACITY];
            if (directory &&
                cbm_repo_rel_join(directory, result->imports.items[0].module_path, path,
                                  sizeof(path)) &&
                target && target->label && !strcmp(target->label, "File") && target->file_path &&
                !strcmp(path, target->file_path))
                file->php_include = cbm_arena_strdup(&bindings->arena, path);
            cbm_pipeline_php_vendor_names_free(&imported);
            if (!file->php_include)
                file->php_complete = false;
        }
    }
    if (!cbm_php_collect_source_facts(temporary, source, length, result->module_qn, facts, count)) {
        file->php_complete = false;
        return true;
    }
    for (int i = 0; i < count; i++) {
        uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC |
                            CBM_SOURCE_DECL_TOP_LEVEL | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
        classes[i]->public_class = (facts[i].source_flags & required) == required;
        if (!classes[i]->public_class || classes[i]->ambiguous)
            file->php_complete = false;
    }
    return true;
}

/* The following adapters certify declaration identity only. They deliberately
 * do not supply member dispatch or broaden the callable proof registries. */
typedef struct {
    cbm_semantic_bindings_t *bindings;
    cbm_pipeline_ctx_t *ctx;
    semantic_source_t *file;
    const cbm_file_info_t *info;
    const CBMFileResult *result;
    const char *source;
    CBMLSPDef *facts;
    semantic_class_t **entries;
    int count, capacity;
    CBMArena *temporary;
} semantic_types_t;

static bool semantic_typed_language(CBMLanguage language) {
    return language == CBM_LANG_CSHARP || language == CBM_LANG_CPP || language == CBM_LANG_RUST;
}

static bool semantic_comment(TSNode node) {
    return semantic_is(node, "comment") || semantic_is(node, "line_comment") ||
           semantic_is(node, "block_comment");
}

/* Check the complete raw tree, including method bodies. A conditional or macro
 * must not disappear merely because the declaration census skips its subtree. */
static bool semantic_unconditional(TSNode root, CBMLanguage language) {
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    bool ok = true;
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if ((!strncmp(kind, "preproc_", 8) &&
             !(language == CBM_LANG_CPP && semantic_is(node, "preproc_include") &&
               ts_node_eq(ts_node_parent(node), root))) ||
            (language == CBM_LANG_RUST &&
             (semantic_is(node, "attribute_item") || semantic_is(node, "inner_attribute_item") ||
              semantic_is(node, "macro_invocation")))) {
            ok = false;
            break;
        }
        if (ts_tree_cursor_goto_first_child(&cursor))
            continue;
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor))
                goto done;
        }
    }
done:
    ts_tree_cursor_delete(&cursor);
    return ok;
}

static bool semantic_named_children(TSNode node, const char *const *allowed) {
    if (ts_node_is_null(node))
        return false;
    bool all = true;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            bool found = semantic_comment(child);
            for (int j = 0; !found && allowed[j]; j++)
                found = semantic_is(child, allowed[j]);
            all = found;
        } while (all && ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return all;
}

static const char *semantic_typed_base(CBMArena *arena, TSNode node, const char *source,
                                       CBMLanguage language) {
    TSNode clause =
        semantic_child(node, language == CBM_LANG_CPP ? "base_class_clause" : "base_list");
    if (ts_node_is_null(clause))
        return NULL;
    TSNode base = {0};
    for (uint32_t i = 0; i < ts_node_named_child_count(clause); i++) {
        TSNode child = ts_node_named_child(clause, i);
        if (language == CBM_LANG_CPP && semantic_is(child, "access_specifier"))
            continue;
        if (!ts_node_is_null(base) ||
            !semantic_is(child, language == CBM_LANG_CPP ? "type_identifier" : "identifier"))
            return NULL;
        base = child;
    }
    const char *name = ts_node_is_null(base) ? NULL : cbm_node_text(arena, base, source);
    return semantic_simple_name(name) ? name : NULL;
}

static bool semantic_type_add(semantic_types_t *types, TSNode node, const char *label,
                              const char *package) {
    CBMArena *arena = &types->bindings->arena;
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    const char *name =
        ts_node_is_null(name_node) ? NULL : cbm_node_text(arena, name_node, types->source);
    if (!semantic_simple_name(name) || types->count >= types->capacity) {
        types->file->typed_complete = false;
        return true;
    }
    semantic_class_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
    const char *qn = cbm_arena_sprintf(arena, "%s.%s", types->result->module_qn, name);
    if (!entry || !qn)
        return false;
    entry->source = types->file;
    entry->base = semantic_typed_base(arena, node, types->source, types->info->language);
    entry->declaration_start = ts_node_start_byte(node);
    const cbm_gbuf_node_t *graph = cbm_gbuf_find_by_qn(types->ctx->gbuf, qn);
    uint32_t line = ts_node_start_point(node).row + 1;
    if (graph && graph->label && !strcmp(graph->label, label) && graph->file_path &&
        !strcmp(graph->file_path, types->info->rel_path) && graph->start_line == (int)line)
        entry->node = graph;
    else
        types->file->typed_complete = false;
    semantic_class_t *prior = cbm_ht_get(types->bindings->classes, qn);
    if (prior) {
        prior->ambiguous = true;
        entry->ambiguous = true;
    } else if (!semantic_put(types->bindings->classes, qn, entry))
        return false;
    prior = cbm_ht_get(types->file->type_names, name);
    if (prior) {
        prior->ambiguous = true;
        entry->ambiguous = true;
    } else if (!semantic_put(types->file->type_names, name, entry))
        return false;
    if (types->info->language == CBM_LANG_CSHARP) {
        const char *domain = cbm_arena_sprintf(arena, "cs:%s", package);
        const char *member = domain ? cbm_arena_sprintf(arena, "%s:%s", domain, name) : NULL;
        if (!member)
            return false;
        if (cbm_ht_has(types->bindings->packages, member))
            entry->ambiguous = true;
        if (!domain || !semantic_package_name(types->bindings, domain, name, entry))
            return false;
        if (entry->base)
            entry->package_base = cbm_arena_sprintf(arena, "%s:%s", domain, entry->base);
    } else if (types->info->language == CBM_LANG_CPP && entry->base && types->file->include_module)
        entry->package_base =
            cbm_arena_sprintf(arena, "%s.%s", types->file->include_module, entry->base);
    types->facts[types->count] = (CBMLSPDef){.qualified_name = qn,
                                             .graph_qualified_name = qn,
                                             .short_name = name,
                                             .source_start_line = line};
    types->entries[types->count++] = entry;
    if (types->info->language == CBM_LANG_RUST && !strcmp(label, "Interface")) {
        TSNode visibility = semantic_child(node, "visibility_modifier");
        const char *text =
            ts_node_is_null(visibility) ? NULL : cbm_node_text(arena, visibility, types->source);
        entry->public_class = text && !strcmp(text, "pub");
    }
    return true;
}

static bool semantic_cs_types(semantic_types_t *types, TSNode root, const char *package,
                              unsigned depth) {
    if (depth > 64) {
        types->file->typed_complete = false;
        return true;
    }
    TSNode *roots;
    uint32_t roots_count;
    if (!semantic_named_list(types->temporary, root, &roots, &roots_count))
        return false;
    for (uint32_t i = 0; i < roots_count; i++) {
        TSNode node = roots[i];
        if (semantic_comment(node))
            continue;
        if (semantic_is(node, "namespace_declaration") ||
            semantic_is(node, "file_scoped_namespace_declaration")) {
            TSNode name = ts_node_child_by_field_name(node, "name", 4);
            const char *text = ts_node_is_null(name)
                                   ? NULL
                                   : cbm_node_text(&types->bindings->arena, name, types->source);
            const char *nested = text ? cbm_arena_sprintf(&types->bindings->arena, "%s%s%s",
                                                          package, package[0] ? "." : "", text)
                                      : NULL;
            if (!nested)
                return false;
            if (semantic_is(node, "file_scoped_namespace_declaration"))
                package = nested;
            else if (!semantic_cs_types(types, ts_node_child_by_field_name(node, "body", 4), nested,
                                        depth + 1))
                return false;
            continue;
        }
        static const char *const header[] = {"modifier", "identifier", "base_list",
                                             "declaration_list", NULL};
        static const char *const body[] = {"method_declaration", NULL};
        if (!semantic_is(node, "class_declaration") || !semantic_named_children(node, header) ||
            !semantic_named_children(ts_node_child_by_field_name(node, "body", 4), body)) {
            types->file->typed_complete = false;
            continue;
        }
        /* Partial and special class forms need their own assembly/type rules. */
        for (uint32_t m = 0; m < ts_node_named_child_count(node); m++) {
            TSNode modifier = ts_node_named_child(node, m);
            if (!semantic_is(modifier, "modifier"))
                continue;
            const char *text = cbm_node_text(&types->bindings->arena, modifier, types->source);
            if (!text ||
                (strcmp(text, "public") && strcmp(text, "internal") && strcmp(text, "abstract")))
                types->file->typed_complete = false;
        }
        if (!semantic_type_add(types, node, "Class", package))
            return false;
    }
    return true;
}

static bool semantic_typed_imports(semantic_types_t *types) {
    const char **keys = NULL, **values = NULL;
    int count = 0;
    int status = types->info->language == CBM_LANG_RUST
                     ? cbm_pipeline_rust_import_map(types->ctx->gbuf, types->ctx->project_name,
                                                    types->info->rel_path, types->result, &keys,
                                                    &values, &count)
                     : cbm_pipeline_c_include_map(types->ctx->gbuf, types->ctx->project_name,
                                                  types->info->rel_path, types->result, &keys,
                                                  &values, &count);
    bool ok = status == 0;
    for (int i = 0; ok && i < count; i++) {
        const char *key = cbm_arena_strdup(&types->bindings->arena, keys[i]);
        const char *value = cbm_arena_strdup(&types->bindings->arena, values[i]);
        ok = key && value && semantic_put(types->file->imports, key, (void *)value);
    }
    cbm_pxc_free_import_map(keys, values, count);
    return ok;
}

static bool semantic_cpp_types(semantic_types_t *types, TSNode root) {
    CBMFileResult configuration = {0};
    if (!cbm_result_capture_raw_source(&configuration, "", 0, CBM_LANG_CPP, NULL, NULL) ||
        strcmp(configuration.source_configuration_id, types->result->source_configuration_id)) {
        types->file->typed_complete = false;
        return true;
    }
    if (!semantic_typed_imports(types))
        return false;
    int includes = 0;
    TSNode *roots;
    uint32_t roots_count;
    if (!semantic_named_list(types->temporary, root, &roots, &roots_count))
        return false;
    for (uint32_t i = 0; i < roots_count; i++) {
        TSNode node = roots[i];
        if (!semantic_is(node, "preproc_include"))
            continue;
        TSNode path = ts_node_child_by_field_name(node, "path", 4);
        const char *text = ts_node_is_null(path)
                               ? NULL
                               : cbm_node_text(&types->bindings->arena, path, types->source);
        size_t size = text ? strlen(text) : 0;
        if (++includes != 1 || !semantic_is(path, "string_literal") || size < 3 || text[0] != '"' ||
            text[size - 1] != '"' || strchr(text, '\\')) {
            types->file->typed_complete = false;
            continue;
        }
        const char *name = cbm_arena_strndup(&types->bindings->arena, text + 1, size - 2);
        const char *module = name ? cbm_ht_get(types->file->imports, name) : NULL;
        if (!module || !module[0])
            types->file->typed_complete = false;
        else
            types->file->include_module = module;
        types->file->include_end = ts_node_end_byte(node);
    }
    if (includes != types->result->imports.count)
        types->file->typed_complete = false;
    for (uint32_t i = 0; i < roots_count; i++) {
        TSNode node = roots[i];
        if (semantic_comment(node) || semantic_is(node, "preproc_include"))
            continue;
        static const char *const header[] = {"type_identifier", "base_class_clause",
                                             "field_declaration_list", NULL};
        static const char *const body[] = {"function_definition", "access_specifier", NULL};
        if ((!semantic_is(node, "class_specifier") && !semantic_is(node, "struct_specifier")) ||
            ts_node_is_null(ts_node_child_by_field_name(node, "body", 4)) ||
            !semantic_named_children(node, header) ||
            !semantic_named_children(ts_node_child_by_field_name(node, "body", 4), body)) {
            types->file->typed_complete = false;
            continue;
        }
        if (!semantic_type_add(types, node, "Class", ""))
            return false;
    }
    return true;
}

static bool semantic_rust_impl(semantic_types_t *types, TSNode node) {
    TSNode trait = ts_node_child_by_field_name(node, "trait", 5);
    if (ts_node_is_null(trait))
        return true;
    TSNode type = ts_node_child_by_field_name(node, "type", 4);
    static const char *const header[] = {"type_identifier", "declaration_list", NULL};
    static const char *const body[] = {"function_item", NULL};
    if (!semantic_is(trait, "type_identifier") || !semantic_is(type, "type_identifier") ||
        !semantic_named_children(node, header) ||
        !semantic_named_children(ts_node_child_by_field_name(node, "body", 4), body)) {
        types->file->typed_complete = false;
        return true;
    }
    /* `unsafe impl` and negative impls require distinct language contracts. */
    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
        TSNode child = ts_node_child(node, i);
        if (semantic_is(child, "unsafe") || semantic_is(child, "!")) {
            types->file->typed_complete = false;
            return true;
        }
    }
    const char *trait_name = cbm_node_text(&types->bindings->arena, trait, types->source);
    const char *struct_name = cbm_node_text(&types->bindings->arena, type, types->source);
    if (!semantic_simple_name(trait_name) || !semantic_simple_name(struct_name))
        return true;
    const char *key = cbm_arena_sprintf(&types->bindings->arena, "%s:%s", struct_name, trait_name);
    if (!key)
        return false;
    const char *target = cbm_ht_has(types->file->rust_impls, key) ? "" : trait_name;
    return semantic_put(types->file->rust_impls, key, (void *)target);
}

static bool semantic_rust_types(semantic_types_t *types, TSNode root) {
    if (!semantic_typed_imports(types))
        return false;
    TSNode *roots;
    uint32_t roots_count;
    if (!semantic_named_list(types->temporary, root, &roots, &roots_count))
        return false;
    for (uint32_t i = 0; i < roots_count; i++) {
        TSNode node = roots[i];
        if (semantic_comment(node) || semantic_is(node, "function_item") ||
            semantic_is(node, "use_declaration"))
            continue;
        if (semantic_is(node, "impl_item")) {
            if (!semantic_rust_impl(types, node))
                return false;
            continue;
        }
        if (semantic_is(node, "mod_item") &&
            ts_node_is_null(ts_node_child_by_field_name(node, "body", 4)))
            continue;
        if (semantic_is(node, "struct_item")) {
            if (!semantic_type_add(types, node, "Struct", ""))
                return false;
            continue;
        }
        static const char *const header[] = {"visibility_modifier", "type_identifier",
                                             "declaration_list", NULL};
        static const char *const body[] = {"function_signature_item", NULL};
        if (!semantic_is(node, "trait_item") || !semantic_named_children(node, header) ||
            !semantic_named_children(ts_node_child_by_field_name(node, "body", 4), body)) {
            types->file->typed_complete = false;
            continue;
        }
        if (!semantic_type_add(types, node, "Interface", ""))
            return false;
    }
    return true;
}

static bool semantic_typed_facts(cbm_semantic_bindings_t *bindings, cbm_pipeline_ctx_t *ctx,
                                 semantic_source_t *file, const cbm_file_info_t *info,
                                 const CBMFileResult *result, const char *source, int length,
                                 TSNode root, CBMArena *temporary) {
    file->type_names = cbm_ht_create(0);
    file->rust_impls = cbm_ht_create(0);
    if (!file->type_names || !file->rust_impls || result->defs.count < 0)
        return false;
    file->typed_complete = semantic_unconditional(root, info->language);
    semantic_types_t types = {.bindings = bindings,
                              .ctx = ctx,
                              .file = file,
                              .info = info,
                              .result = result,
                              .source = source,
                              .capacity = result->defs.count,
                              .temporary = temporary};
    types.facts = cbm_arena_calloc(temporary, (size_t)types.capacity * sizeof(*types.facts));
    types.entries = cbm_arena_calloc(temporary, (size_t)types.capacity * sizeof(*types.entries));
    if (types.capacity && (!types.facts || !types.entries))
        return false;
    bool ok = true;
    if (file->typed_complete) {
        if (info->language == CBM_LANG_CSHARP)
            ok = semantic_cs_types(&types, root, "", 0);
        else if (info->language == CBM_LANG_CPP)
            ok = semantic_cpp_types(&types, root);
        else
            ok = semantic_rust_types(&types, root);
    }
    if (!ok)
        return false;
    bool audited = info->language == CBM_LANG_CSHARP
                       ? cbm_cs_collect_source_facts(temporary, source, length, result->module_qn,
                                                     types.facts, types.count)
                   : info->language == CBM_LANG_CPP
                       ? cbm_c_collect_source_facts(temporary, source, length, result->module_qn,
                                                    types.facts, types.count)
                       : cbm_rust_collect_source_facts(temporary, source, length, result->module_qn,
                                                       types.facts, types.count);
    file->typed_complete &= audited;
    for (int i = 0; i < types.count; i++) {
        semantic_class_t *entry = types.entries[i];
        if (info->language == CBM_LANG_RUST && entry->node &&
            !strcmp(entry->node->label, "Interface"))
            continue; /* The exact trait AST above supplies its declaration fact. */
        if (!(types.facts[i].source_flags & CBM_SOURCE_DECL_KNOWN))
            file->typed_complete = false;
        entry->public_class =
            info->language == CBM_LANG_CPP
                ? (types.facts[i].source_flags & CBM_SOURCE_CLASS_DOMAIN_COMPLETE) != 0
                : (types.facts[i].source_flags & CBM_SOURCE_DECL_PUBLIC) != 0;
    }
    if (info->language == CBM_LANG_CSHARP && !file->typed_complete)
        bindings->csharp_complete = false;
    return true;
}

static bool semantic_source_facts(cbm_semantic_bindings_t *bindings, cbm_pipeline_ctx_t *ctx,
                                  semantic_source_t *file, const cbm_file_info_t *info,
                                  CBMFileResult *result) {
    int length = result && result->source ? result->source_len : 0;
    char *owned = result && !result->source ? read_file(info->path, &length) : NULL;
    const char *source = result && result->source ? result->source : owned;
    TSTree *owned_tree = NULL;
    bool ok = true;
    if (!result || result->has_error || result->parse_incomplete || result->parse_unusable ||
        result->walk_truncated || result->lsp_skipped || !source || length <= 0 ||
        !cbm_result_source_matches(result, source, (size_t)length, CBM_SOURCE_ORIGIN_RAW)) {
        if (info->language == CBM_LANG_KOTLIN)
            bindings->kotlin_complete = false;
        if (info->language == CBM_LANG_CSHARP)
            bindings->csharp_complete = false;
        goto done;
    }
    const TSTree *tree = result->cached_tree;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (parser && ts_parser_set_language(parser, cbm_ts_language(info->language)))
            owned_tree =
                info->language == CBM_LANG_RUST
                    ? cbm_parse_rust_file(parser, info->rel_path, source, (uint32_t)length,
                                          (TSParseOptions){0})
                    : cbm_parse_source(parser, source, (uint32_t)length, (TSParseOptions){0});
        if (parser)
            ts_parser_delete(parser);
        tree = owned_tree;
    }
    if (!tree || ts_node_has_error(ts_tree_root_node(tree)) ||
        ts_node_end_byte(ts_tree_root_node(tree)) != (uint32_t)length) {
        if (info->language == CBM_LANG_KOTLIN)
            bindings->kotlin_complete = false;
        if (info->language == CBM_LANG_CSHARP)
            bindings->csharp_complete = false;
        goto done;
    }
    CBMFileResult view = *result;
    view.cached_tree = (TSTree *)tree;
    view.cached_lang = info->language;
    CBMArena temporary;
    cbm_arena_init(&temporary);
    cbm_resolve_binding_index_t *index = NULL;
    TSNode root = ts_tree_root_node(tree);
    file->has_imports = result->imports.count != 0;
    if (semantic_ecma(info->language)) {
        index =
            cbm_resolve_binding_build(&temporary, &view, info->language, source, (size_t)length);
        ok = index && semantic_imports(bindings, ctx, file, info, result, index);
    } else if (info->language == CBM_LANG_KOTLIN) {
        TSNode header = semantic_child(root, "package_header");
        TSNode name = semantic_child(header, "identifier");
        file->package = ts_node_is_null(header) ? ""
                        : ts_node_is_null(name) ? NULL
                                                : cbm_node_text(&bindings->arena, name, source);
        if (!file->package)
            ok = false;
    }
    if (ok && semantic_typed_language(info->language))
        ok = semantic_typed_facts(bindings, ctx, file, info, result, source, length, root,
                                  &temporary);
    else if (ok && info->language == CBM_LANG_PHP)
        ok =
            semantic_php_facts(bindings, ctx, file, info, result, source, length, root, &temporary);
    else if (ok)
        ok = semantic_class_facts(bindings, ctx, file, info, result, source, length, root, index,
                                  &temporary);
    cbm_arena_destroy(&temporary);
done:
    if (owned_tree)
        ts_tree_delete(owned_tree);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, owned);
    return ok;
}

static void semantic_check_domain(const cbm_gbuf_node_t *node, void *userdata) {
    cbm_semantic_bindings_t *bindings = userdata;
    if (node->label && !strcmp(node->label, "File") &&
        (!node->file_path || !cbm_ht_has(bindings->files, node->file_path))) {
        bindings->kotlin_complete = false;
        bindings->csharp_complete = false;
    }
}

cbm_semantic_bindings_t *cbm_semantic_bindings_build(cbm_pipeline_ctx_t *ctx,
                                                     const cbm_file_info_t *files, int count,
                                                     CBMFileResult **cache) {
    if (!ctx || count < 0 || (count && !files))
        return NULL;
    bool have_census = false, relevant = false;
    for (int i = 0; i < count; i++) {
        have_census |= files[i].language == CBM_LANG_KOTLIN || files[i].language == CBM_LANG_CSHARP;
        relevant |= files[i].language == CBM_LANG_KOTLIN || files[i].language == CBM_LANG_PHP ||
                    semantic_ecma(files[i].language) || semantic_typed_language(files[i].language);
    }
    cbm_semantic_bindings_t *bindings = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*bindings));
    if (!bindings)
        return NULL;
    cbm_arena_init(&bindings->arena);
    bindings->files = cbm_ht_create(0);
    bindings->classes = cbm_ht_create(0);
    bindings->packages = cbm_ht_create(0);
    bindings->kotlin_complete = true;
    bindings->csharp_complete = true;
    if (!bindings->files || !bindings->classes || !bindings->packages)
        goto failed;
    if (!relevant)
        return bindings;
    for (int i = 0; i < count; i++) {
        if (!have_census && files[i].language != CBM_LANG_PHP &&
            !semantic_ecma(files[i].language) && !semantic_typed_language(files[i].language))
            continue;
        semantic_source_t *file = cbm_arena_calloc(&bindings->arena, sizeof(*file));
        const char *key = cbm_arena_strdup(&bindings->arena, files[i].rel_path);
        if (!file || !key || cbm_ht_has(bindings->files, key))
            goto failed;
        file->language = files[i].language;
        if (!semantic_put(bindings->files, key, file))
            goto failed;
        if (file->language != CBM_LANG_KOTLIN && file->language != CBM_LANG_PHP &&
            !semantic_ecma(file->language) && !semantic_typed_language(file->language))
            continue;
        file->imports = cbm_ht_create(0);
        if (!file->imports)
            goto failed;
        bool loaded = false;
        CBMFileResult *result = cbm_pipeline_result_acquire(ctx, cache, i, NULL, &loaded);
        bool ok = semantic_source_facts(bindings, ctx, file, &files[i], result);
        cbm_pipeline_result_release(result, loaded);
        if (!ok)
            goto failed;
    }
    if (have_census)
        cbm_gbuf_foreach_node(ctx->gbuf, semantic_check_domain, bindings);
    return bindings;
failed:
    cbm_semantic_bindings_free(bindings);
    return NULL;
}

const char *cbm_semantic_class_base(const cbm_semantic_bindings_t *bindings,
                                    const CBMDefinition *definition, const char *base,
                                    const cbm_gbuf_node_t *candidate) {
    /* Existing same-file resolution and non-class relations retain their
     * established path. Every new cross-file Class target needs this audit. */
    if (!bindings || !definition->qualified_name)
        return NULL;
    const semantic_source_t *declaration_source =
        definition->file_path ? cbm_ht_get(bindings->files, definition->file_path) : NULL;
    if (candidate && candidate->label && strcmp(candidate->label, "Class") &&
        (!declaration_source || declaration_source->language != CBM_LANG_CPP))
        return candidate->qualified_name;
    const semantic_class_t *owner = cbm_ht_get(bindings->classes, definition->qualified_name);
    if (declaration_source && declaration_source->language == CBM_LANG_PHP &&
        (!owner || !semantic_simple_name(base)))
        return candidate ? candidate->qualified_name : NULL;
    if (!semantic_simple_name(base))
        return NULL;
    if (candidate && candidate->file_path && definition->file_path &&
        !strcmp(candidate->file_path, definition->file_path) &&
        (!owner || !semantic_ecma(owner->source->language) ||
         !cbm_ht_has(owner->source->imports, base)))
        return candidate->qualified_name;
    /* Existing exact class-use bindings retain their established path.
     * Only the new direct include path consumes the stricter runtime census. */
    if (candidate && owner && owner->source->language == CBM_LANG_PHP &&
        owner->source->php_use_imports)
        return candidate->qualified_name;
    if (!owner || !owner->node || owner->ambiguous || !owner->base || strcmp(owner->base, base))
        return NULL;
    const semantic_class_t *target = NULL;
    if (owner->source->language == CBM_LANG_CSHARP) {
        if (!bindings->csharp_complete || !owner->source->typed_complete)
            return NULL;
        target = owner->package_base ? cbm_ht_get(bindings->packages, owner->package_base) : NULL;
        if (!target || !target->source->typed_complete)
            return NULL;
        if (target->source == owner->source && !target->ambiguous && target->node)
            return target->node->qualified_name;
    } else if (owner->source->language == CBM_LANG_CPP) {
        if (!owner->source->typed_complete)
            return NULL;
        target = cbm_ht_get(owner->source->type_names, base);
        if (target) {
            if (target->declaration_start >= owner->declaration_start)
                return NULL;
        } else {
            if (!owner->source->include_module ||
                owner->source->include_end > owner->declaration_start)
                return NULL;
            target =
                owner->package_base ? cbm_ht_get(bindings->classes, owner->package_base) : NULL;
            if (!target || target->source->has_imports)
                return NULL;
        }
        if (!target || target->source->language != CBM_LANG_CPP || !target->source->typed_complete)
            return NULL;
    } else if (semantic_ecma(owner->source->language)) {
        const char *qn = cbm_ht_get(owner->source->imports, base);
        target = qn ? cbm_ht_get(bindings->classes, qn) : NULL;
    } else if (owner->source->language == CBM_LANG_PHP) {
        const semantic_source_t *source = owner->source;
        if (!source->php_complete || !source->php_include ||
            owner->php_start < source->php_include_end || !owner->package_base ||
            cbm_ht_has(source->php_names, owner->package_base))
            return NULL;
        const semantic_source_t *included = cbm_ht_get(bindings->files, source->php_include);
        if (!included || included->language != CBM_LANG_PHP || !included->php_complete ||
            !included->php_inert || !included->php_names)
            return NULL;
        target = cbm_ht_get(included->php_names, owner->package_base);
        if (!target || !target->php_standalone)
            return NULL;
    } else if (owner->source->language == CBM_LANG_KOTLIN && bindings->kotlin_complete &&
               !owner->source->has_imports) {
        target = owner->package_base ? cbm_ht_get(bindings->packages, owner->package_base) : NULL;
    }
    return target && !target->ambiguous && target->public_class && target->node
               ? target->node->qualified_name
               : NULL;
}

/* The impl occurrence and both declarations must belong to the same audited
 * snapshot. Imports already encode crate/module membership, not graph tails. */
bool cbm_semantic_rust_implementation(const cbm_semantic_bindings_t *bindings, const char *rel,
                                      const CBMImplTrait *implementation, const char **struct_qn,
                                      const char **trait_qn) {
    *struct_qn = NULL;
    *trait_qn = NULL;
    if (!bindings || !rel || !implementation ||
        !semantic_simple_name(implementation->struct_name) ||
        !semantic_simple_name(implementation->trait_name))
        return false;
    const semantic_source_t *file = cbm_ht_get(bindings->files, rel);
    if (!file || file->language != CBM_LANG_RUST || !file->typed_complete)
        return false;
    char key[1024];
    int size = snprintf(key, sizeof(key), "%s:%s", implementation->struct_name,
                        implementation->trait_name);
    if (size <= 0 || (size_t)size >= sizeof(key))
        return false;
    const char *proven = cbm_ht_get(file->rust_impls, key);
    if (!proven || !proven[0])
        return false;
    const semantic_class_t *owner = cbm_ht_get(file->type_names, implementation->struct_name);
    const semantic_class_t *target = cbm_ht_get(file->type_names, implementation->trait_name);
    const char *imported = cbm_ht_get(file->imports, implementation->trait_name);
    if (target && imported)
        return false;
    if (!target && imported && imported[0])
        target = cbm_ht_get(bindings->classes, imported);
    if (!owner || owner->ambiguous || !owner->node || strcmp(owner->node->label, "Struct") ||
        !implementation->struct_qn ||
        strcmp(owner->node->qualified_name, implementation->struct_qn) || !target ||
        target->ambiguous || !target->node || strcmp(target->node->label, "Interface") ||
        !target->source->typed_complete || (target->source != file && !target->public_class) ||
        cbm_ht_has(file->imports, implementation->struct_name))
        return false;
    *struct_qn = owner->node->qualified_name;
    *trait_qn = target->node->qualified_name;
    return true;
}

static const char *itoa_log(int val) {
    enum { RING_BUF_COUNT = 4, RING_BUF_MASK = 3 };
    static CBM_TLS char bufs[RING_BUF_COUNT][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & RING_BUF_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Build per-file import map from cached extraction result or graph buffer
 * edges. */
static int build_import_map(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                            const CBMFileResult *result, const char ***out_keys,
                            const char ***out_vals, int *out_count, CBMLanguage lang) {
    if (semantic_ecma(lang)) {
        int rc = cbm_pipeline_ecma_import_map(ctx, rel_path, result, false, out_keys, out_vals,
                                              out_count);
        if (rc != 0)
            atomic_store(&ctx->resolver_failed, true);
        return rc;
    }
    if (lang == CBM_LANG_PHP) {
        int rc = cbm_pipeline_php_import_map(ctx->gbuf, ctx->project_name, rel_path, result,
                                             out_keys, out_vals, out_count);
        if (rc != 0)
            atomic_store(&ctx->resolver_failed, true);
        return rc;
    }
    if (lang == CBM_LANG_PYTHON) {
        int rc = cbm_pipeline_python_import_map(ctx->gbuf, ctx->project_name, rel_path, result,
                                                true, out_keys, out_vals, out_count);
        if (rc != 0)
            atomic_store(&ctx->resolver_failed, true);
        return rc;
    }
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    /* Fast path: build from cached extraction result (no JSON parsing) */
    if (result && result->imports.count > 0) {
        const char **keys =
            cbm_calloc(CBM_MEM_CLASS_SEMANTIC,
                       (size_t)((size_t)result->imports.count) * (sizeof(const char *)));
        const char **vals =
            cbm_calloc(CBM_MEM_CLASS_SEMANTIC,
                       (size_t)((size_t)result->imports.count) * (sizeof(const char *)));
        int count = 0;

        for (int i = 0; i < result->imports.count; i++) {
            const CBMImport *imp = &result->imports.items[i];
            if (!imp->local_name || !imp->local_name[0] || !imp->module_path) {
                continue;
            }
            char *target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            free(target_qn);
            if (!target) {
                continue;
            }
            keys[count] = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, imp->local_name);
            vals[count] = target->qualified_name;
            count++;
        }

        *out_keys = keys;
        *out_vals = vals;
        *out_count = count;
        return 0;
    }

    /* Slow path: scan graph buffer IMPORTS edges + parse JSON properties */
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    free(file_qn);
    if (!file_node) {
        return 0;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc = cbm_gbuf_find_edges_by_source_type(ctx->gbuf, file_node->id, "IMPORTS", &edges,
                                                &edge_count);
    if (rc != 0 || edge_count == 0) {
        return 0;
    }

    const char **keys =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)(edge_count) * (sizeof(const char *)));
    const char **vals =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)(edge_count) * (sizeof(const char *)));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, e->target_id);
        if (!target || !e->properties_json) {
            continue;
        }

        const char *start = strstr(e->properties_json, "\"local_name\":\"");
        if (start) {
            start += strlen("\"local_name\":\"");
            const char *end = strchr(start, '"');
            if (end && end > start) {
                keys[count] = cbm_strndup(start, end - start);
                vals[count] = target->qualified_name;
                count++;
            }
        }
    }

    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;
}

static void free_import_map(const char **keys, const char **vals, int count, CBMLanguage lang) {
    if (lang == CBM_LANG_PYTHON || lang == CBM_LANG_PHP || semantic_ecma(lang)) {
        cbm_pxc_free_import_map(keys, vals, count);
        return;
    }
    if (keys) {
        for (int i = 0; i < count; i++) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)keys[i]);
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)keys);
    }
    if (vals) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)vals);
    }
}

/* Resolve a class/type name through the registry. Returns borrowed QN or NULL.
 */
static const char *resolve_as_class(const cbm_registry_t *reg, const char *name,
                                    const char *module_qn, const char **imp_keys,
                                    const char **imp_vals, int imp_count, CBMLanguage lang) {
    cbm_resolution_t res =
        cbm_registry_resolve_lang(reg, name, module_qn, imp_keys, imp_vals, imp_count, lang);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return NULL;
    }

    /* Verify it's a type-like container (Class/Struct/Interface/Enum/Type/Trait):
     * a base/embedded type, impl receiver, or inheritance target must resolve to
     * one of these. Struct included so Rust/Go/Swift/D `impl Trait for S` and Go
     * struct embedding resolve. */
    const char *label = cbm_registry_label_of(reg, res.qualified_name);
    if (!cbm_label_is_type_like(label)) {
        return NULL;
    }
    return res.qualified_name;
}

/* Extract decorator function name from the raw attribute/annotation text.
 * Handles the per-language surface syntaxes:
 *   Python/TS:  "@app.route('/api')"   → "app.route"
 *   Java/Kotlin:"@Override"             → "Override"
 *   C#:         "[Log]" / "[Log(...)]"  → "Log"
 *   Rust:       "#[derive(Debug)]"      → "derive"
 *               "#[allow(dead_code)]"   → "allow"
 *   PHP 8:      "#[Route('/users')]"    → "Route"
 *   Swift:      "@discardableResult"    → "discardableResult"
 *   Scala:      "@deprecated(...)"      → "deprecated"
 * Returns the leading identifier path, stopping at the first '(' or '['
 * argument list and trimming any wrapping bracket/`@`/`#` punctuation. */
static void extract_decorator_func(const char *dec, char *out, size_t outsz) {
    out[0] = '\0';
    if (!dec) {
        return;
    }
    const char *start = dec;
    /* Strip leading sigils: '@' (Py/TS/JVM/Swift/Scala), '#' + '[' (Rust/PHP8),
     * or a bare '[' (C# attribute). */
    while (*start == '@' || *start == '#' || *start == '[' || *start == ' ') {
        start++;
    }
    /* The name ends at the first argument list ('(' or '[') or wrapper ']'. */
    size_t len = 0;
    while (start[len] && start[len] != '(' && start[len] != '[' && start[len] != ']' &&
           start[len] != ' ' && start[len] != ',') {
        len++;
    }
    if (len == 0 || len >= outsz) {
        return;
    }
    memcpy(out, start, len);
    out[len] = '\0';
}

/* ── Go-style implicit interface satisfaction ─────────────────── */

/* Check if file_path ends with a suffix. */
static bool fp_ends_with(const char *fp, const char *suffix) {
    if (!fp || !suffix) {
        return false;
    }
    size_t fplen = strlen(fp);
    size_t sflen = strlen(suffix);
    return fplen >= sflen && strcmp(fp + fplen - sflen, suffix) == 0;
}

/* Info about one interface method (name + node ID). */
typedef struct {
    const char *name;
    int64_t id;
} go_imethod_t;

/* Check if class has all interface methods and create IMPLEMENTS + OVERRIDE
 * edges. */
static int check_go_class_implements(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *cls,
                                     const cbm_gbuf_node_t *iface, const go_imethod_t *imethods,
                                     int im_count) {
    if (!cls->file_path || !cls->qualified_name) {
        return 0;
    }
    if (!fp_ends_with(cls->file_path, ".go")) {
        return 0;
    }
    /* Resolve the struct's methods two ways and use whichever finds them:
     *   (a) the struct's DEFINES_METHOD edges, matched by method NAME — the real
     *       pipeline path. Go method QNs are receiver-qualified since #1913
     *       ("pkg.Circle.Area"), so (b) matches them as well, but the edges
     *       stay the primary join; and
     *   (b) the reconstructed QN "<ClassQN>.<methodName>" — used by tests and any
     *       extractor that does emit class-qualified method QNs without
     *       DEFINES_METHOD edges from the class. */
    const cbm_gbuf_edge_t **cls_dm = NULL;
    int cls_dm_count = 0;
    cbm_gbuf_find_edges_by_source_type(ctx->gbuf, cls->id, "DEFINES_METHOD", &cls_dm,
                                       &cls_dm_count);

    char prefix[CBM_SZ_512];
    snprintf(prefix, sizeof(prefix), "%s.", cls->qualified_name);

    /* For each interface method, find the matching struct method node. */
    const cbm_gbuf_node_t *matched[CBM_SZ_128];
    for (int m = 0; m < im_count; m++) {
        const cbm_gbuf_node_t *found = NULL;
        /* (a) DEFINES_METHOD edge by name */
        for (int d = 0; d < cls_dm_count; d++) {
            const cbm_gbuf_node_t *cm = cbm_gbuf_find_by_id(ctx->gbuf, cls_dm[d]->target_id);
            if (cm && cm->name && strcmp(cm->name, imethods[m].name) == 0) {
                found = cm;
                break;
            }
        }
        /* (b) reconstructed "<ClassQN>.<methodName>" */
        if (!found) {
            char method_qn[CBM_SZ_512];
            snprintf(method_qn, sizeof(method_qn), "%s%s", prefix, imethods[m].name);
            found = cbm_gbuf_find_by_qn(ctx->gbuf, method_qn);
        }
        if (!found) {
            return 0; /* struct does not satisfy the interface */
        }
        matched[m] = found;
    }
    cbm_gbuf_insert_edge(ctx->gbuf, cls->id, iface->id, "IMPLEMENTS", "{}");
    int edges = SKIP_ONE;
    for (int m = 0; m < im_count; m++) {
        cbm_gbuf_insert_edge(ctx->gbuf, matched[m]->id, imethods[m].id, "OVERRIDE", "{}");
        edges++;
    }
    return edges;
}

/* ── Method-name index over the concrete types ────────────────────
 *
 * Checking every interface against every struct and named type is
 * interfaces x types work: the scaling lane measured it at 4.00x per 2x corpus
 * (2026-09-17). A type can only satisfy an interface if it supplies EVERY
 * method name, so it is in the candidate list of each of those names: checking
 * just the shortest such list, in the original order, runs exactly the checks
 * that can succeed, in exactly the order they ran before -- the edges and their
 * insertion order are unchanged. A type supplies a name by either lookup
 * check_go_class_implements uses: a DEFINES_METHOD edge to a node of that name,
 * or a node whose QN is "<TypeQN>.<name>". */
typedef struct {
    int *items; /* ascending type indices, no duplicates once sealed */
    int count;
    int cap;
} go_type_list_t;

typedef struct {
    CBMHashTable *by_name;    /* method name (borrowed) -> go_type_list_t* */
    CBMHashTable *type_by_qn; /* type QN (borrowed) -> (void *)(index + 1) */
} go_method_index_t;

static void go_index_add(go_method_index_t *ix, const char *name, int type_index) {
    go_type_list_t *list = cbm_ht_get(ix->by_name, name);
    if (!list) {
        list = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*list));
        if (!list) {
            return;
        }
        cbm_ht_set(ix->by_name, name, list);
    }
    if (list->count > 0 && list->items[list->count - 1] == type_index) {
        return;
    }
    if (list->count == list->cap) {
        int cap = list->cap ? list->cap * PAIR_LEN : CBM_SZ_4;
        int *grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, list->items, (size_t)cap * sizeof(int));
        if (!grown) {
            return;
        }
        list->items = grown;
        list->cap = cap;
    }
    list->items[list->count++] = type_index;
}

static void go_index_visit_node(const cbm_gbuf_node_t *node, void *userdata) {
    go_method_index_t *ix = userdata;
    const char *qn = node->qualified_name;
    /* Last '.' of the base QN (a callable identity suffix never holds one). */
    const char *dot = NULL;
    size_t base_len = cbm_qn_callable_base_len_named(qn, node->name);
    for (size_t i = 0; qn && i < base_len; i++) {
        if (qn[i] == '.') {
            dot = qn + i;
        }
    }
    if (!dot || dot == qn || !dot[SKIP_ONE]) {
        return;
    }
    char parent[CBM_SZ_512];
    size_t len = (size_t)(dot - qn);
    if (len >= sizeof(parent)) {
        return; /* longer than the lookup buffer check_go_class_implements builds */
    }
    memcpy(parent, qn, len);
    parent[len] = '\0';
    void *slot = cbm_ht_get(ix->type_by_qn, parent);
    if (slot) {
        go_index_add(ix, dot + SKIP_ONE, (int)((intptr_t)slot - SKIP_ONE));
    }
}

static int cmp_int_asc(const void *a, const void *b) {
    int x = *(const int *)a;
    int y = *(const int *)b;
    return (x > y) - (x < y);
}

static void go_index_seal_list(const char *key, void *val, void *userdata) {
    (void)key;
    (void)userdata;
    go_type_list_t *list = val;
    if (list->count < PAIR_LEN) {
        return;
    }
    qsort(list->items, (size_t)list->count, sizeof(int), cmp_int_asc);
    int out = SKIP_ONE;
    for (int i = SKIP_ONE; i < list->count; i++) {
        if (list->items[i] != list->items[out - SKIP_ONE]) {
            list->items[out++] = list->items[i];
        }
    }
    list->count = out;
}

static void go_index_free_list(const char *key, void *val, void *userdata) {
    (void)key;
    (void)userdata;
    go_type_list_t *list = val;
    cbm_free(CBM_MEM_CLASS_SEMANTIC, list->items);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, list);
}

int cbm_pipeline_implements_go(cbm_pipeline_ctx_t *ctx) {
    int edge_count = 0;

    /* Find all Interface nodes */
    const cbm_gbuf_node_t **ifaces = NULL;
    int iface_count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Interface", &ifaces, &iface_count) != 0) {
        return 0;
    }

    /* Find candidate concrete types. In Go the type that satisfies an interface
     * is a struct (now labelled "Struct") or a named type (labelled "Class");
     * both sets are checked. Each call returns a borrowed internal array (no
     * free). */
    const cbm_gbuf_node_t **classes = NULL;
    int class_count = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &class_count);
    const cbm_gbuf_node_t **structs = NULL;
    int struct_count = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Struct", &structs, &struct_count);
    if (class_count == 0 && struct_count == 0) {
        return 0;
    }

    /* Candidate index (see go_method_index_t). Type index t is the position in
     * the original iteration order: structs first, then classes. */
    int type_count = struct_count + class_count;
    const cbm_gbuf_node_t **types =
        cbm_alloc(CBM_MEM_CLASS_SEMANTIC, (size_t)type_count * sizeof(*types));
    go_method_index_t ix = {
        .by_name = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, (uint32_t)CBM_SZ_1K),
        .type_by_qn = cbm_ht_create_in(CBM_MEM_CLASS_SEMANTIC, (uint32_t)type_count),
    };
    bool indexed = types && ix.by_name && ix.type_by_qn;
    if (indexed) {
        for (int c = 0; c < struct_count; c++) {
            types[c] = structs[c];
        }
        for (int c = 0; c < class_count; c++) {
            types[struct_count + c] = classes[c];
        }
        for (int t = 0; t < type_count; t++) {
            if (types[t]->qualified_name) {
                cbm_ht_set(ix.type_by_qn, types[t]->qualified_name,
                           (void *)(intptr_t)(t + SKIP_ONE));
            }
            const cbm_gbuf_edge_t **tdm = NULL;
            int tdm_count = 0;
            cbm_gbuf_find_edges_by_source_type(ctx->gbuf, types[t]->id, "DEFINES_METHOD", &tdm,
                                               &tdm_count);
            for (int d = 0; d < tdm_count; d++) {
                const cbm_gbuf_node_t *cm = cbm_gbuf_find_by_id(ctx->gbuf, tdm[d]->target_id);
                if (cm && cm->name) {
                    go_index_add(&ix, cm->name, t);
                }
            }
        }
        cbm_gbuf_foreach_node(ctx->gbuf, go_index_visit_node, &ix);
        cbm_ht_foreach(ix.by_name, go_index_seal_list, NULL);
    }

    for (int i = 0; i < iface_count; i++) {
        const cbm_gbuf_node_t *iface = ifaces[i];
        if (!iface->file_path || !fp_ends_with(iface->file_path, ".go")) {
            continue;
        }

        /* Get interface methods via DEFINES_METHOD edges */
        const cbm_gbuf_edge_t **dm_edges = NULL;
        int dm_count = 0;
        if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, iface->id, "DEFINES_METHOD", &dm_edges,
                                               &dm_count) != 0 ||
            dm_count == 0) {
            continue;
        }

        /* Collect interface method info */
        go_imethod_t imethods[CBM_SZ_128];
        int im_count = 0;
        for (int j = 0; j < dm_count && im_count < CBM_SZ_128; j++) {
            const cbm_gbuf_node_t *m = cbm_gbuf_find_by_id(ctx->gbuf, dm_edges[j]->target_id);
            if (m && m->name) {
                imethods[im_count++] = (go_imethod_t){m->name, m->id};
            }
        }
        if (im_count == 0) {
            continue;
        }

        if (!indexed) {
            /* Index unavailable (allocation failure): the full scan. */
            for (int c = 0; c < struct_count; c++) {
                edge_count += check_go_class_implements(ctx, structs[c], iface, imethods, im_count);
            }
            for (int c = 0; c < class_count; c++) {
                edge_count += check_go_class_implements(ctx, classes[c], iface, imethods, im_count);
            }
            continue;
        }
        /* Only the types that supply the interface's rarest method name. */
        const go_type_list_t *shortest = NULL;
        bool missing = false;
        for (int m = 0; m < im_count; m++) {
            const go_type_list_t *list = cbm_ht_get(ix.by_name, imethods[m].name);
            if (!list || list->count == 0) {
                missing = true; /* no type supplies this name: none can satisfy */
                break;
            }
            if (!shortest || list->count < shortest->count) {
                shortest = list;
            }
        }
        if (missing || !shortest) {
            continue;
        }
        for (int k = 0; k < shortest->count; k++) {
            edge_count += check_go_class_implements(ctx, types[shortest->items[k]], iface, imethods,
                                                    im_count);
        }
    }
    if (ix.by_name) {
        cbm_ht_foreach(ix.by_name, go_index_free_list, NULL);
        cbm_ht_free(ix.by_name);
    }
    cbm_ht_free(ix.type_by_qn);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, types);
    return edge_count;
}

/* Build the QN of the synthetic node that stands in for an external/stdlib
 * decorator whose target is not a local symbol (Rust `#[derive(Debug)]`, Swift
 * `@discardableResult`, Scala `@deprecated`, Python `@cache`, Java `@Override`,
 * ...).  Namespaced with `<decorator:` so it can never collide with a real
 * symbol and so all uses of the same decorator name across a project dedupe to
 * one node by QN. */
static void synth_decorator_qn(const char *func_name, char *out, size_t outsz) {
    snprintf(out, outsz, "<decorator:%s>", func_name);
}

/* Process INHERITS + DECORATES edges for one definition. */
/* Resolve one decorator and create DECORATES edge. */
static void resolve_decorator(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                              const char *decorator, const char *module_qn, const char **imp_keys,
                              const char **imp_vals, int imp_count, int *count, CBMLanguage lang) {
    char func_name[CBM_SZ_256];
    extract_decorator_func(decorator, func_name, sizeof(func_name));
    if (func_name[0] == '\0') {
        return;
    }
    cbm_resolution_t res = cbm_registry_resolve_lang(ctx->registry, func_name, module_qn, imp_keys,
                                                     imp_vals, imp_count, lang);
    if ((!res.qualified_name || res.qualified_name[0] == '\0') && !strchr(func_name, '.')) {
        /* C# attributes are referenced by their short name (`[Log]`) but declared
         * with the conventional `Attribute` suffix (`class LogAttribute`).  Retry
         * with the suffix appended so the declaration resolves. */
        char with_suffix[CBM_SZ_256];
        int wn = snprintf(with_suffix, sizeof(with_suffix), "%sAttribute", func_name);
        if (wn > 0 && (size_t)wn < sizeof(with_suffix)) {
            res = cbm_registry_resolve_lang(ctx->registry, with_suffix, module_qn, imp_keys,
                                            imp_vals, imp_count, lang);
        }
    }
    const cbm_gbuf_node_t *dec = NULL;
    if (res.qualified_name && res.qualified_name[0] != '\0') {
        dec = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
    }
    /* A qualified Rust proc-macro path can collide with the decorated
     * function's own name: resolving `#[tokio::main]` from module `main`
     * may fall back from `tokio::main` to same-module `main`.  That is not a
     * local decorator target (and the self-edge is discarded below), so keep
     * the full external spelling by materialising the synthetic decorator. */
    if (dec && dec->id == node->id && decorator[0] == '#' && decorator[1] == '[' &&
        strstr(func_name, "::")) {
        dec = NULL;
    }
    if (!dec) {
        /* The decorator target is not a local symbol (external attribute /
         * stdlib annotation / proc-macro derive).  Materialise a synthetic
         * "Decorator" node so the DECORATES relation is still recorded.
         * Upsert dedupes by QN, so every use of the same decorator name shares
         * one node (one extra node per distinct external decorator, project-
         * wide).  Created in the single-threaded semantic pass, so direct
         * ctx->gbuf mutation is safe here (mirrors pass_route_nodes). */
        char syn_qn[CBM_SZ_512];
        synth_decorator_qn(func_name, syn_qn, sizeof(syn_qn));
        int64_t syn_id =
            cbm_gbuf_upsert_node(ctx->gbuf, "Decorator", func_name, syn_qn, "", 0, 0, "{}");
        if (syn_id != 0) {
            dec = cbm_gbuf_find_by_qn(ctx->gbuf, syn_qn);
        }
    }
    if (dec && node->id != dec->id) {
        /* Decorator source text can contain quotes and raw newlines — escape
         * it or the edge properties JSON is malformed (twin of the parallel
         * path in pass_parallel.c). */
        char esc_dec[CBM_SZ_256];
        cbm_json_escape(esc_dec, sizeof(esc_dec), decorator);
        char props[CBM_SZ_512];
        snprintf(props, sizeof(props), "{\"decorator\":\"%s\"}", esc_dec);
        cbm_gbuf_insert_edge(ctx->gbuf, node->id, dec->id, "DECORATES", props);
        /* Ensure a reference edge exists so the decorator appears in usage queries
         * without being misclassified as a real call by downstream passes. */
        cbm_gbuf_insert_edge(ctx->gbuf, node->id, dec->id, "USAGE", "{}");
        (*count)++;
    }
}

static void sem_process_def_edges(cbm_pipeline_ctx_t *ctx, const cbm_semantic_bindings_t *bindings,
                                  const CBMDefinition *def, const cbm_file_info_t *fi,
                                  const CBMFileResult *result, const char *module_qn,
                                  const char **imp_keys, const char **imp_vals, int imp_count,
                                  int *inherits_count, int *decorates_count, CBMLanguage lang,
                                  const cbm_php_vendor_names_t *php_vendor) {
    if (!def->qualified_name) {
        return;
    }
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
    if (!node) {
        return;
    }
    if (def->base_classes) {
        for (int b = 0; def->base_classes[b]; b++) {
            /* #1186: `extends Request` with `use Saloon\Http\Request` names the
             * vendor class, never a same-named project class. Mirrors
             * pass_parallel.c. */
            if (cbm_pipeline_php_vendor_bound(php_vendor, def->base_classes[b])) {
                continue;
            }
            /* An earlier sibling of the class body shadows every module or
             * imported name, so it needs no external-base gate. */
            const char *base_qn = cbm_registry_resolve_python_class_body_base(
                ctx->registry, ctx->gbuf, def->qualified_name, def->base_classes[b], lang);
            if (!base_qn) {
                base_qn = resolve_as_class(ctx->registry, def->base_classes[b], module_qn, imp_keys,
                                           imp_vals, imp_count, lang);
                if ((semantic_ecma(lang) || lang == CBM_LANG_KOTLIN || lang == CBM_LANG_PHP ||
                     lang == CBM_LANG_CSHARP || lang == CBM_LANG_CPP) &&
                    def->label && !strcmp(def->label, "Class"))
                    base_qn = cbm_semantic_class_base(
                        bindings, def, def->base_classes[b],
                        base_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, base_qn) : NULL);
                if (!base_qn || cbm_python_external_base_contradicts(
                                    fi->language, &result->imports, def->base_classes[b], base_qn,
                                    ctx->gbuf, ctx->project_name, fi->rel_path)) {
                    continue; /* unresolved, or an external base (`unittest.TestCase`) */
                }
            }
            const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(ctx->gbuf, base_qn);
            if (base_node && node->id != base_node->id) {
                /* A base that resolves to an Interface is an IMPLEMENTS relation
                 * (Java `implements`, C# `: IFace`, TS `implements`); a Class/
                 * Type/Enum base is plain INHERITS. Keyed off the target NODE's
                 * label — the graph truth the edge attaches to — so the
                 * sequential and parallel venues cannot diverge (the parallel
                 * path once hardcoded INHERITS and demoted every explicit
                 * implements at scale). */
                cbm_gbuf_insert_edge(ctx->gbuf, node->id, base_node->id,
                                     cbm_semantic_base_edge_type(base_node), "{}");
                (*inherits_count)++;
            }
        }
    }
    /* Bases only the shared resolver can follow: reexports and star imports. */
    for (size_t b = 0; lang == CBM_LANG_PYTHON; b++) {
        const char *base_qn =
            cbm_shared_resolver_python_base(ctx->shared_resolver, def->qualified_name, b);
        if (!base_qn)
            break;
        const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(ctx->gbuf, base_qn);
        if (base_node && node->id != base_node->id) {
            cbm_gbuf_insert_edge(ctx->gbuf, node->id, base_node->id,
                                 cbm_semantic_base_edge_type(base_node), "{}");
            (*inherits_count)++;
        }
    }
    for (size_t b = 0;; b++) {
        const char *base_qn =
            cbm_shared_resolver_source_base(ctx->shared_resolver, lang, def->qualified_name, b);
        if (!base_qn)
            break;
        const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(ctx->gbuf, base_qn);
        if (base_node && node->id != base_node->id) {
            cbm_gbuf_insert_edge(ctx->gbuf, node->id, base_node->id,
                                 cbm_semantic_base_edge_type(base_node), "{}");
            (*inherits_count)++;
        }
    }
    if (def->decorators) {
        for (int dc = 0; def->decorators[dc]; dc++) {
            resolve_decorator(ctx, node, def->decorators[dc], module_qn, imp_keys, imp_vals,
                              imp_count, decorates_count, lang);
        }
    }
}

/* Get extraction result from cache or re-extract. Sets *owned=true if caller
 * must free. */
static CBMFileResult *sem_get_or_extract(cbm_pipeline_ctx_t *ctx, int file_idx,
                                         const cbm_file_info_t *fi, bool *owned) {
    *owned = false;
    if (ctx->result_cache && ctx->result_cache[file_idx]) {
        return ctx->result_cache[file_idx];
    }
    int source_len = 0;
    char *source = read_file(fi->path, &source_len);
    if (!source) {
        if (source_len != 0) {
            (void)cbm_pipeline_test_extraction_ok(ctx, fi->language, NULL);
        }
        return NULL;
    }
    CBMFileResult *r =
        cbm_pipeline_test_force_extract_null(ctx, fi->language)
            ? NULL
            : cbm_extract_file_ex_with_tests(source, source_len, fi->language, ctx->project_name,
                                             fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL, NULL,
                                             NULL, ctx->test_declarations);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, source);
    (void)cbm_pipeline_test_extraction_ok(ctx, fi->language, r);
    if (r) {
        *owned = true;
    }
    return r;
}

/* Resolve Rust impl traits for one file's extraction results. */
static int resolve_impl_traits(cbm_pipeline_ctx_t *ctx, const cbm_semantic_bindings_t *bindings,
                               const CBMFileResult *result, const char *rel) {
    int count = 0;
    for (int t = 0; t < result->impl_traits.count; t++) {
        const char *trait_qn = NULL, *struct_qn = NULL;
        if (!cbm_semantic_rust_implementation(bindings, rel, &result->impl_traits.items[t],
                                              &struct_qn, &trait_qn))
            continue;
        const cbm_gbuf_node_t *tn = cbm_gbuf_find_by_qn(ctx->gbuf, trait_qn);
        const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_qn(ctx->gbuf, struct_qn);
        if (tn && sn && tn->id != sn->id) {
            cbm_gbuf_insert_edge(ctx->gbuf, sn->id, tn->id, "IMPLEMENTS", "{}");
            count++;
        }
    }
    return count;
}

int cbm_pipeline_pass_semantic(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    cbm_log_info("pass.start", "pass", "semantic", "files", itoa_log(file_count));

    int inherits_count = 0;
    int decorates_count = 0;
    int implements_count = 0;
    int errors = 0;
    cbm_semantic_bindings_t *bindings =
        cbm_semantic_bindings_build(ctx, files, file_count, ctx->result_cache);
    if (!bindings)
        return CBM_PIPELINE_ABORT_PRESERVE_DB;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            cbm_semantic_bindings_free(bindings);
            return CBM_NOT_FOUND;
        }

        const char *rel = files[i].rel_path;

        bool result_owned = false;
        CBMFileResult *result = sem_get_or_extract(ctx, i, &files[i], &result_owned);
        if (!result) {
            if (atomic_load(&ctx->test_declarations_failed)) {
                cbm_semantic_bindings_free(bindings);
                return CBM_PIPELINE_ABORT_PRESERVE_DB;
            }
            errors++;
            continue;
        }

        if (!cbm_pipeline_test_result_ok(ctx, result)) {
            if (result_owned) {
                cbm_free_result(result);
            }
            cbm_semantic_bindings_free(bindings);
            return CBM_PIPELINE_ABORT_PRESERVE_DB;
        }

        /* Build import map for resolution */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count, files[i].language);
        cbm_php_vendor_names_t php_vendor;
        cbm_pipeline_php_vendor_names_build(ctx->gbuf, ctx->project_name, rel, result, &php_vendor);

        char *module_qn = cbm_pipeline_fqn_module_dir(ctx->project_name, rel,
                                                      ps_module_is_dir(files[i].language));

        /* ── INHERITS + DECORATES from definitions ──────────────── */
        for (int d = 0; d < result->defs.count; d++) {
            sem_process_def_edges(ctx, bindings, &result->defs.items[d], &files[i], result,
                                  module_qn, imp_keys, imp_vals, imp_count, &inherits_count,
                                  &decorates_count, files[i].language, &php_vendor);
        }

        /* ── IMPLEMENTS from impl_traits (Rust) ─────────────────── */
        implements_count += resolve_impl_traits(ctx, bindings, result, files[i].rel_path);

        free(module_qn);
        cbm_pipeline_php_vendor_names_free(&php_vendor);
        free_import_map(imp_keys, imp_vals, imp_count, files[i].language);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    cbm_semantic_bindings_free(bindings);

    /* ── Go-style implicit interface satisfaction ──────────────── */
    int go_impl = cbm_pipeline_implements_go(ctx);
    implements_count += go_impl;

    /* ── Explicit-language override detection (full graph, serial) ── */
    int overrides = cbm_pipeline_override_explicit(ctx);

    cbm_log_info("pass.done", "pass", "semantic", "inherits", itoa_log(inherits_count), "decorates",
                 itoa_log(decorates_count), "implements", itoa_log(implements_count), "overrides",
                 itoa_log(overrides), "errors", itoa_log(errors));
    return 0;
}

const char *cbm_semantic_base_edge_type(const cbm_gbuf_node_t *base_node) {
    return (base_node && base_node->label && strcmp(base_node->label, "Interface") == 0)
               ? "IMPLEMENTS"
               : "INHERITS";
}

/* ── Explicit OVERRIDE: nearest declaring ancestor (#1278) ──────────
 *
 * A method overrides the same-named method of its NEAREST ancestor that
 * declares one, not only of its direct base: `Leaf(Intermediate(Processor))`
 * with an Intermediate that redeclares nothing still binds
 * Leaf.process -> Processor.process.
 *
 * The walk is level-synchronous over explicit IMPLEMENTS/INHERITS edges:
 * level 1 is every direct base, level k+1 every not-yet-visited base of
 * level k. A method is bound to every declaring ancestor on the FIRST level
 * that has one and is not looked up further, so the edge set does not depend
 * on edge or node order (a diamond's two equally near declarations both
 * bind; its shared root binds once, via insert-edge dedup). Direct bases are
 * level 1, so every edge the direct-base-only matcher made is still made.
 * A visited set makes every ancestor enter the walk at most once: cycles and
 * self-loops in a malformed hierarchy terminate, and the level count can
 * never exceed the node count (the check below is only that invariant). */

typedef struct {
    int64_t *ids;
    int count;
    int cap;
} ovr_id_list_t;

static bool ovr_ids_push(ovr_id_list_t *list, int64_t id) {
    if (list->count == list->cap) {
        int cap = list->cap ? list->cap * 2 : 16;
        int64_t *grown =
            cbm_realloc(CBM_MEM_CLASS_SEMANTIC, list->ids, (size_t)cap * sizeof(int64_t));
        if (!grown) {
            return false;
        }
        list->ids = grown;
        list->cap = cap;
    }
    list->ids[list->count++] = id;
    return true;
}

static int ovr_cmp_id(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Sort + unique in place. */
static void ovr_ids_normalize(ovr_id_list_t *list) {
    if (list->count < 2) {
        return;
    }
    qsort(list->ids, (size_t)list->count, sizeof(int64_t), ovr_cmp_id);
    int w = 1;
    for (int r = 1; r < list->count; r++) {
        if (list->ids[r] != list->ids[w - 1]) {
            list->ids[w++] = list->ids[r];
        }
    }
    list->count = w;
}

static bool ovr_ids_has(const ovr_id_list_t *sorted, int64_t id) {
    return sorted->count > 0 &&
           bsearch(&id, sorted->ids, (size_t)sorted->count, sizeof(int64_t), ovr_cmp_id) != NULL;
}

static const char *const ovr_base_edge_types[] = {"IMPLEMENTS", "INHERITS"};
enum { OVR_BASE_EDGE_TYPE_COUNT = 2 };

/* Append every explicit base of `node` to `out`. */
static bool ovr_push_bases(const cbm_gbuf_t *gb, int64_t node, ovr_id_list_t *out) {
    for (int t = 0; t < OVR_BASE_EDGE_TYPE_COUNT; t++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int n = 0;
        cbm_gbuf_find_edges_by_source_type(gb, node, ovr_base_edge_types[t], &edges, &n);
        for (int e = 0; e < n; e++) {
            if (!ovr_ids_push(out, edges[e]->target_id)) {
                return false;
            }
        }
    }
    return true;
}

/* Bind each still-unbound method of the class to the same-named methods of
 * one ancestor. Marks nothing: a level's bindings are committed by the caller
 * after the whole level ran, so equally near declarations all bind. */
static int ovr_match_ancestor(cbm_gbuf_t *gb, const cbm_gbuf_edge_t **cls_dm, int cls_dm_count,
                              const bool *bound, bool *bound_now, int64_t ancestor) {
    const cbm_gbuf_edge_t **base_dm = NULL;
    int base_dm_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, ancestor, "DEFINES_METHOD", &base_dm, &base_dm_count);
    int created = 0;
    for (int c = 0; c < cls_dm_count; c++) {
        if (bound[c]) {
            continue;
        }
        const cbm_gbuf_node_t *cm = cbm_gbuf_find_by_id(gb, cls_dm[c]->target_id);
        if (!cm || !cm->name) {
            continue;
        }
        for (int b = 0; b < base_dm_count; b++) {
            const cbm_gbuf_node_t *bm = cbm_gbuf_find_by_id(gb, base_dm[b]->target_id);
            if (bm && bm->name && cm->id != bm->id && strcmp(cm->name, bm->name) == 0) {
                cbm_gbuf_insert_edge(gb, cm->id, bm->id, "OVERRIDE", "{}");
                bound_now[c] = true;
                created++;
                break;
            }
        }
    }
    return created;
}

typedef struct {
    ovr_id_list_t visited; /* sorted */
    ovr_id_list_t level;
    ovr_id_list_t next;
    bool *bound;
    bool *bound_now;
    int flags_cap;
} ovr_walk_t;

static void ovr_walk_free(ovr_walk_t *w) {
    cbm_free(CBM_MEM_CLASS_SEMANTIC, w->visited.ids);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, w->level.ids);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, w->next.ids);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, w->bound);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, w->bound_now);
}

static bool ovr_walk_reserve_flags(ovr_walk_t *w, int n) {
    if (n <= w->flags_cap) {
        return true;
    }
    bool *b = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, w->bound, (size_t)n * sizeof(bool));
    if (!b) {
        return false;
    }
    w->bound = b;
    bool *bn = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, w->bound_now, (size_t)n * sizeof(bool));
    if (!bn) {
        return false;
    }
    w->bound_now = bn;
    w->flags_cap = n;
    return true;
}

/* Drop from `next` every id already visited, then add the rest to visited. */
static bool ovr_walk_advance(ovr_walk_t *w) {
    ovr_ids_normalize(&w->next);
    w->level.count = 0;
    for (int i = 0; i < w->next.count; i++) {
        int64_t id = w->next.ids[i];
        if (!ovr_ids_has(&w->visited, id) && !ovr_ids_push(&w->level, id)) {
            return false;
        }
    }
    for (int i = 0; i < w->level.count; i++) {
        if (!ovr_ids_push(&w->visited, w->level.ids[i])) {
            return false;
        }
    }
    ovr_ids_normalize(&w->visited);
    w->next.count = 0;
    return true;
}

/* All OVERRIDE edges of one class: walk its ancestors level by level until
 * every method is bound or the ancestors run out. */
static int ovr_class_overrides(cbm_gbuf_t *gb, int64_t cls_id, ovr_walk_t *w) {
    const cbm_gbuf_edge_t **cls_dm = NULL;
    int cls_dm_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, cls_id, "DEFINES_METHOD", &cls_dm, &cls_dm_count);
    if (cls_dm_count == 0 || !ovr_walk_reserve_flags(w, cls_dm_count)) {
        return 0;
    }
    memset(w->bound, 0, (size_t)cls_dm_count * sizeof(bool));
    memset(w->bound_now, 0, (size_t)cls_dm_count * sizeof(bool));
    w->visited.count = 0;
    w->next.count = 0;
    if (!ovr_ids_push(&w->visited, cls_id) || !ovr_push_bases(gb, cls_id, &w->next) ||
        !ovr_walk_advance(w)) {
        return 0;
    }
    int created = 0;
    int unbound = cls_dm_count;
    const int max_levels = cbm_gbuf_node_count(gb); /* invariant, never reached */
    for (int depth = 0; w->level.count > 0 && unbound > 0 && depth < max_levels; depth++) {
        for (int i = 0; i < w->level.count; i++) {
            created += ovr_match_ancestor(gb, cls_dm, cls_dm_count, w->bound, w->bound_now,
                                          w->level.ids[i]);
            if (!ovr_push_bases(gb, w->level.ids[i], &w->next)) {
                return created;
            }
        }
        for (int c = 0; c < cls_dm_count; c++) {
            if (w->bound_now[c] && !w->bound[c]) {
                w->bound[c] = true;
                unbound--;
            }
        }
        if (!ovr_walk_advance(w)) {
            return created;
        }
    }
    return created;
}

int cbm_pipeline_override_explicit(cbm_pipeline_ctx_t *ctx) {
    if (!ctx || !ctx->gbuf) {
        return 0;
    }
    /* Every class with at least one explicit base, once, in id order. */
    ovr_id_list_t classes = {0};
    for (int t = 0; t < OVR_BASE_EDGE_TYPE_COUNT; t++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        cbm_gbuf_find_edges_by_type(ctx->gbuf, ovr_base_edge_types[t], &edges, &edge_count);
        for (int e = 0; e < edge_count; e++) {
            if (!ovr_ids_push(&classes, edges[e]->source_id)) {
                cbm_free(CBM_MEM_CLASS_SEMANTIC, classes.ids);
                return 0;
            }
        }
    }
    ovr_ids_normalize(&classes);

    int created = 0;
    ovr_walk_t walk = {0};
    for (int i = 0; i < classes.count; i++) {
        const cbm_gbuf_node_t *cls = cbm_gbuf_find_by_id(ctx->gbuf, classes.ids[i]);
        /* Go's implicit satisfaction already emits OVERRIDE with interface
         * semantics; running both would double-cover .go sources. */
        if (!cls || (cls->file_path && fp_ends_with(cls->file_path, ".go"))) {
            continue;
        }
        created += ovr_class_overrides(ctx->gbuf, cls->id, &walk);
    }
    ovr_walk_free(&walk);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, classes.ids);
    return created;
}
