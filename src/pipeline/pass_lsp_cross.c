/*
 * pass_lsp_cross.c — Cross-file LSP type-aware call resolution pass.
 *
 * See pass_lsp_cross.h for the high-level contract. This file is the
 * pipeline glue that converts the existing per-file extraction state
 * (CBMDefinition / CBMImport / IMPORTS-edge gbuf state) into the input
 * shape each language LSP's cbm_run_X_lsp_cross expects, then merges
 * the resulting CBMResolvedCall entries back into per-file results.
 *
 * The pass is a no-op for any file whose CBMFileResult is missing or
 * whose language has no cross-file LSP entry registered. Per-LSP emit
 * functions dedup against entries already in resolved_calls, so this pass
 * is also idempotent — safe to invoke multiple times if the pipeline gains
 * a re-run path later.
 */
#include "pipeline/pass_lsp_cross.h"

#include "store/store.h"
#include "pipeline/lsp_surface.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/lsp_resolve.h"
#include "lsp/go_lsp.h"
#include "lsp/c_lsp.h"
#include "lsp/py_lsp.h"
#include "lsp/ts_lsp.h"
#include "lsp/php_lsp.h"
#include "lsp/java_lsp.h"
#include "lsp/kotlin_lsp.h"
#include "lsp/rust_lsp.h"
#include "lsp/rust_cargo.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"
#include "foundation/compat.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "yyjson/yyjson.h"
#include "foundation/compat_fs.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ─────────────────────────────────────────────────── */

enum {
    PXC_MAX_FILE_BYTES_FACTOR = 100, /* same cap pass_calls.c uses for source size */
    PXC_ITOA_BUF = 16,
};

/* Hash-table keys reject empty strings, but Java/Kotlin's default package is a
 * real shared namespace: sibling files can reference its declarations without
 * imports. Use an internal key that cannot be a legal JVM package name. */
static const char PXC_DEFAULT_JVM_NAMESPACE[] = "<jvm-default-package>";

/* Format an int into a thread-local rotating buffer for log key=value emission.
 * Mirrors the itoa_log helper in pass_calls.c — kept local so passes don't
 * grow a foundation-wide formatting API just for log output. */
static const char *itoa_buf(int val) {
    static _Thread_local char bufs[PXC_ITOA_BUF][PXC_ITOA_BUF];
    static _Thread_local int slot = 0;
    char *out = bufs[slot];
    slot = (slot + 1) & (PXC_ITOA_BUF - 1);
    snprintf(out, PXC_ITOA_BUF, "%d", val);
    return out;
}

/* ── Local helpers ─────────────────────────────────────────────── */

/* True for languages whose module QN is derived from the CONTAINING DIRECTORY
 * (Java package, Go package) rather than the filename stem. MUST match the
 * extraction-side cbm_lang_module_is_dir() in internal/cbm/helpers.c so the
 * cross-file LSP caller_qn agrees with the def-node QN (the lsp_resolve join
 * keys on exact equality). */
static bool pxc_module_is_dir(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA || lang == CBM_LANG_GO;
}

/* Slurp a file into a malloc'd, NUL-terminated buffer. Mirrors the
 * read_file helper in pass_calls.c / pass_parallel.c (kept local so the
 * pipeline doesn't grow a public read-file API just for this pass). */
static char *pxc_read_file(const char *path, int *out_len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f)
        return NULL;
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)PXC_MAX_FILE_BYTES_FACTOR * (long)CBM_SZ_1K * (long)CBM_SZ_1K) {
        (void)fclose(f);
        return NULL;
    }
    /* +pad: tree-sitter lexer lookahead reads past EOF; keep it in-bounds */
    enum { CBM_TS_LOOKAHEAD_PAD = 16 };
    char *buf = (char *)malloc((size_t)size + CBM_TS_LOOKAHEAD_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    if (nread > (size_t)size)
        nread = (size_t)size;
    memset(buf + nread, 0, CBM_TS_LOOKAHEAD_PAD);
    *out_len = (int)nread;
    return buf;
}

/* Map a CBMDefinition.label to a CBMLSPDef.label. Per-language LSP registrars
 * only care about type-like containers (Class/Struct/Interface/Trait/Enum/Type)
 * plus Protocol/Function/Method — variables, modules, decorators, etc. are
 * skipped. Struct passes through so Rust/Go struct type-registration via the
 * cross-file LSP path is not dropped. */
static const char *pxc_map_label(const char *label) {
    if (!label)
        return NULL;
    if (cbm_label_is_type_like(label) || strcmp(label, "Protocol") == 0 ||
        strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
        /* Properties reach the cross defs so the Kotlin registrar can attach
         * them as fields of their receiver type (kotlin_lookup_property_type
         * was blind across files, leaving property references to the
         * name-only fallback). Every registrar filters by explicit label, so
         * the other languages ignore Variable defs untouched. */
        strcmp(label, "Variable") == 0) {
        return label;
    }
    return NULL;
}

/* Build the embedded_types "|"-separated string from base_classes[].
 * Returns NULL when there are no bases. Allocated in the supplied arena. */
static const char *pxc_join_pipe(CBMArena *arena, const char *const *items) {
    if (!items || !items[0])
        return NULL;
    int count = 0;
    size_t total = 0;
    for (int i = 0; items[i]; i++) {
        count++;
        total += strlen(items[i]);
    }
    if (count == 0)
        return NULL;
    /* count - 1 separators + NUL. */
    size_t bufsz = total + (size_t)(count - 1) + 1;
    char *buf = (char *)cbm_arena_alloc(arena, bufsz);
    if (!buf)
        return NULL;
    char *p = buf;
    for (int i = 0; i < count; i++) {
        size_t n = strlen(items[i]);
        memcpy(p, items[i], n);
        p += n;
        if (i + 1 < count)
            *p++ = '|';
    }
    *p = '\0';
    return buf;
}

static bool pxc_is_jvm_lang(CBMLanguage lang);

static const char *pxc_last_component(const char *qn) {
    if (!qn) {
        return NULL;
    }
    const char *dot = strrchr(qn, '.');
    return dot ? dot + 1 : qn;
}

static const char *pxc_jvm_type_qn(CBMArena *arena, const char *namespace_name,
                                   const char *type_qn_or_name) {
    if (!arena || !namespace_name || !namespace_name[0] || !type_qn_or_name) {
        return type_qn_or_name;
    }
    const char *short_name = pxc_last_component(type_qn_or_name);
    if (!short_name || !short_name[0]) {
        return type_qn_or_name;
    }
    return cbm_arena_sprintf(arena, "%s.%s", namespace_name, short_name);
}

static const char *pxc_jvm_def_qn(CBMArena *arena, const CBMDefinition *src,
                                  const char *namespace_name, const char *label) {
    if (!arena || !src || !namespace_name || !namespace_name[0]) {
        return src ? src->qualified_name : NULL;
    }
    if (strcmp(label, "Method") == 0 || strcmp(label, "Function") == 0 ||
        strcmp(label, "Constructor") == 0) {
        if (src->parent_class && src->parent_class[0]) {
            return cbm_arena_sprintf(arena, "%s.%s.%s", namespace_name,
                                     pxc_last_component(src->parent_class), src->name);
        }
        return cbm_arena_sprintf(arena, "%s.%s", namespace_name, src->name);
    }
    return cbm_arena_sprintf(arena, "%s.%s", namespace_name, src->name);
}

static const char *pxc_infer_jvm_namespace(CBMArena *arena, const char *rel_path,
                                           CBMLanguage lang) {
    if (!arena || !rel_path || !pxc_is_jvm_lang(lang)) {
        return NULL;
    }
    const char *root = NULL;
    const char *lang_root = lang == CBM_LANG_KOTLIN ? "kotlin/" : "java/";
    if (strncmp(rel_path, "src/main/", 9) == 0 &&
        strncmp(rel_path + 9, lang_root, strlen(lang_root)) == 0) {
        root = rel_path + 9 + strlen(lang_root);
    } else if (strncmp(rel_path, "src/test/", 9) == 0 &&
               strncmp(rel_path + 9, lang_root, strlen(lang_root)) == 0) {
        root = rel_path + 9 + strlen(lang_root);
    } else {
        const char *needle = lang == CBM_LANG_KOTLIN ? "/kotlin/" : "/java/";
        root = strstr(rel_path, needle);
        if (root) {
            root += strlen(needle);
        } else if (strncmp(rel_path, "src/", 4) == 0) {
            root = rel_path + 4;
        } else {
            root = strstr(rel_path, "/src/");
            if (root) {
                root += strlen("/src/");
            }
        }
    }
    if (!root || !root[0]) {
        return NULL;
    }
    if (strncmp(root, "main/", 5) == 0 || strncmp(root, "test/", 5) == 0) {
        root += 5;
    }
    if (strncmp(root, "java/", 5) == 0) {
        root += 5;
    } else if (strncmp(root, "kotlin/", 7) == 0) {
        root += 7;
    }
    const char *slash = strrchr(root, '/');
    if (!slash || slash <= root) {
        return NULL;
    }
    size_t len = (size_t)(slash - root);
    char *ns = (char *)cbm_arena_alloc(arena, len + 1);
    if (!ns) {
        return NULL;
    }
    memcpy(ns, root, len);
    ns[len] = '\0';
    for (size_t i = 0; i < len; i++) {
        if (ns[i] == '/') {
            ns[i] = '.';
        }
    }
    return ns;
}

static const char *pxc_qn_leaf(const char *name) {
    if (!name) {
        return NULL;
    }
    const char *leaf = name;
    for (const char *p = name; *p; p++) {
        if (*p == '.' || *p == ':' || *p == '/' || *p == '\\') {
            leaf = p + 1;
        }
    }
    return leaf;
}

/* Convert one CBMDefinition into a CBMLSPDef. Returns 0 on success, -1
 * to skip (unsupported label or missing required field). dst gets borrowed
 * pointers into src and into `arena` for synthesised composites. */
static int pxc_build_lsp_def(CBMArena *arena, const CBMDefinition *src, const char *module_qn,
                             const char *namespace_name, CBMLanguage lang, CBMLSPDef *dst) {
    const char *label = pxc_map_label(src->label);
    if (!label || !src->qualified_name || !src->name)
        return -1;
    memset(dst, 0, sizeof(*dst));
    if (pxc_is_jvm_lang(lang) && namespace_name && namespace_name[0]) {
        dst->qualified_name = pxc_jvm_def_qn(arena, src, namespace_name, label);
        dst->receiver_type = pxc_jvm_type_qn(arena, namespace_name, src->parent_class);
    } else {
        dst->qualified_name = src->qualified_name;
        dst->receiver_type = src->parent_class;
    }
    dst->short_name = src->name;
    dst->label = label;
    dst->def_module_qn = module_qn;
    dst->namespace_name = namespace_name;
    dst->is_interface = (strcmp(label, "Interface") == 0 || strcmp(label, "Protocol") == 0);
    /* Single return-type string. The per-language registrars split on '|'
     * for multi-return languages (Go); single-return languages just see one
     * piece, which is what's already stored. */
    dst->return_types = src->return_type;
    dst->embedded_types = pxc_join_pipe(arena, src->base_classes);
    dst->signature_param_types = src->signature_param_types;
    dst->signature_param_count = src->signature_param_count;
    dst->lang = lang;
    dst->decorators = src->decorators;
    if (lang == CBM_LANG_RUST) {
        /* Exact impl-block provenance is captured while the Rust impl node is
         * still on hand.  Do not reconstruct it later from leaf names. */
        dst->trait_qn = src->impl_trait;
        dst->is_abstract = src->is_abstract;
    }
    return 0;
}

/* Carry one Rust type-level impl independently of any method definition.
 * `impl Trait for Type {}` is semantically meaningful even when the block is
 * empty (the trait may provide defaults), so attaching the relation only to
 * concrete method records is lossy. */
static int pxc_build_rust_impl_relation(CBMArena *arena, const CBMImplTrait *impl,
                                        const char *project_name, const char *rel_path,
                                        const char *module_qn, CBMLSPDef *dst) {
    if (!arena || !impl || !impl->trait_name || !impl->struct_name || !impl->struct_qn) {
        return -1;
    }
    const char *receiver_qn = impl->struct_qn;
    memset(dst, 0, sizeof(*dst));
    dst->qualified_name = receiver_qn;
    dst->short_name = pxc_qn_leaf(receiver_qn);
    dst->label = "RustImpl";
    dst->receiver_type = receiver_qn;
    dst->def_module_qn = module_qn;
    dst->trait_qn = impl->trait_name; /* raw; canonicalized by Rust registry */
    dst->lang = CBM_LANG_RUST;
    dst->is_rust_impl_relation = true;
    return 0;
}

static const char *pxc_json_string_dup(CBMArena *arena, yyjson_val *root, const char *key) {
    if (!arena || !root || !key) {
        return NULL;
    }
    yyjson_val *val = yyjson_obj_get(root, key);
    if (!val || !yyjson_is_str(val)) {
        return NULL;
    }
    const char *str = yyjson_get_str(val);
    return str && str[0] ? cbm_arena_strdup(arena, str) : NULL;
}

static const char **pxc_json_string_array_dup(CBMArena *arena, yyjson_val *root, const char *key) {
    if (!arena || !root || !key) {
        return NULL;
    }
    yyjson_val *arr = yyjson_obj_get(root, key);
    if (!arr || !yyjson_is_arr(arr)) {
        return NULL;
    }
    size_t len = yyjson_arr_size(arr);
    if (len == 0 || len > INT32_MAX) {
        return NULL;
    }
    const char **out = (const char **)cbm_arena_alloc(arena, (len + 1) * sizeof(*out));
    if (!out) {
        return NULL;
    }
    size_t idx = 0;
    size_t max = 0;
    size_t write = 0;
    yyjson_val *val = NULL;
    yyjson_arr_foreach(arr, idx, max, val) {
        if (!yyjson_is_str(val)) {
            continue;
        }
        const char *str = yyjson_get_str(val);
        if (str && str[0]) {
            out[write++] = cbm_arena_strdup(arena, str);
        }
    }
    out[write] = NULL;
    return out;
}

int cbm_pxc_build_lsp_def_from_node(CBMArena *arena, const cbm_node_t *node, CBMLanguage lang,
                                    CBMLSPDef *out) {
    if (!arena || !node || !out || !node->project || !node->qualified_name || !node->name ||
        !node->label || !node->file_path) {
        return -1;
    }

    char *module_heap =
        cbm_pipeline_fqn_module_dir(node->project, node->file_path, pxc_module_is_dir(lang));
    if (!module_heap) {
        return -1;
    }
    const char *module_qn = cbm_arena_strdup(arena, module_heap);
    free(module_heap);
    if (!module_qn) {
        return -1;
    }

    yyjson_doc *doc = NULL;
    yyjson_val *root = NULL;
    const char *props = node->properties_json ? node->properties_json : "{}";
    doc = yyjson_read(props, strlen(props), 0);
    if (doc) {
        root = yyjson_doc_get_root(doc);
        if (!root || !yyjson_is_obj(root)) {
            root = NULL;
        }
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = cbm_arena_strdup(arena, node->name);
    def.qualified_name = cbm_arena_strdup(arena, node->qualified_name);
    def.label = cbm_arena_strdup(arena, node->label);
    def.file_path = cbm_arena_strdup(arena, node->file_path);
    if (root) {
        def.return_type = pxc_json_string_dup(arena, root, "return_type");
        def.parent_class = pxc_json_string_dup(arena, root, "parent_class");
        def.base_classes = pxc_json_string_array_dup(arena, root, "base_classes");
    }
    if (doc) {
        yyjson_doc_free(doc);
    }

    if (!def.name || !def.qualified_name || !def.label || !def.file_path) {
        return -1;
    }
    const char *namespace_name = pxc_infer_jvm_namespace(arena, node->file_path, lang);
    return pxc_build_lsp_def(arena, &def, module_qn, namespace_name, lang, out);
}

/* Collect a project-wide CBMLSPDef[] from all cached results. Returns a
 * malloc'd array (caller frees) of length *out_count. String fields are
 * borrowed from cache[i]->arena and from def_modules[i] (also borrowed). */
CBMLSPDef *cbm_pxc_collect_all_defs(CBMFileResult **cache, const cbm_file_info_t *files,
                                    int file_count, const char *project_name, char **def_modules,
                                    int *out_count, int *out_def_starts) {
    int total = 0;
    for (int i = 0; i < file_count; i++) {
        if (cache[i]) {
            total += cache[i]->defs.count;
            if (files[i].language == CBM_LANG_RUST) {
                total += cache[i]->impl_traits.count;
            }
        }
    }
    if (total == 0) {
        *out_count = 0;
        if (out_def_starts) {
            memset(out_def_starts, 0, (size_t)(file_count + 1) * sizeof(int));
        }
        return NULL;
    }
    CBMLSPDef *defs = (CBMLSPDef *)calloc((size_t)total, sizeof(CBMLSPDef));
    if (!defs) {
        *out_count = 0;
        if (out_def_starts) {
            memset(out_def_starts, 0, (size_t)(file_count + 1) * sizeof(int));
        }
        return NULL;
    }
    int idx = 0;
    for (int fi = 0; fi < file_count; fi++) {
        if (out_def_starts) {
            out_def_starts[fi] = idx;
        }
        if (!cache[fi])
            continue;
        if (!def_modules[fi]) {
            def_modules[fi] = cbm_pipeline_fqn_module_dir(project_name, files[fi].rel_path,
                                                          pxc_module_is_dir(files[fi].language));
        }
        const char *namespace_name = cache[fi]->namespace_name;
        if ((!namespace_name || !namespace_name[0]) && files[fi].rel_path) {
            namespace_name =
                pxc_infer_jvm_namespace(&cache[fi]->arena, files[fi].rel_path, files[fi].language);
            if (namespace_name && namespace_name[0]) {
                cache[fi]->namespace_name = namespace_name;
            }
        }
        for (int di = 0; di < cache[fi]->defs.count; di++) {
            if (pxc_build_lsp_def(&cache[fi]->arena, &cache[fi]->defs.items[di], def_modules[fi],
                                  namespace_name, files[fi].language, &defs[idx]) == 0) {
                idx++;
            }
        }
        if (files[fi].language == CBM_LANG_RUST) {
            for (int ii = 0; ii < cache[fi]->impl_traits.count; ii++) {
                if (pxc_build_rust_impl_relation(
                        &cache[fi]->arena, &cache[fi]->impl_traits.items[ii], project_name,
                        files[fi].rel_path, def_modules[fi], &defs[idx]) == 0) {
                    idx++;
                }
            }
        }
    }
    if (out_def_starts) {
        out_def_starts[file_count] = idx;
    }
    *out_count = idx;
    return defs;
}

/* Return the one source import path bound to `local_name`, or NULL when the
 * extraction metadata is absent or two distinct imports bind the same local.
 * The latter is deliberately fail-closed: choosing either path would turn an
 * ambiguous Python value into a fabricated CALL_REFERENCE. */
static const char *pxc_unique_import_path(const CBMFileResult *result, const char *local_name,
                                          bool *out_ambiguous) {
    if (out_ambiguous)
        *out_ambiguous = false;
    if (!result || !local_name)
        return NULL;
    const char *path = NULL;
    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (!imp->local_name || !imp->module_path || strcmp(imp->local_name, local_name) != 0) {
            continue;
        }
        if (path && strcmp(path, imp->module_path) != 0) {
            if (out_ambiguous)
                *out_ambiguous = true;
            return NULL;
        }
        path = imp->module_path;
    }
    return path;
}

static const char *pxc_import_leaf(const char *path) {
    if (!path)
        return NULL;
    const char *leaf = path;
    for (const char *p = path; *p; p++) {
        if (*p == '.' || *p == '/' || *p == '\\' || *p == ':')
            leaf = p + 1;
    }
    return leaf;
}

static char *pxc_kotlin_import_from_metadata(const CBMFileResult *result, const char *local_name) {
    bool ambiguous = false;
    const char *path = pxc_unique_import_path(result, local_name, &ambiguous);
    const char *leaf = pxc_import_leaf(path);
    if (ambiguous || !path || !path[0] || path[0] == '.' || !leaf ||
        strcmp(leaf, local_name) != 0) {
        return NULL;
    }
    /* Keep the source package/member spelling. Kotlin cross registries are
     * package-qualified (target.actual), not path/project-qualified. The
     * resolver still requires a registered function/type before emitting an
     * exact semantic relationship. */
    return cbm_strdup(path);
}

/* IMPORTS edges correctly target dependency modules, but Python from-imports
 * also carry a source-level member: `from target import handler` is extracted
 * as module_path=`target.handler` while its edge targets Module `P.target`.
 * Reattach that member when the raw metadata proves one unique path. This also
 * preserves `from target import handler as target`: the source member is the
 * path leaf, while `local_name` is only its local binding. The shared Python
 * registry must still materialize the resulting exact QN before it earns
 * CALL_REFERENCE. */
static char *pxc_import_value_qn(CBMLanguage lang, const CBMFileResult *result,
                                 const char *local_name, const cbm_gbuf_node_t *target) {
    if (!target || !target->qualified_name)
        return NULL;
    if (lang == CBM_LANG_KOTLIN)
        return pxc_kotlin_import_from_metadata(result, local_name);
    if (lang != CBM_LANG_PYTHON)
        return cbm_strdup(target->qualified_name);

    bool ambiguous = false;
    const char *path = pxc_unique_import_path(result, local_name, &ambiguous);
    if (ambiguous)
        return NULL;
    const char *source_leaf = pxc_import_leaf(path);
    const char *target_leaf = pxc_import_leaf(target->qualified_name);
    if (!path || !source_leaf || !target_leaf || strcmp(source_leaf, target_leaf) == 0 ||
        !target->label ||
        (strcmp(target->label, "Module") != 0 && strcmp(target->label, "Folder") != 0)) {
        return cbm_strdup(target->qualified_name);
    }

    size_t need = strlen(target->qualified_name) + 1 + strlen(source_leaf) + 1;
    char *qualified = (char *)malloc(need);
    if (!qualified)
        return NULL;
    snprintf(qualified, need, "%s.%s", target->qualified_name, source_leaf);
    return qualified;
}

static bool pxc_import_map_has_local(const char *const *keys, int count, const char *local_name) {
    if (!keys || !local_name)
        return false;
    for (int i = 0; i < count; i++) {
        if (keys[i] && strcmp(keys[i], local_name) == 0)
            return true;
    }
    return false;
}

/* Recover a Python import directly from extraction metadata when graph import
 * materialization produced no edge. Prefer an already materialized exact node
 * (including the shared project-prefix rule); otherwise preserve the unique
 * source path and let the sealed Python registry decide whether that QN denotes
 * a class/function. A wrong candidate cannot earn a semantic edge because the
 * resolver requires registry materialization. */
static char *pxc_python_import_from_metadata(const cbm_gbuf_t *gbuf, const char *project_name,
                                             const char *local_name, const char *module_path) {
    if (!gbuf || !local_name || !module_path)
        return NULL;
    const char *source_leaf = pxc_import_leaf(module_path);
    if (!source_leaf || module_path[0] == '.')
        return NULL;

    const cbm_gbuf_node_t *exact =
        cbm_pipeline_lsp_target_node(gbuf, project_name, module_path, false);
    if (exact && exact->qualified_name)
        return cbm_strdup(exact->qualified_name);

    const char *last_dot = strrchr(module_path, '.');
    if (last_dot && last_dot > module_path) {
        size_t module_len = (size_t)(last_dot - module_path);
        char *module = (char *)malloc(module_len + 1);
        if (!module)
            return NULL;
        memcpy(module, module_path, module_len);
        module[module_len] = '\0';
        const cbm_gbuf_node_t *module_node =
            cbm_pipeline_lsp_target_node(gbuf, project_name, module, false);
        free(module);
        if (module_node && module_node->qualified_name && module_node->label &&
            (strcmp(module_node->label, "Module") == 0 ||
             strcmp(module_node->label, "Folder") == 0)) {
            size_t need = strlen(module_node->qualified_name) + 1 + strlen(source_leaf) + 1;
            char *qualified = (char *)malloc(need);
            if (!qualified)
                return NULL;
            snprintf(qualified, need, "%s.%s", module_node->qualified_name, source_leaf);
            return qualified;
        }
    }

    if (!project_name || !project_name[0])
        return cbm_strdup(module_path);
    size_t need = strlen(project_name) + 1 + strlen(module_path) + 1;
    char *qualified = (char *)malloc(need);
    if (!qualified)
        return NULL;
    snprintf(qualified, need, "%s.%s", project_name, module_path);
    return qualified;
}

/* Build per-file import map (local_name -> semantic import QN) from gbuf
 * IMPORTS edges plus authoritative Python/Kotlin extraction metadata. */
int cbm_pxc_build_import_map(const cbm_gbuf_t *gbuf, const char *project_name, const char *rel_path,
                             CBMLanguage lang, const CBMFileResult *result, const char ***out_keys,
                             const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    const char **edge_keys = NULL;
    const char **edge_vals = NULL;
    int edge_count = 0;
    if (cbm_pipeline_build_import_map_from_edges(gbuf, project_name, rel_path, &edge_keys,
                                                 &edge_vals, &edge_count) != 0) {
        return CBM_NOT_FOUND;
    }

    int metadata_count =
        (lang == CBM_LANG_PYTHON || lang == CBM_LANG_KOTLIN) && result ? result->imports.count : 0;
    if (edge_count == 0 && metadata_count == 0)
        return 0;

    size_t capacity = (size_t)edge_count + (size_t)metadata_count;
    const char **keys = (const char **)calloc(capacity, sizeof(const char *));
    const char **vals = (const char **)calloc(capacity, sizeof(const char *));
    if (!keys || !vals) {
        cbm_pipeline_free_import_map(edge_keys, edge_vals, edge_count);
        free(keys);
        free(vals);
        return 0;
    }
    int count = 0;
    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(gbuf, edge_vals[i]);
        char *local = edge_keys[i] ? cbm_strdup(edge_keys[i]) : NULL;
        if (!local || !target) {
            free(local);
            continue;
        }
        char *value = pxc_import_value_qn(lang, result, local, target);
        if (!value) {
            free(local);
            continue;
        }
        keys[count] = local;
        vals[count] = value;
        count++;
    }
    cbm_pipeline_free_import_map(edge_keys, edge_vals, edge_count);

    /* IMPORTS edges are best-effort graph relationships. The cross-LSP still
     * has the authoritative extraction metadata in both sequential and
     * parallel caches, so a missing edge must not erase an otherwise exact
     * Python import. Add only locals not already represented by an edge. */
    for (int i = 0; i < metadata_count; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (!imp->local_name || !imp->local_name[0] || !imp->module_path ||
            pxc_import_map_has_local(keys, count, imp->local_name)) {
            continue;
        }
        bool ambiguous = false;
        const char *path = pxc_unique_import_path(result, imp->local_name, &ambiguous);
        if (ambiguous || !path)
            continue;
        char *value =
            lang == CBM_LANG_KOTLIN
                ? pxc_kotlin_import_from_metadata(result, imp->local_name)
                : pxc_python_import_from_metadata(gbuf, project_name, imp->local_name, path);
        if (!value)
            continue;
        char *local = cbm_strdup(imp->local_name);
        if (!local) {
            free(value);
            continue;
        }
        keys[count] = local;
        vals[count] = value;
        count++;
    }
    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;
}

void cbm_pxc_free_import_map(const char **keys, const char **vals, int count) {
    if (keys) {
        for (int i = 0; i < count; i++)
            free((void *)keys[i]);
        free((void *)keys);
    }
    if (vals) {
        for (int i = 0; i < count; i++)
            free((void *)vals[i]);
        free((void *)vals);
    }
}

/* Detect TS dialect flags from a relative path. */
void cbm_pxc_ts_modes(CBMLanguage lang, const char *rel_path, bool *out_js, bool *out_jsx,
                      bool *out_dts) {
    *out_js = (lang == CBM_LANG_JAVASCRIPT);
    *out_jsx = (lang == CBM_LANG_TSX);
    *out_dts = false;
    if (!rel_path)
        return;
    size_t rl = strlen(rel_path);
    if (lang == CBM_LANG_JAVASCRIPT && rl >= 4 && strcmp(rel_path + rl - 4, ".jsx") == 0) {
        *out_jsx = true;
    }
    if (lang == CBM_LANG_TYPESCRIPT && rl >= 5 && strcmp(rel_path + rl - 5, ".d.ts") == 0) {
        *out_dts = true;
    }
}

/* Returns true when this language has a cross-file LSP wired up. */
bool cbm_pxc_has_cross_lsp(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_GO:
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_PYTHON:
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_PHP:
    case CBM_LANG_CSHARP: /* tier-2 prebuilt registry path (pass_parallel.c) */
    case CBM_LANG_JAVA:   /* fallback cbm_pxc_run_one path */
    case CBM_LANG_KOTLIN: /* fallback cbm_pxc_run_one path */
    case CBM_LANG_RUST:   /* fallback cbm_pxc_run_one path (manifest-aware) */
        return true;
    default:
        return false;
    }
}

/* Append cross-file results from `src_out` (allocated in a scratch arena
 * about to be destroyed) into `dst_calls` (lives in cache_entry->arena),
 * copying every string field into dst_arena. Skips entries whose
 * (kind, caller_qn, callee_qn, source span) is already present — avoids
 * inflating the array with cross-file duplicates without conflating distinct
 * same-named occurrences. For an exact duplicate, retain the higher-confidence
 * record.
 *
 * Dedup uses a hash table keyed on
 * "kind\x1fcaller\x1fcallee\x1fstart:end\x1forigin", giving O(1)
 * membership.
 * The previous linear strcmp scan made each append O(n), so a file that
 * resolved very many cross-calls turned the whole append into O(n^2) and could
 * peg a core for minutes (observed: an index hung in pxc_append_results/strcmp).
 * The key strings live in a scratch arena that is destroyed after the table. */
static void pxc_append_results(CBMArena *dst_arena, CBMResolvedCallArray *dst_calls,
                               const CBMResolvedCallArray *src_out) {
    if (!dst_calls || !src_out)
        return;

    CBMArena keys;
    cbm_arena_init(&keys);
    CBMHashTable *seen = cbm_ht_create((uint32_t)(dst_calls->count + src_out->count + 1));
    CBMHashTable *by_call = cbm_ht_create((uint32_t)(dst_calls->count + src_out->count + 1));

    if (!seen || !by_call) {
        cbm_ht_free(seen);
        cbm_ht_free(by_call);
        cbm_arena_destroy(&keys);
        return;
    }

    for (int i = 0; i < dst_calls->count; i++) {
        CBMResolvedCall *rc = &dst_calls->items[i];
        if (rc->caller_qn && rc->callee_qn) {
            char *k = cbm_arena_sprintf(&keys, "%u\x1f%s\x1f%s\x1f%u:%u\x1f%u", (unsigned)rc->kind,
                                        rc->caller_qn, rc->callee_qn, rc->site_start_byte,
                                        rc->site_end_byte, (unsigned)rc->source_origin);
            if (k) {
                void *prior = cbm_ht_get(seen, k);
                if (!prior ||
                    rc->confidence > dst_calls->items[(int)(uintptr_t)prior - 1].confidence) {
                    const char *stored = cbm_ht_get_key(seen, k);
                    cbm_ht_set(seen, stored ? stored : k, (void *)(uintptr_t)(i + 1));
                }
            }
            char *call_key = cbm_arena_sprintf(
                &keys, "%u\x1f%s\x1f%s\x1f%u:%u\x1f%u", (unsigned)rc->kind, rc->caller_qn,
                pxc_last_component(rc->callee_qn), rc->site_start_byte, rc->site_end_byte,
                (unsigned)rc->source_origin);
            if (call_key && !cbm_ht_has(by_call, call_key)) {
                cbm_ht_set(by_call, call_key, (void *)(uintptr_t)(i + 1));
            }
        }
    }

    for (int j = 0; j < src_out->count; j++) {
        const CBMResolvedCall *src = &src_out->items[j];
        if (!src->caller_qn || !src->callee_qn)
            continue;
        char *k = cbm_arena_sprintf(&keys, "%u\x1f%s\x1f%s\x1f%u:%u\x1f%u", (unsigned)src->kind,
                                    src->caller_qn, src->callee_qn, src->site_start_byte,
                                    src->site_end_byte, (unsigned)src->source_origin);
        void *prior = k ? cbm_ht_get(seen, k) : NULL;
        if (prior) {
            CBMResolvedCall *dst = &dst_calls->items[(int)(uintptr_t)prior - 1];
            if (src->confidence > dst->confidence) {
                dst->caller_qn = cbm_arena_strdup(dst_arena, src->caller_qn);
                dst->callee_qn = cbm_arena_strdup(dst_arena, src->callee_qn);
                dst->strategy = src->strategy ? cbm_arena_strdup(dst_arena, src->strategy) : NULL;
                dst->confidence = src->confidence;
                dst->reason = src->reason ? cbm_arena_strdup(dst_arena, src->reason) : NULL;
                dst->kind = src->kind;
                dst->site_start_byte = src->site_start_byte;
                dst->site_end_byte = src->site_end_byte;
                dst->source_origin = src->source_origin;
            }
            continue;
        }

        char *call_key = cbm_arena_sprintf(&keys, "%u\x1f%s\x1f%s\x1f%u:%u\x1f%u",
                                           (unsigned)src->kind, src->caller_qn,
                                           pxc_last_component(src->callee_qn), src->site_start_byte,
                                           src->site_end_byte, (unsigned)src->source_origin);
        uintptr_t encoded_index = call_key ? (uintptr_t)cbm_ht_get(by_call, call_key) : 0;
        if (encoded_index > 0) {
            CBMResolvedCall *existing = &dst_calls->items[encoded_index - 1];
            /* A project-wide cross result may replace the per-file guess for
             * this exact occurrence, but never another same-name callsite. */
            if (src->confidence >= existing->confidence &&
                strcmp(src->callee_qn, existing->callee_qn) != 0) {
                const char *callee_qn = cbm_arena_strdup(dst_arena, src->callee_qn);
                if (callee_qn) {
                    existing->callee_qn = callee_qn;
                    existing->strategy =
                        src->strategy ? cbm_arena_strdup(dst_arena, src->strategy) : NULL;
                    existing->confidence = src->confidence;
                    existing->reason =
                        src->reason ? cbm_arena_strdup(dst_arena, src->reason) : NULL;
                }
            }
            continue;
        }

        CBMResolvedCall dst = {0};
        dst.caller_qn = cbm_arena_strdup(dst_arena, src->caller_qn);
        dst.callee_qn = cbm_arena_strdup(dst_arena, src->callee_qn);
        if (!dst.caller_qn || !dst.callee_qn) {
            continue;
        }
        dst.strategy = src->strategy ? cbm_arena_strdup(dst_arena, src->strategy) : NULL;
        dst.confidence = src->confidence;
        dst.reason = src->reason ? cbm_arena_strdup(dst_arena, src->reason) : NULL;
        dst.kind = src->kind;
        dst.site_start_byte = src->site_start_byte;
        dst.site_end_byte = src->site_end_byte;
        dst.source_origin = src->source_origin;
        int prior_count = dst_calls->count;
        cbm_resolvedcall_push(dst_calls, dst_arena, dst);
        if (dst_calls->count == prior_count + 1 && k) {
            cbm_ht_set(seen, k, (void *)(uintptr_t)dst_calls->count);
        }
        if (dst_calls->count == prior_count + 1 && call_key) {
            cbm_ht_set(by_call, call_key, (void *)(uintptr_t)dst_calls->count);
        }
    }

    cbm_ht_free(seen);
    cbm_ht_free(by_call);
    cbm_arena_destroy(&keys);
}

/* Merge exact synthetic call carriers produced by a cross-LSP resolver. The
 * source may live in the per-file result arena or in cbm_pxc_run_one's scratch
 * arena, so every pointer-bearing field is copied explicitly. Invalid/legacy
 * zero-span carriers are rejected: a requires-LSP carrier is safe only when it
 * can join the semantic record for the same source occurrence. */
static void pxc_append_synthetic_calls(CBMArena *dst_arena, CBMCallArray *dst_calls,
                                       const CBMCallArray *src_calls) {
    if (!dst_arena || !dst_calls || !src_calls || src_calls->count <= 0)
        return;

    CBMArena keys;
    cbm_arena_init(&keys);
    CBMHashTable *seen = cbm_ht_create((uint32_t)(dst_calls->count + src_calls->count + 1));
    if (!seen) {
        cbm_arena_destroy(&keys);
        return;
    }

    for (int i = 0; i < dst_calls->count; i++) {
        const CBMCall *call = &dst_calls->items[i];
        if (!call->callee_name || !call->enclosing_func_qn ||
            call->site_end_byte <= call->site_start_byte) {
            continue;
        }
        char *key = cbm_arena_sprintf(
            &keys, "%s\x1f%s\x1f%u:%u\x1f%u\x1f%d:%d", call->enclosing_func_qn, call->callee_name,
            call->site_start_byte, call->site_end_byte, (unsigned)call->source_origin,
            call->requires_lsp_resolution ? 1 : 0, call->is_method ? 1 : 0);
        if (key && !cbm_ht_get(seen, key))
            cbm_ht_set(seen, key, (void *)(uintptr_t)(i + 1));
    }

    for (int i = 0; i < src_calls->count; i++) {
        const CBMCall *src = &src_calls->items[i];
        if (!src->requires_lsp_resolution || !src->callee_name || !src->enclosing_func_qn ||
            src->site_end_byte <= src->site_start_byte) {
            continue;
        }
        char *key = cbm_arena_sprintf(
            &keys, "%s\x1f%s\x1f%u:%u\x1f%u\x1f%d:%d", src->enclosing_func_qn, src->callee_name,
            src->site_start_byte, src->site_end_byte, (unsigned)src->source_origin,
            src->requires_lsp_resolution ? 1 : 0, src->is_method ? 1 : 0);
        if (key && cbm_ht_get(seen, key))
            continue;

        CBMCall dst = *src;
        dst.callee_name = cbm_arena_strdup(dst_arena, src->callee_name);
        dst.enclosing_func_qn = cbm_arena_strdup(dst_arena, src->enclosing_func_qn);
        if (!dst.callee_name || !dst.enclosing_func_qn) {
            continue;
        }
        dst.first_string_arg =
            src->first_string_arg ? cbm_arena_strdup(dst_arena, src->first_string_arg) : NULL;
        dst.second_arg_name =
            src->second_arg_name ? cbm_arena_strdup(dst_arena, src->second_arg_name) : NULL;
        for (int ai = 0; ai < CBM_MAX_CALL_ARGS; ai++) {
            dst.args[ai].expr =
                src->args[ai].expr ? cbm_arena_strdup(dst_arena, src->args[ai].expr) : NULL;
            dst.args[ai].value =
                src->args[ai].value ? cbm_arena_strdup(dst_arena, src->args[ai].value) : NULL;
            dst.args[ai].keyword =
                src->args[ai].keyword ? cbm_arena_strdup(dst_arena, src->args[ai].keyword) : NULL;
        }
        int prior_count = dst_calls->count;
        cbm_calls_push(dst_calls, dst_arena, dst);
        if (key && dst_calls->count == prior_count + 1)
            cbm_ht_set(seen, key, (void *)(uintptr_t)dst_calls->count);
    }

    cbm_ht_free(seen);
    cbm_arena_destroy(&keys);
}

/* ── Rust workspace manifest (Cargo.toml) for cross-CRATE resolution ──
 *
 * cbm_pxc_run_one's signature is shared with the parallel pass
 * (pass_parallel.c) and cannot grow a manifest parameter without touching
 * that file. We therefore pass the parsed workspace manifest to the Rust
 * cross-file resolver through a file-static borrowed pointer that the
 * sequential driver (cbm_pipeline_pass_lsp_cross, below) sets up once per
 * pass run from the project's root Cargo.toml. The manifest's strings are
 * owned by `g_pxc_rust_manifest_arena`; the pointer is borrowed (NULL when
 * the project has no Cargo.toml — single-crate / non-workspace projects,
 * where in-file resolution needs no workspace metadata). */
static _Thread_local const CBMCargoManifest *g_pxc_rust_manifest = NULL;

void cbm_pxc_set_rust_manifest(const CBMCargoManifest *m) {
    g_pxc_rust_manifest = m;
}

const struct CBMCargoManifest *cbm_pxc_get_rust_manifest(void) {
    return g_pxc_rust_manifest;
}

/* Convert a CBMLSPDef array (the pipeline's lingua franca, go_lsp.h:73)
 * into a CBMRustLSPDef array (rust_lsp.h) inside `arena`. The two structs
 * have similar fields but different layouts; CBMLSPDef adds
 * language/namespace metadata, so a memcpy is unsafe — copy field-by-field. */
static CBMRustLSPDef *pxc_lspdefs_to_rust(CBMArena *arena, const CBMLSPDef *defs, int def_count) {
    if (!defs || def_count <= 0)
        return NULL;
    CBMRustLSPDef *out =
        (CBMRustLSPDef *)cbm_arena_alloc(arena, (size_t)def_count * sizeof(CBMRustLSPDef));
    if (!out)
        return NULL;
    for (int i = 0; i < def_count; i++) {
        out[i].qualified_name = defs[i].qualified_name;
        out[i].short_name = defs[i].short_name;
        out[i].label = defs[i].label;
        out[i].receiver_type = defs[i].receiver_type;
        out[i].def_module_qn = defs[i].def_module_qn;
        out[i].return_types = defs[i].return_types;
        out[i].embedded_types = defs[i].embedded_types;
        out[i].field_defs = defs[i].field_defs;
        out[i].method_names_str = defs[i].method_names_str;
        out[i].signature_param_types = defs[i].signature_param_types;
        out[i].signature_param_count = defs[i].signature_param_count;
        out[i].trait_qn = defs[i].trait_qn;
        out[i].is_interface = defs[i].is_interface;
        out[i].is_rust_impl_relation = defs[i].is_rust_impl_relation;
        out[i].is_abstract = defs[i].is_abstract;
    }
    return out;
}

/* Run cross-file LSP for a single file inside a scratch arena that gets
 * freed when the call returns. The LSP would otherwise allocate a fresh
 * type registry + stdlib + all project defs into the supplied arena, and
 * that adds up to O(N×project_size) memory if we used cache[i]->arena
 * directly across N files (test_incremental.c saw 3.5 GB peak on a
 * 1100-file repo before this fix). Output gets copied into the file's own
 * arena and merged into result->resolved_calls. */
void cbm_pxc_run_one(CBMLanguage lang, CBMFileResult *r, const char *source, int source_len,
                     const char *module_qn, CBMLSPDef *defs, int def_count, const char **imp_names,
                     const char **imp_qns, int imp_count) {
    TSTree *tree = r->cached_tree; /* may be NULL — LSP re-parses then */

    CBMArena scratch;
    cbm_arena_init(&scratch);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    CBMCallArray synthetic_calls;
    memset(&synthetic_calls, 0, sizeof(synthetic_calls));

    switch (lang) {
    case CBM_LANG_GO:
        cbm_run_go_lsp_cross(&scratch, source, source_len, module_qn, defs, def_count, imp_names,
                             imp_qns, imp_count, tree, &out);
        break;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA: {
        bool cpp_mode = (lang != CBM_LANG_C);
        /* C/C++ cross LSP takes include_paths/include_ns_qns instead of
         * imports — the existing pipeline doesn't carry C-style include
         * resolution as a separate map, so pass NULL/0 and let the LSP
         * fall back to its own #include scan. */
        cbm_run_c_lsp_cross(&scratch, source, source_len, module_qn, cpp_mode, defs, def_count,
                            NULL, NULL, 0, tree, &out);
        break;
    }
    case CBM_LANG_PYTHON:
        cbm_run_py_lsp_cross(&scratch, source, source_len, module_qn, defs, def_count, imp_names,
                             imp_qns, imp_count, tree, &out, &synthetic_calls);
        break;
    case CBM_LANG_PHP:
        cbm_run_php_lsp_cross(&scratch, source, source_len, module_qn, defs, def_count, imp_names,
                              imp_qns, imp_count, tree, &out);
        break;
    case CBM_LANG_JAVA:
        cbm_run_java_lsp_cross(&scratch, source, source_len, module_qn, defs, def_count, imp_names,
                               imp_qns, imp_count, tree, &out);
        break;
    case CBM_LANG_KOTLIN:
        cbm_run_kotlin_lsp_cross(&scratch, source, source_len, module_qn, defs, def_count,
                                 imp_names, imp_qns, imp_count, tree, &out);
        break;
    case CBM_LANG_RUST: {
        /* The Rust resolver wants CBMRustLSPDef (rust_lsp.h), not the
         * pipeline's CBMLSPDef — the structs share their first 9 fields
         * but diverge after, so convert into the scratch arena. The
         * workspace manifest (set once by the sequential driver) lets
         * `crate_a::foo` route across the crate boundary (#56). */
        CBMRustLSPDef *rdefs = pxc_lspdefs_to_rust(&scratch, defs, def_count);
        cbm_run_rust_lsp_cross_with_manifest(&scratch, source, source_len, module_qn, rdefs,
                                             def_count, imp_names, imp_qns, imp_count, tree,
                                             g_pxc_rust_manifest, &out, &synthetic_calls);
        break;
    }
    default:
        break;
    }

    pxc_append_results(&r->arena, &r->resolved_calls, &out);
    pxc_append_synthetic_calls(&r->arena, &r->calls, &synthetic_calls);
    cbm_arena_destroy(&scratch);
}

/* Variant of cbm_pxc_run_one for TS/JS/JSX/TSX with explicit dialect
 * flags. Same scratch-arena lifecycle as cbm_pxc_run_one. */
void cbm_pxc_run_one_ts(CBMFileResult *r, const char *source, int source_len, const char *module_qn,
                        CBMLSPDef *defs, int def_count, const char **imp_names,
                        const char **imp_qns, int imp_count, bool js_mode, bool jsx_mode,
                        bool dts_mode) {
    CBMArena scratch;
    cbm_arena_init(&scratch);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));

    cbm_run_ts_lsp_cross(&scratch, source, source_len, module_qn, js_mode, jsx_mode, dts_mode, defs,
                         def_count, imp_names, imp_qns, imp_count, r->cached_tree, &out);

    pxc_append_results(&r->arena, &r->resolved_calls, &out);
    cbm_arena_destroy(&scratch);
}

static bool pxc_path_in_list(const char *path, const char *const *paths, int count) {
    if (!path || !paths || count <= 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (paths[i] && strcmp(path, paths[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool pxc_language_for_store_path(const cbm_pipeline_ctx_t *ctx, const char *rel_path,
                                        CBMLanguage *out) {
    if (!ctx || !rel_path || !out || !ctx->store_backed_all_files ||
        ctx->store_backed_all_file_count <= 0) {
        return false;
    }
    for (int i = 0; i < ctx->store_backed_all_file_count; i++) {
        const cbm_file_info_t *file = &ctx->store_backed_all_files[i];
        if (file->rel_path && strcmp(file->rel_path, rel_path) == 0) {
            *out = file->language;
            return true;
        }
    }
    return false;
}

static const char *pxc_find_import_symbol_qn(CBMLSPDef *defs, int def_count, const char *module_qn,
                                             const char *local_name) {
    if (!defs || def_count <= 0 || !module_qn || !module_qn[0] || !local_name || !local_name[0] ||
        strcmp(local_name, "*") == 0) {
        return NULL;
    }

    size_t module_len = strlen(module_qn);
    for (int i = 0; i < def_count; i++) {
        const char *qn = defs[i].qualified_name;
        const char *short_name = defs[i].short_name;
        if (!qn || !short_name || strcmp(short_name, local_name) != 0) {
            continue;
        }
        if (strncmp(qn, module_qn, module_len) == 0 && qn[module_len] == '.' &&
            strcmp(qn + module_len + 1, local_name) == 0) {
            return qn;
        }
    }
    return NULL;
}

const char **cbm_pxc_refine_import_values_from_defs(CBMArena *arena, CBMLSPDef *defs, int def_count,
                                                    const char **imp_keys, const char **imp_vals,
                                                    int imp_count) {
    if (!arena || !defs || def_count <= 0 || !imp_keys || !imp_vals || imp_count <= 0) {
        return NULL;
    }

    const char **refined = NULL;
    for (int i = 0; i < imp_count; i++) {
        const char *override = pxc_find_import_symbol_qn(defs, def_count, imp_vals[i], imp_keys[i]);
        if (!override || (imp_vals[i] && strcmp(override, imp_vals[i]) == 0)) {
            continue;
        }
        if (!refined) {
            refined = (const char **)cbm_arena_alloc(arena, (size_t)imp_count * sizeof(*refined));
            if (!refined) {
                return NULL;
            }
            for (int j = 0; j < imp_count; j++) {
                refined[j] = imp_vals[j];
            }
        }
        refined[i] = override;
    }
    return refined;
}

static CBMLSPDef *pxc_collect_store_backed_defs_for_file(const cbm_pipeline_ctx_t *ctx,
                                                         CBMArena *arena, const char *module_qn,
                                                         const char *const *imp_qns, int imp_count,
                                                         int *out_count, bool *out_truncated) {
    if (out_count) {
        *out_count = 0;
    }
    if (out_truncated) {
        *out_truncated = false;
    }
    if (!ctx || !arena || !out_count || !ctx->store_backed_node_lookup || !ctx->project_name ||
        ctx->store_backed_lsp_scope_cap <= 0 ||
        ((!module_qn || !module_qn[0]) && (!imp_qns || imp_count <= 0))) {
        return NULL;
    }

    int scope_count = imp_count + (module_qn && module_qn[0] ? 1 : 0);
    const char **scope_qns = cbm_arena_alloc(arena, (size_t)scope_count * sizeof(*scope_qns));
    if (!scope_qns) {
        return NULL;
    }
    int scope_index = 0;
    if (module_qn && module_qn[0]) {
        scope_qns[scope_index++] = module_qn;
    }
    for (int i = 0; imp_qns && i < imp_count; i++) {
        scope_qns[scope_index++] = imp_qns[i];
    }

    char **candidate_qns = NULL;
    int candidate_count = 0;
    bool truncated = false;
    int rc = cbm_store_list_symbol_scope_qns_by_qns(
        ctx->store_backed_node_lookup, ctx->project_name, scope_qns, scope_index,
        ctx->store_backed_lsp_scope_cap, &candidate_qns, &candidate_count, &truncated);
    if (rc != CBM_STORE_OK || truncated || candidate_count <= 0) {
        if (truncated) {
            if (out_truncated) {
                *out_truncated = true;
            }
            cbm_log_info("lsp_cross.store_defs_skipped", "reason", "scope_truncated", "cap",
                         itoa_buf(ctx->store_backed_lsp_scope_cap));
        }
        for (int i = 0; i < candidate_count; i++) {
            free(candidate_qns[i]);
        }
        free(candidate_qns);
        return NULL;
    }

    cbm_node_t *nodes = NULL;
    int node_count = 0;
    rc = cbm_store_find_nodes_by_qns(ctx->store_backed_node_lookup, ctx->project_name,
                                     (const char **)candidate_qns, candidate_count, &nodes,
                                     &node_count);
    for (int i = 0; i < candidate_count; i++) {
        free(candidate_qns[i]);
    }
    free(candidate_qns);
    if (rc != CBM_STORE_OK || node_count <= 0) {
        return NULL;
    }

    const char **surface_paths = calloc((size_t)node_count, sizeof(*surface_paths));
    CBMHashTable *seen_paths = cbm_ht_create((size_t)node_count * PAIR_LEN + CBM_SZ_64);
    int surface_path_count = 0;
    if (surface_paths && seen_paths) {
        for (int i = 0; i < node_count; i++) {
            const char *file_path = nodes[i].file_path;
            if (!file_path ||
                pxc_path_in_list(file_path, ctx->store_backed_changed_paths,
                                 ctx->store_backed_changed_path_count) ||
                cbm_ht_has(seen_paths, file_path)) {
                continue;
            }
            cbm_ht_set(seen_paths, file_path, (void *)file_path);
            surface_paths[surface_path_count++] = file_path;
        }
    }
    cbm_lsp_surface_row_t *surface_rows = NULL;
    int surface_row_count = 0;
    int surface_rc = surface_paths && seen_paths
                         ? cbm_store_get_lsp_surfaces_by_paths(
                               ctx->store_backed_node_lookup, ctx->project_name, surface_paths,
                               surface_path_count, &surface_rows, &surface_row_count)
                         : CBM_STORE_ERR;
    CBMLSPDef **surface_defs = NULL;
    int *surface_def_counts = NULL;
    int surface_def_total = 0;
    bool surfaces_complete = surface_rc == CBM_STORE_OK && surface_row_count == surface_path_count;
    if (surfaces_complete) {
        surface_defs =
            calloc((size_t)(surface_row_count ? surface_row_count : 1), sizeof(*surface_defs));
        surface_def_counts = calloc((size_t)(surface_row_count ? surface_row_count : 1),
                                    sizeof(*surface_def_counts));
        surfaces_complete = surface_defs && surface_def_counts;
    }
    for (int i = 0; surfaces_complete && i < surface_row_count; i++) {
        surface_def_counts[i] =
            cbm_lsp_surface_defs_from_json(arena, surface_rows[i].defs_json, &surface_defs[i]);
        if (surface_def_counts[i] < 0) {
            surfaces_complete = false;
        } else {
            surface_def_total += surface_def_counts[i];
        }
    }
    CBMLSPDef *defs = NULL;
    if (surfaces_complete && surface_def_total > 0) {
        defs = calloc((size_t)surface_def_total, sizeof(*defs));
        if (defs) {
            int copied = 0;
            for (int i = 0; i < surface_row_count; i++) {
                if (surface_def_counts[i] > 0) {
                    memcpy(defs + copied, surface_defs[i],
                           (size_t)surface_def_counts[i] * sizeof(*defs));
                    copied += surface_def_counts[i];
                }
            }
            *out_count = copied;
        }
    }
    free(surface_def_counts);
    free(surface_defs);
    cbm_store_free_lsp_surfaces(surface_rows, surface_row_count);
    cbm_ht_free(seen_paths);
    free(surface_paths);
    if (defs) {
        cbm_store_free_nodes(nodes, node_count);
        return defs;
    }

    defs = (CBMLSPDef *)calloc((size_t)node_count, sizeof(*defs));
    if (!defs) {
        cbm_store_free_nodes(nodes, node_count);
        return NULL;
    }
    int def_count = 0;
    for (int i = 0; i < node_count; i++) {
        if (pxc_path_in_list(nodes[i].file_path, ctx->store_backed_changed_paths,
                             ctx->store_backed_changed_path_count)) {
            continue;
        }
        CBMLanguage lang = CBM_LANG_COUNT;
        if (!pxc_language_for_store_path(ctx, nodes[i].file_path, &lang) ||
            !cbm_pxc_has_cross_lsp(lang)) {
            continue;
        }
        if (cbm_pxc_build_lsp_def_from_node(arena, &nodes[i], lang, &defs[def_count]) == 0) {
            def_count++;
        }
    }
    cbm_store_free_nodes(nodes, node_count);
    if (def_count == 0) {
        free(defs);
        return NULL;
    }
    *out_count = def_count;
    return defs;
}

/* Parse the project's root Cargo.toml (if present) into `out_m`, using
 * `marena` for the manifest's owned strings. Returns true when a manifest
 * was parsed (a workspace root or any [package]/[dependencies]); false when
 * there is no readable Cargo.toml, leaving *out_m untouched. The resulting
 * manifest feeds cross-CRATE Rust resolution (#56): its [workspace].members
 * map lets `crate_a::foo` route to the member crate's def. */
/* Per-file cross-LSP dispatch, shared by the PARALLEL resolve worker and the
 * SEQUENTIAL driver. One code path = one semantics: filter the global defs
 * down to the file's own+imported modules via the module-def index, resolve
 * through the shared prebuilt registry when the language has one (per-file
 * OVERLAY pattern — no registry build, no finalize), and only fall back to
 * the per-file registry build (with the FILTERED defs) for languages without
 * a shared-registry variant. Before this helper existed the sequential
 * driver fed the FULL def list into full per-file registry builds —
 * O(files x defs), which ground an 81k-file TS corpus for hours.
 *
 * `rust_shared_get` supplies the lazily-built shared Rust all-defs registry
 * (the parallel resolver owns its once-guard); NULL means "no shared rust
 * registry available" and rust NULL-filter files take the per-file build. */
void cbm_pxc_dispatch_file(CBMLanguage lang, CBMFileResult *result, const char *source,
                           int source_len, const char *rel, const char *def_module,
                           const CBMCrossLspRegistries *cross_registries,
                           const CBMModuleDefIndex *module_def_index, CBMLSPDef *all_defs,
                           int all_def_count, const char **imp_keys, const char **imp_vals,
                           int imp_count, CBMTypeRegistry *(*rust_shared_get)(void *),
                           void *rust_shared_ctx) {
    if (!result) {
        return;
    }
    bool used_prebuilt = false;
    CBMTypeRegistry *prebuilt =
        cross_registries ? cbm_pxc_registry_for_lang(cross_registries, lang) : NULL;
    if (prebuilt) {
        switch (lang) {
        case CBM_LANG_GO:
            /* Tier 3 (metadata-driven): per-file extraction already records
             * receiver-type QNs and package-aliased expressions in Tier-1
             * lsp_unresolved rows. Resolve those rows directly against the
             * finalized project registry—no second parse or AST walk. The old
             * cbm_run_go_lsp_cross_with_registry path re-derived the same
             * metadata before reaching the same lookups, so keeping both paths
             * only added latency and a second lifecycle to maintain. */
            cbm_go_fast_resolve_qualified_calls(result, prebuilt, imp_keys, imp_vals, imp_count);
            used_prebuilt = true;
            break;
        case CBM_LANG_PYTHON: {
            CBMArena scratch;
            cbm_arena_init(&scratch);
            CBMResolvedCallArray out = {0};
            CBMCallArray synthetic_calls = {0};
            cbm_run_py_lsp_cross_with_registry(&scratch, source, source_len, def_module, prebuilt,
                                               imp_keys, imp_vals, imp_count, result->cached_tree,
                                               &out, &synthetic_calls);
            pxc_append_results(&result->arena, &result->resolved_calls, &out);
            pxc_append_synthetic_calls(&result->arena, &result->calls, &synthetic_calls);
            cbm_arena_destroy(&scratch);
            used_prebuilt = true;
            break;
        }
        case CBM_LANG_C:
        case CBM_LANG_CPP:
        case CBM_LANG_CUDA:
            cbm_run_c_lsp_cross_with_registry(
                &result->arena, source, source_len, def_module, (lang != CBM_LANG_C), prebuilt,
                imp_keys, imp_vals, imp_count, result->cached_tree, &result->resolved_calls);
            used_prebuilt = true;
            break;
        case CBM_LANG_CSHARP:
            cbm_run_cs_lsp_cross_with_registry(&result->arena, source, source_len, def_module,
                                               prebuilt, imp_vals, imp_count, result->cached_tree,
                                               &result->resolved_calls);
            used_prebuilt = true;
            break;
        case CBM_LANG_JAVASCRIPT:
        case CBM_LANG_TYPESCRIPT:
        case CBM_LANG_TSX: {
            /* TS: per-file OVERLAY chained to the shared base. Filter to
             * own+imports so the overlay builder can pick out own-module
             * defs without scanning the whole project. */
            bool js;
            bool jsx;
            bool dts;
            cbm_pxc_ts_modes(lang, rel, &js, &jsx, &dts);
            CBMLSPDef *ts_defs = all_defs;
            int ts_def_count = all_def_count;
            CBMLSPDef *ts_filtered = NULL;
            if (module_def_index) {
                int fc = 0;
                bool filter_succeeded = false;
                ts_filtered = cbm_pxc_filter_defs_for_file(
                    module_def_index, all_defs, lang, result->namespace_name, def_module, imp_vals,
                    imp_count, &fc, &filter_succeeded);
                if (filter_succeeded) {
                    ts_defs = ts_filtered;
                    ts_def_count = fc;
                }
            }
            cbm_run_ts_lsp_cross_with_registry(&result->arena, source, source_len, def_module, js,
                                               jsx, dts, prebuilt, ts_defs, ts_def_count, imp_keys,
                                               imp_vals, imp_count, result->cached_tree,
                                               &result->resolved_calls);
            free(ts_filtered);
            used_prebuilt = true;
            break;
        }
        /* PHP falls through to the per-file build path below until its
         * overlay variant lands. */
        default:
            break;
        }
    }

    if (used_prebuilt) {
        return;
    }
    /* Fallback: gopls per-file filter + per-file registry build. RUST is
     * exempt from the module filter: its resolution is Cargo-manifest-aware
     * and a `crate_a::foo` reference routes to defs in ANOTHER workspace
     * crate — a module that is in neither own_module nor the import map, so
     * the filter starves cross-crate resolution (#56 repro red). Rust
     * therefore always resolves against the FULL def universe: the lazily
     * built shared registry when available, else a full per-file build. */
    CBMLSPDef *filtered = NULL;
    CBMLSPDef *file_defs = all_defs;
    int file_def_count = all_def_count;
    if (module_def_index && lang != CBM_LANG_RUST) {
        int filtered_count = 0;
        bool filter_succeeded = false;
        filtered = cbm_pxc_filter_defs_for_file(module_def_index, all_defs, lang,
                                                result->namespace_name, def_module, imp_vals,
                                                imp_count, &filtered_count, &filter_succeeded);
        if (filter_succeeded) {
            file_defs = filtered;
            file_def_count = filtered_count;
        }
    }
    if (lang == CBM_LANG_RUST) {
        CBMTypeRegistry *shared = rust_shared_get ? rust_shared_get(rust_shared_ctx) : NULL;
        if (shared) {
            CBMArena scratch;
            cbm_arena_init(&scratch);
            CBMResolvedCallArray out = {0};
            CBMCallArray synthetic_calls = {0};
            cbm_run_rust_lsp_cross_with_registry(
                &scratch, source, source_len, def_module, shared, imp_keys, imp_vals, imp_count,
                result->cached_tree, cbm_pxc_get_rust_manifest(), &out, &synthetic_calls);
            pxc_append_results(&result->arena, &result->resolved_calls, &out);
            pxc_append_synthetic_calls(&result->arena, &result->calls, &synthetic_calls);
            cbm_arena_destroy(&scratch);
        } else {
            cbm_pxc_run_one(lang, result, source, source_len, def_module, file_defs, file_def_count,
                            imp_keys, imp_vals, imp_count);
        }
    } else if (lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX) {
        bool js;
        bool jsx;
        bool dts;
        cbm_pxc_ts_modes(lang, rel, &js, &jsx, &dts);
        cbm_pxc_run_one_ts(result, source, source_len, def_module, file_defs, file_def_count,
                           imp_keys, imp_vals, imp_count, js, jsx, dts);
    } else {
        cbm_pxc_run_one(lang, result, source, source_len, def_module, file_defs, file_def_count,
                        imp_keys, imp_vals, imp_count);
    }
    free(filtered);
}

static bool pxc_build_rust_manifest(const cbm_pipeline_ctx_t *ctx, CBMArena *marena,
                                    CBMCargoManifest *out_m) {
    if (!ctx || !ctx->repo_path || !marena || !out_m)
        return false;
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/Cargo.toml", ctx->repo_path);
    if (n <= 0 || (size_t)n >= sizeof(path))
        return false;
    int toml_len = 0;
    char *toml = pxc_read_file(path, &toml_len);
    if (!toml || toml_len <= 0) {
        free(toml);
        return false;
    }
    memset(out_m, 0, sizeof(*out_m));
    cbm_cargo_parse(marena, toml, toml_len, out_m);
    free(toml); /* cargo parser copies into marena */
    return true;
}

int cbm_pipeline_pass_lsp_cross(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                int file_count, CBMFileResult **cache) {
    if (!ctx || !files || file_count <= 0 || !cache)
        return 0;

    cbm_log_info("pass.start", "pass", "lsp_cross", "files", itoa_buf(file_count));

    /* Build the Rust workspace manifest once (only when the project has at
     * least one Rust file, to avoid an unconditional Cargo.toml read).
     * The manifest's strings live in `cargo_arena`; the resolver borrows
     * the pointer through the file-static set below. */
    bool have_rust = false;
    for (int i = 0; i < file_count; i++) {
        if (cache[i] && files[i].language == CBM_LANG_RUST) {
            have_rust = true;
            break;
        }
    }
    CBMArena cargo_arena;
    CBMCargoManifest cargo_manifest;
    bool have_manifest = false;
    if (have_rust) {
        cbm_arena_init(&cargo_arena);
        have_manifest = pxc_build_rust_manifest(ctx, &cargo_arena, &cargo_manifest);
        cbm_pxc_set_rust_manifest(have_manifest ? &cargo_manifest : NULL);
    }

    /* Per-file module QN cache so we don't recompute it once per def + once
     * per call. cbm_pipeline_fqn_module mallocs; freed at end. */
    char **def_modules = (char **)calloc((size_t)file_count, sizeof(char *));
    if (!def_modules) {
        cbm_log_error("pass.err", "pass", "lsp_cross", "phase", "alloc");
        /* The manifest pointer borrows from cargo_arena. Clear it before the
         * allocation failure path destroys the arena so no later pass can
         * observe a dangling process-global pointer. */
        if (have_rust) {
            cbm_pxc_set_rust_manifest(NULL);
            cbm_arena_destroy(&cargo_arena);
        }
        return 0;
    }

    int def_count = 0;
    int *def_starts = (int *)calloc((size_t)file_count + 1, sizeof(int));
    CBMLSPDef *all_defs = cbm_pxc_collect_all_defs(cache, files, file_count, ctx->project_name,
                                                   def_modules, &def_count, def_starts);
    /* Same seam as the parallel driver: serialize per-file surfaces while the
     * result cache is alive. Failure only degrades to a full rebuild on the
     * next incremental run. */
    if (ctx->pipeline && all_defs && def_starts) {
        cbm_lsp_surface_row_t *surface_rows = NULL;
        int surface_count = 0;
        if (cbm_lsp_surface_build_rows(ctx->project_name, cache, files, file_count, all_defs,
                                       def_starts, &surface_rows, &surface_count) == 0) {
            cbm_pipeline_set_lsp_surfaces(ctx->pipeline, surface_rows, surface_count);
        }
    }
    free(def_starts);

    /* Shared prepare (mirrors run_parallel_pipeline): inverted module-def
     * index + per-language shared registries, built ONCE for the whole pass.
     * The per-file loop below then dispatches through the SAME helper the
     * parallel resolve worker uses — previously this driver handed the FULL
     * def list to full per-file registry builds (O(files x defs); the
     * ms-typescript sequential crawl). The registries live in the
     * CALLER-OWNED ctx->seq_cross_arena: resolved_calls may borrow registry
     * strings that the later calls pass still reads, so the arena must
     * outlive this pass (run_sequential_pipeline destroys it after all
     * passes; freeing here was a pass_calls use-after-free). */
    CBMModuleDefIndex *module_def_index =
        all_defs ? cbm_pxc_build_module_def_index(all_defs, def_count) : NULL;
    CBMCrossLspRegistries cross_registries = {0};
    if (all_defs) {
        CBMArena *xa = &ctx->seq_cross_arena;
        if (!ctx->seq_cross_arena_live) {
            cbm_arena_init(xa);
            ctx->seq_cross_arena_live = true;
        }
        cross_registries.go = cbm_go_build_cross_registry(xa, all_defs, def_count);
        cross_registries.python = cbm_py_build_cross_registry(xa, all_defs, def_count);
        cross_registries.c = cbm_c_build_cross_registry(xa, all_defs, def_count);
        cross_registries.cs = cbm_cs_build_cross_registry(xa, all_defs, def_count);
        cross_registries.ts = cbm_ts_build_cross_registry(xa, all_defs, def_count);
    }

    int processed = 0;
    int skipped_no_lsp = 0;
    int skipped_no_source = 0;
    int per_lang_calls = 0;
    bool store_scope_truncated = false;

    for (int i = 0; i < file_count; i++) {
        if (!cache[i])
            continue;
        CBMLanguage lang = files[i].language;
        if (!cbm_pxc_has_cross_lsp(lang)) {
            skipped_no_lsp++;
            continue;
        }

        int source_len = 0;
        char *source = pxc_read_file(files[i].path, &source_len);
        if (!source || source_len <= 0) {
            free(source);
            skipped_no_source++;
            continue;
        }

        if (!def_modules[i]) {
            def_modules[i] = cbm_pipeline_fqn_module_dir(ctx->project_name, files[i].rel_path,
                                                         pxc_module_is_dir(files[i].language));
        }

        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        cbm_pxc_build_import_map(ctx->gbuf, ctx->project_name, files[i].rel_path, lang, cache[i],
                                 &imp_keys, &imp_vals, &imp_count);

        CBMArena store_defs_arena;
        cbm_arena_init(&store_defs_arena);
        int store_def_count = 0;
        bool scope_truncated = false;
        CBMLSPDef *store_defs =
            pxc_collect_store_backed_defs_for_file(ctx, &store_defs_arena, def_modules[i], imp_vals,
                                                   imp_count, &store_def_count, &scope_truncated);
        if (scope_truncated) {
            cbm_pxc_free_import_map(imp_keys, imp_vals, imp_count);
            cbm_arena_destroy(&store_defs_arena);
            free(source);
            store_scope_truncated = true;
            break;
        }
        int run_def_count = def_count + store_def_count;
        CBMLSPDef *run_defs = all_defs;
        bool free_run_defs = false;
        if (store_def_count > 0) {
            run_defs = (CBMLSPDef *)calloc((size_t)run_def_count, sizeof(*run_defs));
            if (run_defs) {
                if (def_count > 0 && all_defs) {
                    memcpy(run_defs, all_defs, (size_t)def_count * sizeof(*run_defs));
                }
                memcpy(run_defs + def_count, store_defs,
                       (size_t)store_def_count * sizeof(*run_defs));
                free_run_defs = true;
            } else {
                run_defs = all_defs;
                run_def_count = def_count;
                cbm_log_info("lsp_cross.store_defs_skipped", "reason", "alloc");
            }
        }
        const char **run_imp_vals = imp_vals;
        const char **refined_imp_vals = cbm_pxc_refine_import_values_from_defs(
            &store_defs_arena, run_defs, run_def_count, imp_keys, imp_vals, imp_count);
        if (refined_imp_vals) {
            run_imp_vals = refined_imp_vals;
        }

        /* Journal around the resolve: a hang here must be attributed to THIS
         * file, not to a stale extraction marker (the innocent-quarantine
         * failure mode). Build the same registry/index shape as a fresh pass
         * for the bounded changed+stored def set; the language strategies and
         * confidence metadata are part of canonical graph equivalence. */
        CBMCrossLspRegistries store_registries = {0};
        CBMModuleDefIndex *store_module_def_index = NULL;
        const CBMCrossLspRegistries *run_registries = &cross_registries;
        const CBMModuleDefIndex *run_module_def_index = module_def_index;
        if (store_def_count > 0) {
            store_module_def_index = cbm_pxc_build_module_def_index(run_defs, run_def_count);
            run_module_def_index = store_module_def_index;
            switch (lang) {
            case CBM_LANG_GO:
                store_registries.go =
                    cbm_go_build_cross_registry(&store_defs_arena, run_defs, run_def_count);
                break;
            case CBM_LANG_PYTHON:
                store_registries.python =
                    cbm_py_build_cross_registry(&store_defs_arena, run_defs, run_def_count);
                break;
            case CBM_LANG_C:
            case CBM_LANG_CPP:
            case CBM_LANG_CUDA:
                store_registries.c =
                    cbm_c_build_cross_registry(&store_defs_arena, run_defs, run_def_count);
                break;
            case CBM_LANG_CSHARP:
                store_registries.cs =
                    cbm_cs_build_cross_registry(&store_defs_arena, run_defs, run_def_count);
                break;
            case CBM_LANG_JAVASCRIPT:
            case CBM_LANG_TYPESCRIPT:
            case CBM_LANG_TSX:
                store_registries.ts =
                    cbm_ts_build_cross_registry(&store_defs_arena, run_defs, run_def_count);
                break;
            default:
                break;
            }
            run_registries = &store_registries;
        }
        cbm_index_mark_start(files[i].rel_path);
        cbm_pxc_dispatch_file(lang, cache[i], source, source_len, files[i].rel_path, def_modules[i],
                              run_registries, run_module_def_index, run_defs, run_def_count,
                              imp_keys, run_imp_vals, imp_count, NULL, NULL);
        cbm_index_mark_done(files[i].rel_path);
        if (free_run_defs) {
            free(run_defs);
        }
        cbm_pxc_free_module_def_index(store_module_def_index);
        free(store_defs);
        cbm_arena_destroy(&store_defs_arena);
        per_lang_calls++;
        processed++;

        cbm_pxc_free_import_map(imp_keys, imp_vals, imp_count);
        free(source);
    }

    cbm_pxc_free_module_def_index(module_def_index);
    free(all_defs);
    /* The module-QN strings are borrowed by the shared cross registries in
     * ctx->seq_cross_arena, which deliberately outlive this pass so that
     * pass_calls can read borrowed registry strings. Freeing the strings here
     * while keeping the registries alive was a use-after-free (pass_calls
     * strcmp on a freed module QN, caught by ASan on the kernel corpus tier).
     * Ownership transfers to the ctx; released beside the arena. */
    ctx->seq_cross_def_modules = def_modules;
    ctx->seq_cross_def_module_count = file_count;

    /* Drop the borrowed manifest pointer before its arena dies, so a later
     * pass (or a stale thread-local) can never read freed manifest memory. */
    if (have_rust) {
        cbm_pxc_set_rust_manifest(NULL);
        cbm_arena_destroy(&cargo_arena);
    }
    (void)have_manifest;

    cbm_log_info("pass.done", "pass", "lsp_cross", "files_processed", itoa_buf(processed),
                 "files_skipped_no_lsp", itoa_buf(skipped_no_lsp), "files_skipped_no_source",
                 itoa_buf(skipped_no_source), "defs_total", itoa_buf(def_count), "lsp_calls",
                 itoa_buf(per_lang_calls));
    return store_scope_truncated ? CBM_NOT_FOUND : 0;
}

void cbm_pipeline_release_seq_cross_state(cbm_pipeline_ctx_t *ctx) {
    if (!ctx) {
        return;
    }
    for (int i = 0; i < ctx->seq_cross_def_module_count; i++) {
        free(ctx->seq_cross_def_modules[i]);
    }
    free(ctx->seq_cross_def_modules);
    ctx->seq_cross_def_modules = NULL;
    ctx->seq_cross_def_module_count = 0;
    if (ctx->seq_cross_arena_live) {
        cbm_arena_destroy(&ctx->seq_cross_arena);
        ctx->seq_cross_arena_live = false;
    }
}

/* ── Per-module def index (gopls "package summary" pattern) ──── */

typedef struct {
    int count;
    int cap;
    int *indices; /* malloc'd; indices into the caller's all_defs[] */
} pxc_module_entry_t;

struct CBMModuleDefIndex {
    CBMHashTable *ht;           /* module_qn → pxc_module_entry_t* */
    CBMHashTable *namespace_ht; /* declared package/namespace → pxc_module_entry_t* */
    int def_count;              /* total entries in the all_defs[] array */
};

/* cbm_ht_foreach callback: free each pxc_module_entry_t. */
static void pxc_module_entry_free_cb(const char *key, void *value, void *userdata) {
    (void)key;
    (void)userdata;
    pxc_module_entry_t *e = (pxc_module_entry_t *)value;
    if (!e)
        return;
    free(e->indices);
    free(e);
}
static pxc_module_entry_t *pxc_module_entry_get_or_create(CBMHashTable *ht, const char *key) {
    if (!ht || !key || !key[0]) {
        return NULL;
    }
    pxc_module_entry_t *e = (pxc_module_entry_t *)cbm_ht_get(ht, key);
    if (e) {
        return e;
    }
    e = (pxc_module_entry_t *)calloc(1, sizeof(*e));
    if (!e) {
        return NULL;
    }
    e->cap = 8;
    e->indices = (int *)calloc((size_t)e->cap, sizeof(*e->indices));
    if (!e->indices) {
        free(e);
        return NULL;
    }
    cbm_ht_set(ht, key, e);
    return e;
}

static void pxc_module_entry_add_index(pxc_module_entry_t *e, int index) {
    if (!e) {
        return;
    }
    if (e->count >= e->cap) {
        int new_cap = e->cap * 2;
        int *new_indices = (int *)realloc(e->indices, (size_t)new_cap * sizeof(*new_indices));
        if (!new_indices) {
            return;
        }
        e->indices = new_indices;
        e->cap = new_cap;
    }
    e->indices[e->count++] = index;
}

static bool pxc_is_jvm_lang(CBMLanguage lang);
static bool pxc_def_lang_matches(CBMLanguage caller_lang, CBMLanguage def_lang);

static int pxc_mark_entry_defs(bool *selected, const pxc_module_entry_t *e,
                               const CBMLSPDef *all_defs, CBMLanguage caller_lang) {
    if (!selected || !e) {
        return 0;
    }
    int added = 0;
    for (int j = 0; j < e->count; j++) {
        int idx = e->indices[j];
        const CBMLSPDef *def = &all_defs[idx];
        if (!pxc_def_lang_matches(caller_lang, def->lang) || selected[idx]) {
            continue;
        }
        selected[idx] = true;
        added++;
    }
    return added;
}

static bool pxc_is_jvm_lang(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA || lang == CBM_LANG_KOTLIN;
}

/* Java and Kotlin files without a package declaration still share one JVM
 * default package. The hash table intentionally rejects empty keys, so use an
 * internal-only sentinel to make that package selectable across files. */
static const char *pxc_namespace_index_key(const char *namespace_name) {
    return namespace_name && namespace_name[0] ? namespace_name : PXC_DEFAULT_JVM_NAMESPACE;
}

static bool pxc_def_lang_matches(CBMLanguage caller_lang, CBMLanguage def_lang) {
    if (pxc_is_jvm_lang(caller_lang)) {
        return pxc_is_jvm_lang(def_lang);
    }
    return true;
}

static void pxc_mark_module_defs(const CBMModuleDefIndex *idx, bool *selected,
                                 const CBMLSPDef *all_defs, CBMLanguage caller_lang,
                                 const char *module_qn, int *total) {
    if (!idx || !idx->ht || !module_qn || !module_qn[0]) {
        return;
    }
    pxc_module_entry_t *e = (pxc_module_entry_t *)cbm_ht_get(idx->ht, module_qn);
    int added = pxc_mark_entry_defs(selected, e, all_defs, caller_lang);
    if (total) {
        *total += added;
    }
}

/* Import maps bind source locals to semantic targets. A from-import therefore
 * carries a symbol QN (`project.module.Class` / `project.module.function`),
 * while the def index is keyed by the declaring file module
 * (`project.module`). Select the nearest materialized module prefix; the
 * language registry still has to prove the full target QN before an edge is
 * emitted, so this broadens the candidate set without weakening precision. */
static void pxc_mark_import_defs(const CBMModuleDefIndex *idx, bool *selected,
                                 const CBMLSPDef *all_defs, CBMLanguage caller_lang,
                                 const char *import_qn, int *total) {
    if (!idx || !idx->ht || !import_qn || !import_qn[0]) {
        return;
    }
    char *candidate = cbm_strdup(import_qn);
    if (!candidate) {
        return;
    }
    bool matched = false;
    for (;;) {
        pxc_module_entry_t *entry = (pxc_module_entry_t *)cbm_ht_get(idx->ht, candidate);
        if (entry) {
            int added = pxc_mark_entry_defs(selected, entry, all_defs, caller_lang);
            if (total) {
                *total += added;
            }
            matched = true;
            break;
        }
        char *dot = strrchr(candidate, '.');
        if (!dot) {
            break;
        }
        *dot = '\0';
    }
    /* Kotlin/Java source imports are package-qualified while file modules are
     * path-qualified. If no module prefix matched, walk the declared-package
     * index from the full import toward its package. This only selects a small
     * candidate registry; the language resolver must still prove the symbol. */
    if (!matched && pxc_is_jvm_lang(caller_lang) && idx->namespace_ht) {
        memcpy(candidate, import_qn, strlen(import_qn) + 1U);
        for (;;) {
            pxc_module_entry_t *entry = (pxc_module_entry_t *)cbm_ht_get(
                idx->namespace_ht, pxc_namespace_index_key(candidate));
            if (entry) {
                int added = pxc_mark_entry_defs(selected, entry, all_defs, caller_lang);
                if (total) {
                    *total += added;
                }
                break;
            }
            char *dot = strrchr(candidate, '.');
            if (!dot) {
                break;
            }
            *dot = '\0';
        }
    }
    free(candidate);
}

CBMModuleDefIndex *cbm_pxc_build_module_def_index(CBMLSPDef *all_defs, int def_count) {
    if (!all_defs || def_count <= 0) {
        return NULL;
    }

    CBMHashTable *ht = cbm_ht_create(64);
    CBMHashTable *namespace_ht = cbm_ht_create(64);
    if (!ht || !namespace_ht) {
        cbm_ht_free(ht);
        cbm_ht_free(namespace_ht);
        return NULL;
    }

    /* Single pass: index each def by file module and by declared package.
     * JVM mixed roots (`src/main/java` + `src/main/kotlin`) share the
     * declared package, not the path-derived module prefix. */
    for (int i = 0; i < def_count; i++) {
        pxc_module_entry_add_index(pxc_module_entry_get_or_create(ht, all_defs[i].def_module_qn),
                                   i);
        pxc_module_entry_add_index(
            pxc_module_entry_get_or_create(namespace_ht,
                                           pxc_namespace_index_key(all_defs[i].namespace_name)),
            i);
    }

    CBMModuleDefIndex *idx = (CBMModuleDefIndex *)calloc(1, sizeof(*idx));
    if (!idx) {
        cbm_ht_foreach(ht, pxc_module_entry_free_cb, NULL);
        cbm_ht_free(ht);
        cbm_ht_foreach(namespace_ht, pxc_module_entry_free_cb, NULL);
        cbm_ht_free(namespace_ht);
        return NULL;
    }
    idx->ht = ht;
    idx->namespace_ht = namespace_ht;
    idx->def_count = def_count;
    return idx;
}

void cbm_pxc_free_module_def_index(CBMModuleDefIndex *idx) {
    if (!idx) {
        return;
    }
    if (idx->ht) {
        cbm_ht_foreach(idx->ht, pxc_module_entry_free_cb, NULL);
        cbm_ht_free(idx->ht);
    }
    if (idx->namespace_ht) {
        cbm_ht_foreach(idx->namespace_ht, pxc_module_entry_free_cb, NULL);
        cbm_ht_free(idx->namespace_ht);
    }
    free(idx);
}

CBMLSPDef *cbm_pxc_filter_defs_for_file(const CBMModuleDefIndex *idx, CBMLSPDef *all_defs,
                                        CBMLanguage caller_lang, const char *caller_namespace,
                                        const char *own_module, const char *const *imp_qns,
                                        int imp_count, int *out_count, bool *out_success) {
    if (out_count) {
        *out_count = 0;
    }
    if (out_success) {
        *out_success = false;
    }
    if (!idx || !idx->ht || !all_defs || !out_count || !out_success || idx->def_count <= 0) {
        return NULL;
    }

    bool *selected = (bool *)calloc((size_t)idx->def_count, sizeof(*selected));
    if (!selected) {
        return NULL;
    }

    int total = 0;
    pxc_mark_module_defs(idx, selected, all_defs, caller_lang, own_module, &total);
    for (int i = 0; i < imp_count; i++) {
        pxc_mark_import_defs(idx, selected, all_defs, caller_lang, imp_qns[i], &total);
    }
    if (pxc_is_jvm_lang(caller_lang) && idx->namespace_ht) {
        const char *namespace_key = pxc_namespace_index_key(caller_namespace);
        pxc_module_entry_t *e = (pxc_module_entry_t *)cbm_ht_get(idx->namespace_ht, namespace_key);
        total += pxc_mark_entry_defs(selected, e, all_defs, caller_lang);
    }

    if (total == 0) {
        free(selected);
        *out_success = true;
        return NULL;
    }

    CBMLSPDef *out = (CBMLSPDef *)malloc((size_t)total * sizeof(CBMLSPDef));
    if (!out) {
        free(selected);
        return NULL;
    }

    int n = 0;
    for (int i = 0; i < idx->def_count; i++) {
        if (selected[i]) {
            out[n++] = all_defs[i];
        }
    }
    *out_count = n;
    *out_success = true;
    free(selected);
    return out;
}
