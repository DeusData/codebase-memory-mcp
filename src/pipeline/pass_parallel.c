/*
 * pass_parallel.c — Three-phase parallel pipeline.
 *
 * Phase 3A: Parallel extract + create definition nodes (per-worker gbufs)
 * Phase 3B: Serial registry build + edge creation from cached results
 * Phase 4:  Parallel call/usage/semantic resolution (per-worker edge bufs)
 *
 * Each file is read and parsed ONCE (Phase 3A). The CBMFileResult is cached
 * and reused for resolution (Phase 4), eliminating 3x redundant I/O + parsing.
 *
 * Depends on: worker_pool, graph_buffer (shared IDs + merge), extraction (cbm.h)
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/worker_pool.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "graph_buffer/graph_buffer.h"
#include "service_patterns.h"
// NOLINTNEXTLINE(misc-include-cleaner) — platform.h included for interface contract
#include "foundation/platform.h"
#include "foundation/log.h"
#include "foundation/slab_alloc.h"
#include "foundation/mem.h"
#include "foundation/compat_regex.h"
#include "pipeline/httplink.h"
#include "cbm.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t extract_now_ns(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

/* ── Helpers (duplicated from pass files — kept static for isolation) ── */

/* Read file into a malloc'd buffer (= mimalloc in production). */
static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)100 * 1024 * 1024) {
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Free source buffer. */
static void free_source(char *buf) {
    free(buf);
}

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos. */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || !val[0]) {
        return;
    }
    if (*pos >= bufsize - 10) {
        return;
    }
    size_t p = *pos;
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;
    for (const char *s = val; *s && p < bufsize - 3; s++) {
        switch (*s) {
        case '"':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '"';
            }
            break;
        case '\\':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '\\';
            }
            break;
        case '\n':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'n';
            }
            break;
        case '\r':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'r';
            }
            break;
        case '\t':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 't';
            }
            break;
        default:
            buf[p++] = *s;
            break;
        }
    }
    if (p < bufsize - 1) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Append a JSON array of strings: ,"key":["a","b","c"] */
static void append_json_str_array(char *buf, size_t bufsize, size_t *pos, const char *key,
                                  const char **arr) {
    if (!arr || !arr[0] || *pos >= bufsize - 10) {
        return;
    }
    size_t p = *pos;
    int n = snprintf(buf + p, bufsize - p, ",\"%s\":[", key);
    if (n <= 0 || p + (size_t)n >= bufsize - 2) {
        return;
    }
    p += (size_t)n;
    for (int i = 0; arr[i]; i++) {
        if (i > 0 && p < bufsize - 1) {
            buf[p++] = ',';
        }
        if (p < bufsize - 1) {
            buf[p++] = '"';
        }
        for (const char *s = arr[i]; *s && p < bufsize - 2; s++) {
            if (*s == '"' || *s == '\\') {
                buf[p++] = '\\';
                if (p >= bufsize - 2) {
                    break;
                }
            }
            buf[p++] = *s;
        }
        if (p < bufsize - 1) {
            buf[p++] = '"';
        }
    }
    if (p < bufsize - 1) {
        buf[p++] = ']';
    }
    buf[p] = '\0';
    *pos = p;
}

static void build_def_props(char *buf, size_t bufsize, const CBMDefinition *def) {
    int n = snprintf(buf, bufsize,
                     "{\"complexity\":%d,\"lines\":%d,\"is_exported\":%s,"
                     "\"is_test\":%s,\"is_entry_point\":%s",
                     def->complexity, def->lines, def->is_exported ? "true" : "false",
                     def->is_test ? "true" : "false", def->is_entry_point ? "true" : "false");
    if (n <= 0 || (size_t)n >= bufsize) {
        buf[0] = '\0';
        return;
    }
    size_t pos = (size_t)n;
    append_json_string(buf, bufsize, &pos, "docstring", def->docstring);
    append_json_string(buf, bufsize, &pos, "signature", def->signature);
    append_json_string(buf, bufsize, &pos, "return_type", def->return_type);
    append_json_string(buf, bufsize, &pos, "parent_class", def->parent_class);
    append_json_str_array(buf, bufsize, &pos, "decorators", def->decorators);
    append_json_str_array(buf, bufsize, &pos, "base_classes", def->base_classes);
    append_json_str_array(buf, bufsize, &pos, "param_names", def->param_names);
    append_json_str_array(buf, bufsize, &pos, "param_types", def->param_types);
    if (pos < bufsize - 1) {
        buf[pos] = '}';
        buf[pos + 1] = '\0';
    }
}

/* Build import map from graph buffer IMPORTS edges (read-only access to gbuf). */
static int build_import_map(const cbm_gbuf_t *gbuf, const char *project_name, const char *rel_path,
                            // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                            const char ***out_keys, const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    char *file_qn = cbm_pipeline_fqn_compute(project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(gbuf, file_qn);
    free(file_qn);
    if (!file_node) {
        return 0;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc =
        cbm_gbuf_find_edges_by_source_type(gbuf, file_node->id, "IMPORTS", &edges, &edge_count);
    if (rc != 0 || edge_count == 0) {
        return 0;
    }

    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **keys = calloc(edge_count, sizeof(const char *));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **vals = calloc(edge_count, sizeof(const char *));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, e->target_id);
        if (!target || !e->properties_json) {
            continue;
        }
        const char *start = strstr(e->properties_json, "\"local_name\":\"");
        if (start) {
            start += strlen("\"local_name\":\"");
            const char *end = strchr(start, '"');
            if (end && end > start) {
                // NOLINTNEXTLINE(misc-include-cleaner) — strndup provided by standard header
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

static void free_import_map(const char **keys, const char **vals, int count) {
    if (keys) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
        }
        free((void *)keys);
    }
    if (vals) {
        free((void *)vals);
    }
}

static bool is_checked_exception(const char *name) {
    if (!name) {
        return false;
    }
    return false;
}

static const char *lookup_import_map_value(const char *name, const char **imp_keys,
                                            const char **imp_vals, int imp_count) {
    if (!name || !imp_keys || !imp_vals) return NULL;
    for (int i = 0; i < imp_count; i++) {
        if (imp_keys[i] && imp_vals[i] && strcmp(imp_keys[i], name) == 0) {
            return imp_vals[i];
        }
    }
    return NULL;
}

/* ── GDScript helpers (mirrored from pass_calls.c for parallel parity) ── */

static bool str_ends_with(const char *str, const char *suffix) {
    if (!str || !suffix) return false;
    size_t str_len = strlen(str);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > str_len) return false;
    return strcmp(str + str_len - suffix_len, suffix) == 0;
}

static size_t gdscript_signal_member_suffix_len(const char *callee_name) {
    if (!callee_name) return 0;
    if (str_ends_with(callee_name, ".emit")) return 5;
    if (str_ends_with(callee_name, ".connect")) return 8;
    return 0;
}

static bool strip_last_qn_segment(const char *qn, char *out, size_t out_size) {
    if (!qn || !out || out_size == 0) return false;
    const char *last_dot = strrchr(qn, '.');
    if (!last_dot) return false;
    size_t len = (size_t)(last_dot - qn);
    if (len + 1 > out_size) return false;
    memcpy(out, qn, len);
    out[len] = '\0';
    return true;
}

static bool gdscript_script_anchor_qn(const cbm_gbuf_node_t *source_node, char *out, size_t out_size) {
    if (!source_node || !source_node->qualified_name || !source_node->label) return false;
    if (strcmp(source_node->label, "Class") == 0) {
        snprintf(out, out_size, "%s", source_node->qualified_name);
        return true;
    }
    if (strcmp(source_node->label, "Method") == 0) {
        return strip_last_qn_segment(source_node->qualified_name, out, out_size);
    }
    return false;
}

static bool gdscript_signal_target_qn(const CBMCall *call, const char *anchor_qn, char *out, size_t out_size) {
    if (!call || !call->callee_name || !anchor_qn || !call->first_string_arg || !call->first_string_arg[0])
        return false;
    if (strcmp(call->callee_name, "emit_signal") == 0) {
        snprintf(out, out_size, "%s.signal.%s", anchor_qn, call->first_string_arg);
        return true;
    }
    size_t callee_len = strlen(call->callee_name);
    size_t suffix_len = gdscript_signal_member_suffix_len(call->callee_name);
    if (suffix_len == 0) return false;
    size_t receiver_len = callee_len - suffix_len;
    if (receiver_len == 0) return false;
    const char *last_dot = call->callee_name + receiver_len;
    const char *signal_start = call->callee_name;
    for (const char *p = last_dot; p > call->callee_name; --p) {
        if (*(p - 1) == '.') { signal_start = p; break; }
    }
    size_t signal_len = (size_t)(last_dot - signal_start);
    if (signal_len == 0 || strncmp(signal_start, call->first_string_arg, signal_len) != 0 ||
        call->first_string_arg[signal_len] != '\0') return false;
    if (!(strncmp(call->callee_name, "self.", 5) == 0 ||
          memchr(call->callee_name, '.', receiver_len) == NULL)) return false;
    snprintf(out, out_size, "%s.signal.%s", anchor_qn, call->first_string_arg);
    return true;
}

static bool gdscript_same_script_target_qn(const CBMCall *call, const char *anchor_qn, char *out, size_t out_size) {
    if (!call || !call->callee_name || !anchor_qn) return false;
    if (gdscript_signal_target_qn(call, anchor_qn, out, out_size)) return true;
    if (strchr(call->callee_name, '.') != NULL) return false;
    snprintf(out, out_size, "%s.%s", anchor_qn, call->callee_name);
    return true;
}

static bool gdscript_anchor_matches_module(const char *class_qn, const char *module_qn) {
    if (!class_qn || !module_qn) return false;
    char script_qn[512];
    snprintf(script_qn, sizeof(script_qn), "%s.__script__", module_qn);
    if (strcmp(class_qn, script_qn) == 0) return true;
    size_t prefix_len = strlen(module_qn);
    if (strncmp(class_qn, module_qn, prefix_len) != 0 || class_qn[prefix_len] != '.') return false;
    return strchr(class_qn + prefix_len + 1, '.') == NULL;
}

static bool gdscript_should_skip_generic_fallback(const CBMCall *call) {
    if (!call || !call->callee_name) return false;
    return gdscript_signal_member_suffix_len(call->callee_name) > 0;
}

static const char *find_gdscript_script_anchor(const cbm_gbuf_t *gbuf, const char *file_path,
                                                const char *module_qn) {
    const cbm_gbuf_node_t **classes = NULL;
    int class_count = 0;
    if (cbm_gbuf_find_by_label(gbuf, "Class", &classes, &class_count) != 0) return NULL;
    for (int i = 0; i < class_count; i++) {
        const cbm_gbuf_node_t *cls = classes[i];
        if (!cls->file_path || !cls->qualified_name) continue;
        /* Match: exact path OR file_path ends with the relative path */
        bool path_match = strcmp(cls->file_path, file_path) == 0;
        if (!path_match) {
            size_t fp_len = strlen(cls->file_path);
            size_t rel_len = strlen(file_path);
            if (fp_len > rel_len && cls->file_path[fp_len - rel_len - 1] == '/') {
                path_match = strcmp(cls->file_path + fp_len - rel_len, file_path) == 0;
            }
        }
        if (!path_match) continue;
        if (gdscript_anchor_matches_module(cls->qualified_name, module_qn)) {
            return cls->qualified_name;
        }
    }
    return NULL;
}

static bool is_classish_label(const char *label) {
    return label && (strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                     strcmp(label, "Type") == 0 || strcmp(label, "Enum") == 0);
}

static bool gdscript_extract_module_path(const char *target_name, char *out, size_t out_size) {
    if (!target_name || !out || out_size == 0) return false;
    const char *start = strstr(target_name, "\"");
    if (!start) return false;
    start++;
    const char *end = strstr(start, ".gd\"");
    if (!end) return false;
    end += 3;
    const char *path_start = start;
    if (strncmp(path_start, "res://", 6) == 0) path_start += 6;
    size_t len = (size_t)(end - path_start);
    if (len + 1 > out_size) return false;
    memcpy(out, path_start, len);
    out[len] = '\0';
    return true;
}

static bool gdscript_preload_extends_base_from_source(const char *source, char *out,
                                                       size_t out_size) {
    if (!source || !out || out_size == 0) return false;
    for (const char *line = source; line && *line; ) {
        const char *next = strchr(line, '\n');
        const char *scan = line;
        while (*scan == ' ' || *scan == '\t') scan++;
        if (strncmp(scan, "func ", 5) == 0 || strncmp(scan, "class ", 6) == 0) return false;
        if (strncmp(scan, "extends preload(\"", 17) == 0) {
            const char *start = scan + 17;
            const char *end = strstr(start, ".gd\"");
            if (!end || (next && end > next)) return false;
            end += 3;
            if (strncmp(start, "res://", 6) == 0) start += 6;
            size_t len = (size_t)(end - start);
            if (len == 0 || len + 1 > out_size) return false;
            memcpy(out, start, len);
            out[len] = '\0';
            return true;
        }
        line = next ? next + 1 : NULL;
    }
    return false;
}

static const char *promote_gdscript_target_to_class(const cbm_registry_t *registry,
                                                     const cbm_gbuf_t *gbuf,
                                                     const char *project_name,
                                                     const char *resolved_qn,
                                                     const char *raw_target_name) {
    const char *file_path = NULL;
    char *module_qn_owned = NULL;
    char extracted_path[512];

    if (resolved_qn) {
        const char *label = cbm_registry_label_of(registry, resolved_qn);
        if (is_classish_label(label)) return resolved_qn;
        const cbm_gbuf_node_t *resolved_node = cbm_gbuf_find_by_qn(gbuf, resolved_qn);
        if (resolved_node && resolved_node->file_path &&
            str_ends_with(resolved_node->file_path, ".gd")) {
            file_path = resolved_node->file_path;
        }
    }
    if (!file_path && raw_target_name && str_ends_with(raw_target_name, ".gd")) {
        file_path = raw_target_name;
    }
    if (!file_path && raw_target_name &&
        gdscript_extract_module_path(raw_target_name, extracted_path, sizeof(extracted_path))) {
        file_path = extracted_path;
    }
    if (!file_path) return NULL;

    module_qn_owned = cbm_pipeline_fqn_module(project_name, file_path);
    const char *anchor_qn = find_gdscript_script_anchor(gbuf, file_path, module_qn_owned);
    free(module_qn_owned);
    return anchor_qn;
}

static bool gdscript_receiver_var_name(const CBMCall *call, char *out, size_t out_size) {
    if (!call || !call->callee_name || !out || out_size == 0) return false;
    size_t callee_len = strlen(call->callee_name);
    size_t suffix_len = 0;
    if (str_ends_with(call->callee_name, ".emit")) suffix_len = 5;
    else if (str_ends_with(call->callee_name, ".connect")) suffix_len = 8;
    else return false;
    size_t receiver_len = callee_len - suffix_len;
    const char *first_dot = memchr(call->callee_name, '.', receiver_len);
    if (!first_dot) return false;
    size_t root_len = (size_t)(first_dot - call->callee_name);
    if (root_len == 0 || root_len + 1 > out_size) return false;
    memcpy(out, call->callee_name, root_len);
    out[root_len] = '\0';
    return strcmp(out, "self") != 0;
}

static const char *gdscript_type_assign_for_receiver(const CBMFileResult *result,
                                                      const CBMCall *call,
                                                      const char *receiver_name) {
    if (!result || !call || !receiver_name || !receiver_name[0]) return NULL;
    const char *match = NULL;
    for (int i = 0; i < result->type_assigns.count; i++) {
        const CBMTypeAssign *ta = &result->type_assigns.items[i];
        if (!ta->var_name || !ta->type_name || !ta->enclosing_func_qn) continue;
        if (call->enclosing_func_qn && strcmp(ta->enclosing_func_qn, call->enclosing_func_qn) != 0)
            continue;
        if (strcmp(ta->var_name, receiver_name) == 0) match = ta->type_name;
    }
    return match;
}

static bool gdscript_normalize_type_name(const char *type_name, char *out, size_t out_size) {
    if (!type_name || !type_name[0] || !out || out_size == 0) return false;
    size_t len = strlen(type_name);
    if (len > 4 && strcmp(type_name + len - 4, ".new") == 0) len -= 4;
    if (len + 1 > out_size) return false;
    memcpy(out, type_name, len);
    out[len] = '\0';
    return true;
}

static bool gdscript_receiver_signal_target_qn(const cbm_registry_t *registry,
                                                const cbm_gbuf_t *gbuf,
                                                const char *project_name,
                                                const CBMFileResult *result,
                                                const CBMCall *call,
                                                const char *module_qn,
                                                const char **imp_keys,
                                                const char **imp_vals,
                                                int imp_count,
                                                char *out, size_t out_size) {
    if (!call || !call->callee_name || !call->first_string_arg || !call->first_string_arg[0])
        return false;
    size_t suffix_len = gdscript_signal_member_suffix_len(call->callee_name);
    if (suffix_len == 0) return false;

    char receiver_name[128];
    if (!gdscript_receiver_var_name(call, receiver_name, sizeof(receiver_name))) return false;

    const char *type_name = gdscript_type_assign_for_receiver(result, call, receiver_name);
    if (!type_name) return false;

    char normalized_type[256];
    if (!gdscript_normalize_type_name(type_name, normalized_type, sizeof(normalized_type)))
        return false;

    const char *import_target = lookup_import_map_value(normalized_type, imp_keys, imp_vals, imp_count);
    const char *target_anchor = NULL;
    if (import_target) {
        target_anchor = promote_gdscript_target_to_class(registry, gbuf, project_name,
                                                          import_target, import_target);
    }
    if (!target_anchor) {
        cbm_resolution_t target_res = cbm_registry_resolve(registry, normalized_type, module_qn,
                                                            imp_keys, imp_vals, imp_count);
        if (target_res.qualified_name && target_res.qualified_name[0]) {
            target_anchor = promote_gdscript_target_to_class(registry, gbuf, project_name,
                                                              target_res.qualified_name, NULL);
        }
    }
    if (!target_anchor) return false;

    snprintf(out, out_size, "%s.signal.%s", target_anchor, call->first_string_arg);
    return true;
}

static const char *resolve_as_class(const cbm_registry_t *reg, const char *name,
                                    const char *module_qn, const char **imp_keys,
                                    const char **imp_vals, int imp_count) {
    cbm_resolution_t res =
        cbm_registry_resolve(reg, name, module_qn, imp_keys, imp_vals, imp_count);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return NULL;
    }
    const char *label = cbm_registry_label_of(reg, res.qualified_name);
    if (!label) {
        return NULL;
    }
    if (strcmp(label, "Class") != 0 && strcmp(label, "Interface") != 0 &&
        strcmp(label, "Type") != 0 && strcmp(label, "Enum") != 0) {
        return NULL;
    }
    return res.qualified_name;
}

static void extract_decorator_func(const char *dec, char *out, size_t outsz) {
    out[0] = '\0';
    if (!dec) {
        return;
    }
    const char *start = dec;
    if (*start == '@') {
        start++;
    }
    const char *paren = strchr(start, '(');
    size_t len = paren ? (size_t)(paren - start) : strlen(start);
    if (len == 0 || len >= outsz) {
        return;
    }
    memcpy(out, start, len);
    out[len] = '\0';
}

/* ── File sort for tail-latency reduction ────────────────────────── */

typedef struct {
    int idx;
    // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
    int64_t size;
} file_sort_entry_t;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int compare_by_size_desc(const void *a, const void *b) {
    const file_sort_entry_t *fa = a;
    const file_sort_entry_t *fb = b;
    if (fb->size > fa->size) {
        return 1;
    }
    if (fb->size < fa->size) {
        return -1;
    }
    return 0;
}

/* ── Phase 3A: Parallel Extract ──────────────────────────────────── */

#define CBM_CACHE_LINE 128

typedef struct __attribute__((aligned(CBM_CACHE_LINE))) {
    cbm_gbuf_t *local_gbuf;
    int nodes_created;
    int errors;
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (2 * sizeof(int))];
} extract_worker_state_t;

typedef struct {
    // NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
    const cbm_file_info_t *files;
    file_sort_entry_t *sorted;
    int file_count;
    const char *project_name;
    const char *repo_path;

    extract_worker_state_t *workers;
    int max_workers;
    _Atomic int next_worker_id;

    CBMFileResult **result_cache;
    _Atomic int64_t *shared_ids;
    _Atomic int *cancelled;
    _Atomic int next_file_idx;
} extract_ctx_t;

static void extract_worker(int worker_id, void *ctx_ptr) {
    extract_ctx_t *ec = ctx_ptr;
    extract_worker_state_t *ws = &ec->workers[worker_id];

    /* Lazy gbuf creation */
    if (!ws->local_gbuf) {
        ws->local_gbuf = cbm_gbuf_new_shared_ids(ec->project_name, ec->repo_path, ec->shared_ids);
    }

    /* Pull files from shared atomic counter */
    while (1) {
        int sort_pos = atomic_fetch_add_explicit(&ec->next_file_idx, 1, memory_order_relaxed);
        if (sort_pos >= ec->file_count) {
            break;
        }
        if (atomic_load_explicit(ec->cancelled, memory_order_relaxed)) {
            break;
        }

        int file_idx = ec->sorted[sort_pos].idx;
        const cbm_file_info_t *fi = &ec->files[file_idx];

        /* Read + extract */
        int source_len = 0;
        char *source = read_file(fi->path, &source_len);
        if (!source) {
            ws->errors++;
            continue;
        }

        /* Per-file start log: shows which file each worker is processing.
         * Critical for diagnosing stuck workers on large vendored files. */
        if (sort_pos < 24) { /* first 2 rounds of workers = most interesting */
            cbm_log_info("parallel.extract.file.start", "pos", itoa_log(sort_pos), "size_kb",
                         itoa_log(source_len / 1024), "path", fi->rel_path);
        }

        uint64_t file_t0 = extract_now_ns();

        CBMFileResult *result = cbm_extract_file(source, source_len, fi->language, ec->project_name,
                                                 fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL);

        uint64_t file_elapsed_ms = (extract_now_ns() - file_t0) / 1000000ULL;

        if (!result) {
            if (sort_pos < 24) {
                cbm_log_warn("parallel.extract.file.fail", "pos", itoa_log(sort_pos), "elapsed_ms",
                             itoa_log((int)file_elapsed_ms), "path", fi->rel_path);
            }
            free_source(source);
            ws->errors++;
            continue;
        }

        /* Per-file completion log for large/slow files */
        if (sort_pos < 24 || file_elapsed_ms > 1000) {
            cbm_log_info("parallel.extract.file.done", "pos", itoa_log(sort_pos), "elapsed_ms",
                         itoa_log((int)file_elapsed_ms), "defs", itoa_log(result->defs.count),
                         "path", fi->rel_path);
        }

        /* Create definition nodes in local gbuf */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name || !def->name) {
                continue;
            }

            char props[2048];
            build_def_props(props, sizeof(props), def);

            cbm_gbuf_upsert_node(ws->local_gbuf, def->label ? def->label : "Function", def->name,
                                 def->qualified_name,
                                 def->file_path ? def->file_path : fi->rel_path,
                                 (int)def->start_line, (int)def->end_line, props);
            ws->nodes_created++;
        }

        /* Free TSTree immediately — arena strings survive for registry+resolve.
         * This makes slab reset safe: tree-sitter's internal nodes (in slab)
         * are released before the slab is bulk-reclaimed. */
        cbm_free_tree(result);

        /* Free source buffer — extraction captured everything needed. */
        free_source(source);

        /* Cache result (arena + extracted data, no tree) for Phase 3B and Phase 4 */
        ec->result_cache[file_idx] = result;

        /* Progress logging: log every 10 files (atomic read, no contention) */
        if ((sort_pos + 1) % 10 == 0 || sort_pos + 1 == ec->file_count) {
            cbm_log_info("parallel.extract.progress", "done", itoa_log(sort_pos + 1), "total",
                         itoa_log(ec->file_count));
        }

        /* Reclaim all slab + tier2 memory between files.
         *
         * After cbm_free_tree(result), all tree nodes are on free lists.
         * We then destroy the parser (frees its internal allocations too),
         * leaving ZERO live slab/tier2 pointers. At that point, we can
         * safely munmap/free every page, bounding peak memory per-file
         * instead of accumulating across all 644 files.
         *
         * get_thread_parser() in cbm_extract_file will create a fresh
         * parser for the next file — cost is microseconds vs seconds
         * for parsing. This prevents unbounded memory accumulation and works
         * identically on macOS, Linux, and Windows. */
        cbm_destroy_thread_parser();
        cbm_slab_reclaim();
        cbm_mem_collect();
    }

    /* Final cleanup (parser already destroyed in loop, just slab state) */
    cbm_slab_destroy_thread();
}

int cbm_parallel_extract(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count) {
    if (file_count == 0) {
        return 0;
    }

    cbm_log_info("parallel.extract.start", "files", itoa_log(file_count), "workers",
                 itoa_log(worker_count));

    /* Log per-worker memory budget */
    if (cbm_mem_budget() > 0) {
        size_t worker_budget = cbm_mem_worker_budget(worker_count);
        cbm_log_info("parallel.mem.budget", "total_mb",
                     itoa_log((int)(cbm_mem_budget() / (1024 * 1024))), "per_worker_mb",
                     itoa_log((int)(worker_budget / (1024 * 1024))));
    }

    /* Ensure extraction library is initialized */
    cbm_init();

    /* Slab allocator for tree-sitter (thread-safe via TLS).
     * Safe: extract_worker frees TSTree via cbm_free_tree() before
     * cbm_slab_reset_thread(), so no live tree pointers cross slab boundaries. */
    cbm_slab_install();

    /* Sort files by descending size for tail-latency reduction */
    file_sort_entry_t *sorted = malloc(file_count * sizeof(file_sort_entry_t));
    for (int i = 0; i < file_count; i++) {
        sorted[i].idx = i;
        sorted[i].size = files[i].size;
    }
    qsort(sorted, file_count, sizeof(file_sort_entry_t), compare_by_size_desc);

    /* Allocate per-worker state (cache-line aligned via posix_memalign) */
    extract_worker_state_t *workers = NULL;
    if (cbm_aligned_alloc((void **)&workers, CBM_CACHE_LINE,
                          (size_t)worker_count * sizeof(extract_worker_state_t)) != 0) {
        free(sorted);
        return -1;
    }
    memset(workers, 0, (size_t)worker_count * sizeof(extract_worker_state_t));

    extract_ctx_t ec = {
        .files = files,
        .sorted = sorted,
        .file_count = file_count,
        .project_name = ctx->project_name,
        .repo_path = ctx->repo_path,
        .workers = workers,
        .max_workers = worker_count,
        .result_cache = result_cache,
        .shared_ids = shared_ids,
        .cancelled = ctx->cancelled,
    };
    atomic_init(&ec.next_worker_id, 0);
    atomic_init(&ec.next_file_idx, 0);

    /* Dispatch workers */
    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, extract_worker, &ec, opts);

    /* Merge all local gbufs into main gbuf */
    int total_nodes = 0;
    int total_errors = 0;
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].local_gbuf) {
            cbm_gbuf_merge(ctx->gbuf, workers[i].local_gbuf);
            total_nodes += workers[i].nodes_created;
            total_errors += workers[i].errors;
            cbm_gbuf_free(workers[i].local_gbuf);
        }
    }

    cbm_aligned_free(workers);
    free(sorted);

    if (atomic_load(ctx->cancelled)) {
        return -1;
    }

    /* RSS-based memory stats after extraction */
    if (cbm_mem_budget() > 0) {
        size_t rss_mb = cbm_mem_rss() / (1024 * 1024);
        size_t peak_mb = cbm_mem_peak_rss() / (1024 * 1024);
        size_t budget_mb = cbm_mem_budget() / (1024 * 1024);
        size_t worker_mb = cbm_mem_worker_budget(worker_count) / (1024 * 1024);
        cbm_log_info("parallel.extract.mem", "rss_mb", itoa_log((int)rss_mb), "peak_mb",
                     itoa_log((int)peak_mb), "budget_mb", itoa_log((int)budget_mb), "per_worker_mb",
                     itoa_log((int)worker_mb));
    }

    cbm_log_info("parallel.extract.done", "nodes", itoa_log(total_nodes), "errors",
                 itoa_log(total_errors));
    return 0;
}

/* ── Phase 3B: Serial Registry Build ─────────────────────────────── */

int cbm_build_registry_from_cache(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count, CBMFileResult **result_cache) {
    cbm_log_info("parallel.registry.start", "files", itoa_log(file_count));

    int reg_entries = 0;
    int defines_edges = 0;
    int imports_edges = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return -1;
        }

        CBMFileResult *result = result_cache[i];
        if (!result) {
            continue;
        }

        const char *rel = files[i].rel_path;

        /* Register callable symbols */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->name || !def->qualified_name || !def->label) {
                continue;
            }

            if (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
                strcmp(def->label, "Class") == 0) {
                cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
                reg_entries++;
            }

            /* DEFINES edge: File → Definition */
            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
            const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
            const cbm_gbuf_node_t *def_node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
            if (file_node && def_node) {
                cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, def_node->id, "DEFINES", "{}");
                defines_edges++;
            }
            free(file_qn);

            /* DEFINES_METHOD edge: Class → Method */
            if (def->parent_class && strcmp(def->label, "Method") == 0) {
                const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
                if (parent && def_node) {
                    cbm_gbuf_insert_edge(ctx->gbuf, parent->id, def_node->id, "DEFINES_METHOD",
                                         "{}");
                }
            }
        }

        /* IMPORTS edges */
        for (int j = 0; j < result->imports.count; j++) {
            CBMImport *imp = &result->imports.items[j];
            if (!imp->module_path) {
                continue;
            }

            char *target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);

            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
            const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);

            if (source_node && target) {
                char imp_props[256];
                snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}",
                         imp->local_name ? imp->local_name : "");
                cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
                imports_edges++;
            }
            free(target_qn);
            free(file_qn);
        }
    }

    cbm_log_info("parallel.registry.done", "entries", itoa_log(reg_entries), "defines",
                 itoa_log(defines_edges), "imports", itoa_log(imports_edges));
    return 0;
}

/* ── Phase 4: Parallel Resolution ────────────────────────────────── */

typedef struct __attribute__((aligned(CBM_CACHE_LINE))) {
    cbm_gbuf_t *local_edge_buf;
    int calls_resolved;
    int usages_resolved;
    int semantic_resolved;
    int errors;
    char _pad[CBM_CACHE_LINE - sizeof(cbm_gbuf_t *) - (4 * sizeof(int))];
} resolve_worker_state_t;

typedef struct {
    const cbm_file_info_t *files;
    int file_count;
    const char *project_name;
    const char *repo_path;

    resolve_worker_state_t *workers;
    int max_workers;

    CBMFileResult **result_cache;
    const cbm_gbuf_t *main_gbuf;    /* READ-ONLY during Phase 4 */
    const cbm_registry_t *registry; /* READ-ONLY during Phase 4 */
    _Atomic int64_t *shared_ids;
    _Atomic int *cancelled;
    _Atomic int next_file_idx;
} resolve_ctx_t;

/* Minimum buffer space needed per arg JSON object */
#define CBM_ARG_JSON_GUARD 32

/* Append arg data as JSON to edge properties: ,"args":[{"i":0,"e":"x","v":"val"},...]
 * Returns new position in buffer. */
static size_t append_args_json(char *buf, size_t bufsize, size_t pos, const CBMCall *call) {
    if (call->arg_count == 0 || pos >= bufsize - 20) {
        return pos;
    }
    int n = snprintf(buf + pos, bufsize - pos, ",\"args\":[");
    if (n <= 0) {
        return pos;
    }
    pos += (size_t)n;
    for (int i = 0; i < call->arg_count && pos < bufsize - CBM_ARG_JSON_GUARD; i++) {
        const CBMCallArg *a = &call->args[i];
        if (i > 0 && pos < bufsize - 1) {
            buf[pos++] = ',';
        }
        /* Truncate long expressions to keep edge properties compact */
        char expr_buf[128];
        if (a->expr) {
            snprintf(expr_buf, sizeof(expr_buf), "%.*s", 120, a->expr);
            /* Escape quotes for JSON safety */
            for (char *p = expr_buf; *p; p++) {
                if (*p == '"') {
                    *p = '\'';
                }
                if (*p == '\n' || *p == '\r') {
                    *p = ' ';
                }
            }
        } else {
            expr_buf[0] = '\0';
        }
        if (a->keyword && a->value) {
            n = snprintf(buf + pos, bufsize - pos,
                         "{\"i\":%d,\"k\":\"%s\",\"e\":\"%s\",\"v\":\"%s\"}", a->index, a->keyword,
                         expr_buf, a->value);
        } else if (a->keyword) {
            n = snprintf(buf + pos, bufsize - pos, "{\"i\":%d,\"k\":\"%s\",\"e\":\"%s\"}", a->index,
                         a->keyword, expr_buf);
        } else if (a->value) {
            n = snprintf(buf + pos, bufsize - pos, "{\"i\":%d,\"e\":\"%s\",\"v\":\"%s\"}", a->index,
                         expr_buf, a->value);
        } else {
            n = snprintf(buf + pos, bufsize - pos, "{\"i\":%d,\"e\":\"%s\"}", a->index, expr_buf);
        }
        if (n > 0) {
            pos += (size_t)n;
        }
    }
    if (pos < bufsize - 1) {
        buf[pos++] = ']';
    }
    buf[pos] = '\0';
    return pos;
}

/* Classify a resolved call by library identity and emit the appropriate edge.
 * Extracted from resolve_worker to keep cognitive complexity under threshold. */
static void emit_service_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *source,
                              const cbm_gbuf_node_t *target, const CBMCall *call,
                              const cbm_resolution_t *res, const char *module_qn,
                              const cbm_registry_t *registry, const cbm_gbuf_t *main_gbuf,
                              const char **imp_keys, const char **imp_vals, int imp_count) {
    cbm_svc_kind_t svc = cbm_service_pattern_match(res->qualified_name);
    const char *arg = call->first_string_arg;

    /* Also detect route registration by callee name suffix alone (handles unresolved
     * local variables like app.include_router where QN resolution fails). */
    if (svc == CBM_SVC_NONE && cbm_service_pattern_route_method(call->callee_name) != NULL) {
        svc = CBM_SVC_ROUTE_REG;
    }

    if (svc == CBM_SVC_ROUTE_REG) {
        /* Generalized route registration: find URL path from ANY argument.
         * Handles: router.GET("/path", handler), app.include_router(r, prefix="/path"),
         * app.add_url_rule("/path", view_func=h), app.mount("/path", sub_app), etc.
         * Language-agnostic: any call to a router framework with a path arg. */
        const char *route_path = NULL;
        const char *handler_ref = NULL;

        /* 1. Check first_string_arg (covers router.GET("/path", handler)) */
        if (arg != NULL && arg[0] == '/') {
            route_path = arg;
            handler_ref = call->second_arg_name;
        }

        /* 2. Scan keyword args for path-like values (prefix=, path=, route=, pattern=) */
        if (!route_path) {
            static const char *path_keywords[] = {"prefix",     "path",     "route", "pattern",
                                                  "url",        "endpoint", "rule",  "mount_path",
                                                  "route_path", "url_path", NULL};
            for (int ai = 0; ai < call->arg_count && !route_path; ai++) {
                const CBMCallArg *ca = &call->args[ai];
                const char *val = ca->value ? ca->value : ca->expr;
                if (!val || val[0] != '/') {
                    continue;
                }
                if (ca->keyword) {
                    for (const char **kw = path_keywords; *kw; kw++) {
                        if (strcmp(ca->keyword, *kw) == 0) {
                            route_path = val;
                            break;
                        }
                    }
                } else if (ca->index == 0) {
                    /* First positional arg starting with / is likely a path */
                    route_path = val;
                }
            }
        }

        /* 3. Find handler reference from args (first identifier/attribute arg) */
        if (!handler_ref) {
            for (int ai = 0; ai < call->arg_count && !handler_ref; ai++) {
                const CBMCallArg *ca = &call->args[ai];
                if (!ca->expr || ca->expr[0] == '/' || ca->expr[0] == '"' || ca->expr[0] == '\'') {
                    continue;
                }
                /* Skip known non-handler keywords */
                if (ca->keyword &&
                    (strcmp(ca->keyword, "prefix") == 0 || strcmp(ca->keyword, "name") == 0 ||
                     strcmp(ca->keyword, "tags") == 0)) {
                    continue;
                }
                handler_ref = ca->expr;
            }
        }

        if (route_path) {
            const char *method = cbm_service_pattern_route_method(call->callee_name);
            char route_qn[CBM_ROUTE_QN_SIZE];
            snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method ? method : "ANY",
                     route_path);
            char route_props[256];
            snprintf(route_props, sizeof(route_props), "{\"method\":\"%s\"}",
                     method ? method : "ANY");
            int64_t route_id =
                cbm_gbuf_upsert_node(gbuf, "Route", route_path, route_qn, "", 0, 0, route_props);
            char props[512];
            snprintf(props, sizeof(props),
                     "{\"callee\":\"%s\",\"url_path\":\"%s\",\"via\":\"route_registration\"}",
                     call->callee_name, route_path);
            cbm_gbuf_insert_edge(gbuf, source->id, route_id, "CALLS", props);

            /* Resolve handler and create HANDLES edge */
            if (handler_ref && handler_ref[0]) {
                cbm_resolution_t hres = cbm_registry_resolve(registry, handler_ref, module_qn,
                                                             imp_keys, imp_vals, imp_count);
                if (hres.qualified_name != NULL && hres.qualified_name[0] != '\0') {
                    const cbm_gbuf_node_t *handler =
                        cbm_gbuf_find_by_qn(main_gbuf, hres.qualified_name);
                    if (handler != NULL) {
                        char hprops[256];
                        snprintf(hprops, sizeof(hprops), "{\"handler\":\"%s\"}",
                                 hres.qualified_name);
                        cbm_gbuf_insert_edge(gbuf, handler->id, route_id, "HANDLES", hprops);
                    }
                }
            }
            return;
        }
        /* No path found — fall through to normal CALLS edge */
    }

    int has_url = (arg != NULL && arg[0] != '\0' && (arg[0] == '/' || strstr(arg, "://") != NULL));
    int has_topic = (arg != NULL && arg[0] != '\0' && svc == CBM_SVC_ASYNC && strlen(arg) > 2);

    if ((svc == CBM_SVC_HTTP || svc == CBM_SVC_ASYNC) && (has_url || has_topic)) {
        const char *edge_type = (svc == CBM_SVC_HTTP) ? "HTTP_CALLS" : "ASYNC_CALLS";
        const char *method =
            (svc == CBM_SVC_HTTP) ? cbm_service_pattern_http_method(call->callee_name) : NULL;
        const char *broker =
            (svc == CBM_SVC_ASYNC) ? cbm_service_pattern_broker(res->qualified_name) : NULL;

        char route_qn[CBM_ROUTE_QN_SIZE];
        if (svc == CBM_SVC_HTTP) {
            snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method ? method : "ANY", arg);
        } else {
            snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", broker ? broker : "async", arg);
        }

        char route_props[256];
        if (method != NULL) {
            snprintf(route_props, sizeof(route_props), "{\"method\":\"%s\"}", method);
        } else if (broker != NULL) {
            snprintf(route_props, sizeof(route_props), "{\"broker\":\"%s\"}", broker);
        } else {
            snprintf(route_props, sizeof(route_props), "{}");
        }
        int64_t route_id =
            cbm_gbuf_upsert_node(gbuf, "Route", arg, route_qn, "", 0, 0, route_props);

        char props[2048];
        int n = snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"url_path\":\"%s\"%s%s%s%s%s%s",
                         call->callee_name, arg, method ? ",\"method\":\"" : "",
                         method ? method : "", method ? "\"" : "", broker ? ",\"broker\":\"" : "",
                         broker ? broker : "", broker ? "\"" : "");
        if (n > 0 && (size_t)n < sizeof(props) - 2) {
            size_t pos = append_args_json(props, sizeof(props), (size_t)n, call);
            if (pos < sizeof(props) - 1) {
                props[pos] = '}';
                props[pos + 1] = '\0';
            }
        }
        cbm_gbuf_insert_edge(gbuf, source->id, route_id, edge_type, props);
    } else if (svc == CBM_SVC_CONFIG) {
        char props[2048];
        int n =
            snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"key\":\"%s\",\"confidence\":%.2f",
                     call->callee_name, arg != NULL ? arg : "", res->confidence);
        if (n > 0 && (size_t)n < sizeof(props) - 2) {
            size_t pos = append_args_json(props, sizeof(props), (size_t)n, call);
            if (pos < sizeof(props) - 1) {
                props[pos] = '}';
                props[pos + 1] = '\0';
            }
        }
        cbm_gbuf_insert_edge(gbuf, source->id, target->id, "CONFIGURES", props);
    } else {
        char props[2048];
        int n = snprintf(props, sizeof(props),
                         "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\","
                         "\"candidates\":%d",
                         call->callee_name, res->confidence,
                         res->strategy ? res->strategy : "unknown", res->candidate_count);
        if (n > 0 && (size_t)n < sizeof(props) - 2) {
            size_t pos = append_args_json(props, sizeof(props), (size_t)n, call);
            if (pos < sizeof(props) - 1) {
                props[pos] = '}';
                props[pos + 1] = '\0';
            }
        }
        cbm_gbuf_insert_edge(gbuf, source->id, target->id, "CALLS", props);
    }
}

static void resolve_worker(int worker_id, void *ctx_ptr) {
    resolve_ctx_t *rc = ctx_ptr;
    resolve_worker_state_t *ws = &rc->workers[worker_id];

    if (!ws->local_edge_buf) {
        ws->local_edge_buf =
            cbm_gbuf_new_shared_ids(rc->project_name, rc->repo_path, rc->shared_ids);
    }

    while (1) {
        int file_idx = atomic_fetch_add_explicit(&rc->next_file_idx, 1, memory_order_relaxed);
        if (file_idx >= rc->file_count) {
            break;
        }
        if (atomic_load_explicit(rc->cancelled, memory_order_relaxed)) {
            break;
        }

        CBMFileResult *result = rc->result_cache[file_idx];
        if (!result) {
            continue;
        }

        /* Skip files with nothing to resolve */
        if (result->calls.count == 0 && result->usages.count == 0 && result->throws.count == 0 &&
            result->rw.count == 0 && result->defs.count == 0 && result->impl_traits.count == 0) {
            continue;
        }

        const char *rel = rc->files[file_idx].rel_path;

        /* Build import map (read-only access to main_gbuf) */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(rc->main_gbuf, rc->project_name, rel, &imp_keys, &imp_vals, &imp_count);

        char *module_qn = cbm_pipeline_fqn_module(rc->project_name, rel);

        /* ── CALLS resolution ──────────────────────────────────── */
        for (int c = 0; c < result->calls.count; c++) {
            CBMCall *call = &result->calls.items[c];
            if (!call->callee_name) {
                continue;
            }

            const cbm_gbuf_node_t *source_node = NULL;
            if (call->enclosing_func_qn) {
                source_node = cbm_gbuf_find_by_qn(rc->main_gbuf, call->enclosing_func_qn);
            }
            if (!source_node) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                source_node = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!source_node) {
                continue;
            }

            /* GDScript signal/same-script resolution (mirrored from pass_calls.c) */
            cbm_resolution_t res = {0};
            if (rc->files[file_idx].language == CBM_LANG_GDSCRIPT) {
                char anchor_qn[512];
                char gdscript_target_qn[768];
                bool has_anchor_qn = gdscript_script_anchor_qn(source_node, anchor_qn, sizeof(anchor_qn));
                if (has_anchor_qn &&
                    gdscript_same_script_target_qn(call, anchor_qn, gdscript_target_qn,
                                                   sizeof(gdscript_target_qn))) {
                    res = cbm_registry_resolve(rc->registry, gdscript_target_qn, module_qn,
                                               imp_keys, imp_vals, imp_count);
                }
                if ((!res.qualified_name || res.qualified_name[0] == '\0') &&
                    gdscript_receiver_signal_target_qn(rc->registry, rc->main_gbuf,
                                                        rc->project_name, result, call,
                                                        module_qn, imp_keys, imp_vals, imp_count,
                                                        gdscript_target_qn, sizeof(gdscript_target_qn))) {
                    res = cbm_registry_resolve(rc->registry, gdscript_target_qn, module_qn,
                                               imp_keys, imp_vals, imp_count);
                }
                if ((!res.qualified_name || res.qualified_name[0] == '\0') &&
                    gdscript_should_skip_generic_fallback(call)) {
                    continue;
                }
            }
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                res = cbm_registry_resolve(rc->registry, call->callee_name, module_qn,
                                           imp_keys, imp_vals, imp_count);
            }
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                /* Unresolved call — still check for route registration by callee suffix.
                 * Handles patterns like app.include_router(r, prefix="/path") where
                 * "app" is a local variable that the resolver can't follow. */
                if (cbm_service_pattern_route_method(call->callee_name) != NULL) {
                    cbm_resolution_t fake_res = {.qualified_name = call->callee_name,
                                                 .confidence = 0.5,
                                                 .strategy = "callee_suffix"};
                    emit_service_edge(ws->local_edge_buf, source_node, source_node, call, &fake_res,
                                      module_qn, rc->registry, rc->main_gbuf, imp_keys, imp_vals,
                                      imp_count);
                }
                continue;
            }

            const cbm_gbuf_node_t *target_node =
                cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!target_node || source_node->id == target_node->id) {
                continue;
            }

            /* Classify and emit edge via helper (keeps resolve_worker complexity down) */
            emit_service_edge(ws->local_edge_buf, source_node, target_node, call, &res, module_qn,
                              rc->registry, rc->main_gbuf, imp_keys, imp_vals, imp_count);
            ws->calls_resolved++;
        }

        /* ── USAGE resolution ──────────────────────────────────── */
        for (int u = 0; u < result->usages.count; u++) {
            CBMUsage *usage = &result->usages.items[u];
            if (!usage->ref_name) {
                continue;
            }

            const cbm_gbuf_node_t *src = NULL;
            if (usage->enclosing_func_qn) {
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, usage->enclosing_func_qn);
            }
            if (!src) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(rc->registry, usage->ref_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            {
                char uprops[256];
                snprintf(uprops, sizeof(uprops), "{\"callee\":\"%s\"}", usage->ref_name);
                cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, "USAGE", uprops);
            }
            ws->usages_resolved++;
        }

        /* ── THROWS / RAISES ───────────────────────────────────── */
        for (int t = 0; t < result->throws.count; t++) {
            CBMThrow *thr = &result->throws.items[t];
            if (!thr->exception_name || !thr->enclosing_func_qn) {
                continue;
            }

            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(rc->main_gbuf, thr->enclosing_func_qn);
            if (!src) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *edge_type = is_checked_exception(thr->exception_name) ? "THROWS" : "RAISES";
            cbm_resolution_t res = cbm_registry_resolve(rc->registry, thr->exception_name,
                                                        module_qn, imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, edge_type, "{}");
        }

        /* ── READS / WRITES ────────────────────────────────────── */
        for (int r = 0; r < result->rw.count; r++) {
            CBMReadWrite *rw = &result->rw.items[r];
            if (!rw->var_name) {
                continue;
            }

            const cbm_gbuf_node_t *src = NULL;
            if (rw->enclosing_func_qn) {
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, rw->enclosing_func_qn);
            }
            if (!src) {
                char *file_qn = cbm_pipeline_fqn_compute(rc->project_name, rel, "__file__");
                src = cbm_gbuf_find_by_qn(rc->main_gbuf, file_qn);
                free(file_qn);
            }
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(rc->registry, rw->var_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *etype = rw->is_write ? "WRITES" : "READS";
            cbm_gbuf_insert_edge(ws->local_edge_buf, src->id, tgt->id, etype, "{}");
        }

        /* ── INHERITS + DECORATES ──────────────────────────────── */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name) {
                continue;
            }

            const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(rc->main_gbuf, def->qualified_name);
            if (!node) {
                continue;
            }

            /* INHERITS */
            if (def->base_classes) {
                for (int b = 0; def->base_classes[b]; b++) {
                    const char *base_qn = NULL;

                    /* GDScript: promote .gd targets to script-anchor class QNs */
                    if (rc->files[file_idx].language == CBM_LANG_GDSCRIPT) {
                        fprintf(stderr, "DEBUG INHERITS: file=%s base_classes[%d]=%s\n",
                                rel, b, def->base_classes[b]);
                        /* First try resolve_as_class for named class targets */
                        base_qn = resolve_as_class(rc->registry, def->base_classes[b], module_qn,
                                                   imp_keys, imp_vals, imp_count);
                        fprintf(stderr, "  resolve_as_class -> %s\n", base_qn ? base_qn : "(null)");
                        /* If that failed, try GDScript-specific promotion */
                        if (!base_qn) {
                            /* Check if base_classes[b] is a .gd path or alias */
                            const char *import_target = lookup_import_map_value(
                                def->base_classes[b], imp_keys, imp_vals, imp_count);
                            fprintf(stderr, "  import_target -> %s\n", import_target ? import_target : "(null)");
                            if (import_target && str_ends_with(import_target, ".gd")) {
                                base_qn = promote_gdscript_target_to_class(
                                    rc->registry, rc->main_gbuf, rc->project_name,
                                    import_target, import_target);
                                fprintf(stderr, "  promote(import_target) -> %s\n", base_qn ? base_qn : "(null)");
                            }
                            if (!base_qn && str_ends_with(def->base_classes[b], ".gd")) {
                                base_qn = promote_gdscript_target_to_class(
                                    rc->registry, rc->main_gbuf, rc->project_name,
                                    def->base_classes[b], def->base_classes[b]);
                                fprintf(stderr, "  promote(base_classes) -> %s\n", base_qn ? base_qn : "(null)");
                            }
                            /* Try resolving as a class name and then promoting if it's a .gd file */
                            if (!base_qn) {
                                cbm_resolution_t res = cbm_registry_resolve(
                                    rc->registry, def->base_classes[b], module_qn,
                                    imp_keys, imp_vals, imp_count);
                                fprintf(stderr, "  registry_resolve -> %s\n", res.qualified_name ? res.qualified_name : "(null)");
                                if (res.qualified_name && res.qualified_name[0]) {
                                    base_qn = promote_gdscript_target_to_class(
                                        rc->registry, rc->main_gbuf, rc->project_name,
                                        res.qualified_name, def->base_classes[b]);
                                    fprintf(stderr, "  promote(resolved) -> %s\n", base_qn ? base_qn : "(null)");
                                }
                            }
                        }
                    } else {
                        base_qn = resolve_as_class(rc->registry, def->base_classes[b], module_qn,
                                                   imp_keys, imp_vals, imp_count);
                    }
                    if (!base_qn) {
                        continue;
                    }
                    const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(rc->main_gbuf, base_qn);
                    if (!base_node || node->id == base_node->id) {
                        continue;
                    }
                    cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, base_node->id, "INHERITS",
                                         "{}");
                    ws->semantic_resolved++;
                }
            } else if (rc->files[file_idx].language == CBM_LANG_GDSCRIPT && node->label &&
                       strcmp(node->label, "Class") == 0 && node->qualified_name &&
                       gdscript_anchor_matches_module(node->qualified_name, module_qn)) {
                /* Fallback: read source file for extends preload("...") patterns
                 * that the extractor didn't capture as base_classes */
                char *fallback_source = NULL;
                int fallback_len = 0;
                const char *full_path = rc->files[file_idx].path;
                fallback_source = read_file(full_path, &fallback_len);
                if (fallback_source) {
                    char module_path[512];
                    if (gdscript_preload_extends_base_from_source(fallback_source, module_path,
                                                                   sizeof(module_path))) {
                        const char *base_qn = promote_gdscript_target_to_class(
                            rc->registry, rc->main_gbuf, rc->project_name, NULL, module_path);
                        if (base_qn) {
                            const cbm_gbuf_node_t *base_node =
                                cbm_gbuf_find_by_qn(rc->main_gbuf, base_qn);
                            if (base_node && node->id != base_node->id) {
                                cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, base_node->id,
                                                     "INHERITS", "{}");
                                ws->semantic_resolved++;
                            }
                        }
                    }
                    free(fallback_source);
                }
            }

            /* DECORATES */
            if (def->decorators) {
                for (int dc = 0; def->decorators[dc]; dc++) {
                    char func_name[256];
                    extract_decorator_func(def->decorators[dc], func_name, sizeof(func_name));
                    if (func_name[0] == '\0') {
                        continue;
                    }

                    cbm_resolution_t res = cbm_registry_resolve(rc->registry, func_name, module_qn,
                                                                imp_keys, imp_vals, imp_count);
                    if (!res.qualified_name || res.qualified_name[0] == '\0') {
                        continue;
                    }

                    const cbm_gbuf_node_t *dec_node =
                        cbm_gbuf_find_by_qn(rc->main_gbuf, res.qualified_name);
                    if (!dec_node || node->id == dec_node->id) {
                        continue;
                    }

                    char dprops[256];
                    snprintf(dprops, sizeof(dprops), "{\"decorator\":\"%s\"}", def->decorators[dc]);
                    cbm_gbuf_insert_edge(ws->local_edge_buf, node->id, dec_node->id, "DECORATES",
                                         dprops);
                    ws->semantic_resolved++;
                }
            }
        }

        /* ── IMPLEMENTS (Rust impl) ────────────────────────────── */
        for (int t = 0; t < result->impl_traits.count; t++) {
            CBMImplTrait *it = &result->impl_traits.items[t];
            if (!it->trait_name || !it->struct_name) {
                continue;
            }

            const char *trait_qn = resolve_as_class(rc->registry, it->trait_name, module_qn,
                                                    imp_keys, imp_vals, imp_count);
            if (!trait_qn) {
                continue;
            }
            const char *struct_qn = resolve_as_class(rc->registry, it->struct_name, module_qn,
                                                     imp_keys, imp_vals, imp_count);
            if (!struct_qn) {
                continue;
            }

            const cbm_gbuf_node_t *trait_node = cbm_gbuf_find_by_qn(rc->main_gbuf, trait_qn);
            const cbm_gbuf_node_t *struct_node = cbm_gbuf_find_by_qn(rc->main_gbuf, struct_qn);
            if (!trait_node || !struct_node || trait_node->id == struct_node->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ws->local_edge_buf, struct_node->id, trait_node->id, "IMPLEMENTS",
                                 "{}");
            ws->semantic_resolved++;
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
    }
}

int cbm_parallel_resolve(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count) {
    if (file_count == 0) {
        return 0;
    }

    cbm_log_info("parallel.resolve.start", "files", itoa_log(file_count), "workers",
                 itoa_log(worker_count));

    resolve_worker_state_t *workers = NULL;
    if (cbm_aligned_alloc((void **)&workers, CBM_CACHE_LINE,
                          (size_t)worker_count * sizeof(resolve_worker_state_t)) != 0) {
        return -1;
    }
    memset(workers, 0, (size_t)worker_count * sizeof(resolve_worker_state_t));

    resolve_ctx_t rc = {
        .files = files,
        .file_count = file_count,
        .project_name = ctx->project_name,
        .repo_path = ctx->repo_path,
        .workers = workers,
        .max_workers = worker_count,
        .result_cache = result_cache,
        .main_gbuf = ctx->gbuf,
        .registry = ctx->registry,
        .shared_ids = shared_ids,
        .cancelled = ctx->cancelled,
    };
    atomic_init(&rc.next_file_idx, 0);

    cbm_parallel_for_opts_t opts = {.max_workers = worker_count, .force_pthreads = false};
    cbm_parallel_for(worker_count, resolve_worker, &rc, opts);

    /* Merge all local edge bufs into main gbuf */
    int total_calls = 0;
    int total_usages = 0;
    int total_semantic = 0;
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].local_edge_buf) {
            cbm_gbuf_merge(ctx->gbuf, workers[i].local_edge_buf);
            total_calls += workers[i].calls_resolved;
            total_usages += workers[i].usages_resolved;
            total_semantic += workers[i].semantic_resolved;
            cbm_gbuf_free(workers[i].local_edge_buf);
        }
    }

    cbm_aligned_free(workers);

    /* Go-style implicit interface satisfaction (needs full graph, serial) */
    int go_impl = cbm_pipeline_implements_go(ctx);

    if (atomic_load(ctx->cancelled)) {
        return -1;
    }

    cbm_log_info("parallel.resolve.done", "calls", itoa_log(total_calls), "usages",
                 itoa_log(total_usages), "semantic", itoa_log(total_semantic + go_impl));
    return 0;
}
