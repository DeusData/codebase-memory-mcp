/*
 * pass_calls.c — Resolve function/method calls into CALLS edges.
 *
 * For each discovered file:
 *   1. Re-extract calls (cbm_extract_file)
 *   2. Build per-file import map from IMPORTS edges in graph buffer
 *   3. Resolve each call via registry (import_map → same_module → unique → suffix)
 *   4. Create CALLS edges in graph buffer with confidence/strategy properties
 *
 * Depends on: pass_definitions having populated the registry and graph buffer
 */
#include "foundation/constants.h"

enum { PC_RING = 4, PC_RING_MASK = 3, PC_SIG_SCAN = 15, PC_REGEX_GRP = 2 };
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/str_util.h"
#include "cbm.h"
#include "service_patterns.h"

#include "foundation/compat_regex.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read entire file into heap-allocated buffer. Caller must free(). */
static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 || size > (long)CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K) {
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc(size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, SKIP_ONE, size, f);
    (void)fclose(f);

    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Format int for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[PC_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PC_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Build per-file import map from cached extraction result or graph buffer edges.
 * Returns parallel arrays of (local_name, module_qn) pairs. Caller frees. */
/* Parse "local_name":"value" from JSON properties string. Returns strdup'd key or NULL. */
static char *extract_local_name_from_json(const char *props_json) {
    if (!props_json) {
        return NULL;
    }
    const char *start = strstr(props_json, "\"local_name\":\"");
    if (!start) {
        return NULL;
    }
    start += strlen("\"local_name\":\"");
    const char *end = strchr(start, '"');
    if (!end || end <= start) {
        return NULL;
    }
    return cbm_strndup(start, end - start);
}

static int build_import_map(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                            const CBMFileResult *result, const char ***out_keys,
                            const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    /* Fast path: build from cached extraction result (no JSON parsing) */
    if (result && result->imports.count > 0) {
        const char **keys = calloc((size_t)result->imports.count, sizeof(const char *));
        const char **vals = calloc((size_t)result->imports.count, sizeof(const char *));
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
            keys[count] = strdup(imp->local_name);
            vals[count] = target->qualified_name; /* borrowed from gbuf */
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

    const char **keys = calloc(edge_count, sizeof(const char *));
    const char **vals = calloc(edge_count, sizeof(const char *));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, e->target_id);
        if (!target) {
            continue;
        }
        char *key = extract_local_name_from_json(e->properties_json);
        if (key) {
            keys[count] = key;
            vals[count] = target->qualified_name;
            count++;
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

static bool str_ends_with(const char *text, const char *suffix) {
    if (!text || !suffix) {
        return false;
    }
    size_t text_len = strlen(text);
    size_t suffix_len = strlen(suffix);
    return text_len >= suffix_len && strcmp(text + text_len - suffix_len, suffix) == 0;
}

static size_t gdscript_signal_member_suffix_len(const char *callee_name) {
    if (!callee_name) {
        return 0;
    }
    if (str_ends_with(callee_name, ".emit")) {
        return 5;
    }
    if (str_ends_with(callee_name, ".connect")) {
        return 8;
    }
    return 0;
}

static bool strip_last_qn_segment(const char *qn, char *out, size_t out_size) {
    if (!qn || !out || out_size == 0) {
        return false;
    }
    const char *last_dot = strrchr(qn, '.');
    if (!last_dot) {
        return false;
    }
    size_t len = (size_t)(last_dot - qn);
    if (len + 1 > out_size) {
        return false;
    }
    memcpy(out, qn, len);
    out[len] = '\0';
    return true;
}

static bool gdscript_script_anchor_qn(const cbm_gbuf_node_t *source_node, char *out,
                                      size_t out_size) {
    if (!source_node || !source_node->qualified_name || !source_node->label) {
        return false;
    }
    if (strcmp(source_node->label, "Class") == 0) {
        snprintf(out, out_size, "%s", source_node->qualified_name);
        return true;
    }
    if (strcmp(source_node->label, "Method") == 0) {
        return strip_last_qn_segment(source_node->qualified_name, out, out_size);
    }
    return false;
}

static bool gdscript_signal_target_qn(const CBMCall *call, const char *anchor_qn, char *out,
                                       size_t out_size) {
    if (!call || !call->callee_name || !anchor_qn || !call->first_string_arg ||
        !call->first_string_arg[0]) {
        return false;
    }
    if (strcmp(call->callee_name, "emit_signal") == 0) {
        snprintf(out, out_size, "%s.signal.%s", anchor_qn, call->first_string_arg);
        return true;
    }

    size_t callee_len = strlen(call->callee_name);
    size_t suffix_len = gdscript_signal_member_suffix_len(call->callee_name);
    if (suffix_len == 0) {
        return false;
    }

    size_t receiver_len = callee_len - suffix_len;
    if (receiver_len == 0) {
        return false;
    }

    const char *last_dot = call->callee_name + receiver_len;
    const char *signal_start = call->callee_name;
    for (const char *p = last_dot; p > call->callee_name; --p) {
        if (*(p - 1) == '.') {
            signal_start = p;
            break;
        }
    }
    size_t signal_len = (size_t)(last_dot - signal_start);
    if (signal_len == 0 || strncmp(signal_start, call->first_string_arg, signal_len) != 0 ||
        call->first_string_arg[signal_len] != '\0') {
        return false;
    }

    if (!(strncmp(call->callee_name, "self.", 5) == 0 ||
          memchr(call->callee_name, '.', receiver_len) == NULL)) {
        return false;
    }

    snprintf(out, out_size, "%s.signal.%s", anchor_qn, call->first_string_arg);
    return true;
}

static bool gdscript_same_script_target_qn(const CBMCall *call, const char *anchor_qn, char *out,
                                           size_t out_size) {
    if (!call || !call->callee_name || !anchor_qn) {
        return false;
    }
    if (gdscript_signal_target_qn(call, anchor_qn, out, out_size)) {
        return true;
    }
    if (strchr(call->callee_name, '.') != NULL) {
        return false;
    }
    snprintf(out, out_size, "%s.%s", anchor_qn, call->callee_name);
    return true;
}

static bool is_classish_label(const char *label) {
    return label && (strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                     strcmp(label, "Type") == 0 || strcmp(label, "Enum") == 0);
}

static bool gdscript_anchor_matches_module(const char *class_qn, const char *module_qn) {
    if (!class_qn || !module_qn) {
        return false;
    }
    char script_qn[512];
    snprintf(script_qn, sizeof(script_qn), "%s.__script__", module_qn);
    if (strcmp(class_qn, script_qn) == 0) {
        return true;
    }
    size_t prefix_len = strlen(module_qn);
    if (strncmp(class_qn, module_qn, prefix_len) != 0 || class_qn[prefix_len] != '.') {
        return false;
    }
    return strchr(class_qn + prefix_len + 1, '.') == NULL;
}

static const char *find_gdscript_script_anchor(cbm_pipeline_ctx_t *ctx, const char *file_path,
                                               const char *module_qn) {
    const cbm_gbuf_node_t **classes = NULL;
    int class_count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &class_count) != 0) {
        return NULL;
    }
    for (int i = 0; i < class_count; i++) {
        const cbm_gbuf_node_t *cls = classes[i];
        if (!cls->file_path || !cls->qualified_name) {
            continue;
        }
        if (strcmp(cls->file_path, file_path) != 0) {
            continue;
        }
        if (gdscript_anchor_matches_module(cls->qualified_name, module_qn)) {
            return cls->qualified_name;
        }
    }
    return NULL;
}

static const char *promote_gdscript_target_to_class(cbm_pipeline_ctx_t *ctx,
                                                    const char *resolved_qn,
                                                    const char *raw_target_name) {
    const char *file_path = NULL;
    char *module_qn_owned = NULL;
    const char *module_qn = NULL;

    if (resolved_qn) {
        const char *label = cbm_registry_label_of(ctx->registry, resolved_qn);
        if (is_classish_label(label)) {
            return resolved_qn;
        }
        const cbm_gbuf_node_t *resolved_node = cbm_gbuf_find_by_qn(ctx->gbuf, resolved_qn);
        if (resolved_node && resolved_node->file_path &&
            str_ends_with(resolved_node->file_path, ".gd")) {
            file_path = resolved_node->file_path;
        }
    }

    if (!file_path && raw_target_name && str_ends_with(raw_target_name, ".gd")) {
        file_path = raw_target_name;
    }
    if (!file_path) {
        return NULL;
    }

    module_qn_owned = cbm_pipeline_fqn_module(ctx->project_name, file_path);
    module_qn = module_qn_owned;
    const char *anchor_qn = find_gdscript_script_anchor(ctx, file_path, module_qn);
    free(module_qn_owned);
    return anchor_qn;
}

static bool gdscript_receiver_var_name(const CBMCall *call, char *out, size_t out_size) {
    if (!call || !call->callee_name || !out || out_size == 0) {
        return false;
    }

    size_t callee_len = strlen(call->callee_name);
    size_t suffix_len = 0;
    if (str_ends_with(call->callee_name, ".emit")) {
        suffix_len = 5;
    } else if (str_ends_with(call->callee_name, ".connect")) {
        suffix_len = 8;
    } else {
        return false;
    }

    size_t receiver_len = callee_len - suffix_len;
    const char *first_dot = memchr(call->callee_name, '.', receiver_len);
    if (!first_dot) {
        return false;
    }

    size_t root_len = (size_t)(first_dot - call->callee_name);
    if (root_len == 0 || root_len + 1 > out_size) {
        return false;
    }
    memcpy(out, call->callee_name, root_len);
    out[root_len] = '\0';
    return strcmp(out, "self") != 0;
}

static const char *gdscript_type_assign_for_receiver(const CBMFileResult *result,
                                                     const CBMCall *call,
                                                     const char *receiver_name) {
    if (!result || !call || !receiver_name || !receiver_name[0]) {
        return NULL;
    }
    const char *match = NULL;
    for (int i = 0; i < result->type_assigns.count; i++) {
        const CBMTypeAssign *ta = &result->type_assigns.items[i];
        if (!ta->var_name || !ta->type_name || !ta->enclosing_func_qn) {
            continue;
        }
        if (call->enclosing_func_qn && strcmp(ta->enclosing_func_qn, call->enclosing_func_qn) != 0) {
            continue;
        }
        if (strcmp(ta->var_name, receiver_name) == 0) {
            match = ta->type_name;
        }
    }
    return match;
}

static bool gdscript_normalize_type_name(const char *type_name, char *out, size_t out_size) {
    if (!type_name || !type_name[0] || !out || out_size == 0) {
        return false;
    }
    size_t len = strlen(type_name);
    if (len > 4 && strcmp(type_name + len - 4, ".new") == 0) {
        len -= 4;
    }
    if (len + 1 > out_size) {
        return false;
    }
    memcpy(out, type_name, len);
    out[len] = '\0';
    return true;
}

static const char *lookup_import_map_value(const char *name, const char **imp_keys,
                                           const char **imp_vals, int imp_count) {
    if (!name || !imp_keys || !imp_vals) {
        return NULL;
    }
    for (int i = 0; i < imp_count; i++) {
        if (imp_keys[i] && imp_vals[i] && strcmp(imp_keys[i], name) == 0) {
            return imp_vals[i];
        }
    }
    return NULL;
}

static bool is_identifier_char(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
           ch == '_';
}

static bool range_contains_identifier(const char *start, const char *end, const char *name) {
    if (!start || !end || !name || !name[0] || start >= end) {
        return false;
    }
    size_t name_len = strlen(name);
    const char *p = start;
    while (p && p < end) {
        p = strstr(p, name);
        if (!p || p >= end) {
            return false;
        }
        const char prev = (p == start) ? '\0' : p[-1];
        const char next = (p + name_len >= end) ? '\0' : p[name_len];
        if (!is_identifier_char(prev) && !is_identifier_char(next)) {
            return true;
        }
        p += name_len;
    }
    return false;
}

static bool range_contains_keyword_decl(const char *start, const char *end, const char *keyword,
                                        const char *name) {
    if (!start || !end || !keyword || !name) {
        return false;
    }
    const char *p = start;
    const size_t keyword_len = strlen(keyword);
    while (p && p < end) {
        p = strstr(p, keyword);
        if (!p || p >= end) {
            return false;
        }
        const char *name_start = p + keyword_len;
        while (name_start < end && (*name_start == ' ' || *name_start == '\t')) {
            name_start++;
        }
        if (name_start < end && strncmp(name_start, name, strlen(name)) == 0) {
            const char next = name_start[strlen(name)];
            if (!is_identifier_char(next)) {
                return true;
            }
        }
        p = name_start;
    }
    return false;
}

static bool gdscript_name_shadowed_in_func_source(const char *source, const char *func_qn,
                                                  const char *name) {
    if (!source || !func_qn || !name || !name[0]) {
        return false;
    }

    const char *func_name = strrchr(func_qn, '.');
    func_name = func_name ? func_name + 1 : func_qn;

    size_t func_name_len = strlen(func_name);
    const char *func_start = NULL;
    for (const char *line = source; line && *line; ) {
        const char *next = strchr(line, '\n');
        const char *scan = line;
        while (*scan == ' ' || *scan == '\t') {
            scan++;
        }
        if (strncmp(scan, "func ", 5) == 0 && strncmp(scan + 5, func_name, func_name_len) == 0 &&
            scan[5 + func_name_len] == '(') {
            func_start = scan;
            break;
        }
        line = next ? next + 1 : NULL;
    }
    if (!func_start) {
        return false;
    }
    const char *line_end = strchr(func_start, '\n');
    if (!line_end) {
        line_end = source + strlen(source);
    }

    const char *params_start = strchr(func_start, '(');
    const char *params_end = params_start ? strchr(params_start, ')') : NULL;
    if (params_start && params_end && params_end <= line_end &&
        range_contains_identifier(params_start + 1, params_end, name)) {
        return true;
    }

    const char *func_end = source + strlen(source);
    for (const char *line = line_end; line && *line; ) {
        const char *next = strchr(line, '\n');
        const char *scan = line;
        if (*scan == '\n') {
            scan++;
        }
        while (*scan == ' ' || *scan == '\t') {
            scan++;
        }
        if (strncmp(scan, "func ", 5) == 0 || strncmp(scan, "class ", 6) == 0 ||
            strncmp(scan, "class_name ", 11) == 0) {
            func_end = line;
            break;
        }
        line = next ? next + 1 : NULL;
    }
    return range_contains_keyword_decl(line_end, func_end, "var", name) ||
           range_contains_keyword_decl(line_end, func_end, "const", name);
}

static const CBMDefinition *gdscript_find_definition_by_qn(const CBMFileResult *result,
                                                           const char *qualified_name) {
    if (!result || !qualified_name) {
        return NULL;
    }
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (def->qualified_name && strcmp(def->qualified_name, qualified_name) == 0) {
            return def;
        }
    }
    return NULL;
}

static const char *source_line_start(const char *source, uint32_t line_no) {
    if (!source || line_no == 0) {
        return NULL;
    }
    const char *line = source;
    for (uint32_t current = 1; current < line_no; current++) {
        line = strchr(line, '\n');
        if (!line) {
            return NULL;
        }
        line++;
    }
    return line;
}

static bool gdscript_func_body_range_from_definition(const CBMFileResult *result, const char *source,
                                                     const char *func_qn, const char **out_body_start,
                                                     const char **out_func_end) {
    if (!result || !source || !func_qn || !out_body_start || !out_func_end) {
        return false;
    }

    const CBMDefinition *def = gdscript_find_definition_by_qn(result, func_qn);
    if (!def || def->start_line == 0 || def->end_line < def->start_line) {
        return false;
    }

    const char *header_start = source_line_start(source, def->start_line);
    if (!header_start) {
        return false;
    }

    const char *body_start = strchr(header_start, '\n');
    if (!body_start) {
        body_start = source + strlen(source);
    } else {
        body_start++;
    }

    const char *func_end = source_line_start(source, def->end_line + 1);
    if (!func_end) {
        func_end = source + strlen(source);
    }

    *out_body_start = body_start;
    *out_func_end = func_end;
    return true;
}

static bool gdscript_should_skip_generic_fallback(const CBMCall *call) {
    if (!call || !call->callee_name) {
        return false;
    }
    return gdscript_signal_member_suffix_len(call->callee_name) > 0;
}

static bool gdscript_match_receiver_assignment_line(const char *line_start, const char *line_end,
                                                    const char *receiver_name, char *out,
                                                    size_t out_size) {
    if (!line_start || !line_end || !receiver_name || !receiver_name[0] || !out || out_size == 0) {
        return false;
    }

    const char *scan = line_start;
    while (scan < line_end && (*scan == ' ' || *scan == '\t')) {
        scan++;
    }
    if (scan >= line_end || *scan == '#') {
        return false;
    }

    const char *prefix = NULL;
    if ((size_t)(line_end - scan) > 4 && strncmp(scan, "var ", 4) == 0) {
        prefix = "var ";
    } else if ((size_t)(line_end - scan) > 6 && strncmp(scan, "const ", 6) == 0) {
        prefix = "const ";
    } else {
        return false;
    }
    scan += strlen(prefix);

    size_t receiver_len = strlen(receiver_name);
    if ((size_t)(line_end - scan) <= receiver_len || strncmp(scan, receiver_name, receiver_len) != 0 ||
        is_identifier_char(scan[receiver_len])) {
        return false;
    }
    scan += receiver_len;
    while (scan < line_end && (*scan == ' ' || *scan == '\t')) {
        scan++;
    }
    if (scan >= line_end || *scan != '=') {
        return false;
    }
    scan++;
    while (scan < line_end && (*scan == ' ' || *scan == '\t')) {
        scan++;
    }
    if (scan >= line_end || !(*scan >= 'A' && *scan <= 'Z')) {
        return false;
    }

    const char *call_end = NULL;
    for (const char *p = scan; p + 4 <= line_end; p++) {
        if (strncmp(p, ".new", 4) == 0) {
            call_end = p;
            break;
        }
    }
    if (!call_end || call_end == scan) {
        return false;
    }

    size_t len = (size_t)(call_end - scan);
    if (len + 1 > out_size) {
        return false;
    }
    memcpy(out, scan, len);
    out[len] = '\0';
    return true;
}

static void gdscript_update_triple_quote_state(const char *line_start, const char *line_end,
                                               char *quote_char) {
    if (!line_start || !line_end || !quote_char) {
        return;
    }

    for (const char *p = line_start; p + 2 < line_end; p++) {
        if (*quote_char) {
            if (p[0] == *quote_char && p[1] == *quote_char && p[2] == *quote_char) {
                *quote_char = '\0';
                p += 2;
            }
            continue;
        }
        if ((p[0] == '\'' || p[0] == '"') && p[1] == p[0] && p[2] == p[0]) {
            *quote_char = p[0];
            p += 2;
        }
    }
}

static bool gdscript_infer_receiver_type_from_source(const CBMFileResult *result, const char *source,
                                                     const char *func_qn, const char *receiver_name,
                                                     char *out, size_t out_size) {
    const char *body_start = NULL;
    const char *func_end = NULL;
    if (!result || !source || !func_qn || !receiver_name || !receiver_name[0] || !out ||
        out_size == 0 ||
        !gdscript_func_body_range_from_definition(result, source, func_qn, &body_start, &func_end)) {
        return false;
    }

    char triple_quote = '\0';
    for (const char *line = body_start; line && line < func_end && *line; ) {
        const char *next = strchr(line, '\n');
        const char *line_end = next ? next : func_end;
        if (line_end > func_end) {
            line_end = func_end;
        }

        bool in_triple_quote = (triple_quote != '\0');
        gdscript_update_triple_quote_state(line, line_end, &triple_quote);

        if (!in_triple_quote && triple_quote == '\0' &&
            gdscript_match_receiver_assignment_line(line, line_end, receiver_name, out,
                                                    out_size)) {
            return true;
        }
        line = next ? next + 1 : NULL;
    }

    return false;
}

static bool gdscript_receiver_signal_target_qn(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                                const CBMCall *call, const char *module_qn,
                                                const char **imp_keys, const char **imp_vals,
                                                int imp_count, const char *source_text, char *out,
                                                size_t out_size) {
    if (!ctx || !result || !call || !call->first_string_arg || !call->first_string_arg[0] || !out) {
        return false;
    }

    (void)source_text;

    char receiver_name[128];
    if (!gdscript_receiver_var_name(call, receiver_name, sizeof(receiver_name))) {
        return false;
    }

    char inferred_type[256] = {0};
    const char *type_name = gdscript_type_assign_for_receiver(result, call, receiver_name);
    if (!type_name) {
        if (!gdscript_infer_receiver_type_from_source(result, source_text, call->enclosing_func_qn,
                                                       receiver_name, inferred_type,
                                                       sizeof(inferred_type))) {
            return false;
        }
        type_name = inferred_type;
    }

    char normalized_type[256];
    if (!gdscript_normalize_type_name(type_name, normalized_type, sizeof(normalized_type))) {
        return false;
    }

    const char *target_anchor = NULL;
    const char *import_target = lookup_import_map_value(normalized_type, imp_keys, imp_vals, imp_count);
    if (import_target) {
        target_anchor = promote_gdscript_target_to_class(ctx, import_target, normalized_type);
    } else {
        cbm_resolution_t target_res = cbm_registry_resolve(ctx->registry, normalized_type, module_qn,
                                                           imp_keys, imp_vals, imp_count);
        if (target_res.qualified_name &&
            is_classish_label(cbm_registry_label_of(ctx->registry, target_res.qualified_name))) {
            target_anchor = target_res.qualified_name;
        } else {
            target_anchor = promote_gdscript_target_to_class(ctx, target_res.qualified_name,
                                                             normalized_type);
        }
    }
    if (!target_anchor) {
        return false;
    }

    snprintf(out, out_size, "%s.signal.%s", target_anchor, call->first_string_arg);
    return true;
}

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
/* Handle a route registration call: create Route node + HANDLES edge. */
static void handle_route_registration(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                      const cbm_gbuf_node_t *source_node, const char *module_qn,
                                      const char **imp_keys, const char **imp_vals, int imp_count) {
    const char *method = cbm_service_pattern_route_method(call->callee_name);
    char route_qn[CBM_ROUTE_QN_SIZE];
    snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method ? method : "ANY",
             call->first_string_arg);
    char route_props[CBM_SZ_256];
    snprintf(route_props, sizeof(route_props), "{\"method\":\"%s\"}", method ? method : "ANY");
    int64_t route_id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", call->first_string_arg, route_qn,
                                            "", 0, 0, route_props);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"url_path\":\"%s\",\"via\":\"route_registration\"}",
             call->callee_name, call->first_string_arg);
    cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, route_id, "CALLS", props);
    if (call->second_arg_name != NULL && call->second_arg_name[0] != '\0') {
        cbm_resolution_t hres = cbm_registry_resolve(ctx->registry, call->second_arg_name,
                                                     module_qn, imp_keys, imp_vals, imp_count);
        if (hres.qualified_name != NULL && hres.qualified_name[0] != '\0') {
            const cbm_gbuf_node_t *handler = cbm_gbuf_find_by_qn(ctx->gbuf, hres.qualified_name);
            if (handler != NULL) {
                char hprops[CBM_SZ_256];
                snprintf(hprops, sizeof(hprops), "{\"handler\":\"%s\"}", hres.qualified_name);
                cbm_gbuf_insert_edge(ctx->gbuf, handler->id, route_id, "HANDLES", hprops);
            }
        }
    }
}

/* Emit an HTTP/async route edge for a service call. */
/* Build route QN and upsert Route node for HTTP/async edge. */
static int64_t create_svc_route_node(cbm_pipeline_ctx_t *ctx, const char *url, cbm_svc_kind_t svc,
                                     const char *method, const char *broker) {
    char route_qn[CBM_ROUTE_QN_SIZE];
    const char *prefix;
    if (svc == CBM_SVC_HTTP) {
        prefix = method ? method : "ANY";
    } else {
        prefix = broker ? broker : "async";
    }
    snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", prefix, url);
    const char *rp;
    if (svc == CBM_SVC_HTTP) {
        rp = method ? method : "{}";
    } else {
        rp = broker ? broker : "{}";
    }
    return cbm_gbuf_upsert_node(ctx->gbuf, "Route", url, route_qn, "", 0, 0, rp);
}

static void emit_http_async_edge(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                 const cbm_gbuf_node_t *source, const cbm_gbuf_node_t *target,
                                 const cbm_resolution_t *res, cbm_svc_kind_t svc) {
    const char *url_or_topic = call->first_string_arg;
    bool is_url = (url_or_topic && url_or_topic[0] != '\0' &&
                   (url_or_topic[0] == '/' || strstr(url_or_topic, "://") != NULL));
    bool is_topic = (url_or_topic && url_or_topic[0] != '\0' && svc == CBM_SVC_ASYNC &&
                     strlen(url_or_topic) > PAIR_LEN);
    if (!is_url && !is_topic) {
        char esc_callee[CBM_SZ_256];
        cbm_json_escape(esc_callee, sizeof(esc_callee), call->callee_name);
        char props[CBM_SZ_512];
        snprintf(props, sizeof(props),
                 "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}",
                 esc_callee, res->confidence, res->strategy ? res->strategy : "unknown",
                 res->candidate_count);
        cbm_gbuf_insert_edge(ctx->gbuf, source->id, target->id, "CALLS", props);
        return;
    }
    const char *edge_type = (svc == CBM_SVC_HTTP) ? "HTTP_CALLS" : "ASYNC_CALLS";
    const char *method =
        (svc == CBM_SVC_HTTP) ? cbm_service_pattern_http_method(call->callee_name) : NULL;
    const char *broker =
        (svc == CBM_SVC_ASYNC) ? cbm_service_pattern_broker(res->qualified_name) : NULL;
    int64_t route_id = create_svc_route_node(ctx, url_or_topic, svc, method, broker);
    char esc_callee[CBM_SZ_256];
    char esc_url[CBM_SZ_256];
    cbm_json_escape(esc_callee, sizeof(esc_callee), call->callee_name);
    cbm_json_escape(esc_url, sizeof(esc_url), url_or_topic);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"url_path\":\"%s\"%s%s%s%s%s}", esc_callee,
             esc_url, method ? ",\"method\":\"" : "", method ? method : "", method ? "\"" : "",
             broker ? ",\"broker\":\"" : "", broker ? broker : "");
    if (broker) {
        size_t plen = strlen(props);
        if (plen > 0 && props[plen - SKIP_ONE] != '}') {
            snprintf(props + plen - 1, sizeof(props) - plen + SKIP_ONE, "\"}");
        }
    }
    cbm_gbuf_insert_edge(ctx->gbuf, source->id, route_id, edge_type, props);
}

/* Classify a resolved call and emit the appropriate edge. */
static void emit_classified_edge(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                 const cbm_gbuf_node_t *source, const cbm_gbuf_node_t *target,
                                 const cbm_resolution_t *res, const char *module_qn,
                                 const char **imp_keys, const char **imp_vals, int imp_count) {
    cbm_svc_kind_t svc = cbm_service_pattern_match(res->qualified_name);
    if (svc == CBM_SVC_ROUTE_REG && call->first_string_arg && call->first_string_arg[0] == '/') {
        handle_route_registration(ctx, call, source, module_qn, imp_keys, imp_vals, imp_count);
        return;
    }
    if (svc == CBM_SVC_HTTP || svc == CBM_SVC_ASYNC) {
        emit_http_async_edge(ctx, call, source, target, res, svc);
        return;
    }
    if (svc == CBM_SVC_CONFIG) {
        char esc_c[CBM_SZ_256];
        char esc_k[CBM_SZ_256];
        cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
        cbm_json_escape(esc_k, sizeof(esc_k), call->first_string_arg ? call->first_string_arg : "");
        char props[CBM_SZ_512];
        snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"key\":\"%s\",\"confidence\":%.2f}",
                 esc_c, esc_k, res->confidence);
        cbm_gbuf_insert_edge(ctx->gbuf, source->id, target->id, "CONFIGURES", props);
        return;
    }
    char esc_c2[CBM_SZ_256];
    cbm_json_escape(esc_c2, sizeof(esc_c2), call->callee_name);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}",
             esc_c2, res->confidence, res->strategy ? res->strategy : "unknown",
             res->candidate_count);
    cbm_gbuf_insert_edge(ctx->gbuf, source->id, target->id, "CALLS", props);
}

/* Find source node for a call: enclosing function or file node. */
static const cbm_gbuf_node_t *calls_find_source(cbm_pipeline_ctx_t *ctx, const char *rel,
                                                const char *enclosing_qn) {
    const cbm_gbuf_node_t *src = NULL;
    if (enclosing_qn) {
        src = cbm_gbuf_find_by_qn(ctx->gbuf, enclosing_qn);
    }
    if (!src) {
        char *fqn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        src = cbm_gbuf_find_by_qn(ctx->gbuf, fqn);
        free(fqn);
    }
    return src;
}

/* Resolve one call and emit the appropriate edge. Returns 1 if resolved, 0 if not. */
static int resolve_single_call(cbm_pipeline_ctx_t *ctx, CBMCall *call, const CBMFileResult *result,
                               const char *rel, const char *module_qn, const char **imp_keys,
                               const char **imp_vals, int imp_count, const char *gdscript_source,
                               bool is_gdscript) {
    const cbm_gbuf_node_t *source_node = calls_find_source(ctx, rel, call->enclosing_func_qn);
    if (!source_node) {
        return 0;
    }

    cbm_resolution_t res = {0};

    if (is_gdscript) {
        char anchor_qn[512];
        char gdscript_target_qn[768];
        bool has_anchor_qn = gdscript_script_anchor_qn(source_node, anchor_qn, sizeof(anchor_qn));
        bool allow_same_script_signal = true;

        if (gdscript_source && call->first_string_arg && call->first_string_arg[0] &&
            gdscript_signal_member_suffix_len(call->callee_name) > 0 &&
            memchr(call->callee_name, '.',
                   strlen(call->callee_name) -
                       gdscript_signal_member_suffix_len(call->callee_name)) == NULL) {
            allow_same_script_signal =
                !gdscript_name_shadowed_in_func_source(gdscript_source, call->enclosing_func_qn,
                                                       call->first_string_arg);
        }

        if (has_anchor_qn && allow_same_script_signal &&
            gdscript_same_script_target_qn(call, anchor_qn, gdscript_target_qn,
                                           sizeof(gdscript_target_qn))) {
            res = cbm_registry_resolve(ctx->registry, gdscript_target_qn, module_qn, imp_keys,
                                       imp_vals, imp_count);
        }

        if ((!res.qualified_name || res.qualified_name[0] == '\0') &&
            gdscript_receiver_signal_target_qn(ctx, result, call, module_qn,
                                              imp_keys, imp_vals, imp_count,
                                              (has_anchor_qn &&
                                               gdscript_anchor_matches_module(anchor_qn, module_qn))
                                                  ? gdscript_source
                                                  : NULL,
                                              gdscript_target_qn, sizeof(gdscript_target_qn))) {
            res = cbm_registry_resolve(ctx->registry, gdscript_target_qn, module_qn, imp_keys,
                                       imp_vals, imp_count);
        }

        if ((!res.qualified_name || res.qualified_name[0] == '\0') &&
            gdscript_should_skip_generic_fallback(call)) {
            return 0;
        }
    }

    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        res = cbm_registry_resolve(ctx->registry, call->callee_name, module_qn, imp_keys, imp_vals,
                                  imp_count);
    }

    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return 0;
    }
    const cbm_gbuf_node_t *target_node = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
    if (!target_node || source_node->id == target_node->id) {
        return 0;
    }
    emit_classified_edge(ctx, call, source_node, target_node, &res, module_qn, imp_keys, imp_vals,
                        imp_count);
    return SKIP_ONE;
}

static CBMFileResult *calls_get_or_extract(cbm_pipeline_ctx_t *ctx, int idx,
                                           const cbm_file_info_t *fi, bool *owned) {
    *owned = false;
    if (ctx->result_cache && ctx->result_cache[idx]) {
        return ctx->result_cache[idx];
    }
    int slen = 0;
    char *src = read_file(fi->path, &slen);
    if (!src) {
        return NULL;
    }
    CBMFileResult *r = cbm_extract_file(src, slen, fi->language, ctx->project_name, fi->rel_path,
                                        CBM_EXTRACT_BUDGET, NULL, NULL);
    free(src);
    if (r) {
        *owned = true;
    }
    return r;
}

int cbm_pipeline_pass_calls(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "calls", "files", itoa_log(file_count));

    int total_calls = 0;
    int resolved = 0;
    int unresolved = 0;
    int errors = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return CBM_NOT_FOUND;
        }

        const char *rel = files[i].rel_path;
        bool result_owned = false;
        CBMFileResult *result = calls_get_or_extract(ctx, i, &files[i], &result_owned);
        if (!result) {
            errors++;
            continue;
        }

        char *gdscript_source = NULL;
        int gdscript_source_len = 0;
        if (files[i].language == CBM_LANG_GDSCRIPT) {
            gdscript_source = read_file(files[i].path, &gdscript_source_len);
        }

        if (result->calls.count == 0) {
            free(gdscript_source);
            if (result_owned) {
                cbm_free_result(result);
            }
            continue;
        }

        /* Build import map for this file */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count);

        /* Compute module QN for same-module resolution */
        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        /* Resolve each call */
        for (int c = 0; c < result->calls.count; c++) {
            CBMCall *call = &result->calls.items[c];
            if (!call->callee_name) {
                continue;
            }
            total_calls++;
            if (resolve_single_call(ctx, call, result, rel, module_qn, imp_keys, imp_vals, imp_count,
                                   files[i].language == CBM_LANG_GDSCRIPT ? gdscript_source : NULL,
                                   files[i].language == CBM_LANG_GDSCRIPT)) {
                resolved++;
            } else {
                unresolved++;
            }
        }

        free(module_qn);
        free(gdscript_source);
        free_import_map(imp_keys, imp_vals, imp_count);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    cbm_log_info("pass.done", "pass", "calls", "total", itoa_log(total_calls), "resolved",
                 itoa_log(resolved), "unresolved", itoa_log(unresolved), "errors",
                 itoa_log(errors));

    /* Additional pattern-based edge passes run after normal call resolution */
    cbm_pipeline_pass_fastapi_depends(ctx, files, file_count);

    return 0;
}

/* ── FastAPI Depends() tracking ──────────────────────────────────── */
/* Scans Python function signatures for Depends(func_ref) patterns and
 * creates CALLS edges from the endpoint to the dependency function.
 * Without this, FastAPI auth/DI functions appear as dead code (in_degree=0). */

/* Extract Python function signature text from source starting at given line. Caller frees. */
static char *extract_py_signature(const char *source, int start_line, int end_line) {
    int sig_end = start_line + PC_SIG_SCAN;
    if (end_line > 0 && sig_end > end_line) {
        sig_end = end_line;
    }
    const char *p = source;
    int line = SKIP_ONE;
    while (*p && line < start_line) {
        if (*p == '\n') {
            line++;
        }
        p++;
    }
    const char *sig_start = p;
    while (*p && line < sig_end) {
        if (*p == '\n') {
            line++;
        }
        p++;
        if (p > sig_start + SKIP_ONE && p[-SKIP_ONE] == ':' && p[-PAIR_LEN] == ')') {
            break;
        }
    }
    size_t sig_len = (size_t)(p - sig_start);
    char *sig = malloc(sig_len + SKIP_ONE);
    if (!sig) {
        return NULL;
    }
    memcpy(sig, sig_start, sig_len);
    sig[sig_len] = '\0';
    return sig;
}

/* Scan one function's signature for Depends(func_ref) and create CALLS edges. */
static int scan_depends_in_sig(cbm_pipeline_ctx_t *ctx, const cbm_regex_t *re, const char *sig,
                               const CBMDefinition *def, const char *module_qn, const char **ik,
                               const char **iv, int ic) {
    int count = 0;
    cbm_regmatch_t match[PC_REGEX_GRP];
    const char *scan = sig;
    while (cbm_regexec(re, scan, PC_REGEX_GRP, match, 0) == 0) {
        int ref_len = match[SKIP_ONE].rm_eo - match[SKIP_ONE].rm_so;
        char func_ref[CBM_SZ_256];
        if (ref_len >= (int)sizeof(func_ref)) {
            ref_len = (int)sizeof(func_ref) - SKIP_ONE;
        }
        memcpy(func_ref, scan + match[SKIP_ONE].rm_so, (size_t)ref_len);
        func_ref[ref_len] = '\0';
        cbm_resolution_t res = cbm_registry_resolve(ctx->registry, func_ref, module_qn, ik, iv, ic);
        if (res.qualified_name && res.qualified_name[0] != '\0') {
            const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
            const cbm_gbuf_node_t *tn = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
            if (sn && tn && sn->id != tn->id) {
                cbm_gbuf_insert_edge(ctx->gbuf, sn->id, tn->id, "CALLS",
                                     "{\"confidence\":0.95,\"strategy\":\"fastapi_depends\"}");
                count++;
            }
        }
        scan += match[0].rm_eo;
    }
    return count;
}

static bool is_callable_def(const CBMDefinition *def) {
    return def->qualified_name && def->start_line > 0 && def->label &&
           (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0);
}

static bool file_has_depends_call(const CBMFileResult *result) {
    for (int c = 0; c < result->calls.count; c++) {
        if (result->calls.items[c].callee_name &&
            strcmp(result->calls.items[c].callee_name, "Depends") == 0) {
            return true;
        }
    }
    return false;
}

void cbm_pipeline_pass_fastapi_depends(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count) {
    cbm_regex_t depends_re;
    if (cbm_regcomp(&depends_re, "Depends\\(([A-Za-z_][A-Za-z0-9_.]*)", CBM_REG_EXTENDED) != 0) {
        return;
    }

    int edge_count = 0;
    for (int i = 0; i < file_count; i++) {
        if (files[i].language != CBM_LANG_PYTHON) {
            continue;
        }
        if (cbm_pipeline_check_cancel(ctx)) {
            break;
        }

        CBMFileResult *result = ctx->result_cache ? ctx->result_cache[i] : NULL;
        if (!result || !file_has_depends_call(result)) {
            continue;
        }

        /* Read source and scan for Depends(func_ref) in function signatures */
        int source_len = 0;
        char *source = read_file(files[i].path, &source_len);
        if (!source) {
            continue;
        }

        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, files[i].rel_path);

        /* Build import map for alias resolution */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, files[i].rel_path, result, &imp_keys, &imp_vals, &imp_count);

        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!is_callable_def(def)) {
                continue;
            }

            char *sig = extract_py_signature(source, (int)def->start_line, (int)def->end_line);
            if (!sig) {
                continue;
            }

            edge_count += scan_depends_in_sig(ctx, &depends_re, sig, def, module_qn, imp_keys,
                                              imp_vals, imp_count);
            free(sig);
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
        free(source);
    }

    cbm_regfree(&depends_re);
    if (edge_count > 0) {
        cbm_log_info("pass.fastapi_depends", "edges", itoa_log(edge_count));
    }
}

/* DLL resolve tracking removed — triggered Windows Defender false positive.
 * See issue #89. */
