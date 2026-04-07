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
static int resolve_single_call(cbm_pipeline_ctx_t *ctx, CBMCall *call, const char *rel,
                               const char *module_qn, const char **imp_keys, const char **imp_vals,
                               int imp_count) {
    const cbm_gbuf_node_t *source_node = calls_find_source(ctx, rel, call->enclosing_func_qn);
    if (!source_node) {
        return 0;
    }
    cbm_resolution_t res = cbm_registry_resolve(ctx->registry, call->callee_name, module_qn,
                                                imp_keys, imp_vals, imp_count);
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

        if (result->calls.count == 0) {
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
            if (resolve_single_call(ctx, call, rel, module_qn, imp_keys, imp_vals, imp_count)) {
                resolved++;
            } else {
                unresolved++;
            }
        }

        free(module_qn);
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
