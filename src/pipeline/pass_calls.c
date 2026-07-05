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

enum {
    PC_RING = 4,
    PC_RING_MASK = 3,
    PC_SIG_SCAN = 15,
    PC_REGEX_GRP = 2,
    /* Keep sequential CALLS/HTTP/CONFIG edge property capacity aligned with
     * pass_parallel.c so incremental overlays serialize the same call args as
     * a fresh full index. */
    PC_CALL_PROPS_CAP = CBM_SZ_2K,
};
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "pipeline/lsp_resolve.h"
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

    /* +pad: tree-sitter lexer lookahead reads past EOF; keep it in-bounds */
    enum { CBM_TS_LOOKAHEAD_PAD = 16 };
    char *buf = malloc((size_t)size + CBM_TS_LOOKAHEAD_PAD);
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

/* Format int for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[PC_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PC_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Handle a route registration call: create Route node + HANDLES edge. */
static void handle_route_registration(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                      const cbm_gbuf_node_t *source_node, const char *route_path,
                                      const char *handler_ref, const char *module_qn,
                                      const char **imp_keys, const char **imp_vals, int imp_count) {
    const char *method = cbm_service_pattern_route_method(call->callee_name);
    /* Reject CLI slash-command args (e.g. "/ar:allow") that start with '/' and
     * so pass the caller's first_string_arg[0]=='/' check, but aren't valid
     * route paths. Same gate as pass_route_nodes.c — keeps command-syntax
     * strings from becoming spurious Route nodes. */
    if (!cbm_service_pattern_is_http_route_literal(route_path, call->callee_name)) {
        return;
    }
    int64_t route_id = cbm_pipeline_upsert_service_route(
        ctx->gbuf, route_path, CBM_SVC_HTTP, method, NULL, NULL, NULL);
    if (route_id == 0) {
        return;
    }
    char esc_cn[CBM_SZ_256]; /* sliced source text: escape quotes/newlines */
    char esc_fa[CBM_SZ_256];
    cbm_json_escape(esc_cn, sizeof(esc_cn), call->callee_name);
    cbm_json_escape(esc_fa, sizeof(esc_fa), route_path);
    char props[CBM_SZ_512];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"url_path\":\"%s\",\"via\":\"route_registration\"}", esc_cn,
             esc_fa);
    cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, route_id, "CALLS", props);
    if (handler_ref != NULL && handler_ref[0] != '\0') {
        cbm_resolution_t hres =
            cbm_registry_resolve(ctx->registry, handler_ref, module_qn, imp_keys, imp_vals,
                                 imp_count);
        if (hres.qualified_name != NULL && hres.qualified_name[0] != '\0') {
            const cbm_gbuf_node_t *handler = cbm_gbuf_find_by_qn(ctx->gbuf, hres.qualified_name);
            if (handler == NULL) {
                handler = cbm_pipeline_find_node_by_qn(ctx, hres.qualified_name);
            }
            if (handler != NULL) {
                char hprops[CBM_SZ_1K]; /* must exceed escaped value + wrapper or snprintf cuts the
                                           closing brace */
                char esc_h[CBM_SZ_512];
                cbm_json_escape(esc_h, sizeof(esc_h), hres.qualified_name);
                snprintf(hprops, sizeof(hprops), "{\"handler\":\"%s\"}", esc_h);
                cbm_gbuf_insert_edge(ctx->gbuf, handler->id, route_id, "HANDLES", hprops);
            }
        }
    }
}

/* Insert a call-site edge through the shared finalizer so sequential and
 * parallel indexing serialize call args identically. Only plain CALLS carries
 * line metadata; service/config edges use args for derived route/data-flow
 * passes but keep their existing edge-specific fields as the stable contract. */
static void calls_emit_edge(cbm_gbuf_t *gbuf, int64_t src, int64_t tgt, const char *type,
                            char *props, size_t cap, const CBMCall *call) {
    if (call) {
        size_t len = strlen(props);
        if (len >= SKIP_ONE && props[len - SKIP_ONE] == '}') {
            bool include_line = type && strcmp(type, "CALLS") == 0;
            cbm_pipeline_close_call_edge_props(props, cap, len - SKIP_ONE, call, include_line);
        }
    }
    cbm_gbuf_insert_edge(gbuf, src, tgt, type, props);
}

static void emit_http_async_edge(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                 const cbm_gbuf_node_t *source, const cbm_gbuf_node_t *target,
                                 const cbm_resolution_t *res, cbm_svc_kind_t svc) {
    const char *url_or_topic = call->first_string_arg;
    bool is_url = (url_or_topic && url_or_topic[0] != '\0' &&
                   (url_or_topic[0] == '/' || strstr(url_or_topic, "://") != NULL));
    bool is_topic = (url_or_topic && url_or_topic[0] != '\0' && svc == CBM_SVC_ASYNC &&
                     strlen(url_or_topic) > PAIR_LEN);
    /* An HTTP call whose URL isn't a valid route path (e.g. CLI "/ar:ok" from a
     * .get(...) accessor that the service-pattern matcher misclassified as
     * HTTP) must not become a spurious Route node. Drop the URL so we fall
     * through to the plain CALLS edge — the call relationship is preserved
     * without fabricating a route. (Async topics aren't route paths, so only
     * gate HTTP.) */
    if (svc == CBM_SVC_HTTP && is_url &&
        !cbm_service_pattern_is_http_route_literal(url_or_topic, call->callee_name)) {
        is_url = false;
    }
    if (!is_url && !is_topic) {
        char esc_callee[CBM_SZ_256];
        cbm_json_escape(esc_callee, sizeof(esc_callee), call->callee_name);
        char props[PC_CALL_PROPS_CAP];
        snprintf(props, sizeof(props),
                 "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}",
                 esc_callee, res->confidence, res->strategy ? res->strategy : "unknown",
                 res->candidate_count);
        calls_emit_edge(ctx->gbuf, source->id, target->id, "CALLS", props, sizeof(props), call);
        return;
    }
    const char *edge_type = (svc == CBM_SVC_HTTP) ? "HTTP_CALLS" : "ASYNC_CALLS";
    const char *method =
        (svc == CBM_SVC_HTTP) ? cbm_service_pattern_http_method(call->callee_name) : NULL;
    const char *broker =
        (svc == CBM_SVC_ASYNC) ? cbm_service_pattern_broker(res->qualified_name) : NULL;
    int64_t route_id = cbm_pipeline_upsert_service_route(ctx->gbuf, url_or_topic, svc, method,
                                                         broker, NULL, NULL);
    if (route_id == 0) {
        return;
    }
    char esc_callee[CBM_SZ_256];
    char esc_url[CBM_SZ_256];
    cbm_json_escape(esc_callee, sizeof(esc_callee), call->callee_name);
    cbm_json_escape(esc_url, sizeof(esc_url), url_or_topic);
    char props[PC_CALL_PROPS_CAP];
    snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"url_path\":\"%s\"%s%s%s%s%s}", esc_callee,
             esc_url, method ? ",\"method\":\"" : "", method ? method : "", method ? "\"" : "",
             broker ? ",\"broker\":\"" : "", broker ? broker : "");
    if (broker) {
        size_t plen = strlen(props);
        if (plen > 0 && props[plen - SKIP_ONE] != '}') {
            snprintf(props + plen - 1, sizeof(props) - plen + SKIP_ONE, "\"}");
        }
    }
    calls_emit_edge(ctx->gbuf, source->id, route_id, edge_type, props, sizeof(props), call);
}

/* Classify a resolved call and emit the appropriate edge. */
static bool emit_classified_edge(cbm_pipeline_ctx_t *ctx, const CBMCall *call,
                                 const cbm_gbuf_node_t *source, const cbm_gbuf_node_t *target,
                                 const cbm_resolution_t *res, const char *module_qn,
                                 const char **imp_keys, const char **imp_vals, int imp_count) {
    cbm_svc_kind_t svc = cbm_service_pattern_match(res->qualified_name);
    if (svc == CBM_SVC_ROUTE_REG) {
        const char *handler_ref = NULL;
        const char *route_path = cbm_pipeline_call_route_path_and_handler(call, &handler_ref);
        if (route_path) {
            handle_route_registration(ctx, call, source, route_path, handler_ref, module_qn,
                                      imp_keys, imp_vals, imp_count);
            return false;
        }
    }
    if (svc == CBM_SVC_HTTP || svc == CBM_SVC_ASYNC) {
        emit_http_async_edge(ctx, call, source, target, res, svc);
        return true;
    }
    if (svc == CBM_SVC_CONFIG) {
        char esc_c[CBM_SZ_256];
        char esc_k[CBM_SZ_256];
        cbm_json_escape(esc_c, sizeof(esc_c), call->callee_name);
        cbm_json_escape(esc_k, sizeof(esc_k), call->first_string_arg ? call->first_string_arg : "");
        char props[PC_CALL_PROPS_CAP];
        snprintf(props, sizeof(props), "{\"callee\":\"%s\",\"key\":\"%s\",\"confidence\":%.2f}",
                 esc_c, esc_k, res->confidence);
        calls_emit_edge(ctx->gbuf, source->id, target->id, "CONFIGURES", props, sizeof(props),
                        call);
        return true;
    }
    char esc_c2[CBM_SZ_256];
    cbm_json_escape(esc_c2, sizeof(esc_c2), call->callee_name);
    char props[PC_CALL_PROPS_CAP];
    snprintf(props, sizeof(props),
             "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}",
             esc_c2, res->confidence, res->strategy ? res->strategy : "unknown",
             res->candidate_count);
    calls_emit_edge(ctx->gbuf, source->id, target->id, "CALLS", props, sizeof(props), call);
    return true;
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

static const cbm_gbuf_node_t *calls_lsp_target_node(cbm_pipeline_ctx_t *ctx,
                                                    const char *callee_qn) {
    const cbm_gbuf_node_t *direct = cbm_pipeline_find_node_by_qn(ctx, callee_qn);
    if (direct || !ctx || !ctx->project_name || !callee_qn) {
        return direct;
    }
    size_t proj_len = strlen(ctx->project_name);
    if (strncmp(callee_qn, ctx->project_name, proj_len) == 0 && callee_qn[proj_len] == '.') {
        return NULL;
    }
    char buf[CBM_SZ_1K];
    int written = snprintf(buf, sizeof(buf), "%s.%s", ctx->project_name, callee_qn);
    if (written < 0 || (size_t)written >= sizeof(buf)) {
        return NULL;
    }
    return cbm_pipeline_find_node_by_qn(ctx, buf);
}

static bool calls_is_python_super_init(const CBMCall *call, CBMLanguage lang) {
    return lang == CBM_LANG_PYTHON && call && call->callee_name &&
           strcmp(call->callee_name, "super().__init__") == 0;
}

/* Resolve one call and emit the appropriate edge. Returns 1 if resolved, 0 if not. */
static int resolve_single_call(cbm_pipeline_ctx_t *ctx, CBMCall *call,
                               const CBMResolvedCallArray *lsp_calls, const char *rel,
                               const char *module_qn, const char **imp_keys, const char **imp_vals,
                               int imp_count, CBMLanguage lang,
                               const cbm_lsp_resolution_index_t *lsp_idx) {
    const cbm_gbuf_node_t *source_node = calls_find_source(ctx, rel, call->enclosing_func_qn);
    if (!source_node) {
        return 0;
    }

    /* LSP-resolved calls take precedence over registry-textual matching. */
    const CBMResolvedCall *lsp =
        cbm_lsp_resolution_index_find(lsp_idx, lsp_calls, call, ctx->lsp_confidence_floor);
    if (lsp) {
        const cbm_gbuf_node_t *target_node = calls_lsp_target_node(ctx, lsp->callee_qn);
        if (target_node && source_node->id != target_node->id) {
            cbm_resolution_t res = {0};
            /* Use the gbuf node's QN so downstream edge props show the canonical
             * project-qualified form even when fallback prefixed the project. */
            res.qualified_name = target_node->qualified_name;
            res.confidence = lsp->confidence;
            res.strategy = lsp->strategy;
            res.candidate_count = 1;
            if (emit_classified_edge(ctx, call, source_node, target_node, &res, module_qn,
                                     imp_keys, imp_vals, imp_count)) {
                cbm_pipeline_detect_url_arg_routes(ctx->gbuf, source_node, call, rel, lang);
            }
            return SKIP_ONE;
        }
    }

    if (cbm_service_pattern_route_method(call->callee_name) != NULL) {
        const char *handler_ref = NULL;
        const char *route_path = cbm_pipeline_call_route_path_and_handler(call, &handler_ref);
        if (route_path) {
            handle_route_registration(ctx, call, source_node, route_path, handler_ref, module_qn,
                                      imp_keys, imp_vals, imp_count);
            return SKIP_ONE;
        }
    }

    cbm_resolution_t res = cbm_registry_resolve(ctx->registry, call->callee_name, module_qn,
                                                imp_keys, imp_vals, imp_count);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return 0;
    }
    cbm_pipeline_try_field_type_hint(ctx->registry, ctx->gbuf, &res, call->callee_name,
                                     source_node->id);

    /* Perl call-graph noise guard (#476). Perl has no LSP resolver, so the
     * generic registry chain is the only resolver; for builtins (push/shift/
     * keys/...) and method calls ($obj->m with an unresolved receiver), a *weak*
     * cross-file short-name match to a project sub sharing the name is almost
     * always a false positive. Suppress only those weak matches; KEEP the
     * high-confidence same_module / import_map strategies so a genuine
     * same-file or imported call to a builtin-named sub still resolves. Gated
     * to Perl — other languages are unaffected. */
    if (cbm_perl_suppress_generic_match(lang == CBM_LANG_PERL, call->is_method, call->callee_name,
                                        res.strategy)) {
        return 0;
    }
    if (lsp && calls_is_python_super_init(call, lang) && res.strategy &&
        strcmp(res.strategy, "suffix_match") == 0) {
        /* Python super().__init__ often resolves to an external base via LSP.
         * If that target is not indexed, a weak suffix guess can point back to
         * an unrelated local __init__ and create a false recursive edge. */
        return 0;
    }
    const cbm_gbuf_node_t *target_node = cbm_pipeline_find_node_by_qn(ctx, res.qualified_name);
    if (!target_node || source_node->id == target_node->id) {
        return 0;
    }
    if (emit_classified_edge(ctx, call, source_node, target_node, &res, module_qn, imp_keys,
                             imp_vals, imp_count)) {
        cbm_pipeline_detect_url_arg_routes(ctx->gbuf, source_node, call, rel, lang);
    }
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
    CBMFileResult *r = cbm_extract_file_with_options(
        src, slen, fi->language, ctx->project_name, fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL,
        cbm_pipeline_mode_extracts_macro_nodes(ctx->mode));
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

    /* Sequential mode handles small file counts, including a few very large
     * generated/parser files. Use the registry and service-pattern TLS caches
     * already used by cbm_parallel_resolve() so repeated callee names and
     * service-pattern checks pay the full strategy chain once per file instead
     * of once per callsite. */
    cbm_service_pattern_cache_begin();

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            cbm_service_pattern_cache_end();
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
        cbm_pipeline_build_import_map_from_edges(ctx->gbuf, ctx->project_name, rel, &imp_keys,
                                                 &imp_vals, &imp_count);

        /* Compute module QN for same-module resolution */
        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        cbm_registry_reach_cache_begin(result->calls.count + CBM_SZ_64);
        cbm_registry_import_map_cache_begin(imp_keys, imp_vals, imp_count);
        cbm_registry_resolve_cache_begin(result->calls.count + CBM_SZ_64);
        cbm_lsp_resolution_index_t lsp_idx;
        cbm_lsp_resolution_index_build(&lsp_idx, &result->resolved_calls, result->calls.count,
                                       ctx->lsp_confidence_floor);

        /* Resolve each call */
        for (int c = 0; c < result->calls.count; c++) {
            CBMCall *call = &result->calls.items[c];
            if (!call->callee_name) {
                continue;
            }
            total_calls++;

            /* Resolve + emit edge: source-node lookup, indexed LSP override,
             * then registry-textual fallback. */
            if (resolve_single_call(ctx, call, &result->resolved_calls, rel, module_qn, imp_keys,
                                    imp_vals, imp_count, files[i].language, &lsp_idx)) {
                resolved++;
            } else {
                unresolved++;
            }
        }

        cbm_lsp_resolution_index_free(&lsp_idx);
        cbm_registry_reach_cache_end();
        cbm_registry_import_map_cache_end();
        cbm_registry_resolve_cache_end();

        free(module_qn);
        cbm_pipeline_free_import_map(imp_keys, imp_vals, imp_count);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    cbm_log_info("pass.done", "pass", "calls", "total", itoa_log(total_calls), "resolved",
                 itoa_log(resolved), "unresolved", itoa_log(unresolved), "errors",
                 itoa_log(errors));

    /* Additional pattern-based edge passes run after normal call resolution */
    cbm_pipeline_pass_fastapi_depends(ctx, files, file_count);
    cbm_service_pattern_cache_end();

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
            const cbm_gbuf_node_t *tn = cbm_pipeline_find_node_by_qn(ctx, res.qualified_name);
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
        cbm_pipeline_build_import_map_from_edges(ctx->gbuf, ctx->project_name, files[i].rel_path,
                                                 &imp_keys, &imp_vals, &imp_count);

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
        cbm_pipeline_free_import_map(imp_keys, imp_vals, imp_count);
        free(source);
    }

    cbm_regfree(&depends_re);
    if (edge_count > 0) {
        cbm_log_info("pass.fastapi_depends", "edges", itoa_log(edge_count));
    }
}

/* DLL resolve tracking removed — triggered Windows Defender false positive.
 * See issue #89. */
