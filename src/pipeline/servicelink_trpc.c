/*
 * servicelink_trpc.c -- tRPC protocol linker.
 *
 * Discovers tRPC procedure definitions (routers) and procedure calls
 * (hooks/clients), then creates TRPC_CALLS edges in the graph buffer.
 *
 * Supported languages: TypeScript/JavaScript ONLY (.ts, .tsx, .js, .jsx).
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* -- Constants ------------------------------------------------------------ */

#define TRPC_CONF_EXACT   0.95  /* exact procedure path match */
#define TRPC_CONF_PARTIAL 0.80  /* last-segment match */

/* -- itoa helper (thread-local rotating buffers) -------------------------- */

static const char *itoa_trpc(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* -- Forward declarations ------------------------------------------------- */

static void scan_producers(const char *source, const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count);
static void scan_consumers(const char *source, const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count);

/* -- Regex helpers -------------------------------------------------------- */

/* Add a producer entry if there's room. */
static void add_producer(cbm_sl_producer_t *producers, int *count,
                         const char *identifier, const cbm_gbuf_node_t *node,
                         const char *extra) {
    if (*count >= SL_MAX_PRODUCERS) return;
    cbm_sl_producer_t *p = &producers[*count];
    snprintf(p->identifier, sizeof(p->identifier), "%s", identifier);
    snprintf(p->source_qn, sizeof(p->source_qn), "%s",
             node->qualified_name ? node->qualified_name : "");
    p->source_id = node->id;
    snprintf(p->file_path, sizeof(p->file_path), "%s",
             node->file_path ? node->file_path : "");
    snprintf(p->extra, sizeof(p->extra), "%s", extra ? extra : "");
    (*count)++;
}

/* Add a consumer entry if there's room. */
static void add_consumer(cbm_sl_consumer_t *consumers, int *count,
                         const char *identifier, const cbm_gbuf_node_t *node,
                         const char *extra) {
    if (*count >= SL_MAX_CONSUMERS) return;
    cbm_sl_consumer_t *c = &consumers[*count];
    snprintf(c->identifier, sizeof(c->identifier), "%s", identifier);
    snprintf(c->handler_qn, sizeof(c->handler_qn), "%s",
             node->qualified_name ? node->qualified_name : "");
    c->handler_id = node->id;
    snprintf(c->file_path, sizeof(c->file_path), "%s",
             node->file_path ? node->file_path : "");
    snprintf(c->extra, sizeof(c->extra), "%s", extra ? extra : "");
    (*count)++;
}

/* Extract a regex submatch into a buffer. Returns the buffer for convenience. */
static char *extract_match(const char *str, const cbm_regmatch_t *m,
                           char *buf, size_t bufsz) {
    if (m->rm_so < 0) {
        buf[0] = '\0';
        return buf;
    }
    int len = m->rm_eo - m->rm_so;
    if ((size_t)len >= bufsz) len = (int)bufsz - 1;
    memcpy(buf, str + m->rm_so, (size_t)len);
    buf[len] = '\0';
    return buf;
}

/* -- Procedure path matching ---------------------------------------------- */

/*
 * Match a consumer procedure path against a producer procedure path.
 * Returns confidence: 0.95 for exact match, 0.80 for last-segment match, 0.0 otherwise.
 *
 * Examples:
 *   "user.getAll" vs "user.getAll"  -> 0.95 (exact)
 *   "getAll"      vs "user.getAll"  -> 0.80 (last segment)
 *   "user.getAll" vs "getAll"       -> 0.80 (last segment)
 *   "user.getAll" vs "post.create"  -> 0.0  (no match)
 */
static double match_procedure_path(const char *consumer_path, const char *producer_path) {
    /* Exact match */
    if (strcmp(consumer_path, producer_path) == 0) {
        return TRPC_CONF_EXACT;
    }

    /* Extract last segment of each path (after last '.') */
    const char *c_last = strrchr(consumer_path, '.');
    const char *p_last = strrchr(producer_path, '.');

    const char *c_seg = c_last ? c_last + 1 : consumer_path;
    const char *p_seg = p_last ? p_last + 1 : producer_path;

    if (c_seg[0] && p_seg[0] && strcmp(c_seg, p_seg) == 0) {
        return TRPC_CONF_PARTIAL;
    }

    return 0.0;
}

/* -- Producer scanning (router definitions) ------------------------------- */

/*
 * Scan TypeScript/JavaScript source for tRPC router/procedure definitions.
 *
 * Patterns detected:
 *   - createTRPCRouter({ getUser: publicProcedure... })
 *   - t.router({ user: t.procedure... })
 *   - word: publicProcedure / protectedProcedure / adminProcedure / procedure
 */
static void scan_producers(const char *source, const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /*
     * Pattern: procedureName: (public|protected|admin)?[Pp]rocedure
     * This captures procedure definitions inside router blocks.
     * Works for createTRPCRouter, t.router, router(), etc.
     */
    if (cbm_regcomp(&re,
            "([a-zA-Z_][a-zA-Z0-9_]*)[ \t]*:[ \t]*[a-zA-Z]*[Pp]rocedure",
            CBM_REG_EXTENDED) == CBM_REG_OK) {
        pos = source;
        while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
            char proc_name[128];
            extract_match(pos, &matches[1], proc_name, sizeof(proc_name));

            /* Skip common false positives (keywords, type fields) */
            if (strcmp(proc_name, "input") != 0 &&
                strcmp(proc_name, "output") != 0 &&
                strcmp(proc_name, "type") != 0 &&
                strcmp(proc_name, "const") != 0 &&
                strcmp(proc_name, "let") != 0 &&
                strcmp(proc_name, "var") != 0 &&
                strcmp(proc_name, "export") != 0 &&
                strcmp(proc_name, "default") != 0) {
                add_producer(producers, prod_count, proc_name, node, "router_def");
            }

            pos += matches[0].rm_eo;
        }
        cbm_regfree(&re);
    }

    /*
     * Pattern: t.procedure.input/query/mutation (older tRPC v9 style)
     * Captures: procedureName inside .query()/.mutation() context
     * Already handled by the generic pattern above.
     */
}

/* -- Consumer scanning (hook/client calls) -------------------------------- */

/*
 * Scan TypeScript/JavaScript source for tRPC procedure calls.
 *
 * Patterns detected:
 *   - trpc.user.getAll.useQuery()          -> "user.getAll"
 *   - trpc.user.getAll.useMutation()       -> "user.getAll"
 *   - trpc.user.useInfiniteQuery()         -> "user"
 *   - trpc.user.useSuspenseQuery()         -> "user"
 *   - client.user.getAll.query()           -> "user.getAll"
 *   - client.user.getAll.mutate()          -> "user.getAll"
 *   - api.user.getAll.useQuery()           -> "user.getAll"
 *   - utils.user.getAll.invalidate()       -> "user.getAll"
 */
static void scan_consumers(const char *source, const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[5];
    const char *pos;

    /*
     * React hook pattern:
     *   (trpc|api|utils).path.segments.useQuery/useMutation/useInfiniteQuery/useSuspenseQuery
     * Capture the procedure path between the prefix and the hook method.
     */
    if (cbm_regcomp(&re,
            "(trpc|api|utils)\\.([a-zA-Z_][a-zA-Z0-9_.]*)\\.use(Query|Mutation|InfiniteQuery|SuspenseQuery)",
            CBM_REG_EXTENDED) == CBM_REG_OK) {
        pos = source;
        while (cbm_regexec(&re, pos, 4, matches, 0) == CBM_REG_OK) {
            char path[256];
            extract_match(pos, &matches[2], path, sizeof(path));

            /* The path may include trailing segments; strip the last if it's useX */
            add_consumer(consumers, cons_count, path, node, "react_hook");

            pos += matches[0].rm_eo;
        }
        cbm_regfree(&re);
    }

    /*
     * Vanilla client pattern:
     *   (trpc|client|api).path.segments.query/mutate/subscribe
     * Capture the procedure path between the prefix and the call method.
     */
    if (cbm_regcomp(&re,
            "(trpc|client|api)\\.([a-zA-Z_][a-zA-Z0-9_.]*)\\.("
            "query|mutate|subscribe"
            ")[ \t]*\\(",
            CBM_REG_EXTENDED) == CBM_REG_OK) {
        pos = source;
        while (cbm_regexec(&re, pos, 4, matches, 0) == CBM_REG_OK) {
            char path[256];
            extract_match(pos, &matches[2], path, sizeof(path));
            add_consumer(consumers, cons_count, path, node, "vanilla_client");

            pos += matches[0].rm_eo;
        }
        cbm_regfree(&re);
    }

    /*
     * Utils invalidation pattern:
     *   utils.path.segments.invalidate()
     * Already partially covered above; add explicit pattern for .invalidate/.refetch/.setData
     */
    if (cbm_regcomp(&re,
            "utils\\.([a-zA-Z_][a-zA-Z0-9_.]*)\\.("
            "invalidate|refetch|setData|getData"
            ")[ \t]*\\(",
            CBM_REG_EXTENDED) == CBM_REG_OK) {
        pos = source;
        while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
            char path[256];
            extract_match(pos, &matches[1], path, sizeof(path));
            add_consumer(consumers, cons_count, path, node, "utils_call");

            pos += matches[0].rm_eo;
        }
        cbm_regfree(&re);
    }
}

/* -- Process a single node ------------------------------------------------ */

static void process_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                         cbm_sl_producer_t *producers, int *prod_count,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    if (!node->file_path) return;

    const char *ext = sl_file_ext(node->file_path);

    /* ONLY TypeScript/JavaScript files */
    if (strcmp(ext, ".ts") == 0 || strcmp(ext, ".tsx") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".jsx") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_producers(source, node, producers, prod_count);
            scan_consumers(source, node, consumers, cons_count);
            free(source);
        }
    }
}

/* -- Main entry point ----------------------------------------------------- */

int cbm_servicelink_trpc(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "trpc");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.trpc", "error", "alloc_failed");
        return -1;
    }
    int prod_count = 0;
    int cons_count = 0;

    /* 2. Get Function, Method, Module, Class, and Variable nodes from graph buffer */
    const cbm_gbuf_node_t **funcs = NULL, **methods = NULL, **modules = NULL;
    const cbm_gbuf_node_t **classes = NULL, **vars = NULL;
    int nfuncs = 0, nmethods = 0, nmodules = 0;
    int nclasses = 0, nvars = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Function", &funcs, &nfuncs);
    cbm_gbuf_find_by_label(ctx->gbuf, "Method", &methods, &nmethods);
    cbm_gbuf_find_by_label(ctx->gbuf, "Module", &modules, &nmodules);
    cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &nclasses);
    cbm_gbuf_find_by_label(ctx->gbuf, "Variable", &vars, &nvars);

    /* 3. Process all nodes */
    for (int i = 0; i < nfuncs; i++) {
        process_node(ctx, funcs[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nmethods; i++) {
        process_node(ctx, methods[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nmodules; i++) {
        process_node(ctx, modules[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nclasses; i++) {
        process_node(ctx, classes[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nvars; i++) {
        process_node(ctx, vars[i], producers, &prod_count, consumers, &cons_count);
    }

    cbm_log_info("servicelink.trpc.discovery",
                 "producers", itoa_trpc(prod_count),
                 "consumers", itoa_trpc(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "trpc",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "trpc",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Best-match: find best matching producer for each consumer */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];
        double best_conf = 0.0;
        int best_pi = -1;

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            double conf = match_procedure_path(c->identifier, p->identifier);
            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }
        }

        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            const cbm_sl_producer_t *p = &producers[best_pi];
            sl_insert_edge(ctx, c->handler_id, p->source_id,
                          SL_EDGE_TRPC, c->identifier, best_conf, NULL);
            link_count++;
        }
    }

    cbm_log_info("servicelink.trpc.done", "links", itoa_trpc(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
