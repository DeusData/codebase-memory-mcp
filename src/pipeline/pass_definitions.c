/*
 * pass_definitions.c — Extract definitions from source files.
 *
 * For each discovered file:
 *   1. Read source content from disk
 *   2. Call cbm_extract_file() to get defs, calls, imports
 *   3. Create Function/Class/Method/Variable/Module nodes in graph buffer
 *   4. Register callables in the function registry
 *   5. Store import maps and call sites for later passes
 *
 * Depends on: extraction layer (cbm.h), graph_buffer, pipeline internals
 */
#include "foundation/constants.h"

enum { PD_RING = 4, PD_RING_MASK = 3, PD_JSON_MARGIN = 10, PD_ESC_MARGIN = 3, PD_ESC_SPACE = 2 };
/* Fixed bytes around a serialized JSON field: ,"key":"value" / ,"key":[...]
 * -> comma + 2 key quotes + colon + 2 value quotes (resp. brackets). */
enum { PD_JSON_FIELD_OVERHEAD = 6 };
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/limits.h"
#include "cbm.h"
#include "simhash/minhash.h"
#include "semantic/ast_profile.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read entire file into heap-allocated buffer. Returns NULL on error.
 * Caller must free(). Sets *out_len to byte count. *out_size receives the
 * on-disk size and *out_status the failure reason, so the caller can attribute
 * a skip to the right phase/reason (read vs oversized) instead of a silent
 * drop. Both out params may be NULL. */
static char *read_file(const char *path, int *out_len, long *out_size,
                       cbm_read_status_t *out_status) {
    if (out_size) {
        *out_size = 0;
    }
    if (out_status) {
        *out_status = CBM_READ_OK;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        if (out_status) {
            *out_status = CBM_READ_OPEN_FAIL;
        }
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (out_size) {
        *out_size = size;
    }

    if (size <= 0) {
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_EMPTY;
        }
        return NULL;
    }
    if (size > cbm_max_file_bytes()) { /* generous, env-configurable cap (B4) */
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_OVERSIZED;
        }
        return NULL;
    }

    /* +16 padding: tree-sitter's lexer peeks a few bytes past the final UTF-8
     * character when computing lookahead, reading beyond the logical end.
     * Over-allocate and zero the tail so that read stays in-bounds (ASan
     * flags it as a heap-buffer-overflow otherwise; harmless but real UB). */
    enum { CBM_TS_LOOKAHEAD_PAD = 16 };
    char *buf = malloc((size_t)size + CBM_TS_LOOKAHEAD_PAD);
    if (!buf) {
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_OOM;
        }
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

/* Format int to string for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[PD_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PD_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos.
 * Writes: ,"key":"escaped_value"
 * Handles: \, ", \n, \r, \t */
static int def_json_escape_char(char *buf, size_t avail, char ch) {
    char esc = 0;
    switch (ch) {
    case '"':
        esc = '"';
        break;
    case '\\':
        esc = '\\';
        break;
    case '\n':
        esc = 'n';
        break;
    case '\r':
        esc = 'r';
        break;
    case '\t':
        esc = 't';
        break;
    default:
        if (avail >= SKIP_ONE) {
            /* Any other raw control byte (e.g. form feed) is invalid inside a
             * JSON string — degrade to a space. */
            buf[0] = ((unsigned char)ch < 0x20) ? ' ' : ch;
        }
        return SKIP_ONE;
    }
    if (avail >= PD_ESC_SPACE) {
        buf[0] = '\\';
        buf[SKIP_ONE] = esc;
    }
    return PD_ESC_SPACE;
}

/* Escaped length of a string under def_json_escape_char's rules: escaped
 * characters expand to 2 bytes, everything else stays 1. */
static size_t def_json_escaped_len(const char *s) {
    size_t n = 0;
    for (; *s; s++) {
        switch (*s) {
        case '"':
        case '\\':
        case '\n':
        case '\r':
        case '\t':
            n += PD_ESC_SPACE;
            break;
        default:
            n += SKIP_ONE;
        }
    }
    return n;
}

/* Appends are ATOMIC: a field is emitted only if the WHOLE serialized form
 * fits (with PD_ESC_SPACE bytes reserved for the closing '}' + NUL). Cutting a
 * field mid-value produced unterminated strings/arrays — malformed properties
 * JSON that aborts every json_extract()-based consumer downstream (seen on the
 * Linux kernel: 50-param functions truncated at the 2 KB cap). Dropping an
 * oversized optional field whole keeps the JSON valid. */
static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || val[0] == '\0') {
        return;
    }
    /* ,"key":"<escaped>" — comma + 2 key quotes + colon + 2 value quotes */
    size_t required = strlen(key) + def_json_escaped_len(val) + PD_JSON_FIELD_OVERHEAD;
    if (*pos + required + PD_ESC_SPACE > bufsize) {
        return; /* whole field would not fit — skip it atomically */
    }
    size_t p = *pos;
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;
    for (const char *s = val; *s && p < bufsize - PD_ESC_MARGIN; s++) {
        p += (size_t)def_json_escape_char(buf + p, bufsize - p - PD_ESC_SPACE, *s);
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Append a JSON array of strings: ,"key":["a","b","c"]. Atomic like
 * append_json_string: emitted only if the whole array fits. */
static void append_json_str_array(char *buf, size_t bufsize, size_t *pos, const char *key,
                                  const char **arr) {
    if (!arr || !arr[0] || *pos >= bufsize - PD_JSON_MARGIN) {
        return;
    }
    /* ,"key":[ + per item "<escaped>" + separating commas + ] */
    size_t required = strlen(key) + PD_JSON_FIELD_OVERHEAD;
    for (int i = 0; arr[i]; i++) {
        required += def_json_escaped_len(arr[i]) + PD_ESC_SPACE + (i > 0 ? SKIP_ONE : 0);
    }
    if (*pos + required + PD_ESC_SPACE > bufsize) {
        return; /* whole array would not fit — skip it atomically */
    }
    size_t p = *pos;
    int n = snprintf(buf + p, bufsize - p, ",\"%s\":[", key);
    if (n <= 0 || p + (size_t)n >= bufsize - PD_ESC_SPACE) {
        return;
    }
    p += (size_t)n;
    for (int i = 0; arr[i]; i++) {
        if (i > 0 && p < bufsize - SKIP_ONE) {
            buf[p++] = ',';
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
        /* Full escaping (not just quote/backslash): items like C param types
         * sliced from multi-line declarations carry raw \n/\t bytes, which are
         * invalid inside JSON strings. */
        for (const char *s = arr[i]; *s && p < bufsize - PD_ESC_SPACE; s++) {
            p += (size_t)def_json_escape_char(buf + p, bufsize - p - PD_ESC_SPACE, *s);
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = ']';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Build properties JSON for a definition node. */
static void build_def_props(char *buf, size_t bufsize, const CBMDefinition *def) {
    /* The complexity/loop/recursion metrics are only meaningful for executable
     * units (Function/Method). Emitting them on the millions of Macro/Field/
     * Variable/Class/Enum nodes — where they are always zero — bloats every
     * node's properties (~150 B), inflating RAM, the gbuf merge copy and the
     * dump. Gate the block to functions; other labels keep the lean base. */
    const bool is_fn =
        def->label && (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0);
    int n;
    if (is_fn) {
        n = snprintf(buf, bufsize,
                     "{\"complexity\":%d,\"cognitive\":%d,\"loop_count\":%d,\"loop_depth\":%d,"
                     "\"self_recursive\":%s,\"param_count\":%d,\"max_access_depth\":%d,"
                     "\"linear_scan_in_loop\":%d,\"alloc_in_loop\":%d,\"recursion_in_loop\":%s,"
                     "\"unguarded_recursion\":%s,"
                     "\"lines\":%d,\"is_exported\":%s,\"is_test\":%s,\"is_entry_point\":%s",
                     def->complexity, def->cognitive, def->loop_count, def->loop_depth,
                     def->is_recursive ? "true" : "false", def->param_count, def->max_access_depth,
                     def->linear_scan_in_loop, def->alloc_in_loop,
                     def->recursion_in_loop ? "true" : "false",
                     def->unguarded_recursion ? "true" : "false", def->lines,
                     def->is_exported ? "true" : "false", def->is_test ? "true" : "false",
                     def->is_entry_point ? "true" : "false");
    } else {
        n = snprintf(buf, bufsize,
                     "{\"complexity\":%d,\"lines\":%d,\"is_exported\":%s,\"is_test\":%s,"
                     "\"is_entry_point\":%s",
                     def->complexity, def->lines, def->is_exported ? "true" : "false",
                     def->is_test ? "true" : "false", def->is_entry_point ? "true" : "false");
    }

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
    append_json_string(buf, bufsize, &pos, "route_path", def->route_path);
    append_json_string(buf, bufsize, &pos, "route_method", def->route_method);

    /* MinHash fingerprint — append if present and buffer has room. */
    if (def->fingerprint && def->fingerprint_k > 0 &&
        pos + CBM_MINHASH_HEX_LEN + CBM_MINHASH_JSON_OVERHEAD < bufsize) {
        char fp_hex[CBM_MINHASH_HEX_BUF];
        cbm_minhash_to_hex((const cbm_minhash_t *)def->fingerprint, fp_hex, sizeof(fp_hex));
        append_json_string(buf, bufsize, &pos, "fp", fp_hex);
    }

    /* AST structural profile */
    if (def->structural_profile && pos + CBM_AST_PROFILE_BUF < bufsize) {
        append_json_string(buf, bufsize, &pos, "sp", def->structural_profile);
    }

    /* Body tokens */
    if (def->body_tokens && pos + CBM_SZ_512 < bufsize) {
        append_json_string(buf, bufsize, &pos, "bt", def->body_tokens);
    }

    if (pos < bufsize - SKIP_ONE) {
        buf[pos] = '}';
        buf[pos + SKIP_ONE] = '\0';
    }
}

/* Process one definition: create node, register, DEFINES + DEFINES_METHOD edges. */
static void process_def(cbm_pipeline_ctx_t *ctx, const CBMDefinition *def, const char *rel) {
    if (!def->qualified_name || !def->name) {
        return;
    }
    char props[CBM_SZ_2K];
    build_def_props(props, sizeof(props), def);
    int64_t node_id = cbm_gbuf_upsert_node(
        ctx->gbuf, def->label ? def->label : "Function", def->name, def->qualified_name,
        def->file_path ? def->file_path : rel, (int)def->start_line, (int)def->end_line, props);
    /* Register callable symbols + every type-like container (Class/Struct/
     * Interface/Enum/Type/Trait). Type-like defs must be in the registry so
     * `class Foo : IBar` (INHERITS), `impl Trait for S` (IMPLEMENTS), and method/
     * field resolution can reach them — Struct included so Rust/Go/Swift/D structs
     * resolve as type targets just as a Class did. Variable/Field defs are also
     * registered so pass_usages.c can resolve READS/WRITES accesses (rw->var_name)
     * to a Variable/Field node QN.
     * KEEP IN SYNC with pass_parallel.c and pipeline_incremental.c's seed sets. */
    if (node_id > 0 && def->label &&
        (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
         cbm_label_is_type_like(def->label) || strcmp(def->label, "Variable") == 0 ||
         strcmp(def->label, "Field") == 0)) {
        cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
    }
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    if (file_node && node_id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
    }
    free(file_qn);
    if (def->parent_class && def->label && strcmp(def->label, "Method") == 0) {
        const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
        if (parent && node_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, parent->id, node_id, "DEFINES_METHOD", "{}");
        }
    }
}

/* Create Channel nodes + EMITS / LISTENS_ON edges for one file's channels.
 * Mirrors the parallel path in cbm_build_registry_from_cache — keep in sync. */
/* Find the source node for a channel edge: enclosing function or file node. */
static const cbm_gbuf_node_t *find_channel_source(cbm_pipeline_ctx_t *ctx, const CBMChannel *ch,
                                                  const char *rel) {
    const cbm_gbuf_node_t *node = NULL;
    if (ch->enclosing_func_qn && ch->enclosing_func_qn[0]) {
        node = cbm_gbuf_find_by_qn(ctx->gbuf, ch->enclosing_func_qn);
    }
    if (!node) {
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        free(file_qn);
    }
    return node;
}

static void create_channel_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                          const char *rel) {
    for (int j = 0; j < result->channels.count; j++) {
        const CBMChannel *ch = &result->channels.items[j];
        if (!ch->channel_name || !ch->channel_name[0]) {
            continue;
        }
        char channel_qn[CBM_SZ_512];
        snprintf(channel_qn, sizeof(channel_qn), "__channel__%s__%s",
                 ch->transport ? ch->transport : "unknown", ch->channel_name);
        char channel_props[CBM_SZ_512];
        snprintf(channel_props, sizeof(channel_props), "{\"transport\":\"%s\",\"name\":\"%s\"}",
                 ch->transport ? ch->transport : "unknown", ch->channel_name);
        int64_t channel_id = cbm_gbuf_upsert_node(ctx->gbuf, "Channel", ch->channel_name,
                                                  channel_qn, "", 0, 0, channel_props);

        const cbm_gbuf_node_t *src_node = find_channel_source(ctx, ch, rel);
        if (src_node && channel_id > 0) {
            const char *edge_type = ch->direction == CBM_CHANNEL_EMIT ? "EMITS" : "LISTENS_ON";
            char edge_props[CBM_SZ_128];
            snprintf(edge_props, sizeof(edge_props), "{\"transport\":\"%s\"}",
                     ch->transport ? ch->transport : "unknown");
            cbm_gbuf_insert_edge(ctx->gbuf, src_node->id, channel_id, edge_type, edge_props);
        }
    }
}

/* Create CONFIGURES edges for one file's env accesses.  extract_env_accesses.c
 * records every os.Getenv / process.env / Environment.GetEnvironmentVariable
 * style access into result->env_accesses.  We materialize one EnvVar node per
 * env key and link the enclosing function (or the file node) CONFIGURES-> it,
 * so environment-driven configuration is visible even when the accessor is a
 * stdlib symbol that never resolves to an in-graph callee. */
static int create_env_configures_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                          const char *rel) {
    int count = 0;
    char *file_qn = NULL;
    const cbm_gbuf_node_t *file_node = NULL;
    for (int j = 0; j < result->env_accesses.count; j++) {
        const CBMEnvAccess *ea = &result->env_accesses.items[j];
        if (!ea->env_key || !ea->env_key[0]) {
            continue;
        }
        char env_qn[CBM_SZ_512];
        snprintf(env_qn, sizeof(env_qn), "__env__%s", ea->env_key);
        char env_props[CBM_SZ_512];
        snprintf(env_props, sizeof(env_props), "{\"env_key\":\"%s\"}", ea->env_key);
        int64_t env_id =
            cbm_gbuf_upsert_node(ctx->gbuf, "EnvVar", ea->env_key, env_qn, "", 0, 0, env_props);
        if (env_id <= 0) {
            continue;
        }
        const cbm_gbuf_node_t *src = NULL;
        if (ea->enclosing_func_qn && ea->enclosing_func_qn[0]) {
            src = cbm_gbuf_find_by_qn(ctx->gbuf, ea->enclosing_func_qn);
        }
        if (!src) {
            if (!file_qn) {
                file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
                file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
            }
            src = file_node;
        }
        if (src && src->id != env_id) {
            cbm_gbuf_insert_edge(ctx->gbuf, src->id, env_id, "CONFIGURES",
                                 "{\"strategy\":\"env_access\"}");
            count++;
        }
    }
    free(file_qn);
    return count;
}

/* Create IMPORTS edges for one file's imports.  Mirrors the resolution
 * logic in pass_parallel.c register_and_link_def — keep the two in sync. */
static int create_import_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                        const char *rel, CBMHashTable *namespace_map) {
    int count = 0;
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    if (!source_node) {
        free(file_qn);
        return 0;
    }
    for (int j = 0; j < result->imports.count; j++) {
        const CBMImport *imp = &result->imports.items[j];
        if (!imp->module_path) {
            continue;
        }
        const cbm_gbuf_node_t *target =
            cbm_pipeline_resolve_import_node(ctx, rel, file_qn, imp, namespace_map);
        if (target && target->id != source_node->id) {
            char imp_props[CBM_SZ_256];
            snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}",
                     imp->local_name ? imp->local_name : "");
            cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
            count++;
        }
    }
    free(file_qn);
    return count;
}

int cbm_pipeline_pass_definitions(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count) {
    cbm_log_info("pass.start", "pass", "definitions", "files", itoa_log(file_count));

    /* Ensure extraction library is initialized */
    cbm_init();

    /* Defensive: a prior pipeline run may have left a thread-local parser whose
     * lexer holds pointers into a slab that has since been reclaimed. Drop it
     * here so the first cbm_extract_file below recreates a fresh parser. */
    cbm_destroy_thread_parser();

    int total_defs = 0;
    int total_calls = 0;
    int total_imports = 0;
    int errors = 0;

    /* Sequential pass must extract all defs (which create Module/Function/...
     * nodes) BEFORE resolving imports — otherwise a workspace import in the
     * first file processed can't find the target Module node, because the
     * target file's defs haven't been extracted yet. Result cache is
     * required for this two-phase ordering. */
    CBMFileResult **local_cache = ctx->result_cache;
    bool owns_local_cache = false;
    if (!local_cache) {
        local_cache = (CBMFileResult **)calloc((size_t)file_count, sizeof(CBMFileResult *));
        owns_local_cache = (local_cache != NULL);
    }

    /* Phase 1: extract every file and create def-derived nodes (Modules,
     * Functions, ...) so any file's IMPORTS can resolve against the
     * complete in-memory graph in Phase 2. */
    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return CBM_NOT_FOUND;
        }

        const char *path = files[i].path;
        const char *rel = files[i].rel_path;
        CBMLanguage lang = files[i].language;

        /* Read source file */
        int source_len = 0;
        long file_size = 0;
        cbm_read_status_t rst = CBM_READ_OK;
        char *source = read_file(path, &source_len, &file_size, &rst);
        if (!source) {
            errors++;
            if (rst == CBM_READ_OVERSIZED) {
                /* Never a silent drop: record the oversized skip + WARN so the
                 * file surfaces in the response/logfile with its sizes. */
                long cap = cbm_max_file_bytes();
                char reason[96];
                snprintf(reason, sizeof(reason), "oversized (%lld MB > %lld MB)",
                         (long long)(file_size / (CBM_SZ_1K * CBM_SZ_1K)),
                         (long long)(cap / (CBM_SZ_1K * CBM_SZ_1K)));
                cbm_pipeline_add_file_error(ctx->pipeline, rel, reason, "oversized");
                cbm_log_warn("index.file_oversized", "path", rel, "size_mb",
                             itoa_log((int)(file_size / (CBM_SZ_1K * CBM_SZ_1K))), "cap_mb",
                             itoa_log((int)(cap / (CBM_SZ_1K * CBM_SZ_1K))));
            } else if (rst == CBM_READ_OPEN_FAIL || rst == CBM_READ_OOM) {
                cbm_pipeline_add_file_error(ctx->pipeline, rel, "read failed", "read");
            }
            /* CBM_READ_EMPTY: benign 0-byte file — nothing to index, not reported. */
            continue;
        }

        /* Extract */
        CBMFileResult *result =
            cbm_extract_file(source, source_len, lang, ctx->project_name, rel, CBM_EXTRACT_BUDGET,
                             NULL, NULL /* no extra defines or include paths */
            );
        free(source);

        if (!result) {
            errors++;
            cbm_pipeline_add_file_error(ctx->pipeline, rel, "extract failed", "extract");
            continue;
        }
        /* Consume the previously-ignored has_error flag: a parse timeout /
         * parse failure / unsupported-grammar result carries no defs but must
         * still be reported (phase "extract", reason = the extractor's message).
         * The empty result flows through unchanged (the defs loop is a no-op). */
        if (result->has_error) {
            cbm_pipeline_add_file_error(ctx->pipeline, rel,
                                        result->error_msg ? result->error_msg : "extract failed",
                                        "extract");
            errors++;
        }

        /* Create nodes for each definition */
        for (int d = 0; d < result->defs.count; d++) {
            process_def(ctx, &result->defs.items[d], rel);
            total_defs++;
        }

        /* Store calls for pass_calls (we save them in the extraction results
         * for now — a future optimization would batch these) */
        total_calls += result->calls.count;

        if (local_cache) {
            local_cache[i] = result;
        } else {
            /* Cache unavailable: imports for this file can still only
             * resolve to defs already in the graph, but the file's
             * own defs are now persisted before the lookup. No namespace
             * map is available without the cache (single-file scope). */
            total_imports += create_import_edges_for_file(ctx, result, rel, NULL);
            create_channel_edges_for_file(ctx, result, rel);
            create_env_configures_for_file(ctx, result, rel);
            cbm_free_result(result);
        }
    }

    /* Phase 2: now that all extraction results are cached and Module
     * nodes for every file are in the graph, walk the cache again to
     * create IMPORTS / channel edges. Imports resolve against the full
     * project graph. */
    if (local_cache) {
        /* Build a namespace/package → File-QN map so that namespace imports
         * (C# `using`, Java/Kotlin `import`, PHP `use`) resolve to the file
         * that declares the namespace. */
        const char **rels = (const char **)calloc((size_t)file_count, sizeof(char *));
        if (rels) {
            for (int i = 0; i < file_count; i++) {
                rels[i] = files[i].rel_path;
            }
        }
        CBMHashTable *namespace_map =
            cbm_pipeline_namespace_map_build(ctx->project_name, local_cache, rels, file_count);
        free(rels);
        for (int i = 0; i < file_count; i++) {
            if (cbm_pipeline_check_cancel(ctx)) {
                break;
            }
            CBMFileResult *result = local_cache[i];
            if (!result) {
                continue;
            }
            total_imports +=
                create_import_edges_for_file(ctx, result, files[i].rel_path, namespace_map);
            create_channel_edges_for_file(ctx, result, files[i].rel_path);
            create_env_configures_for_file(ctx, result, files[i].rel_path);
        }
        cbm_pipeline_namespace_map_free(namespace_map);
        if (owns_local_cache) {
            for (int i = 0; i < file_count; i++) {
                if (local_cache[i]) {
                    cbm_free_result(local_cache[i]);
                }
            }
            free(local_cache);
        }
    }

    cbm_log_info("pass.done", "pass", "definitions", "defs", itoa_log(total_defs), "calls",
                 itoa_log(total_calls), "imports", itoa_log(total_imports), "errors",
                 itoa_log(errors));
    return 0;
}
