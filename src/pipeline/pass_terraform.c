/*
 * pass_terraform.c — the Terraform / OpenTofu infrastructure layer.
 *
 * PHASE 3 P3.4: a `resource "helm_release"` DEPLOYS its chart -- an edge from
 * the resource block (its node `resource.helm_release.<name>`) to existing
 * nodes only:
 *   chart = "./charts/x" / "${path.module}/charts/x"   the Chart of that
 *                                                      directory, relative to
 *                                                      the module's directory
 *   repository = "<url>", chart = "<name>"             the shared external
 *   chart = "oci://registry/.../<name>"                Chart of that name
 * Values are evaluated from string literals and ${path.module}; any other
 * expression is unknown, and an unknown chart is no edge. Run at the end of
 * the K8s pass, over the same files.
 */
#include "foundation/constants.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Blocks a helm_release lookup examined (complexity guard counter). */
_Atomic uint64_t g_tf_candidates = 0;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *arena;      /* lookup QNs */
    CBMHashTable *charts; /* Chart.yaml path -> local Chart node id */
    const char *src;
    const char *file;
    char mdir[CBM_SZ_1K]; /* the module's directory */
    int edges;
} tf_t;

static bool tf_type(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

static const char *tf_basename(const char *p) {
    const char *s = strrchr(p, '/');
    return s ? s + SKIP_ONE : p;
}

/* Append [p, p+n) to out (size sz); false when it does not fit. */
static bool tf_add(char *out, size_t sz, size_t *len, const char *p, size_t n) {
    if (*len + n + SKIP_ONE > sz) {
        return false;
    }
    memcpy(out + *len, p, n);
    *len += n;
    out[*len] = '\0';
    return true;
}

/* A string value: a literal, or a template of literals and ${path.module}.
 * false when any part is something else. */
static bool tf_string(const tf_t *t, TSNode n, char *out, size_t sz) {
    size_t len = 0;
    out[0] = '\0';
    while (tf_type(n, "expression") || tf_type(n, "literal_value") || tf_type(n, "template_expr") ||
           tf_type(n, "string_lit") || tf_type(n, "quoted_template")) {
        if (tf_type(n, "string_lit") || tf_type(n, "quoted_template")) {
            break;
        }
        if (ts_node_named_child_count(n) != SKIP_ONE) {
            return false;
        }
        n = ts_node_named_child(n, 0);
    }
    if (!tf_type(n, "string_lit") && !tf_type(n, "quoted_template")) {
        return false;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        uint32_t b = ts_node_start_byte(c);
        uint32_t e = ts_node_end_byte(c);
        if (tf_type(c, "quoted_template_start") || tf_type(c, "quoted_template_end")) {
            continue;
        }
        if (tf_type(c, "template_literal")) {
            if (!tf_add(out, sz, &len, t->src + b, e - b)) {
                return false;
            }
            continue;
        }
        if (!tf_type(c, "template_interpolation")) {
            return false;
        }
        /* ${path.module} only */
        TSNode expr = {0};
        for (uint32_t j = 0; j < ts_node_named_child_count(c); j++) {
            if (tf_type(ts_node_named_child(c, j), "expression")) {
                expr = ts_node_named_child(c, j);
            }
        }
        if (ts_node_is_null(expr)) {
            return false;
        }
        uint32_t xb = ts_node_start_byte(expr);
        uint32_t xe = ts_node_end_byte(expr);
        static const char PATH_MODULE[] = "path.module";
        if (xe - xb != sizeof(PATH_MODULE) - SKIP_ONE ||
            strncmp(t->src + xb, PATH_MODULE, xe - xb) != 0) {
            return false;
        }
        /* an absolute marker: the module directory, from the repo root */
        if (!tf_add(out, sz, &len, "\x01", SKIP_ONE) ||
            !tf_add(out, sz, &len, t->mdir, strlen(t->mdir))) {
            return false;
        }
    }
    return true;
}

/* A path inside the module directory: "./a/../b" -> "<mdir>/b"; NULL past
 * the repository root. A value starting with the \x01 marker is already
 * from the root. */
static bool tf_repo_path(const char *mdir, const char *v, char *out, size_t sz) {
    bool anchored = v[0] == '\x01';
    const char *rel = anchored ? v + SKIP_ONE : v;
    char buf[CBM_SZ_2K];
    if (anchored) {
        snprintf(buf, sizeof(buf), "%s", rel);
    } else {
        snprintf(buf, sizeof(buf), "%s%s%s", mdir, mdir[0] ? "/" : "", rel);
    }
    size_t n = 0;
    out[0] = '\0';
    for (const char *p = buf; *p;) {
        while (*p == '/') {
            p++;
        }
        size_t sl = strcspn(p, "/");
        if (sl == 0) {
            break;
        }
        if (sl == SKIP_ONE && p[0] == '.') {
            p += sl;
            continue;
        }
        if (sl == PAIR_LEN && p[0] == '.' && p[SKIP_ONE] == '.') {
            if (n == 0) {
                return false;
            }
            while (n > 0 && out[n - SKIP_ONE] != '/') {
                n--;
            }
            if (n > 0) {
                n--;
            }
            out[n] = '\0';
            p += sl;
            continue;
        }
        if (n + sl + PAIR_LEN > sz) {
            return false;
        }
        if (n) {
            out[n++] = '/';
        }
        memcpy(out + n, p, sl);
        n += sl;
        out[n] = '\0';
        p += sl;
    }
    return true;
}

/* Chart.yaml path -> its (local) Chart node. */
static CBMHashTable *tf_chart_index(cbm_pipeline_ctx_t *ctx) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (!ix || cbm_gbuf_find_by_label(ctx->gbuf, "Chart", &nodes, &count) != 0) {
        return ix;
    }
    for (int i = 0; i < count; i++) {
        const char *fp = nodes[i]->file_path;
        const char *qn = nodes[i]->qualified_name;
        const char *base = fp ? tf_basename(fp) : "";
        if (fp && qn && !strstr(qn, ".__helm_dep__.") &&
            (strcmp(base, "Chart.yaml") == 0 || strcmp(base, "Chart.yml") == 0)) {
            cbm_ht_set(ix, fp, (void *)(intptr_t)nodes[i]->id);
        }
    }
    return ix;
}

/* The Chart node of repository chart directory `dir`, 0 when none. */
static int64_t tf_local_chart(const tf_t *t, const char *dir) {
    static const char *const names[] = {"Chart.yaml", "Chart.yml"};
    for (size_t k = 0; t->charts && k < sizeof(names) / sizeof(names[0]); k++) {
        char want[CBM_SZ_2K];
        snprintf(want, sizeof(want), "%s%s%s", dir, dir[0] ? "/" : "", names[k]);
        int64_t id = (int64_t)(intptr_t)cbm_ht_get(t->charts, want);
        if (id > 0) {
            return id;
        }
    }
    return 0;
}

/* The shared external Chart of `name` (the one chart dependencies use). */
static int64_t tf_external_chart(cbm_pipeline_ctx_t *ctx, const char *name, const char *file) {
    char qn[CBM_SZ_512];
    snprintf(qn, sizeof(qn), "%s.__helm_dep__.%s", ctx->project_name, name);
    const cbm_gbuf_node_t *known = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
    if (known) {
        return known->id;
    }
    return cbm_gbuf_upsert_node(ctx->gbuf, "Chart", name, qn, file, SKIP_ONE, 0,
                                "{\"source\":\"helm\",\"external\":true}");
}

/* The node of block `resource.helm_release.<name>` in the file, by the QN
 * its definition got -- not among every block of that name: modules that
 * share a release name (`this`) would make each lookup scan all of them. */
static int64_t tf_block_node(const tf_t *t, const char *name) {
    char full[CBM_SZ_512];
    snprintf(full, sizeof(full), "resource.helm_release.%s", name);
    const char *qn = cbm_fqn_compute(t->arena, t->ctx->project_name, t->file, full);
    const cbm_gbuf_node_t *n = qn ? cbm_gbuf_find_by_qn(t->ctx->gbuf, qn) : NULL;
    atomic_fetch_add_explicit(&g_tf_candidates, SKIP_ONE, memory_order_relaxed);
    return n && n->file_path && strcmp(n->file_path, t->file) == 0 ? n->id : 0;
}

static bool tf_plain_name(const char *s) {
    if (!s[0]) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '-' || *p == '_' || *p == '.')) {
            return false;
        }
    }
    return true;
}

/* One `resource "helm_release" "<name>" { ... }` block. */
static void tf_helm_release(tf_t *t, TSNode block, const char *name) {
    TSNode body = {0};
    for (uint32_t i = 0; i < ts_node_named_child_count(block); i++) {
        if (tf_type(ts_node_named_child(block, i), "body")) {
            body = ts_node_named_child(block, i);
        }
    }
    if (ts_node_is_null(body)) {
        return;
    }
    char chart[CBM_SZ_1K] = "";
    bool has_chart = false;
    bool has_repo = false; /* a repository, even of unknown value: not a local chart */
    for (uint32_t i = 0; i < ts_node_named_child_count(body); i++) {
        TSNode a = ts_node_named_child(body, i);
        if (!tf_type(a, "attribute") || ts_node_named_child_count(a) < PAIR_LEN) {
            continue;
        }
        TSNode key = ts_node_named_child(a, 0);
        TSNode val = ts_node_named_child(a, SKIP_ONE);
        uint32_t kb = ts_node_start_byte(key);
        uint32_t ke = ts_node_end_byte(key);
        size_t kl = ke - kb;
        if (kl == strlen("chart") && strncmp(t->src + kb, "chart", kl) == 0) {
            has_chart = tf_string(t, val, chart, sizeof(chart));
        } else if (kl == strlen("repository") && strncmp(t->src + kb, "repository", kl) == 0) {
            has_repo = true;
        }
    }
    if (!has_chart) {
        return;
    }
    int64_t src = tf_block_node(t, name);
    int64_t tgt = 0;
    bool external = false;
    char path[CBM_SZ_1K] = "";
    static const char OCI[] = "oci://";
    if (strncmp(chart, OCI, sizeof(OCI) - SKIP_ONE) == 0) {
        const char *base = tf_basename(chart);
        if (tf_plain_name(base)) {
            snprintf(path, sizeof(path), "%s", base);
            external = true;
        }
    } else if (has_repo) {
        if (tf_plain_name(chart)) {
            snprintf(path, sizeof(path), "%s", chart);
            external = true;
        }
    } else if (tf_repo_path(t->mdir, chart, path, sizeof(path))) {
        tgt = tf_local_chart(t, path);
    }
    if (external) {
        tgt = tf_external_chart(t->ctx, path, t->file);
    }
    if (src <= 0 || tgt <= 0) {
        return;
    }
    char esc[CBM_SZ_1K];
    char props[CBM_SZ_2K];
    cbm_json_escape(esc, (int)sizeof(esc), path);
    snprintf(props, sizeof(props), "{\"via\":\"terraform\",\"chart\":\"%s\"%s,\"line\":%d}", esc,
             external ? ",\"external\":true" : "", (int)ts_node_start_point(block).row + SKIP_ONE);
    cbm_gbuf_insert_edge(t->ctx->gbuf, src, tgt, "DEPLOYS", props);
    t->edges++;
}

/* The text of a string_lit label: "helm_release" -> helm_release. */
static bool tf_label(const tf_t *t, TSNode n, char *out, size_t sz) {
    if (!tf_type(n, "string_lit")) {
        return false;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        if (tf_type(c, "template_literal")) {
            uint32_t b = ts_node_start_byte(c);
            snprintf(out, sz, "%.*s", (int)(ts_node_end_byte(c) - b), t->src + b);
            return true;
        }
    }
    return false;
}

static void tf_file(tf_t *t, TSNode root) {
    TSNode body = {0}; /* comments are children of the file too */
    for (uint32_t i = 0; i < ts_node_named_child_count(root) && ts_node_is_null(body); i++) {
        if (tf_type(ts_node_named_child(root, i), "body")) {
            body = ts_node_named_child(root, i);
        }
    }
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode b = ts_node_named_child(body, i);
        if (!tf_type(b, "block") || ts_node_named_child_count(b) < 3) {
            continue;
        }
        TSNode kw = ts_node_named_child(b, 0);
        uint32_t kb = ts_node_start_byte(kw);
        uint32_t ke = ts_node_end_byte(kw);
        char type[CBM_SZ_128];
        char name[CBM_SZ_256];
        if (ke - kb != strlen("resource") || strncmp(t->src + kb, "resource", ke - kb) != 0 ||
            !tf_label(t, ts_node_named_child(b, SKIP_ONE), type, sizeof(type)) ||
            !tf_label(t, ts_node_named_child(b, PAIR_LEN), name, sizeof(name))) {
            continue;
        }
        if (strcmp(type, "helm_release") == 0) {
            tf_helm_release(t, b, name);
        }
    }
}

static char *tf_read(const char *path, size_t *len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > cbm_max_file_bytes()) {
        (void)fclose(f);
        return NULL;
    }
    enum { TF_TS_PAD = 16 }; /* tree-sitter's lexer reads past the end */
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + TF_TS_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t got = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    memset(buf + got, 0, TF_TS_PAD);
    *len = got;
    return buf;
}

static bool tf_is_module_file(const char *rel) {
    const char *dot = strrchr(tf_basename(rel), '.');
    return dot && (strcmp(dot, ".tf") == 0 || strcmp(dot, ".tofu") == 0);
}

void cbm_pipeline_pass_terraform(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                 int file_count) {
    TSParser *parser = NULL;
    CBMHashTable *charts = NULL;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    int edges = 0;
    for (int i = 0; i < file_count; i++) {
        if (files[i].language != CBM_LANG_HCL || !tf_is_module_file(files[i].rel_path)) {
            continue;
        }
        size_t len = 0;
        char *src = tf_read(files[i].path, &len);
        if (!src) {
            continue;
        }
        if (strstr(src, "helm_release")) {
            if (!parser) {
                parser = ts_parser_new();
                charts = tf_chart_index(ctx);
            }
            TSTree *tree = NULL;
            if (parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_HCL))) {
                tree = ts_parser_parse_string(parser, NULL, src, (uint32_t)len);
            }
            if (tree) {
                tf_t t = {.ctx = ctx,
                          .arena = &arena,
                          .charts = charts,
                          .src = src,
                          .file = files[i].rel_path};
                const char *slash = strrchr(files[i].rel_path, '/');
                snprintf(t.mdir, sizeof(t.mdir), "%.*s",
                         slash ? (int)(slash - files[i].rel_path) : 0, files[i].rel_path);
                tf_file(&t, ts_tree_root_node(tree));
                edges += t.edges;
                ts_tree_delete(tree);
            }
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    if (charts) {
        cbm_ht_free(charts);
    }
    cbm_arena_destroy(&arena);
    if (edges > 0) {
        char buf[CBM_SZ_32];
        snprintf(buf, sizeof(buf), "%d", edges);
        cbm_log_info("pass.terraform.deploys", "edges", buf);
    }
}
