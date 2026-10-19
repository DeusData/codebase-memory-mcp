/*
 * pass_terraform.c — the Terraform / OpenTofu infrastructure layer.
 *
 * PHASE 3 P3.4: a `resource "helm_release"` DEPLOYS its chart -- an edge from
 * the resource block (its node `resource.helm_release.<name>`) to existing
 * nodes only:
 *   chart = "./charts/x" / "${path.module}/charts/x"   the Chart of that
 *                                                      directory, relative to
 *                                                      the module's directory
 *   repository = "<url>", chart = "<name>"             the external Chart of
 *   chart = "oci://registry/.../<name>"                that name (an oci://
 *                                                      reference: of its URL)
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

/* A string value of file text `src` in module directory `mdir`: a literal, or
 * a template of literals and ${path.module}. false when any part is something
 * else. */
static bool tf_string(const char *src, const char *mdir, TSNode n, char *out, size_t sz) {
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
            if (!tf_add(out, sz, &len, src + b, e - b)) {
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
            strncmp(src + xb, PATH_MODULE, xe - xb) != 0) {
            return false;
        }
        /* an absolute marker: the module directory, from the repo root */
        if (!tf_add(out, sz, &len, "\x01", SKIP_ONE) ||
            !tf_add(out, sz, &len, mdir, strlen(mdir))) {
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

bool cbm_repo_rel_join(const char *dir, const char *v, char *out, size_t sz) {
    return tf_repo_path(dir, v, out, sz);
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
        /* an external chart a Chart.yaml depends on carries that file's
         * path too: only the chart it declares is the directory's */
        if (fp && qn && !cbm_pipeline_chart_is_external(nodes[i]) &&
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
            has_chart = tf_string(t->src, t->mdir, val, chart, sizeof(chart));
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
    /* the repository of an oci:// reference; "" when unknown (a
     * `repository =` value is not read) */
    char repo[CBM_SZ_1K] = "";
    char path[CBM_SZ_1K] = "";
    static const char OCI[] = "oci://";
    if (strncmp(chart, OCI, sizeof(OCI) - SKIP_ONE) == 0) {
        const char *base = tf_basename(chart);
        if (tf_plain_name(base)) {
            snprintf(path, sizeof(path), "%s", base);
            external = true;
            /* the reference names its repository: all but the chart */
            if (!cbm_chart_ref_repo(chart, repo, sizeof(repo))) {
                repo[0] = '\0';
            }
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
        tgt = cbm_pipeline_external_chart(t->ctx, path, repo[0] ? repo : NULL, t->file);
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

/* A variable-definitions file (.tfvars; a .tfvars.json is JSON, not read here). */
static bool tf_is_tfvars(const char *rel) {
    const char *dot = strrchr(tf_basename(rel), '.');
    return dot && strcmp(dot, ".tfvars") == 0;
}

/* The body of a config file's root node. */
static TSNode tf_root_body(TSNode root) {
    for (uint32_t i = 0; i < ts_node_named_child_count(root); i++) {
        if (tf_type(ts_node_named_child(root, i), "body")) {
            return ts_node_named_child(root, i);
        }
    }
    return (TSNode){0};
}

/* ── References (PHASE 6 P6.1 - P6.3) ─────────────────────────────── */

/* A Terraform module is a directory: every .tf / .tofu file of it. A
 * reference resolves inside its module only -- var.X to `variable "X"`,
 * local.X to the locals block declaring X, module.X to `module "X"`,
 * data.T.N to `data "T" "N"`, T.N to `resource "T" "N"` (T.N declared there);
 * anything else (each, count, self, path, terraform, a for-expression name, a
 * builtin function) to nothing. Edge: USAGE {ref, via: terraform, line} from
 * the top-level block holding the reference to the declaring block; an entry
 * of the block's depends_on list orders it after the object: DEPENDS_ON, the
 * same properties.
 *
 * A module call with a local source (`module "X" { source = "./net" }`; the
 * directory inside the repository, holding .tf / .tofu files) IMPORTS
 * {local_name: X, via: terraform, line} that directory's Folder, and
 * module.X.Y (also module.X[i].Y, module.X[*].Y) USAGEs {ref: output}, beside
 * module.X, the `output "Y"` of it.
 *
 * A .tfvars file sets variables of the module in its own directory -- or,
 * when that declares none of its names, of the nearest ancestor directory's
 * module that does (applied by a -var-file the repository does not state:
 * the edge says `inferred`): each assignment to a declared variable
 * OVERRIDEs {via: tfvars, line} its `variable` block. */

typedef struct {
    char *file;      /* the referencing file */
    char *src;       /* the holding top-level block's definition name */
    char *key;       /* what it names: variable.X, local.X, module.X, data.T.N, resource.T.N */
    char *out;       /* module.X.Y: Y, the output it reads; else NULL */
    const char *ref; /* static: var, local, module, data, resource */
    int line;
    bool dep; /* stands in the holder's depends_on list */
} tf_ref_t;

/* A module call with a local source. */
typedef struct {
    char *file; /* the calling file */
    char *name; /* its block: module.X */
    char *dir;  /* the source directory, from the repository root */
    int line;
} tf_call_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *arena;
    CBMHashTable *decls; /* key (as tf_ref_t.key) -> declaring file (arena) */
    CBMHashTable *calls; /* module.X -> its local source directory (arena) */
    tf_ref_t *refs;
    int ref_n;
    int ref_cap;
    tf_call_t *mcalls;
    int call_n;
    int call_cap;
    const char *dir;  /* the module's directory */
    const char *src;  /* the file's text */
    const char *file; /* its repository path (arena) */
    bool dep;         /* walking a depends_on list */
} tf_mod_t;

static const char TF_MODULE_PREFIX[] = "module.";

static const char *tf_text(CBMArena *a, const char *src, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    return cbm_arena_strndup(a, src + b, ts_node_end_byte(n) - b);
}

/* A block label: a string_lit's text, or an identifier (`resource aws_x y`). */
static const char *tf_block_label(CBMArena *a, const char *src, TSNode n) {
    if (tf_type(n, "identifier")) {
        return tf_text(a, src, n);
    }
    for (uint32_t i = 0; tf_type(n, "string_lit") && i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        if (tf_type(c, "template_literal")) {
            return tf_text(a, src, c);
        }
    }
    return NULL;
}

/* A top-level block's definition name: its keyword and labels joined by
 * "." (variable.X, resource.T.N, locals) -- the name extract_defs gives it. */
static const char *tf_block_name(CBMArena *a, const char *src, TSNode block, TSNode *body) {
    const char *name = NULL;
    *body = (TSNode){0};
    for (uint32_t i = 0; i < ts_node_named_child_count(block); i++) {
        TSNode c = ts_node_named_child(block, i);
        if (tf_type(c, "body")) {
            *body = c;
        } else if (i == 0 && tf_type(c, "identifier")) {
            name = tf_text(a, src, c);
        } else if (name) {
            const char *label = tf_block_label(a, src, c);
            name = label ? cbm_arena_sprintf(a, "%s.%s", name, label) : name;
        }
    }
    return name;
}

/* Room for one more element in a growing array. */
static bool tf_grow(void **items, int *cap, int n, size_t size) {
    if (n < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_64;
    void *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, *items, (size_t)ncap * size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = ncap;
    return true;
}

static void tf_ref_push(tf_mod_t *m, const char *src_block, const char *ref, const char *key,
                        const char *out, int line) {
    if (tf_grow((void **)&m->refs, &m->ref_cap, m->ref_n, sizeof(*m->refs))) {
        m->refs[m->ref_n++] = (tf_ref_t){
            (char *)m->file, (char *)src_block, (char *)key, (char *)out, ref, line, m->dep};
    }
}

/* The identifier of a get_attr node. */
static const char *tf_attr_name(CBMArena *a, const char *src, TSNode g) {
    for (uint32_t c = 0; c < ts_node_named_child_count(g); c++) {
        if (tf_type(ts_node_named_child(g, c), "identifier")) {
            return tf_text(a, src, ts_node_named_child(g, c));
        }
    }
    return NULL;
}

/* The k-th attribute name of the traversal `v` starts. Its siblings, once the
 * grammar's hidden expression rule is flattened, are get_attr / index /
 * splat nodes: an index ([0], ["a"]) is passed over, a splat's own
 * attributes ([*].y, .*.y) count. NULL past the attributes. */
static const char *tf_attr(CBMArena *a, const char *src, TSNode v, int k) {
    int i = 0;
    for (TSNode s = ts_node_next_named_sibling(v); !ts_node_is_null(s);
         s = ts_node_next_named_sibling(s)) {
        if (tf_type(s, "index")) {
            continue;
        }
        if (tf_type(s, "splat")) {
            TSNode sp = ts_node_named_child_count(s) > 0 ? ts_node_named_child(s, 0) : s;
            for (uint32_t c = 0; c < ts_node_named_child_count(sp); c++) {
                TSNode g = ts_node_named_child(sp, c);
                if (tf_type(g, "get_attr") && i++ == k) {
                    return tf_attr_name(a, src, g);
                }
            }
            continue;
        }
        if (!tf_type(s, "get_attr")) {
            return NULL;
        }
        if (i++ == k) {
            return tf_attr_name(a, src, s);
        }
    }
    return NULL;
}

/* One traversal `root.attr...` inside block `src_block`. */
static void tf_traversal(tf_mod_t *m, const char *src_block, TSNode v) {
    if (ts_node_named_child_count(v) == 0) {
        return;
    }
    CBMArena *a = m->arena;
    const char *root = tf_text(a, m->src, ts_node_named_child(v, 0));
    const char *a1 = tf_attr(a, m->src, v, 0);
    if (!root || !a1) {
        return;
    }
    int line = (int)ts_node_start_point(v).row + SKIP_ONE;
    if (strcmp(root, "var") == 0) {
        tf_ref_push(m, src_block, "var", cbm_arena_sprintf(a, "variable.%s", a1), NULL, line);
    } else if (strcmp(root, "local") == 0) {
        tf_ref_push(m, src_block, "local", cbm_arena_sprintf(a, "local.%s", a1), NULL, line);
    } else if (strcmp(root, "module") == 0) {
        tf_ref_push(m, src_block, "module", cbm_arena_sprintf(a, "module.%s", a1),
                    tf_attr(a, m->src, v, SKIP_ONE), line);
    } else if (strcmp(root, "data") == 0) {
        const char *a2 = tf_attr(a, m->src, v, SKIP_ONE);
        if (a2) {
            tf_ref_push(m, src_block, "data", cbm_arena_sprintf(a, "data.%s.%s", a1, a2), NULL,
                        line);
        }
    } else if (strchr(root, '_')) {
        tf_ref_push(m, src_block, "resource", cbm_arena_sprintf(a, "resource.%s.%s", root, a1),
                    NULL, line);
    }
}

/* Every traversal under `n` (iterative: deep expressions). */
static void tf_walk_refs(tf_mod_t *m, const char *src_block, TSNode n) {
    TSTreeCursor cur = ts_tree_cursor_new(n);
    bool more = true;
    while (more) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (tf_type(c, "variable_expr")) {
            tf_traversal(m, src_block, c);
        }
        if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* Node `n`'s text (in file text `src`) is `word`. */
static bool tf_text_is(const char *src, TSNode n, const char *word) {
    if (ts_node_is_null(n)) {
        return false;
    }
    uint32_t b = ts_node_start_byte(n);
    size_t len = strlen(word);
    return ts_node_end_byte(n) - b == len && memcmp(src + b, word, len) == 0;
}

static bool tf_is_word(const tf_mod_t *m, TSNode n, const char *word) {
    return tf_text_is(m->src, n, word);
}

/* The references of one top-level block's body, those of its depends_on
 * list marked as such. */
static void tf_walk_block(tf_mod_t *m, const char *src_block, TSNode body) {
    for (uint32_t i = 0; i < ts_node_named_child_count(body); i++) {
        TSNode c = ts_node_named_child(body, i);
        m->dep = tf_type(c, "attribute") && ts_node_named_child_count(c) > 0 &&
                 tf_is_word(m, ts_node_named_child(c, 0), "depends_on");
        tf_walk_refs(m, src_block, c);
    }
    m->dep = false;
}

/* `module "X" { source = "./dir" }`: the call, its source resolved from the
 * module's directory. A local source is "./..." or "../..."; any other -- a
 * registry address, git, http, a bucket -- lies outside the repository. */
static void tf_mod_call(tf_mod_t *m, const char *name, TSNode block, TSNode body) {
    for (uint32_t i = 0; i < ts_node_named_child_count(body); i++) {
        TSNode at = ts_node_named_child(body, i);
        if (!tf_type(at, "attribute") || ts_node_named_child_count(at) < PAIR_LEN ||
            !tf_is_word(m, ts_node_named_child(at, 0), "source")) {
            continue;
        }
        char v[CBM_SZ_1K];
        char dir[CBM_SZ_1K];
        if (!tf_string(m->src, m->dir, ts_node_named_child(at, SKIP_ONE), v, sizeof(v)) ||
            (strncmp(v, "./", sizeof("./") - SKIP_ONE) != 0 &&
             strncmp(v, "../", sizeof("../") - SKIP_ONE) != 0) ||
            !tf_repo_path(m->dir, v, dir, sizeof(dir)) ||
            !tf_grow((void **)&m->mcalls, &m->call_cap, m->call_n, sizeof(*m->mcalls))) {
            return;
        }
        char *d = cbm_arena_strdup(m->arena, dir);
        if (d) {
            m->mcalls[m->call_n++] = (tf_call_t){(char *)m->file, (char *)name, d,
                                                 (int)ts_node_start_point(block).row + SKIP_ONE};
            cbm_ht_set(m->calls, name, d);
        }
        return;
    }
}

/* One file of a module: its declarations (blocks, locals entries), module
 * calls and the references of each top-level block. */
static void tf_mod_file(tf_mod_t *m, TSNode root) {
    CBMArena *a = m->arena;
    TSNode body = tf_root_body(root);
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode b = ts_node_named_child(body, i);
        TSNode bb;
        const char *name = tf_type(b, "block") ? tf_block_name(a, m->src, b, &bb) : NULL;
        if (!name) {
            continue;
        }
        cbm_ht_set(m->decls, name, (void *)m->file); /* `{}` declares too */
        if (ts_node_is_null(bb)) {
            continue;
        }
        if (strcmp(name, "locals") == 0) {
            for (uint32_t j = 0; j < ts_node_named_child_count(bb); j++) {
                TSNode at = ts_node_named_child(bb, j);
                if (tf_type(at, "attribute") && ts_node_named_child_count(at) > 0) {
                    const char *key = tf_text(a, m->src, ts_node_named_child(at, 0));
                    cbm_ht_set(m->decls, cbm_arena_sprintf(a, "local.%s", key), (void *)m->file);
                }
            }
        } else if (strncmp(name, TF_MODULE_PREFIX, sizeof(TF_MODULE_PREFIX) - SKIP_ONE) == 0) {
            tf_mod_call(m, name, b, bb);
        }
        tf_walk_block(m, name, bb);
    }
}

/* The node of definition `name` in `file`, by the QN extract_defs gave it. */
static int64_t tf_def_node(tf_mod_t *m, const char *file, const char *name) {
    const char *qn = cbm_fqn_compute(m->arena, m->ctx->project_name, file, name);
    const cbm_gbuf_node_t *n = qn ? cbm_gbuf_find_by_qn(m->ctx->gbuf, qn) : NULL;
    atomic_fetch_add_explicit(&g_tf_candidates, SKIP_ONE, memory_order_relaxed);
    return n && n->file_path && strcmp(n->file_path, file) == 0 ? n->id : 0;
}

/* Order paths by directory, then name: a directory's files are contiguous. */
static int tf_cmp_dir_path(const void *x, const void *y) {
    const char *p = *(const char *const *)x;
    const char *q = *(const char *const *)y;
    const char *ps = strrchr(p, '/');
    const char *qs = strrchr(q, '/');
    size_t pl = ps ? (size_t)(ps - p) : 0;
    size_t ql = qs ? (size_t)(qs - q) : 0;
    size_t n = pl < ql ? pl : ql;
    int c = strncmp(p, q, n);
    if (c == 0 && pl != ql) {
        c = pl < ql ? -1 : 1;
    }
    return c ? c : strcmp(p, q);
}

static bool tf_same_dir(const char *p, const char *q) {
    const char *ps = strrchr(p, '/');
    const char *qs = strrchr(q, '/');
    size_t pl = ps ? (size_t)(ps - p) : 0;
    size_t ql = qs ? (size_t)(qs - q) : 0;
    return pl == ql && strncmp(p, q, pl) == 0;
}

/* One reference edge; 1 when made. */
static int tf_ref_edge(tf_mod_t *m, int64_t src, int64_t tgt, const char *type, const char *ref,
                       int line) {
    if (src <= 0 || tgt <= 0 || src == tgt) {
        return 0;
    }
    char props[CBM_SZ_128];
    snprintf(props, sizeof(props), "{\"ref\":\"%s\",\"via\":\"terraform\",\"line\":%d}", ref, line);
    cbm_gbuf_insert_edge(m->ctx->gbuf, src, tgt, type, props);
    return SKIP_ONE;
}

/* Resolve the module's references against its declarations, module.X.Y
 * against the outputs of X's source directory (`mods`: directory -> its
 * module), and its local module calls to their directories' Folders. */
static int tf_mod_link(tf_mod_t *m, CBMHashTable *mods, CBMHashTable *folders) {
    int edges = 0;
    for (int i = 0; i < m->ref_n; i++) {
        const tf_ref_t *r = &m->refs[i];
        const char *tfile = cbm_ht_get(m->decls, r->key);
        const char *oname = NULL;
        const char *ofile = NULL;
        if (r->out) {
            const char *cdir = cbm_ht_get(m->calls, r->key);
            const tf_mod_t *callee = cdir ? cbm_ht_get(mods, cdir) : NULL;
            oname = callee ? cbm_arena_sprintf(m->arena, "output.%s", r->out) : NULL;
            ofile = oname ? cbm_ht_get(callee->decls, oname) : NULL;
        }
        if (!tfile && !ofile) {
            continue;
        }
        int64_t src = tf_def_node(m, r->file, r->src);
        if (tfile) {
            bool local = strcmp(r->ref, "local") == 0;
            edges += tf_ref_edge(m, src, tf_def_node(m, tfile, local ? "locals" : r->key),
                                 r->dep ? "DEPENDS_ON" : "USAGE", r->ref, r->line);
        }
        if (ofile) {
            edges += tf_ref_edge(m, src, tf_def_node(m, ofile, oname), "USAGE", "output", r->line);
        }
    }
    for (int i = 0; i < m->call_n; i++) {
        const tf_call_t *c = &m->mcalls[i];
        const cbm_gbuf_node_t *folder = NULL;
        if (cbm_ht_get(mods, c->dir)) {
            /* the repository root has no Folder: its Project stands for it */
            folder = c->dir[0] ? (folders ? cbm_ht_get(folders, c->dir) : NULL)
                               : cbm_gbuf_find_by_qn(m->ctx->gbuf, m->ctx->project_name);
        }
        if (folder && !c->dir[0] && (!folder->label || strcmp(folder->label, "Project") != 0)) {
            folder = NULL;
        }
        int64_t src = folder ? tf_def_node(m, c->file, c->name) : 0;
        if (src <= 0) {
            continue;
        }
        char props[CBM_SZ_256];
        snprintf(props, sizeof(props), "{\"local_name\":\"%s\",\"via\":\"terraform\",\"line\":%d}",
                 c->name + sizeof(TF_MODULE_PREFIX) - SKIP_ONE, c->line);
        cbm_gbuf_insert_edge(m->ctx->gbuf, src, folder->id, "IMPORTS", props);
        edges++;
    }
    return edges;
}

static void tf_mod_free(tf_mod_t *m) {
    if (m->decls) {
        cbm_ht_free(m->decls);
    }
    if (m->calls) {
        cbm_ht_free(m->calls);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, m->refs);
    cbm_free(CBM_MEM_CLASS_OTHER, m->mcalls);
    cbm_free(CBM_MEM_CLASS_OTHER, m);
}

/* One module directory, its files `paths[0..n)`: parse each (read from the
 * repository) into its declarations, calls and references. */
static tf_mod_t *tf_mod_parse(cbm_pipeline_ctx_t *ctx, CBMArena *arena, TSParser *parser,
                              const char *const *paths, int n) {
    tf_mod_t *m = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
    if (!m) {
        return NULL;
    }
    const char *slash = strrchr(paths[0], '/');
    *m = (tf_mod_t){.ctx = ctx,
                    .arena = arena,
                    .decls = cbm_ht_create(0),
                    .calls = cbm_ht_create(0),
                    .dir =
                        cbm_arena_strndup(arena, paths[0], slash ? (size_t)(slash - paths[0]) : 0)};
    if (!m->decls || !m->calls || !m->dir) {
        tf_mod_free(m);
        return NULL;
    }
    for (int i = 0; i < n; i++) {
        char path[CBM_SZ_2K];
        snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, paths[i]);
        size_t len = 0;
        char *src = tf_read(path, &len);
        TSTree *tree = src ? ts_parser_parse_string(parser, NULL, src, (uint32_t)len) : NULL;
        if (tree) {
            m->src = src;
            m->file = cbm_arena_strdup(arena, paths[i]);
            tf_mod_file(m, ts_tree_root_node(tree));
            ts_tree_delete(tree);
        }
        m->src = NULL;
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    return m;
}

/* path -> node, over the nodes of one label (Folder, File). */
static CBMHashTable *tf_label_index(cbm_pipeline_ctx_t *ctx, const char *label) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (ix && cbm_gbuf_find_by_label(ctx->gbuf, label, &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            if (nodes[i]->file_path) {
                cbm_ht_set(ix, nodes[i]->file_path, (void *)nodes[i]);
            }
        }
    }
    return ix;
}

/* The module of the first of `d` and its ancestors (d is cut in place)
 * declaring one of `vnames[0..n)`; NULL when none does. */
static tf_mod_t *tf_tfvars_module(CBMHashTable *mods, char *d, const char *const *vnames, int n) {
    for (;;) {
        tf_mod_t *m = cbm_ht_get(mods, d);
        for (int k = 0; m && k < n; k++) {
            if (vnames[k] && cbm_ht_get(m->decls, vnames[k])) {
                return m;
            }
        }
        if (!d[0]) {
            return NULL;
        }
        char *s = strrchr(d, '/');
        if (s) {
            *s = '\0';
        } else {
            d[0] = '\0';
        }
    }
}

/* A .tfvars file sets variables of a module: the one in its own directory
 * when that declares one of its names, else the nearest ancestor directory's
 * that does (a file there is applied by a -var-file the repository does not
 * state: the edge says `inferred`). Each top-level assignment whose name the
 * module declares OVERRIDEs {via: tfvars, line}, from the file's Variable of
 * that name, the `variable` block. */
static int tf_link_tfvars(CBMArena *arena, TSParser *parser, CBMHashTable *mods,
                          const char *repo_path, const char *rel) {
    char path[CBM_SZ_2K];
    snprintf(path, sizeof(path), "%s/%s", repo_path, rel);
    size_t len = 0;
    char *src = tf_read(path, &len);
    TSTree *tree = src ? ts_parser_parse_string(parser, NULL, src, (uint32_t)len) : NULL;
    TSNode body = tree ? tf_root_body(ts_tree_root_node(tree)) : (TSNode){0};
    uint32_t nc = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    const char **vnames = nc ? cbm_arena_alloc(arena, (size_t)nc * sizeof(*vnames)) : NULL;
    TSNode *attrs = nc ? cbm_arena_alloc(arena, (size_t)nc * sizeof(*attrs)) : NULL;
    int n = 0;
    for (uint32_t i = 0; vnames && attrs && i < nc; i++) {
        TSNode at = ts_node_named_child(body, i);
        if (tf_type(at, "attribute") && ts_node_named_child_count(at) > 0) {
            const char *key = tf_text(arena, src, ts_node_named_child(at, 0));
            vnames[n] = key ? cbm_arena_sprintf(arena, "variable.%s", key) : NULL;
            attrs[n++] = at;
        }
    }
    const char *slash = strrchr(rel, '/');
    char d[CBM_SZ_1K];
    snprintf(d, sizeof(d), "%.*s", slash ? (int)(slash - rel) : 0, rel);
    size_t own = strlen(d);
    tf_mod_t *m = n > 0 ? tf_tfvars_module(mods, d, vnames, n) : NULL;
    int edges = 0;
    for (int k = 0; m && k < n; k++) {
        const char *vfile = vnames[k] ? cbm_ht_get(m->decls, vnames[k]) : NULL;
        const char *key = vfile ? vnames[k] + sizeof("variable.") - SKIP_ONE : NULL;
        int64_t from = key ? tf_def_node(m, rel, key) : 0;
        int64_t to = from > 0 ? tf_def_node(m, vfile, vnames[k]) : 0;
        if (to > 0) {
            char props[CBM_SZ_128];
            snprintf(props, sizeof(props), "{\"via\":\"tfvars\",%s\"line\":%d}",
                     strlen(d) == own ? "" : "\"inferred\":true,",
                     (int)ts_node_start_point(attrs[k]).row + SKIP_ONE);
            cbm_gbuf_insert_edge(m->ctx->gbuf, from, to, "OVERRIDE", props);
            edges++;
        }
    }
    if (tree) {
        ts_tree_delete(tree);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, src);
    return edges;
}

/* Every module directory, `paths[0..pn)` sorted by directory: parse all,
 * then link each -- a reference may read an output of another directory --
 * and the .tfvars files `vpaths[0..vn)` to the module whose variables they set. */
static int tf_link_repo(cbm_pipeline_ctx_t *ctx, TSParser *parser, const char *const *paths, int pn,
                        const char *const *vpaths, int vn) {
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    CBMHashTable *mods = cbm_ht_create(0);
    tf_mod_t **list = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)pn * sizeof(*list));
    int nm = 0;
    int calls = 0;
    for (int i = 0; mods && list && i < pn;) {
        int j = i;
        while (j < pn && tf_same_dir(paths[i], paths[j])) {
            j++;
        }
        tf_mod_t *m = tf_mod_parse(ctx, &arena, parser, paths + i, j - i);
        if (m) {
            list[nm++] = m;
            cbm_ht_set(mods, m->dir, m);
            calls += m->call_n;
        }
        i = j;
    }
    CBMHashTable *folders = calls > 0 ? tf_label_index(ctx, "Folder") : NULL;
    int edges = 0;
    for (int k = 0; k < nm; k++) {
        edges += tf_mod_link(list[k], mods, folders);
    }
    for (int k = 0; mods && k < vn; k++) {
        edges += tf_link_tfvars(&arena, parser, mods, ctx->repo_path, vpaths[k]);
    }
    for (int k = 0; k < nm; k++) {
        tf_mod_free(list[k]);
    }
    if (folders) {
        cbm_ht_free(folders);
    }
    if (mods) {
        cbm_ht_free(mods);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, list);
    cbm_arena_destroy(&arena);
    return edges;
}

/* When one of the run's files is a .tf / .tofu / .tfvars file, every module
 * directory of the repository (the graph's File nodes) is resolved whole: on
 * an incremental run an unchanged file's reference may meet what a changed
 * one declares -- in its own directory (a variable, a locals entry), in a
 * module it calls (an output), or a .tfvars file a variable it sets. */
static int tf_link_refs(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                        TSParser *parser) {
    bool any = false;
    for (int i = 0; i < file_count && !any; i++) {
        any = (files[i].language == CBM_LANG_HCL && tf_is_module_file(files[i].rel_path)) ||
              tf_is_tfvars(files[i].rel_path);
    }
    const cbm_gbuf_node_t **fnodes = NULL;
    int fn = 0;
    if (!any || cbm_gbuf_find_by_label(ctx->gbuf, "File", &fnodes, &fn) != 0 || fn <= 0) {
        return 0;
    }
    const char **paths = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)fn * sizeof(char *));
    const char **vpaths = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)fn * sizeof(char *));
    int pn = 0;
    int vn = 0;
    for (int i = 0; paths && vpaths && i < fn; i++) {
        const char *rel = fnodes[i]->file_path;
        if (rel && tf_is_module_file(rel)) {
            paths[pn++] = rel;
        } else if (rel && tf_is_tfvars(rel)) {
            vpaths[vn++] = rel;
        }
    }
    int edges = 0;
    if (pn > 0 && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_HCL))) {
        qsort((void *)paths, (size_t)pn, sizeof(*paths), tf_cmp_dir_path);
        qsort((void *)vpaths, (size_t)vn, sizeof(*vpaths), tf_cmp_dir_path);
        edges = tf_link_repo(ctx, parser, paths, pn, vpaths, vn);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)paths);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)vpaths);
    return edges;
}

/* ── Terragrunt (PHASE 6 P6.4) ─────────────────────────────────────── */

/* A Terragrunt unit is a terragrunt.hcl file. Its expressions are evaluated
 * in the unit's context -- also those of a file it includes, which Terragrunt
 * evaluates as part of the unit:
 *   include "N" { path = E }           IMPORTS {local_name: N, via: terragrunt,
 *                                      line} the file E names, from the block
 *   read_terragrunt_config(E)          IMPORTS {via: terragrunt, read: <the file
 *                                      holding the call>, line} the file E names:
 *                                      from the unit's top-level block holding
 *                                      the call or, for a call in an included
 *                                      file, from the unit's include block that
 *                                      brings that file in
 *   dependency "N" { config_path = E } DEPENDS_ON {local_name: N, via: terragrunt,
 *                                      line} the terragrunt.hcl of directory E
 *   terraform { source = "./..." }     IMPORTS {via: terragrunt, line} the Folder
 * E is a string template of literals and find_in_parent_folders(name) (the
 * first file of that name in the unit's parent directory or above),
 * dirname(x) and get_terragrunt_dir(); anything else is unknown, and an
 * unknown path is no edge. A target must be a file (a directory) the graph
 * holds. */

static const char TG_UNIT[] = "terragrunt.hcl";
static const char TG_INCLUDE[] = "include.";
static const char TG_DEPENDENCY[] = "dependency.";

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *arena;
    CBMHashTable *files;   /* repository path -> File node */
    CBMHashTable *folders; /* directory -> Folder node */
    const char *unit;      /* the unit's path */
    const char *udir;      /* its directory */
    const char *src;       /* the text of the file being read */
    int edges;
} tg_t;

/* The directory part of `p`, in place; an anchored value keeps its anchor. */
static void tg_dirname(char *p) {
    char *s = strrchr(p, '/');
    if (s) {
        *s = '\0';
    } else {
        p[p[0] == '\x01' ? SKIP_ONE : 0] = '\0';
    }
}

/* find_in_parent_folders(name): the first `name` in the unit's parent
 * directory or above, anchored at the repository root. */
static bool tg_find_up(const tg_t *g, const char *name, char *out, size_t sz) {
    char d[CBM_SZ_1K];
    snprintf(d, sizeof(d), "%s", g->udir);
    while (d[0]) {
        tg_dirname(d);
        char cand[CBM_SZ_2K];
        snprintf(cand, sizeof(cand), "%s%s%s", d, d[0] ? "/" : "", name);
        if (cbm_ht_get(g->files, cand)) {
            snprintf(out, sz, "\x01%s", cand);
            return true;
        }
    }
    return false;
}

static bool tg_eval(const tg_t *g, TSNode n, char *out, size_t sz);

/* The argument expressions of a call (up to `max`); their number. */
static int tg_args(TSNode call, TSNode *args, int max) {
    int k = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(call); i++) {
        TSNode c = ts_node_named_child(call, i);
        for (uint32_t j = 0; tf_type(c, "function_arguments") && j < ts_node_named_child_count(c);
             j++) {
            if (tf_type(ts_node_named_child(c, j), "expression")) {
                if (k < max) {
                    args[k] = ts_node_named_child(c, j);
                }
                k++;
            }
        }
    }
    return k;
}

/* A call of a function the evaluator knows. */
static bool tg_call(const tg_t *g, TSNode call, char *out, size_t sz) {
    TSNode fn = ts_node_named_child(call, 0);
    TSNode args[PAIR_LEN];
    int na = tg_args(call, args, PAIR_LEN);
    char a[CBM_SZ_1K];
    if (tf_text_is(g->src, fn, "find_in_parent_folders")) {
        return na == SKIP_ONE && tg_eval(g, args[0], a, sizeof(a)) && a[0] != '\x01' &&
               tg_find_up(g, a, out, sz);
    }
    if (tf_text_is(g->src, fn, "dirname")) {
        if (na != SKIP_ONE || !tg_eval(g, args[0], out, sz)) {
            return false;
        }
        tg_dirname(out);
        return true;
    }
    if (tf_text_is(g->src, fn, "get_terragrunt_dir") && na == 0) {
        snprintf(out, sz, "\x01%s", g->udir);
        return true;
    }
    return false;
}

/* An expression's value: a string template of literals and known calls;
 * false when any part is something else. */
static bool tg_eval(const tg_t *g, TSNode n, char *out, size_t sz) {
    while (
        (tf_type(n, "expression") || tf_type(n, "literal_value") || tf_type(n, "template_expr")) &&
        ts_node_named_child_count(n) == SKIP_ONE) {
        n = ts_node_named_child(n, 0);
    }
    if (tf_type(n, "function_call")) {
        return tg_call(g, n, out, sz);
    }
    if (!tf_type(n, "string_lit") && !tf_type(n, "quoted_template")) {
        return false;
    }
    size_t len = 0;
    out[0] = '\0';
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        if (tf_type(c, "quoted_template_start") || tf_type(c, "quoted_template_end")) {
            continue;
        }
        uint32_t b = ts_node_start_byte(c);
        if (tf_type(c, "template_literal")) {
            if (!tf_add(out, sz, &len, g->src + b, ts_node_end_byte(c) - b)) {
                return false;
            }
            continue;
        }
        TSNode expr = {0};
        for (uint32_t j = 0;
             tf_type(c, "template_interpolation") && j < ts_node_named_child_count(c); j++) {
            if (tf_type(ts_node_named_child(c, j), "expression")) {
                expr = ts_node_named_child(c, j);
            }
        }
        char part[CBM_SZ_1K];
        if (ts_node_is_null(expr) || !tg_eval(g, expr, part, sizeof(part)) ||
            !tf_add(out, sz, &len, part, strlen(part))) {
            return false;
        }
    }
    return true;
}

/* The expression of attribute `key` in a block body; a null node when none. */
static TSNode tg_attr(const tg_t *g, TSNode body, const char *key) {
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode at = ts_node_named_child(body, i);
        if (tf_type(at, "attribute") && ts_node_named_child_count(at) >= PAIR_LEN &&
            tf_text_is(g->src, ts_node_named_child(at, 0), key)) {
            return ts_node_named_child(at, SKIP_ONE);
        }
    }
    return (TSNode){0};
}

/* The node of definition `name` in `file`, by the QN extract_defs gave it. */
static int64_t tg_def_node(const tg_t *g, const char *file, const char *name) {
    const char *qn = cbm_fqn_compute(g->arena, g->ctx->project_name, file, name);
    const cbm_gbuf_node_t *n = qn ? cbm_gbuf_find_by_qn(g->ctx->gbuf, qn) : NULL;
    atomic_fetch_add_explicit(&g_tf_candidates, SKIP_ONE, memory_order_relaxed);
    return n && n->file_path && strcmp(n->file_path, file) == 0 ? n->id : 0;
}

static void tg_edge(tg_t *g, int64_t src, const cbm_gbuf_node_t *tgt, const char *type,
                    const char *props) {
    if (src > 0 && tgt && tgt->id != src) {
        cbm_gbuf_insert_edge(g->ctx->gbuf, src, tgt->id, type, props);
        g->edges++;
    }
}

/* Expression `e` as the repository path of a graph file (relative values
 * from `base`); NULL when unknown or not in the graph. */
static const cbm_gbuf_node_t *tg_file(const tg_t *g, TSNode e, const char *base, char *path,
                                      size_t sz) {
    char v[CBM_SZ_1K];
    if (ts_node_is_null(e) || !tg_eval(g, e, v, sizeof(v)) || !tf_repo_path(base, v, path, sz)) {
        return NULL;
    }
    return cbm_ht_get(g->files, path);
}

/* Every read_terragrunt_config(E) of file `site` (its body `body`): IMPORTS
 * the file E names, relative paths from `base`, from `from` -- or, when 0,
 * from the top-level block (a top-level attribute: the file) holding it. */
static void tg_reads(tg_t *g, TSNode body, const char *site, const char *base, int64_t from) {
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode b = ts_node_named_child(body, i);
        int64_t src = from;
        if (!src) {
            TSNode bb;
            const char *name = tf_type(b, "block") ? tf_block_name(g->arena, g->src, b, &bb) : NULL;
            const cbm_gbuf_node_t *file = cbm_ht_get(g->files, site);
            src = name ? tg_def_node(g, site, name) : file ? file->id : 0;
        }
        TSTreeCursor cur = ts_tree_cursor_new(b);
        bool more = true;
        while (more) {
            TSNode c = ts_tree_cursor_current_node(&cur);
            TSNode args[SKIP_ONE];
            if (tf_type(c, "function_call") &&
                tf_text_is(g->src, ts_node_named_child(c, 0), "read_terragrunt_config") &&
                tg_args(c, args, SKIP_ONE) >= SKIP_ONE) {
                char path[CBM_SZ_1K];
                const cbm_gbuf_node_t *tgt = tg_file(g, args[0], base, path, sizeof(path));
                char props[CBM_SZ_2K];
                snprintf(props, sizeof(props),
                         "{\"via\":\"terragrunt\",\"read\":\"%s\",\"line\":%d}", site,
                         (int)ts_node_start_point(c).row + SKIP_ONE);
                tg_edge(g, src, tgt, "IMPORTS", props);
            }
            if (ts_tree_cursor_goto_first_child(&cur)) {
                continue;
            }
            while (!ts_tree_cursor_goto_next_sibling(&cur)) {
                if (!ts_tree_cursor_goto_parent(&cur)) {
                    more = false;
                    break;
                }
            }
        }
        ts_tree_cursor_delete(&cur);
    }
}

/* One block of the unit: an include, a dependency, the terraform block. An
 * include's file (its path into `inc`, its block into `inc_node`) is read
 * afterwards for the calls it holds. */
static void tg_block(tg_t *g, TSNode b, char *inc, size_t inc_sz, int64_t *inc_node) {
    TSNode bb;
    const char *name = tf_type(b, "block") ? tf_block_name(g->arena, g->src, b, &bb) : NULL;
    if (!name || ts_node_is_null(bb)) {
        return;
    }
    int line = (int)ts_node_start_point(b).row + SKIP_ONE;
    char path[CBM_SZ_1K];
    char props[CBM_SZ_256];
    if (strncmp(name, TG_INCLUDE, sizeof(TG_INCLUDE) - SKIP_ONE) == 0) {
        const cbm_gbuf_node_t *tgt =
            tg_file(g, tg_attr(g, bb, "path"), g->udir, path, sizeof(path));
        int64_t src = tgt ? tg_def_node(g, g->unit, name) : 0;
        snprintf(props, sizeof(props), "{\"local_name\":\"%s\",\"via\":\"terragrunt\",\"line\":%d}",
                 name + sizeof(TG_INCLUDE) - SKIP_ONE, line);
        tg_edge(g, src, tgt, "IMPORTS", props);
        if (src > 0) {
            snprintf(inc, inc_sz, "%s", path);
            *inc_node = src;
        }
    } else if (strncmp(name, TG_DEPENDENCY, sizeof(TG_DEPENDENCY) - SKIP_ONE) == 0) {
        char v[CBM_SZ_1K];
        char dir[CBM_SZ_1K];
        TSNode e = tg_attr(g, bb, "config_path");
        if (!ts_node_is_null(e) && tg_eval(g, e, v, sizeof(v)) &&
            tf_repo_path(g->udir, v, dir, sizeof(dir))) {
            snprintf(path, sizeof(path), "%s%s%s", dir, dir[0] ? "/" : "", TG_UNIT);
            const cbm_gbuf_node_t *tgt = cbm_ht_get(g->files, path);
            snprintf(props, sizeof(props),
                     "{\"local_name\":\"%s\",\"via\":\"terragrunt\",\"line\":%d}",
                     name + sizeof(TG_DEPENDENCY) - SKIP_ONE, line);
            tg_edge(g, tgt ? tg_def_node(g, g->unit, name) : 0, tgt, "DEPENDS_ON", props);
        }
    } else if (strcmp(name, "terraform") == 0) {
        char v[CBM_SZ_1K];
        TSNode e = tg_attr(g, bb, "source");
        if (!ts_node_is_null(e) && tg_eval(g, e, v, sizeof(v)) &&
            (strncmp(v, "./", sizeof("./") - SKIP_ONE) == 0 ||
             strncmp(v, "../", sizeof("../") - SKIP_ONE) == 0) &&
            tf_repo_path(g->udir, v, path, sizeof(path))) {
            const cbm_gbuf_node_t *tgt = g->folders ? cbm_ht_get(g->folders, path) : NULL;
            snprintf(props, sizeof(props), "{\"via\":\"terragrunt\",\"line\":%d}", line);
            tg_edge(g, tgt ? tg_def_node(g, g->unit, name) : 0, tgt, "IMPORTS", props);
        }
    }
}

enum { TG_MAX_INCLUDES = 32 };

/* One unit: its blocks and reads, then the reads of the files it includes. */
static void tg_unit(tg_t *g, TSParser *parser, const char *rel) {
    const char *slash = strrchr(rel, '/');
    g->unit = rel;
    g->udir = cbm_arena_strndup(g->arena, rel, slash ? (size_t)(slash - rel) : 0);
    char path[CBM_SZ_2K];
    snprintf(path, sizeof(path), "%s/%s", g->ctx->repo_path, rel);
    size_t len = 0;
    char *src = g->udir ? tf_read(path, &len) : NULL;
    TSTree *tree = src ? ts_parser_parse_string(parser, NULL, src, (uint32_t)len) : NULL;
    char inc[TG_MAX_INCLUDES][CBM_SZ_1K];
    int64_t inc_node[TG_MAX_INCLUDES];
    int ni = 0;
    if (tree) {
        g->src = src;
        TSNode body = tf_root_body(ts_tree_root_node(tree));
        for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
            inc_node[ni] = 0;
            tg_block(g, ts_node_named_child(body, i), inc[ni], sizeof(inc[ni]), &inc_node[ni]);
            if (ni < TG_MAX_INCLUDES - SKIP_ONE && inc_node[ni] > 0) {
                ni++; /* past the cap, the last slot is overwritten */
            }
        }
        tg_reads(g, body, rel, g->udir, 0);
        ts_tree_delete(tree);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, src);
    for (int k = 0; k < ni; k++) {
        snprintf(path, sizeof(path), "%s/%s", g->ctx->repo_path, inc[k]);
        src = tf_read(path, &len);
        tree = src ? ts_parser_parse_string(parser, NULL, src, (uint32_t)len) : NULL;
        if (tree) {
            g->src = src;
            char base[CBM_SZ_1K];
            snprintf(base, sizeof(base), "%s", inc[k]);
            tg_dirname(base);
            tg_reads(g, tf_root_body(ts_tree_root_node(tree)), inc[k], base, inc_node[k]);
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    g->src = NULL;
}

static int tg_cmp_str(const void *x, const void *y) {
    return strcmp(*(const char *const *)x, *(const char *const *)y);
}

/* Every Terragrunt unit of the repository, when the run holds a .hcl file
 * (a unit, or a file units include or read). */
static int tg_link_units(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         TSParser *parser) {
    bool any = false;
    for (int i = 0; i < file_count && !any; i++) {
        const char *dot = strrchr(tf_basename(files[i].rel_path), '.');
        any = dot && strcmp(dot, ".hcl") == 0;
    }
    const cbm_gbuf_node_t **fnodes = NULL;
    int fn = 0;
    if (!any || cbm_gbuf_find_by_label(ctx->gbuf, "File", &fnodes, &fn) != 0 || fn <= 0) {
        return 0;
    }
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    tg_t g = {.ctx = ctx, .arena = &arena, .files = cbm_ht_create(0)};
    const char **units = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)fn * sizeof(char *));
    int nu = 0;
    for (int i = 0; g.files && units && i < fn; i++) {
        const char *rel = fnodes[i]->file_path;
        if (rel) {
            cbm_ht_set(g.files, rel, (void *)fnodes[i]);
            if (strcmp(tf_basename(rel), TG_UNIT) == 0) {
                units[nu++] = rel;
            }
        }
    }
    if (nu > 0 && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_HCL))) {
        qsort((void *)units, (size_t)nu, sizeof(*units), tg_cmp_str);
        g.folders = tf_label_index(ctx, "Folder");
        for (int i = 0; i < nu; i++) {
            tg_unit(&g, parser, units[i]);
        }
    }
    if (g.folders) {
        cbm_ht_free(g.folders);
    }
    if (g.files) {
        cbm_ht_free(g.files);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)units);
    cbm_arena_destroy(&arena);
    return g.edges;
}

/* ── Kubernetes objects Terraform declares (PHASE 6 P6.5) ──────────── */

/* resource "kubernetes_<type>[_vN]" "N" declares the object of the kind its
 * type names -- a kubernetes_manifest the apiVersion / kind of its manifest
 * -- with metadata name and namespace from string literals. It is a
 * Resource of the K8s identity scheme (IDENTITY.md: <file-base>::<group>/
 * <Kind>/<ns or ->/<name>), with a manifest object's properties plus
 * `ecosystem: terraform`; its file DEFINES it and the block DEPLOYS
 * {via: terraform, line} it. A computed name (var., local., an
 * interpolation) is unknown: no Resource. Types acting on an object declared
 * elsewhere (owner references, readiness waits, label / annotation patches)
 * declare none. Run before the K8s joins: references meet these objects like
 * manifest ones. */

typedef struct {
    const char *type; /* after "kubernetes_", without a _vN suffix */
    const char *group;
    const char *kind;
} tf_k8s_kind_t;

static const tf_k8s_kind_t TF_K8S_KINDS[] = {
    {"namespace", "core", "Namespace"},
    {"service", "core", "Service"},
    {"service_account", "core", "ServiceAccount"},
    {"default_service_account", "core", "ServiceAccount"},
    {"config_map", "core", "ConfigMap"},
    {"secret", "core", "Secret"},
    {"persistent_volume_claim", "core", "PersistentVolumeClaim"},
    {"persistent_volume", "core", "PersistentVolume"},
    {"pod", "core", "Pod"},
    {"limit_range", "core", "LimitRange"},
    {"resource_quota", "core", "ResourceQuota"},
    {"endpoints", "core", "Endpoints"},
    {"replication_controller", "core", "ReplicationController"},
    {"deployment", "apps", "Deployment"},
    {"stateful_set", "apps", "StatefulSet"},
    {"daemonset", "apps", "DaemonSet"},
    {"daemon_set", "apps", "DaemonSet"},
    {"job", "batch", "Job"},
    {"cron_job", "batch", "CronJob"},
    {"cluster_role", "rbac.authorization.k8s.io", "ClusterRole"},
    {"cluster_role_binding", "rbac.authorization.k8s.io", "ClusterRoleBinding"},
    {"role", "rbac.authorization.k8s.io", "Role"},
    {"role_binding", "rbac.authorization.k8s.io", "RoleBinding"},
    {"network_policy", "networking.k8s.io", "NetworkPolicy"},
    {"ingress", "networking.k8s.io", "Ingress"},
    {"ingress_class", "networking.k8s.io", "IngressClass"},
    {"storage_class", "storage.k8s.io", "StorageClass"},
    {"csi_driver", "storage.k8s.io", "CSIDriver"},
    {"priority_class", "scheduling.k8s.io", "PriorityClass"},
    {"horizontal_pod_autoscaler", "autoscaling", "HorizontalPodAutoscaler"},
    {"pod_disruption_budget", "policy", "PodDisruptionBudget"},
    {"runtime_class", "node.k8s.io", "RuntimeClass"},
    {"mutating_webhook_configuration", "admissionregistration.k8s.io",
     "MutatingWebhookConfiguration"},
    {"validating_webhook_configuration", "admissionregistration.k8s.io",
     "ValidatingWebhookConfiguration"},
    {"api_service", "apiregistration.k8s.io", "APIService"},
    {"certificate_signing_request", "certificates.k8s.io", "CertificateSigningRequest"},
};

/* "kubernetes_<type>[_vN[beta|alphaM]]": <type> into out; false for another
 * provider's type. */
static bool tf_k8s_type(const char *rtype, char *out, size_t sz) {
    static const char PREFIX[] = "kubernetes_";
    if (strncmp(rtype, PREFIX, sizeof(PREFIX) - SKIP_ONE) != 0) {
        return false;
    }
    snprintf(out, sz, "%s", rtype + sizeof(PREFIX) - SKIP_ONE);
    char *v = strrchr(out, '_');
    if (v && v[SKIP_ONE] == 'v' && v[PAIR_LEN] >= '0' && v[PAIR_LEN] <= '9') {
        *v = '\0';
    }
    return out[0] != '\0';
}

/* A string literal's text (no interpolation, no ${path.module}); false else. */
static bool tf_literal(const char *src, TSNode n, char *out, size_t sz) {
    return tf_string(src, "", n, out, sz) && !strchr(out, '\x01');
}

/* The value of `key` in an object expression (keys bare or quoted). */
static TSNode tf_object_get(const char *src, TSNode expr, const char *key) {
    TSNode n = expr;
    while ((tf_type(n, "expression") || tf_type(n, "collection_value")) &&
           ts_node_named_child_count(n) > 0) {
        n = ts_node_named_child(n, 0);
    }
    for (uint32_t i = 0; tf_type(n, "object") && i < ts_node_named_child_count(n); i++) {
        TSNode el = ts_node_named_child(n, i);
        if (!tf_type(el, "object_elem")) {
            continue;
        }
        TSNode k = ts_node_child_by_field_name(el, "key", sizeof("key") - SKIP_ONE);
        char text[CBM_SZ_64];
        bool named = tf_literal(src, k, text, sizeof(text));
        if (!named) {
            TSNode id = k;
            while ((tf_type(id, "expression") || tf_type(id, "variable_expr")) &&
                   ts_node_named_child_count(id) > 0) {
                id = ts_node_named_child(id, 0);
            }
            named = tf_type(id, "identifier") && tf_text_is(src, id, key);
            text[0] = '\0';
        }
        if ((named && !text[0]) || (text[0] && strcmp(text, key) == 0)) {
            return ts_node_child_by_field_name(el, "val", sizeof("val") - SKIP_ONE);
        }
    }
    return (TSNode){0};
}

/* The expression of attribute `key` in a block body, or a null node. */
static TSNode tf_body_attr(const char *src, TSNode body, const char *key) {
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode at = ts_node_named_child(body, i);
        if (tf_type(at, "attribute") && ts_node_named_child_count(at) >= PAIR_LEN &&
            tf_text_is(src, ts_node_named_child(at, 0), key)) {
            return ts_node_named_child(at, SKIP_ONE);
        }
    }
    return (TSNode){0};
}

/* The body of the nested block `name` in a block body, or a null node. */
static TSNode tf_body_block(const char *src, TSNode body, const char *name) {
    for (uint32_t i = 0; !ts_node_is_null(body) && i < ts_node_named_child_count(body); i++) {
        TSNode b = ts_node_named_child(body, i);
        if (!tf_type(b, "block") || ts_node_named_child_count(b) == 0 ||
            !tf_text_is(src, ts_node_named_child(b, 0), name)) {
            continue;
        }
        for (uint32_t j = 0; j < ts_node_named_child_count(b); j++) {
            if (tf_type(ts_node_named_child(b, j), "body")) {
                return ts_node_named_child(b, j);
            }
        }
    }
    return (TSNode){0};
}

/* One top-level block of a .tf file: the object it declares, if any. */
static void tf_k8s_block(cbm_pipeline_ctx_t *ctx, CBMArena *a, const char *src, const char *rel,
                         const char *base, int64_t file_id, TSNode block) {
    TSNode body;
    const char *bname = tf_block_name(a, src, block, &body);
    static const char RES[] = "resource.";
    char rtype[CBM_SZ_128];
    char type[CBM_SZ_128];
    if (!bname || ts_node_is_null(body) || strncmp(bname, RES, sizeof(RES) - SKIP_ONE) != 0) {
        return;
    }
    const char *dot = strchr(bname + sizeof(RES) - SKIP_ONE, '.');
    snprintf(rtype, sizeof(rtype), "%.*s", dot ? (int)(dot - bname - (sizeof(RES) - SKIP_ONE)) : 0,
             bname + sizeof(RES) - SKIP_ONE);
    if (!tf_k8s_type(rtype, type, sizeof(type))) {
        return;
    }
    char group[CBM_SZ_256] = "";
    char kind[CBM_SZ_128] = "";
    char version[CBM_SZ_64] = "";
    char name[CBM_SZ_256] = "";
    char ns[CBM_SZ_256] = "";
    if (strcmp(type, "manifest") == 0) {
        TSNode m = tf_body_attr(src, body, "manifest");
        char av[CBM_SZ_256];
        TSNode meta = tf_object_get(src, m, "metadata");
        if (!tf_literal(src, tf_object_get(src, m, "kind"), kind, sizeof(kind)) ||
            !tf_literal(src, tf_object_get(src, m, "apiVersion"), av, sizeof(av)) ||
            !tf_literal(src, tf_object_get(src, meta, "name"), name, sizeof(name))) {
            return;
        }
        const char *slash = strchr(av, '/');
        if (slash) {
            snprintf(group, sizeof(group), "%.*s", (int)(slash - av), av);
        } else {
            snprintf(group, sizeof(group), "core");
        }
        snprintf(version, sizeof(version), "%s", slash ? slash + SKIP_ONE : av);
        if (!tf_literal(src, tf_object_get(src, meta, "namespace"), ns, sizeof(ns))) {
            ns[0] = '\0';
        }
    } else {
        const tf_k8s_kind_t *k = NULL;
        for (size_t i = 0; i < sizeof(TF_K8S_KINDS) / sizeof(TF_K8S_KINDS[0]) && !k; i++) {
            k = strcmp(TF_K8S_KINDS[i].type, type) == 0 ? &TF_K8S_KINDS[i] : NULL;
        }
        TSNode meta = tf_body_block(src, body, "metadata");
        if (!k) {
            return;
        }
        snprintf(group, sizeof(group), "%s", k->group);
        snprintf(kind, sizeof(kind), "%s", k->kind);
        if (strcmp(type, "default_service_account") == 0) {
            snprintf(name, sizeof(name), "default");
        } else if (!tf_literal(src, tf_body_attr(src, meta, "name"), name, sizeof(name))) {
            return;
        }
        if (!tf_literal(src, tf_body_attr(src, meta, "namespace"), ns, sizeof(ns))) {
            ns[0] = '\0';
        }
    }
    if (strchr(name, '/') || strchr(ns, '/') || strchr(kind, '/') || !name[0] || !kind[0]) {
        return; /* the identity key splits on "/" */
    }
    const char *qn =
        cbm_arena_sprintf(a, "%s::%s/%s/%s/%s", base, group, kind, ns[0] ? ns : "-", name);
    const char *rname = cbm_arena_sprintf(a, "%s/%s", kind, name);
    char *props = qn ? cbm_k8s_resource_props(qn, NULL, version, NULL, "terraform") : NULL;
    int64_t id = props ? cbm_gbuf_upsert_node(ctx->gbuf, "Resource", rname, qn, rel,
                                              (int)ts_node_start_point(block).row + SKIP_ONE,
                                              (int)ts_node_end_point(block).row + SKIP_ONE, props)
                       : 0;
    cbm_free(CBM_MEM_CLASS_OTHER, props);
    if (id <= 0) {
        return;
    }
    if (file_id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_id, id, "DEFINES", "{}");
    }
    const char *bqn = cbm_fqn_compute(a, ctx->project_name, rel, bname);
    const cbm_gbuf_node_t *bnode = bqn ? cbm_gbuf_find_by_qn(ctx->gbuf, bqn) : NULL;
    if (bnode && bnode->id != id) {
        char eprops[CBM_SZ_64];
        snprintf(eprops, sizeof(eprops), "{\"via\":\"terraform\",\"line\":%d}",
                 (int)ts_node_start_point(block).row + SKIP_ONE);
        cbm_gbuf_insert_edge(ctx->gbuf, bnode->id, id, "DEPLOYS", eprops);
    }
}

void cbm_pipeline_tf_k8s_objects(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                 int file_count) {
    TSParser *parser = NULL;
    CBMHashTable *file_nodes = NULL;
    for (int i = 0; i < file_count; i++) {
        const char *rel = files[i].rel_path;
        if (files[i].language != CBM_LANG_HCL || !tf_is_module_file(rel)) {
            continue;
        }
        size_t len = 0;
        char *src = tf_read(files[i].path, &len);
        if (!src || !strstr(src, "kubernetes_")) {
            cbm_free(CBM_MEM_CLASS_OTHER, src);
            continue;
        }
        if (!parser) {
            parser = ts_parser_new();
            file_nodes = tf_label_index(ctx, "File");
            if (!parser || !ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_HCL))) {
                cbm_free(CBM_MEM_CLASS_OTHER, src);
                break;
            }
        }
        /* the file base: the File node's QN without ".__file__" (IDENTITY.md) */
        static const char FILE_SUFFIX[] = ".__file__";
        const cbm_gbuf_node_t *file = file_nodes ? cbm_ht_get(file_nodes, rel) : NULL;
        size_t fl = file && file->qualified_name ? strlen(file->qualified_name) : 0;
        size_t sl = sizeof(FILE_SUFFIX) - SKIP_ONE;
        CBMArena arena;
        cbm_arena_init_lazy(&arena, CBM_SZ_4K);
        const char *base = fl > sl && strcmp(file->qualified_name + fl - sl, FILE_SUFFIX) == 0
                               ? cbm_arena_strndup(&arena, file->qualified_name, fl - sl)
                               : NULL;
        TSTree *tree = base ? ts_parser_parse_string(parser, NULL, src, (uint32_t)len) : NULL;
        TSNode body = tree ? tf_root_body(ts_tree_root_node(tree)) : (TSNode){0};
        for (uint32_t j = 0; !ts_node_is_null(body) && j < ts_node_named_child_count(body); j++) {
            TSNode b = ts_node_named_child(body, j);
            if (tf_type(b, "block")) {
                tf_k8s_block(ctx, &arena, src, rel, base, file->id, b);
            }
        }
        cbm_arena_destroy(&arena);
        if (tree) {
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    if (file_nodes) {
        cbm_ht_free(file_nodes);
    }
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
    if (!parser) {
        parser = ts_parser_new();
    }
    int refs = parser ? tf_link_refs(ctx, files, file_count, parser) : 0;
    int units = parser ? tg_link_units(ctx, files, file_count, parser) : 0;
    if (parser) {
        ts_parser_delete(parser);
    }
    if (charts) {
        cbm_ht_free(charts);
    }
    cbm_arena_destroy(&arena);
    if (edges > 0 || refs > 0 || units > 0) {
        char buf[CBM_SZ_32];
        char rbuf[CBM_SZ_32];
        char ubuf[CBM_SZ_32];
        snprintf(buf, sizeof(buf), "%d", edges);
        snprintf(rbuf, sizeof(rbuf), "%d", refs);
        snprintf(ubuf, sizeof(ubuf), "%d", units);
        cbm_log_info("pass.terraform.edges", "deploys", buf, "refs", rbuf, "terragrunt", ubuf);
    }
}
