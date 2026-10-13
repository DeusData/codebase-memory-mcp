/*
 * pass_operator.c — Go Kubernetes operators (PHASE1 P1.2).
 *
 * A controller-runtime operator ties Go code to Kubernetes kinds in three
 * ways the type graph alone does not carry:
 *   - a controller is built `For(&v1.Widget{})`, `Owns(&v1.Gadget{})`,
 *     `Watches(&v1.X{}, ...)` or watches `source.Kind(cache, &v1.X{})`:
 *     HANDLES {via: controller, role} from the function that builds it to
 *     the Go type, resolved as Go does: `v1` is an import of the file, and
 *     the type is the struct of that name in the imported package's
 *     directory (unqualified: the file's own package);
 *   - a CRD's group/kind is backed by `type <Kind> struct` in the package
 *     marked `+groupName=<group>` (or `GroupName = "<group>"`): USAGE
 *     {ref: crd-type} from the CRD Resource to that Struct;
 *   - `//go:embed <patterns>` puts files into the binary: REFERENCES_FILE
 *     {via: go:embed} from the Go file to each embedded file;
 *   - an installer loads a chart of the repository (helm.LoadChart(
 *     filepath.Join(opt.ChartsDirectory, ...)), the Helm SDK's loader):
 *     DEPLOYS {via: helm-sdk} from the function to the Chart (PHASE 3 P3.3).
 * Runs after USAGE resolution; reads each Go file once, and the files that
 * mention a chart again.
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

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OP_SLEN(lit) (sizeof(lit) - SKIP_ONE)

enum { OP_GROW_MIN = 16, OP_IDENT_MAX = 128 };

/* Candidates examined by the resolvers below (pipeline_internal.h). */
_Atomic uint64_t g_op_candidates = 0;

/* Read a whole file into a core-owned NUL-terminated buffer, NULL on error
 * or past the indexer's file-size policy. */
static char *op_read_file(const char *path) {
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
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + SKIP_ONE);
    if (buf) {
        size_t n = fread(buf, SKIP_ONE, (size_t)size, f);
        buf[n] = '\0';
    }
    (void)fclose(f);
    return buf;
}

static void op_dirname(const char *path, char *out, size_t out_sz) {
    const char *slash = strrchr(path, '/');
    snprintf(out, out_sz, "%.*s", slash ? (int)(slash - path) : 0, path);
}

/* ── Functions per file ──────────────────────────────────────────────── */

/* One definition span of a function node: Go method QNs carry no receiver,
 * so the methods of several types in one package (every reconciler's
 * SetupWithManager) are one node standing for spans in several files. */
typedef struct {
    const cbm_gbuf_node_t *node;
    int start;
    int end;
} op_span_t;

typedef struct {
    op_span_t *items;
    int count;
    int cap;
} op_spans_t;

static void op_spans_push(op_spans_t *v, op_span_t span) {
    if (v->count == v->cap) {
        int cap = v->cap ? v->cap * PAIR_LEN : OP_GROW_MIN;
        op_span_t *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, v->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        v->items = grown;
        v->cap = cap;
    }
    v->items[v->count++] = span;
}

static void op_free_spans(const char *key, void *value, void *ud) {
    (void)ud;
    op_spans_t *v = value;
    cbm_free(CBM_MEM_CLASS_OTHER, v->items);
    cbm_free(CBM_MEM_CLASS_OTHER, v);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

typedef struct {
    CBMHashTable *by_file;
    const cbm_gbuf_node_t *node;
} op_index_ctx_t;

static void op_index_span(const char *file, int start, int end, void *ud) {
    op_index_ctx_t *c = ud;
    op_spans_t *v = cbm_ht_get(c->by_file, file);
    if (!v) {
        v = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*v));
        char *key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, file);
        if (!v || !key) {
            cbm_free(CBM_MEM_CLASS_OTHER, v);
            cbm_free(CBM_MEM_CLASS_OTHER, key);
            return;
        }
        cbm_ht_set(c->by_file, key, v);
    }
    op_spans_push(v, (op_span_t){c->node, start, end});
}

/* file_path -> the spans of Function and Method nodes in it (owned keys). */
static CBMHashTable *op_functions_by_file(const cbm_gbuf_t *gb) {
    CBMHashTable *by_file = cbm_ht_create(0);
    static const char *const labels[] = {"Function", "Method"};
    for (size_t l = 0; by_file && l < sizeof(labels) / sizeof(labels[0]); l++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[l], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            op_index_ctx_t c = {by_file, nodes[i]};
            cbm_gbuf_foreach_span(gb, nodes[i], op_index_span, &c);
        }
    }
    return by_file;
}

/* Is span `a` a better enclosing function than `b`: shorter, then (equal
 * spans of two nodes) the lower QN, so the choice never follows index order. */
static bool op_span_better(const op_span_t *a, const op_span_t *b) {
    int la = a->end - a->start;
    int lb = b->end - b->start;
    if (la != lb) {
        return la < lb;
    }
    const char *qa = a->node->qualified_name ? a->node->qualified_name : "";
    const char *qb = b->node->qualified_name ? b->node->qualified_name : "";
    return strcmp(qa, qb) < 0;
}

/* The innermost function of `file` spanning `line`. */
static const cbm_gbuf_node_t *op_enclosing(CBMHashTable *by_file, const char *file, int line) {
    const op_spans_t *v = cbm_ht_get(by_file, file);
    const op_span_t *best = NULL;
    for (int i = 0; v && i < v->count; i++) {
        const op_span_t *sp = &v->items[i];
        if (sp->start <= line && line <= sp->end && (!best || op_span_better(sp, best))) {
            best = sp;
        }
    }
    return best ? best->node : NULL;
}

/* ── Go modules: import path -> directory ─────────────────────────────── */

/* A go.mod of the repository: module path and the directory it roots. */
typedef struct {
    char *path;
    char *dir;
} op_module_t;

typedef struct {
    op_module_t *items;
    int count;
    int cap;
} op_modules_t;

static void op_modules_free(op_modules_t *m) {
    for (int i = 0; i < m->count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, m->items[i].path);
        cbm_free(CBM_MEM_CLASS_OTHER, m->items[i].dir);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, m->items);
    *m = (op_modules_t){0};
}

/* Note the `module` line of a go.mod at `file`. */
static void op_note_module(op_modules_t *m, const char *file, const char *text) {
    for (const char *p = strstr(text, "module"); p; p = strstr(p + SKIP_ONE, "module")) {
        if ((p > text && p[-SKIP_ONE] != '\n') ||
            (p[OP_SLEN("module")] != ' ' && p[OP_SLEN("module")] != '\t')) {
            continue;
        }
        const char *q = p + OP_SLEN("module");
        while (*q == ' ' || *q == '\t' || *q == '"') {
            q++;
        }
        size_t n = strcspn(q, " \t\r\n\"/");
        while (q[n] == '/') { /* module paths contain '/' */
            n += SKIP_ONE + strcspn(q + n + SKIP_ONE, " \t\r\n\"/");
        }
        if (n == 0) {
            return;
        }
        if (m->count == m->cap) {
            int cap = m->cap ? m->cap * PAIR_LEN : OP_GROW_MIN;
            op_module_t *grown =
                cbm_realloc(CBM_MEM_CLASS_OTHER, m->items, (size_t)cap * sizeof(*grown));
            if (!grown) {
                return;
            }
            m->items = grown;
            m->cap = cap;
        }
        char path[CBM_SZ_1K];
        char dir[CBM_SZ_1K];
        snprintf(path, sizeof(path), "%.*s", (int)n, q);
        op_dirname(file, dir, sizeof(dir));
        m->items[m->count].path = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, path);
        m->items[m->count].dir = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, dir);
        m->count++;
        return;
    }
}

/* The repository directory of import path `import`: below the go.mod whose
 * module path is its longest prefix ("" when no module of the repository
 * has it). */
static bool op_import_dir(const op_modules_t *m, const char *import, char *out, size_t out_sz) {
    const op_module_t *best = NULL;
    for (int i = 0; i < m->count; i++) {
        size_t pl = strlen(m->items[i].path);
        if (strncmp(import, m->items[i].path, pl) == 0 &&
            (import[pl] == '\0' || import[pl] == '/') && (!best || pl > strlen(best->path))) {
            best = &m->items[i];
        }
    }
    if (!best) {
        return false;
    }
    const char *rest = import + strlen(best->path);
    rest += *rest == '/';
    snprintf(out, out_sz, "%s%s%s", best->dir, best->dir[0] && rest[0] ? "/" : "", rest);
    return true;
}

/* ── Indexes: Go types by package dir, module roots ───────────────────── */

/* Graph-owned nodes. */
typedef struct {
    const cbm_gbuf_node_t **items;
    int count;
    int cap;
} op_nodes_t;

static void op_nodes_push(op_nodes_t *v, const cbm_gbuf_node_t *n) {
    if (v->count == v->cap) {
        int cap = v->cap ? v->cap * PAIR_LEN : OP_GROW_MIN;
        const cbm_gbuf_node_t **grown =
            cbm_realloc(CBM_MEM_CLASS_OTHER, (void *)v->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        v->items = grown;
        v->cap = cap;
    }
    v->items[v->count++] = n;
}

static void op_free_nodes_entry(const char *key, void *value, void *ud) {
    (void)ud;
    op_nodes_t *v = value;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)v->items);
    cbm_free(CBM_MEM_CLASS_OTHER, v);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

/* "<a>\x1f<b>": a directory (or module root) with a name. */
static void op_key(char *out, size_t out_sz, const char *a, const char *b) {
    snprintf(out, out_sz, "%s\x1f%s", a, b);
}

/* "<dir>\x1f<Name>" -> the Struct / Class / Type nodes Name names in dir. */
static CBMHashTable *op_types_by_dir(const cbm_gbuf_t *gb) {
    CBMHashTable *ix = cbm_ht_create(0);
    static const char *const labels[] = {"Struct", "Class", "Type"};
    for (size_t l = 0; ix && l < sizeof(labels) / sizeof(labels[0]); l++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[l], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            if (!nodes[i]->file_path || !nodes[i]->name) {
                continue;
            }
            char dir[CBM_SZ_1K];
            char key[PAIR_LEN * CBM_SZ_1K];
            op_dirname(nodes[i]->file_path, dir, sizeof(dir));
            op_key(key, sizeof(key), dir, nodes[i]->name);
            op_nodes_t *v = cbm_ht_get(ix, key);
            if (!v) {
                v = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*v));
                char *owned = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
                if (!v || !owned) {
                    cbm_free(CBM_MEM_CLASS_OTHER, v);
                    cbm_free(CBM_MEM_CLASS_OTHER, owned);
                    continue;
                }
                cbm_ht_set(ix, owned, v);
            }
            op_nodes_push(v, nodes[i]);
        }
    }
    return ix;
}

typedef struct {
    const op_modules_t *mods;
    CBMHashTable *dir_types; /* op_types_by_dir */
} op_index_t;

/* ── Controllers: For / Owns / Watches / source.Kind ──────────────────── */

static int op_line_of(const char *text, const char *at) {
    int line = SKIP_ONE;
    for (const char *p = text; p < at; p++) {
        line += *p == '\n';
    }
    return line;
}

static bool op_is_quote(char c) {
    return c == '"' || c == '\'' || c == '`';
}

/* Where a `//` comment starts in the line text [ls, le), outside string and
 * rune literals; le when there is none. */
static const char *op_comment_start(const char *ls, const char *le) {
    char quote = 0;
    for (const char *p = ls; p < le; p++) {
        if (quote) {
            if (*p == '\\' && quote != '`' && p + SKIP_ONE < le) {
                p++;
            } else if (*p == quote) {
                quote = 0;
            }
        } else if (op_is_quote(*p)) {
            quote = *p;
        } else if (*p == '/' && p + SKIP_ONE < le && p[SKIP_ONE] == '/') {
            return p;
        }
    }
    return le;
}

static const char *op_line_start(const char *text, const char *p) {
    while (p > text && p[-SKIP_ONE] != '\n') {
        p--;
    }
    return p;
}

/* The code character before `at`, past blanks and `//` comments: gofmt
 * puts a chain's '.' at the end of an earlier line, and comment lines may
 * sit between. NULL at the start of the text. */
static const char *op_prev_code(const char *text, const char *at) {
    const char *le = at;
    for (;;) {
        const char *ls = op_line_start(text, le);
        const char *end = op_comment_start(ls, le);
        while (end > ls &&
               (end[-SKIP_ONE] == ' ' || end[-SKIP_ONE] == '\t' || end[-SKIP_ONE] == '\r')) {
            end--;
        }
        if (end > ls) {
            return end - SKIP_ONE;
        }
        if (ls == text) {
            return NULL;
        }
        le = ls - SKIP_ONE; /* the previous line, without its '\n' */
    }
}

/* The end of the argument list opened at `open` ('('), balanced; parentheses
 * in `//` comments and literals do not count. */
static const char *op_args_end(const char *open) {
    int depth = 0;
    char quote = 0;
    for (const char *p = open; *p; p++) {
        if (quote) {
            if (*p == '\\' && quote != '`' && p[SKIP_ONE]) {
                p++;
            } else if (*p == quote || (*p == '\n' && quote != '`')) {
                quote = 0;
            }
            continue;
        }
        if (op_is_quote(*p)) {
            quote = *p;
            continue;
        }
        if (*p == '/' && p[SKIP_ONE] == '/') {
            p = strchr(p, '\n');
            if (!p) {
                break;
            }
            continue;
        }
        depth += *p == '(';
        depth -= *p == ')';
        if (depth == 0) {
            return p;
        }
    }
    return open + strlen(open);
}

static bool op_ident_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

/* The import path a file binds to `alias` (an explicit alias, else the
 * path's last segment, Go's usual package name); "" when none. */
static void op_import_path(const char *text, const char *alias, char *out, size_t out_sz) {
    out[0] = '\0';
    size_t al = strlen(alias);
    for (const char *p = strstr(text, "import"); p; p = strstr(p + SKIP_ONE, "import")) {
        if ((p > text && p[-SKIP_ONE] != '\n') || op_ident_char(p[OP_SLEN("import")])) {
            continue;
        }
        const char *q = p + OP_SLEN("import");
        const char *end = q;
        while (*end == ' ' || *end == '\t') {
            end++;
        }
        end = *end == '(' ? strchr(end, ')') : strchr(end, '\n');
        end = end ? end : q + strlen(q);
        /* each import line: [alias] "path" */
        for (const char *l = q; l < end;) {
            while (l < end && (*l == ' ' || *l == '\t' || *l == '(' || *l == '\n' || *l == '\r')) {
                l++;
            }
            const char *name = l;
            while (l < end && op_ident_char(*l)) {
                l++;
            }
            size_t nl = (size_t)(l - name);
            while (l < end && (*l == ' ' || *l == '\t')) {
                l++;
            }
            if (l >= end || *l != '"') {
                while (l < end && *l != '\n') {
                    l++;
                }
                continue;
            }
            const char *ps = l + SKIP_ONE;
            const char *pe = strchr(ps, '"');
            if (!pe || pe > end) {
                break;
            }
            const char *seg = pe;
            while (seg > ps && seg[-SKIP_ONE] != '/') {
                seg--;
            }
            bool hit = nl ? (nl == al && strncmp(name, alias, al) == 0)
                          : ((size_t)(pe - seg) == al && strncmp(seg, alias, al) == 0);
            if (hit) {
                snprintf(out, out_sz, "%.*s", (int)(pe - ps), ps);
                return;
            }
            l = pe + SKIP_ONE;
        }
    }
}

/* Is directory `dir` the package at import path `path`: the path ends with
 * the directory, on a segment boundary (the module path is its prefix). */
static bool op_dir_is_import(const char *dir, const char *path) {
    size_t dl = strlen(dir);
    size_t pl = strlen(path);
    if (dl == 0 || dl > pl || strcmp(path + pl - dl, dir) != 0) {
        return false;
    }
    return dl == pl || path[pl - dl - SKIP_ONE] == '/';
}

/* HANDLES from `fn` to the Go type `type` resolved as Go resolves it: the
 * package the file imports as `qual`, or (unqualified) the file's own -- one
 * directory, looked up in the type index. Only an import no module of the
 * repository has falls back to the same-named types whose directory ends
 * the import path. */
static int op_handle(cbm_gbuf_t *gb, const op_index_t *ix, const cbm_gbuf_node_t *fn,
                     const char *file, const char *text, const char *qual, const char *type,
                     const char *role) {
    char import[CBM_SZ_1K] = "";
    char pkg_dir[CBM_SZ_1K] = "";
    bool in_repo = false;
    if (qual[0]) {
        op_import_path(text, qual, import, sizeof(import));
        if (!import[0]) {
            return 0;
        }
        in_repo = op_import_dir(ix->mods, import, pkg_dir, sizeof(pkg_dir));
    } else {
        op_dirname(file, pkg_dir, sizeof(pkg_dir));
    }
    char props[CBM_SZ_128];
    snprintf(props, sizeof(props), "{\"via\":\"controller\",\"role\":\"%s\"}", role);
    int linked = 0;
    if (!qual[0] || in_repo) {
        char key[PAIR_LEN * CBM_SZ_1K];
        op_key(key, sizeof(key), pkg_dir, type);
        const op_nodes_t *v = cbm_ht_get(ix->dir_types, key);
        atomic_fetch_add_explicit(&g_op_candidates, (uint64_t)(v ? v->count : 0),
                                  memory_order_relaxed);
        for (int i = 0; v && i < v->count; i++) {
            cbm_gbuf_insert_edge(gb, fn->id, v->items[i]->id, "HANDLES", props);
            linked++;
        }
        return linked;
    }
    const cbm_gbuf_node_t **types = NULL;
    int tn = 0;
    if (cbm_gbuf_find_by_name(gb, type, &types, &tn) != 0) {
        return 0;
    }
    atomic_fetch_add_explicit(&g_op_candidates, (uint64_t)(tn), memory_order_relaxed);
    for (int i = 0; i < tn; i++) {
        const cbm_gbuf_node_t *t = types[i];
        if (!t->label || !t->file_path ||
            (strcmp(t->label, "Struct") != 0 && strcmp(t->label, "Class") != 0 &&
             strcmp(t->label, "Type") != 0)) {
            continue;
        }
        char dir[CBM_SZ_1K];
        op_dirname(t->file_path, dir, sizeof(dir));
        if (op_dir_is_import(dir, import)) {
            cbm_gbuf_insert_edge(gb, fn->id, t->id, "HANDLES", props);
            linked++;
        }
    }
    return linked;
}

/* Every `&[pkg.]Type{` in the arguments of a controller builder call. */
static int op_controller_calls(cbm_gbuf_t *gb, CBMHashTable *by_file, const op_index_t *ix,
                               const char *file, const char *text) {
    /* method name + "(", called on a receiver outside a comment: the previous
     * code char is '.', and Kind's receiver is the `source` package. */
    static const struct {
        const char *name;
        const char *recv;
        const char *role;
    } calls[] = {{"For(", NULL, "for"},
                 {"Owns(", NULL, "owns"},
                 {"Watches(", NULL, "watches"},
                 {"Kind(", "source", "watches"}};
    int linked = 0;
    for (size_t c = 0; c < sizeof(calls) / sizeof(calls[0]); c++) {
        for (const char *p = strstr(text, calls[c].name); p;
             p = strstr(p + SKIP_ONE, calls[c].name)) {
            if (p > text && op_ident_char(p[-SKIP_ONE])) {
                continue; /* part of a longer name */
            }
            if (op_comment_start(op_line_start(text, p), p) < p) {
                continue;
            }
            const char *dot = op_prev_code(text, p);
            if (!dot || *dot != '.') {
                continue;
            }
            if (calls[c].recv) {
                size_t rl = strlen(calls[c].recv);
                const char *r = dot - rl;
                if (r < text || strncmp(r, calls[c].recv, rl) != 0 ||
                    (r > text && op_ident_char(r[-SKIP_ONE]))) {
                    continue;
                }
            }
            const char *open = p + strlen(calls[c].name) - SKIP_ONE;
            const char *end = op_args_end(open);
            const cbm_gbuf_node_t *fn = op_enclosing(by_file, file, op_line_of(text, p));
            if (!fn) {
                continue;
            }
            for (const char *a = open; a < end; a++) {
                if (*a == '/' && a[SKIP_ONE] == '/') {
                    const char *nl = strchr(a, '\n');
                    a = nl && nl < end ? nl : end;
                    continue;
                }
                if (*a != '&') {
                    continue;
                }
                const char *q = a + SKIP_ONE;
                const char *first = q;
                const char *last = q;
                while (q < end && (op_ident_char(*q) || *q == '.')) {
                    if (*q == '.') {
                        last = q + SKIP_ONE;
                    }
                    q++;
                }
                if (q < end && *q == '{' && q > last && (size_t)(q - last) < OP_IDENT_MAX &&
                    (size_t)(last - first) < OP_IDENT_MAX) {
                    char type[OP_IDENT_MAX];
                    char qual[OP_IDENT_MAX];
                    snprintf(type, sizeof(type), "%.*s", (int)(q - last), last);
                    snprintf(qual, sizeof(qual), "%.*s",
                             last > first ? (int)(last - first - SKIP_ONE) : 0, first);
                    linked += op_handle(gb, ix, fn, file, text, qual, type, calls[c].role);
                }
            }
        }
    }
    return linked;
}

/* ── API groups: +groupName / GroupName ───────────────────────────────── */

/* A Go identifier-or-DNS token at p (the group name). */
static void op_token(const char *p, char *out, size_t out_sz) {
    size_t n = 0;
    while (p[n] && (op_ident_char(p[n]) || p[n] == '.' || p[n] == '-') && n + SKIP_ONE < out_sz) {
        n++;
    }
    snprintf(out, out_sz, "%.*s", (int)n, p);
}

/* Owned strings. */
typedef struct {
    char **items;
    int count;
    int cap;
} op_strs_t;

static void op_strs_push(op_strs_t *v, const char *s) {
    if (v->count == v->cap) {
        int cap = v->cap ? v->cap * PAIR_LEN : OP_GROW_MIN;
        char **grown = cbm_realloc(CBM_MEM_CLASS_OTHER, v->items, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        v->items = grown;
        v->cap = cap;
    }
    char *copy = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, s);
    if (copy) {
        v->items[v->count++] = copy;
    }
}

/* group -> the package directories that declare it (owned keys and lists);
 * `seen` holds "group\x1fdir" (owned keys) so a repeat is one lookup. */
static void op_note_group(CBMHashTable *groups, CBMHashTable *seen, const char *group,
                          const char *dir) {
    char key[PAIR_LEN * CBM_SZ_1K];
    op_key(key, sizeof(key), group, dir);
    if (!group[0] || cbm_ht_get(seen, key)) {
        return;
    }
    char *seen_key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
    if (!seen_key) {
        return;
    }
    cbm_ht_set(seen, seen_key, seen_key);
    op_strs_t *dirs = cbm_ht_get(groups, group);
    if (!dirs) {
        dirs = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*dirs));
        char *gkey = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, group);
        if (!dirs || !gkey) {
            cbm_free(CBM_MEM_CLASS_OTHER, dirs);
            cbm_free(CBM_MEM_CLASS_OTHER, gkey);
            return;
        }
        cbm_ht_set(groups, gkey, dirs);
    }
    op_strs_push(dirs, dir);
}

static void op_scan_groups(CBMHashTable *groups, CBMHashTable *seen, const char *file,
                           const char *text) {
    char dir[CBM_SZ_1K];
    op_dirname(file, dir, sizeof(dir));
    for (const char *p = strstr(text, "+groupName="); p; p = strstr(p + SKIP_ONE, "+groupName=")) {
        char g[CBM_SZ_256];
        op_token(p + OP_SLEN("+groupName="), g, sizeof(g));
        op_note_group(groups, seen, g, dir);
    }
    for (const char *p = strstr(text, "GroupName"); p; p = strstr(p + SKIP_ONE, "GroupName")) {
        const char *q = p + OP_SLEN("GroupName");
        while (*q == ' ' || *q == '\t') {
            q++;
        }
        if (*q != '=') {
            continue;
        }
        q++;
        while (*q == ' ' || *q == '\t') {
            q++;
        }
        if (*q == '"') {
            char g[CBM_SZ_256];
            op_token(q + SKIP_ONE, g, sizeof(g));
            op_note_group(groups, seen, g, dir);
        }
    }
}

static void op_free_seen(const char *key, void *value, void *ud) {
    (void)value;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

static void op_free_groups(const char *key, void *value, void *ud) {
    (void)ud;
    op_strs_t *dirs = value;
    for (int i = 0; i < dirs->count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, dirs->items[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, dirs->items);
    cbm_free(CBM_MEM_CLASS_OTHER, dirs);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

/* A JSON string property's value (simple, unescaped scalars). */
static bool op_prop(const char *props, const char *key, char *out, size_t out_sz) {
    char pat[CBM_SZ_64];
    snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    const char *p = props ? strstr(props, pat) : NULL;
    if (!p) {
        return false;
    }
    p += strlen(pat);
    const char *e = strchr(p, '"');
    if (!e) {
        return false;
    }
    snprintf(out, out_sz, "%.*s", (int)(e - p), p);
    return true;
}

/* CRD -> the Go type declared under its group, in every package marking it:
 * an API group is one identity, and its CRDs and Go types routinely sit in
 * different modules (cluster-api's api/, kubermatic's sdk/). */
static int op_link_crd_types(cbm_gbuf_t *gb, const op_index_t *ix, CBMHashTable *groups) {
    const cbm_gbuf_node_t **res = NULL;
    int count = 0;
    int linked = 0;
    if (cbm_gbuf_find_by_label(gb, "Resource", &res, &count) != 0) {
        return 0;
    }
    for (int i = 0; i < count; i++) {
        char group[CBM_SZ_256];
        char kind[CBM_SZ_256];
        if (!res[i]->file_path ||
            !op_prop(res[i]->properties_json, "crd_group", group, sizeof(group)) ||
            !op_prop(res[i]->properties_json, "crd_kind", kind, sizeof(kind))) {
            continue;
        }
        char key[PAIR_LEN * CBM_SZ_1K];
        const op_strs_t *dirs = cbm_ht_get(groups, group);
        if (!dirs) {
            continue;
        }
        atomic_fetch_add_explicit(&g_op_candidates, (uint64_t)dirs->count, memory_order_relaxed);
        for (int d = 0; d < dirs->count; d++) {
            op_key(key, sizeof(key), dirs->items[d], kind);
            const op_nodes_t *v = cbm_ht_get(ix->dir_types, key);
            for (int t = 0; v && t < v->count; t++) {
                if (v->items[t]->label && strcmp(v->items[t]->label, "Struct") == 0) {
                    cbm_gbuf_insert_edge(gb, res[i]->id, v->items[t]->id, "USAGE",
                                         "{\"ref\":\"crd-type\"}");
                    linked++;
                }
            }
        }
    }
    return linked;
}

/* ── go:embed ─────────────────────────────────────────────────────────── */

/* Go's path.Match for one segment: '*', '?', '[...]', '\\' escapes. */
static bool op_match(const char *pat, const char *s) {
    while (*pat) {
        if (*pat == '*') {
            pat++;
            for (const char *t = s;; t++) {
                if (op_match(pat, t)) {
                    return true;
                }
                if (!*t) {
                    return false;
                }
            }
        }
        if (!*s) {
            return false;
        }
        if (*pat == '?') {
            pat++;
            s++;
            continue;
        }
        if (*pat == '[') {
            const char *q = pat + SKIP_ONE;
            bool neg = *q == '^';
            q += neg;
            bool hit = false;
            while (*q && *q != ']') {
                char lo = *q;
                char hi = lo;
                if (q[SKIP_ONE] == '-' && q[PAIR_LEN] && q[PAIR_LEN] != ']') {
                    hi = q[PAIR_LEN];
                    q += PAIR_LEN;
                }
                hit = hit || (*s >= lo && *s <= hi);
                q++;
            }
            if (*q != ']' || hit == neg) {
                return false;
            }
            pat = q + SKIP_ONE;
            s++;
            continue;
        }
        if (*pat == '\\' && pat[SKIP_ONE]) {
            pat++;
        }
        if (*pat != *s) {
            return false;
        }
        pat++;
        s++;
    }
    return *s == '\0';
}

/* Does `rel` (a path below the Go file's directory) match the embed
 * pattern: segment by segment, a matched directory taking its files except
 * hidden ones ('.', '_') unless `all`. */
static bool op_embeds(const char *pattern, const char *rel, bool all) {
    char pat[CBM_SZ_1K];
    char path[CBM_SZ_1K];
    snprintf(pat, sizeof(pat), "%s", pattern);
    snprintf(path, sizeof(path), "%s", rel);
    char *ps = pat;
    char *rs = path;
    while (ps && rs) {
        char *pn = strchr(ps, '/');
        char *rn = strchr(rs, '/');
        if (pn) {
            *pn = '\0';
        }
        if (rn) {
            *rn = '\0';
        }
        if (!op_match(ps, rs)) {
            return false;
        }
        ps = pn ? pn + SKIP_ONE : NULL;
        rs = rn ? rn + SKIP_ONE : NULL;
        if (!ps) {
            /* pattern consumed: the file itself, or a file under a matched dir */
            for (char *seg = rs; seg && !all;) {
                if (seg[0] == '.' || seg[0] == '_') {
                    return false;
                }
                char *next = strchr(seg, '/');
                seg = next ? next + SKIP_ONE : NULL;
            }
            return true;
        }
    }
    return false;
}

static int op_cmp_path(const void *a, const void *b) {
    const cbm_gbuf_node_t *x = *(const cbm_gbuf_node_t *const *)a;
    const cbm_gbuf_node_t *y = *(const cbm_gbuf_node_t *const *)b;
    return strcmp(x->file_path, y->file_path);
}

/* REFERENCES_FILE from the Go file to every file its //go:embed lines name;
 * `files` is every File node sorted by path, so a directory's files are one
 * range. */
static int op_embed(cbm_gbuf_t *gb, const cbm_gbuf_node_t *go_file, const char *text,
                    const cbm_gbuf_node_t **files, int nfiles) {
    char dir[CBM_SZ_1K];
    op_dirname(go_file->file_path, dir, sizeof(dir));
    size_t dl = strlen(dir);
    int linked = 0;
    for (const char *p = strstr(text, "//go:embed "); p; p = strstr(p + SKIP_ONE, "//go:embed ")) {
        if (p != text && p[-SKIP_ONE] != '\n') {
            continue; /* a directive starts its line */
        }
        const char *eol = strchr(p, '\n');
        char line[CBM_SZ_1K];
        const char *args = p + OP_SLEN("//go:embed ");
        snprintf(line, sizeof(line), "%.*s", eol ? (int)(eol - args) : (int)strlen(args), args);
        for (char *tok = line; *tok;) {
            while (*tok == ' ' || *tok == '\t' || *tok == '\r') {
                tok++;
            }
            char *e = tok;
            while (*e && *e != ' ' && *e != '\t' && *e != '\r') {
                e++;
            }
            char save = *e;
            *e = '\0';
            bool all = strncmp(tok, "all:", OP_SLEN("all:")) == 0;
            const char *pattern = all ? tok + OP_SLEN("all:") : tok;
            if (pattern[0] && pattern[0] != '"' && pattern[0] != '`') {
                /* files below dir: the sorted range starting at "dir/" */
                int lo = 0;
                int hi = dl ? nfiles : 0;
                while (lo < hi) {
                    int mid = lo + (hi - lo) / PAIR_LEN;
                    if (strncmp(files[mid]->file_path, dir, dl) < 0 ||
                        (strncmp(files[mid]->file_path, dir, dl) == 0 &&
                         files[mid]->file_path[dl] < '/')) {
                        lo = mid + SKIP_ONE;
                    } else {
                        hi = mid;
                    }
                }
                for (int i = lo; i < nfiles; i++) {
                    const char *fp = files[i]->file_path;
                    if (dl && (strncmp(fp, dir, dl) != 0 || fp[dl] != '/')) {
                        break;
                    }
                    const char *rel = dl ? fp + dl + SKIP_ONE : fp;
                    if (files[i] != go_file && op_embeds(pattern, rel, all)) {
                        cbm_gbuf_insert_edge(gb, go_file->id, files[i]->id, "REFERENCES_FILE",
                                             "{\"via\":\"go:embed\"}");
                        linked++;
                    }
                }
            }
            *e = save;
            tok = e;
        }
    }
    return linked;
}

/* ── The pass ─────────────────────────────────────────────────────────── */

static bool op_is_go(const char *path) {
    size_t n = strlen(path);
    return n > OP_SLEN(".go") && strcmp(path + n - OP_SLEN(".go"), ".go") == 0;
}

/* ── Helm charts the Go code loads (PHASE 3 P3.3) ─────────────────────── */

/* An installer written in Go (kubermatic's) deploys charts it loads from a
 * charts directory: helm.LoadChart(filepath.Join(opt.ChartsDirectory,
 * MonitoringChartsPrefix, NodeExporterChartName)). A chart loader is the
 * Helm SDK's loader.Load / loader.LoadDir, or a function of the repository
 * whose body reads `<param>/Chart.yaml` or hands its parameter to the SDK
 * loader. Its call DEPLOYS {via: helm-sdk} the chart directory its argument
 * names, evaluated over string literals, the package's constants (and an
 * imported package's), filepath.Join / path.Join, + and a local assigned
 * once: a leading unknown part (a charts root chosen at run time) leaves a
 * known tail, linked when exactly one chart directory of the repository ends
 * with it. */

typedef struct {
    char *dir;  /* package directory */
    char *name; /* function */
    int param;  /* index of the chart-directory parameter */
} gc_loader_t;

typedef struct {
    const char *dir; /* graph-owned chart dir path */
    int64_t id;
} gc_chart_t;

typedef struct {
    gc_chart_t *items;
    int count;
    int cap;
} gc_charts_t;

/* One chart load whose argument evaluated: linked once all are read. */
typedef struct {
    int64_t fn;
    int line;
    char *tail;
    char *root; /* NULL: no unknown root */
} gc_site_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    const op_modules_t *mods;
    CBMHashTable *by_file;
    const cbm_gbuf_node_t **files; /* sorted File nodes */
    int nfiles;
    gc_loader_t *loaders;
    int nloaders;
    int cap_loaders;
    char **noted; /* Go files that mention a chart */
    int nnoted;
    int cap_noted;
    CBMHashTable *consts; /* dir -> CBMHashTable name -> expression (owned) */
    CBMHashTable *charts; /* chart dir basename -> gc_charts_t */
    gc_site_t *sites;
    int nsites;
    int cap_sites;
    int edges;
} gc_t;

typedef struct {
    char s[CBM_SZ_1K];
    bool known;
    bool open_root;        /* an unknown leading part: s is a tail */
    char root[CBM_SZ_256]; /* that part's source text (opt.ChartsDirectory) */
} gc_val_t;

enum { GC_ARGS_MAX = 8, GC_EVAL_DEPTH = 16 };

static bool gc_grow(void **items, int *cap, int count, size_t elem) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : OP_GROW_MIN;
    void *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, *items, (size_t)ncap * elem);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = ncap;
    return true;
}

/* Past a string / rune literal or a comment starting at p; p when none. */
static const char *gc_skip(const char *p) {
    if (*p == '"' || *p == '\'') {
        char q = *p++;
        while (*p && *p != q && *p != '\n') {
            p += (*p == '\\' && p[SKIP_ONE]) ? PAIR_LEN : SKIP_ONE;
        }
        return *p == q ? p + SKIP_ONE : p;
    }
    if (*p == '`') {
        const char *e = strchr(p + SKIP_ONE, '`');
        return e ? e + SKIP_ONE : p + strlen(p);
    }
    if (p[0] == '/' && p[SKIP_ONE] == '/') {
        const char *e = strchr(p, '\n');
        return e ? e : p + strlen(p);
    }
    if (p[0] == '/' && p[SKIP_ONE] == '*') {
        const char *e = strstr(p + PAIR_LEN, "*/");
        return e ? e + PAIR_LEN : p + strlen(p);
    }
    return p;
}

/* The '}' closing the block opened at `open`. */
static const char *gc_block_end(const char *open) {
    int depth = 0;
    for (const char *p = open; *p;) {
        const char *q = gc_skip(p);
        if (q != p) {
            p = q;
            continue;
        }
        depth += *p == '{';
        depth -= *p == '}';
        if (depth == 0) {
            return p;
        }
        p++;
    }
    return open + strlen(open);
}

/* The top-level comma-separated parts of the list opened at `open` ('(').
 * Returns their count (at most `max`); *end: the closing ')'. */
static int gc_args(const char *open, const char *starts[], size_t lens[], int max,
                   const char **end) {
    int n = 0;
    int depth = 0;
    const char *start = open + SKIP_ONE;
    for (const char *p = open; *p;) {
        const char *q = gc_skip(p);
        if (q != p) {
            p = q;
            continue;
        }
        if (strchr("([{", *p)) {
            depth++;
        } else if (strchr(")]}", *p)) {
            depth--;
            if (depth == 0) {
                if (n < max && p > start) {
                    starts[n] = start;
                    lens[n++] = (size_t)(p - start);
                }
                *end = p;
                return n;
            }
        } else if (*p == ',' && depth == SKIP_ONE && n < max) {
            starts[n] = start;
            lens[n++] = (size_t)(p - start);
            start = p + SKIP_ONE;
        }
        p++;
    }
    *end = open + strlen(open);
    return n;
}

static void gc_trim(const char **s, size_t *n) {
    while (*n && strchr(" \t\r\n", **s)) {
        (*s)++;
        (*n)--;
    }
    while (*n && strchr(" \t\r\n", (*s)[*n - SKIP_ONE])) {
        (*n)--;
    }
}

static bool gc_is_ident(const char *s, size_t n) {
    if (n == 0 || (s[0] >= '0' && s[0] <= '9')) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!op_ident_char(s[i])) {
            return false;
        }
    }
    return true;
}

static bool gc_ident_eq(const char *s, size_t n, const char *name) {
    return strlen(name) == n && strncmp(s, name, n) == 0;
}

/* The repository directory a file binds package qualifier `qual` to. */
static bool gc_qual_dir(const gc_t *g, const char *text, const char *qual, size_t ql, char *out,
                        size_t out_sz) {
    char alias[OP_IDENT_MAX];
    char import[CBM_SZ_1K];
    if (ql == 0 || ql >= sizeof(alias)) {
        return false;
    }
    snprintf(alias, sizeof(alias), "%.*s", (int)ql, qual);
    op_import_path(text, alias, import, sizeof(import));
    return import[0] && op_import_dir(g->mods, import, out, out_sz);
}

static bool gc_qual_is(const char *text, const char *qual, size_t ql, const char *path) {
    char alias[OP_IDENT_MAX];
    char import[CBM_SZ_1K];
    if (ql == 0 || ql >= sizeof(alias)) {
        return false;
    }
    snprintf(alias, sizeof(alias), "%.*s", (int)ql, qual);
    op_import_path(text, alias, import, sizeof(import));
    return strcmp(import, path) == 0;
}

static const char GC_SDK_LOADER[] = "helm.sh/helm/v3/pkg/chart/loader";

/* The flag before a noted file's path: it imports the SDK loader, or not. */
enum { GC_NOTED_SDK = 'S', GC_NOTED_PLAIN = '-' };

/* ── Loaders: functions whose parameter is a chart directory ── */

static void gc_note_loader(gc_t *g, const char *dir, const char *name, size_t nl, int param) {
    for (int i = 0; i < g->nloaders; i++) {
        if (strcmp(g->loaders[i].dir, dir) == 0 && gc_ident_eq(name, nl, g->loaders[i].name)) {
            return;
        }
    }
    if (!gc_grow((void **)&g->loaders, &g->cap_loaders, g->nloaders, sizeof(*g->loaders))) {
        return;
    }
    char nm[OP_IDENT_MAX];
    snprintf(nm, sizeof(nm), "%.*s", (int)nl, name);
    g->loaders[g->nloaders++] = (gc_loader_t){cbm_mem_strdup(CBM_MEM_CLASS_OTHER, dir),
                                              cbm_mem_strdup(CBM_MEM_CLASS_OTHER, nm), param};
}

/* Does body [b, e) load a chart from the parameter named p: Join(p,
 * "Chart.yaml") or the SDK loader on p. */
static bool gc_body_loads(const char *text, const char *b, const char *e, const char *p,
                          size_t pl) {
    for (const char *s = b; s < e;) {
        const char *q = gc_skip(s);
        if (q != s) {
            s = q;
            continue;
        }
        const char *open = NULL;
        bool join = false;
        if (strncmp(s, "filepath.Join(", OP_SLEN("filepath.Join(")) == 0) {
            open = s + OP_SLEN("filepath.Join");
            join = true;
        } else if (strncmp(s, "path.Join(", OP_SLEN("path.Join(")) == 0 &&
                   (s == b || !op_ident_char(s[-SKIP_ONE]))) {
            open = s + OP_SLEN("path.Join");
            join = true;
        } else if ((strncmp(s, ".Load(", OP_SLEN(".Load(")) == 0 ||
                    strncmp(s, ".LoadDir(", OP_SLEN(".LoadDir(")) == 0)) {
            const char *qe = s;
            const char *qs = qe;
            while (qs > text && op_ident_char(qs[-SKIP_ONE])) {
                qs--;
            }
            if (gc_qual_is(text, qs, (size_t)(qe - qs), GC_SDK_LOADER)) {
                open = strchr(s, '(');
            }
        }
        if (!open) {
            s++;
            continue;
        }
        const char *starts[GC_ARGS_MAX];
        size_t lens[GC_ARGS_MAX];
        const char *end = NULL;
        int n = gc_args(open, starts, lens, GC_ARGS_MAX, &end);
        for (int i = 0; i < n; i++) {
            gc_trim(&starts[i], &lens[i]);
        }
        if (n > 0 && lens[0] == pl && strncmp(starts[0], p, pl) == 0) {
            if (!join && n == SKIP_ONE) {
                return true;
            }
            if (join && n == PAIR_LEN &&
                (gc_ident_eq(starts[SKIP_ONE], lens[SKIP_ONE], "\"Chart.yaml\"") ||
                 gc_ident_eq(starts[SKIP_ONE], lens[SKIP_ONE], "\"Chart.yml\""))) {
                return true;
            }
        }
        s = end > s ? end : s + SKIP_ONE;
    }
    return false;
}

/* Note the file's top-level functions (not methods) that load a chart from
 * a parameter. */
static void gc_summarize(gc_t *g, const char *file, const char *text) {
    char dir[CBM_SZ_1K];
    op_dirname(file, dir, sizeof(dir));
    for (const char *p = strstr(text, "func "); p; p = strstr(p + SKIP_ONE, "func ")) {
        if (p > text && p[-SKIP_ONE] != '\n') {
            continue;
        }
        const char *name = p + OP_SLEN("func ");
        const char *ne = name;
        while (op_ident_char(*ne)) {
            ne++;
        }
        if (ne == name || *ne != '(') {
            continue; /* a method, or generic */
        }
        const char *starts[GC_ARGS_MAX];
        size_t lens[GC_ARGS_MAX];
        const char *pend = NULL;
        int np = gc_args(ne, starts, lens, GC_ARGS_MAX, &pend);
        const char *open = pend;
        while (*open && *open != '{' && *open != '\n') {
            open++;
        }
        if (*open != '{') {
            continue;
        }
        const char *close = gc_block_end(open);
        for (int i = 0; i < np; i++) {
            const char *ps = starts[i];
            size_t pl = lens[i];
            gc_trim(&ps, &pl);
            size_t idl = 0;
            while (idl < pl && op_ident_char(ps[idl])) {
                idl++;
            }
            if (idl && gc_body_loads(text, open, close, ps, idl)) {
                gc_note_loader(g, dir, name, (size_t)(ne - name), i);
            }
        }
    }
}

/* ── Constants of a package ── */

static void gc_free_expr(const char *key, void *value, void *ud) {
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
    cbm_free(CBM_MEM_CLASS_OTHER, value);
}

static void gc_free_consts(const char *key, void *value, void *ud) {
    (void)ud;
    cbm_ht_foreach(value, gc_free_expr, NULL);
    cbm_ht_free(value);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

/* One `NAME [TYPE] = expr` of a const declaration, line [ls, le). */
static void gc_const_line(CBMHashTable *t, const char *ls, const char *le) {
    while (ls < le && (*ls == ' ' || *ls == '\t')) {
        ls++;
    }
    const char *ne = ls;
    while (ne < le && op_ident_char(*ne)) {
        ne++;
    }
    const char *eq = memchr(ne, '=', (size_t)(le - ne));
    if (ne == ls || !eq || (eq + SKIP_ONE < le && eq[SKIP_ONE] == '=')) {
        return;
    }
    const char *ve = op_comment_start(eq + SKIP_ONE, le);
    const char *vs = eq + SKIP_ONE;
    size_t vl = (size_t)(ve - vs);
    gc_trim(&vs, &vl);
    char name[OP_IDENT_MAX];
    snprintf(name, sizeof(name), "%.*s", (int)(ne - ls), ls);
    if (vl == 0 || cbm_ht_get(t, name)) {
        return;
    }
    char *key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, name);
    char *val = cbm_alloc(CBM_MEM_CLASS_OTHER, vl + SKIP_ONE);
    if (!key || !val) {
        cbm_free(CBM_MEM_CLASS_OTHER, key);
        cbm_free(CBM_MEM_CLASS_OTHER, val);
        return;
    }
    memcpy(val, vs, vl);
    val[vl] = '\0';
    cbm_ht_set(t, key, val);
}

static void gc_scan_consts(CBMHashTable *t, const char *text) {
    for (const char *p = strstr(text, "const"); p; p = strstr(p + SKIP_ONE, "const")) {
        if ((p > text && p[-SKIP_ONE] != '\n') || op_ident_char(p[OP_SLEN("const")])) {
            continue;
        }
        const char *q = p + OP_SLEN("const");
        while (*q == ' ' || *q == '\t') {
            q++;
        }
        if (*q == '(') {
            const char *starts[SKIP_ONE];
            size_t lens[SKIP_ONE];
            const char *end = NULL;
            (void)gc_args(q, starts, lens, 0, &end);
            for (const char *l = q + SKIP_ONE; l < end;) {
                const char *le = memchr(l, '\n', (size_t)(end - l));
                le = le ? le : end;
                gc_const_line(t, l, le);
                l = le + SKIP_ONE;
            }
        } else {
            const char *le = strchr(q, '\n');
            gc_const_line(t, q, le ? le : q + strlen(q));
        }
    }
}

/* The constants of package directory `dir` (its non-test Go files). */
static CBMHashTable *gc_consts(gc_t *g, const char *dir) {
    CBMHashTable *t = cbm_ht_get(g->consts, dir);
    if (t) {
        return t;
    }
    t = cbm_ht_create(0);
    char *key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, dir);
    if (!t || !key) {
        if (t) {
            cbm_ht_free(t);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, key);
        return NULL;
    }
    cbm_ht_set(g->consts, key, t);
    size_t dl = strlen(dir);
    for (int i = 0; i < g->nfiles; i++) {
        const char *fp = g->files[i]->file_path;
        const char *slash = strrchr(fp, '/');
        size_t fdl = slash ? (size_t)(slash - fp) : 0;
        size_t fl = strlen(fp);
        if (fdl != dl || strncmp(fp, dir, dl) != 0 || !op_is_go(fp) ||
            (fl > OP_SLEN("_test.go") && strcmp(fp + fl - OP_SLEN("_test.go"), "_test.go") == 0)) {
            continue;
        }
        char path[CBM_SZ_4K];
        snprintf(path, sizeof(path), "%s/%s", g->ctx->repo_path, fp);
        char *text = op_read_file(path);
        if (text) {
            gc_scan_consts(t, text);
            cbm_free(CBM_MEM_CLASS_OTHER, text);
        }
    }
    return t;
}

/* ── Values ── */

typedef struct {
    gc_t *g;
    const char *text; /* the call's file */
    const char *dir;  /* its package */
    const char *sig;  /* the enclosing function's `func` .. its body, NULL at top level */
    const char *body;
    const char *body_end;
    const char *at; /* the call */
} gc_scope_t;

/* Is `name` a word of the enclosing function's signature (a parameter). */
static bool gc_is_param(const gc_scope_t *sc, const char *name, size_t nl) {
    for (const char *p = sc->sig; sc->sig && p + nl <= sc->body; p++) {
        if (strncmp(p, name, nl) == 0 && !op_ident_char(p[-SKIP_ONE]) && !op_ident_char(p[nl])) {
            return true;
        }
    }
    return false;
}

static void gc_eval(const gc_scope_t *sc, const char *s, size_t n, int depth, gc_val_t *out);

static void gc_join(gc_val_t *acc, const gc_val_t *part, bool path) {
    if (!acc->known) {
        return;
    }
    bool empty = !acc->s[0];
    if (!part->known || part->open_root) {
        if (!path || !empty) {
            acc->known = false; /* an unknown part after a known one */
            return;
        }
        acc->open_root = true; /* still the leading, unknown root */
        if (!part->known) {
            return;
        }
    }
    size_t al = strlen(acc->s);
    bool sep = path && al && part->s[0] && acc->s[al - SKIP_ONE] != '/' && part->s[0] != '/';
    snprintf(acc->s + al, sizeof(acc->s) - al, "%s%s", sep ? "/" : "", part->s);
}

/* A local assigned exactly once in the enclosing function, before the call. */
static bool gc_local(const gc_scope_t *sc, const char *name, size_t nl, const char **vs,
                     size_t *vlen) {
    if (!sc->body) {
        return false;
    }
    int seen = 0;
    for (const char *p = sc->body; p < sc->body_end;) {
        const char *q = gc_skip(p);
        if (q != p) {
            p = q;
            continue;
        }
        if (strncmp(p, name, nl) == 0 && (p == sc->body || !op_ident_char(p[-SKIP_ONE])) &&
            !op_ident_char(p[nl]) && p[-SKIP_ONE] != '.') {
            const char *a = p + nl;
            while (*a == ' ' || *a == '\t') {
                a++;
            }
            if (a[0] && strchr("+-*/%|&^", a[0]) && a[SKIP_ONE] == '=') {
                return false; /* modified in place */
            }
            bool assign =
                (a[0] == ':' && a[SKIP_ONE] == '=') || (a[0] == '=' && a[SKIP_ONE] != '=');
            if (assign) {
                if (++seen > SKIP_ONE || p > sc->at) {
                    return false;
                }
                const char *v = a + (a[0] == ':' ? PAIR_LEN : SKIP_ONE);
                const char *le = strchr(v, '\n');
                le = le ? le : v + strlen(v);
                *vs = v;
                *vlen = (size_t)(op_comment_start(v, le) - v);
            }
        }
        p++;
    }
    return seen == SKIP_ONE;
}

static void gc_const_value(const gc_scope_t *sc, const char *dir, const char *name, int depth,
                           gc_val_t *out) {
    CBMHashTable *t = gc_consts(sc->g, dir);
    const char *expr = t ? cbm_ht_get(t, name) : NULL;
    if (!expr) {
        out->known = false;
        return;
    }
    gc_scope_t inner = {sc->g, sc->text, dir, NULL, NULL, NULL, NULL};
    gc_eval(&inner, expr, strlen(expr), depth + SKIP_ONE, out);
}

static void gc_eval(const gc_scope_t *sc, const char *s, size_t n, int depth, gc_val_t *out) {
    *out = (gc_val_t){.known = false};
    gc_trim(&s, &n);
    if (n == 0 || depth > GC_EVAL_DEPTH) {
        return;
    }
    /* a + b */
    int pd = 0;
    for (size_t i = 0; i < n; i++) {
        const char *q = gc_skip(s + i);
        if (q != s + i) {
            i = (size_t)(q - s) - SKIP_ONE;
            continue;
        }
        pd += strchr("([{", s[i]) != NULL;
        pd -= strchr(")]}", s[i]) != NULL;
        if (s[i] == '+' && pd == 0) {
            gc_val_t a;
            gc_val_t b;
            gc_eval(sc, s, i, depth + SKIP_ONE, &a);
            gc_eval(sc, s + i + SKIP_ONE, n - i - SKIP_ONE, depth + SKIP_ONE, &b);
            *out = a;
            if (!b.known || b.open_root) {
                out->known = false;
            } else if (out->known) {
                gc_join(out, &b, false);
            }
            return;
        }
    }
    if ((s[0] == '"' || s[0] == '`') && gc_skip(s) == s + n) {
        size_t w = 0;
        for (size_t i = SKIP_ONE; i + SKIP_ONE < n && w + SKIP_ONE < sizeof(out->s); i++) {
            if (s[0] == '"' && s[i] == '\\' && i + PAIR_LEN < n) {
                i++;
            }
            out->s[w++] = s[i];
        }
        out->s[w] = '\0';
        out->known = true;
        return;
    }
    bool fp = n > OP_SLEN("filepath.Join(") &&
              strncmp(s, "filepath.Join(", OP_SLEN("filepath.Join(")) == 0;
    bool pp = n > OP_SLEN("path.Join(") && strncmp(s, "path.Join(", OP_SLEN("path.Join(")) == 0;
    if ((fp || pp) && s[n - SKIP_ONE] == ')') {
        const char *open = s + (fp ? OP_SLEN("filepath.Join") : OP_SLEN("path.Join"));
        const char *starts[GC_ARGS_MAX];
        size_t lens[GC_ARGS_MAX];
        const char *end = NULL;
        int na = gc_args(open, starts, lens, GC_ARGS_MAX, &end);
        if (end != s + n - SKIP_ONE || na == 0 || na == GC_ARGS_MAX) {
            return;
        }
        *out = (gc_val_t){.known = true};
        for (int i = 0; i < na && out->known; i++) {
            gc_val_t part;
            gc_eval(sc, starts[i], lens[i], depth + SKIP_ONE, &part);
            bool was_open = out->open_root;
            gc_join(out, &part, true);
            if (out->known && out->open_root && !was_open) {
                if (part.known && part.root[0]) {
                    snprintf(out->root, sizeof(out->root), "%s", part.root);
                } else {
                    const char *rs = starts[i];
                    size_t rl = lens[i];
                    gc_trim(&rs, &rl);
                    snprintf(out->root, sizeof(out->root), "%.*s", (int)rl, rs);
                }
            } else if (out->known && was_open && !part.known) {
                out->root[0] = '\0'; /* several unknown parts: no one root */
            }
        }
        return;
    }
    if (gc_is_ident(s, n)) {
        const char *vs = NULL;
        size_t vl = 0;
        char name[OP_IDENT_MAX];
        snprintf(name, sizeof(name), "%.*s", (int)n, s);
        if (gc_local(sc, s, n, &vs, &vl)) {
            gc_scope_t outer = *sc;
            outer.at = vs; /* what the local reads, read before it */
            gc_eval(&outer, vs, vl, depth + SKIP_ONE, out);
            return;
        }
        if (gc_is_param(sc, s, n)) {
            return; /* an argument: whatever the caller passes */
        }
        gc_const_value(sc, sc->dir, name, depth, out);
        return;
    }
    const char *dot = memchr(s, '.', n);
    if (dot && gc_is_ident(s, (size_t)(dot - s)) &&
        gc_is_ident(dot + SKIP_ONE, n - (size_t)(dot - s) - SKIP_ONE)) {
        char dir[CBM_SZ_1K];
        char name[OP_IDENT_MAX];
        snprintf(name, sizeof(name), "%.*s", (int)(n - (size_t)(dot - s) - SKIP_ONE),
                 dot + SKIP_ONE);
        if (gc_qual_dir(sc->g, sc->text, s, (size_t)(dot - s), dir, sizeof(dir))) {
            gc_const_value(sc, dir, name, depth, out);
        }
    }
}

/* ── Charts and edges ── */

static void gc_free_chart_list(const char *key, void *value, void *ud) {
    (void)ud;
    gc_charts_t *v = value;
    cbm_free(CBM_MEM_CLASS_OTHER, v->items);
    cbm_free(CBM_MEM_CLASS_OTHER, v);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

static CBMHashTable *gc_chart_index(const cbm_gbuf_t *gb) {
    CBMHashTable *ix = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (!ix || cbm_gbuf_find_by_label(gb, "Chart", &nodes, &count) != 0) {
        return ix;
    }
    for (int i = 0; i < count; i++) {
        const char *fp = nodes[i]->file_path;
        const char *qn = nodes[i]->qualified_name;
        const char *slash = fp ? strrchr(fp, '/') : NULL;
        const char *base = slash ? slash + SKIP_ONE : fp;
        if (!fp || !qn || strstr(qn, ".__helm_dep__.") ||
            (strcmp(base, "Chart.yaml") != 0 && strcmp(base, "Chart.yml") != 0) || !slash) {
            continue;
        }
        const char *db = slash;
        while (db > fp && db[-SKIP_ONE] != '/') {
            db--;
        }
        char key[CBM_SZ_256];
        snprintf(key, sizeof(key), "%.*s", (int)(slash - db), db);
        gc_charts_t *v = cbm_ht_get(ix, key);
        if (!v) {
            v = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*v));
            char *k = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
            if (!v || !k) {
                cbm_free(CBM_MEM_CLASS_OTHER, v);
                cbm_free(CBM_MEM_CLASS_OTHER, k);
                continue;
            }
            cbm_ht_set(ix, k, v);
        }
        if (gc_grow((void **)&v->items, &v->cap, v->count, sizeof(*v->items))) {
            v->items[v->count++] = (gc_chart_t){fp, nodes[i]->id};
        }
    }
    return ix;
}

/* The one chart directory a value names: the tail itself, or the only chart
 * directory ending with it. */
static const gc_chart_t *gc_match(gc_t *g, const gc_val_t *v, char *dir, size_t dir_sz) {
    const char *t = v->s;
    while (t[0] == '.' && t[SKIP_ONE] == '/') {
        t += PAIR_LEN;
    }
    while (*t == '/') {
        t++;
    }
    snprintf(dir, dir_sz, "%s", t);
    size_t dl = strlen(dir);
    while (dl && dir[dl - SKIP_ONE] == '/') {
        dir[--dl] = '\0';
    }
    if (!dl) {
        return NULL;
    }
    const char *base = strrchr(dir, '/');
    base = base ? base + SKIP_ONE : dir;
    const gc_charts_t *list = cbm_ht_get(g->charts, base);
    const gc_chart_t *hit = NULL;
    for (int i = 0; list && i < list->count; i++) {
        atomic_fetch_add_explicit(&g_op_candidates, SKIP_ONE, memory_order_relaxed);
        const char *cd = list->items[i].dir; /* "<dir>/Chart.yaml" */
        size_t cl = (size_t)(strrchr(cd, '/') - cd);
        bool same = cl == dl && strncmp(cd, dir, dl) == 0;
        bool tail = cl > dl && cd[cl - dl - SKIP_ONE] == '/' && strncmp(cd + cl - dl, dir, dl) == 0;
        if (same || tail) {
            if (hit) {
                return NULL;
            }
            hit = &list->items[i];
        }
    }
    if (hit) {
        const char *cd = hit->dir;
        snprintf(dir, dir_sz, "%.*s", (int)(strrchr(cd, '/') - cd), cd);
    }
    return hit;
}

/* The body of the top-level function around `at`, else NULL; *sig: its
 * `func` keyword. */
static const char *gc_body_of(const char *text, const char *at, const char **end,
                              const char **sig) {
    const char *best = NULL;
    for (const char *p = strstr(text, "func "); p && p < at; p = strstr(p + SKIP_ONE, "func ")) {
        if (p == text || p[-SKIP_ONE] == '\n') {
            best = p;
        }
    }
    if (!best) {
        return NULL;
    }
    const char *open = best;
    int pd = 0;
    while (*open && !(*open == '{' && pd == 0)) {
        const char *q = gc_skip(open);
        if (q != open) {
            open = q;
            continue;
        }
        pd += *open == '(';
        pd -= *open == ')';
        open++;
    }
    if (*open != '{') {
        return NULL;
    }
    const char *close = gc_block_end(open);
    if (close < at) {
        return NULL;
    }
    *end = close;
    *sig = best;
    return open;
}

static void gc_link_call(gc_t *g, const char *file, const char *text, const char *name_at,
                         const char *open, int param) {
    const char *starts[GC_ARGS_MAX];
    size_t lens[GC_ARGS_MAX];
    const char *end = NULL;
    int n = gc_args(open, starts, lens, GC_ARGS_MAX, &end);
    if (param >= n) {
        return;
    }
    char dir[CBM_SZ_1K];
    op_dirname(file, dir, sizeof(dir));
    const char *body_end = NULL;
    const char *sig = NULL;
    const char *body = gc_body_of(text, name_at, &body_end, &sig);
    gc_scope_t sc = {g, text, dir, body ? sig : NULL, body, body_end, name_at};
    gc_val_t v;
    gc_eval(&sc, starts[param], lens[param], 0, &v);
    int line = op_line_of(text, name_at);
    const cbm_gbuf_node_t *fn = v.known ? op_enclosing(g->by_file, file, line) : NULL;
    if (!fn || !gc_grow((void **)&g->sites, &g->cap_sites, g->nsites, sizeof(*g->sites))) {
        return;
    }
    g->sites[g->nsites++] =
        (gc_site_t){fn->id, line, cbm_mem_strdup(CBM_MEM_CLASS_OTHER, v.s),
                    v.open_root && v.root[0] ? cbm_mem_strdup(CBM_MEM_CLASS_OTHER, v.root) : NULL};
}

static void gc_edge(gc_t *g, const gc_site_t *s, const gc_chart_t *hit, const char *chart,
                    const char *root) {
    char esc[CBM_SZ_1K];
    char resc[CBM_SZ_512];
    char props[CBM_SZ_2K];
    cbm_json_escape(esc, (int)sizeof(esc), chart);
    cbm_json_escape(resc, (int)sizeof(resc), root ? root : "");
    snprintf(props, sizeof(props), "{\"via\":\"helm-sdk\",\"chart\":\"%s\"%s%s%s,\"line\":%d}", esc,
             root ? ",\"root\":\"" : "", root ? resc : "", root ? "\"" : "", s->line);
    cbm_gbuf_insert_edge(g->ctx->gbuf, s->fn, hit->id, "DEPLOYS", props);
    g->edges++;
}

/* The repository dir a chart load's unknown root stood for: the matched
 * chart directory without the tail. */
static void gc_root_dir(const char *chart, const char *tail, char *out, size_t out_sz) {
    while (tail[0] == '.' && tail[SKIP_ONE] == '/') {
        tail += PAIR_LEN;
    }
    while (*tail == '/') {
        tail++;
    }
    size_t cl = strlen(chart);
    size_t tl = strlen(tail);
    while (tl && tail[tl - SKIP_ONE] == '/') {
        tl--;
    }
    size_t rl = cl > tl ? cl - tl - SKIP_ONE : 0;
    snprintf(out, out_sz, "%.*s", (int)rl, chart);
}

static const char GC_CONFLICT[] = "\x01";

/* Remember that root text `root` stood for directory `rdir`; two different
 * directories for one root text: it stands for none. */
static void gc_note_root(CBMHashTable *roots, const char *root, const char *rdir) {
    const char *known = cbm_ht_get(roots, root);
    if (!known) {
        char *k = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, root);
        char *d = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, rdir);
        if (k && d) {
            cbm_ht_set(roots, k, d);
            return;
        }
        cbm_free(CBM_MEM_CLASS_OTHER, k);
        cbm_free(CBM_MEM_CLASS_OTHER, d);
        return;
    }
    if (strcmp(known, GC_CONFLICT) != 0 && strcmp(known, rdir) != 0) {
        char *c = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, GC_CONFLICT);
        if (c) {
            cbm_free(CBM_MEM_CLASS_OTHER, cbm_ht_set(roots, cbm_ht_get_key(roots, root), c));
        }
    }
}

/* Link the chart loads: a tail exactly one chart directory ends with; else,
 * when the loads under the same unknown root (opt.ChartsDirectory) whose
 * tail matched one directory all put that root at one place, <root>/<tail>. */
static void gc_link_sites(gc_t *g) {
    CBMHashTable *roots = cbm_ht_create(0); /* root text -> its dir (owned) */
    for (int i = 0; roots && i < g->nsites; i++) {
        const gc_site_t *s = &g->sites[i];
        gc_val_t v = {.known = true, .open_root = s->root != NULL};
        snprintf(v.s, sizeof(v.s), "%s", s->tail);
        char chart[CBM_SZ_1K];
        if (s->root && gc_match(g, &v, chart, sizeof(chart))) {
            char rdir[CBM_SZ_1K];
            gc_root_dir(chart, s->tail, rdir, sizeof(rdir));
            gc_note_root(roots, s->root, rdir);
        }
    }
    for (int i = 0; roots && i < g->nsites; i++) {
        const gc_site_t *s = &g->sites[i];
        gc_val_t v = {.known = true, .open_root = s->root != NULL};
        snprintf(v.s, sizeof(v.s), "%s", s->tail);
        char chart[CBM_SZ_1K];
        const gc_chart_t *hit = gc_match(g, &v, chart, sizeof(chart));
        if (hit) {
            gc_edge(g, s, hit, chart, NULL);
            continue;
        }
        const char *rdir = s->root ? cbm_ht_get(roots, s->root) : NULL;
        if (!rdir || strcmp(rdir, GC_CONFLICT) == 0) {
            continue;
        }
        gc_val_t full = {.known = true};
        snprintf(full.s, sizeof(full.s), "%s%s%s", rdir, rdir[0] ? "/" : "", s->tail);
        char want[CBM_SZ_1K];
        snprintf(want, sizeof(want), "%s", full.s);
        hit = gc_match(g, &full, chart, sizeof(chart));
        if (hit && strcmp(chart, want) == 0) {
            gc_edge(g, s, hit, chart, rdir);
        }
    }
    if (roots) {
        cbm_ht_foreach(roots, gc_free_expr, NULL);
        cbm_ht_free(roots);
    }
}

/* The chart loads in one file: calls of a noted loader (qualified by an
 * import of its package, or unqualified in it) and of the SDK loader. */
static void gc_calls(gc_t *g, const char *file, const char *text) {
    char dir[CBM_SZ_1K];
    op_dirname(file, dir, sizeof(dir));
    for (const char *p = text; *p;) {
        const char *q = gc_skip(p);
        if (q != p) {
            p = q;
            continue;
        }
        if (!op_ident_char(*p) || (p > text && op_ident_char(p[-SKIP_ONE]))) {
            p++;
            continue;
        }
        const char *ne = p;
        while (op_ident_char(*ne)) {
            ne++;
        }
        if (*ne != '(') {
            p = ne;
            continue;
        }
        size_t nl = (size_t)(ne - p);
        const char *qual = NULL;
        size_t ql = 0;
        if (p > text && p[-SKIP_ONE] == '.') {
            qual = p - SKIP_ONE;
            while (qual > text && op_ident_char(qual[-SKIP_ONE])) {
                qual--;
            }
            ql = (size_t)(p - SKIP_ONE - qual);
        }
        bool is_def = p >= text + OP_SLEN("func ") &&
                      strncmp(p - OP_SLEN("func "), "func ", OP_SLEN("func ")) == 0;
        int param = -1;
        if (!is_def && qual && (gc_ident_eq(p, nl, "Load") || gc_ident_eq(p, nl, "LoadDir")) &&
            gc_qual_is(text, qual, ql, GC_SDK_LOADER)) {
            param = 0;
        }
        char qdir[CBM_SZ_1K];
        bool resolved = !qual || gc_qual_dir(g, text, qual, ql, qdir, sizeof(qdir));
        for (int i = 0; !is_def && param < 0 && resolved && i < g->nloaders; i++) {
            if (gc_ident_eq(p, nl, g->loaders[i].name) &&
                strcmp(g->loaders[i].dir, qual ? qdir : dir) == 0) {
                param = g->loaders[i].param;
            }
        }
        if (param >= 0) {
            gc_link_call(g, file, text, p, ne, param);
        }
        p = ne;
    }
}

/* A Go file that mentions a chart: its loaders noted now; the file is read
 * again for its loader calls once every loader is known. A file importing
 * the SDK loader is marked: it is read again even when no loader of the
 * repository exists. */
static void gc_note(gc_t *g, const char *file, const char *text) {
    if (!strstr(text, "Chart") && !strstr(text, "chart/loader")) {
        return;
    }
    gc_summarize(g, file, text);
    bool sdk = strstr(text, GC_SDK_LOADER) != NULL;
    size_t fl = strlen(file);
    char *entry = cbm_alloc(CBM_MEM_CLASS_OTHER, fl + PAIR_LEN); /* flag + path */
    if (!entry || !gc_grow((void **)&g->noted, &g->cap_noted, g->nnoted, sizeof(*g->noted))) {
        cbm_free(CBM_MEM_CLASS_OTHER, entry);
        return;
    }
    entry[0] = sdk ? GC_NOTED_SDK : GC_NOTED_PLAIN;
    memcpy(entry + SKIP_ONE, file, fl + SKIP_ONE);
    g->noted[g->nnoted++] = entry;
}

static int gc_run(gc_t *g) {
    if (g->nnoted == 0) {
        return 0;
    }
    g->consts = cbm_ht_create(0);
    g->charts = gc_chart_index(g->ctx->gbuf);
    for (int i = 0; g->consts && g->charts && i < g->nnoted; i++) {
        const char *noted = g->noted[i];
        if (!noted || (g->nloaders == 0 && noted[0] != GC_NOTED_SDK)) {
            continue; /* no loader it could call */
        }
        const char *file = noted + SKIP_ONE;
        char path[CBM_SZ_4K];
        snprintf(path, sizeof(path), "%s/%s", g->ctx->repo_path, file);
        char *text = op_read_file(path);
        if (text) {
            gc_calls(g, file, text);
            cbm_free(CBM_MEM_CLASS_OTHER, text);
        }
    }
    if (g->charts) {
        gc_link_sites(g);
    }
    if (g->consts) {
        cbm_ht_foreach(g->consts, gc_free_consts, NULL);
        cbm_ht_free(g->consts);
    }
    if (g->charts) {
        cbm_ht_foreach(g->charts, gc_free_chart_list, NULL);
        cbm_ht_free(g->charts);
    }
    return g->edges;
}

static void gc_free(gc_t *g) {
    for (int i = 0; i < g->nloaders; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, g->loaders[i].dir);
        cbm_free(CBM_MEM_CLASS_OTHER, g->loaders[i].name);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, g->loaders);
    for (int i = 0; i < g->nnoted; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, g->noted[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)g->noted);
    for (int i = 0; i < g->nsites; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, g->sites[i].tail);
        cbm_free(CBM_MEM_CLASS_OTHER, g->sites[i].root);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, g->sites);
}

int cbm_pipeline_pass_operator(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    const cbm_gbuf_node_t **all = NULL;
    int count = 0;
    if (!gb || !ctx->repo_path || cbm_gbuf_find_by_label(gb, "File", &all, &count) != 0) {
        return 0;
    }
    const cbm_gbuf_node_t **files =
        cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(count ? count : SKIP_ONE) * sizeof(*files));
    if (!files) {
        return CBM_NOT_FOUND;
    }
    int nfiles = 0;
    for (int i = 0; i < count; i++) {
        if (all[i]->file_path) {
            files[nfiles++] = all[i];
        }
    }
    qsort((void *)files, (size_t)nfiles, sizeof(*files), op_cmp_path);
    CBMHashTable *by_file = op_functions_by_file(gb);
    CBMHashTable *groups = cbm_ht_create(0);
    CBMHashTable *seen = cbm_ht_create(0);
    op_modules_t mods = {0};
    for (int i = 0; i < nfiles; i++) {
        const char *slash = strrchr(files[i]->file_path, '/');
        if (strcmp(slash ? slash + SKIP_ONE : files[i]->file_path, "go.mod") == 0) {
            char path[CBM_SZ_4K];
            snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, files[i]->file_path);
            char *text = op_read_file(path);
            if (text) {
                op_note_module(&mods, files[i]->file_path, text);
                cbm_free(CBM_MEM_CLASS_OTHER, text);
            }
        }
    }
    op_index_t ix = {.mods = &mods, .dir_types = op_types_by_dir(gb)};
    gc_t gc = {0};
    int handles = 0;
    int embeds = 0;
    bool ready = by_file && groups && seen && ix.dir_types;
    for (int i = 0; ready && i < nfiles; i++) {
        if (!op_is_go(files[i]->file_path) || cbm_pipeline_check_cancel(ctx)) {
            continue;
        }
        char path[CBM_SZ_4K];
        snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, files[i]->file_path);
        char *text = op_read_file(path);
        if (!text) {
            continue;
        }
        if (strstr(text, "controller-runtime") || strstr(text, "source.Kind(")) {
            handles += op_controller_calls(gb, by_file, &ix, files[i]->file_path, text);
        }
        op_scan_groups(groups, seen, files[i]->file_path, text);
        if (strstr(text, "//go:embed ")) {
            embeds += op_embed(gb, files[i], text, files, nfiles);
        }
        gc_note(&gc, files[i]->file_path, text);
        cbm_free(CBM_MEM_CLASS_OTHER, text);
    }
    int crd_types = ready ? op_link_crd_types(gb, &ix, groups) : 0;
    gc.ctx = ctx;
    gc.mods = &mods;
    gc.by_file = by_file;
    gc.files = files;
    gc.nfiles = nfiles;
    int deploys = ready ? gc_run(&gc) : 0;
    gc_free(&gc);
    if (ix.dir_types) {
        cbm_ht_foreach(ix.dir_types, op_free_nodes_entry, NULL);
        cbm_ht_free(ix.dir_types);
    }
    op_modules_free(&mods);
    if (groups) {
        cbm_ht_foreach(groups, op_free_groups, NULL);
        cbm_ht_free(groups);
    }
    if (seen) {
        cbm_ht_foreach(seen, op_free_seen, NULL);
        cbm_ht_free(seen);
    }
    if (by_file) {
        cbm_ht_foreach(by_file, op_free_spans, NULL);
        cbm_ht_free(by_file);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)files);
    if (handles + embeds + crd_types + deploys > 0) {
        char h[CBM_SZ_32];
        char e[CBM_SZ_32];
        char c[CBM_SZ_32];
        char d[CBM_SZ_32];
        snprintf(h, sizeof(h), "%d", handles);
        snprintf(e, sizeof(e), "%d", embeds);
        snprintf(c, sizeof(c), "%d", crd_types);
        snprintf(d, sizeof(d), "%d", deploys);
        cbm_log_info("pass.operator", "handles", h, "embeds", e, "crd_types", c, "chart_deploys",
                     d);
    }
    return 0;
}
