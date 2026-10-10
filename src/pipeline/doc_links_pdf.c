/*
 * doc_links_pdf.c — PDF mentions -> MENTIONS edges: the resolving half.
 *
 * A PDF page (doc_pdf.c) names code structurally: a path or a qualified name.
 * The tiers are the field-tested ones (E6, frozen resolver), EXACT or UNIQUE
 * only:
 *   path-exact   the mention is a File/Folder node's repository path
 *   path-suffix  exactly one File/Folder whose path ends with the mention at a
 *                '/' boundary
 *   qn-exact     exactly one node whose qualified name (project prefix off)
 *                is the mention
 *   qn-suffix    exactly one node whose qualified name ends with the mention
 *                at a component boundary -- or whose owner-qualified name
 *                does (parent_class + name: a Go method's QN has no receiver)
 * Two candidates that are one entity collapse first (a class and its
 * constructors; duplicate nodes of one definition). A cross-line join is
 * resolved before anything else in its file: when it resolves EXACT or
 * UNIQUE it replaces the line fragments it was built from.
 *
 * Hygiene (the field test's R2 and R4): a reference that resolves to a data
 * file (.json, .yaml, ...) is no code reference; one that resolves into a
 * test, mock or fixture file is a test_only_target row.
 *
 * Unresolved: ambiguous (several entities); missing (nothing, but the
 * mention's first component is a name or directory of this repository).
 * Anything else names code outside the repository: LOCAL, no row.
 *
 * Targets exclude the labels the incremental route does not proxy (Macro,
 * Comment, Section, Branch, Commit, Tag) and the field test's non-code labels
 * (Project, Route, Channel, Decorator), so a re-resolved document binds what a
 * full build binds.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PDFR_TABLE_INIT = 1024,
    PDFR_KEY_CAP = 1024, /* the longest mention form looked up */
    PDFR_MAX_COMPS = 64,
    PDFR_MAX_FORMS = 2,
    PDFR_MEMO_INIT = 64,
};

static const char *const PDFR_EXCLUDED_LABELS[] = {
    "Section", "Project", "Branch", "Route", "Channel", "Decorator", "Macro",
    "Comment", "Commit",  "Tag",    "File",  "Folder",  NULL};

static const char *const PDFR_CONTAINERS[] = {"Class",    "Struct", "Interface", "Enum",
                                              "Trait",    "Type",   "Record",    "Object",
                                              "Protocol", "Union",  NULL};

static const char *const PDFR_DATA_EXT[] = {".json", ".yaml", ".yml",  ".toml",
                                            ".csv",  ".tsv",  ".lock", NULL};

/* R4: the frozen test-file rule, with the directories the field test found it
 * missing (Foundry *.t.sol, test/, mocks/, testutil/, testing/, testdata/). */
static const char *const PDFR_TEST_DIRS[] = {"tests",    "__tests__", "test",     "mocks",
                                             "testutil", "testing",   "testdata", NULL};

static bool in_list(const char *s, const char *const *list) {
    for (int i = 0; list[i]; i++) {
        if (strcmp(s, list[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool ends_with(const char *s, const char *suf) {
    size_t n = strlen(s);
    size_t m = strlen(suf);
    return n >= m && memcmp(s + n - m, suf, m) == 0;
}

static bool starts_with(const char *s, const char *pre) {
    return strncmp(s, pre, strlen(pre)) == 0;
}

/* ── Index ───────────────────────────────────────────────────────── */

typedef struct pdfr_list {
    const cbm_gbuf_node_t *node;
    struct pdfr_list *next;
} pdfr_list_t;

typedef struct {
    const char *project;
    size_t project_len;
    CBMArena arena;
    CBMHashTable *paths;     /* File/Folder repository path -> pdfr_list_t* */
    CBMHashTable *basenames; /* last path component -> pdfr_list_t* */
    CBMHashTable *dirs;      /* every directory component of an indexed path (value 1) */
    /* Code nodes whose qualified name does not end with their name (a Module
     * is named by its file path), keyed by the QN's last component: the
     * field test matched qualified names by their components, not by name. */
    CBMHashTable *irregular;
    bool failed;
} pdfr_index_t;

static void pdfr_destroy(void *index);

static bool list_add(pdfr_index_t *x, CBMHashTable *t, const char *key, const cbm_gbuf_node_t *n) {
    pdfr_list_t *e = (pdfr_list_t *)cbm_arena_alloc(&x->arena, sizeof(*e));
    if (!e) {
        return false;
    }
    e->node = n;
    e->next = (pdfr_list_t *)cbm_ht_get(t, key);
    cbm_ht_set(t, key, e);
    return true;
}

static bool index_paths(pdfr_index_t *x, const cbm_gbuf_t *graph, const char *label) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(graph, label, &nodes, &count) != 0) {
        return true;
    }
    for (int i = 0; i < count; i++) {
        const char *fp = nodes[i]->file_path;
        if (!fp || !fp[0] || fp[0] == '<') {
            continue;
        }
        while (*fp == '/') {
            fp++;
        }
        size_t n = strlen(fp);
        while (n > 0 && fp[n - 1] == '/') {
            n--;
        }
        char *p = cbm_arena_strndup(&x->arena, fp, n);
        if (!p || !list_add(x, x->paths, p, nodes[i])) {
            return false;
        }
        const char *slash = strrchr(p, '/');
        if (!list_add(x, x->basenames, slash ? slash + 1 : p, nodes[i])) {
            return false;
        }
        /* directory components, for "is this a name of the repository" */
        const char *s = p;
        for (const char *q = p;; q++) {
            if (*q == '/' || *q == '\0') {
                if (q > s) {
                    char *c = cbm_arena_strndup(&x->arena, s, (size_t)(q - s));
                    if (!c) {
                        return false;
                    }
                    cbm_ht_set(x->dirs, c, (void *)(uintptr_t)1);
                }
                if (!*q) {
                    break;
                }
                s = q + 1;
            }
        }
    }
    return true;
}

static bool code_node(const pdfr_index_t *x, const cbm_gbuf_node_t *n);
static const char *rel_qn(const pdfr_index_t *x, const char *qn);

static void index_irregular(const cbm_gbuf_node_t *n, void *ud) {
    pdfr_index_t *x = (pdfr_index_t *)ud;
    if (x->failed || !code_node(x, n)) {
        return;
    }
    const char *rel = rel_qn(x, n->qualified_name);
    const char *dot = strrchr(rel, '.');
    const char *last = dot ? dot + 1 : rel;
    if (n->name && strcmp(last, n->name) == 0) {
        return;
    }
    if (!list_add(x, x->irregular, last, n)) {
        x->failed = true;
    }
}

static void *pdfr_build(const cbm_doclink_build_in_t *in) {
    if (in->file_count == 0) {
        return NULL; /* no PDF in the repository: nothing to resolve */
    }
    pdfr_index_t *x = (pdfr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*x));
    if (!x) {
        return NULL;
    }
    cbm_arena_init(&x->arena);
    x->project = in->ctx ? in->ctx->project_name : NULL;
    x->project_len = x->project ? strlen(x->project) : 0;
    x->paths = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, PDFR_TABLE_INIT);
    x->basenames = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, PDFR_TABLE_INIT);
    x->dirs = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, PDFR_TABLE_INIT);
    x->irregular = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, PDFR_TABLE_INIT);
    if (!x->project || !x->paths || !x->basenames || !x->dirs || !x->irregular ||
        !index_paths(x, in->graph, "File") || !index_paths(x, in->graph, "Folder")) {
        pdfr_destroy(x);
        return NULL;
    }
    cbm_gbuf_foreach_node(in->graph, index_irregular, x);
    if (x->failed) {
        pdfr_destroy(x);
        return NULL;
    }
    return x;
}

static void pdfr_destroy(void *index) {
    pdfr_index_t *x = (pdfr_index_t *)index;
    if (!x) {
        return;
    }
    cbm_ht_free(x->paths);
    cbm_ht_free(x->basenames);
    cbm_ht_free(x->dirs);
    cbm_ht_free(x->irregular);
    cbm_arena_destroy(&x->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, x);
}

/* ── Candidate sets ──────────────────────────────────────────────── */

typedef struct {
    const cbm_gbuf_node_t **v;
    int n;
    int cap;
    bool fail;
} nodeset_t;

static void ns_add(nodeset_t *s, const cbm_gbuf_node_t *n) {
    for (int i = 0; i < s->n; i++) {
        if (s->v[i] == n) {
            return;
        }
    }
    if (s->fail) {
        return;
    }
    if (s->n == s->cap) {
        int ncap = s->cap ? s->cap * 2 : 8;
        const cbm_gbuf_node_t **g = (const cbm_gbuf_node_t **)cbm_realloc(
            CBM_MEM_CLASS_OTHER, (void *)s->v, (size_t)ncap * sizeof(*g));
        if (!g) {
            s->fail = true;
            return;
        }
        s->v = g;
        s->cap = ncap;
    }
    s->v[s->n++] = n;
}

static void ns_free(nodeset_t *s) {
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)s->v);
    memset(s, 0, sizeof(*s));
}

/* The qualified name without the project prefix. */
static const char *rel_qn(const pdfr_index_t *x, const char *qn) {
    if (qn && x->project_len && strncmp(qn, x->project, x->project_len) == 0 &&
        qn[x->project_len] == '.') {
        return qn + x->project_len + 1;
    }
    return qn ? qn : "";
}

static bool code_node(const pdfr_index_t *x, const cbm_gbuf_node_t *n) {
    if (!n->label || in_list(n->label, PDFR_EXCLUDED_LABELS) || !n->qualified_name ||
        !n->qualified_name[0]) {
        return false;
    }
    if (n->file_path && n->file_path[0] == '<') {
        return false; /* builtins */
    }
    return strncmp(rel_qn(x, n->qualified_name), "__", 2) != 0; /* dependency stand-ins */
}

/* s is a component-aligned suffix of q ('.'-separated) */
static bool comp_suffix(const char *q, const char *s) {
    size_t ql = strlen(q);
    size_t sl = strlen(s);
    if (sl > ql || memcmp(q + ql - sl, s, sl) != 0) {
        return false;
    }
    return sl == ql || q[ql - sl - 1] == '.';
}

/* The owner-qualified name of a node: rel(parent_class) + "." + name, when the
 * node has a parent_class (properties JSON). */
static bool owner_name(const pdfr_index_t *x, const cbm_gbuf_node_t *n, char *buf, size_t cap) {
    const char *p = n->properties_json ? strstr(n->properties_json, "\"parent_class\":\"") : NULL;
    if (!p) {
        return false;
    }
    p += strlen("\"parent_class\":\"");
    char pc[PDFR_KEY_CAP];
    size_t k = 0;
    while (*p && *p != '"' && k + 1 < sizeof(pc)) {
        if (*p == '\\' && p[1]) {
            p++;
        }
        pc[k++] = *p++;
    }
    pc[k] = '\0';
    if (*p != '"') {
        return false;
    }
    const char *prel = rel_qn(x, pc);
    int w = snprintf(buf, cap, "%s.%s", prel, n->name ? n->name : "");
    return w > 0 && (size_t)w < cap;
}

static void qn_candidate(const pdfr_index_t *x, const cbm_gbuf_node_t *n, const char *s,
                         nodeset_t *out, nodeset_t *exact) {
    if (!code_node(x, n)) {
        return;
    }
    const char *rel = rel_qn(x, n->qualified_name);
    char alt[PDFR_KEY_CAP];
    bool owned = owner_name(x, n, alt, sizeof(alt));
    /* A member whose QN leaves its owner out (a Go method's has no receiver)
     * is named through the owner: `time.Now` is package time's function,
     * never the method Now of a type that package time declares. */
    bool ownerless_qn = owned && !comp_suffix(rel, alt);
    bool via_rel = !ownerless_qn && comp_suffix(rel, s);
    bool via_alt = !via_rel && owned && strcmp(alt, rel) != 0 && comp_suffix(alt, s);
    if (via_rel || via_alt) {
        ns_add(out, n);
        if (via_rel && strcmp(rel, s) == 0) {
            ns_add(exact, n);
        }
    }
}

/* Nodes whose QN or owner-qualified name ends with the mention s (>= 2
 * components). A node's name may itself hold dots, so every component suffix
 * of s is tried as a name. */
static void qn_suffix_set(const pdfr_index_t *x, const cbm_gbuf_t *graph, const char *s,
                          nodeset_t *out, nodeset_t *exact) {
    const char *starts[PDFR_MAX_COMPS];
    int nc = 0;
    starts[nc++] = s;
    for (const char *q = s; *q && nc < PDFR_MAX_COMPS; q++) {
        if (*q == '.') {
            starts[nc++] = q + 1;
        }
    }
    if (nc < 2) {
        return;
    }
    for (int j = nc - 1; j >= 0; j--) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_name(graph, starts[j], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            qn_candidate(x, nodes[i], s, out, exact);
        }
    }
    for (const pdfr_list_t *l = (const pdfr_list_t *)cbm_ht_get(x->irregular, starts[nc - 1]); l;
         l = l->next) {
        qn_candidate(x, l->node, s, out, exact);
    }
}

static int node_id_cmp(const void *a, const void *b) {
    const cbm_gbuf_node_t *x = *(const cbm_gbuf_node_t *const *)a;
    const cbm_gbuf_node_t *y = *(const cbm_gbuf_node_t *const *)b;
    return x->id < y->id ? -1 : x->id > y->id ? 1 : 0;
}

/* The field test's _collapse: one entity's several nodes become one. */
static const cbm_gbuf_node_t *collapse(const pdfr_index_t *x, nodeset_t *s) {
    if (s->n == 1) {
        return s->v[0];
    }
    if (s->n == 0) {
        return NULL;
    }
    qsort((void *)s->v, (size_t)s->n, sizeof(*s->v), node_id_cmp);
    const cbm_gbuf_node_t *c = NULL;
    int containers = 0;
    for (int i = 0; i < s->n; i++) {
        if (s->v[i]->label && in_list(s->v[i]->label, PDFR_CONTAINERS)) {
            c = s->v[i];
            containers++;
        }
    }
    if (containers == 1) {
        const char *crel = rel_qn(x, c->qualified_name);
        size_t cl = strlen(crel);
        bool all = true;
        for (int i = 0; i < s->n && all; i++) {
            const cbm_gbuf_node_t *n = s->v[i];
            if (n == c) {
                continue;
            }
            const char *rel = rel_qn(x, n->qualified_name);
            all = n->name && c->name && strcmp(n->name, c->name) == 0 &&
                  strncmp(rel, crel, cl) == 0 && rel[cl] == '.';
        }
        if (all) {
            return c; /* a class and its constructors */
        }
    }
    const cbm_gbuf_node_t *f = s->v[0];
    for (int i = 1; i < s->n; i++) {
        const cbm_gbuf_node_t *n = s->v[i];
        if (strcmp(n->file_path ? n->file_path : "", f->file_path ? f->file_path : "") != 0 ||
            n->start_line != f->start_line ||
            strcmp(n->name ? n->name : "", f->name ? f->name : "") != 0) {
            return NULL;
        }
    }
    return f; /* duplicate nodes of one definition: the lowest id */
}

/* ── One mention form ────────────────────────────────────────────── */

typedef enum { RES_NONE = 0, RES_AMBIGUOUS, RES_HIT } res_kind_t;

typedef struct {
    res_kind_t kind;
    bool exact;
    const cbm_gbuf_node_t *node;
} res_t;

static res_t outcome(const pdfr_index_t *x, nodeset_t *s, bool exact) {
    res_t r = {RES_NONE, false, NULL};
    if (s->n == 0) {
        return r;
    }
    const cbm_gbuf_node_t *n = collapse(x, s);
    if (n) {
        r.kind = RES_HIT;
        r.exact = exact;
        r.node = n;
    } else {
        r.kind = RES_AMBIGUOUS;
    }
    return r;
}

static void list_to_set(const pdfr_list_t *l, nodeset_t *s) {
    for (; l; l = l->next) {
        ns_add(s, l->node);
    }
}

static res_t resolve_form(const pdfr_index_t *x, const cbm_gbuf_t *graph, int syntax,
                          const char *form) {
    res_t none = {RES_NONE, false, NULL};
    if (syntax == CBM_DOCLINK_PDF_PATH || syntax == CBM_DOCLINK_PDF_FILE) {
        const char *p = form;
        while (*p == '/') {
            p++;
        }
        char key[PDFR_KEY_CAP];
        size_t n = strlen(p);
        while (n > 0 && p[n - 1] == '/') {
            n--;
        }
        if (n == 0 || n >= sizeof(key)) {
            return none;
        }
        memcpy(key, p, n);
        key[n] = '\0';
        nodeset_t s = {0};
        list_to_set((const pdfr_list_t *)cbm_ht_get(x->paths, key), &s);
        if (s.n) {
            res_t r = outcome(x, &s, true);
            ns_free(&s);
            return r;
        }
        const char *slash = strrchr(key, '/');
        for (const pdfr_list_t *l =
                 (const pdfr_list_t *)cbm_ht_get(x->basenames, slash ? slash + 1 : key);
             l; l = l->next) {
            const char *fp = l->node->file_path;
            while (*fp == '/') {
                fp++;
            }
            size_t fl = strlen(fp);
            while (fl > 0 && fp[fl - 1] == '/') {
                fl--;
            }
            if (fl >= n && memcmp(fp + fl - n, key, n) == 0 && (fl == n || fp[fl - n - 1] == '/')) {
                ns_add(&s, l->node);
            }
        }
        if (s.n) {
            res_t r = outcome(x, &s, false);
            ns_free(&s);
            return r;
        }
        ns_free(&s);
        if (syntax == CBM_DOCLINK_PDF_PATH) {
            return none;
        }
    }
    if (syntax == CBM_DOCLINK_PDF_QN || syntax == CBM_DOCLINK_PDF_FILE) {
        nodeset_t all = {0};
        nodeset_t ex = {0};
        qn_suffix_set(x, graph, form, &all, &ex);
        res_t r = none;
        if (ex.n == 1) {
            r.kind = RES_HIT;
            r.exact = true;
            r.node = ex.v[0];
        } else if (all.n) {
            r = outcome(x, &all, false);
        }
        ns_free(&all);
        ns_free(&ex);
        return r;
    }
    if (syntax == CBM_DOCLINK_PDF_NAME) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        nodeset_t s = {0};
        if (cbm_gbuf_find_by_name(graph, form, &nodes, &count) == 0) {
            for (int i = 0; i < count; i++) {
                if (code_node(x, nodes[i])) {
                    ns_add(&s, nodes[i]);
                }
            }
        }
        res_t r = outcome(x, &s, false);
        ns_free(&s);
        return r;
    }
    return none;
}

/* A join's forms: the first, plus the hyphen-kept alternative. One node hit
 * by the forms is the outcome; two different nodes are ambiguous. */
static res_t resolve_join(const pdfr_index_t *x, const cbm_gbuf_t *graph, int syntax,
                          const char *raw) {
    char buf[PDFR_KEY_CAP];
    const char *end = strchr(raw, '\x1e');
    size_t len = end ? (size_t)(end - raw) : strlen(raw);
    if (len >= sizeof(buf)) {
        res_t none = {RES_NONE, false, NULL};
        return none;
    }
    memcpy(buf, raw, len);
    buf[len] = '\0';
    char *forms[PDFR_MAX_FORMS];
    int nf = 0;
    forms[nf++] = buf;
    char *sep = strchr(buf, '\x1f');
    if (sep) {
        *sep = '\0';
        forms[nf++] = sep + 1;
    }
    res_t first = resolve_form(x, graph, syntax, forms[0]);
    if (nf == 1) {
        return first;
    }
    res_t second = resolve_form(x, graph, syntax, forms[1]);
    if (first.kind == RES_HIT && second.kind == RES_HIT) {
        if (first.node == second.node) {
            return first;
        }
        res_t amb = {RES_AMBIGUOUS, false, NULL};
        return amb;
    }
    if (first.kind == RES_HIT) {
        return first;
    }
    if (second.kind == RES_HIT) {
        return second;
    }
    return first;
}

/* ── Hygiene ─────────────────────────────────────────────────────── */

static bool data_target(const char *fp) {
    char low[PDFR_KEY_CAP];
    size_t n = strlen(fp);
    if (n >= sizeof(low)) {
        return false;
    }
    for (size_t i = 0; i <= n; i++) {
        low[i] = (char)(fp[i] >= 'A' && fp[i] <= 'Z' ? fp[i] + ('a' - 'A') : fp[i]);
    }
    for (int i = 0; PDFR_DATA_EXT[i]; i++) {
        if (ends_with(low, PDFR_DATA_EXT[i])) {
            return true;
        }
    }
    return false;
}

static bool test_target(const char *fp) {
    const char *base = strrchr(fp, '/');
    base = base ? base + 1 : fp;
    if ((starts_with(base, "test_") && ends_with(base, ".py")) || ends_with(base, "_test.py") ||
        ends_with(base, "_test.go") || ends_with(base, "Test.cs") || ends_with(base, "Tests.cs") ||
        ends_with(base, "Test.java") || ends_with(base, "Tests.java") ||
        strcmp(base, "conftest.py") == 0 || ends_with(base, ".t.sol")) {
        return true;
    }
    static const char *const js[] = {".test.js",  ".test.jsx", ".test.ts",  ".test.tsx", ".spec.js",
                                     ".spec.jsx", ".spec.ts",  ".spec.tsx", NULL};
    for (int i = 0; js[i]; i++) {
        if (ends_with(base, js[i]) && strlen(base) > strlen(js[i])) {
            return true;
        }
    }
    const char *s = fp;
    for (const char *q = fp; q < base; q++) {
        if (*q == '/') {
            char comp[PDFR_KEY_CAP];
            size_t n = (size_t)(q - s);
            if (n < sizeof(comp)) {
                memcpy(comp, s, n);
                comp[n] = '\0';
                if (in_list(comp, PDFR_TEST_DIRS)) {
                    return true;
                }
            }
            s = q + 1;
        }
    }
    return false;
}

/* ── Per file ────────────────────────────────────────────────────── */

typedef struct {
    const CBMDocLink *base;
    int n;
    uint8_t *suppressed; /* line-local tokens a resolved join replaces */
    res_t *join;         /* each join token's own outcome */
    /* A page set repeats its paths and names (an audit names one file on
     * every page): mention text -> 1 + the index of the token whose outcome
     * res[] holds, reused for a token of the same family. */
    CBMHashTable *memo;
    res_t *res;
} pdfr_state_t;

static void pdfr_end(void *state);

static void *pdfr_prepare(const void *index, int run_file, const CBMDocLink *links, int n,
                          const cbm_gbuf_t *graph) {
    (void)run_file;
    const pdfr_index_t *x = (const pdfr_index_t *)index;
    pdfr_state_t *st = (pdfr_state_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*st));
    if (!st) {
        return NULL;
    }
    st->base = links;
    st->n = n;
    st->suppressed = (uint8_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(n ? n : 1));
    st->join = (res_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(n ? n : 1) * sizeof(res_t));
    st->res = (res_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)(n ? n : 1) * sizeof(res_t));
    st->memo = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, PDFR_MEMO_INIT);
    if (!st->suppressed || !st->join || !st->res || !st->memo) {
        pdfr_end(st);
        return NULL;
    }
    for (int i = 0; i < n; i++) {
        const CBMDocLink *l = &links[i];
        if (!(l->flags & CBM_DOCLINK_FLAG_JOIN) || !l->raw) {
            continue;
        }
        st->join[i] = resolve_join(x, graph, l->syntax, l->raw);
        if (st->join[i].kind != RES_HIT) {
            continue;
        }
        const char *f = strchr(l->raw, '\x1e');
        while (f && *f) {
            f++;
            char *endp = NULL;
            long k = strtol(f, &endp, 10);
            if (endp == f) {
                break;
            }
            if (k >= 0 && k < n) {
                st->suppressed[k] = 1;
            }
            f = (*endp == ',') ? endp : NULL;
        }
    }
    return st;
}

static void pdfr_end(void *state) {
    pdfr_state_t *st = (pdfr_state_t *)state;
    if (!st) {
        return;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, st->suppressed);
    cbm_free(CBM_MEM_CLASS_OTHER, st->join);
    cbm_free(CBM_MEM_CLASS_OTHER, st->res);
    cbm_ht_free(st->memo);
    cbm_free(CBM_MEM_CLASS_OTHER, st);
}

/* The mention's first component names something of this repository. */
static bool first_known(const pdfr_index_t *x, const cbm_gbuf_t *graph, int syntax,
                        const char *raw) {
    char first[PDFR_KEY_CAP];
    size_t n = 0;
    const char *p = raw;
    while (*p == '/') {
        p++;
    }
    char sep = syntax == CBM_DOCLINK_PDF_PATH ? '/' : '.';
    while (p[n] && p[n] != sep && n + 1 < sizeof(first)) {
        first[n] = p[n];
        n++;
    }
    first[n] = '\0';
    if (!n) {
        return false;
    }
    if (cbm_ht_get(x->dirs, first) || cbm_ht_get(x->basenames, first)) {
        return true;
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    return cbm_gbuf_find_by_name(graph, first, &nodes, &count) == 0 && count > 0;
}

static void finish_hit(cbm_doclink_outcome_t *out, res_t r) {
    const char *fp = r.node->file_path ? r.node->file_path : "";
    if (data_target(fp)) {
        out->kind = CBM_DOCLINK_LOCAL; /* R2: a data file, not code */
        return;
    }
    if (test_target(fp)) {
        out->kind = CBM_DOCLINK_UNRESOLVED; /* R4 */
        out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
        return;
    }
    out->kind = CBM_DOCLINK_EDGE;
    out->target = r.node;
    out->exact = r.exact;
}

static void pdfr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                         const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)run_file;
    const pdfr_index_t *x = (const pdfr_index_t *)index;
    pdfr_state_t *st = (pdfr_state_t *)state;
    out->kind = CBM_DOCLINK_LOCAL;
    if (!x || !link->raw) {
        return;
    }
    int i = st ? (int)(link - st->base) : -1;
    bool in_file = st && i >= 0 && i < st->n;
    if (link->flags & CBM_DOCLINK_FLAG_JOIN) {
        /* a join stands only when it resolved; a bare-name join never links */
        if (in_file && st->join[i].kind == RES_HIT && link->syntax != CBM_DOCLINK_PDF_NAME) {
            finish_hit(out, st->join[i]);
        }
        return;
    }
    if (in_file && st->suppressed[i]) {
        return; /* a resolved join replaced this fragment */
    }
    if (link->syntax == CBM_DOCLINK_PDF_NAME) {
        return;
    }
    res_t r;
    uintptr_t seen = in_file ? (uintptr_t)cbm_ht_get(st->memo, link->raw) : 0;
    if (seen && st->base[seen - 1].syntax == link->syntax) {
        r = st->res[seen - 1];
    } else {
        r = resolve_form(x, graph, link->syntax, link->raw);
        if (in_file && !seen) {
            st->res[i] = r;
            cbm_ht_set(st->memo, link->raw, (void *)(uintptr_t)(i + 1));
        }
    }
    if (r.kind == RES_HIT) {
        finish_hit(out, r);
    } else if (r.kind == RES_AMBIGUOUS) {
        out->kind = CBM_DOCLINK_UNRESOLVED;
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
    } else if (first_known(x, graph, link->syntax, link->raw)) {
        out->kind = CBM_DOCLINK_UNRESOLVED;
        out->reason = CBM_DOCLINK_REASON_MISSING;
    }
}

const cbm_doclink_resolver_t cbm_doclink_pdf_resolver = {
    .langs = {CBM_LANG_PDF},
    .lang_count = 1,
    .scope_tag = NULL,
    .build = pdfr_build,
    .destroy = pdfr_destroy,
    .file_prepare = pdfr_prepare,
    .file_end = pdfr_end,
    .resolve = pdfr_resolve,
    .via = "pdf",
};
