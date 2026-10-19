/*
 * doc_links_php.c — resolving PHP doc references (doclink_php.c). A reference
 * is a FQSEN as phpDocumentor reads it, resolved with PHP's own name rules.
 *
 * Forms: `\Ns\Type`, `Type`, `Type::method()`, `Type::$property`,
 * `Type::CONSTANT` (an enum case too), `function()` and `\Ns\function()`;
 * `self::`, `static::` and `parent::` with a member. A member written without
 * `()` and without `$` is a constant or enum case, else a method.
 *
 * Names. A leading backslash is taken as written. Otherwise the first segment
 * of a type name is replaced when it is the alias of a class `use` of the
 * namespace block the reference is written in (aliases compare without case),
 * else the block's namespace is put in front: phpDocumentor's rule. Two rules
 * go beyond that tool, as PHP itself has them: an unqualified function name
 * is taken from a `use function` import, else from the current namespace,
 * else from the global namespace; and `self` and `static` name the type the
 * doc sits in, `parent` that type's superclass. Beyond both, as authors
 * write it: a bare `name()` in the doc of a type (or of its members) that
 * declares or inherits a method of that name is that method; a `use
 * function` import of the name still wins. Type, function
 * and method names compare without case, properties and constants with it.
 * The global fallback cannot see a function a dependency declares in the
 * current namespace: a reference to one binds the global function of that
 * name (a residual).
 *
 * Members are looked up on the type, then on the traits it uses, its
 * superclass and its interfaces, each of them the same way (depth first; the
 * first declaration found wins). Nothing found where a supertype is outside
 * the repository is external; nothing found with every supertype in the
 * repository is missing.
 *
 * Outcomes. A type, function or method binds to its graph node. A property,
 * constant or enum case has none (a graph gap), nor has a type or function
 * the graph folds into another one of the same name in its file (the scope
 * flags those). A type or qualified function the repository does not declare
 * is external when the first segment of its name is no namespace the
 * repository declares (PHP's built-ins, Composer dependencies), else missing;
 * an unqualified function nobody declares is external (PHP's built-in
 * functions). Two declarations of one name are ambiguous unless one is in the
 * reference's own file. Test code binds only from test code. Edges are exact
 * for a name written fully qualified or bound by a use import, unique
 * otherwise.
 */
#include "pipeline/doc_links.h"
#include "cbm.h"
#include "doclink.h"
#include "helpers.h" /* cbm_fqn_module_source_lang, cbm_is_test_file */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PR_QN_MAX = 2048,
    PR_NAME_MAX = 1024,
    PR_FIELDS = 6,
    PR_MAX_DEPTH = 16,
    PR_VISIT_MAX = 64,
    PR_USE_FIELDS = 5,  /* U blk kind alias name */
    PR_LINE_FIELDS = 4, /* N blk name first last / T kind fqn flags first last: lines from here */
};

/* ── The index ───────────────────────────────────────────────────── */

typedef struct {
    char kind; /* m p c e */
    const char *name;
} pr_member_t;

typedef struct {
    char rel; /* e i t */
    const char *fqn;
} pr_base_t;

typedef struct {
    int file;
    char kind; /* c i t e */
    bool dup;  /* folded into another declaration of its file */
    const char *fqn;
    const char *name; /* the tail of fqn */
    uint32_t first;   /* lines: only in a fresh blob */
    uint32_t last;
    pr_base_t *bases;
    int nbases;
    int cap_bases;
    pr_member_t *members;
    int nmembers;
    int cap_members;
    int next; /* the next type of the same name (without case), or -1 */
} pr_type_t;

typedef struct {
    int file;
    bool dup;
    const char *fqn;
    const char *name;
    int next;
} pr_func_t;

typedef struct {
    int blk;
    const char *name;
    uint32_t first;
    uint32_t last;
} pr_ns_t;

typedef struct {
    const char *path;
    const char *module_qn;
    bool test;
    pr_ns_t *nss;
    int nnss;
    int cap_nss;
    cbm_doclink_php_use_t *uses; /* only in a fresh blob; a block's are adjacent */
    int *use_blk;
    int nuses;
    int cap_uses;
    int cap_use_blk;
    int type_first; /* its types: types[type_first, type_first + type_count) */
    int type_count;
} pr_file_t;

typedef struct {
    CBMArena arena;
    const char *project;
    pr_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    pr_type_t *types;
    int ntypes;
    int cap_types;
    pr_func_t *funcs;
    int nfuncs;
    int cap_funcs;
    CBMHashTable *type_at; /* lowercase fqn -> the first type's index + 1 */
    CBMHashTable *func_at; /* lowercase fqn -> the first function's index + 1 */
    CBMHashTable *roots;   /* lowercase first segment of a declared namespace */
    CBMHashTable *by_path; /* rel_path -> file index + 1 */
    bool oom;
} pr_index_t;

static const char *pr_copy(pr_index_t *ix, const char *s, size_t n) {
    char *c = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !c;
    return c;
}

static const char *pr_lower(pr_index_t *ix, const char *s, size_t n) {
    char *c = cbm_arena_strndup(&ix->arena, s, n);
    if (!c) {
        ix->oom = true;
        return NULL;
    }
    for (size_t i = 0; i < n; i++) {
        c[i] = (char)tolower((unsigned char)c[i]);
    }
    return c;
}

/* Room for one more item in a growing arena array. */
static bool pr_grow(pr_index_t *ix, void **items, int count, int *cap, size_t size) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_8;
    void *g = cbm_arena_alloc(&ix->arena, (size_t)ncap * size);
    if (!g) {
        ix->oom = true;
        return false;
    }
    if (count) {
        memcpy(g, *items, (size_t)count * size);
    }
    *items = g;
    *cap = ncap;
    return true;
}

static bool pr_dash(const char *s, size_t n) {
    return n == SKIP_ONE && s[0] == '-';
}

static uint32_t pr_num(const char *s) {
    return (uint32_t)strtoul(s, NULL, CBM_DECIMAL_BASE);
}

static pr_type_t *pr_last_type(pr_index_t *ix, const pr_file_t *f) {
    return f->type_count ? &ix->types[f->type_first + f->type_count - SKIP_ONE] : NULL;
}

static void pr_read_type(pr_index_t *ix, pr_file_t *f, int fi, const char **fl, const size_t *ln,
                         int n) {
    if (!pr_grow(ix, (void **)&ix->types, ix->ntypes, &ix->cap_types, sizeof(pr_type_t))) {
        return;
    }
    if (!f->type_count) {
        f->type_first = ix->ntypes;
    }
    pr_type_t *t = &ix->types[ix->ntypes++];
    f->type_count++;
    memset(t, 0, sizeof(*t));
    t->file = fi;
    t->kind = fl[1][0];
    t->fqn = pr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN]);
    t->dup = fl[CBM_SZ_3][0] == 'd';
    t->next = CBM_NOT_FOUND;
    if (n > PR_LINE_FIELDS) {
        t->first = pr_num(fl[PR_LINE_FIELDS]);
        t->last = n > PR_LINE_FIELDS + SKIP_ONE ? pr_num(fl[PR_LINE_FIELDS + SKIP_ONE]) : 0;
    }
    if (t->fqn) {
        const char *bs = strrchr(t->fqn, '\\');
        t->name = bs ? bs + SKIP_ONE : t->fqn;
    }
}

static void pr_read_record(pr_index_t *ix, pr_file_t *f, int fi, const char **fl, const size_t *ln,
                           int n) {
    char tag = fl[0][0];
    pr_type_t *t = NULL;
    if (tag == 'N' && n >= CBM_SZ_3 &&
        pr_grow(ix, (void **)&f->nss, f->nnss, &f->cap_nss, sizeof(pr_ns_t))) {
        pr_ns_t *s = &f->nss[f->nnss++];
        s->blk = (int)pr_num(fl[1]);
        s->name =
            pr_dash(fl[PAIR_LEN], ln[PAIR_LEN]) ? "" : pr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN]);
        s->first = n > CBM_SZ_3 ? pr_num(fl[CBM_SZ_3]) : 0;
        s->last = n > CBM_SZ_4 ? pr_num(fl[CBM_SZ_4]) : 0;
    } else if (tag == 'U' && n >= PR_USE_FIELDS &&
               pr_grow(ix, (void **)&f->uses, f->nuses, &f->cap_uses,
                       sizeof(cbm_doclink_php_use_t)) &&
               pr_grow(ix, (void **)&f->use_blk, f->nuses, &f->cap_use_blk, sizeof(int))) {
        cbm_doclink_php_use_t *u = &f->uses[f->nuses];
        f->use_blk[f->nuses++] = (int)pr_num(fl[1]);
        u->kind = fl[PAIR_LEN][0];
        u->alias = pr_copy(ix, fl[CBM_SZ_3], ln[CBM_SZ_3]);
        u->alias_len = ln[CBM_SZ_3];
        u->name = pr_copy(ix, fl[CBM_SZ_4], ln[CBM_SZ_4]);
        u->name_len = ln[CBM_SZ_4];
    } else if (tag == 'T' && n >= CBM_SZ_4) {
        pr_read_type(ix, f, fi, fl, ln, n);
    } else if (tag == 'B' && n >= CBM_SZ_3 && (t = pr_last_type(ix, f)) &&
               pr_grow(ix, (void **)&t->bases, t->nbases, &t->cap_bases, sizeof(pr_base_t))) {
        t->bases[t->nbases++] = (pr_base_t){fl[1][0], pr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN])};
    } else if (tag == 'K' && n >= CBM_SZ_3 && (t = pr_last_type(ix, f)) &&
               pr_grow(ix, (void **)&t->members, t->nmembers, &t->cap_members,
                       sizeof(pr_member_t))) {
        t->members[t->nmembers++] =
            (pr_member_t){fl[1][0], pr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN])};
    } else if (tag == 'F' && n >= CBM_SZ_3 &&
               pr_grow(ix, (void **)&ix->funcs, ix->nfuncs, &ix->cap_funcs, sizeof(pr_func_t))) {
        pr_func_t *fn = &ix->funcs[ix->nfuncs++];
        fn->file = fi;
        fn->fqn = pr_copy(ix, fl[1], ln[1]);
        fn->dup = fl[PAIR_LEN][0] == 'd';
        fn->next = CBM_NOT_FOUND;
        const char *bs = fn->fqn ? strrchr(fn->fqn, '\\') : NULL;
        fn->name = bs ? bs + SKIP_ONE : fn->fqn;
    }
}

static void pr_read_blob(pr_index_t *ix, pr_file_t *f, int fi, const char *blob) {
    const char *line = strchr(blob, '\n'); /* the tag line */
    while (line && !ix->oom) {
        const char *b = line + SKIP_ONE;
        const char *e = strchr(b, '\n');
        if (!e) {
            e = b + strlen(b);
        }
        const char *fl[PR_FIELDS];
        size_t ln[PR_FIELDS];
        int n = 0;
        const char *s = b;
        for (const char *p = b; p <= e && n < PR_FIELDS; p++) {
            if (p == e || *p == '\t') {
                fl[n] = s;
                ln[n] = (size_t)(p - s);
                n++;
                s = p + SKIP_ONE;
            }
        }
        if (n > 0 && ln[0] == SKIP_ONE) {
            pr_read_record(ix, f, fi, fl, ln, n);
        }
        line = *e ? e : NULL;
    }
}

static void pr_destroy(void *index) {
    pr_index_t *ix = (pr_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->type_at);
    cbm_ht_free(ix->func_at);
    cbm_ht_free(ix->roots);
    cbm_ht_free(ix->by_path);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

/* The namespace root of a qualified name ("" for a global one). */
static void pr_add_root(pr_index_t *ix, const char *name) {
    const char *bs = name ? strchr(name, '\\') : NULL;
    size_t n = bs ? (size_t)(bs - name) : (name ? strlen(name) : 0);
    if (!n) {
        return;
    }
    const char *key = pr_lower(ix, name, n);
    if (key && !cbm_ht_get(ix->roots, key)) {
        cbm_ht_set(ix->roots, key, (void *)(uintptr_t)SKIP_ONE);
    }
}

/* Chain every type and function into its name's list, and collect the
 * namespace roots. */
static bool pr_link(pr_index_t *ix) {
    ix->type_at = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(ix->ntypes + SKIP_ONE));
    ix->func_at = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(ix->nfuncs + SKIP_ONE));
    ix->roots = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_64);
    if (!ix->type_at || !ix->func_at || !ix->roots) {
        return false;
    }
    for (int i = 0; i < ix->ntypes && !ix->oom; i++) {
        pr_type_t *t = &ix->types[i];
        const char *key = t->fqn ? pr_lower(ix, t->fqn, strlen(t->fqn)) : NULL;
        if (!key) {
            continue;
        }
        void *prev = cbm_ht_get(ix->type_at, key);
        t->next = prev ? (int)((uintptr_t)prev - SKIP_ONE) : CBM_NOT_FOUND;
        cbm_ht_set(ix->type_at, key, (void *)(uintptr_t)(i + SKIP_ONE));
        if (strchr(t->fqn, '\\')) {
            pr_add_root(ix, t->fqn);
        }
    }
    for (int i = 0; i < ix->nfuncs && !ix->oom; i++) {
        pr_func_t *fn = &ix->funcs[i];
        const char *key = fn->fqn ? pr_lower(ix, fn->fqn, strlen(fn->fqn)) : NULL;
        if (!key) {
            continue;
        }
        void *prev = cbm_ht_get(ix->func_at, key);
        fn->next = prev ? (int)((uintptr_t)prev - SKIP_ONE) : CBM_NOT_FOUND;
        cbm_ht_set(ix->func_at, key, (void *)(uintptr_t)(i + SKIP_ONE));
        if (strchr(fn->fqn, '\\')) {
            pr_add_root(ix, fn->fqn);
        }
    }
    for (int i = 0; i < ix->nfiles && !ix->oom; i++) {
        for (int k = 0; k < ix->files[i].nnss; k++) {
            pr_add_root(ix, ix->files[i].nss[k].name);
        }
    }
    return !ix->oom;
}

static void *pr_build(const cbm_doclink_build_in_t *in) {
    pr_index_t *ix = (pr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->nfiles = in->file_count;
    ix->run_count = in->run_file_count;
    ix->files =
        in->file_count
            ? (pr_file_t *)cbm_arena_calloc(&ix->arena, (size_t)in->file_count * sizeof(pr_file_t))
            : NULL;
    ix->run_to_file = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                        (size_t)(in->run_file_count + SKIP_ONE) * sizeof(int));
    ix->by_path = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(in->file_count + SKIP_ONE));
    bool ok = (!in->file_count || ix->files) && ix->run_to_file && ix->by_path;
    for (int r = 0; ok && r < in->run_file_count; r++) {
        ix->run_to_file[r] = CBM_NOT_FOUND;
    }
    for (int i = 0; ok && i < in->file_count && !ix->oom; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        pr_file_t *f = &ix->files[i];
        f->path = src->rel_path;
        f->module_qn =
            cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_PHP);
        f->test = cbm_is_test_file(src->rel_path, CBM_LANG_PHP);
        ix->oom = ix->oom || !f->module_qn;
        cbm_ht_set(ix->by_path, src->rel_path, (void *)(uintptr_t)(i + SKIP_ONE));
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (src->scope) {
            pr_read_blob(ix, f, i, src->scope);
        }
    }
    ok = ok && !ix->oom && pr_link(ix);
    if (!ok) {
        cbm_log_error("doc_links.php.error", "step", "index", "reason", "alloc");
        pr_destroy(ix);
        return NULL;
    }
    char b[CBM_SZ_3][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[SKIP_ONE], sizeof(b[SKIP_ONE]), "%d", ix->ntypes);
    snprintf(b[PAIR_LEN], sizeof(b[PAIR_LEN]), "%d", ix->nfuncs);
    cbm_log_info("doc_links.php.index", "files", b[0], "types", b[SKIP_ONE], "functions",
                 b[PAIR_LEN]);
    return ix;
}

/* ── References ──────────────────────────────────────────────────── */

typedef struct {
    const char *cls; /* a type or function name, NULL for none */
    size_t cls_len;
    const char *mem; /* a member name (no `$`), NULL for none */
    size_t mem_len;
    bool prop; /* `::$name` */
    bool call; /* `()` at the end */
} pr_ref_t;

static bool pr_ident_start(char c) {
    return isalpha((unsigned char)c) || c == '_' || (unsigned char)c >= 0x80;
}

static bool pr_ident_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || (unsigned char)c >= 0x80;
}

/* The end of an identifier at s[i] (i when there is none). */
static size_t pr_ident_end(const char *s, size_t i, size_t n) {
    if (i >= n || !pr_ident_start(s[i])) {
        return i;
    }
    while (i < n && pr_ident_char(s[i])) {
        i++;
    }
    return i;
}

/* The end of a name (`\`-joined identifiers, a leading `\` allowed) at s[0]. */
static size_t pr_name_end(const char *s, size_t n) {
    size_t i = (n > 0 && s[0] == '\\') ? SKIP_ONE : 0;
    size_t e = pr_ident_end(s, i, n);
    if (e == i) {
        return 0;
    }
    while (e + SKIP_ONE < n && s[e] == '\\' && pr_ident_start(s[e + SKIP_ONE])) {
        e = pr_ident_end(s, e + SKIP_ONE, n);
    }
    return e;
}

static bool pr_parse(const char *s, size_t n, pr_ref_t *r) {
    memset(r, 0, sizeof(*r));
    size_t i = pr_name_end(s, n);
    if (i == 0) {
        return false; /* a member needs its type: `::name` is no FQSEN */
    }
    r->cls = s;
    r->cls_len = i;
    if (i + PAIR_LEN <= n && s[i] == ':' && s[i + SKIP_ONE] == ':') {
        i += PAIR_LEN;
        if (i < n && s[i] == '$') {
            r->prop = true;
            i++;
        }
        size_t e = pr_ident_end(s, i, n);
        if (e == i) {
            return false;
        }
        r->mem = s + i;
        r->mem_len = e - i;
        i = e;
    }
    if (!r->prop && i + PAIR_LEN == n && s[i] == '(' && s[i + SKIP_ONE] == ')') {
        r->call = true;
        i = n;
    }
    return i == n;
}

/* ── Resolution ──────────────────────────────────────────────────── */

typedef enum {
    PR_NONE = 0, /* nothing found */
    PR_TYPE,     /* a type of the index (`type`) */
    PR_NODE,
    PR_GAP,
    PR_EXTERNAL,
    PR_MISSING,
    PR_AMBIGUOUS,
} pr_kind_t;

typedef struct {
    pr_kind_t kind;
    int type;
    const cbm_gbuf_node_t *node;
    bool exact;
} pr_res_t;

typedef struct {
    const pr_index_t *ix;
    const cbm_gbuf_t *g;
    int src;
    const char *ns; /* the reference's namespace block */
    size_t ns_len;
    const cbm_doclink_php_use_t *uses; /* that block's imports */
    int nuses;
    int cur; /* the type the doc sits in, or -1 */
} pr_ctx_t;

static pr_res_t pr_mk(pr_kind_t k) {
    return (pr_res_t){k, CBM_NOT_FOUND, NULL, false};
}

static bool pr_eq_nocase(const char *a, size_t an, const char *b, size_t bn) {
    if (an != bn) {
        return false;
    }
    for (size_t i = 0; i < an; i++) {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i])) {
            return false;
        }
    }
    return true;
}

static void pr_lower_into(char *out, const char *s, size_t n) {
    for (size_t i = 0; i < n; i++) {
        out[i] = (char)tolower((unsigned char)s[i]);
    }
    out[n] = '\0';
}

/* The namespace block of `line` in file f and its imports. */
static void pr_block_at(pr_ctx_t *c, const pr_file_t *f, uint32_t line) {
    int blk = f->nnss ? CBM_NOT_FOUND : 0;
    c->ns = "";
    c->ns_len = 0;
    for (int i = 0; i < f->nnss; i++) {
        if (line >= f->nss[i].first && line <= f->nss[i].last) {
            blk = f->nss[i].blk;
            c->ns = f->nss[i].name;
            c->ns_len = strlen(c->ns);
            break;
        }
    }
    c->uses = NULL;
    c->nuses = 0;
    for (int i = 0; i < f->nuses; i++) {
        if (f->use_blk[i] != blk) {
            continue;
        }
        if (!c->uses) {
            c->uses = &f->uses[i];
        }
        c->nuses++;
    }
}

/* The type of file f whose lines hold `line`, or -1. */
static int pr_type_at(const pr_index_t *ix, const pr_file_t *f, uint32_t line) {
    for (int i = 0; i < f->type_count; i++) {
        const pr_type_t *t = &ix->types[f->type_first + i];
        if (line >= t->first && line <= t->last) {
            return f->type_first + i;
        }
    }
    return CBM_NOT_FOUND;
}

/* The node at `qn` in file `path`, or NULL. */
static const cbm_gbuf_node_t *pr_node(const pr_ctx_t *c, const char *qn, const char *path) {
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    if (!n || !n->label || !n->file_path || strcmp(n->file_path, path) != 0) {
        return NULL;
    }
    return n;
}

/* A declared type's own node: a gap when the graph folded it or has none. */
static pr_res_t pr_type_node(const pr_ctx_t *c, int ti) {
    const pr_type_t *t = &c->ix->types[ti];
    const pr_file_t *f = &c->ix->files[t->file];
    char qn[PR_QN_MAX];
    int n = snprintf(qn, sizeof(qn), "%s.%s", f->module_qn, t->name);
    const cbm_gbuf_node_t *node =
        (!t->dup && n > 0 && (size_t)n < sizeof(qn)) ? pr_node(c, qn, f->path) : NULL;
    pr_res_t r = pr_mk(node ? PR_NODE : PR_GAP);
    r.node = node;
    return r;
}

/* The repository's type named `fqn`: one, several (ambiguous, unless one is
 * in the reference's file) or none. */
static pr_res_t pr_find_type(const pr_ctx_t *c, const char *fqn, size_t len) {
    char key[PR_NAME_MAX];
    if (len >= sizeof(key)) {
        return pr_mk(PR_NONE);
    }
    pr_lower_into(key, fqn, len);
    void *v = cbm_ht_get(c->ix->type_at, key);
    int first = v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND;
    int count = 0;
    int own = CBM_NOT_FOUND;
    for (int i = first; i >= 0; i = c->ix->types[i].next) {
        count++;
        own = c->ix->types[i].file == c->src ? i : own;
    }
    if (count == 0) {
        return pr_mk(PR_NONE);
    }
    pr_res_t r = pr_mk(count == SKIP_ONE || own >= 0 ? PR_TYPE : PR_AMBIGUOUS);
    r.type = count == SKIP_ONE ? first : own;
    return r;
}

/* A name the repository does not declare: missing inside one of its
 * namespaces, else external. */
static pr_res_t pr_absent(const pr_ctx_t *c, const char *fqn, size_t len) {
    const char *bs = memchr(fqn, '\\', len);
    char key[PR_NAME_MAX];
    size_t rl = bs ? (size_t)(bs - fqn) : 0;
    if (!rl || rl >= sizeof(key)) {
        return pr_mk(PR_EXTERNAL);
    }
    pr_lower_into(key, fqn, rl);
    return pr_mk(cbm_ht_get(c->ix->roots, key) ? PR_MISSING : PR_EXTERNAL);
}

/* True when the first segment of `name` is a class-use alias. */
static bool pr_aliased(const pr_ctx_t *c, const char *name, size_t len) {
    const char *bs = memchr(name, '\\', len);
    size_t first = bs ? (size_t)(bs - name) : len;
    for (int i = 0; i < c->nuses; i++) {
        if (c->uses[i].kind == 'c' &&
            pr_eq_nocase(c->uses[i].alias, c->uses[i].alias_len, name, first)) {
            return true;
        }
    }
    return false;
}

static bool pr_is(const char *s, size_t n, const char *word) {
    return pr_eq_nocase(s, n, word, strlen(word));
}

/* The superclass of type ti, as a type of the index. */
static pr_res_t pr_parent(const pr_ctx_t *c, int ti) {
    const pr_type_t *t = &c->ix->types[ti];
    for (int i = 0; i < t->nbases; i++) {
        if (t->bases[i].rel == 'e' && t->bases[i].fqn) {
            pr_res_t r = pr_find_type(c, t->bases[i].fqn, strlen(t->bases[i].fqn));
            return r.kind == PR_NONE ? pr_mk(PR_EXTERNAL) : r;
        }
    }
    return pr_mk(PR_MISSING);
}

/* The type a reference's type part names. */
static pr_res_t pr_type_of(const pr_ctx_t *c, const pr_ref_t *r) {
    if (pr_is(r->cls, r->cls_len, "self") || pr_is(r->cls, r->cls_len, "static")) {
        pr_res_t res = pr_mk(c->cur >= 0 ? PR_TYPE : PR_MISSING);
        res.type = c->cur;
        return res;
    }
    if (pr_is(r->cls, r->cls_len, "parent")) {
        return c->cur >= 0 ? pr_parent(c, c->cur) : pr_mk(PR_MISSING);
    }
    char fqn[PR_NAME_MAX];
    size_t fl = cbm_doclink_php_qualify(fqn, sizeof(fqn), c->ns, c->ns_len, c->uses, c->nuses,
                                        r->cls, r->cls_len);
    if (!fl) {
        return pr_mk(PR_MISSING);
    }
    pr_res_t res = pr_find_type(c, fqn, fl);
    if (res.kind == PR_NONE) {
        res = pr_absent(c, fqn, fl);
    }
    res.exact = r->cls[0] == '\\' || pr_aliased(c, r->cls, r->cls_len);
    return res;
}

/* ── Members across supertypes ───────────────────────────────────── */

typedef struct {
    const pr_ctx_t *c;
    char want; /* 'm' a method, 'p' a property, 'c' a constant or enum case */
    const char *name;
    size_t len;
    int visited[PR_VISIT_MAX];
    int nvisited;
    bool open; /* a supertype outside the repository */
    bool amb;  /* a supertype with several declarations */
    int owner; /* found: the declaring type and member */
    const pr_member_t *found;
} pr_search_t;

static bool pr_member_is(const pr_search_t *s, const pr_member_t *m) {
    size_t ml = strlen(m->name);
    if (s->want == 'm') {
        return m->kind == 'm' && pr_eq_nocase(m->name, ml, s->name, s->len);
    }
    bool kind = s->want == 'p' ? m->kind == 'p' : (m->kind == 'c' || m->kind == 'e');
    return kind && ml == s->len && memcmp(m->name, s->name, ml) == 0;
}

static bool pr_seen(pr_search_t *s, int ti) {
    for (int i = 0; i < s->nvisited; i++) {
        if (s->visited[i] == ti) {
            return true;
        }
    }
    if (s->nvisited == PR_VISIT_MAX) {
        s->open = true; /* too wide to follow: not every supertype was read */
        return true;
    }
    s->visited[s->nvisited++] = ti;
    return false;
}

static bool pr_search(pr_search_t *s, int ti, int depth) {
    if (depth > PR_MAX_DEPTH) {
        s->open = true;
        return false;
    }
    if (pr_seen(s, ti)) {
        return false;
    }
    const pr_type_t *t = &s->c->ix->types[ti];
    for (int i = 0; i < t->nmembers; i++) {
        if (pr_member_is(s, &t->members[i])) {
            s->owner = ti;
            s->found = &t->members[i];
            return true;
        }
    }
    static const char order[] = {'t', 'e', 'i'}; /* traits, superclass, interfaces */
    for (size_t o = 0; o < sizeof(order); o++) {
        for (int i = 0; i < t->nbases; i++) {
            if (t->bases[i].rel != order[o] || !t->bases[i].fqn) {
                continue;
            }
            pr_res_t r = pr_find_type(s->c, t->bases[i].fqn, strlen(t->bases[i].fqn));
            if (r.kind == PR_TYPE && pr_search(s, r.type, depth + SKIP_ONE)) {
                return true;
            }
            s->amb = s->amb || r.kind == PR_AMBIGUOUS;
            s->open = s->open || r.kind == PR_NONE;
        }
    }
    return false;
}

/* The member's outcome: a method's node, else a graph gap. */
static pr_res_t pr_member_node(const pr_ctx_t *c, const pr_search_t *s) {
    const pr_type_t *t = &c->ix->types[s->owner];
    if (s->found->kind != 'm' || t->dup) {
        return pr_mk(PR_GAP);
    }
    const pr_file_t *f = &c->ix->files[t->file];
    char qn[PR_QN_MAX];
    int n = snprintf(qn, sizeof(qn), "%s.%s.%s", f->module_qn, t->name, s->found->name);
    const cbm_gbuf_node_t *node =
        (n > 0 && (size_t)n < sizeof(qn)) ? pr_node(c, qn, f->path) : NULL;
    pr_res_t r = pr_mk(node ? PR_NODE : PR_GAP);
    r.node = node;
    return r;
}

static pr_res_t pr_member(const pr_ctx_t *c, int ti, const pr_ref_t *r) {
    pr_search_t s = {.c = c, .name = r->mem, .len = r->mem_len, .owner = CBM_NOT_FOUND};
    s.want = r->prop ? 'p' : r->call ? 'm' : 'c';
    bool hit = pr_search(&s, ti, 0);
    if (!hit && s.want == 'c') {
        /* `Type::name` without parentheses names a method as often as a
         * constant: no constant or case of that name, then a method */
        bool open = s.open;
        bool amb = s.amb;
        s.want = 'm';
        s.nvisited = 0;
        hit = pr_search(&s, ti, 0);
        s.open = s.open || open;
        s.amb = s.amb || amb;
    }
    if (hit) {
        return pr_member_node(c, &s);
    }
    return pr_mk(s.amb ? PR_AMBIGUOUS : s.open ? PR_EXTERNAL : PR_MISSING);
}

/* ── Functions ───────────────────────────────────────────────────── */

static pr_res_t pr_find_func(const pr_ctx_t *c, const char *fqn, size_t len) {
    char key[PR_NAME_MAX];
    if (len >= sizeof(key)) {
        return pr_mk(PR_NONE);
    }
    pr_lower_into(key, fqn, len);
    void *v = cbm_ht_get(c->ix->func_at, key);
    int first = v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND;
    int count = 0;
    int own = CBM_NOT_FOUND;
    for (int i = first; i >= 0; i = c->ix->funcs[i].next) {
        count++;
        own = c->ix->funcs[i].file == c->src ? i : own;
    }
    if (count == 0) {
        return pr_mk(PR_NONE);
    }
    if (count > SKIP_ONE && own < 0) {
        return pr_mk(PR_AMBIGUOUS);
    }
    const pr_func_t *fn = &c->ix->funcs[count == SKIP_ONE ? first : own];
    const pr_file_t *f = &c->ix->files[fn->file];
    char qn[PR_QN_MAX];
    int n = snprintf(qn, sizeof(qn), "%s.%s", f->module_qn, fn->name);
    const cbm_gbuf_node_t *node =
        (!fn->dup && n > 0 && (size_t)n < sizeof(qn)) ? pr_node(c, qn, f->path) : NULL;
    pr_res_t r = pr_mk(node ? PR_NODE : PR_GAP);
    r.node = node;
    return r;
}

static pr_res_t pr_function(const pr_ctx_t *c, const pr_ref_t *r) {
    char fqn[PR_NAME_MAX];
    if (r->cls[0] == '\\' || memchr(r->cls, '\\', r->cls_len)) {
        /* qualified: the type rules (a namespace alias may start it) */
        size_t fl = cbm_doclink_php_qualify(fqn, sizeof(fqn), c->ns, c->ns_len, c->uses, c->nuses,
                                            r->cls, r->cls_len);
        pr_res_t res = fl ? pr_find_func(c, fqn, fl) : pr_mk(PR_MISSING);
        if (res.kind == PR_NONE) {
            res = pr_absent(c, fqn, fl);
        }
        res.exact = r->cls[0] == '\\' || pr_aliased(c, r->cls, r->cls_len);
        return res;
    }
    for (int i = 0; i < c->nuses; i++) {
        const cbm_doclink_php_use_t *u = &c->uses[i];
        if (u->kind == 'f' && pr_eq_nocase(u->alias, u->alias_len, r->cls, r->cls_len)) {
            pr_res_t res = pr_find_func(c, u->name, u->name_len);
            if (res.kind == PR_NONE) {
                res = pr_absent(c, u->name, u->name_len);
            }
            res.exact = true;
            return res;
        }
    }
    if (c->cur >= 0) {
        /* a method the documented type declares or inherits */
        pr_search_t s = {
            .c = c, .want = 'm', .name = r->cls, .len = r->cls_len, .owner = CBM_NOT_FOUND};
        if (pr_search(&s, c->cur, 0)) {
            return pr_member_node(c, &s);
        }
    }
    pr_res_t res = pr_mk(PR_NONE);
    if (c->ns_len) {
        int n = snprintf(fqn, sizeof(fqn), "%.*s\\%.*s", (int)c->ns_len, c->ns, (int)r->cls_len,
                         r->cls);
        res = (n > 0 && (size_t)n < sizeof(fqn)) ? pr_find_func(c, fqn, (size_t)n) : res;
    }
    if (res.kind == PR_NONE) {
        res = pr_find_func(c, r->cls, r->cls_len); /* PHP's global fallback */
    }
    return res.kind == PR_NONE ? pr_mk(PR_EXTERNAL) : res; /* none: a built-in */
}

/* ── Outcomes ────────────────────────────────────────────────────── */

static void pr_out(const pr_ctx_t *c, pr_res_t r, cbm_doclink_outcome_t *out) {
    switch (r.kind) {
    case PR_NODE: {
        const pr_file_t *src = &c->ix->files[c->src];
        void *v = r.node->file_path ? cbm_ht_get(c->ix->by_path, r.node->file_path) : NULL;
        int tf = v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND;
        if (tf >= 0 && c->ix->files[tf].test && !src->test) {
            out->kind = CBM_DOCLINK_UNRESOLVED;
            out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
            return;
        }
        out->kind = CBM_DOCLINK_EDGE;
        out->target = r.node;
        out->exact = r.exact;
        return;
    }
    case PR_EXTERNAL:
        out->reason = CBM_DOCLINK_REASON_EXTERNAL;
        break;
    case PR_AMBIGUOUS:
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        break;
    case PR_GAP:
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        break;
    default:
        out->reason = CBM_DOCLINK_REASON_MISSING;
        break;
    }
    out->kind = CBM_DOCLINK_UNRESOLVED;
}

static void pr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* PHP keeps no per-file working state */
    const pr_index_t *ix = (const pr_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    pr_ctx_t c = {.ix = ix, .g = graph, .src = ix->run_to_file[run_file]};
    const pr_file_t *f = &ix->files[c.src];
    size_t rl = strlen(link->raw);
    pr_ref_t r;
    if (rl > CBM_DOCLINK_PHP_REF_MAX || !pr_parse(link->raw, rl, &r)) {
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    pr_block_at(&c, f, link->line);
    c.cur = pr_type_at(ix, f, link->def_line);
    pr_res_t res;
    if (!r.mem && r.call) {
        res = pr_function(&c, &r);
    } else if (!r.mem) {
        res = pr_type_of(&c, &r);
        if (res.kind == PR_TYPE) {
            bool exact = res.exact;
            res = pr_type_node(&c, res.type);
            res.exact = exact;
        }
    } else {
        res = pr_type_of(&c, &r);
        if (res.kind == PR_TYPE) {
            bool exact = res.exact;
            res = pr_member(&c, res.type, &r);
            res.exact = exact;
        }
    }
    pr_out(&c, res, out);
}

/* A top-level function (F fqn flags) or a member of the type above it (K kind
 * name) declares one name; reported as a reference spells it, the last
 * segment of a qualified name. A type, a supertype, a namespace block or a
 * use import changes how others resolve: GLOBAL. */
static bool pr_removable(const char *line, size_t len, const char **name, size_t *nlen) {
    if (len <= PAIR_LEN || line[1] != '\t') {
        return false;
    }
    if (line[0] == 'K') {
        return cbm_doclinks_scope_field(line, len, PAIR_LEN, name, nlen);
    }
    if (line[0] != 'F' || !cbm_doclinks_scope_field(line, len, 1, name, nlen)) {
        return false;
    }
    for (size_t i = *nlen; i > 0; i--) {
        if ((*name)[i - SKIP_ONE] == '\\') {
            *nlen -= i;
            *name += i;
            break;
        }
    }
    return true;
}

static int pr_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    return cbm_doclinks_scope_delta_lines(stored, fresh, pr_removable, removed, ud);
}

const cbm_doclink_resolver_t cbm_doclink_php_resolver = {
    .langs = {CBM_LANG_PHP},
    .lang_count = SKIP_ONE,
    .scope_tag = CBM_DOCLINK_PHP_SCOPE_TAG,
    .build = pr_build,
    .destroy = pr_destroy,
    .resolve = pr_resolve,
    .scope_delta = pr_scope_delta,
};
