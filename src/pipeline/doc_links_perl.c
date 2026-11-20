/*
 * doc_links_perl.c — resolving Perl POD L<> references (doclink_perl.c).
 *
 * A page is a module. L<Mojo::Base> binds the module whose file declares
 * `package Mojo::Base;` (the scope blob's P records). When several files
 * declare it, the one the library layout names (…/Mojo/Base.pm) is the
 * module; more than one such file, or none among several, is ambiguous. A page
 * the repository does not declare (CPAN, the Perl core, a .pod page:
 * discovery keeps no .pod file, so nothing can say it is in the repository)
 * is external, and so is a man page (L<crontab(5)>).
 *
 * The target of a module is its File node: a POD page is the file it is
 * written in (the field test's audited rule). Its Module node would do for
 * most, but not where a same-named Folder holds the module's qualified name
 * (lib/Mojo.pm next to lib/Mojo/).
 *
 * A section, L<Module/section>: when a heading of the module's file reads
 * `section` (POD text as it renders, cbm_doclink_perl_plain_text) and names a
 * sub that has a definition in that file, the target is the sub; headings
 * that name two subs are ambiguous. A section that names no sub is a place in
 * the module's documentation: the target is the module. L</section> is the
 * same page: a sub its heading names binds; a heading that names no sub is a
 * place in the source's own page, no reference worth an edge or a row
 * (local); a section the page has no heading for is missing (doc rot).
 *
 * Test code (t/, xt/, *.t, and cbm_is_test_file) is bound only from test code.
 * Every edge is exact: a module name and a heading are spelled out in full.
 *
 * The scope blob carries no lines (doclink_perl.c). A dropped sub is repaired
 * file by file (pl_scope_delta); a change that adds a sub, or adds or drops a
 * package or a heading, is a full re-resolution; an edit inside a sub changes
 * no blob.
 */
#include "pipeline/doc_links.h"
#include "pipeline/pipeline_internal.h" /* cbm_pipeline_file_node */
#include "cbm.h"
#include "doclink.h"
#include "helpers.h" /* cbm_fqn_module_source_lang, cbm_is_test_file */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── The index ───────────────────────────────────────────────────── */

typedef struct {
    const char *sub; /* the sub the heading names; NULL: none */
    const char *key; /* the heading as it renders */
} pr_head_t;

typedef struct {
    const char *path;      /* rel_path */
    const char *module_qn; /* the file's module qualified name */
    bool test;
    int nsubs;
    const char **subs; /* subs with a definition in the file */
    int nheads;
    pr_head_t *heads;
} pr_file_t;

typedef struct {
    const char *name;
    int file;
} pr_decl_t;

typedef struct {
    int first; /* into decls */
    int count;
} pr_range_t;

typedef struct {
    CBMArena arena;
    const char *project;
    pr_file_t *files;
    int nfiles;
    int *run_to_file; /* run_file -> files index, -1: none */
    int run_count;
    pr_decl_t *decls; /* every package declaration, by name, then path */
    int ndecls;
    CBMHashTable *pkg; /* package name -> pr_range_t */
    bool oom;
} pr_index_t;

static bool pr_test_path(const char *path) {
    if (cbm_is_test_file(path, CBM_LANG_PERL)) {
        return true;
    }
    size_t n = strlen(path);
    if (n >= PAIR_LEN && strcmp(path + n - PAIR_LEN, ".t") == 0) {
        return true;
    }
    return strncmp(path, "t/", PAIR_LEN) == 0 || strncmp(path, "xt/", CBM_SZ_3) == 0 ||
           strstr(path, "/t/") != NULL || strstr(path, "/xt/") != NULL;
}

/* One record line of a blob: its fields, split at tabs (at most `cap`). */
static int pr_fields(const char *b, const char *e, const char **f, size_t *fl, int cap) {
    int n = 0;
    const char *s = b;
    for (const char *p = b; p <= e && n < cap; p++) {
        if (p == e || *p == '\t') {
            f[n] = s;
            fl[n] = (size_t)(p - s);
            n++;
            s = p + SKIP_ONE;
        }
    }
    return n;
}

enum { PR_MAX_FIELDS = 4, PR_F_TAG = 0, PR_F_A = 1, PR_F_B = 2 };

static const char *pr_copy(pr_index_t *ix, const char *s, size_t n) {
    char *c = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !c;
    return c;
}

/* Count, then fill: the records of one file's blob. */
static void pr_read_blob(pr_index_t *ix, int fi, const char *blob, bool fill) {
    pr_file_t *f = &ix->files[fi];
    int np = 0;
    int ns = 0;
    int nh = 0;
    const char *line = strchr(blob, '\n'); /* the tag line */
    while (line && !ix->oom) {
        const char *b = line + SKIP_ONE;
        const char *e = strchr(b, '\n');
        if (!e) {
            e = b + strlen(b);
        }
        const char *fld[PR_MAX_FIELDS];
        size_t fln[PR_MAX_FIELDS];
        int n = pr_fields(b, e, fld, fln, PR_MAX_FIELDS);
        if (n >= PAIR_LEN && fln[PR_F_TAG] == SKIP_ONE) {
            char tag = fld[PR_F_TAG][0];
            if (tag == 'P') {
                if (fill) {
                    ix->decls[ix->ndecls++] =
                        (pr_decl_t){pr_copy(ix, fld[PR_F_A], fln[PR_F_A]), fi};
                }
                np++;
            } else if (tag == 'S' && n >= CBM_SZ_3) {
                if (fill) {
                    f->subs[f->nsubs++] = pr_copy(ix, fld[PR_F_B], fln[PR_F_B]);
                }
                ns++;
            } else if (tag == 'H' && n >= CBM_SZ_3) {
                if (fill) {
                    bool none = fln[PR_F_A] == SKIP_ONE && fld[PR_F_A][0] == '-';
                    f->heads[f->nheads++] = (pr_head_t){
                        none ? NULL : pr_copy(ix, fld[PR_F_A], fln[PR_F_A]),
                        pr_copy(ix, fld[PR_F_B], fln[PR_F_B]),
                    };
                }
                nh++;
            }
        }
        line = *e ? e : NULL;
    }
    if (!fill) {
        ix->ndecls += np;
        f->subs =
            ns ? (const char **)cbm_arena_alloc(&ix->arena, (size_t)ns * sizeof(char *)) : NULL;
        f->heads =
            nh ? (pr_head_t *)cbm_arena_alloc(&ix->arena, (size_t)nh * sizeof(pr_head_t)) : NULL;
        ix->oom = ix->oom || (ns && !f->subs) || (nh && !f->heads);
    }
}

static int pr_decl_cmp(const void *a, const void *b) {
    const pr_decl_t *x = (const pr_decl_t *)a;
    const pr_decl_t *y = (const pr_decl_t *)b;
    int c = strcmp(x->name, y->name);
    return c != 0 ? c : (x->file > y->file) - (x->file < y->file);
}

static void pr_destroy(void *index) {
    pr_index_t *ix = (pr_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->pkg);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static bool pr_index_files(pr_index_t *ix, const cbm_doclink_build_in_t *in) {
    ix->nfiles = in->file_count;
    ix->files =
        in->file_count
            ? (pr_file_t *)cbm_arena_calloc(&ix->arena, (size_t)in->file_count * sizeof(pr_file_t))
            : NULL;
    ix->run_count = in->run_file_count;
    ix->run_to_file = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                        (size_t)(in->run_file_count + SKIP_ONE) * sizeof(int));
    if ((in->file_count && !ix->files) || !ix->run_to_file) {
        return false;
    }
    for (int r = 0; r < in->run_file_count; r++) {
        ix->run_to_file[r] = CBM_NOT_FOUND;
    }
    for (int i = 0; i < in->file_count && !ix->oom; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        pr_file_t *f = &ix->files[i];
        f->path = src->rel_path; /* valid until destroy */
        f->module_qn =
            cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_PERL);
        f->test = pr_test_path(src->rel_path);
        ix->oom = ix->oom || !f->module_qn;
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (src->scope) {
            pr_read_blob(ix, i, src->scope, false);
        }
    }
    return !ix->oom;
}

static bool pr_index_packages(pr_index_t *ix, const cbm_doclink_build_in_t *in) {
    int total = ix->ndecls;
    ix->ndecls = 0;
    ix->decls =
        total ? (pr_decl_t *)cbm_arena_alloc(&ix->arena, (size_t)total * sizeof(pr_decl_t)) : NULL;
    if (total && !ix->decls) {
        return false;
    }
    for (int i = 0; i < in->file_count && !ix->oom; i++) {
        if (in->files[i].scope) {
            pr_read_blob(ix, i, in->files[i].scope, true);
        }
    }
    if (ix->oom) {
        return false;
    }
    qsort(ix->decls, (size_t)ix->ndecls, sizeof(pr_decl_t), pr_decl_cmp);
    ix->pkg = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(ix->ndecls + SKIP_ONE));
    if (!ix->pkg) {
        return false;
    }
    for (int i = 0; i < ix->ndecls;) {
        int j = i;
        while (j < ix->ndecls && strcmp(ix->decls[j].name, ix->decls[i].name) == 0) {
            j++;
        }
        pr_range_t *r = (pr_range_t *)cbm_arena_alloc(&ix->arena, sizeof(pr_range_t));
        if (!r) {
            return false;
        }
        *r = (pr_range_t){i, j - i};
        cbm_ht_set(ix->pkg, ix->decls[i].name, r);
        i = j;
    }
    return true;
}

static void *pr_build(const cbm_doclink_build_in_t *in) {
    pr_index_t *ix = (pr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    if (!pr_index_files(ix, in) || !pr_index_packages(ix, in)) {
        cbm_log_error("doc_links.perl.error", "step", "index", "reason", "alloc");
        pr_destroy(ix);
        return NULL;
    }
    char b[PAIR_LEN][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[SKIP_ONE], sizeof(b[SKIP_ONE]), "%u", cbm_ht_count(ix->pkg));
    cbm_log_info("doc_links.perl.index", "files", b[0], "packages", b[SKIP_ONE]);
    return ix;
}

/* ── Resolution ──────────────────────────────────────────────────── */

typedef struct {
    const pr_index_t *ix;
    const cbm_gbuf_t *g;
    const pr_file_t *src;
    cbm_doclink_outcome_t *out;
} pr_ctx_t;

static void pr_row(const pr_ctx_t *c, int reason) {
    c->out->kind = CBM_DOCLINK_UNRESOLVED;
    c->out->reason = reason;
}

static void pr_edge(const pr_ctx_t *c, const pr_file_t *f, const cbm_gbuf_node_t *node) {
    if (!node) {
        pr_row(c, CBM_DOCLINK_REASON_GRAPH_GAP);
        return;
    }
    if (f->test && !c->src->test) {
        pr_row(c, CBM_DOCLINK_REASON_TEST_ONLY);
        return;
    }
    c->out->kind = CBM_DOCLINK_EDGE;
    c->out->target = node;
    c->out->exact = true;
}

static bool pr_node_is(const cbm_gbuf_node_t *n, const pr_file_t *f, const char *label) {
    return n && n->label && n->file_path && strcmp(n->label, label) == 0 &&
           strcmp(n->file_path, f->path) == 0;
}

/* The module's node: its File node. */
static const cbm_gbuf_node_t *pr_module_node(const pr_ctx_t *c, const pr_file_t *f) {
    return cbm_pipeline_file_node(c->g, c->ix->project, f->path);
}

/* The node of sub `name` of file f; NULL when it has none of its own. */
static const cbm_gbuf_node_t *pr_sub_node(const pr_ctx_t *c, const pr_file_t *f, const char *name) {
    size_t ql = strlen(f->module_qn);
    size_t nl = strlen(name);
    char stack[CBM_SZ_512];
    char *qn = ql + nl + PAIR_LEN <= sizeof(stack)
                   ? stack
                   : (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, ql + nl + PAIR_LEN);
    if (!qn) {
        return NULL;
    }
    memcpy(qn, f->module_qn, ql);
    qn[ql] = '.';
    memcpy(qn + ql + SKIP_ONE, name, nl + SKIP_ONE);
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    if (qn != stack) {
        cbm_free(CBM_MEM_CLASS_OTHER, qn);
    }
    return (pr_node_is(n, f, "Function") || pr_node_is(n, f, "Method")) ? n : NULL;
}

static bool pr_has_sub(const pr_file_t *f, const char *name) {
    for (int i = 0; i < f->nsubs; i++) {
        if (strcmp(f->subs[i], name) == 0) {
            return true;
        }
    }
    return false;
}

/* The one sub a heading `key` of file f names (it has a definition there):
 * 1 found, 0 none, 2 several. */
static int pr_heading_sub(const pr_file_t *f, const char *key, const char **sub) {
    *sub = NULL;
    for (int i = 0; i < f->nheads; i++) {
        const pr_head_t *h = &f->heads[i];
        if (!h->sub || strcmp(h->key, key) != 0 || !pr_has_sub(f, h->sub)) {
            continue;
        }
        if (*sub && strcmp(*sub, h->sub) != 0) {
            return PAIR_LEN;
        }
        *sub = h->sub;
    }
    return *sub ? SKIP_ONE : 0;
}

static bool pr_has_heading(const pr_file_t *f, const char *key) {
    for (int i = 0; i < f->nheads; i++) {
        if (strcmp(f->heads[i].key, key) == 0) {
            return true;
        }
    }
    return false;
}

static void pr_section(const pr_ctx_t *c, const pr_file_t *f, const char *section, bool same_page) {
    char key[CBM_DOCLINK_PERL_KEY_MAX];
    cbm_doclink_perl_plain_text(section, strlen(section), key, sizeof(key));
    const char *sub = NULL;
    int found = pr_heading_sub(f, key, &sub);
    if (found > SKIP_ONE) {
        pr_row(c, CBM_DOCLINK_REASON_AMBIGUOUS);
    } else if (found == SKIP_ONE) {
        pr_edge(c, f, pr_sub_node(c, f, sub));
    } else if (same_page && pr_has_heading(f, key)) {
        c->out->kind = CBM_DOCLINK_LOCAL; /* a place in the source's own page */
    } else if (same_page) {
        pr_row(c, CBM_DOCLINK_REASON_MISSING); /* a section the page does not have */
    } else {
        pr_edge(c, f, pr_module_node(c, f));
    }
}

/* True when `path` is the file the library layout gives module `page`. */
static bool pr_layout_names(const char *path, const char *page) {
    char want[CBM_DOCLINK_PERL_KEY_MAX + CBM_SZ_8];
    size_t w = 0;
    for (const char *p = page; *p && w + CBM_SZ_4 < sizeof(want); p++) {
        if (p[0] == ':' && p[SKIP_ONE] == ':') {
            want[w++] = '/';
            p++;
        } else {
            want[w++] = *p;
        }
    }
    memcpy(want + w, ".pm", CBM_SZ_4);
    w += CBM_SZ_3;
    size_t n = strlen(path);
    return n >= w && strcmp(path + n - w, want) == 0 && (n == w || path[n - w - SKIP_ONE] == '/');
}

/* The file of module `page`: NULL with *ambiguous false when the repository
 * declares no such package. */
static const pr_file_t *pr_page_file(const pr_index_t *ix, const char *page, bool *ambiguous) {
    *ambiguous = false;
    const pr_range_t *r = (const pr_range_t *)cbm_ht_get(ix->pkg, page);
    if (!r || r->count == 0) {
        return NULL;
    }
    if (r->count == SKIP_ONE) {
        return &ix->files[ix->decls[r->first].file];
    }
    const pr_file_t *hit = NULL;
    for (int i = r->first; i < r->first + r->count; i++) {
        const pr_file_t *f = &ix->files[ix->decls[i].file];
        if (f == hit || !pr_layout_names(f->path, page)) {
            continue;
        }
        if (hit) {
            *ambiguous = true;
            return NULL;
        }
        hit = f;
    }
    *ambiguous = hit == NULL;
    return hit;
}

static void pr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* Perl keeps no per-file working state */
    const pr_index_t *ix = (const pr_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    pr_ctx_t c = {.ix = ix, .g = graph, .src = &ix->files[ix->run_to_file[run_file]], .out = out};
    CBMPerlLink pl;
    if (!cbm_doclink_perl_parse_link(link->raw, &pl)) {
        pr_row(&c, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (pl.kind == CBM_PERL_LINK_MAN) {
        pr_row(&c, CBM_DOCLINK_REASON_EXTERNAL);
        return;
    }
    if (pl.kind == CBM_PERL_LINK_SECTION) {
        pr_section(&c, c.src, pl.section, true);
        return;
    }
    bool ambiguous = false;
    const pr_file_t *f = pr_page_file(ix, pl.page, &ambiguous);
    if (!f) {
        pr_row(&c, ambiguous ? CBM_DOCLINK_REASON_AMBIGUOUS : CBM_DOCLINK_REASON_EXTERNAL);
        return;
    }
    if (pl.section[0]) {
        pr_section(&c, f, pl.section, false);
        return;
    }
    pr_edge(&c, f, pr_module_node(&c, f));
}

/* A sub (S pkg name) declares one name. A package (its index places every
 * sub after it), a heading (a section is found by its text) and the
 * parse-error mark change how others resolve: GLOBAL. */
static bool pl_removable(const char *line, size_t len, const char **name, size_t *nlen) {
    return len > PAIR_LEN && line[0] == 'S' && line[1] == '\t' &&
           cbm_doclinks_scope_field(line, len, PAIR_LEN, name, nlen);
}

static int pl_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    return cbm_doclinks_scope_delta_lines(stored, fresh, pl_removable, removed, ud);
}

const cbm_doclink_resolver_t cbm_doclink_perl_resolver = {
    .langs = {CBM_LANG_PERL},
    .lang_count = 1,
    .scope_tag = CBM_DOCLINK_PERL_SCOPE_TAG,
    .build = pr_build,
    .destroy = pr_destroy,
    .resolve = pr_resolve,
    .scope_delta = pl_scope_delta,
};
