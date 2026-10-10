/*
 * doc_links_md.c — Markdown references -> MENTIONS edges: the resolving half.
 *
 * A Markdown reference (doclink_md.c) names a repository PATH, so it is looked
 * up in the graph as written: the File node, or the Folder node, at that path,
 * read relative to the document's own directory and -- for a path with a
 * directory part -- relative to the repository root as well (documents are
 * written both ways). A file name alone is read relative to the document
 * only: a file of that name elsewhere in the repository is not what the
 * document says (the field tests' rule for bare file names).
 *
 * Into the file: a line range picks the innermost code definition that holds
 * the WHOLE range; a member (`path::Name`, `path::Owner::name`) picks the one
 * definition of that name in the file. Otherwise the edge goes to the file,
 * and the range stays on it ("target_lines"). Every edge is exact: the
 * document wrote the path.
 *
 * Unresolved:
 *   missing    the path is not in the graph, but its directory is: the
 *              document names something that is not there (doc rot)
 *   ambiguous  a file name alone that is not next to the document but exists
 *              elsewhere in the repository: the document does not say which
 * Not a reference to this repository at all (LOCAL: no edge, no row): a link
 * back into the same document; a path that is neither indexed nor of a type
 * the indexer reads (an image, an archive, an extensionless page); a path
 * whose directory is not in the repository either (a file a tutorial asks
 * the reader to create, a system path, a path above the root); and, in a
 * code span, a rooted single segment without a file extension, which is a
 * URL route (`/docs`, `/health`), not the directory of that name.
 */
#include "pipeline/doc_links.h"

#include "discover/discover.h" /* cbm_language_for_filename */
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
    MDR_PATH_CAP = 1024,      /* the longest repository path looked up */
    MDR_TRIES = 2,            /* relative to the document, relative to the root */
    MDR_BASENAME_INIT = 1024, /* first capacity of the file-name table */
    MDR_HEX_BASE = 16,
    MDR_PCT_LEN = 3,    /* `%XX` */
    MDR_ADR_DIGITS = 5, /* an ADR number has at most this many digits */
    MDR_PRIO_FIELD = 3, /* H8's label priorities: types 0, Type 1, callables 2 */
    MDR_PRIO_OTHER = 4,
};

/* Labels whose nodes are code a range or a member can name. */
/* Labels whose nodes are code a range, a member or a name can bind. Every one
 * of them is proxied on the incremental route (pipeline_delta.c preseed), so
 * a re-resolved document binds what a full build binds; Macro nodes are not
 * proxied, and are no target here. */
static const char *const MDR_CODE_LABELS[] = {
    "Function", "Method",   "Class", "Struct", "Interface", "Trait",   "Enum", "Type",
    "Variable", "Constant", "Field", "Table",  "View",      "Trigger", NULL};

static unsigned char mdr_norm_c(unsigned char c);
static bool mdr_in(const char *s, size_t n, const char *const *list);
static void mdr_destroy(void *index);

/* A module or package a qualified name can name: a code file without its
 * extension (`pkg/routing.py` is `pkg.routing`), a package directory through
 * its index file (`pkg/__init__.py`, `mod.rs`, `index.ts`, `lib.rs`), or a
 * directory. */
typedef struct mdr_pkg {
    const char *chain; /* the pieces before its name, '.'-joined */
    const cbm_gbuf_node_t *node;
    const char *file;     /* its code file (NULL: a directory) */
    struct mdr_pkg *next; /* the next one of the same name */
} mdr_pkg_t;

/* A code definition and its file: the by-file list ranges and members are
 * read from. */
typedef struct {
    const char *file;
    const cbm_gbuf_node_t *node;
} mdr_def_t;

typedef struct {
    const char *project;
    size_t project_len;
    const char **run_paths; /* run_file -> rel_path (NULL: not a Markdown file of this run) */
    int run_count;
    /* base name -> number of File nodes with it (a file name alone that is
     * not next to the document: ambiguous when the name exists anywhere) */
    CBMHashTable *basenames;
    CBMHashTable *pkgs; /* name -> mdr_pkg_t list */
    CBMHashTable *top;  /* normalized top-level directory and code-file names */
    CBMArena arena;     /* the strings and records of pkgs and top */
    mdr_def_t *defs;    /* every code definition, sorted by (file, start line, id) */
    int ndefs;
    CBMHashTable *adr_by_file; /* file path -> its ADR node (doc_adr.c) */
    CBMHashTable *folders;     /* directory path -> its Folder node */
} mdr_index_t;

static const char *mdr_base(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + SKIP_ONE : path;
}

static bool mdr_code_file(const char *path) {
    CBMLanguage lang = cbm_language_for_filename(mdr_base(path));
    return lang != CBM_LANG_COUNT && lang != CBM_LANG_MARKDOWN && lang != CBM_LANG_RST &&
           lang != CBM_LANG_ASCIIDOC && lang != CBM_LANG_PDF && lang != CBM_LANG_HTML &&
           lang != CBM_LANG_CSS && lang != CBM_LANG_SCSS && !cbm_has_config_extension(path);
}

/* The stems of a package directory's index file. */
static const char *const MDR_INDEX_STEMS[] = {"__init__", "mod", "index", "lib", NULL};

/* Lower-cased copy, '-' read as '_'. */
static char *mdr_norm_dup(CBMArena *a, const char *s, size_t n) {
    char *out = cbm_arena_strndup(a, s, n);
    for (size_t i = 0; out && i < n; i++) {
        out[i] = (char)mdr_norm_c((unsigned char)out[i]);
    }
    return out;
}

/* Add the module `key` (a path without extension, '/'-separated, whose last
 * piece may hold dots) unless one is there. False when memory ran out. */
static bool mdr_add_pkg(mdr_index_t *x, CBMHashTable *seen, const char *key, size_t klen,
                        const cbm_gbuf_node_t *node, const char *file) {
    char *seen_key = cbm_arena_strndup(&x->arena, key, klen); /* borrowed by `seen` */
    char *k = seen_key ? cbm_arena_strndup(&x->arena, key, klen) : NULL;
    if (!k) {
        return false;
    }
    if (cbm_ht_get(seen, seen_key)) {
        return true; /* the first file of a key wins (files come sorted by path) */
    }
    cbm_ht_set(seen, seen_key, (void *)node);
    /* pieces: '/'- and '.'-separated; the name is the last one */
    for (char *c = k; *c; c++) {
        if (*c == '/') {
            *c = '.';
        }
    }
    char *last = strrchr(k, '.');
    const char *name = last ? last + SKIP_ONE : k;
    if (!name[0]) {
        return true;
    }
    mdr_pkg_t *p = (mdr_pkg_t *)cbm_arena_alloc(&x->arena, sizeof(*p));
    if (!p) {
        return false;
    }
    if (last) {
        *last = '\0';
    }
    p->chain = last ? k : "";
    p->node = node;
    p->file = file;
    p->next = (mdr_pkg_t *)cbm_ht_get(x->pkgs, name);
    cbm_ht_set(x->pkgs, name, p);
    return true;
}

static bool mdr_add_top(mdr_index_t *x, const char *s, size_t n) {
    char *k = mdr_norm_dup(&x->arena, s, n);
    if (!k) {
        return false;
    }
    cbm_ht_set(x->top, k, (void *)k);
    return true;
}

static int mdr_def_cmp(const void *a, const void *b) {
    const mdr_def_t *x = (const mdr_def_t *)a;
    const mdr_def_t *y = (const mdr_def_t *)b;
    int c = strcmp(x->file, y->file);
    if (c != 0) {
        return c;
    }
    if (x->node->start_line != y->node->start_line) {
        return x->node->start_line < y->node->start_line ? -1 : 1;
    }
    return (x->node->id > y->node->id) - (x->node->id < y->node->id);
}

/* Every code definition of the graph, sorted by file. False when memory ran
 * out. */
static bool mdr_build_defs(mdr_index_t *x, const cbm_gbuf_t *graph) {
    /* the ADR records first: a repository of documents alone has records but
     * no code definitions, and its supersedes links still need them */
    const cbm_gbuf_node_t **adrs = NULL;
    int nadr = 0;
    if (cbm_gbuf_find_by_label(graph, "ADR", &adrs, &nadr) == 0) {
        for (int i = 0; i < nadr; i++) {
            if (adrs[i]->file_path && adrs[i]->file_path[0]) {
                cbm_ht_set(x->adr_by_file, adrs[i]->file_path, (void *)adrs[i]);
            }
        }
    }
    int total = 0;
    for (size_t l = 0; MDR_CODE_LABELS[l]; l++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(graph, MDR_CODE_LABELS[l], &nodes, &count) == 0) {
            total += count;
        }
    }
    if (total == 0) {
        return true;
    }
    x->defs = (mdr_def_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)total * sizeof(*x->defs));
    if (!x->defs) {
        return false;
    }
    for (size_t l = 0; MDR_CODE_LABELS[l]; l++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(graph, MDR_CODE_LABELS[l], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count && x->ndefs < total; i++) {
            if (nodes[i]->file_path && nodes[i]->file_path[0]) {
                x->defs[x->ndefs++] = (mdr_def_t){.file = nodes[i]->file_path, .node = nodes[i]};
            }
        }
    }
    qsort(x->defs, (size_t)x->ndefs, sizeof(*x->defs), mdr_def_cmp);
    return true;
}

/* The module index over the graph's code files and directories. */
static bool mdr_build_pkgs(mdr_index_t *x, const cbm_gbuf_t *graph) {
    CBMHashTable *seen = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    if (!seen) {
        return false;
    }
    bool ok = true;
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(graph, "File", &nodes, &count) == 0) {
        for (int i = 0; ok && i < count; i++) {
            const char *fp = nodes[i]->file_path;
            if (!fp || !fp[0] || !mdr_code_file(fp)) {
                continue;
            }
            const char *base = mdr_base(fp);
            const char *dot = strrchr(base, '.');
            size_t stem_len = dot && dot != base ? (size_t)(dot - base) : strlen(base);
            size_t dir_len = base > fp ? (size_t)(base - fp - SKIP_ONE) : 0;
            ok = mdr_add_pkg(x, seen, fp, (size_t)(base - fp) + stem_len, nodes[i], fp);
            if (ok && dir_len > 0 && mdr_in(base, stem_len, MDR_INDEX_STEMS)) {
                ok = mdr_add_pkg(x, seen, fp, dir_len, nodes[i], fp);
            }
            if (ok && dir_len == 0) {
                ok = mdr_add_top(x, base, stem_len);
            }
        }
    }
    if (ok && cbm_gbuf_find_by_label(graph, "Folder", &nodes, &count) == 0) {
        for (int i = 0; ok && i < count; i++) {
            const char *dp = nodes[i]->file_path;
            if (!dp || !dp[0]) {
                continue;
            }
            ok = mdr_add_pkg(x, seen, dp, strlen(dp), nodes[i], NULL);
            if (ok && !strchr(dp, '/')) {
                ok = mdr_add_top(x, dp, strlen(dp));
            }
        }
    }
    cbm_ht_free(seen);
    return ok;
}

static void *mdr_build(const cbm_doclink_build_in_t *in) {
    mdr_index_t *x = (mdr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*x));
    if (!x) {
        return NULL;
    }
    cbm_arena_init(&x->arena);
    x->project = in->ctx ? in->ctx->project_name : NULL;
    x->project_len = x->project ? strlen(x->project) : 0;
    x->run_count = in->run_file_count;
    x->run_paths = in->run_file_count > 0
                       ? (const char **)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                                   (size_t)in->run_file_count * sizeof(char *))
                       : NULL;
    x->basenames = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    x->pkgs = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    x->top = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    x->adr_by_file = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    x->folders = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, MDR_BASENAME_INIT);
    if ((in->run_file_count > 0 && !x->run_paths) || !x->basenames || !x->pkgs || !x->top ||
        !x->adr_by_file || !x->folders || !x->project || !mdr_build_pkgs(x, in->graph) ||
        !mdr_build_defs(x, in->graph)) {
        mdr_destroy(x);
        return NULL;
    }
    for (int i = 0; i < in->file_count; i++) {
        int rf = in->files[i].run_file;
        if (rf >= 0 && rf < x->run_count) {
            x->run_paths[rf] = in->files[i].rel_path; /* valid until destroy */
        }
    }
    const cbm_gbuf_node_t **folders = NULL;
    int nfolders = 0;
    if (cbm_gbuf_find_by_label(in->graph, "Folder", &folders, &nfolders) == 0) {
        for (int i = 0; i < nfolders; i++) {
            if (folders[i]->file_path && folders[i]->file_path[0]) {
                /* the graph outlives the index: its strings may be keys */
                cbm_ht_set(x->folders, folders[i]->file_path, (void *)folders[i]);
            }
        }
    }
    const cbm_gbuf_node_t **files = NULL;
    int nfiles = 0;
    if (cbm_gbuf_find_by_label(in->graph, "File", &files, &nfiles) == 0) {
        for (int i = 0; i < nfiles; i++) {
            if (!files[i]->file_path || !files[i]->file_path[0]) {
                continue;
            }
            const char *base = mdr_base(files[i]->file_path); /* the graph outlives the index */
            uintptr_t n = (uintptr_t)cbm_ht_get(x->basenames, base);
            cbm_ht_set(x->basenames, base, (void *)(n + SKIP_ONE));
        }
    }
    return x;
}

static void mdr_destroy(void *index) {
    mdr_index_t *x = (mdr_index_t *)index;
    if (!x) {
        return;
    }
    cbm_ht_free(x->basenames);
    cbm_ht_free(x->pkgs);
    cbm_ht_free(x->top);
    cbm_ht_free(x->adr_by_file);
    cbm_ht_free(x->folders);
    cbm_arena_destroy(&x->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, x->defs);
    cbm_free(CBM_MEM_CLASS_OTHER, x->run_paths);
    cbm_free(CBM_MEM_CLASS_OTHER, x);
}

/* ── Paths ───────────────────────────────────────────────────────── */

static int mdr_hex(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + CBM_DECIMAL_BASE;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + CBM_DECIMAL_BASE;
    }
    return CBM_NOT_FOUND;
}

/* `base` + "/" + `p`, normalized: "." dropped, ".." pops a segment. False
 * when the path climbs above the root or does not fit. */
static bool mdr_join(const char *base, const char *p, char *out, size_t cap) {
    size_t w = 0;
    const char *parts[PAIR_LEN] = {base, p};
    for (int k = 0; k < PAIR_LEN; k++) {
        const char *s = parts[k];
        while (s && *s) {
            const char *e = strchr(s, '/');
            size_t n = e ? (size_t)(e - s) : strlen(s);
            if (n == 0 || (n == SKIP_ONE && s[0] == '.')) {
                /* empty or "." */
            } else if (n == PAIR_LEN && s[0] == '.' && s[SKIP_ONE] == '.') {
                if (w == 0) {
                    return false;
                }
                while (w > 0 && out[w - SKIP_ONE] != '/') {
                    w--;
                }
                if (w > 0) {
                    w--; /* the slash before the popped segment */
                }
            } else {
                if (w + n + PAIR_LEN > cap) {
                    return false;
                }
                if (w > 0) {
                    out[w++] = '/';
                }
                memcpy(out + w, s, n);
                w += n;
            }
            s = e ? e + SKIP_ONE : NULL;
        }
    }
    out[w] = '\0';
    return true;
}

static const cbm_gbuf_node_t *mdr_folder(const mdr_index_t *x, const cbm_gbuf_t *graph,
                                         const char *path) {
    (void)graph;
    return path[0] ? (const cbm_gbuf_node_t *)cbm_ht_get(x->folders, path) : NULL;
}

static bool mdr_code_label(const char *label) {
    for (size_t i = 0; label && MDR_CODE_LABELS[i]; i++) {
        if (strcmp(label, MDR_CODE_LABELS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The member text's last segment (after "::" or "."), and the one before it. */
static void mdr_member_names(const char *member, const char **name, size_t *name_len,
                             const char **owner, size_t *owner_len) {
    const char *segs[PAIR_LEN] = {NULL, NULL};
    size_t lens[PAIR_LEN] = {0, 0};
    const char *s = member;
    while (*s) {
        size_t n = strcspn(s, ":.");
        if (n > 0) {
            segs[0] = segs[SKIP_ONE];
            lens[0] = lens[SKIP_ONE];
            segs[SKIP_ONE] = s;
            lens[SKIP_ONE] = n;
        }
        s += n;
        while (*s == ':' || *s == '.') {
            s++;
        }
    }
    *name = segs[SKIP_ONE];
    *name_len = lens[SKIP_ONE];
    *owner = segs[0];
    *owner_len = lens[0];
}

/* The QN segment before the node's own name ends with `owner`. */
static bool mdr_owned_by(const cbm_gbuf_node_t *n, const char *owner, size_t owner_len) {
    const char *qn = n->qualified_name;
    size_t ql = qn ? strlen(qn) : 0;
    size_t nl = n->name ? strlen(n->name) : 0;
    if (!owner || ql < nl + owner_len + PAIR_LEN) {
        return false;
    }
    size_t end = ql - nl - SKIP_ONE; /* the '.' before the name */
    if (qn[end] != '.') {
        return false;
    }
    size_t start = end - owner_len;
    return memcmp(qn + start, owner, owner_len) == 0 && (start == 0 || qn[start - SKIP_ONE] == '.');
}

/* The code definitions of `path`: [*lo, *hi) of the index's sorted list. */
static void mdr_file_defs(const mdr_index_t *x, const char *path, int *lo, int *hi) {
    int a = 0;
    int b = x->ndefs;
    while (a < b) {
        int mid = a + ((b - a) / PAIR_LEN);
        if (strcmp(x->defs[mid].file, path) < 0) {
            a = mid + SKIP_ONE;
        } else {
            b = mid;
        }
    }
    int e = a;
    while (e < x->ndefs && strcmp(x->defs[e].file, path) == 0) {
        e++;
    }
    *lo = a;
    *hi = e;
}

/* The code definition of file `path` that the range or the member names, or
 * NULL. Read from the index's definitions by file, never from DEFINES edges:
 * an incremental run's graph holds the edges of the files it re-extracts
 * only, while every node is there. */
static const cbm_gbuf_node_t *mdr_segment(const mdr_index_t *x, const char *path, uint32_t first,
                                          uint32_t last, const char *member) {
    int lo = 0;
    int hi = 0;
    mdr_file_defs(x, path, &lo, &hi);
    const cbm_gbuf_node_t *best = NULL;
    if (first > 0) {
        for (int i = lo; i < hi; i++) {
            const cbm_gbuf_node_t *n = x->defs[i].node;
            if (n->start_line <= 0 || (uint32_t)n->start_line > first ||
                (uint32_t)n->end_line < last) {
                continue;
            }
            int span = n->end_line - n->start_line;
            int best_span = best ? best->end_line - best->start_line : 0;
            if (!best || span < best_span ||
                (span == best_span && (n->start_line > best->start_line ||
                                       (n->start_line == best->start_line && n->id < best->id)))) {
                best = n;
            }
        }
        if (best) {
            return best;
        }
    }
    if (!member) {
        return NULL;
    }
    const char *name;
    const char *owner;
    size_t name_len;
    size_t owner_len;
    mdr_member_names(member, &name, &name_len, &owner, &owner_len);
    if (!name) {
        return NULL;
    }
    const cbm_gbuf_node_t *hit = NULL;
    int hits = 0;
    const cbm_gbuf_node_t *owned = NULL;
    int owned_hits = 0;
    for (int i = lo; i < hi; i++) {
        const cbm_gbuf_node_t *n = x->defs[i].node;
        if (!n->name || strlen(n->name) != name_len || memcmp(n->name, name, name_len) != 0) {
            continue;
        }
        hit = n;
        hits++;
        if (owner && mdr_owned_by(n, owner, owner_len)) {
            owned = n;
            owned_hits++;
        }
    }
    if (owned_hits == SKIP_ONE) {
        return owned;
    }
    return owned_hits == 0 && hits == SKIP_ONE ? hit : NULL;
}

/* Is `path` of a file type the indexer reads? */
static bool mdr_indexable(const char *path) {
    return cbm_language_for_filename(mdr_base(path)) != CBM_LANG_COUNT;
}

/* ── Qualified names ─────────────────────────────────────────────── */

/* Path pieces a written qualifier may leave out (`pkg.Foo` for
 * `pkg/src/Foo`, `pkg.mod.Foo` for `pkg/mod.rs`'s Foo). */
static const char *const MDR_TRANSPARENT[] = {
    "src", "lib", "mod", "__init__", "index", "main", "java", "kotlin", "scala", "groovy", NULL};

/* Directory names and file names of test code. */
static const char *const MDR_TEST_DIRS[] = {"test",
                                            "tests",
                                            "testing",
                                            "__tests__",
                                            "spec",
                                            "specs",
                                            "testdata",
                                            "test_data",
                                            "fixtures",
                                            "e2e",
                                            "integration_tests",
                                            "testutil",
                                            "testutils",
                                            "benchmark",
                                            "benchmarks",
                                            "bench",
                                            "mocks",
                                            "mock",
                                            "fakes",
                                            "fake",
                                            "testlib",
                                            NULL};

static unsigned char mdr_norm_c(unsigned char c) {
    if (c >= 'A' && c <= 'Z') {
        return (unsigned char)(c - 'A' + 'a');
    }
    return c == '-' ? '_' : c;
}

/* Equal after lower-casing and reading '-' as '_'. */
static bool mdr_norm_eq(const char *a, size_t al, const char *b, size_t bl) {
    if (al != bl) {
        return false;
    }
    for (size_t i = 0; i < al; i++) {
        if (mdr_norm_c((unsigned char)a[i]) != mdr_norm_c((unsigned char)b[i])) {
            return false;
        }
    }
    return true;
}

/* s[0..n) ends with `suffix`. */
static bool mdr_ends(const char *s, size_t n, const char *suffix) {
    size_t k = strlen(suffix);
    return n >= k && memcmp(s + n - k, suffix, k) == 0;
}

static bool mdr_in(const char *s, size_t n, const char *const *list) {
    for (size_t i = 0; list[i]; i++) {
        if (mdr_norm_eq(s, n, list[i], strlen(list[i]))) {
            return true;
        }
    }
    return false;
}

/* A test file or a file in a test directory. */
static bool mdr_test_path(const char *path) {
    const char *s = path;
    for (const char *slash = strchr(s, '/'); slash; slash = strchr(s, '/')) {
        if (mdr_in(s, (size_t)(slash - s), MDR_TEST_DIRS)) {
            return true;
        }
        s = slash + SKIP_ONE;
    }
    const char *base = s;
    if (strncmp(base, "test_", strlen("test_")) == 0 || strcmp(base, "conftest.py") == 0) {
        return true;
    }
    const char *dot = strrchr(base, '.');
    if (!dot) {
        return false;
    }
    size_t stem = (size_t)(dot - base);
    const char *ext = dot + SKIP_ONE;
    bool lower_ext = ext[0] != '\0';
    for (const char *e = ext; *e; e++) {
        lower_ext = lower_ext && *e >= 'a' && *e <= 'z';
    }
    static const char *const jvm_like[] = {"java", "kt", "cs", "scala", "groovy", "php", NULL};
    return (lower_ext && (mdr_ends(base, stem, "_test") || mdr_ends(base, stem, ".test") ||
                          mdr_ends(base, stem, ".spec"))) ||
           (strcmp(ext, "rb") == 0 && mdr_ends(base, stem, "_spec")) ||
           (mdr_in(ext, strlen(ext), jvm_like) &&
            (mdr_ends(base, stem, "Test") || mdr_ends(base, stem, "Tests")));
}

typedef struct {
    const char *path; /* as written (no fragment, decoded) */
    uint32_t first;
    uint32_t last;
    const char *member;
    bool name_only; /* a file name alone: the document's directory only */
    bool dir;       /* written as a directory */
    bool link;      /* a link: relative to the document by definition */
} mdr_ref_t;

static void mdr_edge(cbm_doclink_outcome_t *out, const cbm_gbuf_node_t *target, uint32_t first,
                     uint32_t last) {
    out->kind = CBM_DOCLINK_EDGE;
    out->target = target;
    out->exact = true;
    out->target_first = first;
    out->target_last = last;
}

enum { MDR_MAX_PIECES = 80 }; /* a code span is at most 150 bytes */

typedef struct {
    const char *s[MDR_MAX_PIECES];
    size_t n[MDR_MAX_PIECES];
    int count;
} mdr_pieces_t;

/* The '.'-separated pieces of s. False when there are too many. */
static bool mdr_split(const char *s, mdr_pieces_t *p) {
    p->count = 0;
    for (;;) {
        if (p->count == MDR_MAX_PIECES) {
            return false;
        }
        size_t n = strcspn(s, ".");
        p->s[p->count] = s;
        p->n[p->count] = n;
        p->count++;
        if (!s[n]) {
            return true;
        }
        s += n + SKIP_ONE;
    }
}

/* Is the written qualifier q[0..nq) a piece-aligned suffix of `chain` (a
 * node's '.'-joined qualifier pieces), when chain pieces in MDR_TRANSPARENT
 * may be skipped? `exact`: every piece as written; otherwise lower-cased and
 * '-' read as '_' (`rootMulti.Store` names package rootmulti), except that
 * with `types_exact` a capitalized piece (a type) must be written as is
 * (`debug.log` is no member of class Debug). `left`: where the chain left of
 * the match ends. */
static bool mdr_chain_match(const char *chain, size_t clen, const mdr_pieces_t *q, int nq,
                            bool exact, bool types_exact, size_t *left) {
    size_t ce = clen;
    bool more = clen > 0;
    int j = nq - SKIP_ONE;
    while (j >= 0) {
        if (!more) {
            return false;
        }
        size_t cs = ce;
        while (cs > 0 && chain[cs - SKIP_ONE] != '.') {
            cs--;
        }
        const char *piece = chain + cs;
        size_t pl = ce - cs;
        bool as_written = pl == q->n[j] && memcmp(piece, q->s[j], pl) == 0;
        bool same = exact ? as_written : mdr_norm_eq(piece, pl, q->s[j], q->n[j]);
        if (same && !as_written && types_exact && piece[0] >= 'A' && piece[0] <= 'Z') {
            same = false; /* a type is named in its own case */
        }
        if (same) {
            j--;
        } else if (!mdr_in(piece, pl, MDR_TRANSPARENT)) {
            return false;
        }
        more = cs > 0;
        ce = more ? cs - SKIP_ONE : 0;
    }
    if (left) {
        *left = more ? ce : 0;
    }
    return true;
}

/* Reverse-domain roots of JVM packages: a name written from one of them is
 * absolute (`com.google.protobuf.Message`), never the tail of a relocated
 * copy (`org.apache.hadoop.hbase.shaded.com.google.protobuf.Message`). */
static const char *const MDR_JVM_ROOTS[] = {"com",      "org",    "net",     "io",      "edu",
                                            "gov",      "java",   "javax",   "jakarta", "android",
                                            "androidx", "kotlin", "kotlinx", NULL};

/* After a match that left chain[0, left): the written name starts the chain
 * or follows a source-root piece (`src/main/java`), so it is the package as
 * declared, not a relocated copy's tail. */
static bool mdr_rooted(const char *chain, size_t left) {
    if (left == 0) {
        return true;
    }
    size_t s = left;
    while (s > 0 && chain[s - SKIP_ONE] != '.') {
        s--;
    }
    return mdr_in(chain + s, left - s, MDR_TRANSPARENT);
}

static bool mdr_ext_in(const char *path, const char *const *exts) {
    const char *dot = strrchr(path, '.');
    return dot && !strchr(dot, '/') && mdr_in(dot + SKIP_ONE, strlen(dot + SKIP_ONE), exts);
}

/* Languages whose types are written in their own case (`Debug`, never `debug`). */
static bool mdr_cased_types_file(const char *path) {
    static const char *const exts[] = {"java", "kt", "kts", "scala", "groovy", "cs", NULL};
    return mdr_ext_in(path, exts);
}

static bool mdr_jvm_file(const char *path) {
    static const char *const exts[] = {"java", "kt", "kts", "scala", "groovy", NULL};
    return mdr_ext_in(path, exts);
}

static bool mdr_c_family_file(const char *path) {
    static const char *const exts[] = {"c",  "h",   "cc",  "cpp", "cxx", "hpp",
                                       "hh", "hxx", "ipp", "inl", "tpp", NULL};
    return mdr_ext_in(path, exts);
}

/* Is `dir` a JVM package folder: is the first code definition at or below it
 * in a Java/Kotlin/Scala/Groovy file? A dotted name in documentation that
 * matches such a folder is far more often a configuration key, an OSGi or JMX
 * id or a Maven coordinate than the package (`snapshot.mode` of
 * io/debezium/snapshot/mode), so a code name never binds one; a Python
 * package is what its dotted name says and keeps binding. */
static bool mdr_jvm_folder(const mdr_index_t *x, const char *dir) {
    char prefix[MDR_PATH_CAP];
    int n = snprintf(prefix, sizeof(prefix), "%s/", dir);
    if (n < 0 || (size_t)n >= sizeof(prefix)) {
        return false;
    }
    int a = 0;
    int b = x->ndefs;
    while (a < b) {
        int mid = a + ((b - a) / PAIR_LEN);
        if (strcmp(x->defs[mid].file, prefix) < 0) {
            a = mid + SKIP_ONE;
        } else {
            b = mid;
        }
    }
    return a < x->ndefs && strncmp(x->defs[a].file, prefix, (size_t)n) == 0 &&
           mdr_jvm_file(x->defs[a].file);
}

/* A repository can carry several copies of one project (snapshots side by
 * side, a vendored copy). A definition of another copy never answers the
 * document: below the directory where the document's path and the file's part,
 * the document's own branch holds the same rest of the path, at least two
 * directories deep, as a file with definitions. A shared file name alone
 * (`__init__.py` of two packages) is no copy, and different modules of one
 * build hold different paths there, so their links stay. */
static bool mdr_other_copy(const mdr_index_t *x, const char *doc, const char *file) {
    enum { MIN_SHARED_SLASHES = 3 }; /* "/dir/dir/file" */
    size_t common = 0;
    for (size_t i = 0; doc[i] && doc[i] == file[i]; i++) {
        if (doc[i] == '/') {
            common = i + SKIP_ONE;
        }
    }
    const char *dslash = strchr(doc + common, '/');
    const char *fslash = strchr(file + common, '/');
    if (!dslash || !fslash) {
        return false;
    }
    int slashes = 0;
    for (const char *c = fslash; *c; c++) {
        slashes += *c == '/';
    }
    if (slashes < MIN_SHARED_SLASHES) {
        return false;
    }
    char twin[MDR_PATH_CAP];
    int n = snprintf(twin, sizeof(twin), "%.*s%s", (int)(dslash - doc), doc, fslash);
    if (n < 0 || (size_t)n >= sizeof(twin)) {
        return false;
    }
    int lo = 0;
    int hi = 0;
    mdr_file_defs(x, twin, &lo, &hi);
    return hi > lo;
}

/* The node's qualifier chain (its QN without the project and its own name);
 * false when the QN does not have that shape. */
static bool mdr_node_chain(const mdr_index_t *x, const cbm_gbuf_node_t *n, const char **chain,
                           size_t *clen) {
    const char *qn = n->qualified_name;
    size_t name_len = n->name ? strlen(n->name) : 0;
    if (!qn || name_len == 0 || strncmp(qn, x->project, x->project_len) != 0 ||
        qn[x->project_len] != '.') {
        return false;
    }
    const char *rest = qn + x->project_len + SKIP_ONE;
    size_t rl = strlen(rest);
    if (rl < name_len || strcmp(rest + rl - name_len, n->name) != 0) {
        return false;
    }
    if (rl == name_len) {
        *chain = rest;
        *clen = 0;
        return true;
    }
    if (rest[rl - name_len - SKIP_ONE] != '.') {
        return false;
    }
    *chain = rest;
    *clen = rl - name_len - SKIP_ONE;
    return true;
}

static bool mdr_python_file(const char *path) {
    return mdr_ends(path, strlen(path), ".py") || mdr_ends(path, strlen(path), ".pyi");
}

static bool mdr_classlike(const char *label) {
    static const char *const labels[] = {"Class", "Struct", "Interface", "Trait",
                                         "Enum",  "Type",   NULL};
    for (size_t i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Does the repository itself define the written root (`fastapi` of
 * `fastapi.routing.X`): a top-level directory or code file, or a type? */
static bool mdr_root_known(const mdr_index_t *x, const cbm_gbuf_t *graph, const char *root,
                           size_t len) {
    char key[MDR_PATH_CAP];
    if (len >= sizeof(key)) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        key[i] = (char)mdr_norm_c((unsigned char)root[i]);
    }
    key[len] = '\0';
    if (cbm_ht_get(x->top, key)) {
        return true;
    }
    memcpy(key, root, len);
    key[len] = '\0';
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(graph, key, &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            if (mdr_classlike(nodes[i]->label) && nodes[i]->file_path &&
                mdr_code_file(nodes[i]->file_path)) {
                return true;
            }
        }
    }
    return false;
}

/* The one non-member definition named `name` in the module file `file`. */
static const cbm_gbuf_node_t *mdr_module_item(const mdr_index_t *x, const char *file,
                                              const char *name) {
    int lo = 0;
    int hi = 0;
    mdr_file_defs(x, file, &lo, &hi);
    const cbm_gbuf_node_t *hit = NULL;
    for (int i = lo; i < hi; i++) {
        const cbm_gbuf_node_t *n = x->defs[i].node;
        if (!n->name || strcmp(n->name, name) != 0 || strcmp(n->label, "Method") == 0 ||
            strcmp(n->label, "Field") == 0) {
            continue;
        }
        if (hit) {
            return NULL;
        }
        hit = n;
    }
    return hit;
}

/* Candidates of one class: the first one, and how many (counting stops at
 * two: one is an edge, two are ambiguous). */
typedef struct {
    const cbm_gbuf_node_t *node;
    const mdr_pkg_t *pkg;
    int hits;
} mdr_tally_t;

static void mdr_tally(mdr_tally_t *t, const cbm_gbuf_node_t *node, const mdr_pkg_t *pkg) {
    if (t->hits == 0) {
        t->node = node;
        t->pkg = pkg;
    }
    if (t->hits < PAIR_LEN) {
        t->hits++;
    }
}

/* A qualified name written in a code span: EXACT only. The one code
 * definition named like its last piece whose qualifier chain ends with the
 * written qualifier, or the one module or package so named. A match with
 * every piece as written outranks one that needs case folding, so
 * `fastapi.middleware` names the package fastapi/middleware, not the method
 * FastAPI.middleware. Order: definitions as written, modules as written,
 * definitions folded, modules folded; the first class with a candidate
 * decides. */
static void mdr_resolve_name(const mdr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                             const CBMDocLinkMdPath *p, cbm_doclink_outcome_t *out) {
    mdr_pieces_t q;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
    if (!mdr_split(p->path, &q) || q.count < PAIR_LEN) {
        return;
    }
    int nq = q.count - SKIP_ONE; /* the qualifier pieces */
    char name[MDR_PATH_CAP];
    size_t name_len = q.n[nq];
    if (name_len >= sizeof(name)) {
        return;
    }
    memcpy(name, q.s[nq], name_len);
    name[name_len] = '\0';
    bool doc_test = mdr_test_path(doc);
    /* `std::hash` is the standard library's, never a C/C++ definition of this
     * repository (a specialization of it, a header named like it) */
    bool std_root = q.n[0] == strlen("std") && memcmp(q.s[0], "std", q.n[0]) == 0;
    bool jvm_root = mdr_in(q.s[0], q.n[0], MDR_JVM_ROOTS);
    enum { DEF_EXACT, MOD_EXACT, DEF_FOLDED, MOD_FOLDED, TALLIES };
    mdr_tally_t tally[TALLIES] = {{0}};
    int test_hits = 0;
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(graph, name, &nodes, &count) != 0) {
        count = 0;
    }
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        const char *chain = NULL;
        size_t clen = 0;
        if (!mdr_code_label(n->label) || !n->file_path || !mdr_code_file(n->file_path) ||
            !mdr_node_chain(x, n, &chain, &clen)) {
            continue;
        }
        if (p->colon && !mdr_python_file(n->file_path)) {
            continue; /* `module:attr` is Python's form */
        }
        if (strcmp(n->label, "Field") == 0) {
            /* a field only through its type, written as the type is named:
             * never through an instance (`self.x`, `app.state`) */
            const char *owner = chain + clen;
            while (owner > chain && owner[-SKIP_ONE] != '.') {
                owner--;
            }
            size_t ol = (size_t)(chain + clen - owner);
            if (p->instance || ol != q.n[nq - SKIP_ONE] ||
                memcmp(owner, q.s[nq - SKIP_ONE], ol) != 0) {
                continue;
            }
        }
        size_t left = 0;
        bool exact = mdr_chain_match(chain, clen, &q, nq, true, false, &left);
        bool folded = !exact && mdr_chain_match(chain, clen, &q, nq, false, false, &left);
        if (!exact && !folded) {
            continue;
        }
        /* The name-scope rules only ever take a link away: a refused candidate
         * still counts (two candidates stay ambiguous) but is never linked. */
        bool refused = (std_root && mdr_c_family_file(n->file_path)) ||
                       (folded && mdr_cased_types_file(n->file_path) &&
                        !mdr_chain_match(chain, clen, &q, nq, false, true, NULL)) ||
                       (jvm_root && mdr_jvm_file(n->file_path) && !mdr_rooted(chain, left)) ||
                       mdr_other_copy(x, doc, n->file_path);
        if (!doc_test && mdr_test_path(n->file_path)) {
            test_hits++;
            continue;
        }
        mdr_tally(&tally[exact ? DEF_EXACT : DEF_FOLDED], refused ? NULL : n, NULL);
    }
    /* modules and packages so named */
    for (const mdr_pkg_t *m = (const mdr_pkg_t *)cbm_ht_get(x->pkgs, name); m; m = m->next) {
        size_t cl = strlen(m->chain);
        const char *mfile = m->file ? m->file : m->node->file_path;
        size_t left = 0;
        bool exact = mdr_chain_match(m->chain, cl, &q, nq, true, false, &left);
        if (!exact && !mdr_chain_match(m->chain, cl, &q, nq, false, false, &left)) {
            continue;
        }
        bool refused =
            mfile && ((std_root && mdr_c_family_file(mfile)) ||
                      (jvm_root && mdr_jvm_file(mfile) && !mdr_rooted(m->chain, left)) ||
                      mdr_other_copy(x, doc, mfile) ||
                      (!m->file && m->node->label && strcmp(m->node->label, "Folder") == 0 &&
                       mdr_jvm_folder(x, mfile)));
        if (!doc_test && mdr_test_path(m->file ? m->file : m->node->file_path)) {
            test_hits++;
            continue;
        }
        if (refused) {
            mdr_tally(&tally[exact ? MOD_EXACT : MOD_FOLDED], NULL, NULL);
        } else {
            mdr_tally(&tally[exact ? MOD_EXACT : MOD_FOLDED], m->node, m);
        }
    }
    for (int k = 0; k < TALLIES; k++) {
        if (tally[k].hits == 0) {
            continue;
        }
        if (tally[k].hits > SKIP_ONE) {
            out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
            return;
        }
        if (!tally[k].node) {
            break; /* its one candidate is refused: no link, and no later class decides */
        }
        const mdr_pkg_t *mod = tally[k].pkg;
        const cbm_gbuf_node_t *item = mod && mod->file ? mdr_module_item(x, mod->file, name) : NULL;
        mdr_edge(out, item ? item : tally[k].node, 0, 0);
        return;
    }
    if (test_hits > 0) {
        out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
        return;
    }
    if (mdr_root_known(x, graph, q.s[0], q.n[0])) {
        out->reason = CBM_DOCLINK_REASON_MISSING; /* the repository's name, but nothing so named */
        return;
    }
    out->kind = CBM_DOCLINK_LOCAL; /* code of another project (a library, the standard one) */
}

static void mdr_resolve_path(const mdr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                             const mdr_ref_t *r, cbm_doclink_outcome_t *out) {
    const char *p = r->path;
    bool rooted = p[0] == '/';
    bool relative = strncmp(p, "./", PAIR_LEN) == 0 || strncmp(p, "../", PAIR_LEN + 1) == 0;
    char doc_dir[MDR_PATH_CAP];
    const char *slash = strrchr(doc, '/');
    size_t dl = slash ? (size_t)(slash - doc) : 0;
    if (dl >= sizeof(doc_dir)) {
        out->kind = CBM_DOCLINK_UNRESOLVED;
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    memcpy(doc_dir, doc, dl);
    doc_dir[dl] = '\0';
    char tries[MDR_TRIES][MDR_PATH_CAP];
    int nt = 0;
    int attempted = 0;
    if (!rooted) {
        attempted++;
        nt += mdr_join(doc_dir, p, tries[nt], MDR_PATH_CAP);
    }
    if (!relative && !r->name_only) {
        attempted++;
        const char *q = p;
        while (*q == '/') {
            q++;
        }
        if (mdr_join("", q, tries[nt], MDR_PATH_CAP) && (nt == 0 || strcmp(tries[0], tries[nt]))) {
            nt++;
        }
    }
    for (int i = 0; i < nt; i++) {
        const char *t = tries[i];
        if (!t[0]) {
            continue; /* the repository root itself */
        }
        const cbm_gbuf_node_t *file = cbm_pipeline_file_node(graph, x->project, t);
        if (file) {
            if (strcmp(t, doc) == 0) {
                out->kind = CBM_DOCLINK_LOCAL; /* a link into the same document */
                return;
            }
            const cbm_gbuf_node_t *seg = (r->first > 0 || r->member)
                                             ? mdr_segment(x, t, r->first, r->last, r->member)
                                             : NULL;
            mdr_edge(out, seg ? seg : file, r->first, r->last);
            return;
        }
        const cbm_gbuf_node_t *folder = mdr_folder(x, graph, t);
        if (folder) {
            mdr_edge(out, folder, 0, 0);
            return;
        }
    }
    out->kind = CBM_DOCLINK_UNRESOLVED;
    if ((attempted > 0 && nt == 0) || (!r->dir && !mdr_indexable(p))) {
        /* above the repository root; a file or a page the indexer does not read */
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    if (r->name_only && !r->link && cbm_ht_get(x->basenames, mdr_base(p))) {
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        return;
    }
    /* doc rot when the reference points into the repository: its directory is
     * there (a bare name in a link: the document's own directory) */
    for (int i = 0; i < nt; i++) {
        char parent[MDR_PATH_CAP];
        const char *ps = strrchr(tries[i], '/');
        size_t pl = ps ? (size_t)(ps - tries[i]) : 0;
        memcpy(parent, tries[i], pl);
        parent[pl] = '\0';
        if (pl > 0 && mdr_folder(x, graph, parent) && (strchr(p, '/') || r->link)) {
            out->reason = CBM_DOCLINK_REASON_MISSING;
            return;
        }
        if (pl == 0 && r->link && !rooted) {
            out->reason = CBM_DOCLINK_REASON_MISSING;
            return;
        }
    }
    out->kind = CBM_DOCLINK_LOCAL; /* not in this repository, nor its directory */
}

/* A link destination: `<...>` dropped, %XX decoded, `#L` fragment read, query
 * dropped. False when nothing of a path is left (an anchor or a query). */
static bool mdr_link_path(const char *raw, char *buf, size_t cap, mdr_ref_t *r) {
    size_t n = strlen(raw);
    while (n > 0 && (raw[0] == '<' || raw[0] == '>')) {
        raw++;
        n--;
    }
    while (n > 0 && (raw[n - SKIP_ONE] == '<' || raw[n - SKIP_ONE] == '>')) {
        n--;
    }
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        if (w + SKIP_ONE >= cap) {
            return false;
        }
        int h1 = i + PAIR_LEN < n && raw[i] == '%' ? mdr_hex(raw[i + SKIP_ONE]) : CBM_NOT_FOUND;
        int h2 = h1 >= 0 ? mdr_hex(raw[i + PAIR_LEN]) : CBM_NOT_FOUND;
        if (h1 >= 0 && h2 >= 0 && (h1 * MDR_HEX_BASE) + h2 != 0) {
            buf[w++] = (char)((h1 * MDR_HEX_BASE) + h2);
            i += MDR_PCT_LEN - SKIP_ONE;
        } else {
            buf[w++] = raw[i];
        }
    }
    buf[w] = '\0';
    char *hash = strchr(buf, '#');
    if (hash) {
        *hash = '\0';
        (void)cbm_doclink_md_line_fragment(hash + SKIP_ONE, &r->first, &r->last);
    }
    char *query = strchr(buf, '?');
    if (query) {
        *query = '\0';
    }
    r->path = buf;
    r->dir = buf[0] && buf[strlen(buf) - SKIP_ONE] == '/';
    r->name_only = !strchr(buf, '/');
    return buf[0] != '\0';
}

/* `ADR-12`, `ADR 012`, `adr_7` (the whole text): the canonical node name. */
static bool mdr_adr_id(const char *raw, char *name, size_t cap) {
    size_t n = strlen(raw);
    if (n < PAIR_LEN * PAIR_LEN || mdr_norm_c((unsigned char)raw[0]) != 'a' ||
        mdr_norm_c((unsigned char)raw[SKIP_ONE]) != 'd' ||
        mdr_norm_c((unsigned char)raw[PAIR_LEN]) != 'r' ||
        (raw[PAIR_LEN + SKIP_ONE] != '-' && raw[PAIR_LEN + SKIP_ONE] != '_' &&
         raw[PAIR_LEN + SKIP_ONE] != ' ')) {
        return false;
    }
    unsigned v = 0;
    size_t digits = 0;
    for (size_t i = PAIR_LEN * PAIR_LEN; i < n; i++) {
        if (raw[i] < '0' || raw[i] > '9' || digits >= MDR_ADR_DIGITS) {
            return false;
        }
        v = (v * CBM_DECIMAL_BASE) + (unsigned)(raw[i] - '0');
        digits++;
    }
    return digits > 0 && snprintf(name, cap, "ADR-%u", v) > 0;
}

/* An ADR's own "supersedes X": X by its id, or the file a link names, to its
 * ADR node. */
static void mdr_resolve_adr(const mdr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                            const char *raw, cbm_doclink_outcome_t *out) {
    char name[MDR_PATH_CAP];
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    if (mdr_adr_id(raw, name, sizeof(name))) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        const cbm_gbuf_node_t *hit = NULL;
        int hits = 0;
        if (cbm_gbuf_find_by_name(graph, name, &nodes, &count) == 0) {
            for (int i = 0; i < count; i++) {
                if (nodes[i]->label && strcmp(nodes[i]->label, "ADR") == 0) {
                    hit = nodes[i];
                    hits++;
                }
            }
        }
        if (hits == SKIP_ONE) {
            mdr_edge(out, hit, 0, 0);
        } else if (hits > SKIP_ONE) {
            out->reason = CBM_DOCLINK_REASON_AMBIGUOUS; /* two ADR logs number alike */
        }
        return;
    }
    char buf[MDR_PATH_CAP];
    mdr_ref_t r = {.link = true};
    if (!mdr_link_path(raw, buf, sizeof(buf), &r)) {
        return;
    }
    mdr_resolve_path(x, graph, doc, &r, out);
    if (out->kind != CBM_DOCLINK_EDGE) {
        return; /* missing, ambiguous, or no file of this repository */
    }
    const cbm_gbuf_node_t *adr =
        out->target->file_path
            ? (const cbm_gbuf_node_t *)cbm_ht_get(x->adr_by_file, out->target->file_path)
            : NULL;
    if (!adr) {
        out->kind = CBM_DOCLINK_UNRESOLVED; /* the file is there, but it is no record */
        out->reason = CBM_DOCLINK_REASON_MISSING;
        return;
    }
    mdr_edge(out, adr, 0, 0);
}

static void mdr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                        const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state;
    const mdr_index_t *x = (const mdr_index_t *)index;
    const char *doc = (run_file >= 0 && run_file < x->run_count) ? x->run_paths[run_file] : NULL;
    if (!doc || !link->raw) {
        out->kind = CBM_DOCLINK_UNRESOLVED;
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    char buf[MDR_PATH_CAP];
    mdr_ref_t r = {0};
    switch (link->syntax) {
    case CBM_DOCLINK_MD_SUPERSEDES:
    case CBM_DOCLINK_MD_SUPERSEDES_PROSE:
        mdr_resolve_adr(x, graph, doc, link->raw, out);
        return;
    case CBM_DOCLINK_MD_LINK:
        r.link = true;
        if (!mdr_link_path(link->raw, buf, sizeof(buf), &r)) {
            out->kind = CBM_DOCLINK_LOCAL; /* an anchor, a query: nothing of a path */
            return;
        }
        break;
    case CBM_DOCLINK_MD_PATH: {
        size_t n = strlen(link->raw);
        if (n >= sizeof(buf)) {
            break; /* longer than any path looked up: unparseable */
        }
        memcpy(buf, link->raw, n + SKIP_ONE);
        r.path = buf;
        break;
    }
    case CBM_DOCLINK_MD_CODE_NAME: {
        CBMDocLinkMdPath p;
        if (cbm_doclink_md_classify_span(link->raw, strlen(link->raw), buf, sizeof(buf), &p) &&
            p.shape == CBM_DOCLINK_MD_QUALIFIED) {
            mdr_resolve_name(x, graph, doc, &p, out);
            return;
        }
        break;
    }
    case CBM_DOCLINK_MD_BARE_PATH:
    case CBM_DOCLINK_MD_CODE_PATH: {
        CBMDocLinkMdPath p;
        if (!cbm_doclink_md_classify_span(link->raw, strlen(link->raw), buf, sizeof(buf), &p) ||
            p.shape == CBM_DOCLINK_MD_QUALIFIED) {
            break;
        }
        r.path = p.path;
        r.member = p.member;
        r.first = p.first_line;
        r.last = p.last_line;
        r.name_only = p.shape == CBM_DOCLINK_MD_FILENAME;
        r.dir = p.shape == CBM_DOCLINK_MD_DIR;
        if (r.dir && r.path[0] == '/' && !strchr(r.path + SKIP_ONE, '/')) {
            /* `/docs`, `/health`: one rooted segment is a URL route, not the
             * directory of that name (`/x/bank` stays a path) */
            out->kind = CBM_DOCLINK_LOCAL;
            return;
        }
        break;
    }
    default:
        break;
    }
    if (!r.path || !r.path[0]) {
        out->kind = CBM_DOCLINK_UNRESOLVED;
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    mdr_resolve_path(x, graph, doc, &r, out);
}

/* ── Shared with the other document resolvers (doc_links.h) ──────── */

const cbm_gbuf_node_t *cbm_doclink_md_segment(const void *md_index, const char *path,
                                              uint32_t first, uint32_t last, const char *member) {
    return md_index ? mdr_segment((const mdr_index_t *)md_index, path, first, last, member) : NULL;
}

/* ── A region of a file's text (the field test's H8 bind_range) ─────── */

/* A blank or comment-only line (H8 _skippable). */
static bool mdr_skippable(const char *s, size_t n) {
    size_t i = 0;
    while (i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r')) {
        i++;
    }
    if (i == n) {
        return true;
    }
    const char *t = s + i;
    size_t r = n - i;
    if ((r >= PAIR_LEN && t[0] == '/' && (t[1] == '/' || t[1] == '*')) || t[0] == '*' ||
        t[0] == ';' || t[0] == '%' || (r >= PAIR_LEN && t[0] == '\'' && t[1] == ' ') ||
        (r >= PAIR_LEN && t[0] == '-' && t[1] == '-' && r > PAIR_LEN &&
         (t[PAIR_LEN] == ' ' || t[PAIR_LEN] == '\t')) ||
        (r >= strlen("<!--") && memcmp(t, "<!--", strlen("<!--")) == 0)) {
        return true;
    }
    return t[0] == '#' && !(r >= PAIR_LEN && (t[1] == '[' || t[1] == '!'));
}

static bool mdr_annotation(const char *s, size_t n) {
    size_t i = 0;
    while (i < n && (s[i] == ' ' || s[i] == '\t')) {
        i++;
    }
    return i < n && (s[i] == '@' || (i + SKIP_ONE < n && s[i] == '#' && s[i + SKIP_ONE] == '['));
}

static bool mdr_container(const char *label) {
    static const char *const labels[] = {"Function", "Method", "Class", "Struct", "Interface",
                                         "Enum",     "Type",   "Trait", "Record", NULL};
    for (int i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

static int mdr_label_prio(const char *label) {
    if (!label) {
        return MDR_PRIO_OTHER;
    }
    if (mdr_classlike(label) && strcmp(label, "Type") != 0) {
        return 0;
    }
    if (strcmp(label, "Type") == 0) {
        return SKIP_ONE;
    }
    if (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0) {
        return PAIR_LEN;
    }
    return strcmp(label, "Field") == 0 ? MDR_PRIO_FIELD : MDR_PRIO_OTHER;
}

/* (s, -e, label priority, id): the order H8 picks maximal nodes in. */
static int mdr_region_cmp(const void *a, const void *b) {
    const cbm_gbuf_node_t *x = *(const cbm_gbuf_node_t *const *)a;
    const cbm_gbuf_node_t *y = *(const cbm_gbuf_node_t *const *)b;
    if (x->start_line != y->start_line) {
        return x->start_line < y->start_line ? -1 : 1;
    }
    if (x->end_line != y->end_line) {
        return x->end_line > y->end_line ? -1 : 1;
    }
    int px = mdr_label_prio(x->label);
    int py = mdr_label_prio(y->label);
    if (px != py) {
        return px < py ? -1 : 1;
    }
    return (x->id > y->id) - (x->id < y->id);
}

/* Do the maximal nodes cover every line of [a, b] that is not blank or a
 * comment (an annotation line counts when it sits right above a covered
 * node, H8's exact-cover)? */
static bool mdr_exact_cover(const cbm_gbuf_node_t *const *max, int nmax, const size_t *starts,
                            const char *text, size_t text_len, uint32_t a, uint32_t b) {
    for (uint32_t ln = a; ln <= b; ln++) {
        bool covered = false;
        for (int k = 0; k < nmax && !covered; k++) {
            covered = (uint32_t)max[k]->start_line <= ln && (uint32_t)max[k]->end_line >= ln;
        }
        size_t s0 = starts[ln - SKIP_ONE];
        size_t s1 = starts[ln] > s0 ? starts[ln] - SKIP_ONE : s0;
        if (covered || s0 >= text_len || mdr_skippable(text + s0, s1 - s0)) {
            continue;
        }
        if (!mdr_annotation(text + s0, s1 - s0)) {
            return false;
        }
        /* annotations, blanks and comments down to a covered node's first line */
        uint32_t nxt = ln + SKIP_ONE;
        while (nxt <= b) {
            size_t t0 = starts[nxt - SKIP_ONE];
            size_t t1 = starts[nxt] > t0 ? starts[nxt] - SKIP_ONE : t0;
            if (!mdr_skippable(text + t0, t1 - t0) && !mdr_annotation(text + t0, t1 - t0)) {
                break;
            }
            nxt++;
        }
        bool starts_node = false;
        for (int k = 0; k < nmax && !starts_node; k++) {
            starts_node = (uint32_t)max[k]->start_line <= nxt && (uint32_t)max[k]->end_line >= nxt;
        }
        if (!starts_node) {
            return false;
        }
    }
    return true;
}

const cbm_gbuf_node_t *cbm_doclink_md_region(const void *md_index, const char *path,
                                             const char *text, size_t text_len, uint32_t first,
                                             uint32_t last) {
    const mdr_index_t *x = (const mdr_index_t *)md_index;
    if (!x || !text || first == 0 || last < first) {
        return NULL;
    }
    /* line starts: starts[k] is the offset of line k + 1; one past the end */
    uint32_t nlines = SKIP_ONE;
    for (size_t i = 0; i < text_len; i++) {
        nlines += text[i] == '\n';
    }
    if (last > nlines) {
        last = nlines;
    }
    if (first > last) {
        return NULL;
    }
    size_t *starts =
        (size_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, ((size_t)nlines + SKIP_ONE) * sizeof(size_t));
    if (!starts) {
        return NULL;
    }
    uint32_t k = 0;
    starts[k++] = 0;
    for (size_t i = 0; i < text_len; i++) {
        if (text[i] == '\n') {
            starts[k++] = i + SKIP_ONE;
        }
    }
    starts[k] = text_len + SKIP_ONE;
    /* edge blank and comment lines trimmed */
    uint32_t a = first;
    uint32_t b = last;
    while (a <= b && mdr_skippable(text + starts[a - SKIP_ONE],
                                   starts[a] - SKIP_ONE - starts[a - SKIP_ONE])) {
        a++;
    }
    while (b >= a && mdr_skippable(text + starts[b - SKIP_ONE],
                                   starts[b] - SKIP_ONE - starts[b - SKIP_ONE])) {
        b--;
    }
    if (a > b) {
        a = first;
        b = last;
    }
    int lo = 0;
    int hi = 0;
    mdr_file_defs(x, path, &lo, &hi);
    const cbm_gbuf_node_t *enc = NULL;
    for (int i = lo; i < hi; i++) {
        const cbm_gbuf_node_t *n = x->defs[i].node;
        if (!mdr_container(n->label) || n->start_line <= 0 || (uint32_t)n->start_line > a ||
            (uint32_t)n->end_line < b) {
            continue;
        }
        int span = n->end_line - n->start_line;
        int best = enc ? enc->end_line - enc->start_line : 0;
        if (!enc || span < best ||
            (span == best && (n->start_line > enc->start_line ||
                              (n->start_line == enc->start_line &&
                               mdr_label_prio(n->label) < mdr_label_prio(enc->label))))) {
            enc = n;
        }
    }
    const cbm_gbuf_node_t **inside = (const cbm_gbuf_node_t **)cbm_alloc(
        CBM_MEM_CLASS_OTHER, (size_t)(hi - lo + SKIP_ONE) * sizeof(*inside));
    const cbm_gbuf_node_t *pick = NULL;
    if (inside) {
        int ni = 0;
        for (int i = lo; i < hi; i++) {
            const cbm_gbuf_node_t *n = x->defs[i].node;
            if (n == enc || n->start_line <= 0 || (uint32_t)n->start_line < a ||
                (uint32_t)n->end_line > b ||
                (enc && (n->start_line < enc->start_line || n->end_line > enc->end_line))) {
                continue;
            }
            inside[ni++] = n;
        }
        qsort(inside, (size_t)ni, sizeof(*inside), mdr_region_cmp);
        int nmax = 0; /* maximal ones, compacted in place */
        for (int i = 0; i < ni; i++) {
            bool nested = false;
            for (int m = 0; m < nmax && !nested; m++) {
                nested = inside[m]->start_line <= inside[i]->start_line &&
                         inside[i]->end_line <= inside[m]->end_line;
            }
            if (!nested) {
                inside[nmax++] = inside[i];
            }
        }
        bool type_enc = !enc || (mdr_classlike(enc->label));
        bool exact =
            nmax > 0 && type_enc && mdr_exact_cover(inside, nmax, starts, text, text_len, a, b);
        if (exact && enc) {
            pick = nmax == SKIP_ONE ? inside[0] : enc; /* several: the type holding them */
        } else if (enc) {
            pick = enc;
        } else if (nmax == SKIP_ONE) {
            pick = inside[0];
        }
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)inside);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, starts);
    return pick;
}

const cbm_gbuf_node_t *cbm_doclink_md_folder(const void *md_index, const char *path) {
    return md_index && path ? mdr_folder((const mdr_index_t *)md_index, NULL, path) : NULL;
}

bool cbm_doclink_md_test_path(const char *path) {
    return path && mdr_test_path(path);
}

const cbm_doclink_resolver_t cbm_doclink_md_resolver = {
    .langs = {CBM_LANG_MARKDOWN},
    .lang_count = 1,
    .scope_tag = NULL,
    .build = mdr_build,
    .destroy = mdr_destroy,
    .resolve = mdr_resolve,
    .via = "markdown",
};
