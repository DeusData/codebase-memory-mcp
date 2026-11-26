/*
 * doc_links_go.c — Go doc-link resolution for doc_links.h.
 *
 * Extraction (internal/cbm/doclink_go.c) emits every bracketed text of a doc
 * comment that go/doc/comment COULD take for a doc link. This file makes the
 * rest of go/doc's decision, which needs the whole package, and then binds:
 *
 *   [Name], [Recv.Name]       the documented file's package. A name the
 *                             package does not declare is plain text for
 *                             go/doc: CBM_DOCLINK_LOCAL, neither edge nor row.
 *                             So is the documented declaration's own type
 *                             parameter. Recv.Name is a method declared with
 *                             that receiver, a method of that interface, or
 *                             a field of that struct (the field is a
 *                             deliberate difference from go/doc: see
 *                             go_bind_member).
 *   [pkg], [pkg.Name],        pkg with a '/' is an import path. Otherwise, in
 *   [pkg.Recv.Name]           order: the documented FILE's import of that
 *                             name; the package's own name; a standard-library
 *                             package with that one-element path. None of
 *                             them: plain text (see go_lookup_qualifier for
 *                             the one place this is stricter than go/doc).
 *   import path               a module of the repository (every go.mod, the
 *                             longest module path first) -> its directory's
 *                             package; no package there: missing. No module:
 *                             external. An import without a name is called by
 *                             the real package name of an in-repo directory,
 *                             else by go/doc's assumed name.
 *   member through an alias   `type X = pkg.Y`: [p.X.Member] is looked up on Y.
 *
 * The members of a type are the methods declared with that receiver, the
 * fields of its own struct declaration and the methods of its own interface
 * declaration: nothing promoted through embedding.
 *
 * Binding. The graph keeps one node per qualified name, and <dir> is the
 * directory, not the package clause. Go method nodes are receiver-qualified
 * (<dir>.<Recv>.<Method>, #1913) and are found through their receiver only.
 * Functions, types and values of one name, in every file of the directory,
 * share one node <dir>.<name>, and the node is ONE of those
 * declarations (cbm_gbuf_upsert_node keeps the smallest file path, then the
 * last line). So a reference binds only when the node IS the entity it names:
 * the node's file is a file of the named package, the declaration that keeps
 * the name in that file (the w field of its scope record) is the named entity
 * (its kind; for a method, its receiver), and the node has that kind's label.
 * Every other declared entity is a graph_gap. A receiver-qualified method node
 * (<dir>.<Recv>.<Method>) is tried first, so a graph that names methods that
 * way binds them all. Only a node's qualified name, label and file_path are
 * read: the proxy nodes of an incremental run carry no lines and no
 * properties (the one exception is the SOURCE node, see go_source_owns_node).
 *
 * One name, several declarations. Files whose build constraints exclude each
 * other declare variants of ONE entity, and any of them may hold the node. Two
 * files built unconditionally that declare the same name are no package (a
 * directory of stand-alone programs, test data): two entities share the name.
 * Such a name binds only from a file that declares it itself, to that file's
 * own declaration; from anywhere else it is ambiguous. A node kept by a test
 * file is no target for product code.
 *
 * Reasons: external (standard library, module dependencies, vendored or
 * not), missing (a repository package that does not declare the name; an
 * import path inside a module without a package there), graph_gap (declared,
 * but the node is another declaration's or absent: block variables, later
 * names of a declaration, embedded fields;
 * also an undeclared name in a package with a parse error), test_only_target
 * (product code naming a declaration that only test files have), ambiguous
 * (several modules or packages fit; a name declared as two kinds; a name two
 * unconstrained files declare),
 * unparseable (a reference longer than CBM_DOCLINK_GO_REF_MAX, an import path
 * the scan could not record, an alias chain longer than GO_ALIAS_DEPTH).
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    GO_KEY_BUF = 2304,   /* id + type + name, or id + an import path */
    GO_STACK_BUF = 1024, /* paths and qualified names shorter than this need no heap */
    GO_MAX_FIELDS = 6,   /* one more than the fields of the longest scope record */
    GO_ALIAS_DEPTH = 8,  /* alias-of-alias hops followed */
    GO_KEY_SEP = 0x1F,   /* between the parts of a composite key */
    GO_NAME_MAX = CBM_DOCLINK_GO_REF_MAX,
    GO_PATH_MAX = 1024, /* longest import or module path a scope records */
    GO_VERSION_MIN = 2, /* "v" + at least one digit */
    GO_GO_PREFIX = 3,   /* strlen("go-") */
    GO_ASCII_END = 0x80,
    GO_KIND_FUNC = 1, /* what a name is in a package (go_sym_t) or a file (go_flat_t) */
    GO_KIND_TYPE = 2,
    GO_KIND_VALUE = 4,
    GO_KIND_METHOD = 8, /* go_flat_t only: a method keeps the file's flat node name */
    GO_MEM_METHOD = 1,  /* what Type.Name is in a package (go_mem_t) or a file (go_fmem_t) */
    GO_MEM_FIELD = 2,
    GO_MEM_IFACE = 4,
};

/* ── Index data ──────────────────────────────────────────────────── */

typedef struct {
    const char *local; /* the qualifier this import gives the file */
    const char *path;  /* "?": the scan could not record it */
    bool conflict;     /* the file has two imports called `local` */
} go_import_t;

typedef struct {
    uint32_t line;     /* first line of the generic declaration */
    uint32_t last;     /* ... and its last */
    const char *names; /* ','-joined type parameters */
} go_generic_t;

struct go_pkg;

typedef struct go_dir {
    int idx;
    const char *path; /* "" for the repository root */
    const char *qn;   /* <project>.<path segments>: the prefix of its nodes */
    struct go_pkg **pkgs;
    int npkgs;
    int pkgs_cap;
    bool has_module;                  /* a go.mod sits here */
    bool is_std;                      /* ... and it says `module std` */
    const struct go_dir *module_root; /* the nearest directory at or above with a go.mod */
} go_dir_t;

typedef struct go_pkg {
    int idx;
    const go_dir_t *dir;
    const char *name;
    bool importable; /* has a file that is neither *_test.go nor build-ignored */
    bool has_prod;   /* has a file that is not test code */
    bool incomplete; /* a file of it has a parse error: declarations may hide */
} go_pkg_t;

typedef struct {
    int idx;
    const char *rel_path; /* borrowed: valid until destroy */
    const char *scope;    /* borrowed: read while the index is built */
    go_dir_t *dir;
    go_pkg_t *pkg;    /* NULL: no package clause, or a go.mod file */
    bool is_test;     /* test code by the indexer's rule: never a product target */
    bool go_test;     /* *_test.go: invisible to importers */
    bool ignored;     /* //go:build ignore: in no package */
    bool constrained; /* built only under a constraint (a build line, or its name) */
    go_import_t *imports;
    int nimports;
    int imports_cap;
    go_generic_t *generics; /* fresh scopes only */
    int ngenerics;
    int generics_cap;
} go_file_t;

typedef struct {
    const char *path;
    const go_dir_t *dir;
} go_module_t;

/* A standard-library package of a `module std` tree in the repository. */
typedef struct {
    const go_dir_t *dir;
    int count; /* trees that have it */
} go_stdpkg_t;

/* How many files that are built unconditionally declare one name. A package
 * that compiles has at most one: two declarations of a name can only live in
 * files whose build constraints exclude each other, and those are variants of
 * ONE entity. Two unconstrained files that declare the same name are no
 * package at all (a directory of stand-alone programs, test data): their
 * declarations are different entities that share a name. */
typedef struct {
    const go_file_t *last; /* the unconstrained file counted last */
    int files;             /* 0, 1, or GO_TWINS for two and more */
} go_free_t;

enum { GO_TWINS = 2 };

/* A package-level name. */
typedef struct {
    int kinds;                   /* GO_KIND_* of its declarations */
    bool prod;                   /* declared by product code */
    const char *alias;           /* every declaration is `type X = alias`, else NULL */
    const go_file_t *alias_file; /* the file whose imports name the alias target's package */
    bool alias_mixed;
    go_free_t free;
} go_sym_t;

/* Type.Name in a package. */
typedef struct {
    int kinds; /* GO_MEM_* */
    bool prod;
    go_free_t free;
} go_mem_t;

/* What ONE FILE declares under the flat node name <dir>.<name>, and which of
 * its declarations keeps that name (the w field of its scope records): the
 * file's last declaration of that name. */
typedef struct {
    int kinds;               /* GO_KIND_* the file declares under that name */
    int owner;               /* the GO_KIND_* that keeps the name; 0: none */
    const char *method_recv; /* owner GO_KIND_METHOD: that method's receiver */
    bool contested;          /* two declarations of that name start on the last line */
} go_flat_t;

/* What ONE FILE declares under <dir>.<type>.<name>. */
typedef struct {
    int kinds;      /* GO_MEM_* it declares */
    int node_kinds; /* ... and which of them get a node (not every field does) */
} go_fmem_t;

typedef struct {
    CBMArena arena;
    bool oom;
    const char *project;
    go_file_t *files;
    int nfiles;
    go_file_t **run_to_file;
    int run_count;
    go_dir_t **dirs;
    int ndirs;
    int dirs_cap;
    int npkgs;
    go_module_t *modules; /* sorted by (path, directory) */
    int nmodules;
    int modules_cap;
    int nstd;
    CBMHashTable *dir_by_path;  /* path -> go_dir_t */
    CBMHashTable *pkg_by_key;   /* dir SEP name -> go_pkg_t */
    CBMHashTable *file_by_path; /* rel_path -> go_file_t */
    CBMHashTable *module_first; /* module path -> the first go_module_t of that path */
    CBMHashTable *module_at;    /* dir SEP module path -> go_module_t */
    CBMHashTable *std_pkgs;     /* path below a `module std` root -> go_stdpkg_t */
    CBMHashTable *syms;         /* pkg SEP name -> go_sym_t */
    CBMHashTable *mems;         /* pkg SEP type SEP name -> go_mem_t */
    CBMHashTable *flat;         /* file SEP name -> go_flat_t */
    CBMHashTable *fmem;         /* file SEP type SEP name -> go_fmem_t */
} go_index_t;

/* ── Small helpers ───────────────────────────────────────────────── */

static void *ix_alloc(go_index_t *ix, size_t n) {
    void *p = cbm_arena_alloc(&ix->arena, n ? n : SKIP_ONE);
    ix->oom = ix->oom || !p;
    return p;
}

static char *ix_strndup(go_index_t *ix, const char *s, size_t n) {
    char *p = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !p;
    return p;
}

/* Room for one more element of an index array; NULL when memory ran out. */
static void *ix_grow(go_index_t *ix, void *items, int count, int *cap, size_t elem) {
    if (count < *cap) {
        return items;
    }
    int ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_8;
    void *grown = ix_alloc(ix, (size_t)ncap * elem);
    if (!grown) {
        return NULL;
    }
    if (count > 0) {
        memcpy(grown, items, (size_t)count * elem);
    }
    *cap = ncap;
    return grown;
}

/* A slice of text. */
typedef struct {
    const char *s;
    size_t n;
} go_str_t;

static const go_str_t GO_NO_STR = {NULL, 0};

static go_str_t go_str(const char *s) {
    return (go_str_t){s, s ? strlen(s) : 0};
}

static bool go_str_eq(go_str_t a, const char *b) {
    return strlen(b) == a.n && memcmp(a.s, b, a.n) == 0;
}

/* A text buffer that lives on the stack unless the text is long. */
typedef struct {
    char stack[GO_STACK_BUF];
    char *heap;
} go_buf_t;

/* At least `need` bytes; NULL when memory ran out. Released with go_buf_free. */
static char *go_buf_get(go_buf_t *b, size_t need) {
    b->heap = NULL;
    if (need <= sizeof(b->stack)) {
        return b->stack;
    }
    b->heap = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, need);
    return b->heap;
}

static void go_buf_free(go_buf_t *b) {
    cbm_free(CBM_MEM_CLASS_OTHER, b->heap);
    b->heap = NULL;
}

/* "<id> SEP a [SEP b]" into buf; false when it does not fit (the parts are
 * names and paths a scope records, which are bounded). */
static bool go_key(char *buf, size_t cap, int id, go_str_t a, go_str_t b) {
    int n = snprintf(buf, cap, "%d%c", id, GO_KEY_SEP);
    if (n < 0 || (size_t)n + a.n + b.n + PAIR_LEN >= cap) {
        return false;
    }
    size_t w = (size_t)n;
    memcpy(buf + w, a.s, a.n);
    w += a.n;
    if (b.s) {
        buf[w++] = GO_KEY_SEP;
        memcpy(buf + w, b.s, b.n);
        w += b.n;
    }
    buf[w] = '\0';
    return true;
}

/* The value stored under `key`, or a new zeroed one of `size` bytes. */
static void *ix_entry(go_index_t *ix, CBMHashTable *ht, const char *key, size_t size) {
    void *v = cbm_ht_get(ht, key);
    if (v) {
        return v;
    }
    v = ix_alloc(ix, size);
    char *k = ix_strndup(ix, key, strlen(key));
    if (!v || !k) {
        return NULL;
    }
    memset(v, 0, size);
    cbm_ht_set(ht, k, v);
    return v;
}

/* The entry of a composite key, created when absent; NULL when the key does
 * not fit or memory ran out. */
static void *ix_keyed(go_index_t *ix, CBMHashTable *ht, int id, go_str_t a, go_str_t b,
                      size_t size) {
    char key[GO_KEY_BUF];
    return go_key(key, sizeof(key), id, a, b) ? ix_entry(ix, ht, key, size) : NULL;
}

static const void *go_lookup(const CBMHashTable *ht, int id, go_str_t a, go_str_t b) {
    char key[GO_KEY_BUF];
    return go_key(key, sizeof(key), id, a, b) ? cbm_ht_get(ht, key) : NULL;
}

/* The value stored under the text s[0..n). */
static void *go_lookup_text(const CBMHashTable *ht, const char *s, size_t n) {
    go_buf_t b;
    char *key = go_buf_get(&b, n + SKIP_ONE);
    if (!key) {
        return NULL;
    }
    memcpy(key, s, n);
    key[n] = '\0';
    void *hit = cbm_ht_get(ht, key);
    go_buf_free(&b);
    return hit;
}

static bool go_has_suffix(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t sl = strlen(suffix);
    return n >= sl && memcmp(s + n - sl, suffix, sl) == 0;
}

static bool go_has_non_ascii(go_str_t s) {
    for (size_t i = 0; i < s.n; i++) {
        if ((unsigned char)s.s[i] >= GO_ASCII_END) {
            return true;
        }
    }
    return false;
}

/* ── Directories and packages ────────────────────────────────────── */

static go_dir_t *go_dir_find(const go_index_t *ix, const char *path, size_t len) {
    return (go_dir_t *)go_lookup_text(ix->dir_by_path, path, len);
}

static go_dir_t *go_dir_get(go_index_t *ix, const char *path, size_t len, const char *rel_path) {
    go_dir_t *d = go_dir_find(ix, path, len);
    if (d) {
        return d;
    }
    go_dir_t **grown =
        (go_dir_t **)ix_grow(ix, ix->dirs, ix->ndirs, &ix->dirs_cap, sizeof(go_dir_t *));
    d = (go_dir_t *)ix_alloc(ix, sizeof(*d));
    char *own = ix_strndup(ix, path, len);
    if (!grown || !d || !own) {
        return NULL;
    }
    memset(d, 0, sizeof(*d));
    d->idx = ix->ndirs;
    d->path = own;
    /* The same helper that names the directory's definitions. */
    d->qn = cbm_fqn_module_source_lang(&ix->arena, ix->project, rel_path, CBM_LANG_GO);
    ix->oom = ix->oom || !d->qn;
    ix->dirs = grown;
    ix->dirs[ix->ndirs++] = d;
    cbm_ht_set(ix->dir_by_path, own, d);
    return d;
}

static go_pkg_t *go_pkg_get(go_index_t *ix, go_dir_t *dir, go_str_t name) {
    char key[GO_KEY_BUF];
    if (!go_key(key, sizeof(key), dir->idx, name, GO_NO_STR)) {
        return NULL;
    }
    go_pkg_t *p = (go_pkg_t *)cbm_ht_get(ix->pkg_by_key, key);
    if (p) {
        return p;
    }
    go_pkg_t **dir_pkgs =
        (go_pkg_t **)ix_grow(ix, dir->pkgs, dir->npkgs, &dir->pkgs_cap, sizeof(go_pkg_t *));
    p = (go_pkg_t *)ix_alloc(ix, sizeof(*p));
    char *own_key = ix_strndup(ix, key, strlen(key));
    char *own_name = ix_strndup(ix, name.s, name.n);
    if (!dir_pkgs || !p || !own_key || !own_name) {
        return NULL;
    }
    memset(p, 0, sizeof(*p));
    p->idx = ix->npkgs++;
    p->dir = dir;
    p->name = own_name;
    dir->pkgs = dir_pkgs;
    dir->pkgs[dir->npkgs++] = p;
    cbm_ht_set(ix->pkg_by_key, own_key, p);
    return p;
}

/* ── Scope records ───────────────────────────────────────────────── */

/* Split a scope line (a copy the caller owns) at its tabs. */
static int go_fields(char *line, char **out, int max) {
    int n = 0;
    out[n++] = line;
    for (char *p = line; *p && n < max; p++) {
        if (*p == '\t') {
            *p = '\0';
            out[n++] = p + SKIP_ONE;
        }
    }
    return n;
}

/* A name field as a scan writes it: not empty, not longer than a name. */
static bool go_name_ok(const char *s) {
    size_t n = strlen(s);
    return n > 0 && n <= GO_NAME_MAX;
}

static bool go_path_ok(const char *s) {
    size_t n = strlen(s);
    return n > 0 && n <= GO_PATH_MAX;
}

typedef void (*go_record_fn)(go_index_t *ix, go_file_t *file, char **f, int nf);

/* Calls `fn` for every record line of a scope blob (the tag line is skipped);
 * each line is handed over split into its fields. False when memory ran out. */
static bool go_each_record(go_index_t *ix, go_file_t *file, const char *blob, go_record_fn fn) {
    const char *p = strchr(blob, '\n');
    while (p && p[SKIP_ONE]) {
        p++;
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        go_buf_t b;
        char *line = go_buf_get(&b, len + SKIP_ONE);
        if (!line) {
            ix->oom = true;
            return false;
        }
        memcpy(line, p, len);
        line[len] = '\0';
        char *f[GO_MAX_FIELDS];
        int nf = go_fields(line, f, GO_MAX_FIELDS);
        if (f[0][0] && !f[0][SKIP_ONE]) {
            fn(ix, file, f, nf);
        }
        go_buf_free(&b);
        p = nl;
    }
    return !ix->oom;
}

static void go_add_module(go_index_t *ix, go_file_t *file, const char *path) {
    file->dir->has_module = true;
    if (strcmp(path, "std") == 0) {
        /* the standard library's own module: its packages are imported by
         * their bare directory path */
        file->dir->is_std = true;
        ix->nstd++;
        return;
    }
    go_module_t *grown = (go_module_t *)ix_grow(ix, ix->modules, ix->nmodules, &ix->modules_cap,
                                                sizeof(go_module_t));
    char *own = ix_strndup(ix, path, strlen(path));
    if (!grown || !own) {
        return;
    }
    ix->modules = grown;
    ix->modules[ix->nmodules++] = (go_module_t){.path = own, .dir = file->dir};
}

static void go_add_import(go_index_t *ix, go_file_t *f, const char *local, const char *path) {
    go_import_t *grown =
        (go_import_t *)ix_grow(ix, f->imports, f->nimports, &f->imports_cap, sizeof(go_import_t));
    char *own_path = ix_strndup(ix, path, strlen(path));
    /* "-": not renamed; the name is derived once every package is known */
    char *own_local = strcmp(local, "-") == 0 ? NULL : ix_strndup(ix, local, strlen(local));
    if (!grown || !own_path || ix->oom) {
        return;
    }
    f->imports = grown;
    f->imports[f->nimports++] = (go_import_t){.local = own_local, .path = own_path};
}

/* A line number field of a scope record; false when it is none. */
static bool go_line_field(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long long value = strtoull(s, &end, CBM_DECIMAL_BASE);
    if (end == s || *end != '\0' || value > UINT32_MAX) {
        return false;
    }
    *out = (uint32_t)value;
    return true;
}

/* `g first last names`. */
static void go_add_generic(go_index_t *ix, go_file_t *f, const char *first, const char *last,
                           const char *names) {
    go_generic_t g = {.names = ix_strndup(ix, names, strlen(names))};
    go_generic_t *grown = (go_generic_t *)ix_grow(ix, f->generics, f->ngenerics, &f->generics_cap,
                                                  sizeof(go_generic_t));
    if (!grown || !g.names || !go_line_field(first, &g.line) || !go_line_field(last, &g.last)) {
        return;
    }
    f->generics = grown;
    f->generics[f->ngenerics++] = g;
}

static bool go_word_in(go_str_t word, const char *const *list, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (go_str_eq(word, list[i])) {
            return true;
        }
    }
    return false;
}

/* go/build goodOSArchFile: a file named *_GOOS.go, *_GOARCH.go or
 * *_GOOS_GOARCH.go (before an optional _test) is built only for that target.
 * The two lists are the toolchain's own closed lists (go/build syslist); a
 * name they do not know leaves the file unconstrained, which can only make
 * this resolver more careful (see go_free_t). */
static bool go_name_constrained(const char *rel_path) {
    static const char *const known_os[] = {
        "aix",     "android", "darwin",  "dragonfly", "freebsd", "hurd",
        "illumos", "ios",     "js",      "linux",     "nacl",    "netbsd",
        "openbsd", "plan9",   "solaris", "wasip1",    "windows", "zos",
    };
    static const char *const known_arch[] = {
        "386",       "amd64",       "amd64p32", "arm",    "armbe",   "arm64",
        "arm64be",   "loong64",     "mips",     "mipsle", "mips64",  "mips64le",
        "mips64p32", "mips64p32le", "ppc",      "ppc64",  "ppc64le", "riscv",
        "riscv64",   "s390",        "s390x",    "sparc",  "sparc64", "wasm",
    };
    static const char ext[] = ".go";
    static const char test[] = "_test";
    const char *slash = strrchr(rel_path, '/');
    go_str_t base = go_str(slash ? slash + SKIP_ONE : rel_path);
    if (base.n >= sizeof(ext) - SKIP_ONE && go_has_suffix(base.s, ext)) {
        base.n -= sizeof(ext) - SKIP_ONE;
    }
    size_t tl = sizeof(test) - SKIP_ONE;
    if (base.n >= tl && memcmp(base.s + base.n - tl, test, tl) == 0) {
        base.n -= tl;
    }
    size_t last = base.n;
    while (last > 0 && base.s[last - SKIP_ONE] != '_') {
        last--;
    }
    if (last == 0) {
        return false; /* no underscore: `linux.go` is an ordinary file */
    }
    go_str_t tag = {base.s + last, base.n - last};
    return go_word_in(tag, known_os, sizeof(known_os) / sizeof(known_os[0])) ||
           go_word_in(tag, known_arch, sizeof(known_arch) / sizeof(known_arch[0]));
}

static void go_set_package(go_index_t *ix, go_file_t *f, const char *name, const char *flags) {
    if (f->pkg) {
        return;
    }
    f->ignored = strchr(flags, 'i') != NULL;
    f->constrained = strchr(flags, 'c') != NULL || go_name_constrained(f->rel_path);
    f->pkg = go_pkg_get(ix, f->dir, go_str(name));
    if (!f->pkg) {
        ix->oom = true;
        return;
    }
    go_pkg_t *p = f->pkg;
    p->importable = p->importable || (!f->go_test && !f->ignored);
    p->has_prod = p->has_prod || (!f->is_test && !f->ignored);
    p->incomplete = p->incomplete || strchr(flags, 'e') != NULL;
}

/* First pass over a file's scope: package, module, imports, generics. */
static void go_record_head(go_index_t *ix, go_file_t *file, char **f, int nf) {
    enum { P_FIELDS = 3, I_FIELDS = 3, G_FIELDS = 2, GEN_FIELDS = 4, GEN_NAMES = 3 };
    char kind = f[0][0];
    if (kind == 'P' && nf == P_FIELDS && go_name_ok(f[SKIP_ONE])) {
        go_set_package(ix, file, f[SKIP_ONE], f[PAIR_LEN]);
    } else if (kind == 'G' && nf == G_FIELDS && go_path_ok(f[SKIP_ONE])) {
        go_add_module(ix, file, f[SKIP_ONE]);
    } else if (kind == 'I' && nf == I_FIELDS && go_name_ok(f[SKIP_ONE]) &&
               go_path_ok(f[PAIR_LEN])) {
        go_add_import(ix, file, f[SKIP_ONE], f[PAIR_LEN]);
    } else if (kind == 'g' && nf == GEN_FIELDS) {
        go_add_generic(ix, file, f[SKIP_ONE], f[PAIR_LEN], f[GEN_NAMES]);
    }
}

/* ── Modules ─────────────────────────────────────────────────────── */

static int go_module_cmp(const void *a, const void *b) {
    const go_module_t *x = (const go_module_t *)a;
    const go_module_t *y = (const go_module_t *)b;
    int c = strcmp(x->path, y->path);
    return c ? c : strcmp(x->dir->path, y->dir->path);
}

/* Sort the modules; index the first entry of every module path, and every
 * (directory, module path) pair. */
static void go_finish_modules(go_index_t *ix) {
    if (ix->nmodules > SKIP_ONE) {
        qsort(ix->modules, (size_t)ix->nmodules, sizeof(ix->modules[0]), go_module_cmp);
    }
    for (int i = 0; i < ix->nmodules; i++) {
        go_module_t *m = &ix->modules[i];
        if (i == 0 || strcmp(m->path, ix->modules[i - SKIP_ONE].path) != 0) {
            cbm_ht_set(ix->module_first, m->path, m);
        }
        char key[GO_KEY_BUF];
        char *own = go_key(key, sizeof(key), m->dir->idx, go_str(m->path), GO_NO_STR)
                        ? ix_strndup(ix, key, strlen(key))
                        : NULL;
        if (own) {
            cbm_ht_set(ix->module_at, own, m);
        }
    }
}

/* The nearest directory at or above `d` that holds a go.mod. */
static const go_dir_t *go_module_root(const go_index_t *ix, const go_dir_t *d) {
    size_t len = strlen(d->path);
    for (;;) {
        const go_dir_t *up = go_dir_find(ix, d->path, len);
        if (up && up->has_module) {
            return up;
        }
        if (len == 0) {
            return NULL;
        }
        while (len > 0 && d->path[len - SKIP_ONE] != '/') {
            len--;
        }
        if (len > 0) {
            len--; /* the slash */
        }
    }
}

/* Give every directory its module root, and list the packages of the
 * repository's `module std` trees under their bare import paths. */
static void go_finish_dirs(go_index_t *ix) {
    for (int i = 0; i < ix->ndirs; i++) {
        go_dir_t *d = ix->dirs[i];
        d->module_root = go_module_root(ix, d);
        const go_dir_t *root = d->module_root;
        if (!root || !root->is_std || root == d || d->npkgs == 0) {
            continue;
        }
        size_t rl = strlen(root->path);
        const char *rel = d->path + rl + (rl > 0 ? SKIP_ONE : 0);
        go_stdpkg_t *sp = (go_stdpkg_t *)ix_entry(ix, ix->std_pkgs, rel, sizeof(go_stdpkg_t));
        if (sp) {
            sp->dir = sp->count == 0 ? d : sp->dir;
            sp->count++;
        }
    }
}

/* ── Import paths ────────────────────────────────────────────────── */

typedef enum {
    GO_PATH_EXTERNAL = 0, /* no module of the repository holds it */
    GO_PATH_PACKAGE,      /* a directory of the repository with Go files */
    GO_PATH_NO_PACKAGE,   /* inside a module, but no Go package there */
    GO_PATH_AMBIGUOUS,    /* several modules of that path, none of them the file's own */
    GO_PATH_UNKNOWN,      /* not decidable: an import the scan could not record, no memory */
} go_path_kind_t;

/* `<root>/<rest>` when it is a directory of the repository with Go files that
 * belongs to the module rooted at `root` (no nested go.mod on the way). */
static go_path_kind_t go_module_pkg_dir(const go_index_t *ix, const go_dir_t *root, go_str_t rest,
                                        const go_dir_t **dir_out) {
    size_t rl = strlen(root->path);
    go_buf_t b;
    char *buf = go_buf_get(&b, rl + rest.n + PAIR_LEN);
    if (!buf) {
        return GO_PATH_UNKNOWN;
    }
    size_t w = rl;
    memcpy(buf, root->path, rl);
    if (rest.n > 0) {
        if (w > 0) {
            buf[w++] = '/';
        }
        memcpy(buf + w, rest.s, rest.n);
        w += rest.n;
    }
    const go_dir_t *dir = go_dir_find(ix, buf, w);
    go_buf_free(&b);
    if (!dir || dir->npkgs == 0 || dir->module_root != root) {
        return GO_PATH_NO_PACKAGE;
    }
    *dir_out = dir;
    return GO_PATH_PACKAGE;
}

/* The module a module path means for file `src`: the only module of that
 * path, else the one the file itself lives in. NULL: nothing picks one. */
static const go_module_t *go_pick_module(const go_index_t *ix, const go_module_t *first,
                                         const go_file_t *src) {
    const go_module_t *next = first + SKIP_ONE;
    if (next >= ix->modules + ix->nmodules || strcmp(next->path, first->path) != 0) {
        return first;
    }
    const go_dir_t *own = src ? src->dir->module_root : NULL;
    return own ? (const go_module_t *)go_lookup(ix->module_at, own->idx, go_str(first->path),
                                                GO_NO_STR)
               : NULL;
}

/* A path no module prefixes: a package of the repository's own standard
 * library, when it has one. */
static go_path_kind_t go_classify_std(const go_index_t *ix, const go_file_t *src, go_str_t path,
                                      const go_dir_t **dir_out) {
    if (ix->nstd == 0) {
        return GO_PATH_EXTERNAL;
    }
    const go_stdpkg_t *sp = (const go_stdpkg_t *)go_lookup_text(ix->std_pkgs, path.s, path.n);
    if (!sp) {
        return GO_PATH_EXTERNAL;
    }
    if (sp->count == SKIP_ONE) {
        *dir_out = sp->dir;
        return GO_PATH_PACKAGE;
    }
    const go_dir_t *own = src ? src->dir->module_root : NULL;
    if (own && own->is_std && go_module_pkg_dir(ix, own, path, dir_out) == GO_PATH_PACKAGE) {
        return GO_PATH_PACKAGE;
    }
    return GO_PATH_AMBIGUOUS;
}

/* Where an import path leads: the longest module path of the repository that
 * prefixes it decides. */
static go_path_kind_t go_classify_path(const go_index_t *ix, const go_file_t *src, go_str_t path,
                                       const go_dir_t **dir_out) {
    if (path.n == 0 || go_str_eq(path, "?")) {
        return GO_PATH_UNKNOWN;
    }
    go_buf_t b;
    char *buf = go_buf_get(&b, path.n + SKIP_ONE);
    if (!buf) {
        return GO_PATH_UNKNOWN;
    }
    memcpy(buf, path.s, path.n);
    buf[path.n] = '\0';
    const go_module_t *first = NULL;
    size_t cut = path.n;
    for (; cut > 0 && !first; cut--) {
        if (cut != path.n && buf[cut] != '/') {
            continue;
        }
        char saved = buf[cut];
        buf[cut] = '\0';
        first = (const go_module_t *)cbm_ht_get(ix->module_first, buf);
        buf[cut] = saved;
    }
    go_buf_free(&b);
    if (!first) {
        return go_classify_std(ix, src, path, dir_out);
    }
    const go_module_t *m = go_pick_module(ix, first, src);
    if (!m) {
        return GO_PATH_AMBIGUOUS;
    }
    size_t used = strlen(m->path);
    go_str_t rest = {path.s + used, path.n - used};
    if (rest.n > 0) {
        rest.s++; /* the slash after the module path */
        rest.n--;
    }
    return go_module_pkg_dir(ix, m->dir, rest, dir_out);
}

/* The package an import path means in `dir`: its only importable package. With
 * several, the one called `hint` (the qualifier, or the path's last element).
 * NULL for none; *ambiguous when several fit and nothing picks one. */
static const go_pkg_t *go_dir_package(const go_dir_t *dir, go_str_t hint, bool *ambiguous) {
    const go_pkg_t *only = NULL;
    const go_pkg_t *named = NULL;
    int count = 0;
    for (int i = 0; i < dir->npkgs; i++) {
        const go_pkg_t *p = dir->pkgs[i];
        if (!p->importable) {
            continue;
        }
        count++;
        only = p;
        if (hint.s && go_str_eq(hint, p->name)) {
            named = p;
        }
    }
    *ambiguous = count > SKIP_ONE && !named;
    return count > SKIP_ONE ? named : only;
}

static bool go_name_byte(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
           c >= GO_ASCII_END;
}

/* The last element of a slash-separated path. */
static go_str_t go_path_base(go_str_t path) {
    size_t start = path.n;
    while (start > 0 && path.s[start - SKIP_ONE] != '/') {
        start--;
    }
    return (go_str_t){path.s + start, path.n - start};
}

/* go/doc assumedPackageName: the last path element; for a major-version
 * element (`v2`) the one before it; without a `go-` prefix; cut at the first
 * byte that cannot be in an identifier. */
static go_str_t go_assumed_name(go_str_t path) {
    go_str_t base = go_path_base(path);
    bool version = base.n >= GO_VERSION_MIN && base.s[0] == 'v';
    for (size_t i = SKIP_ONE; version && i < base.n; i++) {
        version = base.s[i] >= '0' && base.s[i] <= '9';
    }
    if (version && base.n < path.n) {
        base = go_path_base((go_str_t){path.s, path.n - base.n - SKIP_ONE});
    }
    if (base.n >= GO_GO_PREFIX && memcmp(base.s, "go-", GO_GO_PREFIX) == 0) {
        base.s += GO_GO_PREFIX;
        base.n -= GO_GO_PREFIX;
    }
    size_t n = 0;
    while (n < base.n && go_name_byte((unsigned char)base.s[n])) {
        n++;
    }
    return (go_str_t){base.s, n};
}

static int go_import_cmp(const void *a, const void *b) {
    const go_import_t *x = (const go_import_t *)a;
    const go_import_t *y = (const go_import_t *)b;
    int c = strcmp(x->local, y->local);
    return c ? c : strcmp(x->path, y->path);
}

static int go_generic_cmp(const void *a, const void *b) {
    const go_generic_t *x = (const go_generic_t *)a;
    const go_generic_t *y = (const go_generic_t *)b;
    return (x->line > y->line) - (x->line < y->line);
}

/* The name an import without one is called by: the real package name of an
 * in-repo directory, else go/doc's assumed name. */
static go_str_t go_unnamed_import_name(const go_index_t *ix, const go_file_t *f,
                                       const go_import_t *im) {
    go_str_t path = go_str(im->path);
    go_str_t name = go_assumed_name(path);
    const go_dir_t *dir = NULL;
    if (go_classify_path(ix, f, path, &dir) == GO_PATH_PACKAGE) {
        bool ambiguous = false;
        const go_pkg_t *pkg = go_dir_package(dir, name, &ambiguous);
        if (pkg) {
            return go_str(pkg->name);
        }
    }
    return name;
}

/* Name the unnamed imports of a file, sort its imports by name and mark the
 * names two different paths claim. Its type-parameter records are sorted by
 * line on the way. */
static void go_finish_file(go_index_t *ix, go_file_t *f) {
    if (f->ngenerics > SKIP_ONE) {
        qsort(f->generics, (size_t)f->ngenerics, sizeof(f->generics[0]), go_generic_cmp);
    }
    for (int i = 0; i < f->nimports; i++) {
        go_import_t *im = &f->imports[i];
        if (!im->local) {
            go_str_t name = go_unnamed_import_name(ix, f, im);
            im->local = ix_strndup(ix, name.s, name.n);
        }
        if (!im->local) {
            f->nimports = 0;
            return;
        }
    }
    if (f->nimports > SKIP_ONE) {
        qsort(f->imports, (size_t)f->nimports, sizeof(f->imports[0]), go_import_cmp);
    }
    int i = 0;
    while (i < f->nimports) {
        int end = i + SKIP_ONE; /* [i, end): the imports called alike, sorted by path */
        while (end < f->nimports && strcmp(f->imports[end].local, f->imports[i].local) == 0) {
            end++;
        }
        bool conflict = strcmp(f->imports[i].path, f->imports[end - SKIP_ONE].path) != 0;
        for (int k = i; k < end; k++) {
            f->imports[k].conflict = conflict;
        }
        i = end;
    }
}

/* The file's import called `name`; NULL when it has none. */
static const go_import_t *go_file_import(const go_file_t *f, go_str_t name) {
    int lo = 0;
    int hi = f->nimports - SKIP_ONE;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const char *local = f->imports[mid].local;
        size_t ll = strlen(local);
        int c = memcmp(name.s, local, name.n < ll ? name.n : ll);
        if (c == 0 && name.n != ll) {
            c = name.n < ll ? -SKIP_ONE : SKIP_ONE;
        }
        if (c == 0) {
            return &f->imports[mid];
        }
        if (c < 0) {
            hi = mid - SKIP_ONE;
        } else {
            lo = mid + SKIP_ONE;
        }
    }
    return NULL;
}

/* ── Declarations ────────────────────────────────────────────────── */

/* Count file `f` as a declarer of one name when it is built unconditionally. A
 * file's records are read in one run, so its declarations of one name arrive
 * together. */
static void go_count_free(go_free_t *fr, const go_file_t *f) {
    if (f->constrained || fr->last == f) {
        return;
    }
    fr->last = f;
    if (fr->files < GO_TWINS) {
        fr->files++;
    }
}

static void go_add_sym(go_index_t *ix, go_file_t *f, int kind_bit, const char *name,
                       const char *alias) {
    if (!f->pkg || f->ignored) {
        return;
    }
    bool fresh = !go_lookup(ix->syms, f->pkg->idx, go_str(name), GO_NO_STR);
    go_sym_t *s =
        (go_sym_t *)ix_keyed(ix, ix->syms, f->pkg->idx, go_str(name), GO_NO_STR, sizeof(go_sym_t));
    if (!s) {
        return;
    }
    s->kinds |= kind_bit;
    s->prod = s->prod || !f->is_test;
    go_count_free(&s->free, f);
    bool is_alias = alias && strcmp(alias, "-") != 0;
    if (fresh && is_alias) {
        s->alias = ix_strndup(ix, alias, strlen(alias));
        s->alias_file = f;
    } else if (!is_alias || !s->alias || strcmp(s->alias, alias) != 0) {
        s->alias_mixed = true; /* declarations disagree: nothing to follow */
    }
}

static void go_add_member(go_index_t *ix, go_file_t *f, int kind_bit, const char *type,
                          const char *name, bool has_node) {
    go_fmem_t *fm =
        (go_fmem_t *)ix_keyed(ix, ix->fmem, f->idx, go_str(type), go_str(name), sizeof(go_fmem_t));
    if (fm) {
        fm->kinds |= kind_bit;
        fm->node_kinds |= has_node ? kind_bit : 0;
    }
    if (!f->pkg || f->ignored) {
        return;
    }
    go_mem_t *m = (go_mem_t *)ix_keyed(ix, ix->mems, f->pkg->idx, go_str(type), go_str(name),
                                       sizeof(go_mem_t));
    if (m) {
        m->kinds |= kind_bit;
        m->prod = m->prod || !f->is_test;
        go_count_free(&m->free, f);
    }
}

/* Record that file `f` declares `name` as `kind`, and what the declaration's
 * w field says: `1` it keeps the file's flat node name <dir>.<name>, `x` two
 * declarations contest it, `0` a later one keeps it, `-` it has no node.
 * `recv` is set for a method. */
static void go_add_claim(go_index_t *ix, const go_file_t *f, int kind, const char *name,
                         const char *recv, const char *w) {
    go_flat_t *fl =
        (go_flat_t *)ix_keyed(ix, ix->flat, f->idx, go_str(name), GO_NO_STR, sizeof(go_flat_t));
    if (!fl) {
        return;
    }
    fl->kinds |= kind;
    if (w[0] == 'x') {
        fl->contested = true;
    } else if (w[0] == '1') {
        fl->owner = kind;
        fl->method_recv = recv ? ix_strndup(ix, recv, strlen(recv)) : NULL;
    }
}

/* Second pass over a file's scope: its declarations. */
static void go_record_decl(go_index_t *ix, go_file_t *file, char **f, int nf) {
    enum {
        F_FIELDS = 3,
        M_FIELDS = 4,
        T_FIELDS = 5,
        V_FIELDS = 3,
        FIELD_FIELDS = 4,
        IM_FIELDS = 3
    };
    enum { F_OWNER = 2, T_ALIAS = 3, T_OWNER = 4, V_OWNER = 2, FIELD_NODE = 3 };
    char kind = f[0][0];
    if (nf < IM_FIELDS || !go_name_ok(f[SKIP_ONE])) {
        return;
    }
    if (kind == 'F' && nf == F_FIELDS) {
        go_add_sym(ix, file, GO_KIND_FUNC, f[SKIP_ONE], NULL);
        go_add_claim(ix, file, GO_KIND_FUNC, f[SKIP_ONE], NULL, f[F_OWNER]);
    } else if (kind == 'T' && nf == T_FIELDS) {
        go_add_sym(ix, file, GO_KIND_TYPE, f[SKIP_ONE], f[T_ALIAS]);
        go_add_claim(ix, file, GO_KIND_TYPE, f[SKIP_ONE], NULL, f[T_OWNER]);
    } else if (kind == 'V' && nf == V_FIELDS) {
        go_add_sym(ix, file, GO_KIND_VALUE, f[SKIP_ONE], NULL);
        go_add_claim(ix, file, GO_KIND_VALUE, f[SKIP_ONE], NULL, f[V_OWNER]);
    } else if (kind == 'M' && nf == M_FIELDS && go_name_ok(f[PAIR_LEN])) {
        /* #1913: a method is found through its receiver only; it claims no
         * flat <dir>.<name> (its record says w `-`), so a type, function or
         * value of the same name keeps that node. */
        if (strcmp(f[SKIP_ONE], "?") != 0) {
            go_add_member(ix, file, GO_MEM_METHOD, f[SKIP_ONE], f[PAIR_LEN], true);
        }
    } else if (kind == 'f' && nf == FIELD_FIELDS && go_name_ok(f[PAIR_LEN])) {
        go_add_member(ix, file, GO_MEM_FIELD, f[SKIP_ONE], f[PAIR_LEN], f[FIELD_NODE][0] == '1');
    } else if (kind == 'm' && nf == IM_FIELDS && go_name_ok(f[PAIR_LEN])) {
        go_add_member(ix, file, GO_MEM_IFACE, f[SKIP_ONE], f[PAIR_LEN], true);
    }
}

/* ── Index lifecycle ─────────────────────────────────────────────── */

static void go_destroy(void *index) {
    go_index_t *ix = (go_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->dir_by_path);
    cbm_ht_free(ix->pkg_by_key);
    cbm_ht_free(ix->file_by_path);
    cbm_ht_free(ix->module_first);
    cbm_ht_free(ix->module_at);
    cbm_ht_free(ix->std_pkgs);
    cbm_ht_free(ix->syms);
    cbm_ht_free(ix->mems);
    cbm_ht_free(ix->flat);
    cbm_ht_free(ix->fmem);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static bool go_index_tables(go_index_t *ix, int file_count, int run_file_count) {
    ix->dir_by_path = cbm_ht_create(CBM_SZ_1K);
    ix->pkg_by_key = cbm_ht_create(CBM_SZ_1K);
    ix->file_by_path = cbm_ht_create(CBM_SZ_4K);
    ix->module_first = cbm_ht_create(CBM_SZ_64);
    ix->module_at = cbm_ht_create(CBM_SZ_64);
    ix->std_pkgs = cbm_ht_create(CBM_SZ_64);
    ix->syms = cbm_ht_create(CBM_SZ_4K);
    ix->mems = cbm_ht_create(CBM_SZ_4K);
    ix->flat = cbm_ht_create(CBM_SZ_4K);
    ix->fmem = cbm_ht_create(CBM_SZ_4K);
    ix->nfiles = file_count;
    ix->files =
        (go_file_t *)ix_alloc(ix, (size_t)(file_count ? file_count : SKIP_ONE) * sizeof(go_file_t));
    ix->run_count = run_file_count;
    ix->run_to_file = (go_file_t **)cbm_calloc(
        CBM_MEM_CLASS_OTHER,
        (size_t)(run_file_count ? run_file_count : SKIP_ONE) * sizeof(go_file_t *));
    return ix->dir_by_path && ix->pkg_by_key && ix->file_by_path && ix->module_first &&
           ix->module_at && ix->std_pkgs && ix->syms && ix->mems && ix->flat && ix->fmem &&
           ix->files && ix->run_to_file;
}

/* True for a file below a directory called `testdata`: the go tool ignores
 * such directories, so what they hold is fixture data, never product code. */
static bool go_in_testdata(const char *rel_path) {
    static const char dir[] = "testdata";
    const size_t dl = sizeof(dir) - SKIP_ONE;
    for (const char *p = rel_path;;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            return false; /* the last element is the file's name */
        }
        if ((size_t)(slash - p) == dl && memcmp(p, dir, dl) == 0) {
            return true;
        }
        p = slash + SKIP_ONE;
    }
}

/* File i of the index: its directory, its test flags, the head of its scope. */
static bool go_index_file(go_index_t *ix, int i, const cbm_doclink_file_t *src) {
    go_file_t *f = &ix->files[i];
    memset(f, 0, sizeof(*f));
    f->idx = i;
    f->rel_path = src->rel_path;
    f->scope = src->scope;
    f->is_test = cbm_is_test_file(src->rel_path, CBM_LANG_GO) || go_in_testdata(src->rel_path);
    f->go_test = go_has_suffix(src->rel_path, "_test.go");
    const char *slash = strrchr(src->rel_path, '/');
    f->dir =
        go_dir_get(ix, src->rel_path, slash ? (size_t)(slash - src->rel_path) : 0, src->rel_path);
    if (!f->dir) {
        ix->oom = true;
        return false;
    }
    cbm_ht_set(ix->file_by_path, f->rel_path, f);
    if (src->run_file >= 0 && src->run_file < ix->run_count) {
        ix->run_to_file[src->run_file] = f;
    }
    return !f->scope || go_each_record(ix, f, f->scope, go_record_head);
}

static void *go_build(const cbm_doclink_build_in_t *in) {
    go_index_t *ix = (go_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    bool ok = go_index_tables(ix, in->file_count, in->run_file_count);
    for (int i = 0; ok && i < in->file_count; i++) {
        ok = go_index_file(ix, i, &in->files[i]);
    }
    if (ok) {
        go_finish_modules(ix);
        go_finish_dirs(ix);
    }
    for (int i = 0; ok && i < ix->nfiles; i++) {
        go_finish_file(ix, &ix->files[i]);
        ok = !ix->oom;
    }
    for (int i = 0; ok && i < ix->nfiles; i++) {
        go_file_t *f = &ix->files[i];
        ok = !f->scope || go_each_record(ix, f, f->scope, go_record_decl);
        f->scope = NULL; /* read: the index keeps no pointer into a blob */
    }
    if (!ok || ix->oom) {
        cbm_log_error("doc_links.go.error", "step", "index", "reason", "alloc");
        go_destroy(ix);
        return NULL;
    }
    char b[CBM_SZ_4][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[SKIP_ONE], sizeof(b[SKIP_ONE]), "%d", ix->npkgs);
    snprintf(b[PAIR_LEN], sizeof(b[PAIR_LEN]), "%d", ix->nmodules + ix->nstd);
    snprintf(b[CBM_SZ_3], sizeof(b[CBM_SZ_3]), "%u", cbm_ht_count(ix->syms));
    cbm_log_info("doc_links.go.index", "files", b[0], "packages", b[SKIP_ONE], "modules",
                 b[PAIR_LEN], "symbols", b[CBM_SZ_3]);
    return ix;
}

/* ── Resolution ──────────────────────────────────────────────────── */

typedef struct {
    const go_index_t *ix;
    const cbm_gbuf_t *g;
    const go_file_t *src;
    cbm_doclink_outcome_t *out;
} go_ctx_t;

static void go_row(const go_ctx_t *c, int reason) {
    c->out->kind = CBM_DOCLINK_UNRESOLVED;
    c->out->reason = reason;
}

/* Plain text for go/doc: neither an edge nor a row. */
static void go_not_a_link(const go_ctx_t *c) {
    c->out->kind = CBM_DOCLINK_LOCAL;
}

static void go_edge(const go_ctx_t *c, const cbm_gbuf_node_t *node, bool exact) {
    if (!node) {
        go_row(c, CBM_DOCLINK_REASON_GRAPH_GAP);
        return;
    }
    c->out->kind = CBM_DOCLINK_EDGE;
    c->out->target = node;
    c->out->exact = exact;
}

/* A name the target package does not declare. Written bare it is plain text.
 * Qualified it is a broken link; but in a package with a parse error the
 * declaration may be hidden from the scan (a graph gap, not doc rot), and
 * text with a non-ASCII byte may be no identifier at all, which this scan
 * cannot tell (go/doc asks the Unicode letter classes): plain text. */
static void go_undeclared(const go_ctx_t *c, const go_pkg_t *pkg, bool qualified, go_str_t recv,
                          go_str_t name) {
    if (!qualified || go_has_non_ascii(recv) || go_has_non_ascii(name)) {
        go_not_a_link(c);
    } else {
        go_row(c, pkg->incomplete ? CBM_DOCLINK_REASON_GRAPH_GAP : CBM_DOCLINK_REASON_MISSING);
    }
}

/* The node <dir>.<a>[.<b>] of the package's directory, or NULL. */
static const cbm_gbuf_node_t *go_find_node(const go_ctx_t *c, const go_pkg_t *pkg, go_str_t a,
                                           go_str_t b) {
    size_t ql = strlen(pkg->dir->qn);
    go_buf_t buf;
    char *qn = go_buf_get(&buf, ql + a.n + b.n + CBM_SZ_3);
    if (!qn) {
        return NULL;
    }
    size_t w = ql;
    memcpy(qn, pkg->dir->qn, ql);
    qn[w++] = '.';
    memcpy(qn + w, a.s, a.n);
    w += a.n;
    if (b.s) {
        qn[w++] = '.';
        memcpy(qn + w, b.s, b.n);
        w += b.n;
    }
    qn[w] = '\0';
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    go_buf_free(&buf);
    return n;
}

/* The file a node belongs to, when that is a file of package `pkg` whose
 * declarations the reference may bind: not build-ignored, and no test code
 * unless the reference itself is written in test code (a node kept by a test
 * file's twin of a product declaration is no target for product code). */
static const go_file_t *go_node_file(const go_ctx_t *c, const cbm_gbuf_node_t *n,
                                     const go_pkg_t *pkg) {
    if (!n || !n->file_path || !n->label) {
        return NULL;
    }
    const go_file_t *f = (const go_file_t *)cbm_ht_get(c->ix->file_by_path, n->file_path);
    if (!f || f->pkg != pkg || f->ignored || (f->is_test && !c->src->is_test)) {
        return NULL;
    }
    return f;
}

static bool go_label_is(const cbm_gbuf_node_t *n, const char *label) {
    return strcmp(n->label, label) == 0;
}

/* The declaration of the node's own file that keeps the flat name <dir>.<name>;
 * NULL when the node is no file's of package `pkg`, or two declarations
 * contest the name there. */
static const go_flat_t *go_flat_owner(const go_ctx_t *c, const cbm_gbuf_node_t *n,
                                      const go_pkg_t *pkg, go_str_t name) {
    const go_file_t *f = go_node_file(c, n, pkg);
    const go_flat_t *fl =
        f ? (const go_flat_t *)go_lookup(c->ix->flat, f->idx, name, GO_NO_STR) : NULL;
    return (fl && !fl->contested) ? fl : NULL;
}

/* The node of <dir>.<name> when it IS the package's function, type or value
 * of that name: the node's file keeps the name for a declaration of that
 * kind, and the node has its label. */
static const cbm_gbuf_node_t *go_symbol_node(const go_ctx_t *c, const go_pkg_t *pkg, int kind,
                                             go_str_t name) {
    const cbm_gbuf_node_t *n = go_find_node(c, pkg, name, GO_NO_STR);
    const go_flat_t *fl = go_flat_owner(c, n, pkg, name);
    if (!fl || fl->owner != kind) {
        return NULL;
    }
    bool label_ok = (kind == GO_KIND_FUNC && go_label_is(n, "Function")) ||
                    (kind == GO_KIND_TYPE && cbm_label_is_type_like(n->label)) ||
                    (kind == GO_KIND_VALUE && go_label_is(n, "Variable"));
    return label_ok ? n : NULL;
}

/* The node of <dir>.<type>.<name> when it is that member of the package: a
 * field or an interface method, or a method in a graph that names methods by
 * their receiver. */
static const cbm_gbuf_node_t *go_member_node(const go_ctx_t *c, const go_pkg_t *pkg, int kind,
                                             go_str_t type, go_str_t name) {
    const cbm_gbuf_node_t *n = go_find_node(c, pkg, type, name);
    const go_file_t *f = go_node_file(c, n, pkg);
    if (!f || !go_label_is(n, kind == GO_MEM_FIELD ? "Field" : "Method")) {
        return NULL;
    }
    const go_fmem_t *fm = (const go_fmem_t *)go_lookup(c->ix->fmem, f->idx, type, name);
    return (fm && (fm->node_kinds & kind)) ? n : NULL;
}

/* The node of the method `recv.name`: its receiver-qualified node. The flat
 * fallback below (the method that kept <dir>.<name>) is unreachable since
 * #1913: no method record claims a flat name any more. */
static const cbm_gbuf_node_t *go_method_node(const go_ctx_t *c, const go_pkg_t *pkg, go_str_t recv,
                                             go_str_t name) {
    const cbm_gbuf_node_t *qualified = go_member_node(c, pkg, GO_MEM_METHOD, recv, name);
    if (qualified) {
        return qualified;
    }
    const cbm_gbuf_node_t *n = go_find_node(c, pkg, name, GO_NO_STR);
    const go_flat_t *fl = go_flat_owner(c, n, pkg, name);
    bool owns = fl && fl->owner == GO_KIND_METHOD && fl->method_recv &&
                go_str_eq(recv, fl->method_recv) && go_label_is(n, "Method");
    return owns ? n : NULL;
}

static bool go_one_bit(int bits) {
    return bits != 0 && (bits & (bits - SKIP_ONE)) == 0;
}

/* Bind `n`, the node of a name declared once (build variants count as once).
 * A name that two unconstrained files of the package declare (go_free_t) is
 * two entities: written in a file that declares it itself (`declared_here`),
 * the reference means that file's own declaration, which binds only when it
 * is the one that has the node; written anywhere else, nothing picks one. */
static void go_bind_node(const go_ctx_t *c, const cbm_gbuf_node_t *n, bool twins,
                         bool declared_here, bool exact) {
    if (twins && !declared_here) {
        go_row(c, CBM_DOCLINK_REASON_AMBIGUOUS);
        return;
    }
    bool other_file = n && twins && strcmp(n->file_path, c->src->rel_path) != 0;
    go_edge(c, other_file ? NULL : n, exact);
}

/* [Name] / [pkg.Name] in package `pkg`. */
static void go_bind_symbol(const go_ctx_t *c, const go_pkg_t *pkg, go_str_t name, bool qualified) {
    const go_sym_t *s = (const go_sym_t *)go_lookup(c->ix->syms, pkg->idx, name, GO_NO_STR);
    if (!s) {
        go_undeclared(c, pkg, qualified, GO_NO_STR, name);
    } else if (!go_one_bit(s->kinds)) {
        go_row(c, CBM_DOCLINK_REASON_AMBIGUOUS); /* a function here, a type there */
    } else if (!s->prod && !c->src->is_test) {
        go_row(c, CBM_DOCLINK_REASON_TEST_ONLY);
    } else {
        const go_flat_t *own =
            pkg == c->src->pkg
                ? (const go_flat_t *)go_lookup(c->ix->flat, c->src->idx, name, GO_NO_STR)
                : NULL;
        go_bind_node(c, go_symbol_node(c, pkg, s->kinds, name), s->free.files >= GO_TWINS,
                     own && (own->kinds & s->kinds), qualified);
    }
}

/* The package a qualifier of file `file` names through its imports. */
static go_path_kind_t go_import_package(const go_index_t *ix, const go_file_t *file,
                                        go_str_t qualifier, const go_pkg_t **pkg_out) {
    const go_import_t *im = go_file_import(file, qualifier);
    if (!im || im->conflict) {
        return GO_PATH_NO_PACKAGE;
    }
    const go_dir_t *dir = NULL;
    go_path_kind_t kind = go_classify_path(ix, file, go_str(im->path), &dir);
    if (kind != GO_PATH_PACKAGE) {
        return kind;
    }
    bool ambiguous = false;
    *pkg_out = go_dir_package(dir, qualifier, &ambiguous);
    if (ambiguous) {
        return GO_PATH_AMBIGUOUS;
    }
    return *pkg_out ? GO_PATH_PACKAGE : GO_PATH_NO_PACKAGE;
}

enum { GO_ALIAS_NONE = 0, GO_ALIAS_FOUND, GO_ALIAS_EXTERNAL, GO_ALIAS_TOO_DEEP };

/* [X.Member] where X is `type X = Y` or `= pkg.Y`: follow the alias (and an
 * alias of an alias) to the type that declares Member; *pkg and *recv then
 * name that type. */
static int go_follow_alias(const go_ctx_t *c, const go_pkg_t **pkg, go_str_t *recv, go_str_t name) {
    for (int depth = 0; depth < GO_ALIAS_DEPTH; depth++) {
        const go_sym_t *s = (const go_sym_t *)go_lookup(c->ix->syms, (*pkg)->idx, *recv, GO_NO_STR);
        if (!s || !s->alias || s->alias_mixed || s->kinds != GO_KIND_TYPE) {
            return GO_ALIAS_NONE;
        }
        const char *dot = strchr(s->alias, '.');
        if (dot) {
            go_str_t q = {s->alias, (size_t)(dot - s->alias)};
            const go_pkg_t *next = NULL;
            go_path_kind_t kind = go_import_package(c->ix, s->alias_file, q, &next);
            if (kind == GO_PATH_EXTERNAL) {
                return GO_ALIAS_EXTERNAL;
            }
            if (kind != GO_PATH_PACKAGE) {
                return GO_ALIAS_NONE;
            }
            *pkg = next;
        }
        *recv = dot ? go_str(dot + SKIP_ONE) : go_str(s->alias);
        if (go_lookup(c->ix->mems, (*pkg)->idx, *recv, name)) {
            return GO_ALIAS_FOUND;
        }
    }
    return GO_ALIAS_TOO_DEEP; /* never an answer taken from half the chain */
}

/* The member `recv.name` of package `*pkg`, looked up through an alias when
 * the type does not declare it itself (*pkg and *recv then name the type that
 * does). NULL with *row false: not declared. NULL with *row true: a row was
 * set (the alias leads out of the repository, or too deep). */
static const go_mem_t *go_find_member(const go_ctx_t *c, const go_pkg_t **pkg, go_str_t *recv,
                                      go_str_t name, bool *row) {
    *row = false;
    const go_mem_t *m = (const go_mem_t *)go_lookup(c->ix->mems, (*pkg)->idx, *recv, name);
    if (m) {
        return m;
    }
    int followed = go_follow_alias(c, pkg, recv, name);
    if (followed == GO_ALIAS_EXTERNAL || followed == GO_ALIAS_TOO_DEEP) {
        go_row(c, followed == GO_ALIAS_EXTERNAL ? CBM_DOCLINK_REASON_EXTERNAL
                                                : CBM_DOCLINK_REASON_UNPARSEABLE);
        *row = true;
        return NULL;
    }
    return followed == GO_ALIAS_FOUND
               ? (const go_mem_t *)go_lookup(c->ix->mems, (*pkg)->idx, *recv, name)
               : NULL;
}

/* [Recv.Name] / [pkg.Recv.Name] in package `pkg`.
 *
 * What binds: a member the named type declares ITSELF. That is a method
 * declared with that receiver, a method of that interface type, or a field
 * written in that type's own struct declaration; nothing promoted through an
 * embedded type. A name the type does not declare is plain text when it is
 * written without a package and a broken link (`missing`) when it is written
 * with one. `type X = Y` is followed to the type that declares the member
 * only for a reference written with a package.
 *
 * What go/doc does, as far as it is known here (no Go toolchain was at hand
 * to check it; gopls sets its doc-comment parser up the same way): without a
 * package, `Recv.Name` is a link only for a METHOD, so `[Type.Field]` inside
 * its own package renders as plain text; with a package, go/doc links
 * whatever follows it without looking.
 *
 * Binding the same-package field is a deliberate difference from go/doc. The
 * brackets are doc-link syntax by the doc-comment specification ("[Name1.
 * Name2] refers to exported identifiers in the current package"), and the
 * field named is one declared entity, so nothing is guessed: its tier is
 * `exact`. */
static void go_bind_member(const go_ctx_t *c, const go_pkg_t *pkg, go_str_t recv, go_str_t name,
                           bool qualified) {
    const go_pkg_t *named_pkg = pkg;
    go_str_t named_recv = recv;
    bool row = false;
    const go_mem_t *m = qualified ? go_find_member(c, &pkg, &recv, name, &row)
                                  : (const go_mem_t *)go_lookup(c->ix->mems, pkg->idx, recv, name);
    if (row) {
        return;
    }
    if (!m) {
        go_undeclared(c, named_pkg, qualified, named_recv, name);
    } else if (!go_one_bit(m->kinds)) {
        go_row(c, CBM_DOCLINK_REASON_AMBIGUOUS); /* a method here, a field there */
    } else if (!m->prod && !c->src->is_test) {
        go_row(c, CBM_DOCLINK_REASON_TEST_ONLY);
    } else {
        /* the member of one type: twins of the TYPE split its members too */
        const go_sym_t *ts = (const go_sym_t *)go_lookup(c->ix->syms, pkg->idx, recv, GO_NO_STR);
        bool twins = m->free.files >= GO_TWINS || (ts && ts->free.files >= GO_TWINS);
        const go_fmem_t *own =
            pkg == c->src->pkg ? (const go_fmem_t *)go_lookup(c->ix->fmem, c->src->idx, recv, name)
                               : NULL;
        const cbm_gbuf_node_t *n = m->kinds == GO_MEM_METHOD
                                       ? go_method_node(c, pkg, recv, name)
                                       : go_member_node(c, pkg, m->kinds, recv, name);
        bool exact = qualified || m->kinds == GO_MEM_FIELD;
        go_bind_node(c, n, twins, own && (own->kinds & m->kinds), exact);
    }
}

/* [pkg]: the package's directory node. The graph has a Folder node per
 * directory and no node per Go package; the repository root is the Project
 * node, which is no package. */
static void go_bind_package(const go_ctx_t *c, const go_pkg_t *pkg) {
    if (!pkg->has_prod && !c->src->is_test) {
        go_row(c, CBM_DOCLINK_REASON_TEST_ONLY);
        return;
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, pkg->dir->qn);
    bool folder = n && n->label && strcmp(n->label, "Folder") == 0 && n->file_path &&
                  strcmp(n->file_path, pkg->dir->path) == 0;
    go_edge(c, folder ? n : NULL, true);
}

/* Standard-library packages with a one-element import path (go/doc/comment
 * stdPkgs): go/doc's own closed list, the only names it links without an
 * import. A name of a later Go release that is missing here stays plain text;
 * neither way is it an edge, unless the repository is the standard library
 * itself (a `module std` tree), where the package is found by its path. */
static bool go_is_std_single(go_str_t name) {
    static const char *const std[] = {
        "bufio",  "bytes",   "cmp",     "context", "crypto", "embed",   "encoding",
        "errors", "expvar",  "flag",    "fmt",     "hash",   "html",    "image",
        "io",     "iter",    "log",     "maps",    "math",   "mime",    "net",
        "os",     "path",    "plugin",  "reflect", "regexp", "runtime", "slices",
        "sort",   "strconv", "strings", "structs", "sync",   "syscall", "testing",
        "time",   "unicode", "unique",  "unsafe",  "uuid",   "weak",
    };
    return go_word_in(name, std, sizeof(std) / sizeof(std[0]));
}

typedef enum {
    GO_QUAL_NONE = 0, /* names no package: plain text */
    GO_QUAL_SELF,     /* the documented file's own package */
    GO_QUAL_PATH,     /* an import path */
} go_qual_t;

/* go/doc lookupPkg for a qualifier without a slash.
 *
 * go/doc itself knows only the UNION of the imports of all files of the
 * package (one name with two paths is no name), then the package's own name,
 * then its list of one-element standard-library paths. This resolver asks the
 * documented FILE's own imports instead of the union: a strict subset of what
 * go/doc links (a file can only disagree with the union by importing nothing
 * under that name), so nothing is guessed, and a file's imports stay the
 * file's own business -- with the union, every import edit would change its
 * siblings' links and could not be repaired file by file. A qualifier that
 * only a sibling file imports is therefore plain text here.
 *
 * A name two imports claim is no import name for go/doc, and it does not ask
 * the package's own name then either: only the standard-library list is left. */
static go_qual_t go_lookup_qualifier(const go_ctx_t *c, go_str_t q, go_str_t *path) {
    const go_import_t *im = go_file_import(c->src, q);
    if (im && !im->conflict) {
        *path = go_str(im->path);
        return GO_QUAL_PATH;
    }
    if (!im && go_str_eq(q, c->src->pkg->name)) {
        return GO_QUAL_SELF;
    }
    if (go_is_std_single(q)) {
        *path = q;
        return GO_QUAL_PATH;
    }
    return GO_QUAL_NONE;
}

/* True when the bare name is a type parameter in scope of the documented
 * declaration: its own, or that of the generic type it is declared in (a
 * field, an interface method). Generic declarations are top-level and do not
 * overlap, so the one that starts last at or before the line is the only
 * candidate. */
static bool go_own_type_param(const go_file_t *f, uint32_t def_line, go_str_t name) {
    int lo = 0;
    int hi = f->ngenerics - SKIP_ONE;
    const go_generic_t *hit = NULL;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->generics[mid].line <= def_line) {
            hit = &f->generics[mid];
            lo = mid + SKIP_ONE;
        } else {
            hi = mid - SKIP_ONE;
        }
    }
    if (hit && def_line > hit->last) {
        hit = NULL;
    }
    for (const char *p = hit ? hit->names : ""; *p;) {
        size_t n = strcspn(p, ",");
        if (n == name.n && memcmp(p, name.s, n) == 0) {
            return true;
        }
        p += n + (p[n] == ',');
    }
    return false;
}

/* The package an import path means for the documented file; a row or the
 * plain-text outcome is set, and NULL returned, when it means none. */
static const go_pkg_t *go_path_package(const go_ctx_t *c, go_str_t path, go_str_t hint) {
    const go_dir_t *dir = NULL;
    go_path_kind_t kind = go_classify_path(c->ix, c->src, path, &dir);
    bool ambiguous = kind == GO_PATH_AMBIGUOUS;
    const go_pkg_t *pkg = kind == GO_PATH_PACKAGE ? go_dir_package(dir, hint, &ambiguous) : NULL;
    if (kind == GO_PATH_EXTERNAL) {
        go_row(c, CBM_DOCLINK_REASON_EXTERNAL);
    } else if (kind == GO_PATH_UNKNOWN) {
        go_row(c, CBM_DOCLINK_REASON_UNPARSEABLE);
    } else if (ambiguous) {
        go_row(c, CBM_DOCLINK_REASON_AMBIGUOUS);
    } else if (!pkg) {
        go_row(c, CBM_DOCLINK_REASON_MISSING); /* a module path without a package there */
    }
    return ambiguous ? NULL : pkg;
}

/* A reference qualified by `pkg` (an import path, or a name to look up). */
static void go_resolve_qualified(const go_ctx_t *c, const CBMDocLinkGoRef *ref) {
    go_str_t q = {ref->pkg, ref->pkg_len};
    go_str_t path = q;
    go_str_t hint = q;
    const go_pkg_t *pkg = c->src->pkg;
    if (memchr(q.s, '/', q.n)) {
        hint = go_path_base(q);
    } else {
        go_qual_t kind = go_lookup_qualifier(c, q, &path);
        if (kind == GO_QUAL_NONE) {
            go_not_a_link(c);
            return;
        }
        if (kind == GO_QUAL_SELF) {
            path.n = 0;
        }
    }
    if (path.n > 0) {
        pkg = go_path_package(c, path, hint);
        if (!pkg) {
            return;
        }
    }
    go_str_t name = {ref->name, ref->name_len};
    go_str_t recv = {ref->recv, ref->recv_len};
    if (name.n == 0) {
        go_bind_package(c, pkg);
    } else if (recv.n == 0) {
        go_bind_symbol(c, pkg, name, true);
    } else {
        go_bind_member(c, pkg, recv, name, true);
    }
}

/* True when the node the core takes for the edge's source IS the documented
 * declaration: it sits in the file being resolved, at the declaration's line.
 * Go method nodes share a flat name, so without this the doc of a method that
 * lost the name (or of a build-tag twin) would be attributed to another
 * declaration's node, in another file or on another line. That node's lines
 * are known here: the file is one this run extracted. */
static bool go_source_owns_node(const go_ctx_t *c, const CBMDocLink *link) {
    if ((link->flags & CBM_DOCLINK_FLAG_FILE) || !link->source_qn) {
        return true; /* the file's own doc: its File node is the source */
    }
    const cbm_gbuf_node_t *src = cbm_gbuf_find_by_qn(c->g, link->source_qn);
    if (!src) {
        return true; /* no node at all: the core drops the reference */
    }
    return src->file_path && strcmp(src->file_path, c->src->rel_path) == 0 &&
           src->start_line == (int)link->def_line;
}

/* A well-formed reference of a file that has a package: by its shape. */
static void go_resolve_ref(const go_ctx_t *c, const CBMDocLink *link, const CBMDocLinkGoRef *ref) {
    go_str_t name = {ref->name, ref->name_len};
    go_str_t recv = {ref->recv, ref->recv_len};
    bool of_declaration = !(link->flags & CBM_DOCLINK_FLAG_FILE);
    if (ref->pkg_len > 0) {
        go_resolve_qualified(c, ref);
    } else if (recv.n > 0) {
        go_bind_member(c, c->src->pkg, recv, name, false);
    } else if (of_declaration && go_own_type_param(c->src, link->def_line, name)) {
        go_not_a_link(c); /* the declaration's own type parameter */
    } else {
        go_bind_symbol(c, c->src->pkg, name, false);
    }
}

static void go_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* Go keeps no per-file working state */
    const go_index_t *ix = (const go_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        !ix->run_to_file[run_file]) {
        return;
    }
    go_ctx_t c = {.ix = ix, .g = graph, .src = ix->run_to_file[run_file], .out = out};
    size_t raw_len = strlen(link->raw);
    CBMDocLinkGoRef ref;
    if (raw_len > CBM_DOCLINK_GO_REF_MAX) {
        go_row(&c, CBM_DOCLINK_REASON_UNPARSEABLE); /* clipped at extraction: never resolved */
        return;
    }
    if (!cbm_doclink_go_parse_ref(link->raw, raw_len, &ref)) {
        go_not_a_link(&c); /* go/doc knows no malformed link, only text */
        return;
    }
    if (!c.src->pkg) {
        go_row(&c, CBM_DOCLINK_REASON_GRAPH_GAP); /* no package clause: nothing can be placed */
        return;
    }
    go_resolve_ref(&c, link, &ref);
    if (out->kind == CBM_DOCLINK_EDGE && !go_source_owns_node(&c, link)) {
        go_row(&c, CBM_DOCLINK_REASON_GRAPH_GAP); /* the documented declaration has no node */
    }
}

/* ── Incremental scope rules ─────────────────────────────────────── */

/* The next record line of a blob; false at the end. */
static bool go_delta_next(const char **cursor, go_str_t *line) {
    const char *p = *cursor;
    if (!p || !*p) {
        return false;
    }
    const char *nl = strchr(p, '\n');
    line->s = p;
    line->n = nl ? (size_t)(nl - p) : strlen(p);
    *cursor = nl ? nl + SKIP_ONE : p + line->n;
    return true;
}

/* The first record line of a blob: what follows its tag line. */
static const char *go_delta_start(const char *blob) {
    const char *nl = strchr(blob, '\n');
    return nl ? nl + SKIP_ONE : NULL;
}

static int go_line_cmp(go_str_t a, go_str_t b) {
    int c = memcmp(a.s, b.s, a.n < b.n ? a.n : b.n);
    if (c == 0 && a.n != b.n) {
        c = a.n < b.n ? -SKIP_ONE : SKIP_ONE;
    }
    return c;
}

/* Field `want` (0-based) of a record line; GO_NO_STR when it has fewer. */
static go_str_t go_record_field(go_str_t line, int want) {
    size_t start = 0;
    int field = 0;
    for (size_t i = 0; i <= line.n; i++) {
        if (i < line.n && line.s[i] != '\t') {
            continue;
        }
        if (field == want) {
            return (go_str_t){line.s + start, i - start};
        }
        field++;
        start = i + SKIP_ONE;
    }
    return GO_NO_STR;
}

/* The name a record declares: field 1 of F and V, field 2 of M, f and m. */
static go_str_t go_record_name(go_str_t line) {
    return go_record_field(line, (line.s[0] == 'F' || line.s[0] == 'V') ? SKIP_ONE : PAIR_LEN);
}

/* `T name a pkg.Y w`: an alias whose target is read through the file's
 * imports. */
static bool go_is_qualified_alias(go_str_t line) {
    enum { T_ALIAS_FIELD = 3 };
    if (line.n == 0 || line.s[0] != 'T') {
        return false;
    }
    go_str_t alias = go_record_field(line, T_ALIAS_FIELD);
    return alias.s && memchr(alias.s, '.', alias.n) != NULL;
}

typedef struct {
    cbm_doclink_name_fn removed;
    void *ud;
    bool imports_changed;
    bool qualified_alias;
    bool global;
    bool failed;
} go_delta_t;

/* A record only one of the two scopes has. */
static void go_delta_record(go_delta_t *d, go_str_t line, bool stored_only) {
    char kind = line.s[0];
    if (kind == 'I') {
        d->imports_changed = true;
        return;
    }
    if (kind == 'g') {
        return; /* never persisted; a fresh blob may still carry them */
    }
    bool removable = kind == 'F' || kind == 'M' || kind == 'V' || kind == 'f' || kind == 'm';
    if (!stored_only || !removable) {
        d->global = true; /* a new or changed record, a package, type or module line */
        return;
    }
    go_str_t name = go_record_name(line);
    if (!name.s || name.n == 0 || !d->removed || !d->removed(d->ud, name.s, name.n)) {
        d->failed = true;
    }
}

/* One step of the merge of two sorted scopes: -1 a record only the stored
 * scope has, 1 one only the fresh scope has, 0 the same record in both. */
static int go_delta_step(go_delta_t *d, bool has_stored, go_str_t stored, bool has_fresh,
                         go_str_t fresh) {
    int c = 0;
    if (!has_stored) {
        c = SKIP_ONE;
    } else if (!has_fresh) {
        c = -SKIP_ONE;
    } else {
        c = go_line_cmp(stored, fresh);
    }
    d->qualified_alias = d->qualified_alias || (c <= 0 && go_is_qualified_alias(stored)) ||
                         (c >= 0 && go_is_qualified_alias(fresh));
    if (c < 0) {
        go_delta_record(d, stored, true);
    } else if (c > 0) {
        go_delta_record(d, fresh, false);
    }
    return c;
}

/* Both scopes are sorted, so one merge finds what each has alone.
 *   LOCAL   nothing differs; or only the file's imports differ (they scope
 *           the file itself, which is re-extracted anyway); or declarations
 *           only LEFT: a function, method, value, field or interface method.
 *           Their names are reported: files with an edge into the changed
 *           file are dependents anyway, and files whose unresolved rows name
 *           one re-resolve.
 *   GLOBAL  anything that appeared or changed (a name appearing can turn
 *           another file's plain text into a link or a graph gap; a w field
 *           flipping hands a node to another declaration), the package
 *           clause, a type (an alias may be followed through it), a module
 *           path, and the imports of a file with a qualified alias (other
 *           files follow `type X = pkg.Y` through them). */
static int go_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    go_delta_t d = {.removed = removed, .ud = ud};
    const char *sp = go_delta_start(stored);
    const char *fp = go_delta_start(fresh);
    go_str_t sl = GO_NO_STR;
    go_str_t fl = GO_NO_STR;
    bool hs = go_delta_next(&sp, &sl);
    bool hf = go_delta_next(&fp, &fl);
    while ((hs || hf) && !d.global && !d.failed) {
        int c = go_delta_step(&d, hs, sl, hf, fl);
        if (c <= 0) {
            hs = go_delta_next(&sp, &sl);
        }
        if (c >= 0) {
            hf = go_delta_next(&fp, &fl);
        }
    }
    if (d.failed) {
        return CBM_NOT_FOUND;
    }
    bool global = d.global || (d.imports_changed && d.qualified_alias);
    return global ? CBM_DOCLINK_DELTA_GLOBAL : CBM_DOCLINK_DELTA_LOCAL;
}

const cbm_doclink_resolver_t cbm_doclink_go_resolver = {
    .langs = {CBM_LANG_GO, CBM_LANG_GOMOD},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_GO_SCOPE_TAG,
    .build = go_build,
    .destroy = go_destroy,
    .resolve = go_resolve,
    .scope_delta = go_scope_delta,
};
