/*
 * doc_links_rs.c — Rust intra-doc link resolution for doc_links.h.
 *
 * The index is built from the scope blobs of the repository's Rust files and
 * Cargo manifests (internal/cbm/doclink_rs.c); nothing is read from disk.
 * Graph nodes are looked up by qualified name only, and a node is taken only
 * when its label and file are the declaration's, so the closure-repair route
 * (line-less proxy nodes) resolves exactly as a full build does.
 *
 * Model.
 *   package     a Cargo manifest with [package]: its directory, edition and
 *               dependencies. A dependency names a crate of the repository
 *               only through `path` (its own, or the workspace table's)
 *   crate root  a target root file of a package: the lib (src/lib.rs or
 *               [lib] path), src/main.rs, src/bin/, tests/, benches/,
 *               examples/, build.rs, and explicit target paths
 *   module      a file's root module or an inline `mod x { }`. A `mod x;`
 *               attaches the file rustc would read: x.rs or x/mod.rs beside a
 *               crate root, a mod.rs or a `#[path]` file, under <stem>/ beside
 *               any other file, or the `#[path]` file. A file can be attached
 *               more than once and belong to several crate roots; a file no
 *               root reaches is an orphan
 *   binding     what a module declares under a name: an item, a child
 *               module, a named import, a name a macro invocation may declare
 *
 * Lookup follows rustc's name resolution as rustdoc applies it to intra-doc
 * links, one namespace (type, value, macro) at a time:
 *   first segment  `crate` (every crate root of the file), `self`, `super`
 *                  (repeatable), `Self` (the impl's self type, the trait, or
 *                  the documented type), `::name` (extern prelude), else
 *                    1. the module's own bindings
 *                    2. its glob imports, through re-exporting globs, only
 *                       names visible to the importing module
 *                    3. the extern prelude: std, core, alloc, proc_macro,
 *                       test, the package's dependencies, and the package's
 *                       own lib from its other targets
 *                    4. the std prelude, primitive types, std macros
 *                  The first level with a binding in the namespace decides. A
 *                  level whose content is not known (a glob of an external
 *                  module) is not looked past.
 *   later segments a module's levels 1 and 2; for a type: enum variant, items
 *                  of its inherent impls, then items of trait impls whose
 *                  trait is in scope of the documenting module, then (value
 *                  namespace) fields; for a trait its own items; type aliases
 *                  are followed
 * The candidates of the namespaces are united. One entity binds. A trait and
 * a derive macro of one name are the trait (rustdoc's rule). Anything else
 * with two or more is ambiguous. A disambiguator or suffix restricts the
 * namespace and the kind; an entity of another kind is no match.
 *
 * Nothing is resolved around what the scan could not place: in a file with a
 * parse error (or constructs the blob cannot carry) a name its modules do not
 * show is a graph gap, never looked up further out; so are a name a macro
 * invocation may declare, an import that cannot be followed, a `mod` whose
 * file is not indexed, and `crate::` or `super::` from an orphan file.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "helpers.h" /* cbm_fqn_compute, cbm_fqn_module_source_lang, cbm_is_test_file */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    RS_NS_TYPE = 1,
    RS_NS_VALUE = 2,
    RS_NS_MACRO = 4,
    RS_NS_ALL = 7,
    RS_NS_COUNT = 3,
};

static const int RS_NS_LIST[RS_NS_COUNT] = {RS_NS_TYPE, RS_NS_VALUE, RS_NS_MACRO};

/* What a path names. */
enum {
    RS_E_NONE = 0,
    RS_E_ITEM,     /* idx: item */
    RS_E_MOD,      /* idx: module */
    RS_E_MEMBER,   /* idx: member */
    RS_E_IMPL,     /* idx: impl whose self type is no item of the repository (`Self` only) */
    RS_E_EXTERNAL, /* outside the repository */
    RS_E_GAP,      /* declared in the repository, but not placed or not followed */
};

enum {
    RS_SET_CAP = 8,        /* distinct candidates kept; more is ambiguous either way */
    RS_ROOTS_KEPT = 4,     /* crate roots kept per file; a file of more has no single crate */
    RS_IMPORT_PASSES = 32, /* an import behind a longer chain of imports is not followed */
    RS_ALIAS_HOPS = 8,     /* a chain of type aliases; longer is not followed */
    RS_UP_STEPS = 256,     /* module ancestors walked for one question; more is unknown */
    RS_GLOB_VISIT = 64,    /* modules reached through globs for one name; more is unknown */
    RS_IMPL_SCAN = 256,    /* trait impls of one type searched for a default item */
    RS_QN_BUF = 2048,
    RS_PATH_BUF = 1024,
    RS_PATH_DEPTH = RS_PATH_BUF / 2, /* inline modules around a `mod x;` */
    RS_FIELDS_MAX = 9,
    RS_TAIL = 4, /* path components read from the end */
    RS_EDITION_2015 = 2015,
    RS_EDITION_2018 = 2018,
    RS_EDITION_UNKNOWN = 0,
    RS_EDITION_INHERITED = -1,
    RS_EDITION_UNREADABLE = -2,
    RS_PKG_NONE = -1,
    RS_PKG_CONFLICT = -2,
    RS_VIEW_A = 1, /* out-of-line children are read beside the file */
    RS_VIEW_B = 2, /* ... from the directory named like the file */
    RS_RS_EXT = 3, /* strlen(".rs") */
};

enum { RS_FILE_OTHER = 0, RS_FILE_SOURCE, RS_FILE_MANIFEST };
enum { RS_BIND_ITEM = 0, RS_BIND_CHILD, RS_BIND_IMPORT, RS_BIND_MAYBE };
enum { RS_STATE_NEW = 0, RS_STATE_DONE };
enum {
    RS_TARGET_LIB = 0,
    RS_TARGET_BIN,
    RS_TARGET_EXAMPLE,
    RS_TARGET_TEST,
    RS_TARGET_BENCH,
    RS_TARGET_BUILD
};

typedef struct {
    uint8_t kind;    /* RS_E_* */
    bool via_import; /* bound by a named import: an exact naming */
    bool weak;       /* the name was not found, but is not known to be absent either (a gap
                      * or an external glob could hold it): a binding found in another
                      * namespace overrides it */
    int idx;
} rs_ent_t;

typedef struct {
    rs_ent_t v[RS_SET_CAP];
    int n;
    bool overflow;
} rs_set_t;

/* ── Index data ──────────────────────────────────────────────────── */

typedef struct {
    int file;
    int parent; /* the inline parent module; CBM_NOT_FOUND for a file's root module */
    const char *name;
    uint32_t start; /* lines: a re-extracted file's only */
    uint32_t end;
    char vis;
    bool cfg_test;
    bool path_attr; /* an inline module with `#[path]`: where its `mod x;` files are is not known */
    bool product;   /* reachable as product code: not test-only */
    int bind_first;
    int bind_count;
    int glob_first;
    int glob_count;
    int named_first;
    int named_count;
    int attach_first; /* a file's root module: the `mod x;` declarations naming the file */
    int attach_count;
} rs_mod_t;

typedef struct {
    int mod; /* the declaring module */
    const char *name;
    const char *path; /* the #[path] literal, NULL when there is none */
    char vis;
    bool cfg_test;
    bool cond_path; /* its file depends on cfg: not followed */
    int child;      /* the declared file's root module, CBM_NOT_FOUND when not known */
} rs_decl_t;

typedef struct {
    int child;
    int decl;
} rs_attach_t;

typedef struct {
    int file;
    int mod;
    const char *name;
    char kind; /* S E U R A F C G M V B P */
    char vis;
    bool test;
    bool macro_export;
    const char *derives; /* a type's derive names, comma-joined; NULL when it has none */
    uint32_t start;
    uint32_t end;
    char label;         /* node label code, 0 when the item has no node */
    const char *suffix; /* the node's qualified name after the file's item prefix */
    const char *alias;  /* A: the aliased path, NULL when it is no path */
    rs_ent_t alias_ent;
    int member_first;
    int member_count;
    int impl_first; /* into rs_index_t.item_impls */
    int impl_count;
} rs_item_t;

typedef struct {
    int file;
    int mod;
    int owner; /* the item it belongs to, or -(impl) - 1 */
    bool owner_test;
    const char *name;
    char kind; /* f c t d v */
    char label;
    const char *suffix;
    int seq;
} rs_member_t;

typedef struct {
    int file;
    int mod;
    uint32_t start;
    uint32_t end;
    const char *self_path;  /* NULL when the self type is no path */
    const char *trait_path; /* NULL for an inherent impl, "?" when it is no path */
    rs_ent_t self_ent;
    rs_ent_t trait_ent;
    int member_first;
    int member_count;
} rs_impl_t;

typedef struct {
    int item;
    int impl;
} rs_item_impl_t;

/* An impl's member, by the type the impl is for. */
typedef struct {
    int item;
    const char *name;
    int impl;
    int member;
} rs_assoc_t;

typedef struct {
    int file;
    int mod;
    char vis;
    char kind; /* b named, s `self` (type namespace only), g glob, x extern crate */
    const char *local;
    const char **segs;
    int nsegs;
    bool global;
    bool spread; /* what it names has no known namespace (external, or a gap): res[] holds
                  * it in all three, and a binding the module really has in one of them
                  * takes precedence there */
    int state;
    int done_pass;
    int seq;
    rs_ent_t res[RS_NS_COUNT]; /* named: per namespace; the others: res[0] */
} rs_import_t;

typedef struct {
    int mod;
    const char *name;
    int kind; /* RS_BIND_* */
    int ref;
    int seq;
    char vis;
} rs_bind_t;

/* An index entry sorted by (mod, seq). */
typedef struct {
    int mod;
    int seq;
    int ref;
} rs_ref_t;

/* A name the body of the `macro_rules!` macro `macro` declares (W records). */
typedef struct {
    const char *name;
    const char *macro;
} rs_macdecl_t;

/* Module `mod` invokes a macro of this name among its items (V records). */
typedef struct {
    int mod;
    const char *macro;
} rs_macuse_t;

/* A range of a file in which `Self` means one type: a type item's own
 * extent, or an impl block. */
typedef struct {
    uint32_t start;
    uint32_t end;
    int item; /* CBM_NOT_FOUND for an impl */
    int impl;
} rs_scope_t;

typedef struct {
    uint32_t line;
    const char *text; /* P: comma-joined names; Z: the unowned definition's QN suffix */
} rs_note_t;

/* From line `from` on, the innermost module is `mod`. */
typedef struct {
    uint32_t from;
    int mod;
} rs_span_t;

typedef struct {
    const char *rel_path;
    const char *item_prefix; /* qualified name of an item of the file, minus its name */
    const char *module_qn;
    int kind;
    bool test_path;
    bool broken; /* its modules may declare names the blob does not show */
    bool fresh;  /* re-extracted in this run: its blob has line numbers */
    bool crate_root;
    bool queued;
    bool roots_top; /* more crate roots than are kept */
    bool in_lib;    /* some crate root of it is a package's lib target */
    uint8_t views;  /* RS_VIEW_* its `mod x;` declarations are read under */
    uint8_t views_done;
    uint32_t err_line; /* first line whose definitions have no scope; 0: none */
    int mod_base;      /* its root module */
    int nmods;
    int decl_first;
    int decl_count;
    int item_first;
    int item_count;
    int root_pkg; /* a crate root: its package (RS_PKG_*) */
    int pkg;      /* the package of its crate roots (RS_PKG_*) */
    int roots[RS_ROOTS_KEPT];
    int nroots;
    int scope_first;
    int scope_count;
    int local_first; /* P records */
    int local_count;
    int unowned_first; /* Z records */
    int unowned_count;
    int span_first;
    int span_count;
    int test_first; /* start lines of the items marked as tests */
    int test_count;
} rs_file_t;

typedef struct {
    char kind; /* d dependencies, v dev, b build, w workspace.dependencies */
    const char *name;
    const char *attr; /* "-", "path", "package", "workspace" */
    const char *value;
    int seq;
} rs_rawdep_t;

typedef struct {
    const char *key; /* the crate's name in scope */
    rs_ent_t target;
} rs_dep_t;

typedef struct {
    int file;
    const char *dir; /* "" for the repository root */
    const char *name;
    const char *lib_name; /* the lib crate's name; NULL when it has none that is known */
    const char *lib_path;
    const char *build;
    bool has_package;
    bool is_workspace;
    bool broken;
    bool no_autobins;
    bool no_autoexamples;
    bool no_autotests;
    bool no_autobenches;
    int edition;
    int ws_edition;
    rs_rawdep_t *raw;
    int nraw;
    const char **targets; /* explicit target paths, as written */
    int ntargets;
    rs_dep_t *deps; /* sorted by key */
    int ndeps;
    int lib_root; /* the lib target's root module, CBM_NOT_FOUND when it has none */
    int ws;       /* its workspace root's package, CBM_NOT_FOUND */
} rs_pkg_t;

typedef struct {
    CBMArena arena;
    bool oom;
    const char *project;
    rs_file_t *files;
    int nfiles;
    int nsources;
    int *run_to_file;
    int run_count;
    rs_mod_t *mods;
    int nmods;
    rs_decl_t *decls;
    int ndecls;
    rs_attach_t *attach;
    int nattach;
    rs_item_t *items;
    int nitems;
    rs_member_t *members;
    int nmembers;
    rs_impl_t *impls;
    int nimpls;
    rs_item_impl_t *item_impls;
    int nitem_impls;
    rs_assoc_t *assoc;
    int nassoc;
    rs_import_t *imports;
    int nimports;
    rs_ref_t *globs; /* the glob imports, by module */
    int nglobs;
    rs_ref_t *named; /* the named imports, by module */
    int nnamed;
    rs_bind_t *binds;
    int nbinds;
    rs_ref_t *maybes; /* Q records: ref indexes maybe_names */
    const char **maybe_names;
    int nmaybes;
    rs_macdecl_t *macdecls; /* sorted by (name, macro) */
    int nmacdecls;
    rs_macuse_t *macuses; /* sorted by (mod, macro) */
    int nmacuses;
    rs_scope_t *scopes;
    int nscopes;
    rs_note_t *locals;
    int nlocals;
    rs_note_t *unowned;
    int nunowned;
    rs_span_t *spans;
    int nspans;
    uint32_t *test_lines;
    int ntest_lines;
    rs_pkg_t *pkgs;
    int npkgs;
    CBMHashTable *file_by_path; /* rel_path -> file + 1 */
    CBMHashTable *pkg_by_dir;   /* directory -> package + 1 */
    CBMHashTable *blanket;      /* names of members of impls for no named type */
    int crate_roots;
    int orphans;
    int gap_imports;
} rs_index_t;

/* ── Small helpers ───────────────────────────────────────────────── */

static void *ix_alloc(rs_index_t *ix, size_t n) {
    void *p = cbm_arena_alloc(&ix->arena, n ? n : SKIP_ONE);
    ix->oom = ix->oom || !p;
    return p;
}

static void *ix_calloc(rs_index_t *ix, size_t count, size_t size) {
    size_t n = count * size;
    void *p = ix_alloc(ix, n);
    if (p) {
        memset(p, 0, n ? n : SKIP_ONE);
    }
    return p;
}

static char *ix_strdup(rs_index_t *ix, const char *s) {
    char *p = cbm_arena_strdup(&ix->arena, s ? s : "");
    ix->oom = ix->oom || !p;
    return p;
}

static char *ix_strndup(rs_index_t *ix, const char *s, size_t n) {
    char *p = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !p;
    return p;
}

/* A name without the raw-identifier marker: `r#type` and `type` are one name. */
static const char *rs_plain(const char *name) {
    return (name[0] == 'r' && name[1] == '#' && name[PAIR_LEN]) ? name + PAIR_LEN : name;
}

/* Split `line` in place at tabs into at most `max` fields; the count. */
static int rs_fields(char *line, char **out, int max) {
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

static bool rs_ends_with(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t sl = strlen(suffix);
    return n >= sl && strcmp(s + n - sl, suffix) == 0;
}

static const char *rs_basename(const char *rel) {
    const char *slash = strrchr(rel, '/');
    return slash ? slash + SKIP_ONE : rel;
}

/* Append "/" and s[0..n) to the path in out[0..*w); nothing when n is 0. false
 * when it does not fit. */
static bool rs_path_put(char *out, size_t cap, size_t *w, const char *s, size_t n) {
    if (n == 0) {
        return true;
    }
    if (*w + n + PAIR_LEN >= cap) {
        return false;
    }
    if (*w > 0) {
        out[(*w)++] = '/';
    }
    memcpy(out + *w, s, n);
    *w += n;
    out[*w] = '\0';
    return true;
}

/* Normalize `dir`/`path` (repository-relative): "." and ".." segments, doubled
 * and trailing slashes. false when it leaves the repository or does not fit. */
static bool rs_join_path(const char *dir, const char *path, char *out, size_t cap) {
    char tmp[RS_PATH_BUF];
    int k = (path[0] == '/') ? CBM_NOT_FOUND
                             : snprintf(tmp, sizeof(tmp), "%s%s%s", dir, dir[0] ? "/" : "", path);
    if (k < 0 || (size_t)k >= sizeof(tmp)) {
        return false;
    }
    size_t w = 0;
    out[0] = '\0';
    for (const char *p = tmp; *p;) {
        const char *slash = strchr(p, '/');
        size_t len = slash ? (size_t)(slash - p) : strlen(p);
        const char *next = slash ? slash + SKIP_ONE : p + len;
        if (len == PAIR_LEN && p[0] == '.' && p[1] == '.') {
            if (w == 0) {
                return false;
            }
            while (w > 0 && out[w - SKIP_ONE] != '/') {
                w--;
            }
            w = w > 0 ? w - SKIP_ONE : 0;
            out[w] = '\0';
        } else if (len > 0 && !(len == SKIP_ONE && p[0] == '.')) {
            if (!rs_path_put(out, cap, &w, p, len)) {
                return false;
            }
        }
        p = next;
    }
    return true;
}

static bool rs_is_test_dir(const char *seg, size_t n) {
    static const char *const dirs[] = {"tests", "test", "benches", "examples", "fixtures"};
    for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
        if (strlen(dirs[i]) == n && memcmp(dirs[i], seg, n) == 0) {
            return true;
        }
    }
    return false;
}

/* Test, bench, example and fixture code by its place in the tree: Cargo's
 * target directories, plus the indexer's own test-file rule. */
static bool rs_is_test_path(const char *rel) {
    if (cbm_is_test_file(rel, CBM_LANG_RUST)) {
        return true;
    }
    for (const char *p = rel;;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            return false;
        }
        if (rs_is_test_dir(p, (size_t)(slash - p))) {
            return true;
        }
        p = slash + SKIP_ONE;
    }
}

static int rs_ns_index(int ns) {
    return ns == RS_NS_TYPE ? 0 : (ns == RS_NS_VALUE ? SKIP_ONE : PAIR_LEN);
}

static int rs_item_ns(char kind) {
    switch (kind) {
    case 'S':
    case 'E':
    case 'U':
    case 'R':
    case 'A':
        return RS_NS_TYPE;
    case 'F':
    case 'C':
    case 'G':
        return RS_NS_VALUE;
    default:
        return RS_NS_MACRO;
    }
}

/* A variant is a type-namespace name here: its constructor in the value
 * namespace is not what a link names (rustdoc drops it). */
static int rs_member_ns(char kind) {
    return (kind == 't' || kind == 'v') ? RS_NS_TYPE : RS_NS_VALUE;
}

static bool rs_item_is_type(char kind) {
    return kind == 'S' || kind == 'E' || kind == 'U' || kind == 'R' || kind == 'A';
}

static bool rs_in_list(const char *const *list, size_t n, const char *name) {
    for (size_t i = 0; i < n; i++) {
        if (strcmp(list[i], name) == 0) {
            return true;
        }
    }
    return false;
}

#define RS_IN(list, name) rs_in_list((list), sizeof(list) / sizeof((list)[0]), (name))

/* ── Candidate sets ──────────────────────────────────────────────── */

static rs_ent_t rs_ent(uint8_t kind, int idx) {
    return (rs_ent_t){.kind = kind, .idx = idx};
}

/* RS_E_GAP or RS_E_EXTERNAL for a name that was not found (rs_ent_t.weak). */
static rs_ent_t rs_weak(uint8_t kind) {
    return (rs_ent_t){.kind = kind, .weak = true};
}

static bool rs_ent_same(rs_ent_t a, rs_ent_t b) {
    return a.kind == b.kind && (a.kind == RS_E_EXTERNAL || a.kind == RS_E_GAP || a.idx == b.idx);
}

/* Add `e`. Every entity outside the repository counts as one candidate (the
 * outcome is `external` whichever it is), and so does every gap. */
static void rs_set_add(rs_set_t *s, rs_ent_t e) {
    if (e.kind == RS_E_NONE) {
        return;
    }
    for (int i = 0; i < s->n; i++) {
        if (rs_ent_same(s->v[i], e)) {
            s->v[i].via_import = s->v[i].via_import || e.via_import;
            s->v[i].weak = s->v[i].weak && e.weak;
            return;
        }
    }
    if (s->n >= RS_SET_CAP) {
        s->overflow = true;
        return;
    }
    s->v[s->n++] = e;
}

static void rs_set_merge(rs_set_t *dst, const rs_set_t *src) {
    for (int i = 0; i < src->n; i++) {
        rs_set_add(dst, src->v[i]);
    }
    dst->overflow = dst->overflow || src->overflow;
}

static bool rs_set_empty(const rs_set_t *s) {
    return s->n == 0 && !s->overflow;
}

static bool rs_set_has(const rs_set_t *s, rs_ent_t e) {
    for (int i = 0; i < s->n; i++) {
        if (rs_ent_same(s->v[i], e)) {
            return true;
        }
    }
    return false;
}

static bool rs_set_equal(const rs_set_t *a, const rs_set_t *b) {
    if (a->n != b->n || a->overflow || b->overflow) {
        return false;
    }
    for (int i = 0; i < a->n; i++) {
        if (!rs_set_has(b, a->v[i])) {
            return false;
        }
    }
    return true;
}

/* ── Scope blobs: counting ───────────────────────────────────────── */

typedef struct {
    int mods;
    int decls;
    int items;
    int members;
    int impls;
    int imports;
    int maybes;
    int locals;
    int unowned;
    int deps;
    int targets;
    int macuses;
    int macdecls;
} rs_counts_t;

/* The number of comma-separated names a W record carries. */
static int rs_count_names(const char *line, const char *end) {
    int n = SKIP_ONE;
    for (const char *p = line; p < end; p++) {
        n += *p == ',';
    }
    return n;
}

static void rs_count_blob(const char *blob, rs_counts_t *c) {
    for (const char *p = blob; p && *p;) {
        const char *eol = strchr(p, '\n');
        switch (p[0]) {
        case 'V':
            c->macuses++;
            break;
        case 'W':
            c->macdecls += rs_count_names(p, eol ? eol : p + strlen(p));
            break;
        case 'N':
            c->mods++;
            break;
        case 'D':
            c->decls++;
            break;
        case 'T':
            c->items++;
            break;
        case 'M':
            c->members++;
            break;
        case 'I':
            c->impls++;
            break;
        case 'U':
            c->imports++;
            break;
        case 'Q':
            c->maybes++;
            break;
        case 'P':
            c->locals++;
            break;
        case 'Z':
            c->unowned++;
            break;
        case 'X':
            c->deps++;
            break;
        case 'a':
            c->targets++;
            break;
        default:
            break;
        }
        p = eol ? eol + SKIP_ONE : NULL;
    }
}

/* True when `scope` is a manifest's blob: the line after the tag is "C". */
static bool rs_blob_is_manifest(const char *scope) {
    const char *nl = scope ? strchr(scope, '\n') : NULL;
    return nl && nl[SKIP_ONE] == 'C' && (nl[PAIR_LEN] == '\n' || nl[PAIR_LEN] == '\0');
}

/* ── Scope blobs: a source file ──────────────────────────────────── */

/* The node field of a T or M record: '!' for none, else a label code and '='
 * (the node's name is the record's name as written) or ':' and the node's
 * name after the item prefix. */
static void rs_parse_node(const char *field, const char *name, char *label, const char **suffix) {
    *label = 0;
    *suffix = NULL;
    if (!field[0] || field[0] == '!') {
        return;
    }
    if (field[SKIP_ONE] == '=') {
        *label = field[0];
        *suffix = name;
    } else if (field[SKIP_ONE] == ':' && field[PAIR_LEN]) {
        *label = field[0];
        *suffix = field + PAIR_LEN;
    }
}

/* A module field of a record of file `f`: the module, or CBM_NOT_FOUND when
 * the number is not one of the file's. */
static int rs_mod_field(const rs_file_t *f, const char *field) {
    char *end = NULL;
    long id = strtol(field, &end, CBM_DECIMAL_BASE);
    if (end == field || *end || id < 0 || id >= f->nmods) {
        return CBM_NOT_FOUND;
    }
    return f->mod_base + (int)id;
}

static uint32_t rs_u32_field(const char *field) {
    return (uint32_t)strtoul(field, NULL, CBM_DECIMAL_BASE);
}

/* What is being built while one source blob is read. */
typedef struct {
    rs_index_t *ix;
    rs_file_t *f;
    int file;
    int next_mod;   /* the id the next N record must carry */
    int owner_item; /* the T or I record members belong to */
    int owner_impl;
    int owner_mod;
    bool owner_test;
} rs_reader_t;

static void rs_read_module(rs_reader_t *r, char **fld, int n) {
    enum { F_ID = 1, F_PARENT, F_START, F_END, F_FLAGS, F_NAME, F_COUNT };
    rs_index_t *ix = r->ix;
    int parent = n == F_COUNT ? rs_mod_field(r->f, fld[F_PARENT]) : CBM_NOT_FOUND;
    if (parent < 0 || (int)rs_u32_field(fld[F_ID]) != r->next_mod || r->next_mod >= r->f->nmods ||
        parent >= r->f->mod_base + r->next_mod || !fld[F_FLAGS][0]) {
        r->f->broken = true;
        return;
    }
    rs_mod_t *m = &ix->mods[r->f->mod_base + r->next_mod];
    r->next_mod++;
    m->parent = parent;
    m->start = rs_u32_field(fld[F_START]);
    m->end = rs_u32_field(fld[F_END]);
    m->vis = fld[F_FLAGS][0];
    m->cfg_test = strchr(fld[F_FLAGS] + SKIP_ONE, 't') != NULL;
    m->path_attr = strchr(fld[F_FLAGS] + SKIP_ONE, 'c') != NULL;
    m->name = rs_plain(fld[F_NAME]);
}

static void rs_read_decl(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_FLAGS, F_NAME, F_PATH, F_COUNT };
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    if (mod < 0 || !fld[F_FLAGS][0]) {
        r->f->broken = true;
        return;
    }
    rs_decl_t *d = &r->ix->decls[r->ix->ndecls++];
    r->f->decl_count++;
    d->mod = mod;
    d->vis = fld[F_FLAGS][0];
    d->cfg_test = strchr(fld[F_FLAGS] + SKIP_ONE, 't') != NULL;
    d->cond_path = strchr(fld[F_FLAGS] + SKIP_ONE, 'c') != NULL;
    d->name = rs_plain(fld[F_NAME]);
    d->path = strcmp(fld[F_PATH], "-") == 0 ? NULL : fld[F_PATH];
    d->child = CBM_NOT_FOUND;
}

static void rs_read_import(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_VIS, F_KIND, F_LOCAL, F_PATH, F_COUNT };
    rs_index_t *ix = r->ix;
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    if (mod < 0) {
        r->f->broken = true;
        return;
    }
    rs_import_t *imp = &ix->imports[ix->nimports];
    imp->file = r->file;
    imp->mod = mod;
    imp->vis = fld[F_VIS][0];
    imp->kind = fld[F_KIND][0];
    imp->local = rs_plain(fld[F_LOCAL]);
    imp->seq = ix->nimports++;
    CBMDocLinkRsPath path;
    if (cbm_doclink_rs_parse_path(fld[F_PATH], strlen(fld[F_PATH]), false, &path) !=
        CBM_DOCLINK_RS_PATH_OK) {
        return; /* no segments: an import that is not followed */
    }
    imp->segs = (const char **)ix_alloc(ix, (size_t)path.nsegs * sizeof(char *));
    imp->global = path.global;
    for (int i = 0; imp->segs && i < path.nsegs; i++) {
        imp->segs[i] = ix_strdup(ix, rs_plain(path.segs[i]));
    }
    imp->nsegs = imp->segs ? path.nsegs : 0;
}

static void rs_read_item(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_START, F_END, F_FLAGS, F_KIND, F_NAME, F_NODE, F_EXTRA, F_COUNT };
    rs_index_t *ix = r->ix;
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    r->owner_item = CBM_NOT_FOUND;
    r->owner_impl = CBM_NOT_FOUND;
    if (mod < 0 || !fld[F_FLAGS][0]) {
        r->f->broken = true;
        return;
    }
    int idx = ix->nitems++;
    rs_item_t *it = &ix->items[idx];
    r->f->item_count++;
    it->file = r->file;
    it->mod = mod;
    it->start = rs_u32_field(fld[F_START]);
    it->end = rs_u32_field(fld[F_END]);
    it->vis = fld[F_FLAGS][0];
    it->test = strchr(fld[F_FLAGS] + SKIP_ONE, 't') != NULL;
    it->macro_export = strchr(fld[F_FLAGS] + SKIP_ONE, 'e') != NULL;
    it->kind = fld[F_KIND][0];
    it->name = rs_plain(fld[F_NAME]);
    rs_parse_node(fld[F_NODE], fld[F_NAME], &it->label, &it->suffix);
    const char *extra = strcmp(fld[F_EXTRA], "-") != 0 ? fld[F_EXTRA] : NULL;
    it->alias = it->kind == 'A' ? extra : NULL;
    it->derives = (it->kind == 'S' || it->kind == 'E' || it->kind == 'U') ? extra : NULL;
    it->member_first = ix->nmembers;
    r->owner_item = idx;
    r->owner_mod = mod;
    r->owner_test = it->test;
    if (r->f->fresh && rs_item_is_type(it->kind)) {
        ix->scopes[ix->nscopes++] =
            (rs_scope_t){.start = it->start, .end = it->end, .item = idx, .impl = CBM_NOT_FOUND};
        r->f->scope_count++;
    }
}

static void rs_read_impl(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_START, F_END, F_SELF, F_TRAIT, F_COUNT };
    rs_index_t *ix = r->ix;
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    r->owner_item = CBM_NOT_FOUND;
    r->owner_impl = CBM_NOT_FOUND;
    if (mod < 0) {
        r->f->broken = true;
        return;
    }
    int idx = ix->nimpls++;
    rs_impl_t *im = &ix->impls[idx];
    im->file = r->file;
    im->mod = mod;
    im->start = rs_u32_field(fld[F_START]);
    im->end = rs_u32_field(fld[F_END]);
    im->self_path = strcmp(fld[F_SELF], "?") == 0 ? NULL : fld[F_SELF];
    im->trait_path = strcmp(fld[F_TRAIT], "-") == 0 ? NULL : fld[F_TRAIT];
    im->member_first = ix->nmembers;
    r->owner_impl = idx;
    r->owner_mod = mod;
    r->owner_test = false;
    if (r->f->fresh) {
        ix->scopes[ix->nscopes++] =
            (rs_scope_t){.start = im->start, .end = im->end, .item = CBM_NOT_FOUND, .impl = idx};
        r->f->scope_count++;
    }
}

static void rs_read_member(rs_reader_t *r, char **fld, int n) {
    enum { F_START = 1, F_KIND, F_NAME, F_NODE, F_COUNT };
    rs_index_t *ix = r->ix;
    if (n != F_COUNT || (r->owner_item < 0 && r->owner_impl < 0)) {
        r->f->broken = true;
        return;
    }
    int idx = ix->nmembers++;
    rs_member_t *m = &ix->members[idx];
    m->file = r->file;
    m->mod = r->owner_mod;
    m->owner_test = r->owner_test;
    m->kind = fld[F_KIND][0];
    m->name = rs_plain(fld[F_NAME]);
    m->seq = idx;
    rs_parse_node(fld[F_NODE], fld[F_NAME], &m->label, &m->suffix);
    if (r->owner_item >= 0) {
        m->owner = r->owner_item;
        ix->items[r->owner_item].member_count++;
    } else {
        m->owner = -r->owner_impl - SKIP_ONE;
        ix->impls[r->owner_impl].member_count++;
    }
}

/* A name a macro invocation may declare. One that may be declared anywhere in
 * the file ('*') follows a parse error, and such a file is `broken` anyway. */
static void rs_read_maybe(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_NAME, F_COUNT };
    rs_index_t *ix = r->ix;
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    if (mod < 0) {
        r->f->broken = true; /* '*', or a record this code did not write */
        return;
    }
    ix->maybe_names[ix->nmaybes] = rs_plain(fld[F_NAME]);
    ix->maybes[ix->nmaybes] = (rs_ref_t){.mod = mod, .seq = ix->nmaybes, .ref = ix->nmaybes};
    ix->nmaybes++;
}

/* `V mod name`: the module invokes a macro of that name. */
static void rs_read_macuse(rs_reader_t *r, char **fld, int n) {
    enum { F_MOD = 1, F_NAME, F_COUNT };
    rs_index_t *ix = r->ix;
    int mod = n == F_COUNT ? rs_mod_field(r->f, fld[F_MOD]) : CBM_NOT_FOUND;
    if (mod < 0) {
        r->f->broken = true;
        return;
    }
    ix->macuses[ix->nmacuses++] = (rs_macuse_t){.mod = mod, .macro = rs_plain(fld[F_NAME])};
}

/* `W name names`: what the body of the macro `name` declares. */
static void rs_read_macdecl(rs_reader_t *r, char **fld, int n) {
    enum { F_MACRO = 1, F_NAMES, F_COUNT };
    rs_index_t *ix = r->ix;
    if (n != F_COUNT) {
        r->f->broken = true;
        return;
    }
    const char *macro = rs_plain(fld[F_MACRO]);
    for (char *p = fld[F_NAMES]; p && *p;) {
        char *comma = strchr(p, ',');
        if (comma) {
            *comma = '\0';
        }
        ix->macdecls[ix->nmacdecls++] = (rs_macdecl_t){.name = rs_plain(p), .macro = macro};
        p = comma ? comma + SKIP_ONE : NULL;
    }
}

static void rs_read_note(rs_reader_t *r, char **fld, int n, bool local) {
    enum { F_LINE = 1, F_TEXT, F_COUNT };
    rs_index_t *ix = r->ix;
    if (n != F_COUNT || !r->f->fresh) {
        return;
    }
    rs_note_t note = {.line = rs_u32_field(fld[F_LINE]), .text = fld[F_TEXT]};
    if (local) {
        ix->locals[ix->nlocals++] = note;
        r->f->local_count++;
    } else {
        ix->unowned[ix->nunowned++] = note;
        r->f->unowned_count++;
    }
}

static void rs_read_record(rs_reader_t *r, char *line) {
    char *fld[RS_FIELDS_MAX];
    char tag = line[0];
    int n = rs_fields(line, fld, RS_FIELDS_MAX);
    switch (tag) {
    case 'B':
        r->f->broken = true;
        break;
    case 'E':
        if (n == PAIR_LEN && r->f->fresh) {
            r->f->err_line = rs_u32_field(fld[SKIP_ONE]);
        }
        break;
    case 'N':
        rs_read_module(r, fld, n);
        break;
    case 'D':
        rs_read_decl(r, fld, n);
        break;
    case 'U':
        rs_read_import(r, fld, n);
        break;
    case 'T':
        rs_read_item(r, fld, n);
        break;
    case 'I':
        rs_read_impl(r, fld, n);
        break;
    case 'M':
        rs_read_member(r, fld, n);
        break;
    case 'Q':
        rs_read_maybe(r, fld, n);
        break;
    case 'V':
        rs_read_macuse(r, fld, n);
        break;
    case 'W':
        rs_read_macdecl(r, fld, n);
        break;
    case 'P':
        rs_read_note(r, fld, n, true);
        break;
    case 'Z':
        rs_read_note(r, fld, n, false);
        break;
    default:
        break;
    }
}

/* Read the scope blob of source file `fi` (its modules are allocated). */
static void rs_read_source(rs_index_t *ix, int fi, const char *blob) {
    rs_file_t *f = &ix->files[fi];
    f->decl_first = ix->ndecls;
    f->item_first = ix->nitems;
    f->scope_first = ix->nscopes;
    f->local_first = ix->nlocals;
    f->unowned_first = ix->nunowned;
    char *buf = blob ? ix_strdup(ix, blob) : NULL;
    if (!buf) {
        f->broken = true; /* no scope: what the file declares is not known */
        return;
    }
    rs_reader_t r = {.ix = ix,
                     .f = f,
                     .file = fi,
                     .next_mod = SKIP_ONE,
                     .owner_item = CBM_NOT_FOUND,
                     .owner_impl = CBM_NOT_FOUND};
    bool first = true;
    for (char *line = buf; line && *line;) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        if (!first) {
            rs_read_record(&r, line);
        }
        first = false;
        line = nl ? nl + SKIP_ONE : NULL;
    }
    if (r.next_mod != f->nmods) {
        f->broken = true; /* a module record was not read: its number names nothing */
    }
}

/* ── Scope blobs: a Cargo manifest ───────────────────────────────── */

static int rs_parse_edition(const char *value) {
    char *end = NULL;
    long year = strtol(value, &end, CBM_DECIMAL_BASE);
    if (end == value || *end || year < RS_EDITION_2015) {
        return RS_EDITION_UNREADABLE;
    }
    return (int)year;
}

static bool rs_is_false(const char *value) {
    return strcmp(value, "false") == 0;
}

static void rs_package_key(rs_pkg_t *p, const char *key, const char *value) {
    if (strcmp(key, "name") == 0) {
        p->name = strcmp(value, "?") != 0 ? value : NULL;
    } else if (strcmp(key, "edition") == 0) {
        p->edition = rs_parse_edition(value);
    } else if (strcmp(key, "edition.workspace") == 0) {
        p->edition = RS_EDITION_INHERITED;
    } else if (strcmp(key, "build") == 0) {
        p->build = value;
    } else if (strcmp(key, "autobins") == 0) {
        p->no_autobins = rs_is_false(value);
    } else if (strcmp(key, "autoexamples") == 0) {
        p->no_autoexamples = rs_is_false(value);
    } else if (strcmp(key, "autotests") == 0) {
        p->no_autotests = rs_is_false(value);
    } else if (strcmp(key, "autobenches") == 0) {
        p->no_autobenches = rs_is_false(value);
    }
}

static void rs_manifest_key(rs_pkg_t *p, const char *table, const char *key, const char *value) {
    if (strcmp(table, "package") == 0) {
        p->has_package = true;
        rs_package_key(p, key, value);
    } else if (strcmp(table, "lib") == 0) {
        if (strcmp(key, "name") == 0) {
            p->lib_name = value;
        } else if (strcmp(key, "path") == 0) {
            p->lib_path = value;
        }
    } else if (strcmp(table, "workspace.package") == 0 && strcmp(key, "edition") == 0) {
        p->ws_edition = rs_parse_edition(value);
    }
}

static void rs_manifest_record(rs_pkg_t *p, char *line) {
    enum { F_1 = 1, F_2, F_3, F_4, K_FIELDS = 4, A_FIELDS = 3, X_FIELDS = 5 };
    char *fld[X_FIELDS];
    char tag = line[0];
    int n = rs_fields(line, fld, X_FIELDS);
    if (tag == 'K' && n == K_FIELDS) {
        rs_manifest_key(p, fld[F_1], fld[F_2], fld[F_3]);
    } else if (tag == 'a' && n == A_FIELDS && strcmp(fld[F_1], "path") == 0) {
        p->targets[p->ntargets++] = fld[F_2];
    } else if (tag == 'W') {
        p->is_workspace = true;
    } else if (tag == 'X' && n == X_FIELDS && fld[F_1][0]) {
        p->raw[p->nraw] = (rs_rawdep_t){.kind = fld[F_1][0],
                                        .name = fld[F_2],
                                        .attr = fld[F_3],
                                        .value = fld[F_4],
                                        .seq = p->nraw};
        p->nraw++;
    } else if (tag == 'B') {
        p->broken = true;
    }
}

static void rs_read_manifest(rs_index_t *ix, int fi, const char *blob) {
    rs_pkg_t *p = &ix->pkgs[ix->npkgs];
    const char *rel = ix->files[fi].rel_path;
    const char *slash = strrchr(rel, '/');
    rs_counts_t c = {0};
    rs_count_blob(blob, &c);
    memset(p, 0, sizeof(*p));
    p->file = fi;
    p->dir = ix_strndup(ix, rel, slash ? (size_t)(slash - rel) : 0);
    p->lib_root = CBM_NOT_FOUND;
    p->ws = CBM_NOT_FOUND;
    p->raw = (rs_rawdep_t *)ix_alloc(ix, (size_t)c.deps * sizeof(rs_rawdep_t));
    p->targets = (const char **)ix_alloc(ix, (size_t)c.targets * sizeof(char *));
    char *buf = ix_strdup(ix, blob);
    if (!buf || !p->dir || !p->raw || !p->targets) {
        return;
    }
    int skipped = 0;
    for (char *line = buf; line && *line;) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        if (skipped >= PAIR_LEN) { /* after the tag line and the "C" line */
            rs_manifest_record(p, line);
        }
        skipped++;
        line = nl ? nl + SKIP_ONE : NULL;
    }
    ix->npkgs++;
}

/* ── Paths -> files and packages ─────────────────────────────────── */

static int rs_file_at(const rs_index_t *ix, const char *rel) {
    return (int)(intptr_t)cbm_ht_get(ix->file_by_path, rel) - SKIP_ONE;
}

/* The source file at `rel`, CBM_NOT_FOUND when none is indexed. */
static int rs_source_at(const rs_index_t *ix, const char *rel) {
    int fi = rs_file_at(ix, rel);
    return (fi >= 0 && ix->files[fi].kind == RS_FILE_SOURCE) ? fi : CBM_NOT_FOUND;
}

/* The package whose manifest is in directory `dir` ("" at the root). */
static int rs_pkg_at(const rs_index_t *ix, const char *dir) {
    return (int)(intptr_t)cbm_ht_get(ix->pkg_by_dir, dir[0] ? dir : ".") - SKIP_ONE;
}

/* `-` in a package name is `_` in the crate's name. */
static const char *rs_crate_name(rs_index_t *ix, const char *name) {
    char *out = ix_strdup(ix, name);
    for (char *p = out; p && *p; p++) {
        if (*p == '-') {
            *p = '_';
        }
    }
    return out;
}

/* The workspace root of package `pi`: the nearest manifest with a [workspace]
 * table at or above its directory. */
static int rs_workspace_of(const rs_index_t *ix, int pi) {
    char dir[RS_PATH_BUF];
    if (snprintf(dir, sizeof(dir), "%s", ix->pkgs[pi].dir) >= (int)sizeof(dir)) {
        return CBM_NOT_FOUND;
    }
    for (;;) {
        int q = rs_pkg_at(ix, dir);
        if (q >= 0 && ix->pkgs[q].is_workspace) {
            return q;
        }
        if (!dir[0]) {
            return CBM_NOT_FOUND;
        }
        char *slash = strrchr(dir, '/');
        if (slash) {
            *slash = '\0';
        } else {
            dir[0] = '\0';
        }
    }
}

/* The edition of package `pkg`: a year, or RS_EDITION_UNKNOWN. A manifest
 * without the key is edition 2015 (Cargo's default). */
static int rs_edition(const rs_index_t *ix, int pkg) {
    if (pkg < 0) {
        return RS_EDITION_UNKNOWN;
    }
    const rs_pkg_t *p = &ix->pkgs[pkg];
    int edition = p->edition;
    if (edition == RS_EDITION_INHERITED) {
        edition = p->ws >= 0 ? ix->pkgs[p->ws].ws_edition : RS_EDITION_UNREADABLE;
    }
    if (edition == 0) {
        return p->broken ? RS_EDITION_UNKNOWN : RS_EDITION_2015;
    }
    return edition > 0 ? edition : RS_EDITION_UNKNOWN;
}

/* ── Crate roots ─────────────────────────────────────────────────── */

static void rs_mark_root(rs_index_t *ix, int fi, int pi, bool lib) {
    rs_file_t *f = &ix->files[fi];
    if (!f->crate_root) {
        f->crate_root = true;
        f->root_pkg = pi;
        ix->crate_roots++;
    } else if (f->root_pkg != pi) {
        f->root_pkg = RS_PKG_CONFLICT; /* a target of two packages */
    }
    if (lib && ix->pkgs[pi].lib_root < 0) {
        ix->pkgs[pi].lib_root = f->mod_base;
    }
}

/* A target path written in a manifest. */
static void rs_claim_path(rs_index_t *ix, int pi, const char *path, bool lib) {
    char rel[RS_PATH_BUF];
    if (strcmp(path, "?") == 0 || !rs_join_path(ix->pkgs[pi].dir, path, rel, sizeof(rel))) {
        return;
    }
    int fi = rs_source_at(ix, rel);
    if (fi >= 0) {
        rs_mark_root(ix, fi, pi, lib);
    }
}

static void rs_explicit_roots(rs_index_t *ix, int pi) {
    const rs_pkg_t *p = &ix->pkgs[pi];
    if (!p->has_package) {
        return;
    }
    if (p->lib_path) {
        rs_claim_path(ix, pi, p->lib_path, true);
    }
    for (int i = 0; i < p->ntargets; i++) {
        rs_claim_path(ix, pi, p->targets[i], false);
    }
    if (p->build && !rs_is_false(p->build) && strcmp(p->build, "true") != 0) {
        rs_claim_path(ix, pi, p->build, false);
    }
}

/* The last components of a path, c[0] the file name. */
typedef struct {
    const char *c[RS_TAIL];
    size_t len[RS_TAIL];
    int n;
} rs_tail_t;

static void rs_tail(const char *rel, rs_tail_t *t) {
    memset(t, 0, sizeof(*t));
    const char *end = rel + strlen(rel);
    while (t->n < RS_TAIL && end > rel) {
        const char *s = end;
        while (s > rel && s[-SKIP_ONE] != '/') {
            s--;
        }
        t->c[t->n] = s;
        t->len[t->n] = (size_t)(end - s);
        t->n++;
        end = s > rel ? s - SKIP_ONE : rel;
    }
}

static bool rs_tail_is(const rs_tail_t *t, int k, const char *name) {
    return k < t->n && strlen(name) == t->len[k] && memcmp(t->c[k], name, t->len[k]) == 0;
}

/* The package whose directory holds component `k` of `rel`. */
static int rs_pkg_before(const rs_index_t *ix, const char *rel, const rs_tail_t *t, int k) {
    char dir[RS_PATH_BUF];
    if (k >= t->n) {
        return CBM_NOT_FOUND;
    }
    size_t dl = (size_t)(t->c[k] - rel);
    size_t n = dl > 0 ? dl - SKIP_ONE : 0;
    if (n >= sizeof(dir)) {
        return CBM_NOT_FOUND;
    }
    memcpy(dir, rel, n);
    dir[n] = '\0';
    return rs_pkg_at(ix, dir);
}

/* A file where Cargo discovers a target of package `pi` on its own. */
static void rs_claim_auto(rs_index_t *ix, int fi, int pi, int target) {
    if (pi < 0 || !ix->pkgs[pi].has_package) {
        return;
    }
    const rs_pkg_t *p = &ix->pkgs[pi];
    bool off =
        (target == RS_TARGET_LIB && p->lib_path) || (target == RS_TARGET_BIN && p->no_autobins) ||
        (target == RS_TARGET_EXAMPLE && p->no_autoexamples) ||
        (target == RS_TARGET_TEST && p->no_autotests) ||
        (target == RS_TARGET_BENCH && p->no_autobenches) || (target == RS_TARGET_BUILD && p->build);
    if (!off) {
        rs_mark_root(ix, fi, pi, target == RS_TARGET_LIB);
    }
}

/* Cargo's target auto-discovery, read off the file's own path. */
static void rs_auto_roots(rs_index_t *ix, int fi) {
    static const struct {
        const char *dir;
        int target;
    } dirs[] = {
        {"examples", RS_TARGET_EXAMPLE}, {"tests", RS_TARGET_TEST}, {"benches", RS_TARGET_BENCH}};
    enum { C0 = 0, C1, C2, C3 };
    const char *rel = ix->files[fi].rel_path;
    rs_tail_t t;
    rs_tail(rel, &t);
    bool main_rs = rs_tail_is(&t, C0, "main.rs");
    if (rs_tail_is(&t, C1, "src")) {
        if (rs_tail_is(&t, C0, "lib.rs")) {
            rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C1), RS_TARGET_LIB);
        }
        if (main_rs) {
            rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C1), RS_TARGET_BIN);
        }
    }
    if (rs_tail_is(&t, C1, "bin") && rs_tail_is(&t, C2, "src")) {
        rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C2), RS_TARGET_BIN);
    }
    if (main_rs && rs_tail_is(&t, C2, "bin") && rs_tail_is(&t, C3, "src")) {
        rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C3), RS_TARGET_BIN);
    }
    for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
        if (rs_tail_is(&t, C1, dirs[i].dir)) {
            rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C1), dirs[i].target);
        }
        if (main_rs && rs_tail_is(&t, C2, dirs[i].dir)) {
            rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C2), dirs[i].target);
        }
    }
    if (rs_tail_is(&t, C0, "build.rs")) {
        rs_claim_auto(ix, fi, rs_pkg_before(ix, rel, &t, C0), RS_TARGET_BUILD);
    }
}

static void rs_crate_roots(rs_index_t *ix) {
    for (int pi = 0; pi < ix->npkgs; pi++) {
        ix->pkgs[pi].ws = rs_workspace_of(ix, pi);
        rs_explicit_roots(ix, pi);
    }
    for (int fi = 0; fi < ix->nfiles; fi++) {
        if (ix->files[fi].kind == RS_FILE_SOURCE) {
            rs_auto_roots(ix, fi);
        }
    }
    for (int pi = 0; pi < ix->npkgs; pi++) {
        rs_pkg_t *p = &ix->pkgs[pi];
        const char *name = p->lib_name ? p->lib_name : p->name;
        p->lib_name = (name && strcmp(name, "?") != 0) ? rs_crate_name(ix, name) : NULL;
    }
}

/* ── `mod x;` -> file ────────────────────────────────────────────── */

/* The directory the file of declaration `d` is looked for in, under view
 * `view` of the declaring file: the file's directory, the file's stem (view
 * B), then the inline modules around the declaration. A `#[path]` directly in
 * a file is relative to the file's directory under either view. false when
 * it is not known (a `#[path]` on an inline module) or too long to be a path. */
static bool rs_decl_dir(const rs_index_t *ix, const rs_decl_t *d, int view, char *out, size_t cap) {
    const char *names[RS_PATH_DEPTH];
    int n = 0;
    int m = d->mod;
    while (ix->mods[m].parent >= 0) {
        if (ix->mods[m].path_attr || n >= RS_PATH_DEPTH) {
            return false;
        }
        names[n++] = ix->mods[m].name;
        m = ix->mods[m].parent;
    }
    const char *rel = ix->files[ix->mods[m].file].rel_path;
    const char *base = rs_basename(rel);
    size_t w = 0;
    out[0] = '\0';
    if (base > rel && !rs_path_put(out, cap, &w, rel, (size_t)(base - rel) - SKIP_ONE)) {
        return false;
    }
    bool stem = view == RS_VIEW_B && !(d->path && n == 0);
    size_t bl = strlen(base);
    if (stem && (bl <= RS_RS_EXT || !rs_path_put(out, cap, &w, base, bl - RS_RS_EXT))) {
        return false;
    }
    for (int i = n - SKIP_ONE; i >= 0; i--) {
        if (!rs_path_put(out, cap, &w, names[i], strlen(names[i]))) {
            return false;
        }
    }
    return true;
}

/* A file a declaration can name, and the view the file is then read under. */
typedef struct {
    int file;
    uint8_t implies;
} rs_cand_t;

static int rs_cand_add(const rs_index_t *ix, const char *rel, uint8_t implies, rs_cand_t *out,
                       int n) {
    int fi = rs_source_at(ix, rel);
    if (fi >= 0) {
        out[n++] = (rs_cand_t){.file = fi, .implies = implies};
    }
    return n;
}

/* The indexed files declaration `d` can name under `view`: rustc reads
 * `<dir>/<name>.rs` (its own children then live under `<name>/`) or
 * `<dir>/<name>/mod.rs`, or the `#[path]` file (children beside it). */
static int rs_decl_cands(const rs_index_t *ix, const rs_decl_t *d, int view,
                         rs_cand_t out[PAIR_LEN]) {
    char dir[RS_PATH_BUF];
    char path[RS_PATH_BUF];
    int n = 0;
    if (d->cond_path || !rs_decl_dir(ix, d, view, dir, sizeof(dir))) {
        return 0;
    }
    if (d->path) {
        if (rs_join_path(dir, d->path, path, sizeof(path))) {
            n = rs_cand_add(ix, path, RS_VIEW_A, out, n);
        }
        return n;
    }
    const char *sep = dir[0] ? "/" : "";
    if (snprintf(path, sizeof(path), "%s%s%s.rs", dir, sep, d->name) < (int)sizeof(path)) {
        n = rs_cand_add(ix, path, RS_VIEW_B, out, n);
    }
    if (snprintf(path, sizeof(path), "%s%s%s/mod.rs", dir, sep, d->name) < (int)sizeof(path)) {
        n = rs_cand_add(ix, path, RS_VIEW_A, out, n);
    }
    return n;
}

typedef struct {
    int *v;
    int n;
} rs_stack_t;

static void rs_views_push(rs_index_t *ix, rs_stack_t *st, int fi, uint8_t bits) {
    rs_file_t *f = &ix->files[fi];
    if ((f->views | bits) == f->views) {
        return;
    }
    f->views |= bits;
    if (!f->queued) {
        f->queued = true;
        st->v[st->n++] = fi;
    }
}

/* Propagate views until nothing changes. A file is processed once per view it
 * gains, so the work is linear in the declarations. */
static void rs_views_run(rs_index_t *ix, rs_stack_t *st) {
    while (st->n > 0) {
        int fi = st->v[--st->n];
        rs_file_t *f = &ix->files[fi];
        f->queued = false;
        uint8_t todo = f->views & (uint8_t)~f->views_done;
        f->views_done |= todo;
        for (int view = RS_VIEW_A; view <= RS_VIEW_B; view++) {
            for (int d = f->decl_first; (todo & view) && d < f->decl_first + f->decl_count; d++) {
                rs_cand_t c[PAIR_LEN];
                int n = rs_decl_cands(ix, &ix->decls[d], view, c);
                for (int k = 0; k < n; k++) {
                    rs_views_push(ix, st, c[k].file, c[k].implies);
                }
            }
        }
    }
}

/* The one file declaration `d` names under the views of its file, or
 * CBM_NOT_FOUND when there is none or more than one. */
static int rs_decl_file(const rs_index_t *ix, const rs_decl_t *d) {
    const rs_file_t *f = &ix->files[ix->mods[d->mod].file];
    int found = CBM_NOT_FOUND;
    for (int view = RS_VIEW_A; view <= RS_VIEW_B; view++) {
        rs_cand_t c[PAIR_LEN];
        int n = (f->views & view) ? rs_decl_cands(ix, d, view, c) : 0;
        for (int k = 0; k < n; k++) {
            if (found >= 0 && found != c[k].file) {
                return CBM_NOT_FOUND;
            }
            found = c[k].file;
        }
    }
    return found;
}

static int rs_attach_cmp(const void *a, const void *b) {
    const rs_attach_t *x = (const rs_attach_t *)a;
    const rs_attach_t *y = (const rs_attach_t *)b;
    if (x->child != y->child) {
        return x->child < y->child ? -1 : 1;
    }
    return (x->decl > y->decl) - (x->decl < y->decl);
}

/* Decide which file every `mod x;` names.
 *
 * Where rustc looks depends on how the DECLARING file was reached: beside it
 * (view A: a crate root, a mod.rs, a `#[path]` file) or under a directory
 * named like it (view B: `name.rs` found by its parent). Views are propagated
 * from the crate roots to a fixed point; a file nothing reaches may be either,
 * and so may one reached both ways. Under both views a declaration names a
 * file only when exactly one indexed file fits. */
static void rs_module_tree(rs_index_t *ix) {
    rs_stack_t st = {.v = (int *)ix_alloc(ix, (size_t)ix->nfiles * sizeof(int))};
    ix->attach = (rs_attach_t *)ix_alloc(ix, (size_t)ix->ndecls * sizeof(rs_attach_t));
    if (!st.v || !ix->attach) {
        return;
    }
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const rs_file_t *f = &ix->files[fi];
        bool beside = f->crate_root || strcmp(rs_basename(f->rel_path), "mod.rs") == 0;
        if (f->kind == RS_FILE_SOURCE && beside) {
            rs_views_push(ix, &st, fi, RS_VIEW_A);
        }
    }
    rs_views_run(ix, &st);
    for (int fi = 0; fi < ix->nfiles; fi++) {
        if (ix->files[fi].kind == RS_FILE_SOURCE && ix->files[fi].views == 0) {
            rs_views_push(ix, &st, fi, RS_VIEW_A | RS_VIEW_B);
        }
    }
    rs_views_run(ix, &st);
    for (int d = 0; d < ix->ndecls; d++) {
        int fi = rs_decl_file(ix, &ix->decls[d]);
        if (fi >= 0) {
            ix->decls[d].child = ix->files[fi].mod_base;
            ix->attach[ix->nattach++] = (rs_attach_t){.child = ix->files[fi].mod_base, .decl = d};
        }
    }
    if (ix->nattach > SKIP_ONE) {
        qsort(ix->attach, (size_t)ix->nattach, sizeof(rs_attach_t), rs_attach_cmp);
    }
    for (int i = 0; i < ix->nattach; i++) {
        rs_mod_t *child = &ix->mods[ix->attach[i].child];
        if (child->attach_count == 0) {
            child->attach_first = i;
        }
        child->attach_count++;
    }
}

/* ── Crate roots of every file, product code ─────────────────────── */

static int rs_pkg_join(int a, int b) {
    if (a == RS_PKG_NONE || a == b) {
        return b;
    }
    return b == RS_PKG_NONE ? a : RS_PKG_CONFLICT;
}

static bool rs_roots_add(rs_file_t *f, int root) {
    for (int i = 0; i < f->nroots; i++) {
        if (f->roots[i] == root) {
            return false;
        }
    }
    if (f->nroots >= RS_ROOTS_KEPT) {
        bool changed = !f->roots_top;
        f->roots_top = true;
        return changed;
    }
    f->roots[f->nroots++] = root;
    return true;
}

/* What file `c` takes over from a file that declares it. true on a change. */
static bool rs_file_inherit(rs_file_t *c, const rs_file_t *p) {
    bool changed = p->roots_top && !c->roots_top;
    c->roots_top = c->roots_top || p->roots_top;
    for (int i = 0; i < p->nroots; i++) {
        changed = rs_roots_add(c, p->roots[i]) || changed;
    }
    int pkg = rs_pkg_join(c->pkg, p->pkg);
    changed = changed || pkg != c->pkg || (p->in_lib && !c->in_lib);
    c->pkg = pkg;
    c->in_lib = c->in_lib || p->in_lib;
    return changed;
}

/* The crate roots, the package and the lib membership of every file: a
 * fixed point over the `mod x;` edges. Each value only grows and is bounded,
 * so a file is revisited a bounded number of times. */
static void rs_file_roots(rs_index_t *ix) {
    rs_stack_t st = {.v = (int *)ix_alloc(ix, (size_t)ix->nfiles * sizeof(int))};
    if (!st.v) {
        return;
    }
    for (int fi = 0; fi < ix->nfiles; fi++) {
        rs_file_t *f = &ix->files[fi];
        if (f->kind != RS_FILE_SOURCE || !f->crate_root) {
            continue;
        }
        f->roots[f->nroots++] = f->mod_base;
        f->pkg = f->root_pkg;
        f->in_lib = f->root_pkg >= 0 && ix->pkgs[f->root_pkg].lib_root == f->mod_base;
        f->queued = true;
        st.v[st.n++] = fi;
    }
    while (st.n > 0) {
        int fi = st.v[--st.n];
        rs_file_t *f = &ix->files[fi];
        f->queued = false;
        for (int d = f->decl_first; d < f->decl_first + f->decl_count; d++) {
            int child = ix->decls[d].child;
            if (child < 0) {
                continue;
            }
            rs_file_t *c = &ix->files[ix->mods[child].file];
            if (rs_file_inherit(c, f) && !c->queued) {
                c->queued = true;
                st.v[st.n++] = ix->mods[child].file;
            }
        }
    }
}

/* A file no crate root reaches still lies in some package's directory: its
 * dependencies are that package's. Which target it would belong to (and so
 * what `crate` is) stays unknown. */
static void rs_orphan_packages(rs_index_t *ix) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        rs_file_t *f = &ix->files[fi];
        char dir[RS_PATH_BUF];
        const char *base = rs_basename(f->rel_path);
        size_t dl = base > f->rel_path ? (size_t)(base - f->rel_path) - SKIP_ONE : 0;
        if (f->kind != RS_FILE_SOURCE || f->nroots > 0 || f->pkg != RS_PKG_NONE ||
            dl >= sizeof(dir)) {
            continue;
        }
        memcpy(dir, f->rel_path, dl);
        dir[dl] = '\0';
        for (;;) {
            int q = rs_pkg_at(ix, dir);
            if (q >= 0 && ix->pkgs[q].has_package) {
                f->pkg = q;
                break;
            }
            char *slash = strrchr(dir, '/');
            if (!dir[0]) {
                break;
            }
            dir[slash ? (size_t)(slash - dir) : 0] = '\0';
        }
    }
}

static void rs_product_seed(rs_index_t *ix, rs_stack_t *st, int fi) {
    const rs_file_t *f = &ix->files[fi];
    rs_mod_t *root = &ix->mods[f->mod_base];
    if (root->product || f->test_path) {
        return;
    }
    root->product = true;
    st->v[st->n++] = fi;
}

/* Product code: what is reached from a crate root (or an orphan file) outside
 * the test directories without passing a `#[cfg(test)]` module or
 * declaration. Everything else is test-only. */
static void rs_product(rs_index_t *ix) {
    rs_stack_t st = {.v = (int *)ix_alloc(ix, (size_t)ix->nfiles * sizeof(int))};
    if (!st.v) {
        return;
    }
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const rs_file_t *f = &ix->files[fi];
        if (f->kind != RS_FILE_SOURCE) {
            continue;
        }
        bool orphan = !f->crate_root && ix->mods[f->mod_base].attach_count == 0;
        ix->orphans += orphan;
        if (f->crate_root || orphan) {
            rs_product_seed(ix, &st, fi);
        }
    }
    while (st.n > 0) {
        const rs_file_t *f = &ix->files[st.v[--st.n]];
        for (int m = SKIP_ONE; m < f->nmods; m++) {
            rs_mod_t *mod = &ix->mods[f->mod_base + m];
            mod->product = mod->parent >= 0 && ix->mods[mod->parent].product && !mod->cfg_test;
        }
        for (int d = f->decl_first; d < f->decl_first + f->decl_count; d++) {
            const rs_decl_t *decl = &ix->decls[d];
            if (decl->child >= 0 && !decl->cfg_test && ix->mods[decl->mod].product) {
                rs_product_seed(ix, &st, ix->mods[decl->child].file);
            }
        }
    }
}

/* ── Lookup: state ───────────────────────────────────────────────── */

typedef struct {
    const rs_index_t *ix;
    int pass;     /* while imports are resolved: one done in an earlier pass may be read;
                   * 0: all are done */
    bool assoc;   /* associated items are looked up (not while the index is built) */
    bool blocked; /* an import the answer depends on is not resolved yet */
} rs_look_t;

/* Where a path is written. */
typedef struct {
    rs_look_t *lk;
    int mod;      /* the module */
    int root;     /* the crate root module it is resolved under; CBM_NOT_FOUND: not known */
    int pkg;      /* that crate's package; CBM_NOT_FOUND: not known */
    bool own_lib; /* the package's lib crate is in the extern prelude */
    int edition;
    int skip; /* the import being resolved; CBM_NOT_FOUND */
} rs_at_t;

/* A path as plain segments (no raw-identifier markers). */
typedef struct {
    const char *const *segs;
    int n;
    bool global; /* written with a leading `::` */
} rs_path_t;

/* What a path names. */
typedef struct {
    rs_set_t ns[RS_NS_COUNT]; /* the last segment, per namespace */
    rs_set_t owners;          /* what the last segment was looked up in */
    bool ambiguous;           /* a segment before the last names several things */
} rs_found_t;

static bool rs_is(const char *s, const char *word) {
    return strcmp(s, word) == 0;
}

static bool rs_import_ready(const rs_look_t *lk, const rs_import_t *imp) {
    return imp->state == RS_STATE_DONE && (lk->pass == 0 || imp->done_pass < lk->pass);
}

/* First binding in binds[lo, hi) that is not below `name` (`upper`: that is
 * above it). */
static int rs_bind_bound(const rs_index_t *ix, int lo, int hi, const char *name, bool upper) {
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        int c = strcmp(ix->binds[mid].name, name);
        if (c < 0 || (upper && c == 0)) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* The bindings of module `m` named `name`: their count, the first in *first. */
static int rs_bind_range(const rs_index_t *ix, int m, const char *name, int *first) {
    const rs_mod_t *mod = &ix->mods[m];
    int end = mod->bind_first + mod->bind_count;
    *first = rs_bind_bound(ix, mod->bind_first, end, name, false);
    return rs_bind_bound(ix, *first, end, name, true) - *first;
}

static int rs_member_bound(const rs_index_t *ix, int lo, int hi, const char *name, bool upper) {
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        int c = strcmp(ix->members[mid].name, name);
        if (c < 0 || (upper && c == 0)) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* The members named `name` among members[first, first + count) (sorted by
 * name) whose kind is one of `kinds` and whose namespace is in `ns`. */
static void rs_owner_members(const rs_index_t *ix, int first, int count, const char *name, int ns,
                             const char *kinds, rs_set_t *out) {
    int lo = rs_member_bound(ix, first, first + count, name, false);
    int hi = rs_member_bound(ix, lo, first + count, name, true);
    for (int i = lo; i < hi && !out->overflow; i++) {
        const rs_member_t *m = &ix->members[i];
        if ((rs_member_ns(m->kind) & ns) && strchr(kinds, m->kind)) {
            rs_set_add(out, rs_ent(RS_E_MEMBER, i));
        }
    }
}

/* ── Lookup: module tree questions ───────────────────────────────── */

/* The modules `super` names in module `m`: the inline parent, or every module
 * that declares the file. An orphan file's parent is not known. */
static void rs_parents(const rs_index_t *ix, int m, rs_set_t *out) {
    const rs_mod_t *mod = &ix->mods[m];
    if (mod->parent >= 0) {
        rs_set_add(out, rs_ent(RS_E_MOD, mod->parent));
        return;
    }
    if (mod->attach_count > RS_SET_CAP ||
        (mod->attach_count == 0 && !ix->files[mod->file].crate_root)) {
        rs_set_add(out, rs_weak(RS_E_GAP));
        return;
    }
    for (int i = 0; i < mod->attach_count; i++) {
        rs_set_add(out, rs_ent(RS_E_MOD, ix->decls[ix->attach[mod->attach_first + i].decl].mod));
    }
}

/* Is module `m` the module `ancestor` or inside it? 1 yes, 0 no, -1 not
 * known (an orphan on the way up, or too far to walk). */
static int rs_mod_within(const rs_index_t *ix, int m, int ancestor) {
    int stack[RS_SET_CAP];
    int sp = 0;
    bool unknown = false;
    stack[sp++] = m;
    for (int steps = 0; sp > 0; steps++) {
        int cur = stack[--sp];
        if (cur == ancestor) {
            return SKIP_ONE;
        }
        const rs_mod_t *mod = &ix->mods[cur];
        if (steps >= RS_UP_STEPS || (mod->parent < 0 && mod->attach_count > RS_SET_CAP - sp)) {
            return CBM_NOT_FOUND;
        }
        if (mod->parent >= 0) {
            stack[sp++] = mod->parent;
            continue;
        }
        unknown = unknown || (mod->attach_count == 0 && !ix->files[mod->file].crate_root);
        for (int i = 0; i < mod->attach_count; i++) {
            stack[sp++] = ix->decls[ix->attach[mod->attach_first + i].decl].mod;
        }
    }
    return unknown ? CBM_NOT_FOUND : 0;
}

/* Are two files part of one crate? 1 yes, 0 no, -1 not known. */
static int rs_same_crate(const rs_index_t *ix, int fa, int fb) {
    const rs_file_t *a = &ix->files[fa];
    const rs_file_t *b = &ix->files[fb];
    if (fa == fb) {
        return SKIP_ONE;
    }
    if (a->roots_top || b->roots_top || a->nroots == 0 || b->nroots == 0) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < a->nroots; i++) {
        for (int j = 0; j < b->nroots; j++) {
            if (a->roots[i] == b->roots[j]) {
                return SKIP_ONE;
            }
        }
    }
    return 0;
}

/* Can module `from` see a binding of visibility `vis` declared in module
 * `owner`? 1 yes, 0 no, -1 not known. */
static int rs_visible(const rs_index_t *ix, char vis, int owner, int from) {
    if (vis == 'p' || owner == from) {
        return SKIP_ONE;
    }
    if (vis == 'c') {
        return rs_same_crate(ix, ix->mods[owner].file, ix->mods[from].file);
    }
    if (vis == '-') {
        return rs_mod_within(ix, from, owner);
    }
    if (vis != 's') {
        return CBM_NOT_FOUND; /* pub(in path): the path is not carried */
    }
    rs_set_t up = {0};
    rs_parents(ix, owner, &up);
    int best = up.n == 0 ? rs_mod_within(ix, from, owner) : 0;
    for (int i = 0; i < up.n && best <= 0; i++) {
        int v = up.v[i].kind == RS_E_MOD ? rs_mod_within(ix, from, up.v[i].idx) : CBM_NOT_FOUND;
        best = (v > 0 || best == 0) ? v : best;
    }
    return best;
}

/* ── Lookup: names a macro's body declares ───────────────────────── */

static int rs_macuse_cmp(const void *a, const void *b) {
    const rs_macuse_t *x = (const rs_macuse_t *)a;
    const rs_macuse_t *y = (const rs_macuse_t *)b;
    if (x->mod != y->mod) {
        return x->mod < y->mod ? -1 : 1;
    }
    return strcmp(x->macro, y->macro);
}

static int rs_macdecl_cmp(const void *a, const void *b) {
    const rs_macdecl_t *x = (const rs_macdecl_t *)a;
    const rs_macdecl_t *y = (const rs_macdecl_t *)b;
    int c = strcmp(x->name, y->name);
    return c ? c : strcmp(x->macro, y->macro);
}

/* True when module `m` invokes a macro named `macro` among its items. */
static bool rs_mod_invokes(const rs_index_t *ix, int m, const char *macro) {
    rs_macuse_t key = {.mod = m, .macro = macro};
    int lo = 0;
    int hi = ix->nmacuses;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (rs_macuse_cmp(&ix->macuses[mid], &key) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < ix->nmacuses && rs_macuse_cmp(&ix->macuses[lo], &key) == 0;
}

/* May a macro invoked in module `m` declare `name` there? True when the body
 * of some `macro_rules!` macro of the repository declares that name and the
 * module invokes a macro of that macro's name. Macros are matched by name
 * only (which macro an invocation means is not resolved), so this can say
 * "maybe" too often, never too rarely. */
static bool rs_macro_may_declare(const rs_index_t *ix, int m, const char *name) {
    int lo = 0;
    int hi = ix->nmacdecls;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (strcmp(ix->macdecls[mid].name, name) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < ix->nmacdecls && rs_is(ix->macdecls[i].name, name); i++) {
        if (i - lo >= RS_GLOB_VISIT || rs_mod_invokes(ix, m, ix->macdecls[i].macro)) {
            return true; /* declared by more macros than are checked: maybe */
        }
    }
    return false;
}

/* ── Lookup: level 1, a module's own bindings ────────────────────── */

typedef struct {
    int skip; /* an import that is not consulted; CBM_NOT_FOUND */
    int from; /* a glob import of this module asks: only what it can see;
               * CBM_NOT_FOUND: everything */
} rs_own_opt_t;

/* What binding `b` is in namespace `ns`; RS_E_NONE when it is nothing there.
 * `glob`: asked through a glob import, which a textual macro does not pass. */
static rs_ent_t rs_bind_ent(rs_look_t *lk, const rs_bind_t *b, int ns, bool glob) {
    const rs_index_t *ix = lk->ix;
    rs_ent_t none = {0};
    if (b->kind == RS_BIND_ITEM) {
        const rs_item_t *it = &ix->items[b->ref];
        bool textual = it->kind == 'M' && !it->macro_export;
        return ((rs_item_ns(it->kind) & ns) && !(glob && textual)) ? rs_ent(RS_E_ITEM, b->ref)
                                                                   : none;
    }
    if (b->kind == RS_BIND_CHILD) {
        if (ns != RS_NS_TYPE) {
            return none;
        }
        return b->ref >= 0 ? rs_ent(RS_E_MOD, b->ref) : rs_ent(RS_E_GAP, 0);
    }
    const rs_import_t *imp = &ix->imports[b->ref];
    if (!rs_import_ready(lk, imp)) {
        lk->blocked = true;
        return none;
    }
    bool type_only = imp->kind != 'b';
    if (type_only && ns != RS_NS_TYPE) {
        return none;
    }
    rs_ent_t e = imp->res[type_only ? 0 : rs_ns_index(ns)];
    e.via_import = e.kind != RS_E_NONE;
    return e;
}

/* Level 1: what module `m` itself binds as `name` in namespace `ns`. A name
 * it does not show may still be declared there when a macro invocation could
 * declare it or the file did not parse: that is a gap, not an absence. */
static void rs_own(rs_look_t *lk, int m, const char *name, int ns, const rs_own_opt_t *opt,
                   rs_set_t *out) {
    const rs_index_t *ix = lk->ix;
    int first = 0;
    int n = rs_bind_range(ix, m, name, &first);
    bool maybe = false;
    rs_set_t vague = {0}; /* imports of something whose namespace is not known */
    for (int i = first; i < first + n && !out->overflow; i++) {
        const rs_bind_t *b = &ix->binds[i];
        if (b->kind == RS_BIND_MAYBE) {
            maybe = true;
            continue;
        }
        if (b->kind == RS_BIND_IMPORT && b->ref == opt->skip) {
            continue;
        }
        /* visibility first: a binding the asking module cannot see is not
         * looked at, so an import of its own that is not resolved yet does
         * not hold the answer up */
        int v = opt->from >= 0 ? rs_visible(ix, b->vis, m, opt->from) : SKIP_ONE;
        if (v == 0) {
            continue;
        }
        rs_ent_t e = rs_bind_ent(lk, b, ns, opt->from >= 0);
        if (e.kind == RS_E_NONE) {
            continue;
        }
        bool spread = b->kind == RS_BIND_IMPORT && ix->imports[b->ref].spread;
        rs_set_add(spread ? &vague : out, v > 0 ? e : rs_weak(RS_E_GAP));
    }
    if (rs_set_empty(out)) {
        /* nothing the module really has in this namespace: the import may be it */
        rs_set_merge(out, &vague);
    }
    if (rs_set_empty(out) &&
        (maybe || ix->files[ix->mods[m].file].broken || rs_macro_may_declare(ix, m, name))) {
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* ── Lookup: level 2, glob imports ───────────────────────────────── */

static bool rs_int_in(const int *v, int n, int x) {
    for (int i = 0; i < n; i++) {
        if (v[i] == x) {
            return true;
        }
    }
    return false;
}

/* What a walk through glob imports met besides bindings. */
typedef struct {
    bool open;    /* a glob of something outside the repository: it may hold the name */
    bool gap;     /* a glob that cannot be followed, or a binding of unknown visibility */
    bool pending; /* a glob that is not resolved yet (only while imports are resolved) */
} rs_glob_flags_t;

/* One glob import `imp` of module `cur`, asked for `name` on behalf of module
 * `m`. Returns the module whose own globs are to be asked next (the target
 * does not bind the name itself), or CBM_NOT_FOUND. */
static int rs_glob_step(rs_look_t *lk, const rs_import_t *imp, int cur, const rs_own_opt_t *opt,
                        const char *name, int ns, rs_set_t *out, rs_glob_flags_t *fl) {
    const rs_index_t *ix = lk->ix;
    int m = opt->from;
    int v = cur == m ? SKIP_ONE : rs_visible(ix, imp->vis, cur, m);
    if (v <= 0) {
        fl->gap = fl->gap || v < 0;
        return CBM_NOT_FOUND;
    }
    if (!rs_import_ready(lk, imp)) {
        fl->pending = true;
        return CBM_NOT_FOUND;
    }
    rs_ent_t t = imp->res[0];
    if (t.kind == RS_E_MOD) {
        rs_set_t found = {0};
        rs_own(lk, t.idx, name, ns, opt, &found);
        for (int i = 0; i < found.n; i++) {
            if (found.v[i].weak) {
                fl->gap = true;
            } else {
                rs_set_add(out, found.v[i]);
            }
        }
        out->overflow = out->overflow || found.overflow;
        return rs_set_empty(&found) ? t.idx : CBM_NOT_FOUND;
    }
    if (t.kind == RS_E_ITEM && ix->items[t.idx].kind == 'E') {
        const rs_item_t *en = &ix->items[t.idx];
        rs_owner_members(ix, en->member_first, en->member_count, name, ns, "v", out);
        return CBM_NOT_FOUND;
    }
    fl->open = fl->open || t.kind == RS_E_EXTERNAL;
    fl->gap = fl->gap || t.kind != RS_E_EXTERNAL;
    return CBM_NOT_FOUND;
}

/* Level 2: what the glob imports of module `m` bring in as `name`, through
 * re-exporting globs. `skip`: the import being resolved, which is neither
 * asked as a glob nor read as a binding (it cannot supply its own name).
 * Anything that cannot be followed is a gap. */
static void rs_globs(rs_look_t *lk, int m, const char *name, int ns, int skip, rs_set_t *out,
                     rs_glob_flags_t *fl) {
    const rs_index_t *ix = lk->ix;
    rs_own_opt_t opt = {.skip = skip, .from = m};
    int seen[RS_GLOB_VISIT]; /* also the queue: seen[next, nseen) is still to be asked */
    int nseen = 0;
    int next = 0;
    int steps = 0;
    seen[nseen++] = m;
    while (next < nseen && !out->overflow) {
        int cur = seen[next++];
        const rs_mod_t *cm = &ix->mods[cur];
        for (int g = 0; g < cm->glob_count; g++) {
            int gi = ix->globs[cm->glob_first + g].ref;
            if (gi == skip) {
                continue;
            }
            if (++steps > RS_UP_STEPS) {
                fl->gap = true;
                break;
            }
            int into = rs_glob_step(lk, &ix->imports[gi], cur, &opt, name, ns, out, fl);
            if (into < 0 || rs_int_in(seen, nseen, into)) {
                continue;
            }
            if (nseen >= RS_GLOB_VISIT) {
                fl->gap = true;
                continue;
            }
            seen[nseen++] = into;
        }
    }
    if (rs_set_empty(out) && fl->gap) {
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* Levels 1 and 2 of module `m`: what `m::name` names. A glob that is not
 * resolved yet may still bring the name in: the answer has to wait. */
static void rs_mod_member(rs_look_t *lk, int m, const char *name, int ns, int skip, rs_set_t *out) {
    rs_own_opt_t opt = {.skip = skip, .from = CBM_NOT_FOUND};
    rs_glob_flags_t fl = {0};
    rs_own(lk, m, name, ns, &opt, out);
    if (!rs_set_empty(out)) {
        return;
    }
    rs_globs(lk, m, name, ns, skip, out, &fl);
    bool real = out->n > 0 && !out->v[0].weak;
    if (fl.pending && !real) {
        memset(out, 0, sizeof(*out));
        lk->blocked = true;
        return;
    }
    if (rs_set_empty(out) && fl.open) {
        rs_set_add(out, rs_weak(RS_E_EXTERNAL));
    }
}

/* ── Lookup: levels 3 and 4, the preludes ────────────────────────── */

/* Defined by the language and its standard library, not by any corpus. */
static const char *const RS_BUILTIN_CRATES[] = {"std", "core", "alloc", "proc_macro", "test"};

/* The std prelude, type namespace (all editions; a name of a later edition's
 * prelude is still the standard library's). */
static const char *const RS_PRELUDE_TYPES[] = {"Option",
                                               "Result",
                                               "Vec",
                                               "String",
                                               "Box",
                                               "Some",
                                               "None",
                                               "Ok",
                                               "Err",
                                               "Send",
                                               "Sized",
                                               "Sync",
                                               "Unpin",
                                               "Copy",
                                               "Drop",
                                               "Fn",
                                               "FnMut",
                                               "FnOnce",
                                               "AsyncFn",
                                               "AsyncFnMut",
                                               "AsyncFnOnce",
                                               "AsMut",
                                               "AsRef",
                                               "From",
                                               "Into",
                                               "Clone",
                                               "Default",
                                               "Eq",
                                               "Ord",
                                               "PartialEq",
                                               "PartialOrd",
                                               "Extend",
                                               "IntoIterator",
                                               "Iterator",
                                               "DoubleEndedIterator",
                                               "ExactSizeIterator",
                                               "ToOwned",
                                               "ToString",
                                               "TryFrom",
                                               "TryInto",
                                               "FromIterator",
                                               "Future",
                                               "IntoFuture"};

static const char *const RS_PRELUDE_VALUES[] = {
    "Some", "None", "Ok", "Err", "drop", "size_of", "size_of_val", "align_of", "align_of_val"};

/* std's macros, and the derive and attribute macros of the prelude. */
static const char *const RS_STD_MACROS[] = {"assert",
                                            "assert_eq",
                                            "assert_ne",
                                            "cfg",
                                            "column",
                                            "compile_error",
                                            "concat",
                                            "dbg",
                                            "debug_assert",
                                            "debug_assert_eq",
                                            "debug_assert_ne",
                                            "env",
                                            "eprint",
                                            "eprintln",
                                            "file",
                                            "format",
                                            "format_args",
                                            "include",
                                            "include_bytes",
                                            "include_str",
                                            "is_x86_feature_detected",
                                            "line",
                                            "matches",
                                            "module_path",
                                            "option_env",
                                            "panic",
                                            "print",
                                            "println",
                                            "stringify",
                                            "thread_local",
                                            "todo",
                                            "try",
                                            "unimplemented",
                                            "unreachable",
                                            "vec",
                                            "write",
                                            "writeln",
                                            "Clone",
                                            "Copy",
                                            "Debug",
                                            "Default",
                                            "Eq",
                                            "Hash",
                                            "Ord",
                                            "PartialEq",
                                            "PartialOrd",
                                            "derive",
                                            "test",
                                            "bench",
                                            "global_allocator"};

/* rustdoc's names for primitive types (resolve_primitive). */
static const char *const RS_PRIMITIVES[] = {
    "isize",  "i8",    "i16",       "i32",  "i64",   "i128",  "usize", "u8",   "u16",
    "u32",    "u64",   "u128",      "f16",  "f32",   "f64",   "f128",  "char", "bool",
    "true",   "false", "str",       "&str", "slice", "array", "tuple", "unit", "pointer",
    "*const", "*mut",  "reference", "&",    "&mut",  "fn",    "never", "!"};

static bool rs_std_name(const char *name, int ns) {
    if (ns == RS_NS_TYPE) {
        return RS_IN(RS_PRELUDE_TYPES, name);
    }
    return ns == RS_NS_VALUE ? RS_IN(RS_PRELUDE_VALUES, name) : RS_IN(RS_STD_MACROS, name);
}

/* Level 3: the extern prelude of the crate the path is written in. `*unknown`:
 * that crate's dependencies are not known, so a miss says nothing. */
static void rs_extern(const rs_at_t *at, const char *name, rs_set_t *out, bool *unknown) {
    const rs_index_t *ix = at->lk->ix;
    if (RS_IN(RS_BUILTIN_CRATES, name)) {
        rs_set_add(out, rs_ent(RS_E_EXTERNAL, 0));
        return;
    }
    if (at->pkg < 0) {
        *unknown = true;
        return;
    }
    const rs_pkg_t *p = &ix->pkgs[at->pkg];
    if (p->lib_root >= 0 && p->lib_name && rs_is(p->lib_name, name)) {
        /* the package's own lib: in scope in its other targets only, and which
         * target a file without a crate root belongs to is not known */
        if (at->own_lib) {
            rs_set_add(out, rs_ent(RS_E_MOD, p->lib_root));
        }
        *unknown = !at->own_lib && at->root < 0;
        if (at->own_lib || *unknown) {
            return;
        }
    }
    int lo = 0;
    int hi = p->ndeps;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (strcmp(p->deps[mid].key, name) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    if (lo < p->ndeps && rs_is(p->deps[lo].key, name)) {
        rs_set_add(out, p->deps[lo].target);
        return;
    }
    *unknown = p->broken;
}

/* A `macro_rules!` macro is in textual scope in the modules below the one
 * that defines it. */
static void rs_textual_macro(const rs_index_t *ix, int m, const char *name, rs_set_t *out) {
    int cur = m;
    for (int step = 0; step < RS_UP_STEPS; step++) {
        const rs_mod_t *mod = &ix->mods[cur];
        int up = mod->parent;
        if (up < 0 && mod->attach_count == SKIP_ONE) {
            up = ix->decls[ix->attach[mod->attach_first].decl].mod;
        }
        if (up < 0) {
            return;
        }
        int first = 0;
        int n = rs_bind_range(ix, up, name, &first);
        for (int i = first; i < first + n; i++) {
            const rs_bind_t *b = &ix->binds[i];
            if (b->kind == RS_BIND_ITEM && ix->items[b->ref].kind == 'M') {
                rs_set_add(out, rs_ent(RS_E_ITEM, b->ref));
                return;
            }
        }
        cur = up;
    }
    rs_set_add(out, rs_weak(RS_E_GAP)); /* deeper than is walked: not known */
}

/* The lexical lookup of a name: levels 1 to 4, the first that binds it in the
 * namespace.
 *
 * Level 2 can hold a name without showing it: a glob of something outside the
 * repository, a glob that cannot be followed, and (while imports are being
 * resolved) a glob that is not resolved yet. What then follows depends on
 * whose path it is:
 *   - an import path may look on. rustc rejects an import path whose first
 *     name is brought in by a glob AND bound further out (E0659), so in
 *     source that compiles a name found at level 3 or 4 is not in the glob;
 *   - any other path (a doc link, a type) may not: there a glob's name
 *     silently wins over the preludes. The lookup ends with a weak answer.
 * A name found nowhere waits for an unresolved glob, which may still bring
 * it in. */
static void rs_lexical(const rs_at_t *at, const char *name, int ns, rs_set_t *out) {
    rs_look_t *lk = at->lk;
    rs_own_opt_t opt = {.skip = at->skip, .from = CBM_NOT_FOUND};
    rs_glob_flags_t fl = {0};
    rs_set_t outer = {0};
    bool unknown = false;
    rs_own(lk, at->mod, name, ns, &opt, out);
    if (rs_set_empty(out) && ns == RS_NS_MACRO) {
        rs_textual_macro(lk->ix, at->mod, name, out);
    }
    if (!rs_set_empty(out)) {
        if (out->v[0].weak && at->skip >= 0 && ns == RS_NS_TYPE) {
            /* an import path in a module that may declare more than it shows:
             * a crate of the extern prelude stands all the same, because an
             * import path whose first name is both a local name and a crate
             * is an error (E0659) */
            rs_extern(at, name, &outer, &unknown);
            if (!rs_set_empty(&outer)) {
                *out = outer;
            }
        }
        return;
    }
    rs_globs(lk, at->mod, name, ns, at->skip, out, &fl);
    if (out->n > 0 && !out->v[0].weak) {
        return; /* brought in by a glob that is resolved */
    }
    bool hidden = fl.open || fl.pending || !rs_set_empty(out);
    if (!hidden || at->skip >= 0) {
        if (ns == RS_NS_TYPE) {
            rs_extern(at, name, &outer, &unknown);
        }
        if (rs_set_empty(&outer) && rs_std_name(name, ns)) {
            rs_set_add(&outer, rs_ent(RS_E_EXTERNAL, 0));
        }
        if (!rs_set_empty(&outer)) {
            *out = outer;
            return;
        }
    }
    if (fl.pending) {
        memset(out, 0, sizeof(*out));
        lk->blocked = true;
        return;
    }
    if (rs_set_empty(out) && (fl.open || unknown)) {
        rs_set_add(out, rs_weak(fl.open ? RS_E_EXTERNAL : RS_E_GAP));
    }
    if (rs_set_empty(out) && at->skip >= 0 && ns == RS_NS_TYPE && at->pkg >= 0) {
        /* the first name of an import path, bound nowhere: the module shows
         * everything it declares (else the answer above was a gap), so what
         * is left is a crate of the extern prelude under a name the manifest
         * does not give (a lib target named unlike its package) */
        rs_set_add(out, rs_weak(RS_E_EXTERNAL));
    }
}

/* ── Lookup: associated items ────────────────────────────────────── */

/* Declarations of one entity under different cfg: two items of one module,
 * name and kind, or two members of one owner. */
static bool rs_cfg_twins(const rs_index_t *ix, rs_ent_t a, rs_ent_t b) {
    if (a.kind != b.kind) {
        return false;
    }
    if (a.kind == RS_E_ITEM) {
        const rs_item_t *x = &ix->items[a.idx];
        const rs_item_t *y = &ix->items[b.idx];
        return x->mod == y->mod && x->kind == y->kind && rs_is(x->name, y->name);
    }
    if (a.kind == RS_E_MEMBER) {
        const rs_member_t *x = &ix->members[a.idx];
        const rs_member_t *y = &ix->members[b.idx];
        return x->owner == y->owner && x->kind == y->kind && rs_is(x->name, y->name);
    }
    return false;
}

static bool rs_has_node(const rs_index_t *ix, rs_ent_t e) {
    if (e.kind == RS_E_ITEM) {
        return ix->items[e.idx].label != 0;
    }
    return e.kind == RS_E_MEMBER && ix->members[e.idx].label != 0;
}

/* The graph keeps one node per qualified name, so of two cfg twins the one
 * that owns the node stands for both. Two with a node each stay two. */
static void rs_collapse(const rs_index_t *ix, rs_set_t *s) {
    for (int i = 0; i < s->n; i++) {
        for (int j = i + SKIP_ONE; j < s->n;) {
            bool ni = rs_has_node(ix, s->v[i]);
            bool nj = rs_has_node(ix, s->v[j]);
            if (!rs_cfg_twins(ix, s->v[i], s->v[j]) || (ni && nj)) {
                j++;
                continue;
            }
            if (nj) {
                bool via = s->v[i].via_import || s->v[j].via_import;
                s->v[i] = s->v[j];
                s->v[i].via_import = via;
            }
            s->v[j] = s->v[--s->n];
        }
    }
}

/* The item a chain of type aliases ends at; an external or gap entity when it
 * leaves what is known. */
static rs_ent_t rs_alias_end(const rs_index_t *ix, int item) {
    rs_ent_t e = rs_ent(RS_E_ITEM, item);
    for (int hop = 0; hop < RS_ALIAS_HOPS; hop++) {
        if (ix->items[e.idx].kind != 'A') {
            return e;
        }
        e = ix->items[e.idx].alias_ent;
        if (e.kind != RS_E_ITEM) {
            return rs_weak(e.kind == RS_E_EXTERNAL ? RS_E_EXTERNAL : RS_E_GAP);
        }
    }
    return rs_weak(RS_E_GAP);
}

static int rs_assoc_bound(const rs_index_t *ix, int item, const char *name, bool upper) {
    int lo = 0;
    int hi = ix->nassoc;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const rs_assoc_t *a = &ix->assoc[mid];
        int c = a->item != item ? (a->item < item ? -1 : 1) : strcmp(a->name, name);
        if (c < 0 || (upper && c == 0)) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static const char *rs_last_segment(const char *path) {
    const char *last = path;
    for (const char *p = path; *p; p++) {
        if (p[0] == ':' && p[SKIP_ONE] == ':') {
            last = p + PAIR_LEN;
        }
    }
    return rs_plain(last);
}

/* Is the trait of impl `im` in scope in the module the path is written in?
 * rustdoc offers a trait impl's items only then. 1 yes, 0 no, -1 not known.
 * A trait outside the repository is known by its name only. */
static int rs_trait_in_scope(const rs_at_t *at, const rs_impl_t *im) {
    const rs_index_t *ix = at->lk->ix;
    rs_ent_t t = im->trait_ent;
    if (t.kind != RS_E_ITEM && t.kind != RS_E_EXTERNAL) {
        return CBM_NOT_FOUND;
    }
    const char *name =
        t.kind == RS_E_ITEM ? ix->items[t.idx].name : rs_last_segment(im->trait_path);
    rs_set_t found = {0};
    rs_lexical(at, name, RS_NS_TYPE, &found);
    if (rs_set_has(&found, t)) {
        return SKIP_ONE;
    }
    const rs_mod_t *mod = &ix->mods[at->mod];
    for (int i = 0; i < mod->named_count; i++) { /* imported under another name */
        const rs_import_t *imp = &ix->imports[ix->named[mod->named_first + i].ref];
        rs_ent_t e = imp->res[0];
        if (imp->kind != 'b' || e.kind != t.kind || imp->nsegs == 0) {
            continue;
        }
        if (t.kind == RS_E_ITEM ? e.idx == t.idx : rs_is(imp->segs[imp->nsegs - SKIP_ONE], name)) {
            return SKIP_ONE;
        }
    }
    return rs_set_has(&found, rs_ent(RS_E_GAP, 0)) ? CBM_NOT_FOUND : 0;
}

/* Items a trait declares and an impl of it for type `item` does not define
 * itself. Returns true when some could not be decided. */
static bool rs_trait_defaults(const rs_at_t *at, int item, const char *name, int ns,
                              rs_set_t *out) {
    const rs_index_t *ix = at->lk->ix;
    const rs_item_t *it = &ix->items[item];
    bool unsure = it->impl_count > RS_IMPL_SCAN;
    for (int i = 0; i < it->impl_count && i < RS_IMPL_SCAN && !out->overflow; i++) {
        const rs_impl_t *im = &ix->impls[ix->item_impls[it->impl_first + i].impl];
        if (!im->trait_path || im->trait_ent.kind != RS_E_ITEM) {
            continue;
        }
        const rs_item_t *tr = &ix->items[im->trait_ent.idx];
        rs_set_t own = {0};
        rs_set_t dflt = {0};
        rs_owner_members(ix, im->member_first, im->member_count, name, ns, "fct", &own);
        if (!rs_set_empty(&own) || tr->kind != 'R') {
            continue;
        }
        rs_owner_members(ix, tr->member_first, tr->member_count, name, ns, "fct", &dflt);
        if (rs_set_empty(&dflt)) {
            continue;
        }
        int v = rs_trait_in_scope(at, im);
        if (v > 0) {
            rs_set_merge(out, &dflt);
        }
        unsure = unsure || v < 0;
    }
    return unsure;
}

/* Two impls of one trait: the scope question has one answer for both. */
static bool rs_same_trait(const rs_impl_t *a, const rs_impl_t *b) {
    if (a->trait_ent.kind != b->trait_ent.kind) {
        return false;
    }
    if (a->trait_ent.kind == RS_E_ITEM) {
        return a->trait_ent.idx == b->trait_ent.idx;
    }
    return a->trait_ent.kind == RS_E_EXTERNAL &&
           rs_is(rs_last_segment(a->trait_path), rs_last_segment(b->trait_path));
}

/* The items of the impls of type `item` named `name`: inherent impls first,
 * then the trait impls whose trait is in scope where the path is written. */
static void rs_impl_items(const rs_at_t *at, int item, const char *name, int ns, rs_set_t *out) {
    const rs_index_t *ix = at->lk->ix;
    int first = rs_assoc_bound(ix, item, name, false);
    int end = rs_assoc_bound(ix, item, name, true);
    bool unsure = false;
    const rs_impl_t *asked = NULL; /* the impl whose trait was asked about last */
    int answer = 0;
    for (int i = first; i < end && !out->overflow; i++) {
        const rs_assoc_t *a = &ix->assoc[i];
        if (!ix->impls[a->impl].trait_path && (rs_member_ns(ix->members[a->member].kind) & ns)) {
            rs_set_add(out, rs_ent(RS_E_MEMBER, a->member));
        }
    }
    bool inherent = !rs_set_empty(out);
    for (int i = first; i < end && !inherent && !out->overflow; i++) {
        const rs_assoc_t *a = &ix->assoc[i];
        const rs_impl_t *im = &ix->impls[a->impl];
        if (!im->trait_path || !(rs_member_ns(ix->members[a->member].kind) & ns)) {
            continue;
        }
        if (!asked || !rs_same_trait(asked, im)) {
            asked = im;
            answer = rs_trait_in_scope(at, im);
        }
        if (answer > 0) {
            rs_set_add(out, rs_ent(RS_E_MEMBER, a->member));
        }
        unsure = unsure || answer < 0;
    }
    if (rs_set_empty(out)) {
        unsure = rs_trait_defaults(at, item, name, ns, out) || unsure;
    }
    if (rs_set_empty(out) && unsure) {
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* `Type::name`: a variant, an item of an impl, a field (rustdoc's order). */
static void rs_type_assoc(const rs_at_t *at, int item, const char *name, int ns, rs_set_t *out) {
    const rs_index_t *ix = at->lk->ix;
    rs_ent_t end = rs_alias_end(ix, item);
    if (end.kind != RS_E_ITEM) {
        rs_set_add(out, end);
        return;
    }
    const rs_item_t *it = &ix->items[end.idx];
    if (it->kind == 'R') {
        rs_owner_members(ix, it->member_first, it->member_count, name, ns, "fct", out);
        return;
    }
    if (it->kind != 'S' && it->kind != 'E' && it->kind != 'U') {
        return;
    }
    if (it->kind == 'E') {
        rs_owner_members(ix, it->member_first, it->member_count, name, ns, "v", out);
    }
    if (rs_set_empty(out)) {
        rs_impl_items(at, end.idx, name, ns, out);
    }
    if (rs_set_empty(out) && ns == RS_NS_VALUE) {
        rs_owner_members(ix, it->member_first, it->member_count, name, ns, "d", out);
    }
}

/* What `name` is inside item `item`. While the index is being built only an
 * enum's variants are known; anything else through a type is not followed. */
static void rs_item_member(const rs_at_t *at, int item, const char *name, int ns, rs_set_t *out) {
    const rs_index_t *ix = at->lk->ix;
    const rs_item_t *it = &ix->items[item];
    if (!rs_item_is_type(it->kind)) {
        return; /* a function, a constant, a macro: nothing is inside */
    }
    if (at->lk->assoc) {
        rs_type_assoc(at, item, name, ns, out);
        return;
    }
    if (it->kind == 'E') {
        rs_owner_members(ix, it->member_first, it->member_count, name, ns, "v", out);
    }
    if (rs_set_empty(out)) {
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* What `name` is inside `owner`, in namespace `ns`. */
static void rs_member(const rs_at_t *at, rs_ent_t owner, const char *name, int ns, rs_set_t *out) {
    const rs_index_t *ix = at->lk->ix;
    switch (owner.kind) {
    case RS_E_MOD:
        rs_mod_member(at->lk, owner.idx, name, ns, at->skip, out);
        break;
    case RS_E_ITEM:
        rs_item_member(at, owner.idx, name, ns, out);
        break;
    case RS_E_IMPL: {
        const rs_impl_t *im = &ix->impls[owner.idx];
        rs_owner_members(ix, im->member_first, im->member_count, name, ns, "fct", out);
        if (rs_set_empty(out)) {
            rs_set_add(out, rs_weak(im->self_ent.kind == RS_E_EXTERNAL ? RS_E_EXTERNAL : RS_E_GAP));
        }
        break;
    }
    case RS_E_MEMBER:
        if (ix->members[owner.idx].kind == 'v') {
            rs_set_add(out, rs_weak(RS_E_GAP)); /* a variant's fields are not carried */
        }
        break;
    case RS_E_EXTERNAL:
    case RS_E_GAP:
        rs_set_add(out, rs_weak(owner.kind));
        break;
    default:
        break;
    }
}

/* ── Lookup: a whole path ────────────────────────────────────────── */

/* The modules or the type that the keywords a path starts with name: `crate`,
 * `self`, `super` (repeatable), `Self`, or a leading `::`. Returns how many
 * segments they are; 0 when the path starts with a plain name. */
static int rs_path_head(const rs_at_t *at, const rs_path_t *p, rs_ent_t self, rs_set_t *cur) {
    const rs_index_t *ix = at->lk->ix;
    const char *first = p->segs[0];
    int i = SKIP_ONE;
    if (p->global) {
        bool unknown = false;
        rs_extern(at, first, cur, &unknown);
        if (rs_set_empty(cur) && (unknown || at->edition < RS_EDITION_2018)) {
            rs_set_add(cur, rs_weak(RS_E_GAP)); /* before 2018 `::name` is crate-relative */
        }
    } else if (rs_is(first, "crate")) {
        rs_set_add(cur, at->root >= 0 ? rs_ent(RS_E_MOD, at->root) : rs_weak(RS_E_GAP));
    } else if (rs_is(first, "self")) {
        rs_set_add(cur, rs_ent(RS_E_MOD, at->mod));
    } else if (rs_is(first, "super")) {
        rs_parents(ix, at->mod, cur);
    } else if (rs_is(first, "Self")) {
        rs_set_add(cur, self);
    } else {
        return 0;
    }
    for (; i < p->n && rs_is(p->segs[i], "super"); i++) {
        rs_set_t up = {0};
        for (int k = 0; k < cur->n; k++) {
            if (cur->v[k].kind == RS_E_MOD) {
                rs_parents(ix, cur->v[k].idx, &up);
            } else {
                rs_set_add(&up, rs_weak(RS_E_GAP));
            }
        }
        up.overflow = up.overflow || cur->overflow;
        *cur = up;
    }
    return i;
}

/* A name looked up from the crate root: an edition-2015 `use` path. */
static void rs_crate_relative(const rs_at_t *at, const char *name, int ns, rs_set_t *out) {
    bool unknown = false;
    if (ns == RS_NS_TYPE && RS_IN(RS_BUILTIN_CRATES, name)) {
        rs_set_add(out, rs_ent(RS_E_EXTERNAL, 0));
        return;
    }
    if (at->root < 0) {
        rs_set_add(out, rs_weak(RS_E_GAP));
        return;
    }
    rs_mod_member(at->lk, at->root, name, ns, at->skip, out);
    if (rs_set_empty(out) && ns == RS_NS_TYPE) {
        rs_extern(at, name, out, &unknown);
    }
    if (rs_set_empty(out) && unknown) {
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* The first segment of a path that starts with a plain name. A doc link, a
 * type path and an edition-2018 import look it up lexically; an edition-2015
 * import starts at the crate root. With the edition unknown both must agree. */
static void rs_first(const rs_at_t *at, const char *name, int ns, bool import, rs_set_t *out) {
    if (!import || at->edition >= RS_EDITION_2018) {
        rs_lexical(at, name, ns, out);
        return;
    }
    rs_set_t rooted = {0};
    rs_crate_relative(at, name, ns, &rooted);
    if (at->edition == RS_EDITION_2015) {
        *out = rooted;
        return;
    }
    rs_lexical(at, name, ns, out);
    if (!rs_set_equal(out, &rooted)) {
        memset(out, 0, sizeof(*out));
        rs_set_add(out, rs_weak(RS_E_GAP));
    }
}

/* One segment before the last: what `name` is, as a type or module, inside
 * each thing of `cur`. false when one of them holds several. */
static bool rs_walk_step(const rs_at_t *at, const rs_set_t *cur, const char *name, rs_set_t *next) {
    for (int k = 0; k < cur->n; k++) {
        rs_set_t one = {0};
        rs_member(at, cur->v[k], name, RS_NS_TYPE, &one);
        rs_collapse(at->lk->ix, &one);
        if (one.n > SKIP_ONE || one.overflow) {
            return false;
        }
        rs_set_merge(next, &one);
    }
    return true;
}

/* Resolve path `p` written at `at`: the candidates of its last segment in
 * every namespace of `mask`. */
static void rs_walk(const rs_at_t *at, const rs_path_t *p, int mask, rs_ent_t self, bool import,
                    rs_found_t *out) {
    rs_set_t cur = {0};
    int i = rs_path_head(at, p, self, &cur);
    if (i == 0 && p->n == SKIP_ONE) {
        for (int k = 0; k < RS_NS_COUNT; k++) {
            if (mask & RS_NS_LIST[k]) {
                rs_first(at, p->segs[0], RS_NS_LIST[k], import, &out->ns[k]);
            }
        }
        return;
    }
    if (i == 0) {
        rs_first(at, p->segs[0], RS_NS_TYPE, import, &cur);
        rs_collapse(at->lk->ix, &cur);
        if (cur.n > SKIP_ONE) {
            out->ambiguous = true;
            return;
        }
        if (rs_set_empty(&cur) && RS_IN(RS_PRIMITIVES, p->segs[0])) {
            rs_set_add(&cur, rs_ent(RS_E_EXTERNAL, 0)); /* `str::len`: of a primitive type */
        }
        i = SKIP_ONE;
    }
    if (cur.overflow) {
        out->ambiguous = true;
        return;
    }
    if (i == p->n) { /* the keywords are the whole path */
        if (mask & RS_NS_TYPE) {
            rs_set_merge(&out->ns[0], &cur);
        }
        return;
    }
    for (; i + SKIP_ONE < p->n && !rs_set_empty(&cur); i++) {
        rs_set_t next = {0};
        if (!rs_walk_step(at, &cur, p->segs[i], &next)) {
            out->ambiguous = true;
            return;
        }
        cur = next;
    }
    for (int k = 0; k < RS_NS_COUNT; k++) {
        for (int e = 0; (mask & RS_NS_LIST[k]) && e < cur.n; e++) {
            rs_member(at, cur.v[e], p->segs[p->n - SKIP_ONE], RS_NS_LIST[k], &out->ns[k]);
        }
    }
    out->owners = cur;
}

/* The contexts a path written in module `mod` is resolved under: one per
 * crate root of its file, or a single one without a root when the file has
 * none it can name (an orphan, or a file of more crates than are kept). */
static int rs_contexts(rs_look_t *lk, int mod, int skip, rs_at_t out[RS_ROOTS_KEPT]) {
    const rs_index_t *ix = lk->ix;
    const rs_file_t *f = &ix->files[ix->mods[mod].file];
    rs_at_t base = {
        .lk = lk, .mod = mod, .root = CBM_NOT_FOUND, .pkg = CBM_NOT_FOUND, .skip = skip};
    if (f->nroots == 0 || f->roots_top) {
        base.pkg = f->pkg >= 0 ? f->pkg : CBM_NOT_FOUND;
        base.own_lib = base.pkg >= 0 && f->roots_top && !f->in_lib;
        base.edition = rs_edition(ix, base.pkg);
        out[0] = base;
        return SKIP_ONE;
    }
    for (int i = 0; i < f->nroots; i++) {
        int pkg = ix->files[ix->mods[f->roots[i]].file].root_pkg;
        out[i] = base;
        out[i].root = f->roots[i];
        out[i].pkg = pkg >= 0 ? pkg : CBM_NOT_FOUND;
        out[i].own_lib = pkg >= 0 && ix->pkgs[pkg].lib_root != f->roots[i];
        out[i].edition = rs_edition(ix, out[i].pkg);
    }
    return f->nroots;
}

/* Resolve `p` written in module `mod`, under every crate root of its file:
 * what the roots find is united, so roots that disagree make it ambiguous.
 * `import`: the import whose path it is, CBM_NOT_FOUND for any other path. */
static void rs_resolve_path(rs_look_t *lk, int mod, const rs_path_t *p, int mask, rs_ent_t self,
                            int import, rs_found_t *out) {
    rs_at_t at[RS_ROOTS_KEPT];
    int n = rs_contexts(lk, mod, import, at);
    memset(out, 0, sizeof(*out));
    for (int i = 0; i < n; i++) {
        rs_found_t one;
        memset(&one, 0, sizeof(one));
        rs_walk(&at[i], p, mask, self, import >= 0, &one);
        for (int k = 0; k < RS_NS_COUNT; k++) {
            rs_set_merge(&out->ns[k], &one.ns[k]);
        }
        rs_set_merge(&out->owners, &one.owners);
        out->ambiguous = out->ambiguous || one.ambiguous;
    }
}

/* The one thing among the candidates of `s` that were really found: RS_E_NONE
 * when there is none, a gap when there are several. */
static rs_ent_t rs_only_strong(const rs_index_t *ix, const rs_set_t *s) {
    rs_set_t strong = {0};
    for (int i = 0; i < s->n; i++) {
        if (!s->v[i].weak) {
            rs_set_add(&strong, s->v[i]);
        }
    }
    rs_collapse(ix, &strong);
    if (s->overflow || strong.n > SKIP_ONE) {
        return rs_ent(RS_E_GAP, 0);
    }
    return strong.n ? strong.v[0] : (rs_ent_t){0};
}

/* What the weak candidates of `s` come to: a gap before external. */
static uint8_t rs_only_weak(const rs_set_t *s) {
    uint8_t kind = RS_E_NONE;
    for (int i = 0; i < s->n; i++) {
        if (s->v[i].weak && kind != RS_E_GAP) {
            kind = s->v[i].kind;
        }
    }
    return kind;
}

/* ── Build: dependencies ─────────────────────────────────────────── */

static int rs_rawdep_cmp(const void *a, const void *b) {
    const rs_rawdep_t *x = (const rs_rawdep_t *)a;
    const rs_rawdep_t *y = (const rs_rawdep_t *)b;
    int c = strcmp(x->name, y->name);
    return c ? c : (x->seq > y->seq) - (x->seq < y->seq);
}

static int rs_dep_cmp(const void *a, const void *b) {
    return strcmp(((const rs_dep_t *)a)->key, ((const rs_dep_t *)b)->key);
}

/* What the dependency tables of a manifest say about one dependency. */
typedef struct {
    const char *name;
    const char *path;    /* NULL: none written */
    const char *package; /* set: `name` is a rename */
    bool workspace;      /* inherited from the workspace table */
    bool conflict;       /* two tables give it different paths */
} rs_depspec_t;

static void rs_depspec_add(rs_depspec_t *spec, const rs_rawdep_t *r) {
    if (rs_is(r->attr, "path")) {
        spec->conflict = spec->conflict || (spec->path && !rs_is(spec->path, r->value));
        spec->path = r->value;
    } else if (rs_is(r->attr, "package")) {
        spec->package = r->value;
    } else if (rs_is(r->attr, "workspace") && rs_is(r->value, "true")) {
        spec->workspace = true;
    }
}

/* The entry `name` of the [workspace.dependencies] table of `ws` (its raw
 * records are sorted by name). false when there is none. */
static bool rs_ws_dep(const rs_pkg_t *ws, const char *name, rs_depspec_t *spec) {
    int lo = 0;
    int hi = ws->nraw;
    bool found = false;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (strcmp(ws->raw[mid].name, name) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < ws->nraw && rs_is(ws->raw[i].name, name); i++) {
        if (ws->raw[i].kind == 'w') {
            found = true;
            rs_depspec_add(spec, &ws->raw[i]);
        }
    }
    return found;
}

/* The crate a dependency brings into the extern prelude. Only a `path`
 * dependency names a crate of the repository; one whose path, manifest or
 * lib target is not known is a gap, never matched by name. */
static rs_dep_t rs_dep_make(rs_index_t *ix, const rs_pkg_t *p, rs_depspec_t spec) {
    rs_dep_t dep = {.key = rs_crate_name(ix, spec.name), .target = rs_ent(RS_E_GAP, 0)};
    const char *base = p->dir;
    char dir[RS_PATH_BUF];
    if (spec.workspace) {
        const rs_pkg_t *ws = p->ws >= 0 ? &ix->pkgs[p->ws] : NULL;
        rs_depspec_t inherited = {.name = spec.name};
        if (!ws || !rs_ws_dep(ws, spec.name, &inherited)) {
            return dep;
        }
        base = ws->dir;
        spec = inherited;
    }
    if (spec.conflict) {
        return dep;
    }
    if (!spec.path) {
        dep.target = rs_ent(RS_E_EXTERNAL, 0);
        return dep;
    }
    if (rs_is(spec.path, "?") || !rs_join_path(base, spec.path, dir, sizeof(dir))) {
        return dep;
    }
    int q = rs_pkg_at(ix, dir);
    if (q < 0 || !ix->pkgs[q].has_package || ix->pkgs[q].lib_root < 0) {
        return dep;
    }
    dep.target = rs_ent(RS_E_MOD, ix->pkgs[q].lib_root);
    if (!spec.package && ix->pkgs[q].lib_name) {
        dep.key = ix->pkgs[q].lib_name; /* not renamed: the lib crate's own name */
    }
    return dep;
}

static void rs_pkg_deps(rs_index_t *ix, int pi) {
    rs_pkg_t *p = &ix->pkgs[pi];
    p->deps = (rs_dep_t *)ix_alloc(ix, (size_t)p->nraw * sizeof(rs_dep_t));
    if (!p->deps) {
        return;
    }
    for (int i = 0; i < p->nraw;) {
        rs_depspec_t spec = {.name = p->raw[i].name};
        bool own = false;
        int j = i;
        for (; j < p->nraw && rs_is(p->raw[j].name, spec.name); j++) {
            if (p->raw[j].kind != 'w') {
                own = true;
                rs_depspec_add(&spec, &p->raw[j]);
            }
        }
        if (own) {
            p->deps[p->ndeps++] = rs_dep_make(ix, p, spec);
        }
        i = j;
    }
    if (p->ndeps > SKIP_ONE) {
        qsort(p->deps, (size_t)p->ndeps, sizeof(rs_dep_t), rs_dep_cmp);
    }
    int w = 0; /* one crate name for two targets: which one is not known */
    for (int i = 0; i < p->ndeps; i++) {
        if (w > 0 && rs_is(p->deps[w - SKIP_ONE].key, p->deps[i].key)) {
            if (!rs_ent_same(p->deps[w - SKIP_ONE].target, p->deps[i].target)) {
                p->deps[w - SKIP_ONE].target = rs_ent(RS_E_GAP, 0);
            }
            continue;
        }
        p->deps[w++] = p->deps[i];
    }
    p->ndeps = w;
}

static void rs_deps(rs_index_t *ix) {
    for (int pi = 0; pi < ix->npkgs; pi++) {
        rs_pkg_t *p = &ix->pkgs[pi];
        if (p->nraw > SKIP_ONE) {
            qsort(p->raw, (size_t)p->nraw, sizeof(rs_rawdep_t), rs_rawdep_cmp);
        }
    }
    for (int pi = 0; pi < ix->npkgs; pi++) {
        rs_pkg_deps(ix, pi);
    }
}

/* ── Build: bindings ─────────────────────────────────────────────── */

static int rs_bind_cmp(const void *a, const void *b) {
    const rs_bind_t *x = (const rs_bind_t *)a;
    const rs_bind_t *y = (const rs_bind_t *)b;
    if (x->mod != y->mod) {
        return x->mod < y->mod ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    return c ? c : (x->seq > y->seq) - (x->seq < y->seq);
}

static int rs_ref_cmp(const void *a, const void *b) {
    const rs_ref_t *x = (const rs_ref_t *)a;
    const rs_ref_t *y = (const rs_ref_t *)b;
    if (x->mod != y->mod) {
        return x->mod < y->mod ? -1 : 1;
    }
    return (x->seq > y->seq) - (x->seq < y->seq);
}

static int rs_member_cmp(const void *a, const void *b) {
    const rs_member_t *x = (const rs_member_t *)a;
    const rs_member_t *y = (const rs_member_t *)b;
    int c = strcmp(x->name, y->name);
    return c ? c : (x->seq > y->seq) - (x->seq < y->seq);
}

/* The members of every type, trait and impl, by name. */
static void rs_sort_members(rs_index_t *ix) {
    for (int i = 0; i < ix->nitems; i++) {
        const rs_item_t *it = &ix->items[i];
        if (it->member_count > SKIP_ONE) {
            qsort(&ix->members[it->member_first], (size_t)it->member_count, sizeof(rs_member_t),
                  rs_member_cmp);
        }
    }
    for (int i = 0; i < ix->nimpls; i++) {
        const rs_impl_t *im = &ix->impls[i];
        if (im->member_count > SKIP_ONE) {
            qsort(&ix->members[im->member_first], (size_t)im->member_count, sizeof(rs_member_t),
                  rs_member_cmp);
        }
    }
}

static void rs_bind_put(rs_index_t *ix, int mod, const char *name, int kind, int ref, char vis) {
    ix->binds[ix->nbinds] = (rs_bind_t){
        .mod = mod, .name = name, .kind = kind, .ref = ref, .seq = ix->nbinds, .vis = vis};
    ix->nbinds++;
}

/* An item: bound in its module, and a `#[macro_export]` macro also at the
 * crate roots of its file. */
static void rs_bind_item(rs_index_t *ix, int idx) {
    const rs_item_t *it = &ix->items[idx];
    const rs_file_t *f = &ix->files[it->file];
    rs_bind_put(ix, it->mod, it->name, RS_BIND_ITEM, idx, it->vis);
    for (int r = 0; it->macro_export && !f->roots_top && r < f->nroots; r++) {
        if (f->roots[r] != it->mod) {
            rs_bind_put(ix, f->roots[r], it->name, RS_BIND_ITEM, idx, 'p');
        }
    }
}

/* Sort `refs` by module and set each module's range through `first`/`count`
 * (offsets of the two fields in rs_mod_t). */
static void rs_ref_ranges(rs_index_t *ix, rs_ref_t *refs, int n, bool globs) {
    if (n > SKIP_ONE) {
        qsort(refs, (size_t)n, sizeof(rs_ref_t), rs_ref_cmp);
    }
    for (int i = 0; i < n; i++) {
        rs_mod_t *m = &ix->mods[refs[i].mod];
        int *first = globs ? &m->glob_first : &m->named_first;
        int *count = globs ? &m->glob_count : &m->named_count;
        if (*count == 0) {
            *first = i;
        }
        (*count)++;
    }
}

static void rs_bindings(rs_index_t *ix) {
    size_t cap = ((size_t)ix->nitems * (RS_ROOTS_KEPT + SKIP_ONE)) + (size_t)ix->ndecls +
                 (size_t)ix->nmods + (size_t)ix->nimports + (size_t)ix->nmaybes;
    ix->binds = (rs_bind_t *)ix_alloc(ix, cap * sizeof(rs_bind_t));
    ix->globs = (rs_ref_t *)ix_alloc(ix, (size_t)ix->nimports * sizeof(rs_ref_t));
    ix->named = (rs_ref_t *)ix_alloc(ix, (size_t)ix->nimports * sizeof(rs_ref_t));
    if (!ix->binds || !ix->globs || !ix->named) {
        return;
    }
    for (int i = 0; i < ix->nitems; i++) {
        rs_bind_item(ix, i);
    }
    for (int m = 0; m < ix->nmods; m++) {
        if (ix->mods[m].parent >= 0) {
            rs_bind_put(ix, ix->mods[m].parent, ix->mods[m].name, RS_BIND_CHILD, m,
                        ix->mods[m].vis);
        }
    }
    for (int d = 0; d < ix->ndecls; d++) {
        const rs_decl_t *decl = &ix->decls[d];
        rs_bind_put(ix, decl->mod, decl->name, RS_BIND_CHILD, decl->child, decl->vis);
    }
    for (int i = 0; i < ix->nimports; i++) {
        const rs_import_t *imp = &ix->imports[i];
        rs_ref_t ref = {.mod = imp->mod, .seq = imp->seq, .ref = i};
        if (imp->kind == 'g') {
            ix->globs[ix->nglobs++] = ref;
        } else {
            ix->named[ix->nnamed++] = ref;
            rs_bind_put(ix, imp->mod, imp->local, RS_BIND_IMPORT, i, imp->vis);
        }
    }
    for (int i = 0; i < ix->nmaybes; i++) {
        rs_bind_put(ix, ix->maybes[i].mod, ix->maybe_names[i], RS_BIND_MAYBE, i, 'p');
    }
    if (ix->nbinds > SKIP_ONE) {
        qsort(ix->binds, (size_t)ix->nbinds, sizeof(rs_bind_t), rs_bind_cmp);
    }
    for (int i = 0; i < ix->nbinds; i++) {
        rs_mod_t *m = &ix->mods[ix->binds[i].mod];
        if (m->bind_count == 0) {
            m->bind_first = i;
        }
        m->bind_count++;
    }
    rs_ref_ranges(ix, ix->globs, ix->nglobs, true);
    rs_ref_ranges(ix, ix->named, ix->nnamed, false);
}

/* ── Build: imports ──────────────────────────────────────────────── */

/* `extern crate name;`: the crate of that name, or `self` for the crate
 * itself. */
static void rs_import_extern(rs_look_t *lk, const rs_import_t *imp, int idx, rs_found_t *found) {
    rs_at_t at[RS_ROOTS_KEPT];
    int n = rs_contexts(lk, imp->mod, idx, at);
    for (int i = 0; i < n; i++) {
        bool unknown = false;
        if (rs_is(imp->segs[0], "self")) {
            unknown = at[i].root < 0;
            rs_set_add(&found->ns[0], unknown ? rs_weak(RS_E_GAP) : rs_ent(RS_E_MOD, at[i].root));
            continue;
        }
        rs_set_t one = {0};
        rs_extern(&at[i], imp->segs[0], &one, &unknown);
        if (rs_set_empty(&one)) {
            /* a crate the manifest names differently (see rs_lexical), unless
             * the crate's dependencies are not known at all */
            rs_set_add(&one, rs_weak(unknown ? RS_E_GAP : RS_E_EXTERNAL));
        }
        rs_set_merge(&found->ns[0], &one);
    }
}

/* Keep what an import path was found to name. An import DECLARES its name:
 * whatever the path leads to, the name is bound, so a path that leads into
 * something unknown binds the name to a gap (or to something external) in
 * every namespace rather than leaving it unbound. */
static void rs_import_store(rs_index_t *ix, rs_import_t *imp, const rs_found_t *found, int pass) {
    bool strong = false;
    uint8_t weak = RS_E_NONE;
    for (int k = 0; k < RS_NS_COUNT; k++) {
        uint8_t w = rs_only_weak(&found->ns[k]);
        imp->res[k] = rs_only_strong(ix, &found->ns[k]);
        imp->res[k].via_import = false;
        strong = strong || imp->res[k].kind != RS_E_NONE;
        weak = (weak == RS_E_GAP || w == RS_E_NONE) ? weak : w;
    }
    if (found->ambiguous || (!strong && weak != RS_E_EXTERNAL)) {
        weak = RS_E_GAP;
        ix->gap_imports++;
    }
    imp->spread = imp->kind == 'b' && (found->ambiguous || !strong);
    for (int k = 0; k < RS_NS_COUNT && (found->ambiguous || !strong); k++) {
        imp->res[k] = rs_ent(weak, 0);
    }
    imp->state = RS_STATE_DONE;
    imp->done_pass = pass;
}

/* Resolve import `idx` in pass `pass`. An import that waits for another one
 * stays unresolved, unless this is the `last` attempt: then it is not
 * followed. */
static void rs_import_resolve(rs_index_t *ix, int idx, int pass, bool last) {
    rs_import_t *imp = &ix->imports[idx];
    rs_look_t lk = {.ix = ix, .pass = pass};
    rs_found_t found;
    memset(&found, 0, sizeof(found));
    if (imp->nsegs > 0 && imp->kind == 'x') {
        rs_import_extern(&lk, imp, idx, &found);
    } else if (imp->nsegs > 0) {
        rs_path_t p = {.segs = imp->segs, .n = imp->nsegs, .global = imp->global};
        rs_ent_t no_self = {0};
        int mask = imp->kind == 'b' ? RS_NS_ALL : RS_NS_TYPE;
        rs_resolve_path(&lk, imp->mod, &p, mask, no_self, idx, &found);
    }
    if (lk.blocked && !last) {
        return;
    }
    if (lk.blocked) {
        memset(&found, 0, sizeof(found));
    }
    rs_import_store(ix, imp, &found, pass);
}

/* Resolve every import. Imports depend on each other (a path can run through
 * an imported name or a glob), so they are resolved in passes: a pass reads
 * only what EARLIER passes resolved, which makes the result independent of
 * the order of the imports. What is still waiting after the last pass (a
 * cycle, or a chain longer than the passes) is not followed. */
static void rs_imports(rs_index_t *ix) {
    int pending = ix->nimports;
    int pass = SKIP_ONE;
    for (; pass <= RS_IMPORT_PASSES && pending > 0; pass++) {
        int before = pending;
        for (int i = 0; i < ix->nimports; i++) {
            if (ix->imports[i].state != RS_STATE_DONE) {
                rs_import_resolve(ix, i, pass, false);
                pending -= ix->imports[i].state == RS_STATE_DONE;
            }
        }
        if (pending == before) {
            break;
        }
    }
    for (int i = 0; pending > 0 && i < ix->nimports; i++) {
        if (ix->imports[i].state != RS_STATE_DONE) {
            rs_import_resolve(ix, i, pass + SKIP_ONE, true);
        }
    }
}

/* ── Build: aliases and impls ────────────────────────────────────── */

/* What the type path `text`, written in module `mod`, names. */
static rs_ent_t rs_type_path(rs_index_t *ix, int mod, const char *text) {
    CBMDocLinkRsPath parsed;
    const char *segs[CBM_DOCLINK_RS_MAX_SEGS];
    if (cbm_doclink_rs_parse_path(text, strlen(text), false, &parsed) != CBM_DOCLINK_RS_PATH_OK) {
        return rs_ent(RS_E_GAP, 0);
    }
    for (int i = 0; i < parsed.nsegs; i++) {
        segs[i] = rs_plain(parsed.segs[i]);
    }
    rs_path_t p = {.segs = segs, .n = parsed.nsegs, .global = parsed.global};
    rs_look_t lk = {.ix = ix};
    rs_found_t found;
    rs_ent_t no_self = {0};
    rs_resolve_path(&lk, mod, &p, RS_NS_TYPE, no_self, CBM_NOT_FOUND, &found);
    if (found.ambiguous) {
        return rs_ent(RS_E_GAP, 0);
    }
    rs_ent_t e = rs_only_strong(ix, &found.ns[0]);
    uint8_t weak = rs_only_weak(&found.ns[0]);
    return (e.kind == RS_E_NONE && weak != RS_E_NONE) ? rs_ent(weak, 0) : e;
}

static int rs_item_impl_cmp(const void *a, const void *b) {
    const rs_item_impl_t *x = (const rs_item_impl_t *)a;
    const rs_item_impl_t *y = (const rs_item_impl_t *)b;
    if (x->item != y->item) {
        return x->item < y->item ? -1 : 1;
    }
    return (x->impl > y->impl) - (x->impl < y->impl);
}

static int rs_assoc_cmp(const void *a, const void *b) {
    const rs_assoc_t *x = (const rs_assoc_t *)a;
    const rs_assoc_t *y = (const rs_assoc_t *)b;
    if (x->item != y->item) {
        return x->item < y->item ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    if (c) {
        return c;
    }
    return x->impl != y->impl ? (x->impl < y->impl ? -1 : 1)
                              : (x->member > y->member) - (x->member < y->member);
}

/* The type an impl is for and the trait it implements. */
static void rs_impl_types(rs_index_t *ix, rs_impl_t *im) {
    rs_ent_t none = {0};
    rs_ent_t self = im->self_path ? rs_type_path(ix, im->mod, im->self_path) : none;
    if (self.kind == RS_E_ITEM) {
        self = rs_alias_end(ix, self.idx);
        self.weak = false;
    }
    if (self.kind == RS_E_ITEM && !rs_item_is_type(ix->items[self.idx].kind)) {
        self = rs_ent(RS_E_GAP, 0);
    }
    im->self_ent = self;
    if (im->trait_path) {
        rs_ent_t trait = rs_is(im->trait_path, "?") ? rs_ent(RS_E_GAP, 0)
                                                    : rs_type_path(ix, im->mod, im->trait_path);
        im->trait_ent = trait.kind == RS_E_NONE ? rs_ent(RS_E_GAP, 0) : trait;
    }
}

/* Resolve alias targets and impl headers, and index every impl's members by
 * the type the impl is for. */
static void rs_types(rs_index_t *ix) {
    int members = 0;
    for (int i = 0; i < ix->nitems; i++) {
        rs_item_t *it = &ix->items[i];
        if (it->alias) {
            it->alias_ent = rs_type_path(ix, it->mod, it->alias);
        }
    }
    for (int i = 0; i < ix->nimpls; i++) {
        rs_impl_types(ix, &ix->impls[i]);
        members += ix->impls[i].member_count;
    }
    ix->item_impls = (rs_item_impl_t *)ix_alloc(ix, (size_t)ix->nimpls * sizeof(rs_item_impl_t));
    ix->assoc = (rs_assoc_t *)ix_alloc(ix, (size_t)members * sizeof(rs_assoc_t));
    if (!ix->item_impls || !ix->assoc) {
        return;
    }
    for (int i = 0; i < ix->nimpls; i++) {
        const rs_impl_t *im = &ix->impls[i];
        for (int k = 0; k < im->member_count; k++) {
            int m = im->member_first + k;
            if (im->self_ent.kind == RS_E_ITEM) {
                ix->assoc[ix->nassoc++] = (rs_assoc_t){
                    .item = im->self_ent.idx, .name = ix->members[m].name, .impl = i, .member = m};
            } else if (!im->self_path) {
                cbm_ht_set(ix->blanket, ix->members[m].name, (void *)(intptr_t)SKIP_ONE);
            }
        }
        if (im->self_ent.kind == RS_E_ITEM) {
            ix->item_impls[ix->nitem_impls++] =
                (rs_item_impl_t){.item = im->self_ent.idx, .impl = i};
        }
    }
    if (ix->nitem_impls > SKIP_ONE) {
        qsort(ix->item_impls, (size_t)ix->nitem_impls, sizeof(rs_item_impl_t), rs_item_impl_cmp);
    }
    if (ix->nassoc > SKIP_ONE) {
        qsort(ix->assoc, (size_t)ix->nassoc, sizeof(rs_assoc_t), rs_assoc_cmp);
    }
    for (int i = 0; i < ix->nitem_impls; i++) {
        rs_item_t *it = &ix->items[ix->item_impls[i].item];
        if (it->impl_count == 0) {
            it->impl_first = i;
        }
        it->impl_count++;
    }
}

/* ── Build: line indexes of the re-extracted files ───────────────── */

typedef struct {
    uint32_t line;
    int order; /* at one line: modules are left innermost first, then entered outermost first */
    int mod;   /* the innermost module from that line on */
} rs_event_t;

static int rs_event_cmp(const void *a, const void *b) {
    const rs_event_t *x = (const rs_event_t *)a;
    const rs_event_t *y = (const rs_event_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return (x->order > y->order) - (x->order < y->order);
}

static int rs_scope_cmp(const void *a, const void *b) {
    const rs_scope_t *x = (const rs_scope_t *)a;
    const rs_scope_t *y = (const rs_scope_t *)b;
    if (x->start != y->start) {
        return x->start < y->start ? -1 : 1;
    }
    return (x->end > y->end) - (x->end < y->end);
}

static int rs_note_cmp(const void *a, const void *b) {
    const rs_note_t *x = (const rs_note_t *)a;
    const rs_note_t *y = (const rs_note_t *)b;
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return strcmp(x->text, y->text);
}

static int rs_line_cmp(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* The spans of file `f`: for every line, the innermost inline module. */
static void rs_file_spans(rs_index_t *ix, rs_file_t *f, rs_event_t *ev) {
    int n = 0;
    for (int m = SKIP_ONE; m < f->nmods; m++) {
        const rs_mod_t *mod = &ix->mods[f->mod_base + m];
        if (mod->parent < 0) {
            continue;
        }
        ev[n++] = (rs_event_t){.line = mod->start, .order = m, .mod = f->mod_base + m};
        ev[n++] = (rs_event_t){.line = mod->end + SKIP_ONE, .order = -m, .mod = mod->parent};
    }
    if (n > SKIP_ONE) {
        qsort(ev, (size_t)n, sizeof(rs_event_t), rs_event_cmp);
    }
    f->span_first = ix->nspans;
    ix->spans[ix->nspans++] = (rs_span_t){.from = 0, .mod = f->mod_base};
    for (int i = 0; i < n; i++) {
        ix->spans[ix->nspans++] = (rs_span_t){.from = ev[i].line, .mod = ev[i].mod};
    }
    f->span_count = ix->nspans - f->span_first;
}

static void rs_file_lines(rs_index_t *ix, rs_file_t *f, rs_event_t *ev) {
    rs_file_spans(ix, f, ev);
    if (f->scope_count > SKIP_ONE) {
        qsort(&ix->scopes[f->scope_first], (size_t)f->scope_count, sizeof(rs_scope_t),
              rs_scope_cmp);
    }
    if (f->local_count > SKIP_ONE) {
        qsort(&ix->locals[f->local_first], (size_t)f->local_count, sizeof(rs_note_t), rs_note_cmp);
    }
    if (f->unowned_count > SKIP_ONE) {
        qsort(&ix->unowned[f->unowned_first], (size_t)f->unowned_count, sizeof(rs_note_t),
              rs_note_cmp);
    }
    f->test_first = ix->ntest_lines;
    for (int i = f->item_first; i < f->item_first + f->item_count; i++) {
        if (ix->items[i].test) {
            ix->test_lines[ix->ntest_lines++] = ix->items[i].start;
        }
    }
    f->test_count = ix->ntest_lines - f->test_first;
    if (f->test_count > SKIP_ONE) {
        qsort(&ix->test_lines[f->test_first], (size_t)f->test_count, sizeof(uint32_t), rs_line_cmp);
    }
}

static void rs_line_indexes(rs_index_t *ix) {
    size_t spans = 0;
    size_t events = 0;
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const rs_file_t *f = &ix->files[fi];
        if (f->kind == RS_FILE_SOURCE && f->fresh) {
            size_t ev = (size_t)(f->nmods - SKIP_ONE) * PAIR_LEN;
            spans += ev + SKIP_ONE;
            events = ev > events ? ev : events;
        }
    }
    ix->spans = (rs_span_t *)ix_alloc(ix, spans * sizeof(rs_span_t));
    ix->test_lines = (uint32_t *)ix_alloc(ix, (size_t)ix->nitems * sizeof(uint32_t));
    rs_event_t *ev = (rs_event_t *)ix_alloc(ix, events * sizeof(rs_event_t));
    if (!ix->spans || !ix->test_lines || !ev) {
        return;
    }
    for (int fi = 0; fi < ix->nfiles; fi++) {
        if (ix->files[fi].kind == RS_FILE_SOURCE && ix->files[fi].fresh) {
            rs_file_lines(ix, &ix->files[fi], ev);
        }
    }
}

/* ── Build ───────────────────────────────────────────────────────── */

/* The scope blob of a Rust source file, NULL when it has none this code
 * reads. */
static const char *rs_source_blob(const char *scope) {
    static const char tag[] = CBM_DOCLINK_RS_SCOPE_TAG;
    const size_t tl = sizeof(tag) - SKIP_ONE;
    if (!scope || strncmp(scope, tag, tl) != 0 || (scope[tl] != '\n' && scope[tl] != '\0') ||
        rs_blob_is_manifest(scope)) {
        return NULL;
    }
    return scope;
}

static void rs_count_add(rs_counts_t *sum, const rs_counts_t *c) {
    sum->mods += c->mods;
    sum->decls += c->decls;
    sum->items += c->items;
    sum->members += c->members;
    sum->impls += c->impls;
    sum->imports += c->imports;
    sum->maybes += c->maybes;
    sum->locals += c->locals;
    sum->unowned += c->unowned;
    sum->macuses += c->macuses;
    sum->macdecls += c->macdecls;
}

/* The facts of source file `f` that need no blob, and the size of its blob. */
static void rs_source_meta(rs_index_t *ix, rs_file_t *f, const cbm_doclink_file_t *src,
                           rs_counts_t *sum) {
    rs_counts_t c = {0};
    /* the qualified name of an item `x` of this file, minus the `x` */
    const char *probe = cbm_fqn_compute(&ix->arena, ix->project, src->rel_path, "x");
    size_t pl = probe ? strlen(probe) : 0;
    f->kind = RS_FILE_SOURCE;
    f->fresh = src->run_file >= 0;
    f->test_path = rs_is_test_path(src->rel_path);
    f->item_prefix = pl > 0 ? ix_strndup(ix, probe, pl - SKIP_ONE) : NULL;
    f->module_qn =
        cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_RUST);
    ix->oom = ix->oom || !f->item_prefix || !f->module_qn;
    rs_count_blob(rs_source_blob(src->scope), &c);
    f->nmods = c.mods + SKIP_ONE;
    f->mod_base = ix->nmods;
    ix->nmods += f->nmods;
    ix->nsources++;
    rs_count_add(sum, &c);
}

static bool rs_alloc_tables(rs_index_t *ix, const rs_counts_t *c, int manifests) {
    ix->mods = (rs_mod_t *)ix_calloc(ix, (size_t)ix->nmods, sizeof(rs_mod_t));
    ix->decls = (rs_decl_t *)ix_calloc(ix, (size_t)c->decls, sizeof(rs_decl_t));
    ix->items = (rs_item_t *)ix_calloc(ix, (size_t)c->items, sizeof(rs_item_t));
    ix->members = (rs_member_t *)ix_calloc(ix, (size_t)c->members, sizeof(rs_member_t));
    ix->impls = (rs_impl_t *)ix_calloc(ix, (size_t)c->impls, sizeof(rs_impl_t));
    ix->imports = (rs_import_t *)ix_calloc(ix, (size_t)c->imports, sizeof(rs_import_t));
    ix->maybes = (rs_ref_t *)ix_calloc(ix, (size_t)c->maybes, sizeof(rs_ref_t));
    ix->maybe_names = (const char **)ix_calloc(ix, (size_t)c->maybes, sizeof(char *));
    ix->macuses = (rs_macuse_t *)ix_calloc(ix, (size_t)c->macuses, sizeof(rs_macuse_t));
    ix->macdecls = (rs_macdecl_t *)ix_calloc(ix, (size_t)c->macdecls, sizeof(rs_macdecl_t));
    ix->scopes =
        (rs_scope_t *)ix_calloc(ix, (size_t)c->items + (size_t)c->impls, sizeof(rs_scope_t));
    ix->locals = (rs_note_t *)ix_calloc(ix, (size_t)c->locals, sizeof(rs_note_t));
    ix->unowned = (rs_note_t *)ix_calloc(ix, (size_t)c->unowned, sizeof(rs_note_t));
    ix->pkgs = (rs_pkg_t *)ix_calloc(ix, (size_t)manifests, sizeof(rs_pkg_t));
    return !ix->oom;
}

/* Read every file's scope blob into the index. */
static bool rs_load(rs_index_t *ix, const cbm_doclink_build_in_t *in) {
    rs_counts_t sum = {0};
    int manifests = 0;
    ix->nfiles = in->file_count;
    ix->files = (rs_file_t *)ix_calloc(ix, (size_t)in->file_count, sizeof(rs_file_t));
    if (!ix->files) {
        return false;
    }
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        rs_file_t *f = &ix->files[i];
        f->rel_path = src->rel_path;
        f->root_pkg = RS_PKG_NONE;
        f->pkg = RS_PKG_NONE;
        cbm_ht_set(ix->file_by_path, f->rel_path, (void *)(intptr_t)(i + SKIP_ONE));
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (rs_ends_with(src->rel_path, ".rs")) {
            rs_source_meta(ix, f, src, &sum);
        } else if (rs_is(rs_basename(src->rel_path), "Cargo.toml") &&
                   rs_blob_is_manifest(src->scope)) {
            f->kind = RS_FILE_MANIFEST;
            manifests++;
        }
    }
    if (!rs_alloc_tables(ix, &sum, manifests)) {
        return false;
    }
    for (int i = 0; i < in->file_count; i++) {
        rs_file_t *f = &ix->files[i];
        if (f->kind == RS_FILE_MANIFEST) {
            rs_read_manifest(ix, i, in->files[i].scope);
        }
        for (int m = 0; f->kind == RS_FILE_SOURCE && m < f->nmods; m++) {
            ix->mods[f->mod_base + m].file = i;
            ix->mods[f->mod_base + m].parent = CBM_NOT_FOUND;
        }
        if (f->kind == RS_FILE_SOURCE) {
            rs_read_source(ix, i, rs_source_blob(in->files[i].scope));
        }
    }
    for (int pi = 0; pi < ix->npkgs; pi++) {
        const char *dir = ix->pkgs[pi].dir;
        cbm_ht_set(ix->pkg_by_dir, dir[0] ? dir : ".", (void *)(intptr_t)(pi + SKIP_ONE));
    }
    if (ix->nmacuses > SKIP_ONE) {
        qsort(ix->macuses, (size_t)ix->nmacuses, sizeof(rs_macuse_t), rs_macuse_cmp);
    }
    if (ix->nmacdecls > SKIP_ONE) {
        qsort(ix->macdecls, (size_t)ix->nmacdecls, sizeof(rs_macdecl_t), rs_macdecl_cmp);
    }
    return !ix->oom;
}

static void rs_destroy(void *index) {
    rs_index_t *ix = (rs_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->file_by_path);
    cbm_ht_free(ix->pkg_by_dir);
    cbm_ht_free(ix->blanket);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static void rs_log_index(const rs_index_t *ix) {
    enum { N_FILES = 0, N_PKGS, N_ROOTS, N_ORPHANS, N_MODS, N_ITEMS, N_IMPORTS, N_GAPS, N_COUNT };
    char b[N_COUNT][CBM_SZ_32];
    snprintf(b[N_FILES], sizeof(b[N_FILES]), "%d", ix->nsources);
    snprintf(b[N_PKGS], sizeof(b[N_PKGS]), "%d", ix->npkgs);
    snprintf(b[N_ROOTS], sizeof(b[N_ROOTS]), "%d", ix->crate_roots);
    snprintf(b[N_ORPHANS], sizeof(b[N_ORPHANS]), "%d", ix->orphans);
    snprintf(b[N_MODS], sizeof(b[N_MODS]), "%d", ix->nmods);
    snprintf(b[N_ITEMS], sizeof(b[N_ITEMS]), "%d", ix->nitems);
    snprintf(b[N_IMPORTS], sizeof(b[N_IMPORTS]), "%d", ix->nimports);
    snprintf(b[N_GAPS], sizeof(b[N_GAPS]), "%d", ix->gap_imports);
    cbm_log_info("doc_links.rs.index", "files", b[N_FILES], "manifests", b[N_PKGS], "crate_roots",
                 b[N_ROOTS], "orphan_files", b[N_ORPHANS], "modules", b[N_MODS], "items",
                 b[N_ITEMS], "imports", b[N_IMPORTS], "unfollowed_imports", b[N_GAPS]);
}

static void *rs_build(const cbm_doclink_build_in_t *in) {
    rs_index_t *ix = (rs_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->file_by_path = cbm_ht_create(CBM_SZ_4K);
    ix->pkg_by_dir = cbm_ht_create(CBM_SZ_64);
    ix->blanket = cbm_ht_create(CBM_SZ_256);
    ix->run_count = in->run_file_count;
    ix->run_to_file = (int *)cbm_alloc(
        CBM_MEM_CLASS_OTHER,
        (size_t)(in->run_file_count ? in->run_file_count : SKIP_ONE) * sizeof(int));
    if (!ix->file_by_path || !ix->pkg_by_dir || !ix->blanket || !ix->run_to_file) {
        cbm_log_error("doc_links.rs.error", "step", "tables", "reason", "alloc");
        rs_destroy(ix);
        return NULL;
    }
    for (int i = 0; i < in->run_file_count; i++) {
        ix->run_to_file[i] = CBM_NOT_FOUND;
    }
    if (rs_load(ix, in)) {
        rs_crate_roots(ix);
        rs_module_tree(ix);
        rs_file_roots(ix);
        rs_orphan_packages(ix);
        rs_product(ix);
        rs_deps(ix);
        rs_sort_members(ix);
        rs_bindings(ix);
    }
    if (!ix->oom) {
        rs_imports(ix);
        rs_types(ix);
        rs_line_indexes(ix);
    }
    if (ix->oom) {
        cbm_log_error("doc_links.rs.error", "step", "index", "reason", "alloc");
        rs_destroy(ix);
        return NULL;
    }
    rs_log_index(ix);
    return ix;
}

/* ── One reference ───────────────────────────────────────────────── */

typedef struct {
    const rs_index_t *ix;
    const rs_file_t *f;
    const CBMDocLink *link;
    const char *segs[CBM_DOCLINK_RS_MAX_SEGS];
    rs_path_t path;
    int dis;
    int mod;       /* the module the documented definition is in */
    rs_ent_t self; /* what `Self` names there */
    bool single;   /* one plain name: no `::`, no keyword */
    bool file_level;
    bool product; /* the documented definition is product code */
} rs_link_t;

static bool rs_is_keyword(const char *seg) {
    return rs_is(seg, "crate") || rs_is(seg, "self") || rs_is(seg, "super") || rs_is(seg, "Self");
}

/* The innermost module of file `f` at `line`. */
static int rs_mod_at(const rs_index_t *ix, const rs_file_t *f, uint32_t line) {
    int lo = f->span_first;
    int hi = lo + f->span_count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (ix->spans[mid].from <= line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo > f->span_first ? ix->spans[lo - SKIP_ONE].mod : f->mod_base;
}

/* What `Self` names at `line`: the type or trait whose definition holds the
 * line, the self type of the impl that does, or nothing. */
static rs_ent_t rs_self_at(const rs_index_t *ix, const rs_file_t *f, uint32_t line) {
    rs_ent_t none = {0};
    int lo = f->scope_first;
    int hi = lo + f->scope_count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (ix->scopes[mid].start <= line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    if (lo == f->scope_first || ix->scopes[lo - SKIP_ONE].end < line) {
        return none;
    }
    const rs_scope_t *s = &ix->scopes[lo - SKIP_ONE];
    if (s->item >= 0) {
        return rs_ent(RS_E_ITEM, s->item);
    }
    rs_ent_t self = ix->impls[s->impl].self_ent;
    return self.kind == RS_E_ITEM ? self : rs_ent(RS_E_IMPL, s->impl);
}

/* First note of notes[first, first + count) at or after `line`. */
static int rs_note_at(const rs_note_t *notes, int first, int count, uint32_t line) {
    int lo = first;
    int hi = first + count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (notes[mid].line < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* True when the documented definition does not own its graph node: another
 * definition of the file has the same qualified name and keeps the node, so
 * an edge would leave from that other definition. */
static bool rs_unowned(const rs_index_t *ix, const rs_file_t *f, const CBMDocLink *link) {
    size_t pl = strlen(f->item_prefix);
    const char *qn = link->source_qn ? link->source_qn : "";
    int end = f->unowned_first + f->unowned_count;
    for (int i = rs_note_at(ix->unowned, f->unowned_first, f->unowned_count, link->def_line);
         i < end && ix->unowned[i].line == link->def_line; i++) {
        if (strncmp(qn, f->item_prefix, pl) == 0 && rs_is(qn + pl, ix->unowned[i].text)) {
            return true;
        }
    }
    return false;
}

/* True when `name` is a parameter or type parameter of the definition the
 * reference documents. */
static bool rs_is_local(const rs_link_t *q, const char *name) {
    const rs_index_t *ix = q->ix;
    const rs_file_t *f = q->f;
    size_t nl = strlen(name);
    int end = f->local_first + f->local_count;
    if (q->file_level) {
        return false;
    }
    for (int i = rs_note_at(ix->locals, f->local_first, f->local_count, q->link->def_line);
         i < end && ix->locals[i].line == q->link->def_line; i++) {
        for (const char *p = ix->locals[i].text; *p;) {
            const char *comma = strchr(p, ',');
            size_t len = comma ? (size_t)(comma - p) : strlen(p);
            const char *plain = (len > PAIR_LEN && p[0] == 'r' && p[1] == '#') ? p + PAIR_LEN : p;
            size_t plen = len - (size_t)(plain - p);
            if (plen == nl && memcmp(plain, name, nl) == 0) {
                return true;
            }
            p = comma ? comma + SKIP_ONE : p + len;
        }
    }
    return false;
}

/* Is the documented definition product code? Not in a test-only module, not
 * an item marked as a test, not inside a type marked as one. */
static bool rs_source_product(const rs_link_t *q) {
    const rs_index_t *ix = q->ix;
    const rs_file_t *f = q->f;
    if (!ix->mods[q->mod].product) {
        return false;
    }
    if (q->file_level) {
        return true;
    }
    if (q->self.kind == RS_E_ITEM && ix->items[q->self.idx].test) {
        return false;
    }
    int lo = f->test_first;
    int hi = lo + f->test_count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (ix->test_lines[mid] < q->link->def_line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return !(lo < f->test_first + f->test_count && ix->test_lines[lo] == q->link->def_line);
}

static const char *rs_label_name(char code) {
    static const struct {
        char code;
        const char *label;
    } table[] = {
        {'S', "Struct"},   {'E', "Enum"},     {'C', "Class"},  {'I', "Interface"}, {'T', "Type"},
        {'F', "Function"}, {'V', "Variable"}, {'M', "Method"}, {'D', "Field"},
    };
    for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
        if (table[i].code == code) {
            return table[i].label;
        }
    }
    return NULL;
}

/* The node of a declaration of file `fi`: looked up by qualified name, and
 * taken only when its label and file are the declaration's (another
 * definition can own that name: a same-named item of another kind, another
 * file's module). */
static const cbm_gbuf_node_t *rs_node(const rs_index_t *ix, const cbm_gbuf_t *graph, int fi,
                                      const char *label, const char *qn) {
    const cbm_gbuf_node_t *n = (label && qn) ? cbm_gbuf_find_by_qn(graph, qn) : NULL;
    if (!n || !n->label || !n->file_path || !rs_is(n->label, label) ||
        !rs_is(n->file_path, ix->files[fi].rel_path)) {
        return NULL;
    }
    return n;
}

static const cbm_gbuf_node_t *rs_decl_node(const rs_index_t *ix, const cbm_gbuf_t *graph, int fi,
                                           char label, const char *suffix) {
    char qn[RS_QN_BUF];
    if (!label || !suffix ||
        snprintf(qn, sizeof(qn), "%s%s", ix->files[fi].item_prefix, suffix) >= (int)sizeof(qn)) {
        return NULL;
    }
    return rs_node(ix, graph, fi, rs_label_name(label), qn);
}

/* The outcome for the one entity a reference names. */
static void rs_outcome(const rs_link_t *q, const cbm_gbuf_t *graph, rs_ent_t e,
                       cbm_doclink_outcome_t *out) {
    const rs_index_t *ix = q->ix;
    const cbm_gbuf_node_t *node = NULL;
    bool product = true;
    out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
    if (e.kind == RS_E_ITEM) {
        const rs_item_t *it = &ix->items[e.idx];
        product = ix->mods[it->mod].product && !it->test;
        node = rs_decl_node(ix, graph, it->file, it->label, it->suffix);
    } else if (e.kind == RS_E_MEMBER) {
        const rs_member_t *m = &ix->members[e.idx];
        product = ix->mods[m->mod].product && !m->owner_test;
        node = rs_decl_node(ix, graph, m->file, m->label, m->suffix);
    } else if (e.kind == RS_E_MOD) {
        const rs_mod_t *mod = &ix->mods[e.idx];
        product = mod->product;
        node = mod->parent < 0
                   ? rs_node(ix, graph, mod->file, "Module", ix->files[mod->file].module_qn)
                   : NULL; /* an inline module has no node */
    } else {
        bool external = e.kind == RS_E_EXTERNAL ||
                        (e.kind == RS_E_IMPL && ix->impls[e.idx].self_ent.kind == RS_E_EXTERNAL);
        out->reason = external ? CBM_DOCLINK_REASON_EXTERNAL : CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    if (q->product && !product) {
        out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
    } else if (node) {
        out->kind = CBM_DOCLINK_EDGE;
        out->target = node;
    }
}

/* The namespaces a disambiguator or suffix asks for. */
static int rs_dis_mask(int dis) {
    switch (dis) {
    case CBM_DOCLINK_RS_DIS_NONE:
        return RS_NS_ALL;
    case CBM_DOCLINK_RS_DIS_CONST:
    case CBM_DOCLINK_RS_DIS_STATIC:
    case CBM_DOCLINK_RS_DIS_FN:
    case CBM_DOCLINK_RS_DIS_FIELD:
    case CBM_DOCLINK_RS_DIS_VALUE_NS:
        return RS_NS_VALUE;
    case CBM_DOCLINK_RS_DIS_DERIVE:
    case CBM_DOCLINK_RS_DIS_MACRO_BANG:
    case CBM_DOCLINK_RS_DIS_MACRO_NS:
        return RS_NS_MACRO;
    default:
        return RS_NS_TYPE;
    }
}

/* The item kinds and member kinds a disambiguator accepts ("" for none, NULL
 * for any). */
static void rs_dis_kinds(int dis, const char **items, const char **members, bool *mod) {
    *items = "";
    *members = "";
    *mod = false;
    switch (dis) {
    case CBM_DOCLINK_RS_DIS_NONE:
    case CBM_DOCLINK_RS_DIS_TYPE_NS:
    case CBM_DOCLINK_RS_DIS_VALUE_NS:
    case CBM_DOCLINK_RS_DIS_MACRO_NS:
        *items = NULL;
        *members = NULL;
        *mod = true;
        break;
    case CBM_DOCLINK_RS_DIS_STRUCT:
        *items = "S";
        break;
    case CBM_DOCLINK_RS_DIS_ENUM:
        *items = "E";
        break;
    case CBM_DOCLINK_RS_DIS_TRAIT:
        *items = "R";
        break;
    case CBM_DOCLINK_RS_DIS_UNION:
        *items = "U";
        break;
    case CBM_DOCLINK_RS_DIS_TYALIAS:
        *items = "A";
        break;
    case CBM_DOCLINK_RS_DIS_MOD:
        *mod = true;
        break;
    case CBM_DOCLINK_RS_DIS_CONST:
        *items = "C";
        *members = "c";
        break;
    case CBM_DOCLINK_RS_DIS_STATIC:
        *items = "G";
        break;
    case CBM_DOCLINK_RS_DIS_FN:
        *items = "F";
        *members = "f";
        break;
    case CBM_DOCLINK_RS_DIS_DERIVE:
        *items = "V";
        break;
    case CBM_DOCLINK_RS_DIS_FIELD:
        *members = "d";
        break;
    case CBM_DOCLINK_RS_DIS_VARIANT:
        *members = "v";
        break;
    case CBM_DOCLINK_RS_DIS_MACRO_BANG:
        *items = "MP";
        break;
    default:
        break;
    }
}

/* Drop the candidates of another kind than the disambiguator names. What is
 * outside the repository or not placed has no known kind and stays. */
static void rs_filter_dis(const rs_index_t *ix, rs_set_t *s, int dis) {
    const char *items = NULL;
    const char *members = NULL;
    bool mod = false;
    int w = 0;
    rs_dis_kinds(dis, &items, &members, &mod);
    for (int i = 0; i < s->n; i++) {
        rs_ent_t e = s->v[i];
        bool keep = true;
        if (e.kind == RS_E_ITEM) {
            keep = !items || strchr(items, ix->items[e.idx].kind) != NULL;
        } else if (e.kind == RS_E_MEMBER) {
            keep = !members || strchr(members, ix->members[e.idx].kind) != NULL;
        } else if (e.kind == RS_E_MOD) {
            keep = mod;
        }
        if (keep) {
            s->v[w++] = e;
        }
    }
    s->n = w;
}

/* rustdoc: a trait and a derive macro of one name are the trait. A derive
 * macro of another crate is known only as something external of that name. */
static void rs_trait_wins(const rs_index_t *ix, rs_set_t *s) {
    if (s->overflow || s->n != PAIR_LEN) {
        return;
    }
    for (int i = 0; i < PAIR_LEN; i++) {
        rs_ent_t a = s->v[i];
        rs_ent_t b = s->v[SKIP_ONE - i];
        bool trait = a.kind == RS_E_ITEM && ix->items[a.idx].kind == 'R';
        bool derive =
            b.kind == RS_E_EXTERNAL || (b.kind == RS_E_ITEM && ix->items[b.idx].kind == 'V');
        if (trait && derive) {
            s->v[0] = a;
            s->n = SKIP_ONE;
            return;
        }
    }
}

/* The candidates of a reference: what was really found in any namespace, or,
 * when nothing was, the one weak answer (`*weak`). */
static void rs_candidates(const rs_index_t *ix, const rs_found_t *found, rs_set_t *out,
                          bool *weak) {
    uint8_t w = RS_E_NONE;
    for (int k = 0; k < RS_NS_COUNT; k++) {
        const rs_set_t *s = &found->ns[k];
        uint8_t wk = rs_only_weak(s);
        for (int i = 0; i < s->n; i++) {
            if (!s->v[i].weak) {
                rs_set_add(out, s->v[i]);
            }
        }
        out->overflow = out->overflow || s->overflow;
        w = (w == RS_E_GAP || wk == RS_E_NONE) ? w : wk;
    }
    rs_collapse(ix, out);
    *weak = rs_set_empty(out) && w != RS_E_NONE;
    if (*weak) {
        rs_set_add(out, rs_ent(w, 0));
    }
}

/* True when one of the standard derives in the comma-joined list `derives`
 * generates a method `name` (the methods of the trait each one implements:
 * defined by the language). What a derive of another crate generates is not
 * known and is not assumed. */
static bool rs_derive_declares(const char *derives, const char *name) {
    static const struct {
        const char *derive;
        const char *methods; /* each between commas */
    } table[] = {
        {"Clone", ",clone,clone_from,"},
        {"Debug", ",fmt,"},
        {"Default", ",default,"},
        {"PartialEq", ",eq,ne,"},
        {"PartialOrd", ",partial_cmp,lt,le,gt,ge,"},
        {"Ord", ",cmp,max,min,clamp,"},
        {"Hash", ",hash,hash_slice,"},
    };
    char key[CBM_SZ_64];
    if (snprintf(key, sizeof(key), ",%s,", name) >= (int)sizeof(key)) {
        return false;
    }
    for (const char *p = derives; *p;) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        for (size_t i = 0; i < sizeof(table) / sizeof(table[0]); i++) {
            if (strlen(table[i].derive) == len && memcmp(table[i].derive, p, len) == 0 &&
                strstr(table[i].methods, key)) {
                return true;
            }
        }
        p = comma ? comma + SKIP_ONE : p + len;
    }
    return false;
}

/* A name no associated item of type `item` carries: is it still declared by
 * something the scan does not show as a member? By a standard derive of the
 * type, or by an impl for no named type (`impl<T: Bound> Trait for T`), or
 * possibly among more impls than are searched. */
static bool rs_assoc_hidden(const rs_index_t *ix, int item, const char *name) {
    rs_ent_t end = rs_alias_end(ix, item);
    if (end.kind != RS_E_ITEM) {
        return false;
    }
    const rs_item_t *it = &ix->items[end.idx];
    if (it->kind != 'S' && it->kind != 'E' && it->kind != 'U') {
        return false;
    }
    return (it->derives && rs_derive_declares(it->derives, name)) ||
           it->impl_count > RS_IMPL_SCAN || cbm_ht_get(ix->blanket, name) != NULL;
}

/* A reference that names nothing that was found. */
static void rs_link_miss(const rs_link_t *q, const rs_set_t *owners, bool mismatch,
                         cbm_doclink_outcome_t *out) {
    const char *last = q->segs[q->path.n - SKIP_ONE];
    bool hidden = false;
    for (int i = 0; !mismatch && i < owners->n && !hidden; i++) {
        hidden = owners->v[i].kind == RS_E_ITEM && rs_assoc_hidden(q->ix, owners->v[i].idx, last);
    }
    if (hidden) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
    } else if (!mismatch && q->single && rs_is_local(q, q->segs[0])) {
        out->kind = CBM_DOCLINK_LOCAL;
    }
}

static void rs_link_decide(const rs_link_t *q, const cbm_gbuf_t *graph,
                           cbm_doclink_outcome_t *out) {
    const rs_index_t *ix = q->ix;
    rs_look_t lk = {.ix = ix, .assoc = true};
    rs_found_t found;
    rs_set_t cand = {0};
    bool weak = false;
    bool prim = q->single && RS_IN(RS_PRIMITIVES, q->segs[0]);
    bool any_type = q->dis == CBM_DOCLINK_RS_DIS_NONE || q->dis == CBM_DOCLINK_RS_DIS_TYPE_NS;
    rs_resolve_path(&lk, q->mod, &q->path, rs_dis_mask(q->dis), q->self, CBM_NOT_FOUND, &found);
    if (found.ambiguous) {
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        return;
    }
    rs_candidates(ix, &found, &cand, &weak);
    if (q->dis == CBM_DOCLINK_RS_DIS_PRIM || (prim && (weak || rs_set_empty(&cand)))) {
        /* a primitive type: rustdoc's fallback when the path names nothing */
        out->reason = prim ? CBM_DOCLINK_REASON_EXTERNAL : CBM_DOCLINK_REASON_MISSING;
        return;
    }
    if (prim && any_type) {
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS; /* the primitive, or what the name binds */
        return;
    }
    bool had = !rs_set_empty(&cand);
    rs_filter_dis(ix, &cand, q->dis);
    rs_trait_wins(ix, &cand);
    if (cand.overflow || cand.n > SKIP_ONE) {
        out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        return;
    }
    if (cand.n == 0) {
        rs_link_miss(q, &found.owners, had, out);
        return;
    }
    rs_outcome(q, graph, cand.v[0], out);
    out->exact = !q->single || cand.v[0].via_import;
    if (weak && cand.v[0].kind == RS_E_GAP && q->single && rs_is_local(q, q->segs[0])) {
        out->kind = CBM_DOCLINK_LOCAL;
    }
}

/* One reference of run file `run_file`. */
static void rs_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* Rust keeps no per-file working state */
    const rs_index_t *ix = (const rs_index_t *)index;
    CBMDocLinkRsPath parsed;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    const rs_file_t *f = &ix->files[ix->run_to_file[run_file]];
    if (f->kind != RS_FILE_SOURCE) {
        return;
    }
    if (cbm_doclink_rs_parse_path(link->raw, strlen(link->raw), false, &parsed) !=
        CBM_DOCLINK_RS_PATH_OK) {
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    rs_link_t q = {.ix = ix,
                   .f = f,
                   .link = link,
                   .dis = parsed.dis,
                   .file_level = (link->flags & CBM_DOCLINK_FLAG_FILE) != 0};
    if (!q.file_level && f->err_line && link->def_line >= f->err_line) {
        /* the definition lies where the file no longer parses: its scope is not known */
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    for (int i = 0; i < parsed.nsegs; i++) {
        q.segs[i] = rs_plain(parsed.segs[i]);
    }
    q.path = (rs_path_t){.segs = q.segs, .n = parsed.nsegs, .global = parsed.global};
    q.single = parsed.nsegs == SKIP_ONE && !parsed.global && !rs_is_keyword(q.segs[0]);
    q.mod = q.file_level ? f->mod_base : rs_mod_at(ix, f, link->def_line);
    if (!q.file_level) {
        q.self = rs_self_at(ix, f, link->def_line);
    }
    q.product = rs_source_product(&q);
    rs_link_decide(&q, graph, out);
    if (out->kind == CBM_DOCLINK_EDGE && !q.file_level && rs_unowned(ix, f, link)) {
        out->kind = CBM_DOCLINK_UNRESOLVED; /* no node of its own to leave from */
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        out->target = NULL;
        out->exact = false;
    }
}

/* ── Incremental scope rules ─────────────────────────────────────── */

/* The next line of a blob; false at the end. */
static bool rs_delta_line(const char **cursor, const char **line, size_t *len) {
    const char *p = *cursor;
    if (!p || !*p) {
        return false;
    }
    const char *nl = strchr(p, '\n');
    *line = p;
    *len = nl ? (size_t)(nl - p) : strlen(p);
    *cursor = nl ? nl + SKIP_ONE : p + *len;
    return true;
}

/* The name of an `M start kind name node` line whose member no path can reach
 * under another name: a function, a constant, a type or a field (not a
 * variant, which `use` can rename). The name must be one the incremental
 * route can find in an unresolved reference: ASCII letters, digits and `_`. */
static bool rs_delta_member(const char *line, size_t len, const char **name, size_t *nlen) {
    enum { F_KIND = 2, F_NAME = 3 };
    const char *field[F_NAME + PAIR_LEN];
    int n = 0;
    field[n++] = line;
    for (size_t i = 0; i < len && n < F_NAME + PAIR_LEN; i++) {
        if (line[i] == '\t') {
            field[n++] = line + i + SKIP_ONE;
        }
    }
    if (line[0] != 'M' || n != F_NAME + PAIR_LEN || field[F_NAME] - field[F_KIND] != PAIR_LEN ||
        !strchr("fctd", field[F_KIND][0])) {
        return false;
    }
    const char *s = field[F_NAME];
    size_t sl = (size_t)(field[F_NAME + SKIP_ONE] - s) - SKIP_ONE;
    if (sl > PAIR_LEN && s[0] == 'r' && s[1] == '#') {
        s += PAIR_LEN;
        sl -= PAIR_LEN;
    }
    for (size_t i = 0; i < sl; i++) {
        if (!(isalnum((unsigned char)s[i]) || s[i] == '_')) {
            return false;
        }
    }
    *name = s;
    *nlen = sl;
    return sl > 0;
}

/* Compare a changed file's stored and fresh scopes line by line, in order
 * (both in the persisted form: no line numbers, no per-definition records).
 *
 * Equal scopes leave every other file's resolution alone: other files see a
 * file only through these records.
 *
 * One difference does too: a member (function, constant, type, field of a
 * type, trait or impl) the fresh scope no longer has. A member is reached
 * only by a path that ends in its name, so a reference that depended on it
 * either bound it (an edge into this file: re-resolved with the closure) or
 * names it in its unresolved row: the name is reported.
 *
 * Everything else is GLOBAL: a new or changed line; a removed item, module,
 * import or variant (`use` can bring any of them into another file under
 * another name, so no name finds the references that depended on it); a
 * changed order (same-named declarations own their node by order); and any
 * change of a manifest. */
static int rs_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    const char *sp = stored;
    const char *fp = fresh;
    const char *sl = NULL;
    const char *fl = NULL;
    size_t slen = 0;
    size_t flen = 0;
    bool hs = rs_delta_line(&sp, &sl, &slen);
    bool hf = rs_delta_line(&fp, &fl, &flen);
    while (hs || hf) {
        if (hs && hf && slen == flen && memcmp(sl, fl, slen) == 0) {
            hs = rs_delta_line(&sp, &sl, &slen);
            hf = rs_delta_line(&fp, &fl, &flen);
            continue;
        }
        const char *name = NULL;
        size_t nlen = 0;
        if (!hs || !rs_delta_member(sl, slen, &name, &nlen)) {
            return CBM_DOCLINK_DELTA_GLOBAL;
        }
        if (!removed || !removed(ud, name, nlen)) {
            return CBM_NOT_FOUND;
        }
        hs = rs_delta_line(&sp, &sl, &slen);
    }
    return CBM_DOCLINK_DELTA_LOCAL;
}

const cbm_doclink_resolver_t cbm_doclink_rs_resolver = {
    .langs = {CBM_LANG_RUST, CBM_LANG_TOML},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_RS_SCOPE_TAG,
    .build = rs_build,
    .destroy = rs_destroy,
    .resolve = rs_resolve,
    .scope_delta = rs_scope_delta,
};
