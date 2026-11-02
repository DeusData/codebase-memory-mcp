/*
 * doc_links_jvm.c — Javadoc and KDoc reference resolution for doc_links.h.
 *
 * One index over the Java and the Kotlin files of a repository, built from
 * their doc-link scopes (internal/cbm/doclink_jvm.c): on the JVM a package is
 * one namespace whatever language declares into it, so a Kotlin import of a
 * Java class and a Javadoc link to a Kotlin class must meet in one type table.
 * The language of the SOURCE file decides the lookup order; where a target's
 * node is comes from the target's own scope record (slot, own), never from a
 * naming scheme assumed here. Graph nodes are looked up by qualified name
 * only, and only their label and file path are read, so the closure-repair
 * route (whose unchanged nodes are line-less proxies) resolves exactly as a
 * full build does.
 *
 * Entities. A type entity is its binary-style name: package + '.' + the
 * dotted logical path (com.acme.Outer.Inner). Several declarations of one
 * name (source sets, expect/actual) are one entity. A member belongs to the
 * declaration that holds it; overloads of one type share a node.
 *
 * Lookup (the first level with a visible candidate decides; more than one
 * entity there is ambiguous, never a pick):
 *   Java type name (javac's order)
 *     the method's own type variables (local) -> the enclosing types,
 *     innermost first (the type and its member types, its type variables, the
 *     member types it inherits) -> the file's own top-level types and its
 *     single imports (a static import imports a member type too) -> the
 *     file's package -> on-demand imports, with java.lang among them.
 *     A qualified name: the head through those levels, the rest as member
 *     types; a head no level has is read from the root (packages, then a
 *     type). `Type.member` is no reference: Javadoc wants a '#'.
 *   Java METHOD, `#m` and `Type#m`: javadoc's own search (jr_walk, read in
 *     the JDK source), from the documented definition's type or the type
 *     named: the type's own methods -> its superclass, by the whole search
 *     (its superclass, its interfaces, the type around IT) -> its
 *     interfaces in the order written, likewise -> the type around it,
 *     likewise; each type once, the first method found. A type outside the
 *     repository on the way, or a method the language gives (Object's,
 *     Enum's, Annotation's, an enum's values/valueOf, a record's
 *     equals/hashCode/toString), ends it as `external`. Static imports are
 *     no step: javadoc does not search them for a member.
 *   Java FIELD, `#x` and `Type#x` without a parameter list: javadoc looks in
 *     the type, then the types around it, then the supertypes; an inherited
 *     field binds only when no type further out has one of that name, else
 *     it is ambiguous. ({@value Type#X}: the type and its supertypes.)
 *   Java kinds
 *     `#x` is the field when javadoc takes it over the method its search
 *     finds: the field's class is the method's class or below it, also for
 *     every type outside the repository the search passes (jr_java_field);
 *     else that method, when it is one of the repository with nothing
 *     unseen before it (javadoc links it), or external.
 *     A parameter list selects callables:
 *     the first type whose overloads have that signature, never another
 *     overload. A declared type variable matches its own name or its erasure.
 *     A method name without a list that has several signatures is ambiguous.
 *     A record's component declares an accessor of its name (no node).
 *     `#Type(..)` names the constructors of that type only. A bare
 *     identifier that is no type is javadoc's `#identifier`.
 *   Kotlin (the order KDoc resolves in)
 *     the definition's own parameters, type parameters and `this` (local) ->
 *     each enclosing type, innermost first: what it holds itself together
 *     with its companion (one scope), its type parameters and constructor
 *     parameters (local), then what it inherits and what its companion
 *     inherits -> explicit imports -> the file's package (types and top-level
 *     declarations) -> star imports -> default imports.
 *     A FUNCTION or PROPERTY found further out than an enclosing type binds
 *     only when that type is known not to have a member of the name: a type
 *     that has it from the language (kotlin.Any's members, an enum class's),
 *     or whose supertypes leave the repository, makes it `external`
 *     (jr_kotlin_simple). A type found further out binds.
 *     A dotted name: the qualifier is a type (through those levels, or from
 *     the root) and the last name what that type holds, inherits, or gets
 *     from an extension in scope whose receiver it is (an extension only
 *     where the type's supertypes all lie in the repository: a member comes
 *     before an extension); or the qualifier is a
 *     package and the last name one of its types or top-level declarations.
 *   Kotlin kinds
 *     a type and a function of one name at one level (a class and its factory
 *     function) are the type: KDoc resolves a classifier before a function
 *     (jr_kl_result). A type without a node of its own is then a graph gap,
 *     never the function. A property and a function of one name are
 *     ambiguous, and so is an overload group.
 *     A `[name]` that resolves to nothing is plain text to KDoc (Markdown):
 *     no reference at all. @see, @throws and @sample name their subject and
 *     keep a row.
 * Visibility: product code never binds a declaration that only test code
 * holds (test_only_target, no fallback to it); `@sample` is exempt.
 * External: a name a single import brings from a package the repository does
 * not declare; a qualified `pkg.Type` whose package it does not declare; a
 * name only an on-demand import of such a package can supply; java.lang and
 * the Kotlin default imports (the languages' own implicit imports, the only
 * name lists here, and consulted after the repository's own declarations); an
 * all-lower-case path under `java`, `javax` or `kotlin` (the platforms' own
 * namespaces; no library family is one), or under a package the file imports
 * from and the repository does not declare; a
 * member that a supertype outside the repository may declare; the members
 * every type has without declaring them (Object's, an enum's).
 * Not indexed: a package below one the repository owns, in a directory the
 * indexer skips by name (discovery's own rule, no file system access here).
 * Graph gaps: a declaration without a definition (slot '-'), one whose
 * qualified name another declaration took (own 0), a node of another kind or
 * file, a package (it has no node), a member a parse error hides, a name the
 * scan could not place, a definition in a part of its file it could not
 * place -- and a reference that resolves, written on a definition that has
 * no node of its own to leave from.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "helpers.h"           /* cbm_fqn_module_source_lang */
#include "discover/discover.h" /* cbm_should_skip_dir */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    JR_KEY_BUF = 2048,
    JR_MAX_SEGS = 32,   /* dotted segments of one reference; more is unparseable */
    JR_MAX_PARAMS = 32, /* written parameters of one reference; more is unparseable */
    JR_PARAM_BUF = 160,
    JR_T_FIELDS = 10,
    JR_M_FIELDS = 12,
    JR_NONE = -1,
};

/* ── Index data ──────────────────────────────────────────────────── */

typedef struct {
    char kind; /* s single, w on-demand, m static single, n static on-demand */
    const char *alias;
    const char *target;
} jr_import_t;

typedef struct {
    uint32_t from;
    uint32_t to;
} jr_lines_t;

/* Line numbers are meaningful only in a file re-extracted by this run: the
 * persisted scope of every other file carries zeros. They are read for the
 * SOURCE file's own context, never for a target. */
typedef struct {
    uint32_t start;
    uint32_t end;
    char kind; /* c i e r a o C t */
    bool incomplete;
    bool own;         /* its definition is the one the graph keeps for the slot */
    const char *path; /* logical path */
    const char *name;
    const char *tparams;
    const char *supers;
    const char
        *slot; /* NULL: no definition; "=": the logical path; else the name below the module */
    const char *pnames; /* run files only */
    int outer;          /* the enclosing type of this file, or JR_NONE */
    int entity;
} jr_type_t;

typedef struct {
    uint32_t start;
    char kind;      /* c callable, v value */
    bool ctor;      /* K */
    bool component; /* p: a record component or constructor property */
    bool own;
    int owner;       /* its type in this file; JR_NONE: a Kotlin top-level declaration */
    int order;       /* position in the scope */
    int shares;      /* the member of this file whose node it shares; JR_NONE: it has none */
    int recv_entity; /* a Kotlin extension's receiver type, when the repository declares it */
    const char *name;
    const char *tparams;
    const char *sig;
    const char *recv;
    const char *slot;
    const char *pnames;
} jr_member_t;

typedef struct {
    const char *rel_path;
    const char *module_qn;
    const char *package;
    bool kotlin;
    bool is_test;
    bool scoped;          /* its scope was read: without one nothing in it resolves */
    jr_import_t *imports; /* single imports, sorted by alias then target */
    int nimports;
    jr_import_t *wild; /* on-demand imports, scope order */
    int nwild;
    jr_type_t *types; /* scope order: an outer type before its nested ones */
    int ntypes;
    jr_member_t *members; /* sorted by (owner, name, order) */
    int nmembers;
    int *by_start; /* member indices sorted by (start, order) */
    jr_lines_t *unplaced;
    int nunplaced;
} jr_file_t;

typedef struct {
    int file;
    int type;
} jr_decl_t;

/* A step of javadoc's search for a method (jr_walk) that is no type of the
 * repository. */
enum {
    JR_W_NONE = JR_NONE,  /* no step: an interface has no superclass */
    JR_W_OBJECT = -2,     /* java.lang.Object */
    JR_W_ENUM = -3,       /* java.lang.Enum, then Object */
    JR_W_ANNOTATION = -4, /* java.lang.annotation.Annotation */
    JR_W_OUTSIDE = -5,    /* a type outside the repository, or a step whose place
                             in the order the index cannot tell */
};

typedef struct {
    const char *fqn;
    const char *name;
    char kind;
    jr_decl_t *decls; /* sorted by file (path order), then scope order */
    int ndecls;
    int dcap;
    int *bases; /* the supertypes its declarations name, as far as the repository declares them */
    int nbases;
    int *closure; /* every supertype the repository declares, nearest first */
    int nclosure;
    int companion;   /* Kotlin: its companion object, or JR_NONE */
    bool open;       /* a supertype outside the repository, or one that could not be bound */
    bool incomplete; /* a declaration of it has members a parse error hides */
    bool any_prod;
    /* javadoc's search for a method at this type (jr_walk): the superclass
     * step (a type, or JR_W_*), the interface steps in the order written,
     * and the type around it */
    int wsuper;
    int *wifaces;
    int nwifaces;
    int encl;
} jr_entity_t;

typedef struct {
    int *items;
    int count;
    int cap;
} jr_ilist_t;

/* What the resolve phase counts for the run's log line. The index itself is
 * read-only then; these are the only words it writes. */
typedef struct {
    _Atomic int64_t source_gaps;       /* resolved, but the documented definition has no node */
    _Atomic int64_t plain_text;        /* KDoc `[name]` that names nothing: plain text */
    _Atomic int64_t type_and_function; /* KDoc name that is a type and a function at one
                                        * level: resolved as the type */
    _Atomic int64_t type_and_function_node; /* ... where the type has a node (an edge) */
    /* Members further on that a lookup did not take, because something
     * before them may have the member unseen (javadoc's search, jr_walk; the
     * guards of jr_kotlin_simple and jr_kotlin_in_type): `external`. */
    _Atomic int64_t java_given;   /* Javadoc method: the language gives one first */
    _Atomic int64_t java_open;    /* ... or a type outside the repository comes first */
    _Atomic int64_t kotlin_given; /* KDoc name: the same two cases */
    _Atomic int64_t kotlin_open;
    _Atomic int64_t kotlin_extension; /* KDoc `Type.name`: an extension, the type's
                                       * hierarchy leaving the repository */
    /* Javadoc fields not taken, because a method that javadoc's search may
     * reach first, outside the repository, may be what it links
     * (jr_java_field). */
    _Atomic int64_t java_field;       /* `#x` */
    _Atomic int64_t java_field_named; /* `Type#x` */
} jr_stats_t;

typedef struct {
    CBMArena arena;
    bool oom;
    jr_stats_t *stats;
    int mode; /* the run's index mode: which directories discovery skips */
    const char *project;
    jr_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    jr_entity_t *ents;
    int nents;
    int ecap;
    CBMHashTable *ent_by_fqn;   /* fqn -> entity + 1 */
    CBMHashTable *pkg_types;    /* "package\x1f name" -> entity + 1 (top-level types) */
    CBMHashTable *packages;     /* every package a file declares */
    CBMHashTable *pkg_prefixes; /* ... and every prefix of one */
    CBMHashTable *pkg_members;  /* "package\x1f name" -> jr_ilist_t* of files (Kotlin top level) */
    CBMHashTable *extensions;   /* "<entity>\x1f name" -> jr_ilist_t* of (file, member) pairs */
    CBMHashTable *quarantine;   /* names of types declared where no scope is known */
    CBMHashTable *quarantine_test; /* the same, declared by test code only */
} jr_index_t;

/* ── Small helpers ───────────────────────────────────────────────── */

/* Index memory. A failed allocation is remembered: whatever a caller does
 * with its NULL, the build as a whole reports failure instead of handing out
 * an index that silently lacks a declaration. */
static void *jr_alloc(jr_index_t *ix, size_t n) {
    void *p = cbm_arena_alloc(&ix->arena, n ? n : SKIP_ONE);
    ix->oom = ix->oom || !p;
    return p;
}

static char *jr_strdup(jr_index_t *ix, const char *s) {
    char *p = cbm_arena_strdup(&ix->arena, s ? s : "");
    ix->oom = ix->oom || !p;
    return p;
}

static bool jr_ilist_push(jr_index_t *ix, jr_ilist_t *l, int v) {
    if (l->count >= l->cap) {
        int ncap = l->cap ? l->cap * PAIR_LEN : CBM_SZ_4;
        int *grown = (int *)jr_alloc(ix, (size_t)ncap * sizeof(int));
        if (!grown) {
            return false;
        }
        if (l->count > 0) {
            memcpy(grown, l->items, (size_t)l->count * sizeof(int));
        }
        l->items = grown;
        l->cap = ncap;
    }
    l->items[l->count++] = v;
    return true;
}

static jr_ilist_t *jr_ht_ilist(jr_index_t *ix, CBMHashTable *ht, const char *key, bool create) {
    jr_ilist_t *l = (jr_ilist_t *)cbm_ht_get(ht, key);
    if (l || !create) {
        return l;
    }
    l = (jr_ilist_t *)jr_alloc(ix, sizeof(*l));
    char *k = jr_strdup(ix, key);
    if (!l || !k) {
        return NULL;
    }
    memset(l, 0, sizeof(*l));
    cbm_ht_set(ht, k, l);
    return l;
}

/* Add `key` to a name set; false when memory ran out. */
static bool jr_mark(jr_index_t *ix, CBMHashTable *ht, const char *key) {
    if (cbm_ht_get(ht, key)) {
        return true;
    }
    char *k = jr_strdup(ix, key);
    if (!k) {
        return false;
    }
    cbm_ht_set(ht, k, (void *)k);
    return true;
}

static bool jr_ci_equal(const char *s, size_t n, const char *word) {
    if (strlen(word) != n) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (tolower((unsigned char)s[i]) != word[i]) {
            return false;
        }
    }
    return true;
}

static bool jr_ci_contains(const char *s, size_t n, const char *word) {
    size_t wl = strlen(word);
    for (size_t i = 0; i + wl <= n; i++) {
        size_t k = 0;
        while (k < wl && tolower((unsigned char)s[i + k]) == word[k]) {
            k++;
        }
        if (k == wl) {
            return true;
        }
    }
    return false;
}

/* True when one word of the directory name s[0, n) says test code: the name
 * is split at '-', '_', '.' and at a lower-to-upper case step (`commonTest`,
 * `json-tests`, `testFixtures`, `smoke-test-http`), and a word is compared
 * whole, so `latest` and `attestation` say nothing. */
static bool jr_test_dir_name(const char *s, size_t n) {
    static const char *const words[] = {"test",   "tests",   "fixture", "fixtures",
                                        "sample", "samples", "example", "examples"};
    size_t start = 0;
    for (size_t i = 0; i <= n; i++) {
        bool sep = i == n || s[i] == '-' || s[i] == '_' || s[i] == '.';
        bool step = !sep && i > start && isupper((unsigned char)s[i]) &&
                    !isupper((unsigned char)s[i - SKIP_ONE]);
        if (!sep && !step) {
            continue;
        }
        for (size_t w = 0; w < sizeof(words) / sizeof(words[0]); w++) {
            if (jr_ci_equal(s + start, i - start, words[w])) {
                return true;
            }
        }
        start = sep ? i + SKIP_ONE : i;
    }
    return false;
}

/* Test code, by where a build puts it:
 *   - the source set after the last `src` directory holds "test" (`src/test`,
 *     `src/jvmTest`, `src/testFixtures`, `src/internalClusterTest`), or
 *   - a directory above that source root (any directory, when there is no
 *     `src`) has a word test, fixture, sample or example in its name: a test
 *     project, a `commonTest/src` source set, a samples tree.
 * Directories below a source set are packages: `src/main/java/org/acme/test/
 * Tool.java` is product code. A file name decides nothing. */
static bool jr_is_test_path(const char *rel) {
    const char *src_set = NULL; /* the segment after the last `src` directory */
    size_t src_set_len = 0;
    const char *last_src = NULL;
    for (const char *p = rel;;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            break;
        }
        if (slash - p == 3 && memcmp(p, "src", 3) == 0) {
            const char *next = slash + SKIP_ONE;
            const char *end = strchr(next, '/');
            last_src = p;
            src_set = end ? next : NULL; /* a file right under src is no source set */
            src_set_len = end ? (size_t)(end - next) : 0;
        }
        p = slash + SKIP_ONE;
    }
    if (src_set && jr_ci_contains(src_set, src_set_len, "test")) {
        return true;
    }
    for (const char *p = rel; !last_src || p < last_src;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            break;
        }
        if (jr_test_dir_name(p, (size_t)(slash - p))) {
            return true;
        }
        p = slash + SKIP_ONE;
    }
    return false;
}

/* Split `s` in place at tabs into at most `max` fields; returns the count. */
static int jr_split(char *s, char **out, int max) {
    int n = 0;
    out[n++] = s;
    for (char *p = s; *p && n < max; p++) {
        if (*p == '\t') {
            *p = '\0';
            out[n++] = p + SKIP_ONE;
        }
    }
    return n;
}

/* True when name[0, nl) is one of the ','-joined names of `list`. An entry may
 * carry a bound (`T:Comparable`, a Java type variable's): *bound / *blen are
 * set to it (length 0 when it has none). */
static bool jr_listed_n(const char *list, const char *name, size_t nl, const char **bound,
                        size_t *blen) {
    for (const char *p = list; p && *p;) {
        const char *e = strchr(p, ',');
        size_t n = e ? (size_t)(e - p) : strlen(p);
        const char *colon = (const char *)memchr(p, ':', n);
        size_t name_len = colon ? (size_t)(colon - p) : n;
        if (name_len == nl && memcmp(p, name, nl) == 0) {
            if (bound) {
                *bound = colon ? colon + SKIP_ONE : p + n;
                *blen = colon ? n - name_len - SKIP_ONE : 0;
            }
            return true;
        }
        p = e ? e + SKIP_ONE : NULL;
    }
    return false;
}

static bool jr_listed(const char *list, const char *name) {
    return jr_listed_n(list, name, strlen(name), NULL, NULL);
}

/* ── Scope parsing ───────────────────────────────────────────────── */

static int jr_import_cmp(const void *a, const void *b) {
    const jr_import_t *x = (const jr_import_t *)a;
    const jr_import_t *y = (const jr_import_t *)b;
    int c = strcmp(x->alias, y->alias);
    return c ? c : strcmp(x->target, y->target);
}

static int jr_member_cmp(const void *a, const void *b) {
    const jr_member_t *x = (const jr_member_t *)a;
    const jr_member_t *y = (const jr_member_t *)b;
    if (x->owner != y->owner) {
        return x->owner < y->owner ? -1 : 1;
    }
    int c = strcmp(x->name, y->name);
    if (c) {
        return c;
    }
    return (x->order > y->order) - (x->order < y->order);
}

typedef struct {
    uint32_t start;
    int order;
    int idx;
} jr_start_key_t;

static int jr_start_cmp(const void *a, const void *b) {
    const jr_start_key_t *x = (const jr_start_key_t *)a;
    const jr_start_key_t *y = (const jr_start_key_t *)b;
    if (x->start != y->start) {
        return x->start < y->start ? -1 : 1;
    }
    return (x->order > y->order) - (x->order < y->order);
}

static int jr_lines_cmp(const void *a, const void *b) {
    const jr_lines_t *x = (const jr_lines_t *)a;
    const jr_lines_t *y = (const jr_lines_t *)b;
    if (x->from != y->from) {
        return x->from < y->from ? -1 : 1;
    }
    return (x->to > y->to) - (x->to < y->to);
}

static void jr_parse_type(jr_file_t *f, char **fld, int *stack, int *sp) {
    jr_type_t *t = &f->types[f->ntypes];
    memset(t, 0, sizeof(*t));
    t->start = (uint32_t)strtoul(fld[1], NULL, 10);
    t->end = (uint32_t)strtoul(fld[2], NULL, 10);
    t->kind = fld[3][0];
    t->incomplete = fld[3][0] && fld[3][1] == '!';
    t->path = fld[4];
    const char *dot = strrchr(t->path, '.');
    t->name = dot ? dot + SKIP_ONE : t->path;
    t->tparams = fld[5];
    t->supers = fld[6];
    t->slot = strcmp(fld[7], "-") == 0 ? NULL : fld[7];
    t->own = fld[8][0] == '1';
    t->pnames = fld[9];
    t->entity = JR_NONE;
    /* the enclosing type: the nearest earlier type whose path is this path
     * without its last name (a nested type follows its outer type) */
    size_t outer_len = dot ? (size_t)(dot - t->path) : 0;
    while (*sp > 0) {
        const char *op = f->types[stack[*sp - SKIP_ONE]].path;
        if (dot && strlen(op) == outer_len && memcmp(op, t->path, outer_len) == 0) {
            break;
        }
        (*sp)--;
    }
    t->outer = (dot && *sp > 0) ? stack[*sp - SKIP_ONE] : JR_NONE;
    stack[(*sp)++] = f->ntypes;
    f->ntypes++;
}

static bool jr_parse_member(jr_file_t *f, char **fld) {
    jr_member_t *m = &f->members[f->nmembers];
    memset(m, 0, sizeof(*m));
    m->start = (uint32_t)strtoul(fld[1], NULL, 10);
    m->kind = fld[2][0];
    m->ctor = strchr(fld[3], 'K') != NULL;
    m->component = strchr(fld[3], 'p') != NULL;
    m->owner = strcmp(fld[4], "-") == 0 ? JR_NONE : atoi(fld[4]);
    if (m->owner >= f->ntypes || (m->owner < 0 && m->owner != JR_NONE) || !fld[5][0] ||
        (m->kind != 'c' && m->kind != 'v')) {
        return false; /* not a record this code wrote */
    }
    m->name = fld[5];
    m->tparams = fld[6];
    m->sig = fld[7];
    m->recv = fld[8];
    m->slot = strcmp(fld[9], "-") == 0 ? NULL : fld[9];
    m->own = fld[10][0] == '1';
    m->pnames = fld[11];
    m->order = f->nmembers;
    m->shares = JR_NONE;
    m->recv_entity = JR_NONE;
    f->nmembers++;
    return true;
}

/* Which member's node each member shares. A value has its own or none.
 * Callables of one name in one type (overloads) share the node of the one
 * that owns their slot. */
static void jr_link_shared_nodes(jr_file_t *f) {
    for (int i = 0; i < f->nmembers;) {
        int j = i;
        int owner = JR_NONE; /* the callable of this run that owns the node */
        while (j < f->nmembers && f->members[j].owner == f->members[i].owner &&
               strcmp(f->members[j].name, f->members[i].name) == 0) {
            jr_member_t *m = &f->members[j];
            if (m->kind == 'c' && m->slot && m->own) {
                owner = j;
            }
            j++;
        }
        for (int k = i; k < j; k++) {
            jr_member_t *m = &f->members[k];
            if (!m->slot) {
                continue;
            }
            if (m->own) {
                m->shares = k;
            } else if (m->kind == 'c' && owner != JR_NONE &&
                       strcmp(f->members[owner].slot, m->slot) == 0 &&
                       f->members[owner].ctor == m->ctor) {
                m->shares = owner;
            }
        }
        i = j;
    }
}

/* Parse one file's scope. false when it is not a scope this code wrote, or
 * memory ran out. */
static bool jr_parse_scope(jr_index_t *ix, jr_file_t *f, const char *blob) {
    char *buf = jr_strdup(ix, blob);
    if (!buf) {
        return false;
    }
    int ni = 0;
    int nt = 0;
    int nm = 0;
    int nx = 0;
    for (const char *p = buf; *p;) {
        ni += *p == 'I';
        nt += *p == 'T';
        nm += *p == 'M';
        nx += *p == 'X';
        const char *nl = strchr(p, '\n');
        if (!nl) {
            break;
        }
        p = nl + SKIP_ONE;
    }
    f->imports = (jr_import_t *)jr_alloc(ix, (size_t)ni * sizeof(jr_import_t));
    f->wild = (jr_import_t *)jr_alloc(ix, (size_t)ni * sizeof(jr_import_t));
    f->types = (jr_type_t *)jr_alloc(ix, (size_t)nt * sizeof(jr_type_t));
    f->members = (jr_member_t *)jr_alloc(ix, (size_t)nm * sizeof(jr_member_t));
    f->unplaced = (jr_lines_t *)jr_alloc(ix, (size_t)nx * sizeof(jr_lines_t));
    int *stack = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)(nt + SKIP_ONE) * sizeof(int));
    if (!f->imports || !f->wild || !f->types || !f->members || !f->unplaced || !stack) {
        cbm_free(CBM_MEM_CLASS_OTHER, stack);
        ix->oom = true;
        return false;
    }
    int sp = 0;
    bool first = true;
    bool ok = true;
    for (char *line = buf; ok && line && *line;) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        char *fld[JR_M_FIELDS];
        if (first) {
            first = false;
            ok = strcmp(line, CBM_DOCLINK_JVM_SCOPE_TAG) == 0;
        } else if (line[0] == 'L') {
            f->kotlin = jr_split(line, fld, PAIR_LEN) == PAIR_LEN && fld[1][0] == 'k';
        } else if (line[0] == 'P') {
            if (jr_split(line, fld, PAIR_LEN) == PAIR_LEN) {
                f->package = fld[1];
            }
        } else if (line[0] == 'I') {
            if (jr_split(line, fld, CBM_SZ_4) == CBM_SZ_4 && fld[1][0] && fld[3][0]) {
                jr_import_t imp = {.kind = fld[1][0], .alias = fld[2], .target = fld[3]};
                if (imp.kind == 'w' || imp.kind == 'n') {
                    f->wild[f->nwild++] = imp;
                } else {
                    f->imports[f->nimports++] = imp;
                }
            }
        } else if (line[0] == 'T') {
            /* a member names its type by position: a type record that cannot
             * be read spoils the whole scope */
            ok = jr_split(line, fld, JR_T_FIELDS) == JR_T_FIELDS && fld[4][0];
            if (ok) {
                jr_parse_type(f, fld, stack, &sp);
            }
        } else if (line[0] == 'M') {
            ok = jr_split(line, fld, JR_M_FIELDS) == JR_M_FIELDS && jr_parse_member(f, fld);
        } else if (line[0] == 'X') {
            if (jr_split(line, fld, 3) == 3) {
                f->unplaced[f->nunplaced++] =
                    (jr_lines_t){.from = (uint32_t)strtoul(fld[1], NULL, 10),
                                 .to = (uint32_t)strtoul(fld[2], NULL, 10)};
            }
        } else if (line[0] == 'Q') {
            if (jr_split(line, fld, PAIR_LEN) == PAIR_LEN && fld[1][0]) {
                ok = jr_mark(ix, f->is_test ? ix->quarantine_test : ix->quarantine, fld[1]);
            }
        }
        line = nl ? nl + SKIP_ONE : NULL;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, stack);
    if (!ok || first) {
        return false;
    }
    if (f->nimports > 1) {
        qsort(f->imports, (size_t)f->nimports, sizeof(jr_import_t), jr_import_cmp);
    }
    if (f->nmembers > 1) {
        qsort(f->members, (size_t)f->nmembers, sizeof(jr_member_t), jr_member_cmp);
    }
    jr_link_shared_nodes(f);
    /* the unplaced line ranges: sorted and disjoint, for a binary search */
    if (f->nunplaced > 1) {
        qsort(f->unplaced, (size_t)f->nunplaced, sizeof(jr_lines_t), jr_lines_cmp);
        int w = 0;
        for (int i = SKIP_ONE; i < f->nunplaced; i++) {
            if (f->unplaced[i].from <= f->unplaced[w].to) {
                if (f->unplaced[i].to > f->unplaced[w].to) {
                    f->unplaced[w].to = f->unplaced[i].to;
                }
            } else {
                f->unplaced[++w] = f->unplaced[i];
            }
        }
        f->nunplaced = w + SKIP_ONE;
    }
    f->by_start = (int *)jr_alloc(ix, (size_t)f->nmembers * sizeof(int));
    jr_start_key_t *keys =
        f->nmembers > 0
            ? (jr_start_key_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)f->nmembers * sizeof(*keys))
            : NULL;
    if (f->nmembers > 0 && (!f->by_start || !keys)) {
        cbm_free(CBM_MEM_CLASS_OTHER, keys);
        ix->oom = true;
        return false;
    }
    for (int i = 0; i < f->nmembers; i++) {
        keys[i] =
            (jr_start_key_t){.start = f->members[i].start, .order = f->members[i].order, .idx = i};
    }
    if (f->nmembers > 1) {
        qsort(keys, (size_t)f->nmembers, sizeof(*keys), jr_start_cmp);
    }
    for (int i = 0; i < f->nmembers; i++) {
        f->by_start[i] = keys[i].idx;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, keys);
    return true;
}

/* ── Entities ────────────────────────────────────────────────────── */

enum { JR_ENT_FAIL = -1, JR_ENT_SKIP = -2 };

/* "package.path", or "path" in the default package. false when it does not
 * fit (a name longer than any real one). */
static bool jr_fqn(const char *package, const char *path, char *out, size_t cap) {
    int n = (package && package[0]) ? snprintf(out, cap, "%s.%s", package, path)
                                    : snprintf(out, cap, "%s", path);
    return n >= 0 && (size_t)n < cap;
}

static bool jr_pair_key(const char *a, const char *b, char *out, size_t cap) {
    int n = snprintf(out, cap, "%s\x1f%s", a ? a : "", b);
    return n >= 0 && (size_t)n < cap;
}

static int jr_ent(const jr_index_t *ix, const char *fqn) {
    intptr_t v = (intptr_t)cbm_ht_get(ix->ent_by_fqn, fqn);
    return v > 0 ? (int)(v - SKIP_ONE) : JR_NONE;
}

/* The entity `fqn`, created on first sight. */
static int jr_entity_get(jr_index_t *ix, const char *fqn, const char *name, char kind) {
    int found = jr_ent(ix, fqn);
    if (found != JR_NONE) {
        return found;
    }
    if (ix->nents >= ix->ecap) {
        int ncap = ix->ecap ? ix->ecap * PAIR_LEN : CBM_SZ_1K;
        jr_entity_t *grown =
            (jr_entity_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * sizeof(jr_entity_t));
        if (!grown) {
            return JR_ENT_FAIL;
        }
        if (ix->nents > 0) {
            memcpy(grown, ix->ents, (size_t)ix->nents * sizeof(jr_entity_t));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
        ix->ents = grown;
        ix->ecap = ncap;
    }
    int id = ix->nents;
    jr_entity_t *e = &ix->ents[id];
    memset(e, 0, sizeof(*e));
    e->fqn = jr_strdup(ix, fqn);
    e->name = name;
    e->kind = kind;
    e->companion = JR_NONE;
    e->wsuper = JR_W_OUTSIDE; /* until jr_resolve_supers has read its declarations */
    e->encl = JR_NONE;
    if (!e->fqn) {
        return JR_ENT_FAIL;
    }
    ix->nents++;
    cbm_ht_set(ix->ent_by_fqn, e->fqn, (void *)(intptr_t)(id + SKIP_ONE));
    return id;
}

static bool jr_entity_add_decl(jr_index_t *ix, jr_entity_t *e, int file, int type) {
    if (e->ndecls >= e->dcap) {
        int ncap = e->dcap ? e->dcap * PAIR_LEN : CBM_SZ_2;
        jr_decl_t *grown = (jr_decl_t *)jr_alloc(ix, (size_t)ncap * sizeof(jr_decl_t));
        if (!grown) {
            return false;
        }
        if (e->ndecls > 0) {
            memcpy(grown, e->decls, (size_t)e->ndecls * sizeof(jr_decl_t));
        }
        e->decls = grown;
        e->dcap = ncap;
    }
    e->decls[e->ndecls++] = (jr_decl_t){.file = file, .type = type};
    e->any_prod = e->any_prod || !ix->files[file].is_test;
    e->incomplete = e->incomplete || ix->files[file].types[type].incomplete;
    return true;
}

static bool jr_mark_package(jr_index_t *ix, const char *package) {
    if (!package || !package[0]) {
        return true;
    }
    if (!jr_mark(ix, ix->packages, package)) {
        return false;
    }
    char buf[JR_KEY_BUF];
    if (snprintf(buf, sizeof(buf), "%s", package) >= (int)sizeof(buf)) {
        return true;
    }
    for (char *dot = strrchr(buf, '.'); dot; dot = strrchr(buf, '.')) {
        *dot = '\0';
        if (!jr_mark(ix, ix->pkg_prefixes, buf)) {
            return false;
        }
    }
    return true;
}

/* The entities of every declared type and the package tables. Files come in
 * path order and types in scope order, so every declaration list is in that
 * order too. *skipped counts declarations whose name is too long to index;
 * their names are quarantined, so a reference to one binds nothing else. */
static bool jr_build_entities(jr_index_t *ix, int *skipped) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        jr_file_t *f = &ix->files[fi];
        if (!jr_mark_package(ix, f->package)) {
            return false;
        }
        for (int ti = 0; ti < f->ntypes; ti++) {
            jr_type_t *t = &f->types[ti];
            char fqn[JR_KEY_BUF];
            char key[JR_KEY_BUF];
            int id = jr_fqn(f->package, t->path, fqn, sizeof(fqn)) &&
                             jr_pair_key(f->package, t->name, key, sizeof(key))
                         ? jr_entity_get(ix, fqn, t->name, t->kind)
                         : JR_ENT_SKIP;
            if (id == JR_ENT_SKIP) {
                (*skipped)++;
                if (!jr_mark(ix, f->is_test ? ix->quarantine_test : ix->quarantine, t->name)) {
                    return false;
                }
                continue;
            }
            if (id < 0 || !jr_entity_add_decl(ix, &ix->ents[id], fi, ti)) {
                return false;
            }
            t->entity = id;
            if (t->kind == 'C' && t->outer != JR_NONE && f->types[t->outer].entity >= 0) {
                ix->ents[f->types[t->outer].entity].companion = id;
            }
            if (t->outer == JR_NONE && !cbm_ht_get(ix->pkg_types, key)) {
                char *k = jr_strdup(ix, key);
                if (!k) {
                    return false;
                }
                cbm_ht_set(ix->pkg_types, k, (void *)(intptr_t)(id + SKIP_ONE));
            }
        }
        /* Kotlin top-level declarations: the files of a package that declare
         * a name (each file once: its members of one name are one run) */
        for (int mi = 0; mi < f->nmembers && f->members[mi].owner == JR_NONE; mi++) {
            const jr_member_t *m = &f->members[mi];
            if (mi > 0 && strcmp(f->members[mi - SKIP_ONE].name, m->name) == 0) {
                continue;
            }
            char key[JR_KEY_BUF];
            if (!jr_pair_key(f->package, m->name, key, sizeof(key))) {
                continue;
            }
            jr_ilist_t *l = jr_ht_ilist(ix, ix->pkg_members, key, true);
            if (!l || !jr_ilist_push(ix, l, fi)) {
                return false;
            }
        }
    }
    return true;
}

/* ── The languages' own implicit imports ─────────────────────────────
 *
 * java.lang (Java and Kotlin/JVM) and the Kotlin default imports are part of
 * the lookup order of every file. They are consulted last, after everything
 * the repository declares, and only say "this simple name is the language's":
 * external. Nothing else here is a list of names. */

static const char *const JR_JAVA_LANG[] = {
    "AbstractMethodError",
    "Appendable",
    "ArithmeticException",
    "ArrayIndexOutOfBoundsException",
    "ArrayStoreException",
    "AssertionError",
    "AutoCloseable",
    "Boolean",
    "BootstrapMethodError",
    "Byte",
    "CharSequence",
    "Character",
    "Class",
    "ClassCastException",
    "ClassCircularityError",
    "ClassFormatError",
    "ClassLoader",
    "ClassNotFoundException",
    "ClassValue",
    "CloneNotSupportedException",
    "Cloneable",
    "Comparable",
    "Compiler",
    "Deprecated",
    "Double",
    "Enum",
    "EnumConstantNotPresentException",
    "Error",
    "Exception",
    "ExceptionInInitializerError",
    "Float",
    "FunctionalInterface",
    "IllegalAccessError",
    "IllegalAccessException",
    "IllegalArgumentException",
    "IllegalCallerException",
    "IllegalMonitorStateException",
    "IllegalStateException",
    "IllegalThreadStateException",
    "IncompatibleClassChangeError",
    "IndexOutOfBoundsException",
    "InheritableThreadLocal",
    "InstantiationError",
    "InstantiationException",
    "Integer",
    "InternalError",
    "InterruptedException",
    "Iterable",
    "LayerInstantiationException",
    "LinkageError",
    "Long",
    "MatchException",
    "Math",
    "Module",
    "ModuleLayer",
    "NegativeArraySizeException",
    "NoClassDefFoundError",
    "NoSuchFieldError",
    "NoSuchFieldException",
    "NoSuchMethodError",
    "NoSuchMethodException",
    "NullPointerException",
    "Number",
    "NumberFormatException",
    "Object",
    "OutOfMemoryError",
    "Override",
    "Package",
    "Process",
    "ProcessBuilder",
    "ProcessHandle",
    "Readable",
    "Record",
    "ReflectiveOperationException",
    "Runnable",
    "Runtime",
    "RuntimeException",
    "RuntimePermission",
    "SafeVarargs",
    "ScopedValue",
    "SecurityException",
    "SecurityManager",
    "Short",
    "StackOverflowError",
    "StackTraceElement",
    "StackWalker",
    "StrictMath",
    "String",
    "StringBuffer",
    "StringBuilder",
    "StringIndexOutOfBoundsException",
    "StringTemplate",
    "SuppressWarnings",
    "System",
    "Thread",
    "ThreadDeath",
    "ThreadGroup",
    "ThreadLocal",
    "Throwable",
    "TypeNotPresentException",
    "UnknownError",
    "UnsatisfiedLinkError",
    "UnsupportedClassVersionError",
    "UnsupportedOperationException",
    "VerifyError",
    "VirtualMachineError",
    "Void",
    "WrongThreadException",
};

/* kotlin.*, kotlin.annotation.*, kotlin.collections.*, kotlin.comparisons.*,
 * kotlin.io.*, kotlin.ranges.*, kotlin.sequences.*, kotlin.text.* and
 * kotlin.jvm.*: their types and the standard scope functions. */
static const char *const JR_KOTLIN_DEFAULT[] = {
    "AbstractCollection",
    "AbstractIterator",
    "AbstractList",
    "AbstractMap",
    "AbstractMutableCollection",
    "AbstractMutableList",
    "AbstractMutableMap",
    "AbstractMutableSet",
    "AbstractSet",
    "Annotation",
    "AnnotationRetention",
    "AnnotationTarget",
    "Any",
    "Array",
    "ArrayDeque",
    "ArrayList",
    "Boolean",
    "BooleanArray",
    "BuilderInference",
    "Byte",
    "ByteArray",
    "Char",
    "CharArray",
    "CharCategory",
    "CharProgression",
    "CharRange",
    "CharSequence",
    "Charsets",
    "ClosedFloatingPointRange",
    "ClosedRange",
    "Collection",
    "Comparable",
    "Comparator",
    "ConcurrentModificationException",
    "DeepRecursiveFunction",
    "Deprecated",
    "DeprecationLevel",
    "Double",
    "DoubleArray",
    "DslMarker",
    "Enum",
    "Float",
    "FloatArray",
    "Function",
    "Grouping",
    "HashMap",
    "HashSet",
    "IndexedValue",
    "Int",
    "IntArray",
    "IntProgression",
    "IntRange",
    "Iterable",
    "Iterator",
    "JvmField",
    "JvmInline",
    "JvmName",
    "JvmOverloads",
    "JvmRecord",
    "JvmStatic",
    "JvmSuppressWildcards",
    "JvmSynthetic",
    "JvmWildcard",
    "KotlinVersion",
    "Lazy",
    "LazyThreadSafetyMode",
    "LinkedHashMap",
    "LinkedHashSet",
    "List",
    "ListIterator",
    "Long",
    "LongArray",
    "LongProgression",
    "LongRange",
    "Map",
    "MatchGroup",
    "MatchGroupCollection",
    "MatchResult",
    "Metadata",
    "MustBeDocumented",
    "MutableCollection",
    "MutableIterable",
    "MutableIterator",
    "MutableList",
    "MutableListIterator",
    "MutableMap",
    "MutableSet",
    "NoSuchElementException",
    "NotImplementedError",
    "Nothing",
    "Number",
    "OpenEndRange",
    "OptIn",
    "Pair",
    "ParameterName",
    "PublishedApi",
    "RandomAccess",
    "Regex",
    "RegexOption",
    "Repeatable",
    "ReplaceWith",
    "RequiresOptIn",
    "Result",
    "Retention",
    "Sequence",
    "SequenceScope",
    "Set",
    "Short",
    "ShortArray",
    "SinceKotlin",
    "String",
    "StringBuilder",
    "Strictfp",
    "Suppress",
    "Synchronized",
    "Target",
    "Throws",
    "Transient",
    "Triple",
    "UByte",
    "UByteArray",
    "UInt",
    "UIntArray",
    "UIntRange",
    "ULong",
    "ULongArray",
    "ULongRange",
    "UShort",
    "UShortArray",
    "UninitializedPropertyAccessException",
    "Unit",
    "Volatile",
    /* functions */
    "TODO",
    "also",
    "apply",
    "arrayListOf",
    "arrayOf",
    "arrayOfNulls",
    "assert",
    "buildList",
    "buildMap",
    "buildSet",
    "buildString",
    "check",
    "checkNotNull",
    "compareBy",
    "compareValues",
    "emptyArray",
    "emptyList",
    "emptyMap",
    "emptySequence",
    "emptySet",
    "enumValueOf",
    "enumValues",
    "error",
    "generateSequence",
    "hashMapOf",
    "hashSetOf",
    "lazy",
    "lazyOf",
    "let",
    "linkedMapOf",
    "linkedSetOf",
    "listOf",
    "listOfNotNull",
    "mapOf",
    "maxOf",
    "minOf",
    "mutableListOf",
    "mutableMapOf",
    "mutableSetOf",
    "print",
    "println",
    "readLine",
    "readln",
    "repeat",
    "require",
    "requireNotNull",
    "run",
    "runCatching",
    "sequence",
    "sequenceOf",
    "setOf",
    "synchronized",
    "takeIf",
    "takeUnless",
    "to",
    "use",
    "with",
};

static const char *const JR_JAVA_PRIMITIVES[] = {"boolean", "byte",  "char",   "short", "int",
                                                 "long",    "float", "double", "void"};

/* The members a type has without declaring them. These lists are defined by
 * the two languages, not collected from a library: the methods of
 * java.lang.Object with their parameter lists ('|'-joined, as a declared
 * signature is normalized), the three members of kotlin.Any, and what an enum
 * has in either language (the methods of java.lang.Enum with the two static
 * ones the language declares; the Kotlin counterparts). Written from knowledge
 * of the two language specifications; not checked online. */
typedef struct {
    const char *name;
    const char *sig;
} jr_given_t;

static const jr_given_t JR_OBJECT_METHODS[] = {
    {"equals", "Object"}, {"hashCode", ""}, {"toString", ""},    {"clone", ""},
    {"finalize", ""},     {"getClass", ""}, {"notify", ""},      {"notifyAll", ""},
    {"wait", ""},         {"wait", "long"}, {"wait", "long|int"}};
static const char *const JR_ANY_MEMBERS[] = {"equals", "hashCode", "toString"};
static const char *const JR_JAVA_ENUM_MEMBERS[] = {
    "values", "valueOf", "name", "ordinal", "compareTo", "getDeclaringClass", "describeConstable"};
static const char *const JR_KOTLIN_ENUM_MEMBERS[] = {"values", "valueOf", "entries",
                                                     "name",   "ordinal", "compareTo"};

static bool jr_named(const char *const *table, size_t n, const char *name) {
    for (size_t i = 0; i < n; i++) {
        if (strcmp(table[i], name) == 0) {
            return true;
        }
    }
    return false;
}

#define JR_NAMED(table, name) jr_named(table, sizeof(table) / sizeof((table)[0]), name)

/* ── Resolution context ──────────────────────────────────────────── */

/* The scan nests types JV_MAX_DEPTH (128) deep at most; a chain is never
 * longer. */
enum { JR_CHAIN_MAX = 136 };

enum { JR_OK = 0, JR_UNRES, JR_LOCAL };

typedef struct {
    int st;
    const cbm_gbuf_node_t *node;
    bool exact;
    int reason;
} jr_res_t;

typedef struct {
    const jr_index_t *ix;
    const cbm_gbuf_t *g;
    int file;
    const jr_file_t *f;
    bool prod;                                 /* test-only declarations are invisible */
    bool sample;                               /* a `@sample` subject: it names a function */
    bool inherit;                              /* false while supertypes are being bound */
    int chain[JR_CHAIN_MAX];                   /* enclosing types, innermost first */
    const jr_type_t *chain_decl[JR_CHAIN_MAX]; /* ... and their declarations in this file */
    int nchain;
    const jr_member_t *member; /* the documented member, when the definition is one */
    const jr_type_t *type;     /* the documented type, when the definition is one */
} jr_ctx_t;

static jr_res_t jr_edge(const cbm_gbuf_node_t *n, bool exact) {
    return (jr_res_t){.st = JR_OK, .node = n, .exact = exact};
}

static jr_res_t jr_unres(int reason) {
    return (jr_res_t){.st = JR_UNRES, .reason = reason};
}

static bool jr_visible(const jr_ctx_t *c, int ent) {
    return !c->prod || c->ix->ents[ent].any_prod;
}

/* The enclosing types of type `t` of the context's file, `t` first. */
static void jr_chain_from(jr_ctx_t *c, int t) {
    while (t != JR_NONE && c->nchain < JR_CHAIN_MAX) {
        const jr_type_t *ty = &c->f->types[t];
        if (ty->entity >= 0) {
            c->chain[c->nchain] = ty->entity;
            c->chain_decl[c->nchain] = ty;
            c->nchain++;
        }
        t = ty->outer;
    }
}

/* ── Type candidates of one scope level ──────────────────────────── */

typedef struct {
    char kind;      /* 0 none; T a type, X a name outside the repository, G a name the
                       repository declares where no scope shows it, N a name in a
                       package of the repository that the indexer does not read */
    int ent;        /* T: the type */
    int count;      /* distinct candidates; 2 stands for "several" */
    bool invisible; /* a candidate product code may not bind was passed over */
    bool exact;     /* named by an explicit import */
} jr_cands_t;

static void jr_cand(jr_cands_t *out, char kind, int ent) {
    if (out->count > 0 && out->kind == kind && out->ent == ent) {
        return;
    }
    if (out->count == 0) {
        out->kind = kind;
        out->ent = ent;
    }
    out->count += out->count < PAIR_LEN;
}

/* A type, unless the source may not name it: product code does not see what
 * only test code declares, and Java does not see a Kotlin type alias. */
static void jr_cand_type(const jr_ctx_t *c, jr_cands_t *out, int ent) {
    if (ent < 0 || (!c->f->kotlin && c->ix->ents[ent].kind == 't')) {
        return;
    }
    if (!jr_visible(c, ent)) {
        out->invisible = true;
        return;
    }
    jr_cand(out, 'T', ent);
}

/* ── Types: lookup tables ────────────────────────────────────────── */

static int jr_pkg_type(const jr_index_t *ix, const char *package, const char *name) {
    char key[JR_KEY_BUF];
    if (!jr_pair_key(package, name, key, sizeof(key))) {
        return JR_NONE;
    }
    intptr_t v = (intptr_t)cbm_ht_get(ix->pkg_types, key);
    return v > 0 ? (int)(v - SKIP_ONE) : JR_NONE;
}

/* The type `name` nested directly in entity `ent`. */
static int jr_nested(const jr_index_t *ix, int ent, const char *name) {
    char fqn[JR_KEY_BUF];
    if (!jr_fqn(ix->ents[ent].fqn, name, fqn, sizeof(fqn))) {
        return JR_NONE;
    }
    return jr_ent(ix, fqn);
}

/* The member type `name` of `ent`: its own, else the one of the nearest
 * supertype that declares one. */
static int jr_member_type(const jr_ctx_t *c, int ent, const char *name) {
    int own = jr_nested(c->ix, ent, name);
    if (own != JR_NONE || !c->inherit) {
        return own;
    }
    const jr_entity_t *e = &c->ix->ents[ent];
    for (int i = 0; i < e->nclosure; i++) {
        int n = jr_nested(c->ix, e->closure[i], name);
        if (n != JR_NONE) {
            return n;
        }
    }
    return JR_NONE;
}

static bool jr_open(const jr_index_t *ix, int ent) {
    const jr_entity_t *e = &ix->ents[ent];
    if (e->open) {
        return true;
    }
    for (int i = 0; i < e->nclosure; i++) {
        if (ix->ents[e->closure[i]].open) {
            return true;
        }
    }
    return false;
}

/* True when `sup` is `ent` or one of its supertypes. */
static bool jr_is_or_extends(const jr_index_t *ix, int ent, int sup) {
    if (ent == sup) {
        return true;
    }
    const jr_entity_t *e = &ix->ents[ent];
    for (int i = 0; i < e->nclosure; i++) {
        if (e->closure[i] == sup) {
            return true;
        }
    }
    return false;
}

/* ── Node binding ────────────────────────────────────────────────── */

/* Representative ordering of the declarations of one entity, without node
 * ids or lines: the source file itself, the longest common directory prefix
 * with it, then path and declaration order. */
static int jr_common_dirs(const char *a, const char *b) {
    int n = 0;
    for (;;) {
        const char *sa = strchr(a, '/');
        const char *sb = strchr(b, '/');
        if (!sa || !sb) {
            return n;
        }
        size_t la = (size_t)(sa - a);
        if (la != (size_t)(sb - b) || memcmp(a, b, la) != 0) {
            return n;
        }
        n++;
        a = sa + SKIP_ONE;
        b = sb + SKIP_ONE;
    }
}

static bool jr_decl_better(const jr_index_t *ix, int fa, int oa, int fb, int ob, const char *src) {
    const char *ra = ix->files[fa].rel_path;
    const char *rb = ix->files[fb].rel_path;
    bool sa = strcmp(ra, src) == 0;
    bool sb = strcmp(rb, src) == 0;
    if (sa != sb) {
        return sa;
    }
    int pa = jr_common_dirs(ra, src);
    int pb = jr_common_dirs(rb, src);
    if (pa != pb) {
        return pa > pb;
    }
    int cmp = strcmp(ra, rb);
    if (cmp != 0) {
        return cmp < 0;
    }
    return oa < ob;
}

static bool jr_label_callable(const char *l) {
    return l && (strcmp(l, "Method") == 0 || strcmp(l, "Function") == 0);
}

static bool jr_label_value(const char *l) {
    return l && (strcmp(l, "Field") == 0 || strcmp(l, "Variable") == 0);
}

/* The node at "<module>.<tail>" when it is this file's own: one node per
 * qualified name, and across files the graph keeps one file's. */
static const cbm_gbuf_node_t *jr_node_at(const jr_ctx_t *c, const jr_file_t *f, const char *tail) {
    char qn[JR_KEY_BUF];
    if (snprintf(qn, sizeof(qn), "%s.%s", f->module_qn, tail) >= (int)sizeof(qn)) {
        return NULL;
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    return (n && n->file_path && strcmp(n->file_path, f->rel_path) == 0) ? n : NULL;
}

/* The node of a type declaration; NULL when it has none of its own. */
static const cbm_gbuf_node_t *jr_type_node(const jr_ctx_t *c, jr_decl_t d) {
    const jr_file_t *f = &c->ix->files[d.file];
    const jr_type_t *t = &f->types[d.type];
    if (!t->slot || !t->own) {
        return NULL;
    }
    const cbm_gbuf_node_t *n = jr_node_at(c, f, strcmp(t->slot, "=") == 0 ? t->path : t->slot);
    return (n && cbm_label_is_type_like(n->label)) ? n : NULL;
}

/* The node of a member: its own, or the one the overloads of its name share.
 * NULL when it has none, or when the slot holds a node of the other kind. */
static const cbm_gbuf_node_t *jr_member_node(const jr_ctx_t *c, int file, const jr_member_t *m) {
    const jr_file_t *f = &c->ix->files[file];
    if (m->shares == JR_NONE) {
        return NULL;
    }
    const jr_member_t *o = &f->members[m->shares];
    char tail[JR_KEY_BUF];
    const char *slot = o->slot;
    if (strcmp(slot, "=") == 0) {
        int n = o->owner == JR_NONE
                    ? snprintf(tail, sizeof(tail), "%s", o->name)
                    : snprintf(tail, sizeof(tail), "%s.%s", f->types[o->owner].path, o->name);
        if (n < 0 || n >= (int)sizeof(tail)) {
            return NULL;
        }
        slot = tail;
    }
    const cbm_gbuf_node_t *n = jr_node_at(c, f, slot);
    if (!n) {
        return NULL;
    }
    return (o->kind == 'c' ? jr_label_callable(n->label) : jr_label_value(n->label)) ? n : NULL;
}

/* The entity's representative node: never a test declaration for product
 * code. An entity none of whose eligible declarations has a node is a graph
 * gap. */
static jr_res_t jr_type_result(const jr_ctx_t *c, int ent, bool exact) {
    const jr_index_t *ix = c->ix;
    const jr_entity_t *e = &ix->ents[ent];
    const cbm_gbuf_node_t *best = NULL;
    int bf = 0;
    int bo = 0;
    for (int d = 0; d < e->ndecls; d++) {
        int file = e->decls[d].file;
        if (c->prod && ix->files[file].is_test) {
            continue;
        }
        const cbm_gbuf_node_t *n = jr_type_node(c, e->decls[d]);
        if (n && (!best || jr_decl_better(ix, file, e->decls[d].type, bf, bo, c->f->rel_path))) {
            best = n;
            bf = file;
            bo = e->decls[d].type;
        }
    }
    return best ? jr_edge(best, exact) : jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
}

/* ── Reference syntax ────────────────────────────────────────────── */

typedef struct {
    char text[CBM_DOCLINK_JVM_REF_MAX + CBM_SZ_8]; /* the segments point in here */
    char *segs[JR_MAX_SEGS];
    int nsegs;
    char *member; /* Java: the name after '#' (or of a bare `name(..)`); NULL when none */
    bool has_params;
    char params[JR_MAX_PARAMS][JR_PARAM_BUF];
    int nparams;
} jr_ref_t;

static bool jr_ident(const char *s) {
    unsigned char first = (unsigned char)s[0];
    if (!(isalpha(first) || first == '_' || first == '$' || first >= 0x80)) {
        return false;
    }
    for (const char *p = s + SKIP_ONE; *p; p++) {
        unsigned char u = (unsigned char)*p;
        if (!(isalnum(u) || u == '_' || u == '$' || u >= 0x80)) {
            return false;
        }
    }
    return true;
}

/* Split a dotted path in place into identifiers; false when a part is none
 * or there are more parts than a name has. */
static bool jr_split_path(char *s, jr_ref_t *r) {
    r->nsegs = 0;
    if (!s[0]) {
        return true;
    }
    for (char *p = s;;) {
        char *dot = strchr(p, '.');
        if (dot) {
            *dot = '\0';
        }
        if (r->nsegs >= JR_MAX_SEGS || !jr_ident(p)) {
            return false;
        }
        r->segs[r->nsegs++] = p;
        if (!dot) {
            return true;
        }
        p = dot + SKIP_ONE;
    }
}

/* The written parameter list s[0, n) of a Javadoc member reference:
 * normalized like the declared signatures. false when it has more
 * parameters than any method. */
static bool jr_parse_params(const char *s, size_t n, jr_ref_t *r) {
    r->has_params = true;
    r->nparams = 0;
    bool any = false;
    for (size_t i = 0; i < n; i++) {
        any = any || !isspace((unsigned char)s[i]);
    }
    if (!any) {
        return true;
    }
    size_t start = 0;
    int depth = 0;
    for (size_t i = 0; i <= n; i++) {
        char ch = i < n ? s[i] : ',';
        if (ch == '<' || ch == '(' || ch == '[') {
            depth++;
        } else if (ch == '>' || ch == ')' || ch == ']') {
            depth--;
        } else if (ch == ',' && depth <= 0) {
            if (r->nparams >= JR_MAX_PARAMS) {
                return false;
            }
            char *p = r->params[r->nparams];
            cbm_doclink_java_norm_type(s + start, i - start, p, sizeof(r->params[0]));
            if (p[0] == '?' && p[SKIP_ONE] == '\0') {
                return false; /* no type, or one longer than any */
            }
            r->nparams++;
            start = i + SKIP_ONE;
        }
    }
    return true;
}

/* `[module/]Type[#member[(params)]]`, `#member[(params)]`, `member(params)`. */
static bool jr_parse_java(const char *raw, jr_ref_t *r) {
    memset(r, 0, sizeof(*r));
    size_t len = strlen(raw);
    if (len == 0 || len > CBM_DOCLINK_JVM_REF_MAX) {
        return false; /* over-long: never resolve a truncated prefix */
    }
    memcpy(r->text, raw, len + SKIP_ONE);
    char *s = r->text;
    char *hash = strchr(s, '#');
    char *paren = strchr(s, '(');
    char *slash = strchr(s, '/');
    if (slash && (!hash || slash < hash) && (!paren || slash < paren)) {
        s = slash + SKIP_ONE; /* a module prefix */
    }
    char *type = s;
    char *member = NULL;
    if (hash) {
        *hash = '\0';
        member = hash + SKIP_ONE;
    } else if (paren) {
        member = s; /* `name(..)`: a member of the documented type */
        type = NULL;
    }
    if (member) {
        char *mp = strchr(member, '(');
        if (mp) {
            size_t ml = strlen(mp);
            if (mp[ml - SKIP_ONE] != ')' || !jr_parse_params(mp + SKIP_ONE, ml - PAIR_LEN, r)) {
                return false; /* a list that does not close, or is longer than any */
            }
            *mp = '\0';
        }
        if (!jr_ident(member)) {
            return false;
        }
        r->member = member;
    }
    if (type) {
        /* without type arguments and array brackets */
        char *lt = strchr(type, '<');
        if (lt) {
            *lt = '\0';
        }
        size_t tl = strlen(type);
        while (tl >= PAIR_LEN && type[tl - PAIR_LEN] == '[' && type[tl - SKIP_ONE] == ']') {
            tl -= PAIR_LEN;
            type[tl] = '\0';
        }
        if (!jr_split_path(type, r)) {
            return false;
        }
    }
    return r->nsegs > 0 || r->member != NULL;
}

/* A KDoc name: dotted, a segment may be backticked. */
static bool jr_parse_kotlin(const char *raw, jr_ref_t *r) {
    memset(r, 0, sizeof(*r));
    size_t len = strlen(raw);
    if (len == 0 || len > CBM_DOCLINK_JVM_REF_MAX) {
        return false;
    }
    /* copy without backticks; a dot inside backticks is no separator, and a
     * name that holds one cannot be a path segment here */
    size_t w = 0;
    bool quoted = false;
    for (size_t i = 0; i < len; i++) {
        if (raw[i] == '`') {
            quoted = !quoted;
        } else if (quoted && raw[i] == '.') {
            return false;
        } else {
            r->text[w++] = raw[i];
        }
    }
    r->text[w] = '\0';
    if (quoted) {
        return false;
    }
    if (w > PAIR_LEN && r->text[w - PAIR_LEN] == '(' && r->text[w - SKIP_ONE] == ')') {
        r->text[w - PAIR_LEN] = '\0'; /* `@see name()` */
    }
    r->nsegs = 0;
    for (char *p = r->text;;) {
        char *dot = strchr(p, '.');
        if (dot) {
            *dot = '\0';
        }
        if (r->nsegs >= JR_MAX_SEGS || !p[0] || strpbrk(p, "\t()#<>[]{}/\\:;,\"'")) {
            return false;
        }
        r->segs[r->nsegs++] = p;
        if (!dot) {
            break;
        }
        p = dot + SKIP_ONE;
    }
    return r->nsegs > 0;
}

/* ── Members of a type ───────────────────────────────────────────── */

enum {
    JR_SEL_VALUES = 1,    /* fields, properties, enum constants, components */
    JR_SEL_CALLABLES = 2, /* methods and functions, no constructors */
    JR_SEL_CTORS = 4,     /* constructors only */
    JR_SEL_SIG = 8,       /* of the callables, those with the written signature */
};

typedef struct {
    int count;                   /* the selected members */
    bool several;                /* ... are more than one thing: different signatures,
                                    or members of different owners */
    bool component;              /* one is a record component or constructor property */
    bool invisible;              /* members left out because product code may not bind
                                    what only test code declares */
    bool uncompared;             /* callables left out because a Javadoc signature cannot
                                    be compared with a Kotlin declaration's */
    const char *sig;             /* the first one's */
    const cbm_gbuf_node_t *node; /* the representative node of those that have one */
    int node_file;
    int node_order;
} jr_found_t;

/* Index of the first member of type `type` in `f` named `name` (the members
 * are sorted by owner, name, order); f->nmembers when there is none before
 * the end. */
static int jr_member_lower(const jr_file_t *f, int type, const char *name) {
    int lo = 0;
    int hi = f->nmembers;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        const jr_member_t *m = &f->members[mid];
        bool below = m->owner != type ? m->owner < type : strcmp(m->name, name) < 0;
        if (below) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/* The suffix of a normalized type that says how many array levels it has. */
static size_t jr_array_suffix(const char *s, size_t n) {
    size_t e = n;
    while (e >= PAIR_LEN && s[e - PAIR_LEN] == '[' && s[e - SKIP_ONE] == ']') {
        e -= PAIR_LEN;
    }
    return e;
}

/* True when `name[0, n)` is a type variable in scope of a member of type
 * `owner` whose own type parameters are `mtparams`: the member's, its type's
 * or an enclosing type's. *bound / *blen: its first bound. */
static bool jr_is_tvar(const jr_file_t *f, const char *mtparams, int owner, const char *name,
                       size_t n, const char **bound, size_t *blen) {
    if (n == 0) {
        return false;
    }
    if (jr_listed_n(mtparams, name, n, bound, blen)) {
        return true;
    }
    for (int t = owner; t != JR_NONE; t = f->types[t].outer) {
        if (jr_listed_n(f->types[t].tparams, name, n, bound, blen)) {
            return true;
        }
    }
    return false;
}

/* One written parameter type against one declared type: the same name, or
 * the erasure of a declared type variable (its first bound, Object when it
 * has none) with the same array rank. Nothing else: javadoc matches a
 * signature against the declared types and against their erasures. */
static bool jr_param_match(const jr_file_t *f, const char *mtparams, int owner, const char *written,
                           const char *declared, size_t dn) {
    static const char object[] = "Object";
    size_t wn = strlen(written);
    if (wn == SKIP_ONE && written[0] == '?') {
        return false; /* a written type that is no type */
    }
    if (wn == dn && memcmp(written, declared, wn) == 0) {
        return true;
    }
    size_t wb = jr_array_suffix(written, wn);
    size_t db = jr_array_suffix(declared, dn);
    const char *bound = NULL;
    size_t blen = 0;
    if ((wn - wb) != (dn - db) || !jr_is_tvar(f, mtparams, owner, declared, db, &bound, &blen)) {
        return false;
    }
    if (blen == 0) {
        bound = object;
        blen = sizeof(object) - SKIP_ONE;
    }
    return wb == blen && memcmp(written, bound, blen) == 0;
}

/* True when the written parameter list is the canonical one of the record
 * `owner` declares: its components' types, in declaration order. The written
 * list is at most JR_MAX_PARAMS long, which bounds the passes over the
 * record's members. */
static bool jr_canonical_match(const jr_file_t *f, int owner, const jr_ref_t *r) {
    int first = jr_member_lower(f, owner, "");
    int components = 0;
    for (int i = first; i < f->nmembers && f->members[i].owner == owner; i++) {
        components += f->members[i].component;
    }
    if (components != r->nparams) {
        return false;
    }
    int last_order = JR_NONE;
    for (int k = 0; k < r->nparams; k++) {
        const jr_member_t *next = NULL; /* the component with the next larger order */
        for (int i = first; i < f->nmembers && f->members[i].owner == owner; i++) {
            const jr_member_t *x = &f->members[i];
            if (x->component && x->order > last_order && (!next || x->order < next->order)) {
                next = x;
            }
        }
        if (!next || !jr_param_match(f, "", owner, r->params[k], next->sig, strlen(next->sig))) {
            return false;
        }
        last_order = next->order;
    }
    return true;
}

/* The written parameter types against a declared signature. */
static bool jr_sig_match(const jr_file_t *f, const jr_member_t *m, const jr_ref_t *r) {
    if (m->ctor && strcmp(m->sig, "*") == 0) {
        return jr_canonical_match(f, m->owner, r);
    }
    const char *p = m->sig;
    for (int i = 0; i < r->nparams; i++) {
        if (!p || !*p) {
            return false; /* fewer declared than written */
        }
        const char *bar = strchr(p, '|');
        size_t dn = bar ? (size_t)(bar - p) : strlen(p);
        if (!jr_param_match(f, m->tparams, m->owner, r->params[i], p, dn)) {
            return false;
        }
        p = bar ? bar + SKIP_ONE : NULL;
    }
    return r->nparams == 0 ? m->sig[0] == '\0' : p == NULL;
}

/* Count member `m` of `file` among the selected ones. */
static void jr_found_take(const jr_ctx_t *c, jr_found_t *out, int file, const jr_member_t *m) {
    if (out->count == 0) {
        out->sig = m->sig;
    } else if (strcmp(out->sig, m->sig) != 0) {
        out->several = true;
    }
    out->count++;
    out->component = out->component || m->component;
    const cbm_gbuf_node_t *n = jr_member_node(c, file, m);
    if (n && (!out->node || jr_decl_better(c->ix, file, m->order, out->node_file, out->node_order,
                                           c->f->rel_path))) {
        out->node = n;
        out->node_file = file;
        out->node_order = m->order;
    }
}

/* Add to `out` the members named `name` that `sel` selects among those `type`
 * of `file` declares (JR_NONE: the file's top-level declarations). */
static void jr_members_in(const jr_ctx_t *c, int file, int type, const char *name, int sel,
                          const jr_ref_t *r, jr_found_t *out) {
    const jr_file_t *f = &c->ix->files[file];
    for (int i = jr_member_lower(f, type, name); i < f->nmembers; i++) {
        const jr_member_t *m = &f->members[i];
        if (m->owner != type || strcmp(m->name, name) != 0) {
            break;
        }
        bool take = m->kind == 'v' ? (sel & JR_SEL_VALUES) != 0
                                   : (sel & (m->ctor ? JR_SEL_CTORS : JR_SEL_CALLABLES)) != 0;
        if (!take) {
            continue;
        }
        if (c->prod && f->is_test) {
            out->invisible = true;
            continue;
        }
        if ((sel & JR_SEL_SIG) && m->kind == 'c') {
            if (f->kotlin) {
                out->uncompared = true; /* Kotlin writes its types another way */
                continue;
            }
            if (!jr_sig_match(f, m, r)) {
                continue;
            }
        }
        jr_found_take(c, out, file, m);
    }
}

/* The members of entity `ent` named `name` that `sel` selects, across its
 * declarations. */
static jr_found_t jr_members(const jr_ctx_t *c, int ent, const char *name, int sel,
                             const jr_ref_t *r) {
    const jr_entity_t *e = &c->ix->ents[ent];
    jr_found_t out = {0};
    for (int d = 0; d < e->ndecls; d++) {
        jr_members_in(c, e->decls[d].file, e->decls[d].type, name, sel, r, &out);
    }
    return out;
}

/* The top-level declarations named `name` of package `pkg`, across its
 * files. */
static jr_found_t jr_top_members(const jr_ctx_t *c, const char *pkg, const char *name, int sel) {
    jr_found_t out = {0};
    char key[JR_KEY_BUF];
    if (!jr_pair_key(pkg, name, key, sizeof(key))) {
        return out;
    }
    const jr_ilist_t *l = (const jr_ilist_t *)cbm_ht_get(c->ix->pkg_members, key);
    for (int i = 0; l && i < l->count; i++) {
        jr_members_in(c, l->items[i], JR_NONE, name, sel, NULL, &out);
    }
    return out;
}

/* Join the members found under one name in two places. Members of different
 * owners are different things, whatever their signatures. */
static void jr_found_add(const jr_ctx_t *c, jr_found_t *into, const jr_found_t *add,
                         bool same_owner) {
    into->invisible = into->invisible || add->invisible;
    into->uncompared = into->uncompared || add->uncompared;
    if (add->count == 0) {
        return;
    }
    if (into->count == 0) {
        bool invisible = into->invisible;
        bool uncompared = into->uncompared;
        *into = *add;
        into->invisible = invisible;
        into->uncompared = uncompared;
        return;
    }
    into->several =
        into->several || add->several || !same_owner || strcmp(into->sig, add->sig) != 0;
    into->count += add->count;
    into->component = into->component || add->component;
    if (add->node &&
        (!into->node || jr_decl_better(c->ix, add->node_file, add->node_order, into->node_file,
                                       into->node_order, c->f->rel_path))) {
        into->node = add->node;
        into->node_file = add->node_file;
        into->node_order = add->node_order;
    }
}

/* The selected members as a result: their node, or a graph gap when the
 * source declares them and the graph has no node for them. */
static jr_res_t jr_found_result(const jr_found_t *found, bool exact) {
    return found->node ? jr_edge(found->node, exact) : jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
}

/* ── Type paths ──────────────────────────────────────────────────── */

typedef enum { JR_TP_OK = 0, JR_TP_PARTIAL, JR_TP_RES } jr_tp_kind_t;

typedef struct {
    jr_tp_kind_t st;
    int ent;        /* OK: the type; PARTIAL: the last type reached */
    int failed_seg; /* PARTIAL: index of the segment that is no member type */
    bool exact;
    bool unfound; /* RES: no scope level has the head (the result says why not) */
    jr_res_t res; /* RES */
} jr_tp_t;

static jr_tp_t jr_tp_res(jr_res_t r) {
    return (jr_tp_t){.st = JR_TP_RES, .res = r};
}

static bool jr_join(char *const *segs, int from, int to, char *out, size_t cap) {
    size_t w = 0;
    out[0] = '\0';
    for (int i = from; i < to; i++) {
        int k = snprintf(out + w, cap - w, "%s%s", w ? "." : "", segs[i]);
        if (k < 0 || (size_t)k >= cap - w) {
            return false;
        }
        w += (size_t)k;
    }
    return true;
}

/* Walk segs[from, to) as member types of `cur` (own, then inherited). */
static jr_tp_t jr_walk_nested(const jr_ctx_t *c, int cur, char *const *segs, int from, int to,
                              bool exact) {
    for (int i = from; i < to; i++) {
        int next = jr_member_type(c, cur, segs[i]);
        if (next == JR_NONE) {
            return (jr_tp_t){.st = JR_TP_PARTIAL, .ent = cur, .failed_seg = i, .exact = exact};
        }
        if (!jr_visible(c, next)) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
        }
        cur = next;
    }
    return (jr_tp_t){.st = JR_TP_OK, .ent = cur, .exact = exact};
}

/* The file's single import of the simple name `name` with one of `kinds`;
 * the next one after `prev` (the imports are sorted by alias). */
static const jr_import_t *jr_import_next(const jr_file_t *f, const char *name,
                                         const jr_import_t *prev) {
    int i;
    if (prev) {
        i = (int)(prev - f->imports) + SKIP_ONE;
    } else {
        int lo = 0;
        int hi = f->nimports;
        while (lo < hi) {
            int mid = lo + ((hi - lo) / PAIR_LEN);
            if (strcmp(f->imports[mid].alias, name) < 0) {
                lo = mid + SKIP_ONE;
            } else {
                hi = mid;
            }
        }
        i = lo;
    }
    return (i < f->nimports && strcmp(f->imports[i].alias, name) == 0) ? &f->imports[i] : NULL;
}

/* True when the repository declares package `p`. */
static bool jr_is_package(const jr_index_t *ix, const char *p) {
    return p[0] && cbm_ht_get(ix->packages, p) != NULL;
}

/* The package part of a qualified name: everything before its last name.
 * false for a simple name. */
static bool jr_package_of(const char *fqn, char *out, size_t cap) {
    const char *dot = strrchr(fqn, '.');
    if (!dot || (size_t)(dot - fqn) >= cap) {
        return false;
    }
    memcpy(out, fqn, (size_t)(dot - fqn));
    out[dot - fqn] = '\0';
    return true;
}

/* True when the file's scope is open: an on-demand import of a package the
 * repository does not declare (or of a type it does not declare) can supply
 * any simple name. */
static bool jr_scope_open(const jr_ctx_t *c) {
    for (int i = 0; i < c->f->nwild; i++) {
        const char *target = c->f->wild[i].target;
        if (!jr_is_package(c->ix, target) && jr_ent(c->ix, target) == JR_NONE) {
            return true;
        }
    }
    return false;
}

/* True when package `pkg` lies in a directory the indexer never reads, below
 * a package the repository owns. A JVM package is a directory path, and
 * discovery skips some directory names wherever they occur (`env`, `target`,
 * `tmp` ...): what is declared there is on disk, but not in the graph. */
static bool jr_not_indexed(const jr_index_t *ix, const char *pkg) {
    char buf[JR_KEY_BUF];
    size_t n = strlen(pkg);
    if (n == 0 || n >= sizeof(buf)) {
        return false;
    }
    memcpy(buf, pkg, n + SKIP_ONE);
    bool owned = false; /* a prefix before this segment is the repository's */
    size_t seg = 0;
    for (size_t i = 0; i <= n; i++) {
        if (buf[i] != '.' && buf[i] != '\0') {
            continue;
        }
        char keep = buf[i];
        buf[i] = '\0'; /* buf: the prefix through this segment; buf + seg: the segment */
        if (owned && cbm_should_skip_dir(buf + seg, (cbm_index_mode_t)ix->mode)) {
            return true;
        }
        owned = owned || cbm_ht_get(ix->packages, buf) || cbm_ht_get(ix->pkg_prefixes, buf);
        buf[i] = keep;
        seg = i + SKIP_ONE;
    }
    return false;
}

/* What a name that no scope shows is, by its package: declared by the
 * repository without a scope record (G: generated code, a declaration the
 * scan does not see), in a part of it the indexer does not read (N), or
 * outside the repository (X). `pkg`: the package, or the type, the name is
 * said to be in. */
static char jr_unseen_kind(const jr_index_t *ix, const char *pkg) {
    if (pkg[0] && (jr_is_package(ix, pkg) || jr_ent(ix, pkg) != JR_NONE)) {
        return 'G';
    }
    return jr_not_indexed(ix, pkg) ? 'N' : 'X';
}

static int jr_kind_reason(char kind) {
    return kind == 'G'   ? CBM_DOCLINK_REASON_GRAPH_GAP
           : kind == 'N' ? CBM_DOCLINK_REASON_NOT_INDEXED
                         : CBM_DOCLINK_REASON_EXTERNAL;
}

/* What an explicit import of `fqn` names: the type, or what jr_unseen_kind
 * says of its package. */
static void jr_cand_import_type(const jr_ctx_t *c, jr_cands_t *out, const char *fqn) {
    int ent = jr_ent(c->ix, fqn);
    if (ent != JR_NONE) {
        jr_cand_type(c, out, ent);
        return;
    }
    char pkg[JR_KEY_BUF];
    if (!jr_package_of(fqn, pkg, sizeof(pkg))) {
        pkg[0] = '\0';
    }
    jr_cand(out, jr_unseen_kind(c->ix, pkg), JR_NONE);
}

/* ── Why a name was not found ────────────────────────────────────── */

/* A simple name no scope level has: the scope can still explain it. */
static jr_res_t jr_unfound_simple(const jr_ctx_t *c, const char *name) {
    if (!c->f->kotlin && JR_NAMED(JR_JAVA_PRIMITIVES, name)) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (jr_scope_open(c)) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    for (int i = 0; c->inherit && i < c->nchain; i++) {
        if (jr_open(c->ix, c->chain[i])) {
            return jr_unres(CBM_DOCLINK_REASON_EXTERNAL); /* a supertype outside may declare it */
        }
    }
    return jr_unres(CBM_DOCLINK_REASON_MISSING);
}

/* True when the file imports something of package `pkg` (or the package
 * itself on demand) and the repository does not declare that package. */
static bool jr_imports_outside(const jr_ctx_t *c, const char *pkg) {
    if (jr_is_package(c->ix, pkg)) {
        return false;
    }
    size_t pl = strlen(pkg);
    for (int i = 0; i < c->f->nwild; i++) {
        if (strcmp(c->f->wild[i].target, pkg) == 0) {
            return true;
        }
    }
    for (int i = 0; i < c->f->nimports; i++) {
        const char *t = c->f->imports[i].target;
        if (strncmp(t, pkg, pl) == 0 && t[pl] == '.') {
            return true;
        }
    }
    return false;
}

/* A qualified name that is no type, no member and no top-level declaration.
 *   - a package of the repository (or the start of one): packages have no node
 *   - `pkg.Type..` (a capitalized name after the package part): the type is
 *     missing when the repository declares that package, else it is outside
 *   - all lower case: the languages' own namespaces and what the file imports
 *     from outside are external; anything else names nothing
 *   - `Type.x` with an unknown head: as the simple name
 * The roots are the namespaces the two platforms define themselves (`java`,
 * `javax`, `kotlin`). A library family is none, however common: a path under
 * `kotlinx` or `org` is external only where the file imports from it. */
static jr_res_t jr_unfound_path(const jr_ctx_t *c, char *const *segs, int n) {
    static const char *const roots[] = {"java", "javax", "kotlin"};
    const jr_index_t *ix = c->ix;
    char buf[JR_KEY_BUF];
    if (!jr_join(segs, 0, n, buf, sizeof(buf))) {
        return jr_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    if (cbm_ht_get(ix->packages, buf) || cbm_ht_get(ix->pkg_prefixes, buf)) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (n == SKIP_ONE) {
        return jr_unfound_simple(c, segs[0]);
    }
    int k = 0;
    while (k < n && !isupper((unsigned char)segs[k][0])) {
        k++;
    }
    if (k == 0) {
        return jr_unfound_simple(c, segs[0]);
    }
    if (k < n) {
        (void)jr_join(segs, 0, k, buf, sizeof(buf));
        if (jr_is_package(ix, buf)) {
            return jr_unres(CBM_DOCLINK_REASON_MISSING);
        }
        return jr_unres(jr_kind_reason(jr_unseen_kind(ix, buf)));
    }
    if (JR_NAMED(roots, segs[0])) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (jr_not_indexed(ix, buf)) {
        return jr_unres(CBM_DOCLINK_REASON_NOT_INDEXED); /* a package the indexer skips */
    }
    for (int i = n; i >= SKIP_ONE; i--) {
        (void)jr_join(segs, 0, i, buf, sizeof(buf));
        if (jr_imports_outside(c, buf)) {
            return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
    }
    return jr_unres(CBM_DOCLINK_REASON_MISSING);
}

/* ── Members across supertypes ───────────────────────────────────── */

/* How many supertypes that declare a name are compared with the nearest one.
 * Past that the name is ambiguous: an unresolved outcome with its reason,
 * never a pick. */
enum { JR_CLASH_CHECKS = 16 };

typedef struct {
    jr_found_t found; /* the fields of the first type that has any */
    int ent;          /* that type; JR_NONE when none has */
    bool gap;         /* a type with members a parse error hides was reached before
                         any was found: one of those may be the field */
    bool open;        /* nothing found, and a supertype outside the repository */
    bool clash;       /* a second type declares the name, and neither comes first */
    bool invisible;
} jr_msearch_t;

/* The fields `name` of `ent` (enum constants and record components among
 * them): its own, else those of the nearest supertype that declares any. A
 * second supertype with the name must be one the first extends (hidden);
 * else neither comes first: a clash. Methods are javadoc's search (jr_walk). */
static void jr_fields_in(const jr_ctx_t *c, int ent, const char *name, jr_msearch_t *out) {
    const jr_index_t *ix = c->ix;
    const jr_entity_t *e = &ix->ents[ent];
    memset(out, 0, sizeof(*out));
    out->ent = JR_NONE;
    jr_found_t own = jr_members(c, ent, name, JR_SEL_VALUES, NULL);
    out->invisible = own.invisible;
    if (own.count > 0) {
        out->found = own;
        out->ent = ent;
        return;
    }
    if (e->incomplete) {
        out->gap = true;
        return;
    }
    int checks = 0;
    for (int k = 0; k < e->nclosure; k++) {
        int sup = e->closure[k];
        jr_found_t inh = jr_members(c, sup, name, JR_SEL_VALUES, NULL);
        out->invisible = out->invisible || inh.invisible;
        if (inh.count == 0) {
            if (out->ent == JR_NONE && ix->ents[sup].incomplete) {
                out->gap = true;
                return;
            }
            continue;
        }
        if (out->ent == JR_NONE) {
            out->found = inh;
            out->ent = sup;
        } else if (!out->clash &&
                   (++checks > JR_CLASH_CHECKS || !jr_is_or_extends(ix, out->ent, sup))) {
            out->clash = true;
        }
    }
    out->open = out->ent == JR_NONE && jr_open(ix, ent);
}

/* True when java.lang.Object has a method `name`. With `r`, a reference that
 * writes a parameter list: one with exactly that list. */
static bool jr_object_method(const char *name, const jr_ref_t *r) {
    for (size_t i = 0; i < sizeof(JR_OBJECT_METHODS) / sizeof(JR_OBJECT_METHODS[0]); i++) {
        const char *p = JR_OBJECT_METHODS[i].sig;
        bool same = strcmp(JR_OBJECT_METHODS[i].name, name) == 0;
        for (int k = 0; same && r && k < r->nparams; k++) {
            size_t n = strlen(r->params[k]);
            same = n > 0 && strncmp(p, r->params[k], n) == 0 && (p[n] == '|' || p[n] == '\0');
            p += same ? n + (p[n] == '|' ? SKIP_ONE : 0) : 0;
        }
        if (same && (!r || *p == '\0')) {
            return true;
        }
    }
    return false;
}

/* The members every type has without declaring them, in either language:
 * Object's, an enum's, an annotation's. By name alone, and wider than any one
 * language: it only tells `external` from `missing` once nothing was found. */
static bool jr_implicit_member(const jr_index_t *ix, int ent, const char *name) {
    char kind = ix->ents[ent].kind;
    if (jr_object_method(name, NULL)) {
        return true;
    }
    if (kind == 'e') {
        return JR_NAMED(JR_JAVA_ENUM_MEMBERS, name) || JR_NAMED(JR_KOTLIN_ENUM_MEMBERS, name);
    }
    return kind == 'a' && strcmp(name, "annotationType") == 0;
}

/* True when the type `ent`, a type around the documented definition of a
 * Kotlin file, has a member `name` without declaring it: a member of
 * kotlin.Any, at an enum class what an enum class has (the name alone
 * decides). Java's counterpart is javadoc's own search (jr_walk). */
static bool jr_given_member(const jr_ctx_t *c, int ent, const char *name) {
    char kind = c->ix->ents[ent].kind;
    return JR_NAMED(JR_ANY_MEMBERS, name) ||
           (kind == 'e' && JR_NAMED(JR_KOTLIN_ENUM_MEMBERS, name));
}

/* The fields `name` through the context's chain of types, innermost first: a
 * type and what it inherits before the type around it. *level is the chain
 * index of the hit. javadoc looks for a field in a type, then in the types
 * around it, then in its supertypes; where the two orders find different
 * fields, jr_java_member says ambiguous. */
static void jr_fields_around(const jr_ctx_t *c, const char *name, jr_msearch_t *out, int *level) {
    bool open = false;
    bool invisible = false;
    memset(out, 0, sizeof(*out));
    out->ent = JR_NONE;
    *level = 0;
    for (int i = 0; i < c->nchain; i++) {
        jr_msearch_t one;
        jr_fields_in(c, c->chain[i], name, &one);
        open = open || one.open;
        invisible = invisible || one.invisible;
        if (one.ent != JR_NONE || one.gap) {
            *out = one;
            *level = i;
            break;
        }
    }
    out->open = out->ent == JR_NONE && !out->gap && open;
    out->invisible = out->invisible || invisible;
}

/* ── Java: javadoc's search for a method ─────────────────────────── */

/* javadoc links `#m` and `Type#m` to the first method its own search finds
 * (JavacTrees.searchMethod; read in the JDK 21 source on 2026-10-04, stated
 * here in our words). It is not javac's scoping. From a type it looks at
 *   1. the type's own methods of the name; with a written parameter list,
 *      one whose parameters match it;
 *   2. the superclass, by this whole search: the superclass's own methods,
 *      its superclass, its interfaces and the type around the SUPERCLASS
 *      all come before step 3;
 *   3. the interfaces, in the order written, each by this whole search;
 *   4. the type around it, by this whole search;
 * and at each type once. `#m` starts at the type of the documented
 * definition, `Type#m` at the type named. The steps of each type come from
 * its declarations (jr_walk_steps). java.lang.Object, java.lang.Enum and
 * java.lang.annotation.Annotation are steps whose methods the language
 * defines; an enum has values() and valueOf(String), a record equals,
 * hashCode and toString without declaring them. A type outside the
 * repository is a step the index cannot look into: a method there would be
 * the one linked. These lists are the language's, written from knowledge of
 * the Java SE API. */
static const char *const JR_ENUM_OWN_METHODS[] = {"values", "valueOf"};
static const char *const JR_RECORD_OWN_METHODS[] = {"equals", "hashCode", "toString"};
static const char *const JR_ENUM_CLASS_METHODS[] = {
    "name",    "ordinal", "compareTo", "getDeclaringClass", "describeConstable",
    "valueOf", "equals",  "hashCode",  "toString",          "clone",
    "finalize"};
static const char *const JR_ANNOTATION_METHODS[] = {"annotationType", "equals", "hashCode",
                                                    "toString"};

/* How many types one search looks at, at most. A search that would look at
 * more ends as ambiguous (JR_WK_LIMIT): never cut short into a result. */
enum { JR_WALK_MAX = 512, JR_WALK_SLOTS = 1024 /* a power of two above twice the max */ };
static const unsigned JR_WALK_MIX = 2654435761U; /* multiplicative hashing */

typedef enum {
    JR_WK_NONE = 0,   /* no type on the way has the method */
    JR_WK_FOUND,      /* a type of the repository declares it */
    JR_WK_GIVEN,      /* the language gives it */
    JR_WK_OUTSIDE,    /* a type outside the repository comes first */
    JR_WK_GAP,        /* a type whose members a parse error hides comes first */
    JR_WK_UNCOMPARED, /* a Kotlin declaration of the name comes first, which a written
                         parameter list cannot be compared with: it may be the one */
    JR_WK_LIMIT,      /* more than JR_WALK_MAX types */
} jr_wk_t;

typedef struct {
    jr_wk_t kind;
    /* FOUND, GAP, UNCOMPARED: the type; OUTSIDE: the type whose supertype
     * lies outside; GIVEN: the type that has the method without declaring
     * it, or JR_W_OBJECT, JR_W_ENUM, JR_W_ANNOTATION */
    int ent;
    jr_found_t found;    /* FOUND */
    bool invisible;      /* a declaration only test code holds was passed over */
    bool passed_outside; /* for a field: a step outside was passed (jr_walk_ends) */
    bool passed_gap;     /* ... a type whose members a parse error hides */
} jr_walk_t;

enum { JR_WS_OWN = 0, JR_WS_SUPER, JR_WS_IFACES, JR_WS_DONE };

typedef struct {
    int ent;
    int stage; /* JR_WS_* */
    int next;  /* JR_WS_IFACES: the next interface step */
} jr_wframe_t;

/* Marks type `ent` as looked at (`slots` holds ent + 1); false when it was.
 * The caller marks at most JR_WALK_MAX + 1 types, so a free slot is found. */
static bool jr_walk_mark(int *slots, int ent) {
    unsigned mask = (unsigned)JR_WALK_SLOTS - SKIP_ONE;
    unsigned h = ((unsigned)ent * JR_WALK_MIX) & mask;
    for (int i = 0; i < JR_WALK_SLOTS; i++) {
        if (slots[h] == ent + SKIP_ONE) {
            return false;
        }
        if (slots[h] == 0) {
            slots[h] = ent + SKIP_ONE;
            return true;
        }
        h = (h + SKIP_ONE) & mask;
    }
    return false;
}

/* Step 1 at a type of the repository: what the type itself has under the
 * name. True when that ends the step, as *out says. */
static bool jr_walk_own(const jr_ctx_t *c, int ent, const char *name, int sel, const jr_ref_t *r,
                        jr_walk_t *out) {
    const jr_entity_t *e = &c->ix->ents[ent];
    jr_found_t own = jr_members(c, ent, name, sel, r);
    out->invisible = out->invisible || own.invisible;
    out->ent = ent;
    if (own.count > 0) {
        out->kind = JR_WK_FOUND;
        out->found = own;
        return true;
    }
    if (own.uncompared) {
        out->kind = JR_WK_UNCOMPARED;
        return true;
    }
    if (e->kind == 'r' && (!(sel & JR_SEL_SIG) || r->nparams == 0)) {
        jr_found_t comp = jr_members(c, ent, name, JR_SEL_VALUES, NULL);
        if (comp.count > 0 && comp.component) {
            /* a component's accessor: the record's own method, without a node */
            memset(&out->found, 0, sizeof(out->found));
            out->found.count = SKIP_ONE;
            out->kind = JR_WK_FOUND;
            return true;
        }
    }
    if ((e->kind == 'e' && JR_NAMED(JR_ENUM_OWN_METHODS, name)) ||
        (e->kind == 'r' && JR_NAMED(JR_RECORD_OWN_METHODS, name))) {
        out->kind = JR_WK_GIVEN;
        return true;
    }
    if (e->incomplete) {
        out->kind = JR_WK_GAP;
        return true;
    }
    out->ent = JR_NONE;
    return false;
}

/* A step that is no type of the repository, taken from type `owner`. True
 * when it ends the search, as *out says. With a written parameter list
 * Object's methods are matched by it; an enum's and an annotation's by name. */
static bool jr_walk_marker(int step, int owner, const char *name, int sel, const jr_ref_t *r,
                           jr_walk_t *out) {
    const jr_ref_t *sig = (sel & JR_SEL_SIG) ? r : NULL;
    int given = JR_NONE;
    if (step == JR_W_OUTSIDE) {
        out->kind = JR_WK_OUTSIDE;
        out->ent = owner;
        return true;
    }
    if (step == JR_W_OBJECT && jr_object_method(name, sig)) {
        given = JR_W_OBJECT;
    } else if (step == JR_W_ENUM && JR_NAMED(JR_ENUM_CLASS_METHODS, name)) {
        given = JR_W_ENUM;
    } else if (step == JR_W_ENUM && jr_object_method(name, sig)) {
        given = JR_W_OBJECT;
    } else if (step == JR_W_ANNOTATION && JR_NAMED(JR_ANNOTATION_METHODS, name)) {
        given = JR_W_ANNOTATION;
    }
    if (given == JR_NONE) {
        return false;
    }
    out->kind = JR_WK_GIVEN;
    out->ent = given;
    return true;
}

/* Whether the search ends where *out says. A method is linked where it is
 * found. For a field of class `cf` (jr_java_field) a step outside the
 * repository, or a type whose members a parse error hides, does not end it
 * when `cf` is below the type the step belongs to: whatever method is there,
 * the field comes first, so the search goes on to what may come after it.
 * `peek`, for a count only, passes everything but a method of the
 * repository. */
static bool jr_walk_ends(const jr_index_t *ix, jr_walk_t *out, int cf, bool peek) {
    bool pass = false;
    if (peek) {
        pass = out->kind != JR_WK_FOUND;
    } else if (cf != JR_NONE && (out->kind == JR_WK_OUTSIDE || out->kind == JR_WK_GAP) &&
               jr_is_or_extends(ix, cf, out->ent)) {
        pass = true;
        if (out->kind == JR_WK_OUTSIDE) {
            out->passed_outside = true;
        } else {
            out->passed_gap = true;
        }
    }
    if (pass) {
        out->kind = JR_WK_NONE;
        out->ent = JR_NONE;
    }
    return !pass;
}

/* javadoc's search for a method `name` (`sel`, `r`: a written parameter
 * list) from type `start`, without recursion: an explicit stack, each type
 * once. `cf` and `peek`: jr_walk_ends. */
static void jr_walk(const jr_ctx_t *c, int start, const char *name, int sel, const jr_ref_t *r,
                    int cf, bool peek, jr_walk_t *out) {
    const jr_index_t *ix = c->ix;
    int slots[JR_WALK_SLOTS];
    jr_wframe_t stack[JR_WALK_MAX];
    memset(slots, 0, sizeof(slots));
    memset(out, 0, sizeof(*out));
    out->ent = JR_NONE;
    (void)jr_walk_mark(slots, start);
    int marked = SKIP_ONE;
    int depth = 0;
    stack[depth++] = (jr_wframe_t){.ent = start, .stage = JR_WS_OWN, .next = 0};
    while (depth > 0) {
        jr_wframe_t *fr = &stack[depth - SKIP_ONE];
        const jr_entity_t *e = &ix->ents[fr->ent];
        int step = JR_W_NONE;
        if (fr->stage == JR_WS_OWN) {
            fr->stage = JR_WS_SUPER;
            if (jr_walk_own(c, fr->ent, name, sel, r, out) && jr_walk_ends(ix, out, cf, peek)) {
                return;
            }
            continue;
        }
        if (fr->stage == JR_WS_SUPER) {
            fr->stage = JR_WS_IFACES;
            step = e->wsuper;
        } else if (fr->stage == JR_WS_IFACES && fr->next < e->nwifaces) {
            step = e->wifaces[fr->next++];
        } else if (fr->stage == JR_WS_IFACES) {
            fr->stage = JR_WS_DONE;
            step = e->encl;
        } else {
            depth--;
            continue;
        }
        if (step < 0) {
            if (jr_walk_marker(step, fr->ent, name, sel, r, out) &&
                jr_walk_ends(ix, out, cf, peek)) {
                return;
            }
            continue;
        }
        if (!jr_walk_mark(slots, step)) {
            continue;
        }
        if (++marked > JR_WALK_MAX) {
            out->kind = JR_WK_LIMIT;
            out->ent = JR_NONE;
            return;
        }
        stack[depth++] = (jr_wframe_t){.ent = step, .stage = JR_WS_OWN, .next = 0};
    }
}

/* ── Java ────────────────────────────────────────────────────────── */

typedef enum { JR_LK_NONE = 0, JR_LK_FOUND, JR_LK_INVISIBLE, JR_LK_LOCAL } jr_lk_t;

/* Closes one scope level: true when it has a candidate, which then decides
 * the lookup. */
static bool jr_level_done(jr_cands_t *lv, jr_cands_t *out, bool *invisible) {
    if (lv->count > 0) {
        *out = *lv;
        return true;
    }
    *invisible = *invisible || lv->invisible;
    memset(lv, 0, sizeof(*lv));
    return false;
}

/* True when the type `ent` has a declaration in file `file`. */
static bool jr_declared_in(const jr_index_t *ix, int ent, int file) {
    const jr_entity_t *e = &ix->ents[ent];
    for (int d = 0; d < e->ndecls; d++) {
        if (e->decls[d].file == file) {
            return true;
        }
    }
    return false;
}

/* The member types an on-demand import of `target` offers under `name`. */
static void jr_java_on_demand(const jr_ctx_t *c, jr_cands_t *lv, const jr_import_t *imp,
                              const char *name) {
    const jr_index_t *ix = c->ix;
    if (imp->kind == 'w' && jr_is_package(ix, imp->target)) {
        jr_cand_type(c, lv, jr_pkg_type(ix, imp->target, name));
    }
    int type = jr_ent(ix, imp->target); /* `import a.Outer.*;`, `import static a.B.*;` */
    if (type != JR_NONE) {
        jr_cand_type(c, lv, jr_member_type(c, type, name));
    }
}

/* A simple type name in a Java file, in javac's order. */
static jr_lk_t jr_java_type_lookup(const jr_ctx_t *c, const char *name, jr_cands_t *out) {
    const jr_index_t *ix = c->ix;
    const jr_file_t *f = c->f;
    jr_cands_t lv = {0};
    bool invisible = false;
    memset(out, 0, sizeof(*out));
    if (c->member && jr_listed(c->member->tparams, name)) {
        return JR_LK_LOCAL; /* the method's own type variable */
    }
    /* enclosing types, innermost first: the type and its member types, its
     * type variables, the member types it inherits */
    for (int i = 0; i < c->nchain; i++) {
        int ent = c->chain[i];
        if (strcmp(ix->ents[ent].name, name) == 0) {
            jr_cand_type(c, &lv, ent);
        }
        jr_cand_type(c, &lv, jr_nested(ix, ent, name));
        if (jr_level_done(&lv, out, &invisible)) {
            return JR_LK_FOUND;
        }
        if (jr_listed(c->chain_decl[i]->tparams, name)) {
            return JR_LK_LOCAL;
        }
        const jr_entity_t *e = &ix->ents[ent];
        for (int k = 0; c->inherit && k < e->nclosure && lv.count == 0; k++) {
            jr_cand_type(c, &lv, jr_nested(ix, e->closure[k], name));
        }
        if (jr_level_done(&lv, out, &invisible)) {
            return JR_LK_FOUND;
        }
    }
    /* the file's own top-level types and its single imports */
    int same = jr_pkg_type(ix, f->package, name);
    if (same != JR_NONE && jr_declared_in(ix, same, c->file)) {
        jr_cand_type(c, &lv, same);
    }
    for (const jr_import_t *imp = jr_import_next(f, name, NULL); imp;
         imp = jr_import_next(f, name, imp)) {
        if (imp->kind == 's') {
            jr_cand_import_type(c, &lv, imp->target);
            continue;
        }
        /* `import static a.B.name;` imports the member TYPE of that name too */
        int owner = jr_ent(ix, imp->target);
        if (owner != JR_NONE) {
            jr_cand_type(c, &lv, jr_member_type(c, owner, name));
        } else {
            /* a member of a type no scope shows: whatever it is, it is not
             * one of the repository's known declarations */
            char pkg[JR_KEY_BUF];
            if (!jr_package_of(imp->target, pkg, sizeof(pkg))) {
                pkg[0] = '\0';
            }
            jr_cand(&lv, jr_unseen_kind(ix, pkg), JR_NONE);
        }
    }
    if (lv.count > 0) {
        lv.exact = true;
    }
    if (jr_level_done(&lv, out, &invisible)) {
        return JR_LK_FOUND;
    }
    /* the file's package */
    jr_cand_type(c, &lv, same);
    if (jr_level_done(&lv, out, &invisible)) {
        return JR_LK_FOUND;
    }
    /* on-demand imports; java.lang is one of them */
    for (int i = 0; i < f->nwild; i++) {
        jr_java_on_demand(c, &lv, &f->wild[i], name);
    }
    int lang = jr_pkg_type(ix, "java.lang", name);
    if (lang != JR_NONE) {
        jr_cand_type(c, &lv, lang);
    } else if (JR_NAMED(JR_JAVA_LANG, name)) {
        jr_cand(&lv, 'X', JR_NONE);
    }
    if (jr_level_done(&lv, out, &invisible)) {
        return JR_LK_FOUND;
    }
    return invisible ? JR_LK_INVISIBLE : JR_LK_NONE;
}

/* A (qualified) type name in a Java file: the head through the scope, the
 * rest as member types; a head no scope level has is read from the root, as
 * packages and then a type. */
static jr_tp_t jr_java_type_path(const jr_ctx_t *c, char *const *segs, int n) {
    const jr_index_t *ix = c->ix;
    jr_cands_t cands;
    jr_lk_t lk = jr_java_type_lookup(c, segs[0], &cands);
    if (lk == JR_LK_LOCAL) {
        return jr_tp_res((jr_res_t){.st = JR_LOCAL});
    }
    if (lk == JR_LK_INVISIBLE) {
        return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
    }
    if (lk == JR_LK_FOUND) {
        if (cands.count > SKIP_ONE) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
        }
        if (cands.kind != 'T') {
            return jr_tp_res(jr_unres(jr_kind_reason(cands.kind)));
        }
        return jr_walk_nested(c, cands.ent, segs, SKIP_ONE, n, cands.exact);
    }
    for (int i = SKIP_ONE; i < n; i++) {
        char pkg[JR_KEY_BUF];
        if (!jr_join(segs, 0, i, pkg, sizeof(pkg))) {
            break;
        }
        int ent = jr_pkg_type(ix, pkg, segs[i]);
        if (ent == JR_NONE || ix->ents[ent].kind == 't') {
            continue;
        }
        if (!jr_visible(c, ent)) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
        }
        return jr_walk_nested(c, ent, segs, i + SKIP_ONE, n, true);
    }
    return jr_tp_res(jr_unfound_path(c, segs, n));
}

/* True when a record declaration of `ent` has exactly the written parameter
 * list as its components. */
static bool jr_record_canonical(const jr_ctx_t *c, int ent, const jr_ref_t *r) {
    const jr_index_t *ix = c->ix;
    const jr_entity_t *e = &ix->ents[ent];
    for (int d = 0; d < e->ndecls; d++) {
        const jr_file_t *f = &ix->files[e->decls[d].file];
        if (f->types[e->decls[d].type].kind == 'r' && !(c->prod && f->is_test) &&
            jr_canonical_match(f, e->decls[d].type, r)) {
            return true;
        }
    }
    return false;
}

/* `Type#Type(..)` and `#Type(..)`: the constructors of that type alone; none
 * is inherited. A constructor the type has without declaring it (the default
 * one, a record's canonical one) has no node. */
static jr_res_t jr_java_ctor(const jr_ctx_t *c, int ent, const jr_ref_t *r, bool exact) {
    const jr_entity_t *e = &c->ix->ents[ent];
    bool params = r->has_params;
    jr_found_t all = jr_members(c, ent, e->name, JR_SEL_CTORS, r);
    if (!params && all.count > 0) {
        return all.several ? jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS) : jr_found_result(&all, exact);
    }
    if (params) {
        jr_found_t sel = jr_members(c, ent, e->name, JR_SEL_CTORS | JR_SEL_SIG, r);
        if (sel.count > 0) {
            return jr_found_result(&sel, exact);
        }
        if (sel.uncompared) {
            return jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
        }
    }
    if (e->incomplete) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (all.count == 0 && all.invisible) {
        return jr_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    bool has_ctors = e->kind == 'c' || e->kind == 'e' || e->kind == 'r';
    if (has_ctors && params && jr_record_canonical(c, ent, r)) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (has_ctors && all.count == 0 && (!params || (r->nparams == 0 && e->kind != 'r'))) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    return jr_unres(CBM_DOCLINK_REASON_MISSING);
}

/* A method reference's outcome from javadoc's search `w` from `base`; `vs`:
 * the field search that came first, if any. A search that ended at a method
 * javadoc would link that the repository does not show is external; when a
 * method of the repository lies further on, the count says so. */
static jr_res_t jr_java_method(const jr_ctx_t *c, int base, const char *name, int sel,
                               const jr_ref_t *r, const jr_walk_t *w, const jr_msearch_t *vs,
                               bool exact) {
    if (w->kind == JR_WK_FOUND) {
        /* without a list, several signatures are an overload group */
        return (!(sel & JR_SEL_SIG) && w->found.several) ? jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS)
                                                         : jr_found_result(&w->found, exact);
    }
    if (w->kind == JR_WK_GAP) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (w->invisible || (vs && vs->invisible)) {
        return jr_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    if (w->kind == JR_WK_UNCOMPARED || w->kind == JR_WK_LIMIT) {
        return jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
    }
    if (w->kind == JR_WK_GIVEN || w->kind == JR_WK_OUTSIDE) {
        jr_walk_t further;
        jr_walk(c, base, name, sel, r, JR_NONE, true, &further);
        if (further.kind == JR_WK_FOUND) {
            jr_stats_t *st = c->ix->stats;
            atomic_fetch_add(w->kind == JR_WK_GIVEN ? &st->java_given : &st->java_open, 1);
        }
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (jr_implicit_member(c->ix, base, name) || (vs && vs->open)) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    return jr_unres(CBM_DOCLINK_REASON_MISSING);
}

/* A field `vs` that javadoc's field search found for a reference without a
 * parameter list, weighed against javadoc's search for a METHOD of the name
 * from `start` (jr_walk). javadoc takes the field when no method is found, or
 * when the field's class is the method's class or below it
 * (JavacTrees.attributeDocReference, read in the JDK 21 source on
 * 2026-10-04). So the field binds only when that holds whatever the steps the
 * index cannot see hold:
 *   - a step outside the repository may hold the method; the field's class
 *     must be below the type the step belongs to (then it is below the
 *     outside type as well), and the search goes on past it;
 *   - a method the language gives is weighed like a declared one: Object's
 *     never stops the field; Enum's or Annotation's only an enum's or an
 *     annotation type's; a record's or an enum's own implicit one, a field
 *     of that type or below it;
 *   - against a method of the repository the field binds when its class is
 *     below the method's class. Otherwise, with nothing the index cannot see
 *     before that method, javadoc links the method, and so does the reference
 *     (the lead's ruling of 2026-10-04: a certain binding), with the tier of
 *     a member the search finds; an overload group without a list stays
 *     ambiguous, as for any method. If a step outside was passed, the method
 *     there may be the one: external;
 *   - a type whose members a parse error hides may hold the method: a graph
 *     gap, unless the field's class is below it (then it is passed, and only
 *     a field outcome survives it).
 * *yielded: the field was not taken because of what lies outside. */
static jr_res_t jr_java_field(const jr_ctx_t *c, int start, const char *name, const jr_ref_t *r,
                              const jr_msearch_t *vs, bool exact, bool *yielded) {
    const jr_index_t *ix = c->ix;
    int cf = vs->ent;
    char cf_kind = ix->ents[cf].kind;
    jr_walk_t w;
    jr_walk(c, start, name, JR_SEL_CALLABLES, r, cf, false, &w);
    bool field = false;
    int reason = CBM_DOCLINK_REASON_EXTERNAL;
    switch (w.kind) {
    case JR_WK_NONE:
        field = true;
        break;
    case JR_WK_FOUND:
        field = jr_is_or_extends(ix, cf, w.ent);
        if (!field && !w.passed_outside && !w.passed_gap) {
            return w.found.several ? jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS)
                                   : jr_found_result(&w.found, exact);
        }
        break;
    case JR_WK_GIVEN:
        field = w.ent == JR_W_OBJECT || (w.ent == JR_W_ENUM && cf_kind == 'e') ||
                (w.ent == JR_W_ANNOTATION && cf_kind == 'a') ||
                (w.ent >= 0 && jr_is_or_extends(ix, cf, w.ent));
        break;
    case JR_WK_OUTSIDE:
        break;
    case JR_WK_GAP:
        reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        break;
    default: /* a Kotlin declaration (only with a list), or too many types */
        reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        break;
    }
    if (field) {
        return jr_found_result(&vs->found, exact);
    }
    if (w.passed_gap) {
        reason = CBM_DOCLINK_REASON_GRAPH_GAP;
    }
    *yielded = reason == CBM_DOCLINK_REASON_EXTERNAL;
    return jr_unres(reason);
}

/* The scope of `Type#x`: the type named and the types around it, innermost
 * first. javadoc's field search goes through them as it does for `#x`. */
static void jr_ctx_named(jr_ctx_t *tc, const jr_ctx_t *c, int type) {
    *tc = *c;
    tc->nchain = 0;
    for (int t = type; t != JR_NONE && tc->nchain < JR_CHAIN_MAX; t = c->ix->ents[t].encl) {
        tc->chain[tc->nchain] = t;
        tc->chain_decl[tc->nchain] = NULL;
        tc->nchain++;
    }
}

/* A Javadoc member reference. `type` is the type it names, or JR_NONE for
 * `#member`, which starts at the type of the documented definition.
 *   - a METHOD is javadoc's search (jr_walk): the first method it finds is the
 *     one; a type outside the repository on the way, or a method the language
 *     gives, ends it as external; a parameter list selects the overloads with
 *     that signature, never another overload, and without one several
 *     signatures are ambiguous
 *   - without a list a FIELD comes first. javadoc looks for one in the type,
 *     then in the types around it, then in the supertypes; javac scoping
 *     takes the inherited one first: an inherited field binds only when no
 *     type further out has one of that name (else ambiguous). The field is
 *     then weighed against the method (jr_java_field)
 *   - {@value} names a field and nothing else; for `Type#x` it searches the
 *     type and its supertypes only, as before */
static jr_res_t jr_java_member(const jr_ctx_t *c, const char *name, const jr_ref_t *r, int type,
                               bool params, bool exact, bool values_only) {
    const jr_index_t *ix = c->ix;
    bool in_scope = type == JR_NONE;
    int base = in_scope ? (c->nchain > 0 ? c->chain[0] : JR_NONE) : type;
    if (base == JR_NONE) {
        return jr_unres(CBM_DOCLINK_REASON_MISSING); /* no type to be a member of */
    }
    if (!values_only && strcmp(name, ix->ents[base].name) == 0) {
        return jr_java_ctor(c, base, r, exact);
    }
    int sel = JR_SEL_CALLABLES | (params ? JR_SEL_SIG : 0);
    jr_walk_t mw; /* the method, as javadoc searches for it */
    memset(&mw, 0, sizeof(mw));
    mw.ent = JR_NONE;
    if (params) {
        jr_walk(c, base, name, sel, r, JR_NONE, false, &mw);
        return jr_java_method(c, base, name, sel, r, &mw, NULL, exact);
    }
    jr_ctx_t named;
    const jr_ctx_t *fc = c; /* the chain the field is searched through */
    if (!in_scope && !values_only) {
        jr_ctx_named(&named, c, base);
        fc = &named;
    }
    bool chain = in_scope || !values_only;
    jr_msearch_t vs;
    int vlevel = 0;
    if (chain) {
        jr_fields_around(fc, name, &vs, &vlevel);
    } else {
        jr_fields_in(c, base, name, &vs);
    }
    if (vs.ent != JR_NONE) {
        for (int j = vlevel + SKIP_ONE;
             chain && vs.ent != fc->chain[vlevel] && !vs.clash && j < fc->nchain; j++) {
            jr_msearch_t outer;
            jr_fields_in(fc, fc->chain[j], name, &outer);
            vs.clash = outer.ent != JR_NONE || outer.gap;
        }
        if (vs.clash) {
            return jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
        }
        if (values_only) {
            return jr_found_result(&vs.found, exact);
        }
        bool yielded = false;
        jr_res_t res = jr_java_field(c, base, name, r, &vs, exact, &yielded);
        if (yielded) {
            atomic_fetch_add(in_scope ? &ix->stats->java_field : &ix->stats->java_field_named, 1);
        }
        return res;
    }
    if (vs.gap) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* a hidden field would come first */
    }
    if (!values_only) {
        jr_walk(c, base, name, sel, r, JR_NONE, false, &mw);
    }
    return jr_java_method(c, base, name, sel, r, &mw, &vs, exact);
}

static jr_res_t jr_java_resolve(const jr_ctx_t *c, const jr_ref_t *r, bool values_only) {
    if (r->nsegs == 0) {
        return jr_java_member(c, r->member, r, JR_NONE, r->has_params, false, values_only);
    }
    jr_tp_t tp = jr_java_type_path(c, r->segs, r->nsegs);
    if (r->member) {
        if (tp.st == JR_TP_OK) {
            return jr_java_member(c, r->member, r, tp.ent, r->has_params, tp.exact, values_only);
        }
        /* a member type the named type does not have: javadoc finds no type */
        return tp.st == JR_TP_PARTIAL ? jr_unres(CBM_DOCLINK_REASON_MISSING) : tp.res;
    }
    if (tp.st == JR_TP_OK) {
        return values_only ? jr_unres(CBM_DOCLINK_REASON_MISSING)
                           : jr_type_result(c, tp.ent, tp.exact);
    }
    if (tp.st == JR_TP_PARTIAL) {
        return jr_unres(CBM_DOCLINK_REASON_MISSING); /* `Type.member`: Javadoc wants a '#' */
    }
    if (r->nsegs == SKIP_ONE && tp.res.st == JR_UNRES &&
        tp.res.reason == CBM_DOCLINK_REASON_MISSING) {
        /* javadoc's own fixup: a bare identifier that is no type is read as
         * `#identifier`, a member in the enclosing types. Only where the scope
         * is closed: an open one may hold a type of that name, which comes
         * first. */
        jr_res_t m = jr_java_member(c, r->segs[0], r, JR_NONE, false, false, values_only);
        if (!(m.st == JR_UNRES && m.reason == CBM_DOCLINK_REASON_MISSING)) {
            return m;
        }
    }
    return tp.res;
}

/* ── Kotlin ──────────────────────────────────────────────────────── */

/* The packages every Kotlin file imports on demand. */
static const char *const JR_KOTLIN_DEFAULT_PKGS[] = {
    "kotlin",        "kotlin.annotation", "kotlin.collections", "kotlin.comparisons", "kotlin.io",
    "kotlin.ranges", "kotlin.sequences",  "kotlin.text",        "java.lang",          "kotlin.jvm"};

/* What one scope level holds under a name: types, properties, functions. */
typedef struct {
    jr_cands_t types;
    jr_found_t vals;
    jr_found_t calls;
    bool clash; /* two supertypes declare the name and neither comes first */
} jr_klevel_t;

static bool jr_kl_any(const jr_klevel_t *lv) {
    return lv->types.count > 0 || lv->vals.count > 0 || lv->calls.count > 0;
}

static bool jr_kl_invisible(const jr_klevel_t *lv) {
    return lv->types.invisible || lv->vals.invisible || lv->calls.invisible;
}

/* The members `name` of type `ent`, joined to the level. */
static void jr_kl_members(const jr_ctx_t *c, jr_klevel_t *lv, int ent, const char *name) {
    jr_found_t v = jr_members(c, ent, name, JR_SEL_VALUES, NULL);
    jr_found_t k = jr_members(c, ent, name, JR_SEL_CALLABLES, NULL);
    jr_found_add(c, &lv->vals, &v, false);
    jr_found_add(c, &lv->calls, &k, false);
}

/* The top-level declarations `name` of package `pkg`, joined to the level. */
static void jr_kl_top(const jr_ctx_t *c, jr_klevel_t *lv, const char *pkg, const char *name) {
    jr_found_t v = jr_top_members(c, pkg, name, JR_SEL_VALUES);
    jr_found_t k = jr_top_members(c, pkg, name, JR_SEL_CALLABLES);
    jr_found_add(c, &lv->vals, &v, false);
    jr_found_add(c, &lv->calls, &k, false);
}

/* The type `name` nested in `ent`; through a type alias, in the type it
 * names. */
static int jr_kotlin_nested(const jr_index_t *ix, int ent, const char *name) {
    int n = jr_nested(ix, ent, name);
    const jr_entity_t *e = &ix->ents[ent];
    if (n == JR_NONE && e->kind == 't' && e->nclosure > 0) {
        n = jr_nested(ix, e->closure[0], name);
    }
    return n;
}

/* What type `ent` holds under `name` itself: a nested type, its members, and
 * the same of its companion object. One level: a class's member scope and its
 * companion's are one scope to a KDoc name. */
static void jr_kl_type_scope(const jr_ctx_t *c, jr_klevel_t *lv, int ent, const char *name,
                             bool members) {
    const jr_index_t *ix = c->ix;
    jr_cand_type(c, &lv->types, jr_kotlin_nested(ix, ent, name));
    if (members) {
        jr_kl_members(c, lv, ent, name);
    }
    int comp = ix->ents[ent].companion;
    if (comp != JR_NONE) {
        jr_cand_type(c, &lv->types, jr_nested(ix, comp, name));
        if (members) {
            jr_kl_members(c, lv, comp, name);
        }
    }
}

/* What the supertypes of `ent` hold under `name`: the nearest one that has
 * anything (`types`: nested types, `members`: properties and functions). A
 * further one that the nearest does not extend is a clash. *gap: a supertype
 * with members a parse error hides came before any hit. */
static void jr_kl_inherited(const jr_ctx_t *c, jr_klevel_t *lv, int ent, const char *name,
                            bool types, bool members, bool *gap) {
    const jr_index_t *ix = c->ix;
    const jr_entity_t *e = &ix->ents[ent];
    int first = JR_NONE;
    int checks = 0;
    bool invisible = false;
    for (int k = 0; k < e->nclosure; k++) {
        int sup = e->closure[k];
        jr_klevel_t one;
        memset(&one, 0, sizeof(one));
        if (types) {
            jr_cand_type(c, &one.types, jr_nested(ix, sup, name));
        }
        if (members) {
            jr_kl_members(c, &one, sup, name);
        }
        if (!jr_kl_any(&one)) {
            invisible = invisible || jr_kl_invisible(&one);
            if (first == JR_NONE && members && ix->ents[sup].incomplete) {
                *gap = true;
                break;
            }
            continue;
        }
        if (first == JR_NONE) {
            *lv = one;
            first = sup;
        } else if (++checks > JR_CLASH_CHECKS || !jr_is_or_extends(ix, first, sup)) {
            lv->clash = true;
            break;
        }
    }
    lv->types.invisible = lv->types.invisible || invisible;
}

/* What an explicit import of `fqn` puts in scope: a type, the top-level
 * declarations of that name, a member of an object or an enum. When no scope
 * shows anything of that name it is either declared by the repository where
 * no scope sees it (its package or type is the repository's), or outside. */
static void jr_kl_import(const jr_ctx_t *c, jr_klevel_t *lv, const char *fqn, bool members) {
    const jr_index_t *ix = c->ix;
    const char *dot = strrchr(fqn, '.');
    const char *last = dot ? dot + SKIP_ONE : fqn;
    char pkg[JR_KEY_BUF];
    bool has_pkg = jr_package_of(fqn, pkg, sizeof(pkg));
    if (!has_pkg) {
        pkg[0] = '\0';
    }
    jr_klevel_t one;
    memset(&one, 0, sizeof(one));
    jr_cand_type(c, &one.types, jr_ent(ix, fqn));
    jr_kl_top(c, &one, pkg, last);
    int owner = has_pkg ? jr_ent(ix, pkg) : JR_NONE;
    if (owner != JR_NONE) {
        jr_kl_members(c, &one, owner, last);
    }
    if (!jr_kl_any(&one)) {
        if (jr_kl_invisible(&one)) {
            lv->types.invisible = true;
            return;
        }
        jr_cand(&lv->types, jr_unseen_kind(ix, pkg), JR_NONE);
        return;
    }
    if (one.types.count > 0) {
        jr_cand(&lv->types, one.types.kind, one.types.ent);
    }
    if (members) {
        jr_found_add(c, &lv->vals, &one.vals, false);
        jr_found_add(c, &lv->calls, &one.calls, false);
    }
}

/* True when the member scope of `ent` is not fully known: the type or its
 * companion object has a supertype outside the repository, directly or
 * further up. */
static bool jr_scope_unknown(const jr_index_t *ix, int ent) {
    int comp = ix->ents[ent].companion;
    return jr_open(ix, ent) || (comp != JR_NONE && jr_open(ix, comp));
}

/* Why a Kotlin lookup passed an enclosing type that may have the member
 * unseen (jr_kotlin_lookup). */
enum { JR_STOP_NO = 0, JR_STOP_GIVEN, JR_STOP_OPEN };

/* A simple name in a Kotlin file, level by level:
 *   the documented definition's own parameters and type parameters, `this`
 *   each enclosing type, innermost first: what it holds itself (with its
 *     companion), its type parameters and constructor parameters, what it
 *     inherits, what its companion inherits
 *   explicit imports -> the file's package -> star imports -> default imports
 * `members` false: types only (the head of a qualified name, a supertype).
 * *gap: a type with members a parse error hides was passed without a hit; a
 * member found after it may be shadowed by one of those.
 * *stop (JR_STOP_*): an enclosing type was passed without a hit that may have
 * a member of the name unseen (jr_kotlin_simple); what the lookup returns
 * then lies further out than that type. */
static jr_lk_t jr_kotlin_lookup(const jr_ctx_t *c, const char *name, bool members, jr_klevel_t *out,
                                bool *exact, bool *gap, int *stop) {
    const jr_index_t *ix = c->ix;
    const jr_file_t *f = c->f;
    jr_klevel_t lv;
    bool invisible = false;
    memset(out, 0, sizeof(*out));
    *exact = false;
    *gap = false;
    *stop = JR_STOP_NO;
    if (c->member &&
        (jr_listed(c->member->tparams, name) || (members && jr_listed(c->member->pnames, name)))) {
        return JR_LK_LOCAL;
    }
    /* a documented class: every parameter of its primary constructor is its
     * own, a `val` one included (seen from a member's doc that one is a
     * property of the class, further down) */
    if (c->type &&
        (jr_listed(c->type->tparams, name) || (members && jr_listed(c->type->pnames, name)))) {
        return JR_LK_LOCAL;
    }
    if (members && strcmp(name, "this") == 0) {
        return JR_LK_LOCAL;
    }
    for (int i = 0; i < c->nchain; i++) {
        int ent = c->chain[i];
        const jr_type_t *decl = c->chain_decl[i];
        memset(&lv, 0, sizeof(lv));
        if (strcmp(ix->ents[ent].name, name) == 0) {
            jr_cand_type(c, &lv.types, ent);
        }
        jr_kl_type_scope(c, &lv, ent, name, members);
        if (jr_kl_any(&lv)) {
            *out = lv;
            return JR_LK_FOUND;
        }
        invisible = invisible || jr_kl_invisible(&lv);
        if (jr_listed(decl->tparams, name) || (members && jr_listed(decl->pnames, name))) {
            return JR_LK_LOCAL;
        }
        if (members && ix->ents[ent].incomplete) {
            *gap = true;
        }
        if (!c->inherit) {
            continue;
        }
        memset(&lv, 0, sizeof(lv));
        jr_kl_inherited(c, &lv, ent, name, true, members, gap);
        if (jr_kl_any(&lv)) {
            *out = lv;
            return JR_LK_FOUND;
        }
        invisible = invisible || jr_kl_invisible(&lv);
        if (!members) {
            continue;
        }
        /* the companion's member scope is part of the class's, with what the
         * companion inherits */
        int comp = ix->ents[ent].companion;
        if (comp != JR_NONE) {
            memset(&lv, 0, sizeof(lv));
            jr_kl_inherited(c, &lv, comp, name, false, true, gap);
            if (jr_kl_any(&lv)) {
                *out = lv;
                return JR_LK_FOUND;
            }
            invisible = invisible || jr_kl_invisible(&lv);
        }
        if (*stop == JR_STOP_NO) {
            *stop = jr_given_member(c, ent, name) ? JR_STOP_GIVEN
                    : jr_scope_unknown(ix, ent)   ? JR_STOP_OPEN
                                                  : JR_STOP_NO;
        }
    }
    /* explicit imports */
    memset(&lv, 0, sizeof(lv));
    for (const jr_import_t *imp = jr_import_next(f, name, NULL); imp;
         imp = jr_import_next(f, name, imp)) {
        jr_kl_import(c, &lv, imp->target, members);
    }
    if (jr_kl_any(&lv)) {
        *out = lv;
        *exact = true;
        return JR_LK_FOUND;
    }
    invisible = invisible || jr_kl_invisible(&lv);
    /* the file's package */
    memset(&lv, 0, sizeof(lv));
    jr_cand_type(c, &lv.types, jr_pkg_type(ix, f->package, name));
    if (members) {
        jr_kl_top(c, &lv, f->package, name);
    }
    if (jr_kl_any(&lv)) {
        *out = lv;
        return JR_LK_FOUND;
    }
    invisible = invisible || jr_kl_invisible(&lv);
    /* star imports: of a package, or of a type (its nested types and members) */
    memset(&lv, 0, sizeof(lv));
    for (int i = 0; i < f->nwild; i++) {
        const char *target = f->wild[i].target;
        if (jr_is_package(ix, target)) {
            jr_cand_type(c, &lv.types, jr_pkg_type(ix, target, name));
            if (members) {
                jr_kl_top(c, &lv, target, name);
            }
        }
        int type = jr_ent(ix, target);
        if (type != JR_NONE) {
            jr_cand_type(c, &lv.types, jr_kotlin_nested(ix, type, name));
            if (members) {
                jr_kl_members(c, &lv, type, name);
            }
        }
    }
    if (jr_kl_any(&lv)) {
        *out = lv;
        return JR_LK_FOUND;
    }
    invisible = invisible || jr_kl_invisible(&lv);
    /* default imports: the repository's own declarations there first */
    memset(&lv, 0, sizeof(lv));
    for (size_t i = 0; i < sizeof(JR_KOTLIN_DEFAULT_PKGS) / sizeof(JR_KOTLIN_DEFAULT_PKGS[0]);
         i++) {
        jr_cand_type(c, &lv.types, jr_pkg_type(ix, JR_KOTLIN_DEFAULT_PKGS[i], name));
        if (members) {
            jr_kl_top(c, &lv, JR_KOTLIN_DEFAULT_PKGS[i], name);
        }
    }
    if (!jr_kl_any(&lv) && (JR_NAMED(JR_KOTLIN_DEFAULT, name) || JR_NAMED(JR_JAVA_LANG, name))) {
        jr_cand(&lv.types, 'X', JR_NONE);
    }
    if (jr_kl_any(&lv)) {
        *out = lv;
        return JR_LK_FOUND;
    }
    invisible = invisible || jr_kl_invisible(&lv);
    return invisible ? JR_LK_INVISIBLE : JR_LK_NONE;
}

/* What a level with candidates is: one type, one property or one function.
 *
 * A type and a function of one name at one level (the factory-function
 * idiom, `class Json` beside `fun Json(..)`) are the TYPE. The order relied
 * on: KDoc's link resolution asks a scope for the classifier of a name before
 * it asks for functions and properties, and the documentation tool links the
 * first answer. That is stated from knowledge of the Kotlin documentation
 * tooling (the compiler's KDoc link resolver, which Dokka uses); it was not
 * checked online. The type binds with the tier the lookup gave. A type whose
 * node another declaration took (class and factory in one file share the
 * file-based qualified name, and the later one owns it) is a graph gap: the
 * function never stands in for it. However many overloads the function has
 * makes no difference. `@sample` names a function, so that order says nothing
 * about it: there a type and a function of one name stay ambiguous.
 *
 * Everything else with more than one candidate is ambiguous, never a pick: a
 * property and a function, a type and a property, a second type, an overload
 * group, members of two owners. */
static jr_res_t jr_kl_result(const jr_ctx_t *c, const jr_klevel_t *lv, bool exact) {
    int kinds = (lv->types.count > 0) + (lv->vals.count > 0) + (lv->calls.count > 0);
    if (!c->sample && lv->types.count == SKIP_ONE && lv->types.kind == 'T' && lv->calls.count > 0 &&
        lv->vals.count == 0 && !lv->clash) {
        jr_res_t type = jr_type_result(c, lv->types.ent, exact);
        atomic_fetch_add(&c->ix->stats->type_and_function, 1);
        if (type.st == JR_OK) {
            atomic_fetch_add(&c->ix->stats->type_and_function_node, 1);
        }
        return type;
    }
    if (kinds > SKIP_ONE || lv->types.count > SKIP_ONE || lv->clash) {
        return jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
    }
    if (lv->types.count == SKIP_ONE) {
        if (lv->types.kind == 'T') {
            return jr_type_result(c, lv->types.ent, exact);
        }
        return jr_unres(jr_kind_reason(lv->types.kind));
    }
    const jr_found_t *m = lv->vals.count > 0 ? &lv->vals : &lv->calls;
    return m->several ? jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS) : jr_found_result(m, exact);
}

/* A simple KDoc name.
 *
 * A function or a property further out than an enclosing type binds only when
 * that type is known not to have a member of the name. KDoc resolves a name
 * in the member scope of each enclosing class, inherited members included,
 * before it looks at the class around it, the file's imports or its package.
 * So when the lookup passed a type
 *   - that has the name without declaring it (jr_given_member: a member of
 *     kotlin.Any, of an enum class), or
 *   - whose supertypes, or whose companion's, leave the repository
 *     (jr_scope_unknown): one of them may declare the name,
 * a function or a property found further out is not taken: the reference is
 * `external`, and so is a name found nowhere. The order relied on is stated
 * from knowledge of the Kotlin documentation tooling (the compiler's KDoc
 * link resolver); it was not checked online.
 * A TYPE found further out binds as before: the guard is about the member
 * functions and properties a type has, not about nested types a supertype
 * outside the repository might lend. */
static jr_res_t jr_kotlin_simple(const jr_ctx_t *c, const char *name) {
    jr_klevel_t lv;
    bool exact = false;
    bool gap = false;
    int stop = JR_STOP_NO;
    jr_lk_t lk = jr_kotlin_lookup(c, name, true, &lv, &exact, &gap, &stop);
    if (lk == JR_LK_LOCAL) {
        return (jr_res_t){.st = JR_LOCAL};
    }
    if (lk == JR_LK_INVISIBLE) {
        return jr_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    if (lk == JR_LK_FOUND) {
        /* a member found past a type with hidden members: one of those may
         * be the one. A type is safe: a type declared where the scan lost its
         * place is quarantined by name. */
        if (gap && lv.types.count == 0) {
            return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
        }
        if (stop != JR_STOP_NO && lv.types.count == 0) {
            jr_stats_t *st = c->ix->stats;
            atomic_fetch_add(stop == JR_STOP_GIVEN ? &st->kotlin_given : &st->kotlin_open, 1);
            return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
        return jr_kl_result(c, &lv, exact);
    }
    if (stop != JR_STOP_NO) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL); /* the member that type may have */
    }
    jr_res_t res = jr_unfound_simple(c, name);
    return (gap && res.reason == CBM_DOCLINK_REASON_MISSING)
               ? jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP)
               : res;
}

/* A (qualified) type name in a Kotlin file: the head through the scope or,
 * when no level has it, from the root as packages and then a type; the rest
 * as nested types. */
static jr_tp_t jr_kotlin_type_path(const jr_ctx_t *c, char *const *segs, int n) {
    const jr_index_t *ix = c->ix;
    jr_klevel_t lv;
    bool exact = false;
    bool gap = false;
    int stop = JR_STOP_NO; /* types only: nothing to hold back */
    jr_lk_t lk = jr_kotlin_lookup(c, segs[0], false, &lv, &exact, &gap, &stop);
    if (lk == JR_LK_LOCAL) {
        return jr_tp_res((jr_res_t){.st = JR_LOCAL});
    }
    if (lk == JR_LK_INVISIBLE) {
        return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
    }
    int cur = JR_NONE;
    int from = SKIP_ONE;
    if (lk == JR_LK_FOUND) {
        if (lv.types.count > SKIP_ONE) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
        }
        if (lv.types.kind != 'T') {
            return jr_tp_res(jr_unres(jr_kind_reason(lv.types.kind)));
        }
        cur = lv.types.ent;
    }
    for (int i = SKIP_ONE; cur == JR_NONE && i < n; i++) {
        char pkg[JR_KEY_BUF];
        if (!jr_join(segs, 0, i, pkg, sizeof(pkg))) {
            break;
        }
        int ent = jr_pkg_type(ix, pkg, segs[i]);
        if (ent == JR_NONE) {
            continue;
        }
        if (!jr_visible(c, ent)) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
        }
        cur = ent;
        from = i + SKIP_ONE;
        exact = true;
    }
    if (cur == JR_NONE) {
        jr_tp_t tp = jr_tp_res(jr_unfound_path(c, segs, n));
        tp.unfound = true;
        return tp;
    }
    for (int i = from; i < n; i++) {
        int next = jr_kotlin_nested(ix, cur, segs[i]);
        if (next == JR_NONE) {
            return (jr_tp_t){.st = JR_TP_PARTIAL, .ent = cur, .failed_seg = i, .exact = exact};
        }
        if (!jr_visible(c, next)) {
            return jr_tp_res(jr_unres(CBM_DOCLINK_REASON_TEST_ONLY));
        }
        cur = next;
    }
    return (jr_tp_t){.st = JR_TP_OK, .ent = cur, .exact = exact};
}

static bool jr_ext_key(int recv, const char *name, char *out, size_t cap) {
    int n = recv == JR_NONE ? snprintf(out, cap, "?\x1f%s", name)
                            : snprintf(out, cap, "%d\x1f%s", recv, name);
    return n >= 0 && (size_t)n < cap;
}

/* True when the extension `m` of `file` is in scope of the source under its
 * simple name: declared in the source's package, imported, or a member of an
 * enclosing type. KDoc finds an extension for `Type.name` among the callables
 * in scope, nowhere else. */
static bool jr_ext_in_scope(const jr_ctx_t *c, int file, const jr_member_t *m) {
    const jr_file_t *xf = &c->ix->files[file];
    if (m->owner != JR_NONE) {
        int owner = xf->types[m->owner].entity;
        for (int i = 0; i < c->nchain; i++) {
            if (c->chain[i] == owner) {
                return true;
            }
        }
        return false;
    }
    if (strcmp(xf->package, c->f->package) == 0) {
        return true;
    }
    for (int i = 0; i < c->f->nwild; i++) {
        if (strcmp(c->f->wild[i].target, xf->package) == 0) {
            return true;
        }
    }
    size_t pl = strlen(xf->package);
    for (const jr_import_t *imp = jr_import_next(c->f, m->name, NULL); imp;
         imp = jr_import_next(c->f, m->name, imp)) {
        const char *t = imp->target;
        if (pl == 0 ? strcmp(t, m->name) == 0
                    : (strncmp(t, xf->package, pl) == 0 && t[pl] == '.' &&
                       strcmp(t + pl + SKIP_ONE, m->name) == 0)) {
            return true;
        }
    }
    return false;
}

/* The extensions `name` in scope whose receiver is `ent`, else one of its
 * supertypes (nearest first), else any type. false when there is none. */
static bool jr_kotlin_extension(const jr_ctx_t *c, int ent, const char *name, jr_res_t *res,
                                bool *invisible) {
    const jr_index_t *ix = c->ix;
    const jr_entity_t *e = &ix->ents[ent];
    for (int k = -1; k <= e->nclosure; k++) {
        int recv = k < 0 ? ent : (k < e->nclosure ? e->closure[k] : JR_NONE);
        char key[JR_KEY_BUF];
        if (!jr_ext_key(recv, name, key, sizeof(key))) {
            return false;
        }
        const jr_ilist_t *l = (const jr_ilist_t *)cbm_ht_get(ix->extensions, key);
        jr_found_t found = {0};
        const char *package = NULL;
        for (int i = 0; l && i + SKIP_ONE < l->count; i += PAIR_LEN) {
            int file = l->items[i];
            const jr_member_t *m = &ix->files[file].members[l->items[i + SKIP_ONE]];
            if (!jr_ext_in_scope(c, file, m)) {
                continue;
            }
            if (c->prod && ix->files[file].is_test) {
                *invisible = true;
                continue;
            }
            /* the same name and signature in two packages are two functions */
            if (package && strcmp(package, ix->files[file].package) != 0) {
                found.several = true;
            }
            package = ix->files[file].package;
            jr_found_take(c, &found, file, m);
        }
        if (found.count > 0) {
            *res = found.several ? jr_unres(CBM_DOCLINK_REASON_AMBIGUOUS)
                                 : jr_found_result(&found, false);
            return true;
        }
    }
    return false;
}

/* `name` in the type `ent`, named explicitly: what the type holds itself
 * (with its companion), then what it inherits and what its companion
 * inherits, the members every type has, and the extensions in scope whose
 * receiver it is.
 *
 * An extension is taken only when the type is known not to have a member of
 * the name: a member comes before an extension. Where the supertypes of the
 * type, or of its companion, leave the repository (jr_scope_unknown), one of
 * them may declare the name: `external`, never the extension. Stated from
 * knowledge of the language and of the KDoc link resolver, which asks the
 * type's member scope before the extensions in scope; not checked online. */
static jr_res_t jr_kotlin_in_type(const jr_ctx_t *c, int ent, const char *name, bool exact) {
    const jr_index_t *ix = c->ix;
    jr_klevel_t lv;
    bool gap = false;
    memset(&lv, 0, sizeof(lv));
    jr_kl_type_scope(c, &lv, ent, name, true);
    if (jr_kl_any(&lv)) {
        return jr_kl_result(c, &lv, exact);
    }
    bool invisible = jr_kl_invisible(&lv);
    if (ix->ents[ent].incomplete) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    memset(&lv, 0, sizeof(lv));
    jr_kl_inherited(c, &lv, ent, name, true, true, &gap);
    if (jr_kl_any(&lv)) {
        return jr_kl_result(c, &lv, exact);
    }
    invisible = invisible || jr_kl_invisible(&lv);
    int comp = ix->ents[ent].companion;
    if (comp != JR_NONE && !gap) {
        memset(&lv, 0, sizeof(lv));
        jr_kl_inherited(c, &lv, comp, name, false, true, &gap);
        if (jr_kl_any(&lv)) {
            return jr_kl_result(c, &lv, exact);
        }
        invisible = invisible || jr_kl_invisible(&lv);
    }
    if (gap) {
        return jr_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (jr_implicit_member(ix, ent, name)) {
        return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    jr_res_t ext;
    bool has_ext = jr_kotlin_extension(c, ent, name, &ext, &invisible);
    if (jr_scope_unknown(ix, ent)) {
        if (has_ext) {
            atomic_fetch_add(&ix->stats->kotlin_extension, 1);
            return jr_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
        return jr_unres(invisible ? CBM_DOCLINK_REASON_TEST_ONLY : CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (has_ext) {
        return ext;
    }
    return jr_unres(invisible ? CBM_DOCLINK_REASON_TEST_ONLY : CBM_DOCLINK_REASON_MISSING);
}

/* True when `name` is a parameter or type parameter of the documented
 * definition or of a type around it. */
static bool jr_kotlin_local(const jr_ctx_t *c, const char *name) {
    if (c->member && (jr_listed(c->member->pnames, name) || jr_listed(c->member->tparams, name))) {
        return true;
    }
    for (int i = 0; i < c->nchain; i++) {
        if (jr_listed(c->chain_decl[i]->pnames, name) ||
            jr_listed(c->chain_decl[i]->tparams, name)) {
            return true;
        }
    }
    return strcmp(name, "this") == 0;
}

/* A dotted KDoc name. The qualifier is a type (found through the scope, or
 * from the root) and the last name what that type holds; or the qualifier is
 * a package and the last name one of its types or top-level declarations. */
static jr_res_t jr_kotlin_qualified(const jr_ctx_t *c, const jr_ref_t *r) {
    const jr_index_t *ix = c->ix;
    int n = r->nsegs;
    const char *last = r->segs[n - SKIP_ONE];
    if (jr_kotlin_local(c, r->segs[0])) {
        return (jr_res_t){.st = JR_LOCAL}; /* `param.member` names nothing elsewhere */
    }
    jr_tp_t qual = jr_kotlin_type_path(c, r->segs, n - SKIP_ONE);
    if (qual.st == JR_TP_OK) {
        return jr_kotlin_in_type(c, qual.ent, last, qual.exact);
    }
    if (qual.st == JR_TP_PARTIAL) {
        return jr_unres(CBM_DOCLINK_REASON_MISSING); /* a type without that nested type */
    }
    if (!qual.unfound) {
        return qual.res; /* the qualifier is known, and is no type of the repository */
    }
    char pkg[JR_KEY_BUF];
    if (!jr_join(r->segs, 0, n - SKIP_ONE, pkg, sizeof(pkg))) {
        return jr_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    jr_klevel_t lv;
    memset(&lv, 0, sizeof(lv));
    jr_cand_type(c, &lv.types, jr_pkg_type(ix, pkg, last));
    jr_kl_top(c, &lv, pkg, last);
    if (jr_kl_any(&lv)) {
        return jr_kl_result(c, &lv, true);
    }
    if (jr_kl_invisible(&lv)) {
        return jr_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    return jr_unfound_path(c, r->segs, n);
}

static jr_res_t jr_kotlin_resolve(const jr_ctx_t *c, const jr_ref_t *r) {
    return r->nsegs == SKIP_ONE ? jr_kotlin_simple(c, r->segs[0]) : jr_kotlin_qualified(c, r);
}

/* ── Context ─────────────────────────────────────────────────────── */

/* True when a declaration with `slot` (see jr_type_t) is the definition whose
 * name below the module is `tail`. `path`: its logical path. */
static bool jr_slot_is(const char *slot, const char *owner_path, const char *name,
                       const char *tail) {
    if (!slot) {
        return false;
    }
    if (strcmp(slot, "=") != 0) {
        return strcmp(slot, tail) == 0;
    }
    if (!owner_path) {
        return strcmp(name, tail) == 0;
    }
    size_t ol = strlen(owner_path);
    return strncmp(tail, owner_path, ol) == 0 && tail[ol] == '.' &&
           strcmp(tail + ol + SKIP_ONE, name) == 0;
}

/* The scope of the definition a reference is written on: the documented
 * member or type (found by its line and its name below the module), and the
 * types around it. */
static void jr_ctx_init(jr_ctx_t *c, const jr_index_t *ix, const cbm_gbuf_t *g, int file,
                        const CBMDocLink *link) {
    memset(c, 0, sizeof(*c));
    c->ix = ix;
    c->g = g;
    c->file = file;
    c->f = &ix->files[file];
    c->prod = !c->f->is_test;
    c->inherit = true;
    if (link->flags & CBM_DOCLINK_FLAG_FILE) {
        return; /* a file-level doc: the file's scope alone */
    }
    const jr_file_t *f = c->f;
    uint32_t line = link->def_line;
    size_t ml = strlen(f->module_qn);
    const char *qn = link->source_qn;
    const char *tail =
        (qn && strncmp(qn, f->module_qn, ml) == 0 && qn[ml] == '.') ? qn + ml + SKIP_ONE : NULL;
    int lo = 0;
    int hi = f->nmembers;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->members[f->by_start[mid]].start < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; tail && i < f->nmembers; i++) {
        const jr_member_t *m = &f->members[f->by_start[i]];
        if (m->start != line) {
            break;
        }
        if (jr_slot_is(m->slot, m->owner == JR_NONE ? NULL : f->types[m->owner].path, m->name,
                       tail)) {
            c->member = m;
            jr_chain_from(c, m->owner);
            return;
        }
    }
    /* types are in source order: the last one that starts at or before the line */
    lo = 0;
    hi = f->ntypes;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->types[mid].start <= line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    int t = lo - SKIP_ONE;
    for (int k = t; tail && k >= 0 && f->types[k].start == line; k--) {
        if (jr_slot_is(f->types[k].slot, NULL, f->types[k].path, tail)) {
            c->type = &f->types[k];
            jr_chain_from(c, k);
            return;
        }
    }
    /* neither: a definition the scan has no record of. The innermost type
     * around its line is its scope. */
    while (t >= 0 && f->types[t].end < line) {
        t = f->types[t].outer;
    }
    if (t >= 0) {
        jr_chain_from(c, t);
    }
}

/* The scope of a type DECLARATION, for the names its header writes
 * (supertypes, an extension's receiver): the types around it. From structure
 * alone, since a stored scope has no lines. */
static void jr_ctx_decl(jr_ctx_t *c, const jr_index_t *ix, const cbm_gbuf_t *g, int file, int outer,
                        bool inherit) {
    memset(c, 0, sizeof(*c));
    c->ix = ix;
    c->g = g;
    c->file = file;
    c->f = &ix->files[file];
    c->prod = false; /* a declared supertype is whatever the compiler bound */
    c->inherit = inherit;
    jr_chain_from(c, outer);
}

/* ── Supertypes, extensions ──────────────────────────────────────── */

/* Split a dotted name in place. Returns the number of names, 0 when there
 * are more than a reference may have. */
static int jr_split_dots(char *s, char **segs) {
    int n = 0;
    for (char *p = s;;) {
        char *dot = strchr(p, '.');
        if (dot) {
            *dot = '\0';
        }
        if (n >= JR_MAX_SEGS || !p[0]) {
            return 0;
        }
        segs[n++] = p;
        if (!dot) {
            return n;
        }
        p = dot + SKIP_ONE;
    }
}

/* The type a declaration header of `file` names with `name[0, len)`, in the
 * scope `c`; JR_NONE when the repository declares no such type. */
static int jr_header_type(const jr_ctx_t *c, const char *name, size_t len) {
    char buf[JR_KEY_BUF];
    char *segs[JR_MAX_SEGS];
    if (len == 0 || len >= sizeof(buf)) {
        return JR_NONE;
    }
    memcpy(buf, name, len);
    buf[len] = '\0';
    int n = jr_split_dots(buf, segs);
    if (n == 0) {
        return JR_NONE;
    }
    jr_tp_t tp = c->f->kotlin ? jr_kotlin_type_path(c, segs, n) : jr_java_type_path(c, segs, n);
    return tp.st == JR_TP_OK ? tp.ent : JR_NONE;
}

/* True when a declaration header of the context's file writes, with
 * `name[0, len)`, the root type every type has anyway: java.lang.Object,
 * kotlin.Any. Written out it adds no member (jr_given_member knows them), so
 * it does not leave the hierarchy open. The simple name counts only where
 * nothing else can supply it: no import of that name, no on-demand import of
 * a package the repository does not declare. */
static bool jr_root_super(const jr_ctx_t *c, const char *name, size_t len) {
    const char *full = c->f->kotlin ? "kotlin.Any" : "java.lang.Object";
    const char *simple = strrchr(full, '.') + SKIP_ONE;
    if (len == strlen(full) && memcmp(name, full, len) == 0) {
        return true;
    }
    return len == strlen(simple) && memcmp(name, simple, len) == 0 &&
           !jr_import_next(c->f, simple, NULL) && !jr_scope_open(c);
}

/* True when the repository's type `step` is an interface to javadoc (an
 * annotation type is one). */
static bool jr_walk_iface(const jr_index_t *ix, int step) {
    return step >= 0 && (ix->ents[step].kind == 'i' || ix->ents[step].kind == 'a');
}

/* The steps of javadoc's search for a method (jr_walk) at one declaration of
 * kind `kind`, from the supertypes its header writes, in the order written
 * (`steps[0, nd)`: types of the repository, JR_W_OBJECT for Object or Any
 * written out, JR_W_OUTSIDE for any other). Returns the superclass step and
 * leaves the interface steps, in order, in steps[0, *n).
 *   - An interface has no superclass; an annotation type has the interface
 *     java.lang.annotation.Annotation, an enum the superclass java.lang.Enum,
 *     a record java.lang.Record (which adds nothing to what the record has
 *     itself and Object's), any other class Object unless its header names
 *     another.
 *   - Java writes the superclass first: a first supertype that is no
 *     interface of the repository is it. One outside the repository may be
 *     the superclass or the first interface; it is taken as the superclass,
 *     the earlier of the two places.
 *   - Kotlin writes the superclass anywhere in the list: the one class of the
 *     repository there is it. Without one, a supertype outside the
 *     repository may be the superclass, which javadoc searches before every
 *     interface: the superclass step is then outside.
 *   - A Kotlin type alias is outside to this search. */
static int jr_walk_steps(const jr_index_t *ix, char kind, bool kotlin, int *steps, int nd, int *n) {
    int sup = JR_W_OBJECT;
    int skip = JR_NONE; /* the entry that is the superclass */
    for (int i = 0; i < nd; i++) {
        if (steps[i] >= 0 && ix->ents[steps[i]].kind == 't') {
            steps[i] = JR_W_OUTSIDE;
        }
    }
    *n = 0;
    if (kind == 'i') {
        sup = JR_W_NONE;
    } else if (kind == 'a') {
        sup = JR_W_ANNOTATION;
    } else if (kind == 'e') {
        sup = JR_W_ENUM;
    } else if (kind == 't') {
        return JR_W_OUTSIDE;
    } else if (kind != 'r' && !kotlin) {
        if (nd > 0 && steps[0] != JR_W_OBJECT && !jr_walk_iface(ix, steps[0])) {
            sup = steps[0];
            skip = 0;
        }
    } else if (kind != 'r') {
        int classes = 0;
        bool outside = false;
        for (int i = 0; i < nd; i++) {
            if (steps[i] >= 0 && !jr_walk_iface(ix, steps[i])) {
                classes++;
                skip = i;
            }
            outside = outside || steps[i] == JR_W_OUTSIDE;
        }
        if (classes > SKIP_ONE) {
            return JR_W_OUTSIDE; /* two superclasses: no order to follow */
        }
        if (classes == SKIP_ONE) {
            sup = steps[skip];
        } else if (outside) {
            sup = JR_W_OUTSIDE;
        }
    }
    int w = 0;
    for (int i = 0; i < nd; i++) {
        if (i != skip && steps[i] != JR_W_OBJECT) {
            steps[w++] = steps[i];
        }
    }
    *n = w;
    return sup;
}

/* One declaration's steps into its type's: the first declaration sets them,
 * a later one that has other steps or another order leaves the type's place
 * in javadoc's search unknown (its superclass step outside). */
static bool jr_walk_decl(jr_index_t *ix, jr_entity_t *e, const jr_type_t *t, bool kotlin,
                         int *steps, int nd, bool merge) {
    int n = 0;
    int sup = jr_walk_steps(ix, t->kind, kotlin, steps, nd, &n);
    if (!merge) {
        e->wsuper = sup;
        if (n > 0) {
            e->wifaces = (int *)jr_alloc(ix, (size_t)n * sizeof(int));
            if (!e->wifaces) {
                return false;
            }
            memcpy(e->wifaces, steps, (size_t)n * sizeof(int));
            e->nwifaces = n;
        }
        return true;
    }
    if (sup != e->wsuper || n != e->nwifaces ||
        (n > 0 && memcmp(steps, e->wifaces, (size_t)n * sizeof(int)) != 0)) {
        e->wsuper = JR_W_OUTSIDE;
        e->nwifaces = 0;
    }
    return true;
}

/* The type around declaration `t` of `f`, as the scope chain has it. */
static int jr_walk_encl(const jr_file_t *f, const jr_type_t *t) {
    int o = t->outer;
    while (o != JR_NONE && f->types[o].entity < 0) {
        o = f->types[o].outer;
    }
    return o == JR_NONE ? JR_NONE : f->types[o].entity;
}

/* The direct supertypes of every type (each declaration's list, bound in that
 * declaration's own scope), then every type's closure: breadth-first, the
 * nearest first, each supertype once. A supertype that is no type of the
 * repository leaves the hierarchy open. Alongside, each type's steps of
 * javadoc's search for a method (jr_walk_steps). */
static bool jr_resolve_supers(jr_index_t *ix, const cbm_gbuf_t *g) {
    size_t n = (size_t)(ix->nents ? ix->nents : SKIP_ONE);
    int *seen = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER, n * sizeof(int));
    int *queue = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, n * sizeof(int));
    int *steps = NULL; /* one declaration's supertypes, as javadoc's search sees them */
    int scap = 0;
    bool ok = seen && queue;
    for (int ei = 0; ok && ei < ix->nents; ei++) {
        jr_entity_t *e = &ix->ents[ei];
        int stamp = ei + SKIP_ONE;
        int nb = 0;
        for (int d = 0; ok && d < e->ndecls; d++) {
            int file = e->decls[d].file;
            const jr_type_t *t = &ix->files[file].types[e->decls[d].type];
            jr_ctx_t c;
            if (t->supers[0]) {
                jr_ctx_decl(&c, ix, g, file, t->outer, false);
            }
            int nd = 0;
            for (const char *p = t->supers; ok && p && *p;) {
                const char *bar = strchr(p, '|');
                size_t len = bar ? (size_t)(bar - p) : strlen(p);
                int base = jr_header_type(&c, p, len);
                int step = base;
                if (base == ei) {
                    e->open = true;
                    step = JR_W_OUTSIDE;
                } else if (base == JR_NONE) {
                    bool root = jr_root_super(&c, p, len);
                    e->open = e->open || !root;
                    step = root ? JR_W_OBJECT : JR_W_OUTSIDE;
                } else if (seen[base] != stamp) {
                    seen[base] = stamp;
                    queue[nb++] = base;
                }
                if (nd >= scap) {
                    int ncap = scap ? scap * PAIR_LEN : CBM_SZ_16;
                    int *grown = (int *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * sizeof(int));
                    ok = grown != NULL;
                    if (ok && nd > 0) {
                        memcpy(grown, steps, (size_t)nd * sizeof(int));
                    }
                    cbm_free(CBM_MEM_CLASS_OTHER, steps);
                    steps = grown;
                    scap = ok ? ncap : 0;
                }
                if (ok) {
                    steps[nd++] = step;
                }
                p = bar ? bar + SKIP_ONE : NULL;
            }
            ok = ok && jr_walk_decl(ix, e, t, ix->files[file].kotlin, steps, nd, d > 0);
            if (d == 0) {
                e->encl = jr_walk_encl(&ix->files[file], t);
            }
        }
        if (ok && nb > 0) {
            e->bases = (int *)jr_alloc(ix, (size_t)nb * sizeof(int));
            ok = e->bases != NULL;
            if (ok) {
                memcpy(e->bases, queue, (size_t)nb * sizeof(int));
                e->nbases = nb;
            }
        }
    }
    memset(seen, 0, ok ? n * sizeof(int) : 0);
    for (int ei = 0; ok && ei < ix->nents; ei++) {
        jr_entity_t *e = &ix->ents[ei];
        int stamp = ei + SKIP_ONE;
        int head = 0;
        int tail = 0;
        seen[ei] = stamp; /* a cycle does not come back to the type itself */
        for (int b = 0; b < e->nbases; b++) {
            if (seen[e->bases[b]] != stamp) {
                seen[e->bases[b]] = stamp;
                queue[tail++] = e->bases[b];
            }
        }
        while (head < tail) {
            const jr_entity_t *s = &ix->ents[queue[head++]];
            for (int b = 0; b < s->nbases; b++) {
                if (seen[s->bases[b]] != stamp) {
                    seen[s->bases[b]] = stamp;
                    queue[tail++] = s->bases[b];
                }
            }
        }
        if (tail > 0) {
            e->closure = (int *)jr_alloc(ix, (size_t)tail * sizeof(int));
            ok = e->closure != NULL;
            if (ok) {
                memcpy(e->closure, queue, (size_t)tail * sizeof(int));
                e->nclosure = tail;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    cbm_free(CBM_MEM_CLASS_OTHER, queue);
    cbm_free(CBM_MEM_CLASS_OTHER, steps);
    return ok;
}

/* The receiver of every Kotlin extension, bound in its file's scope, and the
 * extensions of each receiver type by name. */
static bool jr_build_extensions(jr_index_t *ix, const cbm_gbuf_t *g) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        jr_file_t *f = &ix->files[fi];
        for (int mi = 0; f->kotlin && mi < f->nmembers; mi++) {
            jr_member_t *m = &f->members[mi];
            if (!m->recv[0]) {
                continue;
            }
            if (strcmp(m->recv, "?") != 0) {
                jr_ctx_t c;
                jr_ctx_decl(&c, ix, g, fi, m->owner, true);
                m->recv_entity = jr_header_type(&c, m->recv, strlen(m->recv));
                if (m->recv_entity == JR_NONE) {
                    continue; /* a receiver outside the repository: nobody names it */
                }
            }
            char key[JR_KEY_BUF];
            if (!jr_ext_key(m->recv_entity, m->name, key, sizeof(key))) {
                continue;
            }
            jr_ilist_t *l = jr_ht_ilist(ix, ix->extensions, key, true);
            if (!l || !jr_ilist_push(ix, l, fi) || !jr_ilist_push(ix, l, mi)) {
                return false;
            }
        }
    }
    return true;
}

/* ── Index lifecycle and the resolver hooks ──────────────────────── */

static void jr_destroy(void *index) {
    jr_index_t *ix = (jr_index_t *)index;
    if (!ix) {
        return;
    }
    if (ix->stats) {
        const jr_stats_t *st = ix->stats;
        const _Atomic int64_t *vals[] = {&st->source_gaps,       &st->plain_text,
                                         &st->type_and_function, &st->type_and_function_node,
                                         &st->java_given,        &st->java_open,
                                         &st->kotlin_given,      &st->kotlin_open,
                                         &st->kotlin_extension,  &st->java_field,
                                         &st->java_field_named};
        char b[sizeof(vals) / sizeof(vals[0])][CBM_SZ_32];
        for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); i++) {
            snprintf(b[i], sizeof(b[i]), "%lld", (long long)atomic_load(vals[i]));
        }
        cbm_log_info("doc_links.jvm.done", "source_without_node", b[0], "kdoc_plain_text", b[1],
                     "type_and_function", b[2], "type_and_function_with_node", b[3],
                     "not_taken_java_given", b[4], "not_taken_java_open", b[5],
                     "not_taken_kotlin_given", b[6], "not_taken_kotlin_open", b[7],
                     "not_taken_kotlin_extension", b[8], "not_taken_java_field", b[9],
                     "not_taken_java_field_named", b[10]);
        cbm_free(CBM_MEM_CLASS_OTHER, ix->stats);
    }
    cbm_ht_free(ix->ent_by_fqn);
    cbm_ht_free(ix->pkg_types);
    cbm_ht_free(ix->packages);
    cbm_ht_free(ix->pkg_prefixes);
    cbm_ht_free(ix->pkg_members);
    cbm_ht_free(ix->extensions);
    cbm_ht_free(ix->quarantine);
    cbm_ht_free(ix->quarantine_test);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static void *jr_build_fail(jr_index_t *ix, const char *step) {
    cbm_log_error("doc_links.jvm.error", "step", step, "reason", "alloc");
    jr_destroy(ix);
    return NULL;
}

static bool jr_has_suffix(const char *s, const char *sfx) {
    size_t n = strlen(s);
    size_t sl = strlen(sfx);
    return n >= sl && strcmp(s + n - sl, sfx) == 0;
}

static void *jr_build(const cbm_doclink_build_in_t *in) {
    jr_index_t *ix = (jr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->mode = in->ctx->mode;
    ix->ent_by_fqn = cbm_ht_create(CBM_SZ_4K);
    ix->pkg_types = cbm_ht_create(CBM_SZ_4K);
    ix->packages = cbm_ht_create(CBM_SZ_1K);
    ix->pkg_prefixes = cbm_ht_create(CBM_SZ_1K);
    ix->pkg_members = cbm_ht_create(CBM_SZ_1K);
    ix->extensions = cbm_ht_create(CBM_SZ_256);
    ix->quarantine = cbm_ht_create(CBM_SZ_64);
    ix->quarantine_test = cbm_ht_create(CBM_SZ_64);
    ix->nfiles = in->file_count;
    ix->files = (jr_file_t *)jr_alloc(ix, (size_t)(in->file_count ? in->file_count : SKIP_ONE) *
                                              sizeof(jr_file_t));
    ix->run_count = in->run_file_count;
    ix->run_to_file = (int *)cbm_alloc(
        CBM_MEM_CLASS_OTHER,
        (size_t)(in->run_file_count ? in->run_file_count : SKIP_ONE) * sizeof(int));
    ix->stats = (jr_stats_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix->stats));
    if (!ix->ent_by_fqn || !ix->pkg_types || !ix->packages || !ix->pkg_prefixes ||
        !ix->pkg_members || !ix->extensions || !ix->quarantine || !ix->quarantine_test ||
        !ix->files || !ix->run_to_file || !ix->stats) {
        return jr_build_fail(ix, "tables");
    }
    for (int i = 0; i < in->run_file_count; i++) {
        ix->run_to_file[i] = JR_NONE;
    }
    int bad_scopes = 0;
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        jr_file_t *f = &ix->files[i];
        memset(f, 0, sizeof(*f));
        f->rel_path = src->rel_path; /* stays valid until destroy */
        f->is_test = jr_is_test_path(src->rel_path);
        f->scoped = src->scope && jr_parse_scope(ix, f, src->scope);
        if (!f->scoped) {
            bad_scopes += src->scope != NULL;
            f->nimports = f->nwild = f->ntypes = f->nmembers = f->nunplaced = 0;
        }
        f->kotlin = !jr_has_suffix(src->rel_path, ".java");
        if (!f->package) {
            f->package = "";
        }
        f->module_qn = cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path,
                                                  f->kotlin ? CBM_LANG_KOTLIN : CBM_LANG_JAVA);
        if (!f->module_qn) {
            ix->oom = true;
        }
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
    }
    if (ix->oom) {
        return jr_build_fail(ix, "scopes");
    }
    if (bad_scopes > 0) {
        char b[CBM_SZ_32];
        snprintf(b, sizeof(b), "%d", bad_scopes);
        cbm_log_warn("doc_links.jvm.bad_scope", "files", b);
    }
    int skipped = 0;
    if (!jr_build_entities(ix, &skipped) || ix->oom) {
        return jr_build_fail(ix, "entities");
    }
    if (!jr_resolve_supers(ix, in->graph) || ix->oom) {
        return jr_build_fail(ix, "supertypes");
    }
    if (!jr_build_extensions(ix, in->graph) || ix->oom) {
        return jr_build_fail(ix, "extensions");
    }
    int incomplete = 0;
    for (int i = 0; i < ix->nents; i++) {
        incomplete += ix->ents[i].incomplete;
    }
    char b[5][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[1], sizeof(b[1]), "%d", ix->nents);
    snprintf(b[2], sizeof(b[2]), "%d", skipped);
    snprintf(b[3], sizeof(b[3]), "%d", incomplete);
    snprintf(b[4], sizeof(b[4]), "%u",
             (unsigned)(cbm_ht_count(ix->quarantine) + cbm_ht_count(ix->quarantine_test)));
    cbm_log_info("doc_links.jvm.index", "files", b[0], "types", b[1], "skipped_types", b[2],
                 "incomplete_types", b[3], "quarantined_names", b[4]);
    return ix;
}

/* True when the documented definition has a node of its own (or shares the
 * node of its overload group). One node per qualified name: a definition
 * whose name another declaration took (a Kotlin class and the factory
 * function named like it, two classes' properties of one name in one file)
 * has none, and an edge written for it would leave from that other
 * declaration. A definition the scan has no record of is checked against the
 * node itself: the source file is extracted in this run, so its nodes carry
 * their lines. */
static bool jr_source_has_node(const jr_ctx_t *c, const CBMDocLink *link) {
    if (link->flags & CBM_DOCLINK_FLAG_FILE) {
        return true; /* the File node: the core finds it */
    }
    if (c->member) {
        return jr_member_node(c, c->file, c->member) != NULL;
    }
    if (c->type) {
        jr_decl_t d = {.file = c->file, .type = (int)(c->type - c->f->types)};
        return jr_type_node(c, d) != NULL;
    }
    const cbm_gbuf_node_t *n = link->source_qn ? cbm_gbuf_find_by_qn(c->g, link->source_qn) : NULL;
    return n && n->file_path && strcmp(n->file_path, c->f->rel_path) == 0 &&
           n->start_line == (int)link->def_line;
}

/* True when `line` lies in a part of the file the scan could not place. */
static bool jr_unplaced_line(const jr_file_t *f, uint32_t line) {
    int lo = 0;
    int hi = f->nunplaced;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->unplaced[mid].to < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo < f->nunplaced && f->unplaced[lo].from <= line;
}

static void jr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* Java and Kotlin keep no per-file working state */
    const jr_index_t *ix = (const jr_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    int file = ix->run_to_file[run_file];
    const jr_file_t *f = &ix->files[file];
    if (!f->scoped) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP; /* no scope to resolve in: never a guess */
        return;
    }
    jr_ref_t r;
    if (!link->raw ||
        !(f->kotlin ? jr_parse_kotlin(link->raw, &r) : jr_parse_java(link->raw, &r))) {
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    /* What could not be placed is not resolved around: a definition in a part
     * of its file where the scan lost its place has no known scope, and a
     * name some file declares at an unknown place could be that declaration.
     * Both are declared-but-unplaced: graph gaps. */
    bool sample = link->syntax == CBM_DOCLINK_KT_SAMPLE;
    bool unplaced = !(link->flags & CBM_DOCLINK_FLAG_FILE) && jr_unplaced_line(f, link->def_line);
    for (int i = 0; !unplaced && i < r.nsegs; i++) {
        unplaced = cbm_ht_get(ix->quarantine, r.segs[i]) != NULL ||
                   ((f->is_test || sample) && cbm_ht_get(ix->quarantine_test, r.segs[i]) != NULL);
    }
    if (unplaced) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    jr_ctx_t c;
    jr_ctx_init(&c, ix, graph, file, link);
    if (sample) {
        c.prod = false; /* samples live in test sources */
        c.sample = true;
    }
    jr_res_t res = f->kotlin ? jr_kotlin_resolve(&c, &r)
                             : jr_java_resolve(&c, &r, link->syntax == CBM_DOCLINK_JAVA_VALUE);
    /* KDoc `[name]` is Markdown: a name that resolves to nothing is rendered
     * as the plain text it is, so it is no reference. A block tag (@see,
     * @throws, @sample) names its subject and keeps its row. */
    bool plain_text = link->syntax == CBM_DOCLINK_KT_KDOC_LINK && res.st == JR_UNRES &&
                      res.reason == CBM_DOCLINK_REASON_MISSING;
    if (res.st == JR_LOCAL || plain_text) {
        if (plain_text) {
            atomic_fetch_add(&ix->stats->plain_text, 1);
        }
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    if (res.st == JR_OK && res.node) {
        if (!jr_source_has_node(&c, link)) {
            /* resolved, with nowhere to leave from: the documented definition
             * is the one without a node */
            atomic_fetch_add(&ix->stats->source_gaps, 1);
            out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
            return;
        }
        out->kind = CBM_DOCLINK_EDGE;
        out->target = res.node;
        out->exact = res.exact;
        return;
    }
    out->reason = res.reason;
}

/* ── Incremental scope rules ─────────────────────────────────────── */

/* Scope record fields (0-based, the tag is field 0; internal/cbm/
 * doclink_jvm.c): `I kind alias target`, `T .. supers(6) ..` and
 * `M start kind flags(3) owner name(5) tparams sig recv(8) ..`. */
enum {
    JR_F_I_KIND = 1,
    JR_F_I_ALIAS = 2,
    JR_F_T_SUPERS = 6,
    JR_F_M_FLAGS = 3,
    JR_F_M_NAME = 5,
    JR_F_M_RECV = 8,
    JR_DELTA_IMPORTS = 16, /* changed imports compared by name; more: any name */
};

typedef struct {
    const char *p;
    size_t n;
} jr_span_t;

static jr_span_t jr_delta_field(jr_span_t line, int idx) {
    int f = 0;
    size_t s = 0;
    for (size_t i = 0; i <= line.n; i++) {
        if (i == line.n || line.p[i] == '\t') {
            if (f == idx) {
                return (jr_span_t){.p = line.p + s, .n = i - s};
            }
            f++;
            s = i + SKIP_ONE;
        }
    }
    return (jr_span_t){0};
}

/* The next record; with `imports` the next import record, without it the
 * next record that is none. false at the end. */
static bool jr_delta_next(const char **cursor, jr_span_t *line, bool imports) {
    for (const char *p = *cursor; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t n = nl ? (size_t)(nl - p) : strlen(p);
        const char *next = nl ? nl + SKIP_ONE : p + n;
        if (n > 0 && (p[0] == 'I') == imports) {
            *line = (jr_span_t){.p = p, .n = n};
            *cursor = next;
            return true;
        }
        p = next;
    }
    *cursor = NULL;
    return false;
}

static bool jr_span_eq(jr_span_t a, jr_span_t b) {
    return a.n == b.n && memcmp(a.p, b.p, a.n) == 0;
}

/* The import records of a scope, in order. Returns the count, -1 when memory
 * ran out. */
static int jr_delta_imports(const char *scope, jr_span_t **out) {
    int n = 0;
    jr_span_t line;
    for (const char *p = scope; jr_delta_next(&p, &line, true);) {
        n++;
    }
    *out =
        (jr_span_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)(n ? n : SKIP_ONE) * sizeof(jr_span_t));
    if (!*out) {
        return CBM_NOT_FOUND;
    }
    int w = 0;
    for (const char *p = scope; jr_delta_next(&p, &line, true);) {
        (*out)[w++] = line;
    }
    return n;
}

/* True when a changed import can change what the '|'-joined type names in
 * `names` bind to: an on-demand import can supply any name, a single import
 * the one it is known by. */
static bool jr_delta_names_hit(jr_span_t names, const jr_span_t *changed, int nchanged,
                               bool any_name) {
    for (size_t i = 0; i < names.n;) {
        size_t e = i;
        while (e < names.n && names.p[e] != '|') {
            e++;
        }
        size_t head = i;
        while (head < e && names.p[head] != '.') {
            head++;
        }
        bool named = head > i && !(head - i == SKIP_ONE && names.p[i] == '?');
        if (named && any_name) {
            return true;
        }
        for (int k = 0; named && k < nchanged; k++) {
            jr_span_t alias = jr_delta_field(changed[k], JR_F_I_ALIAS);
            if (alias.n == head - i && memcmp(alias.p, names.p + i, alias.n) == 0) {
                return true;
            }
        }
        i = e + SKIP_ONE;
    }
    return false;
}

/* Imports scope the file itself, which is re-extracted anyway -- except for
 * the names its declaration headers write: a supertype and an extension's
 * receiver are bound in the file's scope, and references in OTHER files
 * resolve through them. A changed import that can re-bind one of those is not
 * repairable file by file. */
static int jr_delta_import_change(const char *stored, const char *fresh) {
    jr_span_t *si = NULL;
    jr_span_t *fi = NULL;
    int ns = jr_delta_imports(stored, &si);
    int nf = jr_delta_imports(fresh, &fi);
    int result = CBM_DOCLINK_DELTA_LOCAL;
    if (ns < 0 || nf < 0) {
        result = CBM_NOT_FOUND;
    } else {
        /* what is left between the common start and the common end changed */
        int a = 0;
        while (a < ns && a < nf && jr_span_eq(si[a], fi[a])) {
            a++;
        }
        int es = ns;
        int ef = nf;
        while (es > a && ef > a && jr_span_eq(si[es - SKIP_ONE], fi[ef - SKIP_ONE])) {
            es--;
            ef--;
        }
        int nchanged = (es - a) + (ef - a);
        bool any_name = nchanged > JR_DELTA_IMPORTS;
        jr_span_t changed[JR_DELTA_IMPORTS];
        int w = 0;
        for (int i = a; !any_name && i < es; i++) {
            changed[w++] = si[i];
        }
        for (int i = a; !any_name && i < ef; i++) {
            changed[w++] = fi[i];
        }
        for (int i = 0; i < w; i++) {
            jr_span_t kind = jr_delta_field(changed[i], JR_F_I_KIND);
            any_name = any_name || (kind.n == SKIP_ONE && (kind.p[0] == 'w' || kind.p[0] == 'n'));
        }
        jr_span_t line;
        for (const char *p = fresh; nchanged > 0 && result == CBM_DOCLINK_DELTA_LOCAL &&
                                    jr_delta_next(&p, &line, false);) {
            int field = line.p[0] == 'T' ? JR_F_T_SUPERS : (line.p[0] == 'M' ? JR_F_M_RECV : 0);
            if (field && jr_delta_names_hit(jr_delta_field(line, field), changed, w, any_name)) {
                result = CBM_DOCLINK_DELTA_GLOBAL;
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, si);
    cbm_free(CBM_MEM_CLASS_OTHER, fi);
    return result;
}

/* Compare a changed file's stored and fresh scopes record by record, in
 * order. Two differences leave every other file's resolution alone:
 *   - a member the fresh scope no longer has (a method, a constructor, a
 *     field, a property, an enum constant): a reference that depended on it
 *     either bound it (an edge into this file) or names it in its unresolved
 *     row, so the name is reported;
 *   - the file's own imports, unless one can re-bind a supertype or an
 *     extension receiver the file declares (jr_delta_import_change).
 * Everything else is GLOBAL: a new or changed record (a type, a member, a
 * signature, the package), a removed type (it may be another type's
 * supertype), a removed record component or constructor property (a record's
 * canonical constructor is matched against them without naming them), a
 * quarantined name or unplaced range that came or went, and a changed order
 * (declarations of one name own their node by order). */
static int jr_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    const char *sp = stored;
    const char *fp = fresh;
    jr_span_t sl = {0};
    jr_span_t fl = {0};
    bool hs = jr_delta_next(&sp, &sl, false);
    bool hf = jr_delta_next(&fp, &fl, false);
    while (hs || hf) {
        if (hs && hf && jr_span_eq(sl, fl)) {
            hs = jr_delta_next(&sp, &sl, false);
            hf = jr_delta_next(&fp, &fl, false);
            continue;
        }
        if (!hs || sl.p[0] != 'M') {
            return CBM_DOCLINK_DELTA_GLOBAL;
        }
        jr_span_t flags = jr_delta_field(sl, JR_F_M_FLAGS);
        jr_span_t name = jr_delta_field(sl, JR_F_M_NAME);
        if (!flags.p || !name.p || name.n == 0) {
            return CBM_NOT_FOUND; /* not a member record this code wrote */
        }
        if (memchr(flags.p, 'p', flags.n)) {
            return CBM_DOCLINK_DELTA_GLOBAL;
        }
        if (!removed || !removed(ud, name.p, name.n)) {
            return CBM_NOT_FOUND;
        }
        hs = jr_delta_next(&sp, &sl, false);
    }
    return jr_delta_import_change(stored, fresh);
}

const cbm_doclink_resolver_t cbm_doclink_jvm_resolver = {
    .langs = {CBM_LANG_JAVA, CBM_LANG_KOTLIN},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_JVM_SCOPE_TAG,
    .build = jr_build,
    .destroy = jr_destroy,
    .resolve = jr_resolve,
    .scope_delta = jr_scope_delta,
};
