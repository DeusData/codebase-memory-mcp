/*
 * include_path.c — the path rule; see include_path.h.
 *
 * Lifted from the parked include-scope branch (round 5, include_scope.c,
 * "2. The include path"): is_join, is_dirname, the listing (is_list,
 * is_list_add, the suffix index), is_pick, is_unique_tail and the import
 * node. The names a listing keeps come from the rows instead of a table per
 * language; evaluated shell and Make paths, run directories and proven
 * absence are not part of this rule.
 */
#include "pipeline/include_path.h"

#include "foundation/arena.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/sha256.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <dirent.h> /* DT_LNK */
#endif

/* A directory entry that is a symbolic link: compat_fs passes the POSIX
 * d_type (DT_LNK is 10 in every POSIX libc) and 0 on Windows. */
#if defined(DT_LNK)
#define IP_DT_LNK DT_LNK
#else
#define IP_DT_LNK 10
#endif

enum { IP_PATH_MAX = 1024 };

/* ── Rows ────────────────────────────────────────────────────────────── */

static const char *const IP_PROTO_SUFFIXES[] = {".proto", NULL};
static const char *const IP_MAKE_SUFFIXES[] = {".mk", ".make", NULL};
static const char *const IP_MAKE_PREFIXES[] = {"Makefile", "makefile", "GNUmakefile", NULL};

static const cbm_include_path_row_t IP_ROWS[] = {
    /* Protobuf: `import "a/b.proto"` names the one repository file that is
     * that path or ends with it, else the file beside the importer. A row
     * that names no `.proto` file is not read as a path. */
    {.lang = CBM_LANG_PROTOBUF,
     .suffixes = IP_PROTO_SUFFIXES,
     .path_needs_suffix = true,
     .base_file_dir = true,
     .whole_path_tail = true},
    /* Make: `include x.mk` names the file from the makefile's directory or
     * the repository root, and only when exactly one of them is a file. */
    {.lang = CBM_LANG_MAKEFILE,
     .suffixes = IP_MAKE_SUFFIXES,
     .prefixes = IP_MAKE_PREFIXES,
     .base_file_dir = true,
     .base_root = true,
     .exactly_one = true},
};

const cbm_include_path_row_t *cbm_include_path_row(CBMLanguage lang) {
    for (size_t i = 0; i < sizeof(IP_ROWS) / sizeof(IP_ROWS[0]); i++) {
        if (IP_ROWS[i].lang == lang) {
            return &IP_ROWS[i];
        }
    }
    return NULL;
}

static bool ip_has_suffix(const char *name, const char *suffix) {
    size_t n = strlen(name);
    size_t s = strlen(suffix);
    return n > s && strcmp(name + n - s, suffix) == 0;
}

static bool ip_row_names(const cbm_include_path_row_t *row, const char *name) {
    for (const char *const *s = row->suffixes; s && *s; s++) {
        if (ip_has_suffix(name, *s)) {
            return true;
        }
    }
    for (const char *const *p = row->prefixes; p && *p; p++) {
        if (strncmp(name, *p, strlen(*p)) == 0) {
            return true;
        }
    }
    return false;
}

bool cbm_include_path_listed_name(const char *name) {
    for (size_t i = 0; name && i < sizeof(IP_ROWS) / sizeof(IP_ROWS[0]); i++) {
        if (ip_row_names(&IP_ROWS[i], name)) {
            return true;
        }
    }
    return false;
}

/* ── Paths ───────────────────────────────────────────────────────────── */

/* `rel` (./ and ../ allowed) inside repository directory `base` ("" = the
 * root), normalized into out; false when it leaves the repository or does
 * not fit. */
static bool ip_join(char *out, size_t sz, const char *base, const char *rel) {
    size_t n = 0;
    out[0] = '\0';
    const char *parts[2] = {base, rel};
    for (int k = 0; k < 2; k++) {
        for (const char *p = parts[k]; p && *p;) {
            while (*p == '/') {
                p++;
            }
            size_t sl = strcspn(p, "/");
            if (sl == 0) {
                break;
            }
            if (sl == 1 && p[0] == '.') {
                p += sl;
                continue;
            }
            if (sl == 2 && p[0] == '.' && p[1] == '.') {
                if (n == 0) {
                    return false;
                }
                while (n > 0 && out[n - 1] != '/') {
                    n--;
                }
                if (n > 0) {
                    n--;
                }
                out[n] = '\0';
                p += sl;
                continue;
            }
            if (n + sl + 2 > sz) {
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
    }
    return n > 0;
}

/* The directory of rel ("" at the root); false when it does not fit. */
static bool ip_dirname(char *out, size_t sz, const char *rel) {
    const char *slash = strrchr(rel, '/');
    size_t n = slash ? (size_t)(slash - rel) : 0;
    if (n >= sz) {
        return false;
    }
    memcpy(out, rel, n);
    out[n] = '\0';
    return true;
}

/* ── The universe and its listing ────────────────────────────────────── */

typedef struct {
    const char *path; /* the one path with this tail, or the first of several */
    int n;
} ip_tail_t;

struct cbm_include_universe {
    char *root;
    CBMArena arena;
    CBMHashTable *listed; /* rel -> non-NULL */
    CBMHashTable *tails;  /* every path suffix on a '/' boundary -> ip_tail_t */
    const char **paths;   /* listed paths, in listing order */
    size_t npaths;
    size_t cap_paths;
    bool did_list;
    bool complete;
    uint64_t visited;    /* directory entries read by the listing */
    uint64_t tail_steps; /* suffix keys made and looked up */
};

cbm_include_universe_t *cbm_include_universe_new(const char *repo_path) {
    if (!repo_path || !repo_path[0]) {
        return NULL;
    }
    cbm_include_universe_t *u = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*u));
    if (!u) {
        return NULL;
    }
    memset(u, 0, sizeof(*u));
    cbm_arena_init(&u->arena);
    size_t n = strlen(repo_path);
    while (n > 1 && repo_path[n - 1] == '/') {
        n--;
    }
    u->root = cbm_arena_alloc(&u->arena, n + 1);
    if (!u->root) {
        cbm_include_universe_free(u);
        return NULL;
    }
    memcpy(u->root, repo_path, n);
    u->root[n] = '\0';
    return u;
}

void cbm_include_universe_free(cbm_include_universe_t *u) {
    if (!u) {
        return;
    }
    if (u->listed) {
        cbm_ht_free(u->listed);
    }
    if (u->tails) {
        cbm_ht_free(u->tails);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, u->paths);
    cbm_arena_destroy(&u->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, u);
}

uint64_t cbm_include_universe_visited(const cbm_include_universe_t *u) {
    return u ? u->visited : 0;
}

uint64_t cbm_include_universe_tail_steps(const cbm_include_universe_t *u) {
    return u ? u->tail_steps : 0;
}

/* One listed path: into the set, the order, and the suffix index. */
static bool ip_list_add(cbm_include_universe_t *u, const char *rel) {
    if (u->npaths >= u->cap_paths) {
        size_t ncap = u->cap_paths ? u->cap_paths * PAIR_LEN : CBM_SZ_256;
        const char **np = cbm_alloc(CBM_MEM_CLASS_OTHER, ncap * sizeof(*np));
        if (!np) {
            return false;
        }
        if (u->npaths) {
            memcpy((void *)np, (const void *)u->paths, u->npaths * sizeof(*np));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)u->paths);
        u->paths = np;
        u->cap_paths = ncap;
    }
    u->paths[u->npaths++] = rel;
    cbm_ht_set(u->listed, rel, (void *)rel);
    for (const char *p = rel; p;) {
        u->tail_steps++;
        ip_tail_t *e = cbm_ht_get(u->tails, p);
        if (!e) {
            e = cbm_arena_alloc(&u->arena, sizeof(*e));
            if (!e) {
                return false;
            }
            e->path = rel;
            e->n = 0;
            cbm_ht_set(u->tails, p, e); /* p points into rel: arena-owned */
        }
        e->n++;
        const char *slash = strchr(p, '/');
        p = slash ? slash + SKIP_ONE : NULL;
    }
    return true;
}

/* Push dir onto the walk's stack (heap, grown by doubling). */
static bool ip_push_dir(char ***stack, int *top, int *cap, char *dir) {
    if (*top >= *cap) {
        int ncap = *cap * PAIR_LEN;
        char **ns = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * sizeof(*ns));
        if (!ns) {
            return false;
        }
        memcpy((void *)ns, (const void *)*stack, (size_t)*top * sizeof(*ns));
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)*stack);
        *stack = ns;
        *cap = ncap;
    }
    (*stack)[(*top)++] = dir;
    return true;
}

/* One directory of the walk: its listed files kept, its subdirectories
 * pushed; false when the listing cannot be whole. */
static bool ip_list_dir(cbm_include_universe_t *u, const char *dir, char ***stack, int *top,
                        int *cap) {
    char abs[IP_PATH_MAX * PAIR_LEN];
    if (snprintf(abs, sizeof(abs), "%s%s%s", u->root, dir[0] ? "/" : "", dir) >= (int)sizeof(abs)) {
        return false;
    }
    cbm_dir_t *d = cbm_opendir(abs);
    if (!d) {
        return false;
    }
    bool ok = true;
    cbm_dirent_t *ent;
    while (ok && (ent = cbm_readdir(d)) != NULL) {
        const char *name = ent->name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0 || strcmp(name, ".git") == 0) {
            continue;
        }
        u->visited++;
        /* a symbolic link is not entered: it may leave the repository, and
         * what it points at inside is listed anyway */
        if (ent->d_type == IP_DT_LNK || (!ent->is_dir && !cbm_include_path_listed_name(name))) {
            continue;
        }
        char *rel = dir[0] ? cbm_arena_sprintf(&u->arena, "%s/%s", dir, name)
                           : cbm_arena_strdup(&u->arena, name);
        if (!rel || strlen(rel) >= IP_PATH_MAX) {
            ok = false; /* a path the rule cannot hold: the listing is not whole */
            break;
        }
        if (!ent->is_dir) {
            ok = ip_list_add(u, rel);
            continue;
        }
        char child[IP_PATH_MAX * PAIR_LEN];
        cbm_path_info_t info;
        if (snprintf(child, sizeof(child), "%s/%s", abs, name) >= (int)sizeof(child) ||
            cbm_path_info_utf8(child, &info) != CBM_PATH_INFO_OK) {
            ok = false;
            break;
        }
        if (!info.is_symlink) { /* a junction (Windows) is not entered either */
            ok = ip_push_dir(stack, top, cap, rel);
        }
    }
    cbm_closedir(d);
    return ok;
}

/* The listing: an explicit stack of directories, each read once. */
static void ip_list(cbm_include_universe_t *u) {
    u->did_list = true;
    u->listed = cbm_ht_create(CBM_SZ_4K);
    u->tails = cbm_ht_create(CBM_SZ_4K);
    if (!u->listed || !u->tails) {
        return;
    }
    int cap = CBM_SZ_64;
    int top = 0;
    char **stack = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)cap * sizeof(*stack));
    bool ok = stack != NULL;
    if (ok) {
        stack[top++] = cbm_arena_strdup(&u->arena, "");
        ok = stack[0] != NULL;
    }
    while (ok && top > 0) {
        const char *dir = stack[--top];
        ok = ip_list_dir(u, dir, &stack, &top, &cap);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)stack);
    u->complete = ok;
    char b[CBM_SZ_32];
    snprintf(b, sizeof(b), "%llu", (unsigned long long)u->visited);
    if (!ok) {
        cbm_log_warn("include_path.listing_incomplete", "visited", b, "effect",
                     "answers_needing_it_abstain");
    } else {
        cbm_log_info("include_path.listing", "visited", b);
    }
}

static bool ip_listing(cbm_include_universe_t *u) {
    if (!u->did_list) {
        ip_list(u);
    }
    return u->complete;
}

enum { IP_EXISTS_NO = 0, IP_EXISTS_YES = 1, IP_EXISTS_UNKNOWN = 2 };

/* A file of the run (a File node), or a listed one; unknown when the listing
 * a listed name needs cannot complete. */
static int ip_exists(cbm_include_universe_t *u, const cbm_gbuf_t *graph, const char *project,
                     const char *rel) {
    if (graph && project && cbm_pipeline_file_node(graph, project, rel)) {
        return IP_EXISTS_YES;
    }
    const char *base = strrchr(rel, '/');
    base = base ? base + SKIP_ONE : rel;
    if (!cbm_include_path_listed_name(base)) {
        return IP_EXISTS_NO; /* not a file of the run, and named like nothing the listing keeps */
    }
    if (!ip_listing(u)) {
        return IP_EXISTS_UNKNOWN;
    }
    return cbm_ht_get(u->listed, rel) ? IP_EXISTS_YES : IP_EXISTS_NO;
}

/* Of the candidates, the one that names a file; NULL when none does, when
 * (exactly one required) several do, or when existence is unknown for a
 * candidate that decides the answer. */
static const char *ip_pick(cbm_include_universe_t *u, const cbm_gbuf_t *graph, const char *project,
                           char cands[][IP_PATH_MAX], int n, bool exactly_one, char *buf,
                           size_t buf_sz) {
    const char *hit = NULL;
    for (int i = 0; i < n; i++) {
        bool dup = false;
        for (int j = 0; j < i; j++) {
            dup = dup || strcmp(cands[i], cands[j]) == 0;
        }
        if (dup) {
            continue;
        }
        int ex = ip_exists(u, graph, project, cands[i]);
        if (ex == IP_EXISTS_UNKNOWN) {
            return NULL;
        }
        if (ex == IP_EXISTS_NO) {
            continue;
        }
        if (!exactly_one) {
            hit = cands[i];
            break;
        }
        if (hit) {
            return NULL;
        }
        hit = cands[i];
    }
    if (!hit || strlen(hit) >= buf_sz) {
        return NULL;
    }
    memcpy(buf, hit, strlen(hit) + SKIP_ONE);
    return buf;
}

/* The one listed file whose repository path is `key` or ends with "/" + key. */
static const char *ip_unique_tail(cbm_include_universe_t *u, const cbm_include_path_row_t *row,
                                  const char *key, char *buf, size_t buf_sz) {
    const char *base = strrchr(key, '/');
    base = base ? base + SKIP_ONE : key;
    if (!base[0] || !ip_row_names(row, base) || !ip_listing(u)) {
        return NULL; /* a name the listing does not keep, or no whole listing */
    }
    u->tail_steps++;
    const ip_tail_t *e = cbm_ht_get(u->tails, key);
    if (!e || e->n != SKIP_ONE || strlen(e->path) >= buf_sz) {
        return NULL; /* none, or two files end with it */
    }
    memcpy(buf, e->path, strlen(e->path) + SKIP_ONE);
    return buf;
}

cbm_include_path_verdict_t cbm_include_path_resolve(cbm_include_universe_t *u,
                                                    const cbm_gbuf_t *graph, const char *project,
                                                    const char *rel, CBMLanguage lang,
                                                    const CBMImport *imp, char *buf,
                                                    size_t buf_sz) {
    const cbm_include_path_row_t *row = cbm_include_path_row(lang);
    if (!row || !imp || !imp->module_path || !imp->module_path[0] || !rel) {
        return CBM_INCLUDE_PATH_NOT_A_PATH;
    }
    const char *p = imp->module_path;
    if (row->path_needs_suffix) {
        bool named = false;
        for (const char *const *s = row->suffixes; s && *s; s++) {
            named = named || ip_has_suffix(p, *s);
        }
        if (!named) {
            return CBM_INCLUDE_PATH_NOT_A_PATH;
        }
    }
    char dir[IP_PATH_MAX];
    if (!u || p[0] == '/' || strpbrk(p, "$`") || !ip_dirname(dir, sizeof(dir), rel)) {
        return CBM_INCLUDE_PATH_NONE; /* a path the rule cannot read */
    }
    if (row->whole_path_tail) {
        char key[IP_PATH_MAX];
        if (ip_join(key, sizeof(key), "", p) && ip_unique_tail(u, row, key, buf, buf_sz)) {
            return CBM_INCLUDE_PATH_FILE;
        }
    }
    char cands[2][IP_PATH_MAX];
    int n = 0;
    if (row->base_file_dir && ip_join(cands[n], sizeof(cands[n]), dir, p)) {
        n++;
    }
    if (row->base_root && ip_join(cands[n], sizeof(cands[n]), "", p)) {
        n++;
    }
    return ip_pick(u, graph, project, cands, n, row->exactly_one, buf, buf_sz)
               ? CBM_INCLUDE_PATH_FILE
               : CBM_INCLUDE_PATH_NONE;
}

/* The file's Module node (named by its path) when it has one: the node a
 * module-path import named before; else its File node. */
static const cbm_gbuf_node_t *ip_file_target(const cbm_pipeline_ctx_t *ctx, const char *path) {
    const cbm_gbuf_node_t **hits = NULL;
    int n = 0;
    if (cbm_gbuf_find_by_name(ctx->gbuf, path, &hits, &n) == 0 && hits) {
        for (int i = 0; i < n; i++) {
            if (hits[i] && hits[i]->label && strcmp(hits[i]->label, "Module") == 0 &&
                hits[i]->file_path && strcmp(hits[i]->file_path, path) == 0) {
                return hits[i];
            }
        }
    }
    return cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path);
}

bool cbm_include_path_decides(const cbm_pipeline_ctx_t *ctx, const char *rel, CBMLanguage lang,
                              const CBMImport *imp, const cbm_gbuf_node_t **out) {
    *out = NULL;
    if (!ctx || !ctx->include_universe || !cbm_include_path_row(lang)) {
        return false;
    }
    char buf[IP_PATH_MAX];
    cbm_include_path_verdict_t v = cbm_include_path_resolve(
        ctx->include_universe, ctx->gbuf, ctx->project_name, rel, lang, imp, buf, sizeof(buf));
    if (v != CBM_INCLUDE_PATH_FILE) {
        return false; /* not a path, or no file: main's strategies decide */
    }
    *out = ip_file_target(ctx, buf);
    return *out != NULL;
}

static int ip_strcmp_ptr(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

int cbm_include_path_listing_digest(const char *repo_path, char out[CBM_SHA256_HEX_LEN + 1]) {
    cbm_include_universe_t *u = cbm_include_universe_new(repo_path);
    if (!u) {
        return CBM_NOT_FOUND;
    }
    ip_list(u);
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    int rc = 0;
    if (!u->complete) {
        static const char marker[] = "include-listing-incomplete-v1";
        cbm_sha256_update(&sha, marker, sizeof(marker) - SKIP_ONE);
    } else {
        const char **sorted =
            u->npaths ? cbm_alloc(CBM_MEM_CLASS_OTHER, u->npaths * sizeof(*sorted)) : NULL;
        if (u->npaths && !sorted) {
            rc = CBM_NOT_FOUND;
        }
        for (size_t i = 0; sorted && i < u->npaths; i++) {
            sorted[i] = u->paths[i];
        }
        if (sorted && u->npaths > SKIP_ONE) {
            qsort((void *)sorted, u->npaths, sizeof(*sorted), ip_strcmp_ptr);
        }
        for (size_t i = 0; sorted && i < u->npaths; i++) {
            cbm_sha256_update(&sha, sorted[i], strlen(sorted[i]) + SKIP_ONE);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, (void *)sorted);
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * PAIR_LEN] = hex[digest[i] >> 4];
        out[i * PAIR_LEN + SKIP_ONE] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
    cbm_include_universe_free(u);
    return rc;
}
