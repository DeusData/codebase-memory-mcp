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

enum { IP_PATH_MAX = 1024, IP_UP_LEN = 3 /* "../" */ };

/* ── Rows ────────────────────────────────────────────────────────────── */

static const char *const IP_PROTO_SUFFIXES[] = {".proto", NULL};
static const char *const IP_MAKE_SUFFIXES[] = {".mk", ".make", NULL};
static const char *const IP_MAKE_PREFIXES[] = {"Makefile", "makefile", "GNUmakefile", NULL};

static const cbm_include_path_row_t IP_ROWS[] = {
    /* Protobuf: `import "a/b.proto"` names the one indexed file that is that
     * path or ends with it. When several do, the file beside the importer is
     * only a guess (protoc reads the one under its -I root). A row that names
     * no `.proto` file, or a path protoc refuses, is not read as a path. */
    {.lang = CBM_LANG_PROTOBUF,
     .suffixes = IP_PROTO_SUFFIXES,
     .path_needs_suffix = true,
     .virtual_path = true,
     .base_file_dir = true,
     .whole_path_tail = true},
    /* Make: `include x.mk` names the file from the makefile's directory or
     * the repository root, and only when exactly one of them is a file. Below
     * the root that is a guess: GNU make reads a relative include from the
     * directory it runs in, which the index does not know. */
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

/* ── The universe: the run's indexed files ───────────────────────────── */

typedef struct {
    const char *path; /* the one path with this tail, or the first of several */
    int n;
} ip_tail_t;

struct cbm_include_universe {
    CBMArena arena;
    CBMHashTable *listed; /* rel -> non-NULL */
    CBMHashTable *tails;  /* every path suffix on a '/' boundary -> ip_tail_t */
    /* the same suffixes in lower case: a path that matches one only when case
     * is ignored is not known absent (a case-insensitive disk opens it) */
    CBMHashTable *ftails;
    /* false when an indexed path did not fit the rule's buffers: then no
     * uniqueness and no absence is proven */
    bool complete;
    uint64_t tail_steps; /* suffix keys made while building */
};

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
    if (u->ftails) {
        cbm_ht_free(u->ftails);
    }
    cbm_arena_destroy(&u->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, u);
}

uint64_t cbm_include_universe_tail_steps(const cbm_include_universe_t *u) {
    return u ? u->tail_steps : 0;
}

/* "a/B.proto" -> "a/b.proto" (ASCII) into out; false when it does not fit. */
static bool ip_fold_into(char *out, size_t sz, const char *p) {
    size_t n = strlen(p);
    if (n >= sz) {
        return false;
    }
    for (size_t i = 0; i <= n; i++) {
        char c = p[i];
        out[i] = (c >= 'A' && c <= 'Z') ? (char)(c - 'A' + 'a') : c;
    }
    return true;
}

/* One indexed path: into the set and the suffix indexes (building only). */
static bool ip_list_add(cbm_include_universe_t *u, const char *path) {
    const char *rel = cbm_arena_strdup(&u->arena, path);
    char fold[IP_PATH_MAX];
    if (!rel || !ip_fold_into(fold, sizeof(fold), rel)) {
        return false;
    }
    const char *folded = cbm_arena_strdup(&u->arena, fold);
    if (!folded) {
        return false;
    }
    for (const char *p = folded; p;) {
        cbm_ht_set(u->ftails, p, (void *)folded);
        const char *slash = strchr(p, '/');
        p = slash ? slash + SKIP_ONE : NULL;
    }
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

cbm_include_universe_t *cbm_include_universe_new(const cbm_file_info_t *files, int count) {
    cbm_include_universe_t *u = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*u));
    if (!u) {
        return NULL;
    }
    memset(u, 0, sizeof(*u));
    cbm_arena_init(&u->arena);
    u->listed = cbm_ht_create(CBM_SZ_4K);
    u->tails = cbm_ht_create(CBM_SZ_4K);
    u->ftails = cbm_ht_create(CBM_SZ_4K);
    if (!u->listed || !u->tails || !u->ftails) {
        cbm_include_universe_free(u);
        return NULL;
    }
    u->complete = true;
    for (int i = 0; files && i < count; i++) {
        const char *rel = files[i].rel_path;
        if (!rel || !rel[0]) {
            continue;
        }
        const char *base = strrchr(rel, '/');
        base = base ? base + SKIP_ONE : rel;
        if (!cbm_include_path_listed_name(base) || cbm_ht_get(u->listed, rel)) {
            continue; /* a name no row's import names, or listed already */
        }
        if (strlen(rel) >= IP_PATH_MAX) {
            u->complete = false; /* a path the rule cannot hold */
            continue;
        }
        if (!ip_list_add(u, rel)) {
            cbm_include_universe_free(u);
            return NULL;
        }
    }
    return u;
}

enum { IP_EXISTS_NO = 0, IP_EXISTS_YES = 1, IP_EXISTS_UNKNOWN = 2 };

/* An indexed file: the listed path itself; unknown when the universe is not
 * whole for a name a row keeps. Never a node found by name: a File node's
 * name joins the path's segments with dots, so a.b.proto and a/b.proto share
 * one (protoc and make open only the file the path names). */
static int ip_exists(const cbm_include_universe_t *u, const char *rel) {
    if (cbm_ht_get(u->listed, rel)) {
        return IP_EXISTS_YES;
    }
    const char *base = strrchr(rel, '/');
    base = base ? base + SKIP_ONE : rel;
    return u->complete || !cbm_include_path_listed_name(base) ? IP_EXISTS_NO : IP_EXISTS_UNKNOWN;
}

/* Of the candidates, the one that names a file; NULL when none does, when
 * (exactly one required) several do, or when existence is unknown for a
 * candidate that decides the answer. */
static const char *ip_pick(const cbm_include_universe_t *u, char cands[][IP_PATH_MAX], int n,
                           bool exactly_one, char *buf, size_t buf_sz) {
    const char *hit = NULL;
    for (int i = 0; i < n; i++) {
        bool dup = false;
        for (int j = 0; j < i; j++) {
            dup = dup || strcmp(cands[i], cands[j]) == 0;
        }
        if (dup) {
            continue;
        }
        int ex = ip_exists(u, cands[i]);
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

/* The one indexed file whose repository path is `key` or ends with "/" + key. */
static const char *ip_unique_tail(const cbm_include_universe_t *u,
                                  const cbm_include_path_row_t *row, const char *key, char *buf,
                                  size_t buf_sz) {
    const char *base = strrchr(key, '/');
    base = base ? base + SKIP_ONE : key;
    if (!base[0] || !ip_row_names(row, base) || !u->complete) {
        return NULL; /* a name the universe does not keep, or not whole */
    }
    const ip_tail_t *e = cbm_ht_get(u->tails, key);
    if (!e || e->n != SKIP_ONE || strlen(e->path) >= buf_sz) {
        return NULL; /* none, or two files end with it */
    }
    memcpy(buf, e->path, strlen(e->path) + SKIP_ONE);
    return buf;
}

/* Proven that no indexed file is `key` or ends with "/" + key, also when
 * letter case is ignored. */
static bool ip_absent_tail(const cbm_include_universe_t *u, const cbm_include_path_row_t *row,
                           const char *key) {
    const char *base = strrchr(key, '/');
    base = base ? base + SKIP_ONE : key;
    char folded[IP_PATH_MAX];
    if (!base[0] || !ip_row_names(row, base) || !u->complete ||
        !ip_fold_into(folded, sizeof(folded), key)) {
        return false;
    }
    return !cbm_ht_get(u->tails, key) && !cbm_ht_get(u->ftails, folded);
}

/* A path protoc accepts as a virtual path: no `.` or `..` segment, no empty
 * segment (`//`), no backslash. */
static bool ip_virtual_ok(const char *p) {
    if (strchr(p, '\\')) {
        return false;
    }
    for (const char *s = p; *s;) {
        size_t sl = strcspn(s, "/");
        if (sl == 0 || (sl == SKIP_ONE && s[0] == '.') ||
            (sl == PAIR_LEN && s[0] == '.' && s[1] == '.')) {
            return false;
        }
        s += sl;
        if (*s == '/') {
            s++;
            if (!*s) {
                return false; /* a trailing slash: an empty segment */
            }
        }
    }
    return true;
}

cbm_include_path_verdict_t cbm_include_path_resolve(const cbm_include_universe_t *u,
                                                    const cbm_gbuf_t *graph, const char *project,
                                                    const char *rel, CBMLanguage lang,
                                                    const CBMImport *imp, char *buf,
                                                    size_t buf_sz) {
    const cbm_include_path_row_t *row = cbm_include_path_row(lang);
    if (!row || !imp || !imp->module_path || !imp->module_path[0] || !rel) {
        return CBM_INCLUDE_PATH_NOT_A_PATH;
    }
    const char *p = imp->module_path;
    (void)graph; /* existence is the listed path, never a node found by name */
    (void)project;
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
    if (!u || p[0] == '/' || strpbrk(p, "$`") || (row->virtual_path && !ip_virtual_ok(p)) ||
        !ip_dirname(dir, sizeof(dir), rel)) {
        return CBM_INCLUDE_PATH_NONE; /* a path the rule cannot read */
    }
    /* The tail key: the path after its leading `./` and `../` segments (a
     * Make path climbing out of its directory, from any directory make runs
     * in, still names a file that ends with the rest). */
    const char *kp = p;
    while (strncmp(kp, "./", PAIR_LEN) == 0 || strncmp(kp, "../", IP_UP_LEN) == 0) {
        kp += kp[1] == '.' ? IP_UP_LEN : PAIR_LEN;
    }
    char key[IP_PATH_MAX];
    bool keyed = ip_join(key, sizeof(key), "", kp);
    if (row->whole_path_tail && keyed && ip_unique_tail(u, row, key, buf, buf_sz)) {
        return CBM_INCLUDE_PATH_FILE;
    }
    char cands[2][IP_PATH_MAX];
    int n = 0;
    if (row->base_file_dir && ip_join(cands[n], sizeof(cands[n]), dir, p)) {
        n++;
    }
    if (row->base_root && ip_join(cands[n], sizeof(cands[n]), "", p)) {
        n++;
    }
    if (ip_pick(u, cands, n, row->exactly_one, buf, buf_sz)) {
        /* A tail row gets here only when several indexed files end with the
         * path: the one beside the importer is an assumption about the include
         * root. Below the root, a base is an assumption about the directory
         * the tool runs in. Only a makefile at the root decides. */
        return row->whole_path_tail || dir[0] ? CBM_INCLUDE_PATH_GUESS : CBM_INCLUDE_PATH_FILE;
    }
    /* Proof of "no file": no indexed file is the path or ends with it, so no
     * include root or run directory inside the index can supply one (absence
     * at the bases alone would not do: another directory may). */
    return keyed && ip_absent_tail(u, row, key) ? CBM_INCLUDE_PATH_NO_FILE : CBM_INCLUDE_PATH_NONE;
}

/* The file's Module node (named by its path) when it has one: the node a
 * module-path import named before; else its File node. Only a node of that
 * exact file (a node found by name may be another file's); NULL when the file
 * has neither. */
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
    const cbm_gbuf_node_t *f = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path);
    return f && f->file_path && strcmp(f->file_path, path) == 0 ? f : NULL;
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
    if (v == CBM_INCLUDE_PATH_FILE) {
        /* the statement names this repository file: its node, or no edge when
         * the graph has none (a file in a skipped directory) */
        *out = ip_file_target(ctx, buf);
        return true;
    }
    /* proven to name no repository file: neither a file nor a symbol of the
     * repository can be its target. Otherwise (not a path, several files,
     * unknown) main's strategies decide. */
    return v == CBM_INCLUDE_PATH_NO_FILE;
}

static int ip_strcmp_ptr(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

int cbm_include_path_inventory_digest(const cbm_file_info_t *files, int count,
                                      char out[CBM_SHA256_HEX_LEN + 1]) {
    const char **sorted =
        count > 0 ? cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)count * sizeof(*sorted)) : NULL;
    if (count > 0 && !sorted) {
        return CBM_NOT_FOUND;
    }
    size_t n = 0;
    for (int i = 0; i < count; i++) {
        const char *rel = files[i].rel_path;
        const char *base = rel ? strrchr(rel, '/') : NULL;
        base = base ? base + SKIP_ONE : rel;
        if (rel && rel[0] && cbm_include_path_listed_name(base)) {
            sorted[n++] = rel;
        }
    }
    if (n > SKIP_ONE) {
        qsort((void *)sorted, n, sizeof(*sorted), ip_strcmp_ptr);
    }
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    static const char version[] = "include-inventory-v1";
    cbm_sha256_update(&sha, version, sizeof(version));
    for (size_t i = 0; i < n; i++) {
        cbm_sha256_update(&sha, sorted[i], strlen(sorted[i]) + SKIP_ONE);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)sorted);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * PAIR_LEN] = hex[digest[i] >> 4];
        out[i * PAIR_LEN + SKIP_ONE] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
    return 0;
}
