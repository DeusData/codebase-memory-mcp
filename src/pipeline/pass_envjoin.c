/*
 * pass_envjoin.c — F1: which program reads the variables a container sets.
 *
 * P4.5 gives each variable a container sets an EnvVar of its own
 * (`<owner QN>#env/<container | ->/<KEY>`); a program's readers point at the
 * global EnvVar of the name (CONFIGURES {strategy:env_access}). The same name
 * is read by many programs, so the join connects a setter only to the
 * readers inside its container's own program -- the files its image is built
 * from:
 *   EnvVar -CONFIGURES {strategy:env_read, dockerfile}-> reader
 * The image: a K8s container's literal image (USAGE {ref:image, container})
 * built from exactly one Dockerfile (Image -DEPENDS_ON {via:build}->); a
 * compose service's own build edge, else its image's; a Dockerfile's own ENV.
 * Its files: the Dockerfile Module's image_files (pass_docker.c) less the
 * image_ignore patterns, never a test file. Measured on 17 corpora: 80
 * pairs, 80 correct; a join through the global name was in the program for
 * 23 % of the pairs.
 *
 * Every run recomputes every such edge. An incremental repair holds only the
 * changed files' nodes and edges: the unchanged files' inputs come from the
 * store (cbm_envjoin_base_t, read before the K8s pass), and the patch replaces
 * the stored edges wholesale.
 */
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* ── Docker's path patterns ─────────────────────────────────────────── */

/* `[...]` at p against c; *end past the class. A malformed class matches
 * nothing. */
static bool ej_class(const char *p, char c, const char **end) {
    const char *q = p + SKIP_ONE;
    bool neg = *q == '^' || *q == '!';
    q += neg;
    bool hit = false;
    for (bool first = true; *q && (first || *q != ']'); first = false) {
        char lo = *q == '\\' && q[SKIP_ONE] ? *++q : *q;
        q++;
        char hi = lo;
        if (q[0] == '-' && q[SKIP_ONE] && q[SKIP_ONE] != ']') {
            q++;
            hi = *q == '\\' && q[SKIP_ONE] ? *++q : *q;
            q++;
        }
        hit = hit || (c >= lo && c <= hi);
    }
    if (*q != ']') {
        *end = NULL;
        return false;
    }
    *end = q + SKIP_ONE;
    return hit != neg;
}

/* Docker's pattern syntax over a whole repository path: `**` any run (`**`
 * followed by `/`: whole segments only), `*` a run inside one segment, `?`
 * one character of a segment, `[...]` a class, `\` an escape. */
static bool ej_match(const char *p, const char *s) {
    while (*p) {
        if (p[0] == '*' && p[SKIP_ONE] == '*') {
            p += PAIR_LEN;
            bool segs = *p == '/';
            p += segs;
            for (const char *t = s;; t++) {
                if ((!segs || t == s || t[-SKIP_ONE] == '/') && ej_match(p, t)) {
                    return true;
                }
                if (!*t) {
                    return false;
                }
            }
        }
        if (*p == '*') {
            p++;
            for (const char *t = s;; t++) {
                if (ej_match(p, t)) {
                    return true;
                }
                if (!*t || *t == '/') {
                    return false;
                }
            }
        }
        if (!*s || (*s == '/' && (*p == '?' || *p == '['))) {
            return false;
        }
        if (*p == '[') {
            const char *end = NULL;
            if (!ej_class(p, *s, &end) || !end) {
                return false;
            }
            p = end;
            s++;
            continue;
        }
        if (*p == '\\' && p[SKIP_ONE]) {
            p++;
        }
        if (*p != '?' && *p != *s) {
            return false;
        }
        p++;
        s++;
    }
    return *s == '\0';
}

/* Pattern `p` matches path `f` or a directory above it (a matched directory
 * holds all of its files). */
static bool ej_match_up(const char *p, const char *f) {
    if (ej_match(p, f)) {
        return true;
    }
    char buf[CBM_SZ_4K];
    for (const char *s = strchr(f, '/'); s; s = strchr(s + SKIP_ONE, '/')) {
        size_t n = (size_t)(s - f);
        if (n >= sizeof(buf)) {
            return false;
        }
        memcpy(buf, f, n);
        buf[n] = '\0';
        if (ej_match(p, buf)) {
            return true;
        }
    }
    return false;
}

/* ── The image's files ──────────────────────────────────────────────── */

typedef struct {
    const char *dockerfile; /* the Dockerfile's path */
    const char **files;     /* repository paths / patterns the image copies */
    int nfiles;
    const char **ignore; /* ignore patterns, "!" = an exception */
    int nignore;
} ej_set_t;

static const char **ej_strings(CBMArena *a, yyjson_val *arr, int *n) {
    *n = 0;
    size_t cnt = yyjson_is_arr(arr) ? yyjson_arr_size(arr) : 0;
    const char **out = cnt ? cbm_arena_alloc(a, cnt * sizeof(*out)) : NULL;
    yyjson_arr_iter it;
    yyjson_val *v;
    for (yyjson_arr_iter_init(arr, &it); out && (v = yyjson_arr_iter_next(&it));) {
        const char *s = yyjson_get_str(v);
        if (s) {
            out[(*n)++] = cbm_arena_strdup(a, s);
        }
    }
    return out;
}

/* A Dockerfile Module's image set, parsed from its properties; NULL: none. */
static ej_set_t *ej_set_parse(CBMArena *a, const char *path, const char *props) {
    if (!props || !strstr(props, "\"image_files\"")) {
        return NULL;
    }
    yyjson_doc *doc = yyjson_read(props, strlen(props), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    ej_set_t *s = root ? cbm_arena_alloc(a, sizeof(*s)) : NULL;
    if (s) {
        *s = (ej_set_t){.dockerfile = cbm_arena_strdup(a, path ? path : "")};
        s->files = ej_strings(a, yyjson_obj_get(root, "image_files"), &s->nfiles);
        s->ignore = ej_strings(a, yyjson_obj_get(root, "image_ignore"), &s->nignore);
    }
    if (doc) {
        yyjson_doc_free(doc);
    }
    return s && s->nfiles > 0 ? s : NULL;
}

static bool ej_in_image(const ej_set_t *s, const char *f) {
    bool in = false;
    for (int i = 0; i < s->nfiles && !in; i++) {
        const char *e = s->files[i];
        if (strpbrk(e, "*?[")) {
            in = ej_match_up(e, f);
        } else {
            size_t el = strlen(e);
            in = el == 0 || strcmp(f, e) == 0 || (strncmp(f, e, el) == 0 && f[el] == '/');
        }
    }
    bool ignored = false;
    for (int i = 0; in && i < s->nignore; i++) {
        bool neg = s->ignore[i][0] == '!';
        if (ej_match_up(s->ignore[i] + neg, f)) {
            ignored = !neg;
        }
    }
    return in && !ignored;
}

bool cbm_envjoin_props_hold(const char *props, const char *path) {
    CBMArena a;
    cbm_arena_init_lazy(&a, CBM_SZ_1K);
    ej_set_t *s = ej_set_parse(&a, "", props);
    bool hit = false;
    if (s) {
        ej_set_t files = *s;
        files.nignore = 0;
        hit = ej_in_image(&files, path);
    }
    cbm_arena_destroy(&a);
    return hit;
}

/* ── The join ───────────────────────────────────────────────────────── */

typedef struct {
    int64_t node;
    const char *file;
} ej_reader_t;

typedef struct {
    int64_t image;
    const char *container; /* NULL: any (compose) */
} ej_img_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena a;
    CBMHashTable *sets;     /* Dockerfile Module id -> ej_set_t */
    CBMHashTable *image_df; /* Image id -> Dockerfile Module id, "" when not one */
    CBMHashTable *owner_df; /* compose service id -> its own build's Dockerfile id */
    CBMHashTable *readers;  /* key -> ej_reader_t[] (count in nreaders) */
    CBMHashTable *nreaders; /* key -> (intptr_t) count */
    CBMHashTable *imgs;     /* owner id -> ej_img_t[] (count in nimgs) */
    CBMHashTable *nimgs;    /* owner id -> (intptr_t) count */
    CBMHashTable *done;     /* "setter:reader" */
    int edges;
} ej_t;

/* Appends `item` to the array table[key] (its count in counts[key]). */
static void ej_push(CBMArena *a, CBMHashTable *table, CBMHashTable *counts, const char *key,
                    const void *item, size_t size) {
    intptr_t n = (intptr_t)cbm_ht_get(counts, key);
    const char *old = cbm_ht_get(table, key);
    char *v = cbm_arena_alloc(a, (size_t)(n + SKIP_ONE) * size);
    const char *k = cbm_arena_strdup(a, key);
    if (!v || !k) {
        return;
    }
    if (n) {
        memcpy(v, old, (size_t)n * size);
    }
    memcpy(v + (size_t)n * size, item, size);
    cbm_ht_set(table, k, v);
    cbm_ht_set(counts, k, (void *)(n + SKIP_ONE));
}

static const char *ej_id(ej_t *j, int64_t id) {
    return cbm_arena_sprintf(&j->a, "%lld", (long long)id);
}

static void ej_add_set(ej_t *j, int64_t node, const char *path, const char *props) {
    const char *k = ej_id(j, node);
    if (!k || cbm_ht_get(j->sets, k)) {
        return;
    }
    ej_set_t *s = ej_set_parse(&j->a, path, props);
    if (s) {
        cbm_ht_set(j->sets, k, s);
    }
}

static void ej_add_img(ej_t *j, int64_t owner, int64_t image, const char *props) {
    const char *c = props ? strstr(props, "\"container\":\"") : NULL;
    ej_img_t im = {image, NULL};
    if (c) {
        c += strlen("\"container\":\"");
        const char *e = strchr(c, '"');
        im.container = e ? cbm_arena_strndup(&j->a, c, (size_t)(e - c)) : NULL;
    }
    const char *k = ej_id(j, owner);
    if (k) {
        ej_push(&j->a, j->imgs, j->nimgs, k, &im, sizeof(im));
    }
}

static void ej_add_reader(ej_t *j, const char *key, int64_t node, const char *file) {
    if (!key || !file || !file[0] || cbm_is_test_path(file)) {
        return;
    }
    ej_reader_t r = {node, cbm_arena_strdup(&j->a, file)};
    if (r.file) {
        ej_push(&j->a, j->readers, j->nreaders, key, &r, sizeof(r));
    }
}

/* Build edges: an Image's Dockerfile (when exactly one), a compose service's
 * own build. */
static void ej_index_builds(ej_t *j) {
    const cbm_gbuf_edge_t **es = NULL;
    int n = 0;
    if (cbm_gbuf_find_edges_by_type(j->ctx->gbuf, "DEPENDS_ON", &es, &n) != 0) {
        return;
    }
    for (int i = 0; i < n; i++) {
        const char *p = es[i]->properties_json;
        if (!p || !strstr(p, "\"via\":\"build\"")) {
            continue;
        }
        const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(j->ctx->gbuf, es[i]->source_id);
        if (!src || !src->label) {
            continue;
        }
        CBMHashTable *t = strcmp(src->label, "Image") == 0      ? j->image_df
                          : strcmp(src->label, "Resource") == 0 ? j->owner_df
                                                                : NULL;
        const char *k = t ? ej_id(j, src->id) : NULL;
        const char *df = k ? ej_id(j, es[i]->target_id) : NULL;
        if (!df) {
            continue;
        }
        const char *have = cbm_ht_get(t, k);
        cbm_ht_set(t, k, (void *)(have && strcmp(have, df) != 0 ? "" : df));
    }
}

static void ej_index_gbuf(ej_t *j) {
    const cbm_gbuf_node_t **mods = NULL;
    int nm = 0;
    if (cbm_gbuf_find_by_label(j->ctx->gbuf, "Module", &mods, &nm) == 0) {
        for (int i = 0; i < nm; i++) {
            ej_add_set(j, mods[i]->id, mods[i]->file_path, mods[i]->properties_json);
        }
    }
    const cbm_gbuf_edge_t **es = NULL;
    int n = 0;
    if (cbm_gbuf_find_edges_by_type(j->ctx->gbuf, "USAGE", &es, &n) == 0) {
        for (int i = 0; i < n; i++) {
            const char *p = es[i]->properties_json;
            if (p && strstr(p, "\"ref\":\"image\"")) {
                ej_add_img(j, es[i]->source_id, es[i]->target_id, p);
            }
        }
    }
    if (cbm_gbuf_find_edges_by_type(j->ctx->gbuf, "CONFIGURES", &es, &n) == 0) {
        for (int i = 0; i < n; i++) {
            const char *p = es[i]->properties_json;
            if (!p || !strstr(p, "\"strategy\":\"env_access\"")) {
                continue;
            }
            const cbm_gbuf_node_t *src = cbm_gbuf_find_by_id(j->ctx->gbuf, es[i]->source_id);
            const cbm_gbuf_node_t *env = cbm_gbuf_find_by_id(j->ctx->gbuf, es[i]->target_id);
            if (src && env) {
                ej_add_reader(j, env->name, src->id, src->file_path);
            }
        }
    }
    ej_index_builds(j);
}

static void ej_index_base(ej_t *j, const cbm_envjoin_base_t *b) {
    for (int i = 0; b && i < b->set_count; i++) {
        ej_add_set(j, b->sets[i].node, b->sets[i].path, b->sets[i].props);
    }
    for (int i = 0; b && i < b->img_count; i++) {
        ej_add_img(j, b->imgs[i].owner, b->imgs[i].image, b->imgs[i].props);
    }
    for (int i = 0; b && i < b->reader_count; i++) {
        ej_add_reader(j, b->readers[i].key, b->readers[i].reader, b->readers[i].file);
    }
}

static void ej_edge(ej_t *j, int64_t setter, const ej_reader_t *r, const ej_set_t *s) {
    const char *k = cbm_arena_sprintf(&j->a, "%lld:%lld", (long long)setter, (long long)r->node);
    if (!k || cbm_ht_get(j->done, k) || setter == r->node) {
        return;
    }
    cbm_ht_set(j->done, k, (void *)k);
    char esc[CBM_SZ_4K];
    cbm_json_escape(esc, (int)sizeof(esc), s->dockerfile);
    char props[CBM_SZ_4K + CBM_SZ_64];
    snprintf(props, sizeof(props), "{\"strategy\":\"env_read\",\"dockerfile\":\"%s\"}", esc);
    if (cbm_gbuf_insert_edge(j->ctx->gbuf, setter, r->node, "CONFIGURES", props) > 0) {
        j->edges++;
    }
}

/* Join one setter with the readers of its key in Dockerfile `df`'s image. */
static void ej_join_df(ej_t *j, int64_t setter, const char *key, const char *df) {
    const ej_set_t *s = df && df[0] ? cbm_ht_get(j->sets, df) : NULL;
    const ej_reader_t *rs = s ? cbm_ht_get(j->readers, key) : NULL;
    intptr_t n = rs ? (intptr_t)cbm_ht_get(j->nreaders, key) : 0;
    for (intptr_t i = 0; i < n; i++) {
        if (ej_in_image(s, rs[i].file)) {
            ej_edge(j, setter, &rs[i], s);
        }
    }
}

/* A scoped EnvVar: its owner, container and key from the QN. */
static void ej_join_setter(ej_t *j, const cbm_gbuf_node_t *ev) {
    const char *mark = strstr(ev->qualified_name, "#env/");
    const char *slash = mark ? strrchr(mark, '/') : NULL;
    if (!mark || !slash || slash < mark + strlen("#env/")) {
        return;
    }
    const char *owner_qn =
        cbm_arena_strndup(&j->a, ev->qualified_name, (size_t)(mark - ev->qualified_name));
    const char *cstart = mark + strlen("#env/");
    const char *container = cbm_arena_strndup(&j->a, cstart, (size_t)(slash - cstart));
    const char *key = slash + SKIP_ONE;
    const cbm_gbuf_node_t *owner = owner_qn ? cbm_gbuf_find_by_qn(j->ctx->gbuf, owner_qn) : NULL;
    if (!owner || !container || !key[0]) {
        return;
    }
    const char *oid = ej_id(j, owner->id);
    if (oid && cbm_ht_get(j->sets, oid)) { /* a Dockerfile's own ENV */
        ej_join_df(j, ev->id, key, oid);
        return;
    }
    const char *own = oid ? cbm_ht_get(j->owner_df, oid) : NULL;
    if (own) { /* a compose service's own build */
        ej_join_df(j, ev->id, key, own);
        return;
    }
    const ej_img_t *ims = oid ? cbm_ht_get(j->imgs, oid) : NULL;
    intptr_t ni = ims ? (intptr_t)cbm_ht_get(j->nimgs, oid) : 0;
    for (intptr_t i = 0; i < ni; i++) {
        if (ims[i].container && strcmp(ims[i].container, container) != 0) {
            continue;
        }
        const char *iid = ej_id(j, ims[i].image);
        ej_join_df(j, ev->id, key, iid ? cbm_ht_get(j->image_df, iid) : NULL);
    }
}

int cbm_pipeline_env_join(cbm_pipeline_ctx_t *ctx) {
    ej_t j = {.ctx = ctx};
    cbm_arena_init_lazy(&j.a, CBM_SZ_4K);
    j.sets = cbm_ht_create(0);
    j.image_df = cbm_ht_create(0);
    j.owner_df = cbm_ht_create(0);
    j.readers = cbm_ht_create(0);
    j.nreaders = cbm_ht_create(0);
    j.imgs = cbm_ht_create(0);
    j.nimgs = cbm_ht_create(0);
    j.done = cbm_ht_create(0);
    const cbm_gbuf_node_t **evs = NULL;
    int n = 0;
    if (j.sets && j.image_df && j.owner_df && j.readers && j.nreaders && j.imgs && j.nimgs &&
        j.done) {
        ej_index_gbuf(&j);
        ej_index_base(&j, ctx->envjoin_base);
        if (cbm_ht_count(j.sets) > 0 &&
            cbm_gbuf_find_by_label(ctx->gbuf, "EnvVar", &evs, &n) == 0) {
            for (int i = 0; i < n; i++) {
                if (evs[i]->qualified_name && strstr(evs[i]->qualified_name, "#env/")) {
                    ej_join_setter(&j, evs[i]);
                }
            }
        }
    }
    CBMHashTable *tables[] = {j.sets,     j.image_df, j.owner_df, j.readers,
                              j.nreaders, j.imgs,     j.nimgs,    j.done};
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++) {
        if (tables[i]) {
            cbm_ht_free(tables[i]);
        }
    }
    cbm_arena_destroy(&j.a);
    if (j.edges > 0) {
        char buf[CBM_SZ_32];
        snprintf(buf, sizeof(buf), "%d", j.edges);
        cbm_log_info("pass.envjoin.edges", "edges", buf);
    }
    return j.edges;
}
