/*
 * pass_docker.c — the container build layer.
 *
 * PHASE 4 P4.1a: what a Dockerfile builds on. Each base it names DEPENDS_ON
 * an Image node -- an edge from the Dockerfile's Module:
 *   FROM <image> [AS <stage>]          {via:from}
 *   COPY --from=<image> ...            {via:copy-from}
 * A stage of the same file is no image (FROM builder, COPY --from=builder or
 * --from=0), nor is `scratch`. ${ARG} references are expanded from the ARG
 * defaults in scope (global ARGs before the first FROM; a stage's own ARG and
 * ENV); a reference without a value leaves the image unknown, and an unknown
 * image is no edge.
 *
 * Image identity is the normalised repository: docker.io for an image
 * without a registry host, library/ for an official one-segment image;
 * tag and digest stay on the edge.
 *
 * PHASE 4 P4.1b: what the container runs. The last stage's ENTRYPOINT (else
 * CMD) SPAWNS -- from the Dockerfile's Module {api:dockerfile}:
 *   the repository file it runs     a script / source copied into the image
 *                                   (an interpreter's script, a path, a CMD
 *                                   file under an inherited ENTRYPOINT)
 *   the Go `main` of the package    a binary `go build -o` made in a stage
 *                                   and the image copied in
 *   a Process                       any other program
 * Container paths map back through COPY: a context source is looked up under
 * the Dockerfile's directory and the repository root -- the one where it is
 * the file (or Go main package) wanted, else nothing; COPY --from=<stage>
 * continues in that stage. WORKDIR makes paths absolute. `sh -c`, a variable
 * program and arguments to an inherited ENTRYPOINT are no edge.
 *
 * PHASE 4 P4.5: the environment a container is given. Each variable a K8s
 * container's env, a compose service's environment or the last stage's ENV
 * (with the stages it is built FROM) sets is an EnvVar of that container
 * (image, for ENV) -- `<owner QN>#env/<container | ->/<name>`, owned by the
 * declaring file -- the owner CONFIGURES {strategy:env_set, source}. The
 * global EnvVar a program's readers share is not joined: the same name is
 * read by other programs (measured: 23 % of such joins in scope). Values are
 * never read into the graph.
 * Run at the end of the K8s pass.
 */
#include "foundation/constants.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { DK_MAX_STAGES = 64, DK_MAX_WORDS = 256, DK_MAX_DEPTH = 8 };

typedef struct dk_copy {
    const char *from;  /* --from= value; NULL: none, or unknown */
    bool has_from;     /* --from given */
    const char **srcs; /* NULL entries: unknown */
    int nsrc;
    const char *dst; /* as written; NULL: unknown */
    const char *workdir;
    struct dk_copy *next; /* the stage's previous COPY */
} dk_copy_t;

typedef struct dk_build {
    const char *out; /* go build -o; NULL: none or unknown */
    const char *pkg; /* NULL: unknown */
    const char *workdir;
    struct dk_build *next; /* the stage's previous build */
} dk_build_t;

typedef struct {
    const char **argv;
    int argc;
    int line;
    const char *workdir;
    bool set;
} dk_entry_t;

typedef struct dk_envkey {
    const char *name;
    int line;
    struct dk_envkey *next; /* the stage's previous ENV key */
} dk_envkey_t;

typedef struct {
    dk_copy_t *copies;  /* newest first */
    dk_build_t *builds; /* newest first */
    dk_envkey_t *envs;  /* newest first */
    int parent;         /* the stage FROM builds on; -1: an image */
} dk_stage_t;

typedef struct {
    CBMHashTable *files;  /* repo path -> File node */
    CBMHashTable *gomain; /* repo dir -> the `main` Function of its package */
} dk_index_t;

typedef struct dk_var {
    const char *name;
    const char *value; /* NULL: declared without a value */
    struct dk_var *next;
} dk_var_t;

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *a;
    const char *src;
    const char *file;
    int64_t module;
    dk_var_t *global;                  /* ARGs before the first FROM */
    dk_var_t *local;                   /* the current stage's ARG / ENV */
    const char *stages[DK_MAX_STAGES]; /* stage names; NULL: unnamed */
    int stage;                         /* index of the current stage; -1 before FROM */
    int edges;
    const char *dir;              /* the Dockerfile's directory ("" at the root) */
    const char *workdir;          /* the current stage's WORKDIR */
    dk_stage_t st[DK_MAX_STAGES]; /* COPY / go build records */
    dk_entry_t entry;             /* the current stage's ENTRYPOINT */
    dk_entry_t cmd;               /* the current stage's CMD */
    const dk_index_t *ix;
} dk_t;

static bool dk_type(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

static const char *dk_text(const dk_t *d, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    char *s = cbm_arena_alloc(d->a, (size_t)(e - b) + SKIP_ONE);
    if (s) {
        memcpy(s, d->src + b, e - b);
        s[e - b] = '\0';
    }
    return s;
}

/* A value as written: quotes of a quoted string removed. */
static const char *dk_value(const dk_t *d, TSNode n) {
    const char *s = dk_text(d, n);
    size_t l = s ? strlen(s) : 0;
    if (l >= PAIR_LEN && (s[0] == '"' || s[0] == '\'') && s[l - SKIP_ONE] == s[0]) {
        char *v = cbm_arena_alloc(d->a, l - SKIP_ONE);
        if (v) {
            memcpy(v, s + SKIP_ONE, l - PAIR_LEN);
            v[l - PAIR_LEN] = '\0';
        }
        return v;
    }
    return s;
}

static const dk_var_t *dk_find(const dk_var_t *v, const char *name, size_t nl) {
    for (; v; v = v->next) {
        if (strlen(v->name) == nl && strncmp(v->name, name, nl) == 0) {
            return v;
        }
    }
    return NULL;
}

static void dk_set(dk_t *d, dk_var_t **list, const char *name, const char *value) {
    dk_var_t *v = cbm_arena_alloc(d->a, sizeof(*v));
    if (v) {
        *v = (dk_var_t){.name = name, .value = value, .next = *list};
        *list = v;
    }
}

/* ASCII case-insensitive equality (stage names, `scratch`). */
static bool dk_ieq(const char *a, const char *b) {
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return false;
        }
    }
    return *a == *b;
}

static bool dk_name_char(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

/* `text` with $X / ${X} / ${X:-d} / ${X-d} expanded from `vars`; NULL when a
 * reference has no value. */
static const char *dk_expand(dk_t *d, const dk_var_t *vars, const char *text) {
    size_t cap = strlen(text) + SKIP_ONE;
    size_t len = 0;
    char *out = cbm_arena_alloc(d->a, cap);
    for (const char *p = text; out && *p;) {
        if (*p != '$') {
            if (len + PAIR_LEN > cap) {
                char *grown = cbm_arena_alloc(d->a, cap * PAIR_LEN);
                if (!grown) {
                    return NULL;
                }
                memcpy(grown, out, len);
                out = grown;
                cap *= PAIR_LEN;
            }
            out[len++] = *p++;
            continue;
        }
        const char *name = p + SKIP_ONE;
        bool braced = *name == '{';
        if (braced) {
            name++;
        }
        size_t nl = 0;
        while (dk_name_char(name[nl])) {
            nl++;
        }
        if (nl == 0) {
            return NULL;
        }
        const char *after = name + nl;
        const char *dflt = NULL;
        size_t dl = 0;
        if (braced) {
            const char *close = strchr(after, '}');
            if (!close) {
                return NULL;
            }
            if ((after[0] == ':' && after[SKIP_ONE] == '-') || after[0] == '-') {
                dflt = after + (after[0] == ':' ? PAIR_LEN : SKIP_ONE);
                dl = (size_t)(close - dflt);
            } else if (after != close) {
                return NULL; /* ${X:+y} and other forms: unknown */
            }
            after = close + SKIP_ONE;
        }
        const dk_var_t *v = dk_find(vars, name, nl);
        const char *val = v && v->value ? v->value : NULL;
        size_t vl = val ? strlen(val) : dl;
        if (!val && !dflt) {
            return NULL;
        }
        const char *from = val ? val : dflt;
        if (len + vl + SKIP_ONE > cap) {
            size_t ncap = (len + vl + SKIP_ONE) * PAIR_LEN;
            char *grown = cbm_arena_alloc(d->a, ncap);
            if (!grown) {
                return NULL;
            }
            memcpy(grown, out, len);
            out = grown;
            cap = ncap;
        }
        memcpy(out + len, from, vl);
        len += vl;
        p = after;
    }
    if (out) {
        out[len] = '\0';
    }
    return out;
}

/* The repository identity of image reference `ref` into out. */
/* Only characters an image reference can hold (name, :tag, @digest) -- a
 * template's placeholder (`<.Values.image>`, `<tpl>`) or an expression is
 * no image. */
static bool dk_ref_chars(const char *ref) {
    for (const char *p = ref; *p; p++) {
        if (!isalnum((unsigned char)*p) && !strchr("._-/:@+", *p)) {
            return false;
        }
    }
    return ref[0] != '\0';
}

static bool dk_normalize(const char *ref, char *out, size_t sz) {
    if (!dk_ref_chars(ref)) {
        return false;
    }
    char r[CBM_SZ_512];
    snprintf(r, sizeof(r), "%s", ref);
    char *at = strchr(r, '@');
    if (at) {
        *at = '\0';
    }
    char *slash = strrchr(r, '/');
    char *colon = strchr(slash ? slash : r, ':');
    if (colon) {
        *colon = '\0';
    }
    if (!r[0] || strchr(r, ' ') || strchr(r, '$')) {
        return false;
    }
    char *first_end = strchr(r, '/');
    if (!first_end) {
        snprintf(out, sz, "docker.io/library/%s", r);
        return true;
    }
    size_t fl = (size_t)(first_end - r);
    bool host = memchr(r, '.', fl) || memchr(r, ':', fl) ||
                (fl == strlen("localhost") && strncmp(r, "localhost", fl) == 0);
    if (!host) {
        snprintf(out, sz, "docker.io/%s", r);
        return true;
    }
    const char *rest = first_end + SKIP_ONE;
    bool hub =
        (fl == strlen("docker.io") && strncmp(r, "docker.io", fl) == 0) ||
        (fl == strlen("index.docker.io") && strncmp(r, "index.docker.io", fl) == 0) ||
        (fl == strlen("registry-1.docker.io") && strncmp(r, "registry-1.docker.io", fl) == 0);
    if (hub) {
        snprintf(out, sz, "docker.io/%s%s", strchr(rest, '/') ? "" : "library/", rest);
    } else {
        snprintf(out, sz, "%.*s/%s", (int)fl, r, rest);
    }
    return true;
}

/* An earlier stage of this file: its name, or a stage index. */
static bool dk_is_stage(const dk_t *d, const char *ref) {
    bool digits = ref[0] != '\0';
    for (const char *p = ref; *p; p++) {
        digits = digits && isdigit((unsigned char)*p);
    }
    if (digits) {
        return true;
    }
    for (int i = 0; i < d->stage && i < DK_MAX_STAGES; i++) {
        if (d->stages[i] && dk_ieq(d->stages[i], ref)) {
            return true;
        }
    }
    return false;
}

/* The stage a --from names: an index, else the last stage of that name. */
static int dk_stage_of(const dk_t *d, const char *ref) {
    if (!ref || !ref[0]) {
        return -1;
    }
    bool digits = true;
    for (const char *p = ref; *p; p++) {
        digits = digits && isdigit((unsigned char)*p);
    }
    if (digits) {
        int k = atoi(ref);
        return k < DK_MAX_STAGES ? k : -1;
    }
    int found = -1;
    for (int i = 0; i <= d->stage && i < DK_MAX_STAGES; i++) {
        if (d->stages[i] && dk_ieq(d->stages[i], ref)) {
            found = i;
        }
    }
    return found;
}

int64_t cbm_pipeline_image_node(cbm_pipeline_ctx_t *ctx, const char *ref) {
    char repo[CBM_SZ_512];
    if (!ref || dk_ieq(ref, "scratch") || !dk_normalize(ref, repo, sizeof(repo))) {
        return 0;
    }
    char qn[CBM_SZ_1K];
    snprintf(qn, sizeof(qn), "__image__%s", repo);
    char esc_repo[CBM_SZ_1K];
    cbm_json_escape(esc_repo, (int)sizeof(esc_repo), repo);
    char node_props[CBM_SZ_2K];
    snprintf(node_props, sizeof(node_props), "{\"repository\":\"%s\"}", esc_repo);
    return cbm_gbuf_upsert_node(ctx->gbuf, "Image", repo, qn, "", 0, 0, node_props);
}

/* An environment variable name: [A-Za-z_][A-Za-z0-9_.-]*. */
static bool dk_env_key(const char *k) {
    if (!k || !(isalpha((unsigned char)k[0]) || k[0] == '_')) {
        return false;
    }
    for (const char *p = k + SKIP_ONE; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_' && *p != '.' && *p != '-') {
            return false;
        }
    }
    return strlen(k) < CBM_SZ_256;
}

int64_t cbm_pipeline_env_setter(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *owner,
                                const char *container, const char *key, const char *source,
                                int line) {
    if (!owner || !owner->qualified_name || !dk_env_key(key) || !source) {
        return 0;
    }
    char qn[CBM_SZ_2K];
    int n = snprintf(qn, sizeof(qn), "%s#env/%s/%s", owner->qualified_name,
                     container ? container : "-", key);
    if (n <= 0 || (size_t)n >= sizeof(qn)) {
        return 0;
    }
    char esc_c[CBM_SZ_512];
    cbm_json_escape(esc_c, (int)sizeof(esc_c), container ? container : "");
    char scope[CBM_SZ_1K];
    if (container) {
        snprintf(scope, sizeof(scope), "\"scope\":\"container\",\"container\":\"%s\"", esc_c);
    } else {
        snprintf(scope, sizeof(scope), "\"scope\":\"image\"");
    }
    char node_props[CBM_SZ_2K];
    snprintf(node_props, sizeof(node_props), "{\"env_key\":\"%s\",%s,\"source\":\"%s\"}", key,
             scope, source);
    int64_t id =
        cbm_gbuf_upsert_node(ctx->gbuf, "EnvVar", key, qn, owner->file_path ? owner->file_path : "",
                             line, line, node_props);
    if (id <= 0 || id == owner->id) {
        return 0;
    }
    char props[CBM_SZ_2K];
    snprintf(props, sizeof(props), "{\"strategy\":\"env_set\",%s,\"source\":\"%s\",\"line\":%d}",
             scope, source, line);
    cbm_gbuf_insert_edge(ctx->gbuf, owner->id, id, "CONFIGURES", props);
    return id;
}

/* DEPENDS_ON from the Dockerfile to the Image of resolved reference `ref`. */
static void dk_link(dk_t *d, const char *ref, const char *via, int line) {
    int64_t img = cbm_pipeline_image_node(d->ctx, ref);
    if (img <= 0) {
        return;
    }
    char esc_ref[CBM_SZ_1K];
    cbm_json_escape(esc_ref, (int)sizeof(esc_ref), ref);
    char props[CBM_SZ_2K];
    snprintf(props, sizeof(props), "{\"via\":\"%s\",\"image\":\"%s\",\"stage\":%d,\"line\":%d}",
             via, esc_ref, d->stage, line);
    cbm_gbuf_insert_edge(d->ctx->gbuf, d->module, img, "DEPENDS_ON", props);
    d->edges++;
}

static int dk_line(TSNode n) {
    return (int)ts_node_start_point(n).row + SKIP_ONE;
}

static void dk_arg(dk_t *d, TSNode ins) {
    TSNode nm = ts_node_child_by_field_name(ins, "name", (uint32_t)strlen("name"));
    TSNode df = ts_node_child_by_field_name(ins, "default", (uint32_t)strlen("default"));
    if (ts_node_is_null(nm)) {
        return;
    }
    const char *name = dk_text(d, nm);
    if (!name) {
        return;
    }
    if (d->stage < 0) {
        dk_set(d, &d->global, name, ts_node_is_null(df) ? NULL : dk_value(d, df));
        return;
    }
    const char *value = NULL;
    if (!ts_node_is_null(df)) {
        const char *raw = dk_value(d, df);
        value = raw ? dk_expand(d, d->local, raw) : NULL;
    } else {
        const dk_var_t *g = dk_find(d->global, name, strlen(name));
        value = g ? g->value : NULL;
    }
    dk_set(d, &d->local, name, value);
}

static void dk_env(dk_t *d, TSNode ins) {
    if (d->stage < 0) {
        return;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        TSNode pair = ts_node_named_child(ins, i);
        if (!dk_type(pair, "env_pair")) {
            continue;
        }
        TSNode nm = ts_node_child_by_field_name(pair, "name", (uint32_t)strlen("name"));
        TSNode vl = ts_node_child_by_field_name(pair, "value", (uint32_t)strlen("value"));
        if (ts_node_is_null(nm)) {
            continue;
        }
        const char *raw = ts_node_is_null(vl) ? NULL : dk_value(d, vl);
        const char *name = dk_text(d, nm);
        dk_set(d, &d->local, name, raw ? dk_expand(d, d->local, raw) : NULL);
        dk_envkey_t *k = d->stage < DK_MAX_STAGES ? cbm_arena_alloc(d->a, sizeof(*k)) : NULL;
        if (k && name) {
            *k = (dk_envkey_t){name, dk_line(pair), d->st[d->stage].envs};
            d->st[d->stage].envs = k;
        }
    }
}

static void dk_from(dk_t *d, TSNode ins) {
    TSNode spec = {0};
    TSNode alias = ts_node_child_by_field_name(ins, "as", (uint32_t)strlen("as"));
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        if (dk_type(ts_node_named_child(ins, i), "image_spec")) {
            spec = ts_node_named_child(ins, i);
        }
    }
    const char *ref = ts_node_is_null(spec) ? NULL : dk_expand(d, d->global, dk_text(d, spec));
    d->stage++;
    d->local = NULL;
    d->workdir = "/";
    d->entry.set = false;
    d->cmd.set = false;
    bool stage = ref && dk_is_stage(d, ref);
    if (ref && !stage) {
        dk_link(d, ref, "from", dk_line(ins));
    }
    if (d->stage < DK_MAX_STAGES) {
        d->st[d->stage].parent = stage ? dk_stage_of(d, ref) : -1;
        d->stages[d->stage] = ts_node_is_null(alias) ? NULL : dk_text(d, alias);
    }
}

/* JSON string `s` (quoted) unquoted; \\ and \" unescaped. */
static const char *dk_json_string(dk_t *d, const char *s) {
    size_t l = s ? strlen(s) : 0;
    if (l < PAIR_LEN || s[0] != '"') {
        return s;
    }
    char *out = cbm_arena_alloc(d->a, l);
    size_t n = 0;
    for (size_t i = SKIP_ONE; out && i + SKIP_ONE < l; i++) {
        if (s[i] == '\\' && i + PAIR_LEN < l) {
            i++;
        }
        out[n++] = s[i];
    }
    if (out) {
        out[n] = '\0';
    }
    return out;
}

/* The strings of a json_string_array node. */
static int dk_json_array(dk_t *d, TSNode arr, const char **out, int max) {
    int n = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(arr) && n < max; i++) {
        TSNode js = ts_node_named_child(arr, i);
        if (dk_type(js, "json_string")) {
            out[n++] = dk_json_string(d, dk_text(d, js));
        }
    }
    return n;
}

static void dk_copy(dk_t *d, TSNode ins) {
    static const char FROM[] = "--from=";
    const char *words[DK_MAX_WORDS];
    int nw = 0;
    dk_copy_t *c = cbm_arena_alloc(d->a, sizeof(*c));
    if (c) {
        *c = (dk_copy_t){.workdir = d->workdir};
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        TSNode p = ts_node_named_child(ins, i);
        if (dk_type(p, "path") && nw < DK_MAX_WORDS) {
            const char *t = dk_text(d, p);
            words[nw++] = t ? dk_expand(d, d->local, t) : NULL;
            continue;
        }
        if (dk_type(p, "json_string_array")) {
            const char *raw[DK_MAX_WORDS];
            int n = dk_json_array(d, p, raw, DK_MAX_WORDS - nw);
            for (int k = 0; k < n; k++) {
                words[nw++] = raw[k] ? dk_expand(d, d->local, raw[k]) : NULL;
            }
            continue;
        }
        if (!dk_type(p, "param")) {
            continue;
        }
        const char *t = dk_text(d, p);
        if (!t || strncmp(t, FROM, sizeof(FROM) - SKIP_ONE) != 0) {
            continue;
        }
        const char *ref = dk_expand(d, d->local, t + sizeof(FROM) - SKIP_ONE);
        if (c) {
            c->has_from = true;
            c->from = ref;
        }
        if (ref && ref[0] && !dk_is_stage(d, ref)) {
            dk_link(d, ref, "copy-from", dk_line(ins));
        }
    }
    if (!c || nw < PAIR_LEN || d->stage >= DK_MAX_STAGES) {
        return;
    }
    c->srcs = cbm_arena_alloc(d->a, (size_t)(nw - SKIP_ONE) * sizeof(*c->srcs));
    if (!c->srcs) {
        return;
    }
    memcpy((void *)c->srcs, words, (size_t)(nw - SKIP_ONE) * sizeof(*c->srcs));
    c->nsrc = nw - SKIP_ONE;
    c->dst = words[nw - SKIP_ONE];
    c->next = d->st[d->stage].copies;
    d->st[d->stage].copies = c;
}

/* ── Paths in the image and in the repository ────────────────────────── */

static const char *dk_join(dk_t *d, const char *a, const char *b) {
    if (!a || !a[0]) {
        return b;
    }
    if (!b || !b[0]) {
        return a;
    }
    size_t al = strlen(a);
    size_t bl = strlen(b);
    char *s = cbm_arena_alloc(d->a, al + bl + PAIR_LEN);
    if (s) {
        memcpy(s, a, al);
        size_t n = al;
        if (a[al - SKIP_ONE] != '/') {
            s[n++] = '/';
        }
        memcpy(s + n, b, bl);
        s[n + bl] = '\0';
    }
    return s;
}

/* `p` with "." and empty segments dropped and ".." applied; an absolute path
 * stays absolute ("/" at most), a relative one climbing out keeps "..". */
static const char *dk_norm(dk_t *d, const char *p) {
    if (!p) {
        return NULL;
    }
    size_t cap = strlen(p) + PAIR_LEN;
    char *out = cbm_arena_alloc(d->a, cap);
    if (!out) {
        return NULL;
    }
    bool abs = p[0] == '/';
    size_t len = 0;
    if (abs) {
        out[len++] = '/';
    }
    size_t base = len;
    for (const char *s = p; *s;) {
        while (*s == '/') {
            s++;
        }
        const char *e = s;
        while (*e && *e != '/') {
            e++;
        }
        size_t sl = (size_t)(e - s);
        if (sl == 0) {
            break;
        }
        bool dot = sl == SKIP_ONE && s[0] == '.';
        bool up = sl == PAIR_LEN && s[0] == '.' && s[SKIP_ONE] == '.';
        if (up) {
            size_t k = len;
            while (k > base && out[k - SKIP_ONE] != '/') {
                k--;
            }
            bool last_up = len - k == PAIR_LEN && out[k] == '.' && out[k + SKIP_ONE] == '.';
            if (len > base && !last_up) {
                len = k > base ? k - SKIP_ONE : base;
            } else if (!abs) {
                if (len > base) {
                    out[len++] = '/';
                }
                out[len++] = '.';
                out[len++] = '.';
            }
        } else if (!dot) {
            if (len > base) {
                out[len++] = '/';
            }
            memcpy(out + len, s, sl);
            len += sl;
        }
        s = e;
    }
    out[len] = '\0';
    return out;
}

/* Container path `p` made absolute against `workdir`, normalised. */
static const char *dk_absolute(dk_t *d, const char *p, const char *workdir) {
    if (!p) {
        return NULL;
    }
    return dk_norm(d, p[0] == '/' ? p : dk_join(d, workdir && workdir[0] ? workdir : "/", p));
}

static const char *dk_base(const char *p) {
    const char *s = strrchr(p, '/');
    return s ? s + SKIP_ONE : p;
}

/* The directory of normalised path `p` ("/" for "/x", "" for "x"). */
static const char *dk_dir(dk_t *d, const char *p) {
    const char *s = strrchr(p, '/');
    if (!s) {
        return "";
    }
    if (s == p) {
        return "/";
    }
    char *out = cbm_arena_alloc(d->a, (size_t)(s - p) + SKIP_ONE);
    if (out) {
        memcpy(out, p, (size_t)(s - p));
        out[s - p] = '\0';
    }
    return out;
}

static bool dk_ends(const char *s, const char *suffix) {
    size_t l = strlen(s);
    size_t k = strlen(suffix);
    return l >= k && strcmp(s + l - k, suffix) == 0;
}

enum { DK_WANT_FILE, DK_WANT_GOMAIN };

/* The repository path of context source `src` (+ `rel`) that is what `want`
 * asks for, the one such path over the context candidates (the Dockerfile's
 * directory, the repository root); NULL when none or two. */
static const char *dk_repo(dk_t *d, const char *src, const char *rel, int want) {
    const char *ctxs[PAIR_LEN] = {d->dir, ""};
    int nctx = d->dir[0] ? PAIR_LEN : SKIP_ONE;
    CBMHashTable *t = want == DK_WANT_FILE ? d->ix->files : d->ix->gomain;
    const char *hit = NULL;
    int hits = 0;
    while (*src == '/') {
        src++;
    }
    for (int i = 0; t && i < nctx; i++) {
        const char *base = dk_join(d, ctxs[i], src);
        const char *p = dk_norm(d, rel && rel[0] ? dk_join(d, base, rel) : base);
        if (!p || strncmp(p, "..", PAIR_LEN) == 0 || !cbm_ht_get(t, p)) {
            continue;
        }
        if (!hit || strcmp(hit, p) != 0) {
            hits++;
            hit = p;
        }
    }
    return hits == SKIP_ONE ? hit : NULL;
}

typedef struct {
    const char *repo;        /* a repository path, or */
    const dk_build_t *build; /* the go build that made the path */
    int build_stage;
} dk_found_t;

/* Where container path `path` of `stage` came from: a repository path that is
 * what `want` asks for, or a go build of the stage; false: nothing known. */
static bool dk_origin(dk_t *d, int stage, const char *path, int want, int depth, dk_found_t *out) {
    if (depth > DK_MAX_DEPTH || stage < 0 || stage >= DK_MAX_STAGES || !path) {
        return false;
    }
    for (const dk_build_t *b = d->st[stage].builds; b; b = b->next) {
        const char *o = b->out ? dk_absolute(d, b->out, b->workdir) : NULL;
        if (o && strcmp(o, path) == 0) {
            out->build = b;
            out->build_stage = stage;
            return true;
        }
    }
    for (const dk_copy_t *c = d->st[stage].copies; c; c = c->next) {
        bool known = c->dst != NULL;
        for (int s = 0; known && s < c->nsrc; s++) {
            known = c->srcs[s] != NULL;
        }
        if (!known) {
            continue;
        }
        const char *dst = dk_absolute(d, c->dst, c->workdir);
        int src_stage = c->has_from ? dk_stage_of(d, c->from) : -1;
        if (!dst || (c->has_from && src_stage < 0)) {
            continue; /* out of an image, or an unknown --from */
        }
        size_t dl = strlen(dst);
        bool into_dir = c->nsrc > SKIP_ONE || dk_ends(c->dst, "/");
        const char *tail = NULL;
        if (strcmp(dst, "/") == 0) {
            tail = strcmp(path, "/") != 0 ? path + SKIP_ONE : NULL;
        } else if (strncmp(path, dst, dl) == 0 && path[dl] == '/') {
            tail = path + dl + SKIP_ONE;
        }
        for (int s = 0; s < c->nsrc; s++) {
            /* the source itself, a path inside a directory source, or a file
             * source copied into the directory dst */
            const char *rels[PAIR_LEN + SKIP_ONE];
            int nc = 0;
            if (strcmp(path, dst) == 0 && !into_dir) {
                rels[nc++] = "";
            }
            if (tail) {
                rels[nc++] = tail;
                const char *sn = dk_norm(d, c->srcs[s]);
                if (sn && strcmp(dk_base(sn), tail) == 0) {
                    rels[nc++] = "";
                }
            }
            for (int k = 0; k < nc; k++) {
                if (src_stage >= 0) {
                    const char *from = rels[k][0] ? dk_join(d, c->srcs[s], rels[k]) : c->srcs[s];
                    if (dk_origin(d, src_stage, dk_absolute(d, from, "/"), want, depth + SKIP_ONE,
                                  out)) {
                        return true;
                    }
                    continue;
                }
                const char *hit = dk_repo(d, c->srcs[s], rels[k], want);
                if (hit) {
                    out->repo = hit;
                    return true;
                }
            }
        }
    }
    return false;
}

static const cbm_gbuf_node_t *dk_gomain_node(const dk_t *d, const char *dir) {
    return dir && d->ix->gomain ? cbm_ht_get(d->ix->gomain, dir) : NULL;
}

/* The `main` of the Go package build `b` (of `stage`) compiled: a file
 * package through that file; a directory through the directory, else through
 * the .go files copied straight into it (all from one repository dir). */
static const cbm_gbuf_node_t *dk_gomain(dk_t *d, const dk_build_t *b, int stage) {
    const char *pkg = b->pkg;
    if (!pkg || strstr(pkg, "...") || strchr(pkg, '$')) {
        return NULL;
    }
    dk_found_t f = {0};
    if (dk_ends(pkg, ".go")) {
        if (!dk_origin(d, stage, dk_absolute(d, pkg, b->workdir), DK_WANT_FILE, 0, &f) || !f.repo) {
            return NULL;
        }
        return dk_gomain_node(d, dk_dir(d, f.repo));
    }
    const char *cdir = dk_absolute(d, pkg, b->workdir);
    if (dk_origin(d, stage, cdir, DK_WANT_GOMAIN, 0, &f) && f.repo) {
        return dk_gomain_node(d, f.repo);
    }
    const char *found = NULL;
    bool many = false;
    for (const dk_copy_t *c = d->st[stage].copies; c && cdir; c = c->next) {
        if (c->has_from || !c->dst || c->nsrc != SKIP_ONE || !c->srcs[0] ||
            !dk_ends(c->srcs[0], ".go") || dk_ends(c->dst, "/")) {
            continue;
        }
        const char *into = dk_absolute(d, c->dst, c->workdir);
        if (!into || strcmp(dk_dir(d, into), cdir) != 0) {
            continue;
        }
        const char *hit = dk_repo(d, c->srcs[0], "", DK_WANT_FILE);
        const char *hd = hit ? dk_dir(d, hit) : NULL;
        if (hd) {
            many = many || (found && strcmp(found, hd) != 0);
            found = hd;
        }
    }
    return found && !many ? dk_gomain_node(d, found) : NULL;
}

/* The words of shell text [p, e): split at blanks outside quotes, quotes
 * and backslash escapes removed (a backslash-newline is a blank). */
static int dk_words(dk_t *d, const char *p, const char *e, const char **out, int max) {
    int n = 0;
    while (p < e && n < max) {
        while (p < e && (isspace((unsigned char)*p) ||
                         (*p == '\\' && p + SKIP_ONE < e && p[SKIP_ONE] == '\n'))) {
            p += *p == '\\' ? PAIR_LEN : SKIP_ONE;
        }
        if (p >= e) {
            break;
        }
        char *w = cbm_arena_alloc(d->a, (size_t)(e - p) + SKIP_ONE);
        if (!w) {
            return n;
        }
        size_t len = 0;
        char quote = 0;
        while (p < e && (quote || !isspace((unsigned char)*p))) {
            char c = *p++;
            if (quote) {
                if (c == quote) {
                    quote = 0;
                } else if (c == '\\' && quote == '"' && p < e &&
                           (*p == '"' || *p == '\\' || *p == '$' || *p == '`')) {
                    w[len++] = *p++;
                } else {
                    w[len++] = c;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '\\' && p < e) {
                if (*p == '\n') {
                    p++;
                    break;
                }
                w[len++] = *p++;
            } else {
                w[len++] = c;
            }
        }
        w[len] = '\0';
        out[n++] = w;
    }
    return n;
}

/* `go build` flags that take the next word as their value (go help build). */
static bool dk_go_value_flag(const char *f) {
    static const char *const FLAGS[] = {
        "-o",       "-ldflags", "-gcflags", "-asmflags", "-gccgoflags", "-tags",
        "-mod",     "-modfile", "-p",       "-pkgdir",   "-buildmode",  "-installsuffix",
        "-overlay", "-pgo",     "-C",       "-toolexec", "-compiler",   NULL};
    for (int i = 0; FLAGS[i]; i++) {
        if (strcmp(f, FLAGS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Each `go build [-o OUT] [PKG]` of one command list; OUT and PKG see the
 * stage's ARG / ENV. */
static void dk_go_builds(dk_t *d, const char **w, int n) {
    for (int j = 0; j + SKIP_ONE < n; j++) {
        if (strcmp(w[j], "go") != 0 || strcmp(w[j + SKIP_ONE], "build") != 0) {
            continue;
        }
        const char *out = NULL;
        const char *pkg = NULL;
        for (int k = j + PAIR_LEN; k < n; k++) {
            const char *a = w[k];
            if (strcmp(a, "-o") == 0 && k + SKIP_ONE < n) {
                out = w[++k];
            } else if (strncmp(a, "-o=", strlen("-o=")) == 0) {
                out = a + strlen("-o=");
            } else if (a[0] == '-') {
                k += dk_go_value_flag(a) ? SKIP_ONE : 0;
            } else if (!pkg) {
                pkg = a;
            }
        }
        dk_build_t *b = cbm_arena_alloc(d->a, sizeof(*b));
        if (!b) {
            return;
        }
        *b = (dk_build_t){.out = out ? dk_expand(d, d->local, out) : NULL,
                          .pkg = dk_expand(d, d->local, pkg ? pkg : "."),
                          .workdir = d->workdir,
                          .next = d->st[d->stage].builds};
        d->st[d->stage].builds = b;
    }
}

static void dk_run(dk_t *d, TSNode ins) {
    if (d->stage >= DK_MAX_STAGES) {
        return;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        TSNode sc = ts_node_named_child(ins, i);
        if (!dk_type(sc, "shell_command")) {
            continue;
        }
        /* command lists split at && || ; (as written, quotes or not) */
        const char *p = d->src + ts_node_start_byte(sc);
        const char *end = d->src + ts_node_end_byte(sc);
        while (p < end) {
            const char *q = p;
            while (q < end &&
                   !(q + SKIP_ONE < end && ((q[0] == '&' && q[SKIP_ONE] == '&') ||
                                            (q[0] == '|' && q[SKIP_ONE] == '|'))) &&
                   *q != ';') {
                q++;
            }
            const char *w[DK_MAX_WORDS];
            int n = dk_words(d, p, q, w, DK_MAX_WORDS);
            dk_go_builds(d, w, n);
            p = q < end ? q + (*q == ';' ? SKIP_ONE : PAIR_LEN) : end;
        }
    }
}

static void dk_workdir(dk_t *d, TSNode ins) {
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        TSNode pn = ts_node_named_child(ins, i);
        if (!dk_type(pn, "path")) {
            continue;
        }
        const char *raw = dk_text(d, pn);
        const char *v = raw ? dk_expand(d, d->local, raw) : NULL;
        const char *w = v ? v : raw;
        if (w) {
            d->workdir = dk_absolute(d, w, d->workdir);
        }
        return;
    }
}

/* ENTRYPOINT / CMD of the current stage: its argv, as written. */
static void dk_entry(dk_t *d, TSNode ins, dk_entry_t *e) {
    const char *w[DK_MAX_WORDS];
    int n = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(ins); i++) {
        TSNode c = ts_node_named_child(ins, i);
        if (dk_type(c, "json_string_array")) {
            n = dk_json_array(d, c, w, DK_MAX_WORDS);
        } else if (dk_type(c, "shell_command")) {
            n = dk_words(d, d->src + ts_node_start_byte(c), d->src + ts_node_end_byte(c), w,
                         DK_MAX_WORDS);
        }
    }
    const char **argv = cbm_arena_alloc(d->a, (size_t)(n ? n : SKIP_ONE) * sizeof(*argv));
    if (!argv) {
        return;
    }
    memcpy((void *)argv, w, (size_t)n * sizeof(*argv));
    *e = (dk_entry_t){
        .argv = argv, .argc = n, .line = dk_line(ins), .workdir = d->workdir, .set = true};
}

static bool dk_interpreter(const char *base) {
    static const char *const NAMES[] = {"node", "php",  "ruby", "perl", "bun",
                                        "deno", "bash", "sh",   NULL};
    for (int i = 0; NAMES[i]; i++) {
        if (strcmp(base, NAMES[i]) == 0) {
            return true;
        }
    }
    if (strncmp(base, "python", strlen("python")) != 0) {
        return false;
    }
    for (const char *p = base + strlen("python"); *p; p++) {
        if (!isdigit((unsigned char)*p) && *p != '.') {
            return false;
        }
    }
    return true;
}

/* SPAWNS from the Dockerfile to what its last stage runs. */
static void dk_spawn(dk_t *d) {
    if (d->stage < 0 || d->stage >= DK_MAX_STAGES || (!d->entry.set && !d->cmd.set)) {
        return;
    }
    const dk_entry_t *e = d->entry.set ? &d->entry : &d->cmd;
    const char *argv[DK_MAX_WORDS];
    int argc = 0;
    for (int i = 0; i < e->argc; i++) {
        if (e->argv[i] && e->argv[i][0]) {
            argv[argc++] = e->argv[i];
        }
    }
    if (argc == 0 || strchr(argv[0], '$') || argv[0][0] == '-') {
        return; /* unknown, or arguments to an inherited ENTRYPOINT */
    }
    const char *prog = argv[0];
    const char *base = dk_base(prog);
    if ((strcmp(base, "sh") == 0 || strcmp(base, "bash") == 0) && argc > SKIP_ONE &&
        strcmp(argv[SKIP_ONE], "-c") == 0) {
        return;
    }
    dk_found_t f = {0};
    const char *path = NULL;
    const cbm_gbuf_node_t *main_fn = NULL;
    if (dk_interpreter(base)) {
        const char *script = NULL;
        for (int i = SKIP_ONE; i < argc && !script; i++) {
            script = argv[i][0] != '-' ? argv[i] : NULL;
        }
        if (script && !strchr(script, '$') &&
            dk_origin(d, d->stage, dk_absolute(d, script, e->workdir), DK_WANT_FILE, 0, &f)) {
            path = f.repo;
        }
    } else if (strchr(prog, '/')) {
        if (dk_origin(d, d->stage, dk_absolute(d, prog, e->workdir), DK_WANT_FILE, 0, &f)) {
            path = f.repo;
            main_fn = f.build ? dk_gomain(d, f.build, f.build_stage) : NULL;
        }
    } else if (!d->entry.set &&
               dk_origin(d, d->stage, dk_absolute(d, prog, e->workdir), DK_WANT_FILE, 0, &f)) {
        path = f.repo; /* a CMD file under an inherited ENTRYPOINT (distroless) */
    }
    int64_t tgt = 0;
    if (path) {
        const char *mqn = cbm_fqn_module(d->a, d->ctx->project_name, path);
        const cbm_gbuf_node_t *m = mqn ? cbm_gbuf_find_by_qn(d->ctx->gbuf, mqn) : NULL;
        const cbm_gbuf_node_t *file = m ? NULL : cbm_ht_get(d->ix->files, path);
        tgt = m ? m->id : file ? file->id : 0;
    } else if (main_fn) {
        tgt = main_fn->id;
    } else {
        char esc_base[CBM_SZ_512];
        cbm_json_escape(esc_base, (int)sizeof(esc_base), base);
        char qn[CBM_SZ_512];
        snprintf(qn, sizeof(qn), "__process__%s", base);
        char node_props[CBM_SZ_1K];
        snprintf(node_props, sizeof(node_props), "{\"program\":\"%s\"}", esc_base);
        tgt = cbm_gbuf_upsert_node(d->ctx->gbuf, "Process", base, qn, "", 0, 0, node_props);
    }
    if (tgt <= 0 || tgt == d->module) {
        return;
    }
    char esc_prog[CBM_SZ_1K];
    cbm_json_escape(esc_prog, (int)sizeof(esc_prog), prog);
    char props[CBM_SZ_2K];
    snprintf(props, sizeof(props),
             "{\"api\":\"dockerfile\",\"instruction\":\"%s\",\"program\":\"%s\",\"line\":%d,"
             "\"confidence\":1.00}",
             d->entry.set ? "ENTRYPOINT" : "CMD", esc_prog, e->line);
    cbm_gbuf_insert_edge(d->ctx->gbuf, d->module, tgt, "SPAWNS", props);
    d->edges++;
}

/* The environment the image of the last stage carries: the ENV keys of that
 * stage and of the earlier stages it is built FROM (a key's nearest
 * definition wins) -- image-scoped EnvVars the Dockerfile sets. */
static void dk_envs(dk_t *d) {
    const cbm_gbuf_node_t *mod = cbm_gbuf_find_by_id(d->ctx->gbuf, d->module);
    if (!mod || d->stage < 0 || d->stage >= DK_MAX_STAGES) {
        return;
    }
    CBMHashTable *seen = cbm_ht_create(0);
    int s = d->stage;
    for (int depth = 0; seen && s >= 0 && depth < DK_MAX_STAGES; depth++) {
        for (const dk_envkey_t *k = d->st[s].envs; k; k = k->next) {
            if (!cbm_ht_get(seen, k->name)) {
                cbm_ht_set(seen, k->name, (void *)k);
                if (cbm_pipeline_env_setter(d->ctx, mod, NULL, k->name, "dockerfile", k->line) >
                    0) {
                    d->edges++;
                }
            }
        }
        int p = d->st[s].parent;
        s = p >= 0 && p < s ? p : -1;
    }
    cbm_ht_free(seen);
}

static void dk_file(dk_t *d, TSNode root) {
    for (uint32_t i = 0; i < ts_node_named_child_count(root); i++) {
        TSNode ins = ts_node_named_child(root, i);
        if (dk_type(ins, "arg_instruction")) {
            dk_arg(d, ins);
        } else if (dk_type(ins, "env_instruction")) {
            dk_env(d, ins);
        } else if (dk_type(ins, "from_instruction")) {
            dk_from(d, ins);
        } else if ((dk_type(ins, "copy_instruction") || dk_type(ins, "add_instruction")) &&
                   d->stage >= 0) {
            dk_copy(d, ins);
        } else if (dk_type(ins, "workdir_instruction") && d->stage >= 0) {
            dk_workdir(d, ins);
        } else if (dk_type(ins, "run_instruction") && d->stage >= 0) {
            dk_run(d, ins);
        } else if (dk_type(ins, "entrypoint_instruction") && d->stage >= 0) {
            dk_entry(d, ins, &d->entry);
        } else if (dk_type(ins, "cmd_instruction") && d->stage >= 0) {
            dk_entry(d, ins, &d->cmd);
        }
    }
    dk_spawn(d);
    dk_envs(d);
}

static char *dk_read(const char *path, size_t *len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > cbm_max_file_bytes()) {
        (void)fclose(f);
        return NULL;
    }
    enum { DK_TS_PAD = 16 }; /* tree-sitter's lexer reads past the end */
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + DK_TS_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t got = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    memset(buf + got, 0, DK_TS_PAD);
    *len = got;
    return buf;
}

/* Does the line [b, e) hold a parser directive `# escape=...`? */
static bool dk_escape_directive(const char *s, size_t b, size_t e) {
    size_t i = b + SKIP_ONE;
    while (i < e && (s[i] == ' ' || s[i] == '\t')) {
        i++;
    }
    static const char kw[] = "escape";
    for (size_t k = 0; kw[k]; k++, i++) {
        if (i >= e || tolower((unsigned char)s[i]) != kw[k]) {
            return false;
        }
    }
    while (i < e && (s[i] == ' ' || s[i] == '\t')) {
        i++;
    }
    return i < e && s[i] == '=';
}

/* Docker drops comment lines before it joins continued lines; the grammar
 * ends an instruction at one (kubermatic: `ENV A=1 \` / `# url` / `B=2`).
 * A comment line inside a continuation becomes a bare continuation (`#` ->
 * `\`, the rest blank) -- rows and offsets stay. A file with an escape
 * directive is left as it is. */
static void dk_uncomment(char *s, size_t len) {
    bool cont = false;
    bool top = true; /* still in the leading directives */
    for (size_t i = 0; i < len;) {
        size_t e = i;
        while (e < len && s[e] != '\n') {
            e++;
        }
        size_t b = i;
        while (b < e && (s[b] == ' ' || s[b] == '\t')) {
            b++;
        }
        if (b < e && s[b] == '#') {
            if (top && dk_escape_directive(s, b, e)) {
                return;
            }
            if (cont) {
                s[b] = '\\';
                for (size_t k = b + SKIP_ONE; k < e && s[k] != '\r'; k++) {
                    s[k] = ' ';
                }
            }
        } else if (b < e && s[b] != '\r') {
            size_t t = e;
            while (t > b &&
                   (s[t - SKIP_ONE] == ' ' || s[t - SKIP_ONE] == '\t' || s[t - SKIP_ONE] == '\r')) {
                t--;
            }
            cont = s[t - SKIP_ONE] == '\\';
            top = false;
        } else {
            top = false;
        }
        i = e + SKIP_ONE;
    }
}

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *a;
    const char *file;
    int edges;
} dk_builds_t;

/* An Image a build configuration builds DEPENDS_ON {via:build} the
 * Dockerfile it is built from. */
static void dk_build_site(const cbm_build_site_t *s, void *ud) {
    dk_builds_t *b = ud;
    const char *mqn = cbm_fqn_module(b->a, b->ctx->project_name, s->dockerfile);
    const cbm_gbuf_node_t *mod = mqn ? cbm_gbuf_find_by_qn(b->ctx->gbuf, mqn) : NULL;
    if (!mod || !mod->file_path || strcmp(mod->file_path, s->dockerfile) != 0) {
        return;
    }
    int64_t img = cbm_pipeline_image_node(b->ctx, s->image);
    if (img <= 0) {
        return;
    }
    char esc_file[CBM_SZ_1K];
    char esc_img[CBM_SZ_1K];
    cbm_json_escape(esc_file, (int)sizeof(esc_file), b->file);
    cbm_json_escape(esc_img, (int)sizeof(esc_img), s->image);
    char props[CBM_SZ_4K];
    snprintf(props, sizeof(props),
             "{\"via\":\"build\",\"tool\":\"%s\",\"file\":\"%s\",\"image\":\"%s\",\"line\":%d}",
             s->via, esc_file, esc_img, s->line);
    cbm_gbuf_insert_edge(b->ctx->gbuf, img, mod->id, "DEPENDS_ON", props);
    b->edges++;
}

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    CBMArena *a;
    const char *file;
    const char *base; /* the compose file's module QN */
    int64_t file_node;
    int pass; /* 0: make the service nodes, 1: their edges */
    int edges;
} dk_compose_t;

static const char *dk_compose_qn(const dk_compose_t *c, const char *name) {
    return cbm_arena_sprintf(c->a, "%s::compose/Service/-/%s", c->base, name);
}

static void dk_compose_edge(dk_compose_t *c, int64_t src, int64_t tgt, const char *type,
                            const char *props) {
    if (src > 0 && tgt > 0 && src != tgt) {
        cbm_gbuf_insert_edge(c->ctx->gbuf, src, tgt, type, props);
        c->edges++;
    }
}

/* A compose service: a Resource {ecosystem:compose, kind:Service} the file
 * DEFINES; it DEPENDS_ON the services it depends on / links, USAGE
 * {ref:image} its image and DEPENDS_ON {via:build} the Dockerfile it builds. */
static void dk_compose_service(const cbm_compose_service_t *s, void *ud) {
    dk_compose_t *c = ud;
    const char *qn = dk_compose_qn(c, s->name);
    if (!qn) {
        return;
    }
    if (c->pass == 0) {
        char esc[CBM_SZ_512];
        cbm_json_escape(esc, (int)sizeof(esc), s->name);
        char props[CBM_SZ_1K];
        snprintf(props, sizeof(props),
                 "{\"ecosystem\":\"compose\",\"kind\":\"Service\",\"service\":\"%s\"}", esc);
        int64_t id = cbm_gbuf_upsert_node(c->ctx->gbuf, "Resource",
                                          cbm_arena_sprintf(c->a, "Service/%s", s->name), qn,
                                          c->file, s->line + SKIP_ONE, s->end_line, props);
        dk_compose_edge(c, c->file_node, id, "DEFINES", "{}");
        const cbm_gbuf_node_t *self = id > 0 ? cbm_gbuf_find_by_id(c->ctx->gbuf, id) : NULL;
        for (int i = 0; self && i < s->nenv; i++) {
            if (cbm_pipeline_env_setter(c->ctx, self, s->name, s->env[i], "compose",
                                        s->env_lines[i]) > 0) {
                c->edges++;
            }
        }
        return;
    }
    const cbm_gbuf_node_t *self = cbm_gbuf_find_by_qn(c->ctx->gbuf, qn);
    if (!self) {
        return;
    }
    for (int i = 0; i < s->ndeps; i++) {
        const char *dq = dk_compose_qn(c, s->deps[i]);
        const cbm_gbuf_node_t *dep = dq ? cbm_gbuf_find_by_qn(c->ctx->gbuf, dq) : NULL;
        dk_compose_edge(c, self->id, dep ? dep->id : 0, "DEPENDS_ON", "{\"via\":\"depends_on\"}");
    }
    if (s->image) {
        char esc[CBM_SZ_1K];
        cbm_json_escape(esc, (int)sizeof(esc), s->image);
        char props[CBM_SZ_2K];
        snprintf(props, sizeof(props), "{\"ref\":\"image\",\"image\":\"%s\"}", esc);
        dk_compose_edge(c, self->id, cbm_pipeline_image_node(c->ctx, s->image), "USAGE", props);
    }
    if (s->dockerfile) {
        const char *mqn = cbm_fqn_module(c->a, c->ctx->project_name, s->dockerfile);
        const cbm_gbuf_node_t *mod = mqn ? cbm_gbuf_find_by_qn(c->ctx->gbuf, mqn) : NULL;
        if (mod && mod->file_path && strcmp(mod->file_path, s->dockerfile) == 0) {
            dk_compose_edge(c, self->id, mod->id, "DEPENDS_ON",
                            "{\"via\":\"build\",\"tool\":\"compose\"}");
        }
    }
}

/* The .env of the directory of `rel` under the repository, NULL when none. */
static char *dk_read_dotenv(const cbm_pipeline_ctx_t *ctx, const char *rel) {
    if (!ctx->repo_path) {
        return NULL;
    }
    const char *slash = strrchr(rel, '/');
    char path[CBM_SZ_4K];
    snprintf(path, sizeof(path), "%s/%.*s%s.env", ctx->repo_path, slash ? (int)(slash - rel) : 0,
             rel, slash ? "/" : "");
    size_t len = 0;
    return dk_read(path, &len);
}

/* Every build configuration of the repository -- read from disk through the
 * File nodes, so an incremental run (which recomputes these edges whole)
 * sees the unchanged ones too. */
static int dk_link_builds(cbm_pipeline_ctx_t *ctx, CBMArena *a) {
    const cbm_gbuf_node_t **files = NULL;
    int n = 0;
    if (!ctx->repo_path || cbm_gbuf_find_by_label(ctx->gbuf, "File", &files, &n) != 0) {
        return 0;
    }
    dk_builds_t b = {.ctx = ctx, .a = a};
    for (int i = 0; i < n; i++) {
        const char *rel = files[i]->file_path;
        if (!rel || !cbm_build_path(rel)) {
            continue;
        }
        char path[CBM_SZ_4K];
        snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, rel);
        size_t len = 0;
        char *src = dk_read(path, &len);
        if (!src) {
            continue;
        }
        char *dotenv = dk_read_dotenv(ctx, rel);
        b.file = rel;
        cbm_build_sites(a, rel, src, len, dotenv, dk_build_site, &b);
        dk_compose_t cs = {.ctx = ctx,
                           .a = a,
                           .file = rel,
                           .base = cbm_fqn_compute(a, ctx->project_name, rel, NULL),
                           .file_node = files[i]->id};
        for (cs.pass = 0; cs.base && cs.pass < PAIR_LEN; cs.pass++) {
            cbm_compose_services(a, rel, src, len, dotenv, dk_compose_service, &cs);
        }
        b.edges += cs.edges;
        if (dotenv) {
            cbm_free(CBM_MEM_CLASS_OTHER, dotenv);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    return b.edges;
}

static bool dk_node_before(const cbm_gbuf_node_t *a, const cbm_gbuf_node_t *b) {
    int c = strcmp(a->file_path, b->file_path);
    return c < 0 || (c == 0 && a->start_line < b->start_line);
}

/* The repository's File nodes by path, and the `main` of each Go main
 * package by directory (the first by file and line). */
static void dk_index_build(cbm_pipeline_ctx_t *ctx, CBMArena *a, dk_index_t *ix) {
    ix->files = cbm_ht_create(0);
    ix->gomain = cbm_ht_create(0);
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (ix->files && cbm_gbuf_find_by_label(ctx->gbuf, "File", &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            if (nodes[i]->file_path) {
                cbm_ht_set(ix->files, nodes[i]->file_path, (void *)nodes[i]);
            }
        }
    }
    if (!ix->gomain || cbm_gbuf_find_by_name(ctx->gbuf, "main", &nodes, &n) != 0) {
        return;
    }
    for (int i = 0; i < n; i++) {
        const cbm_gbuf_node_t *fn = nodes[i];
        const char *fp = fn->file_path;
        if (!fp || strcmp(fn->label, "Function") != 0 || !dk_ends(fp, ".go") ||
            dk_ends(fp, "_test.go")) {
            continue;
        }
        const char *slash = strrchr(fp, '/');
        char *dir = cbm_arena_alloc(a, slash ? (size_t)(slash - fp) + SKIP_ONE : SKIP_ONE);
        if (!dir) {
            continue;
        }
        size_t dl = slash ? (size_t)(slash - fp) : 0;
        memcpy(dir, fp, dl);
        dir[dl] = '\0';
        const cbm_gbuf_node_t *have = cbm_ht_get(ix->gomain, dir);
        if (!have || dk_node_before(fn, have)) {
            cbm_ht_set(ix->gomain, dir, (void *)fn);
        }
    }
}

/* ── Image tag sub-identity (P4.2b) ─────────────────────────────── */

_Atomic uint64_t g_dk_hub_edges = 0; /* complexity guard: Image edges a run examines */

/* A plain JSON string property (an image reference holds no escapes). */
static bool dk_prop(const char *props, const char *key, char *out, size_t out_sz) {
    char pat[CBM_SZ_64];
    snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    const char *p = props ? strstr(props, pat) : NULL;
    if (!p) {
        return false;
    }
    p += strlen(pat);
    const char *e = strchr(p, '"');
    if (!e || (size_t)(e - p) >= out_sz || memchr(p, '\\', (size_t)(e - p))) {
        return false;
    }
    memcpy(out, p, (size_t)(e - p));
    out[e - p] = '\0';
    return true;
}

/* The tag of image reference `ref`: after the ':' that follows its last
 * '/' (a registry port is no tag), up to a '@' digest; "" for none. */
static void dk_tag(const char *ref, char *out, size_t sz) {
    out[0] = '\0';
    const char *slash = strrchr(ref, '/');
    const char *colon = strchr(slash ? slash : ref, ':');
    if (colon) {
        snprintf(out, sz, "%.*s", (int)strcspn(colon + SKIP_ONE, "@"), colon + SKIP_ONE);
    }
}

/* The repository of an Image node's name ("<repo>" or "<repo>:<pattern>"). */
static void dk_repo_of(const char *name, char *out, size_t sz) {
    const char *slash = strrchr(name, '/');
    const char *colon = strchr(slash ? slash : name, ':');
    snprintf(out, sz, "%.*s", (int)(colon ? (size_t)(colon - name) : strlen(name)), name);
}

static bool dk_sep(char c) {
    return c == '-' || c == '_' || c == '.';
}

/* Does `comp` occur in `tag` as whole separator-delimited parts? */
static bool dk_tag_has(const char *tag, const char *comp) {
    size_t cl = strlen(comp);
    for (const char *p = strstr(tag, comp); p; p = strstr(p + SKIP_ONE, comp)) {
        if ((p == tag || dk_sep(p[-SKIP_ONE])) && (p[cl] == '\0' || dk_sep(p[cl]))) {
            return true;
        }
    }
    return false;
}

/* An edge of a repository's Images, copied before any edge moves. */
typedef struct {
    const cbm_gbuf_edge_t *edge; /* stable until deleted */
    int64_t src;
    int64_t tgt;
    const char *type;
    const char *props;
    const char *tag;
    int comp; /* its component; -1: none */
} dk_hub_edge_t;

typedef struct {
    dk_hub_edge_t *v;
    int n;
    int cap;
} dk_hub_edges_t;

typedef struct {
    const cbm_gbuf_edge_t **v;
    int n;
    int cap;
} dk_drop_t;

typedef struct {
    int64_t id;
    const char *name;
    const char *repo;
} dk_img_t;

static int dk_img_cmp(const void *x, const void *y) {
    const dk_img_t *a = x;
    const dk_img_t *b = y;
    int c = strcmp(a->repo, b->repo);
    return c ? c : strcmp(a->name, b->name);
}

/* Grow an arena array of `n` items of `size` to hold one more. */
static void *dk_grow(CBMArena *a, void *v, int n, int *cap, size_t size) {
    if (n < *cap) {
        return v;
    }
    int nc = *cap ? *cap * PAIR_LEN : CBM_SZ_16;
    void *g = cbm_arena_alloc(a, (size_t)nc * size);
    if (g && n) {
        memcpy(g, v, (size_t)n * size);
    }
    if (g) {
        *cap = nc;
    }
    return g;
}

static void dk_hub_push(CBMArena *a, dk_hub_edges_t *l, const cbm_gbuf_edge_t *e, const char *tag) {
    dk_hub_edge_t *v = dk_grow(a, l->v, l->n, &l->cap, sizeof(*v));
    if (!v) {
        return;
    }
    l->v = v;
    l->v[l->n++] = (dk_hub_edge_t){e,
                                   e->source_id,
                                   e->target_id,
                                   cbm_arena_strdup(a, e->type),
                                   cbm_arena_strdup(a, e->properties_json),
                                   cbm_arena_strdup(a, tag),
                                   -1};
}

/* Is an edge of `type` with properties `p` a reference to an image --
 * USAGE {ref:image}, or DEPENDS_ON {via:from|copy-from}? */
static bool dk_hub_ref(const char *type, const char *p) {
    char v[CBM_SZ_32];
    if (strcmp(type, "USAGE") == 0) {
        return dk_prop(p, "ref", v, sizeof(v)) && strcmp(v, "image") == 0;
    }
    return dk_prop(p, "via", v, sizeof(v)) &&
           (strcmp(v, "from") == 0 || strcmp(v, "copy-from") == 0);
}

/* The edges of Image `img`: build edges out (DEPENDS_ON {via:build}) and
 * references in, each with the tag of its `image` reference. */
static void dk_hub_collect(cbm_pipeline_ctx_t *ctx, CBMArena *a, int64_t img,
                           dk_hub_edges_t *builds, dk_hub_edges_t *refs) {
    const cbm_gbuf_edge_t **es = NULL;
    int n = 0;
    char ref[CBM_SZ_1K];
    char tag[CBM_SZ_256];
    if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, img, "DEPENDS_ON", &es, &n) == 0) {
        for (int i = 0; i < n; i++) {
            char via[CBM_SZ_32];
            if (!dk_prop(es[i]->properties_json, "via", via, sizeof(via)) ||
                strcmp(via, "build") != 0) {
                continue;
            }
            tag[0] = '\0';
            if (dk_prop(es[i]->properties_json, "image", ref, sizeof(ref))) {
                dk_tag(ref, tag, sizeof(tag));
            }
            dk_hub_push(a, builds, es[i], tag);
        }
        atomic_fetch_add_explicit(&g_dk_hub_edges, (uint64_t)n, memory_order_relaxed);
    }
    static const char *const REF_TYPES[] = {"USAGE", "DEPENDS_ON"};
    for (size_t t = 0; t < sizeof(REF_TYPES) / sizeof(REF_TYPES[0]); t++) {
        if (cbm_gbuf_find_edges_by_target_type(ctx->gbuf, img, REF_TYPES[t], &es, &n) != 0) {
            continue;
        }
        atomic_fetch_add_explicit(&g_dk_hub_edges, (uint64_t)n, memory_order_relaxed);
        for (int i = 0; i < n; i++) {
            if (dk_hub_ref(REF_TYPES[t], es[i]->properties_json) &&
                dk_prop(es[i]->properties_json, "image", ref, sizeof(ref))) {
                dk_tag(ref, tag, sizeof(tag));
                dk_hub_push(a, refs, es[i], tag);
            }
        }
    }
}

/* The number of distinct Dockerfiles the build edges target. */
static int dk_hub_dockerfiles(CBMArena *a, const dk_hub_edges_t *b) {
    CBMHashTable *seen = cbm_ht_create(0);
    int n = 0;
    for (int i = 0; seen && i < b->n; i++) {
        const char *k = cbm_arena_sprintf(a, "%lld", (long long)b->v[i].tgt);
        if (k && !cbm_ht_has(seen, k)) {
            cbm_ht_set(seen, k, (void *)SKIP_ONE);
            n++;
        }
    }
    if (seen) {
        cbm_ht_free(seen);
    }
    return n;
}

/* The common prefix / suffix of the build tags, cut back to a separator and
 * leaving every tag a non-empty middle where it can: *pre, *suf. */
static void dk_hub_affixes(const dk_hub_edges_t *b, size_t *pre, size_t *suf) {
    const char *t0 = b->v[0].tag;
    size_t l0 = strlen(t0);
    size_t p = l0;
    size_t s = l0;
    for (int i = SKIP_ONE; i < b->n; i++) {
        const char *t = b->v[i].tag;
        size_t tl = strlen(t);
        size_t k = 0;
        while (k < p && k < tl && t[k] == t0[k]) {
            k++;
        }
        p = k;
        k = 0;
        while (k < s && k < tl && t[tl - SKIP_ONE - k] == t0[l0 - SKIP_ONE - k]) {
            k++;
        }
        s = k;
    }
    while (p > 0 && !dk_sep(t0[p - SKIP_ONE])) {
        p--;
    }
    while (s > 0 && !dk_sep(t0[l0 - s])) {
        s--;
    }
    for (int i = 0; i < b->n; i++) {
        size_t tl = strlen(b->v[i].tag);
        while (s > 0 && p + s >= tl) {
            for (s--; s > 0 && !dk_sep(t0[l0 - s]); s--) {}
        }
        while (p > 0 && p + s >= tl) {
            for (p--; p > 0 && !dk_sep(t0[p - SKIP_ONE]); p--) {}
        }
    }
    *pre = p;
    *suf = s;
}

/* Components of a repository built from several Dockerfiles: each build
 * tag without the tags' common prefix / suffix (otel's latest-frontend,
 * latest-cart -> frontend, cart). A component built from exactly one
 * Dockerfile, holding no unknown part, is kept (comps[k] non-NULL); each
 * build edge's comp is set. Returns the number of component slots (0:
 * none kept). */
static int dk_hub_components(CBMArena *a, dk_hub_edges_t *b, size_t pre, size_t suf,
                             const char ***comps) {
    const char **names = cbm_arena_alloc(a, (size_t)b->n * sizeof(*names));
    int64_t *dfs = cbm_arena_alloc(a, (size_t)b->n * sizeof(*dfs));
    CBMHashTable *index = cbm_ht_create(0); /* component -> slot + 1 */
    int nc = 0;
    for (int i = 0; names && dfs && index && i < b->n; i++) {
        const char *t = b->v[i].tag;
        size_t tl = strlen(t);
        const char *c = tl > pre + suf ? cbm_arena_strndup(a, t + pre, tl - pre - suf) : NULL;
        if (!c || strpbrk(c, "$*{}()`\"\\")) {
            continue;
        }
        int k = (int)(intptr_t)cbm_ht_get(index, c) - SKIP_ONE;
        if (k < 0) {
            k = nc++;
            names[k] = c;
            dfs[k] = b->v[i].tgt;
            cbm_ht_set(index, c, (void *)(intptr_t)(k + SKIP_ONE));
        } else if (names[k] && dfs[k] != b->v[i].tgt) {
            names[k] = NULL; /* one name, two Dockerfiles: no identity */
        }
        b->v[i].comp = k;
    }
    if (index) {
        cbm_ht_free(index);
    }
    if (!names || !dfs) {
        return 0;
    }
    int kept = 0;
    for (int k = 0; k < nc; k++) {
        kept += names[k] ? SKIP_ONE : 0;
    }
    for (int i = 0; i < b->n; i++) {
        if (b->v[i].comp >= 0 && !names[b->v[i].comp]) {
            b->v[i].comp = -1;
        }
    }
    *comps = names;
    return kept ? nc : 0;
}

/* The component a reference's tag names: the longest kept one occurring in
 * it as whole parts; -1 for none, or for two of that length. */
static int dk_hub_match(const char *tag, const char *const *comps, int nc) {
    int best = -1;
    size_t bl = 0;
    bool tie = false;
    for (int k = 0; k < nc; k++) {
        if (!comps[k] || !dk_tag_has(tag, comps[k])) {
            continue;
        }
        size_t l = strlen(comps[k]);
        if (l > bl) {
            best = k;
            bl = l;
            tie = false;
        } else if (l == bl) {
            tie = true;
        }
    }
    return tie ? -1 : best;
}

/* The Image of a component: `<repo>:<pattern>` (`*-frontend`: the
 * prefix's version masked) {repository, component, tag_pattern}. */
static int64_t dk_hub_image(cbm_pipeline_ctx_t *ctx, const char *repo, const char *comp,
                            char pre_sep, char suf_sep) {
    char head[PAIR_LEN + SKIP_ONE] = {0};
    char tail[PAIR_LEN + SKIP_ONE] = {0};
    if (pre_sep) {
        head[0] = '*';
        head[SKIP_ONE] = pre_sep;
    }
    if (suf_sep) {
        tail[0] = suf_sep;
        tail[SKIP_ONE] = '*';
    }
    char pattern[CBM_SZ_512];
    char name[CBM_SZ_1K];
    char qn[CBM_SZ_2K];
    snprintf(pattern, sizeof(pattern), "%s%s%s", head, comp, tail);
    snprintf(name, sizeof(name), "%s:%s", repo, pattern);
    snprintf(qn, sizeof(qn), "__image__%s", name);
    char esc_r[CBM_SZ_1K];
    char esc_c[CBM_SZ_1K];
    char esc_p[CBM_SZ_1K];
    cbm_json_escape(esc_r, (int)sizeof(esc_r), repo);
    cbm_json_escape(esc_c, (int)sizeof(esc_c), comp);
    cbm_json_escape(esc_p, (int)sizeof(esc_p), pattern);
    char props[CBM_SZ_4K];
    snprintf(props, sizeof(props),
             "{\"repository\":\"%s\",\"component\":\"%s\",\"tag_pattern\":\"%s\"}", esc_r, esc_c,
             esc_p);
    return cbm_gbuf_upsert_node(ctx->gbuf, "Image", name, qn, "", 0, 0, props);
}

/* Move edge `e` to (src, tgt): insert the copy, queue `e` for deletion. */
static int dk_hub_move(cbm_pipeline_ctx_t *ctx, CBMArena *a, const dk_hub_edge_t *e, int64_t src,
                       int64_t tgt, dk_drop_t *drop) {
    if ((src == e->src && tgt == e->tgt) || src <= 0 || tgt <= 0) {
        return 0;
    }
    const cbm_gbuf_edge_t **v = dk_grow(a, (void *)drop->v, drop->n, &drop->cap, sizeof(*v));
    if (!v) {
        return 0;
    }
    drop->v = v;
    drop->v[drop->n++] = e->edge;
    cbm_gbuf_insert_edge(ctx->gbuf, src, tgt, e->type, e->props);
    return SKIP_ONE;
}

/* One repository's Images (imgs[0..n)): split it by component when it is
 * built from several Dockerfiles, else keep (or bring back) everything on
 * the repository Image. Returns the edges moved. */
static int dk_hub_repo(cbm_pipeline_ctx_t *ctx, CBMArena *a, const dk_img_t *imgs, int n,
                       dk_drop_t *drop) {
    dk_hub_edges_t builds = {0};
    dk_hub_edges_t refs = {0};
    bool has_sub = false;
    for (int i = 0; i < n; i++) {
        has_sub = has_sub || strcmp(imgs[i].name, imgs[i].repo) != 0;
        dk_hub_collect(ctx, a, imgs[i].id, &builds, &refs);
    }
    const char **comps = NULL;
    size_t pre = 0;
    size_t suf = 0;
    int nc = 0;
    if (builds.n > SKIP_ONE && dk_hub_dockerfiles(a, &builds) > SKIP_ONE) {
        dk_hub_affixes(&builds, &pre, &suf);
        nc = dk_hub_components(a, &builds, pre, suf, &comps);
    }
    if (nc == 0 && !has_sub) {
        return 0;
    }
    int64_t repo_img = cbm_pipeline_image_node(ctx, imgs[0].repo);
    int64_t *sub = nc ? cbm_arena_alloc(a, (size_t)nc * sizeof(*sub)) : NULL;
    const char *t0 = builds.n ? builds.v[0].tag : "";
    char pre_sep = pre ? t0[pre - SKIP_ONE] : '\0';
    char suf_sep = suf ? t0[strlen(t0) - suf] : '\0';
    for (int k = 0; k < nc && sub; k++) {
        sub[k] = comps[k] ? dk_hub_image(ctx, imgs[0].repo, comps[k], pre_sep, suf_sep) : 0;
    }
    int moved = 0;
    for (int i = 0; i < builds.n; i++) {
        int k = builds.v[i].comp;
        int64_t src = sub && k >= 0 && sub[k] > 0 ? sub[k] : repo_img;
        moved += dk_hub_move(ctx, a, &builds.v[i], src, builds.v[i].tgt, drop);
    }
    for (int i = 0; i < refs.n; i++) {
        int k = sub ? dk_hub_match(refs.v[i].tag, comps, nc) : -1;
        int64_t tgt = k >= 0 && sub[k] > 0 ? sub[k] : repo_img;
        moved += dk_hub_move(ctx, a, &refs.v[i], refs.v[i].src, tgt, drop);
    }
    return moved;
}

/* P4.2b: a repository built from more than one Dockerfile (otel builds
 * every service as ghcr.io/open-telemetry/demo:<version>-<service>) is no
 * single image. Each component of its build tags built from one Dockerfile
 * becomes an Image of its own, which its build edges leave from; every
 * reference (K8s container, compose service, FROM) whose tag names a
 * component uses that Image; all else stays on the repository Image.
 * Recomputed whole each run from the edges in the buffer. */
static int dk_image_hubs(cbm_pipeline_ctx_t *ctx, CBMArena *a) {
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Image", &nodes, &n) != 0 || n == 0) {
        return 0;
    }
    dk_img_t *imgs = cbm_arena_alloc(a, (size_t)n * sizeof(*imgs));
    if (!imgs) {
        return 0;
    }
    for (int i = 0; i < n; i++) {
        char repo[CBM_SZ_1K];
        dk_repo_of(nodes[i]->name, repo, sizeof(repo));
        imgs[i] = (dk_img_t){nodes[i]->id, cbm_arena_strdup(a, nodes[i]->name),
                             cbm_arena_strdup(a, repo)};
    }
    qsort(imgs, (size_t)n, sizeof(*imgs), dk_img_cmp);
    dk_drop_t drop = {0};
    int moved = 0;
    for (int g = 0; g < n;) {
        int h = g + SKIP_ONE;
        while (h < n && strcmp(imgs[h].repo, imgs[g].repo) == 0) {
            h++;
        }
        moved += dk_hub_repo(ctx, a, imgs + g, h - g, &drop);
        g = h;
    }
    cbm_gbuf_delete_edges(ctx->gbuf, drop.v, drop.n);
    return moved;
}

void cbm_pipeline_pass_docker(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                              int file_count) {
    TSParser *parser = NULL;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_SZ_4K);
    dk_index_t ix = {0};
    int edges = 0;
    for (int i = 0; i < file_count; i++) {
        if (files[i].language != CBM_LANG_DOCKERFILE) {
            continue;
        }
        if (!ix.files) {
            dk_index_build(ctx, &arena, &ix);
        }
        const char *mqn = cbm_fqn_module(&arena, ctx->project_name, files[i].rel_path);
        const cbm_gbuf_node_t *mod = mqn ? cbm_gbuf_find_by_qn(ctx->gbuf, mqn) : NULL;
        if (!mod) {
            continue;
        }
        size_t len = 0;
        char *src = dk_read(files[i].path, &len);
        if (!src) {
            continue;
        }
        dk_uncomment(src, len);
        if (!parser) {
            parser = ts_parser_new();
        }
        TSTree *tree = NULL;
        if (parser && ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_DOCKERFILE))) {
            tree = ts_parser_parse_string(parser, NULL, src, (uint32_t)len);
        }
        if (tree) {
            const char *slash = strrchr(files[i].rel_path, '/');
            char *dir = cbm_arena_alloc(
                &arena, slash ? (size_t)(slash - files[i].rel_path) + SKIP_ONE : SKIP_ONE);
            if (dir) {
                size_t dl = slash ? (size_t)(slash - files[i].rel_path) : 0;
                memcpy(dir, files[i].rel_path, dl);
                dir[dl] = '\0';
            }
            dk_t d = {.ctx = ctx,
                      .a = &arena,
                      .src = src,
                      .file = files[i].rel_path,
                      .module = mod->id,
                      .stage = -1,
                      .dir = dir ? dir : "",
                      .workdir = "/",
                      .ix = &ix};
            dk_file(&d, ts_tree_root_node(tree));
            edges += d.edges;
            ts_tree_delete(tree);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, src);
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    if (ix.files) {
        cbm_ht_free(ix.files);
    }
    if (ix.gomain) {
        cbm_ht_free(ix.gomain);
    }
    edges += dk_link_builds(ctx, &arena);
    edges += dk_image_hubs(ctx, &arena);
    cbm_arena_destroy(&arena);
    if (edges > 0) {
        char buf[CBM_SZ_32];
        snprintf(buf, sizeof(buf), "%d", edges);
        cbm_log_info("pass.docker.edges", "edges", buf);
    }
}
