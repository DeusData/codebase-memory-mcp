/*
 * doc_links_adoc.c — AsciiDoc references -> MENTIONS edges, via "asciidoc"
 * (the resolving half of internal/cbm/doc_adoc.c), with the Antora rules the
 * field test's harvester measured (H8: includes 97.6-100 % resolved, ~100 %
 * precise; javadoc attributes 320/320).
 *
 * Antora: a component is the directory of an antora.yml; its pages live under
 * <component>/modules/<module>/pages. Its attributes and collector scans, and
 * a playbook's attributes, come from the YAML scope blobs (doc_adoc.c), so an
 * incremental run resolves as a full one.
 *
 * include::target[...]: references the page left are expanded from the
 * component's attributes, then the playbook's. A resource ID
 * ([[version@]component:]module:]family$path) names
 * <component>/modules/<module>/<family dir>/<path>, or a file a collector scan
 * contributes there; any other target is relative to the including file. Its
 * `lines=` bind the innermost definition holding them; its `tag=` / `tags=`
 * regions -- the included file's own `tag::x[]` .. `end::x[]` comments, read
 * from that indexed file -- likewise; a tag the file does not have binds the
 * file (the document stays a dependent of that file, so it re-resolves when
 * the file changes, on an incremental run as on a full one).
 *
 * {attribute} references whose value is an Antora javadoc link
 * ({javadoc-root}/<module>/<package path>/<Type>.html[#member]) bind the
 * Java or Kotlin type (or its member) that the repository's main sources
 * declare under that package path.
 *
 * Monospace spans: the Markdown resolver's code-span rules (doc_links_md.c).
 */
#include "pipeline/doc_links.h"

#include "discover/discover.h" /* cbm_language_for_filename */
#include "doclink.h"
#include "foundation/arena.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    RA_PATH_CAP = 1024,
    RA_FIELDS = 6,
    RA_EXPAND_PASSES = 10,             /* nested attribute references (H8's depth) */
    RA_TAG_FILE_MAX = 8 * 1024 * 1024, /* an included file read for its tags */
    RA_TAGS = 16,                      /* tags of one include */
    RA_TAG_LEN = 128,                  /* a tag name */
    RA_INIT = 8,
};

typedef struct {
    const char *key;
    const char *value;
} ra_attr_t;

typedef struct {
    const char *dir;
    const char *into;
} ra_scan_t;

typedef struct {
    const char *root; /* the antora.yml's directory ("" for the repository root) */
    const char *name;
    ra_attr_t *attrs;
    int nattrs;
    ra_scan_t *scans;
    int nscans;
} ra_comp_t;

typedef struct {
    const char *project;
    const char *repo; /* the repository on disk: included files are read for their tags */
    void *md;
    const char **run_paths;
    int run_count;
    CBMArena arena;
    ra_comp_t *comps;
    int ncomps;
    ra_attr_t *pattrs; /* playbook attributes, the first definition of a key wins */
    int npattrs;
} ra_index_t;

static void ra_destroy(void *index);

static const char *ra_base(const char *path) {
    const char *s = strrchr(path, '/');
    return s ? s + SKIP_ONE : path;
}

static bool ra_ends(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t k = strlen(suffix);
    return n >= k && strcmp(s + n - k, suffix) == 0;
}

static bool ra_grow(CBMArena *a, void **items, int count, int *cap, size_t size) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : RA_INIT;
    void *grown = cbm_arena_alloc(a, (size_t)ncap * size);
    if (!grown) {
        return false;
    }
    if (count > 0) {
        memcpy(grown, *items, (size_t)count * size);
    }
    *items = grown;
    *cap = ncap;
    return true;
}

/* One YAML scope blob: a component (C) or a playbook (P). */
static bool ra_read_scope(ra_index_t *x, const char *path, const char *scope, int *comp_cap,
                          int *pattr_cap) {
    char *copy = cbm_arena_strdup(&x->arena, scope);
    if (!copy) {
        return false;
    }
    char *save = NULL;
    (void)strtok_r(copy, "\n", &save); /* the tag line */
    char *line = strtok_r(NULL, "\n", &save);
    if (!line || (strcmp(line, "C") != 0 && strcmp(line, "P") != 0)) {
        return true;
    }
    bool component = line[0] == 'C';
    ra_comp_t comp = {0};
    int acap = 0;
    int scap = 0;
    if (component) {
        const char *b = ra_base(path);
        comp.root =
            cbm_arena_strndup(&x->arena, path, b > path ? (size_t)(b - path - SKIP_ONE) : 0);
        comp.name = "";
        if (!comp.root) {
            return false;
        }
    }
    for (line = strtok_r(NULL, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char *tab = strchr(line, '\t');
        if (!tab) {
            continue;
        }
        *tab = '\0';
        char *v = tab + SKIP_ONE;
        if (strcmp(line, "N") == 0 && component) {
            comp.name = v;
        } else if (strcmp(line, "A") == 0) {
            char *t2 = strchr(v, '\t');
            if (!t2) {
                continue;
            }
            *t2 = '\0';
            ra_attr_t at = {v, t2 + SKIP_ONE};
            if (component) {
                if (!ra_grow(&x->arena, (void **)&comp.attrs, comp.nattrs, &acap, sizeof(at))) {
                    return false;
                }
                comp.attrs[comp.nattrs++] = at;
            } else {
                bool seen = false;
                for (int i = 0; i < x->npattrs; i++) {
                    seen = seen || strcmp(x->pattrs[i].key, at.key) == 0;
                }
                if (!seen) {
                    if (!ra_grow(&x->arena, (void **)&x->pattrs, x->npattrs, pattr_cap,
                                 sizeof(at))) {
                        return false;
                    }
                    x->pattrs[x->npattrs++] = at;
                }
            }
        } else if (strcmp(line, "D") == 0 && component) {
            if (!ra_grow(&x->arena, (void **)&comp.scans, comp.nscans, &scap, sizeof(ra_scan_t))) {
                return false;
            }
            comp.scans[comp.nscans++] = (ra_scan_t){v, ""};
        } else if (strcmp(line, "I") == 0 && component && comp.nscans > 0) {
            comp.scans[comp.nscans - 1].into = v;
        }
    }
    if (component) {
        if (!ra_grow(&x->arena, (void **)&x->comps, x->ncomps, comp_cap, sizeof(comp))) {
            return false;
        }
        x->comps[x->ncomps++] = comp;
    }
    return true;
}

static void *ra_build(const cbm_doclink_build_in_t *in) {
    ra_index_t *x = (ra_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*x));
    if (!x) {
        return NULL;
    }
    cbm_arena_init(&x->arena);
    x->project = in->ctx ? in->ctx->project_name : NULL;
    x->repo = in->ctx ? in->ctx->repo_path : NULL;
    x->run_count = in->run_file_count;
    x->run_paths = in->run_file_count > 0
                       ? (const char **)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                                   (size_t)in->run_file_count * sizeof(char *))
                       : NULL;
    x->md = cbm_doclink_md_resolver.build(in);
    if ((in->run_file_count > 0 && !x->run_paths) || !x->md || !x->project) {
        ra_destroy(x);
        return NULL;
    }
    int comp_cap = 0;
    int pattr_cap = 0;
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *f = &in->files[i];
        if (f->run_file >= 0 && f->run_file < x->run_count) {
            x->run_paths[f->run_file] = f->rel_path; /* valid until destroy */
        }
        if (f->scope &&
            strncmp(f->scope, CBM_DOCLINK_ADOC_SCOPE_TAG "\n",
                    strlen(CBM_DOCLINK_ADOC_SCOPE_TAG "\n")) == 0 &&
            !ra_read_scope(x, f->rel_path, f->scope, &comp_cap, &pattr_cap)) {
            ra_destroy(x);
            return NULL;
        }
    }
    return x;
}

static void ra_destroy(void *index) {
    ra_index_t *x = (ra_index_t *)index;
    if (!x) {
        return;
    }
    if (x->md) {
        cbm_doclink_md_resolver.destroy(x->md);
    }
    cbm_arena_destroy(&x->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, x->run_paths);
    cbm_free(CBM_MEM_CLASS_OTHER, x);
}

/* H8 component_of: the deepest component whose modules/ hold the document. */
static const ra_comp_t *ra_component_of(const ra_index_t *x, const char *doc) {
    const ra_comp_t *best = NULL;
    for (int i = 0; i < x->ncomps; i++) {
        const ra_comp_t *c = &x->comps[i];
        char pref[RA_PATH_CAP];
        snprintf(pref, sizeof(pref), "%s%smodules/", c->root, c->root[0] ? "/" : "");
        if (strncmp(doc, pref, strlen(pref)) == 0 &&
            (!best || strlen(c->root) > strlen(best->root))) {
            best = c;
        }
    }
    return best;
}

static const char *ra_attr(const ra_index_t *x, const ra_comp_t *c, const char *key, size_t n) {
    for (int i = 0; c && i < c->nattrs; i++) {
        if (strlen(c->attrs[i].key) == n && memcmp(c->attrs[i].key, key, n) == 0) {
            return c->attrs[i].value;
        }
    }
    for (int i = 0; i < x->npattrs; i++) {
        if (strlen(x->pattrs[i].key) == n && memcmp(x->pattrs[i].key, key, n) == 0) {
            return x->pattrs[i].value;
        }
    }
    return NULL;
}

static bool ra_word(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

/* The text with the component's and the playbook's attribute references
 * substituted, pass after pass. False when one stays undefined or it does
 * not fit. */
static bool ra_expand(const ra_index_t *x, const ra_comp_t *c, const char *s, char *out,
                      size_t cap) {
    char cur[RA_PATH_CAP];
    char nxt[RA_PATH_CAP];
    if (snprintf(cur, sizeof(cur), "%s", s) >= (int)sizeof(cur)) {
        return false;
    }
    bool undefined = false;
    for (int pass = 0; pass < RA_EXPAND_PASSES; pass++) {
        size_t w = 0;
        bool changed = false;
        undefined = false;
        for (size_t i = 0; cur[i]; i++) {
            const char *v = NULL;
            size_t e = i + SKIP_ONE;
            if (cur[i] == '{' && (i == 0 || cur[i - SKIP_ONE] != '\\') && ra_word(cur[e])) {
                while (cur[e] && (ra_word(cur[e]) || cur[e] == '-')) {
                    e++;
                }
                if (cur[e] == '}') {
                    v = ra_attr(x, c, cur + i + SKIP_ONE, e - i - SKIP_ONE);
                    undefined = undefined || !v;
                }
            }
            if (v) {
                size_t vl = strlen(v);
                if (w + vl >= sizeof(nxt)) {
                    return false;
                }
                memcpy(nxt + w, v, vl);
                w += vl;
                i = e;
                changed = true;
                continue;
            }
            if (w + SKIP_ONE >= sizeof(nxt)) {
                return false;
            }
            nxt[w++] = cur[i];
        }
        nxt[w] = '\0';
        memcpy(cur, nxt, w + SKIP_ONE);
        if (!changed) {
            break;
        }
    }
    if (undefined || snprintf(out, cap, "%s", cur) >= (int)cap) {
        return false;
    }
    return true;
}

/* os.path.normpath(base/p); false when it leaves the repository. */
static bool ra_norm(const char *base, const char *p, char *out, size_t cap) {
    char tmp[RA_PATH_CAP * PAIR_LEN];
    snprintf(tmp, sizeof(tmp), "%s%s%s", base, base[0] && p[0] ? "/" : "", p);
    size_t w = 0;
    out[0] = '\0';
    for (char *s = tmp; *s;) {
        char *e = strchr(s, '/');
        size_t l = e ? (size_t)(e - s) : strlen(s);
        if (l == PAIR_LEN && s[0] == '.' && s[1] == '.') {
            if (w == 0) {
                return false;
            }
            while (w > 0 && out[w - SKIP_ONE] != '/') {
                w--;
            }
            if (w > 0) {
                w--;
            }
            out[w] = '\0';
        } else if (l > 0 && !(l == SKIP_ONE && s[0] == '.')) {
            if (w + l + PAIR_LEN > cap) {
                return false;
            }
            if (w > 0) {
                out[w++] = '/';
            }
            memcpy(out + w, s, l);
            w += l;
            out[w] = '\0';
        }
        if (!e) {
            break;
        }
        s = e + SKIP_ONE;
    }
    return true;
}

static void ra_unresolved(cbm_doclink_outcome_t *out, int reason) {
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = reason;
}

static void ra_edge(cbm_doclink_outcome_t *out, const cbm_gbuf_node_t *n, uint32_t a, uint32_t b) {
    out->kind = CBM_DOCLINK_EDGE;
    out->target = n;
    out->exact = true;
    out->target_first = a;
    out->target_last = b;
}

/* A path of a type the indexer does not read (a service file, an image): no
 * reference to code, decided by its name as the Markdown resolver does -- never
 * by the disk, which an incremental run does not watch for such files. */
static bool ra_unindexable(const char *path) {
    return cbm_language_for_filename(ra_base(path)) == CBM_LANG_COUNT;
}

/* A collector scan of build output (`./build/...`): what it contributes is
 * generated, not a file of the repository (H8). */
static bool ra_build_dir(const char *dir) {
    while (dir[0] == '.' && dir[1] == '/') {
        dir += PAIR_LEN;
    }
    return strcmp(dir, "build") == 0 || strncmp(dir, "build/", strlen("build/")) == 0 ||
           strstr(dir, "/build/") != NULL;
}

static const char *const RA_FAMILIES[][2] = {{"example", "examples"},
                                             {"partial", "partials"},
                                             {"page", "pages"},
                                             {"image", "images"},
                                             {"attachment", "attachments"}};

/* H8 resolve_target: an Antora resource ID or a path relative to the including
 * file. Returns the repository path in `out`, or false with `out->kind` set. */
static bool ra_target(const ra_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                      const ra_comp_t *comp, const char *target, char *path, size_t cap,
                      cbm_doclink_outcome_t *out) {
    out->kind = CBM_DOCLINK_LOCAL;
    if (strstr(target, "://")) {
        return false; /* a URL include */
    }
    const char *dollar = strchr(target, '$');
    if (dollar && comp) {
        char pre[RA_PATH_CAP];
        snprintf(pre, sizeof(pre), "%.*s", (int)(dollar - target), target);
        const char *at = strrchr(pre, '@');
        char *segs = at ? pre + (at - pre) + SKIP_ONE : pre;
        /* family is the last segment; module and component before it */
        char *fam = strrchr(segs, ':');
        const char *module = NULL;
        if (fam) {
            *fam = '\0';
            fam++;
            char *comp_sep = strchr(segs, ':');
            if (comp_sep) {
                *comp_sep = '\0';
                if (strcmp(segs, comp->name) != 0) {
                    return false; /* another component's resource */
                }
                module = comp_sep + SKIP_ONE;
            } else {
                module = segs;
            }
        } else {
            fam = segs;
        }
        char doc_module[RA_PATH_CAP] = "ROOT";
        if (!module) {
            /* the including page's own module: modules/<module>/... */
            const char *rel = doc + strlen(comp->root) + (comp->root[0] ? SKIP_ONE : 0);
            if (strncmp(rel, "modules/", strlen("modules/")) == 0) {
                const char *m = rel + strlen("modules/");
                const char *slash = strchr(m, '/');
                if (slash) {
                    snprintf(doc_module, sizeof(doc_module), "%.*s", (int)(slash - m), m);
                }
            }
            module = doc_module;
        }
        const char *famdir = NULL;
        for (size_t i = 0; i < sizeof(RA_FAMILIES) / sizeof(RA_FAMILIES[0]); i++) {
            if (strcmp(fam, RA_FAMILIES[i][0]) == 0) {
                famdir = RA_FAMILIES[i][1];
            }
        }
        if (!famdir) {
            ra_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            return false;
        }
        char virt[RA_PATH_CAP];
        snprintf(virt, sizeof(virt), "modules/%s/%s/%s", module, famdir, dollar + SKIP_ONE);
        if (ra_norm(comp->root, virt, path, cap) &&
            cbm_pipeline_file_node(graph, x->project, path)) {
            return true;
        }
        /* a file a collector scan contributes into the component */
        for (int i = 0; i < comp->nscans; i++) {
            const char *into = comp->scans[i].into;
            while (into[0] == '.' && into[1] == '/') {
                into += PAIR_LEN;
            }
            size_t il = strlen(into);
            while (il > 0 && into[il - SKIP_ONE] == '/') {
                il--;
            }
            if (il == 0 || strncmp(virt, into, il) != 0 || virt[il] != '/') {
                continue;
            }
            char sub[RA_PATH_CAP];
            snprintf(sub, sizeof(sub), "%s/%s", comp->scans[i].dir, virt + il + SKIP_ONE);
            if (ra_norm(comp->root, sub, path, cap) &&
                cbm_pipeline_file_node(graph, x->project, path)) {
                return true;
            }
            if (ra_build_dir(comp->scans[i].dir)) {
                return false; /* build output (H8 generated): no file of the repository */
            }
        }
        if (ra_unindexable(virt)) {
            return false;
        }
        ra_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return false;
    }
    char dir[RA_PATH_CAP];
    const char *b = ra_base(doc);
    snprintf(dir, sizeof(dir), "%.*s", b > doc ? (int)(b - doc - SKIP_ONE) : 0, doc);
    if (!ra_norm(dir, target, path, cap)) {
        return false; /* above the repository root */
    }
    if (cbm_pipeline_file_node(graph, x->project, path)) {
        return true;
    }
    char parent[RA_PATH_CAP];
    const char *pb = ra_base(path);
    snprintf(parent, sizeof(parent), "%.*s", pb > path ? (int)(pb - path - SKIP_ONE) : 0, path);
    if (!ra_unindexable(path) && parent[0] && cbm_doclink_md_folder(x->md, parent)) {
        ra_unresolved(out, CBM_DOCLINK_REASON_MISSING);
    }
    return false;
}

/* Asciidoctor's TagDirectiveRx \b(?:tag|(e)nd)::(\S+?)\[\](?=$|[ \r]) over the
 * file: the 1-based lines from the first region's first content line to the
 * last region's last one (directive lines excluded). False when a tag has no
 * region. */
static bool ra_tag_span(const char *src, size_t n, const char *const *tags, int ntags,
                        uint32_t *first, uint32_t *last) {
    *first = 0;
    *last = 0;
    for (int t = 0; t < ntags; t++) {
        size_t tl = strlen(tags[t]);
        uint32_t line = 1;
        uint32_t start = 0;
        bool found = false;
        size_t pos = 0;
        while (pos < n) {
            const char *nl = memchr(src + pos, '\n', n - pos);
            size_t end = nl ? (size_t)(nl - src) : n;
            for (size_t i = pos; i + PAIR_LEN < end; i++) {
                bool is_end = false;
                size_t k;
                if (i + strlen("tag::") <= end && memcmp(src + i, "tag::", strlen("tag::")) == 0) {
                    k = i + strlen("tag::");
                } else if (i + strlen("end::") <= end &&
                           memcmp(src + i, "end::", strlen("end::")) == 0) {
                    k = i + strlen("end::");
                    is_end = true;
                } else {
                    continue;
                }
                if (i > pos && ra_word(src[i - SKIP_ONE])) {
                    continue; /* \b */
                }
                if (k + tl + PAIR_LEN > end || memcmp(src + k, tags[t], tl) != 0 ||
                    src[k + tl] != '[' || src[k + tl + SKIP_ONE] != ']') {
                    continue;
                }
                size_t after = k + tl + PAIR_LEN;
                if (after != end && src[after] != ' ' && src[after] != '\r') {
                    continue;
                }
                if (!is_end && start == 0) {
                    start = line + SKIP_ONE;
                } else if (is_end && start != 0) {
                    if (start <= line - SKIP_ONE) {
                        *first = (*first == 0 || start < *first) ? start : *first;
                        *last = line - SKIP_ONE > *last ? line - SKIP_ONE : *last;
                        found = true;
                    }
                    start = 0;
                }
                break;
            }
            line++;
            pos = end + SKIP_ONE;
        }
        if (start != 0 && start <= line - SKIP_ONE) {
            *first = (*first == 0 || start < *first) ? start : *first;
            *last = line - SKIP_ONE > *last ? line - SKIP_ONE : *last;
            found = true;
        }
        if (!found) {
            return false;
        }
    }
    return *first > 0;
}

/* The included file's text, read from the indexed file on disk (at most
 * RA_TAG_FILE_MAX bytes): its tag regions and the lines of a region. NULL
 * when it cannot be read; the caller frees it (cbm_free, OTHER). */
static char *ra_read(const ra_index_t *x, const char *path, size_t *n) {
    *n = 0;
    if (!x->repo) {
        return NULL;
    }
    char abs[RA_PATH_CAP * PAIR_LEN];
    snprintf(abs, sizeof(abs), "%s/%s", x->repo, path);
    FILE *f = cbm_fopen(abs, "rb");
    if (!f) {
        return NULL;
    }
    char *buf = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, RA_TAG_FILE_MAX);
    *n = buf ? fread(buf, SKIP_ONE, RA_TAG_FILE_MAX, f) : 0;
    (void)fclose(f);
    return buf;
}

/* `lines=a..b;c;d..-1`: the span of all ranges (-1: the file's end). */
static bool ra_lines(const char *v, uint32_t *first, uint32_t *last) {
    *first = 0;
    *last = 0;
    const char *p = v;
    while (*p) {
        while (*p == ';' || *p == ',' || *p == ' ' || *p == '"') {
            p++;
        }
        if (!*p) {
            break;
        }
        if (*p < '0' || *p > '9') {
            return false;
        }
        uint32_t a = (uint32_t)strtoul(p, (char **)&p, 10);
        uint32_t b = a;
        if (p[0] == '.' && p[1] == '.') {
            p += PAIR_LEN;
            if (p[0] == '-' && p[1] == '1') {
                b = UINT32_MAX;
                p += PAIR_LEN;
            } else if (*p >= '0' && *p <= '9') {
                b = (uint32_t)strtoul(p, (char **)&p, 10);
            } else {
                return false;
            }
        }
        if (a == 0 || b < a) {
            return false;
        }
        *first = (*first == 0 || a < *first) ? a : *first;
        *last = b > *last ? b : *last;
    }
    return *first > 0;
}

typedef struct {
    char buf[RA_PATH_CAP * PAIR_LEN];
    char *f[RA_FIELDS];
    int n;
} ra_rec_t;

static bool ra_record(const char *raw, ra_rec_t *r) {
    if (snprintf(r->buf, sizeof(r->buf), "%s", raw) >= (int)sizeof(r->buf)) {
        return false;
    }
    r->n = 0;
    char *p = r->buf;
    while (r->n < RA_FIELDS) {
        r->f[r->n++] = p;
        char *tab = strchr(p, '\t');
        if (!tab) {
            break;
        }
        *tab = '\0';
        p = tab + SKIP_ONE;
    }
    for (int i = r->n; i < RA_FIELDS; i++) {
        r->f[i] = "";
    }
    return true;
}

static void ra_include(const ra_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                       const ra_rec_t *r, cbm_doclink_outcome_t *out) {
    const ra_comp_t *comp = ra_component_of(x, doc);
    char target[RA_PATH_CAP];
    if (strchr(r->f[1], '{')) {
        if (!comp || !ra_expand(x, comp, r->f[1], target, sizeof(target))) {
            ra_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE); /* an undefined attribute */
            return;
        }
    } else {
        snprintf(target, sizeof(target), "%s", r->f[1]);
    }
    char path[RA_PATH_CAP];
    if (!ra_target(x, graph, doc, comp, target, path, sizeof(path), out)) {
        return;
    }
    const cbm_gbuf_node_t *file = cbm_pipeline_file_node(graph, x->project, path);
    if (!file) {
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    uint32_t first = 0;
    uint32_t last = 0;
    const char *tagv = r->f[2];
    if (!tagv[0] && r->f[3][0] && !ra_lines(r->f[3], &first, &last)) {
        ra_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    size_t tn = 0;
    char *text = (tagv[0] || first > 0) ? ra_read(x, path, &tn) : NULL;
    if (tagv[0]) {
        /* H8: negated and wildcard tags leave the positive ones; none left
         * includes the whole file */
        char tags[RA_TAGS][RA_TAG_LEN];
        const char *tagp[RA_TAGS];
        int nt = 0;
        for (const char *p = tagv; *p && nt < RA_TAGS;) {
            size_t l = strcspn(p, ";,");
            if (l > 0 && l < RA_TAG_LEN && p[0] != '!' && !memchr(p, '*', l)) {
                snprintf(tags[nt], RA_TAG_LEN, "%.*s", (int)l, p);
                tagp[nt] = tags[nt];
                nt++;
            }
            p += l + (p[l] ? SKIP_ONE : 0);
        }
        if (nt == 0 || !text || !ra_tag_span(text, tn, tagp, nt, &first, &last)) {
            first = 0; /* wildcards only, or a tag the file lacks: the file */
            last = 0;
        }
    }
    if (first > 0) {
        const cbm_gbuf_node_t *seg = text
                                         ? cbm_doclink_md_region(x->md, path, text, tn, first, last)
                                         : cbm_doclink_md_segment(x->md, path, first, last, NULL);
        cbm_free(CBM_MEM_CLASS_OTHER, text);
        ra_edge(out, seg ? seg : file, first, last == UINT32_MAX ? 0 : last);
        return;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, text);
    ra_edge(out, file, 0, 0);
}

static bool ra_class_label(const char *label) {
    static const char *const labels[] = {"Class", "Interface", "Enum", "Type", "Trait", NULL};
    for (int i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool ra_member_label(const char *label) {
    static const char *const labels[] = {"Method", "Field", "Variable", "Function", NULL};
    for (int i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* H8 javadoc: xref:attachment$api/<module>/<pkg path>/<Type>.html[#member]. */
static void ra_javadoc(const ra_index_t *x, const cbm_gbuf_t *graph, const char *val,
                       cbm_doclink_outcome_t *out) {
    static const char key[] = "xref:attachment$api/";
    const char *p = strstr(val, key);
    if (!p) {
        return;
    }
    p += strlen(key);
    const char *slash = strchr(p, '/'); /* after the JPMS module */
    if (!slash) {
        return;
    }
    const char *path = slash + SKIP_ONE;
    const char *html = strstr(path, ".html");
    if (!html || memchr(path, '[', (size_t)(html - path))) {
        return;
    }
    char rel[RA_PATH_CAP];
    snprintf(rel, sizeof(rel), "%.*s", (int)(html - path), path);
    const char *tname = ra_base(rel);
    if (strcmp(tname, "package-summary") == 0 || strcmp(tname, "module-summary") == 0 ||
        strcmp(rel, "index") == 0) {
        return; /* a package or module page: no code node */
    }
    char anchor[RA_PATH_CAP] = "";
    if (html[strlen(".html")] == '#') {
        const char *a = html + strlen(".html") + SKIP_ONE;
        size_t al = strcspn(a, "[(-");
        snprintf(anchor, sizeof(anchor), "%.*s", (int)al, a);
    }
    char pkgdir[RA_PATH_CAP];
    snprintf(pkgdir, sizeof(pkgdir), "%.*s", (int)(tname - rel), rel); /* "org/x/" */
    char outer[RA_PATH_CAP];
    snprintf(outer, sizeof(outer), "%.*s", (int)strcspn(tname, "."), tname);
    const char *want = strrchr(tname, '.') ? strrchr(tname, '.') + SKIP_ONE : tname;
    /* the source files declaring <pkg dir>/<Outer>.java|.kt, main sources first */
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(graph, outer, &nodes, &count) != 0) {
        return;
    }
    const char *files[2] = {NULL, NULL};
    int nfiles = 0;
    for (int pass = 0; pass < PAIR_LEN && nfiles == 0; pass++) {
        bool main_only = pass == 0;
        for (int i = 0; i < count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            const char *fp = n->file_path;
            if (!ra_class_label(n->label) || !fp) {
                continue;
            }
            char tail[RA_PATH_CAP * PAIR_LEN];
            bool java = ra_ends(fp, ".java");
            snprintf(tail, sizeof(tail), "%s%s%s", pkgdir, outer, java ? ".java" : ".kt");
            size_t fl = strlen(fp);
            size_t tl = strlen(tail);
            if (!(java || ra_ends(fp, ".kt")) || fl < tl || strcmp(fp + fl - tl, tail) != 0 ||
                (fl > tl && fp[fl - tl - SKIP_ONE] != '/') ||
                (main_only && !strstr(fp, "/src/main/"))) {
                continue;
            }
            bool seen = false;
            for (int k = 0; k < nfiles; k++) {
                seen = seen || strcmp(files[k], fp) == 0;
            }
            if (!seen && nfiles < PAIR_LEN) {
                files[nfiles++] = fp;
            } else if (!seen) {
                nfiles++;
            }
        }
    }
    if (nfiles == 0) {
        ra_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return;
    }
    if (nfiles > SKIP_ONE) {
        ra_unresolved(out, CBM_DOCLINK_REASON_AMBIGUOUS);
        return;
    }
    /* the type in that file (the first by line for a repeated name) */
    const cbm_gbuf_node_t *type = NULL;
    if (cbm_gbuf_find_by_name(graph, want, &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            if (ra_class_label(n->label) && n->file_path && strcmp(n->file_path, files[0]) == 0 &&
                (!type || n->start_line < type->start_line ||
                 (n->start_line == type->start_line && n->id < type->id))) {
                type = n;
            }
        }
    }
    if (!type) {
        ra_unresolved(out, CBM_DOCLINK_REASON_GRAPH_GAP);
        return;
    }
    const cbm_gbuf_node_t *member = NULL;
    int members = 0;
    if (anchor[0] && cbm_gbuf_find_by_name(graph, anchor, &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            if (ra_member_label(n->label) && n->file_path && strcmp(n->file_path, files[0]) == 0 &&
                n->start_line >= type->start_line && n->start_line <= type->end_line) {
                member = n;
                members++;
            }
        }
    }
    /* an overloaded member: the type that declares it */
    ra_edge(out, members == SKIP_ONE ? member : type, 0, 0);
}

static void ra_attribute(const ra_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                         const ra_rec_t *r, cbm_doclink_outcome_t *out) {
    const ra_comp_t *comp = ra_component_of(x, doc);
    if (!comp) {
        return; /* outside an Antora component: no API attributes (H8) */
    }
    const char *raw = r->f[2][0] ? r->f[2] : ra_attr(x, comp, r->f[1], strlen(r->f[1]));
    if (!raw || !strstr(raw, "{javadoc-root}")) {
        return;
    }
    char val[RA_PATH_CAP];
    if (!ra_expand(x, comp, r->f[2][0] ? r->f[3] : raw, val, sizeof(val))) {
        return;
    }
    ra_javadoc(x, graph, val, out);
}

static void ra_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state;
    const ra_index_t *x = (const ra_index_t *)index;
    out->kind = CBM_DOCLINK_LOCAL;
    const char *doc = (run_file >= 0 && run_file < x->run_count) ? x->run_paths[run_file] : NULL;
    if (!doc || !link->raw) {
        ra_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (link->syntax == CBM_DOCLINK_ADOC_CODE_PATH || link->syntax == CBM_DOCLINK_ADOC_CODE_NAME) {
        CBMDocLink md = *link;
        md.syntax = link->syntax == CBM_DOCLINK_ADOC_CODE_PATH ? CBM_DOCLINK_MD_CODE_PATH
                                                               : CBM_DOCLINK_MD_CODE_NAME;
        cbm_doclink_md_resolver.resolve(x->md, NULL, run_file, &md, graph, out);
        return;
    }
    ra_rec_t *r = (ra_rec_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*r));
    if (!r || !ra_record(link->raw, r)) {
        ra_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        cbm_free(CBM_MEM_CLASS_OTHER, r);
        return;
    }
    if (link->syntax == CBM_DOCLINK_ADOC_INCLUDE) {
        ra_include(x, graph, doc, r, out);
    } else if (link->syntax == CBM_DOCLINK_ADOC_ATTRIBUTE) {
        ra_attribute(x, graph, doc, r, out);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, r);
}

const cbm_doclink_resolver_t cbm_doclink_adoc_resolver = {
    .langs = {CBM_LANG_ASCIIDOC, CBM_LANG_YAML},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_ADOC_SCOPE_TAG,
    .build = ra_build,
    .destroy = ra_destroy,
    .resolve = ra_resolve,
    .via = "asciidoc",
};
