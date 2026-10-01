/*
 * doc_links_msbuild.c — MSBuild global usings of a C# project (R1).
 *
 * A guess-free reader of data, never a build: nothing is executed or
 * fetched. Evaluation model (MSBuild's two passes, reduced):
 *   files       nearest Directory.Build.props (+ literal <Import>s), the
 *               project file, nearest Directory.Build.targets (+ imports),
 *               in that order
 *   pass 1      <PropertyGroup> properties in file order, later wins
 *   pass 2      <ItemGroup><Using Include|Remove> with the final properties;
 *               Static="true" and Alias items are not namespace usings
 *   implicit    ImplicitUsings enable/true adds the SDK default set of the
 *               project's Sdk (Microsoft.NET.Sdk / .Web / .Worker)
 * Conditions are evaluated only in the simple forms `'$(X)' == 'v'`,
 * `'$(X)' != 'v'` joined by and/or; anything else makes the element
 * unevaluable and it is skipped (counted, never guessed). Mirrors the field-
 * tested prototype private/field-tests/tools/h8/h8_msbuild.py.
 */
#include "pipeline/doc_links.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    MSB_MAX_FILE = 1000000, /* the prototype's size cap: larger files are not project files */
    MSB_MAX_DEPTH = 64,
    MSB_MAX_PROPS = 512,
    MSB_MAX_SEQ = 4096,
    MSB_MAX_USINGS = 256,
    MSB_MAX_FILES = 64,
    MSB_PATH = 4096,
    MSB_VALUE = 4096,
};

/* ── Minimal XML tree ────────────────────────────────────────────── */

typedef struct msb_node {
    char *tag; /* local name */
    char **attr_names;
    char **attr_values;
    int nattrs;
    char *text; /* direct text content, entities decoded */
    struct msb_node **kids;
    int nkids;
    int cap;
    struct msb_node *parent;
} msb_node_t;

static void msb_free(msb_node_t *n) {
    if (!n) {
        return;
    }
    for (int i = 0; i < n->nkids; i++) {
        msb_free(n->kids[i]);
    }
    for (int i = 0; i < n->nattrs; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, n->attr_names[i]);
        cbm_free(CBM_MEM_CLASS_OTHER, n->attr_values[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, n->attr_names);
    cbm_free(CBM_MEM_CLASS_OTHER, n->attr_values);
    cbm_free(CBM_MEM_CLASS_OTHER, n->kids);
    cbm_free(CBM_MEM_CLASS_OTHER, n->tag);
    cbm_free(CBM_MEM_CLASS_OTHER, n->text);
    cbm_free(CBM_MEM_CLASS_OTHER, n);
}

static char *msb_decode(const char *s, size_t n) {
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    static const struct {
        const char *ent;
        char ch;
    } ents[] = {{"&lt;", '<'}, {"&gt;", '>'}, {"&amp;", '&'}, {"&quot;", '"'}, {"&apos;", '\''}};
    size_t w = 0;
    for (size_t i = 0; i < n;) {
        bool hit = false;
        if (s[i] == '&') {
            for (size_t e = 0; e < sizeof(ents) / sizeof(ents[0]); e++) {
                size_t el = strlen(ents[e].ent);
                if (i + el <= n && memcmp(s + i, ents[e].ent, el) == 0) {
                    out[w++] = ents[e].ch;
                    i += el;
                    hit = true;
                    break;
                }
            }
        }
        if (!hit) {
            out[w++] = s[i++];
        }
    }
    out[w] = '\0';
    return out;
}

static const char *msb_local(const char *name, size_t *len) {
    const char *colon = memchr(name, ':', *len);
    if (colon) {
        *len -= (size_t)(colon + SKIP_ONE - name);
        return colon + SKIP_ONE;
    }
    return name;
}

static bool msb_add_kid(msb_node_t *parent, msb_node_t *kid) {
    if (parent->nkids >= parent->cap) {
        int ncap = parent->cap ? parent->cap * PAIR_LEN : CBM_SZ_8;
        msb_node_t **grown = (msb_node_t **)cbm_realloc(CBM_MEM_CLASS_OTHER, parent->kids,
                                                        (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        parent->kids = grown;
        parent->cap = ncap;
    }
    parent->kids[parent->nkids++] = kid;
    kid->parent = parent;
    return true;
}

static void msb_append_text(msb_node_t *n, const char *s, size_t len) {
    if (!n || len == 0) {
        return;
    }
    char *dec = msb_decode(s, len);
    if (!dec) {
        return;
    }
    size_t old = n->text ? strlen(n->text) : 0;
    char *grown = (char *)cbm_realloc(CBM_MEM_CLASS_OTHER, n->text, old + strlen(dec) + SKIP_ONE);
    if (grown) {
        memcpy(grown + old, dec, strlen(dec) + SKIP_ONE);
        n->text = grown;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, dec);
}

static bool msb_name_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == ':' || c == '.' || c == '-';
}

/* Parse well-formed XML into a tree rooted at a synthetic node; NULL when the
 * document is not well-formed enough to read. */
static msb_node_t *msb_parse(const char *xml) {
    msb_node_t *doc = (msb_node_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*doc));
    if (!doc) {
        return NULL;
    }
    msb_node_t *cur = doc;
    int depth = 0;
    const char *p = xml;
    while (*p) {
        const char *lt = strchr(p, '<');
        if (!lt) {
            break;
        }
        if (cur != doc) {
            msb_append_text(cur, p, (size_t)(lt - p));
        }
        p = lt;
        if (strncmp(p, "<!--", 4) == 0) {
            const char *e = strstr(p + 4, "-->");
            if (!e) {
                break;
            }
            p = e + 3;
            continue;
        }
        if (strncmp(p, "<![CDATA[", 9) == 0) {
            const char *e = strstr(p + 9, "]]>");
            if (!e) {
                break;
            }
            if (cur != doc && cur) {
                size_t old = cur->text ? strlen(cur->text) : 0;
                size_t add = (size_t)(e - (p + 9));
                char *grown =
                    (char *)cbm_realloc(CBM_MEM_CLASS_OTHER, cur->text, old + add + SKIP_ONE);
                if (grown) {
                    memcpy(grown + old, p + 9, add);
                    grown[old + add] = '\0';
                    cur->text = grown;
                }
            }
            p = e + 3;
            continue;
        }
        if (p[1] == '?' || p[1] == '!') {
            const char *e = strchr(p, '>');
            if (!e) {
                break;
            }
            p = e + SKIP_ONE;
            continue;
        }
        if (p[1] == '/') {
            const char *e = strchr(p, '>');
            if (!e || !cur->parent) {
                msb_free(doc);
                return NULL;
            }
            cur = cur->parent;
            depth--;
            p = e + SKIP_ONE;
            continue;
        }
        /* start tag */
        const char *q = p + SKIP_ONE;
        const char *nm = q;
        while (msb_name_char(*q)) {
            q++;
        }
        size_t nlen = (size_t)(q - nm);
        if (nlen == 0) {
            msb_free(doc);
            return NULL;
        }
        msb_node_t *el = (msb_node_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*el));
        if (!el) {
            msb_free(doc);
            return NULL;
        }
        const char *local = msb_local(nm, &nlen);
        el->tag = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, nlen + SKIP_ONE);
        if (!el->tag || !msb_add_kid(cur, el)) {
            cbm_free(CBM_MEM_CLASS_OTHER, el->tag);
            cbm_free(CBM_MEM_CLASS_OTHER, el);
            msb_free(doc);
            return NULL;
        }
        memcpy(el->tag, local, nlen);
        el->tag[nlen] = '\0';
        bool self_close = false;
        for (;;) {
            while (isspace((unsigned char)*q)) {
                q++;
            }
            if (*q == '/' && q[1] == '>') {
                self_close = true;
                q += PAIR_LEN;
                break;
            }
            if (*q == '>') {
                q++;
                break;
            }
            const char *an = q;
            while (msb_name_char(*q)) {
                q++;
            }
            size_t alen = (size_t)(q - an);
            while (isspace((unsigned char)*q)) {
                q++;
            }
            if (alen == 0 || *q != '=') {
                msb_free(doc);
                return NULL;
            }
            q++;
            while (isspace((unsigned char)*q)) {
                q++;
            }
            char quote = *q;
            if (quote != '"' && quote != '\'') {
                msb_free(doc);
                return NULL;
            }
            const char *vs = q + SKIP_ONE;
            const char *ve = strchr(vs, quote);
            if (!ve) {
                msb_free(doc);
                return NULL;
            }
            char **an_grown = (char **)cbm_realloc(CBM_MEM_CLASS_OTHER, el->attr_names,
                                                   (size_t)(el->nattrs + 1) * sizeof(char *));
            if (an_grown) {
                el->attr_names = an_grown;
            }
            char **av_grown = (char **)cbm_realloc(CBM_MEM_CLASS_OTHER, el->attr_values,
                                                   (size_t)(el->nattrs + 1) * sizeof(char *));
            if (av_grown) {
                el->attr_values = av_grown;
            }
            if (!an_grown || !av_grown) {
                msb_free(doc);
                return NULL;
            }
            size_t llen = alen;
            const char *alocal = msb_local(an, &llen);
            el->attr_names[el->nattrs] = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, llen + SKIP_ONE);
            el->attr_values[el->nattrs] = msb_decode(vs, (size_t)(ve - vs));
            if (!el->attr_names[el->nattrs] || !el->attr_values[el->nattrs]) {
                cbm_free(CBM_MEM_CLASS_OTHER, el->attr_names[el->nattrs]);
                cbm_free(CBM_MEM_CLASS_OTHER, el->attr_values[el->nattrs]);
                msb_free(doc);
                return NULL;
            }
            memcpy(el->attr_names[el->nattrs], alocal, llen);
            el->attr_names[el->nattrs][llen] = '\0';
            el->nattrs++;
            q = ve + SKIP_ONE;
        }
        p = q;
        if (!self_close) {
            if (++depth > MSB_MAX_DEPTH) {
                msb_free(doc);
                return NULL;
            }
            cur = el;
        }
    }
    return doc;
}

static const char *msb_attr(const msb_node_t *n, const char *name) {
    for (int i = 0; i < n->nattrs; i++) {
        if (strcmp(n->attr_names[i], name) == 0) {
            return n->attr_values[i];
        }
    }
    return NULL;
}

/* ── Evaluation ──────────────────────────────────────────────────── */

typedef struct {
    char *name; /* lowercased: MSBuild property names are case-insensitive */
    char *value;
} msb_prop_t;

typedef struct {
    const msb_node_t *el;
    char *dir; /* repo-relative directory of the file the element came from */
} msb_seq_t;

typedef struct {
    const char *repo;
    msb_prop_t props[MSB_MAX_PROPS];
    int nprops;
    msb_seq_t seq[MSB_MAX_SEQ];
    int nseq;
    msb_node_t *docs[MSB_MAX_FILES];
    char *seen[MSB_MAX_FILES];
    int ndocs;
    char sdk[CBM_SZ_128];
    int unevaluable;
} msb_eval_t;

static void lower_copy(char *dst, size_t cap, const char *src, size_t n) {
    size_t i = 0;
    for (; i < n && i + SKIP_ONE < cap; i++) {
        dst[i] = (char)tolower((unsigned char)src[i]);
    }
    dst[i] = '\0';
}

static const char *msb_prop_get(const msb_eval_t *ev, const char *name, size_t n) {
    char key[CBM_SZ_256];
    lower_copy(key, sizeof(key), name, n);
    for (int i = ev->nprops - SKIP_ONE; i >= 0; i--) {
        if (strcmp(ev->props[i].name, key) == 0) {
            return ev->props[i].value;
        }
    }
    return NULL;
}

static void msb_prop_set(msb_eval_t *ev, const char *name, const char *value) {
    char key[CBM_SZ_256];
    lower_copy(key, sizeof(key), name, strlen(name));
    for (int i = 0; i < ev->nprops; i++) {
        if (strcmp(ev->props[i].name, key) == 0) {
            char *v = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, value);
            if (v) {
                cbm_free(CBM_MEM_CLASS_OTHER, ev->props[i].value);
                ev->props[i].value = v;
            }
            return;
        }
    }
    if (ev->nprops >= MSB_MAX_PROPS) {
        return;
    }
    ev->props[ev->nprops].name = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
    ev->props[ev->nprops].value = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, value);
    if (ev->props[ev->nprops].name && ev->props[ev->nprops].value) {
        ev->nprops++;
    } else {
        cbm_free(CBM_MEM_CLASS_OTHER, ev->props[ev->nprops].name);
        cbm_free(CBM_MEM_CLASS_OTHER, ev->props[ev->nprops].value);
    }
}

/* $(Prop) substitution. *undefined is set when a referenced property has no
 * value (MSBuild would evaluate it to the empty string; paths and usings that
 * depend on one are not trusted). */
static void msb_subst(const msb_eval_t *ev, const char *s, const char *fdir, char *out, size_t cap,
                      bool *undefined) {
    size_t w = 0;
    *undefined = false;
    for (const char *p = s; *p && w + SKIP_ONE < cap;) {
        if (p[0] == '$' && p[1] == '(') {
            const char *e = strchr(p + PAIR_LEN, ')');
            if (e) {
                const char *nm = p + PAIR_LEN;
                size_t nl = (size_t)(e - nm);
                char key[CBM_SZ_256];
                lower_copy(key, sizeof(key), nm, nl);
                const char *val = NULL;
                char dirbuf[MSB_PATH];
                if (strcmp(key, "msbuildthisfiledirectory") == 0) {
                    snprintf(dirbuf, sizeof(dirbuf), "%s%s", fdir, fdir[0] ? "/" : "");
                    val = dirbuf;
                } else {
                    val = msb_prop_get(ev, nm, nl);
                }
                if (!val) {
                    *undefined = true;
                    val = "";
                }
                for (const char *v = val; *v && w + SKIP_ONE < cap; v++) {
                    out[w++] = *v;
                }
                p = e + SKIP_ONE;
                continue;
            }
        }
        out[w++] = *p++;
    }
    out[w] = '\0';
}

static void trim_inplace(char *s) {
    size_t b = 0;
    while (s[b] && isspace((unsigned char)s[b])) {
        b++;
    }
    size_t n = strlen(s + b);
    memmove(s, s + b, n + SKIP_ONE);
    while (n > 0 && isspace((unsigned char)s[n - SKIP_ONE])) {
        s[--n] = '\0';
    }
}

static bool ci_eq(const char *a, const char *b) {
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) {
            return false;
        }
    }
    return *a == *b;
}

/* One comparison `'lhs' == 'rhs'` / `!=`. Returns 1/0, or -1 when it is not in
 * a supported form. */
static int msb_cond_atom(const msb_eval_t *ev, const char *atom, const char *fdir) {
    char buf[MSB_VALUE];
    snprintf(buf, sizeof(buf), "%s", atom);
    trim_inplace(buf);
    while (buf[0] == '(') {
        memmove(buf, buf + SKIP_ONE, strlen(buf));
        trim_inplace(buf);
    }
    size_t bl = strlen(buf);
    while (bl > 0 && buf[bl - SKIP_ONE] == ')') {
        buf[--bl] = '\0';
        trim_inplace(buf);
        bl = strlen(buf);
    }
    char *op = strstr(buf, "==");
    bool eq = true;
    if (!op) {
        op = strstr(buf, "!=");
        eq = false;
    }
    if (!op) {
        return CBM_NOT_FOUND;
    }
    char lhs[MSB_VALUE];
    char rhs[MSB_VALUE];
    snprintf(lhs, sizeof(lhs), "%.*s", (int)(op - buf), buf);
    snprintf(rhs, sizeof(rhs), "%s", op + PAIR_LEN);
    trim_inplace(lhs);
    trim_inplace(rhs);
    size_t rl = strlen(rhs);
    if (rl < PAIR_LEN || rhs[0] != '\'' || rhs[rl - SKIP_ONE] != '\'') {
        return CBM_NOT_FOUND;
    }
    rhs[rl - SKIP_ONE] = '\0';
    size_t ll = strlen(lhs);
    if (ll >= PAIR_LEN && lhs[0] == '\'' && lhs[ll - SKIP_ONE] == '\'') {
        lhs[ll - SKIP_ONE] = '\0';
        memmove(lhs, lhs + SKIP_ONE, ll - SKIP_ONE);
    }
    if (strchr(lhs, '\'') || strchr(rhs + SKIP_ONE, '\'')) {
        return CBM_NOT_FOUND;
    }
    char val[MSB_VALUE];
    bool undefined = false;
    msb_subst(ev, lhs, fdir, val, sizeof(val), &undefined);
    bool same = ci_eq(val, rhs + SKIP_ONE);
    return (eq ? same : !same) ? SKIP_ONE : 0;
}

/* `w` starts with the keyword `kw` (case-insensitive) followed by whitespace. */
static bool ci_keyword_at(const char *w, const char *kw) {
    size_t n = strlen(kw);
    for (size_t i = 0; i < n; i++) {
        if (tolower((unsigned char)w[i]) != kw[i]) {
            return false;
        }
    }
    return isspace((unsigned char)w[n]) != 0;
}

enum { MSB_OP_NONE = 0, MSB_OP_AND = 1, MSB_OP_OR = 2 };

/* Evaluate a Condition attribute: 1 true, 0 false, -1 unevaluable. The
 * comparisons are joined left to right by `and` / `or`, as the prototype does. */
static int msb_cond(const msb_eval_t *ev, const char *cond, const char *fdir) {
    if (!cond) {
        return SKIP_ONE;
    }
    char buf[MSB_VALUE];
    snprintf(buf, sizeof(buf), "%s", cond);
    trim_inplace(buf);
    if (!buf[0]) {
        return SKIP_ONE;
    }
    int result = CBM_NOT_FOUND;
    int pending_op = MSB_OP_NONE;
    char *p = buf;
    for (;;) {
        /* the next whitespace-delimited `and` / `or` ends this comparison */
        char *next = NULL;
        int op = MSB_OP_NONE;
        for (char *q = p; *q && !next; q++) {
            if (!isspace((unsigned char)*q)) {
                continue;
            }
            char *w = q;
            while (isspace((unsigned char)*w)) {
                w++;
            }
            if (ci_keyword_at(w, "and")) {
                op = MSB_OP_AND;
                next = w + strlen("and");
            } else if (ci_keyword_at(w, "or")) {
                op = MSB_OP_OR;
                next = w + strlen("or");
            }
            if (next) {
                *q = '\0';
            }
        }
        int v = msb_cond_atom(ev, p, fdir);
        if (v < 0) {
            return CBM_NOT_FOUND;
        }
        if (pending_op == MSB_OP_NONE) {
            result = v;
        } else if (pending_op == MSB_OP_AND) {
            result = result && v;
        } else {
            result = result || v;
        }
        if (!next) {
            break;
        }
        pending_op = op;
        p = next;
    }
    return result;
}

static char *msb_read_file(const char *repo, const char *rel) {
    char full[MSB_PATH];
    if (snprintf(full, sizeof(full), "%s/%s", repo, rel) >= (int)sizeof(full)) {
        return NULL;
    }
    FILE *fp = cbm_fopen(full, "rb");
    if (!fp) {
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    long n = ftell(fp);
    if (n < 0 || n > MSB_MAX_FILE || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    char *buf = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)n + SKIP_ONE);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)n, fp);
    fclose(fp);
    buf[got] = '\0';
    return buf;
}

static bool msb_exists(const char *repo, const char *rel) {
    char full[MSB_PATH];
    if (snprintf(full, sizeof(full), "%s/%s", repo, rel) >= (int)sizeof(full)) {
        return false;
    }
    cbm_path_info_t info;
    return cbm_path_info_utf8(full, &info) == 0 && info.is_regular;
}

static void dir_of(const char *rel, char *out, size_t cap) {
    const char *slash = strrchr(rel, '/');
    if (!slash) {
        out[0] = '\0';
        return;
    }
    snprintf(out, cap, "%.*s", (int)(slash - rel), rel);
}

/* Normalize a repo-relative path: backslashes, "." and ".." segments. false
 * when it would leave the repository. */
static bool norm_rel(const char *in, char *out, size_t cap) {
    char tmp[MSB_PATH];
    snprintf(tmp, sizeof(tmp), "%s", in);
    for (char *c = tmp; *c; c++) {
        if (*c == '\\') {
            *c = '/';
        }
    }
    char *segs[CBM_SZ_256];
    int n = 0;
    char *cursor = tmp;
    while (cursor) {
        char *tok = cursor;
        char *slash = strchr(cursor, '/');
        if (slash) {
            *slash = '\0';
            cursor = slash + SKIP_ONE;
        } else {
            cursor = NULL;
        }
        if (strcmp(tok, ".") == 0 || !tok[0]) {
            continue;
        }
        if (strcmp(tok, "..") == 0) {
            if (n == 0) {
                return false;
            }
            n--;
            continue;
        }
        if (n >= (int)(sizeof(segs) / sizeof(segs[0]))) {
            return false;
        }
        segs[n++] = tok;
    }
    size_t w = 0;
    out[0] = '\0';
    for (int i = 0; i < n; i++) {
        int k = snprintf(out + w, cap - w, "%s%s", i ? "/" : "", segs[i]);
        if (k < 0 || (size_t)k >= cap - w) {
            return false;
        }
        w += (size_t)k;
    }
    return true;
}

/* Nearest `name` from `dir` up to the repository root, or false. */
static bool msb_nearest(const char *repo, const char *dir, const char *name, char *out,
                        size_t cap) {
    char d[MSB_PATH];
    snprintf(d, sizeof(d), "%s", dir);
    for (;;) {
        char cand[MSB_PATH];
        snprintf(cand, sizeof(cand), "%s%s%s", d, d[0] ? "/" : "", name);
        if (msb_exists(repo, cand)) {
            snprintf(out, cap, "%s", cand);
            return true;
        }
        if (!d[0]) {
            return false;
        }
        char *slash = strrchr(d, '/');
        if (slash) {
            *slash = '\0';
        } else {
            d[0] = '\0';
        }
    }
}

static void msb_sequence(msb_eval_t *ev, const char *rel) {
    for (int i = 0; i < ev->ndocs; i++) {
        if (strcmp(ev->seen[i], rel) == 0) {
            return;
        }
    }
    if (ev->ndocs >= MSB_MAX_FILES) {
        ev->unevaluable++;
        return;
    }
    char *text = msb_read_file(ev->repo, rel);
    msb_node_t *doc = text ? msb_parse(text) : NULL;
    cbm_free(CBM_MEM_CLASS_OTHER, text);
    char *seen = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, rel);
    if (!seen) {
        msb_free(doc);
        return;
    }
    ev->seen[ev->ndocs] = seen;
    ev->docs[ev->ndocs] = doc;
    ev->ndocs++;
    if (!doc || doc->nkids == 0) {
        return;
    }
    const msb_node_t *root = doc->kids[0];
    char fdir[MSB_PATH];
    dir_of(rel, fdir, sizeof(fdir));
    if (strcmp(root->tag, "Project") == 0) {
        const char *sdk = msb_attr(root, "Sdk");
        if (sdk && sdk[0]) {
            snprintf(ev->sdk, sizeof(ev->sdk), "%.*s", (int)strcspn(sdk, "/"), sdk);
        }
    }
    for (int i = 0; i < root->nkids; i++) {
        const msb_node_t *el = root->kids[i];
        if (strcmp(el->tag, "Import") == 0) {
            int ok = msb_cond(ev, msb_attr(el, "Condition"), fdir);
            if (ok < 0) {
                ev->unevaluable++;
                continue;
            }
            if (!ok) {
                continue;
            }
            const char *proj = msb_attr(el, "Project");
            char path[MSB_PATH];
            bool undefined = false;
            msb_subst(ev, proj ? proj : "", fdir, path, sizeof(path), &undefined);
            if (undefined || !path[0]) {
                ev->unevaluable++;
                continue;
            }
            char joined[MSB_PATH];
            char normalized[MSB_PATH];
            bool absolute = path[0] == '/';
            snprintf(joined, sizeof(joined), "%s%s%s", absolute ? "" : fdir,
                     (!absolute && fdir[0]) ? "/" : "", path);
            if (!absolute && norm_rel(joined, normalized, sizeof(normalized)) &&
                msb_exists(ev->repo, normalized)) {
                msb_sequence(ev, normalized);
            }
            continue;
        }
        if (strcmp(el->tag, "PropertyGroup") == 0) {
            int ok = msb_cond(ev, msb_attr(el, "Condition"), fdir);
            if (ok < 0) {
                ev->unevaluable++;
                continue;
            }
            if (!ok) {
                continue;
            }
            for (int k = 0; k < el->nkids; k++) {
                const msb_node_t *pe = el->kids[k];
                int pc = msb_cond(ev, msb_attr(pe, "Condition"), fdir);
                if (pc < 0) {
                    ev->unevaluable++;
                    continue;
                }
                if (!pc) {
                    continue;
                }
                char raw[MSB_VALUE];
                snprintf(raw, sizeof(raw), "%s", pe->text ? pe->text : "");
                trim_inplace(raw);
                char val[MSB_VALUE];
                bool undefined = false;
                msb_subst(ev, raw, fdir, val, sizeof(val), &undefined);
                msb_prop_set(ev, pe->tag, val);
            }
        }
        if (ev->nseq < MSB_MAX_SEQ) {
            ev->seq[ev->nseq].el = el;
            ev->seq[ev->nseq].dir = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, fdir);
            if (ev->seq[ev->nseq].dir) {
                ev->nseq++;
            }
        }
    }
}

static void msb_eval_free(msb_eval_t *ev) {
    for (int i = 0; i < ev->nprops; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, ev->props[i].name);
        cbm_free(CBM_MEM_CLASS_OTHER, ev->props[i].value);
    }
    for (int i = 0; i < ev->nseq; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, ev->seq[i].dir);
    }
    for (int i = 0; i < ev->ndocs; i++) {
        msb_free(ev->docs[i]);
        cbm_free(CBM_MEM_CLASS_OTHER, ev->seen[i]);
    }
}

static const char *const SDK_DEFAULT[] = {
    "System",           "System.Collections.Generic", "System.IO", "System.Linq", "System.Net.Http",
    "System.Threading", "System.Threading.Tasks",     NULL};
static const char *const SDK_WEB[] = {"System",
                                      "System.Collections.Generic",
                                      "System.IO",
                                      "System.Linq",
                                      "System.Net.Http",
                                      "System.Net.Http.Json",
                                      "System.Threading",
                                      "System.Threading.Tasks",
                                      "Microsoft.AspNetCore.Builder",
                                      "Microsoft.AspNetCore.Hosting",
                                      "Microsoft.AspNetCore.Http",
                                      "Microsoft.AspNetCore.Routing",
                                      "Microsoft.Extensions.Configuration",
                                      "Microsoft.Extensions.DependencyInjection",
                                      "Microsoft.Extensions.Hosting",
                                      "Microsoft.Extensions.Logging",
                                      NULL};
static const char *const SDK_WORKER[] = {"System",
                                         "System.Collections.Generic",
                                         "System.IO",
                                         "System.Linq",
                                         "System.Net.Http",
                                         "System.Threading",
                                         "System.Threading.Tasks",
                                         "Microsoft.Extensions.Configuration",
                                         "Microsoft.Extensions.DependencyInjection",
                                         "Microsoft.Extensions.Hosting",
                                         "Microsoft.Extensions.Logging",
                                         NULL};

static bool strv_has(char **v, int n, const char *s) {
    for (int i = 0; i < n; i++) {
        if (strcmp(v[i], s) == 0) {
            return true;
        }
    }
    return false;
}

void cbm_doclinks_free_strv(char **v) {
    if (!v) {
        return;
    }
    for (int i = 0; v[i]; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, v[i]);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, v);
}

int cbm_doclinks_msbuild_usings(const char *repo_path, const char *csproj_rel, char ***out) {
    *out = NULL;
    if (!repo_path || !csproj_rel) {
        return 0;
    }
    msb_eval_t *ev = (msb_eval_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ev));
    if (!ev) {
        return 0;
    }
    ev->repo = repo_path;
    snprintf(ev->sdk, sizeof(ev->sdk), "Microsoft.NET.Sdk");
    char proj_dir[MSB_PATH];
    dir_of(csproj_rel, proj_dir, sizeof(proj_dir));
    const char *base = strrchr(csproj_rel, '/');
    base = base ? base + SKIP_ONE : csproj_rel;
    const char *dot = strrchr(base, '.');
    char name[CBM_SZ_256];
    snprintf(name, sizeof(name), "%.*s", (int)(dot ? (size_t)(dot - base) : strlen(base)), base);
    msb_prop_set(ev, "MSBuildProjectName", name);
    msb_prop_set(ev, "MSBuildProjectDirectory", proj_dir);
    char dbp[MSB_PATH];
    char dbt[MSB_PATH];
    if (msb_nearest(repo_path, proj_dir, "Directory.Build.props", dbp, sizeof(dbp))) {
        msb_sequence(ev, dbp);
    }
    msb_sequence(ev, csproj_rel);
    if (msb_nearest(repo_path, proj_dir, "Directory.Build.targets", dbt, sizeof(dbt))) {
        msb_sequence(ev, dbt);
    }
    char *inc[MSB_MAX_USINGS] = {0};
    int ninc = 0;
    char *rem[MSB_MAX_USINGS] = {0};
    int nrem = 0;
    for (int i = 0; i < ev->nseq; i++) {
        const msb_node_t *el = ev->seq[i].el;
        if (strcmp(el->tag, "ItemGroup") != 0) {
            continue;
        }
        const char *fdir = ev->seq[i].dir;
        int ok = msb_cond(ev, msb_attr(el, "Condition"), fdir);
        if (ok <= 0) {
            ev->unevaluable += ok < 0;
            continue;
        }
        for (int k = 0; k < el->nkids; k++) {
            const msb_node_t *it = el->kids[k];
            if (strcmp(it->tag, "Using") != 0) {
                continue;
            }
            int ic = msb_cond(ev, msb_attr(it, "Condition"), fdir);
            if (ic <= 0) {
                ev->unevaluable += ic < 0;
                continue;
            }
            const char *st = msb_attr(it, "Static");
            if ((st && ci_eq(st, "true")) || msb_attr(it, "Alias")) {
                continue;
            }
            const char *attrs[2] = {msb_attr(it, "Include"), msb_attr(it, "Remove")};
            for (int a = 0; a < 2; a++) {
                if (!attrs[a]) {
                    continue;
                }
                char val[MSB_VALUE];
                bool undefined = false;
                msb_subst(ev, attrs[a], fdir, val, sizeof(val), &undefined);
                trim_inplace(val);
                if (undefined || !val[0]) {
                    continue;
                }
                char **list = a == 0 ? inc : rem;
                int *n = a == 0 ? &ninc : &nrem;
                if (*n < MSB_MAX_USINGS && !strv_has(list, *n, val)) {
                    list[*n] = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, val);
                    if (list[*n]) {
                        (*n)++;
                    }
                }
            }
        }
    }
    const char *iu = msb_prop_get(ev, "ImplicitUsings", strlen("ImplicitUsings"));
    const char *const *implicit = NULL;
    if (iu && (ci_eq(iu, "enable") || ci_eq(iu, "true"))) {
        implicit = strcmp(ev->sdk, "Microsoft.NET.Sdk.Web") == 0      ? SDK_WEB
                   : strcmp(ev->sdk, "Microsoft.NET.Sdk.Worker") == 0 ? SDK_WORKER
                                                                      : SDK_DEFAULT;
    }
    char **res = (char **)cbm_calloc(
        CBM_MEM_CLASS_OTHER, ((size_t)MSB_MAX_USINGS * PAIR_LEN + SKIP_ONE) * sizeof(char *));
    int nres = 0;
    if (res) {
        for (int i = 0; implicit && implicit[i]; i++) {
            if (!strv_has(rem, nrem, implicit[i]) && !strv_has(res, nres, implicit[i])) {
                res[nres] = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, implicit[i]);
                nres += res[nres] != NULL;
            }
        }
        for (int i = 0; i < ninc; i++) {
            if (!strv_has(rem, nrem, inc[i]) && !strv_has(res, nres, inc[i])) {
                res[nres] = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, inc[i]);
                nres += res[nres] != NULL;
            }
        }
    }
    for (int i = 0; i < ninc; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, inc[i]);
    }
    for (int i = 0; i < nrem; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, rem[i]);
    }
    msb_eval_free(ev);
    cbm_free(CBM_MEM_CLASS_OTHER, ev);
    if (!res || nres == 0) {
        cbm_free(CBM_MEM_CLASS_OTHER, res);
        return 0;
    }
    *out = res;
    return nres;
}
