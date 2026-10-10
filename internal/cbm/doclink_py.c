/*
 * doclink_py.c — the doc-link scope of a Python file: what the reST resolver
 * (src/pipeline/doc_links_rst.c) needs from Python sources to resolve Sphinx
 * references the way Sphinx does.
 *
 *   I \t level \t module \t name \t alias   a package `__init__.py`'s top-level
 *                                          `from module import name [as alias]`:
 *                                          the package re-exports `alias`
 *                                          (`django.db.models.Model` is
 *                                          django/db/models/base.py's Model).
 *                                          Star imports are not read (the field
 *                                          test's resolver did not follow them).
 *   C                                      the file is a `conf.py`: documents in
 *                                          and below its directory are one
 *                                          Sphinx documentation set
 *   D \t domain                            its primary_domain
 *   X \t role                              an extlink role whose URL is a
 *                                          repository blob/tree path
 *                                          (:source:`django/db/x.py`)
 *   S \t name                              an intersphinx name (`python:` in
 *                                          :class:`python:dict` is external)
 *
 * conf.py is read as text, with the field test's patterns, and never run.
 * Every other Python file has no scope (NULL), so a change to it never
 * changes another file's resolution through this blob.
 */
#include "doclink.h"

#include "arena.h"
#include "cbm.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

enum {
    PY_SCOPE_FIELD_MAX = 256, /* a module, name or setting longer than this is skipped */
    PY_SB_INIT = 256,
    PY_INT_DIGITS = 16,
};

/* A small arena string builder (the blob lives in the result arena). */
typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool failed;
} py_sb_t;

static void py_put(py_sb_t *sb, const char *s, size_t n) {
    if (sb->failed) {
        return;
    }
    if (sb->len + n + 1 > sb->cap) {
        size_t ncap = sb->cap ? sb->cap : PY_SB_INIT;
        while (ncap < sb->len + n + 1) {
            ncap *= 2;
        }
        char *grown = (char *)cbm_arena_alloc(sb->a, ncap);
        if (!grown) {
            sb->failed = true;
            return;
        }
        if (sb->len > 0) {
            memcpy(grown, sb->buf, sb->len);
        }
        sb->buf = grown;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

/* One record: its kind, then each field after a TAB, then a newline. */
static void py_rec(py_sb_t *sb, const char *kind, int nfields, const char *const *f,
                   const size_t *fl) {
    py_put(sb, kind, strlen(kind));
    for (int i = 0; i < nfields; i++) {
        py_put(sb, "\t", 1);
        py_put(sb, f[i], fl[i]);
    }
    py_put(sb, "\n", 1);
}

static const char *py_base(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static bool py_field_ok(const char *s, size_t n) {
    if (n == 0 || n > PY_SCOPE_FIELD_MAX) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '\t' || c == '\n' || c == '\r' || c < ' ') {
            return false;
        }
    }
    return true;
}

static void py_text(const CBMExtractCtx *ctx, TSNode n, const char **s, size_t *len) {
    uint32_t a = ts_node_start_byte(n);
    uint32_t b = ts_node_end_byte(n);
    if (b > (uint32_t)ctx->source_len || a > b) {
        *s = "";
        *len = 0;
        return;
    }
    *s = ctx->source + a;
    *len = b - a;
}

/* One `from module import names` statement of the package's top level. */
static void py_from_import(const CBMExtractCtx *ctx, TSNode stmt, py_sb_t *sb) {
    TSNode mod = ts_node_child_by_field_name(stmt, "module_name", (uint32_t)strlen("module_name"));
    if (ts_node_is_null(mod)) {
        return;
    }
    int level = 0;
    const char *ms = "";
    size_t ml = 0;
    if (strcmp(ts_node_type(mod), "relative_import") == 0) {
        uint32_t nc = ts_node_child_count(mod);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(mod, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "import_prefix") == 0) {
                const char *ps;
                size_t pl;
                py_text(ctx, c, &ps, &pl);
                for (size_t j = 0; j < pl; j++) {
                    level += ps[j] == '.';
                }
            } else if (strcmp(k, "dotted_name") == 0) {
                py_text(ctx, c, &ms, &ml);
            }
        }
    } else {
        py_text(ctx, mod, &ms, &ml);
    }
    if (ml > 0 && !py_field_ok(ms, ml)) {
        return;
    }
    uint32_t nc = ts_node_child_count(stmt);
    for (uint32_t i = 0; i < nc; i++) {
        const char *field = ts_node_field_name_for_child(stmt, i);
        if (!field || strcmp(field, "name") != 0) {
            continue;
        }
        TSNode c = ts_node_child(stmt, i);
        TSNode name = c;
        TSNode alias = c;
        if (strcmp(ts_node_type(c), "aliased_import") == 0) {
            name = ts_node_child_by_field_name(c, "name", (uint32_t)strlen("name"));
            alias = ts_node_child_by_field_name(c, "alias", (uint32_t)strlen("alias"));
            if (ts_node_is_null(name) || ts_node_is_null(alias)) {
                continue;
            }
        }
        const char *ns;
        size_t nl;
        const char *as;
        size_t al;
        py_text(ctx, name, &ns, &nl);
        py_text(ctx, alias, &as, &al);
        if (!py_field_ok(ns, nl) || !py_field_ok(as, al)) {
            continue;
        }
        char lv[PY_INT_DIGITS];
        int lvn = snprintf(lv, sizeof(lv), "%d", level);
        const char *f[] = {lv, ms, ns, as};
        const size_t fl[] = {lvn > 0 ? (size_t)lvn : 0, ml, nl, al};
        py_rec(sb, "I", 4, f, fl);
    }
}

/* ── conf.py, read as text (the field test's patterns) ───────────── */

static bool py_ws(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

static bool py_word(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

static size_t py_skip_ws(const char *s, size_t n, size_t i) {
    while (i < n && py_ws(s[i])) {
        i++;
    }
    return i;
}

/* `^name\s*=\s*` at a line start: the index after it, or 0. */
static size_t py_assign_at(const char *s, size_t n, size_t line, const char *name) {
    size_t nl = strlen(name);
    if (line + nl > n || memcmp(s + line, name, nl) != 0) {
        return 0;
    }
    size_t i = py_skip_ws(s, n, line + nl);
    if (i >= n || s[i] != '=') {
        return 0;
    }
    return py_skip_ws(s, n, i + 1);
}

/* conf_scalar: ^name\s*=\s*["']([^"']*)["'] (first match). */
static bool py_conf_scalar(const char *s, size_t n, const char *name, const char **v, size_t *vl) {
    for (size_t line = 0; line < n;) {
        size_t i = py_assign_at(s, n, line, name);
        if (i > 0 && i < n && (s[i] == '"' || s[i] == '\'')) {
            size_t j = i + 1;
            while (j < n && s[j] != '"' && s[j] != '\'') {
                j++;
            }
            if (j < n) {
                *v = s + i + 1;
                *vl = j - i - 1;
                return true;
            }
        }
        const char *nl = memchr(s + line, '\n', n - line);
        line = nl ? (size_t)(nl - s) + 1 : n;
    }
    return false;
}

/* conf_dict_block: ^name\s*=\s*\{(.*?)^\} -- the text between, or false. */
static bool py_conf_block(const char *s, size_t n, const char *name, size_t *a, size_t *b) {
    for (size_t line = 0; line < n;) {
        size_t i = py_assign_at(s, n, line, name);
        if (i > 0 && i < n && s[i] == '{') {
            size_t k = i + 1;
            while (k < n) {
                if (s[k] == '}' && (k == 0 || s[k - 1] == '\n')) {
                    *a = i + 1;
                    *b = k;
                    return true;
                }
                k++;
            }
            return false;
        }
        const char *nl = memchr(s + line, '\n', n - line);
        line = nl ? (size_t)(nl - s) + 1 : n;
    }
    return false;
}

/* ["']([\w.-]+)["'] at s[i] (extra: also '.'): its end after the closing quote, or 0. */
static size_t py_quoted_key(const char *s, size_t n, size_t i, bool dots, size_t *ka, size_t *kb) {
    if (i >= n || (s[i] != '"' && s[i] != '\'')) {
        return 0;
    }
    size_t j = i + 1;
    while (j < n && (py_word(s[j]) || s[j] == '-' || (dots && s[j] == '.'))) {
        j++;
    }
    if (j == i + 1 || j >= n || (s[j] != '"' && s[j] != '\'')) {
        return 0;
    }
    *ka = i + 1;
    *kb = j;
    return j + 1;
}

/* ["']([^"']+)["'] at s[i]: the value span, or false. */
static bool py_quoted_value(const char *s, size_t n, size_t i, size_t *va, size_t *vb) {
    if (i >= n || (s[i] != '"' && s[i] != '\'')) {
        return false;
    }
    size_t j = i + 1;
    while (j < n && s[j] != '"' && s[j] != '\'') {
        j++;
    }
    if (j == i + 1 || j >= n) {
        return false;
    }
    *va = i + 1;
    *vb = j;
    return true;
}

/* /(blob|tree)/[^/]+/%s in the URL: a repository path pattern. */
static bool py_repo_url(const char *u, size_t n) {
    static const char *const kinds[] = {"/blob/", "/tree/", NULL};
    for (int k = 0; kinds[k]; k++) {
        size_t kl = strlen(kinds[k]);
        for (size_t i = 0; i + kl <= n; i++) {
            if (memcmp(u + i, kinds[k], kl) != 0) {
                continue;
            }
            size_t j = i + kl;
            size_t ref = j;
            while (j < n && u[j] != '/') {
                j++;
            }
            if (j > ref && j + 3 <= n && memcmp(u + j, "/%s", 3) == 0) {
                return true;
            }
        }
    }
    return false;
}

static void py_extlink(py_sb_t *sb, const char *s, size_t ka, size_t kb, size_t va, size_t vb) {
    if (py_repo_url(s + va, vb - va) && py_field_ok(s + ka, kb - ka)) {
        const char *f[] = {s + ka};
        const size_t fl[] = {kb - ka};
        py_rec(sb, "X", 1, f, fl);
    }
}

static void py_conf(const char *s, size_t n, py_sb_t *sb) {
    py_rec(sb, "C", 0, NULL, NULL);
    const char *v;
    size_t vl;
    if (py_conf_scalar(s, n, "primary_domain", &v, &vl) && py_field_ok(v, vl)) {
        const char *f[] = {v};
        const size_t fl[] = {vl};
        py_rec(sb, "D", 1, f, fl);
    }
    size_t a;
    size_t b;
    if (py_conf_block(s, n, "intersphinx_mapping", &a, &b)) {
        for (size_t i = a; i < b; i++) {
            size_t ka;
            size_t kb;
            size_t e = py_quoted_key(s, b, i, true, &ka, &kb);
            if (e == 0) {
                continue;
            }
            size_t c = py_skip_ws(s, b, e);
            if (c < b && s[c] == ':' && (c = py_skip_ws(s, b, c + 1)) < b && s[c] == '(' &&
                py_field_ok(s + ka, kb - ka)) {
                const char *f[] = {s + ka};
                const size_t fl[] = {kb - ka};
                py_rec(sb, "S", 1, f, fl);
            }
            i = e - 1;
        }
    }
    /* extlinks = {'role': ('url%s', ...)} and extlinks['role'] = ('url%s', ...) */
    if (py_conf_block(s, n, "extlinks", &a, &b)) {
        for (size_t i = a; i < b; i++) {
            size_t ka;
            size_t kb;
            size_t e = py_quoted_key(s, b, i, false, &ka, &kb);
            if (e == 0) {
                continue;
            }
            size_t c = py_skip_ws(s, b, e);
            size_t va;
            size_t vb;
            if (c < b && s[c] == ':' && (c = py_skip_ws(s, b, c + 1)) < b && s[c] == '(' &&
                py_quoted_value(s, b, py_skip_ws(s, b, c + 1), &va, &vb)) {
                py_extlink(sb, s, ka, kb, va, vb);
            }
            i = e - 1;
        }
    }
    static const char key_open[] = "extlinks[";
    size_t kol = strlen(key_open);
    for (size_t at = 0; at + kol <= n; at++) {
        if (memcmp(s + at, key_open, kol) != 0) {
            continue;
        }
        size_t i = at + kol;
        size_t ka;
        size_t kb;
        size_t e = py_quoted_key(s, n, i, false, &ka, &kb);
        if (e == 0 || e >= n || s[e] != ']') {
            continue;
        }
        size_t c = py_skip_ws(s, n, e + 1);
        size_t va;
        size_t vb;
        if (c < n && s[c] == '=' && (c = py_skip_ws(s, n, c + 1)) < n && s[c] == '(' &&
            py_quoted_value(s, n, py_skip_ws(s, n, c + 1), &va, &vb)) {
            py_extlink(sb, s, ka, kb, va, vb);
        }
    }
}

const char *cbm_doclink_py_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->rel_path || !ctx->source) {
        return NULL;
    }
    const char *base = py_base(ctx->rel_path);
    bool init = strcmp(base, "__init__.py") == 0;
    bool conf = strcmp(base, "conf.py") == 0;
    if (!init && !conf) {
        return NULL;
    }
    py_sb_t sb = {.a = ctx->arena};
    py_put(&sb, CBM_DOCLINK_PY_SCOPE_TAG "\n", strlen(CBM_DOCLINK_PY_SCOPE_TAG "\n"));
    if (conf) {
        py_conf(ctx->source, (size_t)ctx->source_len, &sb);
    }
    if (init && !ts_node_is_null(ctx->root)) {
        uint32_t nc = ts_node_child_count(ctx->root);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_child(ctx->root, i);
            if (strcmp(ts_node_type(c), "import_from_statement") == 0) {
                py_from_import(ctx, c, &sb);
            }
        }
    }
    if (sb.failed) {
        ctx->result->doc_links.failed = true;
        return NULL;
    }
    return sb.buf;
}
