/*
 * pass_doclinks.c — Documentation → file reference linking (pre-dump pass).
 *
 * Markdown docs reference other repo files constantly — a coding-standards
 * doc links to the modules it governs, a README points at entry points —
 * but none of that surfaced as graph edges, so fan-in queries were blind to
 * documentation hubs: on a docs-heavy repo the top fan-in answer was off by
 * an order of magnitude because the most-referenced doc had zero inbound
 * edges.
 *
 * Three strategies emit REFERENCES_FILE edges between EXISTING File nodes
 * (targets that don't resolve to an indexed file are dropped — the pass
 * never invents nodes):
 *   MD 1. Inline link:    [text](relative/path.ext)   (not http/mailto/#anchor)
 *   MD 2. Backtick path:  `path/with/slash.ext` or `file.ext`
 *   MD 3. Bare mention:   relative/path.ext            (slash + extension)
 *
 * Targets resolve relative to the referencing file's directory AND the repo
 * root (docs are written both ways). Repeated references between the same
 * file pair are collapsed to one edge carrying a "count" property; the edge
 * keeps the highest-confidence strategy that matched.
 *
 * Operates on the graph buffer before dump to .db file.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/compat_fs.h"
#include "foundation/limits.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define SLEN(s) (sizeof(s) - SKIP_ONE)

/* ── Doc link confidence scores ──────────────────────────────────── */
/* Markdown strategies */
#define DOCLINK_MD_INLINE 0.95
#define DOCLINK_MD_BACKTICK 0.85
#define DOCLINK_MD_BARE 0.70

/* Edge type emitted by this pass. */
#define DOCLINK_EDGE_TYPE "REFERENCES_FILE"

enum {
    DOCLINK_MAX_REFS = CBM_SZ_256, /* distinct targets per referencing file */
    DOCLINK_MAX_SEGS = CBM_SZ_64,  /* path segments during normalization */
};

/* ── Path classification ─────────────────────────────────────────── */

/* Extension of the path's basename (including the dot), or NULL. */
static const char *doclink_path_ext(const char *path) {
    if (!path) {
        return NULL;
    }
    const char *base = strrchr(path, '/');
    base = base ? base + SKIP_ONE : path;
    return strrchr(base, '.');
}

static bool doclink_is_markdown_path(const char *path) {
    const char *ext = doclink_path_ext(path);
    return ext && (strcmp(ext, ".md") == 0 || strcmp(ext, ".mdx") == 0);
}

/* ── File reading (mirrors pass_semantic.c read_file, minus TS pad) ── */

static char *doclink_read_file(const char *path) {
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
    char *buf = malloc((size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    buf[nread] = '\0';
    return buf;
}

/* ── Path normalization + resolution ─────────────────────────────── */

/* Normalize "a/./b/../c" into "a/c". Rejects paths that escape the repo
 * root (leading ".."), empty results, and over-long/over-deep inputs. */
static bool doclink_normalize(const char *in, char *out, size_t out_sz) {
    size_t seg_starts[DOCLINK_MAX_SEGS];
    int depth = 0;
    size_t out_len = 0;
    const char *p = in;
    out[0] = '\0';
    while (*p) {
        const char *seg = p;
        const char *slash = strchr(p, '/');
        size_t seg_len = slash ? (size_t)(slash - p) : strlen(p);
        p = slash ? slash + SKIP_ONE : p + seg_len;
        if (seg_len == 0 || (seg_len == SKIP_ONE && seg[0] == '.')) {
            continue;
        }
        if (seg_len == PAIR_LEN && seg[0] == '.' && seg[SKIP_ONE] == '.') {
            if (depth == 0) {
                return false; /* escapes the repo root */
            }
            depth--;
            out_len = seg_starts[depth];
            out[out_len] = '\0';
            continue;
        }
        if (depth >= DOCLINK_MAX_SEGS || out_len + seg_len + PAIR_LEN >= out_sz) {
            return false;
        }
        seg_starts[depth++] = out_len;
        if (out_len > 0) {
            out[out_len++] = '/';
        }
        memcpy(out + out_len, seg, seg_len);
        out_len += seg_len;
        out[out_len] = '\0';
    }
    return out_len > 0;
}

/* Per-file accumulator: dedupes repeated references to the same target. */
typedef struct {
    int64_t target_id;
    int count;
    double confidence;
    const char *strategy; /* static string literal */
} doclink_ref_t;

typedef struct {
    cbm_gbuf_t *gb;
    CBMHashTable *files_by_path; /* rel_path → cbm_gbuf_node_t* (borrowed) */
    const cbm_gbuf_node_t *src;  /* referencing File node */
    char src_dir[CBM_SZ_512];    /* its directory ("" at repo root) */
    doclink_ref_t refs[DOCLINK_MAX_REFS];
    int ref_count;
    bool truncated;
} doclink_ctx_t;

/* Resolve a reference against the referencing file's directory, then the
 * repo root, mirroring how humans write doc links. Returns the already-
 * indexed File node or NULL — unresolvable references are dropped. */
static const cbm_gbuf_node_t *doclink_resolve(doclink_ctx_t *dc, const char *ref) {
    const char *r = ref;
    while (r[0] == '.' && r[SKIP_ONE] == '/') {
        r += PAIR_LEN;
    }
    if (r[0] == '/') {
        r++; /* "/docs/x.md" is repo-root-relative by doc convention */
    }
    if (r[0] == '\0') {
        return NULL;
    }

    char norm[CBM_SZ_512];
    if (dc->src_dir[0] != '\0') {
        char joined[CBM_SZ_512];
        int n = snprintf(joined, sizeof(joined), "%s/%s", dc->src_dir, r);
        if (n > 0 && (size_t)n < sizeof(joined) && doclink_normalize(joined, norm, sizeof(norm))) {
            const cbm_gbuf_node_t *node = cbm_ht_get(dc->files_by_path, norm);
            if (node) {
                return node;
            }
        }
    }
    if (doclink_normalize(r, norm, sizeof(norm))) {
        return cbm_ht_get(dc->files_by_path, norm);
    }
    return NULL;
}

/* Record one match. Same-pair repeats bump the count; a higher-confidence
 * strategy upgrades the edge's confidence + strategy label. */
static void doclink_record(doclink_ctx_t *dc, const cbm_gbuf_node_t *target, double confidence,
                           const char *strategy) {
    if (!target || target->id == dc->src->id) {
        return; /* never self-reference */
    }
    for (int i = 0; i < dc->ref_count; i++) {
        if (dc->refs[i].target_id == target->id) {
            dc->refs[i].count++;
            if (confidence > dc->refs[i].confidence) {
                dc->refs[i].confidence = confidence;
                dc->refs[i].strategy = strategy;
            }
            return;
        }
    }
    if (dc->ref_count >= DOCLINK_MAX_REFS) {
        dc->truncated = true;
        return;
    }
    dc->refs[dc->ref_count].target_id = target->id;
    dc->refs[dc->ref_count].count = SKIP_ONE;
    dc->refs[dc->ref_count].confidence = confidence;
    dc->refs[dc->ref_count].strategy = strategy;
    dc->ref_count++;
}

/* Emit accumulated references as REFERENCES_FILE edges. Returns edge count. */
static int doclink_flush(doclink_ctx_t *dc) {
    int emitted = 0;
    for (int i = 0; i < dc->ref_count; i++) {
        char props[CBM_SZ_256];
        (void)snprintf(props, sizeof(props),
                       "{\"strategy\":\"%s\",\"confidence\":%.2f,\"count\":%d}",
                       dc->refs[i].strategy, dc->refs[i].confidence, dc->refs[i].count);
        if (cbm_gbuf_insert_edge(dc->gb, dc->src->id, dc->refs[i].target_id, DOCLINK_EDGE_TYPE,
                                 props) > 0) {
            emitted++;
        }
    }
    if (dc->truncated) {
        char cap_buf[CBM_SZ_16];
        (void)snprintf(cap_buf, sizeof(cap_buf), "%d", DOCLINK_MAX_REFS);
        cbm_log_info("doclinks.truncated", "file", dc->src->file_path ? dc->src->file_path : "",
                     "cap", cap_buf);
    }
    dc->ref_count = 0;
    dc->truncated = false;
    return emitted;
}

/* ── Markdown scanning ───────────────────────────────────────────── */

/* Characters allowed in a path-shaped token (backtick / bare mention). */
static bool doclink_token_pathlike(const char *tok) {
    bool last_seg_has_dot = false;
    for (const char *p = tok; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '/') {
            last_seg_has_dot = false;
            continue;
        }
        if (c == '.') {
            last_seg_has_dot = true;
            continue;
        }
        if (!isalnum(c) && c != '_' && c != '-' && c != '+' && c != '@' && c != '~') {
            return false;
        }
    }
    /* the basename must carry an extension — bare words are not paths */
    return last_seg_has_dot;
}

/* A markdown link target worth resolving: not a URL, mailto, or pure anchor. */
static bool doclink_md_target_ok(const char *target) {
    if (target[0] == '\0' || target[0] == '#') {
        return false;
    }
    if (strstr(target, "://") != NULL || strncmp(target, "mailto:", SLEN("mailto:")) == 0) {
        return false;
    }
    return true;
}

/* MD 1: inline links [text](target). Consumed spans are blanked so the
 * backtick / bare-mention scans below cannot re-match the same path. */
static void doclink_scan_md_links(doclink_ctx_t *dc, char *line) {
    char *p = line;
    while ((p = strstr(p, "](")) != NULL) {
        char *close = strchr(p + PAIR_LEN, ')');
        if (!close) {
            return;
        }
        char target[CBM_SZ_512];
        size_t tlen = (size_t)(close - (p + PAIR_LEN));
        if (tlen < sizeof(target)) {
            memcpy(target, p + PAIR_LEN, tlen);
            target[tlen] = '\0';
            char *cut = strchr(target, ' '); /* [t](path "title") */
            if (cut) {
                *cut = '\0';
            }
            cut = strchr(target, '#'); /* [t](path#anchor) */
            if (cut) {
                *cut = '\0';
            }
            if (doclink_md_target_ok(target)) {
                doclink_record(dc, doclink_resolve(dc, target), DOCLINK_MD_INLINE,
                               "md_inline_link");
            }
        }
        /* blank the whole [text](target) span, link text included, so a
         * path-shaped link text is not re-counted as a bare mention */
        char *open = p;
        while (open > line && *open != '[') {
            open--;
        }
        if (*open != '[') {
            open = p;
        }
        memset(open, ' ', (size_t)(close - open) + SKIP_ONE);
        p = close + SKIP_ONE;
    }
}

/* MD 2: backtick-quoted paths `src/foo.c` / `build.sh`. */
static void doclink_scan_md_backticks(doclink_ctx_t *dc, char *line) {
    char *p = line;
    while ((p = strchr(p, '`')) != NULL) {
        char *end = strchr(p + SKIP_ONE, '`');
        if (!end) {
            return;
        }
        char tok[CBM_SZ_512];
        size_t tlen = (size_t)(end - (p + SKIP_ONE));
        if (tlen > 0 && tlen < sizeof(tok)) {
            memcpy(tok, p + SKIP_ONE, tlen);
            tok[tlen] = '\0';
            if (doclink_token_pathlike(tok)) {
                doclink_record(dc, doclink_resolve(dc, tok), DOCLINK_MD_BACKTICK,
                               "md_backtick_path");
            }
        }
        memset(p, ' ', (size_t)(end - p) + SKIP_ONE);
        p = end + SKIP_ONE;
    }
}

static bool doclink_md_delim(char c) {
    return isspace((unsigned char)c) || strchr("()[]{}<>\"',;:`*|", c) != NULL;
}

/* MD 3: bare relative path mentions — must contain a slash AND an extension
 * (and, via doclink_resolve, an indexed file) to count. */
static void doclink_scan_md_bare(doclink_ctx_t *dc, const char *line) {
    const char *p = line;
    while (*p) {
        while (*p && doclink_md_delim(*p)) {
            p++;
        }
        const char *start = p;
        while (*p && !doclink_md_delim(*p)) {
            p++;
        }
        size_t tlen = (size_t)(p - start);
        char tok[CBM_SZ_512];
        if (tlen == 0 || tlen >= sizeof(tok)) {
            continue;
        }
        memcpy(tok, start, tlen);
        tok[tlen] = '\0';
        while (tlen > 0 && tok[tlen - SKIP_ONE] == '.') {
            tok[--tlen] = '\0'; /* sentence-ending period */
        }
        if (strchr(tok, '/') != NULL && doclink_token_pathlike(tok)) {
            doclink_record(dc, doclink_resolve(dc, tok), DOCLINK_MD_BARE, "md_bare_mention");
        }
    }
}

static void doclink_scan_md_line(doclink_ctx_t *dc, char *line) {
    doclink_scan_md_links(dc, line);
    doclink_scan_md_backticks(dc, line);
    doclink_scan_md_bare(dc, line);
}

/* ── Per-file driver ─────────────────────────────────────────────── */

/* Scan one referencing file's content line by line and emit its edges. */
static int doclink_scan_file(doclink_ctx_t *dc, const cbm_gbuf_node_t *node, const char *source) {
    dc->src = node;
    dc->ref_count = 0;
    dc->truncated = false;
    dc->src_dir[0] = '\0';
    const char *slash = strrchr(node->file_path, '/');
    if (slash) {
        size_t dlen = (size_t)(slash - node->file_path);
        if (dlen >= sizeof(dc->src_dir)) {
            return 0;
        }
        memcpy(dc->src_dir, node->file_path, dlen);
        dc->src_dir[dlen] = '\0';
    }

    const char *p = source;
    char line[CBM_SZ_4K];
    while (*p) {
        const char *eol = strchr(p, '\n');
        size_t line_len = eol ? (size_t)(eol - p) : strlen(p);
        if (line_len >= sizeof(line)) {
            line_len = sizeof(line) - SKIP_ONE;
        }
        memcpy(line, p, line_len);
        line[line_len] = '\0';
        p = eol ? eol + SKIP_ONE : p + line_len;

        doclink_scan_md_line(dc, line);
    }
    return doclink_flush(dc);
}

/* ── Pass entry point ────────────────────────────────────────────── */

/* True when at least one File node is a markdown file. */
static bool doclink_has_doc_files(const cbm_gbuf_node_t *const *files, int file_count) {
    for (int i = 0; i < file_count; i++) {
        if (doclink_is_markdown_path(files[i]->file_path)) {
            return true;
        }
    }
    return false;
}

/* Scan every markdown File node's on-disk content, emitting edges.
 * md_edges receives the emitted edge count. */
static void doclink_scan_repo(doclink_ctx_t *dc, const char *repo_path,
                              const cbm_gbuf_node_t *const *files, int file_count, int *md_edges) {
    for (int i = 0; i < file_count; i++) {
        if (!files[i]->file_path || !doclink_is_markdown_path(files[i]->file_path)) {
            continue;
        }

        char abs_path[CBM_PATH_MAX];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, files[i]->file_path);
        if (n <= 0 || (size_t)n >= sizeof(abs_path)) {
            continue;
        }
        char *source = doclink_read_file(abs_path);
        if (!source) {
            continue;
        }
        int emitted = doclink_scan_file(dc, files[i], source);
        free(source);
        *md_edges += emitted;
    }
}

int cbm_pipeline_pass_doclinks(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;

    const cbm_gbuf_node_t **files = NULL;
    int file_count = 0;
    if (cbm_gbuf_find_by_label(gb, "File", &files, &file_count) != 0 || file_count == 0) {
        return 0;
    }

    /* Early exit: no markdown/shell files means nothing to scan. */
    if (!doclink_has_doc_files(files, file_count)) {
        cbm_log_info("doclinks.skip", "reason", "no_doc_files");
        return 0;
    }
    if (!ctx->repo_path) {
        cbm_log_info("doclinks.skip", "reason", "no_repo_path");
        return 0;
    }

    doclink_ctx_t dc;
    memset(&dc, 0, sizeof(dc));
    dc.gb = gb;
    dc.files_by_path = cbm_ht_create((uint32_t)file_count);
    if (!dc.files_by_path) {
        return 0;
    }
    for (int i = 0; i < file_count; i++) {
        if (files[i]->file_path) {
            /* key borrowed from the node (owned by gbuf, outlives the pass) */
            cbm_ht_set(dc.files_by_path, files[i]->file_path, (void *)files[i]);
        }
    }

    int md_edges = 0;
    doclink_scan_repo(&dc, ctx->repo_path, files, file_count, &md_edges);
    cbm_ht_free(dc.files_by_path);

    char buf1[CBM_SZ_16];
    (void)snprintf(buf1, sizeof(buf1), "%d", md_edges);
    cbm_log_info("doclinks.strategy", "name", "markdown", "edges", buf1);
    cbm_log_info("doclinks.done", "total", buf1);

    return md_edges;
}
