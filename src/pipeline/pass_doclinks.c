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
#include "foundation/mem_core.h"
#include "foundation/dyn_array.h"

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
    DOCLINK_MAX_SEGS = CBM_SZ_64, /* path segments during normalization */
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

/* ── File reading (mirrors pass_definitions.c read_file, minus TS pad) ──
 *
 * Reports *out_status so the caller can attribute a skip to the right
 * reason (open failure vs. oversized vs. OOM) instead of a silent drop —
 * hitting a limit here degrades to a REPORTED skip, per limits.h. */
static char *doclink_read_file(const char *path, long *out_size, cbm_read_status_t *out_status) {
    if (out_size) {
        *out_size = 0;
    }
    if (out_status) {
        *out_status = CBM_READ_OK;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        if (out_status) {
            *out_status = CBM_READ_OPEN_FAIL;
        }
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (out_size) {
        *out_size = size;
    }
    if (size <= 0) {
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_EMPTY;
        }
        return NULL;
    }
    if (size > cbm_max_file_bytes()) {
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_OVERSIZED;
        }
        return NULL;
    }
    char *buf = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        if (out_status) {
            *out_status = CBM_READ_OOM;
        }
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

/* One raw match before dedup. Collected in a growable vector (no per-file
 * cap) and collapsed by doclink_flush(). */
typedef struct {
    int64_t target_id;
    double confidence;
    const char *strategy; /* static string literal */
} doclink_match_t;

typedef struct {
    cbm_gbuf_t *gb;
    CBMHashTable *files_by_path; /* rel_path → cbm_gbuf_node_t* (borrowed) */
    const cbm_gbuf_node_t *src;  /* referencing File node */
    char src_dir[CBM_SZ_512];    /* its directory ("" at repo root) */
    CBM_DYN_ARRAY(doclink_match_t) matches;
} doclink_ctx_t;

/* How a reference was written, which decides where it may resolve. Mixing
 * these up let a rooted "/x" still match a same-named file in the
 * referencing directory, and let an explicit "./x" fall back to an
 * unrelated same-named file at the repo root. */
typedef enum {
    DOCLINK_REF_BARE = 0, /* "x": referencing dir, then repo root */
    DOCLINK_REF_RELATIVE, /* "./x": referencing dir only */
    DOCLINK_REF_ROOTED,   /* "/x": repo root only */
} doclink_ref_kind_t;

/* Resolve a reference against the referencing file's directory and/or the
 * repo root, per its kind. Returns the already-indexed File node or NULL —
 * unresolvable references are dropped. */
static const cbm_gbuf_node_t *doclink_resolve(doclink_ctx_t *dc, const char *ref) {
    const char *r = ref;
    doclink_ref_kind_t kind = DOCLINK_REF_BARE;
    if (r[0] == '.' && r[SKIP_ONE] == '/') {
        kind = DOCLINK_REF_RELATIVE;
        while (r[0] == '.' && r[SKIP_ONE] == '/') {
            r += PAIR_LEN;
        }
    } else if (r[0] == '/') {
        kind = DOCLINK_REF_ROOTED;
        r++; /* "/docs/x.md" is repo-root-relative by doc convention */
    }
    if (r[0] == '\0') {
        return NULL;
    }

    char norm[CBM_SZ_512];
    if (kind != DOCLINK_REF_ROOTED && dc->src_dir[0] != '\0') {
        char joined[CBM_SZ_512];
        int n = snprintf(joined, sizeof(joined), "%s/%s", dc->src_dir, r);
        if (n > 0 && (size_t)n < sizeof(joined) && doclink_normalize(joined, norm, sizeof(norm))) {
            const cbm_gbuf_node_t *node = cbm_ht_get(dc->files_by_path, norm);
            if (node) {
                return node;
            }
        }
    }
    if (kind != DOCLINK_REF_RELATIVE && doclink_normalize(r, norm, sizeof(norm))) {
        return cbm_ht_get(dc->files_by_path, norm);
    }
    return NULL;
}

/* Record one match. No cap, no dedup here — dc->matches is a plain append
 * log; doclink_flush() sorts and collapses same-target matches so a
 * generated file with hundreds of distinct targets loses nothing. */
static void doclink_record(doclink_ctx_t *dc, const cbm_gbuf_node_t *target, double confidence,
                           const char *strategy) {
    if (!target || target->id == dc->src->id) {
        return; /* never self-reference */
    }
    doclink_match_t m = {.target_id = target->id, .confidence = confidence, .strategy = strategy};
    cbm_da_push(&dc->matches, m);
}

static int doclink_match_cmp(const void *a, const void *b) {
    int64_t ta = ((const doclink_match_t *)a)->target_id;
    int64_t tb = ((const doclink_match_t *)b)->target_id;
    return (ta > tb) - (ta < tb);
}

/* Emit accumulated references as REFERENCES_FILE edges. Sorts by target id
 * (O(M log M)) and collapses same-target runs: count = occurrences, the
 * highest-confidence match in the run wins strategy + confidence. Returns
 * edge count. */
static int doclink_flush(doclink_ctx_t *dc) {
    if (dc->matches.count > 1) {
        qsort(dc->matches.items, (size_t)dc->matches.count, sizeof(dc->matches.items[0]),
              doclink_match_cmp);
    }

    int emitted = 0;
    int i = 0;
    while (i < dc->matches.count) {
        int64_t target_id = dc->matches.items[i].target_id;
        double confidence = dc->matches.items[i].confidence;
        const char *strategy = dc->matches.items[i].strategy;
        int count = SKIP_ONE;
        int j = i + SKIP_ONE;
        while (j < dc->matches.count && dc->matches.items[j].target_id == target_id) {
            count++;
            if (dc->matches.items[j].confidence > confidence) {
                confidence = dc->matches.items[j].confidence;
                strategy = dc->matches.items[j].strategy;
            }
            j++;
        }

        char props[CBM_SZ_256];
        (void)snprintf(props, sizeof(props),
                       "{\"strategy\":\"%s\",\"confidence\":%.2f,\"count\":%d}", strategy,
                       confidence, count);
        if (cbm_gbuf_insert_edge(dc->gb, dc->src->id, target_id, DOCLINK_EDGE_TYPE, props) > 0) {
            emitted++;
        }
        i = j;
    }
    cbm_da_clear(&dc->matches);
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

/* Scan one referencing file's content line by line and emit its edges.
 * `source` is the caller's private mutable buffer (freshly read for this
 * file, never reused): lines are terminated in place ('\n' -> '\0') and
 * scanned by pointer, so there is no fixed-size copy and no line-length
 * cap to straddle a token across (a prior CBM_SZ_4K copy buffer could cut
 * "src/foo.cpp" to "src/foo.c" at the boundary and bind to the wrong
 * file). A trailing '\r' (CRLF) is trimmed the same way. */
static int doclink_scan_file(doclink_ctx_t *dc, const cbm_gbuf_node_t *node, char *source) {
    dc->src = node;
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

    char *p = source;
    while (*p) {
        char *line = p;
        char *eol = strchr(p, '\n');
        if (eol) {
            *eol = '\0';
            p = eol + SKIP_ONE;
        } else {
            p += strlen(p);
        }
        size_t line_len = strlen(line);
        if (line_len > 0 && line[line_len - SKIP_ONE] == '\r') {
            line[line_len - SKIP_ONE] = '\0';
        }
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
 * md_edges receives the emitted edge count. Checks cancellation once per
 * file — on a docs-heavy repo this is the whole pass's cancel latency —
 * and every skip is reported via cbm_pipeline_add_file_error, never
 * silent. */
static void doclink_scan_repo(cbm_pipeline_ctx_t *ctx, doclink_ctx_t *dc, const char *repo_path,
                              const cbm_gbuf_node_t *const *files, int file_count, int *md_edges) {
    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return;
        }
        if (!files[i]->file_path || !doclink_is_markdown_path(files[i]->file_path)) {
            continue;
        }

        char abs_path[CBM_PATH_MAX];
        int n = snprintf(abs_path, sizeof(abs_path), "%s/%s", repo_path, files[i]->file_path);
        if (n <= 0 || (size_t)n >= sizeof(abs_path)) {
            continue;
        }
        long file_size = 0;
        cbm_read_status_t rst = CBM_READ_OK;
        char *source = doclink_read_file(abs_path, &file_size, &rst);
        if (!source) {
            if (rst == CBM_READ_OVERSIZED) {
                long cap = cbm_max_file_bytes();
                char reason[96];
                (void)snprintf(reason, sizeof(reason), "oversized (%lld MB > %lld MB)",
                               (long long)(file_size / (CBM_SZ_1K * CBM_SZ_1K)),
                               (long long)(cap / (CBM_SZ_1K * CBM_SZ_1K)));
                cbm_pipeline_add_file_error(ctx->pipeline, files[i]->file_path, reason,
                                            "oversized");
                cbm_log_warn("doclinks.file_oversized", "path", files[i]->file_path);
            } else if (rst == CBM_READ_OPEN_FAIL || rst == CBM_READ_OOM) {
                cbm_pipeline_add_file_error(ctx->pipeline, files[i]->file_path, "read failed",
                                            "read");
                cbm_log_warn("doclinks.file_unreadable", "path", files[i]->file_path);
            }
            /* CBM_READ_EMPTY: benign 0-byte file, nothing to index, not reported. */
            continue;
        }
        int emitted = doclink_scan_file(dc, files[i], source);
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
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

    /* Early exit: no markdown files means nothing to scan. */
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
    doclink_scan_repo(ctx, &dc, ctx->repo_path, files, file_count, &md_edges);
    cbm_da_free(&dc.matches);
    cbm_ht_free(dc.files_by_path);

    char buf1[CBM_SZ_16];
    (void)snprintf(buf1, sizeof(buf1), "%d", md_edges);
    cbm_log_info("doclinks.strategy", "name", "markdown", "edges", buf1);
    cbm_log_info("doclinks.done", "total", buf1);

    return md_edges;
}
