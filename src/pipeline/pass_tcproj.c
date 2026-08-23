/*
 * pass_tcproj.c — Pipeline pass for TwinCAT project files.
 *
 * For every file (post-extraction):
 *   1. .plcproj  → one "Package" node per PLC project, DEPENDS_ON edges to
 *      the referenced libraries (PlaceholderReference/LibraryReference), and
 *      CONTAINS_FILE edges to the member files (Compile Include=).
 *   2. .tsproj   → DEPENDS_ON edges from the solution file's File node to
 *      each referenced PLC project's Package node, and CONFIGURES edges to
 *      referenced .xti I/O device descriptions.
 *
 * .plcproj files are handled before .tsproj files so the Package nodes the
 * solution references already exist. The XML is scanned with the same
 * hand-rolled tag/attribute idiom as the pom.xml handling in pass_pkgmap.c
 * and the manifest handlers in pass_k8s.c — no XML library.
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/limits.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Internal helpers ────────────────────────────────────────────── */

static char itoa_bufs_tc[4][16];
static int itoa_tc_idx = 0;
static const char *itoa_tc(int v) {
    itoa_tc_idx = (itoa_tc_idx + SKIP_ONE) & 3;
    snprintf(itoa_bufs_tc[itoa_tc_idx], sizeof(itoa_bufs_tc[itoa_tc_idx]), "%d", v);
    return itoa_bufs_tc[itoa_tc_idx];
}

static const char *tcproj_basename(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + SKIP_ONE : path;
}

static bool has_suffix_icase(const char *name, const char *suffix) {
    size_t nl = strlen(name);
    size_t sl = strlen(suffix);
    if (nl < sl) {
        return false;
    }
    const char *tail = name + nl - sl;
    for (size_t i = 0; i < sl; i++) {
        char a = tail[i];
        char b = suffix[i];
        if (a >= 'A' && a <= 'Z') {
            a = (char)(a - 'A' + 'a');
        }
        if (b >= 'A' && b <= 'Z') {
            b = (char)(b - 'A' + 'a');
        }
        if (a != b) {
            return false;
        }
    }
    return true;
}

bool cbm_is_plcproj_file(const char *basename) {
    return has_suffix_icase(basename, ".plcproj");
}

bool cbm_is_tsproj_file(const char *basename) {
    return has_suffix_icase(basename, ".tsproj");
}

/* Read entire file into a heap buffer (same caps as the k8s pass). */
static char *tcproj_read_file(const char *path, int *out_len) {
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
    buf[nread] = '\0';
    if (out_len) {
        *out_len = (int)nread;
    }
    return buf;
}

/* Extract attr="value" inside the tag that starts at `tag` (bounded by '>').
 * Returns false when the attribute is absent. */
static bool tag_attr(const char *tag, const char *attr, char *out, size_t out_sz) {
    out[0] = '\0';
    const char *gt = strchr(tag, '>');
    if (!gt) {
        return false;
    }
    size_t an = strlen(attr);
    for (const char *p = tag; p + an < gt; p++) {
        if (strncmp(p, attr, an) == 0 && p[an] == '=' && (p[an + 1] == '"' || p[an + 1] == '\'')) {
            char q = p[an + 1];
            const char *v = p + an + 2;
            const char *ve = v;
            while (ve < gt && *ve != q) {
                ve++;
            }
            size_t vl = (size_t)(ve - v);
            if (vl >= out_sz) {
                vl = out_sz - SKIP_ONE;
            }
            memcpy(out, v, vl);
            out[vl] = '\0';
            return true;
        }
    }
    return false;
}

/* Join the directory of `anchor_rel` with a project-file relative reference
 * (backslash separators, possible `..`/`.` segments) into a normalized
 * repo-relative path. */
static void resolve_ref_path(const char *anchor_rel, const char *ref, char *out, size_t out_sz) {
    char joined[CBM_SZ_1K];
    const char *slash = strrchr(anchor_rel, '/');
    if (slash) {
        size_t dl = (size_t)(slash - anchor_rel);
        if (dl >= sizeof(joined) - 2) {
            dl = sizeof(joined) - 2;
        }
        memcpy(joined, anchor_rel, dl);
        joined[dl] = '/';
        joined[dl + 1] = '\0';
    } else {
        joined[0] = '\0';
    }
    size_t jl = strlen(joined);
    for (const char *p = ref; *p && jl + 1 < sizeof(joined); p++) {
        joined[jl++] = (*p == '\\') ? '/' : *p;
    }
    joined[jl] = '\0';

    /* Normalize `.` and `..` segments with a component stack. */
    char *segs[64];
    int nsegs = 0;
    char work[CBM_SZ_1K];
    snprintf(work, sizeof(work), "%s", joined);
    for (char *tok = strtok(work, "/"); tok && nsegs < 64; tok = strtok(NULL, "/")) {
        if (strcmp(tok, ".") == 0 || tok[0] == '\0') {
            continue;
        }
        if (strcmp(tok, "..") == 0) {
            if (nsegs > 0) {
                nsegs--;
            }
            continue;
        }
        segs[nsegs++] = tok;
    }
    out[0] = '\0';
    size_t ol = 0;
    for (int i = 0; i < nsegs && ol + strlen(segs[i]) + 2 < out_sz; i++) {
        if (i > 0) {
            out[ol++] = '/';
        }
        size_t sl = strlen(segs[i]);
        memcpy(out + ol, segs[i], sl);
        ol += sl;
        out[ol] = '\0';
    }
}

/* Find the graph File node for a repo-relative path. Returns NULL if the file
 * was not indexed. */
static const cbm_gbuf_node_t *find_file_node(cbm_pipeline_ctx_t *ctx, const char *rel_path) {
    char *qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *node = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
    free(qn);
    return node;
}

/* Strip the extension from a basename into `out`. */
static void file_stem(const char *base, char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s", base);
    char *dot = strrchr(out, '.');
    if (dot && dot != out) {
        *dot = '\0';
    }
}

static void plc_package_qn(cbm_pipeline_ctx_t *ctx, const char *stem, char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s.__plc__.%s", ctx->project_name, stem);
}

/* ── .plcproj handler ────────────────────────────────────────────── */

static void handle_plcproj(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source) {
    char stem[CBM_SZ_256];
    file_stem(tcproj_basename(rel_path), stem, sizeof(stem));

    char pkg_qn[CBM_SZ_512];
    plc_package_qn(ctx, stem, pkg_qn, sizeof(pkg_qn));
    int64_t pkg_id = cbm_gbuf_upsert_node(ctx->gbuf, "Package", stem, pkg_qn, rel_path, SKIP_ONE, 0,
                                          "{\"source\":\"twincat\"}");
    if (pkg_id <= 0) {
        return;
    }
    const cbm_gbuf_node_t *pkg = cbm_gbuf_find_by_id(ctx->gbuf, pkg_id);
    if (!pkg) {
        return;
    }

    int members = 0;
    int deps = 0;
    for (const char *p = source; (p = strstr(p, "<Compile")) != NULL; p++) {
        char include[CBM_SZ_512];
        if (!tag_attr(p, "Include", include, sizeof(include)) || !include[0]) {
            continue;
        }
        char member_rel[CBM_SZ_1K];
        resolve_ref_path(rel_path, include, member_rel, sizeof(member_rel));
        const cbm_gbuf_node_t *file_node = find_file_node(ctx, member_rel);
        if (file_node && file_node->id != pkg_id) {
            cbm_gbuf_insert_edge(ctx->gbuf, pkg_id, file_node->id, "CONTAINS_FILE", "{}");
            members++;
        }
    }

    static const char *ref_tags[] = {"<PlaceholderReference", "<LibraryReference", NULL};
    for (int t = 0; ref_tags[t]; t++) {
        for (const char *p = source; (p = strstr(p, ref_tags[t])) != NULL; p++) {
            char lib[CBM_SZ_256];
            if (!tag_attr(p, "Include", lib, sizeof(lib)) || !lib[0]) {
                continue;
            }
            /* LibraryReference Include="Name,version,vendor" — keep the name */
            char *comma = strchr(lib, ',');
            if (comma) {
                *comma = '\0';
            }
            char dep_qn[CBM_SZ_512];
            snprintf(dep_qn, sizeof(dep_qn), "%s.__plclib_dep__.%s", ctx->project_name, lib);
            int64_t dep_id =
                cbm_gbuf_upsert_node(ctx->gbuf, "Package", lib, dep_qn, rel_path, SKIP_ONE, 0,
                                     "{\"source\":\"twincat-library\",\"external\":true}");
            if (dep_id > 0 && dep_id != pkg_id) {
                cbm_gbuf_insert_edge(ctx->gbuf, pkg_id, dep_id, "DEPENDS_ON", "{}");
                deps++;
            }
        }
    }

    cbm_log_info("pass.tcproj.plcproj", "file", rel_path, "members", itoa_tc(members), "deps",
                 itoa_tc(deps));
}

/* ── .tsproj handler ─────────────────────────────────────────────── */

static void handle_tsproj(cbm_pipeline_ctx_t *ctx, const char *rel_path, const char *source) {
    const cbm_gbuf_node_t *self = find_file_node(ctx, rel_path);
    if (!self) {
        return;
    }
    int64_t self_id = self->id;

    int plc_refs = 0;
    int io_refs = 0;
    for (const char *p = source; (p = strstr(p, "PrjFilePath=")) != NULL; p++) {
        char prj[CBM_SZ_512];
        /* PrjFilePath= is an attribute, not a tag — reuse the scanner from the
         * enclosing tag's start so the bounded '>' lookup applies. */
        const char *tag = p;
        while (tag > source && *tag != '<') {
            tag--;
        }
        if (!tag_attr(tag, "PrjFilePath", prj, sizeof(prj)) || !prj[0]) {
            continue;
        }
        if (!has_suffix_icase(prj, ".plcproj")) {
            continue;
        }
        char prj_rel[CBM_SZ_1K];
        resolve_ref_path(rel_path, prj, prj_rel, sizeof(prj_rel));
        char stem[CBM_SZ_256];
        file_stem(tcproj_basename(prj_rel), stem, sizeof(stem));
        char pkg_qn[CBM_SZ_512];
        plc_package_qn(ctx, stem, pkg_qn, sizeof(pkg_qn));
        const cbm_gbuf_node_t *pkg = cbm_gbuf_find_by_qn(ctx->gbuf, pkg_qn);
        int64_t pkg_id = pkg ? pkg->id
                             : cbm_gbuf_upsert_node(ctx->gbuf, "Package", stem, pkg_qn, prj_rel,
                                                    SKIP_ONE, 0, "{\"source\":\"twincat\"}");
        if (pkg_id > 0 && pkg_id != self_id) {
            cbm_gbuf_insert_edge(ctx->gbuf, self_id, pkg_id, "DEPENDS_ON", "{}");
            plc_refs++;
        }
    }

    /* Referenced I/O device descriptions (File="... .xti") get CONFIGURES
     * edges when the .xti file itself was indexed. */
    for (const char *p = source; (p = strstr(p, ".xti\"")) != NULL; p++) {
        const char *vs = p;
        while (vs > source && *vs != '"') {
            vs--;
        }
        if (vs == source) {
            continue;
        }
        char ref[CBM_SZ_512];
        size_t rl = (size_t)(p + 4 - (vs + 1));
        if (rl == 0 || rl >= sizeof(ref)) {
            continue;
        }
        memcpy(ref, vs + 1, rl);
        ref[rl] = '\0';
        char xti_rel[CBM_SZ_1K];
        resolve_ref_path(rel_path, ref, xti_rel, sizeof(xti_rel));
        const cbm_gbuf_node_t *xti = find_file_node(ctx, xti_rel);
        if (xti && xti->id != self_id) {
            cbm_gbuf_insert_edge(ctx->gbuf, self_id, xti->id, "CONFIGURES", "{}");
            io_refs++;
        }
    }

    cbm_log_info("pass.tcproj.tsproj", "file", rel_path, "plc_refs", itoa_tc(plc_refs), "io_refs",
                 itoa_tc(io_refs));
}

/* ── Pass entry point ────────────────────────────────────────────── */

int cbm_pipeline_pass_tcproj(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                             int file_count) {
    int plcproj_count = 0;
    int tsproj_count = 0;

    /* .plcproj first so the Package nodes exist before .tsproj references them. */
    for (int round = 0; round < PAIR_LEN; round++) {
        for (int i = 0; i < file_count; i++) {
            if (cbm_pipeline_check_cancel(ctx)) {
                return CBM_NOT_FOUND;
            }
            const char *base = tcproj_basename(files[i].rel_path);
            bool is_plc = cbm_is_plcproj_file(base);
            bool is_ts = cbm_is_tsproj_file(base);
            if ((round == 0 && !is_plc) || (round == 1 && !is_ts)) {
                continue;
            }
            int len = 0;
            char *source = tcproj_read_file(files[i].path, &len);
            if (!source) {
                continue;
            }
            if (round == 0) {
                handle_plcproj(ctx, files[i].rel_path, source);
                plcproj_count++;
            } else {
                handle_tsproj(ctx, files[i].rel_path, source);
                tsproj_count++;
            }
            free(source);
        }
    }

    if (plcproj_count > 0 || tsproj_count > 0) {
        cbm_log_info("pass.done", "pass", "tcproj", "plcproj", itoa_tc(plcproj_count), "tsproj",
                     itoa_tc(tsproj_count));
    }
    return 0;
}
