/*
 * test_doclinks.c — Tests for markdown/shell → file reference linking.
 *
 * Unit-test approach mirrors test_configlink.c: create real files in a
 * tmpdir (the pass reads content from disk), set up File nodes in a gbuf,
 * run the pass, check REFERENCES_FILE edges.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* ── Helpers ─────────────────────────────────────────────────────── */

/* Run the pass with a minimal ctx (same shape as run_configlink). */
static int run_doclinks(cbm_gbuf_t *gb, const char *project, const char *repo_path) {
    atomic_int cancelled;
    atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx = {
        .project_name = project,
        .repo_path = repo_path,
        .gbuf = gb,
        .cancelled = &cancelled,
    };
    return cbm_pipeline_pass_doclinks(&ctx);
}

/* Create a File node the way pass_structure does: QN via fqn_compute with
 * "__file__", file_path = repo-relative path. Returns the node id. */
static int64_t add_file_node(cbm_gbuf_t *gb, const char *project, const char *rel) {
    char *qn = cbm_pipeline_fqn_compute(project, rel, "__file__");
    const char *slash = strrchr(rel, '/');
    const char *basename = slash ? slash + 1 : rel;
    int64_t id = cbm_gbuf_upsert_node(gb, "File", basename, qn, rel, 0, 0, NULL);
    free(qn);
    return id;
}

/* Find the REFERENCES_FILE edge between two node ids. NULL if absent. */
static const cbm_gbuf_edge_t *find_ref_edge(cbm_gbuf_t *gb, int64_t src, int64_t tgt) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_type(gb, "REFERENCES_FILE", &edges, &count);
    for (int i = 0; i < count; i++) {
        if (edges[i]->source_id == src && edges[i]->target_id == tgt) {
            return edges[i];
        }
    }
    return NULL;
}

/* True when the edge's props JSON carries the given strategy. */
static bool edge_has_strategy(const cbm_gbuf_edge_t *e, const char *strategy) {
    char needle[64];
    snprintf(needle, sizeof(needle), "\"strategy\":\"%s\"", strategy);
    return e && e->properties_json && strstr(e->properties_json, needle) != NULL;
}

/* Total REFERENCES_FILE edge count. */
static int ref_edge_count(cbm_gbuf_t *gb) {
    return cbm_gbuf_edge_count_by_type(gb, "REFERENCES_FILE");
}

/* Fixture: tmpdir + project + gbuf. */
typedef struct {
    char tmpdir[256];
    char *project;
    cbm_gbuf_t *gb;
} dl_fix_t;

static bool dl_fix_init(dl_fix_t *fx) {
    snprintf(fx->tmpdir, sizeof(fx->tmpdir), "/tmp/cbm_doclinks_XXXXXX");
    if (!cbm_mkdtemp(fx->tmpdir)) {
        return false;
    }
    fx->project = cbm_project_name_from_path(fx->tmpdir);
    fx->gb = cbm_gbuf_new(fx->project, fx->tmpdir);
    return fx->gb != NULL;
}

static void dl_fix_free(dl_fix_t *fx) {
    cbm_gbuf_free(fx->gb);
    free(fx->project);
    th_rmtree(fx->tmpdir);
}

/* ── Markdown: inline link ───────────────────────────────────────── */

TEST(doclinks_md_inline_link) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "docs/guide.md"),
                  "# Guide\n\nSee [the build script](../scripts/build.sh) for details.\n");
    th_write_file(TH_PATH(fx.tmpdir, "scripts/build.sh"), "#!/bin/sh\necho build\n");

    int64_t doc_id = add_file_node(fx.gb, fx.project, "docs/guide.md");
    int64_t script_id = add_file_node(fx.gb, fx.project, "scripts/build.sh");

    int n = run_doclinks(fx.gb, fx.project, fx.tmpdir);
    ASSERT_GT(n, 0);

    const cbm_gbuf_edge_t *e = find_ref_edge(fx.gb, doc_id, script_id);
    ASSERT_NOT_NULL(e);
    ASSERT_TRUE(edge_has_strategy(e, "md_inline_link"));
    ASSERT_NOT_NULL(strstr(e->properties_json, "\"confidence\":0.95"));

    dl_fix_free(&fx);
    PASS();
}

/* ── Markdown: backtick path ─────────────────────────────────────── */

TEST(doclinks_md_backtick_path) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "README.md"),
                  "Run `scripts/build.sh` before pushing. `not_a_file.xyz` is unknown.\n");
    th_write_file(TH_PATH(fx.tmpdir, "scripts/build.sh"), "#!/bin/sh\n");

    int64_t doc_id = add_file_node(fx.gb, fx.project, "README.md");
    int64_t script_id = add_file_node(fx.gb, fx.project, "scripts/build.sh");

    run_doclinks(fx.gb, fx.project, fx.tmpdir);

    const cbm_gbuf_edge_t *e = find_ref_edge(fx.gb, doc_id, script_id);
    ASSERT_NOT_NULL(e);
    ASSERT_TRUE(edge_has_strategy(e, "md_backtick_path"));
    /* `not_a_file.xyz` has no File node → no invented target */
    ASSERT_EQ(ref_edge_count(fx.gb), 1);

    dl_fix_free(&fx);
    PASS();
}

/* ── Markdown: bare mention ──────────────────────────────────────── */

TEST(doclinks_md_bare_mention) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "STANDARDS.md"),
                  "All handlers live in src/handlers.c and follow the pattern there.\n");
    th_write_file(TH_PATH(fx.tmpdir, "src/handlers.c"), "int h(void) { return 0; }\n");

    int64_t doc_id = add_file_node(fx.gb, fx.project, "STANDARDS.md");
    int64_t code_id = add_file_node(fx.gb, fx.project, "src/handlers.c");

    run_doclinks(fx.gb, fx.project, fx.tmpdir);

    const cbm_gbuf_edge_t *e = find_ref_edge(fx.gb, doc_id, code_id);
    ASSERT_NOT_NULL(e);
    ASSERT_TRUE(edge_has_strategy(e, "md_bare_mention"));
    ASSERT_NOT_NULL(strstr(e->properties_json, "\"confidence\":0.70"));

    dl_fix_free(&fx);
    PASS();
}

/* ── Markdown: non-file links ignored (http / mailto / #anchor) ──── */

TEST(doclinks_md_non_file_link_ignored) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "README.md"),
                  "See [the site](https://example.com/scripts/build.sh) or\n"
                  "[mail us](mailto:dev@example.com) or [below](#usage).\n");
    th_write_file(TH_PATH(fx.tmpdir, "scripts/build.sh"), "#!/bin/sh\n");

    add_file_node(fx.gb, fx.project, "README.md");
    add_file_node(fx.gb, fx.project, "scripts/build.sh");

    int n = run_doclinks(fx.gb, fx.project, fx.tmpdir);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(ref_edge_count(fx.gb), 0);

    dl_fix_free(&fx);
    PASS();
}

/* ── Markdown: anchor suffix on a file link still resolves ───────── */

TEST(doclinks_md_link_with_anchor_resolves_file) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "docs/a.md"), "See [setup](../INSTALL.md#quick-start).\n");
    th_write_file(TH_PATH(fx.tmpdir, "INSTALL.md"), "# Install\n");

    int64_t doc_id = add_file_node(fx.gb, fx.project, "docs/a.md");
    int64_t tgt_id = add_file_node(fx.gb, fx.project, "INSTALL.md");

    run_doclinks(fx.gb, fx.project, fx.tmpdir);
    ASSERT_NOT_NULL(find_ref_edge(fx.gb, doc_id, tgt_id));

    dl_fix_free(&fx);
    PASS();
}

/* ── Dedupe: repeated references collapse to one counted edge ────── */

TEST(doclinks_dedupe_keeps_count) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "README.md"),
                  "Use [build](scripts/build.sh) daily.\n"
                  "Run `scripts/build.sh` first, then scripts/build.sh again.\n");
    th_write_file(TH_PATH(fx.tmpdir, "scripts/build.sh"), "#!/bin/sh\n");

    int64_t doc_id = add_file_node(fx.gb, fx.project, "README.md");
    int64_t script_id = add_file_node(fx.gb, fx.project, "scripts/build.sh");

    int n = run_doclinks(fx.gb, fx.project, fx.tmpdir);
    /* three matches, ONE edge */
    ASSERT_EQ(n, 1);
    ASSERT_EQ(ref_edge_count(fx.gb), 1);

    const cbm_gbuf_edge_t *e = find_ref_edge(fx.gb, doc_id, script_id);
    ASSERT_NOT_NULL(e);
    ASSERT_NOT_NULL(strstr(e->properties_json, "\"count\":3"));
    /* highest-confidence match kind wins the edge label */
    ASSERT_TRUE(edge_has_strategy(e, "md_inline_link"));
    ASSERT_NOT_NULL(strstr(e->properties_json, "\"confidence\":0.95"));

    dl_fix_free(&fx);
    PASS();
}

/* ── Path resolution: relative to the referencing file's directory ── */

TEST(doclinks_resolves_relative_to_file_dir) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    /* docs/deep/page.md links an upward "../../src/main.c" path and a
     * sibling-relative "notes.md" path, neither repo-root-relative. */
    th_write_file(TH_PATH(fx.tmpdir, "docs/deep/page.md"),
                  "See [main](../../src/main.c) and [notes](notes.md).\n");
    th_write_file(TH_PATH(fx.tmpdir, "docs/deep/notes.md"), "# notes\n");
    th_write_file(TH_PATH(fx.tmpdir, "src/main.c"), "int main(void) { return 0; }\n");

    int64_t page_id = add_file_node(fx.gb, fx.project, "docs/deep/page.md");
    int64_t notes_id = add_file_node(fx.gb, fx.project, "docs/deep/notes.md");
    int64_t main_id = add_file_node(fx.gb, fx.project, "src/main.c");

    run_doclinks(fx.gb, fx.project, fx.tmpdir);

    ASSERT_NOT_NULL(find_ref_edge(fx.gb, page_id, main_id));
    ASSERT_NOT_NULL(find_ref_edge(fx.gb, page_id, notes_id));

    dl_fix_free(&fx);
    PASS();
}

/* ── Never invent nodes: unresolvable targets produce nothing ────── */

TEST(doclinks_unresolvable_target_no_edge) {
    dl_fix_t fx;
    ASSERT_TRUE(dl_fix_init(&fx));

    th_write_file(TH_PATH(fx.tmpdir, "README.md"),
                  "See [gone](docs/removed.md) and `also/missing.sh`.\n");

    add_file_node(fx.gb, fx.project, "README.md");

    int node_count_before = cbm_gbuf_node_count(fx.gb);
    int n = run_doclinks(fx.gb, fx.project, fx.tmpdir);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(ref_edge_count(fx.gb), 0);
    ASSERT_EQ(cbm_gbuf_node_count(fx.gb), node_count_before);

    dl_fix_free(&fx);
    PASS();
}

/* ── NULL repo_path (configlink-style unit setups) is a clean skip ── */

TEST(doclinks_null_repo_path_skips) {
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp/test");
    add_file_node(gb, "test", "README.md");
    int n = run_doclinks(gb, "test", NULL);
    ASSERT_EQ(n, 0);
    cbm_gbuf_free(gb);
    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────── */

SUITE(doclinks) {
    /* Markdown strategies */
    RUN_TEST(doclinks_md_inline_link);
    RUN_TEST(doclinks_md_backtick_path);
    RUN_TEST(doclinks_md_bare_mention);
    RUN_TEST(doclinks_md_non_file_link_ignored);
    RUN_TEST(doclinks_md_link_with_anchor_resolves_file);

    /* Dedupe + resolution + guards */
    RUN_TEST(doclinks_dedupe_keeps_count);
    RUN_TEST(doclinks_resolves_relative_to_file_dir);
    RUN_TEST(doclinks_unresolvable_target_no_edge);
    RUN_TEST(doclinks_null_repo_path_skips);
}
