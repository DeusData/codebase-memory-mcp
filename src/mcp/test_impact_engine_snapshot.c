/*
 * test_impact_engine_snapshot.c — the HEAD snapshot of one scope:"tests"
 * request: the pinned tree, its classified ledger, the runnable registry, the
 * frozen graph of exactly the ledger's files, and the text index of its
 * C-family sources. All of it is a function of the pinned HEAD commit (D5):
 * nothing reads the worktree.
 */
#include "mcp/test_impact_engine_internal.h"

#include "discover/discover.h"
#include "discover/userconfig.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    TE_MAX_FILES = 1000000,
    TE_MAX_DIRECTORIES = 400000,
    TE_MAX_CONFIG_BYTES = 65536,
};

static bool te_snapshot_fail(te_ctx_t *c, cbm_test_result_fallback_t code, const char *what,
                             const char *detail) {
    char text[512];
    snprintf(text, sizeof(text), "%s%s%s", what, detail && *detail ? ": " : "",
             detail ? detail : "");
    (void)te_fallback(c, code, text);
    return false;
}

/* A private (0700) directory for this request under work_parent. */
bool te_work_dir(te_ctx_t *c) {
    snprintf(c->work_dir, sizeof(c->work_dir), "%s/ti-XXXXXX", c->rq->work_parent);
    if (!cbm_mkdtemp(c->work_dir)) {
        c->work_dir[0] = '\0';
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "no private work directory", c->rq->work_parent);
    }
    char sub[4200];
    snprintf(sub, sizeof(sub), "%s/pin", c->work_dir);
    bool ok = cbm_mkdir_p(sub, 0700);
    snprintf(sub, sizeof(sub), "%s/graph", c->work_dir);
    ok = ok && cbm_mkdir_p(sub, 0700);
    snprintf(c->candidate_db, sizeof(c->candidate_db), "%s/graph/candidate.db", c->work_dir);
    return ok || te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                  "work directory setup failed", c->work_dir);
}

static bool te_pin(te_ctx_t *c) {
    char parent[4200];
    snprintf(parent, sizeof(parent), "%s/pin", c->work_dir);
    cbm_pinned_tree_options_t options = {
        .facts = c->facts,
        .revision = CBM_GIT_REV_HEAD,
        .private_parent = parent,
        .limits = {.max_files = TE_MAX_FILES,
                   .max_directories = TE_MAX_DIRECTORIES,
                   .max_total_content_bytes = (size_t)8 << 30,
                   .max_relative_path_bytes = 4095,
                   .max_arena_bytes = (size_t)2 << 30,
                   .blob_batch = {.max_entries = TE_MAX_FILES,
                                  .max_input_bytes = (size_t)256 << 20,
                                  .max_arena_bytes = (size_t)8 << 30}},
        .control = {.deadline_ms = c->rq->deadline_ms}};
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_status_t status = cbm_pinned_tree_create(&options, &c->tree, &error);
    if (status != CBM_PINNED_TREE_OK) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "pinning HEAD failed", error.diagnostic);
    }
    return true;
}

static bool te_inventory(te_ctx_t *c) {
    cbm_inventory_limits_t limits = {.max_files = TE_MAX_FILES,
                                     .max_directories = TE_MAX_DIRECTORIES,
                                     .max_arena_bytes = (size_t)1 << 30,
                                     .max_ignore_arena_bytes = (size_t)64 << 20,
                                     .max_control_file_bytes = 1 << 20,
                                     .max_control_total_bytes = 64u << 20,
                                     .max_ignore_patterns = 100000,
                                     .max_probe_prefix_bytes = CBM_LANGUAGE_PROBE_MAX,
                                     .max_ignore_work = 1000000000ULL,
                                     .max_verified_file_reads = TE_MAX_FILES,
                                     .max_verified_content_bytes = (uint64_t)16 << 30};
    cbm_inventory_control_t control = {.deadline_ms = c->rq->deadline_ms};
    cbm_inventory_error_t error;
    if (cbm_test_impact_inventory_prepare(c->tree, &limits, &control, &c->inventory, &error) !=
        CBM_INVENTORY_OK) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "inventory of HEAD failed", error.diagnostic);
    }
    return true;
}

/* A configuration file's bytes, at most 64 KiB; absent leaves NULL. */
static bool te_read_config(te_ctx_t *c, const char *path, char **bytes, size_t *len) {
    *bytes = NULL;
    *len = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return true; /* absent */
    }
    char *buf = cbm_arena_alloc(&c->arena, TE_MAX_CONFIG_BYTES + 1);
    size_t got = buf ? fread(buf, 1, TE_MAX_CONFIG_BYTES + 1, f) : 0;
    bool err = ferror(f) != 0;
    fclose(f);
    if (!buf) {
        return te_oom(c);
    }
    if (err || got > TE_MAX_CONFIG_BYTES) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
                                "configuration unreadable or over 64 KiB", path);
    }
    buf[got] = '\0';
    *bytes = buf;
    *len = got;
    return true;
}

/* HEAD's own .codebase-memory.json: what the graph is built under. */
static bool te_head_config(te_ctx_t *c, char **bytes, size_t *len) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(c->tree);
    char path[4200];
    snprintf(path, sizeof(path), "%s/.codebase-memory.json", view->root);
    return te_read_config(c, path, bytes, len);
}

/* The test declarations: the request's configuration when it names one
 * (read in place of the project file, for replay), else HEAD's. */
static bool te_ledger_and_registry(te_ctx_t *c, const char *config_bytes, size_t config_len) {
    if (c->rq->config_path) {
        char *override = NULL;
        size_t override_len = 0;
        if (!te_read_config(c, c->rq->config_path, &override, &override_len)) {
            return false;
        }
        config_bytes = override;
        config_len = override_len;
    }
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(c->tree);
    cbm_userconfig_t *userconfig = cbm_userconfig_load(view->root);
    cbm_inventory_control_t control = {.deadline_ms = c->rq->deadline_ms};
    cbm_inventory_error_t error;
    cbm_inventory_status_t status =
        cbm_test_impact_classify(c->inventory, c->tree, userconfig, &control, &c->ledger, &error);
    cbm_userconfig_free(userconfig);
    if (status != CBM_INVENTORY_OK) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "classifying HEAD failed", error.diagnostic);
    }
    c->declarations = cbm_test_declarations_parse(config_bytes, config_len, config_bytes == NULL);
    if (!c->declarations) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
                                "HEAD test declarations invalid", NULL);
    }
    status = cbm_test_impact_registry(c->ledger, c->inventory, c->tree, c->declarations, &control,
                                      &c->model, &error);
    if (status != CBM_INVENTORY_OK) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "test registry of HEAD failed", error.diagnostic);
    }
    return true;
}

static bool te_frozen_build(te_ctx_t *c, const char *config_bytes, size_t config_len) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(c->tree);
    size_t rows_n = 0;
    const cbm_test_impact_classified_t *rows = cbm_test_impact_ledger_rows(c->ledger, &rows_n);
    cbm_pipeline_frozen_file_t *list =
        cbm_arena_alloc(&c->arena, (rows_n ? rows_n : 1) * sizeof(*list));
    if (!list) {
        return te_oom(c);
    }
    for (size_t i = 0; i < rows_n; i++) {
        list[i] = (cbm_pipeline_frozen_file_t){
            .rel_path = (const char *)view->files[rows[i].file_index].path,
            .language = rows[i].language};
    }
    /* The Git context is display data the build never traverses and the
     * engine never reads back. It is fixed, so the candidate is a function of
     * the pinned tree alone: the per-run pin path or the commit ids here land
     * in the Branch node, make two builds of one tree differ, and turn every
     * new commit into a semantic-input change that declines incremental repair
     * of the team artifact's graph. The answer's receipt binds the commit. */
    char root[] = "/pinned";
    char head[65];
    char merge_base[65];
    char empty[1] = "";
    char branch[] = "pinned";
    size_t oid_len = strlen(c->id->head) == 64 ? 64 : 40;
    memset(head, '0', oid_len);
    head[oid_len] = '\0';
    memcpy(merge_base, head, oid_len + 1);
    cbm_git_context_t git = {.is_git = true,
                             .is_detached = true,
                             .root_exists = true,
                             .input_path = root,
                             .worktree_root = root,
                             .git_dir = empty,
                             .git_common_dir = empty,
                             .canonical_root = root,
                             .branch = branch,
                             .branch_slug = branch,
                             .head_sha = head,
                             .base_sha = merge_base};
    cbm_pipeline_frozen_inputs_t in = {.source_root = view->root,
                                       .candidate_db_path = c->candidate_db,
                                       .project = "test-impact",
                                       .config_state = config_bytes ? CBM_USERCONFIG_SOURCE_PRESENT
                                                                    : CBM_USERCONFIG_SOURCE_ABSENT,
                                       .config_bytes = config_bytes,
                                       .config_len = config_len,
                                       .pinned_git = &git,
                                       .files = list,
                                       .file_count = rows_n,
                                       .base_db_path = c->base_db[0] ? c->base_db : NULL};
    cbm_pipeline_frozen_t *owner = NULL;
    cbm_pipeline_frozen_status_t status = cbm_pipeline_frozen_create(&in, &owner);
    if (status == CBM_PIPELINE_FROZEN_OK) {
        status = cbm_pipeline_frozen_build(owner);
    }
    char detail[512] = "";
    if (status != CBM_PIPELINE_FROZEN_OK) {
        /* Name what refused the graph: the build's negative evidence and its
         * first file error. */
        unsigned flags = 0;
        cbm_file_error_t *errors = NULL;
        int error_count = 0;
        (void)cbm_pipeline_frozen_negative_evidence(owner, &flags);
        const cbm_pipeline_t *diagnostics = owner ? cbm_pipeline_frozen_diagnostics(owner) : NULL;
        if (diagnostics) {
            cbm_pipeline_get_file_errors(diagnostics, &errors, &error_count);
        }
        snprintf(detail, sizeof(detail), "frozen status %d, evidence %u, %d file error(s)%s%s%s%s",
                 (int)status, flags, error_count, error_count ? ": " : "",
                 error_count && errors[0].path ? errors[0].path : "", error_count ? " " : "",
                 error_count && errors[0].reason ? errors[0].reason : "");
    }
    c->route = cbm_pipeline_frozen_route(owner);
    cbm_pipeline_frozen_free(owner);
    if (status != CBM_PIPELINE_FROZEN_OK) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_REJECTED,
                                "the graph of HEAD could not be built", detail);
    }
    c->store = cbm_store_open_path_query(c->candidate_db);
    c->graph = c->store ? cbm_ti_graph_new(c->store, "test-impact") : NULL;
    if (!c->graph) {
        return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                "the graph of HEAD could not be opened", NULL);
    }
    return true;
}

/* The C-family files the reference reads (replay4 is_c: by extension). */
static bool te_c_family_path(const char *path) {
    static const char *const ext[] = {".c", ".h",  ".cc",  ".cpp", ".hpp",
                                      ".m", ".mm", ".cxx", ".hh"};
    size_t n = strlen(path);
    for (size_t i = 0; i < sizeof(ext) / sizeof(ext[0]); i++) {
        size_t e = strlen(ext[i]);
        if (n > e && strcmp(path + n - e, ext[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool te_source_index(te_ctx_t *c) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(c->tree);
    size_t rows_n = 0;
    const cbm_test_impact_classified_t *rows = cbm_test_impact_ledger_rows(c->ledger, &rows_n);
    c->source = cbm_ti_source_new();
    if (!c->source) {
        return te_oom(c);
    }
    cbm_pinned_tree_control_t control = {.deadline_ms = c->rq->deadline_ms};
    for (size_t i = 0; i < rows_n; i++) {
        const cbm_pinned_tree_file_t *file = &view->files[rows[i].file_index];
        const char *path = (const char *)file->path;
        if (!te_c_family_path(path)) {
            continue;
        }
        size_t len = (size_t)file->content_length;
        unsigned char *text = cbm_alloc(CBM_MEM_CLASS_EXTRACT, len ? len : 1);
        if (!text) {
            return te_oom(c);
        }
        size_t copied = 0;
        cbm_pinned_tree_error_t error;
        cbm_pinned_tree_status_t status =
            cbm_pinned_tree_read_prefix(c->tree, rows[i].file_index, file->content_length, text,
                                        len, &copied, &control, &error);
        bool ok = status == CBM_PINNED_TREE_OK && copied == len &&
                  cbm_ti_source_add(c->source, path, (const char *)text, len);
        cbm_free(CBM_MEM_CLASS_EXTRACT, text);
        if (!ok) {
            return te_snapshot_fail(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                                    "reading HEAD sources failed", path);
        }
    }
    return cbm_ti_source_finish(c->source) || te_oom(c);
}

bool te_snapshot(te_ctx_t *c) {
    char *config_bytes = NULL;
    size_t config_len = 0;
    c->snapshot_ok = te_pin(c) && te_inventory(c) &&
                     te_head_config(c, &config_bytes, &config_len) &&
                     te_ledger_and_registry(c, config_bytes, config_len) &&
                     te_frozen_build(c, config_bytes, config_len) && te_source_index(c);
    return c->snapshot_ok;
}

static void te_remove(const char *path) {
    if (path && *path) {
        (void)cbm_unlink(path);
    }
}

void te_snapshot_free(te_ctx_t *c) {
    cbm_ti_source_free(c->source);
    cbm_ti_graph_free(c->graph);
    if (c->store) {
        cbm_store_close(c->store);
    }
    cbm_test_model_free(c->model);
    cbm_test_declarations_free(c->declarations);
    cbm_test_impact_ledger_free(c->ledger);
    cbm_test_impact_inventory_free(c->inventory);
    if (c->tree) {
        cbm_pinned_tree_error_t error;
        (void)cbm_pinned_tree_close(&c->tree, &error);
    }
    if (c->candidate_db[0]) {
        char side[4200];
        te_remove(c->candidate_db);
        snprintf(side, sizeof(side), "%s-wal", c->candidate_db);
        te_remove(side);
        snprintf(side, sizeof(side), "%s-shm", c->candidate_db);
        te_remove(side);
    }
    if (c->base_db[0]) {
        /* The imported team-artifact graph (test_impact_engine_artifact.c). */
        te_remove(c->base_db);
        (void)cbm_remove_db_sidecars(c->base_db);
    }
    if (c->work_dir[0]) {
        char sub[4200];
        snprintf(sub, sizeof(sub), "%s/graph", c->work_dir);
        (void)cbm_rmdir(sub);
        snprintf(sub, sizeof(sub), "%s/pin", c->work_dir);
        (void)cbm_rmdir(sub);
        (void)cbm_rmdir(c->work_dir);
    }
}
