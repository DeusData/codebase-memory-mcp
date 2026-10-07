/*
 * test_impact_engine.c — detect_changes scope:"tests", end to end: git, the
 * merge base's rules, classification of every changed path, the HEAD
 * snapshot, seeding, the walk, the selection and the answer.
 */
#include "mcp/test_impact_engine_internal.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#include "foundation/sha256.h"
#include "store/store_graph_digest.h"
#include "store/store_impact.h"
#include "helpers.h" /* cbm_kind_in_set_free_cache — the job thread's cache teardown */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#endif

/* ── Shared helpers ──────────────────────────────────────────────── */

bool te_grow(void **items, int *cap, int count, size_t size) {
    if (count < *cap) {
        return true;
    }
    int next = *cap ? *cap * 2 : 16;
    void *grown = cbm_realloc(CBM_MEM_CLASS_EXTRACT, *items, (size_t)next * size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = next;
    return true;
}

bool te_oom(te_ctx_t *c) {
    c->oom = true;
    return false;
}

void te_note(te_ctx_t *c, const char *text) {
    if (c->diagnostic && c->diagnostic_len && !c->diagnostic[0] && text) {
        snprintf(c->diagnostic, c->diagnostic_len, "%s", text);
    }
}

bool te_fallback(te_ctx_t *c, cbm_test_result_fallback_t code, const char *diagnostic) {
    te_note(c, diagnostic);
    for (int i = 0; i < c->fallback_count; i++) {
        if (c->fallbacks[i] == code) {
            return true;
        }
    }
    if (!te_grow((void **)&c->fallbacks, &c->fallback_cap, c->fallback_count,
                 sizeof(*c->fallbacks))) {
        return te_oom(c);
    }
    c->fallbacks[c->fallback_count++] = code;
    return true;
}

/* ── git ─────────────────────────────────────────────────────────── */

bool cbm_test_impact_find_git(char *out, size_t out_len) {
    const char *path = getenv("PATH");
#ifdef _WIN32
    const char sep = ';';
    const char *exe = "git.exe";
#else
    const char sep = ':';
    const char *exe = "git";
#endif
    if (!path || !out || out_len == 0) {
        return false;
    }
    const char *p = path;
    while (*p) {
        const char *e = strchr(p, sep);
        size_t len = e ? (size_t)(e - p) : strlen(p);
        bool absolute = len > 0 && (p[0] == '/' || (len > 2 && p[1] == ':'));
        if (absolute && len + 1 + strlen(exe) < out_len) {
            snprintf(out, out_len, "%.*s/%s", (int)len, p, exe);
            struct stat st;
            if (stat(out, &st) == 0 && S_ISREG(st.st_mode)
#ifndef _WIN32
                && access(out, X_OK) == 0
#endif
            ) {
                return true;
            }
        }
        if (!e) {
            break;
        }
        p = e + 1;
    }
    out[0] = '\0';
    return false;
}

bool te_git(te_ctx_t *c) {
    char git[4096];
    if (!cbm_test_impact_find_git(git, sizeof(git))) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE, "no git on PATH") && false;
    }
    cbm_git_facts_options_t options = {.root = c->rq->repo_root,
                                       .base_ref = c->rq->base_ref,
                                       .git_executable = git,
                                       .deadline_ms = c->rq->deadline_ms,
                                       .command_limit = 4096,
                                       .stdout_limit = (size_t)512 << 20,
                                       .stderr_limit = 65536,
                                       .total_output_limit = (size_t)2048 << 20};
    cbm_git_facts_error_t error;
    c->facts = cbm_git_facts_open(&options, &error);
    if (!c->facts || !cbm_git_facts_diff(c->facts, &c->diff, &error)) {
        char text[700];
        snprintf(text, sizeof(text), "git: %s", error.diagnostic);
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE, text) && false;
    }
    c->id = cbm_git_facts_identity(c->facts);
    cbm_changes_status_t status =
        cbm_changes_parse(c->diff.name_status.data, c->diff.name_status.length, c->diff.patch.data,
                          c->diff.patch.length, &c->changes);
    if (status == CBM_CHANGES_OOM) {
        return te_oom(c);
    }
    cbm_changes_state_t state = cbm_changes_state(c->changes);
    c->diff_complete = status == CBM_CHANGES_OK && state != CBM_CHANGES_UNKNOWN &&
                       cbm_changes_patch_reconciled(c->changes);
    c->has_changes = state != CBM_CHANGES_EMPTY;
    return true;
}

/* ── Query-time configuration: the merge base's (M-4) ────────────── */

bool te_config(te_ctx_t *c) {
    const char *path = c->rq->config_path;
    char file[4200] = "";
    bool optional = false;
    if (!path) {
        snprintf(file, sizeof(file), "%s/merge-base-config.json", c->work_dir);
        path = file;
        optional = true;
        cbm_git_blob_t blob;
        cbm_git_facts_error_t error;
        static const char name[] = ".codebase-memory.json";
        cbm_git_blob_status_t found = cbm_git_facts_read_blob(
            c->facts, CBM_GIT_REV_MERGE_BASE, name, sizeof(name) - 1, &blob, &error);
        if (found == CBM_GIT_BLOB_ERROR) {
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
                               "the merge base's .codebase-memory.json could not be read") &&
                   false;
        }
        if (found == CBM_GIT_BLOB_FOUND) {
            FILE *f = cbm_fopen(file, "wb");
            bool ok = f && fwrite(blob.bytes.data, 1, blob.bytes.length, f) == blob.bytes.length;
            if (f) {
                ok = fclose(f) == 0 && ok;
            }
            if (!ok) {
                return te_fallback(c, CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
                                   "the merge base's configuration could not be staged") &&
                       false;
            }
        }
    }
    c->config = cbm_test_config_load(path, optional);
    if (optional) {
        (void)cbm_unlink(file); /* only the merge-base copy staged above */
    }
    if (!c->config) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_CONFIG_INVALID,
                           "the test_impact configuration is invalid") &&
               false;
    }
    c->policy = cbm_test_policy_new(c->config);
    if (!c->policy) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_POLICY_UNAVAILABLE,
                           "the test_impact rules are invalid") &&
               false;
    }
    return true;
}

/* ── Classification of the changed paths ─────────────────────────── */

static bool te_match(te_ctx_t *c, int rule_index) {
    for (int i = 0; i < c->match_count; i++) {
        if (c->matches[i].rule_index == rule_index) {
            return true;
        }
    }
    if (!te_grow((void **)&c->matches, &c->match_cap, c->match_count, sizeof(*c->matches))) {
        return te_oom(c);
    }
    c->matches[c->match_count++] = (te_match_t){.rule_index = rule_index, .resolved = true};
    return true;
}

static bool te_fixture_path(te_ctx_t *c, const char *path, int rule_index) {
    if (!te_grow((void **)&c->fixture_paths, &c->fixture_cap, c->fixture_count,
                 sizeof(*c->fixture_paths)) ||
        !te_grow((void **)&c->fixture_rules, &c->fixture_rule_cap, c->fixture_count,
                 sizeof(*c->fixture_rules))) {
        return te_oom(c);
    }
    c->fixture_paths[c->fixture_count] = path;
    c->fixture_rules[c->fixture_count++] = rule_index;
    return true;
}

static bool te_c_family(const char *path) {
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

/* First pass: the rules. A path no rule matches is decided after the
 * snapshot (te_classify_unmatched). Returns whether the snapshot is needed:
 * a run-all rule decides everything without it. */
static bool te_classify_rules(te_ctx_t *c, bool *need_snapshot, bool **unmatched) {
    size_t count = 0;
    const cbm_change_file_t *files = cbm_changes_files(c->changes, &count);
    int rule_count = 0;
    const cbm_test_rule_t *rules = cbm_test_policy_rules(c->policy, &rule_count);
    *unmatched = cbm_arena_calloc(&c->arena, count + 1);
    if (!*unmatched) {
        return te_oom(c);
    }
    bool run_all = false;
    c->activation_complete = true;
    for (size_t i = 0; i < count; i++) {
        const char *path = (const char *)files[i].path;
        const cbm_test_rule_t *rule = NULL;
        if (!cbm_test_policy_match_path(c->policy, path, &rule)) {
            c->activation_complete = false;
            if (!te_fallback(c, CBM_TEST_RESULT_FALLBACK_POLICY_EVALUATION_FAILED, path)) {
                return false;
            }
            continue;
        }
        if (!rule) {
            (*unmatched)[i] = true;
            continue;
        }
        int index = (int)(rule - rules);
        if (!te_match(c, index)) {
            return false;
        }
        run_all = run_all || rule->action == CBM_TEST_RULE_RUN_ALL;
        if (rule->action == CBM_TEST_RULE_REFERENCING_TESTS && !te_fixture_path(c, path, index)) {
            return false;
        }
    }
    *need_snapshot = !run_all && c->fallback_count == 0;
    return true;
}

static bool te_in_ledger(te_ctx_t *c, const char *path) {
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(c->tree);
    size_t n = 0;
    const cbm_test_impact_classified_t *rows = cbm_test_impact_ledger_rows(c->ledger, &n);
    size_t lo = 0;
    size_t hi = n;
    /* Ledger rows follow the tree's raw-path order. */
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cmp = strcmp((const char *)view->files[rows[mid].file_index].path, path);
        if (cmp == 0) {
            return true;
        }
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return false;
}

static bool te_add_seed_change(te_ctx_t *c, const cbm_change_file_t *f) {
    if (!te_grow((void **)&c->seed_changes, &c->seed_change_cap, c->seed_change_count,
                 sizeof(*c->seed_changes))) {
        return te_oom(c);
    }
    cbm_ti_change_t change = {.path = (const char *)f->path};
    if (f->status == 'D') {
        change.deleted = true;
        cbm_git_blob_t blob;
        cbm_git_facts_error_t error;
        cbm_git_blob_status_t found = cbm_git_facts_read_blob(
            c->facts, CBM_GIT_REV_MERGE_BASE, (const char *)f->path, f->path_length, &blob, &error);
        if (found != CBM_GIT_BLOB_FOUND) {
            return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE,
                               "a deleted file's base text could not be read");
        }
        change.base_text = (const char *)blob.bytes.data;
        change.base_len = blob.bytes.length;
    } else if (f->evidence == CBM_CHANGE_HUNKS && f->patch_file) {
        change.hunks = f->patch_file->hunks;
        change.hunk_count = f->patch_file->hunk_count;
    } else {
        change.whole_file = true;
    }
    c->seed_changes[c->seed_change_count++] = change;
    return true;
}

/* Second pass: a path no rule matched is read through the graph when it is
 * a C-family source of the snapshot (or deleted from it); anything else is a
 * change the test-runner lane cannot reason about (M-10): run everything. */
static bool te_classify_unmatched(te_ctx_t *c, const bool *unmatched) {
    size_t count = 0;
    const cbm_change_file_t *files = cbm_changes_files(c->changes, &count);
    for (size_t i = 0; i < count; i++) {
        if (!unmatched[i]) {
            continue;
        }
        const char *path = (const char *)files[i].path;
        bool readable = te_c_family(path) && (files[i].status == 'D' || te_in_ledger(c, path));
        bool ok = readable ? te_add_seed_change(c, &files[i])
                           : te_fallback(c, CBM_TEST_RESULT_FALLBACK_UNMAPPED_FILE, path);
        if (!ok) {
            return false;
        }
    }
    return true;
}

/* ── Graph scope, seeding, selection ─────────────────────────────── */

static bool te_open_scope(te_ctx_t *c) {
    if (cbm_store_read_scope_open(c->store, NULL, NULL, &c->scope) != CBM_STORE_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                           "the graph of HEAD could not be read") &&
               false;
    }
    static const unsigned char project[] = "test-impact";
    cbm_store_graph_digest_limits_t limits = {.max_rows = (uint64_t)1 << 32,
                                              .max_framed_bytes = (uint64_t)1 << 40};
    cbm_store_graph_digest_t digest;
    if (cbm_store_graph_digest(c->scope, project, sizeof(project) - 1, &limits, &digest) !=
        CBM_STORE_GRAPH_DIGEST_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_REJECTED,
                           "the graph of HEAD could not be digested") &&
               false;
    }
    memcpy(c->graph_sha256, digest.sha256, sizeof(c->graph_sha256));
    c->graph_digest = true;
    return true;
}

static bool te_seed(te_ctx_t *c) {
    cbm_ti_seed_input_t in = {.changes = c->seed_changes,
                              .change_count = c->seed_change_count,
                              .model = c->model,
                              .graph = c->graph,
                              .source = c->source};
    cbm_ti_seed_status_t status = cbm_ti_seed(&in, &c->seeds);
    if (status == CBM_TI_SEED_OOM) {
        return te_oom(c);
    }
    if (status != CBM_TI_SEED_OK) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE,
                           "seeding the graph failed") &&
               false;
    }
    return true;
}

static bool te_select(te_ctx_t *c) {
    cbm_test_selection_input_t input = {.model = c->model,
                                        .reach = c->reach,
                                        .reach_count = c->reach_count,
                                        .suite_triggers = c->triggers,
                                        .suite_trigger_count = c->trigger_count,
                                        .has_changes = c->has_changes,
                                        .diff_complete = c->diff_complete,
                                        .inventory_complete = c->snapshot_ok,
                                        .static_complete = c->static_complete,
                                        .coverage = c->coverage_admitted ? c->coverage : NULL,
                                        .changed_function_ids = c->cov_ids,
                                        .changed_function_count = c->cov_count,
                                        .coverage_admitted = c->coverage_admitted,
                                        .coverage_changes_complete = c->cov_complete};
    c->selection = cbm_test_select(&input);
    if (!c->selection) {
        return te_fallback(c, CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE,
                           "the selection could not be computed");
    }
    return true;
}

/* ── The answer ──────────────────────────────────────────────────── */

static bool te_hex_bytes(const char *hex, unsigned char *out, size_t count) {
    if (!hex || strlen(hex) != count * 2) {
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        unsigned v = 0;
        for (int k = 0; k < 2; k++) {
            char ch = hex[i * 2 + (size_t)k];
            unsigned d = ch >= '0' && ch <= '9'   ? (unsigned)(ch - '0')
                         : ch >= 'a' && ch <= 'f' ? (unsigned)(ch - 'a' + 10)
                         : ch >= 'A' && ch <= 'F' ? (unsigned)(ch - 'A' + 10)
                                                  : 16U;
            if (d > 15) {
                return false;
            }
            v = v * 16 + d;
        }
        out[i] = (unsigned char)v;
    }
    return true;
}

static cbm_test_result_oid_t te_oid(const te_ctx_t *c, const char *hex) {
    cbm_test_result_oid_t oid = {0};
    if (!c->id || !hex || !hex[0]) {
        return oid;
    }
    size_t bytes = c->id->oid_hex_length == 64 ? 32 : 20;
    if (te_hex_bytes(hex, oid.bytes, bytes)) {
        oid.format = bytes == 32 ? CBM_TEST_RESULT_OBJECT_SHA256 : CBM_TEST_RESULT_OBJECT_SHA1;
    } else {
        memset(oid.bytes, 0, sizeof(oid.bytes));
    }
    return oid;
}

static cbm_test_result_digest_t te_digest_hex(const char *hex) {
    cbm_test_result_digest_t d = {0};
    d.present = te_hex_bytes(hex, d.bytes, 32);
    if (!d.present) {
        memset(d.bytes, 0, sizeof(d.bytes));
    }
    return d;
}

static cbm_test_result_digest_t te_digest_of(const cbm_git_bytes_t *b) {
    cbm_test_result_digest_t d = {.present = true};
    cbm_sha256_ctx ctx;
    cbm_sha256_init(&ctx);
    cbm_sha256_update(&ctx, b->data ? b->data : (const unsigned char *)"", b->length);
    cbm_sha256_final(&ctx, d.bytes);
    return d;
}

static void te_receipt(te_ctx_t *c, cbm_test_result_receipt_t *r,
                       cbm_test_result_evidence_reason_t *graph_reason) {
    memset(r, 0, sizeof(*r));
    r->routes = CBM_TEST_RESULT_ROUTES_FOLLOWED;
    if (c->id) {
        r->object_format = c->id->oid_hex_length == 64 ? CBM_TEST_RESULT_OBJECT_SHA256
                                                       : CBM_TEST_RESULT_OBJECT_SHA1;
        r->base = te_oid(c, c->id->base);
        r->head = te_oid(c, c->id->head);
        r->merge_base = te_oid(c, c->id->merge_base);
    }
    if (c->changes) {
        r->diff_sha256 = te_digest_of(&c->diff.patch);
        r->name_status_sha256 = te_digest_of(&c->diff.name_status);
    }
    if (c->config) {
        r->config_sha256 = te_digest_hex(cbm_test_config_digest(c->config));
    }
    if (c->policy) {
        r->policy_sha256 = te_digest_hex(cbm_test_policy_digest(c->policy));
    }
    if (c->graph_digest) {
        r->graph.state = CBM_TEST_RESULT_GRAPH_CERTIFIED;
        r->graph.commit = r->head;
        r->graph.sha256.present = true;
        memcpy(r->graph.sha256.bytes, c->graph_sha256, 32);
    } else if (c->tree) {
        r->graph.state = CBM_TEST_RESULT_GRAPH_REJECTED;
        *graph_reason = CBM_TEST_RESULT_EVIDENCE_DIAGNOSTICS_INCOMPLETE;
        r->graph.rejection_reasons = (cbm_test_result_evidence_reasons_t){graph_reason, 1};
    }
    te_artifact_receipt(c, &r->coverage, te_oid);
}

static cbm_test_impact_status_t te_answer(te_ctx_t *c, cbm_test_result_t **out) {
    cbm_test_result_rule_match_t *matches =
        cbm_arena_calloc(&c->arena, (size_t)(c->match_count + 1) * sizeof(*matches));
    if (!matches) {
        return CBM_TEST_IMPACT_OOM;
    }
    for (int i = 0; i < c->match_count; i++) {
        matches[i] = (cbm_test_result_rule_match_t){.rule_index = c->matches[i].rule_index,
                                                    .target_resolved = c->matches[i].resolved};
    }
    cbm_test_result_receipt_t receipt;
    cbm_test_result_evidence_reason_t graph_reason = CBM_TEST_RESULT_EVIDENCE_PROVIDER_UNAVAILABLE;
    te_receipt(c, &receipt, &graph_reason);
    cbm_test_result_comparison_t comparison = !c->changes || !c->diff_complete
                                                  ? CBM_TEST_RESULT_COMPARISON_UNAVAILABLE
                                              : c->has_changes ? CBM_TEST_RESULT_COMPARISON_CHANGED
                                                               : CBM_TEST_RESULT_COMPARISON_EMPTY;
    if (comparison == CBM_TEST_RESULT_COMPARISON_UNAVAILABLE && c->fallback_count == 0 &&
        !te_fallback(c, CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE, "the change is not complete")) {
        return CBM_TEST_IMPACT_OOM;
    }
    bool with_selection = c->selection && comparison == CBM_TEST_RESULT_COMPARISON_CHANGED;
    cbm_test_result_input_t input = {
        .comparison = comparison,
        .selection = with_selection ? c->selection : NULL,
        .model = with_selection ? c->model : NULL,
        .policy = comparison == CBM_TEST_RESULT_COMPARISON_EMPTY ? NULL : c->policy,
        .inventory_complete = c->snapshot_ok,
        .activation_complete = c->activation_complete,
        .matched_rules = comparison == CBM_TEST_RESULT_COMPARISON_EMPTY ? NULL : matches,
        .matched_rule_count =
            comparison == CBM_TEST_RESULT_COMPARISON_EMPTY ? 0 : (size_t)c->match_count,
        .fallbacks = c->fallbacks,
        .fallback_count = (size_t)c->fallback_count,
        .warnings = c->warnings,
        .warning_count = (size_t)c->warning_count,
        .receipt = &receipt};
    cbm_test_result_limits_t limits = {.max_input_bytes = (uint64_t)1 << 30,
                                       .max_items = (uint64_t)1 << 26,
                                       .max_alloc_bytes = (size_t)1 << 30,
                                       .max_output_bytes = (size_t)256 << 20};
    cbm_test_result_status_t status = cbm_test_result_build(&input, &limits, NULL, NULL, out);
    if (status == CBM_TEST_RESULT_OOM) {
        return CBM_TEST_IMPACT_OOM;
    }
    if (status != CBM_TEST_RESULT_OK) {
        char text[64];
        snprintf(text, sizeof(text), "result codec status %d", (int)status);
        te_note(c, text);
        return CBM_TEST_IMPACT_INVALID;
    }
    return CBM_TEST_IMPACT_OK;
}

/* ── The request ─────────────────────────────────────────────────── */

static void te_free(te_ctx_t *c) {
    cbm_test_selection_free(c->selection);
    cbm_coverage_map_free(c->coverage);
    cbm_free(CBM_MEM_CLASS_OTHER, c->coverage_meta);
    cbm_ti_seeds_free(c->seeds);
    if (c->scope) {
        (void)cbm_store_read_scope_close(c->scope);
    }
    te_snapshot_free(c);
    cbm_test_policy_free(c->policy);
    cbm_test_config_free(c->config);
    cbm_changes_free(c->changes);
    cbm_git_facts_free(c->facts);
    void *arrays[] = {c->matches,       c->fallbacks,     c->warnings, c->seed_changes,
                      c->fixture_paths, c->fixture_rules, c->triggers};
    for (size_t i = 0; i < sizeof(arrays) / sizeof(arrays[0]); i++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, arrays[i]);
    }
    cbm_arena_destroy(&c->arena);
}

static bool te_valid(const cbm_test_impact_request_t *rq) {
    return rq && rq->repo_root && rq->repo_root[0] && rq->base_ref && rq->base_ref[0] &&
           rq->work_parent && rq->work_parent[0] && rq->deadline_ms > 0;
}

/* The evidence steps. Each false is either a recorded run-all reason (the
 * answer still comes) or exhausted memory (c->oom). */
static void te_steps(te_ctx_t *c) {
    if (!te_work_dir(c) || !te_git(c) || !te_config(c)) {
        return;
    }
    if (!c->has_changes || !c->diff_complete) {
        return; /* nothing changed, or the change is not known in full */
    }
    bool need_snapshot = false;
    bool *unmatched = NULL;
    if (!te_classify_rules(c, &need_snapshot, &unmatched) || !need_snapshot) {
        return;
    }
    if (!te_artifact_graph(c) || !te_snapshot(c) || !te_artifact_coverage(c) ||
        !te_classify_unmatched(c, unmatched) || c->fallback_count) {
        return;
    }
    if (!te_open_scope(c) || !te_seed(c) || !te_reach(c) || !te_coverage_ids(c)) {
        return;
    }
    (void)te_select(c);
}

typedef struct {
    const cbm_test_impact_request_t *request;
    cbm_test_result_t **out;
    char *diagnostic;
    size_t diagnostic_len;
    cbm_test_impact_status_t status;
} te_job_t;

static void *te_job_main(void *arg) {
    te_job_t *job = arg;
    te_ctx_t c = {
        .rq = job->request, .diagnostic = job->diagnostic, .diagnostic_len = job->diagnostic_len};
    cbm_arena_init(&c.arena);
    te_steps(&c);
    cbm_test_impact_status_t status = c.oom ? CBM_TEST_IMPACT_OOM : te_answer(&c, job->out);
    if (!c.oom && cbm_now_ms() > job->request->deadline_ms && status == CBM_TEST_IMPACT_OK) {
        cbm_test_result_free(*job->out);
        *job->out = NULL;
        status = CBM_TEST_IMPACT_CANCELLED;
    }
    te_free(&c);
    /* The snapshot build extracted on THIS thread, which ends here: its
     * thread-local node-type bitset cache goes with it (as the MCP index
     * thread and the parallel workers release theirs). */
    cbm_kind_in_set_free_cache();
    job->status = status;
    return NULL;
}

cbm_test_impact_status_t cbm_test_impact_run(const cbm_test_impact_request_t *request,
                                             cbm_test_result_t **out, char *diagnostic,
                                             size_t diagnostic_len) {
    if (diagnostic && diagnostic_len) {
        diagnostic[0] = '\0';
    }
    if (!out) {
        return CBM_TEST_IMPACT_INVALID;
    }
    *out = NULL;
    if (!te_valid(request)) {
        return CBM_TEST_IMPACT_INVALID;
    }
    te_job_t job = {.request = request,
                    .out = out,
                    .diagnostic = diagnostic,
                    .diagnostic_len = diagnostic_len,
                    .status = CBM_TEST_IMPACT_OOM};
    /* The request runs a whole (frozen) index pipeline. Daemon tool calls run
     * on 256 KiB application threads; the pipeline needs the stack every
     * pipeline thread gets (cbm_thread_create's default), so the request runs
     * on its own thread whoever calls it. On cbm, the in-daemon call died
     * silently inside the pipeline without this. */
    cbm_thread_t thread;
    if (cbm_thread_create(&thread, 0, te_job_main, &job) != 0) {
        return CBM_TEST_IMPACT_OOM;
    }
    (void)cbm_thread_join(&thread);
    return job.status;
}

typedef struct {
    const cbm_test_impact_publish_t *request;
    char *diagnostic;
    size_t diagnostic_len;
    cbm_test_impact_status_t status;
} te_publish_job_t;

static void *te_publish_main(void *arg) {
    te_publish_job_t *job = arg;
    const cbm_test_impact_publish_t *p = job->request;
    /* The steps read a selection request; publishing is the empty change of
     * HEAD against itself. */
    cbm_test_impact_request_t rq = {.repo_root = p->repo_root,
                                    .base_ref = "HEAD",
                                    .work_parent = p->work_parent,
                                    .config_path = p->config_path,
                                    .deadline_ms = p->deadline_ms};
    te_ctx_t c = {.rq = &rq, .diagnostic = job->diagnostic, .diagnostic_len = job->diagnostic_len};
    cbm_arena_init(&c.arena);
    bool ok = te_publish(&c, p);
    job->status = c.oom ? CBM_TEST_IMPACT_OOM : ok ? CBM_TEST_IMPACT_OK : CBM_TEST_IMPACT_INVALID;
    te_free(&c);
    cbm_kind_in_set_free_cache(); /* this thread's extraction cache, as in te_job_main */
    return NULL;
}

cbm_test_impact_status_t cbm_test_impact_publish(const cbm_test_impact_publish_t *request,
                                                 char *diagnostic, size_t diagnostic_len) {
    if (diagnostic && diagnostic_len) {
        diagnostic[0] = '\0';
    }
    if (!request || !request->repo_root || !request->work_parent || !request->out_dir ||
        !request->deadline_ms) {
        return CBM_TEST_IMPACT_INVALID;
    }
    te_publish_job_t job = {.request = request,
                            .diagnostic = diagnostic,
                            .diagnostic_len = diagnostic_len,
                            .status = CBM_TEST_IMPACT_OOM};
    cbm_thread_t thread; /* a whole frozen index: the default pipeline stack */
    if (cbm_thread_create(&thread, 0, te_publish_main, &job) != 0) {
        return CBM_TEST_IMPACT_OOM;
    }
    (void)cbm_thread_join(&thread);
    return job.status;
}
