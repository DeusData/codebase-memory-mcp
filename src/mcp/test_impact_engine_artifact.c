/*
 * test_impact_engine_artifact.c — the team artifact on both ends.
 *
 * Producer (cbm_test_impact_publish): the main branch builds the bundle of a
 * commit — the commit's frozen graph, incrementally from the previous bundle's
 * graph when that bundle verifies, its coverage map, and receipt.json.
 *
 * Consumer (te_artifact_graph, te_artifact_coverage, te_coverage_ids): a PR
 * selection given a bundle the caller verified uses the bundle's graph as the
 * frozen build's incremental base and may admit its coverage map. Every doubt
 * ends in the conservative answer: the graph built from scratch, the map not
 * admitted, every suite whole.
 */
#include "mcp/test_impact_engine_internal.h"

#include "foundation/compat_fs.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#include "mcp/test_impact_artifact.h"
#include "pipeline/artifact.h"
#include "store/store_graph_digest.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

enum { TA_FILE_MAX = 512 << 20 };

static void ta_hex(const unsigned char bytes[32], char hex[65]) {
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        hex[2 * i] = digits[bytes[i] >> 4];
        hex[2 * i + 1] = digits[bytes[i] & 15];
    }
    hex[64] = '\0';
}

static bool ta_unhex(const char *hex, unsigned char bytes[32]) {
    if (!hex || strlen(hex) != 64) {
        return false;
    }
    for (int i = 0; i < 64; i++) {
        char ch = hex[i];
        int v = ch >= '0' && ch <= '9' ? ch - '0' : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10 : -1;
        if (v < 0) {
            return false;
        }
        bytes[i / 2] = (unsigned char)(i % 2 ? (bytes[i / 2] | v) : (v << 4));
    }
    return true;
}

/* The exact graph digest of a database: what an importer of it measures. */
bool te_db_digest(const char *db_path, char hex[65]) {
    hex[0] = '\0';
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        return false;
    }
    cbm_store_read_scope_t *scope = NULL;
    bool ok = cbm_store_read_scope_open(store, NULL, NULL, &scope) == CBM_STORE_OK;
    static const unsigned char project[] = "test-impact";
    cbm_store_graph_digest_limits_t limits = {.max_rows = (uint64_t)1 << 32,
                                              .max_framed_bytes = (uint64_t)1 << 40};
    cbm_store_graph_digest_t digest;
    ok = ok && cbm_store_graph_digest(scope, project, sizeof(project) - 1, &limits, &digest) ==
                   CBM_STORE_GRAPH_DIGEST_OK;
    if (scope && cbm_store_read_scope_close(scope) != CBM_STORE_OK) {
        ok = false;
    }
    cbm_store_close(store);
    if (ok) {
        ta_hex(digest.sha256, hex);
    }
    return ok;
}

/* The graph-content digest (store_content_digest.c): equal for equal graphs
 * however they were built; the weekly incremental-equals-full check. */
static bool te_db_content_digest(const char *db_path, char content[65], char topology[65]) {
    content[0] = '\0';
    topology[0] = '\0';
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    unsigned char a[32];
    unsigned char b[32];
    bool ok = store && cbm_store_graph_content_digest(store, "test-impact", a) == CBM_STORE_OK &&
              cbm_store_graph_topology_digest(store, "test-impact", b) == CBM_STORE_OK;
    if (store) {
        cbm_store_close(store);
    }
    if (ok) {
        ta_hex(a, content);
        ta_hex(b, topology);
    }
    return ok;
}

/* Imports a bundle's graph to db_path; true only when it is the graph the
 * receipt names. */
static bool te_bundle_import(const char *bundle, const cbm_ti_receipt_t *receipt,
                             const char *db_path, const char **why) {
    if (cbm_artifact_import(bundle, db_path) != 0) {
        *why = "the bundle's graph could not be imported";
        return false;
    }
    char measured[65];
    if (!te_db_digest(db_path, measured)) {
        *why = "the bundle's imported graph could not be digested";
        return false;
    }
    if (strcmp(measured, receipt->graph_sha256) != 0) {
        *why = "the bundle's imported graph is not the graph its receipt names";
        return false;
    }
    return true;
}

static bool te_read_file(const char *path, char **out, size_t *len) {
    *out = NULL;
    *len = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return false;
    }
    size_t cap = 65536;
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, cap);
    size_t n = 0;
    bool ok = buf != NULL;
    while (ok) {
        if (n == cap) {
            if (cap >= TA_FILE_MAX) {
                ok = false;
                break;
            }
            char *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, buf, cap * 2);
            if (!grown) {
                ok = false;
                break;
            }
            buf = grown;
            cap *= 2;
        }
        size_t got = fread(buf + n, 1, cap - n, f);
        n += got;
        if (got == 0) {
            ok = !ferror(f);
            break;
        }
    }
    (void)fclose(f);
    if (!ok) {
        cbm_free(CBM_MEM_CLASS_OTHER, buf);
        return false;
    }
    *out = buf;
    *len = n;
    return true;
}

/* The coverage map of a bundle (or any directory with the three files). */
static bool te_load_map(const char *dir, cbm_coverage_map_t **map, char **meta, size_t *meta_len) {
    char path[4200];
    char *functions = NULL;
    char *tests = NULL;
    size_t functions_len = 0;
    size_t tests_len = 0;
    *map = NULL;
    *meta = NULL;
    *meta_len = 0;
    snprintf(path, sizeof(path), "%s/functions.tsv", dir);
    bool ok = te_read_file(path, &functions, &functions_len);
    snprintf(path, sizeof(path), "%s/tests.tsv", dir);
    ok = ok && te_read_file(path, &tests, &tests_len);
    snprintf(path, sizeof(path), "%s/meta.json", dir);
    ok = ok && te_read_file(path, meta, meta_len);
    *map = ok ? cbm_coverage_map_parse(functions, functions_len, tests, tests_len) : NULL;
    cbm_free(CBM_MEM_CLASS_OTHER, functions);
    cbm_free(CBM_MEM_CLASS_OTHER, tests);
    if (!*map) {
        cbm_free(CBM_MEM_CLASS_OTHER, *meta);
        *meta = NULL;
        *meta_len = 0;
        return false;
    }
    return true;
}

/* The compatibility digest of this checkout's pinned tree for `platform`,
 * over the configured compatibility paths. */
static bool te_compatibility(te_ctx_t *c, const char *platform, char hex[65]) {
    const cbm_pinned_tree_view_t *view = c->tree ? cbm_pinned_tree_view(c->tree) : NULL;
    int count = 0;
    const char *const *paths = cbm_test_policy_compatibility_paths(c->policy, &count);
    return view && platform && *platform &&
           cbm_ti_compatibility_digest(view->root, paths, count, platform, hex);
}

/* ── Consumer ──────────────────────────────────────────────────────── */

static void te_reject(te_ctx_t *c, cbm_test_result_evidence_reason_t reason, const char *why) {
    if (c->coverage_reason_count <
        (int)(sizeof(c->coverage_reasons) / sizeof(c->coverage_reasons[0]))) {
        bool known = false;
        for (int i = 0; i < c->coverage_reason_count; i++) {
            known = known || c->coverage_reasons[i] == reason;
        }
        if (!known) {
            c->coverage_reasons[c->coverage_reason_count++] = reason;
        }
    }
    te_note(c, why);
}

/* Before the snapshot: a verified bundle of the merge base becomes the frozen
 * build's incremental base. */
bool te_artifact_graph(te_ctx_t *c) {
    const cbm_test_impact_request_t *rq = c->rq;
    if (!rq->artifact_dir) {
        return true;
    }
    c->artifact_requested = true;
    if (!rq->artifact_verified || !rq->artifact_commit) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_SOURCE_UNVERIFIED,
                  "artifact: the caller did not verify the bundle's source; ignored");
        return true;
    }
    if (cbm_ti_receipt_read(rq->artifact_dir, &c->receipt) != CBM_TI_RECEIPT_OK) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_METADATA_INVALID,
                  "artifact: the bundle's receipt is missing or invalid");
        return true;
    }
    if (strcmp(c->receipt.commit, rq->artifact_commit) != 0) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_COMMIT_MISMATCH,
                  "artifact: the receipt names another commit than the verified source");
        return true;
    }
    /* v1 admits only the merge base's own bundle: an older ancestor's map
     * cannot see what main changed since (recorded follow-up). */
    if (strcmp(c->receipt.commit, c->id->merge_base) != 0) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_ANCESTRY_UNPROVED,
                  "artifact: the bundle is not of the merge base");
        return true;
    }
    snprintf(c->base_db, sizeof(c->base_db), "%s/graph/base.db", c->work_dir);
    const char *why = NULL;
    if (!te_bundle_import(rq->artifact_dir, &c->receipt, c->base_db, &why)) {
        (void)cbm_unlink(c->base_db);
        (void)cbm_remove_db_sidecars(c->base_db);
        c->base_db[0] = '\0';
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_CONTENT_MISMATCH, why);
        return true;
    }
    c->artifact_graph = true;
    return true;
}

/* After the snapshot: the receipt policy decides whether the map narrows. */
bool te_artifact_coverage(te_ctx_t *c) {
    const cbm_test_impact_request_t *rq = c->rq;
    if (!c->artifact_graph) {
        return true;
    }
    if (!c->receipt.has_coverage) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_PROVIDER_UNAVAILABLE,
                  "artifact: the bundle carries no coverage map");
        return true;
    }
    char dir[4200];
    snprintf(dir, sizeof(dir), "%s/%s", rq->artifact_dir, CBM_TI_COVERAGE_DIR);
    if (!te_load_map(dir, &c->coverage, &c->coverage_meta, &c->coverage_meta_len)) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_FORMAT_UNSUPPORTED,
                  "artifact: the coverage map could not be read");
        return true;
    }
    char compatibility[65] = "";
    if (!te_compatibility(c, rq->platform, compatibility)) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_COMPATIBILITY_MISMATCH,
                  "artifact: no platform to judge the map's compatibility by");
        return true;
    }
    cbm_coverage_receipt_t receipt = {.commit = c->receipt.commit,
                                      .functions_sha256 = c->receipt.functions_sha256,
                                      .tests_sha256 = c->receipt.tests_sha256,
                                      .metadata_sha256 = c->receipt.metadata_sha256,
                                      .graph_sha256 = c->receipt.graph_sha256,
                                      .compatibility_sha256 = c->receipt.compatibility_sha256,
                                      .oldest_observation_at = c->receipt.oldest_observation_at};
    char measured[65];
    if (!te_db_digest(c->base_db, measured)) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_CONTENT_MISMATCH,
                  "artifact: the imported graph could not be digested");
        return true;
    }
    cbm_coverage_receipt_context_t context = {.source_verified = rq->artifact_verified,
                                              .trusted_commit = rq->artifact_commit,
                                              .ancestor_verified =
                                                  strcmp(c->receipt.commit, c->id->merge_base) == 0,
                                              .ancestor_commit = c->receipt.commit,
                                              .ancestor_merge_base = c->id->merge_base,
                                              .merge_base = c->id->merge_base,
                                              .graph_commit = c->receipt.commit,
                                              .graph_sha256 = measured,
                                              .compatibility_sha256 = compatibility,
                                              .now = (int64_t)time(NULL)};
    unsigned reasons = cbm_coverage_map_check_receipt(c->coverage, c->coverage_meta,
                                                      c->coverage_meta_len, &receipt, &context);
    static const struct {
        unsigned bit;
        cbm_test_result_evidence_reason_t reason;
    } map[] = {
        {CBM_COVERAGE_RECEIPT_INVALID, CBM_TEST_RESULT_EVIDENCE_METADATA_INVALID},
        {CBM_COVERAGE_RECEIPT_SOURCE, CBM_TEST_RESULT_EVIDENCE_SOURCE_UNVERIFIED},
        {CBM_COVERAGE_RECEIPT_ANCESTRY, CBM_TEST_RESULT_EVIDENCE_ANCESTRY_UNPROVED},
        {CBM_COVERAGE_RECEIPT_COMMIT, CBM_TEST_RESULT_EVIDENCE_COMMIT_MISMATCH},
        {CBM_COVERAGE_RECEIPT_CONTENT, CBM_TEST_RESULT_EVIDENCE_CONTENT_MISMATCH},
        {CBM_COVERAGE_RECEIPT_AGE, CBM_TEST_RESULT_EVIDENCE_TOO_OLD},
        {CBM_COVERAGE_RECEIPT_COMPATIBILITY, CBM_TEST_RESULT_EVIDENCE_COMPATIBILITY_MISMATCH},
        {CBM_COVERAGE_RECEIPT_METADATA, CBM_TEST_RESULT_EVIDENCE_METADATA_INVALID}};
    for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (reasons & map[i].bit) {
            te_reject(c, map[i].reason, "artifact: the coverage receipt was refused");
        }
    }
    c->coverage_admitted = reasons == 0;
    return true;
}

/* Fills the coverage half of the answer's receipt. */
void te_artifact_receipt(te_ctx_t *c, cbm_test_result_coverage_receipt_t *r,
                         cbm_test_result_oid_t (*oid)(const te_ctx_t *, const char *)) {
    memset(r, 0, sizeof(*r));
    if (!c->artifact_requested) {
        r->state = CBM_TEST_RESULT_COVERAGE_UNAVAILABLE;
        return;
    }
    r->format = CBM_TEST_RESULT_COVERAGE_FORMAT_FUNCTIONS_V1;
    r->artifact = (cbm_test_result_bytes_t){(const unsigned char *)CBM_TI_RECEIPT_SCHEMA,
                                            sizeof(CBM_TI_RECEIPT_SCHEMA) - 1};
    if (c->receipt.commit[0]) {
        r->commit = oid(c, c->receipt.commit);
    }
    struct {
        const char *hex;
        cbm_test_result_digest_t *out;
    } digests[] = {{c->receipt.functions_sha256, &r->identities_sha256},
                   {c->receipt.tests_sha256, &r->tests_sha256},
                   {c->receipt.metadata_sha256, &r->metadata_sha256},
                   {c->receipt.graph_sha256, &r->graph_sha256},
                   {c->receipt.compatibility_sha256, &r->compatibility_sha256}};
    for (size_t i = 0; i < sizeof(digests) / sizeof(digests[0]); i++) {
        digests[i].out->present = ta_unhex(digests[i].hex, digests[i].out->bytes);
        if (!digests[i].out->present) {
            memset(digests[i].out->bytes, 0, sizeof(digests[i].out->bytes));
        }
    }
    if (c->receipt.oldest_observation_at > 0) {
        r->oldest_observation_at =
            (cbm_test_result_timestamp_t){true, c->receipt.oldest_observation_at};
    }
    if (c->coverage_admitted) {
        r->state = CBM_TEST_RESULT_COVERAGE_ADMITTED;
        return;
    }
    if (!c->coverage_reason_count) {
        te_reject(c, CBM_TEST_RESULT_EVIDENCE_PROVIDER_UNAVAILABLE, NULL);
    }
    r->state = CBM_TEST_RESULT_COVERAGE_REJECTED;
    r->rejection_reasons =
        (cbm_test_result_evidence_reasons_t){c->coverage_reasons, (size_t)c->coverage_reason_count};
}

/* ── Coverage: the changed function set C_eff (R2.1) ────────────────
 *
 * A seed the map knows contributes its own id: the map records every test
 * that executed it, spawned children included, so its callers add nothing.
 * A seed the map does not know (new code, a type, a macro, a field, code the
 * map's image never compiled) contributes the nearest mapped functions on its
 * static inbound closure: a test that executes any of them may now reach the
 * change. Names deleted at head contribute every mapped function of that
 * name. A store failure leaves the set incomplete, which runs everything. */

static const char *const te_cov_types[] = {"CALLS", "ASYNC_CALLS", "USAGE", "CALL_REFERENCE",
                                           "READS"};

typedef struct {
    te_ctx_t *c;
    const cbm_coverage_map_t *map;
    int *ids;
    int count;
    int cap;
    int64_t *queue;
    int queue_len;
    int queue_cap;
    CBMHashTable *visited;
} te_cov_t;

static bool te_cov_add(te_cov_t *k, int id) {
    if (k->count == k->cap) {
        int cap = k->cap ? k->cap * 2 : 64;
        int *grown = cbm_arena_alloc(&k->c->arena, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        if (k->count) {
            memcpy(grown, k->ids, (size_t)k->count * sizeof(*grown));
        }
        k->ids = grown;
        k->cap = cap;
    }
    k->ids[k->count++] = id;
    return true;
}

static bool te_cov_push(te_cov_t *k, int64_t id) {
    char key[24];
    snprintf(key, sizeof(key), "%lld", (long long)id);
    if (cbm_ht_get(k->visited, key)) {
        return true;
    }
    char *owned = cbm_arena_strdup(&k->c->arena, key);
    if (!owned) {
        return false;
    }
    cbm_ht_set(k->visited, owned, owned);
    if (k->queue_len == k->queue_cap) {
        int cap = k->queue_cap ? k->queue_cap * 2 : 256;
        int64_t *grown = cbm_arena_alloc(&k->c->arena, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        if (k->queue_len) {
            memcpy(grown, k->queue, (size_t)k->queue_len * sizeof(*grown));
        }
        k->queue = grown;
        k->queue_cap = cap;
    }
    k->queue[k->queue_len++] = id;
    return true;
}

/* *mapped: the node is a function the map knows (its id was added). */
static bool te_cov_visit(te_cov_t *k, int64_t id, bool *mapped) {
    *mapped = false;
    cbm_node_t node = {0};
    if (cbm_store_find_node_by_id(k->c->store, id, &node) != CBM_STORE_OK) {
        return false;
    }
    const cbm_coverage_function_t *f =
        node.file_path && node.name
            ? cbm_coverage_map_find_function(k->map, node.file_path, node.name)
            : NULL;
    cbm_node_free_fields(&node);
    if (f) {
        *mapped = true;
        return te_cov_add(k, f->id);
    }
    return true;
}

static bool te_cov_expand(te_cov_t *k, int64_t id) {
    for (size_t t = 0; t < sizeof(te_cov_types) / sizeof(te_cov_types[0]); t++) {
        cbm_edge_t *edges = NULL;
        int n = 0;
        if (cbm_store_find_edges_by_target_type(k->c->store, id, te_cov_types[t], &edges, &n) !=
            CBM_STORE_OK) {
            return false;
        }
        bool ok = true;
        for (int i = 0; ok && i < n; i++) {
            ok = te_cov_push(k, edges[i].source_id);
        }
        cbm_store_free_edges(edges, n);
        if (!ok) {
            return false;
        }
    }
    return true;
}

static int te_cov_int_cmp(const void *a, const void *b) {
    int x = *(const int *)a;
    int y = *(const int *)b;
    return (x > y) - (x < y);
}

bool te_coverage_ids(te_ctx_t *c) {
    c->cov_complete = false;
    if (!c->coverage_admitted) {
        return true;
    }
    te_cov_t k = {.c = c, .map = c->coverage, .visited = cbm_ht_create(1024)};
    if (!k.visited) {
        return te_oom(c);
    }
    bool ok = true;
    int seed_count = 0;
    const int64_t *seeds = cbm_ti_seeds_nodes(c->seeds, &seed_count);
    for (int i = 0; ok && i < seed_count; i++) {
        ok = te_cov_push(&k, seeds[i]);
    }
    for (int head = 0; ok && head < k.queue_len; head++) {
        bool mapped = false;
        ok = te_cov_visit(&k, k.queue[head], &mapped) &&
             (mapped || te_cov_expand(&k, k.queue[head]));
    }
    int deleted_count = 0;
    const char *const *deleted = cbm_ti_seeds_deleted_names(c->seeds, &deleted_count);
    int function_count = 0;
    const cbm_coverage_function_t *functions =
        cbm_coverage_map_functions(c->coverage, &function_count);
    for (int d = 0; ok && d < deleted_count; d++) {
        for (int f = 0; ok && f < function_count; f++) {
            if (strcmp(functions[f].name, deleted[d]) == 0) {
                ok = te_cov_add(&k, functions[f].id);
            }
        }
    }
    cbm_ht_free(k.visited);
    if (!ok) {
        return true; /* incomplete: the selection then runs everything */
    }
    if (k.count) {
        qsort(k.ids, (size_t)k.count, sizeof(*k.ids), te_cov_int_cmp);
    }
    int unique = 0;
    for (int i = 0; i < k.count; i++) {
        if (!unique || k.ids[unique - 1] != k.ids[i]) {
            k.ids[unique++] = k.ids[i];
        }
    }
    c->cov_ids = k.ids;
    c->cov_count = unique;
    c->cov_complete = true;
    return true;
}

/* ── Producer ──────────────────────────────────────────────────────── */

static bool te_copy_file(const char *from, const char *to) {
    char *bytes = NULL;
    size_t len = 0;
    if (!te_read_file(from, &bytes, &len)) {
        return false;
    }
    FILE *out = cbm_fopen(to, "wb");
    bool ok = out && fwrite(bytes, 1, len, out) == len;
    if (out) {
        ok = fclose(out) == 0 && ok;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, bytes);
    return ok;
}

static bool te_publish_fail(te_ctx_t *c, const char *why) {
    te_note(c, why);
    return false;
}

/* The bundle of the checked-out commit. c->rq is the request te_work_dir,
 * te_git, te_config and te_snapshot read (base_ref = HEAD). */
bool te_publish(te_ctx_t *c, const cbm_test_impact_publish_t *p) {
    cbm_path_info_t info;
    if (cbm_path_info_utf8(p->out_dir, &info) == 0) {
        return te_publish_fail(c, "publish: the output directory already exists");
    }
    if (!te_work_dir(c) || !te_git(c) || !te_config(c)) {
        return te_publish_fail(c, "publish: git or configuration unavailable");
    }
    cbm_ti_receipt_t previous = {0};
    if (p->previous_dir) {
        const char *why = NULL;
        snprintf(c->base_db, sizeof(c->base_db), "%s/graph/base.db", c->work_dir);
        if (cbm_ti_receipt_read(p->previous_dir, &previous) != CBM_TI_RECEIPT_OK ||
            !te_bundle_import(p->previous_dir, &previous, c->base_db, &why)) {
            /* An unusable previous bundle only costs time: build in full. */
            (void)cbm_unlink(c->base_db);
            (void)cbm_remove_db_sidecars(c->base_db);
            c->base_db[0] = '\0';
            memset(&previous, 0, sizeof(previous));
        }
    }
    if (!te_snapshot(c)) {
        return te_publish_fail(c, "publish: the graph of HEAD could not be built");
    }
    /* The store must be closed before export snapshots it. */
    cbm_ti_graph_free(c->graph);
    c->graph = NULL;
    cbm_store_close(c->store);
    c->store = NULL;
    if (!cbm_mkdir_p(p->out_dir, 0755) ||
        cbm_artifact_export(c->candidate_db, p->out_dir, "test-impact", CBM_ARTIFACT_BEST) != 0) {
        return te_publish_fail(c, "publish: the graph could not be exported");
    }
    cbm_ti_receipt_t r = {0};
    snprintf(r.commit, sizeof(r.commit), "%s", c->id->head);
    snprintf(r.platform, sizeof(r.platform), "%s", p->platform ? p->platform : "");
    char check[4200];
    snprintf(check, sizeof(check), "%s/graph/check.db", c->work_dir);
    if (cbm_artifact_import(p->out_dir, check) != 0 || !te_db_digest(check, r.graph_sha256) ||
        !te_db_content_digest(check, r.graph_content_sha256, r.graph_topology_sha256)) {
        return te_publish_fail(c, "publish: the exported graph does not import");
    }
    (void)cbm_unlink(check);
    (void)cbm_remove_db_sidecars(check);
    if (p->coverage_dir) {
        char dir[4200];
        snprintf(dir, sizeof(dir), "%s/%s", p->out_dir, CBM_TI_COVERAGE_DIR);
        static const char *const files[] = {"functions.tsv", "tests.tsv", "meta.json"};
        char *sums[] = {r.functions_sha256, r.tests_sha256, r.metadata_sha256};
        bool ok = cbm_mkdir_p(dir, 0755);
        for (size_t i = 0; ok && i < 3; i++) {
            char from[4200];
            char to[4200];
            snprintf(from, sizeof(from), "%s/%s", p->coverage_dir, files[i]);
            snprintf(to, sizeof(to), "%s/%s", dir, files[i]);
            ok = te_copy_file(from, to) && cbm_ti_sha256_file(to, sums[i]);
        }
        cbm_coverage_map_t *map = NULL;
        char *meta = NULL;
        size_t meta_len = 0;
        ok = ok && te_load_map(dir, &map, &meta, &meta_len) &&
             cbm_coverage_map_metadata_matches(map, meta, meta_len, r.commit);
        cbm_coverage_map_free(map);
        cbm_free(CBM_MEM_CLASS_OTHER, meta);
        if (!ok) {
            return te_publish_fail(c, "publish: the coverage map is not this commit's");
        }
        r.oldest_observation_at = p->observed_at > 0      ? p->observed_at
                                  : previous.has_coverage ? previous.oldest_observation_at
                                                          : 0;
        if (r.oldest_observation_at <= 0 ||
            !te_compatibility(c, p->platform, r.compatibility_sha256)) {
            return te_publish_fail(c, "publish: the map's observation time or platform is unknown");
        }
        r.has_coverage = true;
    }
    if (!cbm_ti_receipt_write(p->out_dir, &r)) {
        return te_publish_fail(c, "publish: receipt.json could not be written");
    }
    char text[256];
    snprintf(text, sizeof(text), "published %s route=%s topology=%s content=%s", r.commit,
             c->route == CBM_PIPELINE_FROZEN_ROUTE_INCREMENTAL ? "incremental" : "full",
             r.graph_topology_sha256, r.graph_content_sha256);
    te_note(c, text);
    return true;
}
