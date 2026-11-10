#include <foundation/arena.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <pipeline/pipeline.h>
#include <pipeline/pipeline_internal.h>
#include <store/store.h>
#include <discover/test_conventions.h>
#include <pipeline/pass_lsp_cross.h>
#include "result_spill.h"
#include "yyjson/yyjson.h"
#include <stdio.h>
#include <string.h>

#if !defined(CBM_INCREMENTAL_TEST_API) || !CBM_INCREMENTAL_TEST_API || \
    !defined(CBM_ENABLE_TEST_SEAMS) || !CBM_ENABLE_TEST_SEAMS
#error "pipeline_conventions requires the canonical test runner's existing test API defines"
#endif

#define PC_PROJECT "pipelineconventions"
#define PC_REC(role_, macro_, arg_) \
    "{\"language\":\"c\",\"role\":\"" role_ "\",\"define_macro\":\"" macro_ "\",\"name_args\":[" arg_ "]}"
#define PC_CONFIG(arg_) "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[" \
    PC_REC("case", "CHECK", arg_) "," PC_REC("suite", "GROUP", "0") "]}}}"
static const char pc_config0[] = PC_CONFIG("0");
static const char pc_config1[] = PC_CONFIG("1");
static const char pc_empty_config[] = "{\"test_impact\":{\"version\":1,\"tests\":{}}}";
static const char pc_cpp_config[] = "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
    "{\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]}]}}}";
static const char pc_legacy_source[] =
    "void sink(void) {}\nvoid preserved(void) { sink(); }\n";
static const char pc_source[] =
    "void case_sink(void) {}\nvoid suite_sink(void) {}\n"
    "CHECK(before, after) { case_sink(); }\nGROUP(group) { suite_sink(); }\n";
/* Retain the old declarations: a graph-size drop must not satisfy the abort oracle. */
static const char pc_bad_source[] =
    "void sink(void) {}\nvoid preserved(void) { sink(); }\nCHECK() { sink(); }\n";
static const char pc_cpp_source[] =
    "#include \"engine.h\"\nCHECK(alpha) { Engine engine; engine.work(); }\n";
static int pc_init_status;

typedef struct { char home[1024], repo[1024], db[1024], config[1024], saved[1024]; } pc_fixture_t;
typedef struct { char generation[128]; } pc_snapshot_t;
typedef struct { const char *name; bool present; char value[4096]; } pc_env_value_t;
typedef struct { pc_env_value_t values[4]; bool saved; } pc_env_t;

static bool pc_path(char *out, size_t cap, const char *base, const char *leaf) {
    int n = snprintf(out, cap, "%s/%s", base, leaf);
    return n > 0 && (size_t)n < cap;
}
static bool pc_write(const char *path, const char *text) {
    FILE *f = cbm_fopen(path, "wb");
    if (!f) return false;
    size_t size = strlen(text);
    bool ok = fwrite(text, 1, size, f) == size;
    return fclose(f) == 0 && ok;
}
static bool pc_source_write(pc_fixture_t *f, const char *leaf, const char *text) {
    char path[1024];
    return pc_path(path, sizeof(path), f->repo, leaf) && pc_write(path, text);
}
static bool pc_copy(const char *from, const char *to) {
    FILE *a = cbm_fopen(from, "rb"), *b = cbm_fopen(to, "wb");
    bool ok = a && b;
    unsigned char bytes[4096];
    while (ok) {
        size_t n = fread(bytes, 1, sizeof(bytes), a);
        if (n && fwrite(bytes, 1, n, b) != n) { ok = false; break; }
        if (n < sizeof(bytes)) { ok = !ferror(a); break; }
    }
    if (a && fclose(a) != 0) ok = false;
    if (b && fclose(b) != 0) ok = false;
    return ok;
}
static bool pc_files_equal(const char *left, const char *right) {
    FILE *a = cbm_fopen(left, "rb"), *b = cbm_fopen(right, "rb");
    bool ok = a && b;
    unsigned char x[4096], y[4096];
    while (ok) {
        size_t nx = fread(x, 1, sizeof(x), a), ny = fread(y, 1, sizeof(y), b);
        if (nx != ny || memcmp(x, y, nx) != 0) { ok = false; break; }
        if (nx < sizeof(x)) { ok = !ferror(a) && !ferror(b); break; }
    }
    if (a && fclose(a) != 0) ok = false;
    if (b && fclose(b) != 0) ok = false;
    return ok;
}
static bool pc_fixture_open(pc_fixture_t *f, bool parallel) {
    memset(f, 0, sizeof(*f));
    const char *home = th_mktempdir("cbm-pipeline-conventions");
    if (!home || strlen(home) >= sizeof(f->home)) return false;
    memcpy(f->home, home, strlen(home) + 1);
    bool ok = pc_path(f->repo, sizeof(f->repo), home, "repo") &&
        pc_path(f->db, sizeof(f->db), home, "published.db") &&
        pc_path(f->saved, sizeof(f->saved), home, "saved.db") &&
        pc_path(f->config, sizeof(f->config), f->repo, ".codebase-memory.json") &&
        th_mkdir_p(f->repo) == 0 &&
        pc_source_write(f, "aux.c", "void auxiliary(void) {}\n");
    /* The existing full driver needs >50 filtered files as well as >1 worker. */
    for (int i = 0; ok && parallel && i < 51; i++) {
        char leaf[40], body[80];
        int a = snprintf(leaf, sizeof(leaf), "filler_%02d.c", i);
        int b = snprintf(body, sizeof(body), "void filler_%02d(void) {}\n", i);
        ok = a > 0 && (size_t)a < sizeof(leaf) && b > 0 && (size_t)b < sizeof(body) &&
            pc_source_write(f, leaf, body);
    }
    if (!ok) { (void)th_rmtree(f->home); f->home[0] = '\0'; }
    return ok;
}
static bool pc_env_restore(pc_env_t *env) {
    bool ok = true;
    if (!env->saved) return true;
    for (size_t i = 0; i < 4; i++) {
        pc_env_value_t *v = &env->values[i];
        if (v->present) { if (cbm_setenv(v->name, v->value, 1) != 0) ok = false; }
        else (void)cbm_unsetenv(v->name);
        const char *now = getenv(v->name);
        if (v->present ? (!now || strcmp(now, v->value) != 0) : now != NULL) ok = false;
    }
    env->saved = false;
    return ok;
}
static bool pc_env_begin(pc_env_t *env, bool parallel) {
    memset(env, 0, sizeof(*env));
    const char *names[] = {"CBM_WORKERS", "CBM_INDEX_SINGLE_THREAD", "CBM_DISABLE_LSP_CROSS", "CBM_MEM_SPILL"};
    for (size_t i = 0; i < 4; i++) {
        env->values[i].name = names[i];
        const char *value = getenv(names[i]);
        env->values[i].present = value != NULL;
        if (value) {
            if (strlen(value) >= sizeof(env->values[i].value)) return false;
            memcpy(env->values[i].value, value, strlen(value) + 1);
        }
    }
    env->saved = true;
    bool ok = cbm_setenv("CBM_WORKERS", "2", 1) == 0;
    if (parallel) (void)cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    else ok = cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1) == 0 && ok;
    (void)cbm_unsetenv("CBM_DISABLE_LSP_CROSS");
    (void)cbm_unsetenv("CBM_MEM_SPILL");
    const char *single = getenv("CBM_INDEX_SINGLE_THREAD");
    ok = ok && cbm_default_worker_count(true) == 2 &&
        (parallel ? single == NULL : single && strcmp(single, "1") == 0) &&
        getenv("CBM_DISABLE_LSP_CROSS") == NULL && getenv("CBM_MEM_SPILL") == NULL;
    if (!ok) (void)pc_env_restore(env);
    return ok;
}
static bool pc_text(const char *a, const char *b) { return a && b && strcmp(a, b) == 0; }
static bool pc_role(const cbm_node_t *n, const char *role) {
    if (!n || !n->properties_json) return false;
    yyjson_doc *doc = yyjson_read(n->properties_json, strlen(n->properties_json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, "test_role") : NULL;
    bool ok = yyjson_is_obj(root) && (role ? yyjson_is_str(value) &&
        pc_text(yyjson_get_str(value), role) : value == NULL);
    yyjson_doc_free(doc);
    return ok;
}
static bool pc_node(cbm_store_t *s, const char *qn, const char *name, const char *file,
                    const char *role, cbm_node_t *out) {
    memset(out, 0, sizeof(*out));
    return cbm_store_find_node_by_qn(s, PC_PROJECT, qn, out) == CBM_STORE_OK &&
        pc_text(out->qualified_name, qn) && pc_text(out->name, name) &&
        pc_text(out->file_path, file) && pc_role(out, role);
}
static bool pc_call(cbm_store_t *s, int64_t source, int64_t target) {
    cbm_edge_t *edges = NULL; int count = 0;
    bool read = cbm_store_find_edges_by_source_type(s, source, "CALLS", &edges, &count) == CBM_STORE_OK;
    bool found = false;
    if (read) for (int i = 0; i < count; i++)
        if (edges[i].source_id == source && edges[i].target_id == target && pc_text(edges[i].type, "CALLS")) found = true;
    cbm_store_free_edges(edges, count);
    return read && found;
}
static bool pc_c_graph(const char *db, bool after) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    if (!s) return false;
    cbm_node_t c = {0}, g = {0}, cs = {0}, gs = {0};
    bool ok = pc_node(s, after ? PC_PROJECT ".unit.CHECK_after" : PC_PROJECT ".unit.CHECK_before",
        after ? "CHECK_after" : "CHECK_before", "unit.c", "case", &c) &&
        pc_node(s, PC_PROJECT ".unit.GROUP_group", "GROUP_group", "unit.c", "suite", &g) &&
        pc_node(s, PC_PROJECT ".unit.case_sink", "case_sink", "unit.c", NULL, &cs) &&
        pc_node(s, PC_PROJECT ".unit.suite_sink", "suite_sink", "unit.c", NULL, &gs) &&
        pc_call(s, c.id, cs.id) && pc_call(s, g.id, gs.id);
    cbm_node_free_fields(&c); cbm_node_free_fields(&g); cbm_node_free_fields(&cs); cbm_node_free_fields(&gs);
    cbm_store_close(s); return ok;
}
static bool pc_ordinary_graph(const char *db, const char *name) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    if (!s) return false;
    char qn[256]; int n = snprintf(qn, sizeof(qn), PC_PROJECT ".unit.%s", name);
    cbm_node_t owner = {0}, sink = {0};
    bool ok = n > 0 && (size_t)n < sizeof(qn) && pc_node(s, qn, name, "unit.c", NULL, &owner) &&
        pc_node(s, PC_PROJECT ".unit.sink", "sink", "unit.c", NULL, &sink) && pc_call(s, owner.id, sink.id);
    cbm_node_free_fields(&owner); cbm_node_free_fields(&sink); cbm_store_close(s); return ok;
}
static bool pc_no_roles(const char *db) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    cbm_node_t *nodes = NULL; int count = 0;
    bool ok = s && cbm_store_find_nodes(s, PC_PROJECT, &nodes, &count) == CBM_STORE_OK && count > 0;
    if (ok) for (int i = 0; i < count; i++) if (!pc_role(&nodes[i], NULL)) ok = false;
    cbm_store_free_nodes(nodes, count); cbm_store_close(s); return ok;
}
static bool pc_generation(const char *db, char out[128]) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    bool ok = s && cbm_store_generation(s, out, 128) == CBM_STORE_OK && out[0] && strcmp(out, "legacy") != 0;
    cbm_store_close(s); return ok;
}
static bool pc_snapshot(pc_fixture_t *f, pc_snapshot_t *out) {
    return pc_generation(f->db, out->generation) && pc_copy(f->db, f->saved);
}
static bool pc_preserved(pc_fixture_t *f, const pc_snapshot_t *before) {
    char now[128];
    return pc_files_equal(f->db, f->saved) && pc_generation(f->db, now) &&
        strcmp(now, before->generation) == 0 && pc_ordinary_graph(f->db, "preserved");
}
static bool pc_source_hash(const char *db, char out[65]) {
    cbm_store_t *s = cbm_store_open_path_query(db); cbm_file_hash_t h = {0};
    bool ok = s && cbm_store_get_file_hash(s, PC_PROJECT, "unit.c", &h) == CBM_STORE_OK && h.sha256 && strlen(h.sha256) == 64;
    if (ok) memcpy(out, h.sha256, 65);
    cbm_store_clear_file_hash(&h); cbm_store_close(s); return ok;
}
static bool pc_missing(const char *db, const char *qn) {
    cbm_store_t *s = cbm_store_open_path_query(db); cbm_node_t n = {0};
    bool ok = s && cbm_store_find_node_by_qn(s, PC_PROJECT, qn, &n) == CBM_STORE_NOT_FOUND;
    cbm_node_free_fields(&n); cbm_store_close(s); return ok;
}
/* A direct discovery witness for both conditions of the existing full-driver gate.
 * This is not a runtime worker/route observer; the pipeline handle is opaque. */
static bool pc_parallel_inputs(pc_fixture_t *f, cbm_index_mode_t mode) {
    cbm_file_info_t *files = NULL; int count = 0;
    cbm_discover_opts_t opts = {.mode = mode};
    bool ok = cbm_default_worker_count(true) == 2 && getenv("CBM_INDEX_SINGLE_THREAD") == NULL &&
        cbm_discover(f->repo, &opts, &files, &count) == 0 && count > 50;
    for (int n = 0; ok && n < 51; n++) {
        char leaf[40]; (void)snprintf(leaf, sizeof(leaf), "filler_%02d.c", n);
        int matches = 0;
        for (int i = 0; i < count; i++)
            if (pc_text(files[i].rel_path, leaf) && files[i].language == CBM_LANG_C) matches++;
        ok = matches == 1;
    }
    cbm_discover_free(files, count); return ok;
}
static bool pc_snapshot_equal(pc_fixture_t *f, const pc_snapshot_t *before) {
    char now[128];
    return pc_files_equal(f->db, f->saved) && pc_generation(f->db, now) &&
        strcmp(now, before->generation) == 0;
}
static int pc_run(pc_fixture_t *f, cbm_index_mode_t mode, bool legacy, unsigned faults,
                   void (*hook)(cbm_pipeline_t *, const char *, void *), void *ctx,
                   cbm_incremental_route_t *route) {
    cbm_pipeline_incremental_test_reset_faults();
    if (getenv("CBM_INDEX_SINGLE_THREAD") == NULL && !pc_parallel_inputs(f, mode)) return -997;
    cbm_pipeline_t *p = cbm_pipeline_new(f->repo, f->db, mode);
    if (!p) return -999;
    bool named = cbm_pipeline_set_project_name(p, PC_PROJECT);
    cbm_pipeline_set_persistence(p, false);
    if (hook) cbm_pipeline_set_before_publish_hook_for_tests(p, hook, ctx);
    cbm_pipeline_test_set_fault_mask(p, faults);
    if (legacy) cbm_pipeline_incremental_test_force_legacy_partial_once();
    int rc = named ? cbm_pipeline_run(p) : -998;
    if (route) *route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(p);
    cbm_pipeline_incremental_test_reset_faults();
    return rc;
}
static bool pc_seed(pc_fixture_t *f, const char *config) {
    return pc_source_write(f, "unit.c", pc_legacy_source) &&
        (!config || pc_write(f->config, config)) && pc_run(f, CBM_MODE_FULL, false, 0, NULL, NULL, NULL) == 0 &&
        pc_ordinary_graph(f->db, "preserved");
}
static bool pc_close(pc_fixture_t *f, pc_env_t *env) {
    cbm_pipeline_incremental_test_reset_faults();
    bool removed = !f->home[0] || th_rmtree(f->home) == 0;
    return pc_env_restore(env) && removed;
}

static int pc_configured_case(bool parallel) {
    pc_env_t env; pc_fixture_t f = {0};
    bool setup = pc_env_begin(&env, parallel) && pc_fixture_open(&f, parallel) &&
        pc_write(f.config, pc_config0) && pc_source_write(&f, "unit.c", pc_source);
    int rc = setup ? pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) : -999;
    bool graph = setup && pc_c_graph(f.db, false);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(cleanup); ASSERT_EQ(rc, 0); ASSERT_TRUE(graph); PASS();
}
TEST(pipeline_conventions_serial_roles_qns_and_calls) {
    ASSERT_EQ(pc_init_status, 0); return pc_configured_case(false);
}
TEST(pipeline_conventions_parallel_roles_qns_and_calls) {
    ASSERT_EQ(pc_init_status, 0); return pc_configured_case(true);
}
TEST(pipeline_conventions_config_only_change_rebuilds_same_source) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; char before_hash[65], after_hash[65], before_gen[128], after_gen[128];
    bool setup = pc_env_begin(&env, true) && pc_fixture_open(&f, true) && pc_write(f.config, pc_config0) &&
        pc_source_write(&f, "unit.c", pc_source);
    bool control = setup && pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 &&
        pc_c_graph(f.db, false) && pc_source_hash(f.db, before_hash) && pc_generation(f.db, before_gen);
    bool changed = control && pc_write(f.config, pc_config1); /* Source is deliberately not rewritten. */
    int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) : -999;
    bool rebuilt = changed && pc_c_graph(f.db, true) && pc_missing(f.db, PC_PROJECT ".unit.CHECK_before") &&
        pc_source_hash(f.db, after_hash) && strcmp(before_hash, after_hash) == 0 &&
        pc_generation(f.db, after_gen) && strcmp(before_gen, after_gen) != 0;
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(rc, 0); ASSERT_TRUE(rebuilt); PASS();
}
TEST(pipeline_conventions_absent_and_default_omit_role_properties) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0};
    bool setup = pc_env_begin(&env, false) && pc_fixture_open(&f, false);
    bool absent = setup && pc_seed(&f, NULL) && pc_no_roles(f.db);
    bool configured = absent && pc_write(f.config, pc_empty_config);
    /* Both boundary faults are inapplicable without consumed declarations/owners. */
    int rc = configured ? pc_run(&f, CBM_MODE_FULL, false,
        CBM_PIPELINE_TEST_FAULT_EXTRACT_NULL | CBM_PIPELINE_TEST_FAULT_DEF_MODULES_NULL,
        NULL, NULL, NULL) : -999;
    bool defaults = configured && pc_ordinary_graph(f.db, "preserved") && pc_no_roles(f.db);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(absent); ASSERT_TRUE(configured); ASSERT_TRUE(cleanup);
    ASSERT_EQ(rc, 0); ASSERT_TRUE(defaults); PASS();
}
static bool pc_coverage_kind(const char *db, const char *path, const char *kind) {
    cbm_store_t *s = cbm_store_open_path_query(db);
    cbm_coverage_row_t *rows = NULL; int count = 0; bool found = false;
    if (s && cbm_store_coverage_get_path(s, PC_PROJECT, path, &rows, &count) == CBM_STORE_OK)
        for (int i = 0; i < count; i++) found = found || pc_text(rows[i].kind, kind);
    cbm_store_free_coverage(rows, count); cbm_store_close(s); return found;
}
/* A configured form that cannot be mapped degrades its FILE, not the index
 * (user decision 2026-10-04): the run publishes, the file is indexed without
 * configured test roles, and it carries a test_declarations diagnostic. Before,
 * one such file aborted the whole index (cbm's own repro matrices did). */
static int pc_typed_degrade(bool legacy) {
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    bool setup = pc_env_begin(&env, legacy) && pc_fixture_open(&f, legacy);
    bool control = setup && pc_seed(&f, pc_config0) && pc_snapshot(&f, &snapshot);
    bool changed = control && pc_source_write(&f, "unit.c", pc_bad_source);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    int rc = changed ? pc_run(&f, legacy ? CBM_MODE_FULL : CBM_MODE_FAST, legacy, 0, NULL, NULL, &route) : -999;
    char now[128];
    bool published = changed && pc_generation(f.db, now) && strcmp(now, snapshot.generation) != 0 &&
        pc_ordinary_graph(f.db, "preserved") && pc_no_roles(f.db) &&
        pc_coverage_kind(f.db, "unit.c", "test_declarations");
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(route, legacy ? CBM_INCREMENTAL_ROUTE_LEGACY_PARTIAL : CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(rc, 0); ASSERT_TRUE(published); PASS();
}
TEST(pipeline_conventions_typed_failure_full_degrades_the_file) {
    ASSERT_EQ(pc_init_status, 0); return pc_typed_degrade(false);
}
TEST(pipeline_conventions_typed_failure_incremental_degrades_the_file) {
    ASSERT_EQ(pc_init_status, 0); return pc_typed_degrade(true);
}

typedef struct { pc_fixture_t *fixture; int called; bool staged, written; } pc_hook_t;
static void pc_mutate_config(cbm_pipeline_t *p, const char *staging, void *opaque) {
    (void)p; pc_hook_t *h = opaque;
    h->called++;
    h->staged = pc_ordinary_graph(staging, "staged");
    h->written = pc_write(h->fixture->config, pc_config1);
}
TEST(pipeline_conventions_publication_config_mutation_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    bool setup = pc_env_begin(&env, true) && pc_fixture_open(&f, true);
    bool control = setup && pc_seed(&f, pc_config0) && pc_snapshot(&f, &snapshot);
    bool changed = control && pc_source_write(&f, "unit.c", "void sink(void) {}\nvoid staged(void) { sink(); }\n");
    pc_hook_t hook = {.fixture = &f};
    int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, 0, pc_mutate_config, &hook, NULL) : -999;
    bool preserved = changed && pc_preserved(&f, &snapshot);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(hook.called, 1); ASSERT_TRUE(hook.staged); ASSERT_TRUE(hook.written);
    ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB); ASSERT_TRUE(preserved); PASS();
}
TEST(pipeline_conventions_published_owners_survive_join_and_config_edit) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    bool setup = pc_env_begin(&env, true) && pc_fixture_open(&f, true) && pc_write(f.config, pc_config0) &&
        pc_source_write(&f, "unit.c", pc_source);
    bool control = setup && pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 &&
        pc_c_graph(f.db, false) && pc_snapshot(&f, &snapshot);
    cbm_store_t *store = control ? cbm_store_open_path_query(f.db) : NULL;
    cbm_node_t owner = {0}, suite = {0};
    bool acquired = store && pc_node(store, PC_PROJECT ".unit.CHECK_before", "CHECK_before", "unit.c", "case", &owner) &&
        pc_node(store, PC_PROJECT ".unit.GROUP_group", "GROUP_group", "unit.c", "suite", &suite);
    cbm_store_close(store); /* Returned node strings are owned by the caller. */
    bool edited = acquired && pc_write(f.config, "{not valid JSON after the completed run");
    char now[128];
    bool lifetime = edited && pc_text(owner.qualified_name, PC_PROJECT ".unit.CHECK_before") && pc_role(&owner, "case") &&
        pc_text(suite.qualified_name, PC_PROJECT ".unit.GROUP_group") && pc_role(&suite, "suite") &&
        pc_c_graph(f.db, false) && pc_files_equal(f.db, f.saved) && pc_generation(f.db, now) &&
        strcmp(now, snapshot.generation) == 0;
    cbm_node_free_fields(&owner); cbm_node_free_fields(&suite);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(acquired); ASSERT_TRUE(edited);
    ASSERT_TRUE(cleanup); ASSERT_TRUE(lifetime); PASS();
}

static bool pc_cpp_files(pc_fixture_t *f) {
    char old[1024];
    return pc_path(old, sizeof(old), f->repo, "unit.c") && cbm_unlink(old) == 0 &&
        pc_write(f->config, pc_cpp_config) &&
        pc_source_write(f, "engine.h", "struct Engine { void work() {} };\n") &&
        pc_source_write(f, "unit.cpp", pc_cpp_source);
}
static bool pc_cpp_graph(const char *db) {
    cbm_store_t *s = cbm_store_open_path_query(db); cbm_node_t owner = {0}, *nodes = NULL; int count = 0;
    bool ok = s && pc_node(s, PC_PROJECT ".unit.CHECK_alpha", "CHECK_alpha", "unit.cpp", "case", &owner) &&
        cbm_store_find_nodes_by_file(s, PC_PROJECT, "engine.h", &nodes, &count) == CBM_STORE_OK;
    int matches = 0; int64_t target = 0;
    if (ok) for (int i = 0; i < count; i++) {
        if (pc_text(nodes[i].name, "work") && pc_text(nodes[i].label, "Method") &&
            pc_text(nodes[i].qualified_name, PC_PROJECT ".engine.Engine.work")) {
            matches++; target = nodes[i].id;
        }
    }
    ok = ok && matches == 1 && pc_call(s, owner.id, target);
    cbm_node_free_fields(&owner); cbm_store_free_nodes(nodes, count); cbm_store_close(s); return ok;
}
TEST(pipeline_conventions_disabled_required_parallel_crosswalk_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    bool setup = pc_env_begin(&env, true) && pc_fixture_open(&f, true);
    bool control = setup && pc_seed(&f, NULL) && pc_snapshot(&f, &snapshot);
    bool changed = control && pc_cpp_files(&f) && cbm_setenv("CBM_DISABLE_LSP_CROSS", "1", 1) == 0;
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, &route) : -999;
    bool preserved = changed && pc_preserved(&f, &snapshot);
    (void)cbm_unsetenv("CBM_DISABLE_LSP_CROSS");
    bool recovery = changed && getenv("CBM_DISABLE_LSP_CROSS") == NULL &&
        pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 && pc_cpp_graph(f.db);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_FORCED_FULL); ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_TRUE(preserved); ASSERT_TRUE(recovery); PASS();
}
TEST(pipeline_conventions_configured_null_result_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    for (int parallel = 0; parallel < 2; parallel++) {
        pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
        bool setup = pc_env_begin(&env, parallel != 0) && pc_fixture_open(&f, parallel != 0);
        bool control = setup && pc_seed(&f, pc_config0) && pc_snapshot(&f, &snapshot);
        bool changed = control && pc_source_write(&f, "unit.c", pc_source);
        cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
        int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, CBM_PIPELINE_TEST_FAULT_EXTRACT_NULL,
            NULL, NULL, &route) : -999;
        bool preserved = changed && pc_preserved(&f, &snapshot);
        bool recovery = changed && pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 && pc_c_graph(f.db, false);
        bool cleanup = pc_close(&f, &env);
        if (rc != CBM_PIPELINE_ABORT_PRESERVE_DB || !preserved || !recovery)
            fprintf(stderr, "configured NULL-result boundary (%s)\n", parallel ? "parallel" : "serial");
        ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
        ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_FORCED_FULL); ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
        ASSERT_TRUE(preserved); ASSERT_TRUE(recovery);
    }
    PASS();
}
TEST(pipeline_conventions_required_def_modules_null_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    bool setup = pc_env_begin(&env, false) && pc_fixture_open(&f, false);
    bool control = setup && pc_seed(&f, NULL) && pc_snapshot(&f, &snapshot);
    bool changed = control && pc_cpp_files(&f);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, CBM_PIPELINE_TEST_FAULT_DEF_MODULES_NULL,
        NULL, NULL, &route) : -999;
    bool preserved = changed && pc_preserved(&f, &snapshot);
    bool recovery = changed && pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 && pc_cpp_graph(f.db);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_FORCED_FULL); ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB);
    ASSERT_TRUE(preserved); ASSERT_TRUE(recovery); PASS();
}

/* Body-only edit: unchanged declarations, mode, config and inventory keep the
 * natural closure candidate valid. The route assertion prevents full fallback. */
TEST(pipeline_conventions_closure_def_modules_null_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot; char old_hash[65], new_hash[65];
    bool setup = pc_env_begin(&env, false) && pc_fixture_open(&f, false) &&
        pc_write(f.config, pc_config0) && pc_source_write(&f, "unit.c", pc_source);
    bool control = setup && pc_run(&f, CBM_MODE_FULL, false, 0, NULL, NULL, NULL) == 0 &&
        pc_c_graph(f.db, false) && pc_source_hash(f.db, old_hash) && pc_snapshot(&f, &snapshot);
    const char changed_source[] =
        "void case_sink(void) {}\nvoid suite_sink(void) {}\n"
        "CHECK(before, after) { case_sink(); case_sink(); }\nGROUP(group) { suite_sink(); }\n";
    bool changed = control && pc_source_write(&f, "unit.c", changed_source);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE, retry_route = CBM_INCREMENTAL_ROUTE_NONE;
    int rc = changed ? pc_run(&f, CBM_MODE_FULL, false, CBM_PIPELINE_TEST_FAULT_DEF_MODULES_NULL,
        NULL, NULL, &route) : -999;
    bool preserved = changed && pc_snapshot_equal(&f, &snapshot) && pc_c_graph(f.db, false);
    bool recovery = changed && pc_run(&f, CBM_MODE_FULL, false, 0, NULL, NULL, &retry_route) == 0 &&
        pc_c_graph(f.db, false) && pc_source_hash(f.db, new_hash) && strcmp(old_hash, new_hash) != 0;
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_TRUE(cleanup);
    ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
    ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB); ASSERT_TRUE(preserved);
    ASSERT_EQ(retry_route, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR); ASSERT_TRUE(recovery); PASS();
}

typedef struct {
    bool setup, parked, registry, positive, rejected, reload;
} pc_spill_witness_t;
static bool pc_result_case(const CBMFileResult *r, const char *name, const char *qn) {
    if (!r || r->test_declarations_status != CBM_TEST_EXTRACT_OK || !r->has_test_definition_owners)
        return false;
    int matches = 0;
    for (int i = 0; i < r->defs.count; i++)
        if (pc_text(r->defs.items[i].name, name) && pc_text(r->defs.items[i].qualified_name, qn) &&
            r->defs.items[i].test_role == CBM_TEST_ROLE_CASE) matches++;
    return matches == 1;
}
/* First file contributes real definitions before the second file's actual
 * spilled-load boundary fails. Registry and post-failure reload stay real. */
static pc_spill_witness_t pc_collect_spill_witness(pc_fixture_t *f) {
    pc_spill_witness_t w = {0}; char dir[1024], plain_path[1024], owner_path[1024];
    const char plain[] = "void contributor(void) {}\n";
    const char owner[] = "CHECK(parked) {}\n";
    cbm_test_declarations_t *decl = NULL;
    CBMFileResult *cache[2] = {NULL, NULL}, *reload = NULL;
    cbm_result_spill_t *spill = NULL;
    CBMLSPDef *defs = NULL;
    char *modules[2] = {NULL, NULL}; int starts[3] = {-7, -7, -7}, count = -7;
    CBMArena arena; cbm_arena_init_lazy(&arena, 4096);
    atomic_int cancelled; atomic_init(&cancelled, 0);
    cbm_pipeline_ctx_t ctx = {0};
    ctx.project_name = PC_PROJECT; ctx.repo_path = f->repo; ctx.cancelled = &cancelled;
    atomic_init(&ctx.test_declarations_failed, 0);
    atomic_init(&ctx.test_definition_owners_seen, 0);
    atomic_init(&ctx.spill_mode, 0);
    if (!pc_path(dir, sizeof(dir), f->home, "collector-spill") || th_mkdir_p(dir) != 0 ||
        !pc_path(plain_path, sizeof(plain_path), f->repo, "contributor.c") ||
        !pc_path(owner_path, sizeof(owner_path), f->repo, "parked.c")) goto done;
    decl = cbm_test_declarations_parse(pc_config0, strlen(pc_config0), false);
    if (!decl) goto done;
    cache[0] = cbm_extract_file_ex_with_tests(plain, (int)strlen(plain), CBM_LANG_C,
        PC_PROJECT, "contributor.c", 0, NULL, NULL, NULL, NULL, decl);
    if (!cache[0] || cache[0]->test_declarations_status != CBM_TEST_EXTRACT_OK || cache[0]->defs.count < 1)
        goto done;
    cbm_result_compact(cache[0]);
    cache[1] = cbm_extract_file_ex_with_tests(owner, (int)strlen(owner), CBM_LANG_C,
        PC_PROJECT, "parked.c", 0, NULL, NULL, NULL, NULL, decl);
    if (!pc_result_case(cache[1], "CHECK_parked", PC_PROJECT ".parked.CHECK_parked")) goto done;
    cbm_result_compact(cache[1]);
    ctx.test_declarations = decl;
    atomic_store(&ctx.test_definition_owners_seen, 1); /* Actual configured owner was just verified. */
    ctx.gbuf = cbm_gbuf_new(PC_PROJECT, f->repo); ctx.registry = cbm_registry_new();
    spill = cbm_result_spill_open(dir, 1, 2); ctx.spill = spill;
    w.setup = ctx.gbuf && ctx.registry && spill;
    if (!w.setup) goto done;
    if (!cbm_result_spill_park(spill, 0, 1, cache[1])) goto done;
    cache[1] = NULL; /* park consumed it. */
    int64_t parked = 0, bytes = 0, loads_before = 0, loads_registry = 0, loads_control = 0, loads_after = 0;
    cbm_result_spill_stats(spill, &parked, &bytes, &loads_before);
    w.parked = cbm_result_spill_has(spill, 1) && parked == 1 && bytes > 0 && cache[0]->defs.count > 0;
    if (!w.parked) goto done;
    cbm_file_info_t files[2] = {
        {.path = plain_path, .rel_path = "contributor.c", .language = CBM_LANG_C, .size = sizeof(plain)-1},
        {.path = owner_path, .rel_path = "parked.c", .language = CBM_LANG_C, .size = sizeof(owner)-1}
    };
    int registry_rc = cbm_build_registry_from_cache(&ctx, files, 2, cache);
    cbm_result_spill_stats(spill, &parked, &bytes, &loads_registry);
    cbm_resolution_t resolved = cbm_registry_resolve(ctx.registry, "CHECK_parked",
        PC_PROJECT ".parked", NULL, NULL, 0);
    w.registry = registry_rc == 0 && loads_registry > loads_before &&
        pc_text(resolved.qualified_name, PC_PROJECT ".parked.CHECK_parked");
    if (!w.registry) goto done;
    defs = cbm_pxc_collect_all_defs(&ctx, &arena, cache, files, 2, PC_PROJECT, modules, &count, starts);
    cbm_result_spill_stats(spill, &parked, &bytes, &loads_control);
    w.positive = defs && starts[0] == 0 && starts[1] > 0 && starts[2] > starts[1] &&
        count == starts[2] && loads_control > loads_registry;
    free(defs); defs = NULL;
    for (int i = 0; i < 2; i++) { free(modules[i]); modules[i] = NULL; }
    if (!w.positive) goto done;
    ctx.test_fault_mask = CBM_PIPELINE_TEST_FAULT_COLLECT_SPILL_NULL;
    count = -7; starts[0] = starts[1] = starts[2] = -7;
    defs = cbm_pxc_collect_all_defs(&ctx, &arena, cache, files, 2, PC_PROJECT, modules, &count, starts);
    w.rejected = defs == NULL && count == -1 && starts[0] == 0 && starts[1] == 0 && starts[2] == 0;
    reload = cbm_result_spill_load(spill, 1); /* Keep mask set: only collector loads are intercepted. */
    cbm_result_spill_stats(spill, &parked, &bytes, &loads_after);
    w.reload = pc_result_case(reload, "CHECK_parked", PC_PROJECT ".parked.CHECK_parked") &&
        cbm_result_spill_has(spill, 1) && loads_after > loads_control;
done:
    free(defs);
    for (int i = 0; i < 2; i++) { free(modules[i]); cbm_free_result(cache[i]); }
    cbm_free_result(reload); cbm_registry_free(ctx.registry); cbm_gbuf_free(ctx.gbuf);
    if (spill) cbm_result_spill_close(spill);
    cbm_arena_destroy(&arena); cbm_test_declarations_free(decl);
    return w;
}
TEST(pipeline_conventions_spilled_collect_failure_preserves_generation) {
    ASSERT_EQ(pc_init_status, 0);
    pc_env_t env; pc_fixture_t f = {0}; pc_snapshot_t snapshot;
    size_t floor = cbm_result_spill_free_floor_bytes_for_tests(), room = (size_t)1 << 30;
    bool pin_valid = floor <= SIZE_MAX - room;
    size_t old_pin = pin_valid ? cbm_result_spill_pin_free_bytes_for_tests(floor + room) : 0;
    bool setup = pc_env_begin(&env, true) && pc_fixture_open(&f, true) && pin_valid;
    pc_spill_witness_t witness = setup ? pc_collect_spill_witness(&f) : (pc_spill_witness_t){0};
    bool control = setup && pc_seed(&f, NULL) && pc_snapshot(&f, &snapshot);
    bool changed = control && pc_write(f.config, pc_config0) && pc_source_write(&f, "unit.c", pc_source) &&
        cbm_setenv("CBM_MEM_SPILL", "1", 1) == 0 && pc_text(getenv("CBM_MEM_SPILL"), "1") &&
        pc_parallel_inputs(&f, CBM_MODE_FAST);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    int rc = changed ? pc_run(&f, CBM_MODE_FAST, false, CBM_PIPELINE_TEST_FAULT_COLLECT_SPILL_NULL,
        NULL, NULL, &route) : -999;
    bool preserved = changed && pc_preserved(&f, &snapshot);
    bool recovery = changed && pc_run(&f, CBM_MODE_FAST, false, 0, NULL, NULL, NULL) == 0 &&
        pc_c_graph(f.db, false);
    if (pin_valid) (void)cbm_result_spill_pin_free_bytes_for_tests(old_pin);
    bool cleanup = pc_close(&f, &env);
    ASSERT_TRUE(setup); ASSERT_TRUE(cleanup);
    ASSERT_TRUE(witness.setup); ASSERT_TRUE(witness.parked); ASSERT_TRUE(witness.registry);
    ASSERT_TRUE(witness.positive); ASSERT_TRUE(witness.reload); ASSERT_TRUE(witness.rejected);
    ASSERT_TRUE(control); ASSERT_TRUE(changed); ASSERT_EQ(route, CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(rc, CBM_PIPELINE_ABORT_PRESERVE_DB); ASSERT_TRUE(preserved); ASSERT_TRUE(recovery); PASS();
}

SUITE(pipeline_conventions) {
    pc_init_status = cbm_init();
    RUN_TEST(pipeline_conventions_serial_roles_qns_and_calls);
    RUN_TEST(pipeline_conventions_parallel_roles_qns_and_calls);
    RUN_TEST(pipeline_conventions_config_only_change_rebuilds_same_source);
    RUN_TEST(pipeline_conventions_absent_and_default_omit_role_properties);
    RUN_TEST(pipeline_conventions_typed_failure_full_degrades_the_file);
    RUN_TEST(pipeline_conventions_typed_failure_incremental_degrades_the_file);
    RUN_TEST(pipeline_conventions_publication_config_mutation_preserves_generation);
    RUN_TEST(pipeline_conventions_published_owners_survive_join_and_config_edit);
    RUN_TEST(pipeline_conventions_disabled_required_parallel_crosswalk_preserves_generation);
    RUN_TEST(pipeline_conventions_configured_null_result_preserves_generation);
    RUN_TEST(pipeline_conventions_required_def_modules_null_preserves_generation);
    RUN_TEST(pipeline_conventions_closure_def_modules_null_preserves_generation);
    RUN_TEST(pipeline_conventions_spilled_collect_failure_preserves_generation);
    cbm_work_arena_release();
}
