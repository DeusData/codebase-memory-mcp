/* Independent R2 frozen-input candidate tests. No registry/admission claims. */
#include "test_framework.h"
#include "test_helpers.h"
#include "yyjson/yyjson.h"
#include <discover/test_conventions.h>
#include <discover/userconfig.h>
#include <errno.h>
#include <foundation/arena.h>
#include <pipeline/pipeline.h>
#include <pipeline/pipeline_internal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <store/store.h>
#include <string.h>

#if !defined(CBM_ENABLE_TEST_SEAMS) || !CBM_ENABLE_TEST_SEAMS
#error "pipeline_frozen requires the canonical CBM_ENABLE_TEST_SEAMS build"
#endif

#define PF_PROJECT "frozenprobe"
#define PF_H "abcdef0123456789abcdef0123456789abcdef01"
#define PF_H_UPPER "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
#define PF_M "1234567890abcdef1234567890abcdef12345678"
#define PF_DISABLED "537fa3e574ba1b46eabb9e69cbc7b21658a5ffbab54f1d9f42893917212b217a"
#define PF_ABSENT "c3c85f57db7b7d6a97ba99edd37a47f71e9bbacf13b7ac613885dceae1b00c3f"
#define PF_EMPTY "25c76ed59928c7e5c1418d9746738114f6dbb13c95cc715dda3cb7e3d8f3c1b2"
#define PF_CONFIG0_SHA "6ed72ee01069298818620c9ea4199dc8a0875712431a208f2a6af19dba922b75"
#define PF_CONFIG_ARG(arg)                                                      \
    "{\"extra_extensions\":{\".pfsource\":\"C\"},\"test_impact\":{\"version\":" \
    "1,\"tests\":{\"conventions\":["                                            \
    "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_"  \
    "args\":[" arg "]},"                                                        \
    "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"GROUP\",\"name_" \
    "args\":[0]}]}}}"
static const char pf_config0[] = PF_CONFIG_ARG("0");
static const char pf_config1[] = PF_CONFIG_ARG("1");
static const char pf_source[] = "void case_sink(void) {}\nvoid suite_sink(void) {}\n"
                                "CHECK(before, after) {\n"
                                "    case_sink();\n"
                                "}\n"
                                "GROUP(group) {\n"
                                "    suite_sink();\n"
                                "}\n";
static const char pf_ordinary[] = "void sink(void) {}\nvoid ordinary(void) { sink(); }\n";
static const char pf_physical_config[] = "{\"extra_extensions\":{\".pfsource\":\"python\"}}\n";
static int pf_init_status;

typedef struct {
    const char *name;
    bool present;
    char value[4096];
} pf_env_value_t;
typedef struct {
    char home[1024], source[1024], destination[1024], db[1024];
    char control[1024], saved[1024], config[1024];
    pf_env_value_t env[4];
    bool env_saved;
    const cbm_userconfig_t *previous_global;
} pf_fixture_t;

static bool pf_text(const char *a, const char *b) {
    return a && b && !strcmp(a, b);
}
static bool pf_path(char *out, size_t size, const char *base, const char *leaf) {
    int n = snprintf(out, size, "%s/%s", base, leaf);
    return n > 0 && (size_t)n < size;
}
static bool pf_write(const char *path, const char *text) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file)
        return false;
    size_t len = strlen(text);
    bool ok = fwrite(text, 1, len, file) == len;
    return fclose(file) == 0 && ok;
}
static bool pf_source_write(pf_fixture_t *f, const char *leaf, const char *text) {
    char path[1024];
    return pf_path(path, sizeof(path), f->source, leaf) && pf_write(path, text);
}
static bool pf_absent(const char *path) {
    cbm_path_info_t info;
    return cbm_path_info_utf8(path, &info) == CBM_PATH_INFO_ABSENT;
}
static bool pf_file_is(const char *path, const char *text) {
    char data[4096];
    size_t len = strlen(text);
    if (len >= sizeof(data))
        return false;
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return false;
    size_t n = fread(data, 1, len + 1, file);
    bool ok = n == len && !memcmp(data, text, len) && !ferror(file);
    return fclose(file) == 0 && ok;
}
static bool pf_files_equal(const char *a, const char *b) {
    FILE *left = cbm_fopen(a, "rb"), *right = cbm_fopen(b, "rb");
    bool ok = left && right;
    unsigned char x[4096], y[4096];
    while (ok) {
        size_t nx = fread(x, 1, sizeof(x), left), ny = fread(y, 1, sizeof(y), right);
        if (nx != ny || memcmp(x, y, nx)) {
            ok = false;
            break;
        }
        if (nx < sizeof(x)) {
            ok = !ferror(left) && !ferror(right);
            break;
        }
    }
    if (left && fclose(left))
        ok = false;
    if (right && fclose(right))
        ok = false;
    return ok;
}
static bool pf_directory_only(const char *directory, const char *allowed) {
    cbm_dir_t *dir = cbm_opendir(directory);
    if (!dir)
        return false;
    bool ok = true;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (pf_text(entry->name, ".") || pf_text(entry->name, ".."))
            continue;
        if (!allowed || !pf_text(entry->name, allowed))
            ok = false;
    }
    cbm_closedir(dir);
    return ok;
}
static bool pf_env_start(pf_fixture_t *f) {
    static const char *const names[] = {"CBM_WORKERS", "CBM_INDEX_SINGLE_THREAD",
                                        "CBM_DISABLE_LSP_CROSS", "CBM_MEM_SPILL"};
    f->previous_global = cbm_get_user_lang_config();
    for (size_t i = 0; i < 4; ++i) {
        f->env[i].name = names[i];
        const char *value = getenv(names[i]);
        f->env[i].present = value != NULL;
        if (value) {
            if (strlen(value) >= sizeof(f->env[i].value))
                return false;
            memcpy(f->env[i].value, value, strlen(value) + 1);
        }
    }
    f->env_saved = true;
    return cbm_setenv(names[0], "2", 1) == 0 && cbm_setenv(names[1], "1", 1) == 0 &&
           cbm_unsetenv(names[2]) == 0 && cbm_unsetenv(names[3]) == 0;
}
static bool pf_close(pf_fixture_t *f) {
    cbm_set_user_lang_config(f->previous_global);
    bool ok = true;
    if (f->env_saved)
        for (size_t i = 0; i < 4; ++i) {
            pf_env_value_t *v = &f->env[i];
            int rc = v->present ? cbm_setenv(v->name, v->value, 1) : cbm_unsetenv(v->name);
            const char *now = getenv(v->name);
            if (rc || (v->present ? !pf_text(now, v->value) : now != NULL))
                ok = false;
        }
    if (f->home[0] && th_rmtree(f->home))
        ok = false;
    return ok;
}
static bool pf_open(pf_fixture_t *f) {
    memset(f, 0, sizeof(*f));
    f->previous_global = cbm_get_user_lang_config();
    const char *home = th_mktempdir("cbm-frozen-input");
    if (!home || strlen(home) >= sizeof(f->home))
        return false;
    memcpy(f->home, home, strlen(home) + 1);
    return pf_path(f->source, sizeof(f->source), home, "source") &&
           pf_path(f->destination, sizeof(f->destination), home, "destination") &&
           pf_path(f->db, sizeof(f->db), f->destination, "candidate.db") &&
           pf_path(f->control, sizeof(f->control), home, "control.db") &&
           pf_path(f->saved, sizeof(f->saved), home, "saved.db") &&
           pf_path(f->config, sizeof(f->config), f->source, ".codebase-memory.json") &&
           th_mkdir_p(f->source) == 0 && th_mkdir_p(f->destination) == 0 && pf_env_start(f);
}
static bool pf_role(const cbm_node_t *node, const char *role) {
    if (!node->properties_json)
        return false;
    yyjson_doc *doc = yyjson_read(node->properties_json, strlen(node->properties_json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *field = yyjson_is_obj(root) ? yyjson_obj_get(root, "test_role") : NULL;
    bool ok = yyjson_is_obj(root) && (role ? pf_text(yyjson_get_str(field), role) : !field);
    yyjson_doc_free(doc);
    return ok;
}
static bool pf_node(cbm_store_t *store, const char *qn, const char *name, const char *file,
                    const char *role, cbm_node_t *out) {
    memset(out, 0, sizeof(*out));
    return cbm_store_find_node_by_qn(store, PF_PROJECT, qn, out) == CBM_STORE_OK &&
           pf_text(out->project, PF_PROJECT) && pf_text(out->qualified_name, qn) &&
           pf_text(out->name, name) && pf_text(out->file_path, file) && pf_role(out, role);
}
static bool pf_call(cbm_store_t *store, int64_t from, int64_t to) {
    cbm_edge_t *edges = NULL;
    int count = 0;
    bool read =
        cbm_store_find_edges_by_source_type(store, from, "CALLS", &edges, &count) == CBM_STORE_OK;
    bool found = false;
    if (read)
        for (int i = 0; i < count; ++i)
            if (edges[i].source_id == from && edges[i].target_id == to &&
                pf_text(edges[i].type, "CALLS"))
                found = true;
    cbm_store_free_edges(edges, count);
    return read && found;
}
static bool pf_ordinary_graph(const char *path) {
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store)
        return false;
    cbm_node_t a = {0}, b = {0};
    bool ok = pf_node(store, PF_PROJECT ".ordinary.ordinary", "ordinary", "ordinary.c", NULL, &a) &&
              pf_node(store, PF_PROJECT ".ordinary.sink", "sink", "ordinary.c", NULL, &b) &&
              pf_call(store, a.id, b.id);
    cbm_node_free_fields(&a);
    cbm_node_free_fields(&b);
    cbm_store_close(store);
    return ok;
}
static bool pf_real_control(pf_fixture_t *f) {
    if (!pf_source_write(f, "ordinary.c", pf_ordinary))
        return false;
    cbm_pipeline_t *pipeline = cbm_pipeline_new(f->source, f->control, CBM_MODE_FULL);
    if (!pipeline)
        return false;
    cbm_pipeline_set_persistence(pipeline, false);
    bool ok =
        cbm_pipeline_set_project_name(pipeline, PF_PROJECT) && cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    return ok && pf_ordinary_graph(f->control) && cbm_clone_or_copy_file(f->control, f->saved) == 0;
}
static cbm_git_context_t pf_git(pf_fixture_t *f) {
    return (cbm_git_context_t){.is_git = true,
                               .is_detached = true,
                               .root_exists = true,
                               .input_path = f->source,
                               .worktree_root = f->source,
                               .git_dir = "",
                               .git_common_dir = "",
                               .canonical_root = f->source,
                               .branch = "frozen-branch",
                               .branch_slug = "frozen-branch",
                               .head_sha = PF_H_UPPER,
                               .base_sha = PF_M};
}
static cbm_pipeline_frozen_inputs_t pf_inputs(pf_fixture_t *f, const char *config,
                                              cbm_git_context_t *git, atomic_int *cancelled) {
    return (cbm_pipeline_frozen_inputs_t){.source_root = f->source,
                                          .candidate_db_path = f->db,
                                          .project = PF_PROJECT,
                                          .config_state = config ? CBM_USERCONFIG_SOURCE_PRESENT
                                                                 : CBM_USERCONFIG_SOURCE_ABSENT,
                                          .config_bytes = config,
                                          .config_len = config ? strlen(config) : 0,
                                          .pinned_git = git,
                                          .cancelled = cancelled};
}
static cbm_pipeline_frozen_status_t pf_create(pf_fixture_t *f, const char *config,
                                              atomic_int *cancelled, cbm_pipeline_frozen_t **out) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, config, &git, cancelled);
    return cbm_pipeline_frozen_create(&in, out);
}
static bool pf_fixture_graph(const char *path, bool after) {
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store)
        return false;
    cbm_node_t a = {0}, b = {0}, c = {0}, d = {0};
    bool ok =
        pf_node(store, after ? PF_PROJECT ".unit.CHECK_after" : PF_PROJECT ".unit.CHECK_before",
                after ? "CHECK_after" : "CHECK_before", "unit.pfsource", "case", &a) &&
        pf_node(store, PF_PROJECT ".unit.GROUP_group", "GROUP_group", "unit.pfsource", "suite",
                &b) &&
        pf_node(store, PF_PROJECT ".unit.case_sink", "case_sink", "unit.pfsource", NULL, &c) &&
        pf_node(store, PF_PROJECT ".unit.suite_sink", "suite_sink", "unit.pfsource", NULL, &d) &&
        pf_call(store, a.id, c.id) && pf_call(store, b.id, d.id);
    cbm_node_free_fields(&a);
    cbm_node_free_fields(&b);
    cbm_node_free_fields(&c);
    cbm_node_free_fields(&d);
    cbm_store_close(store);
    return ok;
}
static bool pf_branch_graph(pf_fixture_t *f, const char *path) {
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store)
        return false;
    cbm_git_context_t git = pf_git(f);
    git.head_sha = PF_H;
    char expected[2048];
    char *qn = cbm_git_context_branch_qn(PF_PROJECT, &git);
    bool encoded = qn && cbm_git_context_props_json(&git, expected, sizeof(expected)) > 0;
    cbm_node_t *nodes = NULL;
    int count = 0;
    bool ok = encoded &&
              cbm_store_find_nodes_by_label(store, PF_PROJECT, "Branch", &nodes, &count) ==
                  CBM_STORE_OK &&
              count == 1 && pf_text(nodes[0].name, "frozen-branch") &&
              pf_text(nodes[0].qualified_name, qn) && pf_text(nodes[0].properties_json, expected);
    free(qn);
    cbm_store_free_nodes(nodes, count);
    cbm_store_close(store);
    return ok;
}
static bool pf_feature_control(pf_fixture_t *f) {
    cbm_pipeline_frozen_t *owner = NULL;
    bool ok = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
              cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK &&
              pf_text(cbm_pipeline_frozen_candidate_path(owner), f->db) && pf_ordinary_graph(f->db);
    cbm_pipeline_frozen_free(owner);
    bool retained = !pf_absent(f->db);
    bool removed = retained && cbm_unlink(f->db) == 0;
    return ok && retained && removed && pf_directory_only(f->destination, NULL);
}
static bool pf_snapshot_view(const cbm_userconfig_t *config, cbm_userconfig_source_state_t state,
                             const char *expected, size_t len) {
    const char *bytes = "guard";
    size_t length = 123;
    cbm_userconfig_source_state_t got = cbm_userconfig_project_source(config, &bytes, &length);
    if (got != state || length != len)
        return false;
    return state == CBM_USERCONFIG_SOURCE_PRESENT ? bytes && !memcmp(bytes, expected, len)
                                                  : bytes == NULL;
}
static bool pf_config_failure(cbm_userconfig_t *guard, cbm_userconfig_source_state_t state,
                              const void *bytes, size_t len,
                              cbm_userconfig_snapshot_status_t expected) {
    cbm_userconfig_t *out = guard;
    cbm_userconfig_snapshot_status_t status =
        cbm_userconfig_from_project_bytes(state, bytes, len, &out);
    bool ok = status == expected && out == NULL;
    if (out != guard)
        cbm_userconfig_free(out);
    return ok;
}
static bool pf_config_negatives(cbm_userconfig_t *guard) {
    const char *invalid[] = {"",
                             "{",
                             "[]",
                             "null",
                             "7",
                             "{\"extra_extensions\":null}",
                             "{\"extra_extensions\":[]}",
                             "{\"extra_extensions\":{},\"extra_extensions\":{}}",
                             "{\"extra_extensions\":{\".x\":\"c\",\".x\":\"cpp\"}}",
                             "{\"extra_extensions\":{\".x\":\"c\",\"\\u002ex\":\"c\"}}",
                             "{\"extra_extensions\":{\"x\":\"c\"}}",
                             "{\"extra_extensions\":{\"\":\"c\"}}",
                             "{\"extra_extensions\":{\".x\":\"\"}}",
                             "{\"extra_extensions\":{\".x\":9}}",
                             "{\"extra_extensions\":{\".x\\u0000y\":\"c\"}}",
                             "{\"extra_extensions\":{\".x\":\"c\\u0000pp\"}}"};
    bool ok = true;
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        bool one = pf_config_failure(guard, CBM_USERCONFIG_SOURCE_PRESENT, invalid[i],
                                     strlen(invalid[i]), CBM_USERCONFIG_SNAPSHOT_INVALID);
        if (!one)
            printf("  invalid frozen config case %zu\n", i);
        ok = one && ok;
    }
    const char unknown[] = "{\"extra_extensions\":{\".x\":\"unsupported-language\"}}";
    return pf_config_failure(guard, CBM_USERCONFIG_SOURCE_PRESENT, unknown, sizeof(unknown) - 1,
                             CBM_USERCONFIG_SNAPSHOT_UNSUPPORTED) &&
           pf_config_failure(guard, CBM_USERCONFIG_SOURCE_ABSENT, "", 0,
                             CBM_USERCONFIG_SNAPSHOT_INVALID) &&
           pf_config_failure(guard, CBM_USERCONFIG_SOURCE_PRESENT, NULL, 1,
                             CBM_USERCONFIG_SNAPSHOT_INVALID) &&
           pf_config_failure(guard, CBM_USERCONFIG_SOURCE_ERROR, NULL, 0,
                             CBM_USERCONFIG_SNAPSHOT_INVALID) &&
           pf_config_failure(guard, (cbm_userconfig_source_state_t)99, NULL, 0,
                             CBM_USERCONFIG_SNAPSHOT_INVALID) &&
           ok;
}
static bool pf_config_boundary(cbm_userconfig_t *guard) {
    CBMArena arena;
    cbm_arena_init(&arena);
    char *bytes = cbm_arena_alloc(&arena, 65537);
    cbm_userconfig_t *out = NULL;
    bool ok = bytes != NULL;
    if (bytes) {
        memset(bytes, ' ', 65537);
        bytes[0] = '{';
        bytes[65535] = '}';
        ok = cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_PRESENT, bytes, 65536, &out) ==
                 CBM_USERCONFIG_SNAPSHOT_OK &&
             pf_snapshot_view(out, CBM_USERCONFIG_SOURCE_PRESENT, bytes, 65536);
        cbm_userconfig_free(out);
        ok = pf_config_failure(guard, CBM_USERCONFIG_SOURCE_PRESENT, bytes, 65537,
                               CBM_USERCONFIG_SNAPSHOT_LIMIT) &&
             ok;
    }
    cbm_arena_destroy(&arena);
    return ok;
}
static bool pf_unconsumed_config_fields(void) {
    static const char bytes[] = "{\"future\":1,\"future\":2,\"extra_extensions\":"
                                "{\".CUST\":\"CpP\",\".other\":\"c\"}}";
    cbm_userconfig_t *config = NULL;
    bool ok =
        cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_PRESENT, bytes, sizeof(bytes) - 1,
                                          &config) == CBM_USERCONFIG_SNAPSHOT_OK &&
        config && config->count == 2 && config->entries &&
        pf_text(config->entries[0].ext, ".CUST") && config->entries[0].lang == CBM_LANG_CPP &&
        pf_text(config->entries[1].ext, ".other") && config->entries[1].lang == CBM_LANG_C &&
        pf_snapshot_view(config, CBM_USERCONFIG_SOURCE_PRESENT, bytes, sizeof(bytes) - 1) &&
        pf_text(config->global_source_sha256, PF_DISABLED);
    cbm_userconfig_free(config);
    return ok;
}
TEST(pipeline_frozen_config_snapshot_contract) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_userconfig_t *legacy = NULL, *snapshot = NULL, *absent = NULL, *empty = NULL;
    bool setup = pf_open(&f) && pf_write(f.config, "{\"extra_extensions\":{\".legacy\":"
                                                   "\"c\",\".bad\":\"not-a-language\"}}");
    if (setup)
        legacy = cbm_userconfig_load_with_source(f.source);
    bool control = legacy && cbm_userconfig_lookup(legacy, ".legacy") == CBM_LANG_C &&
                   cbm_userconfig_lookup(legacy, ".bad") == CBM_LANG_COUNT;
    CBMArena arena;
    cbm_arena_init(&arena);
    char *copy = cbm_arena_strdup(&arena, pf_config0);
    bool created =
        control && copy &&
        cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_PRESENT, copy, strlen(copy),
                                          &snapshot) == CBM_USERCONFIG_SNAPSHOT_OK &&
        snapshot;
    if (copy)
        memset(copy, 0xa5, strlen(pf_config0));
    cbm_arena_destroy(&arena);
    cbm_test_declarations_t *declarations = NULL;
    const char *raw = NULL;
    size_t len = 0;
    if (created &&
        cbm_userconfig_project_source(snapshot, &raw, &len) == CBM_USERCONFIG_SOURCE_PRESENT)
        declarations = cbm_test_declarations_parse(raw, len, false);
    int count = 0;
    const cbm_test_declaration_t *rows = cbm_test_declarations_items(declarations, &count);
    bool owned =
        created && snapshot->count == 1 && snapshot->entries &&
        pf_text(snapshot->entries[0].ext, ".pfsource") && snapshot->entries[0].lang == CBM_LANG_C &&
        cbm_userconfig_lookup(snapshot, ".pfsource") == CBM_LANG_C &&
        pf_snapshot_view(snapshot, CBM_USERCONFIG_SOURCE_PRESENT, pf_config0, strlen(pf_config0)) &&
        pf_text(snapshot->global_source_sha256, PF_DISABLED) &&
        pf_text(snapshot->project_source_sha256, PF_CONFIG0_SHA) && rows && count == 2 &&
        rows[0].role == CBM_TEST_DECL_CASE && pf_text(rows[0].define_macro, "CHECK") &&
        rows[1].role == CBM_TEST_DECL_SUITE && pf_text(rows[1].define_macro, "GROUP");
    bool states = created &&
                  cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_ABSENT, NULL, 0,
                                                    &absent) == CBM_USERCONFIG_SNAPSHOT_OK &&
                  cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_PRESENT, "{}", 2,
                                                    &empty) == CBM_USERCONFIG_SNAPSHOT_OK &&
                  absent && empty && absent->count == 0 && empty->count == 0 &&
                  pf_snapshot_view(absent, CBM_USERCONFIG_SOURCE_ABSENT, NULL, 0) &&
                  pf_text(absent->project_source_sha256, PF_ABSENT) &&
                  pf_snapshot_view(empty, CBM_USERCONFIG_SOURCE_PRESENT, "{}", 2) &&
                  pf_text(empty->project_source_sha256, PF_EMPTY) &&
                  pf_text(empty->global_source_sha256, PF_DISABLED) &&
                  pf_text(absent->global_source_sha256, PF_DISABLED);
    bool unrelated = created && pf_unconsumed_config_fields();
    bool negative = unrelated && pf_config_negatives(legacy) && pf_config_boundary(legacy) &&
                    cbm_userconfig_from_project_bytes(CBM_USERCONFIG_SOURCE_PRESENT, "{}", 2,
                                                      NULL) == CBM_USERCONFIG_SNAPSHOT_INVALID;
    cbm_test_declarations_free(declarations);
    cbm_userconfig_free(snapshot);
    cbm_userconfig_free(absent);
    cbm_userconfig_free(empty);
    cbm_userconfig_free(legacy);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(created);
    ASSERT_TRUE(owned);
    ASSERT_TRUE(states);
    ASSERT_TRUE(unrelated);
    ASSERT_TRUE(negative);
    PASS();
}

static cbm_pipeline_frozen_status_t pf_create_poisoned(pf_fixture_t *f,
                                                       cbm_pipeline_frozen_t **out) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_git_context_t git = pf_git(f);
    char **fields[] = {&git.input_path,     &git.worktree_root,  &git.git_dir,
                       &git.git_common_dir, &git.canonical_root, &git.branch,
                       &git.branch_slug,    &git.head_sha,       &git.base_sha};
    bool ok = true;
    for (size_t i = 0; i < 9; ++i) {
        *fields[i] = cbm_arena_strdup(&arena, *fields[i]);
        if (!*fields[i])
            ok = false;
    }
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, pf_config0, &git, NULL);
    in.source_root = cbm_arena_strdup(&arena, f->source);
    in.candidate_db_path = cbm_arena_strdup(&arena, f->db);
    in.project = cbm_arena_strdup(&arena, PF_PROJECT);
    in.config_bytes = cbm_arena_strdup(&arena, pf_config0);
    ok = ok && in.source_root && in.candidate_db_path && in.project && in.config_bytes;
    cbm_pipeline_frozen_status_t status =
        ok ? cbm_pipeline_frozen_create(&in, out) : CBM_PIPELINE_FROZEN_OOM;
    if (ok) {
        for (size_t i = 0; i < 9; ++i)
            memset(*fields[i], 0x5a, strlen(*fields[i]));
        memset((void *)in.source_root, 0x5a, strlen(in.source_root));
        memset((void *)in.candidate_db_path, 0x5a, strlen(in.candidate_db_path));
        memset((void *)in.project, 0x5a, strlen(in.project));
        memset((void *)in.config_bytes, 0x5a, in.config_len);
    }
    cbm_arena_destroy(&arena);
    return status;
}
static bool pf_builtin_graph(const char *path) {
    cbm_store_t *store = cbm_store_open_path_query(path);
    if (!store)
        return false;
    cbm_node_t test = {0}, suite = {0}, sink = {0};
    bool ok = pf_node(store, PF_PROJECT ".builtin.TEST_alpha", "TEST_alpha", "builtin.c", "case",
                      &test) &&
              pf_node(store, PF_PROJECT ".builtin.SUITE_core", "SUITE_core", "builtin.c", "suite",
                      &suite) &&
              pf_node(store, PF_PROJECT ".builtin.builtin_sink", "builtin_sink", "builtin.c", NULL,
                      &sink) &&
              pf_call(store, test.id, sink.id) && pf_call(store, suite.id, sink.id);
    cbm_node_free_fields(&test);
    cbm_node_free_fields(&suite);
    cbm_node_free_fields(&sink);
    cbm_store_close(store);
    return ok;
}
static bool pf_builtin_control(pf_fixture_t *f) {
    static const char source[] = "void builtin_sink(void) {}\nTEST(alpha) { builtin_sink(); "
                                 "}\nSUITE(core) { builtin_sink(); }\n";
    static const char config[] = "{\"test_impact\":{\"version\":1,\"tests\":{"
                                 "\"presets\":{\"c-cbm\":true}}}}";
    if (!pf_source_write(f, "builtin.c", source))
        return false;
    cbm_pipeline_frozen_t *owner = NULL;
    bool ok = pf_create(f, config, NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
              cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK &&
              pf_builtin_graph(f->db) && pf_ordinary_graph(f->db) &&
              pf_files_equal(f->control, f->saved);
    cbm_pipeline_frozen_free(owner);
    return ok;
}
static cbm_pipeline_frozen_status_t pf_build_expected_ok(cbm_pipeline_frozen_t *owner) {
    cbm_pipeline_frozen_status_t status = cbm_pipeline_frozen_build(owner);
    if (status != CBM_PIPELINE_FROZEN_OK) {
        unsigned flags = 0;
        cbm_file_error_t *rows = NULL;
        int count = 0;
        bool evidence = cbm_pipeline_frozen_negative_evidence(owner, &flags);
        cbm_pipeline_get_file_errors(cbm_pipeline_frozen_diagnostics(owner), &rows, &count);
        printf("  frozen positive failed: status=%d evidence=%d flags=%u rows=%d\n", (int)status,
               evidence, flags, count);
        for (int i = 0; rows && i < count; i++)
            printf("    path=%s phase=%s reason=%s\n", rows[i].path ? rows[i].path : "(null)",
                   rows[i].phase ? rows[i].phase : "(null)",
                   rows[i].reason ? rows[i].reason : "(null)");
    }
    return status;
}

TEST(pipeline_frozen_detached_graph_and_owned_context) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *owner = NULL;
    bool setup = pf_open(&f);
    bool control = setup && pf_real_control(&f);
    bool source = control && pf_source_write(&f, "unit.pfsource", pf_source);
    char git_path[1024];
    bool detached =
        source && pf_path(git_path, sizeof(git_path), f.source, ".git") && pf_absent(git_path);
    cbm_pipeline_frozen_status_t created =
        detached ? pf_create_poisoned(&f, &owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ready = created == CBM_PIPELINE_FROZEN_OK && owner &&
                 !cbm_pipeline_frozen_candidate_path(owner) &&
                 pf_directory_only(f.destination, NULL);
    const cbm_pipeline_t *diag = ready ? cbm_pipeline_frozen_diagnostics(owner) : NULL;
    bool diagnostics = diag && cbm_pipeline_get_mode(diag) == CBM_MODE_FULL &&
                       pf_text(cbm_pipeline_project_name(diag), PF_PROJECT);
    cbm_pipeline_frozen_status_t status =
        ready ? pf_build_expected_ok(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool graph = status == CBM_PIPELINE_FROZEN_OK &&
                 pf_text(cbm_pipeline_frozen_candidate_path(owner), f.db) &&
                 pf_fixture_graph(f.db, false) && pf_branch_graph(&f, f.db) &&
                 pf_files_equal(f.control, f.saved);
    cbm_pipeline_frozen_free(owner);
    bool survives = graph && pf_fixture_graph(f.db, false);
    bool builtin = survives && cbm_unlink(f.db) == 0 && pf_builtin_control(&f);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(source);
    ASSERT_TRUE(detached);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(diagnostics);
    ASSERT_EQ(status, CBM_PIPELINE_FROZEN_OK);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(survives);
    ASSERT_TRUE(builtin);
    PASS();
}

typedef enum {
    PF_CHANGE_SOURCE,
    PF_CHANGE_CONTROL,
    PF_CHANGE_CONFIG,
    PF_ADD_SOURCE,
    PF_DELETE_SOURCE,
    PF_ADD_CONTROL,
    PF_DELETE_CONTROL,
    PF_ADD_CONFIG,
    PF_DELETE_CONFIG,
    PF_CANCEL_EXTERNAL,
    PF_CANCEL_OWNER
} pf_hook_kind_t;
typedef struct {
    pf_fixture_t *fixture;
    pf_hook_kind_t kind;
    bool called, changed;
    cbm_pipeline_frozen_t *owner;
    atomic_int *external;
    char stage[4096];
    bool rename_called;
} pf_hook_t;
static void pf_before_publish(cbm_pipeline_t *pipeline, const char *stage, void *context) {
    (void)pipeline;
    (void)stage;
    pf_hook_t *hook = context;
    pf_fixture_t *f = hook->fixture;
    hook->called = true;
    char path[1024];
    switch (hook->kind) {
    case PF_CHANGE_SOURCE:
        hook->changed = pf_source_write(
            f, "ordinary.c", "void sink(void) {}\nvoid ordinary(void) { sink(); sink(); }\n");
        break;
    case PF_CHANGE_CONTROL:
        hook->changed = pf_source_write(f, ".gitignore", "# changed physical control\n");
        break;
    case PF_CHANGE_CONFIG:
        hook->changed = pf_write(f->config, "{\"future\":2}\n");
        break;
    case PF_ADD_SOURCE:
        hook->changed = pf_source_write(f, "late.c", "void late_added(void) {}\n");
        break;
    case PF_DELETE_SOURCE:
        hook->changed =
            pf_path(path, sizeof(path), f->source, "ordinary.c") && cbm_unlink(path) == 0;
        break;
    case PF_ADD_CONTROL:
        hook->changed = pf_source_write(f, ".cbmignore", "# newly added\n");
        break;
    case PF_DELETE_CONTROL:
        hook->changed =
            pf_path(path, sizeof(path), f->source, ".gitignore") && cbm_unlink(path) == 0;
        break;
    case PF_ADD_CONFIG:
        hook->changed = pf_write(f->config, "{\"future\":1}\n");
        break;
    case PF_DELETE_CONFIG:
        hook->changed = cbm_unlink(f->config) == 0;
        break;
    case PF_CANCEL_EXTERNAL:
        atomic_store(hook->external, 1);
        hook->changed = true;
        break;
    case PF_CANCEL_OWNER:
        cbm_pipeline_frozen_cancel(hook->owner);
        hook->changed = true;
        break;
    }
}
static int pf_fail_rename(const char *stage, const char *destination, void *context) {
    (void)destination;
    pf_hook_t *hook = context;
    hook->rename_called = true;
    if (strlen(stage) < sizeof(hook->stage))
        memcpy(hook->stage, stage, strlen(stage) + 1);
    errno = EACCES;
    return -1;
}
static bool pf_hook_failure(pf_fixture_t *f, pf_hook_kind_t kind) {
    cbm_pipeline_frozen_t *owner = NULL;
    if (!pf_source_write(f, "ordinary.c", pf_ordinary) ||
        !pf_source_write(f, ".gitignore", "# baseline\n") ||
        !pf_write(f->config, pf_physical_config))
        return false;
    char added[1024];
    if (!pf_path(added, sizeof(added), f->source, ".cbmignore"))
        return false;
    if (!pf_absent(added) && cbm_unlink(added))
        return false;
    if (kind == PF_ADD_CONFIG && cbm_unlink(f->config))
        return false;
    pf_hook_t hook = {.fixture = f, .kind = kind};
    bool ready = pf_create(f, pf_config0, NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
                 cbm_pipeline_frozen_set_publish_hooks_for_tests(owner, pf_before_publish, NULL,
                                                                 &hook) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ok = ready && hook.called && hook.changed && status == CBM_PIPELINE_FROZEN_INPUT_CHANGED &&
              !cbm_pipeline_frozen_candidate_path(owner) && pf_absent(f->db) &&
              pf_directory_only(f->destination, NULL) && pf_files_equal(f->control, f->saved);
    cbm_pipeline_frozen_free(owner);
    if (!ok)
        printf("  final reconciliation variant %d status %d hook %d changed %d\n", (int)kind,
               (int)status, hook.called, hook.changed);
    return ok;
}
TEST(pipeline_frozen_final_reconciliation_and_explicit_override) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *owner = NULL;
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool source = control && pf_source_write(&f, "unit.pfsource", pf_source) &&
                  pf_write(f.config, pf_physical_config);
    bool ready =
        source && pf_create(&f, pf_config0, NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    bool positive = ready && pf_build_expected_ok(owner) == CBM_PIPELINE_FROZEN_OK &&
                    pf_fixture_graph(f.db, false) && pf_file_is(f.config, pf_physical_config);
    cbm_pipeline_frozen_free(owner);
    bool removed = positive && cbm_unlink(f.db) == 0;
    bool rejected = removed;
    for (int kind = PF_CHANGE_SOURCE; rejected && kind <= PF_DELETE_CONFIG; ++kind)
        rejected = pf_hook_failure(&f, (pf_hook_kind_t)kind);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(source);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(removed);
    ASSERT_TRUE(rejected);
    PASS();
}

static bool pf_sidecar_refusal(pf_fixture_t *f, const char *suffix) {
    char path[1100];
    int n = snprintf(path, sizeof(path), "%s%s", f->db, suffix);
    if (n <= 0 || (size_t)n >= sizeof(path) || !pf_write(path, "caller-owned-sidecar\n"))
        return false;
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ok = ready && status == CBM_PIPELINE_FROZEN_DEST_EXISTS &&
              !cbm_pipeline_frozen_candidate_path(owner) &&
              pf_file_is(path, "caller-owned-sidecar\n") && pf_files_equal(f->control, f->saved);
    cbm_pipeline_frozen_free(owner);
    return cbm_unlink(path) == 0 && ok && pf_directory_only(f->destination, NULL);
}
static bool pf_path_refusal(pf_fixture_t *f, int variant) {
    char bad[1024];
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, "{}", &git, NULL);
    if (variant == 0) {
        if (!pf_path(bad, sizeof(bad), f->source, "inside.db"))
            return false;
        in.candidate_db_path = bad;
    } else if (variant == 1) {
        if (!pf_path(bad, sizeof(bad), f->home, "not-created/candidate.db"))
            return false;
        in.candidate_db_path = bad;
    } else {
        if (!pf_path(bad, sizeof(bad), f->source, "ordinary.c"))
            return false;
        in.source_root = bad;
    }
    cbm_pipeline_frozen_t *owner = NULL;
    cbm_pipeline_frozen_status_t created = cbm_pipeline_frozen_create(&in, &owner);
    bool ready = created == CBM_PIPELINE_FROZEN_OK && owner;
    cbm_pipeline_frozen_status_t status = ready ? cbm_pipeline_frozen_build(owner) : created;
    bool input_preserved = variant == 2 ? pf_file_is(bad, pf_ordinary) : pf_absent(bad);
    bool ok = ready && status != CBM_PIPELINE_FROZEN_OK && status != CBM_PIPELINE_FROZEN_BUSY &&
              !cbm_pipeline_frozen_candidate_path(owner) && pf_absent(f->db) &&
              pf_directory_only(f->destination, NULL) && input_preserved;
    cbm_pipeline_frozen_free(owner);
    return ok && pf_files_equal(f->control, f->saved);
}
typedef struct {
    char path[1100];
    bool called, created;
} pf_collision_t;
static void pf_publish_collision(cbm_pipeline_t *pipeline, const char *stage, void *context) {
    (void)pipeline;
    (void)stage;
    pf_collision_t *collision = context;
    collision->called = true;
    collision->created = pf_write(collision->path, "arrived-before-commit\n");
}
static bool pf_final_destination_refusal(pf_fixture_t *f, const char *suffix) {
    pf_collision_t collision = {0};
    int n = snprintf(collision.path, sizeof(collision.path), "%s%s", f->db, suffix);
    if (n <= 0 || (size_t)n >= sizeof(collision.path))
        return false;
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
                 cbm_pipeline_frozen_set_publish_hooks_for_tests(
                     owner, pf_publish_collision, NULL, &collision) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ok = ready && collision.called && collision.created &&
              status == CBM_PIPELINE_FROZEN_DEST_EXISTS &&
              !cbm_pipeline_frozen_candidate_path(owner) &&
              pf_file_is(collision.path, "arrived-before-commit\n") &&
              pf_files_equal(f->control, f->saved);
    cbm_pipeline_frozen_free(owner);
    return cbm_unlink(collision.path) == 0 && ok && pf_directory_only(f->destination, NULL);
}
TEST(pipeline_frozen_preserves_artifacts_and_destination_namespace) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *owner = NULL;
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    char artifact[1024], graph[1024], metadata[1024], attributes[1024];
    bool paths = control && pf_path(artifact, sizeof(artifact), f.source, ".codebase-memory") &&
                 th_mkdir_p(artifact) == 0 &&
                 pf_path(graph, sizeof(graph), artifact, "graph.db.zst") &&
                 pf_path(metadata, sizeof(metadata), artifact, "meta.json") &&
                 pf_path(attributes, sizeof(attributes), f.source, ".gitattributes") &&
                 pf_write(graph, "not-an-artifact-but-owned\n") &&
                 pf_write(metadata, "{\"sentinel\":true}\n") &&
                 pf_write(attributes, "# caller-owned attributes\n");
    bool ready = paths && pf_create(&f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    bool positive = ready && cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK &&
                    pf_ordinary_graph(f.db);
    cbm_pipeline_frozen_free(owner);
    bool preserved = positive && pf_file_is(graph, "not-an-artifact-but-owned\n") &&
                     pf_file_is(metadata, "{\"sentinel\":true}\n") &&
                     pf_file_is(attributes, "# caller-owned attributes\n") &&
                     pf_file_is(TH_PATH(f.source, "ordinary.c"), pf_ordinary);
    bool removed = preserved && cbm_unlink(f.db) == 0;
    const char *suffixes[] = {"", "-wal", "-shm", "-journal"};
    bool refused = removed;
    for (size_t i = 0; refused && i < 4; ++i)
        refused = pf_sidecar_refusal(&f, suffixes[i]);
    for (size_t i = 0; refused && i < 4; ++i)
        refused = pf_final_destination_refusal(&f, suffixes[i]);
    for (int i = 0; refused && i < 3; ++i)
        refused = pf_path_refusal(&f, i);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(paths);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(preserved);
    ASSERT_TRUE(removed);
    ASSERT_TRUE(refused);
    PASS();
}

static bool pf_cancel_failure(pf_fixture_t *f, int variant) {
    atomic_int cancelled;
    atomic_init(&cancelled, variant == 0 ? 1 : 0);
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", &cancelled, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    pf_hook_t hook = {.fixture = f,
                      .kind = variant == 2 ? PF_CANCEL_EXTERNAL : PF_CANCEL_OWNER,
                      .owner = owner,
                      .external = &cancelled};
    if (ready && variant == 1)
        cbm_pipeline_frozen_cancel(owner);
    if (ready && variant >= 2)
        ready = cbm_pipeline_frozen_set_publish_hooks_for_tests(owner, pf_before_publish, NULL,
                                                                &hook) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ok = ready && status == CBM_PIPELINE_FROZEN_CANCELLED &&
              !cbm_pipeline_frozen_candidate_path(owner) &&
              (variant < 2 || (hook.called && hook.changed)) && pf_absent(f->db) &&
              pf_directory_only(f->destination, NULL);
    atomic_store(&cancelled, 0);
    ok = cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_INVALID && ok;
    cbm_pipeline_frozen_free(owner);
    return ok && pf_files_equal(f->control, f->saved);
}
static bool pf_rename_failure(pf_fixture_t *f) {
    cbm_pipeline_frozen_t *owner = NULL;
    pf_hook_t hook = {.fixture = f};
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
                 cbm_pipeline_frozen_set_publish_hooks_for_tests(owner, NULL, pf_fail_rename,
                                                                 &hook) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool ok = ready && hook.rename_called && hook.stage[0] && status == CBM_PIPELINE_FROZEN_IO &&
              !cbm_pipeline_frozen_candidate_path(owner) && pf_absent(hook.stage) &&
              pf_absent(f->db) && pf_directory_only(f->destination, NULL) &&
              pf_files_equal(f->control, f->saved);
    cbm_pipeline_frozen_free(owner);
    return ok;
}
static bool pf_busy_retry(pf_fixture_t *f) {
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    bool locked = ready && cbm_pipeline_try_lock();
    cbm_pipeline_frozen_status_t status =
        locked ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool busy = locked && status == CBM_PIPELINE_FROZEN_BUSY &&
                !cbm_pipeline_frozen_candidate_path(owner) &&
                pf_directory_only(f->destination, NULL);
    if (locked)
        cbm_pipeline_unlock();
    bool retry = busy && cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK &&
                 pf_ordinary_graph(f->db);
    cbm_pipeline_frozen_free(owner);
    return retry && pf_files_equal(f->control, f->saved);
}
TEST(pipeline_frozen_cancel_rename_and_busy_retry) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool positive = control && pf_feature_control(&f);
    bool cancelled = positive;
    for (int variant = 0; cancelled && variant < 4; ++variant)
        cancelled = pf_cancel_failure(&f, variant);
    bool rename = cancelled && pf_rename_failure(&f);
    bool retry = rename && pf_busy_retry(&f);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(cancelled);
    ASSERT_TRUE(rename);
    ASSERT_TRUE(retry);
    PASS();
}

typedef struct {
    bool extraction, manifest, history, invalid;
} pf_route_t;
static void pf_route(const cbm_pipeline_frozen_route_event_t *event, void *context) {
    pf_route_t *seen = context;
    if (!event) {
        seen->invalid = true;
        return;
    }
    switch (event->kind) {
    case CBM_PIPELINE_FROZEN_ROUTE_EXTRACTION_WORKERS:
        seen->extraction = true;
        if (event->worker_count != 1)
            seen->invalid = true;
        break;
    case CBM_PIPELINE_FROZEN_ROUTE_MANIFEST_WORKERS:
        seen->manifest = true;
        if (event->worker_count != 1)
            seen->invalid = true;
        break;
    case CBM_PIPELINE_FROZEN_ROUTE_HISTORY_SKIPPED:
        seen->history = true;
        if (event->worker_count != 0)
            seen->invalid = true;
        break;
    default:
        seen->invalid = true;
        break;
    }
}
static bool pf_many_sources(pf_fixture_t *f) {
    for (int i = 0; i < 65; ++i) {
        char leaf[40], text[80];
        int a = snprintf(leaf, sizeof(leaf), "many_%02d.c", i);
        int b = snprintf(text, sizeof(text), "void many_%02d(void) {}\n", i);
        if (a <= 0 || (size_t)a >= sizeof(leaf) || b <= 0 || (size_t)b >= sizeof(text) ||
            !pf_source_write(f, leaf, text))
            return false;
    }
    cbm_file_info_t *files = NULL;
    int count = 0;
    cbm_discover_opts_t opts = {.mode = CBM_MODE_FULL};
    bool ok = cbm_discover(f->source, &opts, &files, &count) == 0 && count > 64;
    cbm_discover_free(files, count);
    return ok;
}
static bool pf_resource_limit(pf_fixture_t *f) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, "{}", &git, NULL);
    in.resource_policy.max_files.enabled = true;
    in.resource_policy.max_files.value = 1;
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = cbm_pipeline_frozen_create(&in, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    const cbm_pipeline_t *diag = cbm_pipeline_frozen_diagnostics(owner);
    cbm_index_resource_violation_t violation = {0};
    if (diag)
        cbm_pipeline_get_resource_violation(diag, &violation);
    bool ok = ready && status == CBM_PIPELINE_FROZEN_LIMIT && diag &&
              violation.resource == CBM_INDEX_RESOURCE_FILES && violation.limit == 1 &&
              violation.observed > 1 && !cbm_pipeline_frozen_candidate_path(owner) &&
              pf_absent(f->db);
    cbm_pipeline_frozen_free(owner);
    return ok && pf_directory_only(f->destination, NULL);
}
TEST(pipeline_frozen_actual_serial_dispatch_and_history_skip) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *owner = NULL;
    pf_route_t seen = {0}, cleared = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool many = control && pf_many_sources(&f) && cbm_setenv("CBM_WORKERS", "8", 1) == 0 &&
                cbm_unsetenv("CBM_INDEX_SINGLE_THREAD") == 0 && pf_text(getenv("CBM_WORKERS"), "8");
    bool ready = many && pf_create(&f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    bool installed = ready &&
                     cbm_pipeline_frozen_set_route_observer_for_tests(owner, pf_route, &cleared) ==
                         CBM_PIPELINE_FROZEN_OK &&
                     cbm_pipeline_frozen_set_route_observer_for_tests(owner, NULL, &cleared) ==
                         CBM_PIPELINE_FROZEN_OK &&
                     cbm_pipeline_frozen_set_route_observer_for_tests(owner, pf_route, &seen) ==
                         CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        installed ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool graph =
        status == CBM_PIPELINE_FROZEN_OK && pf_ordinary_graph(f.db) && pf_branch_graph(&f, f.db);
    bool observed = seen.extraction && seen.manifest && seen.history && !seen.invalid &&
                    !cleared.extraction && !cleared.manifest && !cleared.history &&
                    !cleared.invalid;
    bool terminal = graph &&
                    cbm_pipeline_frozen_set_route_observer_for_tests(owner, NULL, NULL) ==
                        CBM_PIPELINE_FROZEN_INVALID &&
                    cbm_pipeline_frozen_set_publish_hooks_for_tests(owner, NULL, NULL, NULL) ==
                        CBM_PIPELINE_FROZEN_INVALID;
    cbm_pipeline_frozen_free(owner);
    bool limit = terminal && cbm_unlink(f.db) == 0 && pf_resource_limit(&f);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(many);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(installed);
    ASSERT_EQ(status, CBM_PIPELINE_FROZEN_OK);
    ASSERT_TRUE(graph);
    ASSERT_TRUE(observed);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(limit);
    PASS();
}

static bool pf_create_rejects(const cbm_pipeline_frozen_inputs_t *in,
                              cbm_pipeline_frozen_t *guard) {
    cbm_pipeline_frozen_t *out = guard;
    cbm_pipeline_frozen_status_t status = cbm_pipeline_frozen_create(in, &out);
    bool ok = status == CBM_PIPELINE_FROZEN_INVALID && !out;
    if (out != guard)
        cbm_pipeline_frozen_free(out);
    return ok;
}
static bool pf_wide_oid_control(pf_fixture_t *f) {
    cbm_git_context_t git = pf_git(f);
    git.head_sha = "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789";
    git.base_sha = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, "{}", &git, NULL);
    cbm_pipeline_frozen_t *owner = NULL;
    bool ok = cbm_pipeline_frozen_create(&in, &owner) == CBM_PIPELINE_FROZEN_OK && owner &&
              cbm_pipeline_frozen_diagnostics(owner) &&
              !cbm_pipeline_frozen_candidate_path(owner) && pf_directory_only(f->destination, NULL);
    cbm_pipeline_frozen_free(owner);
    return ok;
}
static bool pf_required_context_fields(pf_fixture_t *f, cbm_pipeline_frozen_t *guard) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, "{}", &git, NULL);
    char **fields[] = {&git.input_path,     &git.worktree_root,  &git.git_dir,
                       &git.git_common_dir, &git.canonical_root, &git.branch,
                       &git.branch_slug,    &git.head_sha,       &git.base_sha};
    bool ok = true;
    for (size_t i = 0; i < 9; ++i) {
        char *saved = *fields[i];
        *fields[i] = NULL;
        bool one = pf_create_rejects(&in, guard);
        *fields[i] = saved;
        if (!one)
            printf("  required frozen context field %zu\n", i);
        ok = one && ok;
    }
    git.is_git = false;
    ok = pf_create_rejects(&in, guard) && ok;
    git.is_git = true;
    git.root_exists = false;
    ok = pf_create_rejects(&in, guard) && ok;
    git.root_exists = true;
    git.canonical_root = "";
    return pf_create_rejects(&in, guard) && ok;
}
static bool pf_invalid_create(pf_fixture_t *f, cbm_pipeline_frozen_t *guard) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, "{}", &git, NULL);
    bool ok = pf_wide_oid_control(f) && pf_required_context_fields(f, guard);
    in.project = "bad/project";
    ok = pf_create_rejects(&in, guard) && ok;
    in.project = PF_PROJECT;
    git.head_sha = "abc";
    ok = pf_create_rejects(&in, guard) && ok;
    git.head_sha = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    ok = pf_create_rejects(&in, guard) && ok; /* Full but different OID widths. */
    git.head_sha = PF_H;
    git.branch = "";
    ok = pf_create_rejects(&in, guard) && ok;
    git.branch = "frozen-branch";
    char long_path[4097];
    memset(long_path, 'x', 4096);
    long_path[0] = '/';
    long_path[4096] = '\0';
    in.source_root = long_path;
    ok = pf_create_rejects(&in, guard) && ok;
    in.source_root = f->source;
    return pf_create_rejects(NULL, guard) &&
           cbm_pipeline_frozen_create(&in, NULL) == CBM_PIPELINE_FROZEN_INVALID && ok;
}
static bool pf_unsupported_create(pf_fixture_t *f, cbm_pipeline_frozen_t *guard) {
    const char *configs[] = {
        "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{"
        "\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_"
        "args\":[0,1]}]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"pytest\":"
        "false}}}}"};
    char missing[1024];
    if (!pf_path(missing, sizeof(missing), f->home, "missing-source") || !pf_absent(missing))
        return false;
    cbm_git_context_t git = pf_git(f);
    git.input_path = missing;
    git.worktree_root = missing;
    git.canonical_root = missing;
    bool ok = true;
    for (size_t i = 0; i < 2; ++i) {
        cbm_pipeline_frozen_inputs_t in = pf_inputs(f, configs[i], &git, NULL);
        in.source_root = missing;
        cbm_pipeline_frozen_t *out = guard;
        cbm_pipeline_frozen_status_t status = cbm_pipeline_frozen_create(&in, &out);
        bool one = status == CBM_PIPELINE_FROZEN_UNSUPPORTED && !out;
        if (out != guard)
            cbm_pipeline_frozen_free(out);
        ok = one && ok;
    }
    cbm_pipeline_frozen_t *out = guard;
    cbm_pipeline_frozen_status_t status =
        pf_create(f, "{\"test_impact\":{\"version\":1,\"tests\":null}}", NULL, &out);
    ok = status == CBM_PIPELINE_FROZEN_CONFIG && !out && ok;
    if (out != guard)
        cbm_pipeline_frozen_free(out);
    return ok && pf_absent(missing);
}
static bool pf_mutate_independent_controls(pf_fixture_t *f, atomic_int *cancelled) {
    cbm_pipeline_t *ordinary = cbm_pipeline_new(f->source, f->control, CBM_MODE_FAST);
    if (!ordinary)
        return false;
    cbm_index_resource_policy_t policy = {0};
    policy.max_files.enabled = true;
    policy.max_files.value = 1;
    cbm_pipeline_set_persistence(ordinary, true);
    cbm_pipeline_set_resource_policy(ordinary, &policy);
    cbm_pipeline_bind_cancel_flag(ordinary, cancelled);
    bool ok = cbm_pipeline_set_project_name(ordinary, "ordinarychanged") &&
              pf_text(cbm_pipeline_project_name(ordinary), "ordinarychanged") &&
              cbm_pipeline_get_mode(ordinary) == CBM_MODE_FAST;
    cbm_pipeline_free(ordinary);
    return ok;
}
TEST(pipeline_frozen_owner_isolation_state_and_global_restoration) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *first = NULL, *second = NULL, *failed = NULL;
    cbm_userconfig_t *ambient = NULL;
    atomic_int ordinary_cancelled;
    atomic_init(&ordinary_cancelled, 1);
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool source = control && pf_source_write(&f, "unit.pfsource", pf_source) &&
                  pf_write(f.config, pf_physical_config);
    if (source)
        ambient = cbm_userconfig_load_with_source(f.source);
    bool ambient_ok = ambient && cbm_userconfig_lookup(ambient, ".pfsource") == CBM_LANG_PYTHON;
    if (ambient_ok)
        cbm_set_user_lang_config(ambient);
    bool ready = ambient_ok && pf_create(&f, pf_config0, NULL, &first) == CBM_PIPELINE_FROZEN_OK &&
                 first && pf_create(&f, pf_config1, NULL, &second) == CBM_PIPELINE_FROZEN_OK &&
                 second && cbm_get_user_lang_config() == ambient;
    const cbm_pipeline_t *first_diag = ready ? cbm_pipeline_frozen_diagnostics(first) : NULL;
    const cbm_pipeline_t *second_diag = ready ? cbm_pipeline_frozen_diagnostics(second) : NULL;
    bool separate =
        first_diag && second_diag && first_diag != second_diag &&
        cbm_pipeline_test_declarations(first_diag) != cbm_pipeline_test_declarations(second_diag);
    bool controls = ready && pf_mutate_independent_controls(&f, &ordinary_cancelled) &&
                    cbm_get_user_lang_config() == ambient;
    bool invalid = controls && pf_invalid_create(&f, first) && pf_unsupported_create(&f, first) &&
                   cbm_pipeline_frozen_build(NULL) == CBM_PIPELINE_FROZEN_INVALID &&
                   !cbm_pipeline_frozen_candidate_path(NULL) &&
                   !cbm_pipeline_frozen_diagnostics(NULL) &&
                   cbm_pipeline_frozen_set_route_observer_for_tests(NULL, NULL, NULL) ==
                       CBM_PIPELINE_FROZEN_INVALID;
    bool built_first = invalid && pf_build_expected_ok(first) == CBM_PIPELINE_FROZEN_OK &&
                       cbm_get_user_lang_config() == ambient && pf_fixture_graph(f.db, false);
    bool state = built_first && cbm_pipeline_frozen_build(first) == CBM_PIPELINE_FROZEN_INVALID &&
                 pf_text(cbm_pipeline_frozen_candidate_path(first), f.db) &&
                 cbm_pipeline_frozen_set_publish_hooks_for_tests(first, NULL, NULL, NULL) ==
                     CBM_PIPELINE_FROZEN_INVALID;
    bool removed = state && cbm_unlink(f.db) == 0;
    cbm_pipeline_frozen_free(first);
    first = NULL;
    bool built_second = removed && cbm_pipeline_frozen_build(second) == CBM_PIPELINE_FROZEN_OK &&
                        cbm_get_user_lang_config() == ambient && pf_fixture_graph(f.db, true) &&
                        cbm_pipeline_get_mode(second_diag) == CBM_MODE_FULL;
    bool failed_publish = built_second && cbm_unlink(f.db) == 0 && pf_rename_failure(&f) &&
                          cbm_get_user_lang_config() == ambient;
    bool reset =
        failed_publish && pf_create(&f, "{}", NULL, &failed) == CBM_PIPELINE_FROZEN_OK && failed;
    if (reset)
        cbm_pipeline_frozen_cancel(failed);
    bool restored = reset && cbm_pipeline_frozen_build(failed) == CBM_PIPELINE_FROZEN_CANCELLED &&
                    cbm_get_user_lang_config() == ambient &&
                    !cbm_pipeline_frozen_candidate_path(failed) &&
                    pf_directory_only(f.destination, NULL) && pf_files_equal(f.control, f.saved);
    cbm_pipeline_frozen_free(first);
    cbm_pipeline_frozen_free(second);
    cbm_pipeline_frozen_free(failed);
    cbm_pipeline_frozen_cancel(NULL);
    cbm_pipeline_frozen_free(NULL);
    cbm_set_user_lang_config(f.previous_global);
    cbm_userconfig_free(ambient);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(source);
    ASSERT_TRUE(ambient_ok);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(separate);
    ASSERT_TRUE(controls);
    ASSERT_TRUE(invalid);
    ASSERT_TRUE(built_first);
    ASSERT_TRUE(state);
    ASSERT_TRUE(built_second);
    ASSERT_TRUE(failed_publish);
    ASSERT_TRUE(restored);
    PASS();
}

/* Independent recorder-integrity checks; append before the frozen suite. */
typedef struct {
    cbm_pipeline_frozen_t *owner;
    bool incomplete, followup, cancel, called, rename_called, building_getter;
    int count_during_hook;
    char stage[4096];
} pf_diag_hook_t;

static void pf_diag_before_publish(cbm_pipeline_t *pipeline, const char *stage, void *context) {
    pf_diag_hook_t *hook = context;
    hook->called = true;
    if (strlen(stage) < sizeof(hook->stage))
        memcpy(hook->stage, stage, strlen(stage) + 1);
    unsigned flags = 123;
    hook->building_getter =
        !cbm_pipeline_frozen_negative_evidence(hook->owner, &flags) && flags == 0;
    /* A blocking phase: a partial parse alone no longer refuses publication. */
    char path[] = "ordinary.c", reason[] = "observed fixture diagnostic", phase[] = "extract";
    cbm_pipeline_add_file_error(pipeline, path, hook->incomplete ? NULL : reason, phase);
    memset(path, 'x', sizeof(path) - 1);
    memset(reason, 'y', sizeof(reason) - 1);
    memset(phase, 'z', sizeof(phase) - 1);
    if (hook->followup)
        cbm_pipeline_add_file_error(pipeline, "followup.c", "retained later diagnostic", "extract");
    cbm_file_error_t *rows = NULL;
    cbm_pipeline_get_file_errors(pipeline, &rows, &hook->count_during_hook);
    if (hook->cancel)
        cbm_pipeline_frozen_cancel(hook->owner);
}

static int pf_diag_forbidden_rename(const char *stage, const char *destination, void *context) {
    (void)stage;
    (void)destination;
    pf_diag_hook_t *hook = context;
    hook->rename_called = true;
    errno = EACCES;
    return -1;
}

static bool pf_diag_ordinary_control(pf_fixture_t *f) {
    cbm_pipeline_t *p = cbm_pipeline_new(f->source, f->control, CBM_MODE_FULL);
    if (!p)
        return false;
    char path[] = "ordinary.c", reason[] = "legacy record", phase[] = "extract";
    cbm_pipeline_add_file_error(p, path, reason, phase);
    memset(path, 'x', sizeof(path) - 1);
    memset(reason, 'y', sizeof(reason) - 1);
    memset(phase, 'z', sizeof(phase) - 1);
    cbm_file_error_t *rows = NULL;
    int count = 0;
    cbm_pipeline_get_file_errors(p, &rows, &count);
    bool ok = count == 1 && rows && pf_text(rows[0].path, "ordinary.c") &&
              pf_text(rows[0].reason, "legacy record") && pf_text(rows[0].phase, "extract");
    cbm_pipeline_free(p);
    return ok;
}

static bool pf_diag_run(pf_fixture_t *f, cbm_pipeline_frozen_diag_fault_t fault, bool incomplete,
                        bool cancel) {
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    pf_diag_hook_t hook = {.owner = owner,
                           .incomplete = incomplete,
                           .followup = incomplete || fault != CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE,
                           .cancel = cancel};
    if (ready)
        ready = cbm_pipeline_frozen_set_diagnostic_fault_for_tests(owner, fault) ==
                    CBM_PIPELINE_FROZEN_OK &&
                cbm_pipeline_frozen_set_publish_hooks_for_tests(owner, pf_diag_before_publish,
                                                                pf_diag_forbidden_rename,
                                                                &hook) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t got =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    cbm_pipeline_frozen_status_t want = cancel ? CBM_PIPELINE_FROZEN_CANCELLED
                                        : fault != CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE
                                            ? CBM_PIPELINE_FROZEN_OOM
                                            : CBM_PIPELINE_FROZEN_PIPELINE_ERROR;
    unsigned flags = 0, expected = CBM_PIPELINE_FROZEN_FILE_DIAGNOSTIC;
    if (incomplete || fault != CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE)
        expected |= CBM_PIPELINE_FROZEN_DIAGNOSTIC_INCOMPLETE;
    if (fault != CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE)
        expected |= CBM_PIPELINE_FROZEN_DIAGNOSTIC_OOM;
    bool evidence = cbm_pipeline_frozen_negative_evidence(owner, &flags) && flags == expected;
    cbm_file_error_t *rows = NULL;
    int count = 0;
    cbm_pipeline_get_file_errors(cbm_pipeline_frozen_diagnostics(owner), &rows, &count);
    bool recorded = count == 1 && rows && hook.count_during_hook == 1 &&
                    pf_text(rows[0].path, hook.followup ? "followup.c" : "ordinary.c") &&
                    pf_text(rows[0].reason, hook.followup ? "retained later diagnostic"
                                                          : "observed fixture diagnostic") &&
                    pf_text(rows[0].phase, "extract");
    bool failed_state =
        !cbm_pipeline_frozen_candidate_path(owner) &&
        cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_INVALID &&
        cbm_pipeline_frozen_set_diagnostic_fault_for_tests(
            owner, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE) == CBM_PIPELINE_FROZEN_INVALID;
    unsigned again = 0;
    bool sticky = cbm_pipeline_frozen_negative_evidence(owner, &again) && again == expected;
    bool preserved = pf_absent(f->db) && pf_directory_only(f->destination, NULL) && hook.stage[0] &&
                     pf_absent(hook.stage) && pf_files_equal(f->control, f->saved);
    bool ok = ready && hook.called && got == want && !hook.rename_called && hook.building_getter &&
              evidence && recorded && failed_state && sticky && preserved;
    if (!ok)
        printf("  diagnostic fault=%d incomplete=%d cancel=%d status=%d expected=%d flags=%u "
               "rows=%d rename=%d\n",
               (int)fault, incomplete, cancel, (int)got, (int)want, flags, count,
               hook.rename_called);
    cbm_pipeline_frozen_free(owner);
    return ok;
}

static bool pf_diag_positive(pf_fixture_t *f) {
    cbm_pipeline_frozen_t *owner = NULL;
    bool ready = pf_create(f, "{}", NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    unsigned flags = 99;
    bool invalid = !cbm_pipeline_frozen_negative_evidence(NULL, &flags) && flags == 0 &&
                   !cbm_pipeline_frozen_negative_evidence(owner, NULL) &&
                   cbm_pipeline_frozen_set_diagnostic_fault_for_tests(
                       NULL, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE) == CBM_PIPELINE_FROZEN_INVALID;
    bool controls =
        ready &&
        cbm_pipeline_frozen_set_diagnostic_fault_for_tests(
            owner, (cbm_pipeline_frozen_diag_fault_t)-1) == CBM_PIPELINE_FROZEN_INVALID &&
        cbm_pipeline_frozen_set_diagnostic_fault_for_tests(
            owner, CBM_PIPELINE_FROZEN_DIAG_FAULT_PATH) == CBM_PIPELINE_FROZEN_OK &&
        cbm_pipeline_frozen_set_diagnostic_fault_for_tests(
            owner, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE) == CBM_PIPELINE_FROZEN_OK;
    bool built = ready && cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK &&
                 pf_ordinary_graph(f->db);
    flags = 99;
    bool zero = cbm_pipeline_frozen_negative_evidence(owner, &flags) && flags == 0;
    cbm_pipeline_frozen_free(owner);
    bool removed = built && cbm_unlink(f->db) == 0 && pf_directory_only(f->destination, NULL);
    return ready && invalid && controls && built && zero && removed;
}

TEST(pipeline_frozen_late_diagnostic_blocks_publication) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool legacy = control && pf_diag_ordinary_control(&f);
    bool positive = legacy && pf_feature_control(&f);
    bool rejected = positive && pf_diag_run(&f, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE, false, false);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(legacy);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(pipeline_frozen_incomplete_diagnostic_is_sticky) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool positive = control && pf_feature_control(&f);
    bool rejected = positive && pf_diag_run(&f, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE, true, false);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(pipeline_frozen_diagnostic_allocation_failures) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool positive = control && pf_feature_control(&f);
    bool rejected = positive;
    for (int fault = CBM_PIPELINE_FROZEN_DIAG_FAULT_GROW;
         positive && fault <= CBM_PIPELINE_FROZEN_DIAG_FAULT_PHASE; fault++)
        rejected =
            pf_diag_run(&f, (cbm_pipeline_frozen_diag_fault_t)fault, false, false) && rejected;
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(pipeline_frozen_diagnostic_state_and_cancel_precedence) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool positive = control && pf_feature_control(&f);
    bool cancelled = positive && pf_diag_run(&f, CBM_PIPELINE_FROZEN_DIAG_FAULT_NONE, false, true);
    bool isolated = cancelled && pf_diag_positive(&f);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(positive);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(cancelled);
    ASSERT_TRUE(isolated);
    PASS();
}

/* The recovered one-line macro retains a real parse-coverage diagnostic. A
 * partial parse is a parse gap (user decision 2026-10-04): the candidate is
 * published, the row is kept, and only the non-blocking PARSE_GAP bit is set,
 * so the selection can treat a change to this file as a parse gap. */
TEST(pipeline_frozen_real_parse_gap_is_recorded_not_refused) {
    ASSERT_EQ(pf_init_status, 0);
    static const char one_line[] =
        "void case_sink(void) {}\nvoid suite_sink(void) {}\n"
        "CHECK(before, after) { case_sink(); }\nGROUP(group) { suite_sink(); }\n";
    pf_fixture_t f = {0};
    cbm_pipeline_frozen_t *owner = NULL;
    bool setup = pf_open(&f), control = setup && pf_real_control(&f);
    bool source = control && pf_source_write(&f, "unit.pfsource", one_line);
    bool ready =
        source && pf_create(&f, pf_config0, NULL, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    cbm_pipeline_frozen_status_t status =
        ready ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    unsigned flags = 0;
    bool evidence = cbm_pipeline_frozen_negative_evidence(owner, &flags);
    cbm_file_error_t *rows = NULL;
    int count = 0;
    cbm_pipeline_get_file_errors(cbm_pipeline_frozen_diagnostics(owner), &rows, &count);
    bool recorded = evidence && flags == CBM_PIPELINE_FROZEN_PARSE_GAP && rows && count == 1 &&
                    pf_text(rows[0].path, "unit.pfsource") &&
                    pf_text(rows[0].phase, "parse_partial") && pf_text(rows[0].reason, "3-3");
    const char *candidate = cbm_pipeline_frozen_candidate_path(owner);
    bool published = status == CBM_PIPELINE_FROZEN_OK && candidate && pf_text(candidate, f.db) &&
                     !pf_absent(f.db) && pf_files_equal(f.control, f.saved);
    cbm_pipeline_frozen_free(owner);
    bool cleanup = pf_close(&f);
    ASSERT_TRUE(setup);
    ASSERT_TRUE(control);
    ASSERT_TRUE(source);
    ASSERT_TRUE(ready);
    ASSERT_TRUE(cleanup);
    ASSERT_TRUE(recorded);
    ASSERT_TRUE(published);
    PASS();
}

/* ── A classified file list instead of discovery ──────────────────── */

static bool pf_has_qn(const char *db, const char *qn) {
    cbm_store_t *store = cbm_store_open_path_query(db);
    if (!store)
        return false;
    cbm_node_t node = {0};
    bool found = cbm_store_find_node_by_qn(store, PF_PROJECT, qn, &node) == CBM_STORE_OK;
    cbm_node_free_fields(&node);
    cbm_store_close(store);
    return found;
}

static cbm_pipeline_frozen_status_t pf_create_listed(pf_fixture_t *f,
                                                     const cbm_pipeline_frozen_file_t *files,
                                                     size_t count, cbm_pipeline_frozen_t **out) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, NULL, &git, NULL);
    in.files = files;
    in.file_count = count;
    return cbm_pipeline_frozen_create(&in, out);
}

/* The build indexes exactly the listed files, under the listed languages: a
 * file on disk that is not listed is not indexed, and a listed language wins
 * over what the name alone would give. */
TEST(pipeline_frozen_indexes_exactly_the_listed_files) {
    pf_fixture_t f;
    cbm_pipeline_frozen_t *owner = NULL;
    ASSERT_TRUE(pf_open(&f));
    bool ok = pf_source_write(&f, "listed.c", pf_ordinary) &&
              pf_source_write(&f, "unlisted.c", "void stray(void) {}\n") &&
              pf_source_write(&f, "script.foo", "def py_fn():\n    return 1\n") &&
              pf_source_write(&f, "README.md", "# readme\n");
    static const cbm_pipeline_frozen_file_t files[] = {
        {"script.foo", CBM_LANG_PYTHON},
        {"listed.c", CBM_LANG_C},
        {"README.md", CBM_LANG_COUNT},
    };
    ok = ok && pf_create_listed(&f, files, 3, &owner) == CBM_PIPELINE_FROZEN_OK &&
         cbm_pipeline_frozen_build(owner) == CBM_PIPELINE_FROZEN_OK;
    bool listed = ok && pf_has_qn(f.db, PF_PROJECT ".listed.ordinary") &&
                  pf_has_qn(f.db, PF_PROJECT ".listed.sink");
    bool unlisted_absent = ok && !pf_has_qn(f.db, PF_PROJECT ".unlisted.stray");
    bool language_from_list = ok && pf_has_qn(f.db, PF_PROJECT ".script.py_fn");
    cbm_pipeline_frozen_free(owner);
    ASSERT_TRUE(pf_close(&f));
    ASSERT_TRUE(ok);
    ASSERT_TRUE(listed);
    ASSERT_TRUE(unlisted_absent);
    ASSERT_TRUE(language_from_list);
    PASS();
}

/* A listed file that is not in the source root means the root is not the
 * snapshot the list came from. */
TEST(pipeline_frozen_missing_listed_file_is_input_changed) {
    pf_fixture_t f;
    cbm_pipeline_frozen_t *owner = NULL;
    ASSERT_TRUE(pf_open(&f));
    bool ok = pf_source_write(&f, "listed.c", pf_ordinary);
    static const cbm_pipeline_frozen_file_t files[] = {{"listed.c", CBM_LANG_C},
                                                       {"gone.c", CBM_LANG_C}};
    ok = ok && pf_create_listed(&f, files, 2, &owner) == CBM_PIPELINE_FROZEN_OK;
    cbm_pipeline_frozen_status_t status =
        ok ? cbm_pipeline_frozen_build(owner) : CBM_PIPELINE_FROZEN_INVALID;
    bool no_candidate = ok && cbm_pipeline_frozen_candidate_path(owner) == NULL && pf_absent(f.db);
    cbm_pipeline_frozen_free(owner);
    ASSERT_TRUE(pf_close(&f));
    ASSERT_TRUE(ok);
    ASSERT_EQ(status, CBM_PIPELINE_FROZEN_INPUT_CHANGED);
    ASSERT_TRUE(no_candidate);
    PASS();
}

/* A list that cannot name files in the source root is refused at create. */
TEST(pipeline_frozen_rejects_malformed_file_lists) {
    pf_fixture_t f;
    ASSERT_TRUE(pf_open(&f));
    static const struct {
        cbm_pipeline_frozen_file_t file;
    } bad[] = {
        {{"../escape.c", CBM_LANG_C}},
        {{"/abs.c", CBM_LANG_C}},
        {{"", CBM_LANG_C}},
        {{"a//b.c", CBM_LANG_C}},
        {{"./a.c", CBM_LANG_C}},
        {{"a/../b.c", CBM_LANG_C}},
        {{"a\\b.c", CBM_LANG_C}},
        {{NULL, CBM_LANG_C}},
        {{"a.c", (CBMLanguage)-1}},
        {{"a.c", (CBMLanguage)(CBM_LANG_COUNT + 1)}},
    };
    bool all_invalid = true;
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        cbm_pipeline_frozen_t *owner = NULL;
        cbm_pipeline_frozen_status_t status = pf_create_listed(&f, &bad[i].file, 1, &owner);
        if (status != CBM_PIPELINE_FROZEN_INVALID || owner) {
            printf("  case %zu: status %d\n", i, (int)status);
            all_invalid = false;
        }
        cbm_pipeline_frozen_free(owner);
    }
    /* The same path twice. */
    static const cbm_pipeline_frozen_file_t twice[] = {{"a.c", CBM_LANG_C}, {"a.c", CBM_LANG_CPP}};
    cbm_pipeline_frozen_t *owner = NULL;
    bool twice_invalid =
        pf_create_listed(&f, twice, 2, &owner) == CBM_PIPELINE_FROZEN_INVALID && owner == NULL;
    cbm_pipeline_frozen_free(owner);
    /* A count without a list. */
    cbm_git_context_t git = pf_git(&f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(&f, NULL, &git, NULL);
    in.file_count = 3;
    owner = NULL;
    bool count_invalid =
        cbm_pipeline_frozen_create(&in, &owner) == CBM_PIPELINE_FROZEN_INVALID && owner == NULL;
    cbm_pipeline_frozen_free(owner);
    /* An empty list is a valid list: it indexes nothing. */
    static const cbm_pipeline_frozen_file_t one[] = {{"a.c", CBM_LANG_C}};
    in.files = one;
    in.file_count = 0;
    owner = NULL;
    bool empty_ok = cbm_pipeline_frozen_create(&in, &owner) == CBM_PIPELINE_FROZEN_OK && owner;
    cbm_pipeline_frozen_free(owner);
    ASSERT_TRUE(pf_close(&f));
    ASSERT_TRUE(all_invalid);
    ASSERT_TRUE(twice_invalid);
    ASSERT_TRUE(count_invalid);
    ASSERT_TRUE(empty_ok);
    PASS();
}

/* A base candidate (the team artifact's graph) is repaired incrementally when
 * the closure route can, rebuilt when it declines, and either way the result
 * is exactly the graph a build from scratch makes of the same tree. */
/* Two independent builds never share row ids or a database identity, so they
 * are compared by graph content. */
static bool pf_digest(const char *db, unsigned char out[32]) {
    cbm_store_t *store = cbm_store_open_path_query(db);
    if (!store)
        return false;
    bool ok = cbm_store_graph_content_digest(store, PF_PROJECT, out) == CBM_STORE_OK;
    cbm_store_close(store);
    return ok;
}

/* Builds the source tree into `keep` (renamed from the candidate); base may be
 * NULL. *route receives the route that built it. */
static bool pf_build_into(pf_fixture_t *f, const char *base, const char *keep,
                          cbm_pipeline_frozen_route_t *route) {
    cbm_git_context_t git = pf_git(f);
    cbm_pipeline_frozen_inputs_t in = pf_inputs(f, NULL, &git, NULL);
    in.base_db_path = base;
    cbm_pipeline_frozen_t *owner = NULL;
    bool ok = cbm_pipeline_frozen_create(&in, &owner) == CBM_PIPELINE_FROZEN_OK &&
              pf_build_expected_ok(owner) == CBM_PIPELINE_FROZEN_OK;
    *route = cbm_pipeline_frozen_route(owner);
    cbm_pipeline_frozen_free(owner);
    return ok && rename(f->db, keep) == 0;
}

TEST(pipeline_frozen_base_repairs_or_rebuilds_to_the_full_graph) {
    ASSERT_EQ(pf_init_status, 0);
    pf_fixture_t f = {0};
    ASSERT_TRUE(pf_open(&f));
    char base[1024], repaired[1024], full[1024], grown[1024], grown_full[1024];
    ASSERT_TRUE(pf_path(base, sizeof(base), f.home, "base.db"));
    ASSERT_TRUE(pf_path(repaired, sizeof(repaired), f.home, "repaired.db"));
    ASSERT_TRUE(pf_path(full, sizeof(full), f.home, "full.db"));
    ASSERT_TRUE(pf_path(grown, sizeof(grown), f.home, "grown.db"));
    ASSERT_TRUE(pf_path(grown_full, sizeof(grown_full), f.home, "grown_full.db"));
    ASSERT_TRUE(pf_source_write(&f, "helper.c", "int helper(int x) { return x + 1; }\n"));
    ASSERT_TRUE(
        pf_source_write(&f, "use.c", "int helper(int x);\nint use(void) { return helper(1); }\n"));
    cbm_pipeline_frozen_route_t route = CBM_PIPELINE_FROZEN_ROUTE_INCREMENTAL;
    ASSERT_TRUE(pf_build_into(&f, NULL, base, &route));
    ASSERT_EQ(route, CBM_PIPELINE_FROZEN_ROUTE_FULL);

    /* A body edit: the closure route repairs the base. */
    ASSERT_TRUE(pf_source_write(&f, "helper.c", "int helper(int x) { return x + 2; }\n"));
    ASSERT_TRUE(pf_build_into(&f, base, repaired, &route));
    ASSERT_EQ(route, CBM_PIPELINE_FROZEN_ROUTE_INCREMENTAL);
    ASSERT_TRUE(pf_build_into(&f, NULL, full, &route));
    ASSERT_EQ(route, CBM_PIPELINE_FROZEN_ROUTE_FULL);
    unsigned char a[32], b[32], c[32];
    ASSERT_TRUE(pf_digest(repaired, a));
    ASSERT_TRUE(pf_digest(full, b));
    ASSERT_MEM_EQ(a, b, 32);
    ASSERT_TRUE(pf_digest(base, c)); /* the digest sees the edit at all */
    ASSERT_TRUE(memcmp(a, c, 32) != 0);

    /* A new definition name: yesterday's graph cannot know its referencers, so
     * the route declines and the build is the full one. */
    ASSERT_TRUE(pf_source_write(&f, "helper.c",
                                "int helper(int x) { return x + 2; }\nint extra(void) { "
                                "return 3; }\n"));
    ASSERT_TRUE(pf_build_into(&f, base, grown, &route));
    ASSERT_EQ(route, CBM_PIPELINE_FROZEN_ROUTE_FULL);
    ASSERT_TRUE(pf_build_into(&f, NULL, grown_full, &route));
    ASSERT_TRUE(pf_digest(grown, a));
    ASSERT_TRUE(pf_digest(grown_full, b));
    ASSERT_MEM_EQ(a, b, 32);

    /* The base is read, never written. */
    char again[1024];
    ASSERT_TRUE(pf_path(again, sizeof(again), f.home, "again.db"));
    unsigned char before[32], after[32];
    ASSERT_TRUE(pf_digest(base, before));
    ASSERT_TRUE(pf_build_into(&f, base, again, &route));
    ASSERT_TRUE(pf_digest(base, after));
    ASSERT_MEM_EQ(before, after, 32);
    ASSERT_TRUE(pf_close(&f));
    PASS();
}

SUITE(pipeline_frozen) {
    RUN_TEST(pipeline_frozen_real_parse_gap_is_recorded_not_refused);
    pf_init_status = cbm_init();
    RUN_TEST(pipeline_frozen_config_snapshot_contract);
    RUN_TEST(pipeline_frozen_detached_graph_and_owned_context);
    RUN_TEST(pipeline_frozen_final_reconciliation_and_explicit_override);
    RUN_TEST(pipeline_frozen_preserves_artifacts_and_destination_namespace);
    RUN_TEST(pipeline_frozen_cancel_rename_and_busy_retry);
    RUN_TEST(pipeline_frozen_actual_serial_dispatch_and_history_skip);
    RUN_TEST(pipeline_frozen_owner_isolation_state_and_global_restoration);
    RUN_TEST(pipeline_frozen_late_diagnostic_blocks_publication);
    RUN_TEST(pipeline_frozen_incomplete_diagnostic_is_sticky);
    RUN_TEST(pipeline_frozen_diagnostic_allocation_failures);
    RUN_TEST(pipeline_frozen_diagnostic_state_and_cancel_precedence);
    RUN_TEST(pipeline_frozen_indexes_exactly_the_listed_files);
    RUN_TEST(pipeline_frozen_missing_listed_file_is_input_changed);
    RUN_TEST(pipeline_frozen_rejects_malformed_file_lists);
    RUN_TEST(pipeline_frozen_base_repairs_or_rebuilds_to_the_full_graph);
    cbm_work_arena_release();
}
