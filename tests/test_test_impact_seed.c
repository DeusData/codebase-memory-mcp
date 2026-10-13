/*
 * test_test_impact_seed.c — what a change seeds (src/mcp/test_impact_seed.c),
 * one test per rule of the measured reference (replay4.py seeding and header
 * symbol propagation), on a small in-memory graph with its sources.
 */
#include "test_framework.h"

#include "mcp/test_impact.h"
#include "mcp/test_impact_seed.h"
#include "mcp/test_impact_source.h"
#include "store/store.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    cbm_store_t *store;
    cbm_ti_graph_t *graph;
    cbm_ti_source_t *source;
    cbm_test_model_t *model;
} tsd_fixture_t;

static int64_t tsd_node(tsd_fixture_t *f, const char *label, const char *name, const char *file,
                        int start, int end) {
    char qn[256];
    snprintf(qn, sizeof(qn), "p.%s.%s.%d", file, name, start);
    cbm_node_t node = {.project = "p",
                       .label = label,
                       .name = name,
                       .qualified_name = qn,
                       .file_path = file,
                       .start_line = start,
                       .end_line = end};
    return cbm_store_upsert_node(f->store, &node);
}

/* Sources: pairs of (path, text). Model: the test sources among them. */
static bool tsd_open(tsd_fixture_t *f, const char *const *files, int count) {
    memset(f, 0, sizeof(*f));
    f->store = cbm_store_open_memory();
    if (!f->store || cbm_store_upsert_project(f->store, "p", "/tmp/p") != CBM_STORE_OK) {
        return false;
    }
    f->graph = cbm_ti_graph_new(f->store, "p");
    f->source = cbm_ti_source_new();
    f->model = cbm_test_model_new(cbm_test_conventions_cbm());
    if (!f->graph || !f->source || !f->model) {
        return false;
    }
    for (int i = 0; i < count; i += 2) {
        size_t len = strlen(files[i + 1]);
        if (!cbm_ti_source_add(f->source, files[i], files[i + 1], len)) {
            return false;
        }
        if (strncmp(files[i], "tests/", 6) == 0 &&
            !cbm_test_model_add_source(f->model, files[i], files[i + 1], len)) {
            return false;
        }
    }
    return cbm_ti_source_finish(f->source) && cbm_test_model_finish(f->model);
}

static void tsd_close(tsd_fixture_t *f) {
    cbm_test_model_free(f->model);
    cbm_ti_source_free(f->source);
    cbm_ti_graph_free(f->graph);
    if (f->store) {
        cbm_store_close(f->store);
    }
}

static cbm_ti_seeds_t *tsd_seed(tsd_fixture_t *f, const cbm_ti_change_t *changes, int count) {
    cbm_ti_seed_input_t in = {.changes = changes,
                              .change_count = count,
                              .model = f->model,
                              .graph = f->graph,
                              .source = f->source};
    cbm_ti_seeds_t *out = NULL;
    return cbm_ti_seed(&in, &out) == CBM_TI_SEED_OK ? out : NULL;
}

static bool tsd_seeded(const cbm_ti_seeds_t *s, int64_t id) {
    int count = 0;
    const int64_t *ids = cbm_ti_seeds_nodes(s, &count);
    for (int i = 0; i < count; i++) {
        if (ids[i] == id) {
            return true;
        }
    }
    return false;
}

static int tsd_seed_count(const cbm_ti_seeds_t *s) {
    int count = 0;
    (void)cbm_ti_seeds_nodes(s, &count);
    return count;
}

/* A hunk at head seeds the definitions it overlaps; a comment-only hunk seeds
 * nothing; a pure deletion seeds the definition around it; a hunk outside
 * every definition seeds its whole file. */
TEST(test_impact_seed_hunks_follow_the_reference) {
    const char *files[] = {"src/a.c", "int g;\nint f(void) {\n  return g;\n}\n"
                                      "int h(void) {\n  return 1;\n}\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 2));
    int64_t g = tsd_node(&f, "Variable", "g", "src/a.c", 1, 1);
    int64_t fn = tsd_node(&f, "Function", "f", "src/a.c", 2, 4);
    int64_t h = tsd_node(&f, "Function", "h", "src/a.c", 5, 7);
    (void)tsd_node(&f, "Module", "a", "src/a.c", 1, 7);

    static const char *const body[] = {"  return g + 1;"};
    static const char *const old_body[] = {"  return g;"};
    cbm_diff_hunk_t edit = {.start = 3,
                            .count = 1,
                            .added = body,
                            .added_count = 1,
                            .removed = old_body,
                            .removed_count = 1};
    cbm_ti_change_t change = {.path = "src/a.c", .hunks = &edit, .hunk_count = 1};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, fn));
    cbm_ti_seeds_free(s);

    static const char *const comment[] = {"  // why"};
    cbm_diff_hunk_t note = {.start = 6, .count = 1, .added = comment, .added_count = 1};
    change.hunks = &note;
    s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 0);
    cbm_ti_seeds_free(s);

    static const char *const removed[] = {"  h_extra();"};
    cbm_diff_hunk_t cut = {.start = 5, .count = 0, .removed = removed, .removed_count = 1};
    change.hunks = &cut;
    s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, h));
    cbm_ti_seeds_free(s);

    static const char *const include[] = {"#include \"b.h\""};
    cbm_diff_hunk_t top = {.start = 8, .count = 1, .added = include, .added_count = 1};
    change.hunks = &top;
    s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 3); /* g, f, h: the Module is no definition */
    ASSERT_TRUE(tsd_seeded(s, g) && tsd_seeded(s, fn) && tsd_seeded(s, h));
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

/* A line that starts with a dereference is code, and so is code with a
 * trailing comment: each such change seeds its definition. The comment
 * filter read `*out = f(...)` as a block-comment line, so a PR's two real
 * edits in scan_route_annotations seeded nothing, and six tests that execute
 * it were not selected (oracle job pr2158). A comment-only edit still
 * seeds nothing. */
TEST(test_impact_seed_dereference_and_trailing_comment_lines_are_code) {
    const char *files[] = {"src/a.c", "void f(int *out) {\n  *out = g(1);\n}\n"
                                      "int h(void) {\n  return 1; /* one */\n}\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 2));
    int64_t fn = tsd_node(&f, "Function", "f", "src/a.c", 1, 3);
    int64_t h = tsd_node(&f, "Function", "h", "src/a.c", 4, 6);

    static const char *const deref[] = {"  *out = g(1, true);"};
    static const char *const old_deref[] = {"  *out = g(1);"};
    cbm_diff_hunk_t edit = {.start = 2,
                            .count = 1,
                            .added = deref,
                            .added_count = 1,
                            .removed = old_deref,
                            .removed_count = 1};
    cbm_ti_change_t change = {.path = "src/a.c", .hunks = &edit, .hunk_count = 1};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, fn));
    cbm_ti_seeds_free(s);

    static const char *const value[] = {"  return 2; /* one */"};
    static const char *const old_value[] = {"  return 1; /* one */"};
    cbm_diff_hunk_t trailing = {.start = 5,
                                .count = 1,
                                .added = value,
                                .added_count = 1,
                                .removed = old_value,
                                .removed_count = 1};
    change.hunks = &trailing;
    s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, h));
    cbm_ti_seeds_free(s);

    static const char *const note[] = {"   * why the dereference stays"};
    cbm_diff_hunk_t comment = {.start = 2, .count = 1, .added = note, .added_count = 1};
    change.hunks = &comment;
    s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 0);
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

/* A changed test case or suite body is reported, never seeded. */
TEST(test_impact_seed_changed_cases_and_suites_are_reported) {
    const char *files[] = {"tests/t.c", "TEST(alpha) {\n  PASS();\n}\n"
                                        "SUITE(s) {\n  RUN_TEST(alpha);\n}\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 2));
    (void)tsd_node(&f, "Function", "alpha", "tests/t.c", 1, 3);
    (void)tsd_node(&f, "Function", "s", "tests/t.c", 4, 6);
    static const char *const line[] = {"  ASSERT_TRUE(1);"};
    cbm_diff_hunk_t hunks[] = {{.start = 2, .count = 1, .added = line, .added_count = 1},
                               {.start = 5, .count = 1, .added = line, .added_count = 1}};
    cbm_ti_change_t change = {.path = "tests/t.c", .hunks = hunks, .hunk_count = 2};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 0);
    int count = 0;
    const int *cases = cbm_ti_seeds_changed_cases(s, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(cases[0], 0);
    const int *suites = cbm_ti_seeds_changed_suites(s, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(suites[0], 0);
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

/* A name a removed line declared that head no longer defines: every
 * definition mentioning it is a seed, a mention at file scope of a source
 * file seeds that file whole, and one in a header outside a definition adds
 * nothing. A deleted file contributes every name it declared. */
TEST(test_impact_seed_deleted_names_are_found_by_text) {
    const char *files[] = {
        "src/a.c",   "int user(void) {\n  return gone();\n}\n",
        "src/b.c",   "static void *table[] = {gone};\nint b(void) { return 0; }\n",
        "inc/x.h",   "int gone(void);\n",
        "src/old.c", "int unrelated(void) { return 1; }\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 8));
    int64_t user = tsd_node(&f, "Function", "user", "src/a.c", 1, 3);
    int64_t b = tsd_node(&f, "Function", "b", "src/b.c", 2, 2);
    (void)tsd_node(&f, "Function", "unrelated", "src/old.c", 1, 1);
    static const char *const removed[] = {"int gone(void) {", "  return 0;", "}"};
    cbm_diff_hunk_t cut = {.start = 1, .count = 0, .removed = removed, .removed_count = 3};
    cbm_ti_change_t change = {.path = "src/old.c", .hunks = &cut, .hunk_count = 1};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    int count = 0;
    const char *const *names = cbm_ti_seeds_deleted_names(s, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(names[0], "gone");
    ASSERT_TRUE(tsd_seeded(s, user)); /* mentions gone inside a definition */
    ASSERT_TRUE(tsd_seeded(s, b));    /* b.c mentions it at file scope: whole file */
    cbm_ti_seeds_free(s);

    cbm_ti_change_t deleted = {.path = "src/gone.c",
                               .deleted = true,
                               .base_text = "int gone(void) {\n  return 0;\n}\n",
                               .base_len = strlen("int gone(void) {\n  return 0;\n}\n")};
    s = tsd_seed(&f, &deleted, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_TRUE(tsd_seeded(s, user));
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

/* A header change names what it changes, closes the names over the headers'
 * macros, and seeds what mentions them in files that include the header. */
TEST(test_impact_seed_header_names_propagate_through_includes_and_macros) {
    const char *files[] = {"inc/h.h",   "#define LIMIT 4\n#define TWICE (LIMIT * 2)\n",
                           "src/a.c",   "#include \"../inc/h.h\"\nint a(void) { return LIMIT; }\n",
                           "src/c.c",   "#include \"inc/h.h\"\nint c(void) { return TWICE; }\n",
                           "src/far.c", "int far(void) { return LIMIT; }\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 8));
    int64_t limit = tsd_node(&f, "Macro", "LIMIT", "inc/h.h", 1, 1);
    int64_t twice = tsd_node(&f, "Macro", "TWICE", "inc/h.h", 2, 2);
    int64_t a = tsd_node(&f, "Function", "a", "src/a.c", 2, 2);
    int64_t c = tsd_node(&f, "Function", "c", "src/c.c", 2, 2);
    int64_t far = tsd_node(&f, "Function", "far", "src/far.c", 1, 1);
    static const char *const added[] = {"#define LIMIT 8"};
    static const char *const removed[] = {"#define LIMIT 4"};
    cbm_diff_hunk_t edit = {.start = 1,
                            .count = 1,
                            .added = added,
                            .added_count = 1,
                            .removed = removed,
                            .removed_count = 1};
    cbm_ti_change_t change = {.path = "inc/h.h", .hunks = &edit, .hunk_count = 1};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_TRUE(tsd_seeded(s, limit));
    ASSERT_TRUE(tsd_seeded(s, twice)); /* the macro whose body names LIMIT */
    ASSERT_TRUE(tsd_seeded(s, a));     /* includes h.h, mentions LIMIT */
    ASSERT_TRUE(tsd_seeded(s, c));     /* includes h.h, mentions TWICE */
    ASSERT_FALSE(tsd_seeded(s, far));  /* mentions LIMIT, never includes h.h */
    int count = 0;
    (void)cbm_ti_seeds_file_level_headers(s, &count);
    ASSERT_EQ(count, 0);
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

/* A preprocessor conditional, or a line the patterns cannot classify outside
 * any definition, makes the header file level: no seeds, an escalation. */
TEST(test_impact_seed_unclassified_header_lines_escalate_the_file) {
    const char *files[] = {"inc/h.h", "#define LIMIT 4\n\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 2));
    (void)tsd_node(&f, "Macro", "LIMIT", "inc/h.h", 1, 1);
    static const char *const conditional[] = {"#ifdef FEATURE"};
    static const char *const odd[] = {"  LIMIT_PLUS_ONE,"};
    cbm_diff_hunk_t cases[] = {
        {.start = 2, .count = 1, .added = conditional, .added_count = 1},
        {.start = 2, .count = 1, .added = odd, .added_count = 1},
    };
    for (int i = 0; i < 2; i++) {
        cbm_ti_change_t change = {.path = "inc/h.h", .hunks = &cases[i], .hunk_count = 1};
        cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
        ASSERT_NOT_NULL(s);
        int count = 0;
        const char *const *headers = cbm_ti_seeds_file_level_headers(s, &count);
        ASSERT_EQ(count, 1);
        ASSERT_STR_EQ(headers[0], "inc/h.h");
        ASSERT_EQ(tsd_seed_count(s), 0);
        cbm_ti_seeds_free(s);
    }
    tsd_close(&f);
    PASS();
}

/* One definition with variants (graph_buffer.c "Definition variants") is one
 * node holding one variant's span; the others are in its `variants`
 * property, in the same file (#if branches) or in another file that DEFINES
 * it too (platform files). A hunk in ANY variant seeds that node. */
TEST(test_impact_seed_variant_spans_seed_their_definition) {
    static const char pick_src[] = "#ifdef _WIN32\nvoid pick(void) {\n  beta();\n}\n#else\n"
                                   "void pick(void) {\n  alpha();\n}\n#endif\n";
    const char *files[] = {"src/pick.c",       pick_src,
                           "src/plat_linux.c", "void plat(void) {\n  alpha();\n}\n",
                           "src/plat_win.c",   "void plat(void) {\n  beta();\n}\n"};
    tsd_fixture_t f;
    ASSERT_TRUE(tsd_open(&f, files, 6));
    cbm_node_t pick = {.project = "p",
                       .label = "Function",
                       .name = "pick",
                       .qualified_name = "p.src.pick.pick",
                       .file_path = "src/pick.c",
                       .start_line = 2,
                       .end_line = 4,
                       .properties_json = "{\"variants\":["
                                          "{\"file_path\":\"src/pick.c\",\"start_line\":2,"
                                          "\"end_line\":4},"
                                          "{\"file_path\":\"src/pick.c\",\"start_line\":6,"
                                          "\"end_line\":8}]}"};
    int64_t pick_id = cbm_store_upsert_node(f.store, &pick);
    cbm_node_t plat = {.project = "p",
                       .label = "Function",
                       .name = "plat",
                       .qualified_name = "p.src.plat.plat",
                       .file_path = "src/plat_linux.c",
                       .start_line = 1,
                       .end_line = 3,
                       .properties_json = "{\"variants\":["
                                          "{\"file_path\":\"src/plat_linux.c\",\"start_line\":1,"
                                          "\"end_line\":3},"
                                          "{\"file_path\":\"src/plat_win.c\",\"start_line\":1,"
                                          "\"end_line\":3}]}"};
    int64_t plat_id = cbm_store_upsert_node(f.store, &plat);
    cbm_node_t win_file = {.project = "p",
                           .label = "File",
                           .name = "plat_win.c",
                           .qualified_name = "p.src.plat_win.c.__file__",
                           .file_path = "src/plat_win.c"};
    int64_t win_file_id = cbm_store_upsert_node(f.store, &win_file);
    cbm_edge_t defines = {
        .project = "p", .source_id = win_file_id, .target_id = plat_id, .type = "DEFINES"};
    ASSERT_TRUE(pick_id > 0 && plat_id > 0 && win_file_id > 0);
    ASSERT_TRUE(cbm_store_insert_edge(f.store, &defines) > 0);

    /* The #else branch (lines 6-8). */
    static const char *const added[] = {"  alpha2();"};
    static const char *const removed[] = {"  alpha();"};
    cbm_diff_hunk_t branch = {.start = 7,
                              .count = 1,
                              .added = added,
                              .added_count = 1,
                              .removed = removed,
                              .removed_count = 1};
    cbm_ti_change_t change = {.path = "src/pick.c", .hunks = &branch, .hunk_count = 1};
    cbm_ti_seeds_t *s = tsd_seed(&f, &change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, pick_id));
    cbm_ti_seeds_free(s);

    /* The Windows platform file, whose variant lives on plat_linux.c's node. */
    static const char *const win_added[] = {"  beta2();"};
    static const char *const win_removed[] = {"  beta();"};
    cbm_diff_hunk_t win = {.start = 2,
                           .count = 1,
                           .added = win_added,
                           .added_count = 1,
                           .removed = win_removed,
                           .removed_count = 1};
    cbm_ti_change_t win_change = {.path = "src/plat_win.c", .hunks = &win, .hunk_count = 1};
    s = tsd_seed(&f, &win_change, 1);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(tsd_seed_count(s), 1);
    ASSERT_TRUE(tsd_seeded(s, plat_id));
    cbm_ti_seeds_free(s);
    tsd_close(&f);
    PASS();
}

SUITE(test_impact_seed) {
    RUN_TEST(test_impact_seed_hunks_follow_the_reference);
    RUN_TEST(test_impact_seed_dereference_and_trailing_comment_lines_are_code);
    RUN_TEST(test_impact_seed_variant_spans_seed_their_definition);
    RUN_TEST(test_impact_seed_changed_cases_and_suites_are_reported);
    RUN_TEST(test_impact_seed_deleted_names_are_found_by_text);
    RUN_TEST(test_impact_seed_header_names_propagate_through_includes_and_macros);
    RUN_TEST(test_impact_seed_unclassified_header_lines_escalate_the_file);
}
