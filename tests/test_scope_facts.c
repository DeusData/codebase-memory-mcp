/*
 * test_scope_facts.c -- the per-file scope facts of the shared cross-file layer
 * (cbm.h CBMScopeEvent, internal/cbm/extract_scope.c) and their stored form
 * (the "is" key of the LSP surface, src/pipeline/lsp_surface.c).
 *
 * Protobuf is the one language with a scope row: definitions with their full
 * names, includes with `import public` as a re-export, qualified type
 * references, parse errors as opaque. Every other language records nothing,
 * and its stored surface is unchanged.
 */
#include "test_framework.h"
#include "../src/pipeline/lsp_surface.h"
#include "../src/store/store.h"
#include "../src/foundation/arena.h"
#include "cbm.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char SF_PROTO[] = "syntax = \"proto3\";\n"
                               "package a.b;\n"
                               "\n"
                               "import \"x/common.proto\";\n"
                               "import public \"x/shared.proto\";\n"
                               "import /* public */ \"x/plain.proto\";\n"
                               "\n"
                               "message Outer {\n"
                               "  message Inner { string s = 1; }\n"
                               "  Inner inner = 1;\n"
                               "  common.Status status = 2;\n"
                               "  .a.b.Outer.Inner abs = 3;\n"
                               "}\n"
                               "\n"
                               "enum Kind { KIND_UNSPECIFIED = 0; }\n";

static CBMFileResult *sf_extract(const char *source, CBMLanguage lang, const char *rel) {
    return cbm_extract_file(source, (int)strlen(source), lang, "p", rel, 0, NULL, NULL);
}

/* The event of `kind` named `name` (NULL: the n-th of that kind), or NULL. */
static const CBMScopeEvent *sf_find(const CBMFileResult *r, uint8_t kind, const char *name,
                                    int nth) {
    for (int i = 0; i < r->scope.count; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        if (e->kind != kind) {
            continue;
        }
        if (name ? (e->name && strcmp(e->name, name) == 0) : nth-- == 0) {
            return e;
        }
    }
    return NULL;
}

static int sf_count(const CBMFileResult *r, uint8_t kind) {
    int n = 0;
    for (int i = 0; i < r->scope.count; i++) {
        n += r->scope.items[i].kind == kind;
    }
    return n;
}

static const char *sf_module(const CBMFileResult *r, const CBMScopeEvent *e) {
    return e && e->index >= 0 && e->index < r->imports.count
               ? r->imports.items[e->index].module_path
               : NULL;
}

TEST(scope_facts_proto_definitions_includes_references) {
    CBMFileResult *r = sf_extract(SF_PROTO, CBM_LANG_PROTOBUF, "proto/a.proto");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(cbm_extract_scope_has_row(CBM_LANG_PROTOBUF));

    /* includes: one per import statement, the importing package as qual,
     * `import public` re-exports, a comment holding the word does not */
    ASSERT_EQ(sf_count(r, CBM_SCOPE_INCLUDE), 3);
    const CBMScopeEvent *i0 = sf_find(r, CBM_SCOPE_INCLUDE, NULL, 0);
    const CBMScopeEvent *i1 = sf_find(r, CBM_SCOPE_INCLUDE, NULL, 1);
    const CBMScopeEvent *i2 = sf_find(r, CBM_SCOPE_INCLUDE, NULL, 2);
    ASSERT_STR_EQ(sf_module(r, i0), "x/common.proto");
    ASSERT_STR_EQ(sf_module(r, i1), "x/shared.proto");
    ASSERT_STR_EQ(sf_module(r, i2), "x/plain.proto");
    ASSERT_EQ(i0->flags & CBM_SCOPE_REEXPORT, 0);
    ASSERT_EQ(i1->flags & CBM_SCOPE_REEXPORT, CBM_SCOPE_REEXPORT);
    ASSERT_EQ(i2->flags & CBM_SCOPE_REEXPORT, 0);
    ASSERT_STR_EQ(i0->qual, "a.b");
    ASSERT_NULL(i0->name);

    /* definitions: the extractor's own definition, with its full name */
    const CBMScopeEvent *outer = sf_find(r, CBM_SCOPE_DEF, "Outer", 0);
    const CBMScopeEvent *inner = sf_find(r, CBM_SCOPE_DEF, "Inner", 0);
    const CBMScopeEvent *kind = sf_find(r, CBM_SCOPE_DEF, "Kind", 0);
    ASSERT_NOT_NULL(outer);
    ASSERT_NOT_NULL(inner);
    ASSERT_NOT_NULL(kind);
    ASSERT_STR_EQ(outer->qual, "a.b.Outer");
    ASSERT_STR_EQ(inner->qual, "a.b.Outer.Inner");
    ASSERT_STR_EQ(kind->qual, "a.b.Kind");
    ASSERT_TRUE(outer->index >= 0 && outer->index < r->defs.count);
    ASSERT_STR_EQ(r->defs.items[outer->index].name, "Outer");
    ASSERT_STR_EQ(r->defs.items[inner->index].name, "Inner");
    ASSERT_EQ(sf_count(r, CBM_SCOPE_ASSIGN), 0);

    /* references as written, qualifier included; index is their start byte */
    const CBMScopeEvent *status = sf_find(r, CBM_SCOPE_QREF, "common.Status", 0);
    const CBMScopeEvent *abs = sf_find(r, CBM_SCOPE_QREF, ".a.b.Outer.Inner", 0);
    ASSERT_NOT_NULL(status);
    ASSERT_NOT_NULL(abs);
    ASSERT_NOT_NULL(sf_find(r, CBM_SCOPE_QREF, "Inner", 0));
    ASSERT_EQ(status->index, (int32_t)(strstr(SF_PROTO, "common.Status") - SF_PROTO));
    ASSERT_EQ(sf_count(r, CBM_SCOPE_OPAQUE), 0);

    /* source order: an inner definition before its outer one */
    for (int i = 1; i < r->scope.count; i++) {
        ASSERT_TRUE(r->scope.items[i - 1].pos <= r->scope.items[i].pos);
    }
    ASSERT_TRUE(inner->pos < outer->pos);
    ASSERT_TRUE(outer->pos < kind->pos);
    cbm_free_result(r);
    PASS();
}

TEST(scope_facts_language_without_row_records_nothing) {
    ASSERT_FALSE(cbm_extract_scope_has_row(CBM_LANG_GO));
    CBMFileResult *r =
        sf_extract("package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println() }\n", CBM_LANG_GO,
                   "main.go");
    ASSERT_NOT_NULL(r);
    ASSERT_GT(r->imports.count, 0);
    ASSERT_EQ(r->scope.count, 0);
    cbm_free_result(r);
    PASS();
}

TEST(scope_facts_parse_error_is_opaque) {
    static const char src[] = "syntax = \"proto3\";\n"
                              "package p;\n"
                              "message A { string s = 1; !!! }\n"
                              "message B { string t = 1; }\n";
    CBMFileResult *r = sf_extract(src, CBM_LANG_PROTOBUF, "e.proto");
    ASSERT_NOT_NULL(r);
    const CBMScopeEvent *opaque = sf_find(r, CBM_SCOPE_OPAQUE, NULL, 0);
    ASSERT_NOT_NULL(opaque);
    ASSERT_EQ(opaque->flags & CBM_SCOPE_CONDITIONAL, CBM_SCOPE_CONDITIONAL);
    ASSERT_NOT_NULL(sf_find(r, CBM_SCOPE_DEF, "B", 0));
    cbm_free_result(r);
    PASS();
}

TEST(scope_facts_survive_compaction) {
    CBMFileResult *r = sf_extract(SF_PROTO, CBM_LANG_PROTOBUF, "proto/a.proto");
    ASSERT_NOT_NULL(r);
    int n = r->scope.count;
    ASSERT_GT(n, 0);
    char before[4096];
    size_t off = 0;
    for (int i = 0; i < n; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        off += (size_t)snprintf(before + off, sizeof(before) - off, "%u|%d|%u|%u|%s|%s\n", e->kind,
                                e->index, e->pos, e->flags, e->name ? e->name : "-",
                                e->qual ? e->qual : "-");
    }
    cbm_result_compact(r);
    ASSERT_EQ(r->scope.count, n);
    char after[4096];
    off = 0;
    for (int i = 0; i < n; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        off += (size_t)snprintf(after + off, sizeof(after) - off, "%u|%d|%u|%u|%s|%s\n", e->kind,
                                e->index, e->pos, e->flags, e->name ? e->name : "-",
                                e->qual ? e->qual : "-");
    }
    ASSERT_STR_EQ(after, before);
    cbm_free_result(r);
    PASS();
}

/* Compaction leaves every array at its exact count in one exact-size block,
 * so an event appended afterwards must grow the array into new memory. With a
 * capacity left at its extraction value, the append would write in place,
 * over whatever the block holds next. */
TEST(scope_facts_append_after_compaction_grows_the_array) {
    CBMFileResult *r = sf_extract(SF_PROTO, CBM_LANG_PROTOBUF, "proto/a.proto");
    ASSERT_NOT_NULL(r);
    cbm_result_compact(r);
    int n = r->scope.count;
    ASSERT_GT(n, 0);
    ASSERT_EQ(r->scope.cap, n); /* exactly sized: no headroom to append into */
    char before[4096];
    size_t off = 0;
    for (int i = 0; i < n; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        off += (size_t)snprintf(before + off, sizeof(before) - off, "%u|%d|%u|%s|%s\n", e->kind,
                                e->index, e->pos, e->name ? e->name : "-", e->qual ? e->qual : "-");
    }
    const CBMScopeEvent *old = r->scope.items;
    CBMScopeEvent ev = {.pos = UINT32_MAX, .index = -1, .kind = CBM_SCOPE_OPAQUE};
    cbm_scope_events_push(&r->scope, &r->arena, ev);
    ASSERT_EQ(r->scope.count, n + 1);
    ASSERT_TRUE(r->scope.items != old); /* grown, not written in place */
    char after[4096];
    off = 0;
    for (int i = 0; i < n; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        off += (size_t)snprintf(after + off, sizeof(after) - off, "%u|%d|%u|%s|%s\n", e->kind,
                                e->index, e->pos, e->name ? e->name : "-", e->qual ? e->qual : "-");
    }
    ASSERT_STR_EQ(after, before);
    ASSERT_EQ(r->scope.items[n].kind, CBM_SCOPE_OPAQUE);
    cbm_free_result(r);
    PASS();
}

/* One file's stored surface row (cbm_lsp_surface_build_rows, no LSP defs). */
static bool sf_row(CBMFileResult *r, const char *rel, CBMLanguage lang, char **json, char **sha) {
    cbm_file_info_t file = {.rel_path = (char *)rel, .language = lang};
    cbm_lsp_surface_row_t *rows = NULL;
    int count = 0;
    int rc = cbm_lsp_surface_build_rows(NULL, "p", &r, &file, 1, NULL, NULL, &rows, &count);
    bool ok = rc == 0 && count == 1 && rows[0].defs_json && rows[0].surface_sha;
    if (ok) {
        *json = strdup(rows[0].defs_json);
        *sha = strdup(rows[0].surface_sha);
    }
    cbm_store_free_lsp_surfaces(rows, count);
    return ok;
}

TEST(scope_facts_surface_round_trip) {
    CBMFileResult *r = sf_extract(SF_PROTO, CBM_LANG_PROTOBUF, "proto/a.proto");
    ASSERT_NOT_NULL(r);
    char *json = NULL;
    char *sha = NULL;
    ASSERT_TRUE(sf_row(r, "proto/a.proto", CBM_LANG_PROTOBUF, &json, &sha));
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_lsp_scope_facts_t facts;
    ASSERT_EQ(cbm_lsp_surface_scope_from_json(&arena, json, &facts), 1);

    /* every stored event comes back: all but the references, in order */
    int stored = 0;
    int rank = -1;
    uint32_t last = 0;
    for (int i = 0; i < r->scope.count; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        if (e->kind == CBM_SCOPE_QREF) {
            continue;
        }
        if (rank < 0 || e->pos != last) {
            rank++;
            last = e->pos;
        }
        ASSERT_TRUE(stored < facts.event_count);
        const CBMScopeEvent *g = &facts.events[stored++];
        ASSERT_EQ(g->kind, e->kind);
        ASSERT_EQ(g->flags, e->flags);
        ASSERT_EQ(g->pos, (uint32_t)rank);
        if (e->kind == CBM_SCOPE_DEF) {
            ASSERT_STR_EQ(g->name, e->name);
            ASSERT_STR_EQ(g->qual, e->qual);
            ASSERT_STR_EQ(facts.defs[g->index].qualified_name,
                          r->defs.items[e->index].qualified_name);
        }
        if (e->kind == CBM_SCOPE_INCLUDE) {
            ASSERT_STR_EQ(facts.modules[g->index], sf_module(r, e));
        }
    }
    ASSERT_EQ(stored, facts.event_count);
    ASSERT_EQ(facts.def_count, sf_count(r, CBM_SCOPE_DEF));
    ASSERT_EQ(facts.module_count, 3);

    /* a language without a row stores no "is" and reads back as none */
    CBMFileResult *go = sf_extract("package main\n\nfunc main() {}\n", CBM_LANG_GO, "main.go");
    char *go_json = NULL;
    char *go_sha = NULL;
    ASSERT_TRUE(sf_row(go, "main.go", CBM_LANG_GO, &go_json, &go_sha));
    ASSERT_NULL(strstr(go_json, "\"is\""));
    ASSERT_EQ(cbm_lsp_surface_scope_from_json(&arena, go_json, &facts), 0);
    ASSERT_EQ(
        cbm_lsp_surface_scope_from_json(&arena, "{\"is\":{\"v\":99,\"ev\":[],\"im\":[]}}", &facts),
        -1);
    cbm_arena_destroy(&arena);
    free(json);
    free(sha);
    free(go_json);
    free(go_sha);
    cbm_free_result(go);
    cbm_free_result(r);
    PASS();
}

/* Bytes that move keep the stored facts and so the surface hash; a new
 * import changes both. */
TEST(scope_facts_surface_hash_follows_facts_not_bytes) {
    char moved[sizeof(SF_PROTO) + 64];
    snprintf(moved, sizeof(moved), "// a comment that moves every byte\n\n%s", SF_PROTO);
    char added[sizeof(SF_PROTO) + 64];
    snprintf(added, sizeof(added), "%simport \"x/more.proto\";\n", SF_PROTO);
    const char *srcs[] = {SF_PROTO, moved, added};
    char *json[3] = {NULL, NULL, NULL};
    char *sha[3] = {NULL, NULL, NULL};
    for (int i = 0; i < 3; i++) {
        CBMFileResult *r = sf_extract(srcs[i], CBM_LANG_PROTOBUF, "proto/a.proto");
        ASSERT_NOT_NULL(r);
        ASSERT_TRUE(sf_row(r, "proto/a.proto", CBM_LANG_PROTOBUF, &json[i], &sha[i]));
        cbm_free_result(r);
    }
    ASSERT_STR_EQ(sha[1], sha[0]);
    ASSERT_STR_NEQ(sha[2], sha[0]);
    for (int i = 0; i < 3; i++) {
        free(json[i]);
        free(sha[i]);
    }
    PASS();
}

/* Counted work (nodes visited plus index probes) of one file with n
 * messages, n imports and n references. */
static uint64_t sf_work(int n) {
    size_t cap = (size_t)n * 96 + 64;
    char *src = malloc(cap);
    if (!src) {
        return 0;
    }
    size_t off = (size_t)snprintf(src, cap, "syntax = \"proto3\";\npackage g;\n");
    for (int i = 0; i < n; i++) {
        off += (size_t)snprintf(src + off, cap - off, "import \"d/f%d.proto\";\n", i);
    }
    for (int i = 0; i < n; i++) {
        off += (size_t)snprintf(src + off, cap - off, "message M%d { other.T%d f = 1; }\n", i, i);
    }
    uint64_t before = cbm_extract_scope_nodes_visited();
    CBMFileResult *r = sf_extract(src, CBM_LANG_PROTOBUF, "g.proto");
    uint64_t work = cbm_extract_scope_nodes_visited() - before;
    bool complete = r && sf_count(r, CBM_SCOPE_DEF) == n && sf_count(r, CBM_SCOPE_INCLUDE) == n &&
                    sf_count(r, CBM_SCOPE_QREF) == n;
    if (r) {
        cbm_free_result(r);
    }
    free(src);
    return complete ? work : 0;
}

TEST(scope_facts_work_grows_linearly) {
    uint64_t w1 = sf_work(100);
    uint64_t w4 = sf_work(400);
    uint64_t w16 = sf_work(1600);
    ASSERT_GT(w1, 0);
    ASSERT_GT(w4, 0);
    ASSERT_GT(w16, 0);
    /* linear up to the index probes' logarithm: at most 4.5 times per 4 times */
    ASSERT_TRUE(w4 * 2 <= w1 * 9);
    ASSERT_TRUE(w16 * 2 <= w4 * 9);
    PASS();
}

SUITE(scope_facts) {
    RUN_TEST(scope_facts_proto_definitions_includes_references);
    RUN_TEST(scope_facts_language_without_row_records_nothing);
    RUN_TEST(scope_facts_parse_error_is_opaque);
    RUN_TEST(scope_facts_survive_compaction);
    RUN_TEST(scope_facts_append_after_compaction_grows_the_array);
    RUN_TEST(scope_facts_surface_round_trip);
    RUN_TEST(scope_facts_surface_hash_follows_facts_not_bytes);
    RUN_TEST(scope_facts_work_grows_linearly);
}
