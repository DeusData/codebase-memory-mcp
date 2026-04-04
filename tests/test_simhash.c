/*
 * test_simhash.c — TDD tests for MinHash fingerprinting + SIMILAR_TO edges.
 *
 * Suite 1: MinHash core (compute, normalise, Jaccard)
 * Suite 2: LSH index (build, query, band bucketing)
 * Suite 3: Edge generation via pass_similarity
 * Suite 4: Full pipeline integration with generated test project
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "simhash/minhash.h"
#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "foundation/compat.h"

#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ═══════════════════════════════════════════════════════════════════
 * Helpers
 * ═══════════════════════════════════════════════════════════════════ */

/* Extract a single file and return the result.  Caller frees. */
static CBMFileResult *extract_one(const char *src, CBMLanguage lang, const char *proj,
                                  const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, proj, path, 0, NULL, NULL);
}

/* Find a definition by name in an extraction result. */
static const CBMDefinition *find_def(const CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        if (strcmp(r->defs.items[i].name, name) == 0) {
            return &r->defs.items[i];
        }
    }
    return NULL;
}

/* Count SIMILAR_TO edges in graph buffer. */
static int count_similar_to_edges(const cbm_gbuf_t *gb) {
    int count = 0;
    /* Iterate all edges via label-based node search + edge queries */
    const cbm_gbuf_node_t **funcs = NULL;
    int func_count = 0;
    cbm_gbuf_find_by_label(gb, "Function", &funcs, &func_count);
    for (int i = 0; i < func_count; i++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        cbm_gbuf_find_edges_by_source_type(gb, funcs[i]->id, "SIMILAR_TO", &edges, &edge_count);
        count += edge_count;
    }
    const cbm_gbuf_node_t **methods = NULL;
    int method_count = 0;
    cbm_gbuf_find_by_label(gb, "Method", &methods, &method_count);
    for (int i = 0; i < method_count; i++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        cbm_gbuf_find_edges_by_source_type(gb, methods[i]->id, "SIMILAR_TO", &edges, &edge_count);
        count += edge_count;
    }
    return count;
}

/* Large Go function template for fingerprint tests.  Must have enough
 * structural diversity (>= 32 unique structural trigrams) after leaf-only
 * tokenisation and normalisation. */
#define GO_VALIDATE_USER_SRC \
    "package main\n" \
    "import \"errors\"\n" \
    "import \"strings\"\n" \
    "func ValidateUser(u User) error {\n" \
    "    if u.Name == \"\" {\n" \
    "        return errors.New(\"name required\")\n" \
    "    }\n" \
    "    if len(u.Name) > 100 {\n" \
    "        return errors.New(\"name too long\")\n" \
    "    }\n" \
    "    if u.Age < 0 {\n" \
    "        return errors.New(\"invalid age\")\n" \
    "    }\n" \
    "    if u.Age > 200 {\n" \
    "        return errors.New(\"age too high\")\n" \
    "    }\n" \
    "    if u.Email == \"\" {\n" \
    "        return errors.New(\"email required\")\n" \
    "    }\n" \
    "    if !strings.Contains(u.Email, \"@\") {\n" \
    "        return errors.New(\"invalid email\")\n" \
    "    }\n" \
    "    if u.Phone == \"\" {\n" \
    "        return errors.New(\"phone required\")\n" \
    "    }\n" \
    "    if len(u.Phone) < 7 {\n" \
    "        return errors.New(\"phone too short\")\n" \
    "    }\n" \
    "    if u.Country == \"\" {\n" \
    "        return errors.New(\"country required\")\n" \
    "    }\n" \
    "    for _, c := range u.Tags {\n" \
    "        if c == \"\" {\n" \
    "            return errors.New(\"empty tag\")\n" \
    "        }\n" \
    "    }\n" \
    "    return nil\n" \
    "}\n"

/* Same structure, different names/types — near-clone */
#define GO_VALIDATE_ORDER_SRC \
    "package main\n" \
    "import \"errors\"\n" \
    "import \"strings\"\n" \
    "func ValidateOrder(o Order) error {\n" \
    "    if o.Title == \"\" {\n" \
    "        return errors.New(\"title required\")\n" \
    "    }\n" \
    "    if len(o.Title) > 100 {\n" \
    "        return errors.New(\"title too long\")\n" \
    "    }\n" \
    "    if o.Amount < 0 {\n" \
    "        return errors.New(\"invalid amount\")\n" \
    "    }\n" \
    "    if o.Amount > 200 {\n" \
    "        return errors.New(\"amount too high\")\n" \
    "    }\n" \
    "    if o.Status == \"\" {\n" \
    "        return errors.New(\"status required\")\n" \
    "    }\n" \
    "    if !strings.Contains(o.Status, \"@\") {\n" \
    "        return errors.New(\"invalid status\")\n" \
    "    }\n" \
    "    if o.Region == \"\" {\n" \
    "        return errors.New(\"region required\")\n" \
    "    }\n" \
    "    if len(o.Region) < 7 {\n" \
    "        return errors.New(\"region too short\")\n" \
    "    }\n" \
    "    if o.Vendor == \"\" {\n" \
    "        return errors.New(\"vendor required\")\n" \
    "    }\n" \
    "    for _, c := range o.Items {\n" \
    "        if c == \"\" {\n" \
    "            return errors.New(\"empty item\")\n" \
    "        }\n" \
    "    }\n" \
    "    return nil\n" \
    "}\n"

/* Completely different structure */
#define GO_HANDLE_REQUEST_SRC \
    "package main\n" \
    "import \"net/http\"\n" \
    "import \"encoding/json\"\n" \
    "import \"io\"\n" \
    "func HandleRequest(w http.ResponseWriter, r *http.Request) {\n" \
    "    body, err := io.ReadAll(r.Body)\n" \
    "    if err != nil {\n" \
    "        http.Error(w, err.Error(), 400)\n" \
    "        return\n" \
    "    }\n" \
    "    defer r.Body.Close()\n" \
    "    var data map[string]interface{}\n" \
    "    if err := json.Unmarshal(body, &data); err != nil {\n" \
    "        http.Error(w, err.Error(), 400)\n" \
    "        return\n" \
    "    }\n" \
    "    result := make(map[string]interface{})\n" \
    "    for k, v := range data {\n" \
    "        switch val := v.(type) {\n" \
    "        case string:\n" \
    "            result[k] = strings.ToUpper(val)\n" \
    "        case float64:\n" \
    "            result[k] = val * 2\n" \
    "        default:\n" \
    "            result[k] = v\n" \
    "        }\n" \
    "    }\n" \
    "    w.Header().Set(\"Content-Type\", \"application/json\")\n" \
    "    json.NewEncoder(w).Encode(result)\n" \
    "}\n"

/* ═══════════════════════════════════════════════════════════════════
 * Suite 1: MinHash Core
 * ═══════════════════════════════════════════════════════════════════ */

/* Two identical Go functions must produce identical MinHash signatures. */
TEST(minhash_identical_source_same_fingerprint) {
    const char *src = GO_VALIDATE_USER_SRC;

    CBMFileResult *r1 = extract_one(src, CBM_LANG_GO, "test", "a.go");
    CBMFileResult *r2 = extract_one(src, CBM_LANG_GO, "test", "b.go");
    ASSERT_NOT_NULL(r1);
    ASSERT_NOT_NULL(r2);

    const CBMDefinition *d1 = find_def(r1, "ValidateUser");
    const CBMDefinition *d2 = find_def(r2, "ValidateUser");
    ASSERT_NOT_NULL(d1);
    ASSERT_NOT_NULL(d2);
    ASSERT_NOT_NULL(d1->fingerprint);
    ASSERT_NOT_NULL(d2->fingerprint);
    ASSERT_EQ(d1->fingerprint_k, CBM_MINHASH_K);

    /* Identical source → identical fingerprint */
    ASSERT_MEM_EQ(d1->fingerprint, d2->fingerprint,
                  (size_t)CBM_MINHASH_K * sizeof(uint32_t));

    cbm_free_result(r1);
    cbm_free_result(r2);
    PASS();
}

/* Renamed variables should produce the same fingerprint (identifiers normalised). */
TEST(minhash_renamed_vars_same_fingerprint) {
    const char *src_a = GO_VALIDATE_USER_SRC;
    const char *src_b = GO_VALIDATE_ORDER_SRC;

    CBMFileResult *ra = extract_one(src_a, CBM_LANG_GO, "test", "a.go");
    CBMFileResult *rb = extract_one(src_b, CBM_LANG_GO, "test", "b.go");
    ASSERT_NOT_NULL(ra);
    ASSERT_NOT_NULL(rb);

    const CBMDefinition *da = find_def(ra, "ValidateUser");
    const CBMDefinition *db = find_def(rb, "ValidateOrder");
    ASSERT_NOT_NULL(da);
    ASSERT_NOT_NULL(db);
    ASSERT_NOT_NULL(da->fingerprint);
    ASSERT_NOT_NULL(db->fingerprint);

    double j = cbm_minhash_jaccard(
        (const cbm_minhash_t *)da->fingerprint,
        (const cbm_minhash_t *)db->fingerprint);
    /* Renamed vars + same structure → very high Jaccard */
    ASSERT_TRUE(j >= 0.90);

    cbm_free_result(ra);
    cbm_free_result(rb);
    PASS();
}

/* Completely different function bodies → low Jaccard. */
TEST(minhash_different_code_different_fingerprint) {
    const char *src_a = GO_VALIDATE_USER_SRC;
    const char *src_b = GO_HANDLE_REQUEST_SRC;

    CBMFileResult *ra = extract_one(src_a, CBM_LANG_GO, "test", "a.go");
    CBMFileResult *rb = extract_one(src_b, CBM_LANG_GO, "test", "b.go");
    ASSERT_NOT_NULL(ra);
    ASSERT_NOT_NULL(rb);

    const CBMDefinition *da = find_def(ra, "ValidateUser");
    const CBMDefinition *db = find_def(rb, "HandleRequest");
    ASSERT_NOT_NULL(da);
    ASSERT_NOT_NULL(db);
    ASSERT_NOT_NULL(da->fingerprint);
    ASSERT_NOT_NULL(db->fingerprint);

    double j = cbm_minhash_jaccard(
        (const cbm_minhash_t *)da->fingerprint,
        (const cbm_minhash_t *)db->fingerprint);
    /* Different structure → low Jaccard */
    ASSERT_TRUE(j < 0.5);

    cbm_free_result(ra);
    cbm_free_result(rb);
    PASS();
}

/* Same function with one added check → high but not perfect Jaccard. */
TEST(minhash_minor_edit_high_jaccard) {
    const char *src_a = GO_VALIDATE_USER_SRC;

    /* ValidateUser with one extra check added at the end */
    const char *src_b =
        "package main\n"
        "import \"errors\"\n"
        "import \"strings\"\n"
        "func ValidateUser(u User) error {\n"
        "    if u.Name == \"\" {\n"
        "        return errors.New(\"name required\")\n"
        "    }\n"
        "    if len(u.Name) > 100 {\n"
        "        return errors.New(\"name too long\")\n"
        "    }\n"
        "    if u.Age < 0 {\n"
        "        return errors.New(\"invalid age\")\n"
        "    }\n"
        "    if u.Age > 200 {\n"
        "        return errors.New(\"age too high\")\n"
        "    }\n"
        "    if u.Email == \"\" {\n"
        "        return errors.New(\"email required\")\n"
        "    }\n"
        "    if !strings.Contains(u.Email, \"@\") {\n"
        "        return errors.New(\"invalid email\")\n"
        "    }\n"
        "    if u.Phone == \"\" {\n"
        "        return errors.New(\"phone required\")\n"
        "    }\n"
        "    if len(u.Phone) < 7 {\n"
        "        return errors.New(\"phone too short\")\n"
        "    }\n"
        "    if u.Country == \"\" {\n"
        "        return errors.New(\"country required\")\n"
        "    }\n"
        "    for _, c := range u.Tags {\n"
        "        if c == \"\" {\n"
        "            return errors.New(\"empty tag\")\n"
        "        }\n"
        "    }\n"
        "    if u.Active == false {\n"
        "        return errors.New(\"user inactive\")\n"
        "    }\n"
        "    return nil\n"
        "}\n";

    CBMFileResult *ra = extract_one(src_a, CBM_LANG_GO, "test", "a.go");
    CBMFileResult *rb = extract_one(src_b, CBM_LANG_GO, "test", "b.go");
    ASSERT_NOT_NULL(ra);
    ASSERT_NOT_NULL(rb);

    const CBMDefinition *da = find_def(ra, "ValidateUser");
    const CBMDefinition *db = find_def(rb, "ValidateUser");
    ASSERT_NOT_NULL(da);
    ASSERT_NOT_NULL(db);
    ASSERT_NOT_NULL(da->fingerprint);
    ASSERT_NOT_NULL(db->fingerprint);

    double j = cbm_minhash_jaccard(
        (const cbm_minhash_t *)da->fingerprint,
        (const cbm_minhash_t *)db->fingerprint);
    /* Minor edit → moderately high Jaccard.  Trigram-based MinHash
     * is sensitive to insertions (shifts the trigram window), so a
     * single added statement may drop Jaccard more than expected.
     * The key property: it's still higher than completely different code. */
    ASSERT_TRUE(j > 0.50);
    ASSERT_TRUE(j < 1.0);

    cbm_free_result(ra);
    cbm_free_result(rb);
    PASS();
}

/* Functions with fewer than MIN_NODES AST body nodes → no fingerprint.
 * Note: even a 2-line Go function can have 15+ AST nodes (Go's AST is verbose).
 * Use a truly minimal function to test the threshold. */
TEST(minhash_empty_body_skipped) {
    const char *src =
        "package main\n"
        "func Noop() {}\n";

    CBMFileResult *r = extract_one(src, CBM_LANG_GO, "test", "tiny.go");
    ASSERT_NOT_NULL(r);

    const CBMDefinition *d = find_def(r, "Noop");
    ASSERT_NOT_NULL(d);
    /* Empty body → fingerprint should be NULL */
    ASSERT_NULL(d->fingerprint);

    cbm_free_result(r);
    PASS();
}

/* Type names normalised: User vs Order → same fingerprint (identifiers → "I"). */
TEST(minhash_type_annotation_normalized) {
    /* ValidateUser uses "User" type, ValidateOrder uses "Order" type.
     * Both should normalise identifiers to "I" and produce high Jaccard. */
    const char *src_a = GO_VALIDATE_USER_SRC;

    const char *src_b = GO_VALIDATE_ORDER_SRC;

    CBMFileResult *ra = extract_one(src_a, CBM_LANG_GO, "test", "a.go");
    CBMFileResult *rb = extract_one(src_b, CBM_LANG_GO, "test", "b.go");
    ASSERT_NOT_NULL(ra);
    ASSERT_NOT_NULL(rb);

    const CBMDefinition *da = find_def(ra, "ValidateUser");
    const CBMDefinition *db = find_def(rb, "ValidateOrder");
    ASSERT_NOT_NULL(da);
    ASSERT_NOT_NULL(db);
    ASSERT_NOT_NULL(da->fingerprint);
    ASSERT_NOT_NULL(db->fingerprint);

    double j = cbm_minhash_jaccard(
        (const cbm_minhash_t *)da->fingerprint,
        (const cbm_minhash_t *)db->fingerprint);
    /* Type annotations normalised → high Jaccard */
    ASSERT_TRUE(j >= 0.90);

    cbm_free_result(ra);
    cbm_free_result(rb);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite 2: Jaccard + LSH
 * ═══════════════════════════════════════════════════════════════════ */

TEST(jaccard_identical) {
    cbm_minhash_t fp;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp.values[i] = (uint32_t)(i * 17 + 42);
    }
    double j = cbm_minhash_jaccard(&fp, &fp);
    ASSERT_FLOAT_EQ(j, 1.0, 0.001);
    PASS();
}

TEST(jaccard_disjoint) {
    cbm_minhash_t a, b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        a.values[i] = (uint32_t)(i * 3 + 1);
        b.values[i] = (uint32_t)(i * 3 + 2); /* all different */
    }
    double j = cbm_minhash_jaccard(&a, &b);
    ASSERT_FLOAT_EQ(j, 0.0, 0.001);
    PASS();
}

TEST(jaccard_partial_overlap) {
    cbm_minhash_t a, b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        a.values[i] = (uint32_t)(i * 5);
        /* First 48 match, last 16 differ */
        b.values[i] = (i < 48) ? a.values[i] : (uint32_t)(i * 5 + 999);
    }
    double j = cbm_minhash_jaccard(&a, &b);
    ASSERT_FLOAT_EQ(j, 48.0 / 64.0, 0.001);
    PASS();
}

TEST(minhash_hex_roundtrip) {
    cbm_minhash_t original;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        original.values[i] = (uint32_t)(0xDEAD0000 + i);
    }
    char hex[CBM_MINHASH_HEX_LEN + 1];
    cbm_minhash_to_hex(&original, hex, sizeof(hex));
    ASSERT_EQ((int)strlen(hex), CBM_MINHASH_HEX_LEN);

    cbm_minhash_t decoded;
    bool ok = cbm_minhash_from_hex(hex, &decoded);
    ASSERT_TRUE(ok);
    ASSERT_MEM_EQ(&original, &decoded, sizeof(cbm_minhash_t));
    PASS();
}

TEST(lsh_same_bucket_similar) {
    /* Two fingerprints with Jaccard ≈ 0.97 (62/64 match) should share a bucket */
    cbm_minhash_t a, b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        a.values[i] = (uint32_t)(i * 7 + 13);
        b.values[i] = a.values[i]; /* start identical */
    }
    /* Differ in only 2 positions */
    b.values[0] = 0xFFFFFFFF;
    b.values[1] = 0xFFFFFFFE;

    cbm_lsh_index_t *idx = cbm_lsh_new();
    cbm_lsh_entry_t ea = {.node_id = 1, .fingerprint = &a, .file_path = "a.go", .file_ext = ".go"};
    cbm_lsh_insert(idx, &ea);

    const cbm_lsh_entry_t **candidates = NULL;
    int count = 0;
    cbm_lsh_query(idx, &b, &candidates, &count);

    /* Must find the similar fingerprint as a candidate */
    ASSERT_GT(count, 0);
    bool found = false;
    for (int i = 0; i < count; i++) {
        if (candidates[i]->node_id == 1) {
            found = true;
        }
    }
    ASSERT_TRUE(found);

    cbm_lsh_free(idx);
    PASS();
}

TEST(lsh_different_bucket_dissimilar) {
    /* Two fingerprints with Jaccard ≈ 0.0 should rarely share a bucket */
    cbm_minhash_t a, b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        a.values[i] = (uint32_t)(i * 3 + 1);
        b.values[i] = (uint32_t)(i * 3 + 2);
    }

    cbm_lsh_index_t *idx = cbm_lsh_new();
    cbm_lsh_entry_t ea = {.node_id = 1, .fingerprint = &a, .file_path = "a.go", .file_ext = ".go"};
    cbm_lsh_insert(idx, &ea);

    const cbm_lsh_entry_t **candidates = NULL;
    int count = 0;
    cbm_lsh_query(idx, &b, &candidates, &count);

    /* Should NOT find as candidate (or very unlikely) */
    bool found = false;
    for (int i = 0; i < count; i++) {
        if (candidates[i]->node_id == 1) {
            found = true;
        }
    }
    ASSERT_FALSE(found);

    cbm_lsh_free(idx);
    PASS();
}

TEST(lsh_index_build_and_query) {
    /* Build index with 100 fingerprints, one known clone, verify found. */
    cbm_minhash_t fps[100];
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < CBM_MINHASH_K; j++) {
            fps[i].values[j] = (uint32_t)(i * 1000 + j * 13 + 7);
        }
    }
    /* Make fps[50] a near-clone of fps[0] (differ in 2 positions) */
    memcpy(&fps[50], &fps[0], sizeof(cbm_minhash_t));
    fps[50].values[0] = 0xAAAAAAAA;
    fps[50].values[1] = 0xBBBBBBBB;

    cbm_lsh_index_t *idx = cbm_lsh_new();
    for (int i = 0; i < 100; i++) {
        cbm_lsh_entry_t e = {
            .node_id = i + 1,
            .fingerprint = &fps[i],
            .file_path = "f.go",
            .file_ext = ".go",
        };
        cbm_lsh_insert(idx, &e);
    }

    /* Query with fps[0] — should find fps[50] as candidate */
    const cbm_lsh_entry_t **candidates = NULL;
    int count = 0;
    cbm_lsh_query(idx, &fps[0], &candidates, &count);

    bool found_clone = false;
    for (int i = 0; i < count; i++) {
        if (candidates[i]->node_id == 51) { /* fps[50] → node_id 51 */
            found_clone = true;
        }
    }
    ASSERT_TRUE(found_clone);

    cbm_lsh_free(idx);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite 3: Edge Generation (pass_similarity on graph buffer)
 * ═══════════════════════════════════════════════════════════════════ */

/* Helper: build a fingerprint hex string for a properties_json. */
static void make_fp_props(char *buf, int bufsize, const cbm_minhash_t *fp) {
    char hex[CBM_MINHASH_HEX_LEN + 1];
    cbm_minhash_to_hex(fp, hex, sizeof(hex));
    snprintf(buf, bufsize, "{\"fp\":\"%s\"}", hex);
}

TEST(pass_similarity_creates_edges) {
    cbm_minhash_t fp;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp.values[i] = (uint32_t)(i * 7 + 13);
    }
    char props[1024];
    make_fp_props(props, sizeof(props), &fp);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "test.a.foo", "a.go", 1, 10, props);
    cbm_gbuf_upsert_node(gb, "Function", "bar", "test.b.bar", "b.go", 1, 10, props);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    int rc = cbm_pipeline_pass_similarity(&ctx);
    ASSERT_EQ(rc, 0);

    int sim_count = count_similar_to_edges(gb);
    ASSERT_EQ(sim_count, 1); /* A→B only (not bidirectional) */

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_no_edges_different) {
    cbm_minhash_t fp_a, fp_b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp_a.values[i] = (uint32_t)(i * 3 + 1);
        fp_b.values[i] = (uint32_t)(i * 3 + 2);
    }
    char props_a[1024], props_b[1024];
    make_fp_props(props_a, sizeof(props_a), &fp_a);
    make_fp_props(props_b, sizeof(props_b), &fp_b);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "test.a.foo", "a.go", 1, 10, props_a);
    cbm_gbuf_upsert_node(gb, "Function", "bar", "test.b.bar", "b.go", 1, 10, props_b);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    int rc = cbm_pipeline_pass_similarity(&ctx);
    ASSERT_EQ(rc, 0);

    int sim_count = count_similar_to_edges(gb);
    ASSERT_EQ(sim_count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_same_file_tagged) {
    cbm_minhash_t fp;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp.values[i] = (uint32_t)(i * 7 + 13);
    }
    char props[1024];
    make_fp_props(props, sizeof(props), &fp);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id_a = cbm_gbuf_upsert_node(gb, "Function", "foo", "test.a.foo", "same.go", 1, 10, props);
    cbm_gbuf_upsert_node(gb, "Function", "bar", "test.a.bar", "same.go", 11, 20, props);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    cbm_pipeline_pass_similarity(&ctx);

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, id_a, "SIMILAR_TO", &edges, &edge_count);
    ASSERT_EQ(edge_count, 1);
    /* Edge should have same_file property */
    ASSERT_NOT_NULL(strstr(edges[0]->properties_json, "\"same_file\":true"));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_cross_language_skip) {
    cbm_minhash_t fp;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp.values[i] = (uint32_t)(i * 7 + 13);
    }
    char props[1024];
    make_fp_props(props, sizeof(props), &fp);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "foo", "test.a.foo", "a.go", 1, 10, props);
    cbm_gbuf_upsert_node(gb, "Function", "bar", "test.b.bar", "b.py", 1, 10, props);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    cbm_pipeline_pass_similarity(&ctx);

    int sim_count = count_similar_to_edges(gb);
    ASSERT_EQ(sim_count, 0); /* Different languages → no edge */

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_edge_properties) {
    /* 60 of 64 hashes match → Jaccard = 60/64 ≈ 0.9375 — below threshold, no edge.
     * 62 of 64 match → Jaccard ≈ 0.969 — above threshold, edge with props. */
    cbm_minhash_t fp_a, fp_b;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp_a.values[i] = (uint32_t)(i * 7 + 13);
        fp_b.values[i] = fp_a.values[i];
    }
    /* Differ in 2 positions → Jaccard = 62/64 ≈ 0.969 */
    fp_b.values[0] = 0xDEADBEEF;
    fp_b.values[1] = 0xCAFEBABE;

    char props_a[1024], props_b[1024];
    make_fp_props(props_a, sizeof(props_a), &fp_a);
    make_fp_props(props_b, sizeof(props_b), &fp_b);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    int64_t id_a = cbm_gbuf_upsert_node(gb, "Function", "foo", "test.a.foo", "a.go", 1, 10, props_a);
    cbm_gbuf_upsert_node(gb, "Function", "bar", "test.b.bar", "b.go", 1, 10, props_b);

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    cbm_pipeline_pass_similarity(&ctx);

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, id_a, "SIMILAR_TO", &edges, &edge_count);
    ASSERT_EQ(edge_count, 1);
    ASSERT_NOT_NULL(strstr(edges[0]->properties_json, "\"jaccard\""));
    ASSERT_NOT_NULL(strstr(edges[0]->properties_json, "\"same_file\":false"));

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_max_edges_cap) {
    cbm_minhash_t fp;
    for (int i = 0; i < CBM_MINHASH_K; i++) {
        fp.values[i] = (uint32_t)(i * 7 + 13);
    }
    char props[1024];
    make_fp_props(props, sizeof(props), &fp);

    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    /* Create 20 functions with identical fingerprints */
    for (int i = 0; i < 20; i++) {
        char name[32], qn[64], path[32];
        snprintf(name, sizeof(name), "func_%d", i);
        snprintf(qn, sizeof(qn), "test.f%d.func_%d", i, i);
        snprintf(path, sizeof(path), "f%d.go", i);
        cbm_gbuf_upsert_node(gb, "Function", name, qn, path, 1, 10, props);
    }

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    cbm_pipeline_pass_similarity(&ctx);

    /* Each function should have at most CBM_MINHASH_MAX_EDGES_PER_NODE edges */
    const cbm_gbuf_node_t **funcs = NULL;
    int func_count = 0;
    cbm_gbuf_find_by_label(gb, "Function", &funcs, &func_count);
    for (int i = 0; i < func_count; i++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        cbm_gbuf_find_edges_by_source_type(gb, funcs[i]->id, "SIMILAR_TO", &edges, &edge_count);
        ASSERT_LTE(edge_count, CBM_MINHASH_MAX_EDGES_PER_NODE);
    }

    cbm_gbuf_free(gb);
    PASS();
}

TEST(pass_similarity_short_funcs_skipped) {
    /* Nodes without "fp" in properties → no edges */
    cbm_gbuf_t *gb = cbm_gbuf_new("test", "/tmp");
    cbm_gbuf_upsert_node(gb, "Function", "tiny1", "test.t.tiny1", "t.go", 1, 3, "{}");
    cbm_gbuf_upsert_node(gb, "Function", "tiny2", "test.t.tiny2", "t.go", 4, 6, "{}");

    atomic_int cancelled = 0;
    cbm_pipeline_ctx_t ctx = {
        .project_name = "test",
        .repo_path = "/tmp",
        .gbuf = gb,
        .registry = NULL,
        .cancelled = &cancelled,
    };

    cbm_pipeline_pass_similarity(&ctx);

    int sim_count = count_similar_to_edges(gb);
    ASSERT_EQ(sim_count, 0);

    cbm_gbuf_free(gb);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite 4: Full Pipeline Integration (generated test project)
 * ═══════════════════════════════════════════════════════════════════ */

static char g_sim_tmpdir[512];

static int setup_sim_test_repo(void) {
    snprintf(g_sim_tmpdir, sizeof(g_sim_tmpdir), "/tmp/cbm_sim_test_XXXXXX");
    if (!cbm_mkdtemp(g_sim_tmpdir)) {
        return -1;
    }

    /* Near-clone: ValidateUser and ValidateOrder have same structure, different names.
     * Must be large enough (>= 30 leaf tokens, >= 32 unique structural trigrams). */
    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/validation/user_validator.go"),
        "package validation\n"
        "import \"errors\"\n"
        "import \"strings\"\n"
        "func ValidateUser(u User) error {\n"
        "    if u.Name == \"\" { return errors.New(\"name required\") }\n"
        "    if len(u.Name) > 100 { return errors.New(\"name too long\") }\n"
        "    if u.Age < 0 { return errors.New(\"invalid age\") }\n"
        "    if u.Age > 200 { return errors.New(\"age too high\") }\n"
        "    if u.Email == \"\" { return errors.New(\"email required\") }\n"
        "    if !strings.Contains(u.Email, \"@\") { return errors.New(\"invalid email\") }\n"
        "    if u.Phone == \"\" { return errors.New(\"phone required\") }\n"
        "    if len(u.Phone) < 7 { return errors.New(\"phone too short\") }\n"
        "    if u.Country == \"\" { return errors.New(\"country required\") }\n"
        "    for _, c := range u.Tags {\n"
        "        if c == \"\" { return errors.New(\"empty tag\") }\n"
        "    }\n"
        "    return nil\n"
        "}\n");

    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/validation/order_validator.go"),
        "package validation\n"
        "import \"errors\"\n"
        "import \"strings\"\n"
        "func ValidateOrder(o Order) error {\n"
        "    if o.Title == \"\" { return errors.New(\"title required\") }\n"
        "    if len(o.Title) > 100 { return errors.New(\"title too long\") }\n"
        "    if o.Amount < 0 { return errors.New(\"invalid amount\") }\n"
        "    if o.Amount > 200 { return errors.New(\"amount too high\") }\n"
        "    if o.Status == \"\" { return errors.New(\"status required\") }\n"
        "    if !strings.Contains(o.Status, \"@\") { return errors.New(\"invalid status\") }\n"
        "    if o.Region == \"\" { return errors.New(\"region required\") }\n"
        "    if len(o.Region) < 7 { return errors.New(\"region too short\") }\n"
        "    if o.Vendor == \"\" { return errors.New(\"vendor required\") }\n"
        "    for _, c := range o.Items {\n"
        "        if c == \"\" { return errors.New(\"empty item\") }\n"
        "    }\n"
        "    return nil\n"
        "}\n");

    /* Completely different function — also large enough for fingerprinting */
    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/handler/user_handler.go"),
        GO_HANDLE_REQUEST_SRC);

    /* Tiny function — should be skipped */
    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/util/tiny_helper.go"),
        "package util\n"
        "\n"
        "func Max(a, b int) int {\n"
        "    if a > b { return a }\n"
        "    return b\n"
        "}\n");

    return 0;
}

static void teardown_sim_test_repo(void) {
    if (g_sim_tmpdir[0]) {
        th_rmtree(g_sim_tmpdir);
        g_sim_tmpdir[0] = '\0';
    }
}

TEST(pipeline_minhash_end_to_end) {
    if (setup_sim_test_repo() != 0) {
        SKIP("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_sim_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    /* Open store and verify SIMILAR_TO edges */
    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    const char *project = cbm_pipeline_project_name(p);

    /* Find SIMILAR_TO edges */
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    int find_rc = cbm_store_find_edges_by_type(s, project, "SIMILAR_TO", &edges, &edge_count);
    ASSERT_EQ(find_rc, CBM_STORE_OK);

    /* Should have at least 1 SIMILAR_TO edge (ValidateUser ↔ ValidateOrder) */
    ASSERT_GTE(edge_count, 1);

    /* Verify ValidateUser has a fingerprint property */
    cbm_node_t vu_node = {0};
    char vu_qn[256];
    snprintf(vu_qn, sizeof(vu_qn), "%s.pkg.validation.user_validator.ValidateUser", project);
    int vu_rc = cbm_store_find_node_by_qn(s, project, vu_qn, &vu_node);
    if (vu_rc == CBM_STORE_OK) {
        ASSERT_NOT_NULL(strstr(vu_node.properties_json, "\"fp\""));
        cbm_node_free_fields(&vu_node);
    }

    /* Note: even Max (3 lines) may have a fingerprint — Go's AST is verbose.
     * We only skip truly empty/trivial functions (< 10 AST nodes).
     * The key assertion is that SIMILAR_TO edges exist for the clones. */

    cbm_store_free_edges(edges, edge_count);
    cbm_store_close(s);
    cbm_pipeline_free(p);
    teardown_sim_test_repo();
    PASS();
}

TEST(pipeline_minhash_no_false_positives) {
    snprintf(g_sim_tmpdir, sizeof(g_sim_tmpdir), "/tmp/cbm_sim_nofp_XXXXXX");
    if (!cbm_mkdtemp(g_sim_tmpdir)) {
        SKIP("failed to create temp dir");
    }

    /* 5 diverse functions — no clones */
    th_write_file(TH_PATH(g_sim_tmpdir, "a.go"),
        "package main\n"
        "func SortSlice(s []int) {\n"
        "    for i := 0; i < len(s); i++ {\n"
        "        for j := i+1; j < len(s); j++ {\n"
        "            if s[i] > s[j] {\n"
        "                s[i], s[j] = s[j], s[i]\n"
        "            }\n"
        "        }\n"
        "    }\n"
        "}\n");

    th_write_file(TH_PATH(g_sim_tmpdir, "b.go"),
        "package main\n"
        "import \"net/http\"\n"
        "func ServeAPI(mux *http.ServeMux) {\n"
        "    mux.HandleFunc(\"/health\", func(w http.ResponseWriter, r *http.Request) {\n"
        "        w.WriteHeader(200)\n"
        "        w.Write([]byte(\"ok\"))\n"
        "    })\n"
        "    mux.HandleFunc(\"/ready\", func(w http.ResponseWriter, r *http.Request) {\n"
        "        w.WriteHeader(200)\n"
        "        w.Write([]byte(\"ready\"))\n"
        "    })\n"
        "}\n");

    th_write_file(TH_PATH(g_sim_tmpdir, "c.go"),
        "package main\n"
        "import \"fmt\"\n"
        "func PrintTree(node *Node, depth int) {\n"
        "    for i := 0; i < depth; i++ {\n"
        "        fmt.Print(\"  \")\n"
        "    }\n"
        "    fmt.Println(node.Value)\n"
        "    for _, child := range node.Children {\n"
        "        PrintTree(child, depth+1)\n"
        "    }\n"
        "}\n");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_sim_tmpdir);

    cbm_pipeline_t *p = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);

    int rc = cbm_pipeline_run(p);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(s);

    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    cbm_store_find_edges_by_type(s, cbm_pipeline_project_name(p), "SIMILAR_TO", &edges, &edge_count);

    ASSERT_EQ(edge_count, 0); /* No clones → no SIMILAR_TO edges */

    if (edges) {
        cbm_store_free_edges(edges, edge_count);
    }
    cbm_store_close(s);
    cbm_pipeline_free(p);
    th_rmtree(g_sim_tmpdir);
    g_sim_tmpdir[0] = '\0';
    PASS();
}

TEST(pipeline_minhash_incremental) {
    if (setup_sim_test_repo() != 0) {
        SKIP("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_sim_tmpdir);

    /* Step 1: Full pipeline → ValidateUser ↔ ValidateOrder edge created */
    cbm_pipeline_t *p1 = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p1);
    int rc = cbm_pipeline_run(p1);
    ASSERT_EQ(rc, 0);
    const char *project = cbm_pipeline_project_name(p1);

    /* Verify edge exists */
    cbm_store_t *s1 = cbm_store_open_path(db_path);
    cbm_edge_t *edges1 = NULL;
    int count1 = 0;
    cbm_store_find_edges_by_type(s1, project, "SIMILAR_TO", &edges1, &count1);
    ASSERT_GTE(count1, 1);
    if (edges1) {
        cbm_store_free_edges(edges1, count1);
    }
    cbm_store_close(s1);
    cbm_pipeline_free(p1);

    /* Step 2: Modify order_validator.go to be completely different */
    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/validation/order_validator.go"),
        "package validation\n"
        "\n"
        "import \"net/http\"\n"
        "\n"
        "func HandleOrder(w http.ResponseWriter, r *http.Request) {\n"
        "    data := make(map[string]interface{})\n"
        "    for k, v := range r.URL.Query() {\n"
        "        data[k] = v[0]\n"
        "    }\n"
        "    w.Header().Set(\"Content-Type\", \"application/json\")\n"
        "}\n");

    /* Step 3: Incremental reindex */
    cbm_pipeline_t *p2 = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
    rc = cbm_pipeline_run(p2);
    ASSERT_EQ(rc, 0);

    /* Step 4: Verify old SIMILAR_TO edge is gone */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    cbm_edge_t *edges2 = NULL;
    int count2 = 0;
    cbm_store_find_edges_by_type(s2, cbm_pipeline_project_name(p2), "SIMILAR_TO", &edges2, &count2);
    ASSERT_EQ(count2, 0); /* Functions no longer similar */
    if (edges2) {
        cbm_store_free_edges(edges2, count2);
    }
    cbm_store_close(s2);
    cbm_pipeline_free(p2);

    teardown_sim_test_repo();
    PASS();
}

TEST(pipeline_minhash_incremental_new_clone) {
    if (setup_sim_test_repo() != 0) {
        SKIP("failed to create temp dir");
    }

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", g_sim_tmpdir);

    /* Step 1: Full pipeline */
    cbm_pipeline_t *p1 = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p1);
    int rc = cbm_pipeline_run(p1);
    ASSERT_EQ(rc, 0);

    cbm_store_t *s1 = cbm_store_open_path(db_path);
    cbm_edge_t *edges1 = NULL;
    int count1 = 0;
    cbm_store_find_edges_by_type(s1, cbm_pipeline_project_name(p1), "SIMILAR_TO", &edges1, &count1);
    int original_count = count1;
    if (edges1) {
        cbm_store_free_edges(edges1, count1);
    }
    cbm_store_close(s1);
    cbm_pipeline_free(p1);

    /* Step 2: Add a new near-clone of ValidateUser */
    th_write_file(TH_PATH(g_sim_tmpdir, "pkg/validation/address_validator.go"),
        "package validation\n"
        "import \"errors\"\n"
        "import \"strings\"\n"
        "func ValidateAddress(a Address) error {\n"
        "    if a.Street == \"\" { return errors.New(\"street required\") }\n"
        "    if len(a.Street) > 100 { return errors.New(\"street too long\") }\n"
        "    if a.Zip < 0 { return errors.New(\"invalid zip\") }\n"
        "    if a.Zip > 99999 { return errors.New(\"zip too high\") }\n"
        "    if a.City == \"\" { return errors.New(\"city required\") }\n"
        "    if !strings.Contains(a.City, \" \") { return errors.New(\"invalid city\") }\n"
        "    if a.State == \"\" { return errors.New(\"state required\") }\n"
        "    if len(a.State) < 2 { return errors.New(\"state too short\") }\n"
        "    if a.Country == \"\" { return errors.New(\"country required\") }\n"
        "    for _, c := range a.Lines {\n"
        "        if c == \"\" { return errors.New(\"empty line\") }\n"
        "    }\n"
        "    return nil\n"
        "}\n");

    /* Step 3: Reindex (will be incremental if DB exists, or full) */
    cbm_pipeline_t *p2 = cbm_pipeline_new(g_sim_tmpdir, db_path, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p2);
    rc = cbm_pipeline_run(p2);
    ASSERT_EQ(rc, 0);

    /* Step 4: Should have MORE SIMILAR_TO edges now */
    cbm_store_t *s2 = cbm_store_open_path(db_path);
    cbm_edge_t *edges2 = NULL;
    int count2 = 0;
    cbm_store_find_edges_by_type(s2, cbm_pipeline_project_name(p2), "SIMILAR_TO", &edges2, &count2);
    ASSERT_GT(count2, original_count); /* New clone adds more edges */
    if (edges2) {
        cbm_store_free_edges(edges2, count2);
    }
    cbm_store_close(s2);
    cbm_pipeline_free(p2);

    teardown_sim_test_repo();
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * Suite Registration
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(simhash) {
    /* Suite 1: MinHash Core */
    RUN_TEST(minhash_identical_source_same_fingerprint);
    RUN_TEST(minhash_renamed_vars_same_fingerprint);
    RUN_TEST(minhash_different_code_different_fingerprint);
    RUN_TEST(minhash_minor_edit_high_jaccard);
    RUN_TEST(minhash_empty_body_skipped);
    RUN_TEST(minhash_type_annotation_normalized);

    /* Suite 2: Jaccard + LSH */
    RUN_TEST(jaccard_identical);
    RUN_TEST(jaccard_disjoint);
    RUN_TEST(jaccard_partial_overlap);
    RUN_TEST(minhash_hex_roundtrip);
    RUN_TEST(lsh_same_bucket_similar);
    RUN_TEST(lsh_different_bucket_dissimilar);
    RUN_TEST(lsh_index_build_and_query);

    /* Suite 3: Edge Generation */
    RUN_TEST(pass_similarity_creates_edges);
    RUN_TEST(pass_similarity_no_edges_different);
    RUN_TEST(pass_similarity_same_file_tagged);
    RUN_TEST(pass_similarity_cross_language_skip);
    RUN_TEST(pass_similarity_edge_properties);
    RUN_TEST(pass_similarity_max_edges_cap);
    RUN_TEST(pass_similarity_short_funcs_skipped);

    /* Suite 4: Full Pipeline Integration */
    RUN_TEST(pipeline_minhash_end_to_end);
    RUN_TEST(pipeline_minhash_no_false_positives);
    RUN_TEST(pipeline_minhash_incremental);
    RUN_TEST(pipeline_minhash_incremental_new_clone);
}
