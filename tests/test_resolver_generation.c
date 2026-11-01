#include "test_framework.h"
#include "graph_buffer/graph_buffer.h"
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include "store/store_resolver.h"
#include "store/store_resolver_evidence.h"
#include "foundation/log.h"
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

static const char generation_source[] = "class Widget:\n"
                                        "    def run(self): pass\n"
                                        "left: Widget\n"
                                        "right: Widget\n"
                                        "if choose_left:\n"
                                        "    value = left\n"
                                        "else:\n"
                                        "    value = right\n"
                                        "value.run()\n"
                                        "value.missing()\n";

typedef struct {
    CBMArena arena;
    cbm_resolve_proof_pool_t pool;
    CBMFileResult *parsed;
    cbm_resolve_file_t file;
    char extracted[65];
    cbm_resolve_proof_source_t source;
    const cbm_resolve_proof_t *left, *right, *joined;
    cbm_resolve_record_t rows[2];
} generation_fixture_t;

static cbm_resolve_result_t generation_type(void) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                           CBM_RESOLVE_FACT_TYPE_BOUND,
                                  .target = "proj.flow.Widget",
                                  .candidate_count = 1,
                                  .reason = "explicit_annotation"};
}

static const cbm_resolve_proof_t *generation_annotation(generation_fixture_t *f,
                                                        const char *binding) {
    const char *at = strstr(generation_source, binding);
    if (!at)
        return NULL;
    cbm_resolve_proof_key_t key = {.binding_identity = binding,
                                   .type_identity = "proj.flow.Widget",
                                   .detail = "explicit source annotation",
                                   .start_byte = (uint32_t)(at - generation_source),
                                   .end_byte =
                                       (uint32_t)(at - generation_source + strlen(binding))};
    cbm_resolve_result_t type = generation_type();
    return cbm_resolve_proof_leaf(&f->pool, CBM_PROOF_SOURCE_FACT, &f->source, &key, &type);
}

/* These are deliberately constructed facts for the fixed source above, not
 * claims that an arbitrary adapter proved them. The test exercises generation
 * ownership and persistence independently of a language engine's coverage. */
static bool generation_fixture_init(generation_fixture_t *f) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init(&f->arena);
    cbm_resolve_proof_pool_init(&f->pool, &f->arena, 0, 0, 0);
    f->parsed = cbm_extract_file(generation_source, (int)strlen(generation_source), CBM_LANG_PYTHON,
                                 "proj", "flow.py", 0, NULL, NULL);
    if (!f->parsed ||
        !cbm_resolve_snapshot(f->parsed, "flow.py", CBM_LANG_PYTHON, &f->file, f->extracted))
        return false;
    f->source = cbm_result_proof_source(f->parsed, CBM_SOURCE_ORIGIN_RAW, "proj.flow");
    f->left = generation_annotation(f, "left: Widget");
    f->right = generation_annotation(f, "right: Widget");
    cbm_resolve_result_t type = generation_type();
    cbm_resolve_proof_key_t join_key = {.binding_identity = "proj.flow.value",
                                        .type_identity = type.target,
                                        .detail = "both branches assign an annotated Widget"};
    const cbm_resolve_proof_t *arms[] = {f->right, f->left};
    f->joined = cbm_resolve_proof_derive(&f->pool, CBM_PROOF_TYPE_JOIN, &f->source, &join_key,
                                         &type, arms, 2, true);
    cbm_resolve_result_t member = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = "proj.flow.Widget.run",
        .candidate_count = 1,
        .reason = "exact_receiver_member"};
    cbm_resolve_proof_key_t declaration_key = {.declaration_identity = member.target,
                                               .detail = "Widget.run declaration"};
    cbm_resolve_result_t declaration = member;
    declaration.rule = CBM_RESOLVE_RULE_NONE;
    declaration.facts = CBM_RESOLVE_FACT_EXACT_TARGET;
    const cbm_resolve_proof_t *decl = cbm_resolve_proof_leaf(
        &f->pool, CBM_PROOF_REGISTERED_DECLARATION, &f->source, &declaration_key, &declaration);
    cbm_resolve_proof_key_t domain_key = {
        .lookup_domain = "proj.flow.Widget",
        .lookup_key = "declared members",
        .detail = "run; no bases, dynamic lookup hooks, or member writes"};
    cbm_resolve_result_t domain = {.domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                   .facts = CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                                   .reason = "closed_declared_member_domain"};
    const cbm_resolve_proof_t *audit = cbm_resolve_proof_leaf(&f->pool, CBM_PROOF_ADAPTER_DOMAIN,
                                                              &f->source, &domain_key, &domain);
    const cbm_resolve_proof_t *positive_inputs[] = {f->joined, decl, audit};
    const cbm_resolve_proof_t *negative_inputs[] = {f->joined, audit};
    const char *names[] = {"run", "missing"};
    for (size_t i = 0; i < 2; i++) {
        const char *site = strstr(generation_source, i ? "value.missing" : "value.run") + 6;
        uint32_t start = (uint32_t)(site - generation_source);
        cbm_resolve_proof_key_t key = {.lookup_domain = "proj.flow.Widget",
                                       .lookup_key = names[i],
                                       .lookup_closed = true,
                                       .declaration_identity = i ? NULL : member.target,
                                       .binding_identity = i ? NULL : member.target,
                                       .detail = "CALLS",
                                       .origin = CBM_SOURCE_ORIGIN_RAW,
                                       .start_byte = start,
                                       .end_byte = start + (uint32_t)strlen(names[i])};
        cbm_resolve_result_t result = member;
        if (i) {
            result.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
            result.facts = CBM_RESOLVE_FACT_LOOKUP_COMPLETE | CBM_RESOLVE_FACT_RECEIVER_TYPED |
                           CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
            result.target = NULL;
            result.candidate_count = 0;
            result.reason = "closed_member_absence";
        }
        result.proof =
            cbm_resolve_proof_derive(&f->pool, CBM_PROOF_REGISTRY_LOOKUP, &f->source, &key, &result,
                                     i ? negative_inputs : positive_inputs, i ? 2 : 3, true);
        f->rows[i] =
            (cbm_resolve_record_t){.file_path = "flow.py",
                                   .scope = "proj.flow",
                                   .relation = "CALLS",
                                   .spelling = names[i],
                                   .start_byte = key.start_byte,
                                   .end_byte = key.end_byte,
                                   .content_identity = f->file.raw_content_identity,
                                   .configuration_identity = f->file.configuration_identity,
                                   .result = result,
                                   .emitted = i == 0};
        if (!result.proof)
            return false;
    }
    cbm_resolve_proof_key_t site_key = {.lookup_domain = f->rows[0].scope,
                                        .lookup_key = f->rows[0].spelling,
                                        .binding_identity = f->rows[0].result.target,
                                        .detail = "CALLS",
                                        .origin = CBM_SOURCE_ORIGIN_RAW,
                                        .start_byte = f->rows[0].start_byte,
                                        .end_byte = f->rows[0].end_byte};
    cbm_resolve_result_t site_fact = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT};
    const cbm_resolve_proof_t *site =
        cbm_resolve_proof_leaf(&f->pool, CBM_PROOF_SOURCE_FACT, &f->source, &site_key, &site_fact);
    const cbm_resolve_proof_t *site_inputs[] = {f->rows[0].result.proof, site};
    /* The synthetic local attestation explicitly connects the actual run
     * occurrence above to its proved receiver/member, scope and CALLS role. */
    f->rows[0].binding_proof =
        cbm_resolve_proof_derive(&f->pool, CBM_PROOF_NAMED_JOIN, &f->source, &site_key,
                                 &f->rows[0].result, site_inputs, 2, true);
    return f->left && f->right && f->joined && f->rows[0].binding_proof;
}

static void generation_fixture_destroy(generation_fixture_t *f) {
    cbm_free_result(f->parsed);
    cbm_arena_destroy(&f->arena);
}

static cbm_gbuf_t *generation_graph(const generation_fixture_t *f, bool reverse) {
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    if (!gb)
        return NULL;
    bool ok = cbm_gbuf_record_resolver_file(gb, &f->file) && cbm_gbuf_resolution_begin(gb);
    for (size_t i = 0; ok && i < 2; i++) {
        cbm_resolve_record_t row = f->rows[reverse ? 1 - i : i];
        row.proof_borrowed = true;
        ok = cbm_gbuf_record_resolution(gb, &row);
    }
    cbm_gbuf_resolution_end(gb);
    ok = ok && cbm_gbuf_resolutions_own_shared(gb) && cbm_gbuf_resolutions_complete(gb);
    if (!ok) {
        cbm_gbuf_free(gb);
        return NULL;
    }
    return gb;
}

static int generation_store(cbm_store_t *store, const cbm_gbuf_t *gb) {
    size_t nr = 0, nf = 0, ns = 0;
    const cbm_resolve_record_t *rows = cbm_gbuf_resolutions(gb, &nr);
    const cbm_resolve_file_t *files = cbm_gbuf_resolver_files(gb, &nf);
    const cbm_resolve_summary_t *summaries = cbm_gbuf_resolver_summaries(gb, &ns);
    return cbm_store_resolver_replace_generation(store, "proj", rows, nr, files, nf, summaries, ns);
}

static sqlite3_int64 generation_scalar(sqlite3 *db, const char *sql, int value) {
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 result = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_bind_parameter_count(stmt))
            sqlite3_bind_int(stmt, 1, value);
        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return result;
}

static bool generation_text(sqlite3 *db, const char *sql, char output[65]) {
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK;
    ok = ok && sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_bytes(stmt, 0) == 64;
    if (ok) {
        memcpy(output, sqlite3_column_text(stmt, 0), 64);
        output[64] = '\0';
    }
    sqlite3_finalize(stmt);
    return ok;
}

static bool generation_query_hash(sqlite3 *db, const char *sql, char output[65]) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
        return false;
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    int step;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        cbm_resolve_hash_number(&hash, (uint64_t)sqlite3_column_count(stmt));
        for (int i = 0; i < sqlite3_column_count(stmt); i++) {
            cbm_resolve_hash_number(&hash, (uint64_t)sqlite3_column_type(stmt, i));
            cbm_resolve_hash_text(&hash, (const char *)sqlite3_column_text(stmt, i));
        }
    }
    sqlite3_finalize(stmt);
    cbm_resolve_hash_finish(&hash, output);
    return step == SQLITE_DONE;
}

static bool generation_database_hash(sqlite3 *db, char output[65]) {
    const char *queries[] = {
        "SELECT * FROM resolver_generations ORDER BY project",
        "SELECT * FROM resolver_occurrences ORDER BY project,ordinal",
        "SELECT * FROM resolver_facts ORDER BY project,id",
        "SELECT * FROM resolver_supports ORDER BY project,fact,ordinal",
        "SELECT * FROM resolver_lookups ORDER BY project,fact",
        "SELECT * FROM resolver_occurrence_evidence ORDER BY project,ordinal",
        "SELECT * FROM resolver_file_summaries ORDER BY project,file_path",
        "SELECT * FROM resolver_derived_bindings ORDER BY project,file_path,key,kind"};
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++) {
        char part[65];
        if (!generation_query_hash(db, queries[i], part))
            return false;
        cbm_resolve_hash_text(&hash, part);
    }
    cbm_resolve_hash_finish(&hash, output);
    return true;
}

TEST(resolver_generation_persists_independent_facts_and_all_join_supports) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_gbuf_t *gb = generation_graph(&f, false);
    ASSERT_NOT_NULL(gb);
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
    cbm_resolve_counts_t counts;
    bool present = false;
    ASSERT_EQ(cbm_store_resolver_summary(store, "proj", &counts, &present), CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(counts.occurrences, 2);
    ASSERT_EQ(counts.emitted, 1);
    ASSERT_EQ(counts.unresolved, 1);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_facts WHERE "
                                "instr(payload,'left: Widget') OR instr(payload,'right: Widget')",
                                0),
              2);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_supports s JOIN resolver_facts f "
                                "ON f.project=s.project AND f.id=s.fact WHERE f.kind=?",
                                CBM_PROOF_TYPE_JOIN),
              2);
    ASSERT_EQ(generation_scalar(
                  db,
                  "SELECT count(DISTINCT s.premise) FROM resolver_supports s "
                  "JOIN resolver_facts f ON f.project=s.project AND f.id=s.fact WHERE f.kind=?",
                  CBM_PROOF_TYPE_JOIN),
              2);
    ASSERT_EQ(generation_scalar(
                  db,
                  "SELECT count(*) FROM resolver_supports s LEFT JOIN "
                  "resolver_facts f ON f.project=s.project AND f.id=s.premise WHERE f.id IS NULL",
                  0),
              0);
    ASSERT_EQ(
        generation_scalar(db, "SELECT count(*) FROM resolver_supports WHERE premise>=fact", 0), 0);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_lookups l JOIN resolver_facts f "
                                "ON f.project=l.project AND f.id=l.fact WHERE f.kind=? AND "
                                "l.key='run' AND l.status='positive'",
                                CBM_PROOF_REGISTRY_LOOKUP),
              1);
    ASSERT_EQ(generation_scalar(
                  db,
                  "SELECT count(*) FROM resolver_lookups l JOIN resolver_facts f "
                  "ON f.project=l.project AND f.id=l.fact WHERE f.kind=? AND l.key='missing' "
                  "AND l.status='closed_negative'",
                  CBM_PROOF_REGISTRY_LOOKUP),
              1);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_occurrence_evidence "
                                "WHERE proof>0 AND source_binding_proof>0",
                                0),
              1);
    ASSERT_EQ(
        generation_scalar(db,
                          "WITH RECURSIVE reachable(id) AS (SELECT proof FROM "
                          "resolver_occurrence_evidence WHERE ordinal=0 UNION SELECT s.premise "
                          "FROM resolver_supports s "
                          "JOIN reachable r ON s.fact=r.id WHERE s.project='proj') SELECT count(*) "
                          "FROM resolver_facts f "
                          "JOIN reachable r ON f.id=r.id WHERE instr(f.payload,'left: Widget') "
                          "OR instr(f.payload,'right: Widget')",
                          0),
        2);
    char stored[65];
    ASSERT(generation_text(db, "SELECT raw_content_identity FROM resolver_file_summaries", stored));
    ASSERT_STR_EQ(stored, f.file.raw_content_identity);
    ASSERT(generation_text(db,
                           "SELECT configuration_identity FROM resolver_occurrence_evidence "
                           "WHERE ordinal=0",
                           stored));
    ASSERT_STR_EQ(stored, f.file.configuration_identity);
    cbm_store_close(store);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_ids_ignore_occurrence_insertion_order) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_gbuf_t *forward = generation_graph(&f, false);
    cbm_gbuf_t *reverse = generation_graph(&f, true);
    ASSERT_NOT_NULL(forward);
    ASSERT_NOT_NULL(reverse);
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    const char *queries[] = {
        "SELECT id,kind,source_file,digest,payload FROM resolver_facts ORDER BY id",
        "SELECT fact,ordinal,premise FROM resolver_supports ORDER BY fact,ordinal",
        "SELECT fact,domain,key,status FROM resolver_lookups ORDER BY fact",
        "SELECT o.spelling,e.proof,e.source_binding_proof FROM resolver_occurrences o JOIN "
        "resolver_occurrence_evidence e ON e.project=o.project AND e.ordinal=o.ordinal ORDER BY "
        "o.spelling"};
    char first[4][65], second[65];
    ASSERT_EQ(generation_store(store, forward), CBM_STORE_OK);
    for (size_t i = 0; i < 4; i++)
        ASSERT(generation_query_hash(cbm_store_get_db(store), queries[i], first[i]));
    ASSERT_EQ(generation_store(store, reverse), CBM_STORE_OK);
    for (size_t i = 0; i < 4; i++) {
        ASSERT(generation_query_hash(cbm_store_get_db(store), queries[i], second));
        ASSERT_STR_EQ(first[i], second);
    }
    cbm_store_close(store);
    cbm_gbuf_free(forward);
    cbm_gbuf_free(reverse);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_source_mismatch_rolls_back_every_metadata_table) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_gbuf_t *gb = generation_graph(&f, false);
    ASSERT_NOT_NULL(gb);
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
    char before[65], after[65];
    ASSERT(generation_database_hash(cbm_store_get_db(store), before));
    char changed[sizeof(generation_source)];
    memcpy(changed, generation_source, sizeof(changed));
    memcpy(strstr(changed, "def run") + 4, "fun", 3);
    CBMFileResult *parsed = cbm_extract_file(changed, (int)strlen(changed), CBM_LANG_PYTHON, "proj",
                                             "flow.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    cbm_resolve_file_t file;
    char extracted[65];
    ASSERT(cbm_resolve_snapshot(parsed, "flow.py", CBM_LANG_PYTHON, &file, extracted));
    ASSERT_EQ(file.raw_length, f.file.raw_length);
    ASSERT_STR_NEQ(file.raw_content_identity, f.file.raw_content_identity);
    size_t nr = 0;
    const cbm_resolve_record_t *rows = cbm_gbuf_resolutions(gb, &nr);
    ASSERT_EQ(cbm_store_resolver_replace_generation(store, "proj", rows, nr, &file, 1, NULL, 0),
              CBM_STORE_ERR);
    ASSERT(generation_database_hash(cbm_store_get_db(store), after));
    ASSERT_STR_EQ(before, after);
    file = f.file;
    file.configuration_identity = "different-parser-configuration";
    ASSERT_EQ(cbm_store_resolver_replace_generation(store, "proj", rows, nr, &file, 1, NULL, 0),
              CBM_STORE_ERR);
    ASSERT(generation_database_hash(cbm_store_get_db(store), after));
    ASSERT_STR_EQ(before, after);
    cbm_free_result(parsed);
    cbm_store_close(store);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_rejects_unsealed_or_stale_occurrence_proofs) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_gbuf_t *gb = generation_graph(&f, false);
    ASSERT_NOT_NULL(gb);
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
    size_t count = 0;
    const cbm_resolve_record_t *rows = cbm_gbuf_resolutions(gb, &count);
    ASSERT_EQ(count, 2);
    ASSERT(cbm_resolve_record_admit(&rows[0]));
    char before[65], after[65];
    ASSERT(generation_database_hash(cbm_store_get_db(store), before));
    for (int mutation = 0; mutation < 11; mutation++) {
        cbm_resolve_record_t bad = rows[0];
        switch (mutation) {
        case 0:
            bad.result.proof = NULL;
            break;
        case 1:
            bad.result.proof = f.rows[0].result.proof;
            break;
        case 2:
            bad.start_byte++;
            break;
        case 3:
            bad.scope = "proj.some_other_scope";
            break;
        case 4:
            bad.spelling = "missing";
            break;
        case 5:
            bad.relation = "CALL_REFERENCE";
            break;
        case 6:
            bad.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            break;
        case 7:
            bad.content_identity = "same-target-different-source";
            break;
        case 8:
            bad.result.rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
            bad.result.facts |= CBM_RESOLVE_FACT_SCOPE_BOUND;
            break;
        case 9:
            bad.result.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
            break;
        case 10:
            bad.configuration_identity = "same-source-different-parser";
            break;
        }
        ASSERT_FALSE(cbm_resolve_record_admit(&bad));
        ASSERT_EQ(
            cbm_store_resolver_replace_generation(store, "proj", &bad, 1, &f.file, 1, NULL, 0),
            CBM_STORE_ERR);
        ASSERT(generation_database_hash(cbm_store_get_db(store), after));
        ASSERT_STR_EQ(before, after);
    }
    cbm_store_close(store);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_coordinator_rejects_contradictory_lookup_roots) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    const cbm_resolve_proof_view_t *original = cbm_resolve_proof_view(f.rows[0].result.proof);
    ASSERT_NOT_NULL(original);
    for (int mutation = 0; mutation < 4; mutation++) {
        cbm_resolve_result_t contradicted = original->outcome;
        switch (mutation) {
        case 0:
            contradicted.target = "proj.flow.Other.run";
            break;
        case 1:
            contradicted.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
            break;
        case 2:
            contradicted.domain = CBM_RESOLVE_DOMAIN_EXTERNAL;
            break;
        case 3:
            contradicted.candidate_count = 2;
            break;
        }
        /* Keeping complete fact bits makes this a check of contradictory
         * semantic outcomes, not merely the existing missing-facts gate. */
        cbm_resolve_record_t row = f.rows[0];
        row.result.proof = cbm_resolve_proof_derive(
            &f.pool, CBM_PROOF_REGISTRY_LOOKUP, &original->source, &original->key, &contradicted,
            original->premises, original->premise_count, true);
        ASSERT_NOT_NULL(row.result.proof);
        cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
        ASSERT_NOT_NULL(gb);
        ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
        ASSERT(cbm_gbuf_record_resolution(gb, &row));
        ASSERT(cbm_gbuf_resolutions_own_shared(gb));
        size_t count = 0;
        const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
        ASSERT_EQ(count, 1);
        ASSERT_FALSE(sealed[0].emitted);
        ASSERT_FALSE(cbm_resolve_record_admit(&sealed[0]));
        ASSERT_EQ(sealed[0].result.facts, 0);
        cbm_gbuf_free(gb);
    }
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_unknown_local_premise_revokes_emission) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_resolve_result_t unknown = {.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                    .reason = "local_binding_not_established"};
    cbm_resolve_proof_key_t key = {.lookup_key = f.rows[0].spelling,
                                   .start_byte = f.rows[0].start_byte,
                                   .end_byte = f.rows[0].end_byte,
                                   .detail = "incomplete local occurrence binding"};
    cbm_resolve_record_t row = f.rows[0];
    row.binding_proof =
        cbm_resolve_proof_leaf(&f.pool, CBM_PROOF_SOURCE_FACT, &f.source, &key, &unknown);
    ASSERT_NOT_NULL(row.binding_proof);
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(gb);
    ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
    ASSERT(cbm_gbuf_record_resolution(gb, &row));
    ASSERT(cbm_gbuf_resolutions_own_shared(gb));
    size_t count = 0;
    const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
    ASSERT_EQ(count, 1);
    ASSERT_FALSE(sealed[0].emitted);
    ASSERT_FALSE(cbm_resolve_record_admit(&sealed[0]));
    ASSERT_EQ(sealed[0].result.analysis, CBM_RESOLVE_ANALYSIS_UNKNOWN);
    ASSERT_EQ(sealed[0].result.facts, 0);
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(sealed[0].result.proof);
    ASSERT_NOT_NULL(proof);
    ASSERT_EQ(proof->premise_count, 2);
    ASSERT_EQ(proof->outcome.analysis, CBM_RESOLVE_ANALYSIS_UNKNOWN);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_coordinator_rejects_local_site_contradictions) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    const cbm_resolve_proof_view_t *original = cbm_resolve_proof_view(f.rows[0].binding_proof);
    ASSERT_NOT_NULL(original);
    for (int mutation = 0; mutation < 13; mutation++) {
        cbm_resolve_proof_source_t source = original->source;
        cbm_resolve_proof_key_t key = original->key;
        cbm_resolve_result_t outcome = original->outcome;
        switch (mutation) {
        case 0:
            source.file = "other.py";
            break;
        case 1:
            source.content_identity = "other-content";
            break;
        case 2:
            source.configuration_identity = "other-configuration";
            break;
        case 3:
            source.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            break;
        case 4:
            key.start_byte++;
            break;
        case 5:
            key.end_byte++;
            break;
        case 6:
            key.origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            break;
        case 7:
            key.lookup_key = "missing";
            break;
        case 8:
            source.project = "other-project";
            break;
        case 9:
            source.scope = "proj.flow.other";
            break;
        case 10:
            key.detail = "CALL_REFERENCE";
            break;
        case 11:
            key.binding_identity = "proj.flow.Other.run";
            break;
        case 12:
            outcome.target = "proj.flow.Other.run";
            break;
        }
        cbm_resolve_record_t row = f.rows[0];
        row.binding_proof =
            cbm_resolve_proof_derive(&f.pool, CBM_PROOF_NAMED_JOIN, &source, &key, &outcome,
                                     original->premises, original->premise_count, true);
        ASSERT_NOT_NULL(row.binding_proof);
        cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
        ASSERT_NOT_NULL(gb);
        ASSERT(cbm_gbuf_record_resolution(gb, &row));
        ASSERT(cbm_gbuf_resolutions_own_shared(gb));
        size_t count = 0;
        const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
        ASSERT_EQ(count, 1);
        ASSERT_FALSE(sealed[0].emitted);
        ASSERT_FALSE(cbm_resolve_record_admit(&sealed[0]));
        ASSERT_EQ(sealed[0].result.facts, 0);
        cbm_gbuf_free(gb);
    }
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_local_site_requires_exact_scope_or_complete_producer) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    const cbm_resolve_proof_view_t *original = cbm_resolve_proof_view(f.rows[0].binding_proof);
    ASSERT_NOT_NULL(original);
    cbm_resolve_proof_source_t wrong_scope = original->source;
    wrong_scope.scope = "unrelated-module-context";
    const cbm_resolve_proof_t *module_binding = cbm_resolve_proof_derive(
        &f.pool, CBM_PROOF_NAMED_JOIN, &wrong_scope, &original->key, &original->outcome,
        original->premises, original->premise_count, true);
    ASSERT_NOT_NULL(module_binding);
    /* Exact local authority and an independently exact hybrid occurrence are
     * both valid. A supplied contradictory local scope is never optional. */
    for (int example = 0; example < 3; example++) {
        cbm_resolve_record_t row = f.rows[0];
        row.binding_proof = example == 0   ? f.rows[0].binding_proof
                            : example == 1 ? NULL
                                           : module_binding;
        cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
        ASSERT_NOT_NULL(gb);
        ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
        ASSERT(cbm_gbuf_record_resolution(gb, &row));
        ASSERT(cbm_gbuf_resolutions_own_shared(gb));
        size_t count = 0;
        const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(gb, &count);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(sealed[0].emitted, example != 2);
        ASSERT_EQ(cbm_resolve_record_admit(&sealed[0]), example != 2);
        const cbm_resolve_proof_t *owned = sealed[0].result.proof;
        if (example != 2) {
            ASSERT(cbm_gbuf_resolutions_own_shared(gb));
            sealed = cbm_gbuf_resolutions(gb, &count);
            ASSERT(sealed[0].result.proof == owned);
            ASSERT(cbm_resolve_record_admit(&sealed[0]));
        }
        cbm_gbuf_free(gb);
    }
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_file_without_occurrences_retains_snapshot) {
    const char *sources[] = {"class First:\n    pass\n", "class Other:\n    pass\n"};
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    char extracted[2][65], derived[2][65];
    for (size_t i = 0; i < 2; i++) {
        CBMFileResult *parsed =
            cbm_extract_file(sources[i], (int)strlen(sources[i]), CBM_LANG_PYTHON, "proj",
                             "empty.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(parsed);
        cbm_resolve_file_t file;
        char digest[65];
        ASSERT(cbm_resolve_snapshot(parsed, "empty.py", CBM_LANG_PYTHON, &file, digest));
        ASSERT_EQ(cbm_store_resolver_replace_generation(store, "proj", NULL, 0, &file, 1, NULL, 0),
                  CBM_STORE_OK);
        ASSERT_EQ(generation_scalar(cbm_store_get_db(store),
                                    "SELECT count(*) FROM resolver_occurrences", 0),
                  0);
        ASSERT_EQ(generation_scalar(cbm_store_get_db(store),
                                    "SELECT count(*) FROM resolver_file_summaries WHERE "
                                    "file_path='empty.py' AND complete=1",
                                    0),
                  1);
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT extracted_summary FROM resolver_file_summaries",
                               extracted[i]));
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT derived_summary FROM resolver_file_summaries", derived[i]));
        ASSERT_STR_EQ(extracted[i], digest);
        cbm_free_result(parsed);
    }
    ASSERT_STR_NEQ(extracted[0], extracted[1]);
    ASSERT_STR_EQ(derived[0], derived[1]);
    cbm_store_close(store);
    PASS();
}

TEST(resolver_generation_type_support_changes_only_derived_summary) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    char extracted[2][65], derived[2][65];
    const cbm_resolve_proof_t *inputs[] = {f.left, f.right};
    for (size_t i = 0; i < 2; i++) {
        cbm_resolve_result_t type = generation_type();
        cbm_resolve_proof_key_t key = {.binding_identity = "proj.flow.observed",
                                       .type_identity = type.target,
                                       .detail = "copy selected annotated binding"};
        const cbm_resolve_proof_t *proof = cbm_resolve_proof_derive(
            &f.pool, CBM_PROOF_TYPE_COPY, &f.source, &key, &type, &inputs[i], 1, true);
        ASSERT_NOT_NULL(proof);
        cbm_resolve_record_t observation = {.file_path = "flow.py",
                                            .scope = "proj.flow",
                                            .relation = "USES_TYPE",
                                            .spelling = "observed",
                                            .content_identity = f.file.raw_content_identity,
                                            .configuration_identity = f.file.configuration_identity,
                                            .result = type};
        observation.result.proof = proof;
        cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
        ASSERT_NOT_NULL(gb);
        ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
        ASSERT(cbm_gbuf_record_resolution(gb, &observation));
        ASSERT(cbm_gbuf_resolutions_own_shared(gb));
        ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT extracted_summary FROM resolver_file_summaries",
                               extracted[i]));
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT derived_summary FROM resolver_file_summaries", derived[i]));
        cbm_gbuf_free(gb);
    }
    ASSERT_STR_EQ(extracted[0], extracted[1]);
    ASSERT_STR_NEQ(derived[0], derived[1]);
    cbm_store_close(store);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_derived_export_state_is_independent_of_extraction) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    char extracted[2][65], derived[2][65];
    for (size_t i = 0; i < 2; i++) {
        cbm_resolve_result_t result = generation_type();
        cbm_resolve_proof_key_t key = {.lookup_domain = "proj.flow.exports",
                                       .lookup_key = "Widget",
                                       .lookup_closed = i == 0,
                                       .detail = "export lookup audit"};
        cbm_resolve_result_t audit = {
            .analysis = i ? CBM_RESOLVE_ANALYSIS_UNKNOWN : CBM_RESOLVE_ANALYSIS_COMPLETE,
            .reason = i ? "export_domain_not_closed" : "export_domain_closed"};
        const cbm_resolve_proof_t *rule =
            cbm_resolve_proof_leaf(&f.pool, CBM_PROOF_ADAPTER_DOMAIN, &f.source, &key, &audit);
        const cbm_resolve_proof_t *inputs[] = {rule, f.joined};
        const cbm_resolve_proof_t *proof = cbm_resolve_proof_derive(
            &f.pool, CBM_PROOF_EXPORT_BINDING, &f.source, &key, &result, inputs, 2, true);
        ASSERT_NOT_NULL(proof);
        result = proof->value.outcome;
        result.proof = proof;
        cbm_resolve_summary_t summary = {.file_path = "flow.py",
                                         .key = "Widget",
                                         .kind = CBM_PROOF_EXPORT_BINDING,
                                         .closed = i == 0,
                                         .result = result,
                                         .proof_borrowed = true};
        cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
        ASSERT_NOT_NULL(gb);
        ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
        ASSERT(cbm_gbuf_record_resolver_summary(gb, &summary));
        ASSERT(cbm_gbuf_resolutions_own_shared(gb));
        ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
        ASSERT_EQ(
            generation_scalar(
                cbm_store_get_db(store),
                "SELECT count(*) FROM resolver_derived_bindings WHERE key='Widget' AND proof>0", 0),
            1);
        ASSERT_EQ(generation_scalar(
                      cbm_store_get_db(store),
                      "SELECT analysis FROM resolver_derived_bindings WHERE key='Widget'", 0),
                  i ? CBM_RESOLVE_ANALYSIS_UNKNOWN : CBM_RESOLVE_ANALYSIS_COMPLETE);
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT extracted_summary FROM resolver_file_summaries",
                               extracted[i]));
        ASSERT(generation_text(cbm_store_get_db(store),
                               "SELECT derived_summary FROM resolver_file_summaries", derived[i]));
        cbm_gbuf_free(gb);
    }
    ASSERT_STR_EQ(extracted[0], extracted[1]);
    ASSERT_STR_NEQ(derived[0], derived[1]);
    cbm_store_close(store);
    generation_fixture_destroy(&f);
    PASS();
}

TEST(resolver_generation_snapshot_partial_extraction_revokes_completeness) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    ASSERT(f.file.complete);
    f.parsed->parse_incomplete = true;
    cbm_resolve_file_t incomplete;
    char digest[65];
    ASSERT(cbm_resolve_snapshot(f.parsed, "flow.py", CBM_LANG_PYTHON, &incomplete, digest));
    ASSERT_FALSE(incomplete.complete);
    ASSERT_STR_NEQ(f.extracted, digest);
    ASSERT_STR_EQ(f.file.raw_content_identity, incomplete.raw_content_identity);
    generation_fixture_destroy(&f);
    PASS();
}

static bool generation_corruption_is_unavailable(const char *sql) {
    generation_fixture_t f;
    if (!generation_fixture_init(&f))
        return false;
    cbm_gbuf_t *gb = generation_graph(&f, false);
    cbm_store_t *store = cbm_store_open_memory();
    size_t count = 0;
    const cbm_resolve_record_t *rows = gb ? cbm_gbuf_resolutions(gb, &count) : NULL;
    /* A persisted unresolved summary is still required generation metadata. */
    cbm_resolve_summary_t summary = {
        .file_path = "flow.py", .key = "unresolved-export", .kind = CBM_PROOF_EXPORT_BINDING};
    cbm_resolve_counts_t counts;
    bool present = false;
    bool ok = gb && store &&
              cbm_store_resolver_replace_generation(store, "proj", rows, count, &f.file, 1,
                                                    &summary, 1) == CBM_STORE_OK &&
              cbm_store_resolver_summary(store, "proj", &counts, &present) == CBM_STORE_OK &&
              present && counts.occurrences == 2;
    if (ok)
        ok = sqlite3_exec(cbm_store_get_db(store), sql, NULL, NULL, NULL) == SQLITE_OK &&
             cbm_store_resolver_summary(store, "proj", &counts, &present) == CBM_STORE_OK &&
             !present && counts.occurrences == 0;
    if (store)
        cbm_store_close(store);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    return ok;
}

/* Corruption now targets what is actually stored. The logical names are views,
 * so losing one of those, or any compact table beneath them, must read as
 * unavailable, exactly as losing a whole table did before. */
TEST(resolver_generation_missing_metadata_tables_are_unavailable) {
    const char *tables[] = {"resolver_projects",       "resolver_text",
                            "resolver_occurrence_rows", "resolver_fact_rows",
                            "resolver_support_rows",   "resolver_file_summaries",
                            "resolver_derived_bindings"};
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "DROP TABLE %s", tables[i]);
        ASSERT(generation_corruption_is_unavailable(sql));
    }
    const char *views[] = {"resolver_occurrences", "resolver_facts", "resolver_supports",
                           "resolver_lookups", "resolver_occurrence_evidence"};
    for (size_t i = 0; i < sizeof(views) / sizeof(views[0]); i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "DROP VIEW %s", views[i]);
        ASSERT(generation_corruption_is_unavailable(sql));
    }
    PASS();
}

TEST(resolver_generation_missing_metadata_rows_are_unavailable) {
    const char *deletes[] = {"DELETE FROM resolver_text WHERE pid=1",
                             "DELETE FROM resolver_occurrence_rows WHERE pid=1",
                             "DELETE FROM resolver_fact_rows WHERE pid=1",
                             "DELETE FROM resolver_support_rows WHERE pid=1",
                             "DELETE FROM resolver_file_summaries WHERE project='proj'",
                             "DELETE FROM resolver_derived_bindings WHERE project='proj'"};
    for (size_t i = 0; i < sizeof(deletes) / sizeof(deletes[0]); i++)
        ASSERT(generation_corruption_is_unavailable(deletes[i]));
    PASS();
}

TEST(resolver_generation_modified_metadata_is_unavailable) {
    ASSERT(generation_corruption_is_unavailable(
        "UPDATE resolver_occurrence_rows SET proof=0 WHERE pid=1"));
    ASSERT(generation_corruption_is_unavailable(
        "UPDATE resolver_text SET value='lost' WHERE pid=1 AND id=1"));
    ASSERT(generation_corruption_is_unavailable(
        "UPDATE resolver_file_summaries SET derived_summary='lost' WHERE project='proj'"));
    ASSERT(generation_corruption_is_unavailable(
        "UPDATE resolver_derived_bindings SET target='wrong' WHERE project='proj'"));
    PASS();
}

/* Everything a row carries must read back byte for byte through the views,
 * including properties in shapes the compact form does not recognise. */
typedef struct {
    const char *relation;
    const char *spelling;
    cbm_resolve_rule_t rule;
    uint32_t start, end, origin;
    const char *properties; /* NULL reads back as the empty object */
    const char *target;
    const char *reason;
} generation_round_trip_t;

TEST(resolver_generation_views_read_back_every_stored_value) {
    static const generation_round_trip_t cases[] = {
        /* What pass_resolver writes for a call. */
        {"CALLS", "run", CBM_RESOLVE_RULE_SCOPE_BINDING, 10, 13, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":1,\"start_byte\":10,\"end_byte\":13,"
         "\"source_origin\":0,\"callee\":\"run\",\"line\":12,\"args\":[{\"i\":0,\"e\":\"x\"}]}",
         "", "binding_not_found"},
        /* The same call elsewhere shares one stored remainder. */
        {"CALLS", "run", CBM_RESOLVE_RULE_SCOPE_BINDING, 90, 93, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":1,\"start_byte\":90,\"end_byte\":93,"
         "\"source_origin\":0,\"callee\":\"run\",\"line\":40,\"args\":[{\"i\":0,\"e\":\"x\"}]}",
         "", "binding_not_found"},
        /* What it writes for a usage: no callee, no line. */
        {"USAGE", "x", CBM_RESOLVE_RULE_NONE, 5, 6, 1,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":5,\"end_byte\":6,"
         "\"source_origin\":1}",
         "proj.mod.x", ""},
        /* A callee spelling that looks like the line key stays inside its string. */
        {"CALLS", "a,\"line\":7", CBM_RESOLVE_RULE_NONE, 1, 2, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":1,\"end_byte\":2,"
         "\"source_origin\":0,\"callee\":\"a,\\\"line\\\":7\",\"line\":12}",
         "", ""},
        /* Decoys: anything but the exact head stays verbatim. */
        {"CALLS", "other_rule", CBM_RESOLVE_RULE_SCOPE_BINDING, 1, 2, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":3,\"start_byte\":1,\"end_byte\":2,"
         "\"source_origin\":0,\"line\":9}",
         "", ""},
        {"CALLS", "other_span", CBM_RESOLVE_RULE_NONE, 1, 2, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":1,\"end_byte\":22,"
         "\"source_origin\":0,\"line\":9}",
         "", ""},
        {"USAGE", "adapter", CBM_RESOLVE_RULE_NONE, 0, 0, 0,
         "{\"strategy\":\"docker\",\"line\":5,\"image\":\"a\"}", "", ""},
        /* A line that does not reproduce from its integer value stays in the text. */
        {"CALLS", "zero", CBM_RESOLVE_RULE_NONE, 3, 4, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":3,\"end_byte\":4,"
         "\"source_origin\":0,\"line\":012}",
         "", ""},
        {"CALLS", "huge", CBM_RESOLVE_RULE_NONE, 3, 4, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":3,\"end_byte\":4,"
         "\"source_origin\":0,\"line\":99999999999999}",
         "", ""},
        /* A raw marker byte cannot be confused with the line marker. */
        {"CALLS", "marker", CBM_RESOLVE_RULE_NONE, 3, 4, 0,
         "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":3,\"end_byte\":4,"
         "\"source_origin\":0,\"callee\":\"\x01\",\"line\":8}",
         "", ""},
        {"USAGE", "none", CBM_RESOLVE_RULE_NONE, 7, 7, 0, NULL, "", ""},
    };
    enum { CASES = sizeof(cases) / sizeof(cases[0]) };
    cbm_resolve_record_t rows[CASES];
    memset(rows, 0, sizeof(rows));
    for (size_t i = 0; i < CASES; i++) {
        rows[i] = (cbm_resolve_record_t){.file_path = "src/deep/path/module.c",
                                         .scope = "proj.src.deep.path.module.owner",
                                         .relation = cases[i].relation,
                                         .spelling = cases[i].spelling,
                                         .properties_json = cases[i].properties,
                                         .start_byte = cases[i].start,
                                         .end_byte = cases[i].end,
                                         .source_origin = cases[i].origin,
                                         .content_identity = "content-identity",
                                         .configuration_identity = "configuration-identity"};
        rows[i].result.rule = cases[i].rule;
        rows[i].result.target = cases[i].target;
        rows[i].result.reason = cases[i].reason;
        rows[i].result.facts = (uint32_t)i;
        rows[i].result.candidate_count = (uint32_t)i + 2;
    }
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_resolver_replace(store, "proj", rows, CASES), CBM_STORE_OK);
    sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *stmt = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(db,
                                 "SELECT project,file_path,scope,relation,spelling,start_byte,"
                                 "end_byte,source_origin,rule,facts,target,candidate_count,reason,"
                                 "emitted,rules_version,properties_json FROM resolver_occurrences "
                                 "ORDER BY ordinal",
                                 -1, &stmt, NULL),
              SQLITE_OK);
    for (size_t i = 0; i < CASES; i++) {
        ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 0), "proj");
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 1), rows[i].file_path);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 2), rows[i].scope);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 3), rows[i].relation);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 4), rows[i].spelling);
        ASSERT_EQ(sqlite3_column_int64(stmt, 5), cases[i].start);
        ASSERT_EQ(sqlite3_column_int64(stmt, 6), cases[i].end);
        ASSERT_EQ(sqlite3_column_int64(stmt, 7), cases[i].origin);
        ASSERT_EQ(sqlite3_column_int(stmt, 8), (int)cases[i].rule);
        ASSERT_EQ(sqlite3_column_int64(stmt, 9), (sqlite3_int64)i);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 10), cases[i].target);
        ASSERT_EQ(sqlite3_column_int64(stmt, 11), (sqlite3_int64)i + 2);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 12), cases[i].reason);
        ASSERT_EQ(sqlite3_column_int(stmt, 13), 0);
        ASSERT_EQ(sqlite3_column_int(stmt, 14), CBM_RESOLVER_RULES_VERSION);
        ASSERT_STR_EQ((const char *)sqlite3_column_text(stmt, 15),
                      cases[i].properties ? cases[i].properties : "{}");
    }
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
    sqlite3_finalize(stmt);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_occurrence_evidence WHERE "
                                "content_identity='content-identity' AND "
                                "configuration_identity='configuration-identity'",
                                0),
              CASES);
    /* The two calls that differ only by position share one stored remainder. */
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_occurrence_rows WHERE line IN "
                                "(12,40) AND spelling=(SELECT id FROM resolver_text WHERE "
                                "value='run')",
                                0),
              2);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(DISTINCT props) FROM resolver_occurrence_rows WHERE "
                                "spelling=(SELECT id FROM resolver_text WHERE value='run')",
                                0),
              1);
    cbm_store_close(store);
    PASS();
}

/* The cost contract, by page count rather than wall time. Every occurrence here
 * repeats long project, file, scope and identity strings. Row storage must not
 * repeat them: bytes per occurrence stay flat and far below the text they name,
 * and the dictionary holds each string once. */
static bool generation_storage_bytes(cbm_resolve_record_t *rows, size_t count, sqlite3_int64 *bytes,
                                     sqlite3_int64 *texts) {
    cbm_store_t *store = cbm_store_open_memory();
    if (!store)
        return false;
    sqlite3 *db = cbm_store_get_db(store);
    const char *project = "a-rather-long-project-name-derived-from-an-absolute-checkout-path";
    bool ok = cbm_store_resolver_replace(store, project, rows, count) == CBM_STORE_OK;
    if (ok) {
        *bytes = (sqlite3_int64)generation_scalar(db, "PRAGMA page_count", 0) *
                 (sqlite3_int64)generation_scalar(db, "PRAGMA page_size", 0);
        *texts = generation_scalar(db, "SELECT count(*) FROM resolver_text", 0);
    }
    cbm_store_close(store);
    return ok;
}

TEST(resolver_generation_storage_does_not_repeat_strings) {
    enum { LARGE = 8192, SMALL = LARGE / 2, PROPERTY_BYTES = 256 };
    cbm_resolve_record_t *rows = calloc(LARGE, sizeof(*rows));
    char *properties = malloc((size_t)LARGE * PROPERTY_BYTES);
    ASSERT_NOT_NULL(rows);
    ASSERT_NOT_NULL(properties);
    const char *content = "8f2d3c6a1b7e49058c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b";
    const char *configuration = "1b2a3f4e5d6c7b8a9f0e1d2c3b4a5f6e7d8c9b0a1f2e3d4c5b6a7f8e9d0c1b2a";
    for (int i = 0; i < LARGE; i++) {
        char *json = properties + (size_t)i * PROPERTY_BYTES;
        uint32_t start = 1000u + (uint32_t)i * 40u, end = start + 12u;
        snprintf(json, PROPERTY_BYTES,
                 "{\"strategy\":\"shared_resolver\",\"resolver_rule\":0,\"start_byte\":%u,"
                 "\"end_byte\":%u,\"source_origin\":0,\"callee\":\"handle_request\",\"line\":%d,"
                 "\"args\":[{\"i\":0,\"e\":\"ctx\"}]}",
                 start, end, i + 1);
        rows[i] = (cbm_resolve_record_t){
            .file_path = "services/gateway/internal/transport/http/handlers/request_router.c",
            .scope = "project.services.gateway.internal.transport.http.handlers.request_router."
                     "dispatch",
            .relation = "CALLS",
            .spelling = "handle_request",
            .properties_json = json,
            .start_byte = start,
            .end_byte = end,
            .content_identity = content,
            .configuration_identity = configuration};
        rows[i].result.reason = "semantic_resolution_required";
        rows[i].result.target = "";
    }
    sqlite3_int64 small_bytes = 0, large_bytes = 0, small_texts = 0, large_texts = 0;
    bool ok = generation_storage_bytes(rows, SMALL, &small_bytes, &small_texts) &&
              generation_storage_bytes(rows, LARGE, &large_bytes, &large_texts);
    free(properties);
    free(rows);
    ASSERT_TRUE(ok);
    /* Each row named over 400 bytes of text before compaction. */
    ASSERT_LT(large_bytes / LARGE, 96);
    ASSERT_LT(large_bytes, small_bytes * 5 / 2);
    ASSERT_EQ(small_texts, large_texts);
    PASS();
}

/* An abstained row's fact is composed when stored. Sealing two identical rows must
 * still give them one fact, as interning did, and a row at another site its own;
 * and the stored fact must cite the row's proof as its premise. */
TEST(resolver_generation_abstained_rows_compose_one_fact_per_site) {
    generation_fixture_t f;
    ASSERT(generation_fixture_init(&f));
    cbm_gbuf_t *gb = cbm_gbuf_new("proj", "/tmp");
    ASSERT_NOT_NULL(gb);
    cbm_resolve_record_t row = f.rows[1];
    ASSERT_FALSE(row.emitted);
    ASSERT_NOT_NULL(row.result.proof);
    ASSERT(cbm_gbuf_record_resolution(gb, &row));
    ASSERT(cbm_gbuf_record_resolution(gb, &row));
    row.start_byte += 100;
    row.end_byte += 100;
    ASSERT(cbm_gbuf_record_resolution(gb, &row));
    ASSERT(cbm_gbuf_record_resolver_file(gb, &f.file));
    ASSERT(cbm_gbuf_resolutions_own_shared(gb));
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(generation_store(store, gb), CBM_STORE_OK);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(generation_scalar(db, "SELECT count(*) FROM resolver_occurrence_evidence", 0), 3);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(DISTINCT proof) FROM resolver_occurrence_evidence "
                                "WHERE proof>0",
                                0),
              2);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_facts WHERE kind=?",
                                CBM_PROOF_OCCURRENCE_BINDING),
              2);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_occurrence_evidence e LEFT JOIN "
                                "resolver_facts f ON f.project=e.project AND f.id=e.proof "
                                "WHERE f.id IS NULL OR f.kind<>?",
                                CBM_PROOF_OCCURRENCE_BINDING),
              0);
    ASSERT_EQ(generation_scalar(db,
                                "SELECT count(*) FROM resolver_occurrence_evidence e WHERE NOT "
                                "EXISTS (SELECT 1 FROM resolver_supports s WHERE s.project="
                                "e.project AND s.fact=e.proof)",
                                0),
              0);
    cbm_store_close(store);
    cbm_gbuf_free(gb);
    generation_fixture_destroy(&f);
    PASS();
}

/* A worker that logs nothing for 15 minutes is killed as hung, and a write that
 * stores hundreds of millions of rows must not look like one. */
static int generation_progress_lines;
static void generation_progress_sink(const char *line) {
    if (strstr(line, "msg=resolver.evidence.progress") && strstr(line, "rows="))
        generation_progress_lines++;
}

TEST(resolver_generation_long_write_reports_progress) {
    enum { ROWS = 64, EVERY = 8 };
    cbm_resolve_record_t rows[ROWS];
    memset(rows, 0, sizeof(rows));
    for (int i = 0; i < ROWS; i++) {
        rows[i].file_path = "src/module.c";
        rows[i].scope = "project.src.module.owner";
        rows[i].relation = "USAGE";
        rows[i].spelling = "name";
        rows[i].start_byte = (uint32_t)i;
        rows[i].end_byte = (uint32_t)i + 1;
    }
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    generation_progress_lines = 0;
    cbm_store_resolver_test_set_progress_rows(EVERY);
    cbm_log_set_sink_ex(generation_progress_sink, CBM_LOG_SINK_REPLACE);
    int rc = cbm_store_resolver_replace(store, "proj", rows, ROWS);
    cbm_log_set_sink(NULL);
    cbm_store_resolver_test_set_progress_rows(0);
    cbm_store_close(store);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_GTE(generation_progress_lines, ROWS / EVERY);
    PASS();
}

/* A private stage must not hold every row twice while publishing: on the largest
 * repositories that second copy decides whether the disk suffices. The seam commits
 * often enough to show it on a small write, and the receipt still comes last. */
TEST(resolver_generation_publish_keeps_the_wal_small) {
    enum { ROWS = 6000 };
    cbm_resolve_record_t *rows = calloc(ROWS, sizeof(*rows));
    ASSERT_NOT_NULL(rows);
    char (*names)[32] = malloc((size_t)ROWS * sizeof(*names));
    ASSERT_NOT_NULL(names);
    for (int i = 0; i < ROWS; i++) {
        snprintf(names[i], sizeof(names[i]), "distinct_name_%d", i);
        rows[i].file_path = "src/module.c";
        rows[i].scope = "project.src.module.owner";
        rows[i].relation = "USAGE";
        rows[i].spelling = names[i];
        rows[i].start_byte = (uint32_t)i;
        rows[i].end_byte = (uint32_t)i + 1;
    }
    char dir[] = "/tmp/cbm_generation_wal_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    char db_path[256], wal_path[256];
    snprintf(db_path, sizeof(db_path), "%s/stage.db", dir);
    snprintf(wal_path, sizeof(wal_path), "%s/stage.db-wal", dir);
    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    /* One page per commit batch is far below the row count's worth of pages. */
    ASSERT_EQ(sqlite3_exec(cbm_store_get_db(store), "PRAGMA wal_autocheckpoint=8", NULL, NULL, NULL),
              SQLITE_OK);
    cbm_store_resolver_test_set_progress_rows(256);
    int rc = cbm_store_resolver_publish_generation(store, "proj", rows, ROWS, NULL, 0, NULL, 0);
    cbm_store_resolver_test_set_progress_rows(0);
    bool present = false;
    int current = cbm_store_resolver_current(store, "proj", &present);
    struct stat wal, db;
    int wal_stat = stat(wal_path, &wal), db_stat = stat(db_path, &db);
    cbm_store_close(store);
    free(names);
    free(rows);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT_EQ(current, CBM_STORE_OK);
    ASSERT_TRUE(present);
    ASSERT_EQ(wal_stat, 0);
    ASSERT_EQ(db_stat, 0);
    ASSERT_GT(db.st_size, 0);
    ASSERT_LT(wal.st_size, db.st_size);
    PASS();
}

TEST(resolver_generation_empty_project_has_complete_metadata) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_resolver_replace_generation(store, "empty", NULL, 0, NULL, 0, NULL, 0),
              CBM_STORE_OK);
    cbm_resolve_counts_t counts;
    bool present = false;
    int rc = cbm_store_resolver_summary(store, "empty", &counts, &present);
    cbm_store_close(store);
    ASSERT_EQ(rc, CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(counts.occurrences, 0);
    PASS();
}

TEST(resolver_generation_missing_guard_cannot_revive_other_project_receipts) {
    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_resolver_replace(store, "first", NULL, 0), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_resolver_replace(store, "second", NULL, 0), CBM_STORE_OK);
    bool present = false;
    ASSERT_EQ(cbm_store_resolver_current(store, "first", &present), CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(sqlite3_exec(cbm_store_get_db(store),
                           "DROP TRIGGER resolver_guard_resolver_fact_rows_DELETE", NULL, NULL,
                           NULL),
              SQLITE_OK);
    ASSERT_EQ(cbm_store_resolver_current(store, "first", &present), CBM_STORE_OK);
    ASSERT_FALSE(present);
    ASSERT_EQ(cbm_store_resolver_current(store, "second", &present), CBM_STORE_OK);
    ASSERT_FALSE(present);
    ASSERT_EQ(cbm_store_resolver_replace(store, "first", NULL, 0), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_resolver_current(store, "first", &present), CBM_STORE_OK);
    ASSERT(present);
    ASSERT_EQ(cbm_store_resolver_current(store, "second", &present), CBM_STORE_OK);
    ASSERT_FALSE(present);
    ASSERT_EQ(cbm_store_resolver_replace(store, "second", NULL, 0), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_resolver_current(store, "second", &present), CBM_STORE_OK);
    ASSERT(present);
    cbm_store_close(store);
    PASS();
}

TEST(resolver_generation_replaced_guard_is_not_trusted) {
    ASSERT(generation_corruption_is_unavailable(
        "DROP TRIGGER resolver_guard_resolver_fact_rows_DELETE;"
        "CREATE TRIGGER resolver_guard_resolver_fact_rows_DELETE AFTER DELETE ON "
        "resolver_fact_rows BEGIN SELECT 1; END"));
    PASS();
}

SUITE(resolver_generation) {
    RUN_TEST(resolver_generation_missing_guard_cannot_revive_other_project_receipts);
    RUN_TEST(resolver_generation_replaced_guard_is_not_trusted);
    RUN_TEST(resolver_generation_missing_metadata_tables_are_unavailable);
    RUN_TEST(resolver_generation_missing_metadata_rows_are_unavailable);
    RUN_TEST(resolver_generation_modified_metadata_is_unavailable);
    RUN_TEST(resolver_generation_empty_project_has_complete_metadata);
    RUN_TEST(resolver_generation_views_read_back_every_stored_value);
    RUN_TEST(resolver_generation_storage_does_not_repeat_strings);
    RUN_TEST(resolver_generation_long_write_reports_progress);
    RUN_TEST(resolver_generation_abstained_rows_compose_one_fact_per_site);
    RUN_TEST(resolver_generation_publish_keeps_the_wal_small);
    RUN_TEST(resolver_generation_persists_independent_facts_and_all_join_supports);
    RUN_TEST(resolver_generation_ids_ignore_occurrence_insertion_order);
    RUN_TEST(resolver_generation_source_mismatch_rolls_back_every_metadata_table);
    RUN_TEST(resolver_generation_rejects_unsealed_or_stale_occurrence_proofs);
    RUN_TEST(resolver_generation_coordinator_rejects_contradictory_lookup_roots);
    RUN_TEST(resolver_generation_unknown_local_premise_revokes_emission);
    RUN_TEST(resolver_generation_coordinator_rejects_local_site_contradictions);
    RUN_TEST(resolver_generation_local_site_requires_exact_scope_or_complete_producer);
    RUN_TEST(resolver_generation_file_without_occurrences_retains_snapshot);
    RUN_TEST(resolver_generation_type_support_changes_only_derived_summary);
    RUN_TEST(resolver_generation_derived_export_state_is_independent_of_extraction);
    RUN_TEST(resolver_generation_snapshot_partial_extraction_revokes_completeness);
}
