/* memory core tests — public API, concurrency, retrieval, and maintenance. */
#include "test_framework.h"
#include "test_helpers.h"

#include "memory/memory.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "store/store.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { MEMORY_TEST_PATH = 1024, MEMORY_TEST_JSON = 32768 };

static bool memory_result_ok(const char *json) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc) {
        return false;
    }
    yyjson_val *value = yyjson_obj_get(yyjson_doc_get_root(doc), "ok");
    bool ok = value && yyjson_is_bool(value) && yyjson_get_bool(value);
    yyjson_doc_free(doc);
    return ok;
}

static bool memory_result_string_is(const char *json, const char *key, const char *expected) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc) {
        return false;
    }
    yyjson_val *value = yyjson_obj_get(yyjson_doc_get_root(doc), key);
    bool equal = value && yyjson_is_str(value) && strcmp(yyjson_get_str(value), expected) == 0;
    yyjson_doc_free(doc);
    return equal;
}

static bool memory_result_copy_string(const char *json, const char *key, char *out,
                                      size_t out_size) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc || !out || out_size == 0) {
        yyjson_doc_free(doc);
        return false;
    }
    yyjson_val *value = yyjson_obj_get(yyjson_doc_get_root(doc), key);
    const char *text = value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
    int written = text ? snprintf(out, out_size, "%s", text) : -1;
    yyjson_doc_free(doc);
    return written >= 0 && (size_t)written < out_size;
}

static int64_t memory_result_int(const char *json, const char *key) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc) {
        return -1;
    }
    yyjson_val *value = yyjson_obj_get(yyjson_doc_get_root(doc), key);
    int64_t result = value && yyjson_is_num(value) ? yyjson_get_sint(value) : -1;
    yyjson_doc_free(doc);
    return result;
}

static int memory_sql_int(cbm_memory_t *memory, const char *sql) {
    sqlite3_stmt *stmt = NULL;
    int value = -1;
    if (sqlite3_prepare_v2(cbm_memory_db(memory), sql, -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW) {
        value = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return value;
}

static char *memory_read_text(const char *path) {
    FILE *file = fopen(path, "rb");
    if (!file) {
        return NULL;
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }
    long size = ftell(file);
    if (size < 0 || fseek(file, 0, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }
    char *body = malloc((size_t)size + 1);
    if (!body) {
        fclose(file);
        return NULL;
    }
    size_t read = fread(body, 1, (size_t)size, file);
    fclose(file);
    if (read != (size_t)size) {
        free(body);
        return NULL;
    }
    body[size] = '\0';
    return body;
}

static char *memory_propose_commit(cbm_memory_t *memory, const char *proposal_id,
                                   const char *operation_id, const char *operations,
                                   const char *expected_revisions) {
    char proposal[MEMORY_TEST_JSON];
    int n = snprintf(proposal, sizeof(proposal),
                     "{\"proposal_id\":\"%s\",\"agent_id\":\"test-agent\","
                     "\"session_id\":\"test-session\",\"expected_revisions\":%s,"
                     "\"operations\":%s}",
                     proposal_id, expected_revisions ? expected_revisions : "{}", operations);
    if (n < 0 || n >= (int)sizeof(proposal)) {
        return NULL;
    }
    char *proposed = cbm_memory_propose_json(memory, proposal);
    if (!memory_result_ok(proposed)) {
        return proposed;
    }
    free(proposed);
    char commit[512];
    n = snprintf(commit, sizeof(commit), "{\"proposal_id\":\"%s\",\"operation_id\":\"%s\"}",
                 proposal_id, operation_id);
    return n >= 0 && n < (int)sizeof(commit) ? cbm_memory_commit_json(memory, commit) : NULL;
}

typedef struct {
    const char *home;
    const char *proposal_json;
    const char *commit_json;
    atomic_int *ready;
    atomic_bool *go;
    char *proposal_result;
    char *commit_result;
} memory_writer_task_t;

static void *memory_writer_thread(void *opaque) {
    memory_writer_task_t *task = opaque;
    cbm_memory_t *memory = cbm_memory_open(task->home);
    if (!memory) {
        atomic_fetch_add_explicit(task->ready, 1, memory_order_release);
        return NULL;
    }
    task->proposal_result = cbm_memory_propose_json(memory, task->proposal_json);
    atomic_fetch_add_explicit(task->ready, 1, memory_order_release);
    while (!atomic_load_explicit(task->go, memory_order_acquire)) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    if (memory_result_ok(task->proposal_result)) {
        task->commit_result = cbm_memory_commit_json(memory, task->commit_json);
    }
    cbm_memory_close(memory);
    return NULL;
}

static int memory_run_two_writers(memory_writer_task_t tasks[2]) {
    cbm_thread_t threads[2];
    if (cbm_thread_create(&threads[0], 0, memory_writer_thread, &tasks[0]) != 0) {
        return -1;
    }
    if (cbm_thread_create(&threads[1], 0, memory_writer_thread, &tasks[1]) != 0) {
        atomic_store_explicit(tasks[0].go, true, memory_order_release);
        cbm_thread_join(&threads[0]);
        return -1;
    }
    while (atomic_load_explicit(tasks[0].ready, memory_order_acquire) < 2) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    atomic_store_explicit(tasks[0].go, true, memory_order_release);
    int first = cbm_thread_join(&threads[0]);
    int second = cbm_thread_join(&threads[1]);
    return first == 0 && second == 0 ? 0 : -1;
}

TEST(memory_open_schema) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_schema");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    ASSERT_STR_EQ(cbm_memory_home(memory), home);
    ASSERT_EQ(cbm_memory_snapshot_epoch(memory), 0);
    sqlite3 *db = cbm_memory_db(memory);
    ASSERT_NOT_NULL(db);
    sqlite3_stmt *stmt = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(db, "PRAGMA user_version;", -1, &stmt, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int(stmt, 0), CBM_MEMORY_SCHEMA_VERSION);
    sqlite3_finalize(stmt);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_ingest_deduplicates) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_ingest");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *args = "{\"content\":\"immutable fact\",\"title\":\"Fact\",\"origin\":\"test\"}";
    char *first = cbm_memory_ingest_json(memory, args);
    char *second = cbm_memory_ingest_json(memory, args);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    ASSERT_NOT_NULL(strstr(first, "\"deduplicated\":false"));
    ASSERT_NOT_NULL(strstr(second, "\"deduplicated\":true"));
    ASSERT_EQ(cbm_memory_snapshot_epoch(memory), 1);
    free(first);
    free(second);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_source_revision_dirties_supported_claim) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_source_revision");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    char *original = cbm_memory_ingest_json(
        memory, "{\"content\":\"API version one\",\"title\":\"API spec v1\"}");
    ASSERT_TRUE(memory_result_ok(original));
    char source_id[128];
    ASSERT_TRUE(memory_result_copy_string(original, "source_id", source_id, sizeof(source_id)));
    free(original);
    char operations[MEMORY_TEST_JSON];
    int n = snprintf(operations, sizeof(operations),
                     "[{\"type\":\"add_claim\",\"claim_id\":\"claim:api-version\","
                     "\"claim_kind\":\"fact\",\"status\":\"active\",\"subject\":\"API\","
                     "\"predicate\":\"version\",\"object\":\"one\",\"source_ids\":[\"%s\"]}]",
                     source_id);
    ASSERT_GTE(n, 0);
    ASSERT_LT(n, (int)sizeof(operations));
    char *claim = memory_propose_commit(memory, "proposal:api-version", "operation:api-version",
                                        operations, "{}");
    ASSERT_TRUE(memory_result_ok(claim));
    free(claim);
    char revision_args[1024];
    n = snprintf(revision_args, sizeof(revision_args),
                 "{\"content\":\"API version two\",\"title\":\"API spec v2\","
                 "\"revision_of\":\"%s\"}",
                 source_id);
    ASSERT_GTE(n, 0);
    ASSERT_LT(n, (int)sizeof(revision_args));
    char *revision = cbm_memory_ingest_json(memory, revision_args);
    ASSERT_TRUE(memory_result_ok(revision));
    free(revision);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_dirty WHERE status='open' AND "
                                     "entity_kind='claim' AND entity_id='claim:api-version' AND "
                                     "reason='source_revision_available';"),
              1);
    char *query =
        cbm_memory_query_json(memory, "{\"query\":\"API version one\",\"entity_kind\":\"claim\"}");
    ASSERT_TRUE(memory_result_ok(query));
    ASSERT_TRUE(memory_result_string_is(query, "route", "verify"));
    ASSERT_NOT_NULL(strstr(query, "source_revision_available"));
    free(query);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_commit_all_epistemic_entities_and_materializes_wiki) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_entities");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);

    const char *operations =
        "[{\"type\":\"upsert_page\",\"page_id\":\"page:core\",\"slug\":\"core\","
        "\"title\":\"Core Memory\",\"page_kind\":\"concept\","
        "\"markdown\":\"# Core Memory\\nCanonical body.\\n\"},"
        "{\"type\":\"add_claim\",\"claim_id\":\"claim:core\",\"claim_kind\":\"fact\","
        "\"status\":\"active\",\"subject\":\"Global Memory\",\"predicate\":\"stores\","
        "\"object\":\"epistemic entities\",\"page_id\":\"page:core\"},"
        "{\"type\":\"add_decision\",\"decision_id\":\"decision:core\","
        "\"title\":\"Storage choice\",\"chosen_option\":\"SQLite WAL\","
        "\"alternatives\":[\"files only\"],\"applicability\":[\"multi-agent\"],"
        "\"exit_criteria\":[\"writer starvation\"]},"
        "{\"type\":\"add_experience\",\"experience_id\":\"experience:core\","
        "\"title\":\"Concurrent commits\",\"context\":{\"store\":\"sqlite\"},"
        "\"observation\":\"CAS prevents overwrite\",\"outcome\":\"conflict surfaced\"},"
        "{\"type\":\"add_preference\",\"preference_id\":\"preference:core\","
        "\"title\":\"Evidence style\",\"value\":\"cite raw sources\","
        "\"scope\":{\"project\":\"all\"},\"context\":{\"task\":\"analysis\"}},"
        "{\"type\":\"add_code_ref\",\"code_ref_id\":\"coderef:core\","
        "\"project\":\"sample\",\"ref_kind\":\"symbol\","
        "\"qualified_name\":\"pkg.core.run\",\"file_path\":\"src/core.c\"},"
        "{\"type\":\"link\",\"source_kind\":\"page\",\"source_id\":\"page:core\","
        "\"target_kind\":\"code_ref\",\"target_id\":\"coderef:core\","
        "\"relation_type\":\"REFERENCES\"}]";
    char *committed =
        memory_propose_commit(memory, "proposal:entities", "operation:entities", operations, "{}");
    ASSERT_NOT_NULL(committed);
    ASSERT_TRUE(memory_result_ok(committed));
    ASSERT_EQ(memory_result_int(committed, "committed_epoch"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_pages;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_claims;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_decisions;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_experiences;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_preferences;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_code_refs;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_relations;"), 3);
    ASSERT_EQ(memory_sql_int(
                  memory, "SELECT count(*) FROM memory_outbox WHERE state='done' AND attempts=1;"),
              1);
    char wiki_path[MEMORY_TEST_PATH];
    snprintf(wiki_path, sizeof(wiki_path), "%s/wiki/concept/core.md", home);
    char *wiki = memory_read_text(wiki_path);
    ASSERT_NOT_NULL(wiki);
    ASSERT_STR_EQ(wiki, "# Core Memory\nCanonical body.\n");
    free(wiki);
    free(committed);

    /* A committed outbox item is recoverable and idempotently rematerialized on open. */
    ASSERT_EQ(cbm_unlink(wiki_path), 0);
    ASSERT_EQ(sqlite3_exec(cbm_memory_db(memory),
                           "UPDATE memory_outbox SET state='pending',lease_owner=NULL,"
                           "lease_until=NULL,processed_at=NULL;",
                           NULL, NULL, NULL),
              SQLITE_OK);
    cbm_memory_close(memory);
    memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    ASSERT_TRUE(cbm_file_exists(wiki_path));
    ASSERT_EQ(memory_sql_int(
                  memory, "SELECT count(*) FROM memory_outbox WHERE state='done' AND attempts=2;"),
              1);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_operation_idempotency_and_key_ownership) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_idempotency");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *one = "[{\"type\":\"add_preference\",\"preference_id\":\"preference:one\","
                      "\"value\":\"deterministic commits\",\"scope\":{\"suite\":\"memory\"}}]";
    char *first = memory_propose_commit(memory, "proposal:one", "operation:shared", one, "{}");
    ASSERT_NOT_NULL(first);
    ASSERT_TRUE(memory_result_ok(first));
    char *repeat = cbm_memory_commit_json(
        memory, "{\"proposal_id\":\"proposal:one\",\"operation_id\":\"operation:shared\"}");
    ASSERT_NOT_NULL(repeat);
    ASSERT_STR_EQ(repeat, first);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_operations;"), 1);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_preferences;"), 1);

    const char *two = "[{\"type\":\"add_preference\",\"preference_id\":\"preference:two\","
                      "\"value\":\"never alias keys\",\"scope\":{\"suite\":\"memory\"}}]";
    char proposal[MEMORY_TEST_JSON];
    snprintf(proposal, sizeof(proposal), "{\"proposal_id\":\"proposal:two\",\"operations\":%s}",
             two);
    char *proposed = cbm_memory_propose_json(memory, proposal);
    ASSERT_TRUE(memory_result_ok(proposed));
    free(proposed);
    char *reused = cbm_memory_commit_json(
        memory, "{\"proposal_id\":\"proposal:two\",\"operation_id\":\"operation:shared\"}");
    ASSERT_NOT_NULL(reused);
    ASSERT_FALSE(memory_result_ok(reused));
    ASSERT_TRUE(memory_result_string_is(reused, "error", "idempotency_key_reused"));
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_preferences;"), 1);
    free(reused);
    free(repeat);
    free(first);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_two_handles_cas_same_page_and_merge_different_pages) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_cas");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *left = cbm_memory_open(home);
    cbm_memory_t *right = cbm_memory_open(home);
    ASSERT_NOT_NULL(left);
    ASSERT_NOT_NULL(right);

    const char *seed = "[{\"type\":\"upsert_page\",\"page_id\":\"page:shared\",\"slug\":\"shared\","
                       "\"markdown\":\"revision one\"}]";
    char *result = memory_propose_commit(left, "proposal:seed", "operation:seed", seed, "{}");
    ASSERT_TRUE(memory_result_ok(result));
    free(result);

    const char *left_update =
        "[{\"type\":\"upsert_page\",\"page_id\":\"page:shared\",\"slug\":\"shared\","
        "\"markdown\":\"revision two from left\",\"expected_revision\":1}]";
    const char *right_update =
        "[{\"type\":\"upsert_page\",\"page_id\":\"page:shared\",\"slug\":\"shared\","
        "\"markdown\":\"revision two from right\",\"expected_revision\":1}]";
    char *left_proposal = cbm_memory_propose_json(
        left, "{\"proposal_id\":\"proposal:left\",\"operations\":[{\"type\":\"upsert_page\","
              "\"page_id\":\"page:shared\",\"slug\":\"shared\","
              "\"markdown\":\"revision two from left\",\"expected_revision\":1}]}");
    char *right_proposal = cbm_memory_propose_json(
        right, "{\"proposal_id\":\"proposal:right\",\"operations\":[{\"type\":\"upsert_page\","
               "\"page_id\":\"page:shared\",\"slug\":\"shared\","
               "\"markdown\":\"revision two from right\",\"expected_revision\":1}]}");
    (void)left_update;
    (void)right_update;
    ASSERT_TRUE(memory_result_ok(left_proposal));
    ASSERT_TRUE(memory_result_ok(right_proposal));
    free(left_proposal);
    free(right_proposal);
    char *left_commit = cbm_memory_commit_json(
        left, "{\"proposal_id\":\"proposal:left\",\"operation_id\":\"operation:left\"}");
    char *right_commit = cbm_memory_commit_json(
        right, "{\"proposal_id\":\"proposal:right\",\"operation_id\":\"operation:right\"}");
    ASSERT_TRUE(memory_result_ok(left_commit));
    ASSERT_FALSE(memory_result_ok(right_commit));
    ASSERT_TRUE(memory_result_string_is(right_commit, "error", "revision_conflict"));
    ASSERT_EQ(memory_sql_int(
                  left, "SELECT current_revision FROM memory_pages WHERE page_id='page:shared';"),
              2);
    free(left_commit);
    free(right_commit);

    /* Proposals from the same snapshot commute when their entity revisions do not overlap. */
    char *different_left = cbm_memory_propose_json(
        left, "{\"proposal_id\":\"proposal:different-left\",\"operations\":[{"
              "\"type\":\"upsert_page\",\"page_id\":\"page:left\",\"slug\":\"left\","
              "\"markdown\":\"left body\"}]}");
    char *different_right = cbm_memory_propose_json(
        right, "{\"proposal_id\":\"proposal:different-right\",\"operations\":[{"
               "\"type\":\"upsert_page\",\"page_id\":\"page:right\",\"slug\":\"right\","
               "\"markdown\":\"right body\"}]}");
    ASSERT_TRUE(memory_result_ok(different_left));
    ASSERT_TRUE(memory_result_ok(different_right));
    free(different_left);
    free(different_right);
    char *different_left_commit =
        cbm_memory_commit_json(left, "{\"proposal_id\":\"proposal:different-left\",\"operation_"
                                     "id\":\"operation:different-left\"}");
    char *different_right_commit =
        cbm_memory_commit_json(right, "{\"proposal_id\":\"proposal:different-right\",\"operation_"
                                      "id\":\"operation:different-right\"}");
    ASSERT_TRUE(memory_result_ok(different_left_commit));
    ASSERT_TRUE(memory_result_ok(different_right_commit));
    ASSERT_EQ(memory_sql_int(left, "SELECT count(*) FROM memory_pages;"), 3);
    free(different_left_commit);
    free(different_right_commit);
    cbm_memory_close(right);
    cbm_memory_close(left);
    th_rmtree(home);
    PASS();
}

TEST(memory_actual_concurrent_writers_preserve_commuting_updates_and_cas) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_concurrent_writers");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *bootstrap = cbm_memory_open(home);
    ASSERT_NOT_NULL(bootstrap);
    cbm_memory_close(bootstrap);

    atomic_int ready = ATOMIC_VAR_INIT(0);
    atomic_bool go = ATOMIC_VAR_INIT(false);
    memory_writer_task_t different[2] = {
        {.home = home,
         .proposal_json = "{\"proposal_id\":\"proposal:thread-left\",\"operations\":[{"
                          "\"type\":\"upsert_page\",\"page_id\":\"page:thread-left\","
                          "\"slug\":\"thread-left\",\"markdown\":\"left thread body\"}]}",
         .commit_json = "{\"proposal_id\":\"proposal:thread-left\","
                        "\"operation_id\":\"operation:thread-left\"}",
         .ready = &ready,
         .go = &go},
        {.home = home,
         .proposal_json = "{\"proposal_id\":\"proposal:thread-right\",\"operations\":[{"
                          "\"type\":\"upsert_page\",\"page_id\":\"page:thread-right\","
                          "\"slug\":\"thread-right\",\"markdown\":\"right thread body\"}]}",
         .commit_json = "{\"proposal_id\":\"proposal:thread-right\","
                        "\"operation_id\":\"operation:thread-right\"}",
         .ready = &ready,
         .go = &go}};
    ASSERT_EQ(memory_run_two_writers(different), 0);
    ASSERT_TRUE(memory_result_ok(different[0].proposal_result));
    ASSERT_TRUE(memory_result_ok(different[1].proposal_result));
    ASSERT_TRUE(memory_result_ok(different[0].commit_result));
    ASSERT_TRUE(memory_result_ok(different[1].commit_result));
    free(different[0].proposal_result);
    free(different[1].proposal_result);
    free(different[0].commit_result);
    free(different[1].commit_result);
    cbm_memory_t *inspection = cbm_memory_open(home);
    ASSERT_NOT_NULL(inspection);
    ASSERT_EQ(memory_sql_int(inspection, "SELECT count(*) FROM memory_pages WHERE page_id IN "
                                         "('page:thread-left','page:thread-right');"),
              2);

    const char *shared_seed = "[{\"type\":\"upsert_page\",\"page_id\":\"page:thread-shared\","
                              "\"slug\":\"thread-shared\",\"markdown\":\"shared revision one\"}]";
    char *seed = memory_propose_commit(inspection, "proposal:thread-seed", "operation:thread-seed",
                                       shared_seed, "{}");
    ASSERT_TRUE(memory_result_ok(seed));
    free(seed);
    cbm_memory_close(inspection);

    atomic_store_explicit(&ready, 0, memory_order_release);
    atomic_store_explicit(&go, false, memory_order_release);
    memory_writer_task_t same[2] = {
        {.home = home,
         .proposal_json = "{\"proposal_id\":\"proposal:thread-same-left\",\"operations\":[{"
                          "\"type\":\"upsert_page\",\"page_id\":\"page:thread-shared\","
                          "\"slug\":\"thread-shared\",\"markdown\":\"left wins maybe\","
                          "\"expected_revision\":1}]}",
         .commit_json = "{\"proposal_id\":\"proposal:thread-same-left\","
                        "\"operation_id\":\"operation:thread-same-left\"}",
         .ready = &ready,
         .go = &go},
        {.home = home,
         .proposal_json = "{\"proposal_id\":\"proposal:thread-same-right\",\"operations\":[{"
                          "\"type\":\"upsert_page\",\"page_id\":\"page:thread-shared\","
                          "\"slug\":\"thread-shared\",\"markdown\":\"right wins maybe\","
                          "\"expected_revision\":1}]}",
         .commit_json = "{\"proposal_id\":\"proposal:thread-same-right\","
                        "\"operation_id\":\"operation:thread-same-right\"}",
         .ready = &ready,
         .go = &go}};
    ASSERT_EQ(memory_run_two_writers(same), 0);
    ASSERT_TRUE(memory_result_ok(same[0].proposal_result));
    ASSERT_TRUE(memory_result_ok(same[1].proposal_result));
    int successes = (memory_result_ok(same[0].commit_result) ? 1 : 0) +
                    (memory_result_ok(same[1].commit_result) ? 1 : 0);
    ASSERT_EQ(successes, 1);
    const char *loser =
        memory_result_ok(same[0].commit_result) ? same[1].commit_result : same[0].commit_result;
    ASSERT_TRUE(memory_result_string_is(loser, "error", "revision_conflict"));
    free(same[0].proposal_result);
    free(same[1].proposal_result);
    free(same[0].commit_result);
    free(same[1].commit_result);
    inspection = cbm_memory_open(home);
    ASSERT_NOT_NULL(inspection);
    ASSERT_EQ(memory_sql_int(inspection, "SELECT current_revision FROM memory_pages WHERE "
                                         "page_id='page:thread-shared';"),
              2);
    ASSERT_EQ(memory_sql_int(inspection, "SELECT count(*) FROM memory_revisions WHERE "
                                         "page_id='page:thread-shared';"),
              2);
    cbm_memory_close(inspection);
    th_rmtree(home);
    PASS();
}

TEST(memory_retrieval_routes_staleness_and_bitemporal_history) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_retrieval");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *operations =
        "[{\"type\":\"add_claim\",\"claim_id\":\"claim:hypothesis\","
        "\"claim_kind\":\"hypothesis\",\"status\":\"stale\","
        "\"subject\":\"adaptive cache\",\"predicate\":\"may reduce\","
        "\"object\":\"tail latency\",\"review_after\":\"2020-01-01T00:00:00Z\"},"
        "{\"type\":\"add_claim\",\"claim_id\":\"claim:history\","
        "\"claim_kind\":\"fact\",\"status\":\"active\","
        "\"subject\":\"legacy API\",\"predicate\":\"uses\",\"object\":\"v1\","
        "\"valid_from\":\"2019-01-01T00:00:00Z\","
        "\"recorded_from\":\"2020-01-01T00:00:00Z\"},"
        "{\"type\":\"add_claim\",\"claim_id\":\"claim:local-target\","
        "\"claim_kind\":\"recommendation\",\"status\":\"active\",\"subject\":\"deployment target\","
        "\"predicate\":\"runs\",\"object\":\"locally\",\"scope\":{\"scope\":\"local\"}},"
        "{\"type\":\"add_claim\",\"claim_id\":\"claim:remote-target\","
        "\"claim_kind\":\"recommendation\",\"status\":\"active\",\"subject\":\"deployment target\","
        "\"predicate\":\"runs\",\"object\":\"remotely\",\"scope\":{\"scope\":\"remote\"}},"
        "{\"type\":\"add_decision\",\"decision_id\":\"decision:retrieval\","
        "\"title\":\"SQLite decision\",\"chosen_option\":\"SQLite WAL\","
        "\"alternatives\":[\"flat files\"],\"applicability\":{\"scope\":\"local\"},"
        "\"exit_criteria\":[\"remote collaboration\"]}]";
    char *seed = memory_propose_commit(memory, "proposal:retrieval", "operation:retrieval",
                                       operations, "{}");
    ASSERT_TRUE(memory_result_ok(seed));
    free(seed);

    char *stale =
        cbm_memory_query_json(memory, "{\"query\":\"adaptive cache latency\",\"mode\":\"search\"}");
    ASSERT_TRUE(memory_result_ok(stale));
    ASSERT_TRUE(memory_result_string_is(stale, "route", "verify"));
    ASSERT_NOT_NULL(strstr(stale, "\"status\":\"stale\""));
    free(stale);
    char *stale_high_impact =
        cbm_memory_query_json(memory, "{\"query\":\"adaptive cache latency\",\"impact\":\"high\"}");
    ASSERT_TRUE(memory_result_string_is(stale_high_impact, "route", "deliberate"));
    free(stale_high_impact);
    char *strict = cbm_memory_query_json(
        memory, "{\"query\":\"adaptive cache latency\",\"freshness\":\"require_current\"}");
    ASSERT_TRUE(memory_result_string_is(strict, "route", "abstain"));
    free(strict);

    char *deliberate =
        cbm_memory_query_json(memory, "{\"query\":\"SQLite decision\",\"impact\":\"high\"}");
    ASSERT_TRUE(memory_result_string_is(deliberate, "route", "deliberate"));
    free(deliberate);
    char *experiment =
        cbm_memory_query_json(memory, "{\"query\":\"SQLite decision\",\"reversible\":true}");
    ASSERT_TRUE(memory_result_string_is(experiment, "route", "experiment"));
    free(experiment);
    char *reuse = cbm_memory_query_json(
        memory, "{\"query\":\"SQLite decision\",\"current_context\":{\"scope\":\"local\"}}");
    ASSERT_TRUE(memory_result_string_is(reuse, "route", "reuse"));
    free(reuse);
    char *mixed_applicability =
        cbm_memory_query_json(memory, "{\"query\":\"deployment target\",\"entity_kind\":\"claim\","
                                      "\"current_context\":{\"scope\":\"local\"}}");
    ASSERT_TRUE(memory_result_string_is(mixed_applicability, "route", "reuse"));
    ASSERT_NOT_NULL(strstr(mixed_applicability, "\"applicability_state\":\"matched\""));
    ASSERT_NOT_NULL(strstr(mixed_applicability, "\"applicability_state\":\"mismatched\""));
    ASSERT_NOT_NULL(strstr(mixed_applicability, "\"memory_not_applicable_to_current_context\""));
    free(mixed_applicability);
    char *unsupported_fact =
        cbm_memory_query_json(memory, "{\"query\":\"legacy API v1\",\"entity_kind\":\"claim\"}");
    ASSERT_TRUE(memory_result_string_is(unsupported_fact, "route", "verify"));
    ASSERT_NOT_NULL(strstr(unsupported_fact, "missing_evidence"));
    free(unsupported_fact);

    const char *status_update = "[{\"type\":\"update_claim_status\",\"claim_id\":\"claim:history\","
                                "\"status\":\"stale\",\"expected_revision\":1}]";
    char *updated = memory_propose_commit(memory, "proposal:history-update",
                                          "operation:history-update", status_update, "{}");
    ASSERT_TRUE(memory_result_ok(updated));
    free(updated);
    ASSERT_EQ(memory_sql_int(memory,
                             "SELECT count(*) FROM memory_claim_revisions WHERE "
                             "claim_id='claim:history' AND revision=1 AND status='active';"),
              1);
    char *historical =
        cbm_memory_query_json(memory, "{\"mode\":\"as_of\",\"valid_at\":\"2021-01-01T00:00:00Z\","
                                      "\"known_at\":\"2021-01-01T00:00:00Z\"}");
    ASSERT_TRUE(memory_result_ok(historical));
    ASSERT_NOT_NULL(strstr(historical, "\"id\":\"claim:history\""));
    ASSERT_NOT_NULL(strstr(historical, "\"status\":\"active\""));
    free(historical);
    char *current =
        cbm_memory_query_json(memory, "{\"mode\":\"as_of\",\"valid_at\":\"2030-01-01T00:00:00Z\","
                                      "\"known_at\":\"2030-01-01T00:00:00Z\"}");
    ASSERT_TRUE(memory_result_ok(current));
    ASSERT_NOT_NULL(strstr(current, "\"claim_kind\":\"hypothesis\""));
    ASSERT_NOT_NULL(strstr(current, "\"id\":\"claim:history\""));
    ASSERT_NOT_NULL(strstr(current, "\"status\":\"stale\""));
    free(current);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_lint_reports_epistemic_and_maintenance_issues) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_lint");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *operations =
        "[{\"type\":\"upsert_page\",\"page_id\":\"page:orphan\",\"slug\":\"orphan\","
        "\"markdown\":\"# Orphan\"},"
        "{\"type\":\"add_claim\",\"claim_id\":\"claim:unsupported\","
        "\"claim_kind\":\"fact\",\"status\":\"active\",\"subject\":\"unsupported\","
        "\"predicate\":\"is\",\"object\":\"asserted\"},"
        "{\"type\":\"add_experience\",\"experience_id\":\"experience:contextless\","
        "\"context\":{},\"observation\":\"an anecdote\"},"
        "{\"type\":\"add_decision\",\"decision_id\":\"decision:thin\","
        "\"chosen_option\":\"first idea\"},"
        "{\"type\":\"add_preference\",\"preference_id\":\"preference:unscoped\","
        "\"value\":\"always do this\"}]";
    char *seed = memory_propose_commit(memory, "proposal:lint", "operation:lint", operations, "{}");
    ASSERT_TRUE(memory_result_ok(seed));
    free(seed);
    char *lint = cbm_memory_lint_json(memory, "{\"limit\":100}");
    ASSERT_NOT_NULL(lint);
    ASSERT_TRUE(memory_result_ok(lint));
    ASSERT_GTE(memory_result_int(lint, "issue_count"), 5);
    ASSERT_NOT_NULL(strstr(lint, "\"code\":\"unsupported_fact\""));
    ASSERT_NOT_NULL(strstr(lint, "\"code\":\"orphan_page\""));
    ASSERT_NOT_NULL(strstr(lint, "\"code\":\"contextless_experience\""));
    ASSERT_NOT_NULL(strstr(lint, "\"code\":\"decision_missing_alternatives\""));
    ASSERT_NOT_NULL(strstr(lint, "\"code\":\"unscoped_preference\""));
    free(lint);
    char *filtered = cbm_memory_lint_json(memory, "{\"checks\":[\"unsupported_fact\"]}");
    ASSERT_TRUE(memory_result_ok(filtered));
    ASSERT_EQ(memory_result_int(filtered, "issue_count"), 1);
    ASSERT_NOT_NULL(strstr(filtered, "\"code\":\"unsupported_fact\""));
    ASSERT_NULL(strstr(filtered, "\"code\":\"orphan_page\""));
    free(filtered);
    char *unknown = cbm_memory_lint_json(memory, "{\"checks\":[\"does_not_exist\"]}");
    ASSERT_FALSE(memory_result_ok(unknown));
    ASSERT_TRUE(memory_result_string_is(unknown, "error", "invalid_checks"));
    free(unknown);
    char *apply = cbm_memory_lint_json(memory, "{\"apply\":true}");
    ASSERT_FALSE(memory_result_ok(apply));
    ASSERT_TRUE(memory_result_string_is(apply, "error", "apply_not_supported"));
    free(apply);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_marks_validates_code_refs_and_dirties_linked_memory) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_coderef");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *operations =
        "[{\"type\":\"upsert_page\",\"page_id\":\"page:code\",\"slug\":\"code\","
        "\"markdown\":\"# Code dependency\"},"
        "{\"type\":\"add_code_ref\",\"code_ref_id\":\"coderef:run\","
        "\"project\":\"sample\",\"ref_kind\":\"symbol\","
        "\"qualified_name\":\"pkg.core.run\",\"file_path\":\"src/core.c\"},"
        "{\"type\":\"link\",\"source_kind\":\"page\",\"source_id\":\"page:code\","
        "\"target_kind\":\"code_ref\",\"target_id\":\"coderef:run\","
        "\"relation_type\":\"REFERENCES\"}]";
    char *seed =
        memory_propose_commit(memory, "proposal:coderef", "operation:coderef", operations, "{}");
    ASSERT_TRUE(memory_result_ok(seed));
    free(seed);
    char *marked = cbm_memory_mark_code_changes_json(
        memory, "{\"project\":\"sample\",\"files\":[\"src/core.c\"],"
                "\"qualified_names\":[\"pkg.core.run\"],\"reason\":\"test_code_change\"}");
    ASSERT_TRUE(memory_result_ok(marked));
    ASSERT_EQ(memory_result_int(marked, "marked"), 1);
    ASSERT_TRUE(memory_result_string_is(marked, "reason", "test_code_change"));
    ASSERT_NOT_NULL(strstr(marked, "\"affected_memory\""));
    free(marked);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_code_refs WHERE "
                                     "code_ref_id='coderef:run' AND resolution_status='changed';"),
              1);
    ASSERT_GTE(memory_sql_int(memory, "SELECT count(*) FROM memory_dirty WHERE status='open' AND "
                                      "entity_id IN ('coderef:run','page:code');"),
               2);
    ASSERT_GTE(memory_sql_int(memory, "SELECT count(*) FROM memory_dirty WHERE status='open' AND "
                                      "reason='test_code_change';"),
               2);

    cbm_store_t *code_store = cbm_store_open_memory();
    ASSERT_NOT_NULL(code_store);
    ASSERT_EQ(cbm_store_upsert_project(code_store, "sample", home), CBM_STORE_OK);
    cbm_node_t node = {.project = "sample",
                       .label = "Function",
                       .name = "run",
                       .qualified_name = "pkg.core.run",
                       .file_path = "src/core.c",
                       .start_line = 10,
                       .end_line = 20,
                       .properties_json = "{}"};
    ASSERT_GT(cbm_store_upsert_node(code_store, &node), 0);
    int64_t before_validation = cbm_memory_snapshot_epoch(memory);
    ASSERT_EQ(cbm_memory_validate_code_refs(memory, code_store, "sample"), 1);
    int64_t after_validation = cbm_memory_snapshot_epoch(memory);
    ASSERT_GT(after_validation, before_validation);
    ASSERT_EQ(cbm_memory_validate_code_refs(memory, code_store, "sample"), 1);
    ASSERT_EQ(cbm_memory_snapshot_epoch(memory), after_validation);
    ASSERT_EQ(memory_sql_int(memory,
                             "SELECT count(*) FROM memory_code_refs WHERE "
                             "code_ref_id='coderef:run' AND resolution_status='resolved' AND "
                             "last_resolved_at IS NOT NULL;"),
              1);
    char *query = cbm_memory_query_json(
        memory, "{\"query\":\"Code dependency\",\"current_context\":{\"project\":\"sample\"}}");
    ASSERT_TRUE(memory_result_ok(query));
    ASSERT_TRUE(memory_result_string_is(query, "route", "verify"));
    ASSERT_NOT_NULL(strstr(query, "\"dirty\":true"));
    ASSERT_NOT_NULL(strstr(query, "test_code_change"));
    free(query);
    cbm_store_close(code_store);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_rebuild_projection_restores_graph_and_search) {
    char home[MEMORY_TEST_PATH];
    char *temporary = th_mktempdir("cbm_memory_rebuild");
    ASSERT_NOT_NULL(temporary);
    snprintf(home, sizeof(home), "%s", temporary);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    const char *operations = "[{\"type\":\"upsert_page\",\"page_id\":\"page:projection\","
                             "\"slug\":\"projection\",\"title\":\"Projection Recovery\","
                             "\"markdown\":\"# Projection Recovery\\nRebuild derived state.\"},"
                             "{\"type\":\"add_claim\",\"claim_id\":\"claim:projection\","
                             "\"claim_kind\":\"hypothesis\",\"subject\":\"projection rebuild\","
                             "\"predicate\":\"restores\",\"object\":\"search documents\","
                             "\"page_id\":\"page:projection\"}]";
    char *seed = memory_propose_commit(memory, "proposal:projection", "operation:projection",
                                       operations, "{}");
    ASSERT_TRUE(memory_result_ok(seed));
    free(seed);
    ASSERT_GT(memory_sql_int(memory, "SELECT count(*) FROM nodes;"), 0);
    ASSERT_GT(memory_sql_int(memory, "SELECT count(*) FROM memory_documents;"), 0);
    ASSERT_EQ(sqlite3_exec(cbm_memory_db(memory),
                           "DELETE FROM edges; DELETE FROM nodes; DELETE FROM memory_documents;",
                           NULL, NULL, NULL),
              SQLITE_OK);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM nodes;"), 0);
    ASSERT_EQ(memory_sql_int(memory, "SELECT count(*) FROM memory_documents;"), 0);
    ASSERT_EQ(cbm_memory_rebuild_projection(memory), 0);
    ASSERT_GT(memory_sql_int(memory, "SELECT count(*) FROM nodes;"), 0);
    ASSERT_GTE(memory_sql_int(memory, "SELECT count(*) FROM edges;"), 2);
    ASSERT_GTE(memory_sql_int(memory, "SELECT count(*) FROM memory_documents;"), 2);
    char *query = cbm_memory_query_json(memory, "{\"query\":\"Projection Recovery\"}");
    ASSERT_TRUE(memory_result_ok(query));
    ASSERT_GT(memory_result_int(query, "count"), 0);
    ASSERT_NOT_NULL(strstr(query, "page:projection"));
    free(query);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

SUITE(memory) {
    RUN_TEST(memory_open_schema);
    RUN_TEST(memory_ingest_deduplicates);
    RUN_TEST(memory_source_revision_dirties_supported_claim);
    RUN_TEST(memory_commit_all_epistemic_entities_and_materializes_wiki);
    RUN_TEST(memory_operation_idempotency_and_key_ownership);
    RUN_TEST(memory_two_handles_cas_same_page_and_merge_different_pages);
    RUN_TEST(memory_actual_concurrent_writers_preserve_commuting_updates_and_cas);
    RUN_TEST(memory_retrieval_routes_staleness_and_bitemporal_history);
    RUN_TEST(memory_lint_reports_epistemic_and_maintenance_issues);
    RUN_TEST(memory_marks_validates_code_refs_and_dirties_linked_memory);
    RUN_TEST(memory_rebuild_projection_restores_graph_and_search);
}
