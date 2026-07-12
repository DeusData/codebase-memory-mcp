/*
 * test_memory_share.c — Global Memory portable bundle and Git transport tests.
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "foundation/compat_fs.h"
#include "foundation/sha256.h"
#include "memory/memory.h"
#include "memory/memory_share.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <sys/stat.h>
#endif

enum { SHARE_TEST_PATH = 1024 };

static bool result_ok(const char *json) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc) {
        return false;
    }
    yyjson_val *ok = yyjson_obj_get(yyjson_doc_get_root(doc), "ok");
    bool value = ok && yyjson_is_bool(ok) && yyjson_get_bool(ok);
    yyjson_doc_free(doc);
    return value;
}

static int64_t result_int(const char *json, const char *key) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    if (!doc) {
        return -1;
    }
    yyjson_val *value = yyjson_obj_get(yyjson_doc_get_root(doc), key);
    int64_t number = value && yyjson_is_num(value) ? yyjson_get_sint(value) : -1;
    yyjson_doc_free(doc);
    return number;
}

static char *read_text(const char *path) {
    FILE *file = fopen(path, "rb");
    if (!file) {
        return NULL;
    }
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    fseek(file, 0, SEEK_SET);
    if (size < 0) {
        fclose(file);
        return NULL;
    }
    char *text = malloc((size_t)size + 1);
    if (!text) {
        fclose(file);
        return NULL;
    }
    size_t got = fread(text, 1, (size_t)size, file);
    fclose(file);
    if (got != (size_t)size) {
        free(text);
        return NULL;
    }
    text[size] = '\0';
    return text;
}

static int query_count(cbm_memory_t *memory, const char *table) {
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
    sqlite3_stmt *statement = NULL;
    int count = -1;
    if (sqlite3_prepare_v2(cbm_memory_db(memory), sql, -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) {
        count = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return count;
}

static int seed_raw_source(cbm_memory_t *memory) {
    static const char content[] = "portable raw source\n";
    char hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(content, strlen(content), hash);
    char relative[SHARE_TEST_PATH];
    snprintf(relative, sizeof(relative), "raw/objects/%.2s/%s.md", hash, hash);
    char absolute[SHARE_TEST_PATH];
    snprintf(absolute, sizeof(absolute), "%s/%s", cbm_memory_home(memory), relative);
    if (th_write_file(absolute, content) != 0) {
        return -1;
    }
    sqlite3_stmt *statement = NULL;
    const char *sql =
        "INSERT INTO memory_sources(source_id,content_hash,object_relpath,title,origin,media_type,"
        " retrieved_at,byte_size,created_at) VALUES('source:portable',?1,?2,'Portable source',"
        " 'test://portable','text/markdown','2026-01-01T00:00:00Z',?3,'2026-01-01T00:00:00Z')";
    if (sqlite3_prepare_v2(cbm_memory_db(memory), sql, -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, hash, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, relative, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 3, (sqlite3_int64)strlen(content));
    int rc = sqlite3_step(statement);
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int seed_full_memory(cbm_memory_t *memory, const char *page_title, const char *ref_status,
                            int ref_epoch) {
    if (seed_raw_source(memory) != 0) {
        return -1;
    }
    sqlite3 *db = cbm_memory_db(memory);
    char *sql = sqlite3_mprintf(
        "BEGIN;"
        "UPDATE memory_state SET memory_epoch=10 WHERE singleton=1;"
        "INSERT INTO memory_pages(page_id,slug,title,page_kind,status,current_revision,"
        " current_revision_id,created_at,updated_at) VALUES('page:portable','portable','%q',"
        " 'concept','active',1,'revision:portable:1','2026-01-01T00:00:00Z',"
        " '2026-01-01T00:00:00Z');"
        "INSERT INTO memory_revisions(revision_id,page_id,revision,base_revision,body_hash,"
        " markdown,author_agent,created_epoch,created_at) VALUES('revision:portable:1',"
        " 'page:portable',1,0,'body-hash','# Portable\n','test-agent',10,"
        " '2026-01-01T00:00:00Z');"
        "INSERT INTO memory_claims(claim_id,claim_kind,subject,predicate,object_text,status,"
        " recorded_from,revision,created_epoch,updated_epoch,created_at,updated_at) VALUES("
        " 'claim:portable','fact','bundle','is','portable','active','2026-01-01T00:00:00Z',2,"
        " 10,10,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_claim_revisions(claim_id,revision,claim_kind,subject,predicate,"
        " object_text,scope_json,status,recorded_from,recorded_to,volatility,closed_epoch,"
        " created_at) VALUES('claim:portable',1,'fact','bundle','is','portable','{}','active',"
        " '2025-01-01T00:00:00Z','2026-01-01T00:00:00Z','normal',10,"
        " '2025-01-01T00:00:00Z');"
        "INSERT INTO memory_decisions(decision_id,title,chosen_option,created_epoch,updated_epoch,"
        " created_at,updated_at) VALUES('decision:portable','Format','JSON',10,10,"
        " '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_experiences(experience_id,title,context_json,observation,outcome,"
        " created_epoch,updated_epoch,created_at,updated_at) VALUES('experience:portable',"
        " 'Round trip','{\"test\":true}','Import worked','success',10,10,"
        " '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_preferences(preference_id,title,value_text,created_epoch,updated_epoch,"
        " created_at,updated_at) VALUES('preference:portable','Format','JSON',10,10,"
        " '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_relations(relation_id,source_kind,source_id,target_kind,target_id,type,"
        " recorded_from,created_epoch) VALUES('relation:portable','page','page:portable','claim',"
        " 'claim:portable','ASSERTS','2026-01-01T00:00:00Z',10);"
        "INSERT INTO memory_code_refs(code_ref_id,project,ref_kind,qualified_name,file_path,"
        " resolution_status,revision,created_epoch,updated_epoch,created_at,updated_at) VALUES("
        " 'code-ref:portable','test-project','symbol','pkg.portable','src/portable.c','%q',1,1,%d,"
        " '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_proposals(proposal_id,base_epoch,status,operations_json,created_at)"
        " VALUES('proposal:portable',10,'committed','[{\"type\":\"add_preference\","
        "\"value\":\"JSON\"}]','2026-01-01T00:00:00Z');"
        "INSERT INTO memory_operations(operation_id,proposal_id,committed_epoch,result_json,"
        " created_at) VALUES('operation:portable','proposal:portable',10,'{\"ok\":true}',"
        " '2026-01-01T00:00:00Z');"
        "INSERT INTO memory_activities(activity_id,operation_id,proposal_id,action,base_epoch,"
        " committed_epoch,details_json,created_at) VALUES('activity:portable','operation:portable',"
        " 'proposal:portable','commit',9,10,'{}','2026-01-01T00:00:00Z');"
        "COMMIT;",
        page_title, ref_status, ref_epoch);
    if (!sql) {
        return -1;
    }
    int rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    sqlite3_free(sql);
    return rc == SQLITE_OK ? 0 : -1;
}

static int seed_conflicting_page(cbm_memory_t *memory) {
    const char *sql =
        "INSERT INTO memory_pages(page_id,slug,title,page_kind,status,current_revision,created_at,"
        " updated_at) VALUES('page:local','portable','Local title','concept','active',0,"
        " '2026-01-02T00:00:00Z','2026-01-02T00:00:00Z')";
    return sqlite3_exec(cbm_memory_db(memory), sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

static char *export_to(cbm_memory_t *memory, const char *path) {
    char args[SHARE_TEST_PATH + 64];
    snprintf(args, sizeof(args), "{\"path\":\"%s\"}", path);
    return cbm_memory_export_json(memory, args);
}

static char *import_from(cbm_memory_t *memory, const char *path, const char *policy) {
    char args[SHARE_TEST_PATH + 128];
    snprintf(args, sizeof(args), "{\"path\":\"%s\",\"policy\":\"%s\"}", path, policy);
    return cbm_memory_import_json(memory, args);
}

static int update_portable_page(cbm_memory_t *memory) {
    char *proposal = cbm_memory_propose_json(
        memory, "{\"proposal_id\":\"proposal:portable-v2\",\"operations\":[{"
                "\"type\":\"upsert_page\",\"page_id\":\"page:portable\",\"slug\":\"portable\","
                "\"title\":\"Portable\",\"page_kind\":\"concept\","
                "\"markdown\":\"# Portable v2\\n\",\"expected_revision\":1}]}");
    if (!result_ok(proposal)) {
        free(proposal);
        return -1;
    }
    free(proposal);
    char *commit = cbm_memory_commit_json(memory, "{\"proposal_id\":\"proposal:portable-v2\","
                                                  "\"operation_id\":\"operation:portable-v2\"}");
    bool ok = result_ok(commit);
    free(commit);
    return ok ? 0 : -1;
}

TEST(memory_share_export_is_deterministic_and_complete) {
    char home[SHARE_TEST_PATH];
    snprintf(home, sizeof(home), "%s", th_mktempdir("cbm_memory_share_export"));
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    ASSERT_EQ(seed_full_memory(memory, "Portable", "resolved", 10), 0);
    char first[SHARE_TEST_PATH];
    char second[SHARE_TEST_PATH];
    snprintf(first, sizeof(first), "%s/first.json", home);
    snprintf(second, sizeof(second), "%s/second.json", home);
    char *one = export_to(memory, first);
    char *two = export_to(memory, second);
    ASSERT_TRUE(result_ok(one));
    ASSERT_TRUE(result_ok(two));
    char *first_text = read_text(first);
    char *second_text = read_text(second);
    ASSERT_NOT_NULL(first_text);
    ASSERT_NOT_NULL(second_text);
    ASSERT_STR_EQ(first_text, second_text);
    ASSERT_NOT_NULL(strstr(first_text, "\"raw_objects\""));
    ASSERT_NOT_NULL(strstr(first_text, "\"preferences\""));
    ASSERT_NOT_NULL(strstr(first_text, "\"activities\""));
    free(first_text);
    free(second_text);
    free(one);
    free(two);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_share_export_preserves_parent_permissions) {
#ifdef _WIN32
    PASS();
#else
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_permissions"));
    char home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(home, sizeof(home), "%s/memory", root);
    snprintf(bundle, sizeof(bundle), "%s/export.json", root);
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    ASSERT_EQ(chmod(root, 0755), 0);
    char *result = export_to(memory, bundle);
    ASSERT_TRUE(result_ok(result));
    struct stat root_stat;
    struct stat bundle_stat;
    ASSERT_EQ(stat(root, &root_stat), 0);
    ASSERT_EQ(stat(bundle, &bundle_stat), 0);
    ASSERT_EQ(root_stat.st_mode & 0777, 0755);
    ASSERT_EQ(bundle_stat.st_mode & 0777, 0600);
    free(result);
    cbm_memory_close(memory);
    th_rmtree(root);
    PASS();
#endif
}

TEST(memory_share_import_round_trip_is_idempotent) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_roundtrip"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 10), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *first = import_from(target, bundle, "reject");
    ASSERT_TRUE(result_ok(first));
    ASSERT_EQ(query_count(target, "memory_sources"), 1);
    ASSERT_EQ(query_count(target, "memory_pages"), 1);
    ASSERT_EQ(query_count(target, "memory_preferences"), 1);
    char wiki_path[SHARE_TEST_PATH];
    snprintf(wiki_path, sizeof(wiki_path), "%s/wiki/concept/portable.md", target_home);
    char *wiki = read_text(wiki_path);
    ASSERT_NOT_NULL(wiki);
    ASSERT_STR_EQ(wiki, "# Portable\n");
    free(wiki);
    char *second = import_from(target, bundle, "reject");
    ASSERT_TRUE(result_ok(second));
    ASSERT_EQ(result_int(second, "added"), 0);
    free(export_result);
    free(first);
    free(second);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_page_fast_forward_materializes_new_revision) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_fast_forward"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 10), 0);
    char *first_export = export_to(source, bundle);
    ASSERT_TRUE(result_ok(first_export));
    char *first_import = import_from(target, bundle, "reject");
    ASSERT_TRUE(result_ok(first_import));
    ASSERT_EQ(update_portable_page(source), 0);
    char *second_export = export_to(source, bundle);
    ASSERT_TRUE(result_ok(second_export));
    char *second_import = import_from(target, bundle, "keep_remote");
    ASSERT_TRUE(result_ok(second_import));
    sqlite3_stmt *statement = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(cbm_memory_db(target),
                                 "SELECT current_revision FROM memory_pages "
                                 "WHERE page_id='page:portable'",
                                 -1, &statement, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int(statement, 0), 2);
    sqlite3_finalize(statement);
    ASSERT_EQ(query_count(target, "memory_revisions"), 2);
    char wiki_path[SHARE_TEST_PATH];
    snprintf(wiki_path, sizeof(wiki_path), "%s/wiki/concept/portable.md", target_home);
    char *wiki = read_text(wiki_path);
    ASSERT_NOT_NULL(wiki);
    ASSERT_STR_EQ(wiki, "# Portable v2\n");
    free(wiki);
    free(first_export);
    free(first_import);
    free(second_export);
    free(second_import);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_natural_identity_conflict_becomes_proposal) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_conflict"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Remote title", "resolved", 10), 0);
    ASSERT_EQ(seed_conflicting_page(target), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *result = import_from(target, bundle, "keep_local");
    ASSERT_TRUE(result_ok(result));
    ASSERT_GT(result_int(result, "conflicts"), 0);
    ASSERT_EQ(query_count(target, "memory_pages"), 1);
    ASSERT_EQ(query_count(target, "memory_revisions"), 0); /* blocked dependent row */
    ASSERT_EQ(query_count(target, "memory_proposals"), 2); /* imported + conflict proposal */
    free(export_result);
    free(result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_reject_rolls_back_canonical_rows) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_reject"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Remote title", "resolved", 10), 0);
    ASSERT_EQ(seed_conflicting_page(target), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *result = import_from(target, bundle, "reject");
    ASSERT_FALSE(result_ok(result));
    ASSERT_EQ(query_count(target, "memory_sources"), 0);
    ASSERT_EQ(query_count(target, "memory_pages"), 1);
    free(export_result);
    free(result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_newest_updates_only_code_ref_resolution) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_newest"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 20), 0);
    ASSERT_EQ(seed_full_memory(target, "Portable", "unresolved", 1), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *result = import_from(target, bundle, "newest");
    ASSERT_TRUE(result_ok(result));
    ASSERT_EQ(result_int(result, "updated"), 1);
    sqlite3_stmt *statement = NULL;
    ASSERT_EQ(sqlite3_prepare_v2(cbm_memory_db(target),
                                 "SELECT resolution_status FROM memory_code_refs WHERE "
                                 "code_ref_id='code-ref:portable'",
                                 -1, &statement, NULL),
              SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    ASSERT_STR_EQ((const char *)sqlite3_column_text(statement, 0), "resolved");
    sqlite3_finalize(statement);
    free(export_result);
    free(result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_rejects_tampered_raw_object) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_tamper"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 10), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *text = read_text(bundle);
    ASSERT_NOT_NULL(text);
    char *content = strstr(text, "\"content\":\"");
    ASSERT_NOT_NULL(content);
    content += strlen("\"content\":\"");
    content[0] = content[0] == '0' ? '1' : '0';
    ASSERT_EQ(th_write_file(bundle, text), 0);
    char *result = import_from(target, bundle, "reject");
    ASSERT_FALSE(result_ok(result));
    ASSERT_EQ(query_count(target, "memory_sources"), 0);
    free(text);
    free(export_result);
    free(result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_rejects_truncated_raw_object_path) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_short_path"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 10), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *text = read_text(bundle);
    ASSERT_NOT_NULL(text);
    char *raw = strstr(text, "\"raw_objects\":[");
    char *field = raw ? strstr(raw, "\"path\":\"") : NULL;
    ASSERT_NOT_NULL(field);
    char *value = field + strlen("\"path\":\"");
    char *end = strchr(value, '"');
    ASSERT_NOT_NULL(end);
    static const char short_path[] = "raw/objects/";
    size_t prefix_len = (size_t)(value - text);
    size_t suffix_len = strlen(end);
    char *malformed = malloc(prefix_len + sizeof(short_path) - 1 + suffix_len + 1);
    ASSERT_NOT_NULL(malformed);
    memcpy(malformed, text, prefix_len);
    memcpy(malformed + prefix_len, short_path, sizeof(short_path) - 1);
    memcpy(malformed + prefix_len + sizeof(short_path) - 1, end, suffix_len + 1);
    ASSERT_EQ(th_write_file(bundle, malformed), 0);
    char *result = import_from(target, bundle, "reject");
    ASSERT_FALSE(result_ok(result));
    ASSERT_EQ(query_count(target, "memory_sources"), 0);
    free(result);
    free(malformed);
    free(text);
    free(export_result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_rejects_broken_relation_endpoint_atomically) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_bad_relation"));
    char source_home[SHARE_TEST_PATH];
    char target_home[SHARE_TEST_PATH];
    char bundle[SHARE_TEST_PATH];
    snprintf(source_home, sizeof(source_home), "%s/source", root);
    snprintf(target_home, sizeof(target_home), "%s/target", root);
    snprintf(bundle, sizeof(bundle), "%s/bundle.json", root);
    cbm_memory_t *source = cbm_memory_open(source_home);
    cbm_memory_t *target = cbm_memory_open(target_home);
    ASSERT_NOT_NULL(source);
    ASSERT_NOT_NULL(target);
    ASSERT_EQ(seed_full_memory(source, "Portable", "resolved", 10), 0);
    char *export_result = export_to(source, bundle);
    ASSERT_TRUE(result_ok(export_result));
    char *text = read_text(bundle);
    ASSERT_NOT_NULL(text);
    char *endpoint = strstr(text, "\"target_id\":\"claim:portable\"");
    ASSERT_NOT_NULL(endpoint);
    char *id = endpoint + strlen("\"target_id\":\"");
    static const char missing_id[] = "claim:missingx";
    ASSERT_EQ(strlen(missing_id), strlen("claim:portable"));
    memcpy(id, missing_id, sizeof(missing_id) - 1);
    ASSERT_EQ(th_write_file(bundle, text), 0);
    char *result = import_from(target, bundle, "keep_local");
    ASSERT_FALSE(result_ok(result));
    ASSERT_EQ(query_count(target, "memory_sources"), 0);
    ASSERT_EQ(query_count(target, "memory_relations"), 0);
    free(result);
    free(text);
    free(export_result);
    cbm_memory_close(source);
    cbm_memory_close(target);
    th_rmtree(root);
    PASS();
}

TEST(memory_share_remote_validation_blocks_credentials_and_accepts_github) {
    char home[SHARE_TEST_PATH];
    snprintf(home, sizeof(home), "%s", th_mktempdir("cbm_memory_share_remote"));
    cbm_memory_t *memory = cbm_memory_open(home);
    ASSERT_NOT_NULL(memory);
    char *https_secret =
        cbm_memory_sync_json(memory, "{\"action\":\"configure_remote\",\"remote\":"
                                     "\"https://user:secret@github.com/example/memory.git\"}");
    char *ssh_secret =
        cbm_memory_sync_json(memory, "{\"action\":\"configure_remote\",\"remote\":"
                                     "\"ssh://user:secret@github.com/example/memory.git\"}");
    char *query_secret =
        cbm_memory_sync_json(memory, "{\"action\":\"configure_remote\",\"remote\":"
                                     "\"https://github.com/example/memory.git?token=secret\"}");
    ASSERT_FALSE(result_ok(https_secret));
    ASSERT_FALSE(result_ok(ssh_secret));
    ASSERT_FALSE(result_ok(query_secret));
    char *github = cbm_memory_sync_json(memory, "{\"action\":\"configure_remote\",\"remote\":"
                                                "\"https://github.com/example/memory.git\"}");
    ASSERT_TRUE(result_ok(github)); /* configuration only: no network access */
    free(https_secret);
    free(ssh_secret);
    free(query_secret);
    free(github);
    cbm_memory_close(memory);
    th_rmtree(home);
    PASS();
}

TEST(memory_share_git_sync_round_trip_uses_local_bare_remote) {
    char root[SHARE_TEST_PATH];
    snprintf(root, sizeof(root), "%s", th_mktempdir("cbm_memory_share_git"));
    char bare[SHARE_TEST_PATH];
    char first_home[SHARE_TEST_PATH];
    char second_home[SHARE_TEST_PATH];
    snprintf(bare, sizeof(bare), "%s/remote.git", root);
    snprintf(first_home, sizeof(first_home), "%s/first", root);
    snprintf(second_home, sizeof(second_home), "%s/second", root);
    const char *init_bare[] = {"git", "init", "--bare", bare, NULL};
    ASSERT_EQ(cbm_exec_no_shell(init_bare), 0);
    cbm_memory_t *first = cbm_memory_open(first_home);
    cbm_memory_t *second = cbm_memory_open(second_home);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    ASSERT_EQ(seed_full_memory(first, "Portable", "resolved", 10), 0);
    char configure[SHARE_TEST_PATH + 160];
    snprintf(configure, sizeof(configure),
             "{\"action\":\"configure_remote\",\"remote\":\"%s\","
             "\"allow_local_remote\":true}",
             bare);
    char *configured_first = cbm_memory_sync_json(first, configure);
    ASSERT_TRUE(result_ok(configured_first));
    char *push = cbm_memory_sync_json(first, "{\"action\":\"push\"}");
    ASSERT_TRUE(result_ok(push));
    char *configured_second = cbm_memory_sync_json(second, configure);
    ASSERT_TRUE(result_ok(configured_second));
    char *pull = cbm_memory_sync_json(second, "{\"action\":\"pull\",\"policy\":\"reject\"}");
    ASSERT_TRUE(result_ok(pull));
    ASSERT_EQ(query_count(second, "memory_sources"), 1);
    ASSERT_EQ(query_count(second, "memory_pages"), 1);
    free(configured_first);
    free(push);
    free(configured_second);
    free(pull);
    cbm_memory_close(first);
    cbm_memory_close(second);
    th_rmtree(root);
    PASS();
}

SUITE(memory_share) {
    RUN_TEST(memory_share_export_is_deterministic_and_complete);
    RUN_TEST(memory_share_export_preserves_parent_permissions);
    RUN_TEST(memory_share_import_round_trip_is_idempotent);
    RUN_TEST(memory_share_page_fast_forward_materializes_new_revision);
    RUN_TEST(memory_share_natural_identity_conflict_becomes_proposal);
    RUN_TEST(memory_share_reject_rolls_back_canonical_rows);
    RUN_TEST(memory_share_newest_updates_only_code_ref_resolution);
    RUN_TEST(memory_share_rejects_tampered_raw_object);
    RUN_TEST(memory_share_rejects_truncated_raw_object_path);
    RUN_TEST(memory_share_rejects_broken_relation_endpoint_atomically);
    RUN_TEST(memory_share_remote_validation_blocks_credentials_and_accepts_github);
    RUN_TEST(memory_share_git_sync_round_trip_uses_local_bare_remote);
}
