#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
#include "pipeline/pipeline.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "store/store.h"
#include <stdatomic.h>

/* The Resource-reference domain here is authored independently of extraction:
 * a genuine reference without an extracted row still contributes a quality
 * miss. Every uncertain case independently forbids a guessed target. */
static const char k8s_account[] =
    "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n  namespace: app\n"
    "imagePullSecrets:\n- name: credentials\n";
static const char k8s_secret[] =
    "apiVersion: v1\nkind: Secret\nmetadata:\n  name: credentials\n  namespace: app\n";
static const char k8s_config[] = "resources:\n- account.yaml\n- secret.yaml\n";

typedef struct {
    RQ_Source sources[96];
    size_t source_count;
    RQ_Expected expected[32];
    size_t expected_count;
    RQ_Forbidden forbidden[32];
    size_t forbidden_count;
    char *owned[192];
    size_t owned_count;
    bool pod;
} K8sOracle;

static const char k8s_pod[] =
    "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\n  namespace: app\n"
    "spec:\n  serviceAccountName: worker\n  imagePullSecrets:\n  - name: token\n"
    "  volumes:\n  - name: config\n    configMap:\n      name: settings\n"
    "  - name: data\n    persistentVolumeClaim:\n      claimName: storage\n"
    "  containers:\n  - name: app\n    image: acme/web:1\n";

/* All five genuine Pod USAGE occurrences are independent goldens, including
 * its synthetic Image ontology target. No observed row supplies an expectation. */
static bool k8s_pod_quality(cbm_store_t *store, const K8sOracle *oracle, RQ_Report *report) {
    static const struct {
        const char *scalar, *target, *file, *name, *label;
    } expected[] = {{"worker", "k8sproof.pod.account.yaml::core/ServiceAccount/app/worker",
                     "pod/account.yaml", "ServiceAccount/worker", "Resource"},
                    {"token", "k8sproof.pod.secret.yaml::core/Secret/app/token", "pod/secret.yaml",
                     "Secret/token", "Resource"},
                    {"settings", "k8sproof.pod.config.yaml::core/ConfigMap/app/settings",
                     "pod/config.yaml", "ConfigMap/settings", "Resource"},
                    {"storage", "k8sproof.pod.claim.yaml::core/PersistentVolumeClaim/app/storage",
                     "pod/claim.yaml", "PersistentVolumeClaim/storage", "Resource"},
                    {"acme/web:1", "__image__acme/web", NULL, "acme/web", "Image"}};
    *report = (RQ_Report){.genuine = 5};
    sqlite3 *db = cbm_store_get_db(store);
    for (size_t i = 0; i < oracle->source_count; i++)
        if (!rq_snapshot(db, "k8sproof", &oracle->sources[i]))
            report->metadata_errors++;
    static const char sql[] =
        "SELECT "
        "r.start_byte,r.end_byte,r.source_origin,r.spelling,r.target,t.file_path,t.name,t.label,"
        "t.start_line,t.end_line,v.content_identity,v.configuration_identity,f.configuration_"
        "identity,"
        "EXISTS(SELECT 1 FROM edges e WHERE e.project=r.project AND e.source_id=s.id "
        "AND e.target_id=t.id AND e.type=r.relation) FROM resolver_occurrences r "
        "LEFT JOIN resolver_occurrence_evidence v ON v.project=r.project AND v.ordinal=r.ordinal "
        "LEFT JOIN resolver_file_summaries f ON f.project=r.project AND f.file_path=r.file_path "
        "LEFT JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
        "LEFT JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
        "WHERE r.project='k8sproof' AND r.file_path='pod/pod.yaml' AND "
        "r.scope='k8sproof.pod.pod.yaml::core/Pod/app/web' AND r.relation='USAGE' AND r.emitted=1";
    sqlite3_stmt *statement = NULL;
    bool found[5] = {false};
    char content[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(k8s_pod, strlen(k8s_pod), content);
    int step = SQLITE_ERROR;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK) {
        while ((step = sqlite3_step(statement)) == SQLITE_ROW) {
            bool matched = false;
            for (size_t i = 0; i < 5; i++) {
                const char *site = strstr(k8s_pod, expected[i].scalar);
                if (sqlite3_column_int64(statement, 0) != site - k8s_pod ||
                    sqlite3_column_int64(statement, 1) !=
                        site - k8s_pod + (int64_t)strlen(expected[i].scalar))
                    continue;
                bool exact =
                    !found[i] && sqlite3_column_int(statement, 2) == CBM_SOURCE_ORIGIN_RAW &&
                    rq_equal((const char *)sqlite3_column_text(statement, 3), expected[i].scalar) &&
                    rq_equal((const char *)sqlite3_column_text(statement, 4), expected[i].target) &&
                    rq_equal((const char *)sqlite3_column_text(statement, 6), expected[i].name) &&
                    rq_equal((const char *)sqlite3_column_text(statement, 7), expected[i].label) &&
                    (!expected[i].file ||
                     (rq_equal((const char *)sqlite3_column_text(statement, 5), expected[i].file) &&
                      sqlite3_column_int(statement, 8) <= 2 &&
                      sqlite3_column_int(statement, 9) >= 2)) &&
                    rq_equal((const char *)sqlite3_column_text(statement, 10), content) &&
                    rq_text((const char *)sqlite3_column_text(statement, 11)) &&
                    rq_equal((const char *)sqlite3_column_text(statement, 11),
                             (const char *)sqlite3_column_text(statement, 12)) &&
                    sqlite3_column_int(statement, 13);
                found[i] = exact;
                matched = exact;
                break;
            }
            report->wrong += !matched;
        }
    }
    report->metadata_errors += step != SQLITE_DONE;
    sqlite3_finalize(statement);
    for (size_t i = 0; i < 5; i++) {
        report->found += found[i];
        report->missing += !found[i];
        report->required_missing += !found[i];
    }
    static const char edges[] =
        "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id WHERE e.project='k8sproof' "
        "AND s.qualified_name='k8sproof.pod.pod.yaml::core/Pod/app/web' AND e.type='USAGE'";
    statement = NULL;
    if (sqlite3_prepare_v2(db, edges, -1, &statement, NULL) != SQLITE_OK ||
        sqlite3_step(statement) != SQLITE_ROW || sqlite3_column_int(statement, 0) != 5)
        report->projection_errors++;
    sqlite3_finalize(statement);
    statement = NULL;
    /* Same field spelling in a custom-resource schema is not PodSpec evidence. */
    static const char forbidden[] =
        "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
        "WHERE e.project='k8sproof' AND s.file_path='pod/custom.yaml' AND e.type='USAGE'";
    if (sqlite3_prepare_v2(db, forbidden, -1, &statement, NULL) != SQLITE_OK ||
        sqlite3_step(statement) != SQLITE_ROW || sqlite3_column_int(statement, 0) != 0)
        report->wrong++;
    sqlite3_finalize(statement);
    fprintf(
        stderr,
        "[K8S-RQ] pod genuine=%zu found=%zu missing=%zu wrong=%zu projection=%zu metadata=%zu\n",
        report->genuine, report->found, report->missing, report->wrong, report->projection_errors,
        report->metadata_errors);
    return !report->missing && !report->wrong && !report->projection_errors &&
           !report->metadata_errors;
}

static char *k8s_test_owned(K8sOracle *oracle, const char *text) {
    if (oracle->owned_count >= sizeof(oracle->owned) / sizeof(oracle->owned[0]))
        return NULL;
    char *copy = strdup(text);
    if (copy)
        oracle->owned[oracle->owned_count++] = copy;
    return copy;
}

static bool k8s_test_source(K8sOracle *oracle, const char *directory, const char *name,
                            const char *text) {
    if (oracle->source_count >= sizeof(oracle->sources) / sizeof(oracle->sources[0]))
        return false;
    char path[256];
    int size = snprintf(path, sizeof(path), "%s/%s", directory, name);
    if (size <= 0 || (size_t)size >= sizeof(path))
        return false;
    const char *stored_path = k8s_test_owned(oracle, path);
    const char *stored_text = k8s_test_owned(oracle, text);
    if (!stored_path || !stored_text)
        return false;
    oracle->sources[oracle->source_count++] = (RQ_Source){stored_path, stored_text, NULL, NULL};
    return true;
}

static bool k8s_test_case(K8sOracle *oracle, const char *id, const char *account,
                          const char *secret, const char *configuration, const char *scalar,
                          const char *namespace, RQ_Mode mode, bool forbidden, const char *why,
                          const char *tried) {
    char caller[256], source[256], target[256], identity[256];
    snprintf(caller, sizeof(caller), "%s.account.yaml::core/ServiceAccount/%s/client", id,
             namespace);
    snprintf(source, sizeof(source), "%s/account.yaml", id);
    snprintf(target, sizeof(target), "%s/secret.yaml", id);
    snprintf(identity, sizeof(identity), "%s.secret.yaml::core/Secret/%s/credentials", id,
             namespace);
    for (char *p = caller; *p; p++)
        if (*p == '/' && p < strstr(caller, "::"))
            *p = '.';
    for (char *p = identity; *p; p++)
        if (*p == '/' && p < strstr(identity, "::"))
            *p = '.';
    RQ_Site site = {.file = k8s_test_owned(oracle, source),
                    .caller = k8s_test_owned(oracle, caller),
                    .needle = scalar};
    if (!site.file || !site.caller || !k8s_test_source(oracle, id, "account.yaml", account) ||
        !k8s_test_source(oracle, id, "secret.yaml", secret) ||
        (configuration && !k8s_test_source(oracle, id, "kustomization.yaml", configuration)))
        return false;
    if (forbidden) {
        oracle->forbidden[oracle->forbidden_count++] =
            (RQ_Forbidden){.case_id = id, .site = site, .relation = "USAGE", .any_target = true};
    } else {
        oracle->expected[oracle->expected_count++] =
            (RQ_Expected){.case_id = id,
                          .site = site,
                          .relation = "USAGE",
                          .target = {.file = k8s_test_owned(oracle, target),
                                     .suffix = k8s_test_owned(oracle, identity),
                                     .label = "Resource",
                                     .declaration_line = 2,
                                     .declaration_text = "kind: Secret"},
                          .mode = mode,
                          .gap_id = mode == RQ_KNOWN_GAP ? id : NULL,
                          .why = why,
                          .tried = tried};
    }
    return true;
}

static bool k8s_test_oracle(K8sOracle *oracle) {
    if (!k8s_test_case(oracle, "literal", k8s_account, k8s_secret, k8s_config, "credentials", "app",
                       RQ_REQUIRED, false, NULL, NULL) ||
        !k8s_test_case(
            oracle, "quoted",
            "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n  namespace: app\n"
            "imagePullSecrets:\n- name: 'credentials'\n",
            k8s_secret, "resources: [account.yaml, secret.yaml]\n", "'credentials'", "app",
            RQ_REQUIRED, false, NULL, NULL) ||
        !k8s_test_case(oracle, "unset",
                       "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n"
                       "imagePullSecrets:\n- name: credentials\n",
                       "apiVersion: v1\nkind: Secret\nmetadata:\n  name: credentials\n", k8s_config,
                       "credentials", "-", RQ_REQUIRED, false, NULL, NULL) ||
        !k8s_test_case(oracle, "outside_decoy", k8s_account, k8s_secret, k8s_config, "credentials",
                       "app", RQ_REQUIRED, false, NULL, NULL) ||
        !k8s_test_source(oracle, "outside_decoy", "unlisted.yaml", k8s_secret) ||
        !k8s_test_case(oracle, "agreeing", k8s_account, k8s_secret, k8s_config, "credentials",
                       "app", RQ_REQUIRED, false, NULL, NULL) ||
        !k8s_test_source(oracle, "agreeing/second", "kustomization.yaml",
                         "resources:\n- ../account.yaml\n- ../secret.yaml\n"))
        return false;
    const char *duplicate_config = "resources:\n- account.yaml\n- secret.yaml\n- duplicate.yaml\n";
    if (!k8s_test_case(oracle, "duplicate", k8s_account, k8s_secret, duplicate_config,
                       "credentials", "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_source(oracle, "duplicate", "duplicate.yaml", k8s_secret) ||
        !k8s_test_case(
            oracle, "duplicate_in_file", k8s_account,
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: credentials\n  namespace: app\n"
            "---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: credentials\n  namespace: app\n",
            k8s_config, "credentials", "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(
            oracle, "own_file_decoy",
            "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n  namespace: app\n"
            "imagePullSecrets:\n- name: credentials\n---\napiVersion: v1\nkind: Secret\n"
            "metadata:\n  name: credentials\n  namespace: app\n",
            k8s_secret, k8s_config, "credentials", "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(
            oracle, "namespace_mismatch", k8s_account,
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: credentials\n  namespace: other\n",
            k8s_config, "credentials", "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(oracle, "namespace_unset",
                       "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n"
                       "imagePullSecrets:\n- name: credentials\n",
                       k8s_secret, k8s_config, "credentials", "-", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(oracle, "unscoped", k8s_account, k8s_secret, NULL, "credentials", "app",
                       RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(
            oracle, "dynamic",
            "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n  namespace: app\n"
            "imagePullSecrets:\n- name: ${secret}\n",
            k8s_secret, k8s_config, "${secret}", "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_case(oracle, "repeated_member", k8s_account, k8s_secret,
                       "resources:\n- account.yaml\n- secret.yaml\n- secret.yaml\n", "credentials",
                       "app", RQ_REQUIRED, true, NULL, NULL))
        return false;
    if (!k8s_test_case(oracle, "conflicting", k8s_account, k8s_secret, k8s_config, "credentials",
                       "app", RQ_REQUIRED, true, NULL, NULL) ||
        !k8s_test_source(oracle, "conflicting/second", "secret.yaml", k8s_secret) ||
        !k8s_test_source(oracle, "conflicting/second", "kustomization.yaml",
                         "resources:\n- ../account.yaml\n- secret.yaml\n"))
        return false;
    return k8s_test_case(
               oracle, "alias",
               "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: client\n  namespace: app\n"
               "  annotations:\n    secret: &credential credentials\nimagePullSecrets:\n"
               "- name: *credential\n",
               k8s_secret, k8s_config, "*credential", "app", RQ_KNOWN_GAP, false,
               "YAML alias expansion requires an audited occurrence map.",
               "A unique literal string anchor establishes the genuine target; alias extraction "
               "is intentionally incomplete and is counted independently.") &&
           k8s_test_case(
               oracle, "prefix", k8s_account, k8s_secret,
               "namePrefix: deployed-\nresources:\n- account.yaml\n- secret.yaml\n", "credentials",
               "app", RQ_KNOWN_GAP, false,
               "Kustomize transforms rewrite both object identity and object references.",
               "The literal namePrefix is known, but no source-preserving transform proof is "
               "implemented; the original declaration remains the known target.") &&
           k8s_test_case(
               oracle, "nested/base", k8s_account, k8s_secret, k8s_config, "credentials", "app",
               RQ_KNOWN_GAP, false,
               "Nested Kustomize build composition has no audited proof bridge yet.",
               "A transform-free parent includes this complete base; the original unique target "
               "is known independently, but nested-build authority remains unsupported.") &&
           k8s_test_source(oracle, "nested", "kustomization.yaml", "resources:\n- base\n");
}

static int k8s_test_run(const K8sOracle *oracle, bool parallel, bool *quality, RQ_Report *report) {
    *quality = false;
    const char *temporary = th_mktempdir("cbm_k8s_object_proof");
    if (!temporary)
        return -1;
    char directory[512];
    snprintf(directory, sizeof(directory), "%s", temporary);
    bool written = true;
    for (size_t i = 0; i < oracle->source_count; i++)
        written &= th_write_file(TH_PATH(directory, oracle->sources[i].path),
                                 oracle->sources[i].text) == 0;
    for (int i = 0; i < 55; i++) {
        char name[64], content[96];
        snprintf(name, sizeof(name), "pad_%02d.py", i);
        snprintf(content, sizeof(content), "def pad_%02d():\n    return %d\n", i, i);
        written &= th_write_file(TH_PATH(directory, name), content) == 0;
    }
    char *saved_workers = getenv("CBM_WORKERS") ? strdup(getenv("CBM_WORKERS")) : NULL;
    char *saved_single =
        getenv("CBM_INDEX_SINGLE_THREAD") ? strdup(getenv("CBM_INDEX_SINGLE_THREAD")) : NULL;
    if (parallel) {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        cbm_setenv("CBM_WORKERS", "4", 1);
    } else {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    }
    char database[1024];
    snprintf(database, sizeof(database), "%s/proof.db", directory);
    cbm_pipeline_t *pipeline =
        written ? cbm_pipeline_new(directory, database, CBM_MODE_FULL) : NULL;
    if (pipeline)
        cbm_pipeline_set_persistence(pipeline, false);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "k8sproof")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_query(database) : NULL;
    if (store) {
        if (oracle->pod)
            *quality = k8s_pod_quality(store, oracle, report);
        else
            *quality = rq_check(
                store, "k8sproof", parallel ? "k8s-object-parallel" : "k8s-object-sequential",
                oracle->sources, oracle->source_count, oracle->expected, oracle->expected_count,
                oracle->forbidden, oracle->forbidden_count, report, stderr);
        cbm_store_close(store);
    }
    cbm_pipeline_free(pipeline);
    if (saved_workers)
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
    else
        cbm_unsetenv("CBM_WORKERS");
    if (saved_single)
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
    else
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    free(saved_workers);
    free(saved_single);
    th_rmtree(directory);
    return rc;
}

TEST(resolver_k8s_flat_build_object_quality_sequential_and_parallel) {
    K8sOracle oracle = {0};
    bool constructed = k8s_test_oracle(&oracle);
    bool quality[2] = {false, false};
    int status[2] = {-1, -1};
    RQ_Report reports[2] = {{0}};
    for (int mode = 0; constructed && mode < 2; mode++)
        status[mode] = k8s_test_run(&oracle, mode != 0, &quality[mode], &reports[mode]);
    for (size_t i = 0; i < oracle.owned_count; i++)
        free(oracle.owned[i]);
    ASSERT(constructed);
    ASSERT_EQ(status[0], 0);
    ASSERT_EQ(status[1], 0);
    ASSERT(quality[0]);
    ASSERT(quality[1]);
    ASSERT_EQ(reports[0].genuine, 8);
    ASSERT_EQ(reports[0].found, 5);
    ASSERT_EQ(reports[0].missing, 3);
    ASSERT_EQ(reports[1].found, reports[0].found);
    ASSERT_EQ(reports[1].missing, reports[0].missing);
    PASS();
}

/* This checks the complete declaration census before graph deduplication and
 * the source spans needed by all four bounded object-reference kinds. */
TEST(resolver_k8s_raw_declarations_and_resource_members_have_exact_spans) {
    const char *pod = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\n  namespace: app\n"
                      "spec:\n  serviceAccountName: worker\n  imagePullSecrets:\n  - name: token\n"
                      "  volumes:\n  - name: config\n    configMap:\n      name: settings\n"
                      "  - name: data\n    persistentVolumeClaim:\n      claimName: storage\n"
                      "  containers:\n  - name: app\n    image: acme/web:1\n";
    const char *names[] = {"worker", "token", "settings", "storage"};
    const char *kinds[] = {"ServiceAccount", "Secret", "ConfigMap", "PersistentVolumeClaim"};
    CBMFileResult *result =
        cbm_extract_file(pod, (int)strlen(pod), CBM_LANG_K8S, "test", "pod.yaml", 0, NULL, NULL);
    bool found[4] = {false, false, false, false};
    int declarations = 0, files = 0;
    for (int i = 0; result && i < result->k8s_facts.count; i++) {
        const CBMK8sFact *fact = &result->k8s_facts.items[i];
        declarations += fact->fact == CBM_K8S_FACT_DECLARATION && fact->proof_supported;
        files += fact->fact == CBM_K8S_FACT_FILE && fact->proof_supported;
        if (fact->fact != CBM_K8S_FACT_REF || !fact->proof_supported)
            continue;
        for (size_t n = 0; n < 4; n++) {
            const char *site = strstr(pod, names[n]);
            found[n] |= site && !strcmp(fact->kind, kinds[n]) && !strcmp(fact->name, names[n]) &&
                        fact->site_start_byte == (uint32_t)(site - pod) &&
                        fact->site_end_byte == (uint32_t)(site - pod + strlen(names[n]));
        }
    }
    cbm_free_result(result);
    ASSERT_EQ(declarations, 1);
    ASSERT_EQ(files, 1);
    for (size_t n = 0; n < 4; n++)
        ASSERT(found[n]);
    PASS();
}

TEST(resolver_k8s_all_core_object_kinds_have_occurrence_proofs) {
    K8sOracle oracle = {.pod = true};
    char custom[1024];
    snprintf(custom, sizeof(custom), "apiVersion: example.org/v1%s", strchr(k8s_pod, '\n'));
    bool constructed =
        k8s_test_source(&oracle, "pod", "custom.yaml", custom) &&
        k8s_test_source(&oracle, "pod", "pod.yaml", k8s_pod) &&
        k8s_test_source(&oracle, "pod", "account.yaml",
                        "apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: worker\n  "
                        "namespace: app\n") &&
        k8s_test_source(
            &oracle, "pod", "secret.yaml",
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: token\n  namespace: app\n") &&
        k8s_test_source(
            &oracle, "pod", "config.yaml",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: app\n") &&
        k8s_test_source(&oracle, "pod", "claim.yaml",
                        "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: storage\n "
                        " namespace: app\n"
                        "spec:\n  accessModes:\n  - ReadWriteOnce\n  resources:\n    requests:\n   "
                        "   storage: 1Gi\n") &&
        k8s_test_source(&oracle, "pod", "kustomization.yaml",
                        "resources:\n- pod.yaml\n- custom.yaml\n- account.yaml\n- secret.yaml\n- "
                        "config.yaml\n- claim.yaml\n");
    bool quality[2] = {false, false};
    int status[2] = {-1, -1};
    RQ_Report reports[2] = {{0}};
    for (int mode = 0; constructed && mode < 2; mode++)
        status[mode] = k8s_test_run(&oracle, mode != 0, &quality[mode], &reports[mode]);
    for (size_t i = 0; i < oracle.owned_count; i++)
        free(oracle.owned[i]);
    ASSERT(constructed);
    ASSERT_EQ(status[0], 0);
    ASSERT_EQ(status[1], 0);
    ASSERT(quality[0]);
    ASSERT(quality[1]);
    PASS();
}

/* Two valid builds select different same-name Secrets for the shared
 * account. The flow-list build must remain in the lookup domain. Unknown
 * membership syntax/composition must never silently disappear either. */
TEST(resolver_k8s_unknown_memberships_never_close_other_builds) {
    static const struct {
        const char *id, *configuration;
    } forms[] = {{"flow_resource_list", "resources: [account.yaml, secret-b.yaml]\n"},
                 {"flow_mapping", "{resources: [account.yaml, secret-b.yaml]}\n"},
                 {"whole_list_alias", "resources: *manifest_files\n"},
                 {"alias_member", "resources:\n- *manifest_file\n- secret-b.yaml\n"},
                 {"merged_mapping", "<<: *resource_mapping\n"},
                 {"anchored_key", "&resource_key resources: [account.yaml, secret-b.yaml]\n"},
                 {"flow_bases", "bases: [nested]\n"},
                 {"flow_components", "components: [nested]\n"},
                 {"unknown_directory", "resources:\n- unknown-directory\n"}};
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        K8sOracle oracle = {0};
        /* No path crosses a build root: root includes shared/account.yaml;
         * the second build in shared/ includes only local file paths. */
        bool constructed =
            k8s_test_source(&oracle, ".", "kustomization.yaml",
                            "resources:\n- shared/account.yaml\n- secret-a.yaml\n") &&
            k8s_test_source(&oracle, "shared", "kustomization.yaml", forms[i].configuration) &&
            k8s_test_source(&oracle, "shared", "account.yaml", k8s_account) &&
            k8s_test_source(&oracle, ".", "secret-a.yaml", k8s_secret) &&
            k8s_test_source(&oracle, "shared", "secret-b.yaml", k8s_secret) &&
            k8s_test_case(&oracle, "control", k8s_account, k8s_secret, k8s_config, "credentials",
                          "app", i == 0 ? RQ_REQUIRED : RQ_KNOWN_GAP, false,
                          i == 0 ? NULL
                                 : "Unknown configuration membership conservatively disables raw "
                                   "object resolution project-wide.",
                          i == 0 ? NULL
                                 : "This separate flat build has an independently known unique "
                                   "target; incomplete negative scope enumeration must be improved "
                                   "before its genuine reference can be recovered.");
        /* The filesystem and snapshots use canonical repository paths. */
        for (size_t j = 0; j < oracle.source_count; j++)
            if (!strncmp(oracle.sources[j].path, "./", 2))
                oracle.sources[j].path += 2;
        oracle.forbidden[oracle.forbidden_count++] =
            (RQ_Forbidden){.case_id = forms[i].id,
                           .site = {.file = "shared/account.yaml",
                                    .caller = "shared.account.yaml::core/ServiceAccount/app/client",
                                    .needle = "credentials"},
                           .relation = "USAGE",
                           .any_target = true};
        bool quality[2] = {false, false};
        int status[2] = {-1, -1};
        RQ_Report reports[2] = {{0}};
        for (int mode = 0; constructed && mode < 2; mode++)
            status[mode] = k8s_test_run(&oracle, mode != 0, &quality[mode], &reports[mode]);
        for (size_t j = 0; j < oracle.owned_count; j++)
            free(oracle.owned[j]);
        fprintf(stderr, "[K8S-RQ] hidden-membership case=%s\n", forms[i].id);
        ASSERT(constructed);
        ASSERT_EQ(status[0], 0);
        ASSERT_EQ(status[1], 0);
        ASSERT(quality[0]);
        ASSERT(quality[1]);
    }
    PASS();
}

/* Independent flat builds share only the negative configuration census. Each
 * account must bind its own Secret even though every build repeats the same
 * Kubernetes identity. A count of stored facts would hide repeated cloning. */
extern _Atomic uint64_t g_k8s_proof_clone_work;

typedef struct {
    char id[32], account[64], secret[64], configuration[64];
    char caller[128], target[128];
} K8sScaleCase;

static bool k8s_test_proof_scaling_run(int copies, uint64_t *work, RQ_Report *report) {
    const char *temporary = th_mktempdir("cbm_k8s_proof_scaling");
    if (!temporary)
        return false;
    char directory[512], database[1024];
    snprintf(directory, sizeof(directory), "%s", temporary);
    snprintf(database, sizeof(database), "%s/proof.db", directory);
    K8sScaleCase *cases = calloc((size_t)copies, sizeof(*cases));
    RQ_Source *sources = calloc((size_t)copies * 3, sizeof(*sources));
    RQ_Expected *expected = calloc((size_t)copies, sizeof(*expected));
    bool ok = cases && sources && expected;
    for (int i = 0; ok && i < copies; i++) {
        K8sScaleCase *item = &cases[i];
        snprintf(item->id, sizeof(item->id), "m%04d", i);
        snprintf(item->account, sizeof(item->account), "%s/account.yaml", item->id);
        snprintf(item->secret, sizeof(item->secret), "%s/secret.yaml", item->id);
        snprintf(item->configuration, sizeof(item->configuration), "%s/kustomization.yaml",
                 item->id);
        snprintf(item->caller, sizeof(item->caller),
                 "%s.account.yaml::core/ServiceAccount/app/client", item->id);
        snprintf(item->target, sizeof(item->target), "%s.secret.yaml::core/Secret/app/credentials",
                 item->id);
        sources[3 * i] = (RQ_Source){.path = item->account, .text = k8s_account};
        sources[3 * i + 1] = (RQ_Source){.path = item->secret, .text = k8s_secret};
        sources[3 * i + 2] = (RQ_Source){.path = item->configuration, .text = k8s_config};
        expected[i] = (RQ_Expected){
            .case_id = item->id,
            .site = {.file = item->account, .caller = item->caller, .needle = "credentials"},
            .relation = "USAGE",
            .target = {.file = item->secret,
                       .suffix = item->target,
                       .label = "Resource",
                       .declaration_line = 2,
                       .declaration_text = "kind: Secret"}};
        for (int j = 0; j < 3; j++)
            ok &= th_write_file(TH_PATH(directory, sources[3 * i + j].path),
                                sources[3 * i + j].text) == 0;
    }
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(directory, database, CBM_MODE_FULL) : NULL;
    if (pipeline)
        cbm_pipeline_set_persistence(pipeline, false);
    uint64_t before = atomic_load_explicit(&g_k8s_proof_clone_work, memory_order_relaxed);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "k8sscale")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    *work = atomic_load_explicit(&g_k8s_proof_clone_work, memory_order_relaxed) - before;
    cbm_store_t *store = rc == 0 ? cbm_store_open_path_query(database) : NULL;
    ok = store && rq_check(store, "k8sscale", "k8s-proof-scaling", sources, (size_t)copies * 3,
                           expected, (size_t)copies, NULL, 0, report, stderr);
    if (store)
        cbm_store_close(store);
    cbm_pipeline_free(pipeline);
    free(expected);
    free(sources);
    free(cases);
    th_rmtree(directory);
    return ok;
}

TEST(resolver_k8s_shared_configuration_proof_work_is_linear) {
    static const char *const keys[] = {"CBM_WORKERS", "CBM_INDEX_SINGLE_THREAD"};
    char *saved[2] = {NULL, NULL};
    bool ready = true;
    for (int i = 0; i < 2; i++) {
        const char *old = getenv(keys[i]);
        saved[i] = old ? strdup(old) : NULL;
        ready &= !old || saved[i] != NULL;
    }
    uint64_t work[2] = {0};
    RQ_Report reports[2] = {{0}};
    bool runs[2] = {false, false};
    int configured = -1, restored = 0;
    enum { BASE_COPIES = 64 };
    if (ready) {
        configured = cbm_setenv(keys[0], "4", 1);
        configured |= cbm_unsetenv(keys[1]);
        if (configured == 0) {
            runs[0] = k8s_test_proof_scaling_run(BASE_COPIES, &work[0], &reports[0]);
            runs[1] = k8s_test_proof_scaling_run(2 * BASE_COPIES, &work[1], &reports[1]);
        }
        for (int i = 0; i < 2; i++)
            restored |= saved[i] ? cbm_setenv(keys[i], saved[i], 1) : cbm_unsetenv(keys[i]);
    }
    for (int i = 0; i < 2; i++)
        free(saved[i]);
    fprintf(stderr, "[K8S-RQ] proof clone work %llu -> %llu\n", (unsigned long long)work[0],
            (unsigned long long)work[1]);
    ASSERT(ready);
    ASSERT_EQ(configured, 0);
    ASSERT_EQ(restored, 0);
    ASSERT(runs[0] && runs[1]);
    ASSERT_EQ(reports[0].genuine, BASE_COPIES);
    ASSERT_EQ(reports[0].found, BASE_COPIES);
    ASSERT_EQ(reports[1].genuine, 2 * BASE_COPIES);
    ASSERT_EQ(reports[1].found, 2 * BASE_COPIES);
    ASSERT_EQ(reports[0].missing + reports[1].missing, 0);
    ASSERT_GT(work[0], 0);
    ASSERT_GT(work[1], work[0]);
    /* Shared ancestors cost about 2x when doubled; per-edge census cloning
     * costs about 4x. No wall-clock or persisted-node count gates this check. */
    ASSERT_LT(work[1], 3 * work[0]);
    PASS();
}

SUITE(resolver_k8s) {
    RUN_TEST(resolver_k8s_shared_configuration_proof_work_is_linear);
    RUN_TEST(resolver_k8s_unknown_memberships_never_close_other_builds);
    RUN_TEST(resolver_k8s_all_core_object_kinds_have_occurrence_proofs);
    RUN_TEST(resolver_k8s_flat_build_object_quality_sequential_and_parallel);
    RUN_TEST(resolver_k8s_raw_declarations_and_resource_members_have_exact_spans);
}
