/* RED contract for exact-build admission shared by one-shot CLI and daemon. */
#include "test_framework.h"
#include "test_helpers.h"

#include "daemon/ipc.h"
#include "daemon/service.h"
#include "daemon/version_cohort.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "foundation/subprocess.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

enum { VERSION_COHORT_TEST_PATH_CAP = 1024 };

static const char VERSION_COHORT_BUILD_A[] =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char VERSION_COHORT_BUILD_B[] =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
static const char VERSION_COHORT_CACHE_A[] =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
static const char VERSION_COHORT_CACHE_B[] =
    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

typedef struct {
    char parent[VERSION_COHORT_TEST_PATH_CAP];
    cbm_daemon_ipc_endpoint_t *endpoint;
} version_cohort_fixture_t;

typedef struct {
    cbm_version_cohort_manager_t *manager;
    uint64_t deadline_ms;
    atomic_int callback_count;
    atomic_bool callback_seen;
    atomic_bool finished;
    cbm_version_cohort_status_t status;
    cbm_version_cohort_quiesce_result_t quiesce_result;
    cbm_version_cohort_lease_t *lease;
} version_cohort_mutation_wait_t;

static cbm_daemon_build_identity_t version_cohort_identity(const char *version, const char *build) {
    cbm_daemon_build_identity_t identity = {
        .semantic_version = version,
        .build_fingerprint = build,
        .cache_fingerprint = VERSION_COHORT_CACHE_A,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return identity;
}

static bool version_cohort_fixture_start(version_cohort_fixture_t *fixture, const char *tag) {
    memset(fixture, 0, sizeof(*fixture));
    int written = snprintf(fixture->parent, sizeof(fixture->parent),
                           "%s/cbm-version-cohort-%s-XXXXXX", cbm_tmpdir(), tag);
    if (written <= 0 || written >= (int)sizeof(fixture->parent) || !cbm_mkdtemp(fixture->parent)) {
        return false;
    }
    fixture->endpoint = cbm_daemon_ipc_endpoint_new("0123456789abcdef", fixture->parent);
    return fixture->endpoint != NULL;
}

static void version_cohort_release(cbm_version_cohort_lease_t **lease) {
    while (lease && *lease && cbm_version_cohort_lease_release(lease) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
}

static void version_cohort_manager_close(cbm_version_cohort_manager_t **manager) {
    while (manager && *manager &&
           cbm_version_cohort_manager_free(manager) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
}

static void version_cohort_fixture_finish(version_cohort_fixture_t *fixture) {
    cbm_daemon_ipc_endpoint_free(fixture->endpoint);
    if (fixture->parent[0]) {
        (void)th_rmtree(fixture->parent);
    }
    memset(fixture, 0, sizeof(*fixture));
}

static void version_cohort_mutation_wait_init(version_cohort_mutation_wait_t *wait,
                                              cbm_version_cohort_manager_t *manager,
                                              uint64_t deadline_ms) {
    memset(wait, 0, sizeof(*wait));
    wait->manager = manager;
    wait->deadline_ms = deadline_ms;
    wait->status = CBM_VERSION_COHORT_IO;
    wait->quiesce_result = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    atomic_init(&wait->callback_count, 0);
    atomic_init(&wait->callback_seen, false);
    atomic_init(&wait->finished, false);
}

static cbm_version_cohort_quiesce_result_t version_cohort_test_request_quiesce(void *context) {
    version_cohort_mutation_wait_t *wait = context;
    (void)atomic_fetch_add_explicit(&wait->callback_count, 1, memory_order_relaxed);
    atomic_store_explicit(&wait->callback_seen, true, memory_order_release);
    return CBM_VERSION_COHORT_QUIESCE_REQUESTED;
}

static void *version_cohort_mutation_wait_thread(void *context) {
    version_cohort_mutation_wait_t *wait = context;
    wait->status = cbm_version_cohort_reserve_for_mutation(
        wait->manager, wait->deadline_ms, version_cohort_test_request_quiesce, wait,
        &wait->quiesce_result, &wait->lease);
    atomic_store_explicit(&wait->finished, true, memory_order_release);
    return NULL;
}

static bool version_cohort_wait_for_atomic(atomic_bool *value, uint64_t deadline_ms) {
    while (!atomic_load_explicit(value, memory_order_acquire) && cbm_now_ms() < deadline_ms) {
        cbm_usleep(1000);
    }
    return atomic_load_explicit(value, memory_order_acquire);
}

TEST(version_cohort_shares_exact_build_rejects_conflict_and_turns_over) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "matrix"));
    cbm_version_cohort_manager_t *first = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *second = cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);

    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_build_identity_t build_b = version_cohort_identity("2.5.0", VERSION_COHORT_BUILD_B);
    cbm_version_cohort_lease_t *a_first = NULL;
    cbm_version_cohort_lease_t *a_second = NULL;
    cbm_version_cohort_lease_t *b_lease = NULL;
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(cbm_version_cohort_acquire(first, &build_a, UINT64_MAX, &a_first, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(a_first);
    ASSERT_EQ(cbm_version_cohort_acquire(second, &build_a, UINT64_MAX, &a_second, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(a_second);
    ASSERT_EQ(cbm_version_cohort_acquire(second, &build_b, cbm_now_ms(), &b_lease, &conflict),
              CBM_VERSION_COHORT_CONFLICT);
    ASSERT_NULL(b_lease);
    ASSERT_EQ(conflict.status, CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_STR_EQ(conflict.active_version, "2.4.0");
    ASSERT_STR_EQ(conflict.requested_version, "2.5.0");

    version_cohort_release(&a_second);
    version_cohort_release(&a_first);
    ASSERT_EQ(cbm_version_cohort_acquire(second, &build_b, UINT64_MAX, &b_lease, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(b_lease);

    version_cohort_release(&b_lease);
    version_cohort_manager_close(&second);
    version_cohort_manager_close(&first);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_rejects_same_hash_with_different_abi) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "abi"));
    cbm_version_cohort_manager_t *first = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *second = cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    cbm_daemon_build_identity_t active = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_build_identity_t requested = active;
    requested.feature_abi++;
    cbm_version_cohort_lease_t *active_lease = NULL;
    cbm_version_cohort_lease_t *requested_lease = NULL;
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(cbm_version_cohort_acquire(first, &active, UINT64_MAX, &active_lease, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_EQ(
        cbm_version_cohort_acquire(second, &requested, cbm_now_ms(), &requested_lease, &conflict),
        CBM_VERSION_COHORT_CONFLICT);
    ASSERT_NULL(requested_lease);
    ASSERT_EQ(conflict.status, CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT);

    version_cohort_release(&active_lease);
    version_cohort_manager_close(&second);
    version_cohort_manager_close(&first);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

/* A cohort identity without a canonical cache fingerprint would reintroduce
 * an unscoped namespace that can silently share with another cache-less
 * process. Cohort admission must fail closed even though the stable daemon
 * HELLO envelope intentionally remains cache-agnostic. */
TEST(version_cohort_rejects_missing_cache_fingerprint) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "missing-cache"));
    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(manager);
    cbm_daemon_build_identity_t identity = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    identity.cache_fingerprint = NULL;
    cbm_version_cohort_lease_t *lease = NULL;
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(cbm_version_cohort_acquire(manager, &identity, cbm_now_ms(), &lease, &conflict),
              CBM_VERSION_COHORT_UNSAFE);
    ASSERT_NULL(lease);

    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

/* One account has one daemon and therefore one canonical cache generation.
 * A second exact-build process with another cache root must fail before it can
 * join lifetime ownership or request activation against the wrong storage. */
TEST(version_cohort_rejects_exact_build_with_different_cache_root) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "cache-root"));
    cbm_version_cohort_manager_t *first = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *second = cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);

    cbm_daemon_build_identity_t active = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_build_identity_t requested = active;
    active.cache_fingerprint = VERSION_COHORT_CACHE_A;
    requested.cache_fingerprint = VERSION_COHORT_CACHE_B;
    cbm_version_cohort_lease_t *active_lease = NULL;
    cbm_version_cohort_lease_t *requested_lease = NULL;
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(cbm_version_cohort_acquire(first, &active, UINT64_MAX, &active_lease, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_EQ(
        cbm_version_cohort_acquire(second, &requested, cbm_now_ms(), &requested_lease, &conflict),
        CBM_VERSION_COHORT_CONFLICT);
    ASSERT_NULL(requested_lease);
    ASSERT_EQ(conflict.status, CBM_DAEMON_HELLO_CACHE_CONFLICT);
    ASSERT_STR_EQ(conflict.active_cache_fingerprint, VERSION_COHORT_CACHE_A);
    ASSERT_STR_EQ(conflict.requested_cache_fingerprint, VERSION_COHORT_CACHE_B);
    char message[CBM_DAEMON_CONFLICT_MESSAGE_SIZE];
    ASSERT_TRUE(cbm_daemon_conflict_format(&conflict, message, sizeof(message)));
    ASSERT_NOT_NULL(strstr(message, "cache"));

    version_cohort_release(&active_lease);
    version_cohort_manager_close(&second);
    version_cohort_manager_close(&first);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_exclusive_activation_blocks_and_is_blocked_by_participants) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "activation"));
    cbm_version_cohort_manager_t *participant_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *activation_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(participant_manager);
    ASSERT_NOT_NULL(activation_manager);
    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_version_cohort_lease_t *participant = NULL;
    cbm_version_cohort_lease_t *activation = NULL;
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(cbm_version_cohort_acquire(participant_manager, &build_a, UINT64_MAX, &participant,
                                         &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_EQ(cbm_version_cohort_reserve_exclusive(activation_manager, cbm_now_ms(), &activation),
              CBM_VERSION_COHORT_BUSY);
    ASSERT_NULL(activation);
    version_cohort_release(&participant);

    ASSERT_EQ(cbm_version_cohort_reserve_exclusive(activation_manager, UINT64_MAX, &activation),
              CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(activation);
    ASSERT_EQ(cbm_version_cohort_acquire(participant_manager, &build_a, cbm_now_ms(), &participant,
                                         &conflict),
              CBM_VERSION_COHORT_BUSY);
    ASSERT_NULL(participant);
    version_cohort_release(&activation);

    ASSERT_EQ(cbm_version_cohort_acquire(participant_manager, &build_a, UINT64_MAX, &participant,
                                         &conflict),
              CBM_VERSION_COHORT_OK);
    version_cohort_release(&participant);
    version_cohort_manager_close(&activation_manager);
    version_cohort_manager_close(&participant_manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_mutation_intent_fails_new_admission_and_spans_lease) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "mutation-intent"));
    cbm_version_cohort_manager_t *participant_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *mutation_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *contender_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(participant_manager);
    ASSERT_NOT_NULL(mutation_manager);
    ASSERT_NOT_NULL(contender_manager);

    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_lease_t *participant = NULL;
    cbm_version_cohort_lease_t *contender = NULL;
    ASSERT_EQ(cbm_version_cohort_acquire(participant_manager, &build_a, UINT64_MAX, &participant,
                                         &conflict),
              CBM_VERSION_COHORT_OK);
    cbm_version_cohort_maintenance_presence_t before =
        cbm_version_cohort_maintenance_presence(contender_manager);

    version_cohort_mutation_wait_t wait;
    version_cohort_mutation_wait_init(&wait, mutation_manager, cbm_now_ms() + 5000U);
    cbm_thread_t thread;
    bool started = cbm_thread_create(&thread, 0, version_cohort_mutation_wait_thread, &wait) == 0;
    bool callback_seen =
        started && version_cohort_wait_for_atomic(&wait.callback_seen, cbm_now_ms() + 2000U);
    cbm_version_cohort_maintenance_presence_t during =
        callback_seen ? cbm_version_cohort_maintenance_presence(contender_manager)
                      : CBM_VERSION_COHORT_MAINTENANCE_IO;
    cbm_version_cohort_status_t racing_status =
        callback_seen ? cbm_version_cohort_acquire(contender_manager, &build_a, UINT64_MAX,
                                                   &contender, &conflict)
                      : CBM_VERSION_COHORT_IO;
    bool racing_lease_absent = contender == NULL;
    bool still_draining =
        callback_seen && !atomic_load_explicit(&wait.finished, memory_order_acquire);

    version_cohort_release(&contender);
    version_cohort_release(&participant);
    bool finished = started && version_cohort_wait_for_atomic(&wait.finished, cbm_now_ms() + 5500U);
    bool joined = started && cbm_thread_join(&thread) == 0;
    cbm_version_cohort_maintenance_presence_t retained =
        finished && wait.lease ? cbm_version_cohort_maintenance_presence(contender_manager)
                               : CBM_VERSION_COHORT_MAINTENANCE_IO;
    cbm_version_cohort_status_t retained_admission_status =
        finished && wait.lease ? cbm_version_cohort_acquire(contender_manager, &build_a, UINT64_MAX,
                                                            &contender, &conflict)
                               : CBM_VERSION_COHORT_IO;
    bool retained_admission_absent = contender == NULL;
    version_cohort_release(&contender);
    version_cohort_release(&wait.lease);
    cbm_version_cohort_maintenance_presence_t after =
        cbm_version_cohort_maintenance_presence(contender_manager);
    cbm_version_cohort_status_t post_status =
        cbm_version_cohort_acquire(contender_manager, &build_a, UINT64_MAX, &contender, &conflict);

    version_cohort_release(&contender);
    version_cohort_manager_close(&contender_manager);
    version_cohort_manager_close(&mutation_manager);
    version_cohort_manager_close(&participant_manager);
    version_cohort_fixture_finish(&fixture);

    ASSERT_EQ(before, CBM_VERSION_COHORT_MAINTENANCE_ABSENT);
    ASSERT_TRUE(started);
    ASSERT_TRUE(callback_seen);
    ASSERT_EQ(during, CBM_VERSION_COHORT_MAINTENANCE_REQUESTED);
    ASSERT_EQ(racing_status, CBM_VERSION_COHORT_BUSY);
    ASSERT_TRUE(racing_lease_absent);
    ASSERT_TRUE(still_draining);
    ASSERT_TRUE(finished);
    ASSERT_TRUE(joined);
    ASSERT_EQ(wait.status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(wait.quiesce_result, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_EQ(atomic_load_explicit(&wait.callback_count, memory_order_relaxed), 1);
    ASSERT_EQ(retained, CBM_VERSION_COHORT_MAINTENANCE_REQUESTED);
    ASSERT_EQ(retained_admission_status, CBM_VERSION_COHORT_BUSY);
    ASSERT_TRUE(retained_admission_absent);
    ASSERT_EQ(after, CBM_VERSION_COHORT_MAINTENANCE_ABSENT);
    ASSERT_EQ(post_status, CBM_VERSION_COHORT_OK);
    PASS();
}

TEST(version_cohort_mutation_waits_for_every_lifetime_participant) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "mutation-drain"));
    cbm_version_cohort_manager_t *first_manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *second_manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *mutation_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(first_manager);
    ASSERT_NOT_NULL(second_manager);
    ASSERT_NOT_NULL(mutation_manager);

    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_lease_t *first = NULL;
    cbm_version_cohort_lease_t *second = NULL;
    ASSERT_EQ(cbm_version_cohort_acquire(first_manager, &build_a, UINT64_MAX, &first, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_EQ(cbm_version_cohort_acquire(second_manager, &build_a, UINT64_MAX, &second, &conflict),
              CBM_VERSION_COHORT_OK);

    version_cohort_mutation_wait_t wait;
    version_cohort_mutation_wait_init(&wait, mutation_manager, cbm_now_ms() + 5000U);
    cbm_thread_t thread;
    bool started = cbm_thread_create(&thread, 0, version_cohort_mutation_wait_thread, &wait) == 0;
    bool callback_seen =
        started && version_cohort_wait_for_atomic(&wait.callback_seen, cbm_now_ms() + 2000U);
    version_cohort_release(&first);
    cbm_usleep(20000);
    bool finished_after_one = atomic_load_explicit(&wait.finished, memory_order_acquire);
    version_cohort_release(&second);
    bool finished = started && version_cohort_wait_for_atomic(&wait.finished, cbm_now_ms() + 5500U);
    bool joined = started && cbm_thread_join(&thread) == 0;

    version_cohort_release(&wait.lease);
    version_cohort_manager_close(&mutation_manager);
    version_cohort_manager_close(&second_manager);
    version_cohort_manager_close(&first_manager);
    version_cohort_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(callback_seen);
    ASSERT_FALSE(finished_after_one);
    ASSERT_TRUE(finished);
    ASSERT_TRUE(joined);
    ASSERT_EQ(wait.status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(wait.quiesce_result, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_EQ(atomic_load_explicit(&wait.callback_count, memory_order_relaxed), 1);
    PASS();
}

TEST(version_cohort_mutation_timeout_releases_all_guards) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "mutation-timeout"));
    cbm_version_cohort_manager_t *participant_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *mutation_manager =
        cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_manager_t *probe_manager = cbm_version_cohort_manager_new(fixture.endpoint);
    ASSERT_NOT_NULL(participant_manager);
    ASSERT_NOT_NULL(mutation_manager);
    ASSERT_NOT_NULL(probe_manager);

    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_lease_t *participant = NULL;
    cbm_version_cohort_lease_t *mutation = NULL;
    cbm_version_cohort_lease_t *probe = NULL;
    ASSERT_EQ(cbm_version_cohort_acquire(participant_manager, &build_a, UINT64_MAX, &participant,
                                         &conflict),
              CBM_VERSION_COHORT_OK);

    version_cohort_mutation_wait_t callback;
    version_cohort_mutation_wait_init(&callback, mutation_manager, cbm_now_ms() + 25U);
    cbm_version_cohort_quiesce_result_t quiesce_result = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    cbm_version_cohort_status_t timeout_status = cbm_version_cohort_reserve_for_mutation(
        mutation_manager, callback.deadline_ms, version_cohort_test_request_quiesce, &callback,
        &quiesce_result, &mutation);
    bool no_mutation_authority = mutation == NULL;
    cbm_version_cohort_maintenance_presence_t after_timeout =
        cbm_version_cohort_maintenance_presence(probe_manager);
    cbm_version_cohort_status_t admission_status =
        cbm_version_cohort_acquire(probe_manager, &build_a, UINT64_MAX, &probe, &conflict);

    version_cohort_release(&probe);
    version_cohort_release(&participant);
    cbm_version_cohort_quiesce_result_t invalid_result = CBM_VERSION_COHORT_QUIESCE_REQUESTED;
    cbm_version_cohort_status_t unbounded_status = cbm_version_cohort_reserve_for_mutation(
        mutation_manager, UINT64_MAX, version_cohort_test_request_quiesce, &callback,
        &invalid_result, &mutation);
    bool unbounded_lease_absent = mutation == NULL;
    cbm_version_cohort_quiesce_result_t retry_result = CBM_VERSION_COHORT_QUIESCE_REQUESTED;
    cbm_version_cohort_status_t retry_status = cbm_version_cohort_reserve_for_mutation(
        mutation_manager, cbm_now_ms() + 250U, version_cohort_test_request_quiesce, &callback,
        &retry_result, &mutation);
    cbm_version_cohort_maintenance_presence_t during_retry =
        mutation ? cbm_version_cohort_maintenance_presence(probe_manager)
                 : CBM_VERSION_COHORT_MAINTENANCE_IO;
    int callback_count_after_retry =
        atomic_load_explicit(&callback.callback_count, memory_order_relaxed);

    version_cohort_release(&mutation);
    cbm_version_cohort_maintenance_presence_t after_retry =
        cbm_version_cohort_maintenance_presence(probe_manager);
    version_cohort_manager_close(&probe_manager);
    version_cohort_manager_close(&mutation_manager);
    version_cohort_manager_close(&participant_manager);
    version_cohort_fixture_finish(&fixture);

    ASSERT_EQ(timeout_status, CBM_VERSION_COHORT_BUSY);
    ASSERT_TRUE(no_mutation_authority);
    ASSERT_EQ(quiesce_result, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_EQ(atomic_load_explicit(&callback.callback_count, memory_order_relaxed), 1);
    ASSERT_EQ(after_timeout, CBM_VERSION_COHORT_MAINTENANCE_ABSENT);
    ASSERT_EQ(admission_status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(unbounded_status, CBM_VERSION_COHORT_UNSAFE);
    ASSERT_EQ(invalid_result, CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED);
    ASSERT_TRUE(unbounded_lease_absent);
    ASSERT_EQ(retry_status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(retry_result, CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED);
    ASSERT_EQ(during_retry, CBM_VERSION_COHORT_MAINTENANCE_REQUESTED);
    ASSERT_EQ(callback_count_after_retry, 1);
    ASSERT_EQ(after_retry, CBM_VERSION_COHORT_MAINTENANCE_ABSENT);
    PASS();
}

TEST(version_cohort_does_not_repurpose_daemon_startup_lock_for_lifetime) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "startup-independent"));
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    ASSERT_EQ(cbm_daemon_ipc_startup_lock_try_acquire(fixture.endpoint, &startup), 1);
    ASSERT_NOT_NULL(startup);
    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_daemon_build_identity_t build_a = version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
    cbm_version_cohort_lease_t *lease = NULL;
    cbm_daemon_conflict_t conflict;
    ASSERT_NOT_NULL(manager);
    ASSERT_EQ(cbm_version_cohort_acquire(manager, &build_a, UINT64_MAX, &lease, &conflict),
              CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(lease);

    cbm_daemon_ipc_startup_lock_release(&startup);
    version_cohort_release(&lease);
    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_distinguishes_coordinated_daemon_without_connecting) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "daemon-marker"));
    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_version_cohort_daemon_claim_t *claim = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    ASSERT_NOT_NULL(manager);

    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_ABSENT);

    /* Startup is also part of the migration boundary: on POSIX the legacy and
     * current startup locks are the same; on Windows current startup retains
     * the security-validated legacy mutex as an interlock. */
    ASSERT_EQ(cbm_daemon_ipc_startup_lock_try_acquire(fixture.endpoint, &startup), 1);
    ASSERT_NOT_NULL(startup);
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_UNCOORDINATED);
    cbm_daemon_ipc_startup_lock_release(&startup);
    startup = NULL;
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_ABSENT);

    /* A pre-cohort daemon owns the stable daemon lifetime reservation but
     * cannot own the new crash-released coordination marker. The local CLI
     * must fail closed without opening a protocol connection. */
    ASSERT_EQ(cbm_daemon_ipc_lifetime_reservation_try_acquire(fixture.endpoint, &lifetime), 1);
    ASSERT_NOT_NULL(lifetime);
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_UNCOORDINATED);

    ASSERT_EQ(cbm_version_cohort_daemon_claim_acquire(manager, &claim), CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(claim);
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_COORDINATED);

    /* RED for daemon turnover: listener/lifetime teardown precedes final
     * application/log cleanup. The still-held exact-generation marker is
     * authoritative during that window, so a new bootstrap waits instead of
     * starting a replacement against the old daemon's live state. */
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_COORDINATED);

    ASSERT_EQ(cbm_version_cohort_daemon_claim_release(&claim), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(claim);
    ASSERT_EQ(cbm_version_cohort_daemon_presence(manager, fixture.endpoint),
              CBM_VERSION_COHORT_DAEMON_ABSENT);

    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_transition_presence_is_authoritative_and_marker_checked) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "transition-presence"));
    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_daemon_ipc_local_transition_t *transition = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    cbm_version_cohort_daemon_claim_t *claim = NULL;
    ASSERT_NOT_NULL(manager);

    ASSERT_EQ(cbm_daemon_ipc_local_transition_try_acquire(fixture.endpoint, &transition), 1);
    ASSERT_NOT_NULL(transition);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_UNSAFE);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_seal_legacy(transition), 1);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_ABSENT);
    ASSERT_TRUE(cbm_daemon_ipc_local_transition_release(&transition));
    ASSERT_NULL(transition);

    ASSERT_EQ(cbm_daemon_ipc_lifetime_reservation_try_acquire(fixture.endpoint, &lifetime), 1);
    ASSERT_NOT_NULL(lifetime);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_try_acquire(fixture.endpoint, &transition), 1);
    ASSERT_NOT_NULL(transition);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_seal_legacy(transition), 1);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_UNCOORDINATED);

    ASSERT_EQ(cbm_version_cohort_daemon_claim_acquire(manager, &claim), CBM_VERSION_COHORT_OK);
    ASSERT_NOT_NULL(claim);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_COORDINATED);

    ASSERT_EQ(cbm_version_cohort_daemon_claim_release(&claim), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(claim);
    ASSERT_TRUE(cbm_daemon_ipc_local_transition_release(&transition));
    ASSERT_NULL(transition);
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_transition_shutdown_order_has_no_false_conflict) {
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "transition-shutdown"));
    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_daemon_ipc_lifetime_reservation_t *lifetime = NULL;
    cbm_version_cohort_daemon_claim_t *claim = NULL;
    cbm_daemon_ipc_local_transition_t *transition = NULL;
    ASSERT_NOT_NULL(manager);

    ASSERT_EQ(cbm_daemon_ipc_lifetime_reservation_try_acquire(fixture.endpoint, &lifetime), 1);
    ASSERT_EQ(cbm_version_cohort_daemon_claim_acquire(manager, &claim), CBM_VERSION_COHORT_OK);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_try_acquire(fixture.endpoint, &transition), 1);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_seal_legacy(transition), 1);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_COORDINATED);

    /* Host teardown closes listener/lifetime before its daemon marker. The
     * overlap is still coordinated, never the pre-cohort conflict state. */
    cbm_daemon_ipc_lifetime_reservation_release(lifetime);
    lifetime = NULL;
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_COORDINATED);
    ASSERT_EQ(cbm_version_cohort_daemon_claim_release(&claim), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_ABSENT);

    ASSERT_TRUE(cbm_daemon_ipc_local_transition_release(&transition));
    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
}

TEST(version_cohort_presence_recovers_current_posix_listener_crash) {
#ifdef _WIN32
    PASS();
#else
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "daemon-crash"));
    int ready_pipe[2] = {-1, -1};
    ASSERT_EQ(pipe(ready_pipe), 0);
    pid_t child = fork();
    if (child == 0) {
        (void)close(ready_pipe[0]);
        cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen(fixture.endpoint);
        char ready = listener ? 'R' : 'E';
        ssize_t reported = write(ready_pipe[1], &ready, 1);
        (void)close(ready_pipe[1]);
        /* Simulate a process crash: no listener_close and therefore no
         * userspace socket/identity cleanup. */
        _exit(listener && reported == 1 ? 0 : 1);
    }
    ASSERT_GT(child, 0);
    (void)close(ready_pipe[1]);
    char ready = 0;
    ASSERT_EQ(read(ready_pipe[0], &ready, 1), 1);
    (void)close(ready_pipe[0]);
    ASSERT_EQ(ready, 'R');
    int child_status = 0;
    ASSERT_EQ(waitpid(child, &child_status, 0), child);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    ASSERT_EQ(cbm_daemon_ipc_lifetime_reservation_probe(fixture.endpoint), 0);
    ASSERT_EQ(cbm_daemon_ipc_endpoint_probe(fixture.endpoint, 1), 1);

    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_daemon_ipc_local_transition_t *transition = NULL;
    ASSERT_NOT_NULL(manager);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_try_acquire(fixture.endpoint, &transition), 1);
    ASSERT_NOT_NULL(transition);
    ASSERT_EQ(cbm_daemon_ipc_local_transition_seal_legacy(transition), 1);
    ASSERT_EQ(
        cbm_version_cohort_daemon_presence_under_transition(manager, fixture.endpoint, transition),
        CBM_VERSION_COHORT_DAEMON_ABSENT);
    ASSERT_EQ(cbm_daemon_ipc_endpoint_probe(fixture.endpoint, 1), 0);

    ASSERT_TRUE(cbm_daemon_ipc_local_transition_release(&transition));
    ASSERT_NULL(transition);
    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
#endif
}

TEST(version_cohort_crash_releases_process_lifetime_lease) {
#ifdef _WIN32
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "crash-win"));
    char ready_path[VERSION_COHORT_TEST_PATH_CAP];
    char self[MAX_PATH];
    DWORD self_length = GetModuleFileNameA(NULL, self, sizeof(self));
    int ready_length = snprintf(ready_path, sizeof(ready_path), "%s/ready", fixture.parent);
    bool launch_ready = self_length > 0 && self_length < sizeof(self) && ready_length > 0 &&
                        ready_length < (int)sizeof(ready_path);
    const char *const argv[] = {
        self, "__cbm_version_cohort_crash_holder", "0123456789abcdef", fixture.parent, ready_path,
        NULL,
    };
    cbm_proc_opts_t options = {
        .bin = self,
        .argv = argv,
        .quiet_timeout_ms = 2000,
        .cancel_grace_ms = 1,
    };
    cbm_subprocess_t *child = NULL;
    int spawn_status = launch_ready ? cbm_subprocess_spawn(&options, &child) : -1;
    bool ready = false;
    bool terminal = false;
    cbm_proc_result_t process_result = {0};
    uint64_t ready_deadline = cbm_now_ms() + 5000U;
    while (child && cbm_now_ms() < ready_deadline) {
        cbm_proc_poll_t poll = cbm_subprocess_poll(child, &process_result);
        if (poll == CBM_PROC_POLL_TERMINAL) {
            terminal = true;
            break;
        }
        if (poll == CBM_PROC_POLL_ERROR) {
            break;
        }
        FILE *marker = cbm_fopen(ready_path, "rb");
        if (marker) {
            ready = fgetc(marker) == 'R';
            (void)fclose(marker);
        }
        if (ready) {
            break;
        }
        cbm_usleep(1000);
    }

    cbm_version_cohort_manager_t *manager =
        ready ? cbm_version_cohort_manager_new(fixture.endpoint) : NULL;
    cbm_daemon_build_identity_t build_b = version_cohort_identity("2.5.0", VERSION_COHORT_BUILD_B);
    cbm_version_cohort_lease_t *lease = NULL;
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_status_t conflict_status =
        manager ? cbm_version_cohort_acquire(manager, &build_b, cbm_now_ms(), &lease, &conflict)
                : CBM_VERSION_COHORT_IO;
    bool conflict_lease_absent = lease == NULL;
    version_cohort_release(&lease);
    bool cancel_requested = child && !terminal && cbm_subprocess_request_cancel(child);
    uint64_t terminal_deadline = cbm_now_ms() + 5000U;
    while (child && !terminal && cbm_now_ms() < terminal_deadline) {
        cbm_proc_poll_t poll = cbm_subprocess_poll(child, &process_result);
        if (poll == CBM_PROC_POLL_TERMINAL) {
            terminal = true;
            break;
        }
        if (poll == CBM_PROC_POLL_ERROR) {
            break;
        }
        cbm_usleep(1000);
    }
    /* The turnover acquire gets its own conflict record: acquire zeroes its
     * conflict_out on entry, so reusing `conflict` here would erase the
     * probe's VERSION_CONFLICT detail before the assertions read it. */
    cbm_daemon_conflict_t turnover_conflict;
    cbm_version_cohort_status_t turnover_status =
        manager && terminal
            ? cbm_version_cohort_acquire(manager, &build_b, UINT64_MAX, &lease, &turnover_conflict)
            : CBM_VERSION_COHORT_IO;

    version_cohort_release(&lease);
    version_cohort_manager_close(&manager);
    if (child && terminal) {
        cbm_subprocess_destroy(child);
        child = NULL;
    }
    (void)cbm_unlink(ready_path);
    version_cohort_fixture_finish(&fixture);

    ASSERT_TRUE(launch_ready);
    ASSERT_EQ(spawn_status, 0);
    ASSERT_TRUE(ready);
    ASSERT_EQ(conflict_status, CBM_VERSION_COHORT_CONFLICT);
    ASSERT_TRUE(conflict_lease_absent);
    ASSERT_EQ(conflict.status, CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_TRUE(cancel_requested);
    ASSERT_TRUE(terminal);
    ASSERT_TRUE(process_result.tree_quiesced);
    ASSERT_FALSE(process_result.supervision_failed);
    ASSERT_EQ(process_result.outcome, CBM_PROC_KILLED);
    ASSERT_EQ(turnover_status, CBM_VERSION_COHORT_OK);
    PASS();
#else
    version_cohort_fixture_t fixture;
    ASSERT_TRUE(version_cohort_fixture_start(&fixture, "crash"));
    int ready_pipe[2] = {-1, -1};
    int command_pipe[2] = {-1, -1};
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(command_pipe), 0);
    pid_t child = fork();
    if (child == 0) {
        (void)close(ready_pipe[0]);
        (void)close(command_pipe[1]);
        cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
        cbm_daemon_build_identity_t build_a =
            version_cohort_identity("2.4.0", VERSION_COHORT_BUILD_A);
        cbm_version_cohort_lease_t *lease = NULL;
        cbm_daemon_conflict_t conflict;
        bool acquired = manager && cbm_version_cohort_acquire(manager, &build_a, UINT64_MAX, &lease,
                                                              &conflict) == CBM_VERSION_COHORT_OK;
        char ready = acquired ? 'R' : 'E';
        ssize_t ignored = write(ready_pipe[1], &ready, 1);
        (void)ignored;
        (void)close(ready_pipe[1]);
        char command = 0;
        ssize_t commanded = read(command_pipe[0], &command, 1);
        (void)close(command_pipe[0]);
        _exit(acquired && commanded == 1 && command == 'X' ? 0 : 1);
        /* no release: kernel must drop the lease */
    }
    (void)close(ready_pipe[1]);
    (void)close(command_pipe[0]);
    char ready = 0;
    ssize_t received = read(ready_pipe[0], &ready, 1);
    (void)close(ready_pipe[0]);
    ASSERT_EQ(received, 1);
    ASSERT_EQ(ready, 'R');
    ASSERT_GT(child, 0);

    cbm_version_cohort_manager_t *manager = cbm_version_cohort_manager_new(fixture.endpoint);
    cbm_daemon_build_identity_t build_b = version_cohort_identity("2.5.0", VERSION_COHORT_BUILD_B);
    cbm_version_cohort_lease_t *lease = NULL;
    cbm_daemon_conflict_t conflict;
    ASSERT_NOT_NULL(manager);
    ASSERT_EQ(cbm_version_cohort_acquire(manager, &build_b, cbm_now_ms(), &lease, &conflict),
              CBM_VERSION_COHORT_CONFLICT);
    ASSERT_NULL(lease);
    ASSERT_EQ(conflict.status, CBM_DAEMON_HELLO_VERSION_CONFLICT);

    char exit_command = 'X';
    ASSERT_EQ(write(command_pipe[1], &exit_command, 1), 1);
    (void)close(command_pipe[1]);
    int child_status = 0;
    ASSERT_EQ(waitpid(child, &child_status, 0), child);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);

    ASSERT_EQ(cbm_version_cohort_acquire(manager, &build_b, UINT64_MAX, &lease, &conflict),
              CBM_VERSION_COHORT_OK);
    version_cohort_release(&lease);
    version_cohort_manager_close(&manager);
    version_cohort_fixture_finish(&fixture);
    PASS();
#endif
}

SUITE(version_cohort) {
    RUN_TEST(version_cohort_shares_exact_build_rejects_conflict_and_turns_over);
    RUN_TEST(version_cohort_rejects_same_hash_with_different_abi);
    RUN_TEST(version_cohort_rejects_missing_cache_fingerprint);
    RUN_TEST(version_cohort_rejects_exact_build_with_different_cache_root);
    RUN_TEST(version_cohort_exclusive_activation_blocks_and_is_blocked_by_participants);
    RUN_TEST(version_cohort_mutation_intent_fails_new_admission_and_spans_lease);
    RUN_TEST(version_cohort_mutation_waits_for_every_lifetime_participant);
    RUN_TEST(version_cohort_mutation_timeout_releases_all_guards);
    RUN_TEST(version_cohort_does_not_repurpose_daemon_startup_lock_for_lifetime);
    RUN_TEST(version_cohort_distinguishes_coordinated_daemon_without_connecting);
    RUN_TEST(version_cohort_transition_presence_is_authoritative_and_marker_checked);
    RUN_TEST(version_cohort_transition_shutdown_order_has_no_false_conflict);
    RUN_TEST(version_cohort_presence_recovers_current_posix_listener_crash);
    RUN_TEST(version_cohort_crash_releases_process_lifetime_lease);
}
