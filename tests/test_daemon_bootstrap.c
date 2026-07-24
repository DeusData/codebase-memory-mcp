/* RED contract for early process-role classification. */
#include "test_framework.h"

#include "daemon/bootstrap.h"
#include "daemon/ipc.h"
#include "daemon/service.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
    BOOTSTRAP_TEST_PATH_CAP = 1024,
    BOOTSTRAP_TEST_TIMEOUT_MS = 2000,
    BOOTSTRAP_TEST_SHORT_TIMEOUT_MS = 20,
};

static const char BOOTSTRAP_BUILD_A[] =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char BOOTSTRAP_BUILD_B[] =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

typedef struct {
    char parent[BOOTSTRAP_TEST_PATH_CAP];
    char runtime_dir[BOOTSTRAP_TEST_PATH_CAP];
    cbm_daemon_ipc_endpoint_t *endpoint;
} bootstrap_endpoint_fixture_t;

typedef struct {
    atomic_int cohort_acquire_count;
    atomic_int cohort_release_count;
    atomic_int lock_held;
    atomic_int spawn_count;
    atomic_int probe_count;
    atomic_int lock_attempt_count;
    atomic_int handoff_count;
    atomic_int diagnostic_count;
    atomic_int reserved_probes_remaining;
    atomic_int terminal_probes_remaining;
    atomic_bool available;
    atomic_bool connect_after_reserved;
    atomic_bool connect_requires_unlocked;
    cbm_daemon_bootstrap_probe_status_t forced_probe;
    cbm_version_cohort_status_t forced_cohort;
    char diagnostic[CBM_DAEMON_CONFLICT_MESSAGE_SIZE];
} bootstrap_fake_ops_t;

typedef struct {
    const cbm_daemon_bootstrap_config_t *config;
    const cbm_daemon_bootstrap_ops_t *ops;
    atomic_int *ready;
    atomic_bool *go;
    cbm_daemon_bootstrap_result_t result;
    cbm_daemon_bootstrap_status_t status;
} bootstrap_thread_call_t;

static cbm_daemon_build_identity_t bootstrap_identity(const char *version, const char *build) {
    cbm_daemon_build_identity_t identity = {
        .semantic_version = version,
        .build_fingerprint = build,
        .cache_fingerprint = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return identity;
}

static bool bootstrap_endpoint_fixture_start(bootstrap_endpoint_fixture_t *fixture,
                                             const char *tag) {
    memset(fixture, 0, sizeof(*fixture));
    int written = snprintf(fixture->parent, sizeof(fixture->parent), "%s/cbm-bootstrap-%s-XXXXXX",
                           cbm_tmpdir(), tag);
    if (written <= 0 || written >= (int)sizeof(fixture->parent) || !cbm_mkdtemp(fixture->parent)) {
        return false;
    }
    fixture->endpoint = cbm_daemon_bootstrap_endpoint_new(fixture->parent);
    const char *runtime_dir =
        fixture->endpoint ? cbm_daemon_ipc_endpoint_runtime_dir(fixture->endpoint) : NULL;
    if (!runtime_dir) {
        return false;
    }
    written = snprintf(fixture->runtime_dir, sizeof(fixture->runtime_dir), "%s", runtime_dir);
    return written > 0 && written < (int)sizeof(fixture->runtime_dir);
}

static void bootstrap_endpoint_fixture_finish(bootstrap_endpoint_fixture_t *fixture) {
    cbm_daemon_ipc_endpoint_free(fixture->endpoint);
    if (fixture->runtime_dir[0] != '\0') {
        (void)cbm_rmdir(fixture->runtime_dir);
    }
    if (fixture->parent[0] != '\0') {
        (void)cbm_rmdir(fixture->parent);
    }
    memset(fixture, 0, sizeof(*fixture));
}

static cbm_daemon_bootstrap_probe_status_t bootstrap_fake_probe(
    void *opaque, const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_build_identity_t *identity, uint32_t timeout_ms,
    cbm_daemon_runtime_client_t **client_out, cbm_daemon_runtime_connect_result_t *result_out) {
    (void)endpoint;
    (void)identity;
    (void)timeout_ms;
    bootstrap_fake_ops_t *fake = opaque;
    atomic_fetch_add(&fake->probe_count, 1);
    memset(result_out, 0, sizeof(*result_out));
    *client_out = NULL;
    int reserved_remaining = atomic_load(&fake->reserved_probes_remaining);
    while (reserved_remaining > 0 &&
           !atomic_compare_exchange_weak(&fake->reserved_probes_remaining, &reserved_remaining,
                                         reserved_remaining - 1)) {}
    if (reserved_remaining > 0) {
        if (reserved_remaining == 1 && atomic_load(&fake->connect_after_reserved)) {
            atomic_store(&fake->available, true);
        }
        return CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED;
    }
    int terminal_remaining = atomic_load(&fake->terminal_probes_remaining);
    while (terminal_remaining > 0 &&
           !atomic_compare_exchange_weak(&fake->terminal_probes_remaining, &terminal_remaining,
                                         terminal_remaining - 1)) {}
    if (terminal_remaining > 0) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL;
    }
    if (fake->forced_probe == CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED) {
        return fake->forced_probe;
    }
    if (fake->forced_probe == CBM_DAEMON_BOOTSTRAP_PROBE_CONFLICT) {
        result_out->status = CBM_DAEMON_RUNTIME_CONNECT_CONFLICT;
        result_out->hello_status = CBM_DAEMON_HELLO_BUILD_CONFLICT;
        snprintf(result_out->message, sizeof(result_out->message),
                 "CBM daemon could not start: conflicting versions");
        return fake->forced_probe;
    }
    if (fake->forced_probe == CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL) {
        return fake->forced_probe;
    }
    if (atomic_load(&fake->available) && atomic_load(&fake->connect_requires_unlocked) &&
        atomic_load(&fake->lock_held) != 0) {
        return CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED;
    }
    if (atomic_load(&fake->available)) {
        result_out->status = CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED;
        result_out->hello_status = CBM_DAEMON_HELLO_COMPATIBLE;
        result_out->client_id = 1;
        *client_out = (cbm_daemon_runtime_client_t *)(uintptr_t)1;
        return CBM_DAEMON_BOOTSTRAP_PROBE_CONNECTED;
    }
    return CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE;
}

static cbm_version_cohort_status_t bootstrap_fake_cohort_acquire(
    void *opaque, const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_build_identity_t *identity, uint64_t deadline_ms,
    cbm_daemon_bootstrap_cohort_t *cohort_out, cbm_daemon_conflict_t *conflict_out) {
    (void)endpoint;
    (void)deadline_ms;
    bootstrap_fake_ops_t *fake = opaque;
    atomic_fetch_add(&fake->cohort_acquire_count, 1);
    *cohort_out = NULL;
    memset(conflict_out, 0, sizeof(*conflict_out));
    if (fake->forced_cohort == CBM_VERSION_COHORT_CONFLICT) {
        cbm_daemon_build_identity_t active = bootstrap_identity("2.3.0", BOOTSTRAP_BUILD_A);
        (void)cbm_daemon_hello_compare(&active, identity, conflict_out);
        return fake->forced_cohort;
    }
    if (fake->forced_cohort != CBM_VERSION_COHORT_OK) {
        return fake->forced_cohort;
    }
    *cohort_out = fake;
    return CBM_VERSION_COHORT_OK;
}

static void bootstrap_fake_cohort_release(void *opaque, cbm_daemon_bootstrap_cohort_t cohort) {
    bootstrap_fake_ops_t *fake = opaque;
    if (cohort == fake) {
        atomic_fetch_add(&fake->cohort_release_count, 1);
    }
}

static int bootstrap_fake_lock(void *opaque, const cbm_daemon_ipc_endpoint_t *endpoint,
                               cbm_daemon_bootstrap_lock_t *lock_out) {
    (void)endpoint;
    bootstrap_fake_ops_t *fake = opaque;
    atomic_fetch_add(&fake->lock_attempt_count, 1);
    int expected = 0;
    if (!atomic_compare_exchange_strong(&fake->lock_held, &expected, 1)) {
        return 0;
    }
    *lock_out = fake;
    return 1;
}

static bool bootstrap_fake_unlock(void *opaque, cbm_daemon_bootstrap_lock_t *lock_io) {
    bootstrap_fake_ops_t *fake = opaque;
    if (lock_io && *lock_io == fake) {
        atomic_store(&fake->lock_held, 0);
        *lock_io = NULL;
        return true;
    }
    return lock_io && !*lock_io;
}

static bool bootstrap_fake_handoff(void *opaque, cbm_daemon_bootstrap_lock_t lock) {
    bootstrap_fake_ops_t *fake = opaque;
    if (lock != fake || atomic_load(&fake->lock_held) != 1) {
        return false;
    }
    atomic_fetch_add(&fake->handoff_count, 1);
    return true;
}

static bool bootstrap_fake_spawn(void *opaque, const cbm_daemon_bootstrap_launch_spec_t *spec) {
    bootstrap_fake_ops_t *fake = opaque;
    /* Client bootstrap must only ever spawn the EPHEMERAL two-argument
     * shape; the permanent shape belongs exclusively to `daemon start`. */
    bool exact = spec && spec->argc == 2U && spec->argv[0] &&
                 spec->argv[1] && !spec->argv[2] &&
                 strcmp(spec->argv[1], CBM_DAEMON_INTERNAL_ARG) == 0 && spec->detached &&
                 !spec->inherit_standard_handles && !spec->use_shell &&
                 atomic_load(&fake->handoff_count) > 0 && atomic_load(&fake->lock_held) == 1;
    if (!exact) {
        return false;
    }
    atomic_fetch_add(&fake->spawn_count, 1);
    atomic_store(&fake->available, true);
    return true;
}

static void bootstrap_fake_diagnostic(void *opaque, const char *message) {
    bootstrap_fake_ops_t *fake = opaque;
    atomic_fetch_add(&fake->diagnostic_count, 1);
    snprintf(fake->diagnostic, sizeof(fake->diagnostic), "%s", message ? message : "");
}

static cbm_daemon_bootstrap_ops_t bootstrap_fake_callbacks(bootstrap_fake_ops_t *fake) {
    cbm_daemon_bootstrap_ops_t ops = {
        .context = fake,
        .cohort_acquire = bootstrap_fake_cohort_acquire,
        .cohort_release = bootstrap_fake_cohort_release,
        .probe = bootstrap_fake_probe,
        .startup_lock_try_acquire = bootstrap_fake_lock,
        .startup_lock_prepare_handoff = bootstrap_fake_handoff,
        .startup_lock_release = bootstrap_fake_unlock,
        .spawn_daemon = bootstrap_fake_spawn,
        .visible_diagnostic = bootstrap_fake_diagnostic,
    };
    return ops;
}

static void *bootstrap_thread_execute(void *opaque) {
    bootstrap_thread_call_t *call = opaque;
    atomic_fetch_add(call->ready, 1);
    while (!atomic_load(call->go)) {
        const struct timespec pause = {0, 1000000L};
        (void)cbm_nanosleep(&pause, NULL);
    }
    call->status = cbm_daemon_bootstrap_execute_with_ops(call->config, call->ops, &call->result);
    return NULL;
}

static cbm_daemon_process_role_t classify(int argc, char **argv) {
    return cbm_daemon_process_role(argc, argv);
}

TEST(daemon_bootstrap_classifies_default_and_ui_as_mcp_clients) {
    char *plain[] = {"codebase-memory-mcp", NULL};
    char *ui[] = {"codebase-memory-mcp", "--ui=true", "--port=9750", NULL};
    ASSERT_EQ(classify(1, plain), CBM_DAEMON_PROCESS_MCP_CLIENT);
    ASSERT_EQ(classify(3, ui), CBM_DAEMON_PROCESS_MCP_CLIENT);
    ASSERT_TRUE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_MCP_CLIENT));
    PASS();
}

TEST(daemon_bootstrap_classifies_stateless_commands_without_client) {
    char *version[] = {"codebase-memory-mcp", "--version", NULL};
    char *help[] = {"codebase-memory-mcp", "--profile", "--help", NULL};
    char *install[] = {"codebase-memory-mcp", "install", "--dry-run", NULL};
    char *uninstall[] = {"codebase-memory-mcp", "uninstall", NULL};
    char *update[] = {"codebase-memory-mcp", "update", "-n", NULL};
    ASSERT_EQ(classify(2, version), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(3, help), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(3, install), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(2, uninstall), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(3, update), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_STATELESS));
    PASS();
}

TEST(daemon_bootstrap_classifies_config_as_coordinated_local_cli) {
    char *list[] = {"codebase-memory-mcp", "config", "list", NULL};
    char *set[] = {"codebase-memory-mcp", "config", "set", "auto_watch", "false", NULL};
    char *help[] = {"codebase-memory-mcp", "config", "--help", NULL};
    ASSERT_EQ(classify(3, list), CBM_DAEMON_PROCESS_LOCAL_CLI);
    ASSERT_EQ(classify(5, set), CBM_DAEMON_PROCESS_LOCAL_CLI);
    ASSERT_EQ(classify(3, help), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_LOCAL_CLI));
    PASS();
}

TEST(daemon_bootstrap_cli_help_is_stateless_but_tool_calls_are_local) {
    char *tool_help[] = {"codebase-memory-mcp", "cli", "search_graph", "--help", NULL};
    char *tool_call[] = {"codebase-memory-mcp", "cli", "search_graph", "{}", NULL};
    ASSERT_EQ(classify(4, tool_help), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(4, tool_call), CBM_DAEMON_PROCESS_LOCAL_CLI);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_LOCAL_CLI));
    PASS();
}

TEST(daemon_bootstrap_cli_arguments_cannot_reclassify_the_process) {
    char *install_value[] = {
        "codebase-memory-mcp", "cli", "search_code", "--query", "install", NULL};
    char *version_value[] = {"codebase-memory-mcp", "cli", "search_code", "--query",
                             "--version",           NULL};
    ASSERT_EQ(classify(5, install_value), CBM_DAEMON_PROCESS_LOCAL_CLI);
    ASSERT_EQ(classify(5, version_value), CBM_DAEMON_PROCESS_LOCAL_CLI);
    PASS();
}

TEST(daemon_bootstrap_internal_roles_never_take_client_leases) {
    static char build[] = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    char *daemon[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, NULL};
    char *worker[] = {"codebase-memory-mcp", "cli", "--index-worker", "--index-worker-build", build,
                      "index_repository",    "{}",  "--response-out", "/tmp/response",        NULL};
    char *malformed_worker[] = {"codebase-memory-mcp", "cli", "--index-worker",
                                "index_repository",    "{}",  NULL};
    char *reserved_user_value[] = {"codebase-memory-mcp", "cli", "search_code", "--query",
                                   "--index-worker",      NULL};
    char *hook[] = {"codebase-memory-mcp", "hook-augment", NULL};
    ASSERT_EQ(classify(2, daemon), CBM_DAEMON_PROCESS_DAEMON);
    ASSERT_EQ(classify(9, worker), CBM_DAEMON_PROCESS_WORKER);
    ASSERT_EQ(classify(5, malformed_worker), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(5, reserved_user_value), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(2, hook), CBM_DAEMON_PROCESS_HOOK_CLIENT);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_DAEMON));
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_WORKER));
    ASSERT_TRUE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_HOOK_CLIENT));
    PASS();
}

TEST(daemon_bootstrap_rejects_ambiguous_internal_daemon_argv) {
    char *missing[] = {NULL};
    char *mixed[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, "cli", NULL};
    ASSERT_EQ(classify(0, missing), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(3, mixed), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_INVALID));
    PASS();
}

TEST(daemon_bootstrap_uses_one_stable_per_account_endpoint) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "stable-endpoint"));
    cbm_daemon_ipc_endpoint_t *second = cbm_daemon_bootstrap_endpoint_new(fixture.parent);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(cbm_daemon_ipc_endpoint_address(fixture.endpoint),
                  cbm_daemon_ipc_endpoint_address(second));
    cbm_daemon_ipc_endpoint_free(second);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_launches_only_exact_detached_hidden_role) {
    cbm_daemon_bootstrap_launch_spec_t spec;
    ASSERT_TRUE(cbm_daemon_bootstrap_launch_spec_init("/tmp/cbm exact", &spec));
    ASSERT_EQ(spec.argc, 2U);
    ASSERT_STR_EQ(spec.executable_path, "/tmp/cbm exact");
    ASSERT_STR_EQ(spec.argv[0], "/tmp/cbm exact");
    ASSERT_STR_EQ(spec.argv[1], CBM_DAEMON_INTERNAL_ARG);
    ASSERT_NULL(spec.argv[2]);
    ASSERT_TRUE(spec.detached);
    ASSERT_FALSE(spec.inherit_standard_handles);
    ASSERT_FALSE(spec.use_shell);
    PASS();
}

TEST(daemon_bootstrap_permanent_daemon_argv_is_byte_exact) {
    char *permanent[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, CBM_DAEMON_PERMANENT_ARG,
                         NULL};
    char *reordered[] = {"codebase-memory-mcp", CBM_DAEMON_PERMANENT_ARG, CBM_DAEMON_INTERNAL_ARG,
                         NULL};
    char *repeated[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, CBM_DAEMON_INTERNAL_ARG,
                        NULL};
    char *extended[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, CBM_DAEMON_PERMANENT_ARG,
                        "extra", NULL};
    char *wrong_flag[] = {"codebase-memory-mcp", CBM_DAEMON_INTERNAL_ARG, "--permanent", NULL};
    ASSERT_EQ(classify(3, permanent), CBM_DAEMON_PROCESS_DAEMON);
    ASSERT_EQ(classify(3, reordered), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(3, repeated), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(4, extended), CBM_DAEMON_PROCESS_INVALID);
    ASSERT_EQ(classify(3, wrong_flag), CBM_DAEMON_PROCESS_INVALID);
    PASS();
}

TEST(daemon_bootstrap_daemon_ctl_token_routes_after_cli) {
    char *start[] = {"codebase-memory-mcp", "daemon", "start", NULL};
    char *stop[] = {"codebase-memory-mcp", "daemon", "stop", NULL};
    char *status[] = {"codebase-memory-mcp", "daemon", "status", NULL};
    char *help[] = {"codebase-memory-mcp", "daemon", "--help", NULL};
    /* `daemon` after `cli` is opaque tool input, never a control command. */
    char *opaque[] = {"codebase-memory-mcp", "cli", "search_code", "daemon", "start", NULL};
    ASSERT_EQ(classify(3, start), CBM_DAEMON_PROCESS_DAEMON_CTL);
    ASSERT_EQ(classify(3, stop), CBM_DAEMON_PROCESS_DAEMON_CTL);
    ASSERT_EQ(classify(3, status), CBM_DAEMON_PROCESS_DAEMON_CTL);
    ASSERT_EQ(classify(3, help), CBM_DAEMON_PROCESS_STATELESS);
    ASSERT_EQ(classify(5, opaque), CBM_DAEMON_PROCESS_LOCAL_CLI);
    ASSERT_FALSE(cbm_daemon_process_role_requires_client(CBM_DAEMON_PROCESS_DAEMON_CTL));
    PASS();
}

TEST(daemon_bootstrap_permanent_launch_spec_is_exact) {
    cbm_daemon_bootstrap_launch_spec_t spec;
    ASSERT_TRUE(cbm_daemon_bootstrap_launch_spec_init_permanent("/tmp/cbm exact", &spec));
    ASSERT_EQ(spec.argc, 3U);
    ASSERT_STR_EQ(spec.argv[0], "/tmp/cbm exact");
    ASSERT_STR_EQ(spec.argv[1], CBM_DAEMON_INTERNAL_ARG);
    ASSERT_STR_EQ(spec.argv[2], CBM_DAEMON_PERMANENT_ARG);
    ASSERT_NULL(spec.argv[3]);
    ASSERT_TRUE(spec.detached);
    ASSERT_FALSE(spec.inherit_standard_handles);
    ASSERT_FALSE(spec.use_shell);
    PASS();
}

TEST(daemon_bootstrap_stateless_roles_bypass_every_daemon_operation) {
    bootstrap_fake_ops_t fake = {0};
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_STATELESS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_BYPASSED);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_BYPASSED);
    ASSERT_EQ(atomic_load(&fake.cohort_acquire_count), 0);
    ASSERT_EQ(atomic_load(&fake.probe_count), 0);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    ASSERT_EQ(atomic_load(&fake.diagnostic_count), 0);
    PASS();
}

TEST(daemon_bootstrap_cohort_conflict_is_visible_before_probe_or_spawn) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "cohort-conflict"));
    bootstrap_fake_ops_t fake = {0};
    fake.forced_cohort = CBM_VERSION_COHORT_CONFLICT;
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_B);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 50,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONFLICT);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONFLICT);
    ASSERT_EQ(atomic_load(&fake.cohort_acquire_count), 1);
    ASSERT_EQ(atomic_load(&fake.cohort_release_count), 0);
    ASSERT_EQ(atomic_load(&fake.probe_count), 0);
    ASSERT_EQ(atomic_load(&fake.lock_attempt_count), 0);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    ASSERT_EQ(atomic_load(&fake.diagnostic_count), 1);
    ASSERT_NOT_NULL(strstr(fake.diagnostic, "conflicting CBM process is active"));
    ASSERT_NOT_NULL(strstr(fake.diagnostic, "Close all CBM sessions and commands"));
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_existing_exact_daemon_connects_without_spawn) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "existing"));
    bootstrap_fake_ops_t fake = {0};
    atomic_store(&fake.available, true);
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 50,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_NOT_NULL(result.client);
    ASSERT_FALSE(result.daemon_spawned);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_conflict_is_visible_and_never_spawns) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "conflict"));
    bootstrap_fake_ops_t fake = {0};
    fake.forced_probe = CBM_DAEMON_BOOTSTRAP_PROBE_CONFLICT;
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_B);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 50,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONFLICT);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONFLICT);
    ASSERT_NULL(result.client);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    ASSERT_EQ(atomic_load(&fake.diagnostic_count), 1);
    ASSERT_NOT_NULL(strstr(fake.diagnostic, "conflicting versions"));
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_terminal_generation_that_never_exits_is_not_replaced) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "terminal"));
    bootstrap_fake_ops_t fake = {0};
    fake.forced_probe = CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL;
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_HOOK_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_SHORT_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_FAILED);
    ASSERT_EQ(atomic_load(&fake.lock_held), 0);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

/* RED for final-session/new-session overlap: STOPPING is a temporary state of
 * the previous same-build generation. Once that generation disappears, the
 * already-running bootstrap attempt must serialize and become the new first
 * client instead of forcing the coding agent to restart its MCP process. */
TEST(daemon_bootstrap_terminal_then_absent_spawns_replacement) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "terminal-absent"));
    bootstrap_fake_ops_t fake = {0};
    atomic_store(&fake.terminal_probes_remaining, 1);
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_TRUE(result.daemon_spawned);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 1);
    ASSERT(atomic_load(&fake.lock_attempt_count) >= 1);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_reserved_generation_becomes_connectable_without_spawn) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "reserved-connect"));
    bootstrap_fake_ops_t fake = {0};
    atomic_store(&fake.reserved_probes_remaining, 2);
    atomic_store(&fake.connect_after_reserved, true);
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_FALSE(result.daemon_spawned);
    ASSERT_EQ(atomic_load(&fake.lock_attempt_count), 0);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 0);
    ASSERT(atomic_load(&fake.probe_count) >= 3);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

/* RED for the equivalent race when the old listener vanishes without first
 * returning the explicit STOPPING response. Startup serialization decides
 * whether absence is now safe; a historical RESERVED sample is not sticky. */
TEST(daemon_bootstrap_reserved_then_absent_spawns_replacement) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "reserved-absent"));
    bootstrap_fake_ops_t fake = {0};
    atomic_store(&fake.reserved_probes_remaining, 1);
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_TRUE(result.daemon_spawned);
    ASSERT(atomic_load(&fake.lock_attempt_count) >= 1);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 1);
    ASSERT(atomic_load(&fake.probe_count) > 1);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

/* RED for the native Windows lock order: after spawn, the generation claim or
 * lifetime reservation becomes visible before the client can connect. Daemon
 * participant teardown needs startup ownership, so bootstrap must release its
 * handoff once that generation is observable. */
TEST(daemon_bootstrap_releases_handoff_when_spawned_generation_is_reserved) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "spawn-admission"));
    bootstrap_fake_ops_t fake = {0};
    atomic_store(&fake.connect_requires_unlocked, true);
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    ASSERT_EQ(cbm_daemon_bootstrap_execute_with_ops(&config, &ops, &result),
              CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(result.status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_TRUE(result.daemon_spawned);
    ASSERT_NOT_NULL(result.client);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 1);
    ASSERT_EQ(atomic_load(&fake.lock_held), 0);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

TEST(daemon_bootstrap_rejected_connect_is_reserved_and_never_unavailable) {
    cbm_daemon_runtime_connect_result_t capacity = {0};
    capacity.status = CBM_DAEMON_RUNTIME_CONNECT_REJECTED;
    snprintf(capacity.message, sizeof(capacity.message), "CBM daemon connection capacity reached");
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&capacity, 1),
              CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED);
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&capacity, 0),
              CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED);
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&capacity, -1),
              CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED);

    cbm_daemon_runtime_connect_result_t stopping = capacity;
    snprintf(stopping.message, sizeof(stopping.message), "CBM daemon is stopping");
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&stopping, 1),
              CBM_DAEMON_BOOTSTRAP_PROBE_TERMINAL);

    cbm_daemon_runtime_connect_result_t absent = {0};
    absent.status = CBM_DAEMON_RUNTIME_CONNECT_ERROR;
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&absent, 1),
              CBM_DAEMON_BOOTSTRAP_PROBE_RESERVED);
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&absent, 0),
              CBM_DAEMON_BOOTSTRAP_PROBE_UNAVAILABLE);
    ASSERT_EQ(cbm_daemon_bootstrap_classify_failed_connect(&absent, -1),
              CBM_DAEMON_BOOTSTRAP_PROBE_ERROR);
    PASS();
}

TEST(daemon_bootstrap_concurrent_first_clients_spawn_one_daemon) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "startup-race"));
    bootstrap_fake_ops_t fake = {0};
    cbm_daemon_bootstrap_ops_t ops = bootstrap_fake_callbacks(&fake);
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = "/tmp/cbm",
        .connect_timeout_ms = 10,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    atomic_int ready = 0;
    atomic_bool go = false;
    bootstrap_thread_call_t calls[2] = {
        {.config = &config, .ops = &ops, .ready = &ready, .go = &go},
        {.config = &config, .ops = &ops, .ready = &ready, .go = &go},
    };
    cbm_thread_t threads[2];
    ASSERT_EQ(cbm_thread_create(&threads[0], 0, bootstrap_thread_execute, &calls[0]), 0);
    ASSERT_EQ(cbm_thread_create(&threads[1], 0, bootstrap_thread_execute, &calls[1]), 0);
    uint64_t ready_deadline = cbm_now_ms() + BOOTSTRAP_TEST_TIMEOUT_MS;
    while (atomic_load(&ready) != 2 && cbm_now_ms() < ready_deadline) {
        const struct timespec pause = {0, 1000000L};
        (void)cbm_nanosleep(&pause, NULL);
    }
    atomic_store(&go, true);
    ASSERT_EQ(cbm_thread_join(&threads[0]), 0);
    ASSERT_EQ(cbm_thread_join(&threads[1]), 0);
    ASSERT_EQ(calls[0].status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(calls[1].status, CBM_DAEMON_BOOTSTRAP_CONNECTED);
    ASSERT_EQ(atomic_load(&fake.spawn_count), 1);
    ASSERT_TRUE(calls[0].result.daemon_spawned != calls[1].result.daemon_spawned);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}

#ifdef __APPLE__
/* RED: the old double-fork returned success before its grandchild attempted
 * posix_spawn, hiding an immediate launch error behind the full timeout. */
TEST(daemon_bootstrap_darwin_launch_failure_is_synchronous) {
    bootstrap_endpoint_fixture_t fixture;
    ASSERT_TRUE(bootstrap_endpoint_fixture_start(&fixture, "darwin-missing"));
    cbm_daemon_build_identity_t identity = bootstrap_identity("2.4.0", BOOTSTRAP_BUILD_A);
    char missing[BOOTSTRAP_TEST_PATH_CAP];
    int written = snprintf(missing, sizeof(missing), "%s/definitely-missing-cbm", fixture.parent);
    ASSERT(written > 0 && written < (int)sizeof(missing));
    cbm_daemon_bootstrap_config_t config = {
        .role = CBM_DAEMON_PROCESS_MCP_CLIENT,
        .endpoint = fixture.endpoint,
        .identity = &identity,
        .executable_path = missing,
        .connect_timeout_ms = 1,
        .startup_timeout_ms = BOOTSTRAP_TEST_TIMEOUT_MS,
    };
    cbm_daemon_bootstrap_result_t result;
    uint64_t started = cbm_now_ms();
    ASSERT_EQ(cbm_daemon_bootstrap_execute(&config, &result), CBM_DAEMON_BOOTSTRAP_FAILED);
    uint64_t elapsed = cbm_now_ms() - started;
    ASSERT_FALSE(result.daemon_spawned);
    ASSERT(elapsed < BOOTSTRAP_TEST_TIMEOUT_MS / 2U);
    bootstrap_endpoint_fixture_finish(&fixture);
    PASS();
}
#endif

SUITE(daemon_bootstrap) {
    RUN_TEST(daemon_bootstrap_classifies_default_and_ui_as_mcp_clients);
    RUN_TEST(daemon_bootstrap_classifies_stateless_commands_without_client);
    RUN_TEST(daemon_bootstrap_classifies_config_as_coordinated_local_cli);
    RUN_TEST(daemon_bootstrap_cli_help_is_stateless_but_tool_calls_are_local);
    RUN_TEST(daemon_bootstrap_cli_arguments_cannot_reclassify_the_process);
    RUN_TEST(daemon_bootstrap_internal_roles_never_take_client_leases);
    RUN_TEST(daemon_bootstrap_rejects_ambiguous_internal_daemon_argv);
    RUN_TEST(daemon_bootstrap_uses_one_stable_per_account_endpoint);
    RUN_TEST(daemon_bootstrap_launches_only_exact_detached_hidden_role);
    RUN_TEST(daemon_bootstrap_permanent_daemon_argv_is_byte_exact);
    RUN_TEST(daemon_bootstrap_daemon_ctl_token_routes_after_cli);
    RUN_TEST(daemon_bootstrap_permanent_launch_spec_is_exact);
    RUN_TEST(daemon_bootstrap_stateless_roles_bypass_every_daemon_operation);
    RUN_TEST(daemon_bootstrap_cohort_conflict_is_visible_before_probe_or_spawn);
    RUN_TEST(daemon_bootstrap_existing_exact_daemon_connects_without_spawn);
    RUN_TEST(daemon_bootstrap_conflict_is_visible_and_never_spawns);
    RUN_TEST(daemon_bootstrap_terminal_generation_that_never_exits_is_not_replaced);
    RUN_TEST(daemon_bootstrap_terminal_then_absent_spawns_replacement);
    RUN_TEST(daemon_bootstrap_reserved_generation_becomes_connectable_without_spawn);
    RUN_TEST(daemon_bootstrap_reserved_then_absent_spawns_replacement);
    RUN_TEST(daemon_bootstrap_releases_handoff_when_spawned_generation_is_reserved);
    RUN_TEST(daemon_bootstrap_rejected_connect_is_reserved_and_never_unavailable);
    RUN_TEST(daemon_bootstrap_concurrent_first_clients_spawn_one_daemon);
#ifdef __APPLE__
    RUN_TEST(daemon_bootstrap_darwin_launch_failure_is_synchronous);
#endif
}
