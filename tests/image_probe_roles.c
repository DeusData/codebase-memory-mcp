/*
 * image_probe_roles.c — the roles of the image probe (tests/test_image_probe.h).
 *
 * Linked into the probe (tests/image_probe_main.c) and into the test runner.
 * The runner dispatches only the image holder from here (daemon_runtime's
 * own-image cache test spawns the runner itself); its role table in
 * tests/test_main.c names every other literal too, so that the test-impact
 * engine still maps a change these roles reach to the tests that start them.
 * Every role is called directly from tf_image_probe_dispatch for the same
 * reason: a call edge, not a function-pointer table, links them.
 *
 * Exit codes: 0 success; 24/25 holder failures; 26 the daemon ANSWERED a HELLO
 * and refused it; 27 the HELLO session did not close; 28 the macOS mapping
 * failed; 29 the daemon answered an activation request and refused it; 30/31 a
 * HELLO/activation that got no answer inside the exchange budget (a slow peer
 * hash fails as itself, never as a refusal); 32 bad serve arguments; 33 the
 * served daemon did not start; 34 its teardown failed.
 */
#include "test_image_probe.h"
#include "test_daemon_runtime_contract.h"

#include "daemon/daemon.h"
#include "daemon/ipc.h"
#include "daemon/runtime.h"
#include "foundation/compat.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#ifdef __APPLE__
#include <fcntl.h>
#include <sys/mman.h>
#endif
#endif

enum {
    /* The in-process daemon_runtime fixture's bounds (RUNTIME_TEST_TIMEOUT_MS,
     * RUNTIME_TEST_CLEANUP_TIMEOUT_MS, RUNTIME_TEST_CLEANUP_FREE_ATTEMPTS):
     * the served daemon behaves exactly like that fixture's. */
    PROBE_TIMEOUT_MS = 2000,
    PROBE_CLEANUP_TIMEOUT_MS = 60000,
    PROBE_CLEANUP_FREE_ATTEMPTS = 3,
    PROBE_PATH_CAP = 1024,
};

static uint64_t probe_process_id(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#else
    return (uint64_t)getpid();
#endif
}

static int probe_role_holder(int argc, char **argv) {
#ifdef _WIN32
    if (argc != 3) {
        return -1;
    }
    HANDLE ready = OpenEventA(EVENT_MODIFY_STATE, FALSE, argv[2]);
    bool announced = ready && SetEvent(ready) != 0;
    if (ready) {
        (void)CloseHandle(ready);
    }
    if (!announced) {
        return 24;
    }
    Sleep(INFINITE);
    return 25;
#else
    /* POSIX copied-image holder: block reading stdin until the parent closes
     * the release pipe, exactly like the cat(1) donor this replaced. A system
     * utility cannot serve as the copied image — a multi-call coreutils
     * binary (uutils cat) refuses to execute under the copied name. */
    (void)argv;
    if (argc != 2) {
        return -1;
    }
    char release[16];
    ssize_t count;
    do {
        count = read(STDIN_FILENO, release, sizeof(release));
    } while (count > 0 || (count < 0 && errno == EINTR));
    return count == 0 ? 0 : 25;
#endif
}

/* Real copied-image HELLO probe: the daemon authenticates and fingerprints an
 * ordinary, separately executed process image; no production-only hook. */
static int probe_role_hello(int argc, char **argv) {
    if (argc != 6) {
        return -1;
    }
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(argv[3], argv[2]);
    cbm_daemon_build_identity_t identity = {
        .semantic_version = argv[4],
        .build_fingerprint = argv[5],
    };
    cbm_daemon_runtime_connect_result_t result;
    memset(&result, 0, sizeof(result));
    cbm_daemon_runtime_client_t *client =
        endpoint ? cbm_daemon_runtime_client_connect(endpoint, &identity,
                                                     TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS, &result)
                 : NULL;
    bool accepted = client && result.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED &&
                    result.hello_status == CBM_DAEMON_HELLO_COMPATIBLE;
    bool closed =
        !client || cbm_daemon_runtime_client_close(client, TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS);
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (!accepted) {
        /* 26 only when the daemon ANSWERED and refused. No answer inside the
         * exchange budget (the daemon still hashing this copy, or I/O) is 30:
         * a slow peer-image hash must fail loudly as itself, never satisfy a
         * rejection assertion by timing out. */
        return result.status == CBM_DAEMON_RUNTIME_CONNECT_ERROR ? 30 : 26;
    }
    return closed ? 0 : 27;
}

/* Real copied-image activation probe. The request identity is supplied by the
 * executing copy so the daemon must authenticate that peer image rather than
 * require the active generation's build fingerprint. */
static int probe_role_activation(int argc, char **argv) {
    if (argc != 7) {
        return -1;
    }
    char *action_end = NULL;
    unsigned long action_value = strtoul(argv[6], &action_end, 10);
    bool action_valid = action_end != argv[6] && *action_end == '\0' &&
                        action_value >= (unsigned long)CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL &&
                        action_value <= (unsigned long)CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL;
    cbm_daemon_ipc_endpoint_t *endpoint =
        action_valid ? cbm_daemon_ipc_endpoint_new(argv[3], argv[2]) : NULL;
    cbm_daemon_build_identity_t identity = {
        .semantic_version = argv[4],
        .build_fingerprint = argv[5],
    };
    cbm_daemon_runtime_activation_result_t result;
    memset(&result, 0, sizeof(result));
    bool exchanged =
        endpoint && cbm_daemon_runtime_request_activation_shutdown(
                        endpoint, &identity, (cbm_daemon_runtime_activation_action_t)action_value,
                        TF_RUNTIME_IMAGE_EXCHANGE_TIMEOUT_MS, &result);
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (!exchanged) {
        return 31; /* no answer inside the exchange budget; see the HELLO probe */
    }
    return result.accepted ? 0 : 29;
}

/* macOS adversarial HELLO probe: execute this mode from a foreign process
 * image while mapping the genuine daemon image RX. The daemon must
 * authenticate the main image, not merely find its own vnode in an arbitrary
 * executable map. */
static int probe_role_mapped_hello(int argc, char **argv) {
#ifdef __APPLE__
    if (argc != 7) {
        return -1;
    }
    int image_fd = open(argv[2], O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    void *mapping =
        image_fd >= 0 ? mmap(NULL, 1, PROT_READ | PROT_EXEC, MAP_PRIVATE, image_fd, 0) : MAP_FAILED;
    bool mapped = mapping != MAP_FAILED;
    if (image_fd >= 0 && close(image_fd) != 0) {
        mapped = false;
    }
    if (!mapped) {
        if (mapping != MAP_FAILED) {
            (void)munmap(mapping, 1);
        }
        return 28;
    }

    char *hello_argv[] = {argv[0], "__cbm_runtime_hello_client", argv[3], argv[4], argv[5],
                          argv[6]};
    int result = probe_role_hello(6, hello_argv);
    if (munmap(mapping, 1) != 0) {
        return 28;
    }
    return result >= 0 ? result : 28;
#else
    (void)argc;
    (void)argv;
    return -1;
#endif
}

static void probe_reply(const char *line) {
    (void)fputs(line, stdout);
    (void)fputc('\n', stdout);
    (void)fflush(stdout);
}

/* The daemon of the copied-image tests, running from THIS image:
 *   __cbm_runtime_serve <parent> <key> <version> <owner:0|1>
 * A runtime service starts only with its own image's fingerprint as identity,
 * so a daemon whose peers are copies of this probe must run here, not in the
 * runner. The configuration is the in-process daemon_runtime fixture's
 * (runtime_test_fixture_start, ABIs of runtime_test_identity), and with owner 1
 * a committed session of the active image is held in this process, like that
 * fixture's in-process owner client.
 *
 * One line answers each step on stdout; commands arrive on stdin, one byte:
 *   start  "ready <own fingerprint>" or "failed <stage>"
 *   'i'    "1" when the held owner session was interrupted (its heartbeat
 *          fails), else "0"
 *   'x'    closes the owner session, then "1" when the service has exited by
 *          itself within the fixture's bound, else "0"
 *   EOF    the runner's release: stop what still runs, free it, exit.
 * No step waits on a clock of its own: every bound is the runtime API's, and
 * the runner closing stdin ends the role. */
static int probe_role_serve(int argc, char **argv) {
    if (argc != 6 || (strcmp(argv[5], "0") != 0 && strcmp(argv[5], "1") != 0)) {
        return 32;
    }
#ifdef _WIN32
    /* Exact bytes both ways: no CRLF translation on the protocol pipes. */
    (void)_setmode(_fileno(stdin), _O_BINARY);
    (void)_setmode(_fileno(stdout), _O_BINARY);
#endif
    bool with_owner = argv[5][0] == '1';
    char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    char log_path[PROBE_PATH_CAP];
    int log_written = snprintf(log_path, sizeof(log_path), "%s/conflicts.ndjson", argv[2]);
    cbm_daemon_ipc_endpoint_t *endpoint = cbm_daemon_ipc_endpoint_new(argv[3], argv[2]);
    const char *failed = NULL;
    if (log_written <= 0 || log_written >= (int)sizeof(log_path)) {
        failed = "conflict-log-path";
    } else if (!endpoint) {
        failed = "endpoint";
    } else if (!cbm_daemon_runtime_process_build_fingerprint(probe_process_id(), build)) {
        failed = "own-image-fingerprint";
    }
    cbm_daemon_build_identity_t identity = {
        .semantic_version = argv[4],
        .build_fingerprint = build,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    cbm_daemon_runtime_service_t *service = NULL;
    if (!failed) {
        cbm_daemon_runtime_service_config_t config = {
            .endpoint = endpoint,
            .identity = identity,
            .conflict_log_path = log_path,
            .conflict_log_cap_bytes = 64U * 1024U,
            .max_clients = 8,
            .lease_timeout_ms = 5000,
            .request_timeout_ms = PROBE_TIMEOUT_MS,
            .shutdown_timeout_ms = PROBE_TIMEOUT_MS,
        };
        service = cbm_daemon_runtime_service_start(&config);
        if (!service ||
            cbm_daemon_runtime_service_state(service) != CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
            failed = "service-start";
        }
    }
    cbm_daemon_runtime_client_t *owner = NULL;
    if (!failed && with_owner) {
        cbm_daemon_runtime_connect_result_t result;
        memset(&result, 0, sizeof(result));
        owner = cbm_daemon_runtime_client_connect(endpoint, &identity, PROBE_TIMEOUT_MS, &result);
        if (!owner || result.status != CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED) {
            failed = "owner-connect";
        }
    }
    char line[CBM_DAEMON_BUILD_FINGERPRINT_SIZE + 16];
    (void)snprintf(line, sizeof(line), "%s %s", failed ? "failed" : "ready",
                   failed ? failed : build);
    probe_reply(line);

    for (int command = failed ? EOF : getchar(); command != EOF; command = getchar()) {
        if (command == 'i') {
            bool interrupted =
                owner && !cbm_daemon_runtime_client_heartbeat(owner, PROBE_TIMEOUT_MS);
            probe_reply(interrupted ? "1" : "0");
        } else if (command == 'x') {
            if (owner) {
                (void)cbm_daemon_runtime_client_close(owner, PROBE_TIMEOUT_MS);
                owner = NULL;
            }
            probe_reply(cbm_daemon_runtime_service_wait_exited(service, PROBE_TIMEOUT_MS) ? "1"
                                                                                           : "0");
        }
    }

    if (owner) {
        (void)cbm_daemon_runtime_client_close(owner, PROBE_TIMEOUT_MS);
    }
    bool freed = service == NULL;
    if (service) {
        /* runtime_test_fixture_finish's teardown, unchanged. */
        bool stopped =
            cbm_daemon_runtime_service_state(service) == CBM_DAEMON_RUNTIME_SERVICE_EXITED ||
            cbm_daemon_runtime_service_stop(service, PROBE_CLEANUP_TIMEOUT_MS);
        for (int attempt = 0; stopped && !freed && attempt < PROBE_CLEANUP_FREE_ATTEMPTS;
             attempt++) {
            freed = cbm_daemon_runtime_service_free(service);
            if (!freed) {
                cbm_usleep(1000);
            }
        }
    }
    cbm_daemon_ipc_endpoint_free(endpoint);
    if (failed) {
        return 33;
    }
    return freed ? 0 : 34;
}

int tf_image_probe_dispatch(int argc, char **argv) {
    if (argc < 2 || !argv || !argv[1]) {
        return -1;
    }
    const char *role = argv[1];
    if (strcmp(role, "__cbm_runtime_image_holder") == 0) {
        return probe_role_holder(argc, argv);
    }
    if (strcmp(role, "__cbm_runtime_serve") == 0) {
        return probe_role_serve(argc, argv);
    }
    if (strcmp(role, "__cbm_runtime_hello_client") == 0) {
        return probe_role_hello(argc, argv);
    }
    if (strcmp(role, "__cbm_runtime_activation_client") == 0) {
        return probe_role_activation(argc, argv);
    }
    if (strcmp(role, "__cbm_runtime_mapped_hello_client") == 0) {
        return probe_role_mapped_hello(argc, argv);
    }
    return -1;
}
