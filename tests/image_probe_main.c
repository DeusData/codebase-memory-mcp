/*
 * image_probe_main.c — entry point of the image probe (tests/test_image_probe.h),
 * the small executable that copied-image and install tests run instead of the
 * test runner. Its roles live in tests/image_probe_roles.c.
 */
#include "test_image_probe.h"

#include <stdio.h>
#include <string.h>

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define PROBE_MSAN_MIMALLOC_OUTPUT 1
#endif
#endif

#ifdef PROBE_MSAN_MIMALLOC_OUTPUT
#include <mimalloc.h>
#include <sanitizer/msan_interface.h>

/* MSan lane only, exactly as the runner's tf_msan_mi_output (tests/test_main.c):
 * mimalloc is uninstrumented there, so text it formats on its own stack must
 * be marked initialized before it reaches an MSan interceptor. */
static bool probe_msan_mi_output_live;

static void probe_msan_mi_output(const char *msg, void *arg) {
    (void)arg;
    if (!probe_msan_mi_output_live || !msg) {
        return;
    }
    __msan_unpoison_string(msg);
    if (!msg[0]) {
        return;
    }
    (void)fputs(msg, stderr);
}
#endif

int main(int argc, char **argv) {
#ifdef PROBE_MSAN_MIMALLOC_OUTPUT
    mi_register_output(probe_msan_mi_output, NULL);
    probe_msan_mi_output_live = true;
#endif
    /* An install verifies the candidate it staged by running it with
     * --version (src/cli/cli.c); the probe stands in for that binary. */
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        (void)puts("codebase-memory-mcp test image probe");
        return 0;
    }
    int role_rc = tf_image_probe_dispatch(argc, argv);
    if (role_rc >= 0) {
        return role_rc;
    }
    (void)fprintf(stderr, "image probe: no role named by argv[1] (tests/test_image_probe.h)\n");
    return 2;
}
