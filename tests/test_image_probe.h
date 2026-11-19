#ifndef TEST_IMAGE_PROBE_H
#define TEST_IMAGE_PROBE_H

/*
 * The image probe: a small executable built beside every test runner
 * (Makefile.cbm, <runner>-image-probe, e.g. build/c/test-runner-image-probe)
 * from tests/image_probe_main.c, tests/image_probe_roles.c, the daemon runtime
 * and foundation (~5 MB with sanitizers, against ~800 MB for the runner).
 *
 * Tests that need a real, separately executed process image use it instead of
 * a copy of the runner: daemon_runtime's copied-image probes run their daemon
 * and clients from it, and the cli install tests publish it as the installed
 * binary. Each of those used to SHA-256 the whole sanitized runner, several
 * times per test, and one such hash costs 70-95 s on some hosted x86 CPUs: a
 * daemon still hashing a peer copy missed the client's exchange budget, and the
 * cli suite ran past its wall-clock ceiling, decided by which CPU a job drew.
 * The production verification code is unchanged; only the image is smaller.
 *
 * Roles (argv[1]), dispatched by tf_image_probe_dispatch:
 *   __cbm_runtime_serve <parent> <key> <version> <owner:0|1>
 *   __cbm_runtime_hello_client <parent> <key> <version> <build>
 *   __cbm_runtime_activation_client <parent> <key> <version> <build> <action>
 *   __cbm_runtime_mapped_hello_client <mapped-image> <parent> <key> <version> <build>
 *   __cbm_runtime_image_holder [<ready-event>]
 * The probe's main also answers --version (install verification runs it).
 */

#include "foundation/compat_fs.h" /* cbm_fopen: the probe path may be non-ASCII */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <stdlib.h>
#else
#include <unistd.h>
#ifdef __APPLE__
#include <libproc.h>
#endif
#endif

/* Runs the role argv[1] names: its exit code, or -1 when argv names none. */
int tf_image_probe_dispatch(int argc, char **argv);

/* The probe built beside the running runner: the runner's own image path
 * without ".exe", then "-image-probe" (".exe" again on Windows). False when
 * the path cannot be formed or names no file: a missing probe is a build
 * defect the calling test reports, never a skip. */
static inline bool th_image_probe_path(char *out, size_t cap) {
    if (!out || cap == 0) {
        return false;
    }
    out[0] = '\0';
    char self[4096] = {0};
#ifdef _WIN32
    char *module = cbm_module_path_utf8();
    size_t module_length = module ? strlen(module) : 0;
    bool resolved = module_length > 0 && module_length < sizeof(self);
    if (resolved) {
        memcpy(self, module, module_length + 1);
    }
    free(module);
    size_t length = resolved ? strlen(self) : 0;
    if (length > 4 && _stricmp(self + length - 4, ".exe") == 0) {
        self[length - 4] = '\0';
    }
    const char *suffix = "-image-probe.exe";
#elif defined(__APPLE__)
    int length = proc_pidpath(getpid(), self, (uint32_t)sizeof(self));
    bool resolved = length > 0 && length < (int)sizeof(self);
    if (resolved) {
        self[length] = '\0';
    }
    const char *suffix = "-image-probe";
#elif defined(__linux__)
    ssize_t length = readlink("/proc/self/exe", self, sizeof(self) - 1);
    bool resolved = length > 0 && length < (ssize_t)sizeof(self) - 1;
    if (resolved) {
        self[length] = '\0';
    }
    const char *suffix = "-image-probe";
#else
    bool resolved = false;
    const char *suffix = "";
#endif
    int written = resolved ? snprintf(out, cap, "%s%s", self, suffix) : -1;
    if (written <= 0 || (size_t)written >= cap) {
        out[0] = '\0';
        return false;
    }
    FILE *probe = cbm_fopen(out, "rb");
    if (!probe) {
        out[0] = '\0';
        return false;
    }
    (void)fclose(probe);
    return true;
}

#endif /* TEST_IMAGE_PROBE_H */
