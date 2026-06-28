/*
 * repro_issue363.c — Reproduce-first case for OPEN bug #363.
 *
 * Issue: #363 — "Linux: cbm_system_info / cbm_default_worker_count don't
 *               respect cgroup CPU/memory limits"
 *
 * ROOT CAUSE (two distinct axes):
 *
 *   CPU axis — FIXED in v0.8.0 (commit a5a3d1d).
 *     cbm_detect_cgroup_cpus() reads /sys/fs/cgroup/cpu.max (v2) or
 *     .../cpu/cpu.cfs_quota_us + .../cpu/cpu.cfs_period_us (v1) and the
 *     result is used by detect_system_linux() in system_info.c:226.
 *     cbm_default_worker_count() also honours the CBM_WORKERS env override
 *     (commit d952238).  Both are thoroughly tested in test_platform.c.
 *
 *   Memory axis — STILL OPEN (confirmed by reporter @mayurpise in the last
 *     open comment on #363, 2026-06-25).
 *     cbm_detect_cgroup_mem() similarly reads /sys/fs/cgroup/memory.max (v2)
 *     or .../memory/memory.limit_in_bytes (v1), and detect_system_linux()
 *     uses it (system_info.c:229).  BUT: there is NO env-override knob on
 *     the memory axis.  The CPU axis has CBM_WORKERS; the memory side has
 *     nothing.  On a bare-metal host with no enclosing cgroup, users cannot
 *     cap cbm_mem_init's budget without wrapping the process in a cgroup
 *     scope (as @mayurpise's workaround shows).
 *
 * EXACT OPEN GAP:
 *   A CBM_MEM_BUDGET_MB environment variable (analogous to CBM_WORKERS) that
 *   cbm_mem_init() checks before computing g_budget from info.total_ram.
 *   If set to a valid integer N, cbm_mem_init() should set
 *   g_budget = N * 1024 * 1024, honouring it regardless of cgroup or host RAM.
 *
 * WHY THIS TEST IS RED:
 *   cbm_mem_init() (src/foundation/mem.c) reads cbm_system_info().total_ram
 *   and multiplies by ram_fraction.  It does NOT call cbm_safe_getenv for
 *   CBM_MEM_BUDGET_MB — the override path does not exist.  Setting
 *   CBM_MEM_BUDGET_MB=4096 has no effect; cbm_mem_budget() returns a value
 *   derived from host RAM (or cgroup RAM when inside a container), not from
 *   the env var.  The assertion ASSERT_EQ(cbm_mem_budget(), 4096*1024*1024)
 *   therefore fails on any host whose cgroup or physical RAM != exactly 4 GiB.
 *
 * ROOT CAUSE LOCATION:
 *   src/foundation/mem.c, cbm_mem_init(), after the mimalloc option block
 *   (currently around line 126):
 *     cbm_system_info_t info = cbm_system_info();
 *     g_budget = (size_t)((double)info.total_ram * ram_fraction);
 *   The fix is to insert a cbm_safe_getenv("CBM_MEM_BUDGET_MB", ...) lookup
 *   BEFORE this line and, if valid, set g_budget directly without involving
 *   info.total_ram — mirroring the CBM_WORKERS pattern in
 *   cbm_default_worker_count() (system_info.c:290).
 *
 * INTENDED FIX:
 *   1. In cbm_mem_init(): read CBM_MEM_BUDGET_MB; if set to a valid positive
 *      integer, use that value (in bytes) as g_budget and log it.
 *   2. Test: set CBM_MEM_BUDGET_MB=4096, call cbm_mem_init(0.5), assert
 *      cbm_mem_budget() == 4096 * 1024 * 1024.  This test goes GREEN when
 *      the override is wired.
 *   3. Complementary: on Linux, confirm cbm_system_info().total_ram is capped
 *      by the cgroup memory limit when present — already covered in
 *      test_platform.c via cbm_detect_cgroup_mem() unit tests, but an
 *      integration path via cbm_system_info() is untestable without a seam
 *      that lets callers override the hardcoded "/sys/fs/cgroup" root in
 *      detect_system_linux() (system_info.c:229).
 *
 * NOTE on cbm_mem_init() caching:
 *   g_budget is initialised once via atomic_compare_exchange_strong.
 *   The test must run in a process where cbm_mem_init() has NOT been called
 *   yet, OR the test must reset g_initialized — neither is supported today.
 *   The repro works as written because the repro runner does not call
 *   cbm_mem_init() before this suite.  If the initialisation guard is an
 *   issue, the fix also needs a cbm_mem_reset_for_test() hook (test-only,
 *   guarded by CBM_TEST_HOOKS or similar).
 */

#include "test_framework.h"
#include <foundation/mem.h>
#include <foundation/compat.h>
#include <stdint.h>
#include <stdlib.h>

#define REPRO363_BUDGET_MB 4096UL
#define REPRO363_BUDGET_BYTES (REPRO363_BUDGET_MB * 1024UL * 1024UL)

/*
 * repro_issue363_mem_budget_env_override
 *
 * Precondition: CBM_MEM_BUDGET_MB=4096 is set before cbm_mem_init() is
 * called.  The budget should be 4096 MiB regardless of host RAM or cgroup.
 *
 * RED condition (current code):
 *   cbm_mem_init() ignores CBM_MEM_BUDGET_MB entirely; cbm_mem_budget()
 *   returns host-RAM * fraction, not 4 GiB.  The assertion fires unless the
 *   test runner happens to be on a machine whose effective RAM is exactly
 *   8 GiB with fraction=0.5 — essentially never.
 *
 * GREEN condition (after fix):
 *   cbm_mem_init() reads CBM_MEM_BUDGET_MB, finds "4096", sets
 *   g_budget = 4096 * 1024 * 1024.  The assertion passes on any machine.
 */
TEST(repro_issue363_mem_budget_env_override) {
    cbm_setenv("CBM_MEM_BUDGET_MB", "4096", 1);

    cbm_mem_init(0.5);

    size_t budget = cbm_mem_budget();

    cbm_unsetenv("CBM_MEM_BUDGET_MB");

    /*
     * RED on current code: budget derives from host/cgroup RAM, not the env
     * var.  On any machine where effective RAM != 8192 MiB this fails.
     * GREEN once CBM_MEM_BUDGET_MB is wired in cbm_mem_init().
     */
    ASSERT_EQ((long long)budget, (long long)REPRO363_BUDGET_BYTES);

    PASS();
}

SUITE(repro_issue363) {
    RUN_TEST(repro_issue363_mem_budget_env_override);
}
