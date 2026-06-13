/*
 * test_perl_lsp.c — Tests for the Perl Light Semantic Pass.
 *
 * Placeholder created in plan 22-01 so the Makefile TEST_PERL_LSP_SRCS var
 * resolves and the build wiring is complete. The real Perl LSP test suite is
 * authored in plan 22-04; this file currently holds a single passing test.
 *
 * TODO(plan 22-04): replace with the full Perl LSP resolution test suite and
 * register suite_perl_lsp in tests/test_main.c.
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/perl_lsp.h"

/* ── Placeholder ───────────────────────────────────────────────── */

TEST(perllsp_placeholder_skeleton_present) {
    /* The skeleton entry point exists and is callable via the header. This
     * test exists only so the suite compiles and links; behavioral coverage
     * arrives in plan 22-04. */
    PASS();
}

/* ── Suite registration ────────────────────────────────────────── */

SUITE(perl_lsp) {
    RUN_TEST(perllsp_placeholder_skeleton_present);
}
