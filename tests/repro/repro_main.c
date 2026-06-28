/*
 * repro_main.c — Entry point for the cumulative BUG-REPRODUCTION suite.
 *
 * This runner is SEPARATE from the gating `make test` (test-runner). It exists
 * to hold reproduce-first cases for every OPEN bug issue. Each case asserts the
 * CORRECT behaviour, so it is **RED until the bug is fixed** — the redness is the
 * deliverable (proof the bug is real + the permanent regression guard).
 *
 * Because these cases are red by design, they MUST NOT live in `ALL_TEST_SRCS`
 * (that would turn the PR gate `ci-ok` red and wedge every merge). They are built
 * + run only via `make test-repro` and the `bug-repro.yml` workflow, neither of
 * which gates branch protection.
 *
 * Exit status: non-zero when any reproduction is still RED (the expected state).
 * The `bug-repro.yml` workflow treats that as the status board, not a hard fail.
 *
 * Adding a cluster:
 *   1. create tests/repro/repro_<cluster>.c exporting `void suite_repro_<cluster>(void)`
 *   2. add it to TEST_REPRO_SRCS in Makefile.cbm
 *   3. forward-declare + RUN_SUITE it below
 */

/* Global test counters (declared extern in test_framework.h) */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;

#include "test_framework.h"

/* Per-suite summary + filter. RUN_SUITE prints a one-line
 * "[SUITE] <name> P passed, F failed" report (greppable for which suites still
 * have reds). When CBM_REPRO_ONLY is set (comma/space list of suite-name
 * substrings), only matching suites run — for fast targeted validation of a
 * single fix without rebuilding intent. */
static int cbm_suite_enabled(const char *name) {
    const char *only = getenv("CBM_REPRO_ONLY");
    if (!only || !*only)
        return 1;
    return strstr(only, name) != NULL;
}
#undef RUN_SUITE
#define RUN_SUITE(name)                                                                  \
    do {                                                                                 \
        if (!cbm_suite_enabled(#name))                                                   \
            break;                                                                       \
        int _p0 = tf_pass_count, _f0 = tf_fail_count;                                    \
        printf("\n%s=== %s ===%s\n", tf_dim(), #name, tf_reset());                       \
        suite_##name();                                                                  \
        printf("[SUITE] %-38s %d passed, %d failed\n", #name, tf_pass_count - _p0,       \
               tf_fail_count - _f0);                                                     \
    } while (0)

/* ── Repro suites (one per bug cluster / issue) ─────────────────── */
extern void suite_repro_extraction(void);
extern void suite_repro_issue495(void);
extern void suite_repro_issue521(void);
extern void suite_repro_issue382(void);
extern void suite_repro_issue408(void);
extern void suite_repro_issue56(void);
extern void suite_repro_issue480(void);
extern void suite_repro_issue571(void);
extern void suite_repro_issue523(void);
extern void suite_repro_issue546(void);
extern void suite_repro_issue627(void);
extern void suite_repro_issue514(void);
extern void suite_repro_issue510(void);
extern void suite_repro_issue557(void);
extern void suite_repro_issue520(void);
extern void suite_repro_issue333(void);
extern void suite_repro_issue570(void);
extern void suite_repro_issue409(void);
extern void suite_repro_issue431(void);
extern void suite_repro_issue607(void);
extern void suite_repro_issue403(void);
extern void suite_repro_issue434(void);
extern void suite_repro_issue471(void);
extern void suite_repro_issue221(void);
extern void suite_repro_issue548(void);
extern void suite_repro_issue363(void);
extern void suite_repro_issue581(void);
/* NEW bugs found by the discovery sweep */
extern void suite_repro_new_ts_class_field_arrow(void);
extern void suite_repro_new_py_tuple_unpack(void);
extern void suite_repro_new_cypher_limit_zero(void);
/* Large INVARIANT test group (graph-quality systemic invariants, QUALITY_ANALYSIS) */
extern void suite_repro_invariant_calls(void);
extern void suite_repro_invariant_graph(void);
extern void suite_repro_invariant_breadth(void);
extern void suite_repro_invariant_enclosing_parity(void);
extern void suite_repro_invariant_lsp_rescue(void);
extern void suite_repro_invariant_discovery_fqn(void);
/* Per-grammar invariant batteries (extract-clean/labels/fqn/ranges/callable-sourcing) */
extern void suite_repro_grammar_core(void);
extern void suite_repro_grammar_scripting(void);
extern void suite_repro_grammar_functional(void);
extern void suite_repro_grammar_systems(void);
extern void suite_repro_grammar_web(void);
extern void suite_repro_grammar_config(void);
extern void suite_repro_grammar_build(void);
extern void suite_repro_grammar_shells(void);
extern void suite_repro_grammar_scientific(void);
extern void suite_repro_grammar_markup(void);
extern void suite_repro_grammar_misc(void);
/* Per-LSP-pass resolution-strategy invariants */
extern void suite_repro_lsp_c_cpp(void);
extern void suite_repro_lsp_go_py(void);
extern void suite_repro_lsp_ts(void);
extern void suite_repro_lsp_java_cs(void);
extern void suite_repro_lsp_kt_php_rust(void);

int main(void) {
    /* Unbuffered: a reproduction may crash/_exit (or a sanitizer may _exit on a
     * leak) before stdio flushes — keep every printed line so the summary and the
     * RED rows always reach the board even on an abnormal exit. */
    setvbuf(stdout, NULL, _IONBF, 0);

    printf("\n");
    printf("════════════════════════════════════════════════════════════\n");
    printf("  CUMULATIVE BUG-REPRODUCTION SUITE\n");
    printf("  RED rows are EXPECTED — each is an open bug reproduced.\n");
    printf("  A row that PASSES means that bug appears FIXED → flip it\n");
    printf("  into the gating suite and close the issue with the guard.\n");
    printf("════════════════════════════════════════════════════════════\n");

    RUN_SUITE(repro_extraction);
    RUN_SUITE(repro_issue495);
    RUN_SUITE(repro_issue521);
    RUN_SUITE(repro_issue382);
    RUN_SUITE(repro_issue408);
    RUN_SUITE(repro_issue56);
    RUN_SUITE(repro_issue480);
    RUN_SUITE(repro_issue571);
    RUN_SUITE(repro_issue523);
    RUN_SUITE(repro_issue546);
    RUN_SUITE(repro_issue627);
    RUN_SUITE(repro_issue514);
    RUN_SUITE(repro_issue510);
    RUN_SUITE(repro_issue557);
    RUN_SUITE(repro_issue520);
    RUN_SUITE(repro_issue333);
    RUN_SUITE(repro_issue570);
    RUN_SUITE(repro_issue409);
    RUN_SUITE(repro_issue431);
    RUN_SUITE(repro_issue607);
    RUN_SUITE(repro_issue403);
    RUN_SUITE(repro_issue434);
    RUN_SUITE(repro_issue471);
    RUN_SUITE(repro_issue221);
    RUN_SUITE(repro_issue548);
    RUN_SUITE(repro_new_ts_class_field_arrow);
    RUN_SUITE(repro_new_py_tuple_unpack);
    RUN_SUITE(repro_new_cypher_limit_zero);
    RUN_SUITE(repro_issue363);
    RUN_SUITE(repro_issue581);
    RUN_SUITE(repro_invariant_calls);
    RUN_SUITE(repro_invariant_graph);
    RUN_SUITE(repro_invariant_breadth);
    RUN_SUITE(repro_invariant_enclosing_parity);
    RUN_SUITE(repro_invariant_lsp_rescue);
    RUN_SUITE(repro_invariant_discovery_fqn);
    RUN_SUITE(repro_grammar_core);
    RUN_SUITE(repro_grammar_scripting);
    RUN_SUITE(repro_grammar_functional);
    RUN_SUITE(repro_grammar_systems);
    RUN_SUITE(repro_grammar_web);
    RUN_SUITE(repro_grammar_config);
    RUN_SUITE(repro_grammar_build);
    RUN_SUITE(repro_grammar_shells);
    RUN_SUITE(repro_grammar_scientific);
    RUN_SUITE(repro_grammar_markup);
    RUN_SUITE(repro_grammar_misc);
    RUN_SUITE(repro_lsp_c_cpp);
    RUN_SUITE(repro_lsp_go_py);
    RUN_SUITE(repro_lsp_ts);
    RUN_SUITE(repro_lsp_java_cs);
    RUN_SUITE(repro_lsp_kt_php_rust);

    TEST_SUMMARY();
}
