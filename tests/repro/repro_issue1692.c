/*
 * repro_issue1692.c — Reproduce-first case for OPEN bug #1692.
 *
 * Issue: #1692 — "C#: ... ASP.NET Core [Route]/[HttpGet] attribute routes
 * not extracted (cross-repo-intelligence returns 0 edges)". The reporter's
 * repro 2 shows a class carrying three stacked attributes
 * ([ServiceFilter]/[Route]/[ApiController]) where DECORATES exists 13,964
 * times elsewhere in the same graph but zero times for that declaration.
 *
 * Root cause (traced further than the issue body): C# does not attach one
 * `attribute_list` wrapper holding every bracket group above a declaration —
 * each `[A]` / `[B]` / `[C]` line compiles to its OWN `attribute_list` node,
 * so a stack of three produces three separate `attribute_list` siblings
 * among the declaration's children. `find_jvm_modifiers()`
 * (internal/cbm/extract_defs.c) located that wrapper with
 * `ts_node_child_by_field_name(node, "attribute_list", ...)`, which only
 * ever returns the FIRST child registered under a repeated field — so only
 * the first bracket group's attribute(s) ever produced a DECORATES edge;
 * every attribute after the first was silently dropped, with no warning and
 * no staleness signal (confirmed indexed, not stale, via grafo_coverage in
 * the downstream McpDevGrafo-bugs.md BUG-004 report that led here).
 *
 * Expected (correct) behaviour:
 *   A method carrying N stacked attributes gets exactly N outbound
 *   DECORATES edges — not just one for the first bracket group.
 *
 * Why RED on current code: only the first attribute's DECORATES edge
 * exists, so the project-wide DECORATES count is 1, not 3.
 *
 * Scope — why this file is C#-only:
 *   find_jvm_modifiers() shares ONE switch across Java/Kotlin/Swift/C#/PHP,
 *   but only C# can reach this bug. Verified against the vendored grammars:
 *   `[A] [B] [C]` in C# parses as THREE sibling `attribute_list` nodes, while
 *   PHP's `#[A] #[B] #[C]` parses as ONE `attribute_list` holding three
 *   `attribute_group` children — so the first-child lookup already found them
 *   all and PHP never dropped an attribute. Java/Kotlin/Swift use the
 *   `modifiers` wrapper, a single node that never repeats. PHP's DECORATES
 *   path stays covered by mkc_c3_php8_attribute (test_matrix_known_classes.c);
 *   a stacked-PHP fixture here would pass with or without the fix and prove
 *   nothing. No other language reaches this function (`default` returns 0).
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "repro_harness.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture: C# ───────────────────────────────────────────────────────────
 *
 * One C# method, three stacked attributes, nothing else in the file that
 * could carry a DECORATES edge — so a project-wide edge count is as precise
 * as scoping to the method node, without the extra node lookup.
 * ──────────────────────────────────────────────────────────────────────── */
static const char *kStackedAttrsCs = "using System;\n"
                                     "\n"
                                     "namespace ReproNS\n"
                                     "{\n"
                                     "    public class AttrController\n"
                                     "    {\n"
                                     "        [Foo]\n"
                                     "        [Bar(\"x\")]\n"
                                     "        [Baz]\n"
                                     "        public void StackedAttrsMethod() { }\n"
                                     "    }\n"
                                     "}\n";

TEST(repro_issue1692_csharp_stacked_attributes_all_decorate) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, "StackedAttrs.cs", kStackedAttrsCs);
    ASSERT_NOT_NULL(store);

    int decorates_count = rh_count_edges(store, lp.project, "DECORATES");
    if (decorates_count != 3) {
        fprintf(stderr,
                "  [1692] FAIL decorates=%d (expected 3 — one per stacked [Foo][Bar][Baz]; "
                "only the first attribute_list's attribute(s) survive find_jvm_modifiers' "
                "single-field lookup)\n",
                decorates_count);
    }
    ASSERT_EQ(3, decorates_count); /* one DECORATES per stacked attribute, not just the first */

    rh_cleanup(&lp, store);
    PASS();
}

SUITE(repro_issue1692) {
    RUN_TEST(repro_issue1692_csharp_stacked_attributes_all_decorate);
}
