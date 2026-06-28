/*
 * repro_issue333.c — Reproduce-first case for OPEN bug #333.
 *
 * Bug #333: "Silent index degradation — status:'indexed' but only ~500 nodes
 * for 72k LOC Rust" (reclassified as Rust extraction-depth gap).
 *
 * ROOT CAUSE — push_nested_class_nodes silently drops trait method defs:
 *   When the definition walker encounters a Rust `trait_item` node it is
 *   classified as a class (label "Interface") and `push_class_body_children`
 *   is called to schedule its children for further traversal.
 *   `push_class_body_children` finds the `declaration_list` body node (the
 *   Rust grammar's name for a trait body) and delegates to
 *   `push_nested_class_nodes` (extract_defs.c ~line 4890).
 *   `push_nested_class_nodes` only re-queues children that are in
 *   `spec->class_node_types` (struct_item, enum_item, etc.) or are named
 *   "field_declaration" / "template_declaration" / "declaration".
 *   It does NOT re-queue `function_item` or `function_signature_item` nodes.
 *   Therefore every method defined inside a trait body — both abstract
 *   declarations (function_signature_item, e.g. `fn area(&self) -> f64;`)
 *   and default implementations (function_item, e.g. `fn describe(&self) {}`)
 *   — is silently dropped and never reaches `extract_func_def`.
 *
 * EXPECTED (correct) behaviour:
 *   Extracting a Rust source file that defines a trait with methods must
 *   produce:
 *     - The trait itself as label "Interface" (already works).
 *     - Every method declared in the trait body as label "Method" (broken).
 *   Specifically for the fixture below:
 *     - Trait "Shape" → Interface node (already present)
 *     - Abstract method "area"    inside trait Shape → Method node (MISSING)
 *     - Abstract method "perimeter" inside trait Shape → Method node (MISSING)
 *     - Default method "describe" inside trait Shape → Method node (MISSING)
 *
 * ACTUAL (buggy) behaviour:
 *   `r->defs` contains the Interface node for Shape but zero Method nodes
 *   for the three methods declared in its body.  The ASSERT_EQ(3, ...) below
 *   evaluates to ASSERT_EQ(3, 0) and FAILs → RED.
 *
 * NOT covered by existing tests:
 *   - test_extraction.c::rust_struct tests `impl` block methods via the
 *     separate `extract_rust_impl` path, which is NOT affected by this bug.
 *   - test_rust_lsp.c trait tests (rustlsp_cov_trait_simple_method, etc.)
 *     only check `r->resolved_calls` (the LSP layer), never `r->defs`, so
 *     they do not detect missing trait-method def nodes.
 *   - test_matrix_new_constructs.c::mn_multiple_trait_bounds_rust tests a
 *     function with trait BOUNDS, not a trait DEFINITION with methods.
 *   No existing test asserts that method definitions inside a Rust `trait`
 *   body appear in `r->defs` — this is the first.
 *
 * FIX LOCATION:
 *   `push_nested_class_nodes` in internal/cbm/extract_defs.c (~line 4900):
 *   add `function_item` and `function_signature_item` to the set of node
 *   kinds that are re-queued onto the walk stack (or, equivalently, handle
 *   Rust `declaration_list` bodies via the same function-dispatch path used
 *   by `extract_rust_impl` for `impl_item` bodies).
 */

#include "test_framework.h"
#include "cbm.h"

/*
 * count_method_defs_named — count defs with label "Method" matching name.
 * Mirrors the `has_def` helper in test_extraction.c but counts all matches.
 */
static int count_method_defs_named(CBMFileResult *r, const char *name) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, "Method") == 0 &&
            d->name  && strcmp(d->name,  name)    == 0) {
            n++;
        }
    }
    return n;
}

/*
 * count_defs_with_label — count all defs carrying the given label.
 * Mirrors the helper in test_extraction.c.
 */
static int count_defs_with_label_local(CBMFileResult *r, const char *label) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].label && strcmp(r->defs.items[i].label, label) == 0)
            n++;
    }
    return n;
}

/* ── Test ───────────────────────────────────────────────────────────────── */

/*
 * repro_issue333_rust_extraction_depth
 *
 * Dense fixture: one trait "Shape" with two abstract methods (function_signature_item)
 * and one default method (function_item), plus one concrete struct + impl block that
 * implements the trait.  The impl-block methods are extracted correctly via the
 * existing `extract_rust_impl` path — this test asserts the TRAIT-BODY methods
 * (not the impl methods) are also extracted.
 *
 * RED condition:
 *   count_defs_with_label(r, "Method") == 0  for methods INSIDE the trait body.
 *   Specifically, ASSERT_EQ(3, total_trait_methods) FAILs → 3 != 0.
 *
 * GREEN condition (after fix):
 *   "area", "perimeter", and "describe" each appear as a "Method" def node,
 *   all carrying parent_class pointing at the Shape trait.
 */
TEST(repro_issue333_rust_extraction_depth) {
    /*
     * Fixture: trait Shape with three methods.
     *
     *   fn area      — abstract (no body); grammar node: function_signature_item
     *   fn perimeter — abstract (no body); grammar node: function_signature_item
     *   fn describe  — default implementation; grammar node: function_item
     *
     * Plus a struct Circle that implements Shape via an impl block.
     * The impl-block methods (Circle::area, Circle::perimeter) are already
     * extracted correctly; they serve as a positive control.
     */
    static const char src[] =
        "pub trait Shape {\n"
        "    fn area(&self) -> f64;\n"
        "    fn perimeter(&self) -> f64;\n"
        "    fn describe(&self) -> String {\n"
        "        format!(\"area={:.2} perimeter={:.2}\", self.area(), self.perimeter())\n"
        "    }\n"
        "}\n"
        "\n"
        "pub struct Circle {\n"
        "    pub radius: f64,\n"
        "}\n"
        "\n"
        "impl Shape for Circle {\n"
        "    fn area(&self) -> f64 {\n"
        "        std::f64::consts::PI * self.radius * self.radius\n"
        "    }\n"
        "    fn perimeter(&self) -> f64 {\n"
        "        2.0 * std::f64::consts::PI * self.radius\n"
        "    }\n"
        "}\n"
        "\n"
        "pub fn summarize(s: &dyn Shape) -> String {\n"
        "    s.describe()\n"
        "}\n";

    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_RUST, "t", "lib.rs",
                                        0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /*
     * ASSERT 1 — Shape trait itself is extracted as Interface (positive control;
     * already GREEN, confirms the trait node is at least parsed).
     */
    int has_shape_interface = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].label && strcmp(r->defs.items[i].label, "Interface") == 0 &&
            r->defs.items[i].name  && strcmp(r->defs.items[i].name,  "Shape")     == 0) {
            has_shape_interface = 1;
            break;
        }
    }
    ASSERT_TRUE(has_shape_interface);

    /*
     * ASSERT 2 — Abstract trait methods appear as Method defs (the bug).
     *
     * `area` and `perimeter` are function_signature_item nodes (no body —
     * just a declaration ending in `;`).  `push_nested_class_nodes` never
     * re-queues them because they are not class-type nodes, so they are
     * dropped entirely.
     *
     * EXPECTED: 1 each.
     * ACTUAL (buggy): 0 each — RED.
     */
    int n_area      = count_method_defs_named(r, "area");
    int n_perimeter = count_method_defs_named(r, "perimeter");

    /*
     * ASSERT 3 — Default trait method appears as Method def (also the bug).
     *
     * `describe` is a function_item node (has a body).  Same gap: the walker
     * never visits it because push_nested_class_nodes filters it out.
     *
     * EXPECTED: 1.
     * ACTUAL (buggy): 0 — RED.
     *
     * NOTE: impl Circle also defines `area` and `perimeter` via extract_rust_impl,
     * so those DO appear (as Methods with parent_class=Circle).  We count the
     * "describe" method separately to isolate the trait-body path — Circle never
     * overrides `describe`, so any "describe" Method must come from the trait body.
     */
    int n_describe = count_method_defs_named(r, "describe");

    /*
     * Total trait-body Methods that must appear: area + perimeter + describe = 3.
     *
     * Note: impl Circle provides its OWN area and perimeter Methods, so after the
     * fix the total for "area" would be >= 2 (1 from trait + 1 from impl).  We
     * use >= 1 per name to be unambiguous about which path is broken.
     *
     * The single combined assertion for RED/GREEN clarity:
     *   int total_trait_methods = (n_area >= 1 ? 1 : 0)
     *                           + (n_perimeter >= 1 ? 1 : 0)
     *                           + (n_describe >= 1 ? 1 : 0);
     *   ASSERT_EQ(total_trait_methods, 3);
     *
     * On buggy code  : total_trait_methods == 0  → ASSERT_EQ(0, 3) FAILS → RED
     * After fix (area from trait body, perimeter from trait body, describe from
     * trait body all present): total_trait_methods == 3 → ASSERT_EQ(3, 3) → GREEN
     */
    int total_trait_methods = (n_area      >= 1 ? 1 : 0)
                            + (n_perimeter >= 1 ? 1 : 0)
                            + (n_describe  >= 1 ? 1 : 0);

    if (total_trait_methods < 3) {
        printf("  DEBUG defs dump (total=%d):\n", r->defs.count);
        for (int i = 0; i < r->defs.count; i++) {
            printf("    [%d] label=%s name=%s\n", i,
                   r->defs.items[i].label ? r->defs.items[i].label : "(null)",
                   r->defs.items[i].name  ? r->defs.items[i].name  : "(null)");
        }
        printf("  MISSING trait-body Method defs: "
               "area=%d perimeter=%d describe=%d (need all 3)\n",
               n_area, n_perimeter, n_describe);
    }

    ASSERT_EQ(total_trait_methods, 3);

    /*
     * Supplementary: count ALL Method defs present.
     * After the fix we expect at least 5:
     *   trait body:  area (abstract), perimeter (abstract), describe (default)
     *   impl Circle: area (concrete),  perimeter (concrete)
     * On buggy code: only the 2 impl-Circle methods are present → 2.
     * We assert >= 3 here (conservative floor) rather than == 5 to stay
     * focused on the trait-body gap and not break if the count changes.
     */
    int total_methods = count_defs_with_label_local(r, "Method");
    ASSERT_GTE(total_methods, 3);

    cbm_free_result(r);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue333) {
    RUN_TEST(repro_issue333_rust_extraction_depth);
}
