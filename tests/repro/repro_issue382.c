/*
 * repro_issue382.c — Reproduce-first case for OPEN bug #382.
 *
 * Bug #382: "Java: @Annotation, signatures, and all AST properties missing
 * from graph nodes"
 *
 * Root cause (confirmed by maintainer + reporter re-open):
 *   extract_decorators() in internal/cbm/extract_defs.c first scans
 *   ts_node_prev_sibling() looking for nodes of type "annotation" /
 *   "marker_annotation".  In the Java AST emitted by tree-sitter-java, those
 *   nodes are NOT prev-siblings of either the class_declaration or the
 *   method_declaration — they live INSIDE the node's own `modifiers` child:
 *
 *     class_declaration
 *       modifiers
 *         marker_annotation  <- @Entity
 *         marker_annotation  <- @RestController
 *       type_identifier: "User"
 *       class_body
 *         method_declaration
 *           modifiers
 *             marker_annotation  <- @Override
 *             annotation         <- @GetMapping("/users")
 *           type_identifier: "String"
 *           ...
 *
 *   The code does have a fallback that calls find_jvm_modifiers() to search
 *   the `modifiers` child when prev-sibling count == 0, which covers the
 *   simple @GetMapping-on-method case already tested in test_extraction.c
 *   (extract_java_method_annotations_issue382, which passes green on v0.7.0).
 *
 *   What is NOT covered by that existing test:
 *     a) CLASS-LEVEL annotations (@Entity, @RestController) on the class node
 *        itself — the existing test only extracts Method nodes; it never
 *        checks the Class node's .decorators.
 *     b) marker_annotation (no-arg form, e.g. @Override, @Entity) on methods
 *        — the existing test uses @GetMapping("/x") which is a full
 *        `annotation` node with arguments and does a substring match against
 *        the whole text "@GetMapping(\"/x\")".  marker_annotations have a
 *        different tree-sitter node type and are historically mis-counted.
 *     c) Multiple stacked annotations on a single method/class.
 *
 *   These cases regress when the fallback path is absent or broken (e.g. the
 *   fix only wired the method path, not the class path, or it works for
 *   `annotation` nodes but not `marker_annotation`).
 *
 * Expected (correct) behaviour:
 *   - The Class def for "User" carries decorators:
 *       decorators[0] contains "Entity"
 *       decorators[1] contains "RestController"  (or vice-versa)
 *   - The Method def for "getUser" carries decorators:
 *       at least one entry contains "Override"
 *       at least one entry contains "GetMapping"
 *   - method "getUser" has a non-empty signature.
 *
 * Actual (buggy) behaviour:
 *   - Class def for "User": decorators == NULL (no annotations extracted)
 *   - Method def for "getUser": marker_annotation @Override is dropped;
 *     decorators may be NULL or miss @Override.
 *   → assertions below are RED on current code if either path is broken.
 *
 * Why this is STRONGER than the existing test_extraction.c #382 reference:
 *   1. It asserts decorators on the CLASS node — never checked before.
 *   2. It specifically asserts that a marker_annotation (@Override, @Entity)
 *      is captured, not just a full annotation with arguments.
 *   3. It asserts BOTH annotations on a multi-annotated class, exercising the
 *      count loop that must find > 1 entry.
 *   4. It uses ASSERT_NOT_NULL(m->decorators) before touching decorators[i],
 *      so a NULL decorators field fails loudly rather than crashing/skipping.
 */

#include "test_framework.h"
#include "cbm.h"

/* Convenience: extract one file, return result (caller frees). */
static CBMFileResult *rx(const char *src, CBMLanguage lang,
                         const char *proj, const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, proj, path,
                            0, NULL, NULL);
}

/* Return the first definition whose label AND name both match (either may be
 * NULL to wildcard). Mirrors the helper in repro_extraction.c. */
static CBMDefinition *find_def(CBMFileResult *r, const char *label,
                               const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        return d;
    }
    return NULL;
}

/* Return 1 if any entry in the NULL-terminated decorators array contains
 * needle as a substring. */
static int decorators_contain(const CBMDefinition *d, const char *needle) {
    if (!d || !d->decorators)
        return 0;
    for (int i = 0; d->decorators[i]; i++) {
        if (strstr(d->decorators[i], needle))
            return 1;
    }
    return 0;
}

/* ───────────────────────────────────────────────────────────────────
 * repro_issue382_java_annotations_on_nodes
 *
 * Asserts that BOTH the Class node AND the Method node produced by
 * cbm_extract_file carry their Java annotations in .decorators:
 *
 *   @Entity
 *   @RestController
 *   public class User {
 *       @Override
 *       @GetMapping("/users")
 *       public String getUser(String id) { return id; }
 *   }
 *
 * RED if:
 *   • The Class "User" has decorators == NULL  (class-level annots dropped)
 *   • The Class "User" decorators do not contain "Entity"
 *   • The Class "User" decorators do not contain "RestController"
 *   • The Method "getUser" has decorators == NULL (method-level annots dropped)
 *   • The Method "getUser" decorators do not contain "Override"  ← marker_annotation
 *   • The Method "getUser" decorators do not contain "GetMapping" ← annotation
 *   • The Method "getUser" has NULL or empty signature
 * ─────────────────────────────────────────────────────────────────── */
TEST(repro_issue382_java_annotations_on_nodes) {
    CBMFileResult *r = rx(
        "@Entity\n"
        "@RestController\n"
        "public class User {\n"
        "    @Override\n"
        "    @GetMapping(\"/users\")\n"
        "    public String getUser(String id) { return id; }\n"
        "}\n",
        CBM_LANG_JAVA, "t", "User.java");

    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* ── Class node: two class-level marker_annotations ── */
    CBMDefinition *cls = find_def(r, "Class", "User");
    ASSERT_NOT_NULL(cls);

    /* The Class def MUST carry a non-NULL decorators array.
     * RED if class-level annotations are silently dropped. */
    ASSERT_NOT_NULL(cls->decorators);

    /* @Entity (marker_annotation) must be present on the Class. */
    ASSERT_TRUE(decorators_contain(cls, "Entity"));

    /* @RestController (marker_annotation) must also be present. */
    ASSERT_TRUE(decorators_contain(cls, "RestController"));

    /* ── Method node: one marker_annotation + one annotation ── */
    CBMDefinition *method = find_def(r, "Method", "getUser");
    ASSERT_NOT_NULL(method);

    /* Method decorators must be non-NULL. */
    ASSERT_NOT_NULL(method->decorators);

    /* @Override is a marker_annotation (no argument list) — historically
     * the most likely to be missed if the extractor only handles the
     * `annotation` node type but not `marker_annotation`. */
    ASSERT_TRUE(decorators_contain(method, "Override"));

    /* @GetMapping("/users") is a full annotation (with argument) — this is
     * what the existing test_extraction.c case checks; include it here too
     * so we catch any regression. */
    ASSERT_TRUE(decorators_contain(method, "GetMapping"));

    /* Signature must be extracted: Java method_declaration has a `parameters`
     * field that the extractor reads into def.signature. */
    ASSERT_NOT_NULL(method->signature);
    ASSERT_TRUE(method->signature[0] != '\0');

    cbm_free_result(r);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────── */
SUITE(repro_issue382) {
    RUN_TEST(repro_issue382_java_annotations_on_nodes);
}
