/*
 * repro_lsp_ts.c — EXHAUSTIVE per-LSP-pass invariant suite for the TypeScript /
 * JavaScript / JSX hybrid LSP (internal/cbm/lsp/ts_lsp.c).
 *
 * WHAT THIS ASSERTS — the LSP RESOLUTION CONTRACT, one invariant per strategy.
 *   The TS cross resolver resolves each call via a specific STRATEGY and tags the
 *   resulting CALLS edge in its properties_json with
 *       "strategy":"lsp_<name>"
 *   (see ts_emit_resolved_call, ts_lsp.c:109-120; every concrete emit site passes
 *   a literal "lsp_ts..." string). Each strategy keys on a precise TS/TSX
 *   construct. This suite builds the MINIMAL fixture that exercises exactly one
 *   strategy, indexes it through the full production pipeline, and asserts TWO
 *   things:
 *     (a) callable-sourcing — the inner call is sourced at a Function/Method
 *         node, never at a Module/File node (inv_count_calls_by_source →
 *         module_sourced == 0). A Module-sourced call is the #554 attribution
 *         bug; this is the broad correctness floor.
 *     (b) strategy-presence — some CALLS edge carries "lsp_<strategy>" in its
 *         properties_json (inv_edge_has_strategy). This is the PRECISE per-pass
 *         invariant: it proves that exact resolution path fired and survived into
 *         the graph.
 *
 * RED vs GREEN — this is a STATUS BOARD, not a pass/fail gate (runs only under
 *   make test-repro / bug-repro.yml, never the branch-protection ci-ok gate):
 *     - GREEN  = the LSP strategy works end-to-end = a permanent regression
 *                guard that it keeps working.
 *     - RED    = the strategy is dropped, or the call lands Module-sourced, or
 *                the rescue is discarded. Either way the per-pass TEST DOCUMENTS
 *                the exact gap for the eventual fixer.
 *
 * TIE TO repro_invariant_lsp_rescue.c — that file pins the MECHANISM by which
 *   these can silently fail: cbm_pipeline_find_lsp_resolution joins each
 *   LSP-resolved call to the tree-sitter call by EXACT caller-QN string equality.
 *   When tree-sitter's enclosing-func walk falls back to the MODULE QN but the
 *   LSP built the real method QN, the strcmp never matches, the LSP rescue is
 *   discarded, and the edge stays Module-sourced with a registry strategy —
 *   NEVER an "lsp_" strategy. So a strategy that is correctly EMITTED by ts_lsp.c
 *   can still be ABSENT from the graph here: the exact-QN join suppresses it.
 *   Whenever a strategy below is RED, suspect that join first (a same-file
 *   in-function fixture sidesteps it; a cross-file fixture exercises it).
 *
 * STRATEGY INVENTORY — every literal "lsp_..." emitted by ts_lsp.c, grepped from
 *   the source (grep '"lsp_' internal/cbm/lsp/ts_lsp.c), with its keying site:
 *     lsp_ts_local      (ts_lsp.c:2322)  bare identifier call f() resolving to a
 *                                        module-local function (call_expression
 *                                        function is an `identifier`, found in the
 *                                        module registry).
 *     lsp_ts_method     (ts_lsp.c:2284)  obj.method() type-based dispatch on a
 *                                        receiver whose type is a NAMED in-file
 *                                        class (member_expression, lookup_method
 *                                        hits).
 *     lsp_ts_namespace  (ts_lsp.c:2246)  Ns.fn() where Ns is a namespace import
 *                                        (`import * as Ns from "./mod"`); the
 *                                        member_expression object is an identifier
 *                                        matching an import local name, fn resolves
 *                                        in that module's registry.
 *     lsp_ts_import     (ts_lsp.c:2334)  bare identifier call to an imported
 *                                        function (`import { helper } ...`); the
 *                                        identifier matches an import local name and
 *                                        resolves in the imported module's registry.
 *     lsp_ts_jsx        (ts_lsp.c:2647)  <Comp/> JSX element whose tag is a
 *                                        module-local component function (TSX only;
 *                                        uppercase tag, resolves via the module
 *                                        registry).
 *     lsp_ts_jsx_import (ts_lsp.c:2657)  <Comp/> JSX element whose tag is an
 *                                        imported component (TSX only; tag matches
 *                                        an import local name → synthetic
 *                                        "<module>.<Comp>" QN). NOTE: this site
 *                                        builds the callee QN WITHOUT verifying the
 *                                        symbol exists in the registry, so it can
 *                                        emit even when the import target is absent.
 *     lsp_ts            (ts_lsp.c:116)   DEFAULT fallback inside ts_emit_resolved_call
 *                                        used only when a caller passes a NULL
 *                                        strategy. Every concrete emit site passes a
 *                                        literal "lsp_ts..." string, so "lsp_ts" is
 *                                        (as of this writing) never emitted as a
 *                                        distinct tag — expected ABSENT (RED). This
 *                                        TEST documents that the bare-"lsp_ts" path
 *                                        has no live caller; if it ever goes GREEN a
 *                                        new NULL-strategy emit site appeared.
 *     lsp_unresolved    (ts_lsp.c:128)   fallback marker for an unresolved call
 *                                        (ts_emit_unresolved_call, confidence 0.0).
 *                                        A 0.0-confidence unresolved entry is
 *                                        typically NOT promoted into a CALLS edge
 *                                        with the strategy tag, so this is expected
 *                                        ABSENT (RED) — it documents whether
 *                                        "lsp_unresolved" surfaces in the graph.
 *
 * LANGUAGE SELECTION — the filename extension picks the language exactly as the
 *   production indexer does: ".ts" → CBM_LANG_TYPESCRIPT, ".tsx" → CBM_LANG_TSX.
 *   jsx_mode (required by resolve_jsx_element, ts_lsp.c:2620) is enabled ONLY for
 *   CBM_LANG_TSX (cbm.c:619, pass_lsp_cross.c:267), so the two JSX fixtures use
 *   ".tsx" files; the non-JSX fixtures use ".ts".
 *
 * NOTE: line comments only inside this header (no nested block comments, per
 * coding rules).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <string.h>

/* ── Shared per-strategy runners (DRY) ───────────────────────────────────── */

/*
 * assert_lsp_strategy_files
 *
 * Index an N-file fixture and assert the per-pass LSP RESOLUTION CONTRACT:
 *   1. the store opened (precondition — a setup failure is a FAIL, not a skip);
 *   2. callable-sourcing: NO CALLS edge is Module/File-sourced, and at least one
 *      callable-sourced CALLS edge exists (else there is no signal at all);
 *   3. strategy-presence: some CALLS edge carries "lsp_<strategy>" in its
 *      properties_json.
 *
 * The filename extension selects the language exactly as the production indexer
 * does (".ts" → TypeScript, ".tsx" → TSX). Returns 0 on PASS (GREEN), non-zero
 * on FAIL (RED) — the redness is the documented per-pass status.
 */
static int assert_lsp_strategy_files(const RFile *files, int nfiles,
                                     const char *strategy) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, nfiles);
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed for strategy %s\n", tf_red(),
               tf_reset(), __FILE__, __LINE__, strategy);
        rh_cleanup(&lp, store);
        return 1;
    }

    int module_sourced = -1;
    int callable_sourced = -1;
    inv_count_calls_by_source(store, lp.project, &module_sourced,
                              &callable_sourced);

    int has_strategy = inv_edge_has_strategy(store, lp.project, strategy);

    int rc = 0;

    /* (a) callable-sourcing floor: zero Module/File-sourced CALLS edges. */
    if (module_sourced != 0) {
        printf("  %sFAIL%s %s:%d: strategy %s: %d Module-sourced CALLS "
               "(expected 0)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy,
               module_sourced);
        rc = 1;
    }
    /* There must be a callable-sourced CALLS edge, else the fixture produced no
     * call signal and the strategy assertion below would be vacuous. */
    if (callable_sourced <= 0) {
        printf("  %sFAIL%s %s:%d: strategy %s: no callable-sourced CALLS edge "
               "(callable=%d)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy,
               callable_sourced);
        rc = 1;
    }

    /* (b) the precise per-pass invariant: the resolution strategy is present. */
    if (!has_strategy) {
        printf("  %sFAIL%s %s:%d: strategy %s ABSENT from any CALLS edge "
               "properties_json\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy);
        rc = 1;
    }

    rh_cleanup(&lp, store);
    return rc;
}

/* Single-file convenience wrapper. */
static int assert_lsp_strategy(const char *filename, const char *src,
                               const char *strategy) {
    RFile f = {filename, src};
    return assert_lsp_strategy_files(&f, 1, strategy);
}

/*
 * assert_no_resolvable_edge — the ACCURATE invariant for a call whose callee is
 * genuinely UNRESOLVABLE (undeclared symbol). No node can exist for it, so no
 * CALLS edge can ever form and no resolution strategy can land on an edge. Index
 * the single-file fixture and assert NO CALLS edge targets a node whose QN
 * contains `callee_substr`. Returns 0 on PASS, non-zero on FAIL.
 */
static int assert_no_resolvable_edge(const char *filename, const char *src,
                                     const char *callee_substr) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, filename, src);
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed for no-edge callee %s\n", tf_red(),
               tf_reset(), __FILE__, __LINE__, callee_substr);
        rh_cleanup(&lp, store);
        return 1;
    }
    int rc = 0;
    /* Exercised-check: the fixture MUST produce at least one callable-sourced
     * CALLS edge (its in-fixture control call). Without it the "no edge to
     * <callee>" invariant is VACUOUS — it also passes when extraction silently
     * produced nothing, so a green would not prove the unresolvable call was
     * actually processed and correctly dropped. */
    int module_sourced = -1;
    int callable_sourced = -1;
    inv_count_calls_by_source(store, lp.project, &module_sourced, &callable_sourced);
    (void)module_sourced;
    if (callable_sourced <= 0) {
        printf("  %sFAIL%s %s:%d: no callable-sourced CALLS edge — fixture not "
               "exercised; the no-edge invariant for %s is vacuous\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, callee_substr);
        rc = 1;
    }
    if (!inv_no_calls_edge_to_qn(store, lp.project, callee_substr)) {
        printf("  %sFAIL%s %s:%d: a CALLS edge unexpectedly targets %s "
               "(expected NONE — callee is unresolvable)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, callee_substr);
        rc = 1;
    }
    rh_cleanup(&lp, store);
    return rc;
}

/*
 * assert_strategy_absent — assert a given strategy tag NEVER surfaces on any
 * CALLS edge. Used for the bare "lsp_ts" probe: the default fallback tag is
 * never emitted as a distinct strategy (every concrete site passes a literal
 * "lsp_ts_*"), and the fixture is an UNRESOLVED call (no "lsp_ts_*" edge to
 * substring-alias against), so its absence is the accurate, intended invariant.
 * Returns 0 on PASS (tag absent), non-zero on FAIL (tag unexpectedly present).
 */
static int assert_strategy_absent(const char *filename, const char *src,
                                  const char *strategy) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, filename, src);
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed for absent-strategy %s\n", tf_red(),
               tf_reset(), __FILE__, __LINE__, strategy);
        rh_cleanup(&lp, store);
        return 1;
    }
    int rc = 0;
    if (inv_edge_has_strategy(store, lp.project, strategy)) {
        printf("  %sFAIL%s %s:%d: strategy %s unexpectedly PRESENT on a CALLS "
               "edge (expected ABSENT — bare fallback tag is never emitted)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, strategy);
        rc = 1;
    }
    rh_cleanup(&lp, store);
    return rc;
}

/* ── Fixtures ────────────────────────────────────────────────────────────────
 *
 * Each fixture is the MINIMAL construct ts_lsp.c keys on for one strategy. The
 * call we care about always lives inside a function or method so callable-
 * sourcing is testable; the callee is also defined in-file (or in a sibling file
 * for the cross-file import strategies) so the registry can resolve it.
 * ───────────────────────────────────────────────────────────────────────── */

/* lsp_ts_local — bare identifier call f() that resolves to a module-local
 * function (ts_lsp.c:2310-2322: call_expression function is an `identifier`,
 * cbm_registry_lookup_symbol_by_args hits on the module QN). */
static const char kTsLocal[] =
    "function helper(x: number): number { return x + 1; }\n"
    "function caller(v: number): number { return helper(v); }\n";

/* lsp_ts_method — obj.method() type-based dispatch on a NAMED in-file class
 * receiver (ts_lsp.c:2257-2284: member_expression, ts_eval_expr_type gives the
 * receiver's NAMED type, lookup_method finds the method). */
static const char kTsMethod[] =
    "class Counter {\n"
    "    inc(x: number): number { return x + 1; }\n"
    "}\n"
    "function caller(): number {\n"
    "    const c = new Counter();\n"
    "    return c.inc(1);\n"
    "}\n";

/* lsp_ts_namespace — Ns.fn() where Ns is a namespace import
 * (`import * as Ns from "./mod"`). ts_lsp.c:2233-2246: the member_expression
 * object is an `identifier` matching an import local name; fn resolves in that
 * imported module's registry → lsp_ts_namespace. Cross-file: util.ts exports the
 * function, main.ts imports the namespace and calls Util.compute(). */
static const RFile kTsNamespace[] = {
    {"util.ts",
     "export function compute(x: number): number { return x * 3; }\n"},
    {"main.ts",
     "import * as Util from \"./util\";\n"
     "function caller(v: number): number { return Util.compute(v); }\n"},
};

/* lsp_ts_import — bare identifier call to an imported function
 * (`import { helper } from "./mod"`). ts_lsp.c:2327-2334: the call_expression
 * function is an `identifier` matching an import local name; helper resolves in
 * the imported module's registry → lsp_ts_import. Cross-file: util.ts exports
 * helper, main.ts imports it by name and calls it bare. */
static const RFile kTsImport[] = {
    {"util.ts",
     "export function helper(x: number): number { return x + 5; }\n"},
    {"main.ts",
     "import { helper } from \"./util\";\n"
     "function caller(v: number): number { return helper(v); }\n"},
};

/* lsp_ts_jsx — <Comp/> JSX element whose tag is a module-local component
 * function (ts_lsp.c:2643-2647). TSX only (jsx_mode); the tag's first letter is
 * uppercase so it is NOT treated as an intrinsic HTML element; it resolves via
 * cbm_registry_lookup_symbol on the module QN. App() renders <Widget/> defined
 * in the same file. */
static const char kTsxJsx[] =
    "function Widget(): any { return null; }\n"
    "function App(): any {\n"
    "    return <Widget />;\n"
    "}\n";

/* lsp_ts_jsx_import — <Comp/> JSX element whose tag is an imported component
 * (ts_lsp.c:2652-2657). TSX only; the tag matches an import local name → a
 * synthetic "<module>.<Comp>" callee QN is emitted (this site does NOT verify
 * the symbol is in the registry). Cross-file: widget.tsx exports Widget,
 * app.tsx imports it and renders <Widget/>. */
static const RFile kTsxJsxImport[] = {
    {"widget.tsx",
     "export function Widget(): any { return null; }\n"},
    {"app.tsx",
     "import { Widget } from \"./widget\";\n"
     "function App(): any {\n"
     "    return <Widget />;\n"
     "}\n"},
};

/* lsp_ts — the DEFAULT fallback strategy inside ts_emit_resolved_call
 * (ts_lsp.c:116): used only when a caller passes a NULL strategy. Every concrete
 * emit site passes a literal "lsp_ts..." string, so "lsp_ts" is never emitted as
 * a distinct tag. This fixture is an ordinary resolved local call; we assert
 * whether the bare "lsp_ts" tag ever surfaces. EXPECTED ABSENT (RED): if it goes
 * GREEN, a new NULL-strategy emit site has appeared and should be audited.
 * NOTE: inv_edge_has_strategy does a substring match, and "lsp_ts" is a prefix of
 * "lsp_ts_local"/"lsp_ts_method"/etc., so a local-call fixture would substring-
 * match "lsp_ts" via "lsp_ts_local" and report a false GREEN. To probe the bare
 * tag in isolation we use an UNRESOLVED call (totallyUnknownFn) whose only
 * possible tag is the unresolved marker — there is no "lsp_ts_*" edge to alias
 * against, so a GREEN here would mean a literal bare "lsp_ts" edge exists. */
static const char kTsDefault[] =
    "function caller(v: number): number { return totallyUnknownFn(v); }\n";

/* lsp_unresolved — a call to a function not in the registry; the resolver
 * records the fallback marker via ts_emit_unresolved_call (ts_lsp.c:122-132,
 * strategy = "lsp_unresolved", confidence 0.0). A 0.0-confidence unresolved entry
 * is typically NOT promoted into a CALLS edge carrying the strategy tag, so this
 * is EXPECTED ABSENT (RED) — it documents whether "lsp_unresolved" surfaces in
 * the graph. */
static const char kTsUnresolved[] =
    "function known(x: number): number { return x + 1; }\n"
    "function caller(v: number): number { return known(v) + totallyUnknownFn(v); }\n";

/* ── Per-strategy tests ──────────────────────────────────────────────────── */

TEST(repro_lsp_ts_local) {
    return assert_lsp_strategy("main.ts", kTsLocal, "lsp_ts_local");
}

TEST(repro_lsp_ts_method) {
    return assert_lsp_strategy("main.ts", kTsMethod, "lsp_ts_method");
}

TEST(repro_lsp_ts_namespace) {
    return assert_lsp_strategy_files(kTsNamespace,
                                     (int)(sizeof(kTsNamespace) /
                                           sizeof(kTsNamespace[0])),
                                     "lsp_ts_namespace");
}

TEST(repro_lsp_ts_import) {
    return assert_lsp_strategy_files(
        kTsImport, (int)(sizeof(kTsImport) / sizeof(kTsImport[0])),
        "lsp_ts_import");
}

TEST(repro_lsp_ts_jsx) {
    return assert_lsp_strategy("app.tsx", kTsxJsx, "lsp_ts_jsx");
}

TEST(repro_lsp_ts_jsx_import) {
    return assert_lsp_strategy_files(kTsxJsxImport,
                                     (int)(sizeof(kTsxJsxImport) /
                                           sizeof(kTsxJsxImport[0])),
                                     "lsp_ts_jsx_import");
}

TEST(repro_lsp_ts_default) {
    /* The bare "lsp_ts" fallback tag is never emitted as a distinct strategy
     * (every concrete site passes a literal "lsp_ts_*"); the fixture is an
     * UNRESOLVED call with no "lsp_ts_*" edge to substring-alias against. Per the
     * fixture header, the accurate invariant is that "lsp_ts" is ABSENT. */
    return assert_strategy_absent("main.ts", kTsDefault, "lsp_ts");
}

TEST(repro_lsp_ts_unresolved) {
    /* totallyUnknownFn is UNDECLARED — no node can exist for it, so no CALLS
     * edge can ever form. Assert the accurate no-resolvable-edge behaviour
     * instead of a resolution strategy on an edge (unachievable by design). */
    return assert_no_resolvable_edge("main.ts", kTsUnresolved, "totallyUnknownFn");
}

/* ── Suite ───────────────────────────────────────────────────────────────── */

SUITE(repro_lsp_ts) {
    RUN_TEST(repro_lsp_ts_local);
    RUN_TEST(repro_lsp_ts_method);
    RUN_TEST(repro_lsp_ts_namespace);
    RUN_TEST(repro_lsp_ts_import);
    RUN_TEST(repro_lsp_ts_jsx);
    RUN_TEST(repro_lsp_ts_jsx_import);
    RUN_TEST(repro_lsp_ts_default);
    RUN_TEST(repro_lsp_ts_unresolved);
}
