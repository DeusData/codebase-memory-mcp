/*
 * test_perl_lsp.c — Tests for the Perl Light Semantic Pass.
 *
 * Coverage mirrors tests/test_php_lsp.c, exercising the ten foundational Perl
 * resolution scenarios from .vbw-planning/phases/22-perl-lsp-semantic-resolution/
 * 22-RESEARCH.md (L331-341):
 *   1.  Method via bless-assignment       (my $o = Foo->new; $o->bar)
 *   2.  Constructor class-method type      (Foo->new returns Foo)
 *   3.  Static package call                (Foo::bar())
 *   4.  $self method dispatch              ($self = shift; $self->m)
 *   5.  @ISA inheritance
 *   6.  use parent MRO
 *   7.  use base MRO
 *   8.  Exporter import (use Mod qw(f); f())
 *   9.  require fallback (require Foo; Foo->bar)
 *   10. Unresolvable receiver emits NO spurious edge (negative test)
 *
 * The resolver populates result->resolved_calls with CBMResolvedCall edges. Per
 * the perl_lsp.c design (file header), sub QNs are `module_qn.subname` — the Perl
 * package is NOT woven into the sub QN. For these single-file fixtures the module
 * QN is `test.main` (from the cbm_extract_file "test"/"main.pl" args), so every
 * resolved sub lands at `test.main.<sub>`. The helpers below use substring
 * matching, so tests assert on the unique `main.<sub>` callee fragment. The Perl
 * package only governs method *dispatch* (which sub a receiver resolves to), not
 * the emitted QN string.
 */
#include "test_framework.h"
#include "cbm.h"
#include "../src/pipeline/lsp_resolve.h"
#include "lsp/perl_lsp.h"
#include "../src/pipeline/pass_lsp_cross.h"
#include <string.h>

/* ── Helpers (mirror test_php_lsp.c) ───────────────────────────── */

static CBMFileResult *extract_perl(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_PERL, "test", "main.pl", 0, NULL,
                            NULL);
}

static int find_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

static int require_resolved(const CBMFileResult *r, const char *callerSub, const char *calleeSub) {
    int idx = find_resolved(r, callerSub, calleeSub);
    if (idx < 0) {
        printf("  MISSING resolved call: caller~%s -> callee~%s (have %d)\n", callerSub, calleeSub,
               r->resolved_calls.count);
        for (int i = 0; i < r->resolved_calls.count; i++) {
            const CBMResolvedCall *rc = &r->resolved_calls.items[i];
            printf("    %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "(null)",
                   rc->callee_qn ? rc->callee_qn : "(null)", rc->strategy ? rc->strategy : "(null)",
                   rc->confidence);
        }
    }
    return idx;
}

static const CBMResolvedCall *find_resolved_with_strategy(const CBMFileResult *r,
                                                          const char *callerSub,
                                                          const char *calleeSub,
                                                          const char *strategy) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (!rc->caller_qn || !rc->callee_qn)
            continue;
        if (!strstr(rc->caller_qn, callerSub))
            continue;
        if (!strstr(rc->callee_qn, calleeSub))
            continue;
        if (strategy && (!rc->strategy || strcmp(rc->strategy, strategy) != 0))
            continue;
        return rc;
    }
    return NULL;
}

/* ── 1. Method dispatch via bless-assignment ($o = Foo->new) ───── */

TEST(perllsp_method_via_bless_assignment) {
    const char *src = "package Foo;\n"
                      "sub new { my $class = shift; return bless {}, $class; }\n"
                      "sub bar { return 1; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $obj = Foo->new;\n"
                      "    $obj->bar;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    /* $obj is typed Foo via Foo->new (bless); $obj->bar dispatches to Foo::bar,
     * emitted as test.main.bar. */
    int idx = require_resolved(r, "main.run", "main.bar");
    ASSERT(idx >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 2. Constructor class-method returns the package type ──────── */

TEST(perllsp_constructor_class_method) {
    /* Foo->new must yield type Foo so the subsequent method resolves on it. */
    const char *src = "package Foo;\n"
                      "sub new { return bless {}, shift; }\n"
                      "sub greet { return 'hi'; }\n"
                      "package main;\n"
                      "sub go {\n"
                      "    my $f = Foo->new();\n"
                      "    $f->greet();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.go", "main.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 3. Static package-qualified call (Foo::bar()) ─────────────── */

TEST(perllsp_static_package_call) {
    const char *src = "package Foo;\n"
                      "sub bar { return 42; }\n"
                      "package main;\n"
                      "sub caller_sub {\n"
                      "    Foo::bar();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.caller_sub", "main.bar") >= 0);
    /* The same edge is retrievable as a full CBMResolvedCall; static calls carry
     * the perl_static_call strategy. */
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "main.caller_sub", "main.bar", "perl_static_call");
    ASSERT(rc != NULL);
    ASSERT(rc->strategy != NULL);
    cbm_free_result(r);
    PASS();
}

/* ── 3b. Multi-level static package call (Foo::Bar::sub()) ──────── */
/* Regression: the resolver split the qualified name on the FIRST "::", so a
 * call to Foo::Bar::sub() was mis-parsed as pkg "Foo" / sub "Bar::sub" and
 * never resolved — falling through to the bare-name fallback, which collapsed
 * distinct packages' same-named subs onto one winner. Splitting on the LAST
 * "::" keeps the full package name so perl_lookup_method resolves correctly. */
TEST(perllsp_static_multilevel_package_call) {
    const char *src = "package Acme::Widget;\n"
                      "sub render { return 1; }\n"
                      "package main;\n"
                      "sub caller_sub {\n"
                      "    Acme::Widget::render();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    /* Before the fix this edge was absent (LSP emitted nothing for the
     * multi-level qualified call). */
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "main.caller_sub", "main.render", "perl_static_call");
    ASSERT(rc != NULL);
    ASSERT(rc->strategy != NULL);
    cbm_free_result(r);
    PASS();
}

/* ── 4. $self method dispatch ($self = shift) ──────────────────── */

TEST(perllsp_self_method) {
    const char *src = "package Widget;\n"
                      "sub new { return bless {}, shift; }\n"
                      "sub render { my $self = shift; $self->draw(); }\n"
                      "sub draw { return 1; }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.render", "main.draw") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_has_qw_arrayref_accessors) {
    /* `has [qw(a b)] => ...` (the qw word-list multi-accessor form) must emit an
     * accessor DEF for EACH name — 97 such accessors across Mojolicious emitted
     * nothing before the quoted_word_list handler, so every `$obj->name` to them
     * was unresolved. (Same-file accessor CALLS resolve via the pipeline's
     * same_module registry, not the per-file LSP, so this asserts the def
     * emission directly.) */
    const char *src = "package Widget;\n"
                      "use Mojo::Base -base;\n"
                      "has [qw(alpha beta)] => undef;\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    int has_alpha = 0, has_beta = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (!d->name || !d->label || strcmp(d->label, "Method") != 0)
            continue;
        if (strcmp(d->name, "alpha") == 0)
            has_alpha = 1;
        if (strcmp(d->name, "beta") == 0)
            has_beta = 1;
    }
    ASSERT(has_alpha);
    ASSERT(has_beta);
    cbm_free_result(r);
    PASS();
}

/* ── 5. @ISA inheritance ───────────────────────────────────────── */

TEST(perllsp_isa_inheritance) {
    /* Derived->new blesses into Derived; speak is inherited from Base via @ISA.
     * The dispatch walks Derived's embedded parent (Base) to find speak, emitted
     * as test.main.speak. */
    const char *src = "package Base;\n"
                      "sub speak { return 'base'; }\n"
                      "package Derived;\n"
                      "our @ISA = ('Base');\n"
                      "sub new { my $class = shift; return bless {}, $class; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $d = Derived->new;\n"
                      "    $d->speak;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.speak") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 6. use parent 'Base' MRO ──────────────────────────────────── */

TEST(perllsp_use_parent_inheritance) {
    const char *src = "package Base;\n"
                      "sub greet { return 'hi'; }\n"
                      "package Child;\n"
                      "use parent -norequire, 'Base';\n"
                      "sub new { my $class = shift; return bless {}, $class; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $c = Child->new;\n"
                      "    $c->greet;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7. use base 'Base' MRO ────────────────────────────────────── */

TEST(perllsp_use_base_inheritance) {
    const char *src = "package Base;\n"
                      "sub greet { return 'hi'; }\n"
                      "package Child;\n"
                      "use base 'Base';\n"
                      "sub new { my $class = shift; return bless {}, $class; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $c = Child->new;\n"
                      "    $c->greet;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7b. use Mojo::Base 'Base' MRO (Mojolicious idiom) ──────────── */

TEST(perllsp_use_mojo_base_inheritance) {
    /* Mojo::Base with a quoted parent establishes @ISA exactly like `use
     * parent`. The trailing -signatures flag must not disturb the parent
     * collection. This is the dominant real-world Perl inheritance idiom. */
    const char *src = "package Base;\n"
                      "sub greet { return 'hi'; }\n"
                      "package Child;\n"
                      "use Mojo::Base 'Base', -signatures;\n"
                      "sub new { my $class = shift; return bless {}, $class; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $c = Child->new;\n"
                      "    $c->greet;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 8. Exporter import (use Module qw(func); func()) ──────────── */

TEST(perllsp_exported_function) {
    /* func() is imported from Helper; the bare call resolves to Helper::func,
     * emitted as test.main.func via the Exporter import map. */
    const char *src = "package Helper;\n"
                      "sub func { return 1; }\n"
                      "package main;\n"
                      "use Helper qw(func);\n"
                      "sub run {\n"
                      "    func();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.func") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 8b. Seeded CPAN Exporter import (use Scalar::Util qw(blessed)) ─
 *
 * Regression for the import-map "::" vs "." mismatch (QA round 1, F1):
 * perl_collect_qw_imports used to build the colon-form target
 * "Scalar::Util::blessed", but the stdlib registry keys curated CPAN subs in
 * dotted form ("Scalar.Util.blessed") and lookup is exact-match — so the
 * import never resolved. The import target must be dotted to match. */
TEST(perllsp_cpan_exported_function) {
    /* blessed is a curated CPAN export (Scalar::Util) seeded by
     * cbm_perl_stdlib_register as "Scalar.Util.blessed". The bare call must
     * resolve to that seeded registry symbol via the Exporter import map. */
    const char *src = "package main;\n"
                      "use Scalar::Util qw(blessed);\n"
                      "sub run {\n"
                      "    my $x = bless {}, 'Foo';\n"
                      "    blessed($x);\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    /* Resolves to the dotted registry QN Scalar.Util.blessed. */
    ASSERT(require_resolved(r, "main.run", "Scalar.Util.blessed") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 9. require fallback (require Foo; Foo->bar()) ─────────────── */

TEST(perllsp_require_fallback) {
    const char *src = "package Foo;\n"
                      "sub bar { return 1; }\n"
                      "package main;\n"
                      "sub run {\n"
                      "    require Foo;\n"
                      "    Foo->bar();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.bar") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 9b. SUPER:: dispatch ($self->SUPER::method) ──────────────────
 *
 * Regression for the dead enclosing_parent_qn field (QA round 1, F4). The
 * header advertised SUPER:: support but the field was never populated/read, so
 * $self->SUPER::method() resolved to nothing. Now process_package_decl records
 * the package's first @ISA parent and perl_resolve_method_call routes a
 * SUPER:: call to that parent's method. */
TEST(perllsp_super_dispatch) {
    /* Child overrides greet and calls $self->SUPER::greet(); the SUPER call
     * must resolve to Base::greet (the parent), tagged perl_method_super. */
    const char *src = "package Base;\n"
                      "sub new { return bless {}, shift; }\n"
                      "sub greet { return 'base'; }\n"
                      "package Child;\n"
                      "our @ISA = ('Base');\n"
                      "sub greet {\n"
                      "    my $self = shift;\n"
                      "    return $self->SUPER::greet();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    /* The SUPER:: call resolves to a greet sub via the dedicated strategy. */
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "main.greet", "main.greet", "perl_method_super");
    ASSERT(rc != NULL);
    cbm_free_result(r);
    PASS();
}

/* ── 9c. SUPER:: with no known parent emits NO edge (zero-edge) ─── */
TEST(perllsp_super_no_parent_no_edge) {
    /* Orphan has no @ISA parent; SUPER::greet() must resolve to nothing rather
     * than guessing an edge. */
    const char *src = "package Orphan;\n"
                      "sub greet {\n"
                      "    my $self = shift;\n"
                      "    return $self->SUPER::greet();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(find_resolved_with_strategy(r, "main.greet", "greet", "perl_method_super") == NULL);
    cbm_free_result(r);
    PASS();
}

/* ── 10. Unresolvable receiver emits NO spurious edge (negative) ─ */

TEST(perllsp_unindexed_receiver_emits_block) {
    /* $thing has no inferable type (parameter from outside, never blessed/typed)
     * and Unknown::Pkg is not indexed. The resolver MUST emit zero edges for
     * these calls rather than guessing. */
    const char *src = "package main;\n"
                      "sub run {\n"
                      "    my $thing = get_external();\n"
                      "    $thing->do_work();\n"
                      "    Unknown::Pkg->mystery();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    /* No edge for the untyped scalar receiver. */
    ASSERT(find_resolved(r, "main.run", "do_work") < 0);
    /* No edge for the unindexed package receiver. */
    ASSERT(find_resolved(r, "main.run", "mystery") < 0);
    cbm_free_result(r);
    PASS();
}

/* Occurrence identity is independent of the pending public-QN decision. This
 * deliberately invokes the same Widget method twice, so both sites resolve to
 * one target under today's flat QNs and would still share one target if that QN
 * later becomes package-qualified. Each semantic record must nevertheless be
 * tied to its own full parser occurrence. */
TEST(perllsp_repeated_target_calls_join_by_exact_site) {
    static const char source[] = "package Widget;\n"
                                 "sub render { return 1; }\n"
                                 "package main;\n"
                                 "sub occurrence_probe {\n"
                                 "    Widget->render();\n"
                                 "    Widget->render();\n"
                                 "}\n";
    static const char call_text[] = "Widget->render()";

    const char *first_site = strstr(source, call_text);
    const char *second_site = first_site ? strstr(first_site + 1, call_text) : NULL;
    ASSERT_NOT_NULL(first_site);
    ASSERT_NOT_NULL(second_site);
    const uint32_t first_start = (uint32_t)(first_site - source);
    const uint32_t first_end = first_start + (uint32_t)strlen(call_text);
    const uint32_t second_start = (uint32_t)(second_site - source);
    const uint32_t second_end = second_start + (uint32_t)strlen(call_text);

    CBMFileResult *r = extract_perl(source);
    ASSERT_NOT_NULL(r);

    const CBMCall *first_call = NULL;
    const CBMCall *second_call = NULL;
    int render_carriers = 0;
    int zero_span_carriers = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (!call->enclosing_func_qn || !strstr(call->enclosing_func_qn, "main.occurrence_probe") ||
            !call->callee_name || !strstr(call->callee_name, "render")) {
            continue;
        }
        render_carriers++;
        if (call->site_end_byte <= call->site_start_byte)
            zero_span_carriers++;
        if (call->site_start_byte == first_start && call->site_end_byte == first_end)
            first_call = call;
        if (call->site_start_byte == second_start && call->site_end_byte == second_end)
            second_call = call;
    }

    ASSERT_EQ(render_carriers, 2);
    ASSERT_EQ(zero_span_carriers, 0);
    ASSERT_NOT_NULL(first_call);
    ASSERT_NOT_NULL(second_call);
    ASSERT_TRUE(first_call != second_call);

    const CBMResolvedCall *first_semantic = NULL;
    const CBMResolvedCall *second_semantic = NULL;
    int render_semantics = 0;
    int zero_span_semantics = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->kind != CBM_RESOLVED_INVOCATION || rc->confidence <= 0.0f || !rc->caller_qn ||
            !strstr(rc->caller_qn, "main.occurrence_probe") || !rc->callee_qn ||
            !strstr(rc->callee_qn, "render")) {
            continue;
        }
        render_semantics++;
        if (rc->site_end_byte <= rc->site_start_byte)
            zero_span_semantics++;
        if (rc->site_start_byte == first_start && rc->site_end_byte == first_end)
            first_semantic = rc;
        if (rc->site_start_byte == second_start && rc->site_end_byte == second_end)
            second_semantic = rc;
    }

    ASSERT_EQ(render_semantics, 2);
    ASSERT_EQ(zero_span_semantics, 0);
    ASSERT_NOT_NULL(first_semantic);
    ASSERT_NOT_NULL(second_semantic);
    ASSERT_TRUE(first_semantic != second_semantic);
    ASSERT_STR_EQ(first_semantic->callee_qn, second_semantic->callee_qn);

    const CBMResolvedCall *first_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, first_call, false);
    const CBMResolvedCall *second_joined =
        cbm_pipeline_find_lsp_resolution(&r->resolved_calls, second_call, false);
    ASSERT_TRUE(first_joined == first_semantic);
    ASSERT_TRUE(second_joined == second_semantic);

    cbm_free_result(r);
    PASS();
}

/* Keep the other ordinary resolver branches under the same occurrence
 * contract. The first regression above is a static method-call expression;
 * this helper also exercises function-call expressions and typed/inherited
 * method dispatch without coupling the tests to the pending package-QN
 * decision. */
static int assert_perl_repeated_exact_join(const char *source, const char *caller_fragment,
                                           const char *callee_fragment, const char *call_text) {
    const char *first_site = strstr(source, call_text);
    const char *second_site = first_site ? strstr(first_site + 1, call_text) : NULL;
    ASSERT_NOT_NULL(first_site);
    ASSERT_NOT_NULL(second_site);
    const uint32_t starts[2] = {(uint32_t)(first_site - source), (uint32_t)(second_site - source)};
    const uint32_t ends[2] = {starts[0] + (uint32_t)strlen(call_text),
                              starts[1] + (uint32_t)strlen(call_text)};

    CBMFileResult *r = extract_perl(source);
    ASSERT_NOT_NULL(r);

    const CBMCall *calls[2] = {NULL, NULL};
    int carrier_count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (!call->enclosing_func_qn || !strstr(call->enclosing_func_qn, caller_fragment) ||
            !call->callee_name || !strstr(call->callee_name, callee_fragment)) {
            continue;
        }
        carrier_count++;
        for (int occurrence = 0; occurrence < 2; occurrence++) {
            if (call->site_start_byte == starts[occurrence] &&
                call->site_end_byte == ends[occurrence]) {
                calls[occurrence] = call;
            }
        }
    }
    ASSERT_EQ(carrier_count, 2);
    ASSERT_NOT_NULL(calls[0]);
    ASSERT_NOT_NULL(calls[1]);

    const CBMResolvedCall *semantics[2] = {NULL, NULL};
    int semantic_count = 0;
    int zero_span_semantics = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &r->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_INVOCATION || resolved->confidence <= 0.0f ||
            !resolved->caller_qn || !strstr(resolved->caller_qn, caller_fragment) ||
            !resolved->callee_qn || !strstr(resolved->callee_qn, callee_fragment)) {
            continue;
        }
        semantic_count++;
        if (resolved->site_end_byte <= resolved->site_start_byte) {
            zero_span_semantics++;
        }
        for (int occurrence = 0; occurrence < 2; occurrence++) {
            if (resolved->site_start_byte == starts[occurrence] &&
                resolved->site_end_byte == ends[occurrence]) {
                semantics[occurrence] = resolved;
            }
        }
    }
    ASSERT_EQ(semantic_count, 2);
    ASSERT_EQ(zero_span_semantics, 0);
    ASSERT_NOT_NULL(semantics[0]);
    ASSERT_NOT_NULL(semantics[1]);
    ASSERT_STR_EQ(semantics[0]->callee_qn, semantics[1]->callee_qn);
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&r->resolved_calls, calls[0], false) ==
                semantics[0]);
    ASSERT_TRUE(cbm_pipeline_find_lsp_resolution(&r->resolved_calls, calls[1], false) ==
                semantics[1]);

    cbm_free_result(r);
    return 0;
}

TEST(perllsp_repeated_static_function_calls_join_by_exact_site) {
    static const char source[] = "package Helper;\n"
                                 "sub work { return 1; }\n"
                                 "package main;\n"
                                 "sub function_occurrence_probe {\n"
                                 "    Helper::work();\n"
                                 "    Helper::work();\n"
                                 "}\n";
    return assert_perl_repeated_exact_join(source, "main.function_occurrence_probe", "work",
                                           "Helper::work()");
}

TEST(perllsp_repeated_inherited_method_calls_join_by_exact_site) {
    static const char source[] = "package Base;\n"
                                 "sub greet { return 1; }\n"
                                 "package Child;\n"
                                 "our @ISA = ('Base');\n"
                                 "sub new { my $class = shift; return bless {}, $class; }\n"
                                 "package main;\n"
                                 "sub inherited_occurrence_probe {\n"
                                 "    my $child = Child->new();\n"
                                 "    $child->greet();\n"
                                 "    $child->greet();\n"
                                 "}\n";
    return assert_perl_repeated_exact_join(source, "main.inherited_occurrence_probe", "greet",
                                           "$child->greet()");
}

/* ── Invocant binding: signatures (5.36+) and classic list unpack ── */

TEST(perllsp_signature_self_dispatch) {
    /* `sub render ($self, $depth)` must bind $self to the enclosing package so
     * $self->draw() dispatches — signatures are stable since Perl 5.36 and the
     * dominant modern method form. */
    const char *src = "use feature 'signatures';\n"
                      "package Widget;\n"
                      "sub draw ($self, $d) { return $d; }\n"
                      "sub render ($self, $depth) {\n"
                      "    $self->draw($depth);\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.render", "main.draw") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_list_unpack_self_dispatch) {
    /* The dominant classic form `my ($self, $x) = @_;` must bind $self exactly
     * like `my $self = shift;` does. */
    const char *src = "package Widget;\n"
                      "sub draw { return 1; }\n"
                      "sub render {\n"
                      "    my ($self, $x) = @_;\n"
                      "    $self->draw($x);\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.render", "main.draw") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_plain_first_param_not_invocant) {
    /* Name gate: a first parameter NOT named $self/$class must stay untyped —
     * $cfg's package is unknown, so $cfg->go() must emit no edge (zero-edge
     * guarantee). */
    const char *src = "use feature 'signatures';\n"
                      "package Widget;\n"
                      "sub go { return 1; }\n"
                      "sub util ($cfg, $n) {\n"
                      "    $cfg->go($n);\n"
                      "}\n"
                      "sub grab {\n"
                      "    my ($cfg, $n) = @_;\n"
                      "    $cfg->go($n);\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "main.util", "main.go") < 0);
    ASSERT(find_resolved(r, "main.grab", "main.go") < 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_signature_class_dispatch) {
    /* $class as leading signature parameter binds to the package so
     * $class->method() (constructor-style) dispatches. */
    const char *src = "use feature 'signatures';\n"
                      "package Widget;\n"
                      "sub fresh { return bless {}, 'Widget'; }\n"
                      "sub make ($class, %args) {\n"
                      "    return $class->fresh();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.make", "main.fresh") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── Corinna OO (5.38 feature 'class') ─────────────────────────── */

TEST(perllsp_corinna_method_dispatch) {
    /* `class`/`method` with :isa inheritance: fetch's implicit $self must
     * dispatch speak through the :isa parent, exactly like @ISA. */
    const char *src = "use v5.38;\n"
                      "use experimental 'class';\n"
                      "class Animal {\n"
                      "    method speak { return 1 }\n"
                      "}\n"
                      "class Dog :isa(Animal) {\n"
                      "    method fetch { return $self->speak() }\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.fetch", "main.speak") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_corinna_constructor_dispatch) {
    /* Corinna's implicit constructor: Dog->new types the receiver Dog, and
     * $d->fetch dispatches into the class's method table. */
    const char *src = "use v5.38;\n"
                      "use experimental 'class';\n"
                      "class Dog {\n"
                      "    method fetch { return 1 }\n"
                      "}\n"
                      "package main;\n"
                      "sub run {\n"
                      "    my $d = Dog->new;\n"
                      "    $d->fetch;\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.fetch") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 5.38 stdlib expansion ─────────────────────────────────────── */

TEST(perllsp_stdlib_file_basename) {
    /* Exporter import of an expanded-table module sub must resolve to the
     * stdlib QN. */
    const char *src = "use File::Basename qw(basename);\n"
                      "sub f {\n"
                      "    return basename('/x/y');\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.f", "File.Basename.basename") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_stdlib_dbi_typed_chain) {
    /* Curated OO chain: DBI->connect types $dbh as DBI.db, whose prepare
     * types $sth as DBI.st, so execute resolves at the stdlib method. */
    const char *src = "use DBI;\n"
                      "sub q1 {\n"
                      "    my $dbh = DBI->connect('dsn');\n"
                      "    my $sth = $dbh->prepare('select 1');\n"
                      "    $sth->execute();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.q1", "DBI.db.prepare") >= 0);
    ASSERT(require_resolved(r, "main.q1", "DBI.st.execute") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── push @ISA inheritance (perl-push-isa) ─────────────────────── */

TEST(perllsp_push_isa_inheritance) {
    /* Classic pre-parent.pm subclassing: push @ISA, 'Base'; must record the
     * inheritance edge exactly like an @ISA assignment. */
    const char *src = "package Base;\n"
                      "sub speak { return 1; }\n"
                      "package Legacy;\n"
                      "push @ISA, 'Base';\n"
                      "sub run { my $self = shift; $self->speak(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.speak") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_unshift_qualified_isa_inheritance) {
    /* unshift + fully-qualified @Pkg::ISA spellings both count. */
    const char *src = "package Base;\n"
                      "sub speak { return 1; }\n"
                      "package Other;\n"
                      "sub noop { return 0; }\n"
                      "package main;\n"
                      "unshift @Other::ISA, 'Base';\n"
                      "sub run {\n"
                      "    my $o = bless {}, 'Other';\n"
                      "    $o->speak();\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.speak") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── qw() word splitting regression ────────────────────────────── */

TEST(perllsp_qw_multiword_import) {
    /* tree-sitter-perl exposes qw(a b) as ONE string_content "a b"; both
     * symbols must import (the old per-child assumption silently dropped
     * every multi-symbol list). */
    const char *src = "use Scalar::Util qw(blessed reftype);\n"
                      "sub f {\n"
                      "    my $x = blessed({});\n"
                      "    my $y = reftype({});\n"
                      "}\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.f", "Scalar.Util.blessed") >= 0);
    ASSERT(require_resolved(r, "main.f", "Scalar.Util.reftype") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── Moose/Moo DSL (perl-moose-attrs) ──────────────────────────── */

TEST(perllsp_moose_extends) {
    const char *src = "package Base;\n"
                      "sub greet { return 1; }\n"
                      "package Child;\n"
                      "use Moose;\n"
                      "extends 'Base';\n"
                      "sub run { my $self = shift; $self->greet(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.run", "main.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_moose_attr_chain) {
    /* has engine => (isa => 'Engine') types $self->engine as Engine so the
     * CHAINED ->start() dispatches; the accessor call itself emits nothing
     * (no indexed sub — zero-edge). */
    const char *src = "package Engine;\n"
                      "sub start { return 1; }\n"
                      "package Car;\n"
                      "use Moo;\n"
                      "has engine => (is => 'ro', isa => 'Engine');\n"
                      "sub go { my $self = shift; $self->engine->start(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.go", "main.start") >= 0);
    ASSERT(find_resolved(r, "main.go", "main.engine") < 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_moose_with_role) {
    /* `with 'Role'` composes the role's methods — flattened into the ISA
     * table (sound approximation for method lookup). */
    const char *src = "package Role::Fast;\n"
                      "sub dash { return 1; }\n"
                      "package Car;\n"
                      "use Moo;\n"
                      "with 'Role::Fast';\n"
                      "sub go { my $self = shift; $self->dash(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.go", "main.dash") >= 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_has_outside_moose_is_inert) {
    /* Per-package gate: `has` in a package that never imported a Moose-like
     * module is an ordinary (unresolvable) call — no attr, no typing, no
     * edges from the chain. */
    const char *src = "package Engine;\n"
                      "sub start { return 1; }\n"
                      "package Plain;\n"
                      "has engine => (is => 'ro', isa => 'Engine');\n"
                      "sub go { my $self = shift; $self->engine->start(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "main.go", "main.start") < 0);
    ASSERT(find_resolved(r, "main.go", "main.engine") < 0);
    cbm_free_result(r);
    PASS();
}

TEST(perllsp_moose_multi_attr_arrayref) {
    /* has ['a','b'] => (isa => 'Engine') declares BOTH attrs. */
    const char *src = "package Engine;\n"
                      "sub start { return 1; }\n"
                      "package Car;\n"
                      "use Moo;\n"
                      "has ['primary', 'backup'] => (is => 'ro', isa => 'Engine');\n"
                      "sub go { my $self = shift; $self->backup->start(); }\n";
    CBMFileResult *r = extract_perl(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "main.go", "main.start") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── Cross-file resolution (perl-cross-file-lsp) ───────────────── */

static int find_resolved_arr(const CBMResolvedCallArray *arr, const char *callerSub,
                             const char *calleeSub) {
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        if (rc->caller_qn && strstr(rc->caller_qn, callerSub) && rc->callee_qn &&
            strstr(rc->callee_qn, calleeSub))
            return i;
    }
    return -1;
}

static void dump_resolved_arr(const CBMResolvedCallArray *arr) {
    printf("  resolved (%d):\n", arr->count);
    for (int i = 0; i < arr->count; i++) {
        const CBMResolvedCall *rc = &arr->items[i];
        printf("    %s -> %s [%s]\n", rc->caller_qn ? rc->caller_qn : "(null)",
               rc->callee_qn ? rc->callee_qn : "(null)", rc->strategy ? rc->strategy : "(null)");
    }
}

TEST(perllsp_cross_imported_function) {
    /* Explicit caller-supplied symbol map (the pipeline shape when a member
     * import resolves): use + bare call lands on the cross-file def QN. */
    const char *source = "use My::Util qw(helper);\n"
                         "sub run { helper(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.run", .short_name = "run", .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.lib.My.Util.helper", .short_name = "helper",
         .label = "Function", .def_module_qn = "test.lib.My.Util"},
    };
    const char *imp_names[] = {"helper"};
    const char *imp_qns[] = {"test.lib.My.Util.helper"};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, imp_names,
                           imp_qns, 1, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "main.run", "lib.My.Util.helper");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_qw_ast_recollection) {
    /* NO caller import map at all: PASS 1 re-collects `use My::Util
     * qw(helper)` from the AST and resolves the module against the filtered
     * defs' module-QN tails (rel path ends My/Util.pm, lib/ root included). */
    const char *source = "use My::Util qw(helper);\n"
                         "sub run { helper(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.My.Util.helper", .short_name = "helper",
         .label = "Function", .def_module_qn = "test.lib.My.Util"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 1, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "main.run", "lib.My.Util.helper");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_package_method_dispatch) {
    /* Foo::Bar->new types the receiver; both the static-ish ->new and the
     * typed ->frob dispatch into the mapped module's method table. */
    const char *source = "use Foo::Bar;\n"
                         "sub go {\n"
                         "    my $o = Foo::Bar->new;\n"
                         "    $o->frob();\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Foo.Bar.new", .short_name = "new", .label = "Function",
         .def_module_qn = "test.lib.Foo.Bar"},
        {.qualified_name = "test.lib.Foo.Bar.frob", .short_name = "frob", .label = "Function",
         .def_module_qn = "test.lib.Foo.Bar"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx_new = find_resolved_arr(&out, "main.go", "lib.Foo.Bar.new");
    int idx_frob = find_resolved_arr(&out, "main.go", "lib.Foo.Bar.frob");
    if (idx_new < 0 || idx_frob < 0)
        dump_resolved_arr(&out);
    ASSERT(idx_new >= 0);
    ASSERT(idx_frob >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_mojo_base_inherited_method) {
    /* Cross-file inheritance: Dog inherits Animal via `use Mojo::Base 'Animal'`,
     * and Animal's speak() lives in ANOTHER module (test.lib.Animal). $self
     * (typed to the enclosing package Dog) must dispatch speak() up the ISA
     * chain to the parent's cross-file sub. Regression for the gap where the
     * ISA parent was registered as a bare type with no method table. */
    const char *source = "package Dog;\n"
                         "use Mojo::Base 'Animal';\n"
                         "sub bark {\n"
                         "    my $self = shift;\n"
                         "    return $self->speak;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Animal.speak", .short_name = "speak", .label = "Function",
         .def_module_qn = "test.lib.Animal"},
        {.qualified_name = "test.lib.Dog.bark", .short_name = "bark", .label = "Function",
         .def_module_qn = "test.lib.Dog"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.Dog", defs, 2, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "Dog.bark", "lib.Animal.speak");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_deep_qn_inherited_sub) {
    /* REAL-MOJOLICIOUS SHAPE (sub): deep path-based module QNs + a `::`-containing
     * parent (use Mojo::Base 'Mojo::Message'). Isolates whether the deep QN /
     * dotted-parent resolution regresses vs the flat-QN mojo_base test. */
    const char *source = "package Mojo::Message::Request;\n"
                         "use Mojo::Base 'Mojo::Message';\n"
                         "sub clone {\n"
                         "    my $self = shift;\n"
                         "    return $self->extract_start_line;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "proj.lib.Mojo.Message.extract_start_line",
         .short_name = "extract_start_line", .label = "Function",
         .def_module_qn = "proj.lib.Mojo.Message"},
        {.qualified_name = "proj.lib.Mojo.Message.Request.clone", .short_name = "clone",
         .label = "Function", .def_module_qn = "proj.lib.Mojo.Message.Request"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "proj.lib.Mojo.Message.Request",
                           defs, 2, NULL, NULL, 0, NULL, &out, NULL, defs, 2);
    int idx = find_resolved_arr(&out, "Request.clone", "Message.extract_start_line");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_deep_qn_inherited_accessor) {
    /* REAL-MOJOLICIOUS SHAPE (has-accessor): identical to the sub case but the
     * inherited target is a synthetic has-accessor def (label "Method", the
     * shape emitted by perl_scan_has_accessors). $self->content in a subclass
     * must dispatch up ISA to the parent module's accessor node. */
    const char *source = "package Mojo::Message::Request;\n"
                         "use Mojo::Base 'Mojo::Message';\n"
                         "sub clone {\n"
                         "    my $self = shift;\n"
                         "    return $self->content;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "proj.lib.Mojo.Message.content", .short_name = "content",
         .label = "Method", .def_module_qn = "proj.lib.Mojo.Message"},
        {.qualified_name = "proj.lib.Mojo.Message.Request.clone", .short_name = "clone",
         .label = "Function", .def_module_qn = "proj.lib.Mojo.Message.Request"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "proj.lib.Mojo.Message.Request",
                           defs, 2, NULL, NULL, 0, NULL, &out, NULL, defs, 2);
    int idx = find_resolved_arr(&out, "Request.clone", "Message.content");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_parent_only_in_all_defs) {
    /* SCALE REPRO: at scale the def filter (cbm_pxc_filter_defs_for_file) narrows
     * the per-file `defs` to own-module + import-map modules. When the parent
     * import row is MISSING (the folder/module QN collision — lib/Mojo/Message.pm
     * beside lib/Mojo/Message/ — drops the `use Mojo::Base 'Mojo::Message'` parent
     * import), the parent's defs land ONLY in all_defs, never in the filtered
     * `defs`. The multi-level chain-walk must still attach the parent's methods
     * from all_defs so $self->content resolves. Regression for the real-repo gap
     * where 274-file Mojolicious produced 0 perl_method_inherited while the same
     * two files in isolation produced 27. */
    const char *source = "package Mojo::Message::Request;\n"
                         "use Mojo::Base 'Mojo::Message';\n"
                         "sub clone {\n"
                         "    my $self = shift;\n"
                         "    return $self->content;\n"
                         "}\n";
    /* FILTERED defs: only this file's own def (parent filtered out — no import). */
    CBMLSPDef defs[] = {
        {.qualified_name = "proj.lib.Mojo.Message.Request.clone", .short_name = "clone",
         .label = "Function", .def_module_qn = "proj.lib.Mojo.Message.Request"},
    };
    /* FULL universe: includes the parent module's has-accessor. */
    CBMLSPDef all_defs[] = {
        {.qualified_name = "proj.lib.Mojo.Message.Request.clone", .short_name = "clone",
         .label = "Function", .def_module_qn = "proj.lib.Mojo.Message.Request"},
        {.qualified_name = "proj.lib.Mojo.Message.content", .short_name = "content",
         .label = "Method", .def_module_qn = "proj.lib.Mojo.Message"},
    };
    const char *req_parents[] = {"Mojo::Message", NULL};
    const char *idx_modules[] = {"proj.lib.Mojo.Message.Request"};
    const char *const *idx_lists[] = {req_parents};
    CBMPerlInheritIndex inherit = {
        .module_qns = idx_modules, .parent_lists = idx_lists, .count = 1};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "proj.lib.Mojo.Message.Request",
                           defs, 1, NULL, NULL, 0, NULL, &out, &inherit, all_defs, 2);
    int idx = find_resolved_arr(&out, "Request.clone", "Message.content");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_toplevel_module_caller) {
    /* Top-level statements (Mojolicious::Lite apps, .t scripts) attribute their
     * calls to the FILE MODULE (matching the unified extractor), so a typed
     * top-level call resolves instead of being dropped for a NULL caller. $t is
     * typed via Test::Mojo->new; $t->get_ok then binds with caller = the file
     * module. Regression for the 12k-site test-suite gap (0 edges before). */
    const char *source = "use Test::Mojo;\n"
                         "my $t = Test::Mojo->new;\n"
                         "$t->get_ok('/');\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Test.Mojo.new", .short_name = "new", .label = "Function",
         .def_module_qn = "test.lib.Test.Mojo", .return_types = "Test::Mojo"},
        {.qualified_name = "test.lib.Test.Mojo.get_ok", .short_name = "get_ok", .label = "Function",
         .def_module_qn = "test.lib.Test.Mojo"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.t.app", defs, 2, NULL, NULL, 0,
                           NULL, &out, NULL, defs, 2);
    int idx = find_resolved_arr(&out, "t.app", "Test.Mojo.get_ok");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_mojo_routing_c_param) {
    /* Mojolicious routing callback: `get '/x' => sub ($c) { $c->render }` — the
     * `$c` param is typed to Mojolicious::Controller (double-gated on the routing
     * DSL name AND the `$c` convention) and the class is seeded into the
     * chain-walk so render dispatches. Top-level attribution supplies the caller
     * QN (the file module) so the edge survives. */
    const char *source = "get '/x' => sub ($c) {\n"
                         "    $c->render(text => 'hi');\n"
                         "};\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Mojolicious.Controller.render", .short_name = "render",
         .label = "Function", .def_module_qn = "test.lib.Mojolicious.Controller"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.myapp", defs, 1, NULL, NULL, 0,
                           NULL, &out, NULL, defs, 1);
    int idx = find_resolved_arr(&out, "myapp", "Mojolicious.Controller.render");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_mojo_listunpack_c_param) {
    /* Mojolicious framework convention: inside a Mojolicious::* module, the 2nd
     * positional of `my ($self, $c) = @_` is the controller (dispatch/render/
     * route methods receive it), so `$c->render/stash/...` dispatches through
     * Mojolicious::Controller. Gated to the framework module path
     * (module_qn contains "Mojolicious"). */
    const char *source = "package Mojolicious::Foo;\n"
                         "use Mojo::Base -base;\n"
                         "sub bar {\n"
                         "    my ($self, $c) = @_;\n"
                         "    return $c->render;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Mojolicious.Controller.render", .short_name = "render",
         .label = "Function", .def_module_qn = "test.lib.Mojolicious.Controller"},
        {.qualified_name = "test.lib.Mojolicious.Foo.bar", .short_name = "bar", .label = "Function",
         .def_module_qn = "test.lib.Mojolicious.Foo"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.Mojolicious.Foo", defs, 2,
                           NULL, NULL, 0, NULL, &out, NULL, defs, 2);
    int idx = find_resolved_arr(&out, "Foo.bar", "Mojolicious.Controller.render");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_mojo_c_shift_param) {
    /* `my $c = shift` in a Mojolicious module is the controller — NOT the
     * enclosing package (here the plugin). Helper/hook callbacks
     * (`$app->helper(x => sub { my $c = shift; $c->render })`) are the dominant
     * `$c` form and were mis-typed to the plugin before this. */
    const char *source = "package Mojolicious::Plugin::Foo;\n"
                         "use Mojo::Base 'Mojolicious::Plugin';\n"
                         "sub helper_body {\n"
                         "    my $c = shift;\n"
                         "    return $c->render;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Mojolicious.Controller.render", .short_name = "render",
         .label = "Function", .def_module_qn = "test.lib.Mojolicious.Controller"},
        {.qualified_name = "test.lib.Mojolicious.Plugin.Foo.helper_body",
         .short_name = "helper_body", .label = "Function",
         .def_module_qn = "test.lib.Mojolicious.Plugin.Foo"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.Mojolicious.Plugin.Foo",
                           defs, 2, NULL, NULL, 0, NULL, &out, NULL, defs, 2);
    int idx = find_resolved_arr(&out, "Foo.helper_body", "Mojolicious.Controller.render");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_return_type_chain) {
    /* Return-type inference: extraction infers make_widget's return type (Widget)
     * from a `return Widget->new` body (perl_infer_return_types) and carries it on
     * CBMLSPDef.return_types; perl_register_lsp_func consumes it so `my $w =
     * $f->make_widget` types $w, and the chained `$w->name` resolves to the
     * cross-file Widget::name (which a bare/unknown return type would drop). */
    const char *source = "package App;\n"
                         "use Factory;\n"
                         "use Widget;\n"
                         "sub run {\n"
                         "    my $self = shift;\n"
                         "    my $f = Factory->new;\n"
                         "    my $w = $f->make_widget;\n"
                         "    $w->name;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Factory.new", .short_name = "new", .label = "Function",
         .def_module_qn = "test.lib.Factory", .return_types = "Factory"},
        {.qualified_name = "test.lib.Factory.make_widget", .short_name = "make_widget",
         .label = "Function", .def_module_qn = "test.lib.Factory", .return_types = "Widget"},
        {.qualified_name = "test.lib.Widget.name", .short_name = "name", .label = "Function",
         .def_module_qn = "test.lib.Widget"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.App", defs, 3, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "App.run", "lib.Widget.name");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_multilevel_inherited_method) {
    /* MULTI-LEVEL cross-file inheritance: Dog -> Animal -> Base, one class per
     * file. Dog->bark calls $self->speak (immediate parent Animal, one level)
     * AND $self->root_method (GRANDPARENT Base, two levels). The project-wide
     * inherit index supplies Animal's own parent (Base), which this file's pass1
     * cannot see, so the chain-walk must dispatch root_method to Base. */
    const char *source = "package Dog;\n"
                         "use Mojo::Base 'Animal';\n"
                         "sub bark {\n"
                         "    my $self = shift;\n"
                         "    my $a = $self->speak;\n"
                         "    my $b = $self->root_method;\n"
                         "    return \"$a $b\";\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Base.root_method", .short_name = "root_method",
         .label = "Function", .def_module_qn = "test.lib.Base"},
        {.qualified_name = "test.lib.Animal.speak", .short_name = "speak", .label = "Function",
         .def_module_qn = "test.lib.Animal"},
        {.qualified_name = "test.lib.Dog.bark", .short_name = "bark", .label = "Function",
         .def_module_qn = "test.lib.Dog"},
    };
    /* module_qn -> tagged parent spellings (as the pipeline assembles it). */
    const char *animal_parents[] = {"Base", NULL};
    const char *dog_parents[] = {"Animal", NULL};
    const char *idx_modules[] = {"test.lib.Animal", "test.lib.Dog"};
    const char *const *idx_lists[] = {animal_parents, dog_parents};
    CBMPerlInheritIndex inherit = {
        .module_qns = idx_modules, .parent_lists = idx_lists, .count = 2};

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.Dog", defs, 3, NULL, NULL,
                           0, NULL, &out, &inherit, defs, 3);
    int idx_speak = find_resolved_arr(&out, "Dog.bark", "lib.Animal.speak");
    int idx_root = find_resolved_arr(&out, "Dog.bark", "lib.Base.root_method");
    if (idx_speak < 0 || idx_root < 0)
        dump_resolved_arr(&out);
    ASSERT(idx_speak >= 0); /* one level (immediate parent) */
    ASSERT(idx_root >= 0);  /* two levels (grandparent via inherit index) */
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_require_package_dispatch) {
    /* require-based loading (even conditional) also feeds the package→module
     * map, so Foo::Bar->new dispatches without a use statement. */
    const char *source = "sub go {\n"
                         "    require Foo::Bar;\n"
                         "    my $o = Foo::Bar->new;\n"
                         "    $o->frob();\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Foo.Bar.new", .short_name = "new", .label = "Function",
         .def_module_qn = "test.lib.Foo.Bar"},
        {.qualified_name = "test.lib.Foo.Bar.frob", .short_name = "frob", .label = "Function",
         .def_module_qn = "test.lib.Foo.Bar"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "main.go", "lib.Foo.Bar.frob");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_default_exports) {
    /* perl-exports-model: `use My::Util;` with NO list imports the module's
     * @EXPORT defaults, carried on the EXPORT Variable def's return_types. */
    const char *source = "use My::Util;\n"
                         "sub run { helper(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.My.Util.helper", .short_name = "helper",
         .label = "Function", .def_module_qn = "test.lib.My.Util"},
        {.qualified_name = "test.lib.My.Util.EXPORT", .short_name = "EXPORT",
         .label = "Variable", .def_module_qn = "test.lib.My.Util", .return_types = "helper"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "main.run", "lib.My.Util.helper");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_export_ok_not_default) {
    /* @EXPORT_OK names are NOT imported by a bare `use Mod;` — only @EXPORT
     * is. Zero-edge negative. */
    const char *source = "use My::Util;\n"
                         "sub run { helper(); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.My.Util.helper", .short_name = "helper",
         .label = "Function", .def_module_qn = "test.lib.My.Util"},
        {.qualified_name = "test.lib.My.Util.EXPORT_OK", .short_name = "EXPORT_OK",
         .label = "Variable", .def_module_qn = "test.lib.My.Util", .return_types = "helper"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 2, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    ASSERT(find_resolved_arr(&out, "main.run", "helper") < 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(perllsp_cross_unresolvable_module_zero_edges) {
    /* A use of a module no def/import resolves must emit NOTHING. */
    const char *source = "use No::Such;\n"
                         "sub run {\n"
                         "    my $o = No::Such->new;\n"
                         "    $o->frob();\n"
                         "    missing();\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.My.Util.helper", .short_name = "helper",
         .label = "Function", .def_module_qn = "test.lib.My.Util"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 1, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    if (out.count != 0)
        dump_resolved_arr(&out);
    ASSERT(out.count == 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── multi-segment return-type chain (colon/dot reconciliation) ──── */

TEST(perllsp_cross_return_type_chain_multiseg) {
    /* make_widget returns a MULTI-segment class My::Widget. Its inferred return
     * type is stored DOTTED ("My.Widget", perl_infer_return_types), but the
     * cross-file used-module type table is keyed by the module name as written
     * in `use My::Widget` (colons). The typed-receiver lookup must reconcile the
     * two spellings so `$w->name` dispatches to My::Widget::name. A single-
     * segment class (perllsp_cross_return_type_chain, "Widget") is dot==colon and
     * cannot exercise this — every real multi-segment accessor chain (Mojo::*)
     * silently failed before the fix. */
    const char *source = "package App;\n"
                         "use Factory;\n"
                         "use My::Widget;\n"
                         "sub run {\n"
                         "    my $self = shift;\n"
                         "    my $f = Factory->new;\n"
                         "    my $w = $f->make_widget;\n"
                         "    $w->name;\n"
                         "}\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.Factory.new", .short_name = "new", .label = "Function",
         .def_module_qn = "test.lib.Factory", .return_types = "Factory"},
        {.qualified_name = "test.lib.Factory.make_widget", .short_name = "make_widget",
         .label = "Function", .def_module_qn = "test.lib.Factory", .return_types = "My.Widget"},
        {.qualified_name = "test.lib.My.Widget.name", .short_name = "name", .label = "Function",
         .def_module_qn = "test.lib.My.Widget"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.App", defs, 3, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "App.run", "lib.My.Widget.name");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── imported nullary function used as `func->method` (Mojo::File curfile) ── */

TEST(perllsp_cross_imported_func_arrow_method) {
    /* The Mojo::File idiom `curfile->sibling(...)`: curfile is an Exporter-
     * imported function (use Mojo::File qw(curfile)), so the lowercase bareword
     * `curfile` before `->` is a FUNCTION CALL, not a class name. It must resolve
     * to the curfile function (perl_imported_function) rather than be read as a
     * static method call on a package literally named "curfile". Because curfile's
     * return type is File, the chained `->sibling` also dispatches to
     * File::sibling — the receiver is typed from the function's return type. */
    const char *source = "use File qw(curfile);\n"
                         "sub run { curfile->sibling; }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.main.run", .short_name = "run", .label = "Function",
         .def_module_qn = "test.main"},
        {.qualified_name = "test.lib.File.curfile", .short_name = "curfile", .label = "Function",
         .def_module_qn = "test.lib.File", .return_types = "File"},
        {.qualified_name = "test.lib.File.sibling", .short_name = "sibling", .label = "Function",
         .def_module_qn = "test.lib.File"},
    };
    const char *imp_names[] = {"curfile"};
    const char *imp_qns[] = {"test.lib.File.curfile"};
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.main", defs, 3, imp_names,
                           imp_qns, 1, NULL, &out, NULL, NULL, 0);
    /* (1) the function-call edge to curfile itself. */
    int call_idx = find_resolved_arr(&out, "main.run", "lib.File.curfile");
    /* (2) the chained method edge, enabled by typing the receiver from curfile's
     * return type. */
    int chain_idx = find_resolved_arr(&out, "main.run", "lib.File.sibling");
    if (call_idx < 0 || chain_idx < 0)
        dump_resolved_arr(&out);
    ASSERT(call_idx >= 0);
    ASSERT(chain_idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* Same idiom, but the import map is NOT caller-supplied — it must be recovered
 * from the file's own `use Mojo::File qw(curfile)` via PASS-1 qw-collection +
 * the used-module map (the real indexing path; import_count = 0). Covers both a
 * top-level `my $x = curfile->...` (attributed to the module) and an in-sub
 * call. Regression for the real-repo finding that curfile->method emitted zero
 * edges. */
TEST(perllsp_cross_imported_func_arrow_method_passone) {
    const char *source = "package My::Mod;\n"
                         "use Mojo::File qw(curfile path);\n"
                         "my $TOP = curfile->sibling('resources');\n"
                         "sub f { my $y = curfile->sibling('b'); return $y; }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.My.Mod.f", .short_name = "f", .label = "Function",
         .def_module_qn = "test.lib.My.Mod"},
        {.qualified_name = "test.lib.Mojo.File.curfile", .short_name = "curfile",
         .label = "Function", .def_module_qn = "test.lib.Mojo.File", .return_types = "Mojo::File"},
        {.qualified_name = "test.lib.Mojo.File.sibling", .short_name = "sibling",
         .label = "Function", .def_module_qn = "test.lib.Mojo.File"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.My.Mod", defs, 3, NULL,
                           NULL, 0, NULL, &out, NULL, NULL, 0);
    /* top-level curfile call attributed to the module. */
    int top_idx = find_resolved_arr(&out, "My.Mod", "lib.Mojo.File.curfile");
    /* in-sub curfile call attributed to f. */
    int sub_idx = find_resolved_arr(&out, "Mod.f", "lib.Mojo.File.curfile");
    if (top_idx < 0 || sub_idx < 0)
        dump_resolved_arr(&out);
    ASSERT(sub_idx >= 0);
    ASSERT(top_idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* curfile's real return type is the literal __PACKAGE__ (Mojo::File's
 * `sub curfile { __PACKAGE__->new }`). The chained `curfile->sibling` must still
 * dispatch: __PACKAGE__ resolves to curfile's own package, reverse-mapped
 * through the used-module (xmod) table to the colon-spelled type key. */
TEST(perllsp_cross_imported_func_arrow_package_chain) {
    const char *source = "package App;\n"
                         "use Mojo::File qw(curfile);\n"
                         "sub run { curfile->sibling; }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.App.run", .short_name = "run", .label = "Function",
         .def_module_qn = "test.lib.App"},
        {.qualified_name = "test.lib.Mojo.File.curfile", .short_name = "curfile",
         .label = "Function", .def_module_qn = "test.lib.Mojo.File", .return_types = "__PACKAGE__"},
        {.qualified_name = "test.lib.Mojo.File.sibling", .short_name = "sibling",
         .label = "Function", .def_module_qn = "test.lib.Mojo.File"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.App", defs, 3, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int call_idx = find_resolved_arr(&out, "App.run", "lib.Mojo.File.curfile");
    int chain_idx = find_resolved_arr(&out, "App.run", "lib.Mojo.File.sibling");
    if (call_idx < 0 || chain_idx < 0)
        dump_resolved_arr(&out);
    ASSERT(call_idx >= 0);
    ASSERT(chain_idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* Same __PACKAGE__ factory, but called as a FUNCTION then chained:
 * `path('a')->child('b')` (Mojo::File's `sub path { __PACKAGE__->new(@_) }`).
 * The function-call receiver's return type (__PACKAGE__) must resolve to
 * Mojo::File so ->child dispatches — the func(...)->method twin of the bareword
 * curfile->method chain. */
TEST(perllsp_cross_imported_func_call_arrow_package_chain) {
    const char *source = "package App;\n"
                         "use Mojo::File qw(path);\n"
                         "sub run { path('a')->child('b'); }\n";
    CBMLSPDef defs[] = {
        {.qualified_name = "test.lib.App.run", .short_name = "run", .label = "Function",
         .def_module_qn = "test.lib.App"},
        {.qualified_name = "test.lib.Mojo.File.path", .short_name = "path", .label = "Function",
         .def_module_qn = "test.lib.Mojo.File", .return_types = "__PACKAGE__"},
        {.qualified_name = "test.lib.Mojo.File.child", .short_name = "child", .label = "Function",
         .def_module_qn = "test.lib.Mojo.File"},
    };
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out = {0};
    cbm_run_perl_lsp_cross(&arena, source, (int)strlen(source), "test.lib.App", defs, 3, NULL, NULL,
                           0, NULL, &out, NULL, NULL, 0);
    int idx = find_resolved_arr(&out, "App.run", "lib.Mojo.File.child");
    if (idx < 0)
        dump_resolved_arr(&out);
    ASSERT(idx >= 0);
    cbm_arena_destroy(&arena);
    PASS();
}

/* ── Suite registration ────────────────────────────────────────── */

SUITE(perl_lsp) {
    RUN_TEST(perllsp_method_via_bless_assignment);
    RUN_TEST(perllsp_constructor_class_method);
    RUN_TEST(perllsp_static_package_call);
    RUN_TEST(perllsp_static_multilevel_package_call);
    RUN_TEST(perllsp_self_method);
    RUN_TEST(perllsp_has_qw_arrayref_accessors);
    RUN_TEST(perllsp_isa_inheritance);
    RUN_TEST(perllsp_use_parent_inheritance);
    RUN_TEST(perllsp_use_base_inheritance);
    RUN_TEST(perllsp_use_mojo_base_inheritance);
    RUN_TEST(perllsp_exported_function);
    RUN_TEST(perllsp_cpan_exported_function);
    RUN_TEST(perllsp_require_fallback);
    RUN_TEST(perllsp_super_dispatch);
    RUN_TEST(perllsp_super_no_parent_no_edge);
    RUN_TEST(perllsp_unindexed_receiver_emits_block);
    RUN_TEST(perllsp_repeated_target_calls_join_by_exact_site);
    RUN_TEST(perllsp_repeated_static_function_calls_join_by_exact_site);
    RUN_TEST(perllsp_repeated_inherited_method_calls_join_by_exact_site);
    RUN_TEST(perllsp_signature_self_dispatch);
    RUN_TEST(perllsp_list_unpack_self_dispatch);
    RUN_TEST(perllsp_plain_first_param_not_invocant);
    RUN_TEST(perllsp_signature_class_dispatch);
    RUN_TEST(perllsp_corinna_method_dispatch);
    RUN_TEST(perllsp_corinna_constructor_dispatch);
    RUN_TEST(perllsp_stdlib_file_basename);
    RUN_TEST(perllsp_stdlib_dbi_typed_chain);
    RUN_TEST(perllsp_push_isa_inheritance);
    RUN_TEST(perllsp_unshift_qualified_isa_inheritance);
    RUN_TEST(perllsp_qw_multiword_import);
    RUN_TEST(perllsp_moose_extends);
    RUN_TEST(perllsp_moose_attr_chain);
    RUN_TEST(perllsp_moose_with_role);
    RUN_TEST(perllsp_has_outside_moose_is_inert);
    RUN_TEST(perllsp_moose_multi_attr_arrayref);
    RUN_TEST(perllsp_cross_imported_function);
    RUN_TEST(perllsp_cross_qw_ast_recollection);
    RUN_TEST(perllsp_cross_package_method_dispatch);
    RUN_TEST(perllsp_cross_mojo_base_inherited_method);
    RUN_TEST(perllsp_cross_deep_qn_inherited_sub);
    RUN_TEST(perllsp_cross_deep_qn_inherited_accessor);
    RUN_TEST(perllsp_cross_parent_only_in_all_defs);
    RUN_TEST(perllsp_cross_toplevel_module_caller);
    RUN_TEST(perllsp_cross_mojo_routing_c_param);
    RUN_TEST(perllsp_cross_mojo_listunpack_c_param);
    RUN_TEST(perllsp_cross_mojo_c_shift_param);
    RUN_TEST(perllsp_cross_return_type_chain);
    RUN_TEST(perllsp_cross_return_type_chain_multiseg);
    RUN_TEST(perllsp_cross_imported_func_arrow_method);
    RUN_TEST(perllsp_cross_imported_func_arrow_method_passone);
    RUN_TEST(perllsp_cross_imported_func_arrow_package_chain);
    RUN_TEST(perllsp_cross_imported_func_call_arrow_package_chain);
    RUN_TEST(perllsp_cross_multilevel_inherited_method);
    RUN_TEST(perllsp_cross_require_package_dispatch);
    RUN_TEST(perllsp_cross_default_exports);
    RUN_TEST(perllsp_cross_export_ok_not_default);
    RUN_TEST(perllsp_cross_unresolvable_module_zero_edges);
}
