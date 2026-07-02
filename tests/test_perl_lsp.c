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
#include "lsp/perl_lsp.h"
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

/* ── Suite registration ────────────────────────────────────────── */

SUITE(perl_lsp) {
    RUN_TEST(perllsp_method_via_bless_assignment);
    RUN_TEST(perllsp_constructor_class_method);
    RUN_TEST(perllsp_static_package_call);
    RUN_TEST(perllsp_static_multilevel_package_call);
    RUN_TEST(perllsp_self_method);
    RUN_TEST(perllsp_isa_inheritance);
    RUN_TEST(perllsp_use_parent_inheritance);
    RUN_TEST(perllsp_use_base_inheritance);
    RUN_TEST(perllsp_exported_function);
    RUN_TEST(perllsp_cpan_exported_function);
    RUN_TEST(perllsp_require_fallback);
    RUN_TEST(perllsp_super_dispatch);
    RUN_TEST(perllsp_super_no_parent_no_edge);
    RUN_TEST(perllsp_unindexed_receiver_emits_block);
}
