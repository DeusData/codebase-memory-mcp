/*
 * test_ruby_lsp.c — Tests for the Ruby Light Semantic Pass.
 *
 * Coverage mirrors tests/test_perl_lsp.c / tests/test_php_lsp.c, exercising
 * the foundational Ruby resolution scenarios:
 *   1.  QN contract (defs carry module_qn.ClassPath.method — the join key
 *       the resolver's emitted rows depend on)
 *   2.  Constructor typing + edge      (a = Animal.new; a.speak)
 *   3.  Constructor edge targets CLASS (textual `Widget.new` -> "Widget")
 *   4.  Implicit/explicit self dispatch
 *   5.  Singleton (class) methods      (def self.m; Animal.m)
 *   6.  Superclass chain lookup        (class B < A)
 *   7.  include + prepend mixin lookup (prepend shadows the class's own)
 *   8.  extend mixin (class-side) lookup
 *   9.  super dispatch
 *   10. Instance-variable typing       (@thing = Foo.new; @thing.bar)
 *   11. Chained call typing            (Foo.new.bar)
 *   12. Nested modules                 (lexical nesting + `A::B` references)
 *   12b. Compact `class A::B` names    (exact extractor spelling, scope)
 *   13. ActiveRecord model typing      (User.find(1).full_name)
 *   14. Top-level function calls
 *   15. Unresolvable receiver emits NO spurious edge (negative)
 *   15b. Scope gates                    (def, params, blocks, reassignment)
 *   16. send() / the whole dynamic-dispatch family emits NO edge (negative)
 *   16c. Blocks that rebind self emit NO self edge (negative)
 *   17. Cross-file: defs from another file resolve
 *   18. Cross-file: ActiveRecord repro mirroring the Rails-shaped e2e fixture
 *   19. Cross-file: a class reopened in a second file
 *   20. Cross-file: compact `class A::B` in another file
 *   21. Cross-file: one prebuilt registry serves many files unchanged
 *   Fail-closed ancestry (7c) and block-nested defs (7d) sit with mixins.
 *
 * Scope: rows 1-16 are SINGLE-FILE, driven through cbm_extract_file so they
 * exercise the per-file resolver exactly as the extraction pipeline calls it.
 * Rows 17-18 drive cbm_run_ruby_lsp_cross directly with a hand-built
 * CBMLSPDef[], which is the shape pass_lsp_cross.c hands it on the
 * cbm_pxc_run_one fallback path.
 *
 * The resolver populates result->resolved_calls with CBMResolvedCall rows.
 * The pipeline joins a row to its caller def by EXACT QN equality, so every
 * row that pins a class-path spelling (nested or compact) asserts the full
 * caller and callee QN with strcmp ("test"/"main.rb" -> module QN test.main).
 * Substring helpers remain for rows where only presence/absence matters.
 */
#include "test_framework.h"
#include "cbm.h"
#include "../src/pipeline/lsp_resolve.h"
#include "lsp/ruby_lsp.h"
#include "lsp/lsp_work.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Helpers (mirror test_perl_lsp.c) ──────────────────────────── */

static CBMFileResult *extract_ruby(const char *source) {
    return cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUBY, "test", "main.rb", 0, NULL,
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

/* Exact-name match on BOTH ends. The pipeline joins a resolved row to its
 * caller def by exact QN equality, so a row whose caller is spelled even
 * slightly differently from the extractor's def is silently dropped —
 * substring matching would hide exactly that failure. */
static int find_resolved_exact(const CBMFileResult *r, const char *caller_qn,
                               const char *callee_qn) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && rc->callee_qn && strcmp(rc->caller_qn, caller_qn) == 0 &&
            strcmp(rc->callee_qn, callee_qn) == 0)
            return i;
    }
    return -1;
}

static void dump_resolved(const CBMFileResult *r) {
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        printf("    %s -> %s [%s %.2f]\n", rc->caller_qn ? rc->caller_qn : "(null)",
               rc->callee_qn ? rc->callee_qn : "(null)", rc->strategy ? rc->strategy : "(null)",
               rc->confidence);
    }
}

static int require_resolved_exact(const CBMFileResult *r, const char *caller_qn,
                                  const char *callee_qn) {
    int idx = find_resolved_exact(r, caller_qn, callee_qn);
    if (idx < 0) {
        printf("  MISSING exact resolved call: %s -> %s (have %d)\n", caller_qn, callee_qn,
               r->resolved_calls.count);
        dump_resolved(r);
    }
    return idx;
}

/* Number of rows with exactly this caller and callee. */
static int count_resolved_exact(const CBMFileResult *r, const char *caller_qn,
                                const char *callee_qn) {
    int n = 0;
    for (int i = 0; i < r->resolved_calls.count; i++) {
        const CBMResolvedCall *rc = &r->resolved_calls.items[i];
        if (rc->caller_qn && rc->callee_qn && strcmp(rc->caller_qn, caller_qn) == 0 &&
            strcmp(rc->callee_qn, callee_qn) == 0)
            n++;
    }
    return n;
}

static const CBMDefinition *find_def(const CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && d->name && strcmp(d->label, label) == 0 && strcmp(d->name, name) == 0)
            return d;
    }
    return NULL;
}

/* ── 1. QN contract: defs weave the class path into method QNs ─── */

TEST(rubylsp_qn_contract) {
    const char *src = "class Animal\n"
                      "  def speak\n"
                      "    'woof'\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    const CBMDefinition *m = find_def(r, "Method", "speak");
    if (!m) {
        printf("  no Method speak def; defs:\n");
        for (int i = 0; i < r->defs.count; i++)
            printf("    [%s] %s qn=%s parent=%s\n", r->defs.items[i].label, r->defs.items[i].name,
                   r->defs.items[i].qualified_name,
                   r->defs.items[i].parent_class ? r->defs.items[i].parent_class : "(null)");
    }
    ASSERT(m);
    if (strcmp(m->qualified_name, "test.main.Animal.speak") != 0) {
        printf("  QN scheme mismatch: got %s (resolver assumes module_qn.ClassPath.method)\n",
               m->qualified_name);
    }
    ASSERT(strcmp(m->qualified_name, "test.main.Animal.speak") == 0);
    cbm_free_result(r);
    PASS();
}

/* ── 2. Constructor typing: a = Animal.new; a.speak ────────────── */

TEST(rubylsp_method_via_constructor_assignment) {
    const char *src = "class Animal\n"
                      "  def initialize(name)\n"
                      "    @name = name\n"
                      "  end\n"
                      "  def speak\n"
                      "    @name\n"
                      "  end\n"
                      "end\n"
                      "class Runner\n"
                      "  def run\n"
                      "    a = Animal.new('rex')\n"
                      "    a.speak\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "Runner.run", "Animal.speak") >= 0);
    ASSERT(find_resolved_with_strategy(r, "Runner.run", "Animal.speak", "ruby_method_typed"));
    cbm_free_result(r);
    PASS();
}

/* ── 3. Constructor edge targets the CLASS node ─────────────────── */

TEST(rubylsp_constructor_edge_targets_class) {
    const char *src = "class Widget\n"
                      "  def initialize\n"
                      "  end\n"
                      "end\n"
                      "class Maker\n"
                      "  def build\n"
                      "    Widget.new\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "Maker.build", "Widget", "ruby_constructor");
    ASSERT(rc);
    /* Callee is the class QN (leaf "Widget"), matching the textual
     * extractor's Widget.new -> "Widget" rewrite so the row joins. */
    ASSERT(strcmp(rc->callee_qn, "test.main.Widget") == 0);
    ASSERT(rc->confidence >= CBM_LSP_CONFIDENCE_FLOOR);
    cbm_free_result(r);
    PASS();
}

/* ── 4. Implicit + explicit self dispatch ───────────────────────── */

TEST(rubylsp_self_dispatch) {
    const char *src = "class Greeter\n"
                      "  def greet\n"
                      "    build_greeting('hi')\n"
                      "    self.build_greeting('yo')\n"
                      "  end\n"
                      "  def build_greeting(word)\n"
                      "    word\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "Greeter.greet", "Greeter.build_greeting") >= 0);
    ASSERT(find_resolved_with_strategy(r, "Greeter.greet", "Greeter.build_greeting",
                                       "ruby_self_dispatch"));
    cbm_free_result(r);
    PASS();
}

/* ── 5. Singleton (class) methods ───────────────────────────────── */

TEST(rubylsp_singleton_method_dispatch) {
    const char *src = "class Registry\n"
                      "  def self.register(key)\n"
                      "    key\n"
                      "  end\n"
                      "end\n"
                      "class App\n"
                      "  def boot\n"
                      "    Registry.register(:db)\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "App.boot", "Registry.register") >= 0);
    ASSERT(find_resolved_with_strategy(r, "App.boot", "Registry.register", "ruby_class_method"));
    cbm_free_result(r);
    PASS();
}

/* Singleton methods must NOT satisfy instance dispatch (and vice versa). */
TEST(rubylsp_singleton_instance_split) {
    const char *src = "class Config\n"
                      "  def self.load\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class App\n"
                      "  def boot\n"
                      "    c = Config.new\n"
                      "    c.load\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* c.load is an INSTANCE call; Config only defines a singleton `load`.
     * Zero-edge guarantee: no resolved row may bind them. */
    ASSERT(find_resolved(r, "App.boot", "Config.load") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 6. Superclass chain ────────────────────────────────────────── */

TEST(rubylsp_inheritance) {
    const char *src = "class Base\n"
                      "  def helper\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Child < Base\n"
                      "  def work\n"
                      "    helper\n"
                      "    self.helper\n"
                      "  end\n"
                      "end\n"
                      "class Driver\n"
                      "  def drive\n"
                      "    c = Child.new\n"
                      "    c.helper\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Inherited method resolves to Base.helper from both dispatch shapes. */
    ASSERT(require_resolved(r, "Child.work", "Base.helper") >= 0);
    ASSERT(require_resolved(r, "Driver.drive", "Base.helper") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7. include mixin ───────────────────────────────────────────── */

TEST(rubylsp_include_mixin) {
    const char *src = "module Greetable\n"
                      "  def greet\n"
                      "    'hello'\n"
                      "  end\n"
                      "end\n"
                      "class Person\n"
                      "  include Greetable\n"
                      "  def hail\n"
                      "    greet()\n"
                      "  end\n"
                      "end\n"
                      "class Caller\n"
                      "  def run\n"
                      "    p = Person.new\n"
                      "    p.greet\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "Person.hail", "Greetable.greet") >= 0);
    ASSERT(require_resolved(r, "Caller.run", "Greetable.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7b. prepend mixin shadows the class's own method ───────────── */

/* Ruby's lookup order puts prepended modules AHEAD of the class itself, which
 * is the whole point of `prepend` over `include`. Both an own method and a
 * prepended one exist here, so a resolver that walked the class first would
 * still emit an edge — just the wrong one. Pin the winner, not merely that
 * something resolved. */
TEST(rubylsp_prepend_mixin) {
    const char *src = "module Audited\n"
                      "  def save\n"
                      "    'audited'\n"
                      "  end\n"
                      "end\n"
                      "class Record\n"
                      "  prepend Audited\n"
                      "  def save\n"
                      "    'plain'\n"
                      "  end\n"
                      "end\n"
                      "class App\n"
                      "  def run\n"
                      "    r = Record.new\n"
                      "    r.save\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* The prepended module wins. */
    ASSERT(require_resolved(r, "App.run", "Audited.save") >= 0);
    /* And the shadowed own method must NOT also be emitted. */
    ASSERT(find_resolved(r, "App.run", "Record.save") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7c. Unresolved ancestry fails closed ───────────────────────── */

/* Three cases where an ancestor the resolver cannot see could define (or
 * shadow) the method being called. Each one has a tempting local answer —
 * the class's own method, a superclass method, a same-named top-level def —
 * and each answer may be wrong, so none may be emitted. */
TEST(rubylsp_unresolved_ancestry_fails_closed) {
    const char *src = /* (a) unresolved PREPEND blocks an own-method hit: the
                       * prepended module may shadow `save`. */
        "class Record\n"
        "  prepend UnseenAudit\n"
        "  def save\n"
        "    1\n"
        "  end\n"
        "  def persist\n"
        "    self.save\n"
        "  end\n"
        "end\n"
        "class RecordUser\n"
        "  def run\n"
        "    r = Record.new\n"
        "    r.save\n"
        "  end\n"
        "end\n"
        /* (b) unresolved INCLUDE stops the walk before the superclass chain:
         * the included module may define `helper` ahead of Base. */
        "class Base\n"
        "  def helper\n"
        "    1\n"
        "  end\n"
        "end\n"
        "class Child < Base\n"
        "  include UnseenConcern\n"
        "  def work\n"
        "    self.helper\n"
        "  end\n"
        "end\n"
        "class ChildUser\n"
        "  def run\n"
        "    c = Child.new\n"
        "    c.helper\n"
        "  end\n"
        "end\n"
        /* (c) a bare call in a class with an unresolved SUPERCLASS must not
         * fall through to a same-named top-level def. */
        "def util\n"
        "  1\n"
        "end\n"
        "class Orphan < UnseenBase\n"
        "  def go\n"
        "    util()\n"
        "  end\n"
        "  def again\n"
        "    super\n"
        "  end\n"
        "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* (a) */
    ASSERT(find_resolved(r, "Record.persist", "save") < 0);
    ASSERT(find_resolved(r, "RecordUser.run", "save") < 0);
    /* (b) */
    ASSERT(find_resolved(r, "Child.work", "helper") < 0);
    ASSERT(find_resolved(r, "ChildUser.run", "helper") < 0);
    /* (c) */
    ASSERT(find_resolved(r, "Orphan.go", "util") < 0);
    ASSERT(find_resolved(r, "Orphan.again", "again") < 0);
    cbm_free_result(r);
    PASS();
}

/* Positive controls for the rule above: resolved ancestry still reaches the
 * superclass and still falls through to a top-level def, and a hit found
 * BEFORE the unseen ancestor in Ruby's lookup order is still emitted. */
TEST(rubylsp_resolved_ancestry_still_resolves) {
    const char *src = "module Seen\n"
                      "  def greet\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "def util\n"
                      "  1\n"
                      "end\n"
                      "class Base\n"
                      "  def helper\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Child < Base\n"
                      "  include Seen\n"
                      "  def work\n"
                      "    self.helper\n"
                      "    util()\n"
                      "  end\n"
                      "end\n"
                      "class Mixed < UnseenBase\n"
                      "  include UnseenConcern\n"
                      "  include Seen\n"
                      "  def own\n"
                      "    2\n"
                      "  end\n"
                      "  def work\n"
                      "    self.own\n"
                      "    self.greet\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved_exact(r, "test.main.Child.work", "test.main.Base.helper") >= 0);
    ASSERT(require_resolved_exact(r, "test.main.Child.work", "test.main.util") >= 0);
    /* Own method precedes includes and the superclass in lookup order. */
    ASSERT(require_resolved_exact(r, "test.main.Mixed.work", "test.main.Mixed.own") >= 0);
    /* `include Seen` comes AFTER `include UnseenConcern`, so Seen is searched
     * first and its hit is trustworthy. */
    ASSERT(require_resolved_exact(r, "test.main.Mixed.work", "test.main.Seen.greet") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 7d. Methods defined inside blocks are not members of the module ── */

/* ActiveSupport's `class_methods do … end` / `included do … end` define
 * methods on whatever class includes the concern (class-side, for
 * class_methods), never instance methods of the concern module itself. A
 * resolver that registered them on the module would then find them through
 * `include Concern` and emit `Foo.new.build` -> Concern.build — an edge to a
 * def the extractor does not even emit. Same for `include X` written INSIDE
 * such a block: it applies to the includer, not the concern. */
TEST(rubylsp_block_defs_not_module_members) {
    const char *src = "module Other\n"
                      "  def other_m\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "module Concern\n"
                      "  class_methods do\n"
                      "    def build\n"
                      "      1\n"
                      "    end\n"
                      "  end\n"
                      "  included do\n"
                      "    include Other\n"
                      "    def injected\n"
                      "      2\n"
                      "    end\n"
                      "  end\n"
                      "  def real\n"
                      "    3\n"
                      "  end\n"
                      "end\n"
                      "class Foo\n"
                      "  include Concern\n"
                      "  def initialize\n"
                      "  end\n"
                      "end\n"
                      "class User\n"
                      "  def run\n"
                      "    f = Foo.new\n"
                      "    f.build\n"
                      "    f.injected\n"
                      "    f.other_m\n"
                      "    f.real\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "User.run", "build") < 0);
    ASSERT(find_resolved(r, "User.run", "injected") < 0);
    ASSERT(find_resolved(r, "User.run", "other_m") < 0);
    /* Positive control: a def directly in the module body IS a member. */
    ASSERT(require_resolved_exact(r, "test.main.User.run", "test.main.Concern.real") >= 0);
    cbm_free_result(r);
    PASS();
}

/* A def inside a top-level block (an RSpec group) has whatever `self` the
 * DSL gave it: bare calls in its body emit nothing, and it is not a
 * file-level function other top-level code can reach. */
TEST(rubylsp_top_level_block_defs_fail_closed) {
    const char *src = "def util\n"
                      "  1\n"
                      "end\n"
                      "describe 'x' do\n"
                      "  def helper\n"
                      "    util()\n"
                      "  end\n"
                      "end\n"
                      "def caller_fn\n"
                      "  helper()\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "helper", "util") < 0);
    ASSERT(find_resolved(r, "caller_fn", "helper") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 8. extend mixin (class-side) ───────────────────────────────── */

TEST(rubylsp_extend_mixin) {
    const char *src = "module Findable\n"
                      "  def locate(id)\n"
                      "    id\n"
                      "  end\n"
                      "end\n"
                      "class Record\n"
                      "  extend Findable\n"
                      "end\n"
                      "class App\n"
                      "  def run\n"
                      "    Record.locate(7)\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "App.run", "Findable.locate") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 9. super dispatch ──────────────────────────────────────────── */

TEST(rubylsp_super_dispatch) {
    const char *src = "class Base\n"
                      "  def setup\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Child < Base\n"
                      "  def setup\n"
                      "    super\n"
                      "    2\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "Child.setup", "Base.setup", "ruby_method_super");
    if (!rc)
        (void)require_resolved(r, "Child.setup", "Base.setup");
    ASSERT(rc);
    cbm_free_result(r);
    PASS();
}

/* ── 10. Instance-variable typing ───────────────────────────────── */

TEST(rubylsp_ivar_typing) {
    const char *src = "class Engine\n"
                      "  def start\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Car\n"
                      "  def initialize\n"
                      "    @engine = Engine.new\n"
                      "  end\n"
                      "  def drive\n"
                      "    @engine.start\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "Car.drive", "Engine.start") >= 0);
    ASSERT(find_resolved_with_strategy(r, "Car.drive", "Engine.start", "ruby_ivar_method"));
    cbm_free_result(r);
    PASS();
}

/* Conflicting ivar assignments must suppress the type (zero-edge). */
TEST(rubylsp_ivar_conflict_no_edge) {
    const char *src = "class A\n"
                      "  def go\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class B\n"
                      "  def go\n"
                      "    2\n"
                      "  end\n"
                      "end\n"
                      "class Holder\n"
                      "  def initialize(flag)\n"
                      "    @x = A.new\n"
                      "    @x = B.new\n"
                      "  end\n"
                      "  def run\n"
                      "    @x.go\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "Holder.run", ".go") < 0);
    cbm_free_result(r);
    PASS();
}

/* Any write to @x that is not a resolved constructor (or nil) must latch the
 * conflict, the same way two different constructors do — a method result, a
 * parameter, an unresolved constant's `.new`, a compound operator, or a
 * multiple assignment. Otherwise `@x = Foo.new` in one method silently types
 * every other method's `@x` even after it was reassigned to who-knows-what. */
TEST(rubylsp_ivar_non_constructor_write_latches) {
    const char *src = "class Engine\n"
                      "  def start\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class ViaMethod\n"
                      "  def initialize\n"
                      "    @e = Engine.new\n"
                      "  end\n"
                      "  def reset\n"
                      "    @e = compute\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n"
                      "class ViaParam\n"
                      "  def initialize(e)\n"
                      "    @e = Engine.new\n"
                      "    @e = e\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n"
                      "class ViaUnknownCtor\n"
                      "  def initialize\n"
                      "    @e = Engine.new\n"
                      "    @e = Mystery.new\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n"
                      "class ViaCompound\n"
                      "  def initialize\n"
                      "    @e = Engine.new\n"
                      "    @e += 1\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n"
                      "class ViaMulti\n"
                      "  def initialize(a)\n"
                      "    @e = Engine.new\n"
                      "    @e, @f = a\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n"
                      "class ViaOrUntyped\n"
                      "  def initialize\n"
                      "    @e = Engine.new\n"
                      "  end\n"
                      "  def lazy\n"
                      "    @e ||= build\n"
                      "  end\n"
                      "  def go\n"
                      "    @e.start\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "ViaMethod.go", "Engine.start") < 0);
    ASSERT(find_resolved(r, "ViaParam.go", "Engine.start") < 0);
    ASSERT(find_resolved(r, "ViaUnknownCtor.go", "Engine.start") < 0);
    ASSERT(find_resolved(r, "ViaCompound.go", "Engine.start") < 0);
    ASSERT(find_resolved(r, "ViaMulti.go", "Engine.start") < 0);
    ASSERT(find_resolved(r, "ViaOrUntyped.go", "Engine.start") < 0);
    cbm_free_result(r);
    PASS();
}

/* `@x ||= Const.new` is the memoised-constructor idiom and counts as a
 * constructor assignment; a `nil` reset says nothing about the type. */
TEST(rubylsp_ivar_or_assign_constructor_types) {
    const char *src = "class Engine\n"
                      "  def start\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Car\n"
                      "  def engine\n"
                      "    @engine ||= Engine.new\n"
                      "  end\n"
                      "  def clear\n"
                      "    @engine = nil\n"
                      "  end\n"
                      "  def drive\n"
                      "    @engine.start\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved_exact(r, "test.main.Car.drive", "test.main.Engine.start") >= 0);
    ASSERT(find_resolved_with_strategy(r, "Car.drive", "Engine.start", "ruby_ivar_method"));
    cbm_free_result(r);
    PASS();
}

/* ── 11. Chained calls ──────────────────────────────────────────── */

TEST(rubylsp_chained_constructor_call) {
    const char *src = "class Builder\n"
                      "  def finish\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class App\n"
                      "  def run\n"
                      "    Builder.new.finish\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved(r, "App.run", "Builder.finish") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 12. Nested modules + lexical nesting ───────────────────────── */

TEST(rubylsp_nested_modules) {
    const char *src = "module Admin\n"
                      "  class User\n"
                      "    def self.lookup(id)\n"
                      "      id\n"
                      "    end\n"
                      "    def name\n"
                      "      'n'\n"
                      "    end\n"
                      "  end\n"
                      "  class Panel\n"
                      "    def show\n"
                      "      User.lookup(1)\n"
                      "      u = User.new\n"
                      "      u.name\n"
                      "    end\n"
                      "  end\n"
                      "end\n"
                      "class Outside\n"
                      "  def probe\n"
                      "    Admin::User.lookup(2)\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Exact spellings: lexically nested declarations join with '.', matching
     * the extractor's `<enclosing_qn>.<name>` — a substring match would let
     * a wrongly-spelled row pass. */
    /* Lexical reference from sibling class inside the module. */
    ASSERT(require_resolved_exact(r, "test.main.Admin.Panel.show", "test.main.Admin.User.lookup") >=
           0);
    ASSERT(require_resolved_exact(r, "test.main.Admin.Panel.show", "test.main.Admin.User") >= 0);
    ASSERT(require_resolved_exact(r, "test.main.Admin.Panel.show", "test.main.Admin.User.name") >=
           0);
    /* Fully-qualified reference from outside. */
    ASSERT(require_resolved_exact(r, "test.main.Outside.probe", "test.main.Admin.User.lookup") >=
           0);
    cbm_free_result(r);
    PASS();
}

/* ── 12b. Compact `class A::B` names match the extractor byte-for-byte ── */

/* The structural extractor names a compact declaration with its RAW name text
 * (`Admin::User`), and the pipeline joins a resolved row to its caller def by
 * exact QN equality. A resolver that normalised the name to `Admin.User` would
 * have every row from or to such a class dropped at the join — silently, since
 * substring assertions would still pass. Pin the exact spelling on both ends,
 * and pin that the extractor's def really is spelled that way. */
TEST(rubylsp_compact_class_name_exact) {
    const char *src = "class Admin::User\n"
                      "  def initialize\n"
                      "  end\n"
                      "  def name\n"
                      "    'n'\n"
                      "  end\n"
                      "  def shout\n"
                      "    self.name\n"
                      "  end\n"
                      "end\n"
                      "class Outside\n"
                      "  def probe\n"
                      "    u = Admin::User.new\n"
                      "    u.name\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* The extractor's contract this resolver must reproduce. */
    const CBMDefinition *m = find_def(r, "Method", "name");
    ASSERT(m);
    ASSERT(strcmp(m->qualified_name, "test.main.Admin::User.name") == 0);
    ASSERT(m->parent_class && strcmp(m->parent_class, "test.main.Admin::User") == 0);
    /* Caller inside the compact class, callee inside it. */
    ASSERT(require_resolved_exact(r, "test.main.Admin::User.shout", "test.main.Admin::User.name") >=
           0);
    /* Caller outside: constructor row lands on the class QN, typed call on
     * the method QN — both in the extractor's spelling. */
    ASSERT(require_resolved_exact(r, "test.main.Outside.probe", "test.main.Admin::User") >= 0);
    ASSERT(require_resolved_exact(r, "test.main.Outside.probe", "test.main.Admin::User.name") >= 0);
    /* And the dotted spelling must NOT appear anywhere. */
    ASSERT(find_resolved(r, "Outside.probe", "Admin.User") < 0);
    cbm_free_result(r);
    PASS();
}

/* Compact declaration nested inside a lexical module: the extractor joins the
 * enclosing class QN with the raw name, giving `Admin.Foo::Bar`. */
TEST(rubylsp_compact_class_inside_module_exact) {
    const char *src = "module Admin\n"
                      "  class Foo::Bar\n"
                      "    def m\n"
                      "      1\n"
                      "    end\n"
                      "  end\n"
                      "  class Panel\n"
                      "    def show\n"
                      "      Foo::Bar.new.m\n"
                      "    end\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    const CBMDefinition *m = find_def(r, "Method", "m");
    ASSERT(m);
    ASSERT(strcmp(m->qualified_name, "test.main.Admin.Foo::Bar.m") == 0);
    ASSERT(require_resolved_exact(r, "test.main.Admin.Panel.show", "test.main.Admin.Foo::Bar") >=
           0);
    ASSERT(require_resolved_exact(r, "test.main.Admin.Panel.show", "test.main.Admin.Foo::Bar.m") >=
           0);
    cbm_free_result(r);
    PASS();
}

/* Inside `class Admin::User`, Ruby's lexical nesting is [Admin::User] only —
 * `Admin` is not opened, so a bare `Helper` does NOT see Admin::Helper. A
 * resolver that probed the dotted prefixes (Admin.User, Admin, "") would
 * invent that constant and emit a wrong constructor edge. The explicit
 * `Admin::Helper` still resolves. */
TEST(rubylsp_compact_class_does_not_open_outer_scope) {
    const char *src = "module Admin\n"
                      "  class Helper\n"
                      "    def initialize\n"
                      "    end\n"
                      "    def assist\n"
                      "      1\n"
                      "    end\n"
                      "  end\n"
                      "end\n"
                      "class Admin::User\n"
                      "  def bare\n"
                      "    h = Helper.new\n"
                      "    h.assist\n"
                      "  end\n"
                      "  def explicit\n"
                      "    h = Admin::Helper.new\n"
                      "    h.assist\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Bare `Helper` inside the compact class: no edge at all. */
    ASSERT(find_resolved(r, "Admin::User.bare", "Helper") < 0);
    /* Fully qualified reference resolves, in exact extractor spelling. */
    ASSERT(require_resolved_exact(r, "test.main.Admin::User.explicit", "test.main.Admin.Helper") >=
           0);
    ASSERT(require_resolved_exact(r, "test.main.Admin::User.explicit",
                                  "test.main.Admin.Helper.assist") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 13. ActiveRecord model typing ──────────────────────────────── */

TEST(rubylsp_activerecord_model_typing) {
    const char *src = "class User < ApplicationRecord\n"
                      "  def full_name\n"
                      "    'x'\n"
                      "  end\n"
                      "end\n"
                      "class UsersController < ApplicationController\n"
                      "  def show\n"
                      "    u = User.find(1)\n"
                      "    u.full_name\n"
                      "  end\n"
                      "  def index\n"
                      "    User.where(active: true).first.full_name\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* User.find returns User (AR query typing); u.full_name resolves. */
    ASSERT(require_resolved(r, "UsersController.show", "User.full_name") >= 0);
    /* Relation approximation keeps the model type through where/first. */
    ASSERT(require_resolved(r, "UsersController.index", "User.full_name") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 14. Top-level functions ────────────────────────────────────── */

TEST(rubylsp_top_level_function) {
    const char *src = "def helper(x)\n"
                      "  x\n"
                      "end\n"
                      "def run\n"
                      "  helper(1)\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    const CBMResolvedCall *rc =
        find_resolved_with_strategy(r, "main.run", "main.helper", "ruby_function_local");
    if (!rc)
        (void)require_resolved(r, "main.run", "main.helper");
    ASSERT(rc);
    cbm_free_result(r);
    PASS();
}

/* ── 15. Negative: unresolvable receiver emits no edge ──────────── */

TEST(rubylsp_unknown_receiver_no_edge) {
    const char *src = "class Safe\n"
                      "  def process(payload)\n"
                      "    payload.transform\n"
                      "    mystery = fetch_thing\n"
                      "    mystery.explode\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Neither the untyped parameter nor the unresolved local may produce
     * a resolved row. */
    ASSERT(find_resolved(r, "Safe.process", "transform") < 0);
    ASSERT(find_resolved(r, "Safe.process", "explode") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 15b. `def` is a scope gate ─────────────────────────────────── */

/* Ruby locals do not cross a `def`: a typed local in the enclosing body (or
 * a sibling method) is invisible inside, and a parameter of the same name is
 * a brand-new, untyped binding. A resolver that chained the method scope to
 * its parent would type `a` from the outside and emit a wrong edge. */
TEST(rubylsp_def_is_scope_gate) {
    const char *src = "class Foo\n"
                      "  def bar\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "a = Foo.new\n"
                      "def top\n"
                      "  a.bar\n"
                      "end\n"
                      "class Worker\n"
                      "  def run\n"
                      "    a = Foo.new\n"
                      "    a.bar\n"
                      "    helper(a)\n"
                      "  end\n"
                      "  def helper(a)\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def later\n"
                      "    a.bar\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Positive control: the typed local inside its own method resolves. */
    ASSERT(require_resolved_exact(r, "test.main.Worker.run", "test.main.Foo.bar") >= 0);
    /* Top-level `a` is not visible inside `def top`. */
    ASSERT(find_resolved(r, "main.top", "Foo.bar") < 0);
    /* Parameter `a` is untyped, whatever the caller passed. */
    ASSERT(find_resolved(r, "Worker.helper", "Foo.bar") < 0);
    /* Sibling method's local is not visible either. */
    ASSERT(find_resolved(r, "Worker.later", "Foo.bar") < 0);
    cbm_free_result(r);
    PASS();
}

/* Block parameters shadow the enclosing local for the block's extent only:
 * `|a|` inside the block is untyped, and `a` after the block is still Foo. */
TEST(rubylsp_block_param_shadows_local) {
    const char *src = "class Foo\n"
                      "  def bar\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Worker\n"
                      "  def run\n"
                      "    a = Foo.new\n"
                      "    [1].each { |a| a.bar }\n"
                      "    [1].each do |x, (y, z), *rest; loc|\n"
                      "      x.bar\n"
                      "      y.bar\n"
                      "      rest.bar\n"
                      "      loc.bar\n"
                      "    end\n"
                      "    f = ->(a) { a.bar }\n"
                      "    a.bar\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    /* Exactly ONE row: the `a.bar` after the blocks. Every `.bar` inside a
     * block is on an untyped block parameter. */
    if (count_resolved_exact(r, "test.main.Worker.run", "test.main.Foo.bar") != 1)
        dump_resolved(r);
    ASSERT(count_resolved_exact(r, "test.main.Worker.run", "test.main.Foo.bar") == 1);
    cbm_free_result(r);
    PASS();
}

/* Reassigning a typed local to anything the resolver cannot type must drop
 * the old type. `a = Foo.new; a = x; a.bar` is NOT a call on Foo. Covers
 * plain reassignment, multiple assignment, `||=` and other compound forms. */
TEST(rubylsp_untyped_reassignment_drops_type) {
    const char *src = "class Foo\n"
                      "  def bar\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Worker\n"
                      "  def plain(x)\n"
                      "    a = Foo.new\n"
                      "    a = x\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def multi(x)\n"
                      "    a = Foo.new\n"
                      "    a, b = x, 1\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def nested_multi(x)\n"
                      "    a = Foo.new\n"
                      "    (a, b), c = x, 1\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def splat_multi(x)\n"
                      "    a = Foo.new\n"
                      "    b, *a = x\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def or_assign(x)\n"
                      "    a = Foo.new\n"
                      "    a ||= x\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def compound(x)\n"
                      "    a = Foo.new\n"
                      "    a += x\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def in_block(x)\n"
                      "    a = Foo.new\n"
                      "    [1].each { a = x }\n"
                      "    a.bar\n"
                      "  end\n"
                      "  def fresh_or_assign\n"
                      "    a ||= Foo.new\n"
                      "    a.bar\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "Worker.plain", "Foo.bar") < 0);
    ASSERT(find_resolved(r, "Worker.multi", "Foo.bar") < 0);
    ASSERT(find_resolved(r, "Worker.nested_multi", "Foo.bar") < 0);
    ASSERT(find_resolved(r, "Worker.splat_multi", "Foo.bar") < 0);
    ASSERT(find_resolved(r, "Worker.or_assign", "Foo.bar") < 0);
    ASSERT(find_resolved(r, "Worker.compound", "Foo.bar") < 0);
    /* Assignment inside a block rebinds the enclosing local (closure). */
    ASSERT(find_resolved(r, "Worker.in_block", "Foo.bar") < 0);
    /* Positive control: `||=` on a fresh name with a typed RHS is a
     * constructor assignment. */
    ASSERT(require_resolved_exact(r, "test.main.Worker.fresh_or_assign", "test.main.Foo.bar") >= 0);
    cbm_free_result(r);
    PASS();
}

/* ── 16. Negative: dynamic dispatch emits no edge ───────────────── */

TEST(rubylsp_send_no_edge) {
    const char *src = "class Target\n"
                      "  def hidden\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Meta\n"
                      "  def invoke\n"
                      "    t = Target.new\n"
                      "    t.send(:hidden)\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "Meta.invoke", "hidden") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 16b. Negative: the whole dynamic-dispatch family emits no edge ── */

/* `send` has its own row above. The zero-edge guarantee is advertised for the
 * whole reflective family, so pin the rest of it too: a resolver that treated
 * any of these as an ordinary call would invent an edge to `hidden`.
 *
 * `Meta` deliberately defines its OWN `hidden`: inside `instance_eval { }`
 * self is the Target, not the Meta, so a resolver that dispatched the bare
 * `hidden()` on the enclosing class would emit Meta.via_instance_eval ->
 * Meta.hidden — a wrong edge that an empty Meta would never reveal. The
 * bare calls are written with `()` because tree-sitter-ruby parses a lone
 * `hidden` as an identifier, which the extractor never emits as a call. */
TEST(rubylsp_dynamic_dispatch_family_no_edge) {
    const char *src = "class Target\n"
                      "  def hidden\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Meta\n"
                      "  def hidden\n"
                      "    0\n"
                      "  end\n"
                      "  def via_public_send\n"
                      "    Target.new.public_send(:hidden)\n"
                      "  end\n"
                      "  def via_underscore_send\n"
                      "    Target.new.__send__(:hidden)\n"
                      "  end\n"
                      "  def via_define_method\n"
                      "    Target.define_method(:hidden) { 2 }\n"
                      "  end\n"
                      "  def via_instance_eval\n"
                      "    Target.new.instance_eval { hidden() }\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "Meta.via_public_send", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_underscore_send", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_define_method", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_instance_eval", "hidden") < 0);
    cbm_free_result(r);
    PASS();
}

/* ── 16c. Negative: blocks that rebind `self` ───────────────────── */

/* instance_eval/exec, class_eval/exec, module_eval/exec, define_method and
 * Class.new/Module.new run their block with a different `self`. Bare calls,
 * `self.` calls and `super` inside them must not dispatch on the enclosing
 * class. Every enclosing class here HAS the method being called, so the old
 * behaviour would emit a wrong edge in each row. Receiver and argument
 * expressions of the same call are ordinary and still resolve. */
TEST(rubylsp_self_rebinding_blocks_no_edge) {
    const char *src = "class Target\n"
                      "  def hidden\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Meta\n"
                      "  def hidden\n"
                      "    0\n"
                      "  end\n"
                      "  def target\n"
                      "    Target.new\n"
                      "  end\n"
                      "  def via_instance_exec\n"
                      "    target().instance_exec(1) { |x| hidden(); self.hidden }\n"
                      "  end\n"
                      "  def via_class_eval\n"
                      "    Target.class_eval do\n"
                      "      hidden()\n"
                      "    end\n"
                      "  end\n"
                      "  def via_class_exec\n"
                      "    Target.class_exec { hidden() }\n"
                      "  end\n"
                      "  def via_module_eval\n"
                      "    Target.module_eval { self.hidden }\n"
                      "  end\n"
                      "  def via_module_exec\n"
                      "    Target.module_exec { hidden() }\n"
                      "  end\n"
                      "  def via_define_method_body\n"
                      "    Target.define_method(:other) { hidden() }\n"
                      "  end\n"
                      "  def via_class_new\n"
                      "    Class.new do\n"
                      "      hidden()\n"
                      "      def hidden\n"
                      "        super\n"
                      "      end\n"
                      "    end\n"
                      "  end\n"
                      "  def via_module_new\n"
                      "    Module.new { self.hidden }\n"
                      "  end\n"
                      "  def after_block\n"
                      "    Target.new.instance_eval { hidden() }\n"
                      "    self.hidden\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(find_resolved(r, "Meta.via_instance_exec", "hidden") < 0);
    /* ...but the receiver expression of that same call still resolves. */
    ASSERT(require_resolved_exact(r, "test.main.Meta.via_instance_exec", "test.main.Meta.target") >=
           0);
    ASSERT(find_resolved(r, "Meta.via_class_eval", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_class_exec", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_module_eval", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_module_exec", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_define_method_body", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_class_new", "hidden") < 0);
    ASSERT(find_resolved(r, "Meta.via_module_new", "hidden") < 0);
    /* The suppression ends with the block: the following statement is an
     * ordinary self dispatch on Meta. */
    ASSERT(count_resolved_exact(r, "test.main.Meta.after_block", "test.main.Meta.hidden") == 1);
    cbm_free_result(r);
    PASS();
}

/* ── 17. Bounded resolver work ────────────────────────────────────
 *
 * Measured with counters, never with a clock (O9): CBM_LSP_WORK
 * (lsp_work.h, seam builds) for lookup work, and the result arena's
 * cumulative allocation for bytes built. Each fixture is measured at n and
 * 2n: linear work doubles (ratio about 2); anything worse lands above the
 * gate of 3. */

static uint64_t rubylsp_counted_work(const char *src, size_t *arena_bytes) {
    (void)cbm_lsp_work_take();
    CBMFileResult *r = extract_ruby(src);
    uint64_t work = cbm_lsp_work_take();
    if (arena_bytes)
        *arena_bytes = r ? cbm_arena_total(&r->arena) : 0;
    if (r)
        cbm_free_result(r);
    return work;
}

/* n modules where every module both prepends and includes the next one;
 * the last defines one method. `Probe` reaches M0 through `include`
 * (instance side) or `extend` (class side) and calls a method nobody
 * defines, so the lookup must miss after searching every ancestor. */
static char *rubylsp_mixin_chain_src(int n, bool class_side) {
    size_t cap = (size_t)n * 96 + 256;
    char *src = (char *)malloc(cap);
    if (!src)
        return NULL;
    size_t off = 0;
    for (int i = 0; i < n; i++) {
        if (i + 1 < n)
            off += (size_t)snprintf(src + off, cap - off,
                                    "module M%d\n  prepend M%d\n  include M%d\nend\n", i, i + 1,
                                    i + 1);
        else
            off += (size_t)snprintf(src + off, cap - off,
                                    "module M%d\n  def leaf\n    1\n  end\nend\n", i);
    }
    if (class_side)
        (void)snprintf(src + off, cap - off,
                       "class Probe\n  extend M0\n  def self.run\n    nobody_defines_this()\n"
                       "  end\nend\n");
    else
        (void)snprintf(src + off, cap - off,
                       "class Probe\n  include M0\n  def run\n    nobody_defines_this()\n"
                       "  end\nend\n");
    return src;
}

static uint64_t rubylsp_mixin_chain_work(int n, bool class_side) {
    char *src = rubylsp_mixin_chain_src(n, class_side);
    if (!src)
        return 0;
    uint64_t work = rubylsp_counted_work(src, NULL);
    free(src);
    return work;
}

/* Review ask (a): Ruby linearizes ancestors, so a module reached through
 * two paths (`prepend M; include M`, a diamond) is searched once. Without a
 * visited set every level of the chain doubles the paths: 2^n visits
 * (measured 4 -> 20,744 for n = 2..14). With it a miss costs one visit per
 * module. Positive controls: the doubled pair still resolves through the
 * prepend, and a diamond resolves to the shared module exactly once. */
TEST(rubylsp_mixin_walk_visits_each_module_once) {
    uint64_t inst_n = rubylsp_mixin_chain_work(6, false);
    uint64_t inst_2n = rubylsp_mixin_chain_work(12, false);
    uint64_t cls_n = rubylsp_mixin_chain_work(6, true);
    uint64_t cls_2n = rubylsp_mixin_chain_work(12, true);
    printf("  mixin chain work: instance n=6 %llu, n=12 %llu; class side n=6 %llu, n=12 %llu\n",
           (unsigned long long)inst_n, (unsigned long long)inst_2n, (unsigned long long)cls_n,
           (unsigned long long)cls_2n);
    ASSERT(inst_n > 0 && cls_n > 0);
    ASSERT(inst_2n <= 3 * inst_n);
    ASSERT(cls_2n <= 3 * cls_n);

    const char *src = "module Shared\n"
                      "  def hello\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "module Left\n"
                      "  include Shared\n"
                      "end\n"
                      "module Right\n"
                      "  include Shared\n"
                      "end\n"
                      "module Twice\n"
                      "  def twice\n"
                      "    2\n"
                      "  end\n"
                      "end\n"
                      "class Diamond\n"
                      "  include Left\n"
                      "  include Right\n"
                      "  prepend Twice\n"
                      "  include Twice\n"
                      "  def run\n"
                      "    hello()\n"
                      "    twice()\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(count_resolved_exact(r, "test.main.Diamond.run", "test.main.Shared.hello") == 1);
    ASSERT(count_resolved_exact(r, "test.main.Diamond.run", "test.main.Twice.twice") == 1);
    cbm_free_result(r);
    PASS();
}

/* `class Probe; def run; S0::S1::...::S<n-1>.new; end; end` */
static char *rubylsp_deep_const_src(int n) {
    size_t cap = (size_t)n * 12 + 128;
    char *src = (char *)malloc(cap);
    if (!src)
        return NULL;
    size_t off = (size_t)snprintf(src, cap, "class Probe\n  def run\n    ");
    for (int i = 0; i < n; i++)
        off += (size_t)snprintf(src + off, cap - off, i ? "::S%d" : "S%d", i);
    (void)snprintf(src + off, cap - off, ".new\n  end\nend\n");
    return src;
}

static size_t rubylsp_deep_const_bytes(int n) {
    char *src = rubylsp_deep_const_src(n);
    if (!src)
        return 0;
    size_t bytes = 0;
    (void)rubylsp_counted_work(src, &bytes);
    free(src);
    return bytes;
}

/* Review ask (b): a constant path is built in one pass. Recursing once per
 * `::` and re-formatting the growing prefix at every level costs one stack
 * frame per segment and quadratic bytes; the loop has no recursion and
 * writes the path once. The positive control pins the exact class QN for a
 * deep path and for a leading `::`. */
TEST(rubylsp_const_path_is_linear) {
    size_t b_n = rubylsp_deep_const_bytes(800);
    size_t b_2n = rubylsp_deep_const_bytes(1600);
    printf("  const path arena bytes: n=800 %zu, n=1600 %zu\n", b_n, b_2n);
    ASSERT(b_n > 0);
    ASSERT(b_2n <= 3 * b_n);

    const char *src = "module L0\n"
                      "  module L1\n"
                      "    module L2\n"
                      "      module L3\n"
                      "        module L4\n"
                      "          class Leaf\n"
                      "          end\n"
                      "        end\n"
                      "      end\n"
                      "    end\n"
                      "  end\n"
                      "end\n"
                      "class Top\n"
                      "  class Inner\n"
                      "  end\n"
                      "end\n"
                      "class Caller\n"
                      "  def deep\n"
                      "    L0::L1::L2::L3::L4::Leaf.new\n"
                      "  end\n"
                      "  def rooted\n"
                      "    ::Top::Inner.new\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r);
    ASSERT(require_resolved_exact(r, "test.main.Caller.deep", "test.main.L0.L1.L2.L3.L4.Leaf") >=
           0);
    ASSERT(require_resolved_exact(r, "test.main.Caller.rooted", "test.main.Top.Inner") >= 0);
    cbm_free_result(r);
    PASS();
}

/* Review ask (c): the stdlib seed must survive allocation failure. A
 * zero-initialised arena has no blocks and no grow size, so every
 * allocation from it returns NULL. The seed must not write through a NULL
 * return-type vector, and must not register an entry whose formatted name
 * came back NULL (a NULL QN would reach strcmp in finalize and in every later
 * lookup). */
TEST(rubylsp_stdlib_seed_survives_alloc_failure) {
    CBMArena reg_arena;
    cbm_arena_init(&reg_arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &reg_arena);
    CBMArena dead;
    memset(&dead, 0, sizeof(dead));
    cbm_ruby_stdlib_register(&reg, &dead);
    ASSERT(reg.type_count > 0);
    ASSERT(reg.func_count > 0); /* Kernel built-ins carry literal names */
    for (int i = 0; i < reg.func_count; i++) {
        ASSERT(reg.funcs[i].qualified_name != NULL);
        ASSERT(reg.funcs[i].short_name != NULL);
    }
    cbm_registry_finalize(&reg);
    ASSERT(cbm_registry_lookup_func(&reg, "puts") != NULL);
    cbm_arena_destroy(&reg_arena);
    PASS();
}

/* ── 17. Cross-file: defs from another file resolve ─────────────── */

TEST(rubylsp_cross_file_defs) {
    /* models/user.rb (other file) provides the class; this file calls it.
     * Simulates the fallback cbm_pxc_run_one path with a combined def
     * list. */
    CBMLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "test.models.user.User";
    defs[0].short_name = "User";
    defs[0].label = "Class";
    defs[0].def_module_qn = "test.models.user";
    defs[0].embedded_types = "ApplicationRecord";
    defs[0].lang = CBM_LANG_RUBY;
    defs[1].qualified_name = "test.models.user.User.full_name";
    defs[1].short_name = "full_name";
    defs[1].label = "Method";
    defs[1].receiver_type = "test.models.user.User";
    defs[1].def_module_qn = "test.models.user";
    defs[1].lang = CBM_LANG_RUBY;
    defs[2].qualified_name = "test.models.user.User.admin?";
    defs[2].short_name = "admin?";
    defs[2].label = "Method";
    defs[2].receiver_type = "test.models.user.User";
    defs[2].def_module_qn = "test.models.user";
    defs[2].lang = CBM_LANG_RUBY;

    const char *src = "class UsersController\n"
                      "  def show\n"
                      "    u = User.new\n"
                      "    u.full_name\n"
                      "    f = User.find(3)\n"
                      "    f.admin?\n"
                      "  end\n"
                      "end\n";

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.controller", defs, 3, NULL,
                           NULL, 0, NULL, &out);

    bool ctor = false;
    bool full_name = false;
    bool admin = false;
    for (int i = 0; i < out.count; i++) {
        if (!out.items[i].callee_qn || !out.items[i].caller_qn)
            continue;
        if (!strstr(out.items[i].caller_qn, "UsersController.show"))
            continue;
        if (strcmp(out.items[i].callee_qn, "test.models.user.User") == 0)
            ctor = true;
        if (strcmp(out.items[i].callee_qn, "test.models.user.User.full_name") == 0)
            full_name = true;
        if (strcmp(out.items[i].callee_qn, "test.models.user.User.admin?") == 0)
            admin = true;
    }
    if (!(ctor && full_name && admin)) {
        printf("  cross-file rows (%d):\n", out.count);
        for (int i = 0; i < out.count; i++)
            printf("    %s -> %s [%s]\n", out.items[i].caller_qn, out.items[i].callee_qn,
                   out.items[i].strategy);
    }
    cbm_arena_destroy(&arena);
    ASSERT(ctor);
    ASSERT(full_name);
    ASSERT(admin);
    PASS();
}

/* ── 18. Cross-file: AR repro mirroring the e2e fixture exactly ── */

TEST(rubylsp_cross_file_ar_repro) {
    CBMLSPDef defs[6];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "e2e.app.models.user.User";
    defs[0].short_name = "User";
    defs[0].label = "Class";
    defs[0].def_module_qn = "e2e.app.models.user";
    defs[0].embedded_types = "ApplicationRecord";
    defs[0].lang = CBM_LANG_RUBY;
    defs[1].qualified_name = "e2e.app.models.user.User.full_name";
    defs[1].short_name = "full_name";
    defs[1].label = "Method";
    defs[1].receiver_type = "e2e.app.models.user.User";
    defs[1].def_module_qn = "e2e.app.models.user";
    defs[1].lang = CBM_LANG_RUBY;
    defs[2].qualified_name = "e2e.app.models.user.User.deactivate!";
    defs[2].short_name = "deactivate!";
    defs[2].label = "Method";
    defs[2].receiver_type = "e2e.app.models.user.User";
    defs[2].def_module_qn = "e2e.app.models.user";
    defs[2].lang = CBM_LANG_RUBY;
    defs[3].qualified_name = "e2e.app.models.application_record.ApplicationRecord";
    defs[3].short_name = "ApplicationRecord";
    defs[3].label = "Class";
    defs[3].def_module_qn = "e2e.app.models.application_record";
    defs[3].embedded_types = "ActiveRecord::Base";
    defs[3].lang = CBM_LANG_RUBY;
    defs[4].qualified_name = "e2e.app.services.greeter.Greeter";
    defs[4].short_name = "Greeter";
    defs[4].label = "Class";
    defs[4].def_module_qn = "e2e.app.services.greeter";
    defs[4].lang = CBM_LANG_RUBY;
    defs[5].qualified_name = "e2e.app.services.greeter.Greeter.greet";
    defs[5].short_name = "greet";
    defs[5].label = "Method";
    defs[5].receiver_type = "e2e.app.services.greeter.Greeter";
    defs[5].def_module_qn = "e2e.app.services.greeter";
    defs[5].lang = CBM_LANG_RUBY;

    const char *src = "class Report\n"
                      "  def generate(id)\n"
                      "    user = User.find(id)\n"
                      "    name = user.full_name\n"
                      "    greeter = Greeter.new(user)\n"
                      "    greeter.greet\n"
                      "    user.deactivate!\n"
                      "    name\n"
                      "  end\n"
                      "end\n";

    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "e2e.app.services.report", defs, 6, NULL,
                           NULL, 0, NULL, &out);

    bool full_name = false;
    bool deact = false;
    bool greet = false;
    for (int i = 0; i < out.count; i++) {
        if (!out.items[i].callee_qn)
            continue;
        if (strcmp(out.items[i].callee_qn, "e2e.app.models.user.User.full_name") == 0)
            full_name = true;
        if (strcmp(out.items[i].callee_qn, "e2e.app.models.user.User.deactivate!") == 0)
            deact = true;
        if (strcmp(out.items[i].callee_qn, "e2e.app.services.greeter.Greeter.greet") == 0)
            greet = true;
    }
    if (!(full_name && deact && greet)) {
        printf("  AR-repro rows (%d):\n", out.count);
        for (int i = 0; i < out.count; i++)
            printf("    %s -> %s [%s]\n", out.items[i].caller_qn, out.items[i].callee_qn,
                   out.items[i].strategy);
    }
    cbm_arena_destroy(&arena);
    ASSERT(greet);
    ASSERT(full_name);
    ASSERT(deact);
    PASS();
}

/* ── 19-21. Cross-file: the shared project registry ─────────────── */

static void cross_def(CBMLSPDef *d, const char *qn, const char *short_name, const char *label,
                      const char *module_qn, const char *receiver, const char *embedded) {
    memset(d, 0, sizeof(*d));
    d->qualified_name = qn;
    d->short_name = short_name;
    d->label = label;
    d->def_module_qn = module_qn;
    d->receiver_type = receiver;
    d->embedded_types = embedded;
    d->lang = CBM_LANG_RUBY;
}

/* Caller and callee both match exactly, as find_resolved_exact does for the
 * single-file rows: a substring match would let a row pass on the wrong caller. */
static bool out_has(const CBMResolvedCallArray *out, const char *caller, const char *callee) {
    for (int i = 0; i < out->count; i++) {
        if (out->items[i].caller_qn && out->items[i].callee_qn &&
            strcmp(out->items[i].caller_qn, caller) == 0 &&
            strcmp(out->items[i].callee_qn, callee) == 0)
            return true;
    }
    return false;
}

static void out_dump(const CBMResolvedCallArray *out) {
    for (int i = 0; i < out->count; i++)
        printf("    %s -> %s [%s]\n", out->items[i].caller_qn, out->items[i].callee_qn,
               out->items[i].strategy);
}

TEST(rubylsp_cross_reopened_class_across_files) {
    /* `class User` is reopened in a second file. Each file's class has its
     * own QN (it carries the file's module QN) but one constant path, so a
     * method defined in either reopening must be found, and the constructor
     * edge lands on the first declaration. */
    CBMLSPDef defs[4];
    cross_def(&defs[0], "test.a.User", "User", "Class", "test.a", NULL, NULL);
    cross_def(&defs[1], "test.a.User.name", "name", "Method", "test.a", "test.a.User", NULL);
    cross_def(&defs[2], "test.b.User", "User", "Class", "test.b", NULL, NULL);
    cross_def(&defs[3], "test.b.User.email", "email", "Method", "test.b", "test.b.User", NULL);

    const char *src = "class Report\n"
                      "  def run\n"
                      "    u = User.new\n"
                      "    u.name\n"
                      "    u.email\n"
                      "  end\n"
                      "end\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.report", defs, 4, NULL, NULL, 0,
                           NULL, &out);
    bool ctor = out_has(&out, "test.app.report.Report.run", "test.a.User");
    bool name = out_has(&out, "test.app.report.Report.run", "test.a.User.name");
    bool email = out_has(&out, "test.app.report.Report.run", "test.b.User.email");
    if (!(ctor && name && email))
        out_dump(&out);
    cbm_arena_destroy(&arena);
    ASSERT(ctor);
    ASSERT(name);
    ASSERT(email);
    PASS();
}

TEST(rubylsp_cross_compact_class_name) {
    /* A compact `class Admin::User` in another file has QN
     * "<module>.Admin::User" but is found through the dotted path
     * "Admin.User". */
    CBMLSPDef defs[2];
    cross_def(&defs[0], "test.admin.Admin::User", "User", "Class", "test.admin", NULL, NULL);
    cross_def(&defs[1], "test.admin.Admin::User.greet", "greet", "Method", "test.admin",
              "test.admin.Admin::User", NULL);

    const char *src = "class Welcome\n"
                      "  def run\n"
                      "    Admin::User.new.greet\n"
                      "  end\n"
                      "end\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.welcome", defs, 2, NULL, NULL,
                           0, NULL, &out);
    bool ctor = out_has(&out, "test.app.welcome.Welcome.run", "test.admin.Admin::User");
    bool greet = out_has(&out, "test.app.welcome.Welcome.run", "test.admin.Admin::User.greet");
    if (!(ctor && greet))
        out_dump(&out);
    cbm_arena_destroy(&arena);
    ASSERT(ctor);
    ASSERT(greet);
    PASS();
}

TEST(rubylsp_cross_registry_built_once_serves_many_files) {
    /* One registry, several files, superclass chain across files: the shared
     * registry is read-only, so later files see exactly what earlier ones did
     * and nothing a file declares leaks into it. */
    CBMLSPDef defs[4];
    cross_def(&defs[0], "test.base.Base", "Base", "Class", "test.base", NULL, NULL);
    cross_def(&defs[1], "test.base.Base.shared_m", "shared_m", "Method", "test.base",
              "test.base.Base", NULL);
    cross_def(&defs[2], "test.child.Child", "Child", "Class", "test.child", NULL, "Base");
    cross_def(&defs[3], "test.child.Child.own_m", "own_m", "Method", "test.child",
              "test.child.Child", NULL);

    CBMArena build;
    cbm_arena_init(&build);
    CBMRubyCrossRegistry *sh = cbm_ruby_build_cross_registry(&build, defs, 4);
    ASSERT(sh != NULL);
    int classes_before = sh->class_count;
    int funcs_before = sh->reg.func_count;

    const char *src1 =
        "class One\n  def run\n    c = Child.new\n    c.shared_m\n    c.own_m\n  end\nend\n";
    const char *src2 = "class Two\n  def run\n    Child.new.shared_m\n  end\nend\n";
    const char *src3 = "class Three\n  def run\n    Local.new.shared_m\n  end\nend\n";
    CBMArena a1;
    CBMArena a2;
    CBMArena a3;
    cbm_arena_init(&a1);
    cbm_arena_init(&a2);
    cbm_arena_init(&a3);
    CBMResolvedCallArray o1;
    CBMResolvedCallArray o2;
    CBMResolvedCallArray o3;
    memset(&o1, 0, sizeof(o1));
    memset(&o2, 0, sizeof(o2));
    memset(&o3, 0, sizeof(o3));
    cbm_run_ruby_lsp_cross_with_registry(&a1, src1, (int)strlen(src1), "test.app.one", sh, NULL,
                                         NULL, 0, NULL, &o1);
    cbm_run_ruby_lsp_cross_with_registry(&a2, src2, (int)strlen(src2), "test.app.two", sh, NULL,
                                         NULL, 0, NULL, &o2);
    cbm_run_ruby_lsp_cross_with_registry(&a3, src3, (int)strlen(src3), "test.app.three", sh, NULL,
                                         NULL, 0, NULL, &o3);
    bool one_inherited = out_has(&o1, "test.app.one.One.run", "test.base.Base.shared_m");
    bool one_own = out_has(&o1, "test.app.one.One.run", "test.child.Child.own_m");
    bool two_inherited = out_has(&o2, "test.app.two.Two.run", "test.base.Base.shared_m");
    bool three_none = !out_has(&o3, "test.app.three.Three.run", "test.base.Base.shared_m");
    bool untouched = sh->class_count == classes_before && sh->reg.func_count == funcs_before;
    if (!(one_inherited && one_own && two_inherited && three_none && untouched)) {
        out_dump(&o1);
        out_dump(&o2);
        out_dump(&o3);
    }
    cbm_arena_destroy(&a1);
    cbm_arena_destroy(&a2);
    cbm_arena_destroy(&a3);
    cbm_arena_destroy(&build);
    ASSERT(one_inherited);
    ASSERT(one_own);
    ASSERT(two_inherited);
    ASSERT(three_none);
    ASSERT(untouched);
    PASS();
}

/* A class reopened in `reopenings` files (`Widget`, one module each, the first
 * declaring `< CxBase`), plus the base. Work is counted for the registry build
 * and for resolving one fixed file whose call sites all miss on the head's own
 * methods (`label` is inherited from CxBase). */
static bool rubylsp_reopen_work(int reopenings, uint64_t *build_work, uint64_t *resolve_work) {
    int n = reopenings * 2 + 2;
    CBMLSPDef *defs = (CBMLSPDef *)calloc((size_t)n, sizeof(CBMLSPDef));
    char (*names)[64] = (char (*)[64])calloc((size_t)n * 3, sizeof(*names));
    if (!defs || !names) {
        free(defs);
        free(names);
        return false;
    }
    int d = 0;
    int nm = 0;
    cross_def(&defs[d++], "test.base.CxBase", "CxBase", "Class", "test.base", NULL, NULL);
    cross_def(&defs[d++], "test.base.CxBase.label", "label", "Method", "test.base",
              "test.base.CxBase", NULL);
    for (int i = 0; i < reopenings; i++) {
        char *mod = names[nm++];
        char *cls = names[nm++];
        char *meth = names[nm++];
        snprintf(mod, 64, "test.m%d", i);
        snprintf(cls, 64, "test.m%d.CxWidget", i);
        snprintf(meth, 64, "test.m%d.CxWidget.own%d", i, i);
        cross_def(&defs[d++], cls, "CxWidget", "Class", mod, NULL, i == 0 ? "CxBase" : NULL);
        cross_def(&defs[d++], meth, "own", "Method", mod, cls, NULL);
    }
    CBMArena build;
    cbm_arena_init(&build);
    (void)cbm_lsp_work_take();
    CBMRubyCrossRegistry *sh = cbm_ruby_build_cross_registry(&build, defs, d);
    *build_work = cbm_lsp_work_take();

    const char *src = "class CxCaller\n  def run\n    w = CxWidget.new\n    w.label\n"
                      "    w.label\n    w.label\n    w.label\n    w.label\n    w.label\n"
                      "    w.label\n    w.label\n  end\nend\n";
    CBMArena a;
    cbm_arena_init(&a);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    (void)cbm_lsp_work_take();
    if (sh)
        cbm_run_ruby_lsp_cross_with_registry(&a, src, (int)strlen(src), "test.app.caller", sh, NULL,
                                             NULL, 0, NULL, &out);
    *resolve_work = cbm_lsp_work_take();
    bool resolved = out_has(&out, "test.app.caller.CxCaller.run", "test.base.CxBase.label");
    cbm_arena_destroy(&a);
    cbm_arena_destroy(&build);
    free(defs);
    free(names);
    return sh && resolved;
}

/* Review items 2b / 2c / 3. Constants are global, so a namespace module or a
 * model is reopened in many files. Doubling the reopenings must at most double
 * the registry build (a per-reopening tail walk makes it R^2/2, ratio 4), and
 * must not change what resolving one fixed file costs at all: a lookup is one
 * probe however many files reopen the class (a group walk per miss makes it
 * grow with R). The resolve gate is therefore 1.5, not the build gate of 3. */
TEST(rubylsp_cross_reopened_class_work_is_bounded) {
    uint64_t build_r = 0;
    uint64_t resolve_r = 0;
    uint64_t build_2r = 0;
    uint64_t resolve_2r = 0;
    ASSERT(rubylsp_reopen_work(64, &build_r, &resolve_r));
    ASSERT(rubylsp_reopen_work(128, &build_2r, &resolve_2r));
    printf("  reopened-class work: build R=64 %llu, 2R %llu; resolve R=64 %llu, 2R %llu\n",
           (unsigned long long)build_r, (unsigned long long)build_2r, (unsigned long long)resolve_r,
           (unsigned long long)resolve_2r);
    ASSERT(build_2r < 3 * (build_r ? build_r : 1));
    ASSERT(2 * resolve_2r < 3 * (resolve_r ? resolve_r : 1));
    PASS();
}

/* Review item 4: the merged record must not depend on def order. A full build
 * orders defs by file index; the incremental closure route appends fresh defs
 * to the persisted ones, so the same project can arrive in any order. The head
 * is the smallest QN, the superclass comes from the smallest-QN declaration
 * that has one, and a method defined by two reopenings resolves to the smaller
 * QN. Built forwards and reversed, the resolved edges must be identical. */
static void rubylsp_order_defs(CBMLSPDef *defs, bool reversed) {
    CBMLSPDef t[10];
    cross_def(&t[0], "test.m2.CxWidget", "CxWidget", "Class", "test.m2", NULL, "CxOther");
    cross_def(&t[1], "test.m2.CxWidget.dup_m", "dup_m", "Method", "test.m2", "test.m2.CxWidget",
              NULL);
    cross_def(&t[2], "test.m1.CxWidget", "CxWidget", "Class", "test.m1", NULL, "CxBase");
    cross_def(&t[3], "test.m1.CxWidget.dup_m", "dup_m", "Method", "test.m1", "test.m1.CxWidget",
              NULL);
    cross_def(&t[4], "test.m0.CxWidget", "CxWidget", "Class", "test.m0", NULL, NULL);
    cross_def(&t[5], "test.m0.CxWidget.own", "own", "Method", "test.m0", "test.m0.CxWidget", NULL);
    cross_def(&t[6], "test.base.CxBase", "CxBase", "Class", "test.base", NULL, NULL);
    cross_def(&t[7], "test.base.CxBase.label", "label", "Method", "test.base", "test.base.CxBase",
              NULL);
    cross_def(&t[8], "test.base.CxOther", "CxOther", "Class", "test.base", NULL, NULL);
    cross_def(&t[9], "test.base.CxOther.label", "label", "Method", "test.base", "test.base.CxOther",
              NULL);
    for (int i = 0; i < 10; i++)
        defs[i] = reversed ? t[9 - i] : t[i];
}

static bool rubylsp_order_edges(bool reversed, bool *ctor, bool *label, bool *dup, bool *own) {
    CBMLSPDef defs[10];
    rubylsp_order_defs(defs, reversed);
    const char *src = "class CxCaller\n  def run\n    w = CxWidget.new\n    w.label\n"
                      "    w.dup_m\n    w.own\n  end\nend\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.caller", defs, 10, NULL, NULL,
                           0, NULL, &out);
    *ctor = out_has(&out, "test.app.caller.CxCaller.run", "test.m0.CxWidget");
    *label = out_has(&out, "test.app.caller.CxCaller.run", "test.base.CxBase.label");
    *dup = out_has(&out, "test.app.caller.CxCaller.run", "test.m1.CxWidget.dup_m");
    *own = out_has(&out, "test.app.caller.CxCaller.run", "test.m0.CxWidget.own");
    if (!(*ctor && *label && *dup && *own))
        out_dump(&out);
    cbm_arena_destroy(&arena);
    return true;
}

TEST(rubylsp_cross_registry_is_def_order_independent) {
    bool f[4];
    bool r[4];
    ASSERT(rubylsp_order_edges(false, &f[0], &f[1], &f[2], &f[3]));
    ASSERT(rubylsp_order_edges(true, &r[0], &r[1], &r[2], &r[3]));
    /* head = smallest QN (m0), superclass from the smallest-QN declaration
     * that has one (m1's CxBase, not m2's CxOther), duplicate method from the
     * smaller QN (m1) — in both orders. */
    for (int i = 0; i < 4; i++) {
        ASSERT(f[i]);
        ASSERT(r[i]);
    }
    PASS();
}

/* ── cross-file: singleton side and mixins (review items 5, 6) ───── */

static const CBMDefinition *rubylsp_find_def(const CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].name && strcmp(r->defs.items[i].name, name) == 0)
            return &r->defs.items[i];
    }
    return NULL;
}

/* The extractor knows what CBMLSPDef used to lose: which methods sit on the
 * singleton side (`def self.m`, `class << self`) and which mixin macros a
 * class body holds. Only constants count; `extend self` is not one. */
TEST(rubylsp_extractor_flags_singleton_methods_and_mixins) {
    const char *src = "class Widget < Base\n"
                      "  include Auditable\n"
                      "  prepend Admin::Guard\n"
                      "  extend Finders\n"
                      "  extend self\n"
                      "  def inst\n  end\n"
                      "  def self.make\n  end\n"
                      "  class << self\n"
                      "    def build\n    end\n"
                      "  end\n"
                      "end\n";
    CBMFileResult *r = extract_ruby(src);
    ASSERT(r != NULL);
    const CBMDefinition *w = rubylsp_find_def(r, "Widget");
    const CBMDefinition *inst = rubylsp_find_def(r, "inst");
    const CBMDefinition *make = rubylsp_find_def(r, "make");
    const CBMDefinition *build = rubylsp_find_def(r, "build");
    bool ok = w && inst && make;
    bool mixins =
        ok && w->mixin_refs && strcmp(w->mixin_refs, "i:Auditable|p:Admin::Guard|e:Finders") == 0;
    /* A method inside `class << self`, when the extractor emits one at all,
     * must be on the singleton side. */
    bool flags = ok && !inst->is_singleton && make->is_singleton && (!build || build->is_singleton);
    printf("    defs: Widget=%d inst=%d make=%d build=%d (class << self def emitted: %s)\n",
           w != NULL, inst != NULL, make != NULL, build != NULL, build ? "yes" : "no");
    if (!(mixins && flags))
        printf("    mixin_refs=%s\n", ok && w->mixin_refs ? w->mixin_refs : "(null)");
    cbm_free_result(r);
    ASSERT(ok);
    ASSERT(mixins);
    ASSERT(flags);
    PASS();
}

/* Review item 5. `Child` defines `def self.run` and inherits instance `run`
 * from Base. An instance call must reach Base#run, the one Ruby would call; a
 * class-side call must reach Child.run. Registering each method on both keys
 * made the instance call stop at the singleton. */
TEST(rubylsp_cross_singleton_and_instance_do_not_shadow) {
    CBMLSPDef defs[4];
    cross_def(&defs[0], "test.base.Base", "Base", "Class", "test.base", NULL, NULL);
    cross_def(&defs[1], "test.base.Base.run", "run", "Method", "test.base", "test.base.Base", NULL);
    cross_def(&defs[2], "test.child.Child", "Child", "Class", "test.child", NULL, "Base");
    cross_def(&defs[3], "test.child.Child.run", "run", "Method", "test.child", "test.child.Child",
              NULL);
    defs[3].is_singleton = true;

    const char *src = "class Caller\n  def go\n    Child.new.run\n    Child.run\n  end\nend\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.caller", defs, 4, NULL, NULL, 0,
                           NULL, &out);
    bool instance = out_has(&out, "test.app.caller.Caller.go", "test.base.Base.run");
    bool klass = out_has(&out, "test.app.caller.Caller.go", "test.child.Child.run");
    if (!(instance && klass))
        out_dump(&out);
    cbm_arena_destroy(&arena);
    ASSERT(instance);
    ASSERT(klass);
    PASS();
}

static bool rubylsp_mixin_call(const char *decl_mixin, bool reopen_elsewhere, bool *module_hit,
                               bool *base_hit) {
    CBMLSPDef defs[8];
    int n = 0;
    cross_def(&defs[n++], "test.base.Base", "Base", "Class", "test.base", NULL, NULL);
    cross_def(&defs[n++], "test.base.Base.greet", "greet", "Method", "test.base", "test.base.Base",
              NULL);
    cross_def(&defs[n++], "test.mod.Overrides", "Overrides", "Module", "test.mod", NULL, NULL);
    cross_def(&defs[n++], "test.mod.Overrides.greet", "greet", "Method", "test.mod",
              "test.mod.Overrides", NULL);
    /* The class is declared in file A (the head); the include is in A, or in
     * a reopening in a later file Z. */
    cross_def(&defs[n++], "test.a.Widget", "Widget", "Class", "test.a", NULL, "Base");
    if (!reopen_elsewhere)
        defs[n - 1].mixin_refs = decl_mixin;
    if (reopen_elsewhere) {
        cross_def(&defs[n++], "test.z.Widget", "Widget", "Class", "test.z", NULL, NULL);
        defs[n - 1].mixin_refs = decl_mixin;
    }
    const char *src = "class Caller\n  def go\n    Widget.new.greet\n  end\nend\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.caller", defs, n, NULL, NULL, 0,
                           NULL, &out);
    *module_hit = out_has(&out, "test.app.caller.Caller.go", "test.mod.Overrides.greet");
    *base_hit = out_has(&out, "test.app.caller.Caller.go", "test.base.Base.greet");
    if (!*module_hit && !*base_hit && out.count > 1)
        out_dump(&out);
    cbm_arena_destroy(&arena);
    return true;
}

/* Review item 6. A module the class includes shadows the superclass's method,
 * whichever file the class and the include are in. A call from file B must not
 * resolve to Base#greet, and an include of a module the table cannot see must
 * block the walk rather than fall through to the superclass. */
TEST(rubylsp_cross_mixin_shadows_superclass_across_files) {
    bool module_hit;
    bool base_hit;
    ASSERT(rubylsp_mixin_call("i:Overrides", false, &module_hit, &base_hit));
    ASSERT(module_hit);
    ASSERT(!base_hit);
    PASS();
}

TEST(rubylsp_cross_mixin_in_reopening_shadows_superclass) {
    bool module_hit;
    bool base_hit;
    ASSERT(rubylsp_mixin_call("i:Overrides", true, &module_hit, &base_hit));
    ASSERT(module_hit);
    ASSERT(!base_hit);
    PASS();
}

TEST(rubylsp_cross_unseen_mixin_blocks_superclass) {
    bool module_hit;
    bool base_hit;
    ASSERT(rubylsp_mixin_call("i:Missing", false, &module_hit, &base_hit));
    ASSERT(!module_hit);
    ASSERT(!base_hit);
    PASS();
}

/* The finder rule (`User.find` -> User, relation chains keep the model) for a
 * model declared in ANOTHER file, the usual Rails layout. It is one switch
 * (ruby_ar_finder_cross_file): on by default; off keeps the rule to models
 * declared in the file being resolved, with nothing else changed. */
static bool rubylsp_ar_finder_cross_edge(void) {
    CBMLSPDef defs[2];
    cross_def(&defs[0], "test.models.user.User", "User", "Class", "test.models.user", NULL,
              "ApplicationRecord");
    cross_def(&defs[1], "test.models.user.User.full_name", "full_name", "Method",
              "test.models.user", "test.models.user.User", NULL);
    const char *src = "class Svc\n  def go\n    User.find(1).full_name\n    "
                      "User.where(active: true).first.full_name\n  end\nend\n";
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolvedCallArray out;
    memset(&out, 0, sizeof(out));
    cbm_run_ruby_lsp_cross(&arena, src, (int)strlen(src), "test.app.svc", defs, 2, NULL, NULL, 0,
                           NULL, &out);
    bool edge = out_has(&out, "test.app.svc.Svc.go", "test.models.user.User.full_name");
    cbm_arena_destroy(&arena);
    return edge;
}

TEST(rubylsp_cross_ar_finder_crosses_files_by_default) {
    ASSERT(rubylsp_ar_finder_cross_edge());
    PASS();
}

TEST(rubylsp_cross_ar_finder_same_file_only_when_switched_off) {
    cbm_ruby_set_ar_finder_cross_file(false);
    bool cross = rubylsp_ar_finder_cross_edge();
    const char *src = "class User < ApplicationRecord\n  def full_name\n    'x'\n  end\nend\n"
                      "class UsersController < ApplicationController\n  def show\n"
                      "    User.find(1).full_name\n  end\nend\n";
    CBMFileResult *r = extract_ruby(src);
    bool same = r && find_resolved(r, "UsersController.show", "User.full_name") >= 0;
    if (r)
        cbm_free_result(r);
    cbm_ruby_set_ar_finder_cross_file(true);
    ASSERT(!cross);
    ASSERT(same);
    PASS();
}

/* ── suite ──────────────────────────────────────────────────────── */

void suite_ruby_lsp(void) {
    RUN_TEST(rubylsp_qn_contract);
    RUN_TEST(rubylsp_method_via_constructor_assignment);
    RUN_TEST(rubylsp_constructor_edge_targets_class);
    RUN_TEST(rubylsp_self_dispatch);
    RUN_TEST(rubylsp_singleton_method_dispatch);
    RUN_TEST(rubylsp_singleton_instance_split);
    RUN_TEST(rubylsp_inheritance);
    RUN_TEST(rubylsp_include_mixin);
    RUN_TEST(rubylsp_prepend_mixin);
    RUN_TEST(rubylsp_unresolved_ancestry_fails_closed);
    RUN_TEST(rubylsp_resolved_ancestry_still_resolves);
    RUN_TEST(rubylsp_block_defs_not_module_members);
    RUN_TEST(rubylsp_top_level_block_defs_fail_closed);
    RUN_TEST(rubylsp_extend_mixin);
    RUN_TEST(rubylsp_super_dispatch);
    RUN_TEST(rubylsp_ivar_typing);
    RUN_TEST(rubylsp_ivar_conflict_no_edge);
    RUN_TEST(rubylsp_ivar_non_constructor_write_latches);
    RUN_TEST(rubylsp_ivar_or_assign_constructor_types);
    RUN_TEST(rubylsp_chained_constructor_call);
    RUN_TEST(rubylsp_nested_modules);
    RUN_TEST(rubylsp_compact_class_name_exact);
    RUN_TEST(rubylsp_compact_class_inside_module_exact);
    RUN_TEST(rubylsp_compact_class_does_not_open_outer_scope);
    RUN_TEST(rubylsp_activerecord_model_typing);
    RUN_TEST(rubylsp_top_level_function);
    RUN_TEST(rubylsp_unknown_receiver_no_edge);
    RUN_TEST(rubylsp_def_is_scope_gate);
    RUN_TEST(rubylsp_block_param_shadows_local);
    RUN_TEST(rubylsp_untyped_reassignment_drops_type);
    RUN_TEST(rubylsp_send_no_edge);
    RUN_TEST(rubylsp_dynamic_dispatch_family_no_edge);
    RUN_TEST(rubylsp_self_rebinding_blocks_no_edge);
    RUN_TEST(rubylsp_mixin_walk_visits_each_module_once);
    RUN_TEST(rubylsp_const_path_is_linear);
    RUN_TEST(rubylsp_stdlib_seed_survives_alloc_failure);
    RUN_TEST(rubylsp_cross_file_defs);
    RUN_TEST(rubylsp_cross_file_ar_repro);
    RUN_TEST(rubylsp_cross_reopened_class_across_files);
    RUN_TEST(rubylsp_cross_compact_class_name);
    RUN_TEST(rubylsp_cross_registry_built_once_serves_many_files);
    RUN_TEST(rubylsp_cross_reopened_class_work_is_bounded);
    RUN_TEST(rubylsp_cross_registry_is_def_order_independent);
    RUN_TEST(rubylsp_extractor_flags_singleton_methods_and_mixins);
    RUN_TEST(rubylsp_cross_singleton_and_instance_do_not_shadow);
    RUN_TEST(rubylsp_cross_mixin_shadows_superclass_across_files);
    RUN_TEST(rubylsp_cross_mixin_in_reopening_shadows_superclass);
    RUN_TEST(rubylsp_cross_unseen_mixin_blocks_superclass);
    RUN_TEST(rubylsp_cross_ar_finder_crosses_files_by_default);
    RUN_TEST(rubylsp_cross_ar_finder_same_file_only_when_switched_off);
}
