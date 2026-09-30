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
 *   13. ActiveRecord model typing      (User.find(1).full_name)
 *   14. Top-level function calls
 *   15. Unresolvable receiver emits NO spurious edge (negative)
 *   16. send() / the whole dynamic-dispatch family emits NO edge (negative)
 *
 * Scope: every row here is SINGLE-FILE, driven through cbm_extract_file so it
 * exercises the per-file resolver exactly as the extraction pipeline calls it.
 * Cross-file resolution (cbm_run_ruby_lsp_cross, wired through
 * pass_lsp_cross.c) is a separate tier and is covered by its own rows.
 *
 * The resolver populates result->resolved_calls with CBMResolvedCall rows.
 * Ruby defs weave the class path into the QN (module_qn.Animal.speak), so
 * for these single-file fixtures ("test"/"main.rb" -> module QN test.main)
 * callee fragments like "main.Animal.speak" are unique join keys.
 */
#include "test_framework.h"
#include "cbm.h"
#include "../src/pipeline/lsp_resolve.h"
#include "lsp/ruby_lsp.h"
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
    /* Lexical reference from sibling class inside the module. */
    ASSERT(require_resolved(r, "Panel.show", "User.lookup") >= 0);
    ASSERT(require_resolved(r, "Panel.show", "User.name") >= 0);
    /* Fully-qualified reference from outside. */
    ASSERT(require_resolved(r, "Outside.probe", "User.lookup") >= 0);
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
 * any of these as an ordinary call would invent an edge to `hidden`. */
TEST(rubylsp_dynamic_dispatch_family_no_edge) {
    const char *src = "class Target\n"
                      "  def hidden\n"
                      "    1\n"
                      "  end\n"
                      "end\n"
                      "class Meta\n"
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
                      "    Target.new.instance_eval { hidden }\n"
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
    RUN_TEST(rubylsp_extend_mixin);
    RUN_TEST(rubylsp_super_dispatch);
    RUN_TEST(rubylsp_ivar_typing);
    RUN_TEST(rubylsp_ivar_conflict_no_edge);
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
}
