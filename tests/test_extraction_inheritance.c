/*
 * test_extraction_inheritance.c — Reproduce-first suite for class-inheritance
 * base-class extraction across the 9 hybrid-LSP languages.
 *
 * DESIGN INTENT
 * ─────────────
 * Every row asserts the CORRECT behaviour: that `CBMDefinition.base_classes`
 * contains real type names, not keywords or punctuation.  For the three broken
 * languages the rows are EXPECTED RED until the extractors are fixed:
 *
 *   RED (broken extractors — rows FAIL until fixed):
 *     TypeScript / TSX — extractor stores the keyword "extends" / "implements"
 *                        instead of the following type name.
 *     PHP             — base_classes is never populated (always NULL / empty).
 *     Kotlin          — `:` supertype syntax is not parsed → base_classes empty.
 *
 *   GREEN (correct extractors — rows PASS as regression guards):
 *     Python  — NOTE: the simple `class Dog(Animal):` case is ALSO red because
 *               collect_bases_from_field does not yet match the plain `identifier`
 *               node used by tree-sitter-python (it grabs the argument_list text
 *               "(Animal)" with parens).  The reproduction for that root-cause is
 *               already in test_extraction.c (python_class_base_extracted_bare).
 *               This suite adds multi-base, generic-base, and qualified-base
 *               variants that share the same root bug — they are all RED until the
 *               identifier-node fix lands.  Python cases that use
 *               `type_identifier` nodes (not yet confirmed) are tentatively
 *               labelled RED.
 *     Java    — single-base and multi-interface extraction is correct (regression
 *               guard for the fix landed with #279).
 *     C#      — extraction is correct (regression guards).
 *     C++     — extraction is correct (regression guards).
 *     Rust    — impl_traits array (not base_classes) is the capture point; Rust
 *               rows verify impl_traits entries for `impl Trait for Struct`.
 *
 * STRUCTURE
 * ─────────
 * inherit_case_t   — one test row.
 * run_inherit_case — generic runner: extract → find def → check base_classes
 *                    (or impl_traits for Rust).
 * One TEST() per language group so failures are readable at the suite level.
 * SUITE(extraction_inheritance) wires them together via RUN_TEST().
 *
 * HOW TO ADD TO THE RUNNER
 * ─────────────────────────
 * In test_main.c add:
 *   extern void suite_extraction_inheritance(void);
 *   RUN_SUITE(extraction_inheritance);
 * (Do NOT do this here — another workstream owns test_main.c.)
 */

#include "test_framework.h"
#include "cbm.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>

/* ── Shared helpers ─────────────────────────────────────────────── */

/* Find a definition by label+name; returns NULL if not found. */
static CBMDefinition *find_def(CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, label) == 0 && d->name && strcmp(d->name, name) == 0)
            return d;
    }
    return NULL;
}

/* Find a definition by name alone (any label). */
static CBMDefinition *find_def_any(CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, name) == 0)
            return d;
    }
    return NULL;
}

/* Return 1 if base_classes contains `want` exactly. */
static int bases_contain(CBMDefinition *d, const char *want) {
    if (!d->base_classes)
        return 0;
    for (const char **b = d->base_classes; *b; b++) {
        if (strcmp(*b, want) == 0)
            return 1;
    }
    return 0;
}

/* Return 1 if ANY base_classes entry contains `substr` as a substring.
 * Used to assert that keywords like "extends" / "implements" / ":" do NOT
 * appear inside any captured base name. */
static int bases_contain_substr(CBMDefinition *d, const char *substr) {
    if (!d->base_classes)
        return 0;
    for (const char **b = d->base_classes; *b; b++) {
        if (strstr(*b, substr))
            return 1;
    }
    return 0;
}

/* Count entries in a NULL-terminated base_classes array. */
static int bases_count(CBMDefinition *d) {
    if (!d->base_classes)
        return 0;
    int n = 0;
    for (const char **b = d->base_classes; *b; b++)
        n++;
    return n;
}

/* ── Table-driven case type ─────────────────────────────────────── */

/* Labels used to look up the class definition in the extraction result.
 * The extractor uses "Class", "Interface", "Struct" — we try all three in
 * find_def_flex() below. */
typedef struct {
    CBMLanguage lang;
    const char *path;              /* file path hint (sets language via extension too) */
    const char *src;               /* source snippet */
    const char *class_name;        /* name to look up */
    const char *expected_bases[8]; /* NULL-terminated list of real type names to find */
    /* bad_strings: substrings that must NOT appear inside any base_classes entry.
     * Used to catch keyword-capture bugs.  NULL-terminated, or {NULL} if unused. */
    const char *bad_strings[6];
    int min_base_count; /* >= this many entries expected (0 = don't check count) */
} inherit_case_t;

/* Find def trying Class / Interface / Struct / any-label fallback. */
static CBMDefinition *find_def_flex(CBMFileResult *r, const char *name) {
    CBMDefinition *d;
    if ((d = find_def(r, "Class", name)))
        return d;
    if ((d = find_def(r, "Interface", name)))
        return d;
    if ((d = find_def(r, "Struct", name)))
        return d;
    /* Fallback: any label — covers languages where the extractor uses custom labels */
    return find_def_any(r, name);
}

/*
 * Generic runner for one inherit_case_t row.
 * Returns 0 on pass, 1 on failure (sets FAIL path via printf + return).
 * Cannot use ASSERT macros directly here because they do `return 1` — which
 * is fine since this helper also returns int.  We use manual checks so we
 * can include the case description in the failure message.
 */
static int run_inherit_case(const inherit_case_t *tc) {
    CBMFileResult *r =
        cbm_extract_file(tc->src, (int)strlen(tc->src), tc->lang, "t", tc->path, 0, NULL, NULL);
    if (!r) {
        printf("  FAIL  [%s] cbm_extract_file returned NULL\n", tc->class_name);
        return 1;
    }

    CBMDefinition *cls = find_def_flex(r, tc->class_name);
    if (!cls) {
        printf("  FAIL  [%s] definition not found in extraction result\n", tc->class_name);
        cbm_free_result(r);
        return 1;
    }

    /* When no bases are expected and no min count is set, this is a
     * no-crash / sanity row — skip the rest of the checks. */
    if (!tc->expected_bases[0] && tc->min_base_count == 0) {
        cbm_free_result(r);
        return 0;
    }

    /* Assert base_classes is non-NULL (at least one base expected). */
    if (!cls->base_classes) {
        printf("  FAIL  [%s] base_classes is NULL (expected non-empty)\n", tc->class_name);
        cbm_free_result(r);
        return 1;
    }

    /* Assert each expected base name is present. */
    int ok = 1;
    for (int i = 0; tc->expected_bases[i]; i++) {
        if (!bases_contain(cls, tc->expected_bases[i])) {
            printf("  FAIL  [%s] expected base \"%s\" not found in base_classes\n", tc->class_name,
                   tc->expected_bases[i]);
            ok = 0;
        }
    }

    /* Assert no bad keyword strings appear inside any base name. */
    for (int i = 0; tc->bad_strings[i]; i++) {
        if (bases_contain_substr(cls, tc->bad_strings[i])) {
            printf("  FAIL  [%s] bad string \"%s\" found inside a base_classes entry\n",
                   tc->class_name, tc->bad_strings[i]);
            ok = 0;
        }
    }

    /* Assert minimum count. */
    if (tc->min_base_count > 0) {
        int cnt = bases_count(cls);
        if (cnt < tc->min_base_count) {
            printf("  FAIL  [%s] base_classes has %d entries, expected >= %d\n", tc->class_name,
                   cnt, tc->min_base_count);
            ok = 0;
        }
    }

    cbm_free_result(r);
    return ok ? 0 : 1;
}

/* Convenience macro: run a table of cases, accumulate failures. */
#define RUN_CASES(table)                                               \
    do {                                                               \
        int _fail = 0;                                                 \
        int _n = (int)(sizeof(table) / sizeof(table[0]));              \
        for (int _i = 0; _i < _n; _i++) {                              \
            if (run_inherit_case(&table[_i]) != 0)                     \
                _fail++;                                               \
        }                                                              \
        if (_fail > 0) {                                               \
            printf("  FAIL  %d / %d cases above failed\n", _fail, _n); \
            return 1;                                                  \
        }                                                              \
    } while (0)

/* ═══════════════════════════════════════════════════════════════════
 * PYTHON  (expected: RED until identifier-node fix in extract_defs.c)
 *
 * Root cause: collect_bases_from_field() does not match the plain
 * `identifier` node used by tree-sitter-python for unqualified base
 * names → raw argument_list text "(Base)" (with parens) is stored
 * instead of "Base". Fix: accept `identifier` as a valid child type.
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t python_cases[] = {
    /* ── single base (bare identifier) ────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class Animal:\n    pass\n\nclass Dog(Animal):\n    pass\n",
     "Dog",
     {"Animal", NULL},
     {"(", ")", NULL},
     1},
    /* ── multiple bases ─────────────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class A: pass\nclass B: pass\nclass C(A, B): pass\n",
     "C",
     {"A", "B", NULL},
     {"(", ")", NULL},
     2},
    /* ── base + mixin ───────────────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class Base: pass\nclass Mixin: pass\nclass Service(Base, Mixin): pass\n",
     "Service",
     {"Base", "Mixin", NULL},
     {"(", ")", NULL},
     2},
    /* ── generic base (subscript, e.g. Generic[T]) ──────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "from typing import Generic, TypeVar\nT = TypeVar('T')\n"
     "class Stack(Generic[T]):\n    pass\n",
     "Stack",
     {"Generic", NULL},
     {"(", ")", NULL},
     1},
    /* ── qualified base (dotted: module.Base) ───────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "import django.db\nclass MyModel(django.db.Model):\n    pass\n",
     "MyModel",
     {"django.db.Model", NULL},
     {"(", ")", NULL},
     1},
    /* ── abstract base (abc.ABC) ────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "from abc import ABC, abstractmethod\nclass Shape(ABC):\n    @abstractmethod\n    def "
     "area(self): pass\n",
     "Shape",
     {"ABC", NULL},
     {"(", ")", NULL},
     1},
    /* ── dataclass with base ────────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "from dataclasses import dataclass\n@dataclass\nclass Point:\n    x: float\n\n"
     "@dataclass\nclass Point3D(Point):\n    z: float\n",
     "Point3D",
     {"Point", NULL},
     {"(", ")", NULL},
     1},
    /* ── exception subclass ─────────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class AppError(Exception): pass\n",
     "AppError",
     {"Exception", NULL},
     {"(", ")", NULL},
     1},
    /* ── three bases ────────────────────────────────────────────── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class X: pass\nclass Y: pass\nclass Z: pass\nclass Multi(X, Y, Z): pass\n",
     "Multi",
     {"X", "Y", "Z", NULL},
     {"(", ")", NULL},
     3},
    /* ── base with keyword argument (metaclass=) should not bleed ── */
    {CBM_LANG_PYTHON,
     "m.py",
     "class Meta: pass\nclass MyClass(object, metaclass=Meta): pass\n",
     "MyClass",
     {"object", NULL},
     {"(", ")", "metaclass", NULL},
     1},
};

TEST(inherit_python) {
    RUN_CASES(python_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * JAVA  (expected: GREEN — regression guards for #279 fix)
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t java_cases[] = {
    /* ── extends only ───────────────────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Dog extends Animal { }",
     "Dog",
     {"Animal", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── implements single interface ─────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class ConcreteList implements List { }",
     "ConcreteList",
     {"List", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── extends + implements one ────────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class MyThread extends Thread implements Runnable { }",
     "MyThread",
     {"Thread", "Runnable", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── extends + implements two (regression for #279) ─────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class DefaultLinkTool extends DefaultDiagramTool implements ILinkTool, Closeable { }",
     "DefaultLinkTool",
     {"DefaultDiagramTool", "ILinkTool", "Closeable", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── implements three interfaces ─────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Svc implements A, B, C { }",
     "Svc",
     {"A", "B", "C", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── generic base (extends List<String>) ─────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "import java.util.*; public class MyList extends ArrayList<String> { }",
     "MyList",
     {"ArrayList", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── generic implements (Comparable<T>) ──────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Box<T> implements Comparable<Box<T>> { public int compareTo(Box<T> o) { return "
     "0; } }",
     "Box",
     {"Comparable", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── abstract class extends ──────────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public abstract class AbstractSvc extends BaseService { protected abstract void run(); }",
     "AbstractSvc",
     {"BaseService", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── interface extends interface ─────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public interface ReadWriteRepo extends ReadRepo, WriteRepo { }",
     "ReadWriteRepo",
     {"ReadRepo", "WriteRepo", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── enum implements interface ───────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public enum Status implements Displayable { OPEN, CLOSED; public String display() { return "
     "name(); } }",
     "Status",
     {"Displayable", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── qualified (imported) type name ─────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Handler extends java.net.ServerSocket { }",
     "Handler",
     {"java.net.ServerSocket", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── extends + implements four ───────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Mega extends Base implements A, B, C, D { }",
     "Mega",
     {"Base", "A", "B", "C", "D", NULL},
     {"extends", "implements", NULL},
     5},
    /* ── nested class with base ──────────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Outer { public static class Inner extends Outer { } }",
     "Inner",
     {"Outer", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── record implements interface (Java 16+) ──────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public record Point(int x, int y) implements Comparable<Point> { "
     "public int compareTo(Point o) { return Integer.compare(x, o.x); } }",
     "Point",
     {"Comparable", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── sealed class (Java 17+) ─────────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public sealed class Shape permits Circle, Rectangle { }",
     "Shape",
     {NULL}, /* permitted-types are not base_classes of Shape; no bases to assert */
     {NULL},
     0},
    /* ── class implements Serializable ──────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "import java.io.*; public class Data extends BaseData implements Serializable, Cloneable { }",
     "Data",
     {"BaseData", "Serializable", "Cloneable", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── generic class extends generic base ──────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Pair<A, B> extends AbstractPair<A, B> implements Iterable<A> { "
     "public java.util.Iterator<A> iterator() { return null; } }",
     "Pair",
     {"AbstractPair", "Iterable", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── class extending Exception ───────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class AppException extends RuntimeException { "
     "public AppException(String msg) { super(msg); } }",
     "AppException",
     {"RuntimeException", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── multiple interfaces no extends ──────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "public class Codec implements Encoder, Decoder, Closeable { }",
     "Codec",
     {"Encoder", "Decoder", "Closeable", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── annotated class with base ───────────────────────────────── */
    {CBM_LANG_JAVA,
     "Svc.java",
     "@Override public class AnnotatedSvc extends BaseSvc { }",
     "AnnotatedSvc",
     {"BaseSvc", NULL},
     {"extends", "implements", NULL},
     1},
};

TEST(inherit_java) {
    RUN_CASES(java_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * C#  (expected: GREEN — regression guards)
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t csharp_cases[] = {
    /* ── single base class ──────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Dog : Animal { }",
     "Dog",
     {"Animal", NULL},
     {":", NULL},
     1},
    /* ── implements single interface ─────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Repo : IRepository { }",
     "Repo",
     {"IRepository", NULL},
     {":", NULL},
     1},
    /* ── base + interface ────────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Service : BaseService, IService { }",
     "Service",
     {"BaseService", "IService", NULL},
     {":", NULL},
     2},
    /* ── base + two interfaces ───────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Worker : BackgroundService, IWorker, IDisposable { }",
     "Worker",
     {"BackgroundService", "IWorker", "IDisposable", NULL},
     {":", NULL},
     3},
    /* ── generic base ────────────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class OrderList : List<Order> { }",
     "OrderList",
     {"List", NULL},
     {":", NULL},
     1},
    /* ── generic base + interface ────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Stack<T> : Collection<T>, IStack<T> { }",
     "Stack",
     {"Collection", "IStack", NULL},
     {":", NULL},
     2},
    /* ── interface extends two interfaces ────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public interface ICrud : IRead, IWrite { }",
     "ICrud",
     {"IRead", "IWrite", NULL},
     {":", NULL},
     2},
    /* ── abstract class with base ────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public abstract class AbstractHandler : BaseHandler { protected abstract void Handle(); }",
     "AbstractHandler",
     {"BaseHandler", NULL},
     {":", NULL},
     1},
    /* ── sealed class ────────────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public sealed class SingletonService : BaseService { }",
     "SingletonService",
     {"BaseService", NULL},
     {":", NULL},
     1},
    /* ── namespace-qualified base ────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "namespace App { public class Handler : System.Net.Http.HttpMessageHandler { "
     "protected override System.Threading.Tasks.Task<System.Net.Http.HttpResponseMessage> "
     "SendAsync(System.Net.Http.HttpRequestMessage r, System.Threading.CancellationToken c) "
     "{ return null; } } }",
     "Handler",
     {"System.Net.Http.HttpMessageHandler", NULL},
     {":", NULL},
     1},
    /* ── partial class with base ─────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public partial class PartialSvc : BaseSvc, IPartial { }",
     "PartialSvc",
     {"BaseSvc", "IPartial", NULL},
     {":", NULL},
     2},
    /* ── record with base ────────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public record OrderRecord(int Id) : BaseRecord(Id), IRecord { }",
     "OrderRecord",
     {"BaseRecord", "IRecord", NULL},
     {":", NULL},
     2},
    /* ── struct implements interface ─────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public struct Point : IEquatable<Point> { public bool Equals(Point other) => true; }",
     "Point",
     {"IEquatable", NULL},
     {":", NULL},
     1},
    /* ── class with four interfaces ──────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Mega : Base, IA, IB, IC, ID { }",
     "Mega",
     {"Base", "IA", "IB", "IC", "ID", NULL},
     {":", NULL},
     5},
    /* ── exception subclass ─────────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class AppException : Exception { public AppException(string msg) : base(msg) {} }",
     "AppException",
     {"Exception", NULL},
     {":", NULL},
     1},
    /* ── nested class with base ──────────────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Outer { public class Inner : Outer { } }",
     "Inner",
     {"Outer", NULL},
     {":", NULL},
     1},
    /* ── class in namespace with base ───────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "namespace MyApp.Services { public class OrderSvc : BaseOrderSvc, IOrderSvc { } }",
     "OrderSvc",
     {"BaseOrderSvc", "IOrderSvc", NULL},
     {":", NULL},
     2},
    /* ── interface with single parent ───────────────────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public interface IAdvancedService : IBasicService { void AdvancedOp(); }",
     "IAdvancedService",
     {"IBasicService", NULL},
     {":", NULL},
     1},
    /* ── generic class implements generic interface ───────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Repo<T> : BaseRepo<T>, IRepo<T> where T : class { }",
     "Repo",
     {"BaseRepo", "IRepo", NULL},
     {":", "where", NULL},
     2},
    /* ── class with IDisposable + IAsyncDisposable ───────────────── */
    {CBM_LANG_CSHARP,
     "Svc.cs",
     "public class Resource : IDisposable, IAsyncDisposable { "
     "public void Dispose() {} "
     "public System.Threading.Tasks.ValueTask DisposeAsync() => default; }",
     "Resource",
     {"IDisposable", "IAsyncDisposable", NULL},
     {":", NULL},
     2},
};

TEST(inherit_csharp) {
    RUN_CASES(csharp_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * C++  (expected: GREEN — regression guards)
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t cpp_cases[] = {
    /* ── public single base ─────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Dog : public Animal { };",
     "Dog",
     {"Animal", NULL},
     {"public", "private", "protected", ":", NULL},
     1},
    /* ── private base ────────────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Impl : private Base { };",
     "Impl",
     {"Base", NULL},
     {"public", "private", "protected", ":", NULL},
     1},
    /* ── multiple inheritance ────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class C : public A, public B { };",
     "C",
     {"A", "B", NULL},
     {"public", "private", "protected", ":", NULL},
     2},
    /* ── struct with base ────────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "struct Derived : Base { int x; };",
     "Derived",
     {"Base", NULL},
     {":", NULL},
     1},
    /* ── virtual inheritance ─────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class D : public virtual B1, public virtual B2 { };",
     "D",
     {"B1", "B2", NULL},
     {"virtual", "public", ":", NULL},
     2},
    /* ── template base ───────────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "template<typename T> class Stack : public std::vector<T> { };",
     "Stack",
     {"std::vector", NULL},
     /* qualified base: `::` is legitimate, so the bare-`:` separator-leak
      * guard does not apply here (it would match inside `std::vector`). */
     {"public", "template", NULL},
     1},
    /* ── CRTP pattern ────────────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "template<typename Derived> class Base { };\n"
     "class Concrete : public Base<Concrete> { };",
     "Concrete",
     {"Base", NULL},
     {"public", ":", NULL},
     1},
    /* ── abstract class (pure virtual) with public base ──────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class AbstractLogger : public ILogger { public: virtual void log(const char*) = 0; };",
     "AbstractLogger",
     {"ILogger", NULL},
     {"public", ":", NULL},
     1},
    /* ── class in namespace ──────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "namespace net { class Socket : public BaseSocket { }; }",
     "Socket",
     {"BaseSocket", NULL},
     {"public", ":", NULL},
     1},
    /* ── three-way diamond inheritance ──────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class A { }; class B : public A { }; class C : public A { };\n"
     "class D : public B, public C { };",
     "D",
     {"B", "C", NULL},
     {"public", ":", NULL},
     2},
    /* ── fully qualified base name ───────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class MyStream : public std::ostream { public: MyStream() : std::ostream(nullptr) {} };",
     "MyStream",
     {"std::ostream", NULL},
     /* qualified base: bare-`:` guard omitted (matches inside `std::ostream`). */
     {"public", NULL},
     1},
    /* ── protected base ──────────────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Node : protected TreeNode { };",
     "Node",
     {"TreeNode", NULL},
     {"protected", ":", NULL},
     1},
    /* ── multiple bases with mixed access ────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Widget : public Drawable, private EventHandler, protected Serializable { };",
     "Widget",
     {"Drawable", "EventHandler", "Serializable", NULL},
     {"public", "private", "protected", ":", NULL},
     3},
    /* ── exception class from std::exception ─────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "#include <stdexcept>\nclass AppError : public std::runtime_error { "
     "public: AppError(const char* m) : std::runtime_error(m) {} };",
     "AppError",
     {"std::runtime_error", NULL},
     /* qualified base: bare-`:` guard omitted (matches inside `std::runtime_error`). */
     {"public", NULL},
     1},
    /* ── template class with multiple template base types ────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "template<typename K, typename V>\n"
     "class LruCache : public Cache<K,V>, public Observable { };",
     "LruCache",
     {"Cache", "Observable", NULL},
     {"public", ":", NULL},
     2},
    /* ── nested class with base ──────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Outer { public: class Inner : public Base { }; };",
     "Inner",
     {"Base", NULL},
     {"public", ":", NULL},
     1},
    /* ── class using final specifier ─────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class Leaf final : public Node { };",
     "Leaf",
     {"Node", NULL},
     {"public", "final", ":", NULL},
     1},
    /* ── struct with scoped base ─────────────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "struct MyVisitor : public boost::static_visitor<int> { int operator()(int x) { return x; } "
     "};",
     "MyVisitor",
     {"boost::static_visitor", NULL},
     /* qualified base: bare-`:` guard omitted (matches inside `boost::static_visitor`). */
     {"public", NULL},
     1},
    /* ── policy-based design (two template base policies) ────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "template<class StoragePolicy, class LogPolicy>\n"
     "class Engine : public StoragePolicy, public LogPolicy { };",
     "Engine",
     {"StoragePolicy", "LogPolicy", NULL},
     {"public", ":", NULL},
     2},
    /* ── empty base optimization (EBO) ──────────────────────────── */
    {CBM_LANG_CPP,
     "svc.cpp",
     "class EboContainer : private Allocator, public ContainerBase { };",
     "EboContainer",
     {"Allocator", "ContainerBase", NULL},
     {"private", "public", ":", NULL},
     2},
};

TEST(inherit_cpp) {
    RUN_CASES(cpp_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * TYPESCRIPT  (expected: RED — keyword-capture bug)
 *
 * Bug: the TS extractor stores "extends" / "implements" as the base
 * name instead of the following type name.
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t typescript_cases[] = {
    /* ── extends single class ────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Dog extends Animal { bark(): void {} }",
     "Dog",
     {"Animal", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── implements single interface ─────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Repo implements IRepository { save(x: any) {} }",
     "Repo",
     {"IRepository", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── extends + implements ────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Service extends BaseService implements IService { run() {} }",
     "Service",
     {"BaseService", "IService", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── implements two interfaces ───────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Codec implements Encoder, Decoder { encode(x: any) {} decode(x: any) {} }",
     "Codec",
     {"Encoder", "Decoder", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── extends + implements three ──────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Mega extends Base implements A, B, C { }",
     "Mega",
     {"Base", "A", "B", "C", NULL},
     {"extends", "implements", NULL},
     4},
    /* ── generic base ────────────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Stack<T> extends Array<T> implements IStack<T> { push(x: T) { return 0; } }",
     "Stack",
     {"Array", "IStack", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── interface extends interface ─────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "interface ReadWriteRepo extends ReadRepo, WriteRepo { }",
     "ReadWriteRepo",
     {"ReadRepo", "WriteRepo", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── abstract class ──────────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "abstract class AbstractSvc extends BaseService { abstract run(): void; }",
     "AbstractSvc",
     {"BaseService", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── class extends Error ─────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class AppError extends Error { constructor(msg: string) { super(msg); } }",
     "AppError",
     {"Error", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── class extends EventEmitter ──────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "import { EventEmitter } from 'events';\n"
     "class Bus extends EventEmitter { emit(ev: string) { return super.emit(ev); } }",
     "Bus",
     {"EventEmitter", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── generic class extends generic base ──────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Pair<A, B> extends AbstractPair<A, B> implements Iterable<A> { "
     "[Symbol.iterator]() { return this as any; } }",
     "Pair",
     {"AbstractPair", "Iterable", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── class implements multiple generic interfaces ─────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Handler implements Middleware<Request, Response>, Disposable { "
     "handle(req: Request): Response { return null as any; } dispose() {} }",
     "Handler",
     {"Middleware", "Disposable", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── class in module namespace ───────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "export class OrderService extends BaseOrderService implements IOrderService { }",
     "OrderService",
     {"BaseOrderService", "IOrderService", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── mixin target class (concrete class consuming a mixin) ──────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class Loggable { log(msg: string) { console.log(msg); } }\n"
     "class Logger extends Loggable implements ILogger { info(msg: string) { this.log(msg); } }",
     "Logger",
     {"Loggable", "ILogger", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── decorator + extends ─────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "function Injectable() { return (c: any) => c; }\n"
     "@Injectable()\nclass UserService extends BaseUserService { }",
     "UserService",
     {"BaseUserService", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── class implementing multiple inferred generics ───────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class BinaryTree<T> extends Tree<T> implements Traversable<T>, Serializable { "
     "traverse() {} serialize() { return ''; } }",
     "BinaryTree",
     {"Tree", "Traversable", "Serializable", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── React component extends ──────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "import React from 'react';\n"
     "class MyComponent extends React.Component<{}, {}> { render() { return null; } }",
     "MyComponent",
     {"React.Component", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── exception hierarchy ─────────────────────────────────────── */
    {CBM_LANG_TYPESCRIPT,
     "svc.ts",
     "class NetworkError extends AppError implements Retryable { retry() {} }",
     "NetworkError",
     {"AppError", "Retryable", NULL},
     {"extends", "implements", NULL},
     2},
};

TEST(inherit_typescript) {
    RUN_CASES(typescript_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * TSX  (expected: RED — same keyword-capture bug as TypeScript)
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t tsx_cases[] = {
    /* ── React.Component subclass ────────────────────────────────── */
    {CBM_LANG_TSX,
     "App.tsx",
     "import React from 'react';\n"
     "class App extends React.Component<{}, {}> { render() { return <div/>; } }",
     "App",
     {"React.Component", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── PureComponent subclass ──────────────────────────────────── */
    {CBM_LANG_TSX,
     "Widget.tsx",
     "import { PureComponent } from 'react';\n"
     "class Widget extends PureComponent { render() { return null; } }",
     "Widget",
     {"PureComponent", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── component + interface ───────────────────────────────────── */
    {CBM_LANG_TSX,
     "Form.tsx",
     "import { Component } from 'react';\n"
     "class Form extends Component<FormProps, FormState> implements IForm { "
     "render() { return <form/>; } submit() {} }",
     "Form",
     {"Component", "IForm", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── abstract component ──────────────────────────────────────── */
    {CBM_LANG_TSX,
     "Base.tsx",
     "import { Component } from 'react';\n"
     "abstract class BaseView extends Component { abstract renderContent(): JSX.Element; }",
     "BaseView",
     {"Component", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── non-React class in .tsx ─────────────────────────────────── */
    {CBM_LANG_TSX,
     "store.tsx",
     "class UserStore extends BaseStore implements IStore { load() {} }",
     "UserStore",
     {"BaseStore", "IStore", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── generic component ───────────────────────────────────────── */
    {CBM_LANG_TSX,
     "List.tsx",
     "import { Component } from 'react';\n"
     "class List<T> extends Component<{ items: T[] }> { render() { return <ul/>; } }",
     "List",
     {"Component", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── interface extends in tsx file ───────────────────────────── */
    {CBM_LANG_TSX,
     "types.tsx",
     "interface IAdvanced extends IBasic, IExtended { doAdvanced(): void; }",
     "IAdvanced",
     {"IBasic", "IExtended", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── multiple interfaces + base ──────────────────────────────── */
    {CBM_LANG_TSX,
     "Mega.tsx",
     "class MegaComponent extends React.Component implements Serializable, Disposable { "
     "render() { return null; } }",
     "MegaComponent",
     {"React.Component", "Serializable", "Disposable", NULL},
     {"extends", "implements", NULL},
     3},
    /* ── error boundary component ────────────────────────────────── */
    {CBM_LANG_TSX,
     "ErrorBoundary.tsx",
     "import { Component, ReactNode, ErrorInfo } from 'react';\n"
     "class ErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean }> { "
     "componentDidCatch(err: Error, info: ErrorInfo) {} render() { return null; } }",
     "ErrorBoundary",
     {"Component", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── class extending custom hook class ───────────────────────── */
    {CBM_LANG_TSX,
     "hook.tsx",
     "class AdvancedHook extends BaseHook implements IHook { use() {} }",
     "AdvancedHook",
     {"BaseHook", "IHook", NULL},
     {"extends", "implements", NULL},
     2},
};

TEST(inherit_tsx) {
    RUN_CASES(tsx_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * PHP  (expected: RED — base_classes never populated)
 *
 * Bug: the PHP extractor does not populate base_classes for `extends`
 * or `implements`.  All rows assert the class is found and base_classes
 * is non-NULL, which fails immediately.
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t php_cases[] = {
    /* ── extends single class ────────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Dog extends Animal { public function bark(): void {} }",
     "Dog",
     {"Animal", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── implements single interface ─────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Repo implements IRepository { public function save($x) {} }",
     "Repo",
     {"IRepository", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── extends + implements ────────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Service extends BaseService implements IService { public function run() {} }",
     "Service",
     {"BaseService", "IService", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── implements two interfaces ───────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Codec implements Encoder, Decoder { "
     "public function encode($x) {} public function decode($x) {} }",
     "Codec",
     {"Encoder", "Decoder", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── extends + implements three ──────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Mega extends Base implements A, B, C { }",
     "Mega",
     {"Base", "A", "B", "C", NULL},
     {"extends", "implements", NULL},
     4},
    /* ── abstract class ──────────────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nabstract class AbstractSvc extends BaseService { abstract public function run(): "
     "void; }",
     "AbstractSvc",
     {"BaseService", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── final class ─────────────────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nfinal class FinalSvc extends BaseSvc { }",
     "FinalSvc",
     {"BaseSvc", NULL},
     {"extends", "implements", NULL},
     1},
    /* ── namespace-qualified base ────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nnamespace App\\Services;\nuse App\\Base\\BaseService;\n"
     "class OrderService extends BaseService implements IOrderService { }",
     "OrderService",
     {"BaseService", "IOrderService", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── interface extends interface ─────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\ninterface IAdvanced extends IBasic, IExtended { public function doAdvanced(): void; }",
     "IAdvanced",
     {"IBasic", "IExtended", NULL},
     {"extends", NULL},
     2},
    /* ── backslash-prefixed fully qualified base ──────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass MyException extends \\RuntimeException { }",
     "MyException",
     {"RuntimeException", NULL},
     {"extends", "\\\\", NULL},
     1},
    /* ── class extending Exception ───────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass AppException extends \\Exception { "
     "public function __construct(string $msg) { parent::__construct($msg); } }",
     "AppException",
     {"Exception", NULL},
     {"extends", NULL},
     1},
    /* ── implements four interfaces ──────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass Adapter implements IA, IB, IC, ID { "
     "public function a() {} public function b() {} "
     "public function c() {} public function d() {} }",
     "Adapter",
     {"IA", "IB", "IC", "ID", NULL},
     {"implements", NULL},
     4},
    /* ── trait-using class also extends ──────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\ntrait Logging { public function log() {} }\n"
     "class MyService extends BaseService { use Logging; }",
     "MyService",
     {"BaseService", NULL},
     {"extends", NULL},
     1},
    /* ── nested namespace with extends ───────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nnamespace App\\Http\\Controllers;\n"
     "class UserController extends Controller { }",
     "UserController",
     {"Controller", NULL},
     {"extends", NULL},
     1},
    /* ── multiple namespaced interfaces ──────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nuse Psr\\Http\\Server\\RequestHandlerInterface;\n"
     "use Psr\\Http\\Message\\ResponseInterface;\n"
     "class PsrHandler extends BaseHandler implements RequestHandlerInterface { "
     "public function handle($req): ResponseInterface { return null; } }",
     "PsrHandler",
     {"BaseHandler", "RequestHandlerInterface", NULL},
     {"extends", "implements", NULL},
     2},
    /* ── readonly class (PHP 8.2) ────────────────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nreadonly class ValueObject extends BaseValue { public function __construct("
     "public readonly string $value) {} }",
     "ValueObject",
     {"BaseValue", NULL},
     {"extends", NULL},
     1},
    /* ── enum implements interface (PHP 8.1) ─────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\ninterface HasLabel { public function label(): string; }\n"
     "enum Status: string implements HasLabel { "
     "case Active = 'active'; public function label(): string { return $this->value; } }",
     "Status",
     {"HasLabel", NULL},
     {"implements", NULL},
     1},
    /* ── extends with constructor promotion ──────────────────────── */
    {CBM_LANG_PHP,
     "Svc.php",
     "<?php\nclass PromotedDto extends BaseDto { "
     "public function __construct(public readonly string $name) { parent::__construct(); } }",
     "PromotedDto",
     {"BaseDto", NULL},
     {"extends", NULL},
     1},
};

TEST(inherit_php) {
    RUN_CASES(php_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * KOTLIN  (expected: RED — `:` supertype syntax not parsed)
 *
 * Bug: the Kotlin extractor does not parse the `: SuperType` syntax,
 * leaving base_classes empty for all Kotlin classes with supertypes.
 * ═══════════════════════════════════════════════════════════════════ */

static const inherit_case_t kotlin_cases[] = {
    /* ── extends single class ────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class Animal(val name: String)\nclass Dog(name: String) : Animal(name)",
     "Dog",
     {"Animal", NULL},
     {":", NULL},
     1},
    /* ── implements single interface ─────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "interface IRepo { fun save(x: Any) }\nclass Repo : IRepo { override fun save(x: Any) {} }",
     "Repo",
     {"IRepo", NULL},
     {":", NULL},
     1},
    /* ── extends + implements ────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class BaseService\ninterface IService { fun run() }\n"
     "class Service : BaseService(), IService { override fun run() {} }",
     "Service",
     {"BaseService", "IService", NULL},
     {":", NULL},
     2},
    /* ── implements two interfaces ───────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "interface Encoder { fun encode(x: Any): String }\n"
     "interface Decoder { fun decode(s: String): Any }\n"
     "class Codec : Encoder, Decoder { "
     "override fun encode(x: Any) = x.toString() "
     "override fun decode(s: String): Any = s }",
     "Codec",
     {"Encoder", "Decoder", NULL},
     {":", NULL},
     2},
    /* ── abstract class ──────────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "abstract class AbstractSvc : BaseService() { abstract fun run() }",
     "AbstractSvc",
     {"BaseService", NULL},
     {":", NULL},
     1},
    /* ── data class with base ────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class BaseEntity(val id: Long)\n"
     "data class User(val name: String, override val id: Long) : BaseEntity(id)",
     "User",
     {"BaseEntity", NULL},
     {":", NULL},
     1},
    /* ── sealed class with base ──────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "sealed class Result\nclass Success(val value: Any) : Result()\nclass Failure(val err: "
     "Throwable) : Result()",
     "Success",
     {"Result", NULL},
     {":", NULL},
     1},
    /* ── object (singleton) implementing interface ───────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "interface Logger { fun log(msg: String) }\nobject ConsoleLogger : Logger { override fun "
     "log(msg: String) = println(msg) }",
     "ConsoleLogger",
     {"Logger", NULL},
     {":", NULL},
     1},
    /* ── generic class with bound ────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class Container<T>\nclass Box<T : Comparable<T>> : Container<T>()",
     "Box",
     {"Container", NULL},
     {":", NULL},
     1},
    /* ── companion object (no base — sanity check, no crash) ─────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "class MyClass { companion object : Factory<MyClass>() { override fun create() = MyClass() } "
     "}",
     "MyClass",
     {NULL}, /* outer class has no base; we just want no crash */
     {NULL},
     0},
    /* ── enum class implementing interface ───────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "interface Displayable { fun display(): String }\n"
     "enum class Status : Displayable { OPEN, CLOSED; override fun display() = name }",
     "Status",
     {"Displayable", NULL},
     {":", NULL},
     1},
    /* ── class extending Exception ───────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "class AppException(message: String) : Exception(message)",
     "AppException",
     {"Exception", NULL},
     {":", NULL},
     1},
    /* ── implements Comparable ───────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "class Version(val major: Int, val minor: Int) : Comparable<Version> { "
     "override fun compareTo(other: Version) = compareValuesBy(this, other, { it.major }, { "
     "it.minor }) }",
     "Version",
     {"Comparable", NULL},
     {":", NULL},
     1},
    /* ── open class hierarchy ────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class Vehicle(val speed: Int)\nopen class MotorVehicle(speed: Int) : Vehicle(speed)\n"
     "class Car(speed: Int) : MotorVehicle(speed)",
     "Car",
     {"MotorVehicle", NULL},
     {":", NULL},
     1},
    /* ── inner class with base ───────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class BaseNode\nclass Tree { inner class Node : BaseNode() { var value: Int = 0 } }",
     "Node",
     {"BaseNode", NULL},
     {":", NULL},
     1},
    /* ── fun interface (SAM) ─────────────────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "fun interface Transformer { fun transform(input: String): String }\n"
     "class UpperTransformer : Transformer { override fun transform(input: String) = "
     "input.uppercase() }",
     "UpperTransformer",
     {"Transformer", NULL},
     {":", NULL},
     1},
    /* ── three bases (base + two interfaces) ─────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class BaseWorker\ninterface Runnable { fun run() }\ninterface Stoppable { fun stop() }\n"
     "class Worker : BaseWorker(), Runnable, Stoppable { override fun run() {} override fun stop() "
     "{} }",
     "Worker",
     {"BaseWorker", "Runnable", "Stoppable", NULL},
     {":", NULL},
     3},
    /* ── annotation class with base ─────────────────────────────────*/
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "annotation class MyAnnotation\nclass AnnotatedClass : BaseClass() { @MyAnnotation fun "
     "doWork() {} }",
     "AnnotatedClass",
     {"BaseClass", NULL},
     {":", NULL},
     1},
    /* ── value class (inline, Kotlin 1.5+) ──────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "interface Printable { fun print() }\n"
     "@JvmInline value class UserId(val id: String) : Printable { override fun print() = "
     "println(id) }",
     "UserId",
     {"Printable", NULL},
     {":", NULL},
     1},
    /* ── class with type-projected bound ─────────────────────────── */
    {CBM_LANG_KOTLIN,
     "Svc.kt",
     "open class Repository<T : Any>\nclass UserRepository : Repository<User>()",
     "UserRepository",
     {"Repository", NULL},
     {":", NULL},
     1},
};

TEST(inherit_kotlin) {
    RUN_CASES(kotlin_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * RUST — impl Trait for Struct  (expected: GREEN — regression guards)
 *
 * Rust does not use `base_classes` on the Struct def.  Instead the
 * extractor populates CBMFileResult.impl_traits (CBMImplTrait array)
 * with {trait_name, struct_name} pairs.
 * ═══════════════════════════════════════════════════════════════════ */

/* Rust helper: check impl_traits array. */
static int rust_has_impl(CBMFileResult *r, const char *trait_name, const char *struct_name) {
    for (int i = 0; i < r->impl_traits.count; i++) {
        CBMImplTrait *it = &r->impl_traits.items[i];
        if (it->trait_name && strcmp(it->trait_name, trait_name) == 0 && it->struct_name &&
            strcmp(it->struct_name, struct_name) == 0)
            return 1;
    }
    return 0;
}

typedef struct {
    const char *src;
    const char *trait_name;
    const char *struct_name;
} rust_impl_case_t;

static int run_rust_impl(const rust_impl_case_t *tc) {
    CBMFileResult *r = cbm_extract_file(tc->src, (int)strlen(tc->src), CBM_LANG_RUST, "t", "lib.rs",
                                        0, NULL, NULL);
    if (!r) {
        printf("  FAIL  [impl %s for %s] extract returned NULL\n", tc->trait_name, tc->struct_name);
        return 1;
    }
    int found = rust_has_impl(r, tc->trait_name, tc->struct_name);
    if (!found) {
        printf("  FAIL  [impl %s for %s] not found in impl_traits\n", tc->trait_name,
               tc->struct_name);
        cbm_free_result(r);
        return 1;
    }
    cbm_free_result(r);
    return 0;
}

#define RUN_RUST_CASES(table)                                                    \
    do {                                                                         \
        int _fail = 0;                                                           \
        int _n = (int)(sizeof(table) / sizeof(table[0]));                        \
        for (int _i = 0; _i < _n; _i++) {                                        \
            if (run_rust_impl(&table[_i]) != 0)                                  \
                _fail++;                                                         \
        }                                                                        \
        if (_fail > 0) {                                                         \
            printf("  FAIL  %d / %d Rust impl cases above failed\n", _fail, _n); \
            return 1;                                                            \
        }                                                                        \
    } while (0)

static const rust_impl_case_t rust_cases[] = {
    /* ── basic trait impl ───────────────────────────────────────── */
    {"trait Greet { fn greet(&self) -> String; }\n"
     "struct Dog;\nimpl Greet for Dog { fn greet(&self) -> String { \"Woof\".into() } }",
     "Greet", "Dog"},
    /* ── Display trait ───────────────────────────────────────────── */
    {"use std::fmt;\nstruct Point { x: f64, y: f64 }\n"
     "impl fmt::Display for Point { fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { "
     "write!(f, \"({},{})\", self.x, self.y) } }",
     "fmt::Display", "Point"},
    /* ── Iterator trait ──────────────────────────────────────────── */
    {"struct Counter { count: u32 }\n"
     "impl Iterator for Counter { type Item = u32; fn next(&mut self) -> Option<u32> { if "
     "self.count < 5 { self.count += 1; Some(self.count) } else { None } } }",
     "Iterator", "Counter"},
    /* ── From trait ──────────────────────────────────────────────── */
    {"struct Meters(f64);\nstruct Feet(f64);\n"
     "impl From<Feet> for Meters { fn from(f: Feet) -> Self { Meters(f.0 * 0.3048) } }",
     "From", "Meters"},
    /* ── Default trait ───────────────────────────────────────────── */
    {"struct Config { debug: bool, level: u32 }\n"
     "impl Default for Config { fn default() -> Self { Config { debug: false, level: 0 } } }",
     "Default", "Config"},
    /* ── PartialEq + Eq ──────────────────────────────────────────── */
    {"struct Id(u64);\nimpl PartialEq for Id { fn eq(&self, other: &Self) -> bool { self.0 == "
     "other.0 } }\nimpl Eq for Id {}",
     "PartialEq", "Id"},
    /* ── Clone + Copy ────────────────────────────────────────────── */
    {"#[derive(Copy)]\nstruct Tag(u8);\nimpl Clone for Tag { fn clone(&self) -> Self { *self } }",
     "Clone", "Tag"},
    /* ── custom trait for generic struct ─────────────────────────── */
    {"trait Serializable { fn serialize(&self) -> Vec<u8>; }\n"
     "struct Buffer<T> { data: Vec<T> }\n"
     "impl<T: Clone> Serializable for Buffer<T> { fn serialize(&self) -> Vec<u8> { vec![] } }",
     "Serializable", "Buffer"},
    /* ── Drop trait ──────────────────────────────────────────────── */
    {"struct Resource { name: String }\n"
     "impl Drop for Resource { fn drop(&mut self) { println!(\"drop {}\", self.name); } }",
     "Drop", "Resource"},
    /* ── crate-qualified trait ───────────────────────────────────── */
    {"mod io { pub trait Write { fn write(&mut self, buf: &[u8]) -> usize; } }\n"
     "struct Socket;\nimpl io::Write for Socket { fn write(&mut self, buf: &[u8]) -> usize { "
     "buf.len() } }",
     "io::Write", "Socket"},
    /* ── trait impl with associated type ────────────────────────── */
    {"trait Producer { type Item; fn produce(&self) -> Self::Item; }\n"
     "struct Factory;\nimpl Producer for Factory { type Item = u32; fn produce(&self) -> u32 { 42 "
     "} }",
     "Producer", "Factory"},
    /* ── Error trait ─────────────────────────────────────────────── */
    {"use std::fmt;\n"
     "#[derive(Debug)]\nstruct AppError { msg: String }\n"
     "impl fmt::Display for AppError { fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { "
     "write!(f, \"{}\", self.msg) } }\n"
     "impl std::error::Error for AppError {}",
     "std::error::Error", "AppError"},
    /* ── Ord trait for newtype ────────────────────────────────────── */
    {"use std::cmp::Ordering;\n#[derive(Eq, PartialEq)]\nstruct Priority(u8);\n"
     "impl PartialOrd for Priority { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { "
     "Some(self.cmp(other)) } }\n"
     "impl Ord for Priority { fn cmp(&self, other: &Self) -> Ordering { self.0.cmp(&other.0) } }",
     "Ord", "Priority"},
    /* ── Index trait ─────────────────────────────────────────────── */
    {"use std::ops::Index;\nstruct Matrix { data: Vec<f64>, cols: usize }\n"
     "impl Index<(usize, usize)> for Matrix { type Output = f64; fn index(&self, idx: (usize, "
     "usize)) -> &f64 { &self.data[idx.0 * self.cols + idx.1] } }",
     "Index", "Matrix"},
    /* ── Add operator overload ───────────────────────────────────── */
    {"use std::ops::Add;\n#[derive(Clone, Copy)]\nstruct Vec2 { x: f32, y: f32 }\n"
     "impl Add for Vec2 { type Output = Vec2; fn add(self, rhs: Vec2) -> Vec2 { Vec2 { x: self.x + "
     "rhs.x, y: self.y + rhs.y } } }",
     "Add", "Vec2"},
    /* ── Send + Sync safety impls ─────────────────────────────────── */
    {"struct SharedState { data: std::sync::Mutex<Vec<u8>> }\n"
     "unsafe impl Send for SharedState {}\nunsafe impl Sync for SharedState {}",
     "Send", "SharedState"},
    /* ── Deref coercion ──────────────────────────────────────────── */
    {"use std::ops::Deref;\nstruct Wrapper<T>(T);\n"
     "impl<T> Deref for Wrapper<T> { type Target = T; fn deref(&self) -> &T { &self.0 } }",
     "Deref", "Wrapper"},
    /* ── AsRef trait ─────────────────────────────────────────────── */
    {"struct Path(String);\nimpl AsRef<str> for Path { fn as_ref(&self) -> &str { &self.0 } }",
     "AsRef", "Path"},
    /* ── Hash trait ──────────────────────────────────────────────── */
    {"use std::hash::{Hash, Hasher};\n#[derive(Eq, PartialEq)]\nstruct Key(String);\n"
     "impl Hash for Key { fn hash<H: Hasher>(&self, state: &mut H) { self.0.hash(state); } }",
     "Hash", "Key"},
    /* ── serde Serialize / Deserialize (external crate trait) ────── */
    {"trait Serialize { fn serialize(&self) -> String; }\n"
     "trait Deserialize { fn deserialize(s: &str) -> Self; }\n"
     "struct JsonRecord { id: u64 }\n"
     "impl Serialize for JsonRecord { fn serialize(&self) -> String { "
     "format!(\"{{\\\"id\\\":{}}}\", self.id) } }\n"
     "impl Deserialize for JsonRecord { fn deserialize(_s: &str) -> Self { JsonRecord { id: 0 } } "
     "}",
     "Serialize", "JsonRecord"},
};

TEST(inherit_rust_impls) {
    RUN_RUST_CASES(rust_cases);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════
 * SUITE declaration
 * ═══════════════════════════════════════════════════════════════════ */

SUITE(extraction_inheritance) {
    /* Languages expected GREEN (correct extractors — regression guards) */
    RUN_TEST(inherit_java);
    RUN_TEST(inherit_csharp);
    RUN_TEST(inherit_cpp);
    RUN_TEST(inherit_rust_impls);

    /* Languages expected RED (broken extractors — reproduce-first) */
    RUN_TEST(inherit_python); /* RED: identifier-node not matched in collect_bases_from_field */
    RUN_TEST(
        inherit_typescript); /* RED: keyword "extends"/"implements" captured instead of type name */
    RUN_TEST(inherit_tsx);   /* RED: same keyword-capture bug as TypeScript */
    RUN_TEST(inherit_php);   /* RED: base_classes never populated */
    RUN_TEST(inherit_kotlin); /* RED: `:` supertype syntax not parsed */
}
