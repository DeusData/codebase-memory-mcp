/*
 * test_doc_mentions_jvm.c — doc-comment references of Java and Kotlin
 * (internal/cbm/doclink_jvm.c, src/pipeline/doc_links_jvm.c).
 *
 * Extraction is tested on source text (tokens, scope blobs, hostile input);
 * resolution through the pipeline on fixture repositories, read back from
 * the published database: every family and reason of both languages, the
 * lookup order of each level by level with a decoy one level further out,
 * references across the two languages, test code, parse errors, the scope
 * changes an incremental run repairs file by file, incremental == full and
 * one worker == several.
 */
#include "test_framework.h"
#include "test_doc_mentions_helpers.h"

/* ── extraction: tokens ──────────────────────────────────────────── */

/* The token written as `raw`, or a stand-in that fails every check. */
static const CBMDocLink *jt_token(const CBMFileResult *r, const char *raw) {
    static const CBMDocLink none = {.raw = "", .syntax = CBM_DOCLINK_NONE};
    const CBMDocLink *l = dm_find_token(r, raw);
    return l ? l : &none;
}

TEST(doc_mentions_jvm_java_tokens) {
    const char *src =
        "package a;\n"                                                            /* 1 */
        "\n"                                                                      /* 2 */
        "/**\n"                                                                   /* 3 */
        " * See {@link Gadget} and {@linkplain Gadget#spin() spin it},\n"         /* 4 */
        " * {@link Helper#run(int,   String) label text}, {@value #MAX}.\n"       /* 5 */
        " * A tag over two lines {@link\n"                                        /* 6 */
        " * Wrapped#name}. {@code {@link InCode}} {@literal {@link InLiteral}}\n" /* 7 */
        " * <!-- {@link InComment} --> <![CDATA[ {@link InCdata} ]]>\n"           /* 8 */
        " * {@linkage NotATag} {@link Open\n"                                     /* 9 */
        " * @see Gadget#spin()\n"                                                 /* 10 */
        " * @see <a href=\"https://example.org/x\">site</a>\n"                    /* 11 */
        " * @see \"The Book\"\n"                                                  /* 12 */
        " * @throws IllegalStateException never\n"                                /* 13 */
        " * @exception a.CustomError worse\n"                                     /* 14 */
        " * @param <T> the type {@link T}\n"                                      /* 15 */
        " * @return {@value} nothing, {@value \"%x\" Helper#MASK}\n"              /* 16 */
        " */\n"                                                                   /* 17 */
        "public class Widget<T> {\n"                                              /* 18 */
        "    /** Max {@value}. {@link Widget#MAX} */\n"                           /* 19 */
        "    public static final int MAX = 10;\n"                                 /* 20 */
        "    /**\n"                                                               /* 21 */
        "     * <pre>{@code\n"                                                    /* 22 */
        "     * @Override {@link InSample}\n"                                     /* 23 */
        "     * }</pre> after {@link AfterSample}\n"                              /* 24 */
        "     */\n"                                                               /* 25 */
        "    void sample() {}\n"                                                  /* 26 */
        "}\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_JAVA, "a/Widget.java");
    ASSERT_NOT_NULL(r);
    /* every family, with the line the reference is written on */
    const CBMDocLink *t = jt_token(r, "Gadget");
    ASSERT_EQ(t->syntax, CBM_DOCLINK_JAVA_LINK);
    ASSERT_EQ(t->line, 4);
    ASSERT_EQ(t->def_line, 18);
    ASSERT_STR_EQ(t->source_qn, "p.a.Widget");
    ASSERT_EQ(dm_count_tokens(r, "Gadget#spin()"), 2);
    ASSERT_EQ(jt_token(r, "Gadget#spin()")->syntax, CBM_DOCLINK_JAVA_LINKPLAIN);
    /* the label is not part of the reference; blanks inside it are one */
    ASSERT_EQ(jt_token(r, "Helper#run(int, String)")->line, 5);
    ASSERT_EQ(jt_token(r, "#MAX")->syntax, CBM_DOCLINK_JAVA_VALUE);
    ASSERT_EQ(jt_token(r, "Wrapped#name")->line, 7);
    ASSERT_EQ(jt_token(r, "IllegalStateException")->syntax, CBM_DOCLINK_JAVA_THROWS);
    ASSERT_EQ(jt_token(r, "a.CustomError")->syntax, CBM_DOCLINK_JAVA_THROWS);
    ASSERT_EQ(jt_token(r, "a.CustomError")->line, 14);
    ASSERT_EQ(jt_token(r, "https://example.org/x")->syntax, CBM_DOCLINK_HREF);
    ASSERT_EQ(jt_token(r, "https://example.org/x")->line, 11);
    /* a format string is not the reference of {@value} */
    ASSERT_EQ(jt_token(r, "Helper#MASK")->syntax, CBM_DOCLINK_JAVA_VALUE);
    /* a reference to the definition's own type parameter is still a token:
     * the resolver, which knows the scope, calls it local */
    ASSERT_EQ(jt_token(r, "T")->line, 15);
    /* not references: code, literal, HTML comment, CDATA, another tag whose
     * name only starts like one, a quoted title, a bare {@value} */
    ASSERT_NULL(dm_find_token(r, "InCode"));
    ASSERT_NULL(dm_find_token(r, "InLiteral"));
    ASSERT_NULL(dm_find_token(r, "InComment"));
    ASSERT_NULL(dm_find_token(r, "InCdata"));
    ASSERT_NULL(dm_find_token(r, "NotATag"));
    ASSERT_NULL(dm_find_token(r, "\"The"));
    /* an inline tag that a block tag cuts off never closed: javadoc reports
     * it, and it links nothing */
    ASSERT_NULL(dm_find_token(r, "Open"));
    /* the field's doc is harvested once, on the Field (its package-level
     * Variable twin carries the same doc) */
    ASSERT_EQ(dm_count_tokens(r, "Widget#MAX"), 1);
    ASSERT_STR_EQ(jt_token(r, "Widget#MAX")->source_qn, "p.a.Widget.MAX");
    /* `@` at the start of a line inside {@code} is sample code, not a block
     * tag, and what follows inside the block is still code */
    ASSERT_NULL(dm_find_token(r, "InSample"));
    ASSERT_EQ(jt_token(r, "AfterSample")->line, 24);
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_jvm_kotlin_tokens) {
    const char *src =
        "package k\n"                                                                   /* 1 */
        "\n"                                                                            /* 2 */
        "/**\n"                                                                         /* 3 */
        " * A [Gadget], [the helper][Helper.run], [text](https://x.org), `[InCode]`.\n" /* 4 */
        " * ``double `tick` [InDouble]`` then [Outer.Inner] and [`quoted name`].\n"     /* 5 */
        " * [label]: https://example.org\n"                                             /* 6 */
        " * <!-- [InComment] --> ![image][ImgLabel] \\[Escaped]\n"                      /* 7 */
        " * Nested /* comment [StillDoc] */ goes on to [AfterNested].\n"                /* 8 */
        " *\n"                                                                          /* 9 */
        " *     [IndentedCode]\n"                                                       /* 10 */
        " *\n"                                                                          /* 11 */
        " * ```\n"                                                                      /* 12 */
        " * [Fenced]\n"                                                                 /* 13 */
        " * ```\n"                                                                      /* 14 */
        " * @param name the [name] of [T]\n"                                            /* 15 */
        " * @property size the size\n"                                                  /* 16 */
        " * @see Gadget.spin\n"                                                         /* 17 */
        " * @see https://example.org\n"                                                 /* 18 */
        " * @throws IllegalStateException never\n"                                      /* 19 */
        " * @exception CustomError worse\n"                                             /* 20 */
        " * @sample k.samples.widgetSample\n"                                           /* 21 */
        " * @return [Result] of [0] and [two words]\n"                                  /* 22 */
        " */\n"                                                                         /* 23 */
        "class Widget<T>(val name: String, size: Int)\n";                               /* 24 */
    CBMFileResult *r = dm_extract(src, CBM_LANG_KOTLIN, "k/Widget.kt");
    ASSERT_NOT_NULL(r);
    const CBMDocLink *t = jt_token(r, "Gadget");
    ASSERT_EQ(t->syntax, CBM_DOCLINK_KT_KDOC_LINK);
    ASSERT_EQ(t->line, 4);
    ASSERT_EQ(t->def_line, 24);
    ASSERT_STR_EQ(t->source_qn, "p.k.Widget.Widget");
    /* [text][Name]: the second bracket names the code */
    ASSERT_EQ(jt_token(r, "Helper.run")->syntax, CBM_DOCLINK_KT_KDOC_LINK);
    ASSERT_NULL(dm_find_token(r, "the helper"));
    ASSERT_EQ(jt_token(r, "Outer.Inner")->line, 5);
    ASSERT_EQ(jt_token(r, "`quoted name`")->line, 5);
    /* a block comment inside the doc is part of its text: Kotlin comments
     * nest, and the doc goes on after it */
    ASSERT_EQ(jt_token(r, "StillDoc")->line, 8);
    ASSERT_EQ(jt_token(r, "AfterNested")->line, 8);
    ASSERT_EQ(jt_token(r, "Gadget.spin")->syntax, CBM_DOCLINK_KT_SEE);
    ASSERT_EQ(jt_token(r, "IllegalStateException")->syntax, CBM_DOCLINK_KT_THROWS);
    ASSERT_EQ(jt_token(r, "CustomError")->syntax, CBM_DOCLINK_KT_THROWS);
    ASSERT_EQ(jt_token(r, "CustomError")->line, 20);
    ASSERT_EQ(jt_token(r, "k.samples.widgetSample")->syntax, CBM_DOCLINK_KT_SAMPLE);
    ASSERT_EQ(jt_token(r, "Result")->line, 22);
    /* own parameter and type parameter: tokens, local to the resolver */
    ASSERT_EQ(jt_token(r, "name")->line, 15);
    ASSERT_EQ(jt_token(r, "T")->line, 15);
    /* not references: a Markdown link, code spans, a link definition, an
     * HTML comment, an image's label, an escaped bracket, an indented and a
     * fenced code block, a URL, and brackets that hold no name */
    ASSERT_NULL(dm_find_token(r, "text"));
    ASSERT_NULL(dm_find_token(r, "InCode"));
    ASSERT_NULL(dm_find_token(r, "InDouble"));
    ASSERT_NULL(dm_find_token(r, "label"));
    ASSERT_NULL(dm_find_token(r, "InComment"));
    ASSERT_NULL(dm_find_token(r, "image"));
    ASSERT_NULL(dm_find_token(r, "ImgLabel"));
    ASSERT_NULL(dm_find_token(r, "Escaped"));
    ASSERT_NULL(dm_find_token(r, "IndentedCode"));
    ASSERT_NULL(dm_find_token(r, "Fenced"));
    ASSERT_NULL(dm_find_token(r, "https://example.org"));
    ASSERT_NULL(dm_find_token(r, "0"));
    ASSERT_NULL(dm_find_token(r, "two words"));
    ASSERT_NULL(dm_find_token(r, "size"));
    cbm_free_result(r);
    PASS();
}

/* ── extraction: hostile input ───────────────────────────────────── */

enum { JT_BIG = 200000, JT_MEGA = 1100000 };

/* Extract `head` + `count` copies of `unit` + `tail` as one doc comment on a
 * class; the result, or NULL when memory ran out. */
static CBMFileResult *jt_extract_repeated(CBMLanguage lang, const char *head, const char *unit,
                                          int count, const char *tail) {
    size_t ul = strlen(unit);
    size_t cap = strlen(head) + (ul * (size_t)count) + strlen(tail) + 64;
    char *src = malloc(cap);
    if (!src) {
        return NULL;
    }
    size_t w = (size_t)snprintf(src, cap, "/**\n * %s", head);
    for (int i = 0; i < count; i++) {
        memcpy(src + w, unit, ul);
        w += ul;
    }
    snprintf(src + w, cap - w, "%s\n */\n%s", tail,
             lang == CBM_LANG_JAVA ? "public class C {}\n" : "class C\n");
    CBMFileResult *r = dm_extract(src, lang, lang == CBM_LANG_JAVA ? "C.java" : "C.kt");
    free(src);
    return r;
}

TEST(doc_mentions_jvm_malformed_and_oversized) {
    /* a reference longer than any name (here: over a megabyte) is kept just
     * past the limit, which is how the resolver knows it was cut: it never
     * resolves a prefix */
    CBMFileResult *r = jt_extract_repeated(CBM_LANG_JAVA, "{@link ", "A", JT_MEGA, "}");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 1);
    ASSERT_GT(strlen(r->doc_links.items[0].raw), (size_t)CBM_DOCLINK_JVM_REF_MAX);
    ASSERT(strlen(r->doc_links.items[0].raw) < (size_t)CBM_DOCLINK_JVM_REF_MAX + 16);
    cbm_free_result(r);
    r = jt_extract_repeated(CBM_LANG_KOTLIN, "[", "A", JT_MEGA, "]");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 1);
    ASSERT_GT(strlen(r->doc_links.items[0].raw), (size_t)CBM_DOCLINK_JVM_REF_MAX);
    ASSERT(strlen(r->doc_links.items[0].raw) < (size_t)CBM_DOCLINK_JVM_REF_MAX + 16);
    cbm_free_result(r);
    /* tags that never close, nested deeper than any text nests */
    r = jt_extract_repeated(CBM_LANG_JAVA, "", "{@link A ", JT_BIG, "");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 0);
    cbm_free_result(r);
    /* ... and closed again: the innermost ones are references, no more than
     * the parser keeps open at once, and nothing overflows */
    r = jt_extract_repeated(CBM_LANG_JAVA, "", "{@link A ", 100, "}}}}}}}}}}");
    ASSERT_NOT_NULL(r);
    ASSERT(r->doc_links.count <= 10);
    cbm_free_result(r);
    /* brackets, backticks and comment openers by the hundred thousand on one
     * line: one pass, no token */
    r = jt_extract_repeated(CBM_LANG_KOTLIN, "", "[", JT_BIG, "");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 0);
    cbm_free_result(r);
    r = jt_extract_repeated(CBM_LANG_KOTLIN, "", "` `` ", JT_BIG, " [After]");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    r = jt_extract_repeated(CBM_LANG_KOTLIN, "[Before] ", "<!-- ", JT_BIG, " [Hidden]");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(dm_find_token(r, "Before"));
    ASSERT_NULL(dm_find_token(r, "Hidden")); /* the comment never ends */
    cbm_free_result(r);
    /* bytes that are no UTF-8, a NUL in the middle of the comment */
    const char bad[] = "/**\n * {@link Caf\xC3} {@link \xFF\xFE#x} [x\x80y]\n */\nclass C {}\n";
    r = dm_extract(bad, CBM_LANG_JAVA, "C.java");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    r = dm_extract(bad, CBM_LANG_KOTLIN, "C.kt");
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    const char nul[] = "/**\n * {@link Before} \0 {@link After}\n */\nclass C {}\n";
    r = cbm_extract_file(nul, (int)sizeof(nul) - 1, CBM_LANG_JAVA, "p", "C.java", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    r = cbm_extract_file(nul, (int)sizeof(nul) - 1, CBM_LANG_KOTLIN, "p", "C.kt", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_jvm_java_norm_type) {
    static const struct {
        const char *in;
        const char *want;
    } cases[] = {
        {"int", "int"},
        {"final String s", "String"},
        {"java.util.List<String>", "List"},
        {"Map.Entry<K, V>", "Entry"},
        {"Map<String, List<int[]>>[]", "Map[]"},
        {"String...", "String[]"},
        {"int[] xs", "int[]"},
        {"int xs[]", "int[]"},
        {"int [] xs", "int[]"},
        {"@NonNull String", "String"},
        {"@Size(max = 3) java.lang.String name", "String"},
        {"  T  ", "T"},
        {"", "?"},
        {"<>", "?"},
    };
    char out[64];
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cbm_doclink_java_norm_type(cases[i].in, strlen(cases[i].in), out, sizeof(out));
        if (strcmp(out, cases[i].want) != 0) {
            printf("  norm_type(\"%s\") = \"%s\", expected \"%s\"\n", cases[i].in, out,
                   cases[i].want);
        }
        ASSERT_STR_EQ(out, cases[i].want);
    }
    /* a name that does not fit is no type: never a truncated one, which
     * could equal another type's name */
    char small[8];
    cbm_doclink_java_norm_type("LongTypeName", strlen("LongTypeName"), small, sizeof(small));
    ASSERT_STR_EQ(small, "?");
    PASS();
}

/* ── extraction: scope blobs ─────────────────────────────────────── */

/* True when the scope blob has the record `line` (tab-separated, a whole
 * line). */
static bool jt_has_line(const char *scope, const char *line) {
    size_t n = strlen(line);
    for (const char *p = scope; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len == n && memcmp(p, line, n) == 0) {
            return true;
        }
        p = nl ? nl + 1 : NULL;
    }
    return false;
}

TEST(doc_mentions_jvm_java_scope_blob) {
    const char *src =
        "package com.acme.core;\n"                   /* 1 */
        "import com.acme.util.Helper;\n"             /* 2 */
        "import com.acme.util.*;\n"                  /* 3 */
        "import static com.acme.util.Helper.once;\n" /* 4 */
        "import static com.acme.util.Helper.*;\n"    /* 5 */
        "public class Widget<T, U extends Comparable<U>> extends Base implements Runnable {\n" /* 6
                                                                                                */
        "    public static final int MAX = 10;\n" /* 7 */
        "    private int size, other;\n"          /* 8 */
        "    public Widget() {}\n"                /* 9 */
        "    public Widget(int size) {}\n"        /* 10 */
        "    public <V extends Number> int size(V scale, java.util.List<String> xs, int... rest) "
        "{ return 0; }\n"                                                            /* 11 */
        "    public void run() {}\n"                                                 /* 12 */
        "    public static class Inner {\n"                                          /* 13 */
        "        public int deep;\n"                                                 /* 14 */
        "    }\n"                                                                    /* 15 */
        "    public enum Mode { FAST, SLOW; public Mode flip() { return this; } }\n" /* 16 */
        "    public record Point(int x, String y) { public Point { } }\n"            /* 17 */
        "    public interface Shape { int SIDES = 4; double area(); }\n"             /* 18 */
        "    public @interface Marker { String value() default \"\"; }\n"            /* 19 */
        "}\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_JAVA, "com/acme/core/Widget.java");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT(strncmp(s, CBM_DOCLINK_JVM_SCOPE_TAG "\n", strlen(CBM_DOCLINK_JVM_SCOPE_TAG) + 1) == 0);
    ASSERT(jt_has_line(s, "L\tj"));
    ASSERT(jt_has_line(s, "P\tcom.acme.core"));
    ASSERT(jt_has_line(s, "I\ts\tHelper\tcom.acme.util.Helper"));
    ASSERT(jt_has_line(s, "I\tw\t-\tcom.acme.util"));
    ASSERT(jt_has_line(s, "I\tm\tonce\tcom.acme.util.Helper"));
    ASSERT(jt_has_line(s, "I\tn\t-\tcom.acme.util.Helper"));
    /* a type: lines, kind, path, type parameters (a Java one with its first
     * bound, its erasure), supertypes, where its node is, whether it owns it */
    ASSERT(jt_has_line(s, "T\t6\t20\tc\tWidget\tT,U:Comparable\tBase|Runnable\t=\t1\t"));
    /* a field's node is named by the field; one named like a method of its
     * class has <Owner>.<name>#field; a second declarator has no definition */
    ASSERT(jt_has_line(s, "M\t7\tv\t\t0\tMAX\t\t-\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t8\tv\t\t0\tsize\t\t-\t\tWidget.size#field\t1\t"));
    ASSERT(jt_has_line(s, "M\t8\tv\t\t0\tother\t\t-\t\t-\t0\t"));
    /* constructors, overloads of one name: one node, owned by the last */
    ASSERT(jt_has_line(s, "M\t9\tc\tK\t0\tWidget\t\t\t\t=\t0\t"));
    ASSERT(jt_has_line(s, "M\t10\tc\tK\t0\tWidget\t\tint\t\t=\t1\t"));
    /* a signature: simple names, no type arguments, an ellipsis is an array */
    ASSERT(jt_has_line(s, "M\t11\tc\t\t0\tsize\tV:Number\tV|List|int[]\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "T\t13\t15\tc\tWidget.Inner\t\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t14\tv\t\t1\tdeep\t\t-\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "T\t16\t16\te\tWidget.Mode\t\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t16\tv\tn\t2\tFAST\t\t-\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t16\tc\t\t2\tflip\t\t\t\t=\t1\t"));
    /* a record: its components and its compact constructor (whose signature
     * is the components' types) are declared, and have no definition */
    ASSERT(jt_has_line(s, "T\t17\t17\tr\tWidget.Point\t\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t17\tv\tp\t3\tx\t\tint\t\t-\t0\t"));
    ASSERT(jt_has_line(s, "M\t17\tv\tp\t3\ty\t\tString\t\t-\t0\t"));
    ASSERT(jt_has_line(s, "M\t17\tc\tK\t3\tPoint\t\t*\t\t-\t0\t"));
    /* an interface constant and an annotation element: declared, no node */
    ASSERT(jt_has_line(s, "M\t18\tv\t\t4\tSIDES\t\t-\t\t-\t0\t"));
    ASSERT(jt_has_line(s, "M\t19\tc\t\t5\tvalue\t\t\t\t-\t0\t"));
    /* the persisted form: no line numbers, nothing else changed */
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT(jt_has_line(portable, "T\t0\t0\tc\tWidget\tT,U:Comparable\tBase|Runnable\t=\t1\t"));
    ASSERT(jt_has_line(portable, "M\t0\tc\tK\t0\tWidget\t\tint\t\t=\t1\t"));
    ASSERT(jt_has_line(portable, "I\ts\tHelper\tcom.acme.util.Helper"));
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_jvm_kotlin_scope_blob) {
    const char *src =
        "package com.acme.kt\n"                                                 /* 1 */
        "import com.acme.core.Gadget\n"                                         /* 2 */
        "import com.acme.far.Far as Farther\n"                                  /* 3 */
        "import com.acme.far.*\n"                                               /* 4 */
        "class Alias<T>(val delegate: T, name: String) : Base<T>(), Marker {\n" /* 5 */
        "    val bodyProp: Int = 1\n"                                           /* 6 */
        "    fun member(param: Int): Int {\n"                                   /* 7 */
        "        return param\n"                                                /* 8 */
        "    }\n"                                                               /* 9 */
        "    fun member(param: String): Int {\n"                                /* 10 */
        "        return 0\n"                                                    /* 11 */
        "    }\n"                                                               /* 12 */
        "    class Nested\n"                                                    /* 13 */
        "    companion object {\n"                                              /* 14 */
        "        val LIMIT = 3\n"                                               /* 15 */
        "    }\n"                                                               /* 16 */
        "    constructor(x: Int) : this(x as T, \"n\")\n"                       /* 17 */
        "}\n"                                                                   /* 18 */
        "fun <T : Base<*>> T.alias(name: String): Int {\n"                      /* 19 */
        "    return 1\n"                                                        /* 20 */
        "}\n"                                                                   /* 21 */
        "fun <T> T.anything(): Int {\n"                                         /* 22 */
        "    return 1\n"                                                        /* 23 */
        "}\n"                                                                   /* 24 */
        "val com.acme.far.Far.ext: Int get() = 1\n"                             /* 25 */
        "enum class Color { RED, GREEN }\n"                                     /* 26 */
        "typealias Name = com.acme.far.Far\n"                                   /* 27 */
        "typealias Handler = (Int) -> Unit\n";                                  /* 28 */
    CBMFileResult *r = dm_extract(src, CBM_LANG_KOTLIN, "com/acme/kt/Alias.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT(jt_has_line(s, "L\tk"));
    ASSERT(jt_has_line(s, "P\tcom.acme.kt"));
    ASSERT(jt_has_line(s, "I\ts\tGadget\tcom.acme.core.Gadget"));
    ASSERT(jt_has_line(s, "I\ts\tFarther\tcom.acme.far.Far")); /* an import alias */
    ASSERT(jt_has_line(s, "I\tw\t-\tcom.acme.far"));
    /* a class: its constructor's parameter names are its own (the last
     * field; the persisted form drops them) */
    ASSERT(jt_has_line(s, "T\t5\t18\tc\tAlias\tT\tBase|Marker\t=\t1\tdelegate,name"));
    /* a `val` constructor parameter is a property without a definition */
    ASSERT(jt_has_line(s, "M\t5\tv\tp\t0\tdelegate\t\t-\t\t-\t0\t"));
    /* a class-body property: its node is named below the FILE's module */
    ASSERT(jt_has_line(s, "M\t6\tv\t\t0\tbodyProp\t\t-\t\tbodyProp\t1\t"));
    ASSERT(jt_has_line(s, "M\t7\tc\t\t0\tmember\t\tInt\t\t=\t0\tparam"));
    ASSERT(jt_has_line(s, "M\t10\tc\t\t0\tmember\t\tString\t\t=\t1\tparam"));
    /* a nested type's node is flat: named below the file's module */
    ASSERT(jt_has_line(s, "T\t13\t13\tc\tAlias.Nested\t\t\tNested\t1\t"));
    /* an unnamed companion and its members: declared, no definitions */
    ASSERT(jt_has_line(s, "T\t14\t16\tC\tAlias.Companion\t\t\t-\t0\t"));
    ASSERT(jt_has_line(s, "M\t15\tv\t\t2\tLIMIT\t\t-\t\t-\t0\t"));
    ASSERT(jt_has_line(s, "M\t17\tc\tK\t0\tAlias\t\tInt\t\t=\t1\tx"));
    /* an extension: its receiver as written; a type parameter stands for its
     * bound, one without a bound for any type */
    ASSERT(jt_has_line(s, "M\t19\tc\t\t-\talias\tT\tR=T|String\tBase\t=\t1\tname"));
    ASSERT(jt_has_line(s, "M\t22\tc\t\t-\tanything\tT\tR=T\t?\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t25\tv\t\t-\text\t\tR=com.acme.far.Far\tcom.acme.far.Far\t=\t1\t"));
    ASSERT(jt_has_line(s, "T\t26\t26\te\tColor\t\t\t=\t1\t"));
    ASSERT(jt_has_line(s, "M\t26\tv\tn\t3\tRED\t\t-\t\t-\t0\t"));
    /* a type alias: the type it names stands where a supertype does */
    ASSERT(jt_has_line(s, "T\t27\t27\tt\tName\t\tcom.acme.far.Far\t=\t1\t"));
    ASSERT(jt_has_line(s, "T\t28\t28\tt\tHandler\t\t?\t=\t1\t"));
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT(jt_has_line(portable, "T\t0\t0\tc\tAlias\tT\tBase|Marker\t=\t1\t"));
    ASSERT(jt_has_line(portable, "M\t0\tc\t\t0\tmember\t\tString\t\t=\t1\t"));
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);
    PASS();
}

/* ── the published database ──────────────────────────────────────── */

/* A fixture repository in a temporary directory, and its index. */
typedef struct {
    char tmp[256];
    char repo[400];
    char db[512];
} jt_repo_t;

static bool jt_begin(jt_repo_t *r, const char *tag) {
    snprintf(r->tmp, sizeof(r->tmp), "/tmp/cbm_dm_jvm_%s_XXXXXX", tag);
    if (!cbm_mkdtemp(r->tmp)) {
        return false;
    }
    snprintf(r->repo, sizeof(r->repo), "%s/repo", r->tmp);
    snprintf(r->db, sizeof(r->db), "%s/index.db", r->tmp);
    return true;
}

static void jt_write(const jt_repo_t *r, const char *rel, const char *content) {
    th_write_file(TH_PATH(r->repo, rel), content);
}

static void jt_end(jt_repo_t *r) {
    dm_unlink_db(r->db);
    th_rmtree(r->tmp);
}

/* Number of MENTIONS edges between the definitions whose qualified names end
 * with these paths; their properties are left in jt_props. */
static char jt_props[512];

static int jt_edge(const char *db, const char *src, const char *tgt) {
    int n = 0;
    dm_edge(db, src, tgt, jt_props, sizeof(jt_props), &n);
    return n;
}

/* Reason of the unresolved row with this raw text in this file; "" when the
 * reference has no row. */
static const char *jt_reason(const char *db, const char *rel, const char *raw) {
    static char reason[64];
    dm_row(db, rel, raw, reason, sizeof(reason), NULL, 0);
    return reason;
}

/* ── Java: every family, every reason ────────────────────────────── */

#define JT_GADGET                                    \
    "package a;\n"                                   \
    "public class Gadget {\n"                        \
    "    public int count;\n"                        \
    "    public static final String NAME = \"g\";\n" \
    "    public void spin() {}\n"                    \
    "    public void spin(int times) {}\n"           \
    "    public void stop() {}\n"                    \
    "}\n"                                            \
    "class Gr\xC3\xB6\xC3\x9F"                       \
    "e {\n"                                          \
    "    public int l\xC3\xA4nge = 1;\n"             \
    "}\n"

#define JT_HELPER                               \
    "package b;\n"                              \
    "public class Helper {\n"                   \
    "    public static final int MASK = 255;\n" \
    "    public void run(int n, String s) {}\n" \
    "}\n"

#define JT_SHAPES                                                     \
    "package a;\n"                                                    \
    "public class Shapes {\n"                                         \
    "    public record Point(int x, int y) {}\n"                      \
    "    public interface Shape { int SIDES = 4; double area(); }\n"  \
    "    public @interface Marker { String value() default \"\"; }\n" \
    "    public static class Sized {\n"                               \
    "        private int size;\n"                                     \
    "        public int size() { return size; }\n"                    \
    "    }\n"                                                         \
    "    public static class Made {\n"                                \
    "        public Made(int n) {}\n"                                 \
    "    }\n"                                                         \
    "}\n"

#define JT_WIDGET                                                                                          \
    "package a;\n"             /* 1 */                                                                     \
    "\n"                       /* 2 */                                                                     \
    "import b.Helper;\n"       /* 3 */                                                                     \
    "import a.env.Config;\n"   /* 4 */                                                                     \
    "import java.util.List;\n" /* 5 */                                                                     \
    "import b.Generated;\n"    /* 6 */                                                                     \
    "/**\n"                    /* 7 */                                                                     \
    " * {@link Gadget} {@linkplain Gadget#spin(int) spin} {@link Helper#run(int, String)}\n" /* 8          \
                                                                                              */           \
    " * {@value Helper#MASK} {@link List} {@link String} {@link T} {@link Gadget#spin}\n" /* 9 */          \
    " * {@link NoSuchType} {@link Gadget#nope} {@link GadgetTests} {@link Config}\n"      /* 10 */         \
    " * {@link a.env.Other} {@link b} {@link Shapes.Point#x} {@link Shapes.Shape#SIDES}\n" /* 11           \
                                                                                            */             \
    " * {@link Shapes.Marker#value()} {@link Shapes.Sized#size} {@link Shapes.Sized#size()}\n" /* 12 */    \
    " * {@link Gadget#Gadget()} {@link Shapes.Made#Made(int)} {@link Shapes.Made#Made(String)}\n" /* 13 */ \
    " * {@link Gadget#a#b} {@link Gadget#spin(int} {@link Gr\xC3\xB6\xC3\x9F"                              \
    "e#l\xC3\xA4nge}\n" /* 14 */                                                                           \
    " * {@link org.outside.Thing} {@link b.Missing} {@link Gadget.stop} {@link Generated}\n" /* 15         \
                                                                                              */           \
    " * {@link Gadget#spin(int, int)} {@link Gadget#spin(long)} {@link Shapes#Sized}\n" /* 16 */           \
    " * {@value Gadget#stop}\n"                                                         /* 17 */           \
    " * @see Gadget#count\n"                                                            /* 18 */           \
    " * @see <a href=\"https://example.org\">site</a>\n"                                /* 19 */           \
    " * @throws CustomError never\n"                                                    /* 20 */           \
    " * @throws java.io.IOException bad\n"                                              /* 21 */           \
    " */\n"                                                                             /* 22 */           \
    "public class Widget<T> {\n"                                                        /* 23 */           \
    "}\n"

TEST(doc_mentions_jvm_java_families_and_reasons) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfam"));
    jt_write(&r, "src/main/java/a/Gadget.java", JT_GADGET);
    jt_write(&r, "src/main/java/a/CustomError.java",
             "package a;\npublic class CustomError extends RuntimeException {}\n");
    jt_write(&r, "src/main/java/b/Helper.java", JT_HELPER);
    jt_write(&r, "src/main/java/a/Shapes.java", JT_SHAPES);
    jt_write(&r, "src/main/java/a/Widget.java", JT_WIDGET);
    /* a real package in a directory the indexer skips by name */
    jt_write(&r, "src/main/java/a/env/Config.java", "package a.env;\npublic class Config {}\n");
    jt_write(&r, "src/test/java/a/GadgetTests.java",
             "package a;\n/** Tests {@link Gadget}. */\npublic class GadgetTests {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    const char *w = "src/main/java/a/Widget.java";

    /* link: a type of the file's package, unique at its first level */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Gadget"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"via\":\"doc_comment\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"link\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"unique\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"line\":8"));
    /* linkplain, with a signature that selects one overload (overloads of a
     * type share a node) */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Gadget.spin"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"linkplain\""));
    /* ... and nothing is relaxed: another parameter count, another parameter
     * type, a nested type named as a member and a method named as a value
     * bind nothing (that edge counts the one reference) */
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1"));
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#spin(int, int)"), "missing");
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#spin(long)"), "missing");
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes#Sized"), "missing");
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Shapes.Sized"), 0);
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#stop"), "missing");
    /* a type named by a single import: exact */
    ASSERT_EQ(jt_edge(db, "a.Widget", "b.Helper.run"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    /* value: a field, whose node is named by its declarator */
    ASSERT_EQ(jt_edge(db, "a.Widget", "b.Helper.MASK"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"value\""));
    /* see */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Gadget.count"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"see\""));
    /* throws */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.CustomError"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"throws\""));

    /* external: a single import of a package the repository does not declare,
     * java.lang, a qualified name of such a package, a URL */
    ASSERT_STR_EQ(jt_reason(db, w, "List"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "String"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "java.io.IOException"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "org.outside.Thing"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "https://example.org"), "external");
    /* local: the class's own type parameter is neither an edge nor a row */
    ASSERT_STR_EQ(jt_reason(db, w, "T"), "");
    /* ambiguous: an overload group named without a parameter list */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#spin"), "ambiguous");
    /* missing: nothing of that name anywhere; a member the type does not
     * have; a type the repository's own package does not declare */
    ASSERT_STR_EQ(jt_reason(db, w, "NoSuchType"), "missing");
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#nope"), "missing");
    ASSERT_STR_EQ(jt_reason(db, w, "b.Missing"), "missing");
    /* `Type.member` is no Javadoc reference (it wants a '#'): no binding of
     * the method that happens to have that name */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget.stop"), "missing");
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Gadget.stop"), 0);
    /* test_only_target: product code names a declaration only tests hold */
    ASSERT_STR_EQ(jt_reason(db, w, "GadgetTests"), "test_only_target");
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.GadgetTests"), 0);
    /* ... while test code binds product code */
    ASSERT_EQ(jt_edge(db, "a.GadgetTests", "a.Gadget"), 1);
    /* not_indexed: a package of the repository in a directory discovery
     * skips, named by an import and by its qualified name */
    ASSERT_STR_EQ(jt_reason(db, w, "Config"), "not_indexed");
    ASSERT_STR_EQ(jt_reason(db, w, "a.env.Other"), "not_indexed");
    /* graph_gap: a package (no node), a record component, an interface
     * constant and an annotation element (declared, no definition), a
     * constructor the class has without declaring it */
    ASSERT_STR_EQ(jt_reason(db, w, "b"), "graph_gap");
    /* ... and a type the file imports from a package of the repository that
     * no indexed file declares (generated code): the import vouches for it,
     * where the same name written out without one is missing (b.Missing) */
    ASSERT_STR_EQ(jt_reason(db, w, "Generated"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes.Point#x"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes.Shape#SIDES"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes.Marker#value()"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#Gadget()"), "graph_gap");
    /* a field named like its accessor has a node of its own
     * (<Owner>.<name>#field): the field reference binds it */
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes.Sized#size"), "");
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Shapes.Sized.size#field"), 1);
    /* ... while the accessor itself, named as a method, binds */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Shapes.Sized.size"), 1);
    /* a constructor reference: the declared one binds, another signature is
     * missing (never the constructor that is there) */
    ASSERT_EQ(jt_edge(db, "a.Widget", "a.Shapes.Made.Made"), 1);
    ASSERT_STR_EQ(jt_reason(db, w, "Shapes.Made#Made(String)"), "missing");
    /* unparseable: two '#', a parameter list that does not close */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#a#b"), "unparseable");
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget#spin(int"), "unparseable");
    /* identifiers are Unicode */
    ASSERT_EQ(jt_edge(db, "a.Widget", "l\xC3\xA4nge"), 1);
    jt_end(&r);
    PASS();
}

/* An oversized reference is unparseable in both languages: the resolver
 * never resolves the prefix that was kept. */
TEST(doc_mentions_jvm_oversized_reference) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "big"));
    enum { JT_LONG = 3000 };
    char *java = malloc(JT_LONG + 256);
    char *kotlin = malloc(JT_LONG + 256);
    ASSERT_NOT_NULL(java);
    ASSERT_NOT_NULL(kotlin);
    size_t w = (size_t)snprintf(java, 128, "package a;\n/** {@link Target");
    memset(java + w, 'x', JT_LONG);
    snprintf(java + w + JT_LONG, 100, "} */\npublic class Big {}\n");
    w = (size_t)snprintf(kotlin, 128, "package a\n/** [Target");
    memset(kotlin + w, 'x', JT_LONG);
    snprintf(kotlin + w + JT_LONG, 100, "] */\nclass BigK\n");
    jt_write(&r, "src/main/java/a/Big.java", java);
    jt_write(&r, "src/main/kotlin/a/BigK.kt", kotlin);
    /* the prefix that survives the cut names a real class */
    jt_write(&r, "src/main/java/a/Target.java", "package a;\npublic class Target {}\n");
    free(java);
    free(kotlin);
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), 0);
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = "
                             "'unparseable'"),
              2);
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM doc_link_unresolved"), 2);
    jt_end(&r);
    PASS();
}

/* ── Java: the lookup order, level by level ──────────────────────── */

/* One class per boundary of javac's order for a simple type name. Each binds
 * at the nearer level although the same name exists one level further out. */
TEST(doc_mentions_jvm_java_type_levels) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jlvl"));
    jt_write(&r, "src/main/java/far/Thing.java",
             "package far;\npublic class Thing { public void go() {} }\n");
    jt_write(&r, "src/main/java/far/Tool.java", "package far;\npublic class Tool {}\n");
    jt_write(&r, "src/main/java/far/Dup.java", "package far;\npublic class Dup {}\n");
    jt_write(&r, "src/main/java/far/Iterable.java", "package far;\npublic class Iterable {}\n");
    jt_write(&r, "src/main/java/near/Dup.java", "package near;\npublic class Dup {}\n");
    jt_write(&r, "src/main/java/lv/Thing.java", "package lv;\npublic class Thing {}\n");
    jt_write(&r, "src/main/java/lv/Tool.java", "package lv;\npublic class Tool {}\n");
    /* 1. a method's own type variable, before a member type of its class */
    jt_write(&r, "src/main/java/lv/B1.java",
             "package lv;\n"
             "public class B1 {\n"
             "    public static class T {}\n"
             "    /** {@link T} */\n"
             "    public <T> void m(T x) {}\n"
             "}\n");
    /* 2. a class's own member type, before its type variable of that name */
    jt_write(&r, "src/main/java/lv/B2.java",
             "package lv;\n"
             "public class B2<Part> {\n"
             "    public static class Part {}\n"
             "    /** {@link Part} */\n"
             "    public void m() {}\n"
             "}\n");
    /* 3. a class's type variable, before a member type it inherits */
    jt_write(&r, "src/main/java/lv/B3.java",
             "package lv;\n"
             "public class B3<Item> extends B3Base {\n"
             "    /** {@link Item} */\n"
             "    public void m() {}\n"
             "}\n"
             "class B3Base {\n"
             "    public static class Item {}\n"
             "}\n");
    /* 4. what the inner type inherits, before a member type of the type
     * around it */
    jt_write(&r, "src/main/java/lv/B4.java",
             "package lv;\n"
             "public class B4 {\n"
             "    public static class Piece {}\n"
             "    public static class Inner extends B4Base {\n"
             "        /** {@link Piece} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n"
             "class B4Base {\n"
             "    public static class Piece {}\n"
             "}\n");
    /* 5. a member type of an enclosing type, before a single import */
    jt_write(&r, "src/main/java/lv/B5.java",
             "package lv;\n"
             "import far.Thing;\n"
             "public class B5 {\n"
             "    public static class Thing {}\n"
             "    /** {@link Thing} */\n"
             "    public void m() {}\n"
             "}\n");
    /* 6. a single import, before the file's package */
    jt_write(&r, "src/main/java/lv/B6.java",
             "package lv;\n"
             "import far.Thing;\n"
             "/** {@link Thing} */\n"
             "public class B6 {}\n");
    /* 7. the file's package, before an on-demand import */
    jt_write(&r, "src/main/java/lv/B7.java",
             "package lv;\n"
             "import far.*;\n"
             "/** {@link Tool} */\n"
             "public class B7 {}\n");
    /* 8. on-demand imports are one level, java.lang among them: two
     * suppliers of a name are ambiguous, never a pick */
    jt_write(&r, "src/main/java/lv/B8.java",
             "package lv;\n"
             "import far.*;\n"
             "import near.*;\n"
             "/** {@link Dup} {@link Iterable} {@link Runnable} */\n"
             "public class B8 {}\n");
    /* 9. a qualified name: the head through the scope, else from the root */
    jt_write(&r, "src/main/java/lv/B9.java",
             "package lv;\n"
             "/**\n"
             " * {@link B5.Thing} {@link far.Thing} {@link B5.Nope} {@link far.Thing.go}\n"
             " * {@link lv.B4.Inner}\n"
             " */\n"
             "public class B9 {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* 1: local, neither an edge nor a row */
    ASSERT_EQ(dm_mentions_from(db, "lv.B1.m"), 0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B1.java", "T"), "");
    /* 2: the member type */
    ASSERT_EQ(jt_edge(db, "lv.B2.m", "lv.B2.Part"), 1);
    /* 3: local, not the inherited member type */
    ASSERT_EQ(dm_mentions_from(db, "lv.B3.m"), 0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B3.java", "Item"), "");
    /* 4: the inherited one, not the outer type's */
    ASSERT_EQ(jt_edge(db, "lv.B4.Inner.m", "lv.B4Base.Piece"), 1);
    ASSERT_EQ(jt_edge(db, "lv.B4.Inner.m", "lv.B4.Piece"), 0);
    /* 5: the member type, not the import */
    ASSERT_EQ(jt_edge(db, "lv.B5.m", "lv.B5.Thing"), 1);
    ASSERT_EQ(jt_edge(db, "lv.B5.m", "far.Thing"), 0);
    /* 6: the import (exact), not the package's type */
    ASSERT_EQ(jt_edge(db, "lv.B6", "far.Thing"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, "lv.B6", "lv.Thing"), 0);
    /* 7: the package's type (unique), not the on-demand import's */
    ASSERT_EQ(jt_edge(db, "lv.B7", "lv.Tool"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"unique\""));
    ASSERT_EQ(jt_edge(db, "lv.B7", "far.Tool"), 0);
    /* 8 */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B8.java", "Dup"), "ambiguous");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B8.java", "Iterable"), "ambiguous");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B8.java", "Runnable"), "external");
    ASSERT_EQ(dm_mentions_from(db, "lv.B8"), 0);
    /* 9 */
    ASSERT_EQ(jt_edge(db, "lv.B9", "lv.B5.Thing"), 1);
    ASSERT_EQ(jt_edge(db, "lv.B9", "far.Thing"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, "lv.B9", "lv.B4.Inner"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B9.java", "B5.Nope"), "missing");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/lv/B9.java", "far.Thing.go"), "missing");
    ASSERT_EQ(jt_edge(db, "lv.B9", "far.Thing.go"), 0);
    jt_end(&r);
    PASS();
}

/* Member references: the order of the levels, the kind rule, and no
 * relaxation across overloads, arities, kinds or constructors. */
TEST(doc_mentions_jvm_java_member_levels) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jmem"));
    jt_write(&r, "src/main/java/far/Thing.java",
             "package far;\npublic class Thing { public static void go() {} }\n");
    /* 1. the type's own method, before the one it inherits */
    jt_write(&r, "src/main/java/mv/M1.java",
             "package mv;\n"
             "public class M1 extends M1Base {\n"
             "    public void go() {}\n"
             "    /** {@link #go()} */\n"
             "    public void m() {}\n"
             "}\n"
             "class M1Base {\n"
             "    public void go() {}\n"
             "}\n");
    /* 2. what the inner type inherits, before the type around it */
    jt_write(&r, "src/main/java/mv/M2.java",
             "package mv;\n"
             "public class M2 {\n"
             "    public void go() {}\n"
             "    public class Inner extends M2Base {\n"
             "        /** {@link #go()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n"
             "class M2Base {\n"
             "    public void go() {}\n"
             "}\n");
    /* 3. a FIELD: javadoc looks in the enclosing types before the
     * supertypes, javac scoping the other way round. Where both have one
     * the reference binds neither; with only the inherited one it binds */
    jt_write(&r, "src/main/java/mv/M3.java",
             "package mv;\n"
             "public class M3 {\n"
             "    public int level = 1;\n"
             "    public class Inner extends M3Base {\n"
             "        /** {@link #level} {@link #depth} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n"
             "class M3Base {\n"
             "    public int level = 2;\n"
             "    public int depth = 3;\n"
             "}\n");
    /* 4. `Type#m` is javadoc's search from the type named: its supertypes,
     * then the types around it (JDK source, JavacTrees.searchMethod) */
    jt_write(&r, "src/main/java/mv/M4.java",
             "package mv;\n"
             "public class M4 {\n"
             "    public void outer() {}\n"
             "    public static class Inner {}\n"
             "}\n"
             "/** {@link M4.Inner#outer()} */\n"
             "class M4User {}\n");
    /* 5. kinds: a field comes first when its class is the method's or below
     * it; for a field ABOVE the method javadoc links the method */
    jt_write(&r, "src/main/java/mv/M5.java",
             "package mv;\n"
             "public class M5 extends M5Base {\n"
             "    public int size = 0;\n"
             "    public int size() { return size; }\n"
             "    public int total() { return 0; }\n"
             "    /** {@link #size} {@link #size()} {@link #total} {@link #total()} */\n"
             "    public void m() {}\n"
             "}\n"
             "class M5Base {\n"
             "    public int total = 1;\n"
             "}\n");
    /* 6. signatures */
    jt_write(&r, "src/main/java/mv/M6.java",
             "package mv;\n"
             "public class M6 extends M6Base {\n"
             "    public void put(int a) {}\n"
             "    public <T> void take(T item) {}\n"
             "    public <N extends Number> void add(N n) {}\n"
             "    public static M6 of(int... xs) { return null; }\n"
             "    /**\n"
             "     * {@link #put(int)} {@link #put(long)} {@link #put(String)} {@link #put()}\n"
             "     * {@link #take(Object)} {@link #take(String)} {@link #add(Number)}\n"
             "     * {@link #add(Object)} {@link #of(int...)} {@link #of(int[])} {@link #of(int)}\n"
             "     */\n"
             "    public void m() {}\n"
             "}\n"
             "class M6Base {\n"
             "    public void put(long a) {}\n"
             "}\n");
    /* 7. constructors: of the named type only, none inherited */
    jt_write(&r, "src/main/java/mv/M7.java",
             "package mv;\n"
             "public class M7 {\n"
             "    public M7(int n) {}\n"
             "    /** {@link #M7(int)} {@link #M7(String)} {@link M7Sub#M7Sub(int)} */\n"
             "    public void m() {}\n"
             "}\n"
             "class M7Sub extends M7 {\n"
             "    M7Sub() { super(1); }\n"
             "}\n");
    /* 8. a record: its component's accessor is its own method (no node);
     * the interface method of that name does not stand in */
    jt_write(&r, "src/main/java/mv/M8.java",
             "package mv;\n"
             "/** {@link M8#x()} {@link M8#x} {@link M8#M8(int, String)} {@link M8#M8(int)} */\n"
             "public record M8(int x, String y) implements M8Shape {}\n"
             "interface M8Shape {\n"
             "    int x();\n"
             "}\n");
    /* 9. among supertypes a class's method comes before an interface's,
     * wherever the interface sits */
    jt_write(&r, "src/main/java/mv/M9.java",
             "package mv;\n"
             "public class M9 extends M9Mid implements M9Api {\n"
             "    /** {@link #name()} */\n"
             "    public void m() {}\n"
             "}\n"
             "interface M9Api {\n"
             "    String name();\n"
             "}\n"
             "class M9Mid extends M9Base {}\n"
             "class M9Base {\n"
             "    public String name() { return null; }\n"
             "}\n");
    /* 10. javadoc reads a bare identifier that is no type as `#identifier`;
     * not where an on-demand import of an unknown package may hold a type
     * of that name, which would come first */
    jt_write(&r, "src/main/java/mv/M10.java",
             "package mv;\n"
             "public class M10 {\n"
             "    public void helper() {}\n"
             "    /** {@link helper} */\n"
             "    public void m() {}\n"
             "}\n");
    jt_write(&r, "src/main/java/mv/M10b.java",
             "package mv;\n"
             "import org.outside.*;\n"
             "public class M10b {\n"
             "    public void helper() {}\n"
             "    /** {@link helper} */\n"
             "    public void m() {}\n"
             "}\n");
    /* 11. a static import is no level for a member reference */
    jt_write(&r, "src/main/java/mv/M11.java",
             "package mv;\n"
             "import static far.Thing.go;\n"
             "public class M11 {\n"
             "    /** {@link #go()} */\n"
             "    public void m() {}\n"
             "}\n");
    /* 12. {@value} names a field: a method of that name is no target */
    jt_write(&r, "src/main/java/mv/M12.java",
             "package mv;\n"
             "public class M12 {\n"
             "    public static final int LIMIT = 3;\n"
             "    public int count() { return 0; }\n"
             "    /** {@value #LIMIT} {@value #count} */\n"
             "    public void m() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* 1 */
    ASSERT_EQ(jt_edge(db, "mv.M1.m", "mv.M1.go"), 1);
    ASSERT_EQ(jt_edge(db, "mv.M1.m", "mv.M1Base.go"), 0);
    /* 2 */
    ASSERT_EQ(jt_edge(db, "mv.M2.Inner.m", "mv.M2Base.go"), 1);
    ASSERT_EQ(jt_edge(db, "mv.M2.Inner.m", "mv.M2.go"), 0);
    /* 3 */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M3.java", "#level"), "ambiguous");
    ASSERT_EQ(jt_edge(db, "mv.M3.Inner.m", "mv.M3Base.depth"), 1);
    ASSERT_EQ(dm_mentions_from(db, "mv.M3.Inner.m"), 1);
    /* 4 */
    ASSERT_EQ(jt_edge(db, "mv.M4User", "mv.M4.outer"), 1);
    ASSERT_EQ(dm_mentions_from(db, "mv.M4User"), 1);
    /* 5 */
    ASSERT_EQ(jt_edge(db, "mv.M5.m", "mv.M5.size"), 1); /* the field */
    ASSERT_EQ(jt_edge(db, "mv.M5.m", "mv.M5.size"), 1); /* the method, by its list */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M5.java", "#total"), ""); /* no row */
    ASSERT_EQ(jt_edge(db, "mv.M5.m", "mv.M5.total"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":2")); /* `#total` and `#total()` */
    ASSERT_EQ(jt_edge(db, "mv.M5.m", "mv.M5Base.total"), 0);
    /* 6 */
    ASSERT_EQ(jt_edge(db, "mv.M6.m", "mv.M6.put"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1"));      /* put(int) only */
    ASSERT_EQ(jt_edge(db, "mv.M6.m", "mv.M6Base.put"), 1); /* put(long): the supertype's */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M6.java", "#put(String)"), "missing");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M6.java", "#put()"), "missing");
    ASSERT_EQ(jt_edge(db, "mv.M6.m", "mv.M6.take"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1")); /* take(Object), the erasure */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M6.java", "#take(String)"), "missing");
    ASSERT_EQ(jt_edge(db, "mv.M6.m", "mv.M6.add"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M6.java", "#add(Object)"), "missing");
    ASSERT_EQ(jt_edge(db, "mv.M6.m", "mv.M6.of"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":2")); /* of(int...) and of(int[]) */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M6.java", "#of(int)"), "missing");
    /* 7 */
    ASSERT_EQ(jt_edge(db, "mv.M7.m", "mv.M7.M7"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M7.java", "#M7(String)"), "missing");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M7.java", "M7Sub#M7Sub(int)"), "missing");
    /* 8 */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M8.java", "M8#x()"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M8.java", "M8#x"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M8.java", "M8#M8(int, String)"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M8.java", "M8#M8(int)"), "missing");
    ASSERT_EQ(jt_edge(db, "mv.M8", "mv.M8Shape.x"), 0);
    /* 9 */
    ASSERT_EQ(jt_edge(db, "mv.M9.m", "mv.M9Base.name"), 1);
    ASSERT_EQ(jt_edge(db, "mv.M9.m", "mv.M9Api.name"), 0);
    /* 10 */
    ASSERT_EQ(jt_edge(db, "mv.M10.m", "mv.M10.helper"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M10b.java", "helper"), "external");
    ASSERT_EQ(dm_mentions_from(db, "mv.M10b.m"), 0);
    /* 11 */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M11.java", "#go()"), "missing");
    ASSERT_EQ(dm_mentions_from(db, "mv.M11.m"), 0);
    /* 12 */
    ASSERT_EQ(jt_edge(db, "mv.M12.m", "mv.M12.LIMIT"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/mv/M12.java", "#count"), "missing");
    ASSERT_EQ(jt_edge(db, "mv.M12.m", "mv.M12.count"), 0);
    jt_end(&r);
    PASS();
}

/* ── Kotlin: every family, every reason ──────────────────────────── */

#define JT_K_GADGET                             \
    "package k\n"                               \
    "\n"                                        \
    "class Gadget {\n"                          \
    "    val count: Int = 0\n"                  \
    "    fun spin() {}\n"                       \
    "    fun turn(times: Int) {}\n"             \
    "    fun turn(name: String) {}\n"           \
    "    val both: Int = 1\n"                   \
    "    fun both() {}\n"                       \
    "    companion object Factory {\n"          \
    "        fun create(): Gadget = Gadget()\n" \
    "    }\n"                                   \
    "}\n"                                       \
    "enum class Color { RED, GREEN }\n"         \
    "class CustomError : RuntimeException()\n"  \
    "typealias Alias = Gadget\n"

#define JT_K_WIDGET                                                                                    \
    "package k\n"                                                                         /* 1 */      \
    "\n"                                                                                  /* 2 */      \
    "import far.Helper\n"                                                                 /* 3 */      \
    "import java.sql.SQLException\n"                                                      /* 4 */      \
    "\n"                                                                                  /* 5 */      \
    "/**\n"                                                                               /* 6 */      \
    " * A [Gadget], [the count][Gadget.count], [Gadget.spin], [Helper], [String], [T],\n" /* 7 */      \
    " * [name], [Gadget.turn], [Gadget.both], [Color.RED], [NoSuchThing], [Gadget.nope],\n" /* 8       \
                                                                                             */        \
    " * [GadgetTests], [Gadget.create], [Alias], [Alias.spin], [far.Helper.run], [far.sub],\n" /* 9 */ \
    " * [org.outside.Thing], [far.Missing], [k.env.Config]\n" /* 10 */                                 \
    " * @see Gadget.spin\n"                                   /* 11 */                                 \
    " * @see NoSuchThing\n"                                   /* 12 */                                 \
    " * @throws CustomError never\n"                          /* 13 */                                 \
    " * @throws SQLException bad\n"                           /* 14 */                                 \
    " * @throws IllegalStateException worse\n"                /* 15 */                                 \
    " * @sample k.samples.widgetSample\n"                     /* 16 */                                 \
    " * @sample k.samples.noSuchSample\n"                     /* 17 */                                 \
    " * @see Gadget..spin\n"                                  /* 18 */                                 \
    " */\n"                                                   /* 19 */                                 \
    "class Widget<T>(val name: String)\n"

TEST(doc_mentions_jvm_kotlin_families_and_reasons) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "kfam"));
    jt_write(&r, "src/main/kotlin/k/Gadget.kt", JT_K_GADGET);
    jt_write(&r, "src/main/kotlin/k/Widget.kt", JT_K_WIDGET);
    jt_write(&r, "src/main/kotlin/far/Helper.kt",
             "package far\n\nclass Helper {\n    fun run() {}\n}\n");
    jt_write(&r, "src/main/kotlin/far/sub/Deep.kt", "package far.sub\n\nclass Deep\n");
    /* a real package in a directory the indexer skips by name */
    jt_write(&r, "src/main/kotlin/k/env/Config.kt", "package k.env\n\nclass Config\n");
    jt_write(&r, "src/test/kotlin/k/GadgetTests.kt", "package k\n\nclass GadgetTests\n");
    jt_write(&r, "src/test/kotlin/k/samples/Samples.kt",
             "package k.samples\n\nfun widgetSample() {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    const char *w = "src/main/kotlin/k/Widget.kt";
    const char *widget = "k.Widget.Widget";

    /* kdoc_link: a type of the file's package */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.Gadget"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"kdoc_link\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"unique\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"line\":7"));
    /* [text][Type.member]: a property (its node is named below the file) */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.count"), 1);
    /* a member function, named twice: once in the text, once by @see */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.Gadget.spin"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":3")); /* ... and once through the alias */
    /* an explicit import: exact */
    ASSERT_EQ(jt_edge(db, widget, "far.Helper.Helper"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, widget, "far.Helper.Helper.run"), 1);
    /* a member of the companion object, named through its class */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.Factory.create"), 1);
    /* a type alias has a node of its own */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.Alias"), 1);
    /* throws */
    ASSERT_EQ(jt_edge(db, widget, "k.Gadget.CustomError"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"throws\""));
    /* sample: a function in test sources binds although Widget is product
     * code (samples live in tests) */
    ASSERT_EQ(jt_edge(db, widget, "k.samples.Samples.widgetSample"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"sample\""));

    /* external: a default import, an import of a package the repository
     * does not declare, a qualified name of such a package */
    ASSERT_STR_EQ(jt_reason(db, w, "String"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "SQLException"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "IllegalStateException"), "external");
    ASSERT_STR_EQ(jt_reason(db, w, "org.outside.Thing"), "external");
    /* local: the class's own type parameter and constructor parameter */
    ASSERT_STR_EQ(jt_reason(db, w, "T"), "");
    ASSERT_STR_EQ(jt_reason(db, w, "name"), "");
    /* ambiguous: an overload group; a property and a function of one name */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget.turn"), "ambiguous");
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget.both"), "ambiguous");
    /* graph_gap: an enum entry (declared, no node), a package (no node) */
    ASSERT_STR_EQ(jt_reason(db, w, "Color.RED"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, w, "far.sub"), "graph_gap");
    /* test_only_target */
    ASSERT_STR_EQ(jt_reason(db, w, "GadgetTests"), "test_only_target");
    /* not_indexed: a package of the repository in a directory discovery skips */
    ASSERT_STR_EQ(jt_reason(db, w, "k.env.Config"), "not_indexed");
    /* unparseable: a path with an empty name in it */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget..spin"), "unparseable");
    /* a `[name]` that names nothing is Markdown text to KDoc: no row. The
     * block tags name their subject: missing */
    ASSERT_STR_EQ(jt_reason(db, w, "Gadget.nope"), "");
    ASSERT_STR_EQ(jt_reason(db, w, "far.Missing"), "");
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw = 'NoSuchThing'"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw = 'NoSuchThing' "
                           "AND syntax = 'see' AND reason = 'missing' AND line = 12"),
              1);
    ASSERT_STR_EQ(jt_reason(db, w, "k.samples.noSuchSample"), "missing");
    jt_end(&r);
    PASS();
}

/* ── Kotlin: the lookup order, level by level ────────────────────── */

TEST(doc_mentions_jvm_kotlin_levels) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "klvl"));
    jt_write(&r, "src/main/kotlin/far/Thing.kt", "package far\n\nclass Thing\n");
    jt_write(&r, "src/main/kotlin/far/Tool.kt", "package far\n\nclass Tool\n");
    jt_write(&r, "src/main/kotlin/far/List.kt", "package far\n\nclass List\n");
    jt_write(&r, "src/main/kotlin/far/Funs.kt", "package far\n\nfun topFun() {}\n");
    jt_write(&r, "src/main/kotlin/far/Ext.kt", "package far\n\nfun Thing.shine() {}\n");
    jt_write(&r, "src/main/kotlin/kl/Thing.kt", "package kl\n\nclass Thing\n");
    jt_write(&r, "src/main/kotlin/kl/Tool.kt", "package kl\n\nclass Tool\n");
    /* 1. the definition's own parameter, before a member of its class */
    jt_write(&r, "src/main/kotlin/kl/K1.kt",
             "package kl\n"
             "\n"
             "class K1 {\n"
             "    val label: Int = 0\n"
             "    /** The [label]. */\n"
             "    fun m(label: String) {}\n"
             "}\n");
    /* 2. a constructor parameter is the class's own in the class's doc. In
     * a member's doc a `val` one is a property of the class (declared, no
     * node): it shadows the package's property of that name */
    jt_write(&r, "src/main/kotlin/kl/K2.kt",
             "package kl\n"
             "\n"
             "val shade: Int = 1\n"
             "\n"
             "/** Uses [shade] and [plain]. */\n"
             "class K2(val shade: Int, plain: Int) {\n"
             "    /** Reads [shade], also [plain]. */\n"
             "    fun m() {}\n"
             "}\n");
    /* 3. what the type holds itself and in its companion (one scope), then
     * what it inherits: its own override, not the supertype's */
    jt_write(&r, "src/main/kotlin/kl/K3.kt",
             "package kl\n"
             "\n"
             "open class K3Base {\n"
             "    fun inherited() {}\n"
             "    open fun shared() {}\n"
             "}\n"
             "\n"
             "class K3 : K3Base() {\n"
             "    fun own() {}\n"
             "    override fun shared() {}\n"
             "    companion object Tools {\n"
             "        fun make() {}\n"
             "    }\n"
             "    /** [own], [make], [inherited], [shared] */\n"
             "    fun m() {}\n"
             "}\n"
             "\n"
             "fun K3Base.glow() {}\n"
             "\n"
             "fun <T> T.anyExt() {}\n");
    /* 4. a type nested in an enclosing type, before an explicit import */
    jt_write(&r, "src/main/kotlin/kl/K4.kt",
             "package kl\n"
             "\n"
             "import far.Thing\n"
             "\n"
             "class K4 {\n"
             "    class Thing\n"
             "    /** [Thing] */\n"
             "    fun m() {}\n"
             "}\n");
    /* 5. an explicit import, before the file's package */
    jt_write(&r, "src/main/kotlin/kl/K5.kt",
             "package kl\n"
             "\n"
             "import far.Thing\n"
             "\n"
             "/** [Thing] */\n"
             "class K5\n");
    /* 6. the file's package, before a star import */
    jt_write(&r, "src/main/kotlin/kl/K6.kt",
             "package kl\n"
             "\n"
             "import far.*\n"
             "\n"
             "/** [Tool] */\n"
             "class K6\n");
    /* 7. a star import, before the default imports; 8. the default imports */
    jt_write(&r, "src/main/kotlin/kl/K7.kt",
             "package kl\n"
             "\n"
             "import far.*\n"
             "\n"
             "/** [List] */\n"
             "class K7\n");
    jt_write(&r, "src/main/kotlin/kl/K8.kt",
             "package kl\n"
             "\n"
             "/** [List], [listOf], [Runnable] */\n"
             "class K8\n");
    /* 9. dotted names */
    jt_write(
        &r, "src/main/kotlin/kl/K9.kt",
        "package kl\n"
        "\n"
        "/** [K4.Thing], [far.Thing], [far.topFun], [K3.make], [K3.inherited], [K3.shared] */\n"
        "class K9\n");
    /* 10. an extension is found for `Type.name` among the callables in
     * scope, by its receiver: imported, declared in the file's package (on a
     * supertype, on any type), and not when nothing brings it into scope */
    jt_write(&r, "src/main/kotlin/kl/K10.kt",
             "package kl\n"
             "\n"
             "import far.Thing\n"
             "import far.shine\n"
             "\n"
             "/** [Thing.shine], [K3.glow], [K3.anyExt] */\n"
             "class K10\n");
    jt_write(&r, "src/main/kotlin/kl/K10b.kt",
             "package kl\n"
             "\n"
             "/** @see far.Thing.shine */\n"
             "class K10b\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* 1 */
    ASSERT_EQ(dm_mentions_from(db, "kl.K1.K1.m"), 0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K1.kt", "label"), "");
    /* 2: the class's doc leaves no row; the member's doc one gap for the
     * property and nothing for the plain parameter */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/kotlin/kl/K2.kt'"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/kotlin/kl/K2.kt' AND raw = 'shade' AND line = 7 AND "
                           "reason = 'graph_gap'"),
              1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges e JOIN nodes t ON t.id = e.target_id "
                           "WHERE e.type = 'MENTIONS' AND t.qualified_name LIKE '%.kl.K2.shade'"),
              0);
    /* 3 */
    ASSERT_EQ(jt_edge(db, "kl.K3.K3.m", "kl.K3.K3.own"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K3.K3.m", "kl.K3.Tools.make"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K3.K3.m", "kl.K3.K3Base.inherited"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K3.K3.m", "kl.K3.K3.shared"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K3.K3.m", "kl.K3.K3Base.shared"), 0);
    /* 4 */
    ASSERT_EQ(jt_edge(db, "kl.K4.K4.m", "kl.K4.Thing"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K4.K4.m", "far.Thing.Thing"), 0);
    /* 5 */
    ASSERT_EQ(jt_edge(db, "kl.K5.K5", "far.Thing.Thing"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, "kl.K5.K5", "kl.Thing.Thing"), 0);
    /* 6 */
    ASSERT_EQ(jt_edge(db, "kl.K6.K6", "kl.Tool.Tool"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K6.K6", "far.Tool.Tool"), 0);
    /* 7 */
    ASSERT_EQ(jt_edge(db, "kl.K7.K7", "far.List.List"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K7.kt", "List"), "");
    /* 8 */
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K8.kt", "List"), "external");
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K8.kt", "listOf"), "external");
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K8.kt", "Runnable"), "external");
    /* 9 */
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "kl.K4.Thing"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "far.Thing.Thing"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "far.Funs.topFun"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "kl.K3.Tools.make"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "kl.K3.K3Base.inherited"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "kl.K3.K3.shared"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K9.K9", "kl.K3.K3Base.shared"), 0);
    /* 10 */
    ASSERT_EQ(jt_edge(db, "kl.K10.K10", "far.Ext.shine"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K10.K10", "kl.K3.glow"), 1);
    ASSERT_EQ(jt_edge(db, "kl.K10.K10", "kl.K3.anyExt"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/kl/K10b.kt", "far.Thing.shine"), "missing");
    ASSERT_EQ(dm_mentions_from(db, "kl.K10b.K10b"), 0);
    jt_end(&r);
    PASS();
}

/* ── a member a nearer type may have unseen ──────────────────────── */

/* Both languages resolve a member name in a type, with what the type
 * inherits, before they look at the type around it (or, in Kotlin, at the
 * file's imports and package). A member found further out binds only when the
 * nearer type is KNOWN not to have one: not where the language gives every
 * type a member of that name, and not where a supertype outside the
 * repository may declare it. Those references are external rows. */

/* Number of unresolved rows of file `rel` with this raw text and reason. */
static int jt_rows(const char *db, const char *rel, const char *raw, const char *reason) {
    char sql[700];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = '%s' AND raw = '%s' "
             "AND reason = '%s'",
             rel, raw, reason);
    return dm_count(db, sql);
}

/* Java: a method every class has (java.lang.Object's), or every enum, is the
 * documented class's own. `#toString()` in a nested class never names the
 * method of the class around it. */
TEST(doc_mentions_jvm_java_given_members) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jgiv"));
    const char *g1 = "src/main/java/gj/G1.java";
    jt_write(&r, g1,
             "package gj;\n"
             "public class G1 {\n"
             "    public int hashCode = 2;\n"
             "    public String toString() { return \"\"; }\n"
             "    public boolean equals(String key, int n) { return false; }\n"
             "    public void helper() {}\n"
             "    public static class Bare {\n"
             "        /**\n"
             "         * {@link #toString()} {@link #toString} {@link #equals(Object)}\n"
             "         * {@link #wait(long)} {@link #equals(String, int)} {@link #helper()}\n"
             "         * {@link #hashCode}\n"
             "         */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Own {\n"
             "        public String toString() { return \"\"; }\n"
             "        /** {@link #toString()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    const char *g2 = "src/main/java/gj/G2.java";
    jt_write(&r, g2,
             "package gj;\n"
             "public class G2 {\n"
             "    public static void values() {}\n"
             "    public void helper() {}\n"
             "    public enum Mode {\n"
             "        ON;\n"
             "        /** {@link #values()} {@link #helper()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* Object's methods, with one of their own parameter lists or with none
     * written: external, and no edge to the method of the class around */
    ASSERT_EQ(jt_rows(db, g1, "#toString()", "external"), 1);
    ASSERT_EQ(jt_rows(db, g1, "#toString", "external"), 1);
    ASSERT_EQ(jt_rows(db, g1, "#equals(Object)", "external"), 1);
    ASSERT_EQ(jt_rows(db, g1, "#wait(long)", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G1.Bare.m", "gj.G1.toString"), 0);
    /* decoys: another parameter list is no method of Object; another name is
     * none at all; a FIELD of such a name keeps javadoc's order for fields */
    ASSERT_EQ(jt_edge(db, "gj.G1.Bare.m", "gj.G1.equals"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G1.Bare.m", "gj.G1.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G1.Bare.m", "gj.G1.hashCode"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gj.G1.Bare.m"), 3);
    /* ... and a class that declares the method binds its own */
    ASSERT_EQ(jt_edge(db, "gj.G1.Own.m", "gj.G1.Own.toString"), 1);
    /* what an enum has, and beside it a name it does not have */
    ASSERT_EQ(jt_rows(db, g2, "#values()", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G2.Mode.m", "gj.G2.values"), 0);
    ASSERT_EQ(jt_edge(db, "gj.G2.Mode.m", "gj.G2.helper"), 1);
    jt_end(&r);
    PASS();
}

/* The supertypes the Java cases share: one whose own hierarchy leaves the
 * repository, one without members, one with them. */
static void jt_guard_java_bases(const jt_repo_t *r) {
    jt_write(r, "src/main/java/gj/Mid.java",
             "package gj;\n"
             "public class Mid extends java.io.InputStream {\n"
             "    public int read() { return 0; }\n"
             "}\n");
    jt_write(r, "src/main/java/gj/Plain.java", "package gj;\npublic class Plain {}\n");
    jt_write(r, "src/main/java/gj/Has.java",
             "package gj;\n"
             "public class Has {\n"
             "    public void close() {}\n"
             "    public void helper() {}\n"
             "}\n");
}

/* Java: a method of the class around a nested class binds only when every
 * supertype of the nested class is in the repository and none has it. A
 * supertype outside the repository, directly or further up, may declare it,
 * and then javadoc links that one. */
TEST(doc_mentions_jvm_java_open_supertypes) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jopen"));
    jt_guard_java_bases(&r);
    const char *g3 = "src/main/java/gj/G3.java";
    jt_write(&r, g3,
             "package gj;\n"
             "public class G3 {\n"
             "    public int count = 1;\n"
             "    public void close() {}\n"
             "    public void helper() {}\n"
             "    public static class Stream extends java.io.InputStream {\n"
             "        public int read() { return 0; }\n"
             "        /** {@link #close()} {@link #helper} {@link #count} {@link #read()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Deep extends Mid {\n"
             "        /** {@link #close()} {@link #count} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Wrap extends java.io.InputStream {\n"
             "        public int read() { return 0; }\n"
             "        public class In {\n"
             "            /** {@link #helper()} {@link #read()} */\n"
             "            public void m() {}\n"
             "        }\n"
             "    }\n"
             "}\n");
    const char *g4 = "src/main/java/gj/G4.java";
    jt_write(&r, g4,
             "package gj;\n"
             "public class G4 {\n"
             "    public int count = 1;\n"
             "    public void close() {}\n"
             "    public void helper() {}\n"
             "    public void extra() {}\n"
             "    public static class Lacks extends Plain {\n"
             "        /** {@link #close()} {@link #helper} {@link #count} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Gets extends Has {\n"
             "        /** {@link #close()} {@link #helper} {@link #extra()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    const char *g5 = "src/main/java/gj/G5.java";
    jt_write(&r, g5,
             "package gj;\n"
             "public class G5 {\n"
             "    public int total() { return 0; }\n"
             "    public class Mid5 extends java.io.InputStream {\n"
             "        public int read() { return 0; }\n"
             "        public class In {\n"
             "            public int total = 1;\n"
             "            /** {@link #total} */\n"
             "            public void m() {}\n"
             "        }\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* a supertype outside the repository, written on the class (Stream) and
     * one level up (Deep, through Mid): java.io.InputStream may declare
     * close() and helper() */
    ASSERT_EQ(jt_rows(db, g3, "#close()", "external"), 2);
    ASSERT_EQ(jt_rows(db, g3, "#helper", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G3.Stream.m", "gj.G3.close"), 0);
    ASSERT_EQ(jt_edge(db, "gj.G3.Stream.m", "gj.G3.helper"), 0);
    ASSERT_EQ(jt_edge(db, "gj.G3.Deep.m", "gj.G3.close"), 0);
    /* the class's own method is found before the lookup would leave it */
    ASSERT_EQ(jt_edge(db, "gj.G3.Stream.m", "gj.G3.Stream.read"), 1);
    /* a FIELD of the class around, through the same shapes: the supertype
     * outside may have a METHOD count(), which javadoc would link instead */
    ASSERT_EQ(jt_rows(db, g3, "#count", "external"), 2);
    ASSERT_EQ(jt_edge(db, "gj.G3.Stream.m", "gj.G3.count"), 0);
    ASSERT_EQ(dm_mentions_from(db, "gj.G3.Stream.m"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gj.G3.Deep.m"), 0);
    /* the type that leaves the repository need not be the innermost one */
    ASSERT_EQ(jt_rows(db, g3, "#helper()", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G3.Wrap.In.m", "gj.G3.Wrap.read"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gj.G3.Wrap.In.m"), 1);
    /* every supertype in the repository and none has the method: the class
     * around it, as before */
    ASSERT_EQ(jt_edge(db, "gj.G4.Lacks.m", "gj.G4.close"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G4.Lacks.m", "gj.G4.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G4.Lacks.m", "gj.G4.count"), 1);
    /* a supertype in the repository has it: that one */
    ASSERT_EQ(jt_edge(db, "gj.G4.Gets.m", "gj.Has.close"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G4.Gets.m", "gj.Has.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G4.Gets.m", "gj.G4.close"), 0);
    ASSERT_EQ(jt_edge(db, "gj.G4.Gets.m", "gj.G4.extra"), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/java/gj/G4.java'"),
              0);
    /* the field of In against javadoc's search for a method: it passes Mid5,
     * whose supertype lies outside, before it reaches total() of G5. In is
     * not below Mid5: a method total() there would win, and so would G5's
     * (In is not below G5 either): external */
    ASSERT_EQ(jt_rows(db, g5, "#total", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gj.G5.Mid5.In.m"), 0);
    jt_end(&r);
    PASS();
}

/* Java: a Kotlin declaration of the name cannot be compared with a written
 * Javadoc parameter list. It may be the method meant, so the lookup does not
 * go on to the class around it. */
TEST(doc_mentions_jvm_java_uncompared_level) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "juncmp"));
    jt_write(&r, "src/main/kotlin/gj/KBase.kt",
             "package gj\n"
             "\n"
             "open class KBase {\n"
             "    fun send(x: Int) {}\n"
             "}\n"
             "\n"
             "open class KPlain\n");
    const char *g6 = "src/main/java/gj/G6.java";
    jt_write(&r, g6,
             "package gj;\n"
             "public class G6 {\n"
             "    public void send(int x) {}\n"
             "    public static class Sub extends KBase {\n"
             "        /** {@link #send(int)} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Sub2 extends KPlain {\n"
             "        /** {@link #send(int)} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_rows(db, g6, "#send(int)", "ambiguous"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gj.G6.Sub.m"), 0);
    /* decoy: a Kotlin supertype without the name is known not to have it */
    ASSERT_EQ(jt_edge(db, "gj.G6.Sub2.m", "gj.G6.send"), 1);
    jt_end(&r);
    PASS();
}

/* Kotlin: a member of kotlin.Any, or of an enum class, is the documented
 * class's own. `[toString]` in a class is neither the package's function of
 * that name nor plain text. */
TEST(doc_mentions_jvm_kotlin_given_members) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "kgiv"));
    jt_write(&r, "src/main/kotlin/gk/Top.kt",
             "package gk\n"
             "\n"
             "fun toString(x: Int): String = \"\"\n"
             "\n"
             "fun values() {}\n"
             "\n"
             "fun helper() {}\n");
    const char *k1 = "src/main/kotlin/gk/K1.kt";
    jt_write(&r, k1,
             "package gk\n"
             "\n"
             "/** [toString], [hashCode], [helper] */\n"
             "class Bare\n"
             "\n"
             "class Own {\n"
             "    override fun toString(): String = \"\"\n"
             "    /** [toString] */\n"
             "    fun m() {}\n"
             "}\n"
             "\n"
             "enum class Color {\n"
             "    RED;\n"
             "    /** [values], [helper] */\n"
             "    fun m() {}\n"
             "}\n"
             "\n"
             "/** [toString] */\n"
             "fun free() {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* external: with a function of that name in the package, and with
     * nothing of that name anywhere */
    ASSERT_EQ(jt_rows(db, k1, "toString", "external"), 1);
    ASSERT_EQ(jt_rows(db, k1, "hashCode", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K1.Bare", "gk.Top.toString"), 0);
    /* decoys: another name; a class that declares the member; a definition
     * with no class around it */
    ASSERT_EQ(jt_edge(db, "gk.K1.Bare", "gk.Top.helper"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K1.Bare"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K1.Own.m", "gk.K1.Own.toString"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K1.free", "gk.Top.toString"), 1);
    /* what an enum class has, and beside it a name it does not have (the
     * graph names a method of an enum class below the file) */
    ASSERT_EQ(jt_rows(db, k1, "values", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K1.m", "gk.Top.helper"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K1.m"), 1);
    jt_end(&r);
    PASS();
}

/* Kotlin: a function or property of the package, or of the class around,
 * binds only when every supertype of the class (and of its companion) is in
 * the repository and none has the name. A type found further out binds as
 * before. */
TEST(doc_mentions_jvm_kotlin_open_supertypes) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "kopen"));
    jt_write(&r, "src/main/kotlin/gk/Top.kt",
             "package gk\n"
             "\n"
             "fun close() {}\n"
             "\n"
             "fun helper() {}\n"
             "\n"
             "val depth: Int = 0\n"
             "\n"
             "class Thing\n");
    jt_write(&r, "src/main/kotlin/gk/Bases.kt",
             "package gk\n"
             "\n"
             "open class Mid : java.io.InputStream() {\n"
             "    override fun read(): Int = 0\n"
             "}\n"
             "\n"
             "open class Plain\n"
             "\n"
             "open class Has {\n"
             "    fun helper() {}\n"
             "}\n");
    const char *k2 = "src/main/kotlin/gk/K2.kt";
    jt_write(&r, k2,
             "package gk\n"
             "\n"
             "/** [close], [helper], [depth], [read], [Thing] */\n"
             "class Stream : java.io.InputStream() {\n"
             "    override fun read(): Int = 0\n"
             "}\n"
             "\n"
             "/** [close], [Thing] */\n"
             "class Deep : Mid()\n");
    const char *k3 = "src/main/kotlin/gk/K3.kt";
    jt_write(&r, k3,
             "package gk\n"
             "\n"
             "/** [close], [helper], [depth] */\n"
             "class Lacks : Plain()\n"
             "\n"
             "/** [close], [helper] */\n"
             "class Gets : Has()\n");
    const char *k4 = "src/main/kotlin/gk/K4.kt";
    jt_write(&r, k4,
             "package gk\n"
             "\n"
             "class Outer {\n"
             "    fun aid() {}\n"
             "\n"
             "    inner class Wrapped : java.io.InputStream() {\n"
             "        override fun read(): Int = 0\n"
             "        /** [aid] */\n"
             "        fun m() {}\n"
             "    }\n"
             "\n"
             "    inner class Simple {\n"
             "        /** [aid] */\n"
             "        fun m() {}\n"
             "    }\n"
             "}\n"
             "\n"
             "class Locked {\n"
             "    companion object Lock : java.io.Closeable {\n"
             "        override fun close() {}\n"
             "    }\n"
             "    /** [helper], [close] */\n"
             "    fun m() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* a supertype outside the repository, written on the class (Stream) and
     * one level up (Deep, through Mid): no function or property of the
     * package */
    ASSERT_EQ(jt_rows(db, k2, "close", "external"), 2);
    ASSERT_EQ(jt_rows(db, k2, "helper", "external"), 1);
    ASSERT_EQ(jt_rows(db, k2, "depth", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K2.Stream", "gk.Top.close"), 0);
    ASSERT_EQ(jt_edge(db, "gk.K2.Deep", "gk.Top.close"), 0);
    /* the class's own member, and a TYPE of the package, bind */
    ASSERT_EQ(jt_edge(db, "gk.K2.Stream", "gk.K2.Stream.read"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K2.Stream", "gk.Top.Thing"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K2.Stream"), 2);
    ASSERT_EQ(jt_edge(db, "gk.K2.Deep", "gk.Top.Thing"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K2.Deep"), 1);
    /* every supertype in the repository and none has the name: the package,
     * as before */
    ASSERT_EQ(jt_edge(db, "gk.K3.Lacks", "gk.Top.close"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K3.Lacks", "gk.Top.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K3.Lacks", "gk.Top.depth"), 1);
    /* a supertype in the repository has it: that one */
    ASSERT_EQ(jt_edge(db, "gk.K3.Gets", "gk.Bases.Has.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K3.Gets", "gk.Top.helper"), 0);
    ASSERT_EQ(jt_edge(db, "gk.K3.Gets", "gk.Top.close"), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/kotlin/gk/K3.kt'"),
              0);
    /* the class around instead of the package: the same rule */
    ASSERT_EQ(jt_rows(db, k4, "aid", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K4.Wrapped.m"), 0);
    ASSERT_EQ(jt_edge(db, "gk.K4.Simple.m", "gk.K4.Outer.aid"), 1);
    /* the companion object's supertypes count: its members are in the
     * class's scope. What the companion declares itself binds */
    ASSERT_EQ(jt_rows(db, k4, "helper", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K4.Locked.m", "gk.K4.Lock.close"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K4.Locked.m"), 1);
    jt_end(&r);
    PASS();
}

/* Kotlin: what the companion object inherits is in the class's scope, before
 * the package. */
TEST(doc_mentions_jvm_kotlin_companion_inherits) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "kcomp"));
    jt_write(&r, "src/main/kotlin/gk/Top.kt",
             "package gk\n"
             "\n"
             "fun create() {}\n"
             "\n"
             "fun helper() {}\n");
    jt_write(&r, "src/main/kotlin/gk/K5.kt",
             "package gk\n"
             "\n"
             "open class Maker {\n"
             "    fun create() {}\n"
             "}\n"
             "\n"
             "class Entity {\n"
             "    companion object : Maker()\n"
             "    /** [create], [helper] */\n"
             "    fun m() {}\n"
             "}\n"
             "\n"
             "/** [Entity.create] */\n"
             "class User\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_edge(db, "gk.K5.Entity.m", "gk.K5.Maker.create"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K5.Entity.m", "gk.Top.create"), 0);
    /* decoy: a name the companion's hierarchy does not have is the package's */
    ASSERT_EQ(jt_edge(db, "gk.K5.Entity.m", "gk.Top.helper"), 1);
    /* the same member, named through the class */
    ASSERT_EQ(jt_edge(db, "gk.K5.User", "gk.K5.Maker.create"), 1);
    jt_end(&r);
    PASS();
}

/* Kotlin `Type.name`: a member comes before an extension. Where a supertype
 * of the type lies outside the repository it may declare the name, so the
 * extension in scope is not taken. */
TEST(doc_mentions_jvm_kotlin_extension_guard) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "kext"));
    jt_write(&r, "src/main/kotlin/gk/Types.kt",
             "package gk\n"
             "\n"
             "class Shut : java.io.Closeable {\n"
             "    override fun close() {}\n"
             "}\n"
             "\n"
             "open class Plain\n"
             "\n"
             "class Calm : Plain()\n");
    jt_write(&r, "src/main/kotlin/gk/ExtA.kt", "package gk\n\nfun Shut.shine() {}\n");
    jt_write(&r, "src/main/kotlin/gk/ExtB.kt", "package gk\n\nfun Calm.glow() {}\n");
    const char *k6 = "src/main/kotlin/gk/K6.kt";
    jt_write(&r, k6,
             "package gk\n"
             "\n"
             "/** [Shut.shine], [Calm.glow], [Shut.close] */\n"
             "class ExtUser\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_rows(db, k6, "Shut.shine", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K6.ExtUser", "gk.ExtA.shine"), 0);
    /* decoys: a receiver whose supertypes are all in the repository; a
     * member the open type declares itself */
    ASSERT_EQ(jt_edge(db, "gk.K6.ExtUser", "gk.ExtB.glow"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K6.ExtUser", "gk.Types.Shut.close"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K6.ExtUser"), 2);
    jt_end(&r);
    PASS();
}

/* java.lang.Object and kotlin.Any written out as a supertype are no supertype
 * outside the repository: every type has them, and their members are known. */
TEST(doc_mentions_jvm_root_supertype_written) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "groot"));
    jt_write(&r, "src/main/java/gj/G7.java",
             "package gj;\n"
             "public class G7 {\n"
             "    public void helper() {}\n"
             "    public static class Rooted extends Object {\n"
             "        /** {@link #helper()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Full extends java.lang.Object {\n"
             "        /** {@link #helper()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    jt_write(&r, "src/main/kotlin/gk/Top.kt", "package gk\n\nfun helper() {}\n");
    jt_write(&r, "src/main/kotlin/gk/K7.kt",
             "package gk\n"
             "\n"
             "/** [helper] */\n"
             "class Rooted : Any()\n"
             "\n"
             "/** [helper] */\n"
             "class Full : kotlin.Any()\n");
    /* decoy: where an import supplies the simple name, the supertype is
     * whatever that import names */
    const char *k7b = "src/main/kotlin/gk/K7b.kt";
    jt_write(&r, k7b,
             "package gk\n"
             "\n"
             "import org.outside.Any\n"
             "\n"
             "/** [helper] */\n"
             "class Other : Any()\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_edge(db, "gj.G7.Rooted.m", "gj.G7.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gj.G7.Full.m", "gj.G7.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K7.Rooted", "gk.Top.helper"), 1);
    ASSERT_EQ(jt_edge(db, "gk.K7.Full", "gk.Top.helper"), 1);
    ASSERT_EQ(jt_rows(db, k7b, "helper", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gk.K7b.Other"), 0);
    jt_end(&r);
    PASS();
}

/* The guard reads supertypes that OTHER files declare: what a file's own
 * references bind changes when a supertype elsewhere loses a member, or gains
 * or loses a supertype outside the repository. Incremental equals full. */
TEST(doc_mentions_jvm_guard_incremental_equals_full) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "ginc"));
    const char *base = "src/main/java/gi/Base.java";
    const char *outer = "src/main/java/gi/Outer.java";
    jt_write(&r, base, "package gi;\npublic class Base {\n    public void near() {}\n}\n");
    jt_write(&r, outer,
             "package gi;\n"
             "public class Outer {\n"
             "    public void near() {}\n"
             "    public void far() {}\n"
             "    public class In extends Base {\n"
             "        /** {@link #near()} {@link #far()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    char full_db[600];
    snprintf(full_db, sizeof(full_db), "%s/full.db", r.tmp);
    const char *db = r.db;
    ASSERT_EQ(dm_index(r.repo, db, NULL), 0);
    ASSERT_EQ(jt_edge(db, "gi.Outer.In.m", "gi.Base.near"), 1);
    ASSERT_EQ(jt_edge(db, "gi.Outer.In.m", "gi.Outer.far"), 1);
    /* the supertype loses the member: the class around has one */
    jt_write(&r, base, "package gi;\npublic class Base {\n}\n");
    ASSERT_EQ(dm_step(r.repo, db, full_db, "supertype member removed",
                      CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    ASSERT_EQ(jt_edge(db, "gi.Outer.In.m", "gi.Outer.near"), 1);
    /* the supertype leaves the repository: no method of the class around */
    jt_write(&r, base,
             "package gi;\n"
             "public class Base extends java.io.InputStream {\n"
             "    public int read() { return 0; }\n"
             "}\n");
    ASSERT_EQ(dm_step(r.repo, db, full_db, "supertype opened", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    ASSERT_EQ(dm_mentions_from(db, "gi.Outer.In.m"), 0);
    ASSERT_EQ(jt_rows(db, outer, "#far()", "external"), 1);
    /* ... and comes back into it */
    jt_write(&r, base, "package gi;\npublic class Base {\n}\n");
    ASSERT_EQ(dm_step(r.repo, db, full_db, "supertype closed", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    ASSERT_EQ(jt_edge(db, "gi.Outer.In.m", "gi.Outer.near"), 1);
    ASSERT_EQ(jt_edge(db, "gi.Outer.In.m", "gi.Outer.far"), 1);
    dm_unlink_db(full_db);
    jt_end(&r);
    PASS();
}

/* ── both languages in one index ─────────────────────────────────── */

/* A package is one namespace whatever language declares into it: a KDoc name
 * finds a Java class, a Javadoc link a Kotlin class. What cannot be compared
 * across the two (a Javadoc signature against a Kotlin declaration, a type
 * alias seen from Java) is a row, never a guess. */
TEST(doc_mentions_jvm_cross_language) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "cross"));
    jt_write(&r, "src/main/java/x/JBox.java",
             "package x;\n"
             "public class JBox {\n"
             "    public int size = 1;\n"
             "    public void open() {}\n"
             "    public void open(int n) {}\n"
             "    public void shut() {}\n"
             "}\n");
    jt_write(&r, "src/main/kotlin/x/KBox.kt",
             "package x\n"
             "\n"
             "class KBox {\n"
             "    fun close() {}\n"
             "    val lid: Int = 0\n"
             "}\n"
             "\n"
             "typealias KAlias = KBox\n");
    jt_write(&r, "src/main/kotlin/x/KUser.kt",
             "package x\n"
             "\n"
             "/** [JBox], [JBox.size], [JBox.shut], [JBox.open] */\n"
             "class KUser\n");
    jt_write(&r, "src/main/java/x/JUser.java",
             "package x;\n"
             "/**\n"
             " * {@link KBox} {@link KBox#close} {@link KBox#lid} {@link KBox#close()}\n"
             " * {@link KAlias}\n"
             " */\n"
             "public class JUser {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* Kotlin -> Java */
    ASSERT_EQ(jt_edge(db, "x.KUser.KUser", "x.JBox"), 1);
    ASSERT_EQ(jt_edge(db, "x.KUser.KUser", "x.JBox.size"), 1);
    ASSERT_EQ(jt_edge(db, "x.KUser.KUser", "x.JBox.shut"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/x/KUser.kt", "JBox.open"), "ambiguous");
    /* Java -> Kotlin */
    ASSERT_EQ(jt_edge(db, "x.JUser", "x.KBox.KBox"), 1);
    ASSERT_EQ(jt_edge(db, "x.JUser", "x.KBox.KBox.close"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1")); /* `#close`, not `#close()` */
    ASSERT_EQ(jt_edge(db, "x.JUser", "x.KBox.lid"), 1);
    /* a Javadoc parameter list cannot be compared with a Kotlin signature */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/x/JUser.java", "KBox#close()"), "ambiguous");
    /* Java does not see a Kotlin type alias */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/x/JUser.java", "KAlias"), "missing");
    ASSERT_EQ(jt_edge(db, "x.JUser", "x.KBox.KAlias"), 0);
    jt_end(&r);
    PASS();
}

/* ── one node per qualified name ─────────────────────────────────── */

/* Several declarations can have one qualified name, and the graph keeps one
 * node. A reference binds only the declaration that owns the node, and an
 * edge leaves only from a definition that owns its own: a doc written on a
 * definition whose name another declaration took has nowhere to leave from. */
TEST(doc_mentions_jvm_node_ownership) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "own"));
    /* Kotlin: a class and the factory function named like it share the
     * file-based name; the later declaration owns the node */
    jt_write(&r, "src/main/kotlin/c/Maker.kt",
             "package c\n"
             "\n"
             "/** Makes a [Part], takes [T] and [String]. */\n"
             "class Maker<T>\n"
             "\n"
             "class Part\n"
             "\n"
             "/** Factory of a [Part]. */\n"
             "fun Maker(): Maker<Int> = Maker()\n");
    /* Kotlin: two classes' properties of one name in one file */
    jt_write(&r, "src/main/kotlin/c/Twins.kt",
             "package c\n"
             "\n"
             "class First {\n"
             "    /** In [First]. */\n"
             "    val id: Int = 1\n"
             "}\n"
             "\n"
             "class Second {\n"
             "    /** In [Second]. */\n"
             "    val id: Int = 2\n"
             "}\n"
             "\n"
             "/** [First.id] and [Second.id] */\n"
             "class TwinUser\n");
    /* Java: a field and its accessor share `Holder.value`; the method,
     * declared later, owns the node */
    jt_write(&r, "src/main/java/c/Holder.java",
             "package c;\n"
             "public class Holder {\n"
             "    /** The {@link Other}. */\n"
             "    private int value;\n"
             "    /** Reads {@link Other}, see {@link #value} and {@link #value()}. */\n"
             "    public int value() { return value; }\n"
             "}\n"
             "class Other {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    const char *maker = "src/main/kotlin/c/Maker.kt";
    /* the function's doc: an edge from the node the function owns */
    ASSERT_EQ(jt_edge(db, "c.Maker.Maker", "c.Maker.Part"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1"));
    ASSERT_NOT_NULL(strstr(jt_props, "\"line\":8"));
    /* the class's doc: what would be an edge is a gap; what is local or
     * external anyway keeps its own outcome */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/kotlin/c/Maker.kt' AND raw = 'Part' AND line = 3 AND "
                           "reason = 'graph_gap'"),
              1);
    ASSERT_STR_EQ(jt_reason(db, maker, "T"), "");
    ASSERT_STR_EQ(jt_reason(db, maker, "String"), "external");
    /* the property that owns `id` is the later one: Second's */
    ASSERT_EQ(jt_edge(db, "c.Twins.id", "c.Twins.Second"), 1);
    ASSERT_EQ(jt_edge(db, "c.Twins.id", "c.Twins.First"), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/kotlin/c/Twins.kt' AND raw = 'First' AND reason = "
                           "'graph_gap'"),
              1);
    ASSERT_EQ(jt_edge(db, "c.Twins.TwinUser", "c.Twins.id"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1")); /* Second.id only */
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/c/Twins.kt", "First.id"), "graph_gap");
    /* Java: a field named like its accessor has a node of its own
     * (<Owner>.<name>#field), so its doc is a source too; `#value` is the
     * field, `#value()` the method itself */
    ASSERT_EQ(jt_edge(db, "c.Holder.value", "c.Other"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1"));
    ASSERT_NOT_NULL(strstr(jt_props, "\"line\":5"));
    ASSERT_EQ(jt_edge(db, "c.Holder.value#field", "c.Other"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"line\":3"));
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE rel_path = "
                           "'src/main/java/c/Holder.java' AND raw = 'Other' AND line = 3"),
              0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/c/Holder.java", "#value"), "");
    ASSERT_EQ(jt_edge(db, "c.Holder.value", "c.Holder.value#field"), 1);
    jt_end(&r);
    PASS();
}

/* A Kotlin class and a function of its name at one scope level (the factory
 * idiom, `class Json` beside `fun Json(..)`): KDoc resolves the classifier
 * first, so the name is the CLASS, with the tier the lookup gives. A class
 * without a node of its own is a graph gap. The function never stands in for
 * the class, with a node or without. */
TEST(doc_mentions_jvm_kotlin_type_and_function) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "tfun"));
    /* class and factory in different files: each has its node. Two factory
     * overloads: the function side is a group, which changes nothing */
    jt_write(&r, "src/main/kotlin/f/Json.kt",
             "package f\n\nclass Json {\n    fun encode() {}\n}\n");
    jt_write(&r, "src/main/kotlin/f/Builders.kt",
             "package f\n"
             "\n"
             "fun Json(strict: Boolean): Json = Json()\n"
             "\n"
             "fun Json(name: String): Json = Json()\n");
    /* class and factory in ONE file share the file-based qualified name
     * `f.Alias.Alias`; the function, declared later, owns the node, and the
     * class has none */
    jt_write(&r, "src/main/kotlin/f/Alias.kt",
             "package f\n"
             "\n"
             "class Alias {\n"
             "    fun name() {}\n"
             "}\n"
             "\n"
             "fun Alias(text: String): Alias = Alias()\n");
    /* the same idiom inside a type: a nested class and a member function */
    jt_write(&r, "src/main/kotlin/f/Holder.kt",
             "package f\n"
             "\n"
             "class Holder {\n"
             "    class Part\n"
             "    fun Part(size: Int): Part = Part()\n"
             "}\n");
    /* not the idiom: a class and a PROPERTY of one name stay ambiguous */
    jt_write(&r, "src/main/kotlin/f/Mode.kt", "package f\n\nclass Mode\n");
    jt_write(&r, "src/main/kotlin/f/Modes.kt", "package f\n\nval Mode: Int = 1\n");
    jt_write(&r, "src/main/kotlin/f/User.kt",
             "package f\n"
             "\n"
             "/** A [Json] with [Json.encode], a [Holder.Part], a [Mode]. */\n"
             "class User\n");
    jt_write(&r, "src/main/kotlin/f/GapUser.kt",
             "package f\n"
             "\n"
             "/** An [Alias]. */\n"
             "class GapUser\n");
    /* the block tags that name code resolve like a link ... */
    jt_write(&r, "src/main/kotlin/f/TagUser.kt",
             "package f\n"
             "\n"
             "/**\n"
             " * @see Json\n"
             " * @throws Json never\n"
             " * @see Alias\n"
             " */\n"
             "class TagUser\n");
    /* ... but a sample is a function: there the two stay ambiguous */
    jt_write(&r, "src/main/kotlin/f/SampleUser.kt",
             "package f\n"
             "\n"
             "/** @sample f.Json */\n"
             "class SampleUser\n");
    /* an explicit import brings the class and the function: the class, exact */
    jt_write(&r, "src/main/kotlin/g/Importer.kt",
             "package g\n"
             "\n"
             "import f.Json\n"
             "\n"
             "/** A [Json]. */\n"
             "class Importer\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* the class has a node: an edge to the class, with the tier of the level
     * that found it (the file's package: unique), and no row */
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Json.Json"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"kdoc_link\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"unique\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":1"));
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/f/User.kt", "Json"), "");
    /* ... never to the function */
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Builders.Json"), 0);
    /* a qualifier is a type: the function is no candidate there */
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Json.Json.encode"), 1);
    /* inside a type: the nested class, not the member function */
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Holder.Part"), 1);
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Holder.Holder.Part"), 0);
    /* a class and a property: ambiguous, as before */
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/f/User.kt", "Mode"), "ambiguous");
    ASSERT_EQ(jt_edge(db, "f.User.User", "f.Mode.Mode"), 0);
    ASSERT_EQ(dm_mentions_from(db, "f.User.User"), 3);
    /* the class has NO node: a graph gap, and no edge at all -- the node at
     * `f.Alias.Alias` is the function's */
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/f/GapUser.kt", "Alias"), "graph_gap");
    ASSERT_EQ(jt_edge(db, "f.GapUser.GapUser", "f.Alias.Alias"), 0);
    ASSERT_EQ(dm_mentions_from(db, "f.GapUser.GapUser"), 0);
    /* @see and @throws: the class (one edge for both references), and the gap */
    ASSERT_EQ(jt_edge(db, "f.TagUser.TagUser", "f.Json.Json"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"see\""));
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":2"));
    ASSERT_EQ(jt_edge(db, "f.TagUser.TagUser", "f.Builders.Json"), 0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/f/TagUser.kt", "Alias"), "graph_gap");
    ASSERT_EQ(dm_mentions_from(db, "f.TagUser.TagUser"), 1);
    /* @sample: neither */
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/f/SampleUser.kt", "f.Json"), "ambiguous");
    ASSERT_EQ(dm_mentions_from(db, "f.SampleUser.SampleUser"), 0);
    /* through an explicit import: the class, exact */
    ASSERT_EQ(jt_edge(db, "g.Importer.Importer", "f.Json.Json"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"tier\":\"exact\""));
    ASSERT_EQ(jt_edge(db, "g.Importer.Importer", "f.Builders.Json"), 0);
    ASSERT_EQ(dm_mentions_from(db, "g.Importer.Importer"), 1);
    jt_end(&r);
    PASS();
}

/* ── parse errors ────────────────────────────────────────────────── */

TEST(doc_mentions_jvm_scope_parse_errors) {
    /* an error inside a method's own block: the type is complete */
    CBMFileResult *r = dm_extract("package pe;\n"
                                  "public class InBody {\n"
                                  "    public void before() {}\n"
                                  "    public void bad() {\n"
                                  "        int x = ;\n"
                                  "        foo(;\n"
                                  "    }\n"
                                  "    public void after() {}\n"
                                  "}\n",
                                  CBM_LANG_JAVA, "pe/InBody.java");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT(jt_has_line(r->doc_scope, "T\t2\t9\tc\tInBody\t\t\t=\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "M\t4\tc\t\t0\tbad\t\t\t\t=\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "M\t8\tc\t\t0\tafter\t\t\t\t=\t1\t"));
    cbm_free_result(r);
    /* an error among the members: the type does not show all of them ('!'),
     * and the member the error hit is not listed */
    r = dm_extract("package pe;\n"
                   "public class Among {\n"
                   "    public void before() {}\n"
                   "    public void bad( { }\n"
                   "    public void after() {}\n"
                   "}\n",
                   CBM_LANG_JAVA, "pe/Among.java");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT(jt_has_line(r->doc_scope, "T\t2\t6\tc!\tAmong\t\t\t=\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "M\t3\tc\t\t0\tbefore\t\t\t\t=\t1\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\tbad\t"));
    cbm_free_result(r);
    /* braces that do not pair: nothing behind the first one is placed. The
     * names declared there are kept apart (Q), the lines marked (X) */
    r = dm_extract("package pe;\n"
                   "public class Open {\n"
                   "    public void before() {}\n"
                   "    public static class Lost {\n"
                   "        public void inLost() {\n"
                   "    }\n"
                   "    public void after() {}\n"
                   "}\n",
                   CBM_LANG_JAVA, "pe/Open.java");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NULL(strstr(r->doc_scope, "\nT\t"));
    ASSERT(jt_has_line(r->doc_scope, "Q\tOpen"));
    ASSERT(jt_has_line(r->doc_scope, "Q\tLost"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nX\t2\t"));
    cbm_free_result(r);
    /* Kotlin: an error held by braces inside a member (an accessor's body, a
     * `when`) does not reach past the member: the type is complete */
    r = dm_extract("package pe\n"
                   "\n"
                   "class Getter {\n"
                   "    val ok: Int = 1\n"
                   "    val broken: Int\n"
                   "        get() {\n"
                   "            val x = when (this) {\n"
                   "                is Getter if true -> 1\n"
                   "                else -> 2\n"
                   "            }\n"
                   "            return x\n"
                   "        }\n"
                   "    fun bad(e: Any): Int = when (e) {\n"
                   "        is String if e.isEmpty() -> 1\n"
                   "        else -> 2\n"
                   "    }\n"
                   "    fun after() {}\n"
                   "}\n",
                   CBM_LANG_KOTLIN, "pe/Getter.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT(jt_has_line(r->doc_scope, "T\t3\t18\tc\tGetter\t\t\t=\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "M\t5\tv\t\t0\tbroken\t\t-\t\tbroken\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "M\t13\tc\t\t0\tbad\t\tAny\t\t=\t1\te"));
    ASSERT(jt_has_line(r->doc_scope, "M\t17\tc\t\t0\tafter\t\t\t\t=\t1\t"));
    cbm_free_result(r);
    /* ... while one among the members leaves it incomplete */
    r = dm_extract("package pe\n"
                   "\n"
                   "class KStray {\n"
                   "    fun before() {}\n"
                   "    ) ) )\n"
                   "    fun after() {}\n"
                   "}\n",
                   CBM_LANG_KOTLIN, "pe/KStray.kt");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT(jt_has_line(r->doc_scope, "T\t3\t7\tc!\tKStray\t\t\t=\t1\t"));
    ASSERT(jt_has_line(r->doc_scope, "X\t5\t5"));
    cbm_free_result(r);
    /* types nested deeper than any program nests them: the scan stops
     * descending, marks the rest unplaced, and does not overflow */
    enum { JT_DEEP = 400 };
    size_t deep_cap = (size_t)JT_DEEP * 40 + 64;
    char *deep = malloc(deep_cap);
    ASSERT_NOT_NULL(deep);
    size_t w = (size_t)snprintf(deep, deep_cap, "package pe;\n");
    for (int i = 0; i < JT_DEEP; i++) {
        w += (size_t)snprintf(deep + w, deep_cap - w, "class D%d {\n", i);
    }
    for (int i = 0; i < JT_DEEP; i++) {
        w += (size_t)snprintf(deep + w, deep_cap - w, "}\n");
    }
    r = dm_extract(deep, CBM_LANG_JAVA, "pe/Deep.java");
    free(deep);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\tD0.D1.D2\t"));
    ASSERT_NULL(strstr(r->doc_scope,
                       "\tD0.D1.D2.D3.D4.D5.D6.D7.D8.D9.D10.D11.D12.D13.D14.D15.D16."
                       "D17.D18.D19.D20.D21.D22.D23.D24.D25.D26.D27.D28.D29.D30.D31."
                       "D32.D33.D34.D35.D36.D37.D38.D39.D40.D41.D42.D43.D44.D45.D46."
                       "D47.D48.D49.D50.D51.D52.D53.D54.D55.D56.D57.D58.D59.D60.D61."
                       "D62.D63.D64.D65.D66.D67.D68.D69.D70.D71.D72.D73.D74.D75.D76."
                       "D77.D78.D79.D80.D81.D82.D83.D84.D85.D86.D87.D88.D89.D90.D91."
                       "D92.D93.D94.D95.D96.D97.D98.D99.D100.D101.D102.D103.D104.D105."
                       "D106.D107.D108.D109.D110.D111.D112.D113.D114.D115.D116.D117."
                       "D118.D119.D120.D121.D122.D123.D124.D125.D126.D127.D128\t"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nQ\tD128\n"));
    cbm_free_result(r);
    PASS();
}

/* What the resolver does with those scopes: a member a parse error may hide
 * is a graph gap (never "missing", never an outer declaration of that name);
 * a name declared where the scan lost its place is not resolved around. */
TEST(doc_mentions_jvm_resolver_parse_errors) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "perr"));
    jt_write(&r, "src/main/java/pe/Among.java",
             "package pe;\n"
             "public class Among {\n"
             "    /** {@link #bad()} {@link #after()} {@link Whole#gone()} */\n"
             "    public void before() {}\n"
             "    public void bad( { }\n"
             "    public void after() {}\n"
             "}\n");
    jt_write(&r, "src/main/java/pe/Whole.java",
             "package pe;\n"
             "/** {@link Among#bad()} {@link Among#before()} {@link Open} {@link Lost} */\n"
             "public class Whole {}\n");
    jt_write(&r, "src/main/java/pe/Open.java",
             "package pe;\n"
             "public class Open {\n"
             "    public void before() {}\n"
             "    public static class Lost {\n"
             "        public void inLost() {\n"
             "    }\n"
             "    public void after() {}\n"
             "}\n");
    /* the same simple name, placed, in a package imported on demand: the
     * unplaced declaration could be the one meant, so it does not bind */
    jt_write(&r, "src/main/java/other/Lost.java", "package other;\npublic class Lost {}\n");
    jt_write(&r, "src/main/java/pe/Importer.java",
             "package pe;\n"
             "import other.*;\n"
             "/** {@link Lost} */\n"
             "public class Importer {}\n");
    jt_write(&r, "src/main/kotlin/pk/Getter.kt",
             "package pk\n"
             "\n"
             "class Getter {\n"
             "    val ok: Int = 1\n"
             "    val broken: Int\n"
             "        get() {\n"
             "            val x = when (this) {\n"
             "                is Getter if true -> 1\n"
             "                else -> 2\n"
             "            }\n"
             "            return x\n"
             "        }\n"
             "    /**\n"
             "     * [ok], [broken]\n"
             "     * @see Getter.nope\n"
             "     */\n"
             "    fun after() {}\n"
             "}\n");
    jt_write(&r, "src/main/kotlin/pk/KStray.kt",
             "package pk\n"
             "\n"
             "fun topLevel() {}\n"
             "\n"
             "class KStray {\n"
             "    fun own() {}\n"
             "    /**\n"
             "     * [own], [topLevel], [Getter]\n"
             "     * @see KStray.hidden\n"
             "     */\n"
             "    fun documented() {}\n"
             "    ) ) )\n"
             "    fun after() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* Java, inside the incomplete type: its parsed members bind; the one the
     * error hid is a gap; a complete type's absent member is missing */
    ASSERT_EQ(jt_edge(db, "pe.Among.before", "pe.Among.after"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Among.java", "#bad()"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Among.java", "Whole#gone()"), "missing");
    /* ... and named from outside */
    ASSERT_EQ(jt_edge(db, "pe.Whole", "pe.Among.before"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Whole.java", "Among#bad()"), "graph_gap");
    /* names declared where the braces do not pair */
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Whole.java", "Open"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Whole.java", "Lost"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, "src/main/java/pe/Importer.java", "Lost"), "graph_gap");
    ASSERT_EQ(jt_edge(db, "pe.Importer", "other.Lost"), 0);
    /* Kotlin: errors held inside a member's braces leave the type whole */
    ASSERT_EQ(jt_edge(db, "pk.Getter.Getter.after", "pk.Getter.ok"), 1);
    ASSERT_EQ(jt_edge(db, "pk.Getter.Getter.after", "pk.Getter.broken"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/pk/Getter.kt", "Getter.nope"), "missing");
    /* Kotlin, inside an incomplete type: its own parsed member binds, a TYPE
     * further out binds (a type the scan lost is kept apart by name), a
     * MEMBER further out does not (a hidden member could shadow it) */
    ASSERT_EQ(jt_edge(db, "pk.KStray.KStray.documented", "pk.KStray.KStray.own"), 1);
    ASSERT_EQ(jt_edge(db, "pk.KStray.KStray.documented", "pk.Getter.Getter"), 1);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/pk/KStray.kt", "topLevel"), "graph_gap");
    ASSERT_EQ(jt_edge(db, "pk.KStray.KStray.documented", "pk.KStray.topLevel"), 0);
    ASSERT_STR_EQ(jt_reason(db, "src/main/kotlin/pk/KStray.kt", "KStray.hidden"), "graph_gap");
    jt_end(&r);
    PASS();
}

/* ── test code ───────────────────────────────────────────────────── */

/* Where a build puts test code, in the layouts the two languages use. In
 * each a product class names a test-only class (a row) and the test class
 * names the product class (an edge). */
TEST(doc_mentions_jvm_test_only_layouts) {
    static const struct {
        const char *dir; /* the directory of the test class */
        const char *name;
        bool test;
    } cases[] = {
        {"src/test/kotlin/t", "InSrcTest", true},
        {"src/jvmTest/kotlin/t", "InSourceSet", true},
        {"src/testFixtures/kotlin/t", "InFixtures", true},
        {"core/commonTest/src/t", "SetBeforeSrc", true},
        {"core/common/test/t", "PlainTestDir", true},
        {"json-tests/src/main/kotlin/t", "TestProject", true},
        {"samples/app/src/main/kotlin/t", "InSamples", true},
        /* a directory BELOW the source set is a package, and a name that
         * merely contains the letters is no test directory */
        {"src/main/kotlin/t/test", "PackageNamedTest", false},
        {"latest/src/main/kotlin/t", "InLatest", false},
    };
    jt_repo_t r;
    ASSERT(jt_begin(&r, "tonly"));
    char doc[2048] = "package t\n\n/**";
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char path[512];
        char body[512];
        snprintf(path, sizeof(path), "%s/%s.kt", cases[i].dir, cases[i].name);
        snprintf(body, sizeof(body), "package t\n\n/** Tests [Product]. */\nclass %s\n",
                 cases[i].name);
        jt_write(&r, path, body);
        size_t used = strlen(doc);
        snprintf(doc + used, sizeof(doc) - used, " [%s]", cases[i].name);
    }
    size_t used = strlen(doc);
    snprintf(doc + used, sizeof(doc) - used, " */\nclass Product\n");
    jt_write(&r, "src/main/kotlin/t/Product.kt", doc);
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char sql[512];
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                 "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                 "AND s.name = 'Product' AND t.name = '%s'",
                 cases[i].name);
        const char *reason = jt_reason(r.db, "src/main/kotlin/t/Product.kt", cases[i].name);
        if (dm_count(r.db, sql) != (cases[i].test ? 0 : 1) ||
            strcmp(reason, cases[i].test ? "test_only_target" : "") != 0) {
            printf("  %s/%s: edges %d, reason '%s'\n", cases[i].dir, cases[i].name,
                   dm_count(r.db, sql), reason);
        }
        ASSERT_EQ(dm_count(r.db, sql), cases[i].test ? 0 : 1);
        ASSERT_STR_EQ(reason, cases[i].test ? "test_only_target" : "");
        /* every one of them, test or not, may name the product class */
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                 "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
                 "AND s.name = '%s' AND t.name = 'Product'",
                 cases[i].name);
        ASSERT_EQ(dm_count(r.db, sql), 1);
    }
    jt_end(&r);
    PASS();
}

/* ── the family table ────────────────────────────────────────────── */

/* The families' names are public surface (the "syntax" edge property and
 * column); each belongs to its language and ships. */
TEST(doc_mentions_jvm_family_names) {
    static const struct {
        int syntax;
        CBMLanguage lang;
        const char *name;
    } families[] = {
        {CBM_DOCLINK_JAVA_LINK, CBM_LANG_JAVA, "link"},
        {CBM_DOCLINK_JAVA_LINKPLAIN, CBM_LANG_JAVA, "linkplain"},
        {CBM_DOCLINK_JAVA_SEE, CBM_LANG_JAVA, "see"},
        {CBM_DOCLINK_JAVA_THROWS, CBM_LANG_JAVA, "throws"},
        {CBM_DOCLINK_JAVA_VALUE, CBM_LANG_JAVA, "value"},
        {CBM_DOCLINK_KT_KDOC_LINK, CBM_LANG_KOTLIN, "kdoc_link"},
        {CBM_DOCLINK_KT_SEE, CBM_LANG_KOTLIN, "see"},
        {CBM_DOCLINK_KT_THROWS, CBM_LANG_KOTLIN, "throws"},
        {CBM_DOCLINK_KT_SAMPLE, CBM_LANG_KOTLIN, "sample"},
    };
    /* both languages have a doc-reference parser */
    ASSERT_TRUE(cbm_doclink_lang_supported(CBM_LANG_JAVA));
    ASSERT_TRUE(cbm_doclink_lang_supported(CBM_LANG_KOTLIN));
    for (size_t i = 0; i < sizeof(families) / sizeof(families[0]); i++) {
        const CBMDocLinkFamily *f = cbm_doclink_family(families[i].syntax);
        ASSERT_NOT_NULL(f);
        ASSERT_EQ(f->lang, families[i].lang);
        ASSERT_STR_EQ(f->name, families[i].name);
        ASSERT_STR_EQ(cbm_doclink_syntax_name(families[i].syntax), families[i].name);
        ASSERT_FALSE(cbm_doclink_syntax_is_external(families[i].syntax));
    }
    /* all ship in production: their held-out audit passed */
    cbm_doclink_test_reset_ships();
    int shipping_in_production = 0;
    for (size_t i = 0; i < sizeof(families) / sizeof(families[0]); i++) {
        shipping_in_production += cbm_doclink_syntax_ships(families[i].syntax) ? 1 : 0;
    }
    dm_ship_held_families();
    ASSERT_EQ(shipping_in_production, (int)(sizeof(families) / sizeof(families[0])));
    for (size_t i = 0; i < sizeof(families) / sizeof(families[0]); i++) {
        ASSERT_TRUE(cbm_doclink_syntax_ships(families[i].syntax));
    }
    PASS();
}

/* ── which scope changes are repairable file by file ─────────────── */

#define JT_DJ_HEAD "package n;\nimport acme.local.Base;\nimport acme.util.Tool;\n"
#define JT_DJ_OPEN "public class W extends Base {\n"
#define JT_DJ_RUN_INT "    public void run(int n) {}\n"
#define JT_DJ_RUN_STR "    public void run(String s) {}\n"
#define JT_DJ_SIZE "    public int size;\n"
#define JT_DJ_CLOSE "}\nclass Other {}\n"
#define JT_DJ_BODY JT_DJ_RUN_INT JT_DJ_RUN_STR JT_DJ_SIZE
#define JT_DJ_TYPES JT_DJ_OPEN JT_DJ_BODY JT_DJ_CLOSE
#define JT_DJ_BASE JT_DJ_HEAD JT_DJ_TYPES
/* no declaration header of this file names a type */
#define JT_DJ_PLAIN "public class Plain {\n    public void go() {}\n}\n"

#define JT_DK_HEAD "package n\n\nimport acme.local.Base\nimport acme.util.Tool\n\n"
#define JT_DK_OPEN "class W(val id: Int) : Base() {\n"
#define JT_DK_RUN_INT "    fun run(n: Int) {}\n"
#define JT_DK_RUN_STR "    fun run(s: String) {}\n"
#define JT_DK_SIZE "    val size: Int = 0\n"
#define JT_DK_CLOSE "}\n\nfun Base.ext() {}\n\nclass Other\n"
#define JT_DK_BODY JT_DK_RUN_INT JT_DK_RUN_STR JT_DK_SIZE
#define JT_DK_TYPES JT_DK_OPEN JT_DK_BODY JT_DK_CLOSE
#define JT_DK_BASE JT_DK_HEAD JT_DK_TYPES
#define JT_DK_PLAIN "class Plain {\n    fun go() {}\n}\n"

TEST(doc_mentions_jvm_scope_delta_rules) {
    const int local = CBM_DOCLINK_DELTA_LOCAL;
    const int global = CBM_DOCLINK_DELTA_GLOBAL;
    const CBMLanguage java = CBM_LANG_JAVA;
    const CBMLanguage kotlin = CBM_LANG_KOTLIN;
    struct {
        const char *what;
        CBMLanguage lang;
        const char *before;
        const char *after;
        int want;
        const char *names; /* NULL: any */
    } cases[] = {
        /* ── Java ── */
        {"java unchanged", java, JT_DJ_BASE, JT_DJ_BASE, local, ""},
        /* nothing of the persisted scope carries a line number, a body or a
         * doc comment */
        {"java lines moved", java, JT_DJ_BASE, "// moved\n\n" JT_DJ_BASE, local, ""},
        {"java body edit", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN
         "    public void run(int n) { n++; }\n" JT_DJ_RUN_STR JT_DJ_SIZE JT_DJ_CLOSE,
         local, ""},
        {"java doc edit", java, JT_DJ_BASE, JT_DJ_HEAD "/** {@link Other} */\n" JT_DJ_TYPES, local,
         ""},
        /* imports scope the file itself ... */
        {"java import added", java, JT_DJ_BASE, JT_DJ_HEAD "import acme.more.Thing;\n" JT_DJ_TYPES,
         local, ""},
        {"java import removed", java, JT_DJ_BASE,
         "package n;\nimport acme.local.Base;\n" JT_DJ_TYPES, local, ""},
        {"java static import added", java, JT_DJ_BASE,
         JT_DJ_HEAD "import static acme.S.max;\n" JT_DJ_TYPES, local, ""},
        {"java on-demand import, no header names a type", java, "package n;\n" JT_DJ_PLAIN,
         "package n;\nimport acme.more.*;\n" JT_DJ_PLAIN, local, ""},
        /* ... except for the names its declaration headers write: other files
         * resolve inherited members through the supertype they bind */
        {"java supertype's import changed", java, JT_DJ_BASE,
         "package n;\nimport acme.other.Base;\nimport acme.util.Tool;\n" JT_DJ_TYPES, global, NULL},
        {"java supertype's import removed", java, JT_DJ_BASE,
         "package n;\nimport acme.util.Tool;\n" JT_DJ_TYPES, global, NULL},
        {"java on-demand import added", java, JT_DJ_BASE,
         JT_DJ_HEAD "import acme.more.*;\n" JT_DJ_TYPES, global, NULL},
        {"java static on-demand import added", java, JT_DJ_BASE,
         JT_DJ_HEAD "import static acme.S.*;\n" JT_DJ_TYPES, global, NULL},
        {"java static import of the supertype's name", java, JT_DJ_BASE,
         JT_DJ_HEAD "import static acme.S.Base;\n" JT_DJ_TYPES, global, NULL},
        /* a removed member is reported by name */
        {"java overload removed", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_STR JT_DJ_SIZE JT_DJ_CLOSE, local, "run"},
        {"java field removed", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_INT JT_DJ_RUN_STR JT_DJ_CLOSE, local, "size"},
        {"java two members removed", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_STR JT_DJ_CLOSE, local, "run,size"},
        {"java constructor removed", java,
         "package n;\npublic class C {\n    public C(int n) {}\n    public void m() {}\n}\n",
         "package n;\npublic class C {\n    public void m() {}\n}\n", local, "C"},
        {"java enum constant removed", java, "package n;\npublic enum Color { RED, GREEN }\n",
         "package n;\npublic enum Color { RED }\n", local, "GREEN"},
        /* ... unless the overloads' one node was its own: the node moves to
         * the declaration that stays */
        {"java node owner removed", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_INT JT_DJ_SIZE JT_DJ_CLOSE, global, NULL},
        /* everything else can re-route references of files with no edge here */
        {"java member added", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_BODY "    public int more;\n" JT_DJ_CLOSE, global, NULL},
        {"java signature changed", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN
         "    public void run(long n) {}\n" JT_DJ_RUN_STR JT_DJ_SIZE JT_DJ_CLOSE,
         global, NULL},
        {"java members reordered", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_STR JT_DJ_RUN_INT JT_DJ_SIZE JT_DJ_CLOSE, global, NULL},
        /* the first line of a field's declarator is part of its node's name */
        /* a field's node is named by the field alone: a new initializer
         * declares the same names */
        {"java field initializer", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_RUN_INT JT_DJ_RUN_STR "    public int size = 1;\n" JT_DJ_CLOSE,
         local, ""},
        {"java type removed", java, JT_DJ_BASE, JT_DJ_HEAD JT_DJ_OPEN JT_DJ_BODY "}\n", global,
         NULL},
        {"java type added", java, JT_DJ_BASE, JT_DJ_BASE "class New {}\n", global, NULL},
        {"java supertype changed", java, JT_DJ_BASE,
         JT_DJ_HEAD "public class W extends Tool {\n" JT_DJ_BODY JT_DJ_CLOSE, global, NULL},
        {"java type parameter bound", java, JT_DJ_BASE,
         JT_DJ_HEAD "public class W<T extends Other> extends Base {\n" JT_DJ_BODY JT_DJ_CLOSE,
         global, NULL},
        {"java package renamed", java, JT_DJ_BASE,
         "package m;\nimport acme.local.Base;\nimport acme.util.Tool;\n" JT_DJ_TYPES, global, NULL},
        /* a record's canonical constructor is matched against its components */
        {"java record component removed", java, "package n;\npublic record P(int x, int y) {}\n",
         "package n;\npublic record P(int x) {}\n", global, NULL},
        /* a member the parser can no longer show makes its type incomplete */
        {"java member hidden", java, JT_DJ_BASE,
         JT_DJ_HEAD JT_DJ_OPEN JT_DJ_BODY "    public void bad( { }\n" JT_DJ_CLOSE, global, NULL},
        {"java file deleted", java, JT_DJ_BASE, NULL, global, NULL},
        {"java file added", java, NULL, JT_DJ_BASE, global, NULL},
        /* ── Kotlin ── */
        {"kotlin unchanged", kotlin, JT_DK_BASE, JT_DK_BASE, local, ""},
        {"kotlin lines moved", kotlin, JT_DK_BASE, "// moved\n\n" JT_DK_BASE, local, ""},
        {"kotlin body edit", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN
         "    fun run(n: Int) { println(n) }\n" JT_DK_RUN_STR JT_DK_SIZE JT_DK_CLOSE,
         local, ""},
        {"kotlin property initializer", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_RUN_INT JT_DK_RUN_STR "    val size: Int = 1\n" JT_DK_CLOSE,
         local, ""},
        {"kotlin import added", kotlin, JT_DK_BASE,
         "package n\n\nimport acme.local.Base\nimport acme.more.Thing\nimport "
         "acme.util.Tool\n\n" JT_DK_TYPES,
         local, ""},
        {"kotlin import alias that no header writes", kotlin, JT_DK_BASE,
         "package n\n\nimport acme.local.Base\nimport acme.more.Thing as Renamed\n"
         "import acme.util.Tool\n\n" JT_DK_TYPES,
         local, ""},
        {"kotlin star import, no header names a type", kotlin, "package n\n\n" JT_DK_PLAIN,
         "package n\n\nimport acme.more.*\n\n" JT_DK_PLAIN, local, ""},
        /* a supertype and an extension's receiver are bound in the file's scope */
        {"kotlin import alias equal to the supertype", kotlin, JT_DK_BASE,
         "package n\n\nimport acme.other.Thing as Base\nimport acme.util.Tool\n\n" JT_DK_TYPES,
         global, NULL},
        {"kotlin star import added", kotlin, JT_DK_BASE,
         "package n\n\nimport acme.local.Base\nimport acme.more.*\nimport "
         "acme.util.Tool\n\n" JT_DK_TYPES,
         global, NULL},
        {"kotlin receiver's import changed", kotlin,
         "package n\n\nimport acme.local.Recv\n\nfun Recv.ext() {}\n",
         "package n\n\nimport acme.other.Recv\n\nfun Recv.ext() {}\n", global, NULL},
        {"kotlin overload removed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_RUN_STR JT_DK_SIZE JT_DK_CLOSE, local, "run"},
        {"kotlin property removed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_RUN_INT JT_DK_RUN_STR JT_DK_CLOSE, local, "size"},
        {"kotlin extension removed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_BODY "}\n\nclass Other\n", local, "ext"},
        /* a constructor property goes with the constructor's signature */
        {"kotlin constructor property removed", kotlin, JT_DK_BASE,
         JT_DK_HEAD "class W() : Base() {\n" JT_DK_BODY JT_DK_CLOSE, global, NULL},
        {"kotlin member added", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_BODY "    val more: Int = 0\n" JT_DK_CLOSE, global, NULL},
        {"kotlin signature changed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN "    fun run(n: Long) {}\n" JT_DK_RUN_STR JT_DK_SIZE JT_DK_CLOSE,
         global, NULL},
        {"kotlin receiver changed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_BODY "}\n\nfun Other.ext() {}\n\nclass Other\n", global, NULL},
        {"kotlin type removed", kotlin, JT_DK_BASE,
         JT_DK_HEAD JT_DK_OPEN JT_DK_BODY "}\n\nfun Base.ext() {}\n", global, NULL},
        {"kotlin type added", kotlin, JT_DK_BASE, JT_DK_BASE "\nclass New\n", global, NULL},
        {"kotlin supertype changed", kotlin, JT_DK_BASE,
         JT_DK_HEAD "class W(val id: Int) : Tool() {\n" JT_DK_BODY JT_DK_CLOSE, global, NULL},
        {"kotlin package renamed", kotlin, JT_DK_BASE,
         "package m\n\nimport acme.local.Base\nimport acme.util.Tool\n\n" JT_DK_TYPES, global,
         NULL},
        {"kotlin type alias target", kotlin, "package n\n\nclass A\nclass B\ntypealias N = A\n",
         "package n\n\nclass A\nclass B\ntypealias N = B\n", global, NULL},
        {"kotlin file deleted", kotlin, JT_DK_BASE, NULL, global, NULL},
        {"kotlin file added", kotlin, NULL, JT_DK_BASE, global, NULL},
    };
    char names[256];
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *rel = cases[i].lang == java ? "n/W.java" : "n/W.kt";
        int got = dm_scope_delta(cases[i].lang, rel, cases[i].before, cases[i].after, names,
                                 sizeof(names));
        if (got != cases[i].want || (cases[i].names && strcmp(names, cases[i].names) != 0)) {
            printf("  %s: delta %d [%s], want %d [%s]\n", cases[i].what, got, names, cases[i].want,
                   cases[i].names ? cases[i].names : "any");
            FAIL("scope delta");
        }
    }
    PASS();
}

/* ── incremental == full ─────────────────────────────────────────── */

#define JT_I_MAIN_IMPORTS  \
    "package app;\n"       \
    "import lib.Ov;\n"     \
    "import lib.Sig;\n"    \
    "import lib.Target;\n" \
    "import p.*;\n"        \
    "import q.*;\n"
#define JT_I_MAIN_DOC                                                                      \
    "/**\n"                                                                                \
    " * {@link Target}, {@link Target#go(int)}, {@link Target#LIMIT}, {@link Ov#run},\n"   \
    " * {@link Dup}, {@link Extra}, {@link Sig#go(int)}, {@link d.Derived#inherited()},\n" \
    " * {@link kt.KHelper}, {@link lib.Gone#m()}"
#define JT_I_MAIN_END "\n */\npublic class Main {}\n"
static const char JT_I_MAIN[] = JT_I_MAIN_IMPORTS JT_I_MAIN_DOC JT_I_MAIN_END;
static const char JT_I_MAIN_EDITED[] =
    JT_I_MAIN_IMPORTS JT_I_MAIN_DOC ", {@link Ov#run(String)}" JT_I_MAIN_END;
static const char JT_I_MAIN_IMPORT[] =
    JT_I_MAIN_IMPORTS "import s.Extra;\n" JT_I_MAIN_DOC ", {@link Ov#run(String)}" JT_I_MAIN_END;
static const char JT_I_TARGET[] = "package lib;\n"
                                  "public class Target {\n"
                                  "    public static final int LIMIT = 10;\n"
                                  "    public void go(int n) {}\n"
                                  "}\n";
static const char JT_I_TARGET_INIT[] = "package lib;\n"
                                       "public class Target {\n"
                                       "    public static final int LIMIT = 20;\n"
                                       "    public void go(int n) {}\n"
                                       "}\n";
static const char JT_I_TARGET_RENAMED[] = "package lib;\n"
                                          "public class Renamed {\n"
                                          "    public static final int LIMIT = 20;\n"
                                          "    public void go(int n) {}\n"
                                          "}\n";
/* one node for both overloads; it belongs to the later declaration */
static const char JT_I_OV[] = "package lib;\n"
                              "public class Ov {\n"
                              "    public void run(int n) {}\n"
                              "    public void run(String s) {}\n"
                              "}\n";
static const char JT_I_OV_ONE[] = "package lib;\n"
                                  "public class Ov {\n"
                                  "    public void run(String s) {}\n"
                                  "}\n";
static const char JT_I_SIG[] = "package lib;\n"
                               "public class Sig {\n"
                               "    public void go(int n) {}\n"
                               "}\n";
static const char JT_I_SIG_BODY[] = "package lib;\n"
                                    "public class Sig {\n"
                                    "    public void go(int n) { n++; }\n"
                                    "}\n";
static const char JT_I_SIG_MOVED[] = "// moved\n"
                                     "\n"
                                     "package lib;\n"
                                     "\n"
                                     "public class Sig {\n"
                                     "\n"
                                     "    public void go(int n) { n++; }\n"
                                     "}\n";
static const char JT_I_P[] = "package p;\n"
                             "public class Dup {}\n"
                             "class Keep {}\n";
static const char JT_I_P_NO_DUP[] = "package p;\n"
                                    "class Keep {}\n";
static const char JT_I_DERIVED[] = "package d;\n"
                                   "import lib.Base;\n"
                                   "public class Derived extends Base {}\n";
static const char JT_I_DERIVED_ALT[] = "package d;\n"
                                       "import alt.Base;\n"
                                       "public class Derived extends Base {}\n";
static const char JT_I_KHELPER[] = "package kt\n"
                                   "\n"
                                   "import lib.Target\n"
                                   "\n"
                                   "/**\n"
                                   " * [Target], [Target.go], [help]\n"
                                   " * @see lib.Ov.run\n"
                                   " * @see KOther.gone\n"
                                   " */\n"
                                   "class KHelper {\n"
                                   "    fun help() {}\n"
                                   "}\n";
static const char JT_I_KHELPER_RENAMED[] = "package kt\n"
                                           "\n"
                                           "import lib.Target\n"
                                           "\n"
                                           "/**\n"
                                           " * [Target], [Target.go], [help]\n"
                                           " * @see lib.Ov.run\n"
                                           " * @see KOther.gone\n"
                                           " */\n"
                                           "class KHelp {\n"
                                           "    fun help() {}\n"
                                           "}\n";
static const char JT_I_KOTHER[] = "package kt\n"
                                  "\n"
                                  "/** [KHelper.help] */\n"
                                  "class KOther {\n"
                                  "    fun gone() {}\n"
                                  "    fun stays() {}\n"
                                  "}\n";
static const char JT_I_KOTHER_EDITED[] = "package kt\n"
                                         "\n"
                                         "/** [KHelper.help], [KHelper] */\n"
                                         "class KOther {\n"
                                         "    fun gone() {}\n"
                                         "    fun stays() {}\n"
                                         "}\n";
static const char JT_I_KOTHER_NO_GONE[] = "package kt\n"
                                          "\n"
                                          "/** [KHelper.help], [KHelper] */\n"
                                          "class KOther {\n"
                                          "    fun stays() {}\n"
                                          "}\n";
/* an extension function: the import decides which `Recv` is its receiver */
static const char JT_I_EXT[] = "package kt\n"
                               "\n"
                               "import recv.one.Recv\n"
                               "\n"
                               "fun Recv.shine() {}\n";
static const char JT_I_EXT_OTHER[] = "package kt\n"
                                     "\n"
                                     "import recv.two.Recv\n"
                                     "\n"
                                     "fun Recv.shine() {}\n";
static const char JT_I_EXTUSER[] = "package kt\n"
                                   "\n"
                                   "import recv.one.Recv\n"
                                   "\n"
                                   "/** @see Recv.shine */\n"
                                   "class ExtUser\n";

TEST(doc_mentions_jvm_incremental_equals_full) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "inc"));
    const char *main_java = "src/main/java/app/Main.java";
    const char *khelper_kt = "src/main/kotlin/kt/KHelper.kt";
    jt_write(&r, main_java, JT_I_MAIN);
    jt_write(&r, "src/main/java/lib/Target.java", JT_I_TARGET);
    jt_write(&r, "src/main/java/lib/Ov.java", JT_I_OV);
    jt_write(&r, "src/main/java/lib/Sig.java", JT_I_SIG);
    jt_write(&r, "src/main/java/lib/Gone.java",
             "package lib;\npublic class Gone {\n    public void m() {}\n}\n");
    jt_write(&r, "src/main/java/lib/Base.java",
             "package lib;\npublic class Base {\n    public void inherited() {}\n}\n");
    jt_write(&r, "src/main/java/alt/Base.java",
             "package alt;\npublic class Base {\n    public void other() {}\n}\n");
    jt_write(&r, "src/main/java/d/Derived.java", JT_I_DERIVED);
    jt_write(&r, "src/main/java/p/Types.java", JT_I_P);
    jt_write(&r, "src/main/java/q/Dup.java", "package q;\npublic class Dup {}\n");
    jt_write(&r, "src/main/java/s/Extra.java", "package s;\npublic class Extra {}\n");
    jt_write(&r, khelper_kt, JT_I_KHELPER);
    jt_write(&r, "src/main/kotlin/kt/KOther.kt", JT_I_KOTHER);
    const char *ext_kt = "src/main/kotlin/kt/Ext.kt";
    const char *extuser_kt = "src/main/kotlin/kt/ExtUser.kt";
    jt_write(&r, "src/main/kotlin/recv/one/Recv.kt", "package recv.one\n\nclass Recv\n");
    jt_write(&r, "src/main/kotlin/recv/two/Recv.kt", "package recv.two\n\nclass Recv\n");
    jt_write(&r, ext_kt, JT_I_EXT);
    jt_write(&r, extuser_kt, JT_I_EXTUSER);
    char full_db[600];
    snprintf(full_db, sizeof(full_db), "%s/full.db", r.tmp);
    const char *db = r.db;
    const cbm_incremental_route_t repair = CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR;
    const cbm_incremental_route_t forced = CBM_INCREMENTAL_ROUTE_FORCED_FULL;
    ASSERT_EQ(dm_index(r.repo, db, NULL), 0);
    /* the preconditions the steps below move away from */
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target"), 1);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target.go"), 1);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target.LIMIT"), 1);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Sig.go"), 1);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Gone.m"), 1);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Base.inherited"), 1);    /* through d.Derived */
    ASSERT_EQ(jt_edge(db, "app.Main", "kt.KHelper.KHelper"), 1);    /* a Kotlin class */
    ASSERT_STR_EQ(jt_reason(db, main_java, "Ov#run"), "ambiguous"); /* an overload group */
    ASSERT_STR_EQ(jt_reason(db, main_java, "Dup"), "ambiguous");    /* p.* and q.* */
    ASSERT_STR_EQ(jt_reason(db, main_java, "Extra"), "missing");    /* s is not imported */
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "lib.Target"), 1);  /* a Java class */
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "lib.Target.go"), 1);
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "kt.KOther.KOther.gone"), 1);
    ASSERT_STR_EQ(jt_reason(db, khelper_kt, "lib.Ov.run"), "ambiguous");
    ASSERT_EQ(jt_edge(db, "kt.KOther.KOther", "kt.KHelper.KHelper.help"), 1);
    ASSERT_EQ(jt_edge(db, "kt.ExtUser.ExtUser", "kt.Ext.shine"), 1); /* an extension of Recv */

    /* a doc-comment edit re-extracts the file alone */
    jt_write(&r, main_java, JT_I_MAIN_EDITED);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "doc edit", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Ov.run"), 1);

    /* a body edit in a TARGET file: its scope is unchanged, and the edges
     * into it stand */
    jt_write(&r, "src/main/java/lib/Sig.java", JT_I_SIG_BODY);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "target body edit", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Sig.go"), 1);

    /* moving every declaration of a target file to another line changes no
     * scope: the stored scopes carry no line numbers */
    jt_write(&r, "src/main/java/lib/Sig.java", JT_I_SIG_MOVED);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "lines moved", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Sig.go"), 1);

    /* an import no declaration header of the file writes scopes the file
     * alone */
    jt_write(&r, main_java, JT_I_MAIN_IMPORT);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "import added", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "s.Extra"), 1);

    /* ... but the import that binds a supertype decides what OTHER files
     * inherit through the type: Main has no edge into Derived.java */
    jt_write(&r, "src/main/java/d/Derived.java", JT_I_DERIVED_ALT);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "supertype re-bound by an import", forced), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Base.inherited"), 0);
    ASSERT_STR_EQ(jt_reason(db, main_java, "d.Derived#inherited()"), "missing");

    /* the same for the import that binds an extension's RECEIVER: of which
     * type `shine` is an extension decides what other files find under
     * `Recv.shine`. Away from the type ExtUser names ... */
    jt_write(&r, ext_kt, JT_I_EXT_OTHER);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "receiver re-bound by an import", forced), 0);
    ASSERT_EQ(jt_edge(db, "kt.ExtUser.ExtUser", "kt.Ext.shine"), 0);
    ASSERT_STR_EQ(jt_reason(db, extuser_kt, "Recv.shine"), "missing");
    /* ... and back to it. ExtUser has no edge into Ext.kt now and Ext.kt
     * removed no name, so no file-by-file repair would look at ExtUser again */
    jt_write(&r, ext_kt, JT_I_EXT);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "receiver bound back by an import", forced), 0);
    ASSERT_EQ(jt_edge(db, "kt.ExtUser.ExtUser", "kt.Ext.shine"), 1);
    ASSERT_STR_EQ(jt_reason(db, extuser_kt, "Recv.shine"), "");

    /* a removed member (an overload group shrinks to one) re-resolves the
     * files whose rows name it, of either language, although they have no
     * edge into the changed file */
    jt_write(&r, "src/main/java/lib/Ov.java", JT_I_OV_ONE);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "member removed", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Ov.run"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":2"));
    ASSERT_STR_EQ(jt_reason(db, main_java, "Ov#run"), "");
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "lib.Ov.run"), 1);

    /* a Java field's node is named by the field alone: a changed initializer
     * keeps the node, and the edge stays on it */
    jt_write(&r, "src/main/java/lib/Target.java", JT_I_TARGET_INIT);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "field initializer", repair), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target.LIMIT"), 1);

    /* a removed TYPE is not repaired file by file (it may be another type's
     * supertype): one of two ambiguous candidates goes, the other binds */
    jt_write(&r, "src/main/java/p/Types.java", JT_I_P_NO_DUP);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "type removed", forced), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "q.Dup"), 1);

    /* renaming a target, named from both languages. Both files still import
     * the old name: an import of a type that the repository's own package
     * does not show vouches for a declaration the graph lacks */
    jt_write(&r, "src/main/java/lib/Target.java", JT_I_TARGET_RENAMED);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "rename", forced), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target"), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Target.go"), 0);
    ASSERT_STR_EQ(jt_reason(db, main_java, "Target"), "graph_gap");
    ASSERT_STR_EQ(jt_reason(db, main_java, "Target#go(int)"), "graph_gap");
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "lib.Target"), 0);
    ASSERT_STR_EQ(jt_reason(db, khelper_kt, "Target"), "graph_gap");

    /* Kotlin: a doc-comment edit */
    jt_write(&r, "src/main/kotlin/kt/KOther.kt", JT_I_KOTHER_EDITED);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "kotlin doc edit", repair), 0);
    ASSERT_EQ(jt_edge(db, "kt.KOther.KOther", "kt.KHelper.KHelper"), 1);

    /* Kotlin: a removed member takes the edge into it along */
    jt_write(&r, "src/main/kotlin/kt/KOther.kt", JT_I_KOTHER_NO_GONE);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "kotlin member removed", repair), 0);
    ASSERT_EQ(jt_edge(db, "kt.KHelper.KHelper", "kt.KOther.KOther.gone"), 0);
    ASSERT_STR_EQ(jt_reason(db, khelper_kt, "KOther.gone"), "missing");

    /* a deleted file takes its types along: the name Main writes out in full
     * (no import vouches for it) is now missing in a package that stays */
    ASSERT_EQ(unlink(TH_PATH(r.repo, "src/main/java/lib/Gone.java")), 0);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "delete file", forced), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "lib.Gone.m"), 0);
    ASSERT_STR_EQ(jt_reason(db, main_java, "lib.Gone#m()"), "missing");

    /* Kotlin: renaming a target a Java file names */
    jt_write(&r, khelper_kt, JT_I_KHELPER_RENAMED);
    ASSERT_EQ(dm_step(r.repo, db, full_db, "kotlin rename", forced), 0);
    ASSERT_EQ(jt_edge(db, "app.Main", "kt.KHelper.KHelper"), 0);
    ASSERT_STR_EQ(jt_reason(db, main_java, "kt.KHelper"), "missing");

    dm_unlink_db(full_db);
    jt_end(&r);
    PASS();
}

/* ── the parallel path == the sequential path ────────────────────── */

enum { JT_RING = 32 }; /* per language: above MIN_FILES_FOR_PARALLEL together */

/* A ring of Java classes and a ring of Kotlin classes. Every class documents
 * its successor, its counterpart in the other language, a shared target (by
 * name, by overload group and by signature) and a name nothing declares. */
static void jt_write_ring_fixture(const jt_repo_t *r) {
    for (int i = 0; i < JT_RING; i++) {
        char path[128];
        char body[1024];
        int next = (i + 1) % JT_RING;
        snprintf(path, sizeof(path), "src/main/java/ring/J%02d.java", i);
        snprintf(body, sizeof(body),
                 "package ring;\n"
                 "import ring.shared.Hub;\n"
                 "/**\n"
                 " * Next {@link J%02d}, shared {@linkplain Hub the hub}, {@link Hub#run},\n"
                 " * {@link Nothing%02d}, in Kotlin {@link ringk.K%02d}.\n"
                 " * @see Hub#run(String)\n"
                 " */\n"
                 "public class J%02d {}\n",
                 next, i, i, i);
        jt_write(r, path, body);
        snprintf(path, sizeof(path), "src/main/kotlin/ringk/K%02d.kt", i);
        snprintf(body, sizeof(body),
                 "package ringk\n"
                 "\n"
                 "import ring.shared.Hub\n"
                 "\n"
                 "/**\n"
                 " * Next [K%02d], shared [Hub], in Java [ring.J%02d].\n"
                 " * @see Hub.run\n"
                 " * @see Nothing%02d\n"
                 " */\n"
                 "class K%02d\n",
                 next, i, i, i);
        jt_write(r, path, body);
    }
    jt_write(r, "src/main/java/ring/shared/Hub.java",
             "package ring.shared;\n"
             "public class Hub {\n"
             "    public void run(int n) {}\n"
             "    public void run(String s) {}\n"
             "}\n");
}

/* The worker pipeline (extract workers, resolve workers, per-file rows merged
 * at the end) and the sequential passes publish the same edges and rows. */
TEST(doc_mentions_jvm_parallel_equals_sequential) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "par"));
    jt_write_ring_fixture(&r);
    char seq_db[600];
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", r.tmp);
    ASSERT_EQ(dm_workers_agree(r.repo, r.db, seq_db), 0);
    /* per Java class: successor, Hub, Hub.run(String), its Kotlin counterpart;
     * per Kotlin class: successor, Hub, its Java counterpart */
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"),
              JT_RING * 4 + JT_RING * 3);
    /* per class of either language: the overload group and the undeclared
     * name */
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM doc_link_unresolved"), JT_RING * 4);
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'ambiguous'"),
              JT_RING * 2);
    ASSERT_EQ(dm_count(r.db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'missing'"),
              JT_RING * 2);
    dm_unlink_db(seq_db);
    jt_end(&r);
    PASS();
}

/* ── the platforms' own namespaces ───────────────────────────────── */

/* An all-lower-case path that nothing declares is external under the
 * namespaces the two platforms define themselves: `java`, `javax`, `kotlin`.
 * No library family is such a root. A path under `kotlinx` is external only
 * where the file imports from that package (which the repository does not
 * declare); without an import it names nothing, like any other path. */
TEST(doc_mentions_jvm_platform_roots) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "roots"));
    const char *with = "src/main/kotlin/rt/WithImport.kt";
    jt_write(&r, with,
             "package rt\n"
             "\n"
             "import kotlinx.coroutines.flow.map\n"
             "\n"
             "/**\n"
             " * @see java.util.concurrent\n"
             " * @see javax.annotation.processing\n"
             " * @see kotlin.collections.map\n"
             " * @see kotlinx.coroutines.flow.onEach\n"
             " */\n"
             "class WithImport\n");
    const char *without = "src/main/kotlin/rt/NoImport.kt";
    jt_write(&r, without,
             "package rt\n"
             "\n"
             "/**\n"
             " * [kotlinx.coroutines.flow.map]\n"
             " * @see kotlinx.coroutines.flow.onEach\n"
             " * @see org.outside.lib.helper\n"
             " */\n"
             "class NoImport\n");
    const char *java = "src/main/java/rt/JRoots.java";
    jt_write(&r, java,
             "package rt;\n"
             "/** {@link java.util.concurrent} {@link kotlinx.coroutines.flow} */\n"
             "public class JRoots {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* the three roots */
    ASSERT_EQ(jt_rows(db, with, "java.util.concurrent", "external"), 1);
    ASSERT_EQ(jt_rows(db, with, "javax.annotation.processing", "external"), 1);
    ASSERT_EQ(jt_rows(db, with, "kotlin.collections.map", "external"), 1);
    ASSERT_EQ(jt_rows(db, java, "java.util.concurrent", "external"), 1);
    /* `kotlinx` with an import of that package: external by the import */
    ASSERT_EQ(jt_rows(db, with, "kotlinx.coroutines.flow.onEach", "external"), 1);
    /* ... and without one: a block tag keeps its row as missing, a `[name]`
     * is plain text, exactly as for any other library's path */
    ASSERT_EQ(jt_rows(db, without, "kotlinx.coroutines.flow.onEach", "missing"), 1);
    ASSERT_EQ(jt_rows(db, without, "org.outside.lib.helper", "missing"), 1);
    ASSERT_STR_EQ(jt_reason(db, without, "kotlinx.coroutines.flow.map"), "");
    ASSERT_EQ(jt_rows(db, java, "kotlinx.coroutines.flow", "missing"), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), 0);
    jt_end(&r);
    PASS();
}

/* ── a field against a method from outside ───────────────────────── */

/* A bare Javadoc `#x` that finds a FIELD. javadoc looks for a method of the
 * name as well and takes the field only when its class is the method's class
 * or below it. The index cannot see the methods of a supertype outside the
 * repository, or what an enum has from the language; where one of those may
 * come first, the reference is external. */

/* A type consulted BEFORE the field's level that may have the method unseen:
 * the field of the class around is not below that type's supertypes. */
TEST(doc_mentions_jvm_java_field_before_level) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfbef"));
    const char *f1 = "src/main/java/gf/F1.java";
    jt_write(&r, f1,
             "package gf;\n"
             "public class F1 {\n"
             "    public int available = 1;\n"
             "    public int hashCode = 2;\n"
             "    public int name = 3;\n"
             "    public int other = 4;\n"
             "    public class In extends java.io.InputStream {\n"
             "        public int read() { return 0; }\n"
             "        /** {@link #available} {@link #hashCode} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public class Closed {\n"
             "        /** {@link #available} {@link #hashCode} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public enum Mode {\n"
             "        ON;\n"
             "        /** {@link #name} {@link #other} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* java.io.InputStream may declare available(); it may also override
     * hashCode(), and then the method is no longer Object's */
    ASSERT_EQ(jt_rows(db, f1, "#available", "external"), 1);
    ASSERT_EQ(jt_rows(db, f1, "#hashCode", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gf.F1.In.m"), 0);
    /* decoy: a nested class without a supertype outside binds both. Object's
     * own hashCode() is no reason to yield: every class is below Object */
    ASSERT_EQ(jt_edge(db, "gf.F1.Closed.m", "gf.F1.available"), 1);
    ASSERT_EQ(jt_edge(db, "gf.F1.Closed.m", "gf.F1.hashCode"), 1);
    /* an enum has name() from the language, and the class around is not
     * below java.lang.Enum; a name the enum does not have binds */
    ASSERT_EQ(jt_rows(db, f1, "#name", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gf.F1.Mode.m", "gf.F1.other"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gf.F1.Mode.m"), 1);
    jt_end(&r);
    PASS();
}

/* At the field's OWN level: the field binds when every supertype outside the
 * repository of that level's type is a supertype of the field's class too. A
 * field the type declares itself always is; an inherited one only when the
 * type adds no supertype from outside of its own. The same for the type a
 * reference names (`Type#x`). */
TEST(doc_mentions_jvm_java_field_at_open_level) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jflvl"));
    jt_write(&r, "src/main/java/gf/F2.java",
             "package gf;\n"
             "public class F2 extends java.io.InputStream {\n"
             "    public int own = 1;\n"
             "    public int read() { return 0; }\n"
             "    /** {@link #own} */\n"
             "    public void m() {}\n"
             "}\n");
    const char *f3 = "src/main/java/gf/F3.java";
    jt_write(&r, f3,
             "package gf;\n"
             "public class F3 extends F3Base implements java.io.Closeable {\n"
             "    public void close() {}\n"
             "    /** {@link #LIMIT} */\n"
             "    public void m() {}\n"
             "}\n"
             "class F3Base {\n"
             "    public static final int LIMIT = 1;\n"
             "}\n");
    jt_write(&r, "src/main/java/gf/F4.java",
             "package gf;\n"
             "public class F4 extends F4Base {\n"
             "    /** {@link #shared} */\n"
             "    public void m() {}\n"
             "}\n"
             "class F4Base extends java.io.InputStream {\n"
             "    public int shared = 1;\n"
             "    public int read() { return 0; }\n"
             "}\n");
    const char *f5 = "src/main/java/gf/F5.java";
    jt_write(&r, f5,
             "package gf;\n"
             "/** {@link F2#own} {@link F3#LIMIT} {@link F4#shared} {@value F3#LIMIT} */\n"
             "public class F5 {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* declared by the type itself: below everything the type is below */
    ASSERT_EQ(jt_edge(db, "gf.F2.m", "gf.F2.own"), 1);
    /* inherited, and F3 itself adds java.io.Closeable, which is no supertype
     * of F3Base: a method LIMIT() there would be what javadoc links */
    ASSERT_EQ(jt_rows(db, f3, "#LIMIT", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gf.F3.m"), 0);
    /* inherited from the one type of the hierarchy that leaves the
     * repository: its supertypes are the field's class's own */
    ASSERT_EQ(jt_edge(db, "gf.F4.m", "gf.F4Base.shared"), 1);
    /* the same three at a type the reference names */
    ASSERT_EQ(jt_edge(db, "gf.F5", "gf.F2.own"), 1);
    ASSERT_EQ(jt_rows(db, f5, "F3#LIMIT", "external"), 1);
    ASSERT_EQ(jt_edge(db, "gf.F5", "gf.F4Base.shared"), 1);
    /* {@value} names a field and nothing else: as before */
    ASSERT_EQ(jt_edge(db, "gf.F5", "gf.F3Base.LIMIT"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"syntax\":\"value\""));
    ASSERT_EQ(dm_mentions_from(db, "gf.F5"), 3);
    jt_end(&r);
    PASS();
}

/* ── javadoc's search for a method (JavacTrees.searchMethod in the JDK) ──
 *
 * From a type: its own methods, its superclass by the whole search (with the
 * superclass's own interfaces and the type around the superclass), its
 * interfaces in the order written, then the type around it; each type once.
 * A type outside the repository on the way makes the reference external. */

/* Step 2 takes in the type around the SUPERCLASS, before the type around the
 * documented one. */
TEST(doc_mentions_jvm_java_search_superclass_around) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jsenc"));
    jt_write(&r, "src/main/java/sa/Base2.java",
             "package sa;\n"
             "public class Base2 {\n"
             "    public void go() {}\n"
             "    public static class Inner {}\n"
             "}\n");
    jt_write(&r, "src/main/java/sa/Plain2.java", "package sa;\npublic class Plain2 {}\n");
    jt_write(&r, "src/main/java/sa/Holder.java",
             "package sa;\n"
             "public class Holder {\n"
             "    public void go() {}\n"
             "    public static class Sub extends Base2.Inner {\n"
             "        /** {@link #go()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "    public static class Sub2 extends Plain2 {\n"
             "        /** {@link #go()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* Base2.Inner is searched with Base2 around it, before Holder */
    ASSERT_EQ(jt_edge(db, "sa.Holder.Sub.m", "sa.Base2.go"), 1);
    ASSERT_EQ(jt_edge(db, "sa.Holder.Sub.m", "sa.Holder.go"), 0);
    /* decoy: a superclass nested in nothing: the type around Sub2 */
    ASSERT_EQ(jt_edge(db, "sa.Holder.Sub2.m", "sa.Holder.go"), 1);
    jt_end(&r);
    PASS();
}

/* Step 2 before step 3: a superclass outside the repository comes before an
 * interface of the repository. */
TEST(doc_mentions_jvm_java_search_outside_superclass) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jsout"));
    const char *s1 = "src/main/java/sb/S1.java";
    jt_write(&r, s1,
             "package sb;\n"
             "public abstract class S1 extends java.io.InputStream implements Api {\n"
             "    /** {@link #close()} */\n"
             "    public void m() {}\n"
             "}\n"
             "interface Api {\n"
             "    void close();\n"
             "}\n"
             "class Plain {}\n"
             "abstract class S2 extends Plain implements Api {\n"
             "    /** {@link #close()} */\n"
             "    public void m() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* java.io.InputStream is searched first; its close() is the one */
    ASSERT_EQ(jt_rows(db, s1, "#close()", "external"), 1);
    ASSERT_EQ(jt_edge(db, "sb.S1.m", "sb.Api.close"), 0);
    /* decoy: a superclass of the repository without the method */
    ASSERT_EQ(jt_edge(db, "sb.S2.m", "sb.Api.close"), 1);
    jt_end(&r);
    PASS();
}

/* Step 3: the interfaces in the order written; the first with the method is
 * the one. */
TEST(doc_mentions_jvm_java_search_interface_order) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jsord"));
    jt_write(&r, "src/main/java/sc/Both.java",
             "package sc;\n"
             "public abstract class Both implements First, Second {\n"
             "    /** {@link #run()} */\n"
             "    public void m() {}\n"
             "}\n"
             "interface First {\n"
             "    void run();\n"
             "}\n"
             "interface Second {\n"
             "    void run();\n"
             "}\n"
             "abstract class Other implements Second, First {\n"
             "    /** {@link #run()} */\n"
             "    public void m() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_edge(db, "sc.Both.m", "sc.First.run"), 1);
    ASSERT_EQ(jt_edge(db, "sc.Both.m", "sc.Second.run"), 0);
    /* decoy: the other order */
    ASSERT_EQ(jt_edge(db, "sc.Other.m", "sc.Second.run"), 1);
    ASSERT_EQ(jt_edge(db, "sc.Other.m", "sc.First.run"), 0);
    jt_end(&r);
    PASS();
}

/* An interface has no superclass step: Object's methods are not searched at
 * it, and the search goes on to the type around it. */
TEST(doc_mentions_jvm_java_search_interface_no_object) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jsifc"));
    const char *h = "src/main/java/sd/Host.java";
    jt_write(&r, h,
             "package sd;\n"
             "public class Host {\n"
             "    public String toString() { return \"\"; }\n"
             "    public interface Shape {\n"
             "        /** {@link #toString()} */\n"
             "        void m();\n"
             "    }\n"
             "    public static class Box {\n"
             "        /** {@link #toString()} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_edge(db, "sd.Host.Shape.m", "sd.Host.toString"), 1);
    /* decoy: a class has the superclass Object, whose toString() comes first */
    ASSERT_EQ(jt_rows(db, h, "#toString()", "external"), 1);
    ASSERT_EQ(jt_edge(db, "sd.Host.Box.m", "sd.Host.toString"), 0);
    jt_end(&r);
    PASS();
}

/* A Kotlin class on the way writes its superclass anywhere in its list: the
 * class among its supertypes is searched before its interfaces. */
TEST(doc_mentions_jvm_java_search_kotlin_superclass) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jskt"));
    jt_write(&r, "src/main/kotlin/se/KApi.kt",
             "package se\n\ninterface KApi {\n    fun run()\n}\n");
    jt_write(&r, "src/main/kotlin/se/KBase.kt",
             "package se\n\nopen class KBase {\n    fun run() {}\n}\n");
    jt_write(&r, "src/main/kotlin/se/KMix.kt", "package se\n\nopen class KMix : KApi, KBase()\n");
    jt_write(&r, "src/main/kotlin/se/KOnly.kt", "package se\n\nabstract class KOnly : KApi\n");
    jt_write(&r, "src/main/java/se/J.java",
             "package se;\n"
             "public class J extends KMix {\n"
             "    /** {@link #run} */\n"
             "    public void m() {}\n"
             "}\n"
             "abstract class J2 extends KOnly {\n"
             "    /** {@link #run} */\n"
             "    public void m() {}\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_edge(db, "se.J.m", "se.KBase.KBase.run"), 1);
    ASSERT_EQ(jt_edge(db, "se.J.m", "se.KApi.KApi.run"), 0);
    /* decoy: no class among the supertypes: the interface */
    ASSERT_EQ(jt_edge(db, "se.J2.m", "se.KApi.KApi.run"), 1);
    jt_end(&r);
    PASS();
}

/* ── The field rule against that search ───────────────────────────────
 *
 * A field javadoc's field search finds binds only when its class is below
 * every type outside the repository that the search for a METHOD passes
 * before a type of the repository that declares one, and below that method's
 * class. */

/* A field of the innermost type; the class AROUND it leaves the repository. */
TEST(doc_mentions_jvm_java_field_outer_outside) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfout"));
    const char *o = "src/main/java/ga/Outer.java";
    jt_write(&r, o,
             "package ga;\n"
             "public class Outer extends java.io.InputStream {\n"
             "    public int read() { return 0; }\n"
             "    public class In {\n"
             "        public int available = 1;\n"
             "        /** {@link #available} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    jt_write(&r, "src/main/java/ga/Shut.java",
             "package ga;\n"
             "public class Shut {\n"
             "    public class In2 {\n"
             "        public int available = 1;\n"
             "        /** {@link #available} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* the search passes In, then Outer and java.io.InputStream, whose
     * available() would win: In is not below it */
    ASSERT_EQ(jt_rows(db, o, "#available", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "ga.Outer.In.m"), 0);
    /* decoy: the class around stays in the repository */
    ASSERT_EQ(jt_edge(db, "ga.Shut.In2.m", "ga.Shut.In2.available"), 1);
    jt_end(&r);
    PASS();
}

/* The type's OWN field, the type leaving the repository itself and a class
 * further out as well: the field is below the first, not the second. */
TEST(doc_mentions_jvm_java_field_own_far_outside) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfown"));
    const char *far_src = "src/main/java/gb/Far.java";
    jt_write(&r, far_src,
             "package gb;\n"
             "public class Far extends java.io.InputStream {\n"
             "    public int read() { return 0; }\n"
             "    public abstract class Near extends java.io.Reader {\n"
             "        public int level = 1;\n"
             "        /** {@link #level} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    jt_write(&r, "src/main/java/gb/Far2.java",
             "package gb;\n"
             "public class Far2 {\n"
             "    public abstract class Near2 extends java.io.Reader {\n"
             "        public int level = 1;\n"
             "        /** {@link #level} */\n"
             "        public void m() {}\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_rows(db, far_src, "#level", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gb.Far.Near.m"), 0);
    /* decoy: only the type's own supertype leaves the repository */
    ASSERT_EQ(jt_edge(db, "gb.Far2.Near2.m", "gb.Far2.Near2.level"), 1);
    jt_end(&r);
    PASS();
}

/* Past a step outside the repository the field's class is below, a method of
 * the repository the field's class is NOT below: javadoc links that method,
 * or one the step outside holds (then the field): external. Without the step
 * outside, javadoc links the method, and so does the reference. */
TEST(doc_mentions_jvm_java_field_method_past_outside) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfpast"));
    const char *top = "src/main/java/gc/Top.java";
    jt_write(&r, top,
             "package gc;\n"
             "public class Top {\n"
             "    public int count() { return 0; }\n"
             "    public class Mid extends java.io.InputStream {\n"
             "        public int count = 1;\n"
             "        public int read() { return 0; }\n"
             "        public class Doc {\n"
             "            /** {@link #count} */\n"
             "            public void m() {}\n"
             "        }\n"
             "    }\n"
             "    public class Mid2 {\n"
             "        public int count = 1;\n"
             "        public class Doc2 {\n"
             "            /** {@link #count} */\n"
             "            public void m() {}\n"
             "        }\n"
             "    }\n"
             "}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    ASSERT_EQ(jt_rows(db, top, "#count", "external"), 1);
    ASSERT_EQ(dm_mentions_from(db, "gc.Top.Mid.Doc.m"), 0);
    /* decoy: no step outside on the way: the method of Top */
    ASSERT_EQ(jt_edge(db, "gc.Top.Mid2.Doc2.m", "gc.Top.count"), 1);
    ASSERT_EQ(jt_rows(db, top, "#count", "ambiguous"), 0);
    jt_end(&r);
    PASS();
}

/* `Type#x`: javadoc's field search goes through the types around the type
 * named, as its method search does. A field there comes first when its class
 * is below the method's class. */
TEST(doc_mentions_jvm_java_field_named_around) {
    jt_repo_t r;
    ASSERT(jt_begin(&r, "jfnam"));
    jt_write(&r, "src/main/java/ge/Base3.java",
             "package ge;\n"
             "public class Base3 {\n"
             "    public int x() { return 0; }\n"
             "}\n");
    jt_write(&r, "src/main/java/ge/E.java",
             "package ge;\n"
             "public class E extends Base3 {\n"
             "    public int x = 1;\n"
             "    public static class T {}\n"
             "}\n");
    jt_write(&r, "src/main/java/ge/User.java",
             "package ge;\n"
             "/** {@link E.T#x} {@link E#x} */\n"
             "public class User {}\n");
    ASSERT_EQ(dm_index(r.repo, r.db, NULL), 0);
    const char *db = r.db;
    /* the method search from E.T reaches Base3.x() through E; the field
     * search reaches E.x first, and E is below Base3 */
    ASSERT_EQ(jt_edge(db, "ge.User", "ge.E.x"), 1);
    ASSERT_NOT_NULL(strstr(jt_props, "\"count\":2"));
    ASSERT_EQ(jt_edge(db, "ge.User", "ge.Base3.x"), 0);
    jt_end(&r);
    PASS();
}

SUITE(doc_mentions_jvm) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_jvm_java_tokens);
    RUN_TEST(doc_mentions_jvm_kotlin_tokens);
    RUN_TEST(doc_mentions_jvm_malformed_and_oversized);
    RUN_TEST(doc_mentions_jvm_java_norm_type);
    RUN_TEST(doc_mentions_jvm_java_scope_blob);
    RUN_TEST(doc_mentions_jvm_kotlin_scope_blob);
    RUN_TEST(doc_mentions_jvm_scope_parse_errors);
    RUN_TEST(doc_mentions_jvm_resolver_parse_errors);
    RUN_TEST(doc_mentions_jvm_java_families_and_reasons);
    RUN_TEST(doc_mentions_jvm_oversized_reference);
    RUN_TEST(doc_mentions_jvm_java_type_levels);
    RUN_TEST(doc_mentions_jvm_java_member_levels);
    RUN_TEST(doc_mentions_jvm_kotlin_families_and_reasons);
    RUN_TEST(doc_mentions_jvm_kotlin_levels);
    RUN_TEST(doc_mentions_jvm_java_given_members);
    RUN_TEST(doc_mentions_jvm_java_open_supertypes);
    RUN_TEST(doc_mentions_jvm_java_uncompared_level);
    RUN_TEST(doc_mentions_jvm_kotlin_given_members);
    RUN_TEST(doc_mentions_jvm_kotlin_open_supertypes);
    RUN_TEST(doc_mentions_jvm_kotlin_companion_inherits);
    RUN_TEST(doc_mentions_jvm_kotlin_extension_guard);
    RUN_TEST(doc_mentions_jvm_root_supertype_written);
    RUN_TEST(doc_mentions_jvm_guard_incremental_equals_full);
    RUN_TEST(doc_mentions_jvm_cross_language);
    RUN_TEST(doc_mentions_jvm_node_ownership);
    RUN_TEST(doc_mentions_jvm_kotlin_type_and_function);
    RUN_TEST(doc_mentions_jvm_test_only_layouts);
    RUN_TEST(doc_mentions_jvm_family_names);
    RUN_TEST(doc_mentions_jvm_scope_delta_rules);
    RUN_TEST(doc_mentions_jvm_incremental_equals_full);
    RUN_TEST(doc_mentions_jvm_parallel_equals_sequential);
    RUN_TEST(doc_mentions_jvm_platform_roots);
    RUN_TEST(doc_mentions_jvm_java_field_before_level);
    RUN_TEST(doc_mentions_jvm_java_field_at_open_level);
    RUN_TEST(doc_mentions_jvm_java_search_superclass_around);
    RUN_TEST(doc_mentions_jvm_java_search_outside_superclass);
    RUN_TEST(doc_mentions_jvm_java_search_interface_order);
    RUN_TEST(doc_mentions_jvm_java_search_interface_no_object);
    RUN_TEST(doc_mentions_jvm_java_search_kotlin_superclass);
    RUN_TEST(doc_mentions_jvm_java_field_outer_outside);
    RUN_TEST(doc_mentions_jvm_java_field_own_far_outside);
    RUN_TEST(doc_mentions_jvm_java_field_method_past_outside);
    RUN_TEST(doc_mentions_jvm_java_field_named_around);
}
