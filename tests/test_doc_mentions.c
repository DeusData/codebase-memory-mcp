/*
 * test_doc_mentions.c — doc-comment references -> MENTIONS edges.
 *
 * Extraction (doclink.c / doclink_cs.c), the C# resolver (doc_links_cs.c),
 * MSBuild global usings, publication into doc_link_unresolved, the
 * index_status block, delete_project, and incremental == full across edits.
 * Every pipeline test indexes a real fixture through cbm_pipeline_run and
 * reads the published database.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"

#include "cbm.h"
#include "doclink.h"
#include "foundation/mem_core.h"
#include "mcp/mcp.h"
#include "pipeline/doc_links.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "sqlite3.h"
#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── helpers ─────────────────────────────────────────────────────── */

static const CBMDocLink *dm_find_token(const CBMFileResult *r, const char *raw) {
    for (int i = 0; i < r->doc_links.count; i++) {
        if (strcmp(r->doc_links.items[i].raw, raw) == 0) {
            return &r->doc_links.items[i];
        }
    }
    return NULL;
}

static int dm_count_tokens(const CBMFileResult *r, const char *raw) {
    int n = 0;
    for (int i = 0; i < r->doc_links.count; i++) {
        n += strcmp(r->doc_links.items[i].raw, raw) == 0;
    }
    return n;
}

static int dm_index(const char *repo, const char *db, char **project_out) {
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    if (project_out) {
        *project_out = strdup(cbm_pipeline_project_name(p));
    }
    cbm_pipeline_free(p);
    return rc;
}

/* Properties of the MENTIONS edge whose endpoints' qualified names END with
 * the given local paths ("Widget", "Helper.Once"); "" when absent; count via
 * *n. */
static void dm_edge(const char *db, const char *src_suffix, const char *tgt_suffix, char *props,
                    size_t cap, int *n) {
    props[0] = '\0';
    *n = 0;
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        *n = -1;
        return;
    }
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT e.properties FROM edges e JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'MENTIONS' "
        "AND (s.qualified_name LIKE '%.' || ?1) AND (t.qualified_name LIKE '%.' || ?2)";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, src_suffix, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tgt_suffix, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            (*n)++;
            snprintf(props, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
}

static int dm_mentions_from(const char *db, const char *src_suffix) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT COUNT(*) FROM edges e JOIN nodes s ON s.id = e.source_id "
                               "WHERE e.type = 'MENTIONS' AND s.qualified_name LIKE '%.' || ?1",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, src_suffix, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                n = sqlite3_column_int(st, 0);
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* The single integer a query returns; -1 when it cannot be read. */
static int dm_count(const char *db, const char *sql) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK &&
            sqlite3_step(st) == SQLITE_ROW) {
            n = sqlite3_column_int(st, 0);
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* Reason of the unresolved row with this raw text in this file ("" when
 * none). */
static void dm_row(const char *db, const char *rel, const char *raw, char *reason, size_t cap,
                   char *syntax, size_t scap) {
    reason[0] = '\0';
    if (syntax) {
        syntax[0] = '\0';
    }
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT reason, syntax FROM doc_link_unresolved WHERE rel_path = ?1 "
                               "AND raw = ?2",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, rel, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, raw, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) {
                snprintf(reason, cap, "%s", (const char *)sqlite3_column_text(st, 0));
                if (syntax) {
                    snprintf(syntax, scap, "%s", (const char *)sqlite3_column_text(st, 1));
                }
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
}

/* Canonical text of every MENTIONS edge and unresolved row: what a full and
 * an incremental index of the same tree must agree on byte for byte. */
static char *dm_doclink_state(const char *db) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return NULL;
    }
    size_t cap = 4096;
    size_t len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    const char *queries[] = {
        "SELECT 'E ' || s.qualified_name || ' -> ' || t.qualified_name || ' ' || e.properties "
        "FROM edges e JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
        "WHERE e.type = 'MENTIONS' ORDER BY 1",
        "SELECT 'R ' || rel_path || ':' || line || ' ' || syntax || ' [' || raw || '] ' || reason "
        "FROM doc_link_unresolved ORDER BY 1",
    };
    for (size_t q = 0; q < sizeof(queries) / sizeof(queries[0]); q++) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h, queries[q], -1, &st, NULL) != SQLITE_OK) {
            continue;
        }
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *line = (const char *)sqlite3_column_text(st, 0);
            size_t l = strlen(line);
            if (len + l + 2 > cap) {
                cap = (len + l + 2) * 2;
                buf = realloc(buf, cap);
            }
            memcpy(buf + len, line, l);
            len += l;
            buf[len++] = '\n';
            buf[len] = '\0';
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return buf;
}

static void dm_unlink_db(const char *db) {
    char side[600];
    unlink(db);
    snprintf(side, sizeof(side), "%s-wal", db);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", db);
    unlink(side);
}

/* ── extraction ──────────────────────────────────────────────────── */

TEST(doc_mentions_extract_cs_tokens) {
    const char *src =
        "namespace N\n"                                                           /* 1 */
        "{\n"                                                                     /* 2 */
        "    /// <summary>A <see cref=\"Foo\"/> and\n"                            /* 3 */
        "    /// <seealso cref='Bar.Baz(int)'/>, not <paramref name=\"x\"/>\n"    /* 4 */
        "    /// or <typeparamref name=\"T\"/> or <see langword=\"null\"/>.\n"    /* 5 */
        "    /// <see href=\"https://x.org\"/> <see cref=\"List&lt;int&gt;\"/>\n" /* 6 */
        "    /// </summary>\n"                                                    /* 7 */
        "    /// <exception cref=\"Oops\">bad</exception>\n"                      /* 8 */
        "    /// <inheritdoc cref=\"Base.M\"/> <see\n"                            /* 9 */
        "    /// cref=\"Wrapped.Name\"/>\n"                                       /* 10 */
        "    public class C\n"                                                    /* 11 */
        "    {\n"
        "        /// <summary>Field <see cref=\"Foo\"/>.</summary>\n"
        "        public const int K = 1;\n"
        "    }\n"
        "}\n";
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), CBM_LANG_CSHARP, "p", "C.cs", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    const CBMDocLink *foo = dm_find_token(r, "Foo");
    ASSERT_NOT_NULL(foo);
    ASSERT_EQ(foo->line, 3);
    ASSERT_EQ(foo->syntax, CBM_DOCLINK_CS_SEE);
    ASSERT_STR_EQ(foo->source_qn, "p.C.C");
    ASSERT_EQ(foo->def_line, 11);
    const CBMDocLink *baz = dm_find_token(r, "Bar.Baz(int)");
    ASSERT_NOT_NULL(baz);
    ASSERT_EQ(baz->line, 4);
    ASSERT_EQ(baz->syntax, CBM_DOCLINK_CS_SEEALSO);
    /* local parameter references and keywords are not references */
    ASSERT_NULL(dm_find_token(r, "x"));
    ASSERT_NULL(dm_find_token(r, "T"));
    ASSERT_NULL(dm_find_token(r, "null"));
    const CBMDocLink *href = dm_find_token(r, "https://x.org");
    ASSERT_NOT_NULL(href);
    ASSERT_EQ(href->syntax, CBM_DOCLINK_HREF);
    ASSERT_EQ(href->line, 6);
    /* XML entities are decoded */
    ASSERT_NOT_NULL(dm_find_token(r, "List<int>"));
    const CBMDocLink *ex = dm_find_token(r, "Oops");
    ASSERT_NOT_NULL(ex);
    ASSERT_EQ(ex->syntax, CBM_DOCLINK_CS_EXCEPTION);
    ASSERT_EQ(ex->line, 8);
    const CBMDocLink *inh = dm_find_token(r, "Base.M");
    ASSERT_NOT_NULL(inh);
    ASSERT_EQ(inh->syntax, CBM_DOCLINK_CS_INHERITDOC);
    /* a tag spanning two comment lines */
    const CBMDocLink *wrapped = dm_find_token(r, "Wrapped.Name");
    ASSERT_NOT_NULL(wrapped);
    ASSERT_EQ(wrapped->line, 9);
    /* the constant's Field and its Variable twin carry the doc once */
    ASSERT_EQ(dm_count_tokens(r, "Foo"), 2); /* the class's and the constant's */
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_cs_scope_blob) {
    const char *src = "global using Acme.G;\n"
                      "using static Acme.S;\n"
                      "using A = Acme.Util.Helper<int>;\n"
                      "namespace Outer.Inner\n"
                      "{\n"
                      "    using Acme.Local;\n"
                      "    public partial class W<T, U> : Base<T>, IThing\n"
                      "    {\n"
                      "        void IThing.Do(int x) { }\n"
                      "        public void Go(ref string s, params int[] rest) { }\n"
                      "        public event System.EventHandler Fired;\n"
                      "        public int P { get; set; }\n"
                      "        public record R(int Width);\n"
                      "        public delegate void D();\n"
                      "        public enum E { One }\n"
                      "    }\n"
                      "}\n";
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), CBM_LANG_CSHARP, "p", "W.cs", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT(strncmp(s, "cs1\n", 4) == 0);
    ASSERT_NOT_NULL(strstr(s, "U\t0\tg\t-\tAcme.G\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t0\ts\t-\tAcme.S\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t0\ta\tA\tAcme.Util.Helper<int>\n"));
    ASSERT_NOT_NULL(strstr(s, "\tOuter.Inner\n"));          /* the region */
    ASSERT_NOT_NULL(strstr(s, "U\t1\tn\t-\tAcme.Local\n")); /* a namespace-block using */
    ASSERT_NOT_NULL(strstr(s, "\tc\tW\tT,U\tBase<T>|IThing\n"));
    ASSERT_NOT_NULL(strstr(s, "\tc\t1\tW.Do\t\tint\n")); /* explicit implementation */
    ASSERT_NOT_NULL(strstr(s, "\tc\t0\tW.Go\t\tstring|int[]\n"));
    ASSERT_NOT_NULL(strstr(s, "\te\t0\tW.Fired\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "\tv\t0\tW.P\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "\tr\tW.R\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "\tv\t0\tW.R.Width\t\t-\n")); /* positional record property */
    ASSERT_NOT_NULL(strstr(s, "\td\tW.D\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "\tv\t0\tW.E.One\t\t-\n"));
    /* the persisted scope drops every line number, nothing else */
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT_NOT_NULL(strstr(portable, "R\t1\t0\t0\t0\tOuter.Inner\n"));
    ASSERT_NOT_NULL(strstr(portable, "T\t1\t0\t0\tc\tW\tT,U\tBase<T>|IThing\n"));
    ASSERT_NOT_NULL(strstr(portable, "M\t0\tc\t0\tW.Go\t\tstring|int[]\n"));
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);
    PASS();
}

/* The scope blob of one C# source; the result owns it. */
static CBMFileResult *dm_scope(const char *src) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_CSHARP, "p", "S.cs", 0, NULL, NULL);
}

/* A file whose tree has parse errors takes its nesting from the braces: error
 * recovery closes blocks early and late, which would otherwise move the
 * declarations after the error into another namespace or outer type. */
TEST(doc_mentions_cs_scope_parse_errors) {
    /* `ref *(int*)p` is beyond the vendored grammar: the method swallows the
     * class's closing brace, `P` becomes a local, `Two` a nested class of
     * `One`, and the namespace an error node. */
    const char *late = "namespace A\n"                                           /* 1 */
                       "{\n"                                                     /* 2 */
                       "    public class One\n"                                  /* 3 */
                       "    {\n"                                                 /* 4 */
                       "        unsafe void M(void* p) { _r = ref *(int*)p; }\n" /* 5 */
                       "        public int P;\n"                                 /* 6 */
                       "    }\n"                                                 /* 7 */
                       "    public class Two { }\n"                              /* 8 */
                       "}\n"                                                     /* 9 */
                       "namespace B\n"                                           /* 10 */
                       "{\n"
                       "    public class Three { }\n" /* 12 */
                       "}\n";                         /* 13 */
    CBMFileResult *r = dm_scope(late);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(strstr(s, "R\t1\t0\t1\t9\tA\n")); /* the block, not the tree's node */
    ASSERT_NOT_NULL(
        strstr(s, "T\t1\t3\t7\tc!\tOne\t\t\n")); /* ends at its own brace; a member is hidden */
    ASSERT_NOT_NULL(strstr(s, "M\t5\tc\t0\tOne.M\t\tvoid*\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t8\t8\tc\tTwo\t\t\n")); /* a sibling in A, not One.Two */
    ASSERT_NOT_NULL(strstr(s, "R\t2\t0\t10\t13\tB\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t2\t12\t12\tc\tThree\t\t\n"));
    ASSERT_NULL(strstr(s, "One.Two"));
    ASSERT_NULL(strstr(s, "\nQ\t"));
    ASSERT_NULL(strstr(s, "\nX\t"));
    cbm_free_result(r);

    /* `ref partial struct` is not parsed as a declaration: its header is read
     * from the text (kind, name; bases unknown), and the types after it stay
     * in the namespace its closing brace seemed to end. */
    const char *early = "namespace Acme\n" /* 1 */
                        "{\n"
                        "    public class Before { }\n"           /* 3 */
                        "    public ref partial struct Iter<T>\n" /* 4 */
                        "    {\n"
                        "        public void End() { }\n"
                        "    }\n"                  /* 7 */
                        "    public class After\n" /* 8 */
                        "    {\n"
                        "        public int Size;\n" /* 10 */
                        "    }\n"                    /* 11 */
                        "}\n";                       /* 12 */
    r = dm_scope(early);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NOT_NULL(strstr(s, "R\t1\t0\t1\t12\tAcme\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t3\tc\tBefore\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t4\t7\ts!\tIter\tT\t?\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t8\t11\tc\tAfter\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t10\tv\t0\tAfter.Size\t\t-\n"));
    ASSERT_NULL(strstr(s, "T\t0\t")); /* nothing fell into the global namespace */
    cbm_free_result(r);

    /* Both branches of a conditional open the same block: one closing brace
     * serves both, and the members belong to the type either way. */
    const char *branches = "namespace Net\n" /* 1 */
                           "{\n"
                           "#if DEBUG\n"
                           "    internal abstract class Cred : DebugHandle {\n" /* 4 */
                           "#else\n"
                           "    internal abstract class Cred : PlainHandle {\n" /* 6 */
                           "#endif\n"
                           "        public int Size;\n"   /* 8 */
                           "    }\n"                      /* 9 */
                           "    public class After { }\n" /* 10 */
                           "}\n";
    r = dm_scope(branches);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NOT_NULL(strstr(s, "R\t1\t0\t1\t11\tNet\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t4\t9\tc!\tCred\t\t"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t6\t9\tc!\tCred\t\t"));
    ASSERT_NOT_NULL(strstr(s, "M\t8\tv\t0\tCred.Size\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t10\t10\tc\tAfter\t\t\n"));
    ASSERT_NULL(strstr(s, "\nX\t"));
    cbm_free_result(r);

    /* A string the parser's own lexer loses the thread in ($@"..." with a
     * doubled quote): it then reads code as string content, so the braces come
     * from this scan's own reading of the text. */
    const char *derailed = "namespace Net\n" /* 1 */
                           "{\n"
                           "    public class Verb\n" /* 3 */
                           "    {\n"
                           "        void M() { s += $@\", K = \"\"{K.Name}\"\"\"; }\n" /* 5 */
                           "        public int Q;\n"
                           "    }\n"                      /* 7 */
                           "    public class After { }\n" /* 8 */
                           "}\n";                         /* 9 */
    r = dm_scope(derailed);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NOT_NULL(strstr(s, "R\t1\t0\t1\t9\tNet\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t7\tc!\tVerb\t\t"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t8\t8\tc"));
    ASSERT_NOT_NULL(strstr(s, "\tAfter\t\t"));
    ASSERT_NULL(strstr(s, "\nX\t"));
    cbm_free_result(r);

    /* Braces that do not pair: nothing after the first unpaired one is
     * placed; the type names are kept, to be resolved to nothing else. */
    const char *open = "namespace Acme\n" /* 1 */
                       "{\n"              /* 2 */
                       "    public class Ok { }\n"
                       "    public class Broken\n"
                       "    {\n"
                       "        public void M() {\n"
                       "    public class Lost { }\n";
    r = dm_scope(open);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NOT_NULL(strstr(s, "\nX\t2\t")); /* from the namespace's brace to the end */
    ASSERT_NOT_NULL(strstr(s, "Q\tOk\n"));
    ASSERT_NOT_NULL(strstr(s, "Q\tBroken\n"));
    ASSERT_NOT_NULL(strstr(s, "Q\tLost\n"));
    ASSERT_NULL(strstr(s, "\nT\t"));
    ASSERT_NULL(strstr(s, "\nR\t"));
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT_NOT_NULL(strstr(portable, "\nX\t0\t0\n")); /* line numbers like the others */
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);

    /* A namespace only one configuration can name is not named at all. */
    const char *either = "#if GEN\n"
                         "namespace Gen.Interop\n"
                         "#else\n"
                         "namespace Run.Interop\n"
                         "#endif\n"
                         "{\n"
                         "    public enum Mode { One }\n"
                         "}\n";
    r = dm_scope(either);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NULL(strstr(s, "\nR\t"));
    ASSERT_NOT_NULL(strstr(s, "Q\tMode\n"));
    ASSERT_NOT_NULL(strstr(s, "\nX\t")); /* and what is documented there has no scope */
    cbm_free_result(r);

    /* A member whose header did not parse is hidden, and its type says so:
     * `safe extern` comes back as a method named `extern`. */
    const char *hidden = "namespace C\n"
                         "{\n"
                         "    public class Holey\n" /* 3 */
                         "    {\n"
                         "        public safe extern int Hidden();\n" /* 5 */
                         "        public int Seen;\n"                 /* 6 */
                         "    }\n"                                    /* 7 */
                         "}\n";
    r = dm_scope(hidden);
    ASSERT_NOT_NULL(r);
    s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t7\tc!\tHoley\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t6\tv\t0\tHoley.Seen\t\t-\n"));
    ASSERT_NULL(strstr(s, "Holey.extern"));
    ASSERT_NULL(strstr(s, "Holey.Hidden"));
    cbm_free_result(r);
    PASS();
}

TEST(doc_mentions_cs_norm_type) {
    struct {
        const char *in;
        const char *out;
    } cases[] = {
        {"ref int x", "int"},
        {"params string[] args", "string[]"},
        {"[NotNull] System.Collections.Generic.List<int> xs", "List"},
        {"Nullable{System.Int32}", "int"},
        {"System.String", "string"},
        {"T?", "T"},
        {"byte*", "byte*"},
        {"int[,]", "int[,]"},
        {"``0", "?"},
        {"", "?"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char out[128];
        cbm_doclink_cs_norm_type(cases[i].in, strlen(cases[i].in), out, sizeof(out));
        if (strcmp(out, cases[i].out) != 0) {
            printf("  norm(%s) = %s, want %s\n", cases[i].in, out, cases[i].out);
            FAIL("normalized type");
        }
    }
    PASS();
}

/* ── MSBuild global usings (R1) ──────────────────────────────────── */

TEST(doc_mentions_msbuild_usings) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_msb_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "Directory.Build.props"),
                  "<Project>\n"
                  "  <PropertyGroup><Flavor>web</Flavor></PropertyGroup>\n"
                  "  <ItemGroup Condition=\"'$(Flavor)' == 'web'\">\n"
                  "    <Using Include=\"From.Props\" />\n"
                  "  </ItemGroup>\n"
                  "  <ItemGroup Condition=\"'$(Flavor)' == 'console'\">\n"
                  "    <Using Include=\"Never.Here\" />\n"
                  "  </ItemGroup>\n"
                  "</Project>\n");
    th_write_file(TH_PATH(tmp, "src/App/App.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                  "  <PropertyGroup><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>\n"
                  "  <ItemGroup>\n"
                  "    <Using Include=\"Acme.Extra\" />\n"
                  "    <Using Remove=\"System.Linq\" />\n"
                  "    <Using Include=\"Acme.Static\" Static=\"true\" />\n"
                  "    <Using Include=\"Acme.Aliased\" Alias=\"AA\" />\n"
                  "  </ItemGroup>\n"
                  "</Project>\n");
    char **u = NULL;
    int n = cbm_doclinks_msbuild_usings(tmp, "src/App/App.csproj", &u);
    ASSERT_GT(n, 0);
    bool has_system = false, has_extra = false, has_props = false, has_linq = false,
         has_never = false, has_static = false, has_alias = false;
    for (int i = 0; i < n; i++) {
        has_system = has_system || strcmp(u[i], "System") == 0;
        has_extra = has_extra || strcmp(u[i], "Acme.Extra") == 0;
        has_props = has_props || strcmp(u[i], "From.Props") == 0;
        has_linq = has_linq || strcmp(u[i], "System.Linq") == 0;
        has_never = has_never || strcmp(u[i], "Never.Here") == 0;
        has_static = has_static || strcmp(u[i], "Acme.Static") == 0;
        has_alias = has_alias || strcmp(u[i], "Acme.Aliased") == 0;
    }
    cbm_doclinks_free_strv(u);
    ASSERT_TRUE(has_system);  /* ImplicitUsings: the SDK default set */
    ASSERT_TRUE(has_extra);   /* <Using Include> */
    ASSERT_TRUE(has_props);   /* Directory.Build.props, condition true */
    ASSERT_FALSE(has_linq);   /* <Using Remove> */
    ASSERT_FALSE(has_never);  /* condition false */
    ASSERT_FALSE(has_static); /* Static="true" is not a namespace using */
    ASSERT_FALSE(has_alias);  /* an alias is not a namespace using */
    th_rmtree(tmp);
    PASS();
}

/* ── the C# resolver end to end ──────────────────────────────────── */

static void dm_write_resolver_fixture(const char *tmp) {
    th_write_file(TH_PATH(tmp, "src/Lib/Lib.csproj"), "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                                                      "  <ItemGroup>\n"
                                                      "    <Using Include=\"Acme.Extra\" />\n"
                                                      "    <Using Include=\"Acme.Removed\" />\n"
                                                      "    <Using Remove=\"Acme.Removed\" />\n"
                                                      "  </ItemGroup>\n"
                                                      "</Project>\n");
    th_write_file(TH_PATH(tmp, "src/Lib/Extra.cs"), "namespace Acme.Extra\n"
                                                    "{\n"
                                                    "    public class Tool { }\n"
                                                    "}\n"
                                                    "namespace Acme.Removed\n"
                                                    "{\n"
                                                    "    public class Gone { }\n"
                                                    "}\n");
    th_write_file(TH_PATH(tmp, "src/Lib/Helper.cs"),
                  "namespace Acme.Util\n"
                  "{\n"
                  "    public static class Helper\n"
                  "    {\n"
                  "        public static void Run(int n) { }\n"
                  "        public static void Run(string s) { }\n"
                  "        public static void Once(int n) { }\n"
                  "    }\n"
                  "}\n");
    th_write_file(TH_PATH(tmp, "src/Lib/Global.cs"), "public class Size { }\n");
    /* the usual polyfill: it makes `System` a namespace of the repository */
    th_write_file(TH_PATH(tmp, "src/Lib/Shim.cs"), "namespace System.Runtime.CompilerServices\n"
                                                   "{\n"
                                                   "    internal static class IsExternalInit { }\n"
                                                   "}\n");
    th_write_file(TH_PATH(tmp, "src/Lib/tests/OnlyInTests.cs"),
                  "namespace Acme.Core\n"
                  "{\n"
                  "    public class TestOnlyThing { }\n"
                  "}\n");
    th_write_file(
        TH_PATH(tmp, "src/Lib/Widgets.cs"),
        "using Acme.Util;\n"
        "using H = Acme.Util.Helper;\n"
        "\n"
        "namespace Acme.Core\n"
        "{\n"
        "    /// <summary>\n"
        "    /// A widget: <see cref=\"Gadget\"/> <seealso cref=\"Helper.Once(int)\"/>\n"
        "    /// <see cref=\"Tool\"/> <see cref=\"Gone\"/> <paramref name=\"x\"/>\n"
        "    /// <see href=\"https://example.org/docs\"/> <see cref=\"H\"/> <see "
        "cref=\"Acme.Core.NoSuchType\"/>\n"
        "    /// <see cref=\"string\"/> <see cref=\"System.Text.StringBuilder\"/> <see "
        "cref=\"Acme.Util\"/>\n"
        "    /// <see cref=\"TestOnlyThing\"/> <see cref=\"Bad Syntax!\"/> <seealso "
        "cref=\"NoSuchThing\"/>\n"
        "    /// </summary>\n"
        "    /// <exception cref=\"WidgetError\">when bad</exception>\n"
        "    public class Widget\n"
        "    {\n"
        "        /// <inheritdoc cref=\"Gadget.Spin\"/>\n"
        "        public void Spin() { }\n"
        "\n"
        "        /// <summary>Group <see cref=\"Helper.Run\"/>, one\n"
        "        /// <see cref=\"Helper.Run(string)\"/>, wrong <see "
        "cref=\"Helper.Run(double)\"/>.\n"
        "        /// </summary>\n"
        "        public void Go() { }\n"
        "\n"
        "        /// <summary>Twice <see cref=\"Gadget\"/>, <see cref=\"Gadget\"/>; self\n"
        "        /// <see cref=\"Self\"/>; member <see cref=\"Size\"/>.</summary>\n"
        "        public void Self() { }\n"
        "\n"
        "        /// <summary>Arity: <see cref=\"Box{T}\"/> and <see cref=\"Box\"/>.</summary>\n"
        "        public int Size;\n"
        "    }\n"
        "\n"
        "    /// <summary>Gadget.</summary>\n"
        "    public class Gadget\n"
        "    {\n"
        "        public void Spin() { }\n"
        "    }\n"
        "\n"
        "    public class WidgetError { }\n"
        "\n"
        "    public class Box { }\n"
        "    public class Box<T> { }\n"
        "\n"
        "    public interface IPinger { void Ping(int n); }\n"
        "\n"
        "    public class Ping { }\n"
        "\n"
        "    public class Pinger : IPinger\n"
        "    {\n"
        "        void IPinger.Ping(int n) { }\n"
        "\n"
        "        /// <summary>Explicit implementations are not addressable:\n"
        "        /// <see cref=\"Ping(int)\"/>.</summary>\n"
        "        public void Other() { }\n"
        "    }\n"
        "}\n");
    th_write_file(
        TH_PATH(tmp, "src/Other/Plain.cs"),
        "namespace Acme.Plain\n"
        "{\n"
        "    /// <summary><see cref=\"Nowhere\"/> and <see cref=\"Plain.Missing\"/>.</summary>\n"
        "    public class Plain { }\n"
        "}\n");
    /* arity, constructors, generic methods, same-path declarations */
    th_write_file(TH_PATH(tmp, "src/Other/Lists.cs"),
                  "namespace Coll\n"
                  "{\n"
                  "    public interface IList { int IndexOf(object o); }\n"
                  "}\n");
    th_write_file(TH_PATH(tmp, "src/Other/GenericLists.cs"), "namespace Coll.Generic\n"
                                                             "{\n"
                                                             "    public interface IList<T> { }\n"
                                                             "    public class OnlyGeneric<T> { }\n"
                                                             "}\n");
    th_write_file(
        TH_PATH(tmp, "src/Other/Arity.cs"),
        "using Coll;\n"
        "using Coll.Generic;\n"
        "\n"
        "namespace Acme.Arity\n"
        "{\n"
        "    public class Pair { public int Left; }\n"
        "    public class Pair<T> { public int Left; public int Right; }\n"
        "\n"
        "    public class Conv\n"
        "    {\n"
        "        public Conv() { }\n"
        "        public Conv(int x) { }\n"
        "        public void To(int x) { }\n"
        "        public void To<T>(T x) { }\n"
        "        public void Many(string first, params int[] rest) { }\n"
        "    }\n"
        "\n"
        "    public class Derived : Conv { }\n"
        "\n"
        "    /// <summary>\n"
        "    /// <see cref=\"IList.IndexOf\"/> <see cref=\"OnlyGeneric\"/> <see cref=\"To{T}\"/>\n"
        "    /// <see cref=\"Pair.Left\"/> <see cref=\"Pair{T}.Left\"/> <see "
        "cref=\"Pair{T}.Right\"/>\n"
        "    /// <see cref=\"Conv\"/> <see cref=\"Conv(int)\"/> <see cref=\"Derived.Conv\"/>\n"
        "    /// <see cref=\"Many(string, int[])\"/>\n"
        "    /// </summary>\n"
        "    public class Uses : Conv { }\n"
        "}\n");
    /* declarations the parser cannot place */
    th_write_file(
        TH_PATH(tmp, "src/Other/Recovered.cs"),
        "namespace Acme.Rec\n"
        "{\n"
        "    public class Before { }\n"
        "    public ref partial struct Iter\n"
        "    {\n"
        "        public void End() { }\n"
        "    }\n"
        "    public class Holey\n"
        "    {\n"
        "        public safe extern int Hidden();\n"
        "        /// <summary><see cref=\"Hidden\"/> <see cref=\"int\"/></summary>\n"
        "        public int Seen;\n"
        "    }\n"
        "    /// <summary><see cref=\"Before\"/> <see cref=\"Iter\"/> <see "
        "cref=\"Holey.Hidden\"/>\n"
        "    /// <see cref=\"Holey.Seen\"/> <see cref=\"Lost\"/> <see cref=\"Gadget\"/></summary>\n"
        "    public class Middle { }\n"
        "}\n");
    th_write_file(TH_PATH(tmp, "src/Other/Unbalanced.cs"), "namespace Acme.Rec\n"
                                                           "{\n"
                                                           "    public class Lost { }\n"
                                                           "    public class Open\n"
                                                           "    {\n"
                                                           "        public void M() {\n");
    /* a namespace only one build configuration can name: its block is not
     * placed, though the declarations in it are extracted */
    th_write_file(TH_PATH(tmp, "src/Other/Either.cs"),
                  "#if GEN\n"
                  "namespace Gen.Interop\n"
                  "#else\n"
                  "namespace Run.Interop\n"
                  "#endif\n"
                  "{\n"
                  "    /// <summary><see cref=\"Before\"/></summary>\n"
                  "    public class InEither { }\n"
                  "}\n");
    /* an incomplete type in a file that imports a namespace the repository
     * does not declare */
    th_write_file(TH_PATH(tmp, "src/Other/OpenScope.cs"),
                  "using Outside.Lib;\n"
                  "\n"
                  "namespace Acme.Rec2\n"
                  "{\n"
                  "    public class Holey2\n"
                  "    {\n"
                  "        public safe extern int Hidden2();\n"
                  "        /// <summary><see cref=\"Thing\"/></summary>\n"
                  "        public int Seen2;\n"
                  "    }\n"
                  "}\n");
    /* a field and a method of one name (two declarations of a partial type
     * can do that to a reader who sees no build configuration) */
    th_write_file(TH_PATH(tmp, "src/Other/Kinds.cs"),
                  "namespace Acme.Kinds\n"
                  "{\n"
                  "    public class Mixed\n"
                  "    {\n"
                  "        public int Both;\n"
                  "        public void Both(int x) { }\n"
                  "        public int Only;\n"
                  "    }\n"
                  "    /// <summary><see cref=\"Mixed.Both\"/> <see cref=\"Mixed.Both(int)\"/>\n"
                  "    /// <see cref=\"Mixed.Only\"/></summary>\n"
                  "    public class UsesKinds { }\n"
                  "}\n");
    /* two test programs, each with types in the global namespace */
    th_write_file(TH_PATH(tmp, "tests/ProgA/ProgA.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n");
    th_write_file(TH_PATH(tmp, "tests/ProgA/Shared.cs"),
                  "public class Shared { }\n"
                  "/// <summary><see cref=\"Shared\"/></summary>\n"
                  "public class UsesOwn { }\n");
    th_write_file(TH_PATH(tmp, "tests/ProgB/ProgB.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n");
    th_write_file(TH_PATH(tmp, "tests/ProgB/Uses.cs"),
                  "/// <summary><see cref=\"Shared\"/></summary>\n"
                  "public class UsesOther { }\n");
    /* ... and one only test code fails to place: no concern of product code */
    th_write_file(TH_PATH(tmp, "src/Other/tests/Half.cs"), "namespace Acme.Core\n"
                                                           "{\n"
                                                           "    public class Gadget { }\n"
                                                           "    public class Cut\n"
                                                           "    {\n"
                                                           "        public void M() {\n");
}

TEST(doc_mentions_resolver_rules) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_res_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    dm_write_resolver_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/res.db", tmp);
    char *project = NULL;
    ASSERT_EQ(dm_index(tmp, db, &project), 0);
    free(project);
    char props[512];
    char reason[64];
    char syntax[64];
    int n = 0;
    const char *widgets = "src/Lib/Widgets.cs";

    /* every cref-bearing tag becomes an edge with its syntax */
    dm_edge(db, "Widgets.Widget", "Widgets.Gadget", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"via\":\"doc_comment\""));
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"see\""));
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"unique\""));
    ASSERT_NOT_NULL(strstr(props, "\"line\":7"));
    dm_edge(db, "Widgets.Widget", "Helper.Helper.Once", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"seealso\""));
    dm_edge(db, "Widgets.Widget", "Widgets.WidgetError", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"exception\""));
    dm_edge(db, "Widgets.Widget.Spin", "Widgets.Gadget.Spin", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"syntax\":\"inheritdoc\""));
    /* alias: exact */
    dm_edge(db, "Widgets.Widget", "Helper.Helper", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"tier\":\"exact\""));
    /* R1: a namespace imported only by the project's MSBuild <Using> */
    dm_edge(db, "Widgets.Widget", "Extra.Tool", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* ... and not one the project file removes again */
    dm_edge(db, "Widgets.Widget", "Extra.Gone", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, widgets, "Gone", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* local and keyword references are neither edges nor rows */
    dm_row(db, widgets, "x", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    /* a URL is external */
    dm_row(db, widgets, "https://example.org/docs", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "external");
    ASSERT_STR_EQ(syntax, "href");
    /* R4: keyword aliases and System.* names outside the corpus -- although a
     * polyfill (Shim.cs) makes `System` one of the repository's namespaces */
    dm_row(db, widgets, "string", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "external");
    dm_row(db, widgets, "System.Text.StringBuilder", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "external");
    /* a type the repository's own namespace does not declare is doc rot, not
     * a gap of the graph */
    dm_row(db, widgets, "Acme.Core.NoSuchType", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* a namespace itself is declared code without a node */
    dm_row(db, widgets, "Acme.Util", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    /* product code never binds a test-only declaration */
    dm_row(db, widgets, "TestOnlyThing", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "test_only_target");
    /* a test program binds its own global-namespace types, never another
     * program's */
    dm_edge(db, "Shared.UsesOwn", "Shared.Shared", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(dm_mentions_from(db, "Uses.UsesOther"), 0);
    dm_row(db, "tests/ProgB/Uses.cs", "Shared", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "test_only_target");
    dm_row(db, widgets, "Bad Syntax!", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "unparseable");

    /* overloads: a group without a signature is ambiguous, a signature picks */
    dm_row(db, widgets, "Helper.Run", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous");
    dm_edge(db, "Widgets.Widget.Go", "Helper.Helper.Run", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, widgets, "Helper.Run(double)", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* one edge per (source, target): count and the first line; no self edge */
    dm_edge(db, "Widgets.Widget.Self", "Widgets.Gadget", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"count\":2"));
    ASSERT_NOT_NULL(strstr(props, "\"line\":24"));
    dm_edge(db, "Widgets.Widget.Self", "Widgets.Widget.Self", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, widgets, "Self", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    /* a single segment never takes a qualified shortcut: the member Size,
     * not the global-namespace class Size */
    dm_edge(db, "Widgets.Widget.Self", "Widgets.Widget.Size", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "Widgets.Widget.Self", "Global.Size", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);

    /* R3: type arguments select the generic type; R2: `Box` names the
     * arity-0 type, whose node the generic twin took -- a graph gap, never a
     * fallback to Box<T> */
    dm_edge(db, "Widgets.Widget.Size", "Widgets.Box", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, widgets, "Box", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");

    /* explicit interface implementations are not addressable by simple name:
     * the interface member binds, not the implementing class's node */
    dm_edge(db, "Widgets.Pinger.Other", "Widgets.IPinger.Ping", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "Widgets.Pinger.Other", "Widgets.Pinger.Ping", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);

    /* a closed scope: missing */
    dm_row(db, "src/Other/Plain.cs", "Nowhere", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_row(db, "src/Other/Plain.cs", "Plain.Missing", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* Arity (R2/R3), constructors, generic methods, `params`, and declarations
 * that share one qualified name. */
TEST(doc_mentions_resolver_arity_and_members) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_ar_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    dm_write_resolver_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/res.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    char reason[64];
    int n = 0;
    const char *arity = "src/Other/Arity.cs";

    /* R2: `IList` is the arity-0 interface, wherever one is in scope -- the
     * generic IList<T> of another imported namespace is no rival */
    dm_edge(db, "Arity.Uses", "Lists.IList.IndexOf", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, arity, "IList.IndexOf", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    /* ... and only when no arity-0 type is in scope at all does the generic
     * one of that name bind */
    dm_edge(db, "Arity.Uses", "GenericLists.OnlyGeneric", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* R3: type arguments also select a generic METHOD of that arity */
    dm_edge(db, "Arity.Uses", "Arity.Conv.To", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* `Pair.Left` and `Pair<T>.Left` share one node, and it is the later
     * declaration's: the arity-0 member is a graph gap, like its type */
    dm_row(db, arity, "Pair.Left", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    dm_edge(db, "Arity.Uses", "Arity.Pair.Left", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"count\":1")); /* Pair{T}.Left only */
    dm_edge(db, "Arity.Uses", "Arity.Pair.Right", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* a bare `Conv` is the type: the constructors of a base class are neither
     * inherited nor what a name without a parameter list means */
    dm_edge(db, "Arity.Uses", "Arity.Conv", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, arity, "Conv", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    /* a parameter list names the constructor */
    dm_edge(db, "Arity.Uses", "Arity.Conv.Conv", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* ... of that type only */
    dm_row(db, arity, "Derived.Conv", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* a `params` parameter is part of the signature */
    dm_edge(db, "Arity.Uses", "Arity.Conv.Many", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* member kind: a field and a method of one name are ambiguous without a
     * parameter list; with one, only the callable is meant */
    const char *kinds = "src/Other/Kinds.cs";
    dm_row(db, kinds, "Mixed.Both", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous");
    dm_row(db, kinds, "Mixed.Both(int)", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");
    dm_edge(db, "Kinds.UsesKinds", "Kinds.Mixed.Both", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"count\":1"));
    dm_edge(db, "Kinds.UsesKinds", "Kinds.Mixed.Only", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* Declarations the parser cannot place are never resolved around. */
TEST(doc_mentions_resolver_parse_errors) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_pe_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    dm_write_resolver_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/res.db", tmp);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    char props[512];
    char reason[64];
    int n = 0;
    const char *rec = "src/Other/Recovered.cs";

    /* `Middle` follows a struct the grammar cannot parse; the tree puts it in
     * the global namespace, the braces keep it in Acme.Rec, where `Before` is */
    dm_edge(db, "Recovered.Middle", "Recovered.Before", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* the unparsed struct is declared (its header is read from the text) but
     * has no node */
    dm_row(db, rec, "Iter", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    /* a member behind a parse error: a gap, not doc rot */
    dm_row(db, rec, "Holey.Hidden", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    dm_edge(db, "Recovered.Middle", "Recovered.Holey.Seen", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* inside the incomplete type: a simple name found nowhere may be the
     * hidden member -- a gap, where it would otherwise be reported missing */
    dm_row(db, rec, "Hidden", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    /* ... but a keyword alias is never a member, */
    dm_row(db, rec, "int", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "external");
    /* ... and a name an imported namespace outside the repository may supply
     * stays external */
    dm_row(db, "src/Other/OpenScope.cs", "Thing", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "external");
    /* a type in a file whose braces do not pair: its name resolves to nothing */
    dm_row(db, rec, "Lost", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    /* what is documented in a block that could not be placed has no scope to
     * resolve in: a gap, not a lookup from the global namespace (which would
     * report `Before`, a class of Acme.Rec, as missing) */
    dm_row(db, "src/Other/Either.cs", "Before", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    ASSERT_EQ(dm_mentions_from(db, "Either.InEither"), 0);
    /* ... unless only TEST code fails to place the name: product code never
     * binds test declarations anyway (tests/Half.cs quarantines `Gadget`) */
    dm_edge(db, "Widgets.Widget", "Widgets.Gadget", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, rec, "Gadget", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing"); /* Acme.Core.Gadget is not in Acme.Rec's scope */

    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* The ship gate: a link family that is switched off still resolves, but its
 * resolved references are rows (below_bar_tier), not edges. */
TEST(doc_mentions_ship_gate) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_gate_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    dm_write_resolver_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/gate.db", tmp);
    const char *widgets = "src/Lib/Widgets.cs";
    char props[512];
    char reason[64];
    char syntax[64];
    int n = 0;

    /* the families and their names */
    ASSERT_STR_EQ(cbm_doclink_syntax_name(CBM_DOCLINK_CS_SEE), "see");
    ASSERT_STR_EQ(cbm_doclink_syntax_name(CBM_DOCLINK_CS_SEEALSO), "seealso");
    ASSERT_STR_EQ(cbm_doclink_syntax_name(CBM_DOCLINK_CS_EXCEPTION), "exception");
    ASSERT_STR_EQ(cbm_doclink_syntax_name(CBM_DOCLINK_CS_INHERITDOC), "inheritdoc");
    for (int s = CBM_DOCLINK_CS_SEE; s <= CBM_DOCLINK_CS_INHERITDOC; s++) {
        const CBMDocLinkFamily *f = cbm_doclink_family(s);
        ASSERT_NOT_NULL(f);
        ASSERT_EQ(f->lang, CBM_LANG_CSHARP);
        ASSERT_TRUE(cbm_doclink_syntax_ships(s)); /* every implemented family ships */
    }
    ASSERT_FALSE(cbm_doclink_syntax_ships(CBM_DOCLINK_HREF)); /* a URL is never an edge */
    ASSERT_NULL(cbm_doclink_family(CBM_DOCLINK_NONE));
    ASSERT_NULL(cbm_doclink_family(CBM_DOCLINK_SYNTAX_COUNT));

    cbm_doclink_test_set_ships(CBM_DOCLINK_CS_SEEALSO, false);
    int rc = dm_index(tmp, db, NULL);
    cbm_doclink_test_reset_ships();
    ASSERT_EQ(rc, 0);
    /* the resolved seealso is a row with the gate's reason and no edge */
    dm_edge(db, "Widgets.Widget", "Helper.Helper.Once", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, widgets, "Helper.Once(int)", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "below_bar_tier");
    ASSERT_STR_EQ(syntax, "seealso");
    /* a seealso that does not resolve keeps its own reason */
    dm_row(db, widgets, "NoSuchThing", reason, sizeof(reason), syntax, sizeof(syntax));
    ASSERT_STR_EQ(reason, "missing");
    ASSERT_STR_EQ(syntax, "seealso");
    /* the other families are untouched */
    dm_edge(db, "Widgets.Widget", "Widgets.Gadget", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "Widgets.Widget", "Widgets.WidgetError", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "Widgets.Widget.Spin", "Widgets.Gadget.Spin", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* with the family shipping again, the same reference is an edge */
    dm_unlink_db(db);
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    dm_edge(db, "Widgets.Widget", "Helper.Helper.Once", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(db, widgets, "Helper.Once(int)", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "");

    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── publication: index_status, delete_project, older databases ──── */

/* The text content of an MCP tool result (the report itself); the caller
 * frees it. */
static char *dm_tool_text(const char *mcp_result) {
    yyjson_doc *doc = mcp_result ? yyjson_read(mcp_result, strlen(mcp_result), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *content = root ? yyjson_obj_get(root, "content") : NULL;
    yyjson_val *item = content ? yyjson_arr_get(content, 0) : NULL;
    const char *text = item ? yyjson_get_str(yyjson_obj_get(item, "text")) : NULL;
    char *out = text ? strdup(text) : NULL;
    yyjson_doc_free(doc);
    return out;
}

/* index_status of the project as text, from a server of its own (nothing
 * cached from an earlier call). */
static char *dm_index_status(const char *project, bool full) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }
    char args[1200];
    snprintf(args, sizeof(args), "{\"project\":\"%s\"%s}", project,
             full ? ",\"diagnostics\":\"full\"" : "");
    char *resp = cbm_mcp_handle_tool(srv, "index_status", args);
    char *text = dm_tool_text(resp);
    free(resp);
    cbm_mcp_server_free(srv);
    return text;
}

/* Every reason the table holds is listed under doc_links.unresolved with its
 * row count. The number of reasons checked; -1 when a line is missing. */
static int dm_reason_lines(const char *db, const char *block) {
    sqlite3 *h = NULL;
    int n = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(h,
                               "SELECT reason, COUNT(*) FROM doc_link_unresolved GROUP BY reason",
                               -1, &st, NULL) == SQLITE_OK) {
            n = 0;
            while (n >= 0 && sqlite3_step(st) == SQLITE_ROW) {
                char line[128];
                snprintf(line, sizeof(line), "\n    %s: %d\n",
                         (const char *)sqlite3_column_text(st, 0), sqlite3_column_int(st, 1));
                if (strstr(block, line)) {
                    n++;
                } else {
                    printf("  no line%s  in\n%s\n", line, block);
                    n = -1;
                }
            }
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(h);
    return n;
}

/* The checks of doc_mentions_index_status_and_delete; the caller owns the
 * environment and the directories. */
static int dm_index_status_checks(const char *tmp, const char *repo, const char *cache_dir) {
    char *project = cbm_project_name_from_path(repo);
    ASSERT_NOT_NULL(project);
    char db[1024];
    snprintf(db, sizeof(db), "%s/%s.db", cache_dir, project);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    int edges = dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'");
    ASSERT_GT(edges, 0);

    /* doc_links: the edge count, every reason with its row count, the status */
    char *text = dm_index_status(project, false);
    ASSERT_NOT_NULL(text);
    const char *block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    char want[64];
    snprintf(want, sizeof(want), "\n  mentions: %d\n", edges);
    ASSERT_NOT_NULL(strstr(block, want));
    ASSERT_NOT_NULL(strstr(block, "\n  unresolved:\n"));
    ASSERT_GT(dm_reason_lines(db, block), 4);
    ASSERT_NOT_NULL(strstr(block, "\n    test_only_target: 2\n"));
    ASSERT_NOT_NULL(strstr(block, "\n    unparseable: 1\n"));
    ASSERT_NOT_NULL(strstr(block, "\n  status: ok"));
    ASSERT_NULL(strstr(block, "hint"));
    ASSERT_NULL(strstr(block, "samples")); /* only under diagnostics=full */
    free(text);
    text = dm_index_status(project, true);
    ASSERT_NOT_NULL(text);
    block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    ASSERT_NOT_NULL(strstr(block, "samples"));
    ASSERT_NOT_NULL(strstr(block, "TestOnlyThing"));
    ASSERT_NOT_NULL(strstr(block, "src/Lib/Widgets.cs"));
    free(text);

    /* A doc-link layer that fails does not fail the index, and is not
     * published as "no references" either: the generation carries the error,
     * index_status reports it with what to do, and doing it rebuilds. */
    dm_unlink_db(db);
    cbm_doclinks_test_fail_build_once();
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved"), 1);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'"), 1);
    text = dm_index_status(project, false);
    ASSERT_NOT_NULL(text);
    block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    ASSERT_NOT_NULL(strstr(block, "\n  mentions: 0\n"));
    ASSERT_NOT_NULL(strstr(block, "\n  status: error"));
    ASSERT_NOT_NULL(strstr(block, "index_repository"));
    ASSERT_NULL(strstr(block, "\n    error: "));
    free(text);
    ASSERT_EQ(dm_index(repo, db, NULL), 0); /* no file changed */
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), edges);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'"), 0);

    /* the marker row is the status, never a reference: with rows beside it
     * the counts and the samples are theirs alone */
    cbm_store_t *s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_doc_link_row_t marker[2] = {
        {.rel_path = "src/Lib/Widgets.cs",
         .line = 9,
         .syntax = "see",
         .raw = "Gone",
         .reason = "missing"},
        {.rel_path = "",
         .line = 0,
         .syntax = "",
         .raw = "doc-link layer failed",
         .reason = "error"},
    };
    ASSERT_EQ(cbm_store_doc_links_replace(s, project, marker, 2), CBM_STORE_OK);
    cbm_store_close(s);
    text = dm_index_status(project, true);
    ASSERT_NOT_NULL(text);
    block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    ASSERT_NOT_NULL(strstr(block, "\n  status: error"));
    ASSERT_NOT_NULL(strstr(block, "hint"));
    ASSERT_NOT_NULL(strstr(block, "\n    missing: 1\n"));
    ASSERT_NULL(strstr(block, "\n    error: "));
    /* nor is it a sample */
    ASSERT_NOT_NULL(strstr(block, "\n  samples: 1  (cols: rel_path line syntax raw reason)\n"
                                  "    src/Lib/Widgets.cs 9 see Gone missing\n"));
    free(text);
    /* ... and the re-run the hint asks for rebuilds, although no file changed */
    int rows_before = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    ASSERT_EQ(rows_before, 2);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_GT(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved"), rows_before);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'"), 0);

    /* an index from before the layer has no table: index_status reports an
     * error and what to do, not zero unresolved references */
    sqlite3 *h = NULL;
    ASSERT_EQ(sqlite3_open_v2(db, &h, SQLITE_OPEN_READWRITE, NULL), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(h, "DROP TABLE doc_link_unresolved", NULL, NULL, NULL), SQLITE_OK);
    sqlite3_close(h);
    text = dm_index_status(project, false);
    ASSERT_NOT_NULL(text);
    block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    ASSERT_NOT_NULL(strstr(block, "\n  status: error"));
    ASSERT_NOT_NULL(strstr(block, "predates"));
    free(text);
    /* ... and the next index run creates the table again */
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_GT(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved"), rows_before);
    text = dm_index_status(project, false);
    ASSERT_NOT_NULL(text);
    block = strstr(text, "doc_links:\n");
    ASSERT_NOT_NULL(block);
    ASSERT_NOT_NULL(strstr(block, "\n  status: ok"));
    free(text);
    /* a healthy generation with unchanged inputs is current */
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_NOOP);

    /* store-level delete_project removes the rows */
    s = cbm_store_open_path(db);
    ASSERT_NOT_NULL(s);
    cbm_doc_link_row_t *rows = NULL;
    int count = 0;
    bool present = false;
    ASSERT_EQ(cbm_store_doc_links_get(s, project, &rows, &count, &present), CBM_STORE_OK);
    ASSERT_TRUE(present);
    ASSERT_GT(count, rows_before);
    cbm_store_free_doc_links(rows, count);
    ASSERT_EQ(cbm_store_delete_project(s, project), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_doc_links_get(s, project, &rows, &count, &present), CBM_STORE_OK);
    ASSERT_EQ(count, 0);
    cbm_store_free_doc_links(rows, count);
    cbm_store_close(s);

    /* a database without the table reads as "no data" at the store level, and
     * delete_project still works */
    char old_db[600];
    snprintf(old_db, sizeof(old_db), "%s/old.db", tmp);
    cbm_store_t *o = cbm_store_open_path(old_db);
    ASSERT_NOT_NULL(o);
    ASSERT_EQ(cbm_store_doc_links_get(o, "x", &rows, &count, &present), CBM_STORE_OK);
    ASSERT_FALSE(present);
    ASSERT_EQ(count, 0);
    cbm_doc_link_reason_count_t *reasons = NULL;
    int nreasons = 0;
    cbm_doc_link_row_t *samples = NULL;
    int nsamples = 0;
    ASSERT_EQ(
        cbm_store_doc_links_summary(o, "x", &reasons, &nreasons, &samples, &nsamples, 5, &present),
        CBM_STORE_OK);
    ASSERT_FALSE(present);
    ASSERT_EQ(cbm_store_delete_project(o, "x"), CBM_STORE_OK);
    cbm_store_close(o);

    dm_unlink_db(db);
    dm_unlink_db(old_db);
    free(project);
    return 0;
}

TEST(doc_mentions_index_status_and_delete) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_st_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    char cache_dir[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(cache_dir, sizeof(cache_dir), "%s/cache", tmp);
    dm_write_resolver_fixture(repo);
    cbm_mkdir_p(cache_dir, 0700);
    /* the tool reads the project's database from the cache directory: a
     * private one for this test, restored whatever the checks say */
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache_dir, 1);
    int rc = dm_index_status_checks(tmp, repo, cache_dir);
    if (saved_cache_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
        free(saved_cache_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_rmtree(tmp);
    return rc;
}

/* ── incremental == full ─────────────────────────────────────────── */

static const char DM_MAIN[] =
    "using P;\n"
    "using Q;\n"
    "namespace N\n"
    "{\n"
    "    /// <summary>See <see cref=\"Target\"/>, <see cref=\"Dup\"/>, <see cref=\"Keep\"/>.\n"
    "    /// <see cref=\"ViaProject\"/> <see cref=\"Sig.Go(int)\"/> <see cref=\"Extra\"/>\n"
    "    /// <see cref=\"Ov.Run\"/> <see cref=\"Tw{T}\"/>\n"
    "    /// </summary>\n"
    "    public class Main { }\n"
    "}\n";
static const char DM_MAIN_EDITED[] =
    "using P;\n"
    "using Q;\n"
    "namespace N\n"
    "{\n"
    "    /// <summary>See <see cref=\"Target\"/>, <see cref=\"Dup\"/>, <see cref=\"Keep\"/>.\n"
    "    /// <see cref=\"ViaProject\"/> <see cref=\"Sig.Go(int)\"/> <see cref=\"Extra\"/>\n"
    "    /// <see cref=\"Ov.Run\"/> <see cref=\"Tw{T}\"/>\n"
    "    /// Also <seealso cref=\"Other\"/>.</summary>\n"
    "    public class Main { }\n"
    "}\n";
static const char DM_MAIN_USING[] =
    "using P;\n"
    "using Q;\n"
    "using S;\n"
    "namespace N\n"
    "{\n"
    "    /// <summary>See <see cref=\"Target\"/>, <see cref=\"Dup\"/>, <see cref=\"Keep\"/>.\n"
    "    /// <see cref=\"ViaProject\"/> <see cref=\"Sig.Go(int)\"/> <see cref=\"Extra\"/>\n"
    "    /// <see cref=\"Ov.Run\"/> <see cref=\"Tw{T}\"/>\n"
    "    /// Also <seealso cref=\"Other\"/>.</summary>\n"
    "    public class Main { }\n"
    "}\n";
/* a file with a row and no edge into any file the steps change */
static const char DM_SIDE[] = "using Q;\n"
                              "namespace N\n"
                              "{\n"
                              "    /// <summary><see cref=\"Tw\"/></summary>\n"
                              "    public class Side { }\n"
                              "}\n";
static const char DM_P[] = "namespace P\n"
                           "{\n"
                           "    public class Target { }\n"
                           "    public class Dup { }\n"
                           "    public class Keep { }\n"
                           "}\n";
static const char DM_P_NO_DUP[] = "namespace P\n"
                                  "{\n"
                                  "    public class Target { }\n"
                                  "    public class Keep { }\n"
                                  "}\n";
static const char DM_P_RENAMED[] = "namespace P\n"
                                   "{\n"
                                   "    public class Target { }\n"
                                   "    public class Kept { }\n"
                                   "}\n";
static const char DM_P_NO_TARGET[] = "namespace P\n"
                                     "{\n"
                                     "    public class Kept { }\n"
                                     "}\n";
static const char DM_Q[] = "namespace Q\n"
                           "{\n"
                           "    public class Dup { }\n"
                           "    public class Other { }\n"
                           "}\n";
static const char DM_Q_TWIN[] = "namespace Q\n"
                                "{\n"
                                "    public class Dup { }\n"
                                "    public class Other { }\n"
                                "    public class Target { }\n"
                                "}\n";
/* the same declarations, every one on another line */
static const char DM_Q_TWIN_MOVED[] = "// moved\n"
                                      "\n"
                                      "namespace Q\n"
                                      "{\n"
                                      "    public class Dup { }\n"
                                      "\n"
                                      "    public class Other { }\n"
                                      "    public class Target { }\n"
                                      "}\n";
static const char DM_SIG[] = "namespace Q\n"
                             "{\n"
                             "    public class Sig\n"
                             "    {\n"
                             "        public void Go(int x) { }\n"
                             "    }\n"
                             "}\n";
static const char DM_SIG_BODY[] = "namespace Q\n"
                                  "{\n"
                                  "    public class Sig\n"
                                  "    {\n"
                                  "        public void Go(int x) { x++; }\n"
                                  "    }\n"
                                  "}\n";
static const char DM_SIG_CHANGED[] = "namespace Q\n"
                                     "{\n"
                                     "    public class Sig\n"
                                     "    {\n"
                                     "        public void Go(string x) { }\n"
                                     "    }\n"
                                     "}\n";
static const char DM_OV[] = "namespace Q\n"
                            "{\n"
                            "    public class Ov\n"
                            "    {\n"
                            "        public void Run(int n) { }\n"
                            "        public void Run(string s) { }\n"
                            "    }\n"
                            "}\n";
static const char DM_OV_ONE[] = "namespace Q\n"
                                "{\n"
                                "    public class Ov\n"
                                "    {\n"
                                "        public void Run(int n) { }\n"
                                "    }\n"
                                "}\n";
/* `Tw` and `Tw<T>` share one node; it belongs to the later declaration */
static const char DM_TW[] = "namespace Q\n"
                            "{\n"
                            "    public class Tw { }\n"
                            "    public class Tw<T> { }\n"
                            "}\n";
static const char DM_TW_SWAPPED[] = "namespace Q\n"
                                    "{\n"
                                    "    public class Tw<T> { }\n"
                                    "    public class Tw { }\n"
                                    "}\n";
static const char DM_R[] = "namespace R\n"
                           "{\n"
                           "    public class ViaProject { }\n"
                           "}\n";
static const char DM_S[] = "namespace S\n"
                           "{\n"
                           "    public class Extra { }\n"
                           "}\n";
static const char DM_CSPROJ[] = "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                                "  <ItemGroup>\n"
                                "    <Using Include=\"R\" />\n"
                                "  </ItemGroup>\n"
                                "</Project>\n";
static const char DM_CSPROJ_NO_USING[] = "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                                         "</Project>\n";

/* Index `repo` incrementally into `inc_db`, then fully into a fresh database,
 * and require identical MENTIONS edges and unresolved rows. */
static int dm_step(const char *repo, const char *inc_db, const char *full_db, const char *what,
                   cbm_incremental_route_t want_route) {
    cbm_pipeline_incremental_test_reset_faults();
    if (dm_index(repo, inc_db, NULL) != 0) {
        printf("  %s: incremental index failed\n", what);
        return -1;
    }
    cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
    dm_unlink_db(full_db);
    if (dm_index(repo, full_db, NULL) != 0) {
        printf("  %s: full index failed\n", what);
        return -1;
    }
    char *inc = dm_doclink_state(inc_db);
    char *full = dm_doclink_state(full_db);
    int rc = 0;
    if (!inc || !full || strcmp(inc, full) != 0) {
        printf("  %s: incremental != full\n--- incremental (route %d)\n%s--- full\n%s", what,
               (int)route, inc ? inc : "(null)", full ? full : "(null)");
        rc = -1;
    } else if (route != want_route) {
        printf("  %s: route %d, expected %d\n", what, (int)route, (int)want_route);
        rc = -1;
    }
    free(inc);
    free(full);
    return rc;
}

TEST(doc_mentions_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "src/Main.cs"), DM_MAIN);
    th_write_file(TH_PATH(repo, "src/Side.cs"), DM_SIDE);
    th_write_file(TH_PATH(repo, "src/P.cs"), DM_P);
    th_write_file(TH_PATH(repo, "src/Q.cs"), DM_Q);
    th_write_file(TH_PATH(repo, "src/Sig.cs"), DM_SIG);
    th_write_file(TH_PATH(repo, "src/Ov.cs"), DM_OV);
    th_write_file(TH_PATH(repo, "src/Tw.cs"), DM_TW);
    th_write_file(TH_PATH(repo, "src/R.cs"), DM_R);
    th_write_file(TH_PATH(repo, "src/S.cs"), DM_S);
    th_write_file(TH_PATH(repo, "src/App.csproj"), DM_CSPROJ);
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    char props[512];
    char reason[64];
    int n = 0;
    const char *main_cs = "src/Main.cs";
    const char *side_cs = "src/Side.cs";
    /* the preconditions the steps below move away from */
    dm_edge(inc_db, "Main.Main", "P.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(inc_db, "Main.Main", "R.ViaProject", props, sizeof(props), &n);
    ASSERT_EQ(n, 1); /* through the project file's <Using> */
    dm_edge(inc_db, "Main.Main", "Sig.Sig.Go", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(inc_db, "Main.Main", "Tw.Tw", props, sizeof(props), &n);
    ASSERT_EQ(n, 1); /* Tw<T>, the later declaration, owns the node */
    dm_row(inc_db, main_cs, "Dup", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous"); /* P.Dup and Q.Dup through the usings */
    dm_row(inc_db, main_cs, "Extra", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing"); /* namespace S is not imported */
    dm_row(inc_db, main_cs, "Ov.Run", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous"); /* an overload group */
    dm_row(inc_db, side_cs, "Tw", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap"); /* the arity-0 twin has no node */

    /* a doc-comment edit re-extracts the file alone */
    th_write_file(TH_PATH(repo, "src/Main.cs"), DM_MAIN_EDITED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    dm_edge(inc_db, "Main.Main", "Q.Other", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* a body edit in a TARGET file: its scope is unchanged, and the edge into
     * it stands */
    th_write_file(TH_PATH(repo, "src/Sig.cs"), DM_SIG_BODY);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "target body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    dm_edge(inc_db, "Main.Main", "Sig.Sig.Go", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* the file's own using directives scope nothing but the file */
    th_write_file(TH_PATH(repo, "src/Main.cs"), DM_MAIN_USING);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "using added", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    dm_edge(inc_db, "Main.Main", "S.Extra", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* a removed member (an overload group shrinks to one) re-resolves the
     * files whose rows name it, although they have no edge into the changed
     * file */
    th_write_file(TH_PATH(repo, "src/Ov.cs"), DM_OV_ONE);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "member removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    dm_edge(inc_db, "Main.Main", "Ov.Ov.Run", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* a removed TYPE is not repaired file by file (it may be another type's
     * base): one of two ambiguous candidates goes, the other binds */
    th_write_file(TH_PATH(repo, "src/P.cs"), DM_P_NO_DUP);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "type removed", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    dm_edge(inc_db, "Main.Main", "Q.Dup", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* renaming a target */
    th_write_file(TH_PATH(repo, "src/P.cs"), DM_P_RENAMED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "rename", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    dm_row(inc_db, main_cs, "Keep", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* an ambiguous twin */
    th_write_file(TH_PATH(repo, "src/Q.cs"), DM_Q_TWIN);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "ambiguous twin", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    dm_row(inc_db, main_cs, "Target", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "ambiguous");

    /* deleting a target */
    th_write_file(TH_PATH(repo, "src/P.cs"), DM_P_NO_TARGET);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "delete target", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    dm_edge(inc_db, "Main.Main", "Q.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* moving every declaration of a target file to another line changes no
     * scope: the stored scopes carry no line numbers */
    th_write_file(TH_PATH(repo, "src/Q.cs"), DM_Q_TWIN_MOVED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "lines moved", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    dm_edge(inc_db, "Main.Main", "Q.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* a changed signature can re-route references in files with no edge into
     * the changed one */
    th_write_file(TH_PATH(repo, "src/Sig.cs"), DM_SIG_CHANGED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "signature", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    dm_row(inc_db, main_cs, "Sig.Go(int)", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* declaration order decides which same-path declaration owns the node:
     * swapping the twins moves it, for a file with no edge into this one too */
    th_write_file(TH_PATH(repo, "src/Tw.cs"), DM_TW_SWAPPED);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "twins swapped", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    dm_edge(inc_db, "Side.Side", "Tw.Tw", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(inc_db, main_cs, "Tw{T}", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");

    /* the project file sets the global usings of every file of the project */
    th_write_file(TH_PATH(repo, "src/App.csproj"), DM_CSPROJ_NO_USING);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "project file", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    dm_edge(inc_db, "Main.Main", "R.ViaProject", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(inc_db, main_cs, "ViaProject", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* Q.Target, Q.Dup, Q.Other, S.Extra, Ov.Run */
    ASSERT_EQ(dm_mentions_from(inc_db, "Main.Main"), 5);

    /* a deleted file takes its namespace and types along */
    ASSERT_EQ(unlink(TH_PATH(repo, "src/Q.cs")), 0);
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "delete file", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    ASSERT_EQ(dm_mentions_from(inc_db, "Main.Main"), 2); /* S.Extra, Ov.Run */

    dm_unlink_db(inc_db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    PASS();
}

/* ── which scope changes are repairable file by file ─────────────── */

typedef struct {
    char names[256];
} dm_names_t;

static bool dm_name_put(void *ud, const char *name, size_t len) {
    dm_names_t *n = (dm_names_t *)ud;
    size_t used = strlen(n->names);
    if (used + len + 2 > sizeof(n->names)) {
        return false;
    }
    if (used > 0) {
        n->names[used++] = ',';
    }
    memcpy(n->names + used, name, len);
    n->names[used + len] = '\0';
    return true;
}

enum { DM_DELTA_SCAN_FAILED = -2 };

/* The scope delta between two versions of one C# file (NULL: the file does
 * not exist), through the real scanner and the persisted form; the removed
 * names joined by ','. */
static int dm_delta(const char *before, const char *after, char *names, size_t cap) {
    names[0] = '\0';
    CBMFileResult *a = before ? dm_scope(before) : NULL;
    CBMFileResult *b = after ? dm_scope(after) : NULL;
    char *pa = (a && a->doc_scope) ? cbm_doclink_portable_scope(a->doc_scope) : NULL;
    char *pb = (b && b->doc_scope) ? cbm_doclink_portable_scope(b->doc_scope) : NULL;
    dm_names_t n = {{0}};
    int rc = ((before && !pa) || (after && !pb))
                 ? DM_DELTA_SCAN_FAILED
                 : cbm_doclinks_scope_delta(pa, pb, dm_name_put, &n);
    snprintf(names, cap, "%s", n.names);
    cbm_free(CBM_MEM_CLASS_OTHER, pa);
    cbm_free(CBM_MEM_CLASS_OTHER, pb);
    if (a) {
        cbm_free_result(a);
    }
    if (b) {
        cbm_free_result(b);
    }
    return rc;
}

#define DM_DELTA_HEAD "using Acme.Local;\n"
#define DM_DELTA_OPEN "namespace N\n{\n    public class W : Base\n    {\n"
#define DM_DELTA_RUN_INT "        public void Run(int n) { }\n"
#define DM_DELTA_RUN_STR "        public void Run(string s) { }\n"
#define DM_DELTA_SIZE "        public int Size;\n"
#define DM_DELTA_CLOSE "    }\n    public class Other { }\n}\n"

TEST(doc_mentions_scope_delta_rules) {
    const char *base =
        DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE;
    struct {
        const char *what;
        const char *after;
        int want;
        const char *names;
    } cases[] = {
        {"unchanged", base, CBM_DOCLINK_DELTA_LOCAL, ""},
        /* nothing of the persisted scope carries a line number or a body */
        {"lines moved",
         "// moved\n\n" DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE
             DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"body edit",
         DM_DELTA_HEAD DM_DELTA_OPEN
         "        public void Run(int n) { n++; }\n" DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* the file's own usings scope nothing but the file */
        {"using added",
         DM_DELTA_HEAD "using Acme.More;\n" DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"using removed",
         DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        {"static using and alias added",
         DM_DELTA_HEAD "using static Acme.S;\nusing A = Acme.B;\n" DM_DELTA_OPEN DM_DELTA_RUN_INT
             DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, ""},
        /* ... a global using scopes every file of the project */
        {"global using added",
         "global using Acme.G;\n" DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* a removed member is reported by name */
        {"overload removed",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, "Run"},
        {"field removed",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, "Size"},
        {"two members removed", DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL, "Run,Size"},
        /* everything else can re-route references of files with no edge here */
        {"member added",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE
         "        public int More;\n" DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"signature changed",
         DM_DELTA_HEAD DM_DELTA_OPEN
         "        public void Run(long n) { }\n" DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"members reordered",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_STR DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type removed",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE "    }\n}\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"type added",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE
         "    }\n    public class Other { }\n    public class New { }\n}\n",
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"base changed",
         DM_DELTA_HEAD
         "namespace N\n{\n    public class W : Base2\n    {\n" DM_DELTA_RUN_INT DM_DELTA_RUN_STR
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"namespace renamed",
         DM_DELTA_HEAD
         "namespace M\n{\n    public class W : Base\n    {\n" DM_DELTA_RUN_INT DM_DELTA_RUN_STR
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        /* a member the parser can no longer show makes its type incomplete */
        {"member hidden",
         DM_DELTA_HEAD DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR
         "        public safe extern int Size();\n" DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"file deleted", NULL, CBM_DOCLINK_DELTA_GLOBAL, NULL},
    };
    char names[256];
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int got = dm_delta(base, cases[i].after, names, sizeof(names));
        if (got != cases[i].want || (cases[i].names && strcmp(names, cases[i].names) != 0)) {
            printf("  %s: delta %d [%s], want %d [%s]\n", cases[i].what, got, names, cases[i].want,
                   cases[i].names ? cases[i].names : "any");
            FAIL("scope delta");
        }
    }
    /* a scope that appears is as global as one that leaves */
    ASSERT_EQ(dm_delta(NULL, base, names, sizeof(names)), CBM_DOCLINK_DELTA_GLOBAL);
    /* no scope before and after: a file of a language without one */
    ASSERT_EQ(cbm_doclinks_scope_delta(NULL, NULL, dm_name_put, NULL), CBM_DOCLINK_DELTA_LOCAL);
    /* a blob no resolver claims is never repaired file by file */
    dm_names_t none = {{0}};
    ASSERT_EQ(
        cbm_doclinks_scope_delta("zz9\nM\t0\tc\t0\tW.Run\t\tint\n", "zz9\n", dm_name_put, &none),
        CBM_DOCLINK_DELTA_GLOBAL);
    ASSERT_STR_EQ(none.names, "");

    /* scope inputs: the MSBuild files that set a project's global usings */
    ASSERT_TRUE(cbm_doclinks_is_scope_input("src/App/App.csproj"));
    ASSERT_TRUE(cbm_doclinks_is_scope_input("Directory.Build.props"));
    ASSERT_TRUE(cbm_doclinks_is_scope_input("eng/Versions.TARGETS"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input("src/App/App.cs"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input("props"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input(NULL));
    PASS();
}

/* ── the parallel path == the sequential path ────────────────────── */

enum { DM_RING = 64 }; /* above MIN_FILES_FOR_PARALLEL */

/* Every class documents its successor, a shared target (by name, by overload
 * group and by signature) and a name nothing declares. */
static void dm_write_ring_fixture(const char *repo) {
    for (int i = 0; i < DM_RING; i++) {
        char path[512];
        char body[1024];
        snprintf(path, sizeof(path), "%s/src/C%02d.cs", repo, i);
        snprintf(body, sizeof(body),
                 "using Ring.Shared;\n"
                 "namespace Ring\n"
                 "{\n"
                 "    /// <summary>Next <see cref=\"C%02d\"/>, shared <seealso cref=\"Hub\"/>,\n"
                 "    /// <see cref=\"Hub.Run\"/>, <see cref=\"Nothing%02d\"/>.</summary>\n"
                 "    /// <exception cref=\"Hub.Run(int)\">x</exception>\n"
                 "    public class C%02d { }\n"
                 "}\n",
                 (i + 1) % DM_RING, i, i);
        th_write_file(path, body);
    }
    th_write_file(TH_PATH(repo, "src/Hub.cs"), "namespace Ring.Shared\n"
                                               "{\n"
                                               "    public class Hub\n"
                                               "    {\n"
                                               "        public void Run(int n) { }\n"
                                               "        public void Run(string s) { }\n"
                                               "    }\n"
                                               "}\n");
}

/* The worker pipeline (extract workers, resolve workers, per-file rows merged
 * at the end) and the sequential passes publish the same edges and rows. */
TEST(doc_mentions_parallel_equals_sequential) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_par_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    dm_write_ring_fixture(repo);
    char par_db[512];
    char seq_db[512];
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    const char *saved_workers = getenv("CBM_WORKERS");
    char *saved_workers_copy = saved_workers ? strdup(saved_workers) : NULL;
    cbm_setenv("CBM_WORKERS", "4", 1);
    int par_rc = dm_index(repo, par_db, NULL);
    cbm_setenv("CBM_WORKERS", "1", 1); /* one worker: the sequential passes */
    int seq_rc = dm_index(repo, seq_db, NULL);
    if (saved_workers_copy) {
        cbm_setenv("CBM_WORKERS", saved_workers_copy, 1);
        free(saved_workers_copy);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    ASSERT_EQ(par_rc, 0);
    ASSERT_EQ(seq_rc, 0);
    char *par = dm_doclink_state(par_db);
    char *seq = dm_doclink_state(seq_db);
    bool same = par && seq && strcmp(par, seq) == 0;
    if (!same) {
        printf("  parallel != sequential\n--- parallel\n%s--- sequential\n%s", par ? par : "(null)",
               seq ? seq : "(null)");
    }
    free(par);
    free(seq);
    ASSERT_TRUE(same);
    /* per class: successor (see), Hub (seealso), Hub.Run(int) (exception) */
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM edges WHERE type = 'MENTIONS'"), DM_RING * 3);
    /* per class: the overload group (ambiguous) and the undeclared name */
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM doc_link_unresolved"), DM_RING * 2);
    ASSERT_EQ(
        dm_count(par_db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'ambiguous'"),
        DM_RING);
    ASSERT_EQ(dm_count(par_db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'missing'"),
              DM_RING);

    dm_unlink_db(par_db);
    dm_unlink_db(seq_db);
    th_rmtree(tmp);
    PASS();
}

SUITE(doc_mentions) {
    RUN_TEST(doc_mentions_extract_cs_tokens);
    RUN_TEST(doc_mentions_cs_scope_blob);
    RUN_TEST(doc_mentions_cs_scope_parse_errors);
    RUN_TEST(doc_mentions_cs_norm_type);
    RUN_TEST(doc_mentions_msbuild_usings);
    RUN_TEST(doc_mentions_resolver_rules);
    RUN_TEST(doc_mentions_resolver_arity_and_members);
    RUN_TEST(doc_mentions_resolver_parse_errors);
    RUN_TEST(doc_mentions_ship_gate);
    RUN_TEST(doc_mentions_index_status_and_delete);
    RUN_TEST(doc_mentions_scope_delta_rules);
    RUN_TEST(doc_mentions_incremental_equals_full);
    RUN_TEST(doc_mentions_parallel_equals_sequential);
}
