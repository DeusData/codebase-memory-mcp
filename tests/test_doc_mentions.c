/*
 * test_doc_mentions.c — doc-comment references -> MENTIONS edges.
 *
 * Extraction (doclink.c / doclink_cs.c), the C# resolver (doc_links_cs.c),
 * MSBuild global usings, publication into doc_link_unresolved, the
 * index_status block, delete_project, and incremental == full across edits.
 * Every pipeline test indexes a real fixture through cbm_pipeline_run and
 * reads the published database (helpers: test_doc_mentions_helpers.h).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"
#include "lang_specs.h"
#include "foundation/compat_thread.h"
#include "foundation/mem_core.h"
#include "mcp/mcp.h"
#include "mcp/mcp_internal.h"
#include "pipeline/doc_links.h"
#include "pipeline/doc_links_msbuild.h"
#include "pipeline/lsp_surface.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "store/store.h"
#include "sqlite3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <sys/stat.h> /* mkfifo */
#include <unistd.h>   /* symlink */
#endif

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
    const char *src = "global using Acme.G; global using static Acme.GS;\n"                /* 1 */
                      "using static Acme.S;\n"                                             /* 2 */
                      "using A = Acme.Util.Helper<int>;\n"                                 /* 3 */
                      "namespace Outer.Inner\n"                                            /* 4 */
                      "{\n"                                                                /* 5 */
                      "    using Acme.Local;\n"                                            /* 6 */
                      "    public partial class W<T, U> : Base<T>, IThing\n"               /* 7 */
                      "    {\n"                                                            /* 8 */
                      "        void IThing.Do(int x) { }\n"                                /* 9 */
                      "        public void Go(ref string s, params int[] rest) { }\n"      /* 10 */
                      "        public event System.EventHandler Fired;\n"                  /* 11 */
                      "        public int P { get; set; }\n"                               /* 12 */
                      "        public record R(int Width);\n"                              /* 13 */
                      "        public delegate void D();\n"                                /* 14 */
                      "        public enum E { One }\n"                                    /* 15 */
                      "        public static W<T, U> operator +(W<T, U> a, int b) => a;\n" /* 16 */
                      "        public static implicit operator int(W<T, U> w) => 0;\n"     /* 17 */
                      "        public int this[int i] => i;\n"                             /* 18 */
                      "        public static int Count(string s) => 0;\n"                  /* 19 */
                      "        public static int Ext(this string s) => 0;\n"               /* 20 */
                      "        public const int Max = 1;\n"                                /* 21 */
                      "        public int field;\n"                                        /* 22 */
                      "        static W() { }\n"                                           /* 23 */
                      "        public W(T first) { }\n"                                    /* 24 */
                      "        public readonly record struct RS(int X);\n"                 /* 25 */
                      "    }\n"                                                            /* 26 */
                      "}\n";                                                               /* 27 */
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), CBM_LANG_CSHARP, "p", "W.cs", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT(strncmp(s, "cs1\n", 4) == 0);
    /* a using says what it brings in (n namespace, s static, a alias), and
     * `g` when it is a `global using`: of whatever kind */
    ASSERT_NOT_NULL(strstr(s, "U\t0\tng\t-\tAcme.G\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t0\ts\t-\tAcme.S\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t0\ta\tA\tAcme.Util.Helper<int>\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t0\tsg\t-\tAcme.GS\n"));
    /* a region carries its own name and the region it stands in */
    ASSERT_NOT_NULL(strstr(s, "R\t1\t0\t4\t27\tOuter.Inner\n"));
    ASSERT_NOT_NULL(strstr(s, "U\t1\tn\t-\tAcme.Local\n")); /* a namespace-block using */
    /* a type: `p` for partial, `-` for no outer type; a nested one names its
     * outer type by ordinal, a member its type */
    ASSERT_NOT_NULL(strstr(s, "T\t1\t7\t26\tcp\t-\tW\tT,U\tBase<T>|IThing\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t9\tc\t1\t0\tDo\t\tint\n")); /* explicit implementation */
    ASSERT_NOT_NULL(strstr(s, "M\t10\tc\t0\t0\tGo\t\tstring|int[]\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t11\te\t0\t0\tFired\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t12\tp\t0\t0\tP\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t13\t13\tr\t0\tR\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t13\tc\t0\t1\tR\t\tint\n"));   /* its primary constructor */
    ASSERT_NOT_NULL(strstr(s, "M\t13\tp\t0\t1\tWidth\t\t-\n")); /* positional record property */
    ASSERT_NOT_NULL(strstr(s, "T\t1\t14\t14\td\t0\tD\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t15\t15\te\t0\tE\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t15\tvs\t0\t3\tOne\t\t-\n")); /* an enum's member is static */
    /* operators, conversions and indexers are declared, under their token */
    ASSERT_NOT_NULL(strstr(s, "M\t16\to\t0\t0\t+\t\tW|int\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t17\to\t0\t0\timplicit\t\tW\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t18\tx\t0\t0\tthis\t\tint\n"));
    /* `s`: what a `using static` brings in -- static, and no extension method */
    ASSERT_NOT_NULL(strstr(s, "M\t19\tcs\t0\t0\tCount\t\tstring\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t20\tc\t0\t0\tExt\t\tstring\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t21\tvs\t0\t0\tMax\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t22\tv\t0\t0\tfield\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t23\tcs\t0\t0\tW\t\t\n")); /* the static constructor */
    ASSERT_NOT_NULL(strstr(s, "M\t24\tc\t0\t0\tW\t\tT\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t25\t25\tt\t0\tRS\t\t\n")); /* a record struct */
    /* the persisted scope drops every line number, nothing else */
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT_NOT_NULL(strstr(portable, "R\t1\t0\t0\t0\tOuter.Inner\n"));
    ASSERT_NOT_NULL(strstr(portable, "T\t1\t0\t0\tcp\t-\tW\tT,U\tBase<T>|IThing\n"));
    ASSERT_NOT_NULL(strstr(portable, "M\t0\tc\t0\t0\tGo\t\tstring|int[]\n"));
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);
    PASS();
}

/* Every name in a field/event declaration shares its static/const modifiers. */
TEST(doc_mentions_cs_declarator_modifiers) {
    const char *src = "class C {\n"
                      "[System.Obsolete] public int a, b;\n"
                      "[System.Obsolete] public static int c, d;\n"
                      "[System.Obsolete] public const int e = 1, f = 2;\n"
                      "[System.Obsolete] public event System.Action G, H;\n"
                      "[System.Obsolete] public static event System.Action I, J;\n"
                      "}\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_CSHARP, "Fields.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(strstr(s, "M\t2\tv\t0\t0\ta\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t2\tv\t0\t0\tb\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t3\tvs\t0\t0\tc\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t3\tvs\t0\t0\td\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t4\tvs\t0\t0\te\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t4\tvs\t0\t0\tf\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t5\te\t0\t0\tG\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t5\te\t0\t0\tH\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t6\tes\t0\t0\tI\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t6\tes\t0\t0\tJ\t\t-\n"));
    cbm_free_result(r);
    PASS();
}

/* The scope blob of one C# source; the result owns it. */
static CBMFileResult *dm_scope(const char *src) {
    return dm_extract(src, CBM_LANG_CSHARP, "S.cs");
}

/* Controlled parser/scanner observation inputs. No fixture is executed as C#.
 * Each name is exercised in both block and file-scoped declarations. */
static const struct {
    const char *id;
    const char *name;
    bool valid;
    bool recovery;
} dm_namespace_cases[] = {
    {"leading_dot", ".Bad", false, false},
    {"interior_empty", "Bad..Inner", false, false},
    {"trailing_dot", "Bad.", false, false},
    {"dot_only", ".", false, false},
    {"spaced_empty", "Bad. .Inner", false, false},
    {"verbatim_empty", "@Bad..@Inner", false, false},
    {"valid_dotted", "Good.Inner", true, false},
    {"valid_verbatim", "@Good.@class", true, false},
    {"valid_unicode", "Gr\xC3\xBCne.Inner", true, false},
    {"valid_recovery", "Good.Recovered", true, true},
    {"valid_prefix_comment", "/* note */ Good.Inner", true, false},
    {"valid_segment_comments", "Good /* note */ . /* note */ Inner", true, false},
    {"comment_empty", "Bad. /* note */ .Inner", false, false},
    {"trailing_recovery", "Bad.", false, true},
    {"verbatim_recovery", "@Good.@class", true, true},
};

static bool dm_namespace_source(char *out, size_t cap, size_t index, bool file_scoped) {
    const char *recovery = dm_namespace_cases[index].recovery
                               ? "public class ParseEdge { unsafe void M(void* p) { "
                                 "_r = ref *(int*)p; } }\n"
                               : "";
    int n = snprintf(out, cap,
                     "namespace %s%s"
                     "/// <summary><see cref=\"global::Good.Target\"/></summary>\n"
                     "public class FromBad { }\n"
                     "public class Hidden { }\n"
                     "%s%s",
                     dm_namespace_cases[index].name, file_scoped ? ";\n" : " {\n", recovery,
                     file_scoped ? "" : "}\n");
    return n >= 0 && (size_t)n < cap;
}

/* Real parser inputs, including both AST and lexical recovery paths. Valid
 * controls prevent treating every file with a parse error as unplaceable. */
TEST(doc_mentions_cs_namespace_scopes) {
    static const char *normalized[] = {NULL,
                                       NULL,
                                       NULL,
                                       NULL,
                                       NULL,
                                       NULL,
                                       "Good.Inner",
                                       "Good.class",
                                       "Gr\xC3\xBCne.Inner",
                                       "Good.Recovered",
                                       "Good.Inner",
                                       "Good.Inner",
                                       NULL,
                                       NULL,
                                       "Good.class"};
    bool correct = true;
    for (size_t i = 0; i < sizeof(dm_namespace_cases) / sizeof(dm_namespace_cases[0]); i++) {
        for (int file_scoped = 0; file_scoped < 2; file_scoped++) {
            char source[2048];
            bool made = dm_namespace_source(source, sizeof(source), i, file_scoped != 0);
            CBMFileResult *result = made ? dm_extract(source, CBM_LANG_CSHARP, "Bad.cs") : NULL;
            const char *scope = result ? result->doc_scope : NULL;
            int tokens = result ? dm_count_tokens(result, "global::Good.Target") : -1;
            bool valid = dm_namespace_cases[i].valid;
            bool shape = false;
            if (scope && valid) {
                char name[128];
                snprintf(name, sizeof(name), "\t%s\n", normalized[i]);
                shape = strstr(scope, "\nR\t1\t0\t") && strstr(scope, name) &&
                        strstr(scope, "\nT\t1\t3\t3\tc\t-\tFromBad\t\t\n") &&
                        strstr(scope, "\nT\t1\t4\t4\tc\t-\tHidden\t\t\n") &&
                        !strstr(scope, "\nX\t") && !strstr(scope, "\nQ\t");
            } else if (scope) {
                shape = strstr(scope, "\nX\t") && strstr(scope, "\nQ\tFromBad\n") &&
                        strstr(scope, "\nQ\tHidden\n") && !strstr(scope, "\nR\t") &&
                        !strstr(scope, "\nT\t");
            }
            bool one = made && scope && tokens == 1 && shape;
            fprintf(stderr,
                    "doc namespace scope case=%s file_scoped=%d valid=%d tokens=%d shape=%d "
                    "correct=%d\n",
                    dm_namespace_cases[i].id, file_scoped, valid, tokens, shape, one);
            correct = one && correct;
            if (result) {
                cbm_free_result(result);
            }
        }
    }
    ASSERT(correct);
    PASS();
}

/* Shared real source fixture: a malformed namespace must not erase the
 * independent Target link or permit Hidden to bind around an unknown declaration. */
static const char *dm_namespace_healthy_source = "namespace Good {\n"
                                                 "public class Target { }\n"
                                                 "public class Hidden { }\n"
                                                 "/// <summary><see cref=\"Target\"/></summary>\n"
                                                 "public class Uses { }\n"
                                                 "/// <summary><see cref=\"Hidden\"/></summary>\n"
                                                 "public class TriesHidden { }\n"
                                                 "}\n";

/* Check the persisted layer and the public status. No assertion here may skip
 * the caller's environment restoration or fixture cleanup. */
static bool dm_namespace_published(const char *db, const char *project, bool valid,
                                   const char *label) {
    char props[512], inside_reason[64], conflict_reason[64];
    int healthy = 0, inside = 0, conflict = 0;
    dm_edge(db, "Healthy.Uses", "Healthy.Target", props, sizeof(props), &healthy);
    dm_edge(db, "Bad.FromBad", "Healthy.Target", props, sizeof(props), &inside);
    dm_edge(db, "Healthy.TriesHidden", "Healthy.Hidden", props, sizeof(props), &conflict);
    dm_row(db, "Bad.cs", "global::Good.Target", inside_reason, sizeof(inside_reason), NULL, 0);
    dm_row(db, "Healthy.cs", "Hidden", conflict_reason, sizeof(conflict_reason), NULL, 0);
    int errors = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'");
    char *status = dm_index_status(project, false);
    const char *block = status ? strstr(status, "doc_links:\n") : NULL;
    bool status_ok = block && strstr(block, "\n  status: ok");
    int from_bad = dm_mentions_from(db, "Bad.FromBad");
    bool inside_ok = valid
                         ? inside == 1 && from_bad == 1 && !inside_reason[0]
                         : inside == 0 && from_bad == 0 && strcmp(inside_reason, "graph_gap") == 0;
    bool conflict_ok = valid ? conflict == 1 && !conflict_reason[0]
                             : conflict == 0 && strcmp(conflict_reason, "graph_gap") == 0;
    bool correct = errors == 0 && healthy == 1 && inside_ok && conflict_ok && status_ok;
    fprintf(stderr,
            "doc namespace published case=%s valid=%d errors=%d healthy=%d inside=%d "
            "inside_reason=%s conflict=%d conflict_reason=%s status_ok=%d correct=%d\n",
            label, valid, errors, healthy, inside, inside_reason, conflict, conflict_reason,
            status_ok, correct);
    free(status);
    return correct;
}

TEST(doc_mentions_cs_namespace_publication) {
    char tmp[256] = "/tmp/cbm_dm_ns_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400], cache[400], db[1024], full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(cache, sizeof(cache), "%s/cache", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    cbm_mkdir_p(cache, 0700);
    cbm_mkdir_p(repo, 0700); /* project identity canonicalizes the existing path */
    char *project = cbm_project_name_from_path(repo);
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved_cache ? strdup(saved_cache) : NULL;
    bool correct = project && (!saved_cache || saved_copy);
    if (!correct) {
        free(saved_copy);
        free(project);
        th_rmtree(tmp);
        ASSERT(correct);
    }
    snprintf(db, sizeof(db), "%s/%s.db", cache, project);
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    for (size_t i = 0; i < sizeof(dm_namespace_cases) / sizeof(dm_namespace_cases[0]); i++) {
        for (int file_scoped = 0; file_scoped < 2; file_scoped++) {
            char source[2048], label[128];
            bool made = dm_namespace_source(source, sizeof(source), i, file_scoped != 0);
            if (!made) {
                correct = false;
                continue;
            }
            snprintf(label, sizeof(label), "%s/%s", dm_namespace_cases[i].id,
                     file_scoped ? "file" : "block");
            dm_unlink_db(db);
            dm_unlink_db(full_db);
            th_write_file(TH_PATH(repo, "Healthy.cs"), dm_namespace_healthy_source);
            th_write_file(TH_PATH(repo, "Bad.cs"), source);
            char *indexed_project = NULL;
            bool indexed = dm_index(repo, db, &indexed_project) == 0;
            bool identity = indexed_project && strcmp(project, indexed_project) == 0;
            free(indexed_project);
            bool published =
                dm_namespace_published(db, project, dm_namespace_cases[i].valid, label);
            char *before = dm_doclink_state(db);
            cbm_pipeline_incremental_test_reset_faults();
            bool repeated = dm_index(repo, db, NULL) == 0;
            cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
            char *after = dm_doclink_state(db);
            bool stable = before && after && strcmp(before, after) == 0;
            bool noop = route == CBM_INCREMENTAL_ROUTE_NOOP;
            fprintf(stderr,
                    "doc namespace unchanged case=%s indexed=%d identity=%d repeated=%d stable=%d "
                    "noop=%d "
                    "route=%d\n",
                    label, indexed, identity, repeated, stable, noop, (int)route);
            correct = indexed && identity && published && repeated && stable && noop && correct;
            free(before);
            free(after);

            /* These three cases cover clipped names, accepted trailing dots,
             * and scope blobs that formerly failed the entire layer. Exercise
             * both reuse of stored scopes and edits into/out of the bad region. */
            if (i == 0 || i == 2 || i == 4) {
                char edited[2048];
                snprintf(edited, sizeof(edited), "%s\n// body-only edit\n",
                         dm_namespace_healthy_source);
                th_write_file(TH_PATH(repo, "Healthy.cs"), edited);
                int step = dm_step(repo, db, full_db, label, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
                bool state = dm_namespace_published(db, project, false, label);
                correct = step == 0 && state && correct;

                bool repaired = dm_namespace_source(edited, sizeof(edited), 6, file_scoped != 0);
                if (repaired) {
                    th_write_file(TH_PATH(repo, "Bad.cs"), edited);
                    step = dm_step(repo, db, full_db, "namespace repaired",
                                   CBM_INCREMENTAL_ROUTE_FORCED_FULL);
                    state = dm_namespace_published(db, project, true, "namespace repaired");
                    correct = step == 0 && state && correct;
                    th_write_file(TH_PATH(repo, "Bad.cs"), source);
                    step = dm_step(repo, db, full_db, "namespace malformed again",
                                   CBM_INCREMENTAL_ROUTE_FORCED_FULL);
                    state = dm_namespace_published(db, project, false, label);
                    correct = step == 0 && state && correct;
                } else {
                    correct = false;
                }
            }
        }
    }
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    free(project);
    dm_unlink_db(db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    ASSERT(correct);
    PASS();
}

/* Observe a controlled fixture's recovery path without borrowing CBM's parser
 * or retaining Tree-sitter objects across extraction. */
static bool dm_namespace_parser_state(const char *source, size_t length, bool *has_namespace,
                                      bool *has_error) {
    TSParser *parser = ts_parser_new();
    TSTree *tree = NULL;
    TSTreeCursor cursor = {0};
    bool cursor_live = false, correct = false;
    *has_namespace = false;
    *has_error = false;
    if (!parser || !ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_CSHARP))) {
        goto cleanup;
    }
    tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    if (!tree) {
        goto cleanup;
    }
    TSNode root = ts_tree_root_node(tree);
    *has_error = ts_node_has_error(root);
    cursor = ts_tree_cursor_new(root);
    cursor_live = true;
    size_t visited = 0;
    for (;;) {
        if (++visited > 4096) {
            goto cleanup;
        }
        const char *kind = ts_node_type(ts_tree_cursor_current_node(&cursor));
        if (strcmp(kind, "namespace_declaration") == 0 ||
            strcmp(kind, "file_scoped_namespace_declaration") == 0) {
            *has_namespace = true;
            correct = true;
            goto cleanup;
        }
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                correct = true;
                goto cleanup;
            }
        }
    }
cleanup:
    if (cursor_live) {
        ts_tree_cursor_delete(&cursor);
    }
    if (tree) {
        ts_tree_delete(tree);
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    return correct;
}

/* Namespace token boundaries must preserve verbatim identifiers without
 * accepting bare type keywords as recovered namespace names. */
TEST(doc_mentions_cs_namespace_boundaries) {
    static const struct {
        const char *id, *header, *footer, *name;
    } cases[] = {
        {"bare_keyword", "namespace class{\n",
         "public class ParseEdge { unsafe void M(void* p) { _r = ref *(int*)p; } }\n}\n", NULL},
        {"adjacent_file", "namespace@Boundary;\n", "", "Boundary"},
        {"adjacent_block", "namespace@Boundary {\n", "}\n", "Boundary"},
        {"verbatim_keyword_file", "namespace @class;\n", "", "class"},
        {"verbatim_keyword_block", "namespace @class {\n", "}\n", "class"},
    };
    bool correct = true;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char source[2048], normalized[128];
        int n = snprintf(source, sizeof(source),
                         "%s/// <summary><see cref=\"global::Good.Target\"/></summary>\n"
                         "public class FromBad { }\npublic class Hidden { }\n%s",
                         cases[i].header, cases[i].footer);
        bool has_namespace = false, has_error = false;
        bool recovery = cases[i].name ||
                        (n > 0 && (size_t)n < sizeof(source) &&
                         dm_namespace_parser_state(source, (size_t)n, &has_namespace, &has_error) &&
                         !has_namespace && has_error);
        CBMFileResult *result = n > 0 && (size_t)n < sizeof(source)
                                    ? dm_extract(source, CBM_LANG_CSHARP, "Bad.cs")
                                    : NULL;
        const char *scope = result ? result->doc_scope : NULL;
        int tokens = result ? dm_count_tokens(result, "global::Good.Target") : -1;
        bool placed = scope && strstr(scope, "\nR\t");
        bool quarantined = scope && strstr(scope, "\nX\t") && strstr(scope, "\nQ\tFromBad\n") &&
                           strstr(scope, "\nQ\tHidden\n") && !strstr(scope, "\nT\t");
        bool shape = false;
        if (scope && cases[i].name) {
            snprintf(normalized, sizeof(normalized), "\t%s\n", cases[i].name);
            shape = placed && strstr(scope, normalized) &&
                    strstr(scope, "\nT\t1\t3\t3\tc\t-\tFromBad\t\t\n") &&
                    strstr(scope, "\nT\t1\t4\t4\tc\t-\tHidden\t\t\n") && !strstr(scope, "\nX\t") &&
                    !strstr(scope, "\nQ\t");
        } else if (scope) {
            shape = !placed && quarantined;
        }
        bool one = recovery && tokens == 1 && shape;
        fprintf(stderr,
                "doc namespace boundary case=%s tokens=%d placed=%d quarantined=%d "
                "recovery=%d namespace_node=%d parser_error=%d correct=%d\n",
                cases[i].id, tokens, placed, quarantined, recovery, has_namespace, has_error, one);
        correct = one && correct;
        if (result) {
            cbm_free_result(result);
        }
    }
    ASSERT(correct);
    PASS();
}

static const struct {
    const char *id, *text;
} dm_control_cases[] = {
    {"using_name", "using Goo~d.Inner;\n"},
    {"using_dot_before", "using Good~.Inner;\n"},
    {"using_dot_after", "using Good.~Inner;\n"},
    {"using_alias", "using Al~ias = Good.Target;\n"},
    {"using_target", "using Alias = Good.Tar~get;\n"},
    {"using_comment", "using Alias = Good./*~*/Target;\n"},
    {"base_name", "public class Before : Goo~d.Base { }\n"},
    {"base_generic", "public class Before : Good.Ba~se<int> { }\n"},
    {"base_comment", "public class Before : Good./*~*/Base { }\n"},
    {"type_parameter", "public class Before<T~Item> { }\n"},
    {"method_parameter", "public class Before { public void M<T~Item>() { } }\n"},
    {"signature", "public class Before { public void M(Good.Tar~get x) { } }\n"},
};

/* The returned length includes the replaced marker even when byte is NUL. */
static int dm_control_source(char *source, size_t capacity, size_t which, unsigned byte) {
    int n = snprintf(source, capacity,
                     "%s/// <see cref=\"global::Good.Target\"/>\n"
                     "public class Tail { }\nnamespace . { public class Hidden { } }\n",
                     dm_control_cases[which].text);
    if (n <= 0 || (size_t)n >= capacity) {
        return -1;
    }
    char *mark = strchr(source, '~');
    if (!mark) {
        return -1;
    }
    *mark = (char)byte;
    return n;
}

static bool dm_control_write(const char *path, const char *source, size_t length) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return false;
    }
    bool complete = fwrite(source, 1, length, file) == length;
    return fclose(file) == 0 && complete;
}

/* Establish the routing contract using complete byte spans, including NUL.
 * Own imports are local; changes to a declared base can affect other files. */
static bool dm_control_delta(const char *before, int before_length, const char *after,
                             int after_length, int expected, const char *label) {
    CBMFileResult *a =
        cbm_extract_file(before, before_length, CBM_LANG_CSHARP, "p", "Bad.cs", 0, NULL, NULL);
    CBMFileResult *b =
        cbm_extract_file(after, after_length, CBM_LANG_CSHARP, "p", "Bad.cs", 0, NULL, NULL);
    char *pa = a && a->doc_scope ? cbm_doclink_portable_scope(a->doc_scope) : NULL;
    char *pb = b && b->doc_scope ? cbm_doclink_portable_scope(b->doc_scope) : NULL;
    dm_names_t names = {{0}};
    int delta =
        pa && pb ? cbm_doclinks_scope_delta(pa, pb, dm_name_put, &names) : DM_DELTA_SCAN_FAILED;
    bool changed = pa && pb && strcmp(pa, pb) != 0;
    bool correct = changed && delta == expected;
    fprintf(stderr, "doc control delta case=%s changed=%d delta=%d expected=%d correct=%d\n", label,
            changed, delta, expected, correct);
    cbm_free(CBM_MEM_CLASS_OTHER, pa);
    cbm_free(CBM_MEM_CLASS_OTHER, pb);
    if (a) {
        cbm_free_result(a);
    }
    if (b) {
        cbm_free_result(b);
    }
    return correct;
}

TEST(doc_mentions_cs_control_scopes) {
    static const unsigned bytes[] = {0, 1, 127, 32};
    bool correct = true;
    for (size_t i = 0; i < sizeof(dm_control_cases) / sizeof(dm_control_cases[0]); i++) {
        for (size_t j = 0; j < sizeof(bytes) / sizeof(bytes[0]); j++) {
            char source[2048];
            int length = dm_control_source(source, sizeof(source), i, bytes[j]);
            cbm_doclink_cs_test_cost_reset();
            CBMFileResult *result = length > 0 ? cbm_extract_file(source, length, CBM_LANG_CSHARP,
                                                                  "p", "Control.cs", 0, NULL, NULL)
                                               : NULL;
            uint64_t built = cbm_doclink_cs_test_scope_bytes();
            const char *scope = result ? result->doc_scope : NULL;
            size_t visible = scope ? strlen(scope) : 0;
            bool clean = scope != NULL;
            for (size_t k = 0; k < visible; k++) {
                unsigned char c = (unsigned char)scope[k];
                clean = clean && ((c >= 32 && c != 127) || c == '\t' || c == '\n');
            }
            int tokens = result ? dm_count_tokens(result, "global::Good.Target") : -1;
            bool tail = scope && strstr(scope, "\tTail\t");
            bool quarantine = scope && strstr(scope, "\nQ\tHidden\n") && strstr(scope, "\nX\t");
            bool marker = true;
            if (bytes[j] != 32 && (i == 1 || i == 2)) {
                marker = scope && strstr(scope, "\nU\t0\tn\t-\t?\n");
            } else if (bytes[j] != 32 && i == 4) {
                marker = scope && strstr(scope, "\nU\t0\ta\tAlias\t?\n");
            } else if (bytes[j] != 32 && (i == 6 || i == 7 || i == 8)) {
                marker = scope && strstr(scope, "\tBefore\t\t?\n");
            }
            bool one =
                scope && built == visible && clean && tokens == 1 && tail && quarantine && marker;
            fprintf(stderr,
                    "doc control scope case=%s byte=%u built=%llu visible=%zu clean=%d "
                    "tokens=%d tail=%d quarantine=%d marker=%d correct=%d\n",
                    dm_control_cases[i].id, bytes[j], (unsigned long long)built, visible, clean,
                    tokens, tail, quarantine, marker, one);
            correct = one && correct;
            if (result) {
                cbm_free_result(result);
            }
        }
    }
    ASSERT(correct);
    PASS();
}

static bool dm_control_published(const char *db, const char *project, const char *label) {
    char props[512], tail_reason[64], conflict_reason[64];
    int healthy = 0, tail = 0, conflict = 0;
    dm_edge(db, "Healthy.Uses", "Healthy.Target", props, sizeof(props), &healthy);
    dm_edge(db, "Bad.Tail", "Healthy.Target", props, sizeof(props), &tail);
    dm_edge(db, "Healthy.TriesHidden", "Healthy.Hidden", props, sizeof(props), &conflict);
    dm_row(db, "Bad.cs", "global::Good.Target", tail_reason, sizeof(tail_reason), NULL, 0);
    dm_row(db, "Healthy.cs", "Hidden", conflict_reason, sizeof(conflict_reason), NULL, 0);
    int errors = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'");
    char *status = dm_index_status(project, false);
    const char *block = status ? strstr(status, "doc_links:\n") : NULL;
    bool status_ok = block && strstr(block, "\n  status: ok");
    bool correct = healthy == 1 && tail == 1 && !tail_reason[0] && conflict == 0 &&
                   strcmp(conflict_reason, "graph_gap") == 0 && errors == 0 && status_ok;
    fprintf(stderr,
            "doc control published case=%s healthy=%d tail=%d tail_reason=%s conflict=%d "
            "conflict_reason=%s errors=%d status_ok=%d correct=%d\n",
            label, healthy, tail, tail_reason, conflict, conflict_reason, errors, status_ok,
            correct);
    free(status);
    return correct;
}

TEST(doc_mentions_cs_control_publication) {
    static const size_t cases[] = {1, 2, 4, 6, 7, 8};
    static const unsigned bytes[] = {0, 1, 127, 32};
    char tmp[256] = "/tmp/cbm_dm_control_XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400], cache[400], db[1024], full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(cache, sizeof(cache), "%s/cache", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    cbm_mkdir_p(cache, 0700);
    cbm_mkdir_p(repo, 0700);
    char *project = cbm_project_name_from_path(repo);
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved_cache ? strdup(saved_cache) : NULL;
    bool correct = project && (!saved_cache || saved_copy);
    if (!correct) {
        free(saved_copy);
        free(project);
        th_rmtree(tmp);
        ASSERT(correct);
    }
    snprintf(db, sizeof(db), "%s/%s.db", cache, project);
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (size_t j = 0; j < sizeof(bytes) / sizeof(bytes[0]); j++) {
            char source[2048], label[128];
            int length = dm_control_source(source, sizeof(source), cases[i], bytes[j]);
            snprintf(label, sizeof(label), "%s/%u", dm_control_cases[cases[i]].id, bytes[j]);
            dm_unlink_db(db);
            dm_unlink_db(full_db);
            th_write_file(TH_PATH(repo, "Healthy.cs"), dm_namespace_healthy_source);
            bool written =
                length > 0 && dm_control_write(TH_PATH(repo, "Bad.cs"), source, (size_t)length);
            char *indexed_project = NULL;
            bool indexed = written && dm_index(repo, db, &indexed_project) == 0;
            bool identity = indexed_project && strcmp(project, indexed_project) == 0;
            free(indexed_project);
            bool published = dm_control_published(db, project, label);
            char *before = dm_doclink_state(db);
            cbm_pipeline_incremental_test_reset_faults();
            bool repeated = dm_index(repo, db, NULL) == 0;
            cbm_incremental_route_t route = cbm_pipeline_incremental_test_last_route();
            char *after = dm_doclink_state(db);
            bool stable = before && after && strcmp(before, after) == 0;
            bool noop = route == CBM_INCREMENTAL_ROUTE_NOOP;
            fprintf(stderr,
                    "doc control unchanged case=%s written=%d indexed=%d identity=%d repeated=%d "
                    "stable=%d noop=%d route=%d\n",
                    label, written, indexed, identity, repeated, stable, noop, (int)route);
            correct = written && indexed && identity && published && repeated && stable && noop &&
                      correct;
            free(before);
            free(after);

            /* Cover both an import and a base field across stored-scope reuse,
             * a repaired field and reintroduction of the embedded NUL. */
            if (bytes[j] == 0 && (cases[i] == 1 || cases[i] == 7)) {
                char repaired[2048], edited[2048], step_label[160];
                cbm_incremental_route_t changed_route = cases[i] == 1
                                                            ? CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR
                                                            : CBM_INCREMENTAL_ROUTE_FORCED_FULL;
                int changed_delta =
                    cases[i] == 1 ? CBM_DOCLINK_DELTA_LOCAL : CBM_DOCLINK_DELTA_GLOBAL;
                snprintf(edited, sizeof(edited), "%s\n// body-only edit\n",
                         dm_namespace_healthy_source);
                th_write_file(TH_PATH(repo, "Healthy.cs"), edited);
                snprintf(step_label, sizeof(step_label), "%s/reuse", label);
                int step =
                    dm_step(repo, db, full_db, step_label, CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR);
                bool state = dm_control_published(db, project, label);
                correct = step == 0 && state && correct;

                int repaired_length = dm_control_source(repaired, sizeof(repaired), cases[i], 32);
                bool repaired_written =
                    repaired_length > 0 &&
                    dm_control_write(TH_PATH(repo, "Bad.cs"), repaired, (size_t)repaired_length);
                if (repaired_written) {
                    snprintf(step_label, sizeof(step_label), "%s/repaired", label);
                    bool delta = dm_control_delta(source, length, repaired, repaired_length,
                                                  changed_delta, step_label);
                    step = dm_step(repo, db, full_db, step_label, changed_route);
                    state = dm_control_published(db, project, step_label);
                    correct = delta && step == 0 && state && correct;
                    bool restored =
                        dm_control_write(TH_PATH(repo, "Bad.cs"), source, (size_t)length);
                    if (restored) {
                        snprintf(step_label, sizeof(step_label), "%s/reintroduced", label);
                        delta = dm_control_delta(repaired, repaired_length, source, length,
                                                 changed_delta, step_label);
                        step = dm_step(repo, db, full_db, step_label, changed_route);
                        state = dm_control_published(db, project, label);
                        correct = delta && step == 0 && state && correct;
                    } else {
                        correct = false;
                    }
                } else {
                    correct = false;
                }
            }
        }
    }
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    free(project);
    dm_unlink_db(db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    ASSERT(correct);
    PASS();
}

/* Temporary observation: extract real source bytes, then exercise the public
 * surface codec with complete definitions and with each input isolated. */
static bool dm_utf8_probe_surface(CBMFileResult *bad, CBMFileResult *healthy, CBMLanguage language,
                                  const char *path, const char *label, const char *bytes) {
    CBMFileResult *cache[] = {bad, healthy};
    cbm_file_info_t files[] = {{.rel_path = (char *)path, .language = language},
                               {.rel_path = "Healthy.cs", .language = CBM_LANG_CSHARP}};
    CBMArena arena;
    cbm_arena_init(&arena);
    char *modules[2] = {NULL, NULL};
    int starts[3] = {0}, def_count = 0;
    CBMLSPDef *defs =
        cbm_pxc_collect_all_defs(NULL, &arena, cache, files, 2, "p", modules, &def_count, starts);
    cbm_lsp_surface_row_t *rows = NULL, *again = NULL;
    int count = 0, again_count = 0;
    int full_rc =
        cbm_lsp_surface_build_rows(NULL, "p", cache, files, 2, defs, starts, &rows, &count);
    int full_count = count;
    int again_rc =
        cbm_lsp_surface_build_rows(NULL, "p", cache, files, 2, defs, starts, &again, &again_count);
    bool repeat = full_rc == again_rc && count == again_count;
    bool dl_equal = false;
    char *portable = bad->doc_scope ? cbm_doclink_portable_scope(bad->doc_scope) : NULL;
    if (full_rc == 0 && count == 2 && again_rc == 0 && again_count == 2) {
        for (int i = 0; i < count; i++) {
            repeat = repeat && strcmp(rows[i].defs_json, again[i].defs_json) == 0 &&
                     strcmp(rows[i].surface_sha, again[i].surface_sha) == 0;
        }
        yyjson_doc *doc = yyjson_read(rows[0].defs_json, strlen(rows[0].defs_json), 0);
        yyjson_val *dl = doc ? yyjson_obj_get(yyjson_doc_get_root(doc), "dl") : NULL;
        dl_equal = portable && yyjson_is_str(dl) && strcmp(portable, yyjson_get_str(dl)) == 0;
        yyjson_doc_free(doc);
    }
    cbm_store_free_lsp_surfaces(rows, count);
    cbm_store_free_lsp_surfaces(again, again_count);
    rows = NULL;
    count = 0;
    const char *saved_scope = bad->doc_scope;
    bad->doc_scope = NULL;
    int nodl_rc =
        cbm_lsp_surface_build_rows(NULL, "p", cache, files, 2, defs, starts, &rows, &count);
    int nodl_count = count;
    bad->doc_scope = saved_scope;
    cbm_store_free_lsp_surfaces(rows, count);
    rows = NULL;
    count = 0;
    CBMFileResult scope_only = {.doc_scope = saved_scope};
    cache[0] = &scope_only;
    int scope_rc =
        cbm_lsp_surface_build_rows(NULL, "p", cache, files, 2, NULL, NULL, &rows, &count);
    bool retained = saved_scope && strstr(saved_scope, bytes) != NULL;
    printf("doc_utf8_probe case=%s scope=%d retained=%d defs=%d full_rc=%d full_count=%d "
           "nodl_rc=%d nodl_count=%d scope_rc=%d scope_count=%d repeat=%d dl_equal=%d\n",
           label, saved_scope != NULL, retained, def_count, full_rc, full_count, nodl_rc,
           nodl_count, scope_rc, count, repeat, dl_equal);
    cbm_store_free_lsp_surfaces(rows, count);
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    free(defs);
    free(modules[0]);
    free(modules[1]);
    cbm_arena_destroy(&arena);
    return saved_scope != NULL && def_count > 0 && repeat;
}

TEST(doc_mentions_cs_utf8_observation) {
    static const struct {
        const char *label, *bytes;
    } sequences[] = {
        {"ascii", "A"},
        {"valid2", "\xC3\xA9"},
        {"valid3", "\xE6\xBC\xA2"},
        {"valid4", "\xF0\x90\x90\x80"},
        {"continuation", "\x80"},
        {"overlong", "\xC0\xAF"},
        {"surrogate", "\xED\xA0\x80"},
        {"above_limit", "\xF4\x90\x80\x80"},
        {"truncated", "\xE2\x82"},
    };
    static const struct {
        const char *label, *before, *after;
        CBMLanguage language;
    } positions[] = {
        {"using_target", "using Acme.", "Name;\nclass Local {}\n", CBM_LANG_CSHARP},
        {"alias_target", "using Alias = Acme.", "Name;\nclass Local {}\n", CBM_LANG_CSHARP},
        {"namespace_name", "namespace N", "Name { class Local {} }\n", CBM_LANG_CSHARP},
        {"type_name", "class N", "Name {}\n", CBM_LANG_CSHARP},
        {"method_name", "class Local { void N", "Name() {} }\n", CBM_LANG_CSHARP},
        {"parameter_type", "class Local { void M(N", "Name arg) {} }\n", CBM_LANG_CSHARP},
        {"type_parameter", "class Local<N", "Name> {}\n", CBM_LANG_CSHARP},
        {"method_parameter", "class Local { void M<N", "Name>() {} }\n", CBM_LANG_CSHARP},
        {"field_name", "class Local { int N", "Name; }\n", CBM_LANG_CSHARP},
        {"base_type", "class Local : Acme.N", "Name {}\n", CBM_LANG_CSHARP},
        {"property_value", "<Project><PropertyGroup><P>N", "Name</P></PropertyGroup></Project>",
         CBM_LANG_XML},
        {"property_condition", "<Project><PropertyGroup Condition=\"'N",
         "Name'=='yes'\"><P>Value</P></PropertyGroup></Project>", CBM_LANG_XML},
        {"using_include", "<Project><ItemGroup><Using Include=\"Acme.N",
         "Name\" /></ItemGroup></Project>", CBM_LANG_XML},
        {"using_alias", "<Project><ItemGroup><Using Include=\"Acme.Target\" Alias=\"N",
         "Name\" /></ItemGroup></Project>", CBM_LANG_XML},
        {"import_project", "<Project><Import Project=\"N", "Name.props\" /></Project>",
         CBM_LANG_XML},
    };
    const char *tail = "/// <summary><see cref=\"Good.Target\"/></summary>\nclass Tail {}\n";
    CBMFileResult *healthy =
        dm_extract("namespace Good { public class Target {} }\n", CBM_LANG_CSHARP, "Healthy.cs");
    ASSERT_NOT_NULL(healthy);
    bool correct = healthy->doc_scope != NULL;
    cbm_file_info_t healthy_file = {.rel_path = "Healthy.cs", .language = CBM_LANG_CSHARP};
    cbm_lsp_surface_row_t *healthy_rows = NULL;
    int healthy_count = 0;
    int healthy_rc = cbm_lsp_surface_build_rows(NULL, "p", &healthy, &healthy_file, 1, NULL, NULL,
                                                &healthy_rows, &healthy_count);
    printf("doc_utf8_probe_healthy rc=%d count=%d\n", healthy_rc, healthy_count);
    correct = correct && healthy_rc == 0 && healthy_count == 1;
    cbm_store_free_lsp_surfaces(healthy_rows, healthy_count);
    for (size_t p = 0; p < sizeof(positions) / sizeof(positions[0]); p++) {
        for (size_t s = 0; s < sizeof(sequences) / sizeof(sequences[0]); s++) {
            char source[2048], label[96];
            const char *path = positions[p].language == CBM_LANG_CSHARP ? "Bad.cs" : "App.csproj";
            size_t a = strlen(positions[p].before), b = strlen(sequences[s].bytes);
            size_t c = strlen(positions[p].after);
            size_t d = positions[p].language == CBM_LANG_CSHARP ? strlen(tail) : 0;
            memcpy(source, positions[p].before, a);
            memcpy(source + a, sequences[s].bytes, b);
            memcpy(source + a + b, positions[p].after, c);
            if (d) {
                memcpy(source + a + b + c, tail, d);
            }
            source[a + b + c + d] = '\0';
            CBMFileResult *bad = cbm_extract_file(source, (int)(a + b + c + d),
                                                  positions[p].language, "p", path, 0, NULL, NULL);
            snprintf(label, sizeof(label), "%s/%s", positions[p].label, sequences[s].label);
            bool setup = bad && dm_utf8_probe_surface(bad, healthy, positions[p].language, path,
                                                      label, sequences[s].bytes);
            correct = setup && correct;
            cbm_free_result(bad);
        }
    }
    static const char *entities[] = {"&#xD800;", "&#55296;",  "&#xDFFF;", "&#x110000;",
                                     "&#xE9;",   "&#x10400;", "&#x1F600;"};
    for (int position = 0; position < 2; position++) {
        for (size_t e = 0; e < sizeof(entities) / sizeof(entities[0]); e++) {
            char source[1024], label[96];
            const char *format =
                position == 0 ? "<Project><PropertyGroup><P>N%sName</P></PropertyGroup></Project>"
                              : "<Project><ItemGroup><Using Include=\"Acme.N%sName\" "
                                "/></ItemGroup></Project>";
            int length = snprintf(source, sizeof(source), format, entities[e]);
            CBMFileResult *bad =
                cbm_extract_file(source, length, CBM_LANG_XML, "p", "App.csproj", 0, NULL, NULL);
            snprintf(label, sizeof(label), "entity_%s/%zu", position == 0 ? "property" : "using",
                     e);
            bool setup = bad && dm_utf8_probe_surface(bad, healthy, CBM_LANG_XML, "App.csproj",
                                                      label, entities[e]);
            correct = setup && correct;
            cbm_free_result(bad);
        }
    }
    cbm_free_result(healthy);
    ASSERT(correct);
    PASS();
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
        strstr(s, "T\t1\t3\t7\tc!\t-\tOne\t\t\n")); /* ends at its own brace; a member is hidden */
    ASSERT_NOT_NULL(strstr(s, "M\t5\tc\t0\t0\tM\t\tvoid*\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t8\t8\tc\t-\tTwo\t\t\n")); /* a sibling in A, not in One */
    ASSERT_NOT_NULL(strstr(s, "R\t2\t0\t10\t13\tB\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t2\t12\t12\tc\t-\tThree\t\t\n"));
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
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t3\tc\t-\tBefore\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t4\t7\tsp!\t-\tIter\tT\t?\n")); /* partial, incomplete */
    ASSERT_NOT_NULL(strstr(s, "T\t1\t8\t11\tc\t-\tAfter\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t10\tv\t0\t2\tSize\t\t-\n")); /* of the third type */
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
    ASSERT_NOT_NULL(strstr(s, "T\t1\t4\t9\tc!\t-\tCred\t\t"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t6\t9\tc!\t-\tCred\t\t"));
    ASSERT_NOT_NULL(strstr(s, "M\t8\tv\t0\t1\tSize\t\t-\n"));
    ASSERT_NOT_NULL(strstr(s, "T\t1\t10\t10\tc\t-\tAfter\t\t\n"));
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
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t7\tc!\t-\tVerb\t\t"));
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
    ASSERT_NOT_NULL(strstr(s, "T\t1\t3\t7\tc!\t-\tHoley\t\t\n"));
    ASSERT_NOT_NULL(strstr(s, "M\t6\tv\t0\t0\tSeen\t\t-\n"));
    ASSERT_NULL(strstr(s, "\textern\t"));
    ASSERT_NULL(strstr(s, "\tHidden\t"));
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
    /* a name that does not fit the caller's buffer is a type nothing is known
     * about, never a cut name: two long names with one beginning would
     * compare equal */
    char small[8];
    const char *long_name = "LongTypeNameOne";
    ASSERT_EQ(cbm_doclink_cs_norm_type(long_name, strlen(long_name), small, sizeof(small)), 1);
    ASSERT_STR_EQ(small, "?");
    ASSERT_EQ(cbm_doclink_cs_norm_type("Fits", 4, small, sizeof(small)), 4);
    ASSERT_STR_EQ(small, "Fits");
    PASS();
}

/* ── MSBuild global usings (R1) ──────────────────────────────────── */

/* A project file of a test: its path in the repository and its text. */
typedef struct {
    const char *rel_path;
    const char *xml;
} dm_project_file_t;

/* Evaluate `project` over `files`, each through the extractor's scope blob:
 * no file is written, none is read. The result is released with
 * cbm_msb_result_free. false when a file yields no blob or memory runs out. */
static bool dm_msb_eval(const dm_project_file_t *files, int n, const char *project,
                        cbm_msb_result_t *out) {
    cbm_msb_t *m = cbm_msb_new();
    bool ok = m != NULL;
    for (int i = 0; ok && i < n; i++) {
        CBMFileResult *r = dm_extract(files[i].xml, CBM_LANG_XML, files[i].rel_path);
        ok = r && r->doc_scope && cbm_msb_is_project_scope(r->doc_scope) &&
             cbm_msb_add(m, files[i].rel_path, r->doc_scope);
        if (r) {
            cbm_free_result(r);
        }
    }
    ok = ok && cbm_msb_eval(m, project, out);
    cbm_msb_free(m);
    return ok;
}

/* True when the result holds a using of this kind (n namespace, s static,
 * a alias) and target. */
static bool dm_has_using(const cbm_msb_result_t *r, char kind, const char *target) {
    for (int i = 0; i < r->count; i++) {
        if (r->usings[i].kind == kind && strcmp(r->usings[i].target, target) == 0) {
            return true;
        }
    }
    return false;
}

/* Repeated expansion must not retain every superseded value or condition
 * operand. Measure real evaluation arena capacity, not a wall-clock sample. */
static bool dm_msb_add_xml(cbm_msb_t *m, const char *path, const char *xml) {
    CBMFileResult *source = dm_extract(xml, CBM_LANG_XML, path);
    bool ok = source && source->doc_scope && cbm_msb_add(m, path, source->doc_scope);
    if (source) {
        cbm_free_result(source);
    }
    return ok;
}

static bool dm_msb_same(const cbm_msb_result_t *a, const cbm_msb_result_t *b) {
    if (a->count != b->count || a->open != b->open || a->unevaluable != b->unevaluable ||
        a->outside != b->outside) {
        return false;
    }
    for (int i = 0; i < a->count; i++) {
        if (a->usings[i].kind != b->usings[i].kind ||
            strcmp(a->usings[i].alias, b->usings[i].alias) ||
            strcmp(a->usings[i].target, b->usings[i].target)) {
            return false;
        }
    }
    return true;
}

static bool dm_msb_context_matches(cbm_msb_eval_context_t *context, const cbm_msb_t *m,
                                   const char *project) {
    cbm_msb_result_t actual = {0};
    cbm_msb_result_t reference = {0};
    bool ok = cbm_msb_eval_context_eval(context, project, &actual) &&
              cbm_msb_eval(m, project, &reference) && dm_msb_same(&actual, &reference);
    if (!ok) {
        fprintf(stderr, "MSBuild context differs from uncached evaluation: %s\n", project);
    }
    cbm_msb_result_free(&actual);
    cbm_msb_result_free(&reference);
    return ok;
}

/* Include construction, per-project work and context destruction. A hit must
 * not replay/copy all shared properties behind a constant record counter. */
/* The hit-phase comparison grows shared dependencies while keeping local
 * inputs/output fixed. Scanning the entire cached dependency set must fail. */
/* Dependence on project input is exact, including absence versus unknown.
 * Input owners are gone before the next call; cached effects must own any
 * captured local values and preserve import/poison history. */
TEST(doc_mentions_msbuild_targets_isolation) {
    const dm_project_file_t files[] = {
        {"Directory.Build.props",
         "<Project><PropertyGroup><Input>Prefix</Input><Empty></Empty>"
         "</PropertyGroup><Import Project=\"PrefixOnly.props\"/></Project>"},
        {"PrefixOnly.props", "<Project><PropertyGroup><Stable>PrefixStable</Stable>"
                             "</PropertyGroup></Project>"},
        {"Directory.Build.targets",
         "<Project><Import Project=\"PrefixOnly.props\"/>"
         "<Import Project=\"Seen.targets\"/><Import Project=\"Maybe.targets\" "
         "Condition=\"'$(Unknown)' == 'on'\"/><PropertyGroup>"
         "<From>$(Input)</From><EmptyCopy>$(Empty)Tail</EmptyCopy>"
         "<CustomCopy>$(Custom)</CustomCopy><Own>First</Own><Copy>$(Own)</Copy>"
         "<Own>Final</Own></PropertyGroup><ItemGroup>"
         "<Using Include=\"$(From)\" Alias=\"From\"/>"
         "<Using Include=\"$(EmptyCopy)\" Alias=\"Empty\"/>"
         "<Using Include=\"$(CustomCopy)\" Alias=\"Custom\"/>"
         "<Using Include=\"$(SeenValue)\" Alias=\"Seen\"/>"
         "<Using Include=\"$(Maybe)\" Alias=\"Maybe\"/>"
         "<Using Include=\"$(Own)\" Alias=\"Own\"/>"
         "<Using Include=\"$(Copy)\" Alias=\"Copy\"/>"
         "<Using Include=\"$(Stable)\" Alias=\"Stable\"/>"
         "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Name\"/>"
         "</ItemGroup></Project>"},
        {"Seen.targets", "<Project><PropertyGroup><SeenValue>SharedSeen</SeenValue>"
                         "</PropertyGroup></Project>"},
        {"Maybe.targets", "<Project><PropertyGroup><Maybe>Hidden</Maybe>"
                          "</PropertyGroup></Project>"},
        {"PoisonInput.props",
         "<Project><PropertyGroup><Input>Hidden</Input>"
         "<Custom>Hidden</Custom><Empty>Hidden</Empty></PropertyGroup></Project>"},
        {"Irrelevant.props", "<Project><PropertyGroup><Unused>Anything</Unused>"
                             "</PropertyGroup></Project>"},
        {"A.csproj", "<Project><PropertyGroup><Input>Same</Input><Custom>Common</Custom>"
                     "<Own>Project</Own></PropertyGroup></Project>"},
        {"B.csproj", "<Project><PropertyGroup><Input>Same</Input><Custom>Common</Custom>"
                     "</PropertyGroup><Import Project=\"Irrelevant.props\"/></Project>"},
        {"Changed.csproj",
         "<Project><PropertyGroup><Input>Changed</Input>"
         "<Custom>Different</Custom><Empty>Nonempty</Empty></PropertyGroup></Project>"},
        {"Missing.csproj", "<Project/>"},
        {"Empty.csproj", "<Project><PropertyGroup><Input></Input><Custom></Custom>"
                         "</PropertyGroup></Project>"},
        {"Unknown.csproj", "<Project><Import Project=\"PoisonInput.props\" "
                           "Condition=\"'$(Unknown)' == 'on'\"/></Project>"},
        {"EarlySeen.csproj", "<Project><Import Project=\"Seen.targets\"/><PropertyGroup>"
                             "<SeenValue>LocalSeen</SeenValue></PropertyGroup></Project>"},
        {"EarlyPoison.csproj", "<Project><Import Project=\"Maybe.targets\" "
                               "Condition=\"'$(Unknown)' == 'on'\"/><PropertyGroup>"
                               "<Maybe>Restored</Maybe></PropertyGroup></Project>"},
        {"EarlyRoot.csproj", "<Project><Import Project=\"Directory.Build.targets\"/>"
                             "<PropertyGroup><Input>After</Input><Custom>After</Custom>"
                             "</PropertyGroup></Project>"},
        {"Seed/Directory.Build.targets",
         "<Project><PropertyGroup>"
         "<FromName>$(MSBuildProjectName)</FromName></PropertyGroup><ItemGroup>"
         "<Using Include=\"$(FromName)\"/></ItemGroup></Project>"},
        {"Seed/A.csproj", "<Project/>"},
        {"Seed/B.csproj", "<Project/>"},
        {"Seed/left/A.csproj",
         "<Project><PropertyGroup><MSBuildProjectName>Fixed</MSBuildProjectName>"
         "</PropertyGroup></Project>"},
        {"Seed/right/A.csproj", "<Project/>"},
        {"Seed/Unknown.csproj", "<Project><Import Project=\"SeedPoison.props\" "
                                "Condition=\"'$(Unknown)' == 'on'\"/></Project>"},
        {"Seed/SeedPoison.props",
         "<Project><PropertyGroup><MSBuildProjectName>Hidden</MSBuildProjectName>"
         "</PropertyGroup></Project>"},
        {"Seed/Override.csproj",
         "<Project><PropertyGroup><MSBuildProjectName>Fixed</MSBuildProjectName>"
         "</PropertyGroup></Project>"},
        {"Later/Directory.Build.targets", "<Project><Import Project=\"Later.targets\"/></Project>"},
        {"Later/A.csproj", "<Project/>"},
        {"Later/B.csproj", "<Project/>"},
    };
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(dm_msb_add_xml(m, files[i].rel_path, files[i].xml));
    }
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    const char *projects[] = {"A.csproj",
                              "B.csproj",
                              "A.csproj",
                              "Changed.csproj",
                              "A.csproj",
                              "Missing.csproj",
                              "Empty.csproj",
                              "Unknown.csproj",
                              "Missing.csproj",
                              "A.csproj",
                              "EarlySeen.csproj",
                              "Missing.csproj",
                              "EarlyPoison.csproj",
                              "Missing.csproj",
                              "EarlyRoot.csproj",
                              "A.csproj",
                              "Seed/A.csproj",
                              "Seed/B.csproj",
                              "Seed/Override.csproj",
                              "Seed/A.csproj",
                              "Seed/left/A.csproj",
                              "Seed/right/A.csproj",
                              "Seed/Unknown.csproj",
                              "Seed/A.csproj",
                              "Later/A.csproj",
                              "Later/B.csproj"};
    for (size_t i = 0; i < sizeof(projects) / sizeof(projects[0]); i++) {
        ASSERT_TRUE(dm_msb_context_matches(context, m, projects[i]));
    }
    ASSERT_TRUE(dm_msb_add_xml(m, "Later/Later.targets",
                               "<Project><ItemGroup>"
                               "<Using Include=\"Now.Present\"/></ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "Later/A.csproj"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "Later/B.csproj"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "A.csproj"));
    cbm_msb_eval_context_free(context);
    ASSERT_EQ(cbm_msb_test_value_live_bytes(), 0);
    cbm_msb_free(m);
    PASS();
}

/* A target captures project-owned values before it fails. The same context
 * must be usable again, and later failures must leave retained owners sound. */
TEST(doc_mentions_msbuild_targets_failure) {
    char xml[8192];
    size_t w = (size_t)snprintf(xml, sizeof(xml), "<Project><PropertyGroup>");
    for (int i = 0; i < 64; i++) {
        w += (size_t)snprintf(xml + w, sizeof(xml) - w, "<K%02d>$(Input)%02d</K%02d>", i, i, i);
    }
    w += (size_t)snprintf(xml + w, sizeof(xml) - w,
                          "</PropertyGroup><ItemGroup><Using Include=\"$(K00)\"/>"
                          "<Using Include=\"$(K63)\"/></ItemGroup></Project>");
    ASSERT_TRUE(w < sizeof(xml));
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(
        dm_msb_add_xml(m, "Directory.Build.props",
                       "<Project><PropertyGroup><Base>Prefix</Base></PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets", xml));
    ASSERT_TRUE(dm_msb_add_xml(m, "A.csproj",
                               "<Project><PropertyGroup><Input>Shared</Input>"
                               "</PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "B.csproj",
                               "<Project><PropertyGroup><Input>Changed</Input>"
                               "</PropertyGroup></Project>"));
    int failures = 0;
    for (int mode = 0; mode < 2; mode++) {
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        cbm_msb_test_fail_value_alloc_after(mode == 0 ? 16 : 0);
        cbm_msb_test_fail_prop_insert_after(mode == 1 ? 16 : 0);
        cbm_msb_result_t r = {0};
        bool ok = cbm_msb_eval_context_eval(context, "A.csproj", &r);
        bool consumed =
            mode == 0 ? cbm_msb_test_value_alloc_failed() : cbm_msb_test_prop_insert_failed();
        failures += ok || !consumed || r.mem != NULL || r.usings != NULL || r.count != 0;
        cbm_msb_result_free(&r);
        cbm_msb_test_fail_value_alloc_after(0);
        cbm_msb_test_fail_prop_insert_after(0);
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        uint64_t retained = cbm_msb_test_value_live_bytes();
        cbm_msb_test_fail_value_alloc_after(mode == 0 ? 1 : 0);
        cbm_msb_test_fail_prop_insert_after(mode == 1 ? 1 : 0);
        ok = cbm_msb_eval_context_eval(context, "B.csproj", &r);
        consumed =
            mode == 0 ? cbm_msb_test_value_alloc_failed() : cbm_msb_test_prop_insert_failed();
        failures += ok || !consumed || r.mem != NULL || r.usings != NULL || r.count != 0 ||
                    cbm_msb_test_value_live_bytes() > retained;
        cbm_msb_result_free(&r);
        cbm_msb_test_fail_value_alloc_after(0);
        cbm_msb_test_fail_prop_insert_after(0);
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        failures += !dm_msb_context_matches(context, m, "B.csproj");
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        cbm_msb_eval_context_free(context);
        failures += cbm_msb_test_value_live_bytes() != 0;
        fprintf(stderr, "msbuild targets fault mode=%d retained=%llu failures=%d\n", mode,
                (unsigned long long)retained, failures);
    }
    cbm_msb_free(m);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Final shared items read project inputs after target writes. Cached includes
 * and removals must compose with local items and retain diagnostic counts. */
TEST(doc_mentions_msbuild_items_isolation) {
    const dm_project_file_t files[] = {
        {"Directory.Build.props",
         "<Project><Import Project=\"Parts/One.props\"/><ItemGroup>"
         "<Using Include=\"$(Input);Shared.Keep;Shared.Drop\" Alias=\"$(Alias)\" "
         "Static=\"$(Static)\"/><Using Include=\"$(Forced)\" Alias=\"Forced\"/>"
         "<Using Include=\"Shared.Duplicate\"/><Using Include=\"Shared.Duplicate\"/>"
         "<Using Remove=\"$(Remove)\"/><Using Include=\"Unknown.Duplicate\" "
         "Condition=\"'$(Maybe)' == 'on'\"/><Using Include=\"Unknown.Duplicate\" "
         "Condition=\"'$(Maybe)' == 'on'\"/></ItemGroup></Project>"},
        {"Parts/One.props", "<Project><ItemGroup Condition=\"'$(Enabled)' == 'on'\">"
                            "<Using Include=\"File.$(MSBuildThisFileName)\"/>"
                            "<Using Include=\"$(Input)\" Alias=\"Group\"/></ItemGroup></Project>"},
        {"Directory.Build.targets",
         "<Project><PropertyGroup><Forced>Targets.Fixed</Forced></PropertyGroup>"
         "<Import Project=\"Parts/Two.targets\"/><ItemGroup>"
         "<Using Include=\"$(Input)\" Static=\"true\"/>"
         "<Using Include=\"Shared.Drop\"/><Using Remove=\"Project.Drop\"/>"
         "</ItemGroup></Project>"},
        {"Parts/Two.targets", "<Project><ItemGroup>"
                              "<Using Include=\"File.$(MSBuildThisFileName)\"/>"
                              "<Using Include=\"$(Forced)\" Alias=\"Target\"/>"
                              "</ItemGroup></Project>"},
        {"Poison.props", "<Project><PropertyGroup><Input>Hidden</Input><Alias>Hidden</Alias>"
                         "<Static>Hidden</Static><Remove>Hidden</Remove><Enabled>Hidden</Enabled>"
                         "<Forced>Hidden</Forced></PropertyGroup></Project>"},
        {"A.csproj", "<Project><PropertyGroup><Input>Same.Input</Input><Alias>SameAlias</Alias>"
                     "<Static>false</Static><Remove>Shared.Drop</Remove><Enabled>on</Enabled>"
                     "<Forced>Project.A</Forced></PropertyGroup><ItemGroup>"
                     "<Using Include=\"Project.Drop;Project.Keep\"/><Using Remove=\"Shared.Keep\"/>"
                     "</ItemGroup></Project>"},
        {"B.csproj",
         "<Project><PropertyGroup><Input>Same.Input</Input><Alias>SameAlias</Alias>"
         "<Static>false</Static><Remove>Shared.Drop</Remove><Enabled>on</Enabled>"
         "<Forced>Project.B</Forced><Unused>Different</Unused></PropertyGroup><ItemGroup>"
         "<Using Include=\"Shared.Drop;Shared.Keep\"/><Using Remove=\"Shared.Duplicate\"/>"
         "</ItemGroup></Project>"},
        {"Changed.csproj",
         "<Project><PropertyGroup><Input>Changed.Input</Input><Alias>ChangedAlias</Alias>"
         "<Static>true</Static><Remove>Targets.Fixed</Remove><Enabled>off</Enabled>"
         "<Maybe>on</Maybe></PropertyGroup></Project>"},
        {"Empty.csproj", "<Project><PropertyGroup><Input></Input><Alias></Alias>"
                         "<Static></Static><Remove></Remove><Enabled></Enabled>"
                         "</PropertyGroup></Project>"},
        {"Missing.csproj", "<Project/>"},
        {"Unknown.csproj", "<Project><Import Project=\"Poison.props\" "
                           "Condition=\"'$(Unknown)' == 'on'\"/></Project>"},
        {"Other/Directory.Build.props", "<Project><ItemGroup>"
                                        "<Using Include=\"Other.Prefix\"/>"
                                        "</ItemGroup></Project>"},
        {"Other/A.csproj", "<Project/>"},
        {"Target/Directory.Build.targets", "<Project><ItemGroup>"
                                           "<Using Include=\"Other.Target\"/>"
                                           "<Using Remove=\"Shared.Duplicate\"/>"
                                           "</ItemGroup></Project>"},
        {"Target/A.csproj", "<Project/>"},
        {"Later/A.csproj", "<Project/>"},
        {"Later/B.csproj", "<Project/>"},
    };
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(dm_msb_add_xml(m, files[i].rel_path, files[i].xml));
    }
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    const char *projects[] = {
        "A.csproj",       "B.csproj",       "A.csproj",       "Changed.csproj",  "A.csproj",
        "Empty.csproj",   "Missing.csproj", "Unknown.csproj", "Missing.csproj",  "B.csproj",
        "Other/A.csproj", "Other/A.csproj", "A.csproj",       "Target/A.csproj", "Target/A.csproj",
        "A.csproj",       "Later/A.csproj", "Later/B.csproj"};
    for (size_t i = 0; i < sizeof(projects) / sizeof(projects[0]); i++) {
        ASSERT_TRUE(dm_msb_context_matches(context, m, projects[i]));
    }
    ASSERT_TRUE(dm_msb_add_xml(m, "Later/Directory.Build.props",
                               "<Project><ItemGroup><Using Include=\"Now.Prefix\"/>"
                               "</ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "Later/A.csproj"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "Later/B.csproj"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "A.csproj"));
    cbm_msb_eval_context_free(context);
    ASSERT_EQ(cbm_msb_test_value_live_bytes(), 0);
    cbm_msb_free(m);
    PASS();
}

/* The seams fail real allocation/insertion operations. Context-owned state
 * must remain usable, and all allocator-tracked storage must be released. */
TEST(doc_mentions_msbuild_items_failure) {
    char xml[16384];
    size_t w = (size_t)snprintf(xml, sizeof(xml), "<Project><ItemGroup>");
    for (int i = 0; i < 64; i++) {
        w += (size_t)snprintf(xml + w, sizeof(xml) - w,
                              "<Using Include=\"$(Input).N%03d\" Alias=\"SharedAlias\"/>", i);
    }
    w += (size_t)snprintf(xml + w, sizeof(xml) - w, "</ItemGroup></Project>");
    ASSERT_TRUE(w < sizeof(xml));
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props", xml));
    ASSERT_TRUE(dm_msb_add_xml(m, "A.csproj",
                               "<Project><PropertyGroup><Input>Shared</Input>"
                               "</PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "B.csproj",
                               "<Project><PropertyGroup><Input>Shared</Input>"
                               "<Unused>Different</Unused></PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "C.csproj",
                               "<Project><PropertyGroup><Input>Changed</Input>"
                               "</PropertyGroup></Project>"));
    const struct {
        cbm_msb_item_fail_operation_t operation;
        int nth;
        bool warm;
    } cases[] = {
        {CBM_MSB_ITEM_FAIL_CAPTURE_ALLOC, 1, false},  {CBM_MSB_ITEM_FAIL_CAPTURE_ALLOC, 9, false},
        {CBM_MSB_ITEM_FAIL_CAPTURE_ALLOC, 33, false}, {CBM_MSB_ITEM_FAIL_UNIQUE_INSERT, 1, false},
        {CBM_MSB_ITEM_FAIL_UNIQUE_INSERT, 9, false},  {CBM_MSB_ITEM_FAIL_UNIQUE_INSERT, 33, false},
        {CBM_MSB_ITEM_FAIL_APPLY_ALLOC, 1, true},     {CBM_MSB_ITEM_FAIL_PUBLISH_ALLOC, 1, true}};
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t before = cbm_mem_tracked_live_bytes();
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        if (cases[i].warm) {
            failures += !dm_msb_context_matches(context, m, "A.csproj");
        }
        size_t retained = cbm_mem_tracked_live_bytes();
        cbm_msb_test_fail_item_operation(cases[i].operation, cases[i].nth);
        cbm_msb_result_t r = {0};
        bool ok = cbm_msb_eval_context_eval(context, cases[i].warm ? "B.csproj" : "A.csproj", &r);
        bool consumed = cbm_msb_test_item_operation_failed();
        failures += ok || !consumed || r.mem != NULL || r.usings != NULL || r.count != 0;
        cbm_msb_result_free(&r);
        cbm_msb_test_fail_item_operation(CBM_MSB_ITEM_FAIL_NONE, 0);
        if (cases[i].warm) {
            failures += cbm_mem_tracked_live_bytes() != retained;
        }
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        failures += !dm_msb_context_matches(context, m, "B.csproj");
        failures += !dm_msb_context_matches(context, m, "C.csproj");
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        cbm_msb_eval_context_free(context);
        size_t after = cbm_mem_tracked_live_bytes();
        failures += after != before || cbm_msb_test_value_live_bytes() != 0;
        fprintf(stderr,
                "msbuild items fault operation=%d nth=%d consumed=%d "
                "before=%llu after=%llu failures=%d\n",
                (int)cases[i].operation, cases[i].nth, consumed, (unsigned long long)before,
                (unsigned long long)after, failures);
    }
    cbm_msb_free(m);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* A published result owns its strings independently of the retained cache,
 * its replacement and the model. A mismatch must keep the first variant. */
TEST(doc_mentions_msbuild_items_lifetime) {
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props",
                               "<Project><ItemGroup><Using Include=\"$(Input)\" Alias=\"One\"/>"
                               "<Using Include=\"$(Input)\" Alias=\"Two\"/>"
                               "<Using Include=\"$(Input)\" Static=\"true\"/>"
                               "</ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "A.csproj",
                               "<Project><PropertyGroup><Input>First.Value</Input>"
                               "</PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "B.csproj",
                               "<Project><PropertyGroup><Input>Other.Value</Input>"
                               "</PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Other/Directory.Build.props",
                               "<Project><ItemGroup><Using Include=\"Replacement.Value\"/>"
                               "</ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Other/A.csproj", "<Project/>"));
    cbm_msb_result_t expected = {0};
    ASSERT_TRUE(cbm_msb_eval(m, "A.csproj", &expected));
    ASSERT_EQ(expected.count, 3);
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    cbm_msb_result_t held = {0};
    ASSERT_TRUE(cbm_msb_eval_context_eval(context, "A.csproj", &held));
    ASSERT_TRUE(dm_msb_same(&held, &expected));
    cbm_msb_result_t hit = {0};
    cbm_msb_test_cost_reset();
    ASSERT_TRUE(cbm_msb_eval_context_eval(context, "A.csproj", &hit));
    uint64_t before = 0;
    uint64_t peak = 0;
    cbm_msb_test_cost(&before, &peak);
    ASSERT_TRUE(dm_msb_same(&hit, &expected));
    cbm_msb_result_free(&hit);
    ASSERT_TRUE(dm_msb_context_matches(context, m, "B.csproj"));
    cbm_msb_test_cost_reset();
    ASSERT_TRUE(cbm_msb_eval_context_eval(context, "A.csproj", &hit));
    uint64_t after = 0;
    cbm_msb_test_cost(&after, &peak);
    bool same_hit_work = before == after;
    ASSERT_TRUE(dm_msb_same(&hit, &expected));
    cbm_msb_result_free(&hit);
    ASSERT_TRUE(dm_msb_context_matches(context, m, "Other/A.csproj"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets",
                               "<Project><ItemGroup><Using Remove=\"First.Value\"/>"
                               "</ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "A.csproj"));
    cbm_msb_eval_context_free(context);
    cbm_msb_free(m);
    bool independent = dm_msb_same(&held, &expected);
    cbm_msb_result_free(&held);
    cbm_msb_result_free(&expected);
    fprintf(stderr, "msbuild items retained variant before=%llu after=%llu independent=%d\n",
            (unsigned long long)before, (unsigned long long)after, independent);
    ASSERT_TRUE(same_hit_work && independent);
    ASSERT_EQ(cbm_msb_test_value_live_bytes(), 0);
    PASS();
}

/* Shared final-property items can produce no output, or the same unique
 * output, despite a large input span. Warm work must not scale with that
 * span after an exact reusable result is available. */
TEST(doc_mentions_msbuild_items_work) {
    enum { CAP = 131072, PROJECTS = 24 };
    char *xml = malloc(CAP);
    ASSERT_NOT_NULL(xml);
    uint64_t hits[9][2] = {{0}};
    bool correct = true;
    bool storage_bounded = true;
    char long_alias[4096];
    memset(long_alias, 'A', sizeof(long_alias) - 1);
    long_alias[sizeof(long_alias) - 1] = '\0';
    for (int mode = 0; mode < 9; mode++) {
        for (int size = 0; size < 2; size++) {
            int items = 512 << size;
            size_t w = (size_t)snprintf(xml, CAP, "<Project><PropertyGroup>");
            if (mode == 6 || mode == 7) {
                for (int i = 0; i < items; i++) {
                    w += (size_t)snprintf(xml + w, CAP - w, "<K%04d>off</K%04d>", i, i);
                }
            } else if (mode == 8) {
                w += (size_t)snprintf(xml + w, CAP - w, "<LongAlias>%s</LongAlias>", long_alias);
            }
            w += (size_t)snprintf(xml + w, CAP - w, "</PropertyGroup><ItemGroup>");
            for (int i = 0; i < items; i++) {
                if (mode == 0) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Unused%04d\" Condition=\"false\"/>", i);
                } else if (mode == 1) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Same.Target\" Alias=\"SameAlias\"/>");
                } else if (mode == 2) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Unknown%04d\" "
                                          "Condition=\"'$(Missing)' == 'on'\"/>",
                                          i);
                } else if (mode == 3) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Unused%04d\" "
                                          "Condition=\"'$(Enabled)' == 'on'\"/>",
                                          i);
                } else if (mode == 4) {
                    w += (size_t)snprintf(xml + w, CAP - w, "<Using Remove=\"Unused%04d\"/>", i);
                } else if (mode == 5) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Cancelled%04d\"/>"
                                          "<Using Remove=\"Cancelled%04d\"/>",
                                          i, i);
                } else if (mode == 6 || mode == 7) {
                    w += (size_t)snprintf(xml + w, CAP - w,
                                          "<Using Include=\"Unused%04d\" "
                                          "Condition=\"'$(K%04d)' == 'on'\"/>",
                                          i, i);
                } else {
                    w +=
                        (size_t)snprintf(xml + w, CAP - w,
                                         "<Using Include=\"Same.Target\" Alias=\"$(LongAlias)\"/>");
                }
            }
            w += (size_t)snprintf(xml + w, CAP - w, "</ItemGroup></Project>");
            ASSERT_TRUE(w < CAP);
            cbm_msb_t *m = cbm_msb_new();
            ASSERT_NOT_NULL(m);
            ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props", xml));
            if (mode == 7) {
                /* All final K values come from targets; the changing local
                 * K0000 value must not invalidate their item dependencies. */
                ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets", xml));
            }
            ASSERT_TRUE(dm_msb_add_xml(
                m, "src/Warm.csproj",
                mode == 4 ? "<Project><PropertyGroup><Enabled>off</Enabled></PropertyGroup>"
                            "<ItemGroup><Using Include=\"Project.Keep\"/></ItemGroup></Project>"
                          : "<Project><PropertyGroup><Enabled>off</Enabled>"
                            "</PropertyGroup></Project>"));
            for (int i = 0; i < PROJECTS; i++) {
                char path[64];
                char project[256];
                snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                snprintf(project, sizeof(project),
                         "<Project><PropertyGroup><Enabled>off</Enabled><Unused>P%02d</Unused>%s"
                         "</PropertyGroup>%s</Project>",
                         i, mode == 7 ? "<K0000>on</K0000>" : "",
                         mode == 4 ? "<ItemGroup><Using Include=\"Project.Keep\"/></ItemGroup>"
                                   : "");
                ASSERT_TRUE(dm_msb_add_xml(m, path, project));
            }
            cbm_msb_test_cost_reset();
            cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
            ASSERT_NOT_NULL(context);
            cbm_msb_result_t warm = {0};
            ASSERT_TRUE(cbm_msb_eval_context_eval(context, "src/Warm.csproj", &warm));
            correct = correct && warm.count == (mode == 1 || mode == 4 || mode == 8 ? 1 : 0) &&
                      warm.open == (mode == 2) && warm.unevaluable == (mode == 2 ? items : 0) &&
                      !warm.outside;
            cbm_msb_result_free(&warm);
            uint64_t captured = cbm_msb_test_work();
            for (int i = 0; i < PROJECTS; i++) {
                char path[64];
                snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                cbm_msb_result_t r = {0};
                ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &r));
                correct = correct && r.count == (mode == 1 || mode == 4 || mode == 8 ? 1 : 0) &&
                          r.open == (mode == 2) && r.unevaluable == (mode == 2 ? items : 0) &&
                          !r.outside;
                if (mode == 1 || mode == 8) {
                    correct =
                        correct && r.count == 1 && r.usings[0].kind == 'a' &&
                        strcmp(r.usings[0].alias, mode == 8 ? long_alias : "SameAlias") == 0 &&
                        strcmp(r.usings[0].target, "Same.Target") == 0;
                } else if (mode == 4) {
                    correct = correct && r.count == 1 && r.usings[0].kind == 'n' &&
                              strcmp(r.usings[0].alias, "") == 0 &&
                              strcmp(r.usings[0].target, "Project.Keep") == 0;
                }
                cbm_msb_result_free(&r);
            }
            hits[mode][size] = cbm_msb_test_work() - captured;
            cbm_msb_eval_context_free(context);
            uint64_t records = 0;
            uint64_t peak = 0;
            cbm_msb_test_cost(&records, &peak);
            if (mode == 8) {
                /* One long value and one unique output must not leave a
                 * persistent alias copy per duplicate item during capture. */
                uint64_t bound = 16 * (uint64_t)w + 1024 * 1024;
                storage_bounded = storage_bounded && peak <= bound;
                fprintf(stderr, "msbuild items duplicate storage items=%d peak=%llu bound=%llu\n",
                        items, (unsigned long long)peak, (unsigned long long)bound);
            }
            fprintf(stderr,
                    "msbuild items mode=%d items=%d projects=%d interpreted=%llu "
                    "work=%llu hit_work=%llu peak=%llu live=%llu semantics=%d\n",
                    mode, items, PROJECTS, (unsigned long long)records,
                    (unsigned long long)cbm_msb_test_work(), (unsigned long long)hits[mode][size],
                    (unsigned long long)peak, (unsigned long long)cbm_msb_test_value_live_bytes(),
                    correct);
            correct = correct && cbm_msb_test_value_live_bytes() == 0;
            cbm_msb_free(m);
        }
    }
    free(xml);
    ASSERT_TRUE(correct);
    bool bounded = true;
    for (int mode = 0; mode < 9; mode++) {
        ASSERT_TRUE(hits[mode][0] > 0);
        double ratio = (double)hits[mode][1] / (double)hits[mode][0];
        fprintf(stderr, "msbuild items shared-size hit ratio mode=%d ratio=%.3f\n", mode, ratio);
        bounded = bounded && ratio >= 0.9 && ratio <= 1.1;
    }
    ASSERT_TRUE(bounded && storage_bounded);
    PASS();
}

TEST(doc_mentions_msbuild_targets_work) {
    enum { CAP = 131072 };
    char *props = malloc(CAP);
    char *targets = malloc(CAP);
    ASSERT_NOT_NULL(props);
    ASSERT_NOT_NULL(targets);
    uint64_t work[4][2][2] = {{{0}}};
    uint64_t hits[4][2][2] = {{{0}}};
    bool correct = true;
    for (int mode = 0; mode < 4; mode++) {
        for (int size = 0; size < 2; size++) {
            int records = 512 << size;
            size_t p = (size_t)snprintf(props, CAP, "<Project><PropertyGroup>");
            size_t t = (size_t)snprintf(targets, CAP, "<Project><PropertyGroup>");
            for (int i = 0; i < records; i++) {
                if (mode == 1) {
                    p += (size_t)snprintf(props + p, CAP - p, "<K%04d>Shared%04d</K%04d>", i, i, i);
                    t += (size_t)snprintf(targets + t, CAP - t, "<T%04d>$(K%04d)</T%04d>", i, i, i);
                } else if (mode == 2) {
                    t += (size_t)snprintf(targets + t, CAP - t, "<T%04d>$(Custom)%04d</T%04d>", i,
                                          i, i);
                } else {
                    t += (size_t)snprintf(targets + t, CAP - t, "<T%04d>Shared%04d</T%04d>", i, i,
                                          i);
                }
            }
            p += (size_t)snprintf(props + p, CAP - p,
                                  "<Base>Prefix</Base></PropertyGroup></Project>");
            t += (size_t)snprintf(
                targets + t, CAP - t,
                "</PropertyGroup><ItemGroup><Using Include=\"$(T0000)\" Alias=\"First\"/>"
                "<Using Include=\"$(T%04d)\" Alias=\"Last\"/>"
                "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Project\"/>"
                "</ItemGroup></Project>",
                records - 1);
            ASSERT_TRUE(p < CAP && t < CAP);
            for (int count = 0; count < 2; count++) {
                int projects = 16 << count;
                cbm_msb_t *m = cbm_msb_new();
                ASSERT_NOT_NULL(m);
                /* No prefix file in mode 3: absence is a stable baseline,
                 * not a reason to discard a reusable target on every call. */
                if (mode != 3) {
                    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props", props));
                }
                ASSERT_TRUE(dm_msb_add_xml(m, "Shared.targets", targets));
                ASSERT_TRUE(
                    dm_msb_add_xml(m, "Directory.Build.targets",
                                   "<Project><Import Project=\"Shared.targets\"/></Project>"));
                for (int i = 0; i < projects; i++) {
                    char path[64];
                    char xml[512];
                    snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                    snprintf(
                        xml, sizeof(xml),
                        "<Project><PropertyGroup>"
                        "<K0000>Shared0000</K0000><Custom>Shared</Custom><Unused>P%02d</Unused>"
                        "</PropertyGroup></Project>",
                        i);
                    ASSERT_TRUE(dm_msb_add_xml(m, path, xml));
                }
                /* Capture many redundant overrides in mode 1; later projects
                 * omit them. They must not create required local exceptions. */
                ASSERT_TRUE(dm_msb_add_xml(m, "src/Warm.csproj",
                                           mode == 2 ? "<Project><PropertyGroup><Custom>Shared</"
                                                       "Custom></PropertyGroup></Project>"
                                                     : props));
                cbm_msb_test_cost_reset();
                cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
                ASSERT_NOT_NULL(context);
                cbm_msb_result_t warm = {0};
                ASSERT_TRUE(cbm_msb_eval_context_eval(context, "src/Warm.csproj", &warm));
                correct = correct && warm.count == 3 && !warm.open && !warm.unevaluable &&
                          !warm.outside && dm_has_using(&warm, 'a', "Shared0000");
                cbm_msb_result_free(&warm);
                uint64_t captured = cbm_msb_test_work();
                for (int i = 0; i < projects; i++) {
                    char path[64];
                    char name[32];
                    char last[32];
                    snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                    snprintf(name, sizeof(name), "P%02d", i);
                    snprintf(last, sizeof(last), "Shared%04d", records - 1);
                    cbm_msb_result_t r = {0};
                    ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &r));
                    correct = correct && r.count == 3 && !r.open && !r.unevaluable && !r.outside &&
                              dm_has_using(&r, 'a', "Shared0000") && dm_has_using(&r, 'a', last) &&
                              dm_has_using(&r, 'a', name);
                    cbm_msb_result_free(&r);
                }
                hits[mode][size][count] = cbm_msb_test_work() - captured;
                cbm_msb_eval_context_free(context);
                work[mode][size][count] = cbm_msb_test_work();
                uint64_t consumed = 0;
                uint64_t peak = 0;
                cbm_msb_test_cost(&consumed, &peak);
                fprintf(
                    stderr,
                    "msbuild targets mode=%d records=%d projects=%d "
                    "interpreted=%llu work=%llu hit_work=%llu peak=%llu live=%llu semantics=%d\n",
                    mode, records, projects, (unsigned long long)consumed,
                    (unsigned long long)work[mode][size][count],
                    (unsigned long long)hits[mode][size][count], (unsigned long long)peak,
                    (unsigned long long)cbm_msb_test_value_live_bytes(), correct);
                correct = correct && cbm_msb_test_value_live_bytes() == 0;
                cbm_msb_free(m);
            }
        }
    }
    free(props);
    free(targets);
    ASSERT_TRUE(correct);
    bool bounded = true;
    for (int mode = 0; mode < 4; mode++) {
        for (int size = 0; size < 2; size++) {
            ASSERT_TRUE(work[mode][size][0] > 0);
            double ratio = (double)work[mode][size][1] / (double)work[mode][size][0];
            fprintf(stderr, "msbuild targets project ratio mode=%d records=%d ratio=%.3f\n", mode,
                    512 << size, ratio);
            bounded = bounded && ratio >= 0.9 && ratio <= 1.4;
        }
        for (int count = 0; count < 2; count++) {
            ASSERT_TRUE(hits[mode][0][count] > 0);
            double ratio = (double)hits[mode][1][count] / (double)hits[mode][0][count];
            fprintf(stderr,
                    "msbuild targets shared-size hit ratio mode=%d projects=%d ratio=%.3f\n", mode,
                    16 << count, ratio);
            bounded = bounded && ratio >= 0.9 && ratio <= 1.1;
        }
    }
    ASSERT_TRUE(bounded);
    PASS();
}

TEST(doc_mentions_msbuild_nearest_work) {
    enum { DEPTH = 48, PROJECTS = 24 };
    char dir[512] = "";
    for (int i = 0; i < DEPTH; i++) {
        strcat(dir, "deep/");
    }
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props", "<Project/>"));
    for (int i = 0; i < PROJECTS; i++) {
        char path[600];
        snprintf(path, sizeof(path), "%sP%02d.csproj", dir, i);
        ASSERT_TRUE(dm_msb_add_xml(m, path, "<Project/>"));
    }
    cbm_msb_test_cost_reset();
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    bool correct = true;
    for (int i = 0; i < PROJECTS; i++) {
        char path[600];
        snprintf(path, sizeof(path), "%sP%02d.csproj", dir, i);
        cbm_msb_result_t r = {0};
        ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &r));
        correct = correct && r.count == 0 && !r.open && !r.unevaluable && !r.outside;
        cbm_msb_result_free(&r);
    }
    uint64_t probes = cbm_msb_test_nearest_steps();
    char target[600];
    snprintf(target, sizeof(target), "%sDirectory.Build.targets", dir);
    ASSERT_TRUE(dm_msb_add_xml(m, target,
                               "<Project><ItemGroup>"
                               "<Using Include=\"New.Target\"/></ItemGroup></Project>"));
    char project[600];
    snprintf(project, sizeof(project), "%sP00.csproj", dir);
    cbm_msb_result_t r = {0};
    ASSERT_TRUE(cbm_msb_eval_context_eval(context, project, &r));
    correct = correct && r.count == 1 && !r.open && !r.unevaluable && !r.outside &&
              dm_has_using(&r, 'n', "New.Target");
    cbm_msb_result_free(&r);
    uint64_t fresh = cbm_msb_test_nearest_steps() - probes;
    cbm_msb_eval_context_free(context);
    correct = correct && cbm_msb_test_value_live_bytes() == 0;
    cbm_msb_free(m);
    fprintf(stderr,
            "msbuild nearest depth=%d projects=%d probes=%llu after_add=%llu semantics=%d\n", DEPTH,
            PROJECTS, (unsigned long long)probes, (unsigned long long)fresh, correct);
    ASSERT_TRUE(correct);
    ASSERT_TRUE(probes <= 2 * (DEPTH + 1));
    ASSERT_TRUE(fresh > 0 && fresh <= 2 * (DEPTH + 1));
    PASS();
}

/* Distinct nearest-file roots must not replay the common imported state.
 * All models are built before measurement. The warm root and measured roots
 * differ; doubling shared size must not change subsequent per-root work. */
TEST(doc_mentions_msbuild_shared_closure_work) {
    enum { CAP = 131072 };
    char *xml = malloc(CAP);
    ASSERT_NOT_NULL(xml);
    char *definitions = malloc(CAP);
    ASSERT_NOT_NULL(definitions);
    uint64_t work[4][2][2] = {{{0}}};
    uint64_t hits[4][2][2] = {{{0}}};
    bool correct = true;
    for (int mode = 0; mode < 4; mode++) {
        const char *suffix = mode % 2 == 0 ? "props" : "targets";
        for (int size = 0; size < 2; size++) {
            int records = 512 << size;
            size_t w = (size_t)snprintf(xml, CAP, "<Project><PropertyGroup>");
            size_t d = (size_t)snprintf(definitions, CAP, "<Project><PropertyGroup>");
            for (int i = 0; i < records; i++) {
                if (mode >= 2) {
                    d += (size_t)snprintf(definitions + d, CAP - d, "<K%04d>Shared%04d</K%04d>", i,
                                          i, i);
                    w += (size_t)snprintf(xml + w, CAP - w, "<T%04d>$(K%04d)</T%04d>", i, i, i);
                } else {
                    w += (size_t)snprintf(xml + w, CAP - w, "<K%04d>Shared%04d</K%04d>", i, i, i);
                }
            }
            d += (size_t)snprintf(definitions + d, CAP - d, "</PropertyGroup></Project>");
            w += (size_t)snprintf(
                xml + w, CAP - w,
                "</PropertyGroup><ItemGroup><Using Include=\"$(%c0000)\" Alias=\"First\"/>"
                "<Using Include=\"$(%c%04d)\" Alias=\"Last\"/>"
                "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Project\"/>"
                "</ItemGroup></Project>",
                mode >= 2 ? 'T' : 'K', mode >= 2 ? 'T' : 'K', records - 1);
            ASSERT_TRUE(w < CAP && d < CAP);
            for (int count = 0; count < 2; count++) {
                int projects = 16 << count;
                cbm_msb_t *m = cbm_msb_new();
                ASSERT_NOT_NULL(m);
                char shared[64];
                char wrapper[256];
                snprintf(shared, sizeof(shared), "Shared.%s", suffix);
                if (mode >= 2) {
                    char define_path[64];
                    snprintf(define_path, sizeof(define_path), "Definitions.%s", suffix);
                    ASSERT_TRUE(dm_msb_add_xml(m, define_path, definitions));
                    snprintf(wrapper, sizeof(wrapper),
                             "<Project><Import Project=\"../../Definitions.%s\"/>"
                             "<Import Project=\"../../Shared.%s\"/></Project>",
                             suffix, suffix);
                } else {
                    snprintf(wrapper, sizeof(wrapper),
                             "<Project><Import Project=\"../../Shared.%s\"/></Project>", suffix);
                }
                ASSERT_TRUE(dm_msb_add_xml(m, shared, xml));
                for (int i = 0; i <= projects; i++) {
                    char root[96];
                    char project[96];
                    snprintf(root, sizeof(root), "src/R%02d/Directory.Build.%s", i, suffix);
                    snprintf(project, sizeof(project), "src/R%02d/P%02d.csproj", i, i);
                    ASSERT_TRUE(dm_msb_add_xml(m, root, wrapper));
                    ASSERT_TRUE(dm_msb_add_xml(m, project, "<Project/>"));
                }
                cbm_msb_test_cost_reset();
                cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
                ASSERT_NOT_NULL(context);
                uint64_t captured = 0;
                for (int i = 0; i <= projects; i++) {
                    char path[96];
                    char name[32];
                    char last[32];
                    snprintf(path, sizeof(path), "src/R%02d/P%02d.csproj", i, i);
                    snprintf(name, sizeof(name), "P%02d", i);
                    snprintf(last, sizeof(last), "Shared%04d", records - 1);
                    cbm_msb_result_t r = {0};
                    ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &r));
                    cbm_msb_using_t expected_usings[] = {
                        {.kind = 'a', .alias = "Project", .target = name},
                        {.kind = 'a', .alias = "First", .target = "Shared0000"},
                        {.kind = 'a', .alias = "Last", .target = last},
                    };
                    cbm_msb_result_t expected = {.usings = expected_usings, .count = 3};
                    correct = correct && dm_msb_same(&r, &expected);
                    cbm_msb_result_free(&r);
                    if (i == 0) {
                        captured = cbm_msb_test_work();
                    }
                }
                hits[mode][size][count] = cbm_msb_test_work() - captured;
                cbm_msb_eval_context_free(context);
                work[mode][size][count] = cbm_msb_test_work();
                uint64_t interpreted = 0;
                uint64_t peak = 0;
                cbm_msb_test_cost(&interpreted, &peak);
                correct = correct && cbm_msb_test_value_live_bytes() == 0;
                fprintf(stderr,
                        "msbuild shared closure mode=%d records=%d projects=%d interpreted=%llu "
                        "work=%llu hit_work=%llu peak=%llu live=%llu semantics=%d\n",
                        mode, records, projects, (unsigned long long)interpreted,
                        (unsigned long long)work[mode][size][count],
                        (unsigned long long)hits[mode][size][count], (unsigned long long)peak,
                        (unsigned long long)cbm_msb_test_value_live_bytes(), correct);
                cbm_msb_free(m);
            }
        }
    }
    free(xml);
    free(definitions);
    ASSERT_TRUE(correct);
    bool bounded = true;
    for (int mode = 0; mode < 4; mode++) {
        for (int count = 0; count < 2; count++) {
            ASSERT_TRUE(hits[mode][0][count] > 0);
            double ratio = (double)hits[mode][1][count] / (double)hits[mode][0][count];
            fprintf(stderr,
                    "msbuild shared closure shared-size hit ratio mode=%d projects=%d ratio=%.3f\n",
                    mode, 16 << count, ratio);
            bounded = bounded && ratio >= 0.9 && ratio <= 1.1;
        }
        /* Include construction and teardown: sharing must not shift the same
         * product cost into retained-state copying or cleanup. */
        ASSERT_TRUE(work[mode][1][0] > work[mode][0][0]);
        ASSERT_TRUE(work[mode][1][1] > work[mode][0][1]);
        uint64_t small = work[mode][1][0] - work[mode][0][0];
        uint64_t large = work[mode][1][1] - work[mode][0][1];
        double ratio = (double)large / (double)small;
        fprintf(stderr, "msbuild shared closure extra-size ratio mode=%d ratio=%.3f\n", mode,
                ratio);
        bounded = bounded && ratio >= 0.9 && ratio <= 1.2;
    }
    ASSERT_TRUE(bounded);
    PASS();
}

/* Grow the number of shared files, with fixed tiny outputs. Component lookup,
 * item traversal, dependency composition and cleanup must not replay the chain. */
TEST(doc_mentions_msbuild_shared_file_work) {
    uint64_t work[2][2][2] = {{{0}}};
    uint64_t hits[2][2][2] = {{{0}}};
    bool correct = true;
    bool storage_bounded = true;
    for (int mode = 0; mode < 2; mode++) {
        const char *suffix = mode == 0 ? "props" : "targets";
        for (int size = 0; size < 2; size++) {
            int files = 128 << size;
            for (int count = 0; count < 2; count++) {
                int projects = 16 << count;
                cbm_msb_t *m = cbm_msb_new();
                ASSERT_NOT_NULL(m);
                size_t source_bytes = 0;
                for (int i = 0; i < files; i++) {
                    char path[96];
                    char next[96] = "";
                    char xml[512];
                    snprintf(path, sizeof(path), "Shared/Chain%04d.props", i);
                    if (i + 1 < files) {
                        snprintf(next, sizeof(next), "<Import Project=\"Chain%04d.props\"/>",
                                 i + 1);
                    }
                    int n = snprintf(xml, sizeof(xml),
                                     "<Project><PropertyGroup><K%04d>Shared%04d</K%04d>"
                                     "</PropertyGroup>%s</Project>",
                                     i, i, i, next);
                    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(xml));
                    source_bytes += (size_t)n;
                    ASSERT_TRUE(dm_msb_add_xml(m, path, xml));
                }
                char root_xml[1024];
                int n = snprintf(root_xml, sizeof(root_xml),
                                 "<Project><Import Project=\"Chain0000.props\"/><ItemGroup>"
                                 "<Using Include=\"$(K0000)\" Alias=\"First\"/>"
                                 "<Using Include=\"$(K%04d)\" Alias=\"Last\"/>"
                                 "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Project\"/>"
                                 "</ItemGroup></Project>",
                                 files - 1);
                ASSERT_TRUE(n > 0 && (size_t)n < sizeof(root_xml));
                source_bytes += (size_t)n;
                ASSERT_TRUE(dm_msb_add_xml(m, "Shared/Root.props", root_xml));
                const char wrapper[] =
                    "<Project><Import Project=\"../../Shared/Root.props\"/></Project>";
                const char project_xml[] = "<Project/>";
                for (int i = 0; i <= projects; i++) {
                    char root[96];
                    char project[96];
                    snprintf(root, sizeof(root), "src/R%02d/Directory.Build.%s", i, suffix);
                    snprintf(project, sizeof(project), "src/R%02d/P%02d.csproj", i, i);
                    ASSERT_TRUE(dm_msb_add_xml(m, root, wrapper));
                    ASSERT_TRUE(dm_msb_add_xml(m, project, project_xml));
                    source_bytes += sizeof(wrapper) + sizeof(project_xml) - 2;
                }
                cbm_msb_test_cost_reset();
                cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
                ASSERT_NOT_NULL(context);
                uint64_t captured = 0;
                for (int i = 0; i <= projects; i++) {
                    char path[96];
                    char name[32];
                    char last[32];
                    snprintf(path, sizeof(path), "src/R%02d/P%02d.csproj", i, i);
                    snprintf(name, sizeof(name), "P%02d", i);
                    snprintf(last, sizeof(last), "Shared%04d", files - 1);
                    cbm_msb_result_t actual = {0};
                    ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &actual));
                    cbm_msb_using_t expected_usings[] = {
                        {.kind = 'a', .alias = "Project", .target = name},
                        {.kind = 'a', .alias = "First", .target = "Shared0000"},
                        {.kind = 'a', .alias = "Last", .target = last},
                    };
                    cbm_msb_result_t expected = {.usings = expected_usings, .count = 3};
                    correct = dm_msb_same(&actual, &expected) && correct;
                    cbm_msb_result_free(&actual);
                    if (i == 0) {
                        captured = cbm_msb_test_work();
                    }
                }
                hits[mode][size][count] = cbm_msb_test_work() - captured;
                cbm_msb_eval_context_free(context);
                work[mode][size][count] = cbm_msb_test_work();
                uint64_t interpreted = 0;
                uint64_t peak = 0;
                cbm_msb_test_cost(&interpreted, &peak);
                correct = correct && cbm_msb_test_value_live_bytes() == 0;
                /* Allow fixed per-file metadata and bounded-depth state nodes,
                 * but reject a default64KiB retained arena for every tiny file. */
                uint64_t bound = (uint64_t)source_bytes * 64 + 1024 * 1024;
                storage_bounded = storage_bounded && peak <= bound;
                fprintf(stderr,
                        "msbuild shared files mode=%d files=%d projects=%d interpreted=%llu "
                        "work=%llu hit_work=%llu peak=%llu bound=%llu live=%llu semantics=%d\n",
                        mode, files, projects, (unsigned long long)interpreted,
                        (unsigned long long)work[mode][size][count],
                        (unsigned long long)hits[mode][size][count], (unsigned long long)peak,
                        (unsigned long long)bound,
                        (unsigned long long)cbm_msb_test_value_live_bytes(), correct);
                cbm_msb_free(m);
            }
        }
    }
    ASSERT_TRUE(correct);
    bool bounded = true;
    for (int mode = 0; mode < 2; mode++) {
        for (int count = 0; count < 2; count++) {
            ASSERT_TRUE(hits[mode][0][count] > 0);
            double ratio = (double)hits[mode][1][count] / (double)hits[mode][0][count];
            fprintf(stderr, "msbuild shared files size hit ratio mode=%d projects=%d ratio=%.3f\n",
                    mode, 16 << count, ratio);
            bounded = bounded && ratio >= 0.9 && ratio <= 1.1;
        }
        ASSERT_TRUE(work[mode][1][0] > work[mode][0][0]);
        ASSERT_TRUE(work[mode][1][1] > work[mode][0][1]);
        uint64_t small = work[mode][1][0] - work[mode][0][0];
        uint64_t large = work[mode][1][1] - work[mode][0][1];
        double ratio = (double)large / (double)small;
        fprintf(stderr, "msbuild shared files extra-size ratio mode=%d ratio=%.3f\n", mode, ratio);
        bounded = bounded && ratio >= 0.9 && ratio <= 1.2;
    }
    ASSERT_TRUE(bounded && storage_bounded);
    PASS();
}

/* Shared components must preserve import order and history across distinct
 * wrappers; the ordinary evaluator remains the exact semantic oracle. */
TEST(doc_mentions_msbuild_shared_closure_isolation) {
    const dm_project_file_t common[] = {
        {"Shared/State.props",
         "<Project><PropertyGroup><Value>Shared</Value><Copied>$(Before)</Copied>"
         "</PropertyGroup><Import Project=\"Leaf.props\"/>"
         "<Import Project=\"Diamond.props\"/><Import Project=\"Later.props\"/>"
         "<ItemGroup><Using Include=\"$(Value)\" Alias=\"FinalValue\"/>"
         "<Using Include=\"$(Copied)\" Alias=\"Before\"/>"
         "<Using Include=\"$(After)\" Alias=\"After\"/>"
         "<Using Include=\"File.$(MSBuildThisFileName)\" Alias=\"File\"/>"
         "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Project\"/>"
         "</ItemGroup></Project>"},
        {"Shared/Leaf.props", "<Project><PropertyGroup><Leaf>$(Value).Leaf</Leaf></PropertyGroup>"
                              "<Import Project=\"Cycle.props\"/><ItemGroup>"
                              "<Using Include=\"$(Leaf)\" Alias=\"Leaf\"/>"
                              "<Using Include=\"Leaf.$(MSBuildThisFileName)\" Alias=\"LeafFile\"/>"
                              "</ItemGroup></Project>"},
        {"Shared/Diamond.props",
         "<Project><Import Project=\"Leaf.props\"/><ItemGroup>"
         "<Using Include=\"Diamond\" Alias=\"Diamond\"/></ItemGroup></Project>"},
        {"Shared/Cycle.props", "<Project><Import Project=\"State.props\"/><ItemGroup>"
                               "<Using Include=\"Cycle\" Alias=\"Cycle\"/></ItemGroup></Project>"},
    };
    bool correct = true;
    for (int mode = 0; mode < 2; mode++) {
        cbm_msb_t *m = cbm_msb_new();
        ASSERT_NOT_NULL(m);
        for (size_t i = 0; i < sizeof(common) / sizeof(common[0]); i++) {
            ASSERT_TRUE(dm_msb_add_xml(m, common[i].rel_path, common[i].xml));
        }
        const char *suffix = mode == 0 ? "props" : "targets";
        char paths[5][96];
        for (int i = 0; i < 5; i++) {
            char root[96];
            char wrapper[2048];
            char project[512];
            char name = (char)('A' + i);
            snprintf(root, sizeof(root), "Roots/%c/Directory.Build.%s", name, suffix);
            snprintf(paths[i], sizeof(paths[i]), "Roots/%c/%c.csproj", name, name);
            snprintf(wrapper, sizeof(wrapper),
                     "<Project><PropertyGroup><Before>%s</Before><Value>Pre.%c</Value>"
                     "</PropertyGroup>%s<Import Project=\"../../Shared/State.props\"/>"
                     "<Import Project=\"../../Shared/State.props\"/><PropertyGroup>"
                     "<Value>Post.%c</Value><After>After.%c</After></PropertyGroup></Project>",
                     i == 2 ? "Changed" : "Same", name,
                     i == 3 ? "<Import Project=\"../../Shared/State.props\" "
                              "Condition=\"'$(Unknown)' == 'on'\"/>"
                            : "",
                     name, name);
            snprintf(project, sizeof(project),
                     "<Project><PropertyGroup><Value>Project.%c</Value></PropertyGroup>"
                     "%s</Project>",
                     name, i == 4 ? "<Import Project=\"../../Shared/State.props\"/>" : "");
            ASSERT_TRUE(dm_msb_add_xml(m, root, wrapper));
            ASSERT_TRUE(dm_msb_add_xml(m, paths[i], project));
        }
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        cbm_msb_result_t held = {0};
        cbm_msb_result_t expected = {0};
        ASSERT_TRUE(cbm_msb_eval_context_eval(context, paths[0], &held));
        ASSERT_TRUE(cbm_msb_eval(m, paths[0], &expected));
        correct = correct && dm_msb_same(&held, &expected) && held.count == 9 &&
                  dm_has_using(&held, 'a', "Shared.Leaf") &&
                  dm_has_using(&held, 'a', "File.State") && dm_has_using(&held, 'a', "Leaf.Leaf") &&
                  dm_has_using(&held, 'a', mode == 0 ? "Project.A" : "Post.A");
        const int order[] = {0, 1, 2, 0, 3, 1, 4, 0};
        for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
            correct = dm_msb_context_matches(context, m, paths[order[i]]) && correct;
        }
        ASSERT_TRUE(dm_msb_add_xml(m, "Shared/Later.props",
                                   "<Project><ItemGroup><Using Include=\"Later.Added\" "
                                   "Alias=\"Later\"/></ItemGroup></Project>"));
        for (int i = 0; i < 5; i++) {
            correct = dm_msb_context_matches(context, m, paths[i]) && correct;
        }
        cbm_msb_eval_context_free(context);
        cbm_msb_free(m);
        correct = correct && dm_msb_same(&held, &expected) && cbm_msb_test_value_live_bytes() == 0;
        cbm_msb_result_free(&held);
        cbm_msb_result_free(&expected);
    }
    /* Preserve the parent's read of entry X while masking the child's X read
     * after X is overwritten. Y escapes from the child and must join the input
     * requirements. A child-state witness cannot certify the earlier X read. */
    for (int mode = 0; mode < 2; mode++) {
        cbm_msb_t *m = cbm_msb_new();
        ASSERT_NOT_NULL(m);
        ASSERT_TRUE(
            dm_msb_add_xml(m, "Common/Union.props",
                           "<Project><PropertyGroup><Snapshot>$(X)</Snapshot><X>Forced</X>"
                           "</PropertyGroup><Import Project=\"Child.props\"/><ItemGroup>"
                           "<Using Include=\"$(Snapshot)\" Alias=\"Snapshot\"/>"
                           "<Using Include=\"$(FromX)\" Alias=\"FromX\"/>"
                           "<Using Include=\"$(FromY)\" Alias=\"FromY\"/></ItemGroup></Project>"));
        ASSERT_TRUE(dm_msb_add_xml(m, "Common/Child.props",
                                   "<Project><PropertyGroup><FromX>$(X)</FromX><FromY>$(Y)</FromY>"
                                   "</PropertyGroup></Project>"));
        const char *xs[] = {"Old", "Forced", "Old", "Old"};
        const char *ys[] = {"One", "One", "Two", NULL};
        const char *y_records[] = {"<Y>One</Y>", "<Y>One</Y>", "<Y>Two</Y>", ""};
        char paths[4][96];
        for (int i = 0; i < 4; i++) {
            char root[96];
            char xml[512];
            snprintf(root, sizeof(root), "Roots/R%d/Directory.Build.%s", i,
                     mode == 0 ? "props" : "targets");
            snprintf(paths[i], sizeof(paths[i]), "Roots/R%d/P.csproj", i);
            snprintf(xml, sizeof(xml),
                     "<Project><PropertyGroup><X>%s</X>%s</PropertyGroup>"
                     "<Import Project=\"../../Common/Union.props\"/></Project>",
                     xs[i], y_records[i]);
            ASSERT_TRUE(dm_msb_add_xml(m, root, xml));
            ASSERT_TRUE(dm_msb_add_xml(m, paths[i], "<Project/>"));
        }
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        const int order[] = {0, 1, 2, 0, 3, 0};
        for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
            int which = order[i];
            cbm_msb_result_t actual = {0};
            cbm_msb_result_t reference = {0};
            ASSERT_TRUE(cbm_msb_eval_context_eval(context, paths[which], &actual));
            ASSERT_TRUE(cbm_msb_eval(m, paths[which], &reference));
            correct = dm_msb_same(&actual, &reference) && correct;
            bool found[3] = {false, false, false};
            for (int j = 0; j < actual.count; j++) {
                const cbm_msb_using_t *u = &actual.usings[j];
                found[0] = found[0] || (u->kind == 'a' && strcmp(u->alias, "Snapshot") == 0 &&
                                        strcmp(u->target, xs[which]) == 0);
                found[1] = found[1] || (u->kind == 'a' && strcmp(u->alias, "FromX") == 0 &&
                                        strcmp(u->target, "Forced") == 0);
                found[2] =
                    found[2] || (ys[which] && u->kind == 'a' && strcmp(u->alias, "FromY") == 0 &&
                                 strcmp(u->target, ys[which]) == 0);
            }
            correct = correct && found[0] && found[1] && found[2] == (ys[which] != NULL) &&
                      actual.count == (ys[which] ? 3 : 2) && actual.open == (ys[which] == NULL) &&
                      actual.unevaluable == (ys[which] ? 0 : 1) && actual.outside == 0;
            cbm_msb_result_free(&actual);
            cbm_msb_result_free(&reference);
        }
        cbm_msb_eval_context_free(context);
        cbm_msb_free(m);
        correct = correct && cbm_msb_test_value_live_bytes() == 0;
    }
    /* Same-length call-local values exercise revision identity independently
     * of value length. Repeat callers after their temporary state is freed. */
    for (int mode = 0; mode < 2; mode++) {
        cbm_msb_t *m = cbm_msb_new();
        ASSERT_NOT_NULL(m);
        ASSERT_TRUE(
            dm_msb_add_xml(m, "Read.props",
                           "<Project><PropertyGroup><Copied>$(Input)</Copied></PropertyGroup>"
                           "<ItemGroup><Using Include=\"$(Copied)\" Alias=\"Value\"/>"
                           "</ItemGroup></Project>"));
        const char *values[] = {"Old", "New", "Alt"};
        char paths[3][32];
        for (int i = 0; i < 3; i++) {
            char xml[512];
            snprintf(paths[i], sizeof(paths[i]), "P%d.csproj", i);
            snprintf(xml, sizeof(xml),
                     "<Project><PropertyGroup><Input>Tmp</Input><Input>%s</Input>"
                     "</PropertyGroup>%s</Project>",
                     values[i], mode == 0 ? "<Import Project=\"Read.props\"/>" : "");
            ASSERT_TRUE(dm_msb_add_xml(m, paths[i], xml));
        }
        if (mode != 0) {
            ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets",
                                       "<Project><Import Project=\"Read.props\"/></Project>"));
        }
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        const int order[] = {0, 1, 0, 2, 1, 0};
        for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
            int which = order[i];
            cbm_msb_result_t actual = {0};
            cbm_msb_result_t reference = {0};
            ASSERT_TRUE(cbm_msb_eval_context_eval(context, paths[which], &actual));
            ASSERT_TRUE(cbm_msb_eval(m, paths[which], &reference));
            cbm_msb_using_t expected_using = {
                .kind = 'a', .alias = "Value", .target = values[which]};
            cbm_msb_result_t expected = {.usings = &expected_using, .count = 1};
            correct =
                dm_msb_same(&actual, &reference) && dm_msb_same(&actual, &expected) && correct;
            cbm_msb_result_free(&actual);
            cbm_msb_result_free(&reference);
        }
        cbm_msb_eval_context_free(context);
        cbm_msb_free(m);
        correct = correct && cbm_msb_test_value_live_bytes() == 0;
    }
    ASSERT_TRUE(correct);
    PASS();
}

/* Node acquisition failures must take the ordinary OOM path. The retained
 * component remains usable and every transient owner is released. */
TEST(doc_mentions_msbuild_components_failure) {
    char xml[16384];
    size_t w = (size_t)snprintf(xml, sizeof(xml), "<Project><PropertyGroup>");
    for (int i = 0; i < 64; i++) {
        w +=
            (size_t)snprintf(xml + w, sizeof(xml) - w, "<K%03d>$(I%03d).N%03d</K%03d>", i, i, i, i);
    }
    w += (size_t)snprintf(xml + w, sizeof(xml) - w,
                          "</PropertyGroup><Import Project=\"Child.props\"/>"
                          "<ItemGroup><Using Include=\"$(K000)\" Alias=\"First\"/>"
                          "<Using Include=\"$(Last)\" Alias=\"Last\"/>"
                          "</ItemGroup></Project>");
    ASSERT_TRUE(w < sizeof(xml));
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Shared.props", xml));
    ASSERT_TRUE(dm_msb_add_xml(m, "Child.props",
                               "<Project><PropertyGroup><Last>$(K063)</Last>"
                               "<Input>Forced</Input></PropertyGroup></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets",
                               "<Project><PropertyGroup><Before>Before</Before></PropertyGroup>"
                               "<Import Project=\"Shared.props\"/>"
                               "<PropertyGroup><After>After</After></PropertyGroup></Project>"));
    for (int project = 0; project < 3; project++) {
        char path[32];
        snprintf(path, sizeof(path), "%c.csproj", 'A' + project);
        w = (size_t)snprintf(xml, sizeof(xml), "<Project><PropertyGroup><Local>P%d</Local>",
                             project);
        for (int i = 0; i < 64; i++) {
            w += (size_t)snprintf(xml + w, sizeof(xml) - w, "<I%03d>%s</I%03d>", i,
                                  project == 2 ? "BBBB" : "AAAA", i);
        }
        w += (size_t)snprintf(xml + w, sizeof(xml) - w, "</PropertyGroup></Project>");
        ASSERT_TRUE(w < sizeof(xml));
        ASSERT_TRUE(dm_msb_add_xml(m, path, xml));
    }
    const struct {
        cbm_msb_node_fail_operation_t operation;
        int nth;
        bool warm;
    } cases[] = {
        {CBM_MSB_NODE_FAIL_CAPTURE, 1, false},    {CBM_MSB_NODE_FAIL_CAPTURE, 5, false},
        {CBM_MSB_NODE_FAIL_OVERLAY, 1, false},    {CBM_MSB_NODE_FAIL_OVERLAY, 5, false},
        {CBM_MSB_NODE_FAIL_DEPENDENCY, 1, false}, {CBM_MSB_NODE_FAIL_DEPENDENCY, 5, false},
        {CBM_MSB_NODE_FAIL_APPLY, 1, true},       {CBM_MSB_NODE_FAIL_APPLY, 5, true},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t before = cbm_mem_tracked_live_bytes();
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        if (cases[i].warm) {
            failures += !dm_msb_context_matches(context, m, "A.csproj");
        }
        cbm_msb_test_fail_node_alloc(cases[i].operation, cases[i].nth);
        cbm_msb_result_t actual = {0};
        bool ok = cbm_msb_eval_context_eval(context, "A.csproj", &actual);
        bool consumed = cbm_msb_test_node_alloc_failed();
        failures +=
            ok || !consumed || actual.mem != NULL || actual.usings != NULL || actual.count != 0;
        cbm_msb_result_free(&actual);
        cbm_msb_test_fail_node_alloc(CBM_MSB_NODE_FAIL_NONE, 0);
        const char *order[] = {"A.csproj", "B.csproj", "C.csproj", "A.csproj"};
        for (size_t j = 0; j < sizeof(order) / sizeof(order[0]); j++) {
            failures += !dm_msb_context_matches(context, m, order[j]);
        }
        cbm_msb_eval_context_free(context);
        size_t after = cbm_mem_tracked_live_bytes();
        failures += after != before || cbm_msb_test_value_live_bytes() != 0;
        fprintf(stderr,
                "msbuild components fault operation=%d nth=%d consumed=%d "
                "before=%llu after=%llu failures=%d\n",
                (int)cases[i].operation, cases[i].nth, consumed, (unsigned long long)before,
                (unsigned long long)after, failures);
    }
    cbm_msb_free(m);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Revision witnesses must remain exact when short-lived state reuses slots.
 * The observations certify that this fixture actually reaches those paths. */
TEST(doc_mentions_msbuild_components_revision) {
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.targets",
                               "<Project><Import Project=\"Constants.props\"/>"
                               "<Import Project=\"Read.props\"/></Project>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Constants.props",
                               "<Project><PropertyGroup><Stable>Stable.Fixed</Stable>"
                               "</PropertyGroup></Project>"));
    ASSERT_TRUE(
        dm_msb_add_xml(m, "Read.props",
                       "<Project><PropertyGroup><Snapshot>$(Input)</Snapshot><Copy>$(Stable)</Copy>"
                       "</PropertyGroup>"
                       "<ItemGroup><Using Include=\"$(Snapshot)\" Alias=\"Value\"/>"
                       "<Using Include=\"$(Copy)\" Alias=\"Constant\"/></ItemGroup></Project>"));
    const char *values[] = {"AAAA", "BBBB", "CCCC"};
    char paths[3][32];
    for (int i = 0; i < 3; i++) {
        char xml[512];
        snprintf(paths[i], sizeof(paths[i]), "P%d.csproj", i);
        snprintf(xml, sizeof(xml),
                 "<Project><PropertyGroup><Input>Temp</Input><Input>%s</Input>"
                 "<Other>Unchanged</Other></PropertyGroup></Project>",
                 values[i]);
        ASSERT_TRUE(dm_msb_add_xml(m, paths[i], xml));
    }
    size_t before = cbm_mem_tracked_live_bytes();
    cbm_msb_test_cost_reset();
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    const int order[] = {0, 0, 1, 0, 2, 1, 0};
    bool correct = true;
    for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
        int which = order[i];
        cbm_msb_result_t actual = {0};
        cbm_msb_result_t reference = {0};
        ASSERT_TRUE(cbm_msb_eval_context_eval(context, paths[which], &actual));
        ASSERT_TRUE(cbm_msb_eval(m, paths[which], &reference));
        cbm_msb_using_t expected_usings[] = {
            {.kind = 'a', .alias = "Value", .target = values[which]},
            {.kind = 'a', .alias = "Constant", .target = "Stable.Fixed"},
        };
        cbm_msb_result_t expected = {.usings = expected_usings, .count = 2};
        correct = dm_msb_same(&actual, &reference) && dm_msb_same(&actual, &expected) && correct;
        cbm_msb_result_free(&actual);
        cbm_msb_result_free(&reference);
    }
    cbm_msb_state_test_stats_t stats = {0};
    cbm_msb_test_state_stats(&stats);
    cbm_msb_eval_context_free(context);
    size_t after = cbm_mem_tracked_live_bytes();
    correct = correct && before == after && cbm_msb_test_value_live_bytes() == 0;
    cbm_msb_free(m);
    fprintf(stderr,
            "msbuild components revisions allocations=%llu reused=%llu witnessed_reused=%llu "
            "same_length=%llu witness_skips=%llu revision_errors=%llu semantics=%d\n",
            (unsigned long long)stats.allocations, (unsigned long long)stats.slot_reuses,
            (unsigned long long)stats.witnessed_slot_reuses,
            (unsigned long long)stats.same_length_value_changes,
            (unsigned long long)stats.witness_skips, (unsigned long long)stats.revision_errors,
            correct);
    ASSERT_TRUE(correct);
    ASSERT_TRUE(stats.allocations > 0 && stats.slot_reuses > 0 && stats.witnessed_slot_reuses > 0 &&
                stats.same_length_value_changes > 0 && stats.witness_skips > 0 &&
                stats.revision_errors == 0);
    PASS();
}

/* Mixed presence constraints must not use the all-absent empty-range shortcut.
 * Insertion order places A/B at seen keys 4/5, with all other visited files
 * outside that aligned range. Warm Shared sees A present and B absent; Cold
 * reaches the same retained component with the entire range empty. */
TEST(doc_mentions_msbuild_components_mixed_absence) {
    const dm_project_file_t files[] = {
        {"Warm.csproj", "<Project><Import Project=\"A.props\"/>"
                        "<Import Project=\"Shared.props\"/></Project>"},
        {"Cold.csproj", "<Project><Import Project=\"Shared.props\"/></Project>"},
        {"Padding.props", "<Project/>"},
        {"A.props", "<Project><ItemGroup><Using Include=\"A.Effect\"/>"
                    "</ItemGroup></Project>"},
        {"B.props", "<Project><ItemGroup><Using Include=\"B.Effect\"/>"
                    "</ItemGroup></Project>"},
        {"Shared.props", "<Project><Import Project=\"A.props\"/>"
                         "<Import Project=\"B.props\"/></Project>"},
    };
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(dm_msb_add_xml(m, files[i].rel_path, files[i].xml));
    }
    cbm_msb_using_t expected_usings[] = {
        {.kind = 'n', .alias = "", .target = "A.Effect"},
        {.kind = 'n', .alias = "", .target = "B.Effect"},
    };
    cbm_msb_result_t expected = {.usings = expected_usings, .count = 2};
    size_t before = cbm_mem_tracked_live_bytes();
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    bool correct = true;
    const char *order[] = {"Warm.csproj", "Cold.csproj", "Cold.csproj", "Warm.csproj",
                           "Cold.csproj"};
    for (size_t i = 0; i < sizeof(order) / sizeof(order[0]); i++) {
        cbm_msb_result_t actual = {0};
        cbm_msb_result_t reference = {0};
        ASSERT_TRUE(cbm_msb_eval_context_eval(context, order[i], &actual));
        ASSERT_TRUE(cbm_msb_eval(m, order[i], &reference));
        correct = dm_msb_same(&actual, &reference) && dm_msb_same(&actual, &expected) && correct;
        cbm_msb_result_free(&actual);
        cbm_msb_result_free(&reference);
    }
    cbm_msb_eval_context_free(context);
    size_t after = cbm_mem_tracked_live_bytes();
    correct = correct && before == after && cbm_msb_test_value_live_bytes() == 0;
    cbm_msb_free(m);
    fprintf(stderr, "msbuild components mixed absence semantics=%d before=%llu after=%llu\n",
            correct, (unsigned long long)before, (unsigned long long)after);
    ASSERT_TRUE(correct);
    PASS();
}

TEST(doc_mentions_msbuild_prefix_work) {
    enum { CAP = 131072 };
    char *xml = malloc(CAP);
    ASSERT_NOT_NULL(xml);
    uint64_t work[2][2] = {{0}};
    bool correct = true;
    for (int size = 0; size < 2; size++) {
        int records = 512 << size;
        for (int count = 0; count < 2; count++) {
            int projects = 16 << count;
            cbm_msb_t *m = cbm_msb_new();
            ASSERT_NOT_NULL(m);
            size_t w = (size_t)snprintf(xml, CAP, "<Project><PropertyGroup>");
            for (int i = 0; i < records; i++) {
                w += (size_t)snprintf(xml + w, CAP - w, "<K%04d>Shared%04d</K%04d>", i, i, i);
            }
            w += (size_t)snprintf(
                xml + w, CAP - w,
                "</PropertyGroup><ItemGroup><Using Include=\"$(K0000)\" Alias=\"First\"/>"
                "<Using Include=\"$(K%04d)\" Alias=\"Last\"/>"
                "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Project\"/>"
                "</ItemGroup></Project>",
                records - 1);
            ASSERT_TRUE(w < CAP);
            ASSERT_TRUE(dm_msb_add_xml(m, "Shared.props", xml));
            ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props",
                                       "<Project><Import Project=\"Shared.props\"/></Project>"));
            for (int i = 0; i < projects; i++) {
                char path[64];
                snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                ASSERT_TRUE(dm_msb_add_xml(m, path, "<Project/>"));
            }
            cbm_msb_test_cost_reset();
            cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
            ASSERT_NOT_NULL(context);
            for (int i = 0; i < projects; i++) {
                char path[64];
                char project[32];
                char last[32];
                snprintf(path, sizeof(path), "src/P%02d.csproj", i);
                snprintf(project, sizeof(project), "P%02d", i);
                snprintf(last, sizeof(last), "Shared%04d", records - 1);
                cbm_msb_result_t r;
                ASSERT_TRUE(cbm_msb_eval_context_eval(context, path, &r));
                correct = correct && r.count == 3 && !r.open && r.unevaluable == 0 &&
                          r.outside == 0 && dm_has_using(&r, 'a', "Shared0000") &&
                          dm_has_using(&r, 'a', last) && dm_has_using(&r, 'a', project);
                cbm_msb_result_free(&r);
            }
            cbm_msb_eval_context_free(context);
            work[size][count] = cbm_msb_test_work();
            uint64_t consumed = 0;
            uint64_t peak = 0;
            cbm_msb_test_cost(&consumed, &peak);
            fprintf(stderr,
                    "msbuild prefix records=%d projects=%d interpreted=%llu work=%llu "
                    "peak=%llu live=%llu semantics=%d\n",
                    records, projects, (unsigned long long)consumed,
                    (unsigned long long)work[size][count], (unsigned long long)peak,
                    (unsigned long long)cbm_msb_test_value_live_bytes(), correct);
            correct = correct && cbm_msb_test_value_live_bytes() == 0;
            cbm_msb_free(m);
        }
    }
    free(xml);
    ASSERT_TRUE(correct);
    bool bounded = true;
    for (int size = 0; size < 2; size++) {
        ASSERT_TRUE(work[size][0] > 0);
        double ratio = (double)work[size][1] / (double)work[size][0];
        fprintf(stderr, "msbuild prefix project ratio records=%d ratio=%.3f\n", 512 << size, ratio);
        bounded = bounded && ratio >= 0.9 && ratio <= 1.4;
    }
    ASSERT_TRUE(bounded);
    PASS();
}

/* The uncached evaluator is the semantic oracle, including diagnostics and
 * ordering. Exercise shared properties and items across project/root changes. */
TEST(doc_mentions_msbuild_prefix_isolation) {
    const dm_project_file_t files[] = {
        {"Directory.Build.props",
         "<Project><PropertyGroup><Value>Prefix</Value><Empty></Empty>"
         "</PropertyGroup><Import Project=\"Common.props\"/>"
         "<Import Project=\"Later.props\"/><ItemGroup>"
         "<Using Include=\"$(Value)\" Alias=\"Value\"/>"
         "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Name\"/>"
         "<Using Include=\"$(Empty)Tail\" Alias=\"Empty\"/></ItemGroup></Project>"},
        {"Common.props", "<Project><PropertyGroup><Imported>Yes</Imported></PropertyGroup>"
                         "<ItemGroup><Using Include=\"$(Imported)\"/></ItemGroup></Project>"},
        {"Directory.Build.targets",
         "<Project><PropertyGroup><After>$(Flavor)</After>"
         "</PropertyGroup><ItemGroup Condition=\"'$(After)' == 'A'\">"
         "<Using Include=\"Target.A\"/></ItemGroup>"
         "<ItemGroup Condition=\"'$(After)' == 'B'\"><Using Include=\"Target.B\"/>"
         "</ItemGroup></Project>"},
        {"A.csproj",
         "<Project><PropertyGroup><Value>A</Value><Flavor>A</Flavor>"
         "<Empty>Nonempty</Empty></PropertyGroup><Import Project=\"Common.props\"/></Project>"},
        {"B.csproj", "<Project><PropertyGroup><Flavor>B</Flavor></PropertyGroup></Project>"},
        {"Poison.csproj", "<Project><Import Project=\"Poison.props\" "
                          "Condition=\"'$(Unknown)' == 'on'\"/><PropertyGroup><Flavor>B</Flavor>"
                          "</PropertyGroup></Project>"},
        {"Poison.props", "<Project><PropertyGroup><Value>Hidden</Value><Empty>Hidden</Empty>"
                         "</PropertyGroup></Project>"},
        {"Write/Directory.Build.props",
         "<Project><PropertyGroup>"
         "<MSBuildProjectName>Forced</MSBuildProjectName><Copy>$(MSBuildProjectName)</Copy>"
         "</PropertyGroup><ItemGroup><Using Include=\"$(Copy)\" Alias=\"Copy\"/>"
         "<Using Include=\"$(MSBuildProjectName)\" Alias=\"Name\"/></ItemGroup></Project>"},
        {"Write/A.csproj", "<Project/>"},
        {"Write/B.csproj", "<Project/>"},
        {"Read/Directory.Build.props",
         "<Project><PropertyGroup>"
         "<Copy>$(MSBuildProjectName)</Copy></PropertyGroup>"
         "<Import Project=\"$(MSBuildProjectName).props\"/>"
         "<ItemGroup><Using Include=\"$(Copy)\" Alias=\"Copy\"/></ItemGroup></Project>"},
        {"Read/A.csproj", "<Project/>"},
        {"Read/B.csproj", "<Project/>"},
        {"Read/A.props", "<Project><ItemGroup><Using Include=\"Only.A\"/></ItemGroup></Project>"},
        {"Read/B.props", "<Project><ItemGroup><Using Include=\"Only.B\"/></ItemGroup></Project>"},
        {"History/Directory.Build.props",
         "<Project><PropertyGroup><V>Seed</V></PropertyGroup>"
         "<Import Project=\"Owned.csproj\"/><Import Project=\"Maybe.props\" "
         "Condition=\"'$(Unknown)' == 'on'\"/><ItemGroup>"
         "<Using Include=\"$(V)\"/><Using Include=\"$(X)\"/></ItemGroup></Project>"},
        {"History/Owned.csproj", "<Project><PropertyGroup><V>$(V).Again</V></PropertyGroup>"
                                 "</Project>"},
        {"History/Other.csproj",
         "<Project><PropertyGroup><X>Known</X></PropertyGroup>"
         "<Import Project=\"Maybe.props\" Condition=\"'$(Unknown)' == 'on'\"/></Project>"},
        {"History/Maybe.props", "<Project><PropertyGroup><X>Hidden</X></PropertyGroup></Project>"},
        {"Condition/Directory.Build.props",
         "<Project>"
         "<PropertyGroup Condition=\"'$(MSBuildProjectName)' == 'A'\">"
         "<Choice>For.A</Choice></PropertyGroup>"
         "<PropertyGroup Condition=\"'$(MSBuildProjectName)' == 'B'\">"
         "<Choice>For.B</Choice></PropertyGroup>"
         "<ItemGroup><Using Include=\"$(Choice)\"/></ItemGroup></Project>"},
        {"Condition/A.csproj", "<Project/>"},
        {"Condition/B.csproj", "<Project/>"},
        {"PoisonSeed/Directory.Build.props",
         "<Project><PropertyGroup><X>Known</X>"
         "</PropertyGroup><Import Project=\"$(MSBuildProjectName).props\" "
         "Condition=\"'$(Unknown)' == 'on'\"/><ItemGroup>"
         "<Using Include=\"$(X)\"/></ItemGroup></Project>"},
        {"PoisonSeed/A.csproj", "<Project/>"},
        {"PoisonSeed/B.csproj", "<Project/>"},
        {"PoisonSeed/A.props", "<Project><PropertyGroup><X>Hidden</X>"
                               "</PropertyGroup></Project>"},
        {"PoisonSeed/B.props", "<Project><PropertyGroup><Y>Hidden</Y>"
                               "</PropertyGroup></Project>"},
        {"Implicit/Directory.Build.props",
         "<Project><PropertyGroup>"
         "<ImplicitUsings>enable</ImplicitUsings></PropertyGroup></Project>"},
        {"Implicit/A.csproj", "<Project Sdk=\"Microsoft.NET.Sdk.Web\"/>"},
        {"Implicit/B.csproj", "<Project Sdk=\"Microsoft.NET.Sdk.Worker\"/>"},
        {"Implicit/C.csproj", "<Project><PropertyGroup><ImplicitUsings>disable</ImplicitUsings>"
                              "</PropertyGroup></Project>"},
    };
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(dm_msb_add_xml(m, files[i].rel_path, files[i].xml));
    }
    cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
    ASSERT_NOT_NULL(context);
    const char *projects[] = {"A.csproj",
                              "B.csproj",
                              "Poison.csproj",
                              "B.csproj",
                              "Write/A.csproj",
                              "Write/B.csproj",
                              "Read/A.csproj",
                              "Read/B.csproj",
                              "History/Owned.csproj",
                              "History/Other.csproj",
                              "A.csproj",
                              "Write/A.csproj",
                              "B.csproj",
                              "History/Owned.csproj",
                              "Write/B.csproj",
                              "Condition/A.csproj",
                              "Condition/B.csproj",
                              "Condition/A.csproj",
                              "PoisonSeed/A.csproj",
                              "PoisonSeed/B.csproj",
                              "PoisonSeed/A.csproj",
                              "Implicit/A.csproj",
                              "Implicit/B.csproj",
                              "Implicit/C.csproj",
                              "Implicit/A.csproj"};
    for (size_t i = 0; i < sizeof(projects) / sizeof(projects[0]); i++) {
        ASSERT_TRUE(dm_msb_context_matches(context, m, projects[i]));
    }
    ASSERT_TRUE(dm_msb_context_matches(context, m, "A.csproj"));
    ASSERT_TRUE(dm_msb_add_xml(m, "Later.props",
                               "<Project><ItemGroup>"
                               "<Using Include=\"Now.Present\"/></ItemGroup></Project>"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "A.csproj"));
    ASSERT_TRUE(dm_msb_context_matches(context, m, "B.csproj"));
    cbm_msb_eval_context_free(context);
    ASSERT_EQ(cbm_msb_test_value_live_bytes(), 0);
    cbm_msb_free(m);
    PASS();
}

/* Construction failure must leave no partial prefix, and a failed project
 * must not mutate the already retained prefix. Retry on the same context. */
TEST(doc_mentions_msbuild_prefix_failure) {
    char xml[8192];
    size_t w = (size_t)snprintf(xml, sizeof(xml), "<Project><PropertyGroup>");
    for (int i = 0; i < 64; i++) {
        w += (size_t)snprintf(xml + w, sizeof(xml) - w, "<K%02d>Shared%02d</K%02d>", i, i, i);
    }
    w += (size_t)snprintf(xml + w, sizeof(xml) - w,
                          "</PropertyGroup><ItemGroup><Using Include=\"$(K00)\"/>"
                          "<Using Include=\"$(K63)\"/></ItemGroup></Project>");
    ASSERT_TRUE(w < sizeof(xml));
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(dm_msb_add_xml(m, "Directory.Build.props", xml));
    ASSERT_TRUE(dm_msb_add_xml(m, "A.csproj", "<Project/>"));
    ASSERT_TRUE(dm_msb_add_xml(m, "B.csproj",
                               "<Project><PropertyGroup>"
                               "<K00>Local</K00></PropertyGroup></Project>"));
    int failures = 0;
    for (int mode = 0; mode < 2; mode++) {
        cbm_msb_eval_context_t *context = cbm_msb_eval_context_new(m);
        ASSERT_NOT_NULL(context);
        cbm_msb_test_fail_value_alloc_after(mode == 0 ? 8 : 0);
        cbm_msb_test_fail_prop_insert_after(mode == 1 ? 8 : 0);
        cbm_msb_result_t r = {0};
        bool ok = cbm_msb_eval_context_eval(context, "A.csproj", &r);
        bool consumed =
            mode == 0 ? cbm_msb_test_value_alloc_failed() : cbm_msb_test_prop_insert_failed();
        failures += ok || !consumed || r.mem != NULL || r.usings != NULL || r.count != 0 ||
                    cbm_msb_test_value_live_bytes() != 0;
        cbm_msb_result_free(&r);
        cbm_msb_test_fail_value_alloc_after(0);
        cbm_msb_test_fail_prop_insert_after(0);
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        uint64_t retained = cbm_msb_test_value_live_bytes();
        cbm_msb_test_fail_value_alloc_after(mode == 0 ? 1 : 0);
        cbm_msb_test_fail_prop_insert_after(mode == 1 ? 1 : 0);
        ok = cbm_msb_eval_context_eval(context, "B.csproj", &r);
        consumed =
            mode == 0 ? cbm_msb_test_value_alloc_failed() : cbm_msb_test_prop_insert_failed();
        failures += ok || !consumed || r.mem != NULL || r.usings != NULL || r.count != 0 ||
                    cbm_msb_test_value_live_bytes() != retained;
        cbm_msb_result_free(&r);
        cbm_msb_test_fail_value_alloc_after(0);
        cbm_msb_test_fail_prop_insert_after(0);
        failures += !dm_msb_context_matches(context, m, "A.csproj");
        failures += !dm_msb_context_matches(context, m, "B.csproj");
        cbm_msb_eval_context_free(context);
        failures += cbm_msb_test_value_live_bytes() != 0;
        fprintf(stderr, "msbuild prefix fault mode=%d retained=%llu failures=%d\n", mode,
                (unsigned long long)retained, failures);
    }
    cbm_msb_free(m);
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(doc_mentions_msbuild_eval_storage) {
    enum { CAP = 262144, LONG_LEN = 2048 };
    char *xml = malloc(CAP);
    char *long_value = malloc(LONG_LEN + 1);
    ASSERT_NOT_NULL(xml);
    ASSERT_NOT_NULL(long_value);
    memset(long_value, 'A', LONG_LEN);
    long_value[LONG_LEN] = '\0';
    int failures = 0;
    for (int mode = 0; mode < 3; mode++) {
        for (int repeats = 256; repeats <= 512; repeats *= 2) {
            size_t w =
                (size_t)snprintf(xml, CAP, "<Project><PropertyGroup><Long>%s</Long>", long_value);
            if (mode == 0) {
                for (int i = 0; i < repeats; i++) {
                    w += (size_t)snprintf(
                        xml + w, CAP - w,
                        "<Last Condition=\"'$(Long)' == '$(Long)'\">$(Long)</Last>");
                }
                w += (size_t)snprintf(
                    xml + w, CAP - w,
                    "<Snapshot>$(Last)</Snapshot><Last>Short</Last></PropertyGroup>"
                    "<ItemGroup><Using Include=\"$(Snapshot)\" Alias=\"Saved\"/>"
                    "<Using Include=\"$(Last)\"/></ItemGroup></Project>");
            } else if (mode == 1) {
                w += (size_t)snprintf(xml + w, CAP - w, "<Last Condition=\"");
                for (int i = 0; i < repeats; i++) {
                    w += (size_t)snprintf(xml + w, CAP - w, "%s'$(Long)' == '$(Long)'",
                                          i ? " and " : "");
                }
                w += (size_t)snprintf(xml + w, CAP - w,
                                      "\">Kept</Last></PropertyGroup><ItemGroup>"
                                      "<Using Include=\"$(Last)\"/></ItemGroup></Project>");
            } else {
                w += (size_t)snprintf(xml + w, CAP - w, "</PropertyGroup><ItemGroup>");
                for (int i = 0; i < repeats; i++) {
                    w += (size_t)snprintf(
                        xml + w, CAP - w,
                        "<Using Include=\"Never\" Condition=\"'$(Long)' != '$(Long)'\"/>");
                }
                w += (size_t)snprintf(xml + w, CAP - w,
                                      "<Using Include=\"Kept\"/></ItemGroup></Project>");
            }
            ASSERT_TRUE(w < CAP);
            CBMFileResult *source = dm_extract(xml, CBM_LANG_XML, "App.csproj");
            ASSERT_NOT_NULL(source);
            ASSERT_NOT_NULL(source->doc_scope);
            size_t blob_bytes = strlen(source->doc_scope);
            cbm_msb_t *m = cbm_msb_new();
            ASSERT_NOT_NULL(m);
            ASSERT_TRUE(cbm_msb_add(m, "App.csproj", source->doc_scope));
            cbm_free_result(source);
            cbm_msb_test_cost_reset();
            cbm_msb_result_t r;
            ASSERT_TRUE(cbm_msb_eval(m, "App.csproj", &r));
            uint64_t records = 0;
            uint64_t peak = 0;
            cbm_msb_test_cost(&records, &peak);
            bool semantics = !r.open && r.unevaluable == 0 && r.outside == 0;
            if (mode == 0) {
                semantics = semantics && r.count == 2 && dm_has_using(&r, 'a', long_value) &&
                            dm_has_using(&r, 'n', "Short");
                for (int i = 0; i < r.count; i++) {
                    if (r.usings[i].kind == 'a') {
                        semantics = semantics && strcmp(r.usings[i].alias, "Saved") == 0;
                    }
                }
            } else {
                semantics = semantics && r.count == 1 && dm_has_using(&r, 'n', "Kept");
            }
            uint64_t limit = 131072 + 4 * (uint64_t)blob_bytes;
            fprintf(stderr,
                    "msbuild storage mode=%d repeats=%d blob=%zu records=%llu "
                    "peak=%llu limit=%llu semantics=%d\n",
                    mode, repeats, blob_bytes, (unsigned long long)records,
                    (unsigned long long)peak, (unsigned long long)limit, semantics);
            failures += !semantics || !records || !peak || peak > limit;
            cbm_msb_result_free(&r);
            cbm_msb_free(m);
        }
    }
    free(long_value);
    free(xml);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* A growing self-assignment reads the old value before replacing it. A
 * failed value allocation returns no partial result and does not poison a
 * subsequent independent evaluation on the same input model. */
TEST(doc_mentions_msbuild_value_allocation) {
    enum { LONG_LEN = 2048, CAP = 8192 };
    char long_value[LONG_LEN + 2];
    long_value[0] = 'x';
    memset(long_value + 1, 'A', LONG_LEN);
    long_value[LONG_LEN + 1] = '\0';
    char xml[CAP];
    int written = snprintf(xml, sizeof(xml),
                           "<Project><PropertyGroup><V>x</V><Saved>$(V)</Saved>"
                           "<V>$(V)%s</V><LongSaved>$(V)</LongSaved><V>Short</V>"
                           "</PropertyGroup><Import Project=\"More.props\"/><ItemGroup>"
                           "<Using Include=\"$(Saved)\" Alias=\"Saved\"/>"
                           "<Using Include=\"$(LongSaved)\" Alias=\"LongSaved\"/>"
                           "<Using Include=\"$(V)\"/></ItemGroup></Project>",
                           long_value + 1);
    ASSERT_TRUE(written > 0 && written < CAP);
    const dm_project_file_t files[] = {
        {"App.csproj", xml},
        {"More.props", "<Project><PropertyGroup><V>$(V).Imported</V>"
                       "</PropertyGroup></Project>"},
    };
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    for (int i = 0; i < 2; i++) {
        CBMFileResult *source = dm_extract(files[i].xml, CBM_LANG_XML, files[i].rel_path);
        ASSERT_NOT_NULL(source);
        ASSERT_NOT_NULL(source->doc_scope);
        ASSERT_TRUE(cbm_msb_add(m, files[i].rel_path, source->doc_scope));
        cbm_free_result(source);
    }
    const int fault_after[] = {0, 1, 0, 8, 0, 0, 0};
    const int insert_after[] = {0, 0, 0, 0, 0, 6, 0};
    int failures = 0;
    for (size_t i = 0; i < sizeof(fault_after) / sizeof(fault_after[0]); i++) {
        cbm_msb_test_fail_value_alloc_after(fault_after[i]);
        cbm_msb_test_fail_prop_insert_after(insert_after[i]);
        cbm_msb_result_t r;
        bool ok = cbm_msb_eval(m, "App.csproj", &r);
        bool consumed = cbm_msb_test_value_alloc_failed();
        bool insert_failed = cbm_msb_test_prop_insert_failed();
        bool injected = fault_after[i] || insert_after[i];
        failures += consumed != (fault_after[i] > 0) || insert_failed != (insert_after[i] > 0) ||
                    cbm_msb_test_value_live_bytes() != 0;
        if (injected) {
            failures += ok || r.mem != NULL || r.usings != NULL || r.count != 0;
        } else {
            failures += !ok || consumed || r.open || r.count != 3 || !dm_has_using(&r, 'a', "x") ||
                        !dm_has_using(&r, 'a', long_value) ||
                        !dm_has_using(&r, 'n', "Short.Imported");
            for (int k = 0; k < r.count; k++) {
                if (r.usings[k].kind == 'a') {
                    const char *alias =
                        strcmp(r.usings[k].target, "x") == 0 ? "Saved" : "LongSaved";
                    failures += strcmp(r.usings[k].alias, alias) != 0;
                }
            }
        }
        fprintf(stderr,
                "msbuild value allocation nth=%d consumed=%d insert=%d/%d "
                "live=%llu success=%d failures=%d\n",
                fault_after[i], consumed, insert_after[i], insert_failed,
                (unsigned long long)cbm_msb_test_value_live_bytes(), ok, failures);
        cbm_msb_result_free(&r);
    }
    cbm_msb_test_fail_value_alloc_after(0);
    cbm_msb_test_fail_prop_insert_after(0);
    cbm_msb_free(m);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Scratch resets must not change copied properties, output strings, or the
 * poisoning caused by an unknown import (including an imported child). */
TEST(doc_mentions_msbuild_value_lifetimes) {
    const dm_project_file_t files[] = {
        {"App.csproj",
         "<Project><PropertyGroup>"
         "<Seed>One;Two</Seed><Remember>$(Seed)</Remember><Seed>Changed</Seed>"
         "<Choice>Ready</Choice><Alias>AliasKept</Alias>"
         "<Empty></Empty><FromEmpty>$(Empty)Tail</FromEmpty></PropertyGroup>"
         "<Import Project=\"Unknown.props\" Condition=\"'$(Missing)' == 'on'\"/>"
         "<PropertyGroup><Choice>Recovered</Choice><AfterUnknown>$(Empty)</AfterUnknown>"
         "<Empty></Empty><RecoveredEmpty>$(Empty)Again</RecoveredEmpty></PropertyGroup>"
         "<ItemGroup Condition=\"'$(Choice)' == 'Recovered'\">"
         "<Using Include=\"$(Remember)\" Alias=\"$(Alias)\"/>"
         "<Using Include=\"Drop.Me\"/><Using Remove=\"Drop.Me\"/>"
         "<Using Include=\"Static.Kept\" Static=\"true\"/>"
         "<Using Include=\"Tail.Kept\"/><Using Include=\"$(Poisoned)\"/>"
         "<Using Include=\"$(FromEmpty)\"/><Using Include=\"$(RecoveredEmpty)\"/>"
         "<Using Include=\"$(AfterUnknown)\"/>"
         "</ItemGroup></Project>"},
        {"Unknown.props", "<Project><PropertyGroup><Choice>Changed</Choice>"
                          "</PropertyGroup><Import Project=\"Child.props\"/></Project>"},
        {"Child.props",
         "<Project><PropertyGroup><Poisoned>Unknown.Value</Poisoned><Empty>Poison</Empty>"
         "</PropertyGroup></Project>"},
    };
    cbm_msb_result_t r;
    ASSERT_TRUE(dm_msb_eval(files, 3, "App.csproj", &r));
    ASSERT_EQ(r.count, 6);
    ASSERT_TRUE(dm_has_using(&r, 'a', "One"));
    ASSERT_TRUE(dm_has_using(&r, 'a', "Two"));
    ASSERT_TRUE(dm_has_using(&r, 's', "Static.Kept"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "Tail.Kept"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "Tail"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "Again"));
    for (int i = 0; i < r.count; i++) {
        if (r.usings[i].kind == 'a') {
            ASSERT_STR_EQ(r.usings[i].alias, "AliasKept");
        }
    }
    ASSERT_TRUE(r.open);
    ASSERT_EQ(r.unevaluable, 3);
    ASSERT_EQ(r.outside, 0);
    cbm_msb_result_free(&r);
    PASS();
}

TEST(doc_mentions_msbuild_usings) {
    const dm_project_file_t files[] = {
        {"Directory.Build.props", "<Project>\n"
                                  "  <PropertyGroup><Flavor>web</Flavor></PropertyGroup>\n"
                                  "  <ItemGroup Condition=\"'$(Flavor)' == 'web'\">\n"
                                  "    <Using Include=\"From.Props\" />\n"
                                  "  </ItemGroup>\n"
                                  "  <ItemGroup Condition=\"'$(Flavor)' == 'console'\">\n"
                                  "    <Using Include=\"Never.Here\" />\n"
                                  "  </ItemGroup>\n"
                                  "</Project>\n"},
        {"src/App/App.csproj",
         "\xEF\xBB\xBF<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
         "<!-- <Using Include=\"In.A.Comment\" /> -->\r\n"
         "<Project Sdk=\"Microsoft.NET.Sdk\">\r\n"
         "  <PropertyGroup><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>\r\n"
         "  <ItemGroup>\r\n"
         "    <Using Include=\"Acme.Extra\" />\r\n"
         "    <Using Remove=\"System.Linq\" />\r\n"
         "    <Using Include=\"Acme.Static\" Static=\"true\" />\r\n"
         "    <Using Include=\"Acme.Aliased\" Alias=\"AA\" />\r\n"
         "    <Using Include=\"Acme.One; Acme.Two ;;\" />\r\n"
         "    <Compile Include=\"Not.A.Using\" />\r\n"
         "  </ItemGroup>\r\n"
         "  <Target Name=\"T\"><ItemGroup><Using Include=\"In.A.Target\" "
         "/></ItemGroup></Target>\r\n"
         "</Project>\r\n"},
    };
    cbm_msb_result_t r;
    ASSERT_TRUE(dm_msb_eval(files, 2, "src/App/App.csproj", &r));
    ASSERT_TRUE(dm_has_using(&r, 'n', "System"));       /* ImplicitUsings: the SDK default set */
    ASSERT_TRUE(dm_has_using(&r, 'n', "Acme.Extra"));   /* <Using Include> */
    ASSERT_TRUE(dm_has_using(&r, 'n', "From.Props"));   /* Directory.Build.props, condition true */
    ASSERT_FALSE(dm_has_using(&r, 'n', "System.Linq")); /* <Using Remove> */
    ASSERT_FALSE(dm_has_using(&r, 'n', "Never.Here"));  /* condition false */
    /* Static="true" names a type and Alias an alias: neither is a namespace */
    ASSERT_FALSE(dm_has_using(&r, 'n', "Acme.Static"));
    ASSERT_TRUE(dm_has_using(&r, 's', "Acme.Static"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Acme.Aliased"));
    ASSERT_TRUE(dm_has_using(&r, 'a', "Acme.Aliased"));
    /* an Include is a ';'-separated list */
    ASSERT_TRUE(dm_has_using(&r, 'n', "Acme.One"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "Acme.Two"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Acme.One; Acme.Two ;;"));
    /* comments, other items and targets are no usings */
    ASSERT_FALSE(dm_has_using(&r, 'n', "In.A.Comment"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Not.A.Using"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "In.A.Target"));
    /* everything was evaluated: nothing is open */
    ASSERT_FALSE(r.open);
    ASSERT_EQ(r.unevaluable, 0);
    ASSERT_EQ(r.outside, 0);
    cbm_msb_result_free(&r);
    PASS();
}

/* <Import>: a path is relative to the importing file; the two directory
 * properties are absolute and are not joined to it again; what the index
 * does not hold is not imported, and is counted. */
TEST(doc_mentions_msbuild_imports) {
    const dm_project_file_t files[] = {
        {"src/Directory.Build.props",
         "<Project>\n"
         "  <PropertyGroup><Eng>$(MSBuildThisFileDirectory)eng/</Eng></PropertyGroup>\n"
         "  <Import Project=\"$(MSBuildThisFileDirectory)eng/ByThisFile.props\" />\n"
         "  <Import Project=\"$(Eng)ByProperty.props\" />\n"
         "  <Import Project=\"eng\\Relative.props\" />\n"
         "  <Import Project=\"..\\Up.props\" />\n"
         "  <Import Project=\"..\\..\\Outside.props\" />\n"
         "  <Import Project=\"/etc/Absolute.props\" />\n"
         "  <Import Project=\"$(NuGetRoot)pkg.props\" />\n"
         "  <Import Project=\"eng/*.props\" />\n"
         "  <Import Project=\"eng/NotThere.props\" />\n"
         "  <Import Project=\"Sdk.props\" Sdk=\"Microsoft.NET.Sdk\" />\n"
         "  <ImportGroup Condition=\"'$(Eng)' == ''\">\n"
         "    <Import Project=\"eng/NotTaken.props\" />\n"
         "  </ImportGroup>\n"
         "</Project>\n"},
        {"src/eng/ByThisFile.props",
         "<Project><ItemGroup><Using Include=\"By.ThisFile\" /></ItemGroup>\n"
         "  <Import Project=\"ByThisFile.props\" />\n" /* itself: taken once */
         "</Project>\n"},
        {"src/eng/ByProperty.props",
         "<Project><ItemGroup><Using Include=\"By.Property\" /></ItemGroup></Project>\n"},
        {"src/eng/Relative.props",
         "<Project><ItemGroup><Using Include=\"By.Relative\" /></ItemGroup></Project>\n"},
        {"src/eng/NotTaken.props",
         "<Project><ItemGroup><Using Include=\"Not.Taken\" /></ItemGroup></Project>\n"},
        {"Up.props", "<Project><ItemGroup><Using Include=\"By.Up\" /></ItemGroup></Project>\n"},
        {"src/App/App.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                               "  <Import Project=\"$(MSBuildProjectDirectory)\\Local.props\" />\n"
                               "</Project>\n"},
        {"src/App/Local.props",
         "<Project><ItemGroup><Using Include=\"By.ProjectDir\" /></ItemGroup></Project>\n"},
    };
    cbm_msb_result_t r;
    ASSERT_TRUE(dm_msb_eval(files, 8, "src/App/App.csproj", &r));
    ASSERT_TRUE(dm_has_using(&r, 'n', "By.ThisFile"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "By.Property"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "By.Relative"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "By.Up"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "By.ProjectDir"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Not.Taken")); /* its group's condition is false */
    /* the path through an unknown property and the wildcard */
    ASSERT_EQ(r.unevaluable, 2);
    /* above the repository, absolute, and not in the index */
    ASSERT_EQ(r.outside, 3);
    ASSERT_FALSE(r.open); /* an import that cannot be followed leaves nothing open */
    cbm_msb_result_free(&r);
    PASS();
}

/* One condition over fixed properties: the usings it lets through. */
static bool dm_cond_using(const char *cond, cbm_msb_result_t *out) {
    char xml[1024];
    snprintf(xml, sizeof(xml),
             "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
             "  <PropertyGroup>\n"
             "    <A>x</A><B>x</B><C>y</C><Yes>true</Yes><Empty></Empty>\n"
             "    <Spaced> x </Spaced><One>1</One>\n"
             "  </PropertyGroup>\n"
             "  <ItemGroup Condition=\"%s\">\n"
             "    <Using Include=\"Under.Condition\" />\n"
             "  </ItemGroup>\n"
             "</Project>\n",
             cond);
    const dm_project_file_t files[] = {{"P.csproj", xml}};
    return dm_msb_eval(files, 1, "P.csproj", out);
}

/* A condition is true, false, or not known. What is not known is never
 * taken for one of the two: the item is not applied, the project's usings
 * are open, and the count says so. */
TEST(doc_mentions_msbuild_conditions) {
    static const struct {
        const char *cond;
        char want; /* T applied, F not applied, U not known */
    } cases[] = {
        {"'$(A)' == 'x'", 'T'},
        {"'$(A)' == 'X'", 'T'}, /* comparison ignores case */
        {"'$(A)' != 'x'", 'F'},
        {"'$(A)' == '$(B)'", 'T'}, /* both sides are expanded */
        {"'$(A)' == '$(C)'", 'F'},
        {"$(A) == x", 'T'}, /* unquoted operands */
        {"'$(A)-$(C)' == 'x-y'", 'T'},
        {"'$(Empty)' == ''", 'T'},
        {"$(Yes)", 'T'},
        {"!$(Yes)", 'F'},
        {"'$(Yes)' == 'on'", 'T'},  /* booleans compare as booleans */
        {"'$(One)' == '1.0'", 'T'}, /* numbers as numbers */
        {"'$(One)' == '2'", 'F'},
        /* `and` binds tighter than `or`, parentheses group */
        {"'e' == 'e' or 'a' == 'b' and 'c' == 'd'", 'T'},
        {"('e' == 'e' or 'a' == 'b') and 'c' == 'd'", 'F'},
        {"'a' == 'a' or ('b' == 'c' and 'd' == 'e')", 'T'},
        {"!('a' == 'b')", 'T'},
        /* not known: a property no file sets ... */
        {"'$(Undefined)' == ''", 'U'},
        {"'$(Undefined)' != 'v'", 'U'},
        /* ... unless the other operand decides it */
        {"'a' == 'b' and '$(Undefined)' == 'v'", 'F'},
        {"'a' == 'a' or '$(Undefined)' == 'v'", 'T'},
        {"'a' == 'a' and '$(Undefined)' == 'v'", 'U'},
        /* property functions, item lists, function calls, an order */
        {"'$(A.StartsWith('x'))' == 'true'", 'U'},
        {"'@(Compile)' == ''", 'U'},
        {"Exists('x.props')", 'U'},
        {"'a' == 'b' and Exists('x.props')", 'F'},
        {"'$(One)' > '0'", 'U'},
        /* white space a property's element was written with */
        {"'$(Spaced)' == 'x'", 'U'},
        {"'$(Spaced)' == 'y'", 'F'},
        /* no condition this reader can parse */
        {"'a' == ", 'U'},
        {"('a' == 'a'", 'U'},
        {"'a' == 'a' 'b'", 'U'},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cbm_msb_result_t r;
        if (!dm_cond_using(cases[i].cond, &r)) {
            printf("  [%s]: no evaluation\n", cases[i].cond);
            FAIL("condition");
        }
        bool applied = dm_has_using(&r, 'n', "Under.Condition");
        char got = applied ? 'T' : ((r.open && r.unevaluable > 0) ? 'U' : 'F');
        bool consistent =
            applied ? (!r.open && r.unevaluable == 0) : (r.open == (r.unevaluable > 0));
        cbm_msb_result_free(&r);
        if (got != cases[i].want || !consistent) {
            printf("  [%s]: %c, want %c%s\n", cases[i].cond, got, cases[i].want,
                   consistent ? "" : " (open and the count disagree)");
            FAIL("condition");
        }
    }
    PASS();
}

/* What an element under an unknown condition could have set is not known
 * afterwards, and neither is anything that reads it. */
TEST(doc_mentions_msbuild_unknown_spreads) {
    const dm_project_file_t files[] = {
        {"Maybe.props", "<Project>\n"
                        "  <PropertyGroup><FromMaybe>v</FromMaybe></PropertyGroup>\n"
                        "  <ItemGroup><Using Include=\"From.Maybe\" /></ItemGroup>\n"
                        "</Project>\n"},
        {"P.csproj",
         "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
         "  <PropertyGroup>\n"
         "    <Mode>plain</Mode>\n"
         "    <Mode Condition=\"'$(Legacy)' == 'true'\">legacy</Mode>\n"
         "    <Sure>yes</Sure>\n"
         "    <ImplicitUsings Condition=\"'$(Mode)' == 'plain'\">enable</ImplicitUsings>\n"
         "  </PropertyGroup>\n"
         "  <Import Project=\"Maybe.props\" Condition=\"'$(Legacy)' == 'true'\" />\n"
         "  <Choose><When Condition=\"'$(Sure)' == 'yes'\">\n"
         "    <PropertyGroup><InChoose>1</InChoose></PropertyGroup>\n"
         "  </When></Choose>\n"
         "  <ItemGroup>\n"
         "    <Using Include=\"Reads.Poisoned\" Condition=\"'$(Mode)' == 'plain'\" />\n"
         "    <Using Include=\"Reads.Maybe\" Condition=\"'$(FromMaybe)' == 'v'\" />\n"
         "    <Using Include=\"Reads.Choose\" Condition=\"'$(InChoose)' == '1'\" />\n"
         "    <Using Include=\"Reads.Sure\" Condition=\"'$(Sure)' == 'yes'\" />\n"
         "    <Using Include=\"$(Mode).Ns\" />\n"
         "  </ItemGroup>\n"
         "</Project>\n"},
    };
    cbm_msb_result_t r;
    ASSERT_TRUE(dm_msb_eval(files, 2, "P.csproj", &r));
    /* Mode is `plain` or `legacy`: nothing that depends on it is applied */
    ASSERT_FALSE(dm_has_using(&r, 'n', "Reads.Poisoned"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "plain.Ns"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "System")); /* ImplicitUsings: not known either */
    /* a file imported under an unknown condition: its properties and usings */
    ASSERT_FALSE(dm_has_using(&r, 'n', "Reads.Maybe"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "From.Maybe"));
    /* a property a <Choose> sets */
    ASSERT_FALSE(dm_has_using(&r, 'n', "Reads.Choose"));
    /* what does not depend on any of it stands */
    ASSERT_TRUE(dm_has_using(&r, 'n', "Reads.Sure"));
    ASSERT_TRUE(r.open);
    ASSERT_GT(r.unevaluable, 5);
    cbm_msb_result_free(&r);
    PASS();
}

/* Growing one group's condition must not copy it once per child import. */
TEST(doc_mentions_msbuild_import_group_blob_growth) {
    enum { IMPORTS = 64, CONDITION_GROWTH = 512 };
    size_t sizes[2] = {0};
    for (int sample = 0; sample < 2; sample++) {
        char xml[4096];
        const char *head = "<Project><ImportGroup Condition=\"";
        size_t used = strlen(head);
        memcpy(xml, head, used);
        size_t condition_len = 256 + (size_t)sample * CONDITION_GROWTH;
        memset(xml + used, 'a', condition_len);
        used += condition_len;
        memcpy(xml + used, "\">", 2);
        used += 2;
        for (int i = 0; i < IMPORTS; i++) {
            const char *child = "<Import Project=\"x.props\"/>";
            size_t n = strlen(child);
            ASSERT_LT(used + n, sizeof(xml));
            memcpy(xml + used, child, n);
            used += n;
        }
        const char *tail = "</ImportGroup></Project>";
        ASSERT_LT(used + strlen(tail), sizeof(xml));
        memcpy(xml + used, tail, strlen(tail) + 1);
        CBMFileResult *r = dm_extract(xml, CBM_LANG_XML, "App.csproj");
        ASSERT_NOT_NULL(r);
        ASSERT_NOT_NULL(r->doc_scope);
        sizes[sample] = strlen(r->doc_scope);
        cbm_free_result(r);
    }
    ASSERT_GTE(sizes[1], sizes[0]);
    ASSERT_LTE(sizes[1] - sizes[0], 2 * CONDITION_GROWTH);
    PASS();
}

TEST(doc_mentions_msbuild_import_group_roundtrip) {
    const dm_project_file_t files[] = {
        {"App.csproj",
         "<Project>"
         "<ImportGroup Condition=\"true\"><Import Project=\"A.props\"/>"
         "<Import Project=\"B.props\" Condition=\"false\"/></ImportGroup>"
         "<ImportGroup Condition=\"false\"><Import Project=\"Skip.props\"/></ImportGroup>"
         "<Import Project=\"B.props\"/></Project>"},
        {"A.props", "<Project><ImportGroup><Import Project=\"Nested.props\"/></ImportGroup>"
                    "<ItemGroup><Using Include=\"From.A\"/></ItemGroup></Project>"},
        {"B.props", "<Project><ItemGroup><Using Include=\"From.B\"/></ItemGroup></Project>"},
        {"Nested.props", "<Project><ImportGroup Condition=\"false\">"
                         "<Import Project=\"Skip.props\"/></ImportGroup>"
                         "<ItemGroup><Using Include=\"From.Nested\"/></ItemGroup></Project>"},
        {"Skip.props", "<Project><ItemGroup><Using Include=\"Not.Taken\"/></ItemGroup></Project>"},
    };
    cbm_msb_result_t r;
    ASSERT_TRUE(dm_msb_eval(files, 5, "App.csproj", &r));
    ASSERT_TRUE(dm_has_using(&r, 'n', "From.A"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "From.B"));
    ASSERT_TRUE(dm_has_using(&r, 'n', "From.Nested"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Not.Taken"));
    ASSERT_EQ(r.count, 3);
    ASSERT_FALSE(r.open);
    cbm_msb_result_free(&r);
    PASS();
}

TEST(doc_mentions_msbuild_legacy_import_blob) {
    cbm_msb_t *m = cbm_msb_new();
    ASSERT_NOT_NULL(m);
    ASSERT_TRUE(cbm_msb_add(m, "App.csproj",
                            "cs1\nP\t\t-\n"
                            "I\t=true\t\t=Take.props\t\n"
                            "I\t=false\t\t=Skip.props\t\n"));
    ASSERT_TRUE(cbm_msb_add(m, "Take.props", "cs1\nP\t\t-\nH\t\nN\t\t=Taken\t\t\t\n"));
    ASSERT_TRUE(cbm_msb_add(m, "Skip.props", "cs1\nP\t\t-\nH\t\nN\t\t=Skipped\t\t\t\n"));
    cbm_msb_result_t r;
    ASSERT_TRUE(cbm_msb_eval(m, "App.csproj", &r));
    ASSERT_TRUE(dm_has_using(&r, 'n', "Taken"));
    ASSERT_FALSE(dm_has_using(&r, 'n', "Skipped"));
    ASSERT_EQ(r.count, 1);
    ASSERT_FALSE(r.open);
    cbm_msb_result_free(&r);
    cbm_msb_free(m);
    PASS();
}

TEST(doc_mentions_msbuild_bad_import_group_blob) {
    const char *bad[] = {
        "J\t\t\t=Take.props\t\n",                    /* orphan import */
        "B\t=true\nJ\t\t\t=Take.props\t\n",          /* missing end */
        "B\t=true\nB\t=false\nE\nE\n",               /* nested group */
        "E\n",                                       /* orphan end */
        "B\t=true\nI\t\t\t=Take.props\t\nE\n",       /* wrong child */
        "B\t=true\nJ\t=false\t\t=Take.props\t\nE\n", /* inline group condition */
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char blob[256];
        int n = snprintf(blob, sizeof(blob), "cs1\nP\t\t-\n%s", bad[i]);
        ASSERT_GT(n, 0);
        ASSERT_LT((size_t)n, sizeof(blob));
        cbm_msb_t *m = cbm_msb_new();
        ASSERT_NOT_NULL(m);
        ASSERT_TRUE(cbm_msb_add(m, "App.csproj", blob));
        ASSERT_TRUE(cbm_msb_add(m, "Take.props", "cs1\nP\t\t-\nH\t\nN\t\t=Taken\t\t\t\n"));
        cbm_msb_result_t r;
        ASSERT_TRUE(cbm_msb_eval(m, "App.csproj", &r));
        ASSERT_TRUE(r.open);
        ASSERT_GT(r.unevaluable, 0);
        ASSERT_EQ(r.count, 0);
        cbm_msb_result_free(&r);
        cbm_msb_free(m);
    }
    PASS();
}

/* What the scanner makes of a project file. */
TEST(doc_mentions_msbuild_blob) {
    CBMFileResult *r = dm_extract("<Project Sdk=\"Microsoft.NET.Sdk/8.0\">\n"
                                  "  <PropertyGroup Condition=\"'$(A)' &lt; 'b'\">\n"
                                  "    <Plain>one\ttwo</Plain>\n"
                                  "    <Cdata><![CDATA[<raw> & ]]>tail</Cdata>\n"
                                  "    <Nested><Inner /></Nested>\n"
                                  "    <Back>a\\b&#65;&#x42;</Back>\n"
                                  "  </PropertyGroup>\n"
                                  "  <ItemGroup><Reference Include=\"X\" /></ItemGroup>\n"
                                  "  <ItemGroup>\n"
                                  "    <Using Include=\"N\"><Alias>A</Alias></Using>\n"
                                  "    <Using Update=\"N\" Static=\"true\" />\n"
                                  "  </ItemGroup>\n"
                                  "</Project>\n",
                                  CBM_LANG_XML, "src/App.csproj");
    ASSERT_NOT_NULL(r);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT_TRUE(cbm_msb_is_project_scope(s));
    ASSERT(strncmp(s, "cs1\nP\t=Microsoft.NET.Sdk/8.0\t-\n", 30) == 0);
    ASSERT_NOT_NULL(strstr(s, "\nG\t='$(A)' < 'b'\n"));       /* entities are decoded */
    ASSERT_NOT_NULL(strstr(s, "\nV\t\tPlain\t=one\\ttwo\n")); /* a tab is escaped */
    ASSERT_NOT_NULL(strstr(s, "\nV\t\tCdata\t=<raw> & tail\n"));
    ASSERT_NOT_NULL(strstr(s, "\nV\t\tNested\t?\n")); /* no plain text */
    ASSERT_NOT_NULL(strstr(s, "\nV\t\tBack\t=a\\\\bAB\n"));
    ASSERT_NULL(strstr(s, "Reference"));    /* an item group without a <Using> is not kept */
    ASSERT_NOT_NULL(strstr(s, "\nY\nY\n")); /* metadata elements, Update: not evaluated */
    ASSERT_NULL(strstr(s, "\nN\t"));
    /* the persisted form is the blob itself: it has no line numbers */
    char *portable = cbm_doclink_portable_scope(s);
    ASSERT_NOT_NULL(portable);
    ASSERT_STR_EQ(portable, s);
    cbm_free(CBM_MEM_CLASS_OTHER, portable);
    cbm_free_result(r);

    /* XML that is no MSBuild project has no scope: by its name ... */
    r = dm_extract("<Project><ItemGroup><Using Include=\"X\" /></ItemGroup></Project>\n",
                   CBM_LANG_XML, "src/app.config.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    /* ... or, under a project file's name, by its root element */
    r = dm_extract("<Configuration><Using Include=\"X\" /></Configuration>\n", CBM_LANG_XML,
                   "src/Settings.props");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    /* Directory.Build.props and .targets files are project files */
    r = dm_extract("<Project><ItemGroup><Using Include=\"X\" /></ItemGroup></Project>\n",
                   CBM_LANG_XML, "Directory.Build.targets");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nN\t\t=X\t\t\t\n"));
    cbm_free_result(r);
    /* a *.csproj that cannot be read is still a project: its blob says so */
    r = dm_extract("<Project><PropertyGroup><A>1</PropertyGroup>\n", CBM_LANG_XML, "Bad.csproj");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_STR_EQ(r->doc_scope, "cs1\nP\t\t!\n");
    cbm_free_result(r);
    r = dm_extract("not xml at all", CBM_LANG_XML, "Worse.CSPROJ");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_STR_EQ(r->doc_scope, "cs1\nP\t\t!\n");
    cbm_free_result(r);
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
        "        /// <summary>Explicit implementations are not addressable, and an\n"
        "        /// interface's members are not the class's: <see cref=\"Ping\"/>,\n"
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
        "    /// <see cref=\"IList.IndexOf\"/> <see cref=\"OnlyGeneric\"/> <see "
        "cref=\"Conv.To{T}\"/>\n"
        "    /// <see cref=\"Pair.Left\"/> <see cref=\"Pair{T}.Left\"/> <see "
        "cref=\"Pair{T}.Right\"/>\n"
        "    /// <see cref=\"Conv\"/> <see cref=\"Conv(int)\"/> <see cref=\"Derived.Conv\"/>\n"
        "    /// <see cref=\"Conv.Many(string, int[])\"/> <see cref=\"OnlyGeneric{T}\"/>\n"
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

    /* An explicit interface implementation is not addressable by name, and a
     * class does not find the members of an interface it implements (the
     * compiler looks up no inherited member in a cref): `Ping` is the class of
     * that name in the namespace, `Ping(int)` a constructor it does not have. */
    dm_edge(db, "Widgets.Pinger.Other", "Widgets.Ping", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_edge(db, "Widgets.Pinger.Other", "Widgets.IPinger.Ping", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_edge(db, "Widgets.Pinger.Other", "Widgets.Pinger.Ping", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, widgets, "Ping(int)", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

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
    /* ... and a generic type never stands in for a name written without type
     * arguments, not even when no arity-0 type of that name is in scope: the
     * compiler binds `OnlyGeneric` to nothing. Only `OnlyGeneric{T}` names it. */
    dm_row(db, arity, "OnlyGeneric", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_edge(db, "Arity.Uses", "GenericLists.OnlyGeneric", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    ASSERT_NOT_NULL(strstr(props, "\"count\":1")); /* OnlyGeneric{T} only */
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

/* A reference written in a file's own doc has the file's File node as its
 * source. The resolver finds that node with the pipeline's one lookup; this
 * holds it against the graph the pipeline published: the lookup must name the
 * File node of exactly that file. (No language of this suite has file-level
 * docs; the language legs test the references themselves.) */
TEST(doc_mentions_file_node_lookup) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_fn_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    dm_write_resolver_fixture(tmp);
    char db[512];
    snprintf(db, sizeof(db), "%s/res.db", tmp);
    char *project = NULL;
    ASSERT_EQ(dm_index(tmp, db, &project), 0);
    ASSERT_NOT_NULL(project);
    cbm_gbuf_t *gb = cbm_gbuf_new(project, tmp);
    ASSERT_NOT_NULL(gb);
    ASSERT_EQ(cbm_gbuf_load_from_db(gb, db, project), 0);
    const char *paths[] = {"src/Lib/Widgets.cs", "src/Other/tests/Half.cs", "src/Lib/Lib.csproj"};
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        const cbm_gbuf_node_t *n = cbm_pipeline_file_node(gb, project, paths[i]);
        ASSERT_NOT_NULL(n);
        ASSERT_STR_EQ(n->label, "File");
        ASSERT_STR_EQ(n->file_path, paths[i]);
    }
    ASSERT_NULL(cbm_pipeline_file_node(gb, project, "src/Lib/NoSuchFile.cs"));
    cbm_gbuf_free(gb);
    free(project);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ── publication: index_status, delete_project, older databases ──── */

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

/* Each preview test starts from a real private index. Restore process state and
 * remove the fixture even when a deliberately RED check returns failure. */
static int dm_preview_fixture(int (*check)(const char *db, const char *project)) {
    char tmp[256] = "/tmp/cbm_dm_preview_XXXXXX";
    if (!cbm_mkdtemp(tmp)) {
        return 1;
    }
    char repo[400], cache_dir[400], db[1024];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(cache_dir, sizeof(cache_dir), "%s/cache", tmp);
    dm_write_resolver_fixture(repo);
    cbm_mkdir_p(cache_dir, 0700);
    char *project = cbm_project_name_from_path(repo);
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    if (!project || (saved && !saved_copy)) {
        free(project);
        free(saved_copy);
        th_rmtree(tmp);
        return 1;
    }
    snprintf(db, sizeof(db), "%s/%s.db", cache_dir, project);
    cbm_setenv("CBM_CACHE_DIR", cache_dir, 1);
    int rc = dm_index(repo, db, NULL);
    if (rc == 0) {
        rc = check(db, project);
    }
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    dm_unlink_db(db);
    free(project);
    th_rmtree(tmp);
    return rc;
}

/* Return the actual MCP envelope, including structuredContent for JSON, so a
 * test exercises both transport boundaries instead of a formatter in isolation. */
static yyjson_doc *dm_preview_status(const char *project, bool json) {
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        return NULL;
    }
    char args[1200];
    snprintf(args, sizeof(args), "{\"project\":\"%s\",\"diagnostics\":\"full\"%s}", project,
             json ? ",\"format\":\"json\"" : "");
    char *response = cbm_mcp_handle_tool(srv, "index_status", args);
    yyjson_doc *out = response ? yyjson_read(response, strlen(response), 0) : NULL;
    free(response);
    cbm_mcp_server_free(srv);
    return out;
}

static const char *dm_preview_text(yyjson_doc *envelope) {
    yyjson_val *root = envelope ? yyjson_doc_get_root(envelope) : NULL;
    yyjson_val *content = yyjson_obj_get(root, "content");
    return yyjson_get_str(yyjson_obj_get(yyjson_arr_get(content, 0), "text"));
}

static yyjson_val *dm_preview_report(yyjson_doc *envelope) {
    yyjson_val *root = envelope ? yyjson_doc_get_root(envelope) : NULL;
    return yyjson_obj_get(yyjson_obj_get(root, "structuredContent"), "doc_links");
}

static bool dm_preview_status_is(yyjson_doc *envelope, const char *status) {
    const char *actual = yyjson_get_str(yyjson_obj_get(dm_preview_report(envelope), "status"));
    return actual && strcmp(actual, status) == 0;
}

static bool dm_preview_json_consistent(yyjson_doc *envelope) {
    const char *text = dm_preview_text(envelope);
    yyjson_doc *payload = text ? yyjson_read(text, strlen(text), 0) : NULL;
    yyjson_val *root = envelope ? yyjson_doc_get_root(envelope) : NULL;
    yyjson_val *structured = yyjson_obj_get(root, "structuredContent");
    char *a = structured ? yyjson_val_write(structured, 0, NULL) : NULL;
    char *b = payload ? yyjson_val_write(yyjson_doc_get_root(payload), 0, NULL) : NULL;
    bool correct = a && b && strcmp(a, b) == 0;
    free(a);
    free(b);
    yyjson_doc_free(payload);
    return correct;
}

static bool dm_preview_compact_status(yyjson_doc *env, const char *status) {
    const char *text = dm_preview_text(env);
    const char *report = text ? strstr(text, "doc_links:\n") : NULL;
    char expected[50];
    snprintf(expected, sizeof(expected), "\n  status: %s\n", status);
    return report && strstr(report, expected);
}

static bool dm_preview_compact_metadata(yyjson_doc *env, size_t sample, const char *field,
                                        size_t original, size_t included, bool truncated,
                                        bool escaped) {
    const char *text = dm_preview_text(env);
    const char *table = text ? strstr(text, "\n  samples_preview:") : NULL;
    const char *columns = table ? strstr(table, "(cols: sample_index field original_bytes "
                                                "included_source_bytes truncated escaped)\n")
                                : NULL;
    const char *end = table ? strchr(table + 1, '\n') : NULL;
    char row[220];
    snprintf(row, sizeof(row), "\n    %zu %s %zu %zu %s %s\n", sample, field, original, included,
             truncated ? "true" : "false", escaped ? "true" : "false");
    return columns && end && columns < end && strstr(end, row);
}

static yyjson_val *dm_preview_metadata(yyjson_val *report, size_t sample, const char *field) {
    yyjson_val *entries = yyjson_obj_get(report, "samples_preview");
    for (size_t i = 0; i < yyjson_arr_size(entries); i++) {
        yyjson_val *entry = yyjson_arr_get(entries, i);
        const char *name = yyjson_get_str(yyjson_obj_get(entry, "field"));
        yyjson_val *index = yyjson_obj_get(entry, "sample_index");
        if (name && yyjson_is_uint(index) && yyjson_get_uint(index) == sample &&
            strcmp(name, field) == 0) {
            return entry;
        }
    }
    return NULL;
}

static bool dm_preview_metadata_matches(yyjson_val *entry, size_t original, size_t included,
                                        bool truncated, bool escaped) {
    yyjson_val *orig = yyjson_obj_get(entry, "original_bytes");
    yyjson_val *shown = yyjson_obj_get(entry, "included_source_bytes");
    yyjson_val *cut = yyjson_obj_get(entry, "truncated");
    yyjson_val *quoted = yyjson_obj_get(entry, "escaped");
    return yyjson_is_uint(orig) && yyjson_get_uint(orig) == original && yyjson_is_uint(shown) &&
           yyjson_get_uint(shown) == included && yyjson_is_bool(cut) &&
           yyjson_get_bool(cut) == truncated && yyjson_is_bool(quoted) &&
           yyjson_get_bool(quoted) == escaped;
}

static bool dm_preview_full_row(cbm_store_t *store, const char *project,
                                const cbm_doc_link_row_t *expected) {
    cbm_doc_link_row_t *rows = NULL;
    int count = 0;
    bool present = false;
    bool correct =
        cbm_store_doc_links_get(store, project, &rows, &count, &present) == CBM_STORE_OK &&
        present && count == 1;
    if (correct) {
        correct = rows[0].line == expected->line &&
                  strcmp(rows[0].rel_path, expected->rel_path) == 0 &&
                  strcmp(rows[0].syntax, expected->syntax) == 0 &&
                  strcmp(rows[0].raw, expected->raw) == 0 &&
                  strcmp(rows[0].reason, expected->reason) == 0;
    }
    cbm_store_free_doc_links(rows, count);
    return correct;
}

static int dm_preview_bounds_checks(const char *db, const char *project) {
    cbm_store_t *store = cbm_store_open_path(db);
    if (!store) {
        return 1;
    }
    const char *fields[] = {"rel_path", "syntax", "raw", "reason"};
    const size_t limits[] = {1024, 128, 1024, 128};
    const cbm_doc_link_row_t ordinary = {.rel_path = "src/Normal.cs",
                                         .line = 7,
                                         .syntax = "see",
                                         .raw = "Gone",
                                         .reason = "missing"};
    bool shape = cbm_store_doc_links_replace(store, project, &ordinary, 1) == CBM_STORE_OK;
    for (int format = 0; format < 2; format++) {
        yyjson_doc *env = dm_preview_status(project, format != 0);
        const char *text = dm_preview_text(env);
        if (format) {
            yyjson_val *report = dm_preview_report(env);
            yyjson_val *samples = yyjson_obj_get(report, "samples");
            shape = dm_preview_status_is(env, "ok") && dm_preview_json_consistent(env) &&
                    yyjson_arr_size(samples) == 1 &&
                    yyjson_obj_size(yyjson_arr_get(samples, 0)) == 5 &&
                    !yyjson_obj_get(report, "samples_preview") &&
                    !yyjson_obj_get(report, "samples_preview_note") && shape;
        } else {
            shape = text &&
                    strstr(text, "samples: 1  (cols: rel_path line syntax raw reason)\n"
                                 "    src/Normal.cs 7 see Gone missing\n") &&
                    !strstr(text, "samples_preview") && shape;
        }
        yyjson_doc_free(env);
    }
    bool bounded = true, copies = true, preserved = true;
    for (int size = 0; size < 3; size++) {
        size_t length = size == 1 ? 65536 : 8192;
        bool escaped = size == 2;
        char *values[4] = {0};
        bool allocated = true;
        for (int field = 0; field < 4; field++) {
            values[field] = malloc(length + 1);
            if (!values[field]) {
                allocated = false;
                break;
            }
            memset(values[field], escaped ? '\\' : 'a' + field, length);
            values[field][length] = '\0';
        }
        if (!allocated) {
            for (int field = 0; field < 4; field++) {
                free(values[field]);
            }
            cbm_store_close(store);
            return 1;
        }
        cbm_doc_link_row_t row = {.rel_path = values[0],
                                  .line = 42,
                                  .syntax = values[1],
                                  .raw = values[2],
                                  .reason = values[3]};
        preserved =
            cbm_store_doc_links_replace(store, project, &row, 1) == CBM_STORE_OK && preserved;
        for (int format = 0; format < 2; format++) {
            cbm_store_doc_links_test_sample_stats_reset();
            yyjson_doc *env = dm_preview_status(project, format != 0);
            cbm_doc_links_sample_test_stats_t stats = {0};
            cbm_store_doc_links_test_sample_stats(&stats);
            copies = stats.field_copies == 4 && stats.copied_bytes <= 2320 &&
                     stats.requested_bytes == stats.copied_bytes &&
                     stats.max_request_bytes <= 1028 && copies;
            const char *text = dm_preview_text(env);
            if (format) {
                yyjson_val *report = dm_preview_report(env);
                yyjson_val *samples = yyjson_obj_get(report, "samples");
                yyjson_val *sample = yyjson_arr_get(samples, 0);
                yyjson_val *metadata = yyjson_obj_get(report, "samples_preview");
                const char *note = yyjson_get_str(yyjson_obj_get(report, "samples_preview_note"));
                bounded = dm_preview_status_is(env, "ok") && dm_preview_json_consistent(env) &&
                          yyjson_arr_size(samples) == 1 && yyjson_obj_size(sample) == 5 &&
                          yyjson_arr_size(metadata) == 4 && note &&
                          strstr(note, "doc_link_unresolved") && bounded;
                for (int field = 0; field < 4; field++) {
                    yyjson_val *value = yyjson_obj_get(sample, fields[field]);
                    const char *actual = yyjson_get_str(value);
                    bounded = actual && yyjson_get_len(value) == limits[field] &&
                              memcmp(actual, values[field], limits[field]) == 0 &&
                              dm_preview_metadata_matches(
                                  dm_preview_metadata(report, 0, fields[field]), length,
                                  limits[field] / (escaped ? 2 : 1), true, escaped) &&
                              bounded;
                }
                char *encoded = samples ? yyjson_val_write(samples, 0, NULL) : NULL;
                bounded = encoded && strlen(encoded) <= 5000 && bounded;
                free(encoded);
            } else {
                const char *table = text ? strstr(text, "\n  samples:") : NULL;
                const char *metadata = table ? strstr(table, "\n  samples_preview:") : NULL;
                size_t bytes = table ? (metadata ? (size_t)(metadata - table) : strlen(table)) : 0;
                bounded = table && metadata && bytes <= 5000 &&
                          dm_preview_compact_status(env, "ok") &&
                          strstr(metadata, "\n  samples_preview_note:") &&
                          strstr(table, "(cols: rel_path line syntax raw reason)") && bounded;
                for (int field = 0; field < 4; field++) {
                    bounded = dm_preview_compact_metadata(env, 0, fields[field], length,
                                                          limits[field] / (escaped ? 2 : 1), true,
                                                          escaped) &&
                              bounded;
                }
            }
            fprintf(stderr,
                    "doc preview bounds source=%llu escaped=%d json=%d copies=%llu copied=%llu "
                    "requested=%llu "
                    "max_request=%llu bounded=%d copy_bound=%d\n",
                    (unsigned long long)length, escaped, format,
                    (unsigned long long)stats.field_copies, (unsigned long long)stats.copied_bytes,
                    (unsigned long long)stats.requested_bytes,
                    (unsigned long long)stats.max_request_bytes, bounded, copies);
            yyjson_doc_free(env);
        }
        preserved = dm_preview_full_row(store, project, &row) && preserved;
        for (int field = 0; field < 4; field++) {
            free(values[field]);
        }
    }
    cbm_store_close(store);
    fprintf(stderr, "doc preview shape=%d full_rows_preserved=%d\n", shape, preserved);
    return !(shape && bounded && copies && preserved);
}

TEST(doc_mentions_index_status_preview_bounds) {
    int result = dm_preview_fixture(dm_preview_bounds_checks);
    ASSERT_EQ(result, 0);
    PASS();
}

/* Check binary storage through SQLite, without asking the legacy C-string
 * getter to promise NUL support. All non-NUL cases also exercise that getter. */
static bool dm_preview_raw_bytes(const char *db, const char *project, const void *bytes,
                                 size_t length, bool write) {
    sqlite3 *sql = NULL;
    sqlite3_stmt *stmt = NULL;
    bool correct = sqlite3_open_v2(db, &sql, write ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY,
                                   NULL) == SQLITE_OK;
    const char *query = write
                            ? "UPDATE doc_link_unresolved SET raw=?2 WHERE project=?1;"
                            : "SELECT CAST(raw AS BLOB) FROM doc_link_unresolved WHERE project=?1;";
    correct = correct && sqlite3_prepare_v2(sql, query, -1, &stmt, NULL) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    if (correct && write) {
        correct = sqlite3_bind_text(stmt, 2, bytes, (int)length, SQLITE_TRANSIENT) == SQLITE_OK &&
                  sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(sql) == 1;
    } else if (correct) {
        correct = sqlite3_step(stmt) == SQLITE_ROW &&
                  sqlite3_column_bytes(stmt, 0) == (int)length &&
                  (length == 0 || memcmp(sqlite3_column_blob(stmt, 0), bytes, length) == 0) &&
                  sqlite3_step(stmt) == SQLITE_DONE;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(sql);
    return correct;
}

/* Expected preview strings are independent, literal test data. This helper
 * only applies the compact table's quote/backslash framing to an expectation. */
static bool dm_preview_compact_raw(yyjson_doc *env, const char *expected) {
    const char *text = dm_preview_text(env);
    const char *table =
        text ? strstr(text, "samples: 1  (cols: rel_path line syntax raw reason)\n") : NULL;
    const char *row = table ? strchr(table, '\n') + 1 : NULL;
    const char *end = row ? strchr(row, '\n') : NULL;
    if (!row || !end || (size_t)(end - row) > 2200) {
        return false;
    }
    char unquoted[2200], quoted[2200];
    snprintf(unquoted, sizeof(unquoted), "    src/Text.cs 9 see %s missing", expected);
    size_t used = (size_t)snprintf(quoted, sizeof(quoted), "    src/Text.cs 9 see \"");
    for (const unsigned char *p = (const unsigned char *)expected; *p; p++) {
        if (*p == '\\' || *p == '"') {
            quoted[used++] = '\\';
        }
        quoted[used++] = (char)*p;
    }
    snprintf(quoted + used, sizeof(quoted) - used, "\" missing");
    size_t n = (size_t)(end - row);
    return (strlen(unquoted) == n && memcmp(row, unquoted, n) == 0) ||
           (strlen(quoted) == n && memcmp(row, quoted, n) == 0);
}

static bool dm_preview_text_case(cbm_store_t *store, const char *db, const char *project,
                                 const char *name, const char *source, size_t length,
                                 const char *expected, size_t included, bool escaped) {
    cbm_doc_link_row_t row = {
        .rel_path = "src/Text.cs", .line = 9, .syntax = "see", .raw = source, .reason = "missing"};
    bool correct = cbm_store_doc_links_replace(store, project, &row, 1) == CBM_STORE_OK &&
                   dm_preview_raw_bytes(db, project, source, length, true);
    bool truncated = included < length;
    for (int format = 0; format < 2; format++) {
        yyjson_doc *env = dm_preview_status(project, format != 0);
        bool rendered;
        if (format) {
            yyjson_val *report = dm_preview_report(env);
            yyjson_val *samples = yyjson_obj_get(report, "samples");
            yyjson_val *sample = yyjson_arr_get(samples, 0);
            yyjson_val *raw = yyjson_obj_get(sample, "raw");
            const char *actual = yyjson_get_str(raw);
            yyjson_val *metadata = yyjson_obj_get(report, "samples_preview");
            rendered = dm_preview_status_is(env, "ok") && dm_preview_json_consistent(env) &&
                       yyjson_arr_size(samples) == 1 && yyjson_obj_size(sample) == 5 && actual &&
                       yyjson_get_len(raw) == strlen(expected) && strcmp(actual, expected) == 0;
            if (truncated || escaped) {
                const char *note = yyjson_get_str(yyjson_obj_get(report, "samples_preview_note"));
                rendered = rendered && yyjson_arr_size(metadata) == 1 && note &&
                           strstr(note, "doc_link_unresolved") &&
                           dm_preview_metadata_matches(dm_preview_metadata(report, 0, "raw"),
                                                       length, included, truncated, escaped);
            } else {
                rendered = rendered && !metadata && !yyjson_obj_get(report, "samples_preview_note");
            }
        } else {
            rendered =
                dm_preview_compact_raw(env, expected) && dm_preview_compact_status(env, "ok");
            const char *text = dm_preview_text(env);
            if (truncated || escaped) {
                rendered = dm_preview_compact_metadata(env, 0, "raw", length, included, truncated,
                                                       escaped) &&
                           text && strstr(text, "\n  samples_preview_note:") && rendered;
            } else {
                rendered = text && !strstr(text, "samples_preview") && rendered;
            }
        }
        correct = rendered && correct;
        fprintf(stderr, "doc preview text case=%s json=%d rendered=%d\n", name, format, rendered);
        yyjson_doc_free(env);
    }
    correct = dm_preview_raw_bytes(db, project, source, length, false) && correct;
    if (!memchr(source, 0, length)) {
        correct = dm_preview_full_row(store, project, &row) && correct;
    }
    return correct;
}

static int dm_preview_text_checks(const char *db, const char *project) {
    cbm_store_t *store = cbm_store_open_path(db);
    if (!store) {
        return 1;
    }
    const struct {
        const char *name;
        const char *source;
        size_t length;
        const char *expected;
        bool escaped;
    } cases[] = {
        {"ordinary_utf8", "A\xC3\xA9\xE2\x82\xAC\xF0\x9F\x99\x82Z", 11,
         "A\xC3\xA9\xE2\x82\xAC\xF0\x9F\x99\x82Z", false},
        {"c0_del", "A\x01\t\n\r\x1F\x7FZ", 8,
         "A\\u{0001}\\u{0009}\\u{000A}\\u{000D}\\u{001F}\\u{007F}Z", true},
        {"nul", "A\0B", 3, "A\\u{0000}B", true},
        {"literal_escapes", "\\u{000A}\\xFF", 12, "\\\\u{000A}\\\\xFF", true},
        {"reserved_bytes", "@bytes:AA", 9, "\\u{0040}bytes:AA", true},
        {"reserved_utf8", "@utf8:AA", 8, "\\u{0040}utf8:AA", true},
        {"middle_prefix", "A@utf8:AA", 9, "A@utf8:AA", false},
        {"malformed", "\xC0\xAF\xED\xA0\x80\xF4\x90\x80\x80\x80\xC2", 11,
         "\\xC0\\xAF\\xED\\xA0\\x80\\xF4\\x90\\x80\\x80\\x80\\xC2", true},
        {"format_controls",
         "\xE2\x80\x8B\xE2\x80\x8F\xE2\x80\xAA\xE2\x80\xAE"
         "\xE2\x81\xA6\xE2\x81\xA9\xF3\xA0\x80\x80\xF3\xA0\x81\xBF",
         26, "\\u{200B}\\u{200F}\\u{202A}\\u{202E}\\u{2066}\\u{2069}\\u{E0000}\\u{E007F}", true},
        {"adjacent_unicode",
         "\xE2\x80\x8A\xE2\x80\x90\xE2\x80\xA9\xE2\x80\xAF"
         "\xE2\x81\xA5\xE2\x81\xAA\xF3\x9F\xBF\xBF\xF3\xA0\x82\x80",
         26,
         "\xE2\x80\x8A\xE2\x80\x90\xE2\x80\xA9\xE2\x80\xAF"
         "\xE2\x81\xA5\xE2\x81\xAA\xF3\x9F\xBF\xBF\xF3\xA0\x82\x80",
         false},
    };
    bool correct = true;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        correct = dm_preview_text_case(store, db, project, cases[i].name, cases[i].source,
                                       cases[i].length, cases[i].expected, cases[i].length,
                                       cases[i].escaped) &&
                  correct;
    }
    /* One expected token must either fit entirely or contribute no source
     * bytes. The suffix guarantees truncation even for the exactly-fit case. */
    const struct {
        const char *name, *source, *expected;
        size_t source_bytes, display_bytes;
        bool escape;
    } tokens[] = {
        {"utf8_2", "\xC3\xA9", "\xC3\xA9", 2, 2, false},
        {"utf8_3", "\xE2\x82\xAC", "\xE2\x82\xAC", 3, 3, false},
        {"utf8_4", "\xF0\x9F\x99\x82", "\xF0\x9F\x99\x82", 4, 4, false},
        {"newline", "\n", "\\u{000A}", 1, 8, true},
        {"backslash", "\\", "\\\\", 1, 2, true},
        {"invalid", "\xFF", "\\xFF", 1, 4, true},
        {"tag", "\xF3\xA0\x80\x81", "\\u{E0001}", 4, 9, true},
    };
    for (size_t i = 0; i < sizeof(tokens) / sizeof(tokens[0]); i++) {
        for (int fit = 0; fit < 2; fit++) {
            char source[1100], expected[1100], name[60];
            size_t prefix = fit ? 1024 - tokens[i].display_bytes : 1023;
            memset(source, 'p', prefix);
            memcpy(source + prefix, tokens[i].source, tokens[i].source_bytes);
            memcpy(source + prefix + tokens[i].source_bytes, "TAIL", 5);
            memset(expected, 'p', prefix);
            size_t shown = prefix;
            if (fit) {
                memcpy(expected + shown, tokens[i].expected, tokens[i].display_bytes);
                shown += tokens[i].display_bytes;
            }
            expected[shown] = '\0';
            snprintf(name, sizeof(name), "%s_%s", tokens[i].name, fit ? "fits" : "stops");
            correct = dm_preview_text_case(store, db, project, name, source,
                                           prefix + tokens[i].source_bytes + 4, expected,
                                           prefix + (fit ? tokens[i].source_bytes : 0),
                                           fit && tokens[i].escape) &&
                      correct;
        }
    }
    cbm_store_close(store);
    return !correct;
}

TEST(doc_mentions_index_status_preview_text) {
    int result = dm_preview_fixture(dm_preview_text_checks);
    ASSERT_EQ(result, 0);
    PASS();
}

static int dm_preview_order_checks(const char *db, const char *project) {
    cbm_store_t *store = cbm_store_open_path(db);
    if (!store) {
        return 1;
    }
    cbm_doc_link_row_t rows[52] = {0};
    char paths[51][40], raw[1026];
    memset(raw, 'r', sizeof(raw) - 1);
    raw[sizeof(raw) - 1] = '\0';
    /* The marker sorts first and consumes one of the original SQL LIMIT 50.
     * Insert references backwards, so a storage-order accident cannot pass. */
    rows[0] =
        (cbm_doc_link_row_t){.rel_path = "", .line = 0, .syntax = "", .raw = "", .reason = "error"};
    for (int i = 0; i < 51; i++) {
        snprintf(paths[i], sizeof(paths[i]), "src/Case%03d.cs", 50 - i);
        rows[i + 1] = (cbm_doc_link_row_t){
            .rel_path = paths[i], .line = 5, .syntax = "see", .raw = raw, .reason = "missing"};
    }
    bool correct = cbm_store_doc_links_replace(store, project, rows, 52) == CBM_STORE_OK;
    for (int format = 0; format < 2; format++) {
        cbm_store_doc_links_test_sample_stats_reset();
        yyjson_doc *env = dm_preview_status(project, format != 0);
        cbm_doc_links_sample_test_stats_t stats = {0};
        cbm_store_doc_links_test_sample_stats(&stats);
        bool ordered = stats.field_copies >= 196 && stats.field_copies <= 200 &&
                       stats.copied_bytes <= 116000 &&
                       stats.requested_bytes == stats.copied_bytes &&
                       stats.max_request_bytes <= 1028;
        if (format) {
            yyjson_val *report = dm_preview_report(env);
            yyjson_val *samples = yyjson_obj_get(report, "samples");
            ordered = dm_preview_status_is(env, "error") && dm_preview_json_consistent(env) &&
                      yyjson_arr_size(samples) == 49 &&
                      yyjson_arr_size(yyjson_obj_get(report, "samples_preview")) == 49 && ordered;
            for (int i = 0; i < 49; i++) {
                char path[40];
                snprintf(path, sizeof(path), "src/Case%03d.cs", i);
                yyjson_val *sample = yyjson_arr_get(samples, (size_t)i);
                const char *actual = yyjson_get_str(yyjson_obj_get(sample, "rel_path"));
                ordered = actual && strcmp(actual, path) == 0 && yyjson_obj_size(sample) == 5 &&
                          yyjson_get_len(yyjson_obj_get(sample, "raw")) == 1024 &&
                          dm_preview_metadata_matches(dm_preview_metadata(report, (size_t)i, "raw"),
                                                      1025, 1024, true, false) &&
                          ordered;
            }
        } else {
            const char *text = dm_preview_text(env);
            const char *cursor =
                text ? strstr(text, "samples: 49  (cols: rel_path line syntax raw reason)\n")
                     : NULL;
            ordered = cursor && ordered;
            for (int i = 0; i < 49; i++) {
                char prefix[60];
                snprintf(prefix, sizeof(prefix), "\n    src/Case%03d.cs 5 see ", i);
                const char *next = cursor ? strstr(cursor, prefix) : NULL;
                ordered = next && ordered;
                cursor = next ? next + strlen(prefix) : NULL;
            }
            ordered = cursor && !strstr(cursor, "src/Case049.cs") &&
                      !strstr(cursor, "src/Case050.cs") && ordered;
        }
        fprintf(stderr, "doc preview order json=%d copies=%llu copied=%llu ordered=%d\n", format,
                (unsigned long long)stats.field_copies, (unsigned long long)stats.copied_bytes,
                ordered);
        correct = ordered && correct;
        yyjson_doc_free(env);
    }
    cbm_doc_link_row_t *full = NULL;
    int count = 0;
    bool present = false;
    correct = cbm_store_doc_links_get(store, project, &full, &count, &present) == CBM_STORE_OK &&
              present && count == 52 && correct;
    for (int i = 0; i < count; i++) {
        if (full[i].rel_path && full[i].rel_path[0]) {
            correct = strcmp(full[i].raw, raw) == 0 && correct;
        }
    }
    cbm_store_free_doc_links(full, count);
    /* Sort the complete source before projection. These two rows have the same
     * displayed raw prefix, but the longer original sorts first (aa before z). */
    char first[1030], second[1029];
    memset(first, 't', 1027);
    memcpy(first + 1027, "aa", 3);
    memset(second, 't', 1027);
    memcpy(second + 1027, "z", 2);
    cbm_doc_link_row_t ties[] = {
        {.rel_path = "src/Tie.cs", .line = 2, .syntax = "see", .raw = second, .reason = "missing"},
        {.rel_path = "src/Tie.cs", .line = 2, .syntax = "see", .raw = first, .reason = "missing"},
        {.rel_path = "src/Tie.cs", .line = 1, .syntax = "see", .raw = second, .reason = "missing"},
        {.rel_path = "src/Z.cs", .line = 3, .syntax = "see", .raw = first, .reason = "ambiguous"},
    };
    correct = cbm_store_doc_links_replace(store, project, ties, 4) == CBM_STORE_OK && correct;
    yyjson_doc *env = dm_preview_status(project, true);
    yyjson_val *report = dm_preview_report(env);
    yyjson_val *ordered = yyjson_obj_get(report, "samples");
    const size_t original[] = {1029, 1028, 1029, 1028};
    const int lines[] = {3, 1, 2, 2};
    bool ties_correct = dm_preview_status_is(env, "ok") && dm_preview_json_consistent(env) &&
                        yyjson_arr_size(ordered) == 4;
    for (size_t i = 0; i < 4; i++) {
        yyjson_val *sample = yyjson_arr_get(ordered, i);
        const char *path = yyjson_get_str(yyjson_obj_get(sample, "rel_path"));
        ties_correct = path && strcmp(path, i ? "src/Tie.cs" : "src/Z.cs") == 0 &&
                       yyjson_get_int(yyjson_obj_get(sample, "line")) == lines[i] &&
                       dm_preview_metadata_matches(dm_preview_metadata(report, i, "raw"),
                                                   original[i], 1024, true, false) &&
                       ties_correct;
    }
    fprintf(stderr, "doc preview full_source_order=%d\n", ties_correct);
    correct = ties_correct && correct;
    yyjson_doc_free(env);
    cbm_store_close(store);
    return !correct;
}

TEST(doc_mentions_index_status_preview_order) {
    int result = dm_preview_fixture(dm_preview_order_checks);
    ASSERT_EQ(result, 0);
    PASS();
}

static bool dm_preview_error_result(yyjson_doc *env, bool json) {
    if (json) {
        yyjson_val *report = dm_preview_report(env);
        return dm_preview_status_is(env, "error") && dm_preview_json_consistent(env) &&
               yyjson_arr_size(yyjson_obj_get(report, "samples")) == 0 &&
               yyjson_arr_size(yyjson_obj_get(report, "samples_preview")) == 0 &&
               !yyjson_obj_get(report, "samples_preview_note");
    }
    const char *text = dm_preview_text(env);
    const char *report = text ? strstr(text, "doc_links:\n") : NULL;
    return report && strstr(report, "\n  status: error\n") && !strstr(report, "src/Failure") &&
           !strstr(report, "samples_preview");
}

static bool dm_preview_retry_result(yyjson_doc *env, bool json) {
    if (json) {
        yyjson_val *samples = yyjson_obj_get(dm_preview_report(env), "samples");
        if (!dm_preview_status_is(env, "ok") || !dm_preview_json_consistent(env) ||
            yyjson_arr_size(samples) != 2) {
            return false;
        }
        for (size_t i = 0; i < 2; i++) {
            yyjson_val *row = yyjson_arr_get(samples, i);
            const char *raw = yyjson_get_str(yyjson_obj_get(row, "raw"));
            const char *path = yyjson_get_str(yyjson_obj_get(row, "rel_path"));
            if (yyjson_obj_size(row) != 5 || !raw ||
                strcmp(raw, i ? "Second" : "First\\u{000A}") != 0 || !path ||
                strcmp(path, i ? "src/FailureB.cs" : "src/FailureA.cs") != 0) {
                return false;
            }
        }
        yyjson_val *report = dm_preview_report(env);
        return yyjson_arr_size(yyjson_obj_get(report, "samples_preview")) == 1 &&
               dm_preview_metadata_matches(dm_preview_metadata(report, 0, "raw"), 6, 6, false,
                                           true) &&
               yyjson_get_str(yyjson_obj_get(report, "samples_preview_note"));
    }
    const char *text = dm_preview_text(env);
    return text && dm_preview_compact_status(env, "ok") &&
           dm_preview_compact_metadata(env, 0, "raw", 6, 6, false, true) &&
           strstr(text, "\n  samples_preview_note:") &&
           strstr(text, "samples: 2  (cols: rel_path line syntax raw reason)\n"
                        "    src/FailureA.cs 3 see First\\u{000A} missing\n"
                        "    src/FailureB.cs 4 see Second missing\n");
}

static int dm_preview_failure_checks(const char *db, const char *project) {
    cbm_store_t *store = cbm_store_open_path(db);
    if (!store) {
        return 1;
    }
    cbm_doc_link_row_t rows[] = {
        {.rel_path = "src/FailureA.cs",
         .line = 3,
         .syntax = "see",
         .raw = "First\n",
         .reason = "missing"},
        {.rel_path = "src/FailureB.cs",
         .line = 4,
         .syntax = "see",
         .raw = "Second",
         .reason = "missing"},
    };
    bool correct = cbm_store_doc_links_replace(store, project, rows, 2) == CBM_STORE_OK;
    for (int format = 0; format < 2; format++) {
        yyjson_doc *warm = dm_preview_status(project, format != 0);
        correct = dm_preview_retry_result(warm, format != 0) && correct;
        yyjson_doc_free(warm);
    }

    /* Full getters and summary-only callers must not consume sample faults or
     * record projected sample bytes. This also protects incremental callers. */
    cbm_store_doc_links_test_sample_stats_reset();
    cbm_store_doc_links_test_fail_sample_alloc_after(0);
    cbm_mcp_doc_links_test_fail_sample_alloc_after(0);
    cbm_doc_link_row_t *full = NULL, *samples = NULL;
    cbm_doc_link_reason_count_t *reasons = NULL;
    int count = 0, nsamples = 0, nreasons = 0;
    bool present = false;
    bool bypass =
        cbm_store_doc_links_get(store, project, &full, &count, &present) == CBM_STORE_OK &&
        present && count == 2;
    cbm_store_free_doc_links(full, count);
    bypass = cbm_store_doc_links_summary(store, project, &reasons, &nreasons, &samples, &nsamples,
                                         0, &present) == CBM_STORE_OK &&
             present && nsamples == 0 && nreasons == 1 && bypass;
    cbm_store_free_doc_links(samples, nsamples);
    cbm_store_free_doc_link_reasons(reasons, nreasons);
    char *summary = dm_index_status(project, false);
    bypass = summary && !strstr(summary, "src/Failure") && bypass;
    free(summary);
    cbm_doc_links_sample_test_stats_t stats = {0};
    cbm_store_doc_links_test_sample_stats(&stats);
    bypass = stats.field_copies == 0 && stats.copied_bytes == 0 && stats.requested_bytes == 0 &&
             !cbm_store_doc_links_test_sample_alloc_failed() &&
             !cbm_mcp_doc_links_test_sample_alloc_failed() && bypass;
    cbm_store_doc_links_test_fail_sample_alloc_after(-1);
    cbm_mcp_doc_links_test_fail_sample_alloc_after(-1);
    fprintf(stderr, "doc preview sample_only=%d\n", bypass);
    correct = bypass && correct;

    for (int stage = 0; stage < 2; stage++) {
        for (int nth = 0; nth < 2; nth++) {
            for (int format = 0; format < 2; format++) {
                uint64_t before = cbm_mem_tracked_live_bytes();
                if (stage == 0) {
                    cbm_store_doc_links_test_fail_sample_alloc_after(nth ? 4 : 0);
                } else {
                    cbm_mcp_doc_links_test_fail_sample_alloc_after(nth ? 4 : 0);
                }
                yyjson_doc *failed = dm_preview_status(project, format != 0);
                bool consumed = stage == 0 ? cbm_store_doc_links_test_sample_alloc_failed()
                                           : cbm_mcp_doc_links_test_sample_alloc_failed();
                bool error = dm_preview_error_result(failed, format != 0);
                yyjson_doc_free(failed);
                cbm_store_doc_links_test_fail_sample_alloc_after(-1);
                cbm_mcp_doc_links_test_fail_sample_alloc_after(-1);
                uint64_t after_failure = cbm_mem_tracked_live_bytes();
                yyjson_doc *retried = dm_preview_status(project, format != 0);
                bool retry = dm_preview_retry_result(retried, format != 0);
                yyjson_doc_free(retried);
                uint64_t after_retry = cbm_mem_tracked_live_bytes();
                bool clean = before == after_failure && before == after_retry;
                correct = consumed && error && retry && clean && correct;
                fprintf(stderr,
                        "doc preview fault stage=%d nth=%d json=%d consumed=%d error=%d retry=%d "
                        "before=%llu failed=%llu retried=%llu clean=%d\n",
                        stage, nth ? 5 : 1, format, consumed, error, retry,
                        (unsigned long long)before, (unsigned long long)after_failure,
                        (unsigned long long)after_retry, clean);
            }
        }
    }
    cbm_store_close(store);
    return !correct;
}

TEST(doc_mentions_index_status_preview_failure) {
    int result = dm_preview_fixture(dm_preview_failure_checks);
    ASSERT_EQ(result, 0);
    PASS();
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

/* A project file is a file of the index like any other: the planner judges a
 * change to one by its scope blob. An edit the blob does not hold repairs
 * file by file; an edit of a <Using>, a new project file and a deleted one
 * rebuild. Every step ends in what a full index of the same tree holds. */
TEST(doc_mentions_incremental_project_files) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_ipf_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "src/App/Main.cs"),
                  "namespace N\n"
                  "{\n"
                  "    /// <summary><see cref=\"ViaProject\"/> <see cref=\"ViaProps\"/></summary>\n"
                  "    public class Main { }\n"
                  "}\n");
    th_write_file(TH_PATH(repo, "src/R.cs"), "namespace R\n"
                                             "{\n"
                                             "    public class ViaProject { }\n"
                                             "}\n"
                                             "namespace S\n"
                                             "{\n"
                                             "    public class ViaProps { }\n"
                                             "}\n");
    th_write_file(TH_PATH(repo, "src/App/App.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                  "  <ItemGroup><Using Include=\"R\" /></ItemGroup>\n"
                  "  <ItemGroup><PackageReference Include=\"P\" Version=\"1.0\" /></ItemGroup>\n"
                  "</Project>\n");
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    char props[512];
    char reason[64];
    int n = 0;
    dm_edge(inc_db, "Main.Main", "R.ViaProject", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_row(inc_db, "src/App/Main.cs", "ViaProps", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");

    /* an edit the scope blob does not hold: a package version */
    th_write_file(TH_PATH(repo, "src/App/App.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                  "  <ItemGroup><Using Include=\"R\" /></ItemGroup>\n"
                  "  <ItemGroup><PackageReference Include=\"P\" Version=\"2.0\" /></ItemGroup>\n"
                  "</Project>\n");
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "package version", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    dm_edge(inc_db, "Main.Main", "R.ViaProject", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* a project file is added: Directory.Build.props brings namespace S in */
    th_write_file(TH_PATH(repo, "Directory.Build.props"),
                  "<Project>\n"
                  "  <ItemGroup><Using Include=\"S\" /></ItemGroup>\n"
                  "</Project>\n");
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "project file added", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    dm_edge(inc_db, "Main.Main", "R.ViaProps", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);

    /* an edit of what the blob holds: the <Using> goes */
    th_write_file(TH_PATH(repo, "src/App/App.csproj"),
                  "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                  "  <ItemGroup><PackageReference Include=\"P\" Version=\"2.0\" /></ItemGroup>\n"
                  "</Project>\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "using removed", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    dm_edge(inc_db, "Main.Main", "R.ViaProject", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);

    /* a project file is deleted */
    ASSERT_EQ(unlink(TH_PATH(repo, "Directory.Build.props")), 0);
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "project file deleted", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
        0);
    dm_edge(inc_db, "Main.Main", "R.ViaProps", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(dm_mentions_from(inc_db, "Main.Main"), 0);

    dm_unlink_db(inc_db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    PASS();
}

/* ── which scope changes are repairable file by file ─────────────── */

/* The scope delta between two versions of one C# file (NULL: the file does
 * not exist); the removed names joined by ','. */
static int dm_delta(const char *before, const char *after, char *names, size_t cap) {
    return dm_scope_delta(CBM_LANG_CSHARP, "S.cs", before, after, names, cap);
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
        /* the file declares a type with a base list (`W : Base`), and a base
         * list is resolved through the file's usings: what W derives from
         * decides how other files' references to W's members come out */
        {"using added",
         DM_DELTA_HEAD "using Acme.More;\n" DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"using removed",
         DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
        {"static using and alias added",
         DM_DELTA_HEAD "using static Acme.S;\nusing A = Acme.B;\n" DM_DELTA_OPEN DM_DELTA_RUN_INT
             DM_DELTA_RUN_STR DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL, NULL},
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
    /* A file that declares no type with a base list: its own usings scope
     * nothing but the file itself, which is re-extracted anyway. */
#define DM_DELTA_PLAIN "namespace N\n{\n    public class W\n    {\n"
    const char *plain = DM_DELTA_HEAD DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE;
    struct {
        const char *what;
        const char *after;
        int want;
    } usings[] = {
        {"plain: using added",
         DM_DELTA_HEAD
         "using Acme.More;\n" DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL},
        {"plain: using removed", DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL},
        {"plain: using changed",
         "using Acme.Other;\n" DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL},
        {"plain: static using and alias added",
         DM_DELTA_HEAD "using static Acme.S;\nusing A = Acme.B;\n" DM_DELTA_PLAIN DM_DELTA_RUN_INT
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL},
        /* a using inside a namespace declaration is the file's own too */
        {"plain: block using added",
         DM_DELTA_HEAD
         "namespace N\n{\n    using Acme.In;\n    public class W\n    {\n" DM_DELTA_RUN_INT
             DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_LOCAL},
        /* ... and with the edit that gives W a base list, the usings count */
        {"plain: base list and using added together",
         DM_DELTA_HEAD
         "using Acme.More;\n" DM_DELTA_OPEN DM_DELTA_RUN_INT DM_DELTA_SIZE DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"plain: global static using added",
         "global using static Acme.S;\n" DM_DELTA_HEAD DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE
             DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"plain: global alias added",
         "global using A = Acme.B;\n" DM_DELTA_HEAD DM_DELTA_PLAIN DM_DELTA_RUN_INT DM_DELTA_SIZE
             DM_DELTA_CLOSE,
         CBM_DOCLINK_DELTA_GLOBAL},
    };
    for (size_t i = 0; i < sizeof(usings) / sizeof(usings[0]); i++) {
        int got = dm_delta(plain, usings[i].after, names, sizeof(names));
        if (got != usings[i].want || names[0]) {
            printf("  %s: delta %d [%s], want %d\n", usings[i].what, got, names, usings[i].want);
            FAIL("scope delta of a file's own usings");
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

    /* The MSBuild files that set a project's global usings are no scope
     * inputs outside the index: each has a scope blob, and the planner judges
     * a change to one by that blob like any other file's. */
    ASSERT_FALSE(cbm_doclinks_is_scope_input("src/App/App.csproj"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input("Directory.Build.props"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input("src/App/App.cs"));
    ASSERT_FALSE(cbm_doclinks_is_scope_input(NULL));
#define DM_PROJ_HEAD "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
#define DM_PROJ_PROPS "  <PropertyGroup><Nullable>enable</Nullable></PropertyGroup>\n"
#define DM_PROJ_USING "  <ItemGroup><Using Include=\"Acme.Extra\" /></ItemGroup>\n"
#define DM_PROJ_PKG "  <ItemGroup><PackageReference Include=\"P\" Version=\"1.0\" /></ItemGroup>\n"
#define DM_PROJ_TAIL "</Project>\n"
    const char *proj = DM_PROJ_HEAD DM_PROJ_PROPS DM_PROJ_USING DM_PROJ_PKG DM_PROJ_TAIL;
    struct {
        const char *what;
        const char *after;
        int want;
    } projects[] = {
        {"project unchanged", proj, CBM_DOCLINK_DELTA_LOCAL},
        /* what the blob does not hold changes nobody's scope */
        {"package version",
         DM_PROJ_HEAD DM_PROJ_PROPS DM_PROJ_USING "  <ItemGroup><PackageReference Include=\"P\" "
                                                  "Version=\"2.0\" /></ItemGroup>\n" DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_LOCAL},
        {"target added",
         DM_PROJ_HEAD DM_PROJ_PROPS DM_PROJ_USING DM_PROJ_PKG
         "  <Target Name=\"T\"><Message Text=\"x\" /></Target>\n" DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_LOCAL},
        /* what it holds changes every file's of the project */
        {"using removed", DM_PROJ_HEAD DM_PROJ_PROPS DM_PROJ_PKG DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"property changed",
         DM_PROJ_HEAD
         "  <PropertyGroup><Nullable>disable</Nullable></PropertyGroup>\n" DM_PROJ_USING DM_PROJ_PKG
             DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"condition added",
         DM_PROJ_HEAD DM_PROJ_PROPS
         "  <ItemGroup Condition=\"'$(A)' == 'b'\"><Using Include=\"Acme.Extra\" "
         "/></ItemGroup>\n" DM_PROJ_PKG DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"import added",
         DM_PROJ_HEAD
         "  <Import Project=\"x.props\" />\n" DM_PROJ_PROPS DM_PROJ_USING DM_PROJ_PKG DM_PROJ_TAIL,
         CBM_DOCLINK_DELTA_GLOBAL},
        {"no longer readable", DM_PROJ_HEAD DM_PROJ_PROPS, CBM_DOCLINK_DELTA_GLOBAL},
        {"project file deleted", NULL, CBM_DOCLINK_DELTA_GLOBAL},
    };
    for (size_t i = 0; i < sizeof(projects) / sizeof(projects[0]); i++) {
        int got = dm_scope_delta(CBM_LANG_XML, "src/App.csproj", proj, projects[i].after, names,
                                 sizeof(names));
        if (got != projects[i].want || names[0]) {
            printf("  %s: delta %d [%s], want %d\n", projects[i].what, got, names,
                   projects[i].want);
            FAIL("project scope delta");
        }
    }
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
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
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

/* ── the scope scanner's limits and cost ─────────────────────────── */

/* `prefix`, then `unit` n times, then `suffix`; the caller frees it. */
static char *dm_repeated(const char *prefix, const char *unit, int n, const char *suffix) {
    size_t pl = strlen(prefix);
    size_t ul = strlen(unit);
    size_t sl = strlen(suffix);
    char *out = malloc(pl + (ul * (size_t)n) + sl + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, prefix, pl);
    for (int i = 0; i < n; i++) {
        memcpy(out + pl + (ul * (size_t)i), unit, ul);
    }
    memcpy(out + pl + (ul * (size_t)n), suffix, sl + 1);
    return out;
}

typedef struct {
    char *src;
    const char *rel_path;
    CBMFileResult *result;
} dm_extract_job_t;

/* cbm_thread_create body: extract job->src as C#. */
static void *dm_extract_thread(void *arg) {
    dm_extract_job_t *job = (dm_extract_job_t *)arg;
    job->result = dm_extract(job->src, CBM_LANG_CSHARP, job->rel_path);
    return NULL;
}

/* Interpolated strings nest: a hole of code can hold the next string. The
 * brace scan follows them to a fixed depth; a file that goes deeper is not
 * placed, and scanning it costs no stack. */
TEST(doc_mentions_cs_scan_nested_holes) {
    /* three levels, in a file whose tree has a parse error (the unsafe
     * dereference): the braces are read from the text, through the holes */
    const char *nested = "namespace N\n" /* 1 */
                         "{\n"
                         "    public class Deep\n" /* 3 */
                         "    {\n"
                         "        unsafe void E(void* p) { _r = ref *(int*)p; }\n"
                         "        void M() { s = $\"a{$\"b{$\"c{1}\"}\"}\"; }\n"
                         "        public int Q;\n" /* 7 */
                         "    }\n"                 /* 8 */
                         "    public class After { }\n"
                         "}\n";
    CBMFileResult *r = dm_extract(nested, CBM_LANG_CSHARP, "Nested.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "T\t1\t3\t8\tc!\t-\tDeep\t"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\tAfter\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\nX\t"));
    cbm_free_result(r);

    /* a hundred levels, every one closed again: deeper than the scan follows.
     * The file is not placed, and its scope says so */
    enum { DM_DEEP = 100 };
    char *closers = dm_repeated("1", "}\"", DM_DEEP, "; }\n    }\n}\n");
    ASSERT_NOT_NULL(closers);
    char *deep = dm_repeated("namespace N\n"
                             "{\n"
                             "    public class Deep\n"
                             "    {\n"
                             "        unsafe void E(void* p) { _r = ref *(int*)p; }\n"
                             "        void M() { s = ",
                             "$\"{", DM_DEEP, closers);
    free(closers);
    ASSERT_NOT_NULL(deep);
    r = dm_extract(deep, CBM_LANG_CSHARP, "Hundred.cs");
    free(deep);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nX\t"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "Q\tDeep\n"));
    ASSERT_NULL(strstr(r->doc_scope, "\nT\t"));
    cbm_free_result(r);
    PASS();
}

/* ... and following them costs no stack: 8,000 levels left open, extracted on
 * a thread with an eighth of an index worker's stack. A scan that follows
 * them all needs one recursion level per three bytes of source. */
TEST(doc_mentions_cs_scan_holes_stack) {
    enum { DM_HOLES = 8000, DM_SMALL_STACK = 1024 * 1024 };
    dm_extract_job_t job = {.src = dm_repeated("namespace N\n"
                                               "{\n"
                                               "    public class Ok { }\n"
                                               "    public class C\n"
                                               "    {\n"
                                               "        string s = ",
                                               "$\"{", DM_HOLES, "\n"),
                            .rel_path = "Holes.cs"};
    ASSERT_NOT_NULL(job.src);
    cbm_thread_t thread;
    ASSERT_EQ(cbm_thread_create(&thread, DM_SMALL_STACK, dm_extract_thread, &job), 0);
    ASSERT_EQ(cbm_thread_join(&thread), 0);
    free(job.src);
    CBMFileResult *r = job.result;
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    /* nothing is placed: the namespace's brace never closes */
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nX\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\nT\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\nR\t"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "Q\tOk\n"));
    cbm_free_result(r);
    PASS();
}

/* The cost of scanning `src` as C#: text positions, brace-stack entries and
 * modifier children visited, bytes taken from the scratch arena. false when the file has no scope.
 */
static bool dm_scan_cost(const char *src, uint64_t *steps, uint64_t *bytes) {
    cbm_doclink_cs_test_cost_reset();
    CBMFileResult *r = src ? dm_extract(src, CBM_LANG_CSHARP, "Cost.cs") : NULL;
    bool ok = r && r->doc_scope;
    cbm_doclink_cs_test_cost(steps, bytes);
    if (r) {
        cbm_free_result(r);
    }
    return ok;
}

/* The scan's cost of an input twice as large, as a multiple of the smaller
 * one's: about 2 for a scan that is linear, 4 for a quadratic one. -1 when a
 * scan fails. `prefix` + `unit` x n + `mid` + `unit2` x n + `suffix`. */
static double dm_cost_growth(const char *prefix, const char *unit, const char *mid,
                             const char *unit2, int n, bool bytes) {
    uint64_t cost[2] = {0, 0};
    for (int k = 0; k < 2; k++) {
        int reps = n * (k + 1);
        char *tail = dm_repeated(mid, unit2, reps, "\n");
        char *src = tail ? dm_repeated(prefix, unit, reps, tail) : NULL;
        uint64_t steps = 0;
        uint64_t scratch = 0;
        bool ok = dm_scan_cost(src, &steps, &scratch);
        free(tail);
        free(src);
        if (!ok) {
            return -1.0;
        }
        cost[k] = bytes ? scratch : steps;
    }
    return cost[0] ? (double)cost[1] / (double)cost[0] : -1.0;
}

/* The scan's work grows with its input, not faster: no clock decides these,
 * the scanner's own counters do. */

/* A run of `$` that starts no string is passed once, not once per `$`. */
TEST(doc_mentions_cs_scan_dollar_run) {
    double growth = dm_cost_growth("class C { int x = ", "$", "; }", "", 20000, false);
    if (!(growth > 0 && growth < 3.0)) {
        printf("  `$` run: twice the input costs %.2f times the steps\n", growth);
        FAIL("the scan of a `$` run is not linear");
    }
    PASS();
}

/* A conditional remembers where the open braces stood, not a copy of them. */
TEST(doc_mentions_cs_scan_branch_memory) {
    double growth =
        dm_cost_growth("class C { void M() {\n", "{", "\n", "#if X\n#endif\n", 3000, true);
    if (!(growth > 0 && growth < 3.0)) {
        printf("  #if under open braces: twice the input takes %.2f times the memory\n", growth);
        FAIL("the memory of the brace scan is not linear");
    }
    PASS();
}

/* Empty sibling branches cannot revisit a deep stack that predates them. */
TEST(doc_mentions_cs_scan_branch_merge_work) {
    enum { DEPTH = 512, SIBLINGS = 512 };
    char *opened = dm_repeated("class C { void M() {\n", "{", DEPTH, "\n#if A\n");
    ASSERT_NOT_NULL(opened);
    char *closed = dm_repeated(opened, "}", DEPTH, "\n");
    free(opened);
    ASSERT_NOT_NULL(closed);
    char *reopened = dm_repeated(closed, "{", DEPTH, "\n");
    free(closed);
    ASSERT_NOT_NULL(reopened);
    char *branches = dm_repeated(reopened, "#elif B\n", SIBLINGS, "#endif\n");
    free(reopened);
    ASSERT_NOT_NULL(branches);
    char *src = dm_repeated(branches, "}", DEPTH, "\n} }\n");
    free(branches);
    ASSERT_NOT_NULL(src);
    uint64_t steps = 0;
    uint64_t scratch = 0;
    bool ok = dm_scan_cost(src, &steps, &scratch);
    size_t bytes = strlen(src);
    free(src);
    ASSERT_TRUE(ok);
    ASSERT_LTE(steps, (uint64_t)bytes * 16);
    PASS();
}

/* Attributes belong to the declaration, not to each variable it declares. */
TEST(doc_mentions_cs_scan_declarator_modifier_work) {
    enum { ATTRIBUTES = 512, DECLARATORS = 512 };
    char *head = dm_repeated("class C {\n", "[A]\n", ATTRIBUTES, "public int ");
    ASSERT_NOT_NULL(head);
    size_t cap = strlen(head) + DECLARATORS * 16 + 16;
    char *src = malloc(cap);
    ASSERT_NOT_NULL(src);
    size_t pos = (size_t)snprintf(src, cap, "%s", head);
    free(head);
    for (int i = 0; i < DECLARATORS; i++) {
        pos += (size_t)snprintf(src + pos, cap - pos, "%sv%d", i ? "," : "", i);
    }
    pos += (size_t)snprintf(src + pos, cap - pos, ";\n}\n");
    cbm_doclink_cs_test_cost_reset();
    CBMFileResult *r = dm_extract(src, CBM_LANG_CSHARP, "Fields.cs");
    free(src);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    /* Require the grammar to expose every declarator before assessing work. */
    int members = 0;
    const char *p = r->doc_scope;
    while ((p = strstr(p, "\nM\t")) != NULL) {
        members++;
        p++;
    }
    uint64_t steps = 0;
    uint64_t scratch = 0;
    cbm_doclink_cs_test_cost(&steps, &scratch);
    cbm_free_result(r);
    ASSERT_EQ(members, DECLARATORS);
    ASSERT_LTE(steps, (uint64_t)pos * 16);
    PASS();
}

/* Sharing a large comment must not copy or parse its prose per declarator.
 * Tokens remain per source; the measured work excludes that required output. */
TEST(doc_mentions_cs_shared_doc_work) {
    bool bounded = true;
    for (int n = 16; n <= 32; n *= 2) {
        for (int prose = 2048; prose <= 4096; prose *= 2) {
            char *head =
                dm_repeated("class C {\n/// ", "x", prose, " <see cref=\"First\"/>\npublic int ");
            ASSERT_NOT_NULL(head);
            size_t cap = strlen(head) + (size_t)n * 16 + 16;
            char *src = malloc(cap);
            ASSERT_NOT_NULL(src);
            size_t len = (size_t)snprintf(src, cap, "%s", head);
            free(head);
            for (int i = 0; i < n; i++) {
                len += (size_t)snprintf(src + len, cap - len, "%sv%d", i ? "," : "", i);
            }
            len += (size_t)snprintf(src + len, cap - len, ";\n}\n");
            cbm_doclink_test_doc_work_reset();
            CBMFileResult *r = dm_extract(src, CBM_LANG_CSHARP, "Shared.cs");
            free(src);
            ASSERT_NOT_NULL(r);
            int tokens = r->doc_links.count;
            uint64_t copied = 0, parse_input = 0, cleaned = 0;
            cbm_doclink_test_doc_work(&copied, &parse_input, &cleaned);
            cbm_free_result(r);
            printf("  shared doc: n=%d prose=%d input=%zu copied=%llu parse_input=%llu "
                   "cleaned=%llu tokens=%d\n",
                   n, prose, len, (unsigned long long)copied, (unsigned long long)parse_input,
                   (unsigned long long)cleaned, tokens);
            ASSERT_EQ(tokens, n);
            bounded = bounded && copied <= (uint64_t)len * 2 && parse_input <= (uint64_t)len * 2 &&
                      cleaned <= 12;
        }
    }
    ASSERT_TRUE(bounded);
    PASS();
}

/* A one-shot resource failure must not suppress later sources via sharing. */
TEST(doc_mentions_cs_shared_doc_allocation_failure) {
    static const struct {
        int kind, nth, refs, expected[3];
    } cases[] = {
        {CBM_DOCLINK_ALLOC_VALUE, 3, 2, {2, 1, 2}},
        {CBM_DOCLINK_ALLOC_TOKENS, 2, 10, {10, 9, 10}},
        {CBM_DOCLINK_ALLOC_TEXT, 2, 2, {2, 2, 2}},
        /* The second comment collection loses a span while growing past8.
         * It must not become the shared doc for the later variables. */
        {CBM_DOCLINK_ALLOC_SPAN, 4, 12, {12, 12, 12}},
    };
    bool ok = true;
    for (size_t k = 0; k < sizeof(cases) / sizeof(cases[0]); k++) {
        char *src = dm_repeated("class C {\n", "/// <see cref=\"First\"/>\n", cases[k].refs,
                                "public int a,b,c;\n}\n");
        ASSERT_NOT_NULL(src);
        cbm_doclink_test_fail_alloc_after(cases[k].kind, cases[k].nth);
        CBMFileResult *r = dm_extract(src, CBM_LANG_CSHARP, "Failure.cs");
        cbm_doclink_test_reset_alloc();
        free(src);
        ASSERT_NOT_NULL(r);
        int counts[3] = {0};
        for (int i = 0; i < r->doc_links.count; i++) {
            const char *name = strrchr(r->doc_links.items[i].source_qn, '.');
            ASSERT_NOT_NULL(name);
            name++;
            ASSERT_TRUE(name[0] >= 'a' && name[0] <= 'c' && name[1] == '\0');
            counts[name[0] - 'a']++;
        }
        cbm_free_result(r);
        printf("  shared doc allocation: stage=%d counts=%d,%d,%d expected=%d,%d,%d\n",
               cases[k].kind, counts[0], counts[1], counts[2], cases[k].expected[0],
               cases[k].expected[1], cases[k].expected[2]);
        for (int i = 0; i < 3; i++) {
            ok = ok && counts[i] == cases[k].expected[i];
        }
    }
    ASSERT_TRUE(ok);
    PASS();
}

/* Shared lexical tokens survive output growth, distinct comments and files. */
TEST(doc_mentions_cs_shared_doc_replay) {
    enum { REFS = 80 };
    char *first = dm_repeated("class C {\n/// ", "<see cref=\"First\"/> ", REFS,
                              "\npublic int a,\nb,\nc;\n/// ");
    ASSERT_NOT_NULL(first);
    char *src = dm_repeated(first, "<see cref=\"First\"/> ", REFS, "\npublic int d,\ne,\nf;\n}\n");
    free(first);
    ASSERT_NOT_NULL(src);
    CBMFileResult *r = dm_extract(src, CBM_LANG_CSHARP, "Shared.cs");
    free(src);
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 6 * REFS);
    int counts[6] = {0};
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *link = &r->doc_links.items[i];
        const char *name = strrchr(link->source_qn, '.');
        ASSERT_NOT_NULL(name);
        name++;
        ASSERT_TRUE(name[0] >= 'a' && name[0] <= 'f' && name[1] == '\0');
        int n = name[0] - 'a';
        ASSERT_STR_EQ(link->raw, "First");
        ASSERT_EQ(link->line, n < 3 ? 2 : 6);
        ASSERT_EQ(link->def_line, (uint32_t)(n < 3 ? n + 3 : n + 4));
        ASSERT_EQ(link->syntax, CBM_DOCLINK_CS_SEE);
        ASSERT_EQ(link->flags, 0);
        counts[n]++;
    }
    cbm_free_result(r);
    for (int i = 0; i < 6; i++) {
        ASSERT_EQ(counts[i], REFS);
    }
    r = dm_extract("class C {\n/// <see cref=\"Other\"/>\npublic int a,b,c;\n}\n", CBM_LANG_CSHARP,
                   "Shared.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 3);
    for (int i = 0; i < r->doc_links.count; i++) {
        ASSERT_STR_EQ(r->doc_links.items[i].raw, "Other");
        ASSERT_EQ(r->doc_links.items[i].line, 2);
    }
    cbm_free_result(r);
    PASS();
}

/* A declaration keyword's header is read up to the next keyword, so every
 * byte of the file is read a bounded number of times. */
TEST(doc_mentions_cs_scan_header_reads) {
    const char *heads[] = {"class a ", "class a<[ "};
    for (size_t i = 0; i < sizeof(heads) / sizeof(heads[0]); i++) {
        char *src = dm_repeated("namespace N {\n", heads[i], 20000, "\n");
        ASSERT_NOT_NULL(src);
        uint64_t steps = 0;
        uint64_t scratch = 0;
        bool ok = dm_scan_cost(src, &steps, &scratch);
        size_t len = strlen(src);
        free(src);
        ASSERT_TRUE(ok);
        if (steps > (uint64_t)len * 16) {
            printf("  `%s` x 20000: %llu steps for %zu bytes\n", heads[i],
                   (unsigned long long)steps, len);
            FAIL("declaration headers are read over and over");
        }
    }
    PASS();
}

/* The gate of the project scan is the file's name: an XML file that cannot be
 * an MSBuild project file is not looked at, whatever its size, and a project
 * file larger than a project file is not read and says so. No clock decides
 * this: the scan counts the bytes it passes. */
TEST(doc_mentions_msbuild_gate) {
    enum { DM_BIG = 2 * 1024 * 1024 };
    char *filler = malloc(DM_BIG + 1);
    ASSERT_NOT_NULL(filler);
    memset(filler, 'x', DM_BIG);
    filler[DM_BIG] = '\0';
    char *big =
        dm_repeated("<Project><PropertyGroup><P>", filler, 1, "</P></PropertyGroup></Project>\n");
    free(filler);
    ASSERT_NOT_NULL(big);
    uint64_t steps = 0;
    uint64_t bytes = 0;
    /* not a project file's name: no scope, and not one byte passed */
    cbm_doclink_cs_test_cost_reset();
    CBMFileResult *r = dm_extract(big, CBM_LANG_XML, "data/huge.xml");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    cbm_doclink_cs_test_cost(&steps, &bytes);
    ASSERT_EQ(steps, 0);
    /* a project file's name on a file larger than one: not read either, and
     * the blob says that it was not */
    r = dm_extract(big, CBM_LANG_XML, "eng/Huge.props");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_STR_EQ(r->doc_scope, "cs1\nP\t\t>\n");
    cbm_free_result(r);
    cbm_doclink_cs_test_cost(&steps, &bytes);
    ASSERT_EQ(steps, 0);
    /* a project file: every byte is passed once */
    const char *small = "<Project><ItemGroup><Using Include=\"A\" /></ItemGroup></Project>\n";
    r = dm_extract(small, CBM_LANG_XML, "eng/Small.targets");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    cbm_free_result(r);
    cbm_doclink_cs_test_cost(&steps, &bytes);
    ASSERT_EQ(steps, strlen(small));
    /* XML of another kind under a project file's name: the scan stops at its
     * root element */
    cbm_doclink_cs_test_cost_reset();
    char *other = dm_repeated("<Settings>", "<Entry Key=\"k\" Value=\"v\" />", 4000, "</Settings>");
    ASSERT_NOT_NULL(other);
    r = dm_extract(other, CBM_LANG_XML, "eng/Other.props");
    free(other);
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    cbm_doclink_cs_test_cost(&steps, &bytes);
    ASSERT_EQ(steps, strlen("<Settings>"));

    /* a file that was not read is counted where it is evaluated, as the
     * project file itself or as an import; it opens nobody's scope */
    const dm_project_file_t files[] = {
        {"eng/Huge.props", big},
        {"eng/Broken.props", "<Project><PropertyGroup>"},
        {"App.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                       "  <Import Project=\"eng/Huge.props\" />\n"
                       "  <Import Project=\"eng/Broken.props\" />\n"
                       "  <ItemGroup><Using Include=\"Still.Here\" /></ItemGroup>\n"
                       "</Project>\n"},
    };
    cbm_msb_result_t res;
    ASSERT_TRUE(dm_msb_eval(files, 3, "App.csproj", &res));
    ASSERT_TRUE(dm_has_using(&res, 'n', "Still.Here"));
    ASSERT_EQ(res.unevaluable, 2);
    ASSERT_FALSE(res.open);
    cbm_msb_result_free(&res);
    ASSERT_TRUE(dm_msb_eval(files, 2, "eng/Broken.props", &res));
    ASSERT_EQ(res.count, 0);
    ASSERT_EQ(res.unevaluable, 1);
    cbm_msb_result_free(&res);
    free(big);
    PASS();
}

/* ── MSBuild inputs are files of the index, never of the disk ────── */

#ifndef _WIN32
static const char DM_USING_EXTRA[] = "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                                     "  <ItemGroup>\n"
                                     "    <Using Include=\"Acme.Extra\" />\n"
                                     "  </ItemGroup>\n"
                                     "</Project>\n";
static const char DM_USING_MORE[] = "<Project>\n"
                                    "  <ItemGroup>\n"
                                    "    <Using Include=\"Acme.More\" />\n"
                                    "  </ItemGroup>\n"
                                    "</Project>\n";

/* A repository whose project files point out of it: `src/App/Evil.csproj` is a
 * symbolic link to a project file outside, and `src/Lib/Lib.csproj` imports
 * through `src/Lib/ext`, a link to a directory outside. Both outside files
 * hold a <Using> that would bring a type of the repository into scope. */
static void dm_write_linked_fixture(const char *repo, const char *outside) {
    th_write_file(TH_PATH(outside, "Real.csproj"), DM_USING_EXTRA);
    th_write_file(TH_PATH(outside, "dir/Linked.props"), DM_USING_MORE);
    th_write_file(TH_PATH(repo, "src/Decl.cs"), "namespace Acme.Extra\n"
                                                "{\n"
                                                "    public class Tool { }\n"
                                                "}\n"
                                                "namespace Acme.More\n"
                                                "{\n"
                                                "    public class Gear { }\n"
                                                "}\n");
    th_write_file(TH_PATH(repo, "src/App/Uses.cs"),
                  "namespace Acme.App\n"
                  "{\n"
                  "    /// <summary><see cref=\"Tool\"/></summary>\n"
                  "    public class Uses { }\n"
                  "}\n");
    th_write_file(TH_PATH(repo, "src/Lib/Lib.csproj"), "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                                                       "  <Import Project=\"ext/Linked.props\" />\n"
                                                       "</Project>\n");
    th_write_file(TH_PATH(repo, "src/Lib/UsesLib.cs"),
                  "namespace Acme.Lib\n"
                  "{\n"
                  "    /// <summary><see cref=\"Gear\"/></summary>\n"
                  "    public class UsesLib { }\n"
                  "}\n");
}

/* Index the linked fixture under `tmp` into `db` (a buffer of 512 bytes). */
static int dm_index_linked_fixture(const char *tmp, char *db) {
    char repo[400];
    char outside[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(outside, sizeof(outside), "%s/outside", tmp);
    dm_write_linked_fixture(repo, outside);
    char target[512];
    snprintf(target, sizeof(target), "%s/Real.csproj", outside);
    if (symlink(target, TH_PATH(repo, "src/App/Evil.csproj")) != 0) {
        return -1;
    }
    snprintf(target, sizeof(target), "%s/dir", outside);
    if (symlink(target, TH_PATH(repo, "src/Lib/ext")) != 0) {
        return -1;
    }
    /* a named pipe with a project file's name: opening it would block */
    if (mkfifo(TH_PATH(repo, "src/Lib/Pipe.csproj"), 0600) != 0) {
        return -1;
    }
    snprintf(db, 512, "%s/lnk.db", tmp);
    return dm_index(repo, db, NULL);
}

/* What is not a file of the repository is not read: discovery skips symbolic
 * links, and the resolver takes project files from the index alone. A project
 * file that is a link to a file outside has no <Using> in effect. */
TEST(doc_mentions_msbuild_linked_project_file) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_lnk_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char db[512];
    ASSERT_EQ(dm_index_linked_fixture(tmp, db), 0);
    char props[512];
    char reason[64];
    int n = 0;
    dm_edge(db, "Uses.Uses", "Decl.Tool", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, "src/App/Uses.cs", "Tool", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

/* ... and neither has a file imported through a linked directory. */
TEST(doc_mentions_msbuild_linked_import_directory) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_lnd_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char db[512];
    ASSERT_EQ(dm_index_linked_fixture(tmp, db), 0);
    char props[512];
    char reason[64];
    int n = 0;
    dm_edge(db, "UsesLib.UsesLib", "Decl.Gear", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    dm_row(db, "src/Lib/UsesLib.cs", "Gear", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}
#endif /* !_WIN32 */

/* ── the C# lookup rules, one small repository per rule ──────────── */

typedef struct {
    const char *path;
    const char *text;
} dm_source_t;

/* What one written reference must come to. `rel` and `raw` name the
 * reference (a raw text is written once per file, or comes to the same row
 * every time), `src` the documented definition.
 *   target  an edge from `src` to the node whose qualified name ends so, and
 *           no row; `tier` (when set) is in the edge's properties
 *   reason  a row with that reason
 *   neither no row: the reference is local, or only `never` is asked
 *   never   a node `src` must not mention */
typedef struct {
    const char *rel;
    const char *raw;
    const char *src;
    const char *target;
    const char *reason;
    const char *never;
    const char *tier;
} dm_want_t;

#define DM_EXACT "\"tier\":\"exact\""
#define DM_UNIQUE "\"tier\":\"unique\""

/* 1 when the database does not hold what `w` asks for (and says what it
 * holds instead), else 0. */
static int dm_want_failed(const char *db, const dm_want_t *w) {
    char props[512];
    char reason[64];
    int n = 0;
    int bad = 0;
    dm_row(db, w->rel, w->raw, reason, sizeof(reason), NULL, 0);
    if (strcmp(reason, w->reason ? w->reason : "") != 0) {
        printf("  `%s` in %s: row [%s], want [%s]\n", w->raw, w->rel, reason,
               w->reason ? w->reason : "");
        bad = 1;
    }
    if (w->target) {
        dm_edge(db, w->src, w->target, props, sizeof(props), &n);
        if (n != 1) {
            printf("  `%s` in %s: %d edges %s -> %s, want 1\n", w->raw, w->rel, n, w->src,
                   w->target);
            bad = 1;
        } else if (w->tier && !strstr(props, w->tier)) {
            printf("  `%s` in %s: edge %s, want %s\n", w->raw, w->rel, props, w->tier);
            bad = 1;
        }
    }
    if (w->never) {
        dm_edge(db, w->src, w->never, props, sizeof(props), &n);
        if (n != 0) {
            printf("  `%s` in %s: %s -> %s is bound, want no such edge\n", w->raw, w->rel, w->src,
                   w->never);
            bad = 1;
        }
    }
    return bad;
}

/* Index `files` as a repository of their own and hold the result against
 * `wants`. Returns how many of them failed (each is printed), -1 when the
 * repository could not be indexed. */
static int dm_check_repo(const char *tag, const dm_source_t *files, int nfiles,
                         const dm_want_t *wants, int nwants) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_%s_XXXXXX", tag);
    if (!cbm_mkdtemp(tmp)) {
        return -1;
    }
    for (int i = 0; i < nfiles; i++) {
        th_write_file(TH_PATH(tmp, files[i].path), files[i].text);
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/lookup.db", tmp);
    int bad = dm_index(tmp, db, NULL) == 0 ? 0 : -1;
    for (int i = 0; bad >= 0 && i < nwants; i++) {
        bad += dm_want_failed(db, &wants[i]);
    }
    dm_unlink_db(db);
    th_rmtree(tmp);
    return bad;
}

#define DM_COUNT(a) ((int)(sizeof(a) / sizeof((a)[0])))

/* Distinct variables documented together remain distinct MENTIONS sources. */
TEST(doc_mentions_cs_shared_doc_sources) {
    static const char source[] = "namespace N {\n"
                                 "public class First { } public class Second { }\n"
                                 "public class C {\n"
                                 "/// <see cref=\"First\"/> <see cref=\"Second\"/>\n"
                                 "public int a,\n"
                                 "b,\n"
                                 "c;\n"
                                 "}\n}\n";
    CBMFileResult *r = dm_extract(source, CBM_LANG_CSHARP, "Shared.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 6);
    int pairs[3][2] = {{0}};
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *link = &r->doc_links.items[i];
        const char *name = strrchr(link->source_qn, '.');
        ASSERT_NOT_NULL(name);
        name++;
        int src = strcmp(name, "a") == 0   ? 0
                  : strcmp(name, "b") == 0 ? 1
                  : strcmp(name, "c") == 0 ? 2
                                           : -1;
        int dst = strcmp(link->raw, "First") == 0 ? 0 : strcmp(link->raw, "Second") == 0 ? 1 : -1;
        ASSERT_TRUE(src >= 0 && dst >= 0);
        ASSERT_EQ(link->line, 4);
        ASSERT_EQ(link->def_line, (uint32_t)(5 + src));
        pairs[src][dst]++;
    }
    cbm_free_result(r);
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 2; j++) {
            ASSERT_EQ(pairs[i][j], 1);
        }
    }
    const dm_source_t files[] = {{"Shared.cs", source}};
    static const dm_want_t wants[] = {
        {"Shared.cs", "First", "a", "First", NULL, NULL, NULL},
        {"Shared.cs", "Second", "a", "Second", NULL, NULL, NULL},
        {"Shared.cs", "First", "b", "First", NULL, NULL, NULL},
        {"Shared.cs", "Second", "b", "Second", NULL, NULL, NULL},
        {"Shared.cs", "First", "c", "First", NULL, NULL, NULL},
        {"Shared.cs", "Second", "c", "Second", NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("shared_doc", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* The order of the scope levels: an inner namespace declaration's own usings
 * and aliases are asked before an outer namespace's types; a qualified name
 * and a using's target are relative to the namespaces around them before
 * they are absolute; and a nearer namespace that has the first segment is
 * the end of the search. */
TEST(doc_mentions_cs_lookup_order) {
    static const dm_source_t files[] = {
        {"src/AcmeLogger.cs", "namespace Acme\n{\n    public class Logger { }\n}\n"},
        {"src/WidgetsLogger.cs", "namespace Acme.Widgets\n"
                                 "{\n"
                                 "    public class Logger { }\n"
                                 "    public class OnlyWidgets { }\n"
                                 "}\n"},
        {"src/UtilGlobal.cs", "namespace Util\n"
                              "{\n"
                              "    public class Helper { }\n"
                              "    public class OnlyGlobal { }\n"
                              "}\n"},
        {"src/UtilAcme.cs", "namespace Acme.Util\n{\n    public class Helper { }\n}\n"},
        {"src/BlockUsing.cs", "namespace Acme.App\n"
                              "{\n"
                              "    using Acme.Widgets;\n"
                              "\n"
                              "    /// <summary><see cref=\"Logger\"/></summary>\n"
                              "    public class BlockUsing { }\n"
                              "}\n"},
        {"src/BlockAlias.cs", "namespace Acme.App\n"
                              "{\n"
                              "    using Logger = Acme.Widgets.OnlyWidgets;\n"
                              "\n"
                              "    /// <summary><see cref=\"Logger\"/></summary>\n"
                              "    public class BlockAlias { }\n"
                              "}\n"},
        {"src/FileUsing.cs", "using Acme.Widgets;\n"
                             "\n"
                             "namespace Acme.App\n"
                             "{\n"
                             "    /// <summary><see cref=\"Logger\"/></summary>\n"
                             "    public class FileUsing { }\n"
                             "}\n"},
        {"src/Relative.cs",
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Util.Helper\"/> <see cref=\"Util.OnlyGlobal\"/></summary>\n"
         "    public class Relative { }\n"
         "\n"
         "    /// <summary><see cref=\"global::Util.Helper\"/></summary>\n"
         "    public class Absolute { }\n"
         "}\n"},
        {"src/RelativeUsing.cs", "namespace Acme.App\n"
                                 "{\n"
                                 "    using Widgets;\n"
                                 "    using W = Widgets.OnlyWidgets;\n"
                                 "\n"
                                 "    /// <summary><see cref=\"OnlyWidgets\"/></summary>\n"
                                 "    public class RelativeUsing { }\n"
                                 "\n"
                                 "    /// <summary><see cref=\"W\"/></summary>\n"
                                 "    public class RelativeAlias { }\n"
                                 "}\n"},
    };
    static const dm_want_t wants[] = {
        /* the block's using comes before the outer namespace Acme */
        {"src/BlockUsing.cs", "Logger", "BlockUsing.BlockUsing", "WidgetsLogger.Logger", NULL,
         "AcmeLogger.Logger", DM_UNIQUE},
        /* ... and so does its alias */
        {"src/BlockAlias.cs", "Logger", "BlockAlias.BlockAlias", "WidgetsLogger.OnlyWidgets", NULL,
         "AcmeLogger.Logger", DM_EXACT},
        /* the same using at the top of the file comes after every namespace */
        {"src/FileUsing.cs", "Logger", "FileUsing.FileUsing", "AcmeLogger.Logger", NULL,
         "WidgetsLogger.Logger", NULL},
        /* `Util` is Acme.Util from inside Acme.App, not the global Util */
        {"src/Relative.cs", "Util.Helper", "Relative.Relative", "UtilAcme.Helper", NULL,
         "UtilGlobal.Helper", DM_EXACT},
        /* ... and Acme.Util is where the search ends: no second try further out */
        {"src/Relative.cs", "Util.OnlyGlobal", "Relative.Relative", NULL, "missing",
         "UtilGlobal.OnlyGlobal", NULL},
        {"src/Relative.cs", "global::Util.Helper", "Relative.Absolute", "UtilGlobal.Helper", NULL,
         "UtilAcme.Helper", DM_EXACT},
        /* a using's target and an alias's are relative to their block */
        {"src/RelativeUsing.cs", "OnlyWidgets", "RelativeUsing.RelativeUsing",
         "WidgetsLogger.OnlyWidgets", NULL, NULL, DM_UNIQUE},
        {"src/RelativeUsing.cs", "W", "RelativeUsing.RelativeAlias", "WidgetsLogger.OnlyWidgets",
         NULL, NULL, DM_EXACT},
    };
    ASSERT_EQ(dm_check_repo("order", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A type parameter in scope shadows a type of its name: a reference to it
 * names the definition's own parameter, and is neither an edge nor a row. */
TEST(doc_mentions_cs_type_parameters) {
    static const dm_source_t files[] = {
        {"src/Generic.cs",
         "namespace Acme.Gen\n"
         "{\n"
         "    public class TItem { }\n"
         "    public class TKey { }\n"
         "\n"
         "    /// <summary><see cref=\"TItem\"/> <see cref=\"TKey\"/></summary>\n"
         "    public class Bag<TItem>\n"
         "    {\n"
         "        /// <summary><see cref=\"TItem\"/> <see cref=\"TKey\"/></summary>\n"
         "        public void Put<TKey>(TKey key) { }\n"
         "\n"
         "        /// <summary><see cref=\"TKey\"/></summary>\n"
         "        public void Other() { }\n"
         "    }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        /* on the type: its own parameter is local, the class TKey is a class */
        {"src/Generic.cs", "TItem", "Generic.Bag", NULL, NULL, "Generic.TItem", NULL},
        {"src/Generic.cs", "TKey", "Generic.Bag", "Generic.TKey", NULL, NULL, NULL},
        /* on the generic method: both names are parameters in scope */
        {"src/Generic.cs", "TItem", "Generic.Bag.Put", NULL, NULL, "Generic.TItem", NULL},
        {"src/Generic.cs", "TKey", "Generic.Bag.Put", NULL, NULL, "Generic.TKey", NULL},
        /* on a method without the parameter: the class again */
        {"src/Generic.cs", "TKey", "Generic.Bag.Other", "Generic.TKey", NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("tparam", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

#define DM_EMPTY_PROJECT "<Project Sdk=\"Microsoft.NET.Sdk\">\n</Project>\n"

/* `using static` brings in a type's nested types and its static members
 * (no instance member, no extension method). A `global using` -- of a
 * namespace, of a type's static members, of an alias alike -- and a project
 * file's <Using> serve every file of their project, and no other project. */
TEST(doc_mentions_cs_usings) {
    static const dm_source_t files[] = {
        {"src/Lib/Maths.cs", "namespace Acme.Calc\n"
                             "{\n"
                             "    public static class Maths\n"
                             "    {\n"
                             "        public static int Max(int a, int b) { return a; }\n"
                             "        public static int Extension(this string s) { return 0; }\n"
                             "        public const int Limit = 1;\n"
                             "        public class Nested { }\n"
                             "    }\n"
                             "    public class Shape\n"
                             "    {\n"
                             "        public int Instance() { return 0; }\n"
                             "        public static int Area() { return 0; }\n"
                             "    }\n"
                             "    public enum Color { Red }\n"
                             "}\n"},
        {"src/App/UseStatic.cs",
         "using static Acme.Calc.Maths;\n"
         "using static Acme.Calc.Shape;\n"
         "using static Acme.Calc.Color;\n"
         "\n"
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Max\"/> <see cref=\"Limit\"/> <see cref=\"Nested\"/>\n"
         "    /// <see cref=\"Area\"/> <see cref=\"Red\"/> <see cref=\"Instance\"/>\n"
         "    /// <see cref=\"Extension\"/></summary>\n"
         "    public class UseStatic { }\n"
         "}\n"},
        {"src/Proj/Proj.csproj", DM_EMPTY_PROJECT},
        {"src/Proj/Globals.cs", "global using Acme.Calc;\n"
                                "global using static Acme.Calc.Maths;\n"
                                "global using GC = Acme.Calc.Color;\n"},
        {"src/Proj/UseGlobal.cs",
         "namespace Proj.App\n"
         "{\n"
         "    /// <summary><see cref=\"Shape\"/> <see cref=\"Max\"/> <see cref=\"GC\"/></summary>\n"
         "    public class UseGlobal { }\n"
         "}\n"},
        {"src/Other/Other.csproj", DM_EMPTY_PROJECT},
        {"src/Other/NoGlobal.cs",
         "namespace Other.App\n"
         "{\n"
         "    /// <summary><see cref=\"Shape\"/> <see cref=\"Max\"/> <see cref=\"GC\"/></summary>\n"
         "    public class NoGlobal { }\n"
         "}\n"},
        {"src/Msb/Msb.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                               "  <ItemGroup>\n"
                               "    <Using Include=\"Acme.Calc.Maths\" Static=\"true\" />\n"
                               "    <Using Include=\"Acme.Calc.Color\" Alias=\"MC\" />\n"
                               "  </ItemGroup>\n"
                               "</Project>\n"},
        {"src/Msb/UseMsb.cs",
         "namespace Msb.App\n"
         "{\n"
         "    /// <summary><see cref=\"Max\"/> <see cref=\"MC\"/> <see cref=\"Shape\"/></summary>\n"
         "    public class UseMsb { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        {"src/App/UseStatic.cs", "Max", "UseStatic.UseStatic", "Maths.Maths.Max", NULL, NULL,
         DM_UNIQUE},
        {"src/App/UseStatic.cs", "Limit", "UseStatic.UseStatic", "Maths.Maths.Limit", NULL, NULL,
         NULL},
        {"src/App/UseStatic.cs", "Nested", "UseStatic.UseStatic", "Maths.Maths.Nested", NULL, NULL,
         NULL},
        {"src/App/UseStatic.cs", "Area", "UseStatic.UseStatic", "Maths.Shape.Area", NULL, NULL,
         NULL},
        {"src/App/UseStatic.cs", "Red", "UseStatic.UseStatic", "Maths.Color.Red", NULL, NULL, NULL},
        /* an instance member and an extension method do not come with it */
        {"src/App/UseStatic.cs", "Instance", "UseStatic.UseStatic", NULL, "missing",
         "Maths.Shape.Instance", NULL},
        {"src/App/UseStatic.cs", "Extension", "UseStatic.UseStatic", NULL, "missing",
         "Maths.Maths.Extension", NULL},
        /* the three kinds of `global using`, from another file of the project */
        {"src/Proj/UseGlobal.cs", "Shape", "UseGlobal.UseGlobal", "Maths.Shape", NULL, NULL, NULL},
        {"src/Proj/UseGlobal.cs", "Max", "UseGlobal.UseGlobal", "Maths.Maths.Max", NULL, NULL,
         NULL},
        {"src/Proj/UseGlobal.cs", "GC", "UseGlobal.UseGlobal", "Maths.Color", NULL, NULL, DM_EXACT},
        /* ... and not from another project */
        {"src/Other/NoGlobal.cs", "Shape", "NoGlobal.NoGlobal", NULL, "missing", "Maths.Shape",
         NULL},
        {"src/Other/NoGlobal.cs", "Max", "NoGlobal.NoGlobal", NULL, "missing", "Maths.Maths.Max",
         NULL},
        {"src/Other/NoGlobal.cs", "GC", "NoGlobal.NoGlobal", NULL, "missing", "Maths.Color", NULL},
        /* a project file's <Using Static> and <Using Alias> */
        {"src/Msb/UseMsb.cs", "Max", "UseMsb.UseMsb", "Maths.Maths.Max", NULL, NULL, NULL},
        {"src/Msb/UseMsb.cs", "MC", "UseMsb.UseMsb", "Maths.Color", NULL, NULL, DM_EXACT},
        {"src/Msb/UseMsb.cs", "Shape", "UseMsb.UseMsb", NULL, "missing", "Maths.Shape", NULL},
    };
    ASSERT_EQ(dm_check_repo("usings", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* What a type is: its name, its arity, the arity of every type around it, and
 * its assembly. `Outer.Inner` and `Outer<T>.Inner` are two types; two
 * projects that each declare `Acme.Shared.Config` declare two types; the
 * parts of a partial type in one place are one type. */
TEST(doc_mentions_cs_entities) {
    static const dm_source_t files[] = {
        {"src/Twins.cs",
         "namespace Acme.Ent\n"
         "{\n"
         "    public class Outer { public class Inner { public void A() { } } }\n"
         "    public class Outer<T> { public class Inner { public void B() { } } }\n"
         "\n"
         "    /// <summary><see cref=\"Outer.Inner\"/> <see cref=\"Outer.Inner.A\"/>\n"
         "    /// <see cref=\"Outer.Inner.B\"/></summary>\n"
         "    public class UsesPlain { }\n"
         "\n"
         "    /// <summary><see cref=\"Outer{T}.Inner\"/> <see cref=\"Outer{T}.Inner.B\"/>\n"
         "    /// <see cref=\"Outer{T}.Inner.A\"/></summary>\n"
         "    public class UsesGeneric { }\n"
         "}\n"},
        {"src/Part1.cs", "namespace Acme.Parts\n"
                         "{\n"
                         "    public partial class Split { public void First() { } }\n"
                         "}\n"},
        {"src/Part2.cs", "namespace Acme.Parts\n"
                         "{\n"
                         "    public partial class Split\n"
                         "    {\n"
                         "        public void Second() { }\n"
                         "        public class In { public class Deep { } }\n"
                         "    }\n"
                         "\n"
                         "    /// <summary><see cref=\"Split\"/> <see cref=\"Split.First\"/> <see "
                         "cref=\"Split.Second\"/>\n"
                         "    /// <see cref=\"Split.In\"/> <see cref=\"Split.In.Deep\"/> <see "
                         "cref=\"In\"/></summary>\n"
                         "    public class UsesSplit { }\n"
                         "}\n"},
        {"src/P1/P1.csproj", DM_EMPTY_PROJECT},
        {"src/P1/Config.cs",
         "namespace Acme.Shared\n"
         "{\n"
         "    public class Config { public void OnlyOne() { } }\n"
         "\n"
         "    /// <summary><see cref=\"Config\"/> <see cref=\"Config.OnlyOne\"/>\n"
         "    /// <see cref=\"Config.OnlyTwo\"/></summary>\n"
         "    public class UsesOne { }\n"
         "}\n"},
        {"src/P2/P2.csproj", DM_EMPTY_PROJECT},
        {"src/P2/Config.cs",
         "namespace Acme.Shared\n"
         "{\n"
         "    public class Config { public void OnlyTwo() { } }\n"
         "\n"
         "    /// <summary><see cref=\"Config\"/> <see cref=\"Config.OnlyTwo\"/>\n"
         "    /// <see cref=\"Config.OnlyOne\"/></summary>\n"
         "    public class UsesTwo { }\n"
         "}\n"},
        {"src/P3/P3.csproj", DM_EMPTY_PROJECT},
        {"src/P3/Uses.cs", "namespace Acme.Shared\n"
                           "{\n"
                           "    /// <summary><see cref=\"Config\"/></summary>\n"
                           "    public class UsesThree { }\n"
                           "}\n"},
    };
    static const dm_want_t wants[] = {
        /* `Outer.Inner` is the type nested in the arity-0 Outer. The node of
         * that path belongs to the later declaration, the one in Outer<T>: a
         * graph gap, never the twin's node. Its members are its own. */
        {"src/Twins.cs", "Outer.Inner", "Twins.UsesPlain", NULL, "graph_gap", "Twins.Outer.Inner",
         NULL},
        {"src/Twins.cs", "Outer.Inner.A", "Twins.UsesPlain", "Twins.Outer.Inner.A", NULL, NULL,
         NULL},
        {"src/Twins.cs", "Outer.Inner.B", "Twins.UsesPlain", NULL, "missing", "Twins.Outer.Inner.B",
         NULL},
        {"src/Twins.cs", "Outer{T}.Inner", "Twins.UsesGeneric", "Twins.Outer.Inner", NULL, NULL,
         NULL},
        {"src/Twins.cs", "Outer{T}.Inner.B", "Twins.UsesGeneric", "Twins.Outer.Inner.B", NULL, NULL,
         NULL},
        {"src/Twins.cs", "Outer{T}.Inner.A", "Twins.UsesGeneric", NULL, "missing",
         "Twins.Outer.Inner.A", NULL},
        /* a partial type over two files: one type, the members of both, and
         * the node of the declaration nearest to the reference */
        {"src/Part2.cs", "Split", "Part2.UsesSplit", "Part2.Split", NULL, "Part1.Split", NULL},
        {"src/Part2.cs", "Split.First", "Part2.UsesSplit", "Part1.Split.First", NULL, NULL, NULL},
        {"src/Part2.cs", "Split.Second", "Part2.UsesSplit", "Part2.Split.Second", NULL, NULL, NULL},
        /* nested types: by their path, never by their simple name from outside */
        {"src/Part2.cs", "Split.In", "Part2.UsesSplit", "Part2.Split.In", NULL, NULL, NULL},
        {"src/Part2.cs", "Split.In.Deep", "Part2.UsesSplit", "Part2.Split.In.Deep", NULL, NULL,
         NULL},
        {"src/Part2.cs", "In", "Part2.UsesSplit", NULL, "missing", NULL, NULL},
        /* one project's Config is not the other's: each sees its own type
         * and its own type's members */
        {"src/P1/Config.cs", "Config", "P1.Config.UsesOne", "P1.Config.Config", NULL,
         "P2.Config.Config", NULL},
        {"src/P1/Config.cs", "Config.OnlyOne", "P1.Config.UsesOne", "P1.Config.Config.OnlyOne",
         NULL, NULL, NULL},
        {"src/P1/Config.cs", "Config.OnlyTwo", "P1.Config.UsesOne", NULL, "missing",
         "P2.Config.Config.OnlyTwo", NULL},
        {"src/P2/Config.cs", "Config", "P2.Config.UsesTwo", "P2.Config.Config", NULL,
         "P1.Config.Config", NULL},
        {"src/P2/Config.cs", "Config.OnlyOne", "P2.Config.UsesTwo", NULL, "missing",
         "P1.Config.Config.OnlyOne", NULL},
        /* a third project sees two types of that name: neither is chosen */
        {"src/P3/Uses.cs", "Config", "Uses.UsesThree", NULL, "ambiguous", "P1.Config.Config", NULL},
    };
    ASSERT_EQ(dm_check_repo("ent", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* An assembly is named by its project file: projects of one name are one
 * assembly. The parts they declare are one type; complete declarations in
 * two of them are flavours -- the referencing file's own project's binds, and
 * from anywhere else none is chosen. Decoys: a part in a project of another
 * name is no part of it, and a directory that holds two project files is an
 * assembly of its own, whatever they are called. */
TEST(doc_mentions_cs_assemblies_by_name) {
    static const dm_source_t files[] = {
        {"kit/win/Acme.Kit.csproj", DM_EMPTY_PROJECT},
        {"kit/win/EngineWin.cs",
         "namespace Acme.Kit\n"
         "{\n"
         "    public partial class Engine { public void Start() { } }\n"
         "    public class Clock { public void Tick() { } }\n"
         "\n"
         "    /// <summary><see cref=\"Engine.Stop\"/> <see cref=\"Engine.Halt\"/>\n"
         "    /// <see cref=\"Engine.Pair\"/> <see cref=\"Clock\"/> <see cref=\"Clock.Tick\"/>\n"
         "    /// </summary>\n"
         "    public class UsesWin { }\n"
         "}\n"},
        {"kit/unix/Acme.Kit.csproj", DM_EMPTY_PROJECT},
        {"kit/unix/EngineUnix.cs", "namespace Acme.Kit\n"
                                   "{\n"
                                   "    public partial class Engine { public void Stop() { } }\n"
                                   "    public class Clock { public void Tick() { } }\n"
                                   "}\n"},
        {"kit/tools/Acme.Kit.csproj", DM_EMPTY_PROJECT},
        {"kit/tools/Tools.cs",
         "namespace Acme.Kit\n"
         "{\n"
         "    /// <summary><see cref=\"Engine.Start\"/> <see cref=\"Clock\"/> <see "
         "cref=\"Clock.Tick\"/>\n"
         "    /// </summary>\n"
         "    public class UsesTools { }\n"
         "}\n"},
        {"other/Acme.Other.csproj", DM_EMPTY_PROJECT},
        {"other/EngineOther.cs", "namespace Acme.Kit\n"
                                 "{\n"
                                 "    public partial class Engine { public void Halt() { } }\n"
                                 "    public class Solo { }\n"
                                 "}\n"},
        {"pair/Acme.Kit.csproj", DM_EMPTY_PROJECT},
        {"pair/Second.csproj", DM_EMPTY_PROJECT},
        {"pair/EnginePair.cs", "namespace Acme.Kit\n"
                               "{\n"
                               "    public partial class Engine { public void Pair() { } }\n"
                               "}\n"},
        {"app/App.csproj", DM_EMPTY_PROJECT},
        {"app/UsesApp.cs",
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Acme.Kit.Clock\"/> <see cref=\"Acme.Kit.Solo\"/></summary>\n"
         "    public class UsesApp { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        /* the parts of two projects of one name are one type */
        {"kit/win/EngineWin.cs", "Engine.Stop", "EngineWin.UsesWin", "unix.EngineUnix.Engine.Stop",
         NULL, NULL, NULL},
        {"kit/tools/Tools.cs", "Engine.Start", "Tools.UsesTools", "win.EngineWin.Engine.Start",
         NULL, NULL, NULL},
        /* decoys: another name, and a directory with two project files */
        {"kit/win/EngineWin.cs", "Engine.Halt", "EngineWin.UsesWin", NULL, "missing",
         "EngineOther.Engine.Halt", NULL},
        {"kit/win/EngineWin.cs", "Engine.Pair", "EngineWin.UsesWin", NULL, "missing",
         "EnginePair.Engine.Pair", NULL},
        /* flavours: the own project's declaration and its member */
        {"kit/win/EngineWin.cs", "Clock", "EngineWin.UsesWin", "win.EngineWin.Clock", NULL,
         "unix.EngineUnix.Clock", NULL},
        {"kit/win/EngineWin.cs", "Clock.Tick", "EngineWin.UsesWin", "win.EngineWin.Clock.Tick",
         NULL, "unix.EngineUnix.Clock.Tick", NULL},
        /* ... and from a project that has none of them, of the same assembly
         * or of another, none is chosen */
        {"kit/tools/Tools.cs", "Clock", "Tools.UsesTools", NULL, "ambiguous", "win.EngineWin.Clock",
         NULL},
        {"kit/tools/Tools.cs", "Clock.Tick", "Tools.UsesTools", NULL, "ambiguous",
         "win.EngineWin.Clock.Tick", NULL},
        {"app/UsesApp.cs", "Acme.Kit.Clock", "UsesApp.UsesApp", NULL, "ambiguous",
         "unix.EngineUnix.Clock", NULL},
        /* what one other assembly declares binds */
        {"app/UsesApp.cs", "Acme.Kit.Solo", "UsesApp.UsesApp", "EngineOther.Solo", NULL, NULL,
         DM_EXACT},
    };
    ASSERT_EQ(dm_check_repo("asm", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* Inside one assembly a declaration in a project directory named `ref` -- a
 * reference assembly's source -- stands behind the implementation: the
 * implementation's node binds, the stub's only where the implementation has
 * none (here: `Hidden` and `Hidden<T>` share one node in the implementation's
 * file, and it is `Hidden<T>`'s). Decoy: a directory named `ref` inside a
 * project is no reference assembly, and what stands in it is no stub. */
TEST(doc_mentions_cs_reference_source) {
    static const dm_source_t files[] = {
        {"lib/ref/Acme.Lib.csproj", DM_EMPTY_PROJECT},
        {"lib/ref/Stubs.cs",
         "namespace Acme.Lib\n"
         "{\n"
         "    public partial class Widget { public void Spin() { } public void OnlyStub() { } }\n"
         "    public partial class Hidden { public void Peek() { } }\n"
         "}\n"},
        {"lib/src/Acme.Lib.csproj", DM_EMPTY_PROJECT},
        {"lib/src/Widget.cs", "namespace Acme.Lib\n"
                              "{\n"
                              "    public class Widget { public void Spin() { } }\n"
                              "}\n"},
        {"lib/src/Hidden.cs", "namespace Acme.Lib\n"
                              "{\n"
                              "    public class Hidden { public void Peek() { } }\n"
                              "    public class Hidden<T> { }\n"
                              "}\n"},
        {"lib/src/ref/Helper.cs", "namespace Acme.Lib\n"
                                  "{\n"
                                  "    public partial class Helper { public void Near() { } }\n"
                                  "\n"
                                  "    /// <summary><see cref=\"Helper\"/></summary>\n"
                                  "    public class UsesHelper { }\n"
                                  "}\n"},
        {"lib/src/HelperMore.cs", "namespace Acme.Lib\n"
                                  "{\n"
                                  "    public partial class Helper { public void Far() { } }\n"
                                  "}\n"},
        {"app/App.csproj", DM_EMPTY_PROJECT},
        {"app/UsesApp.cs",
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Acme.Lib.Widget\"/> <see cref=\"Acme.Lib.Widget.Spin\"/>\n"
         "    /// <see cref=\"Acme.Lib.Widget.OnlyStub\"/> <see cref=\"Acme.Lib.Hidden\"/>\n"
         "    /// <see cref=\"Acme.Lib.Hidden.Peek\"/></summary>\n"
         "    public class UsesApp { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        /* stub and implementation are one type: the implementation's nodes */
        {"app/UsesApp.cs", "Acme.Lib.Widget", "UsesApp.UsesApp", "src.Widget.Widget", NULL,
         "ref.Stubs.Widget", DM_EXACT},
        {"app/UsesApp.cs", "Acme.Lib.Widget.Spin", "UsesApp.UsesApp", "src.Widget.Widget.Spin",
         NULL, "ref.Stubs.Widget.Spin", NULL},
        /* the stub's, where the implementation has none */
        {"app/UsesApp.cs", "Acme.Lib.Widget.OnlyStub", "UsesApp.UsesApp",
         "ref.Stubs.Widget.OnlyStub", NULL, NULL, NULL},
        {"app/UsesApp.cs", "Acme.Lib.Hidden", "UsesApp.UsesApp", "ref.Stubs.Hidden", NULL,
         "src.Hidden.Hidden", NULL},
        {"app/UsesApp.cs", "Acme.Lib.Hidden.Peek", "UsesApp.UsesApp", "src.Hidden.Hidden.Peek",
         NULL, "ref.Stubs.Hidden.Peek", NULL},
        /* decoy: both parts are implementation, the nearest is the one beside
         * the reference */
        {"lib/src/ref/Helper.cs", "Helper", "ref.Helper.UsesHelper", "ref.Helper.Helper", NULL,
         "HelperMore.Helper", NULL},
    };
    ASSERT_EQ(dm_check_repo("refsrc", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* Declarations of one full name in projects of different names are different
 * types, and a project that declares none of them is not told which one it
 * references: nothing chooses -- not the one that is a stub, not the one that
 * is not, not the nearer directory. Decoys: the declaring assembly sees its
 * own type, and a name only one other assembly declares binds. */
TEST(doc_mentions_cs_assemblies_differ) {
    static const dm_source_t files[] = {
        {"impl/Acme.Impl.csproj", DM_EMPTY_PROJECT},
        {"impl/Gadget.cs", "namespace Acme.Things\n"
                           "{\n"
                           "    public class Gadget { public void Run() { } }\n"
                           "    public class OnlyImpl { }\n"
                           "\n"
                           "    /// <summary><see cref=\"Gadget\"/></summary>\n"
                           "    public class UsesImpl { }\n"
                           "}\n"},
        {"facade/ref/Acme.Facade.csproj", DM_EMPTY_PROJECT},
        {"facade/ref/Stubs.cs", "namespace Acme.Things\n"
                                "{\n"
                                "    public partial class Gadget { public void Run() { } }\n"
                                "}\n"},
        {"impl/near/Near.csproj", DM_EMPTY_PROJECT},
        {"impl/near/UsesNear.cs", "namespace Acme.Near\n"
                                  "{\n"
                                  "    /// <summary><see cref=\"Acme.Things.Gadget\"/> <see "
                                  "cref=\"Acme.Things.Gadget.Run\"/>\n"
                                  "    /// <see cref=\"Acme.Things.OnlyImpl\"/></summary>\n"
                                  "    public class UsesNear { }\n"
                                  "}\n"},
    };
    static const dm_want_t wants[] = {
        {"impl/near/UsesNear.cs", "Acme.Things.Gadget", "UsesNear.UsesNear", NULL, "ambiguous",
         "impl.Gadget.Gadget", NULL},
        {"impl/near/UsesNear.cs", "Acme.Things.Gadget", "UsesNear.UsesNear", NULL, "ambiguous",
         "ref.Stubs.Gadget", NULL},
        {"impl/near/UsesNear.cs", "Acme.Things.Gadget.Run", "UsesNear.UsesNear", NULL, "ambiguous",
         "impl.Gadget.Gadget.Run", NULL},
        {"impl/near/UsesNear.cs", "Acme.Things.OnlyImpl", "UsesNear.UsesNear",
         "impl.Gadget.OnlyImpl", NULL, NULL, DM_EXACT},
        {"impl/Gadget.cs", "Gadget", "Gadget.UsesImpl", "impl.Gadget.Gadget", NULL,
         "ref.Stubs.Gadget", NULL},
    };
    ASSERT_EQ(dm_check_repo("differ", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A file no project file stands above is of a shared tree, and the parts of
 * a partial type there are parts of every assembly's partial type of that
 * name: what a reference sees is its own assembly's parts and the shared
 * trees'. A part in a project of another name is no part of it, and a name
 * such a part declares is, seen without it, neither bound nor missing.
 * Decoy: a complete type takes no shared parts. */
TEST(doc_mentions_cs_shared_parts) {
    static const dm_source_t files[] = {
        {"shared/kit/ToolShared.cs",
         "namespace Acme.Kit\n"
         "{\n"
         "    public partial class Tool { public void Common() { } }\n"
         "    public partial class Gear\n"
         "    {\n"
         "        /// <summary><see cref=\"Spare\"/></summary>\n"
         "        public void Turn() { }\n"
         "        public void Mesh() { }\n"
         "    }\n"
         "    public class Spare { }\n"
         "\n"
         "    /// <summary><see cref=\"Gear.Turn\"/> <see cref=\"Gear.OnlyOne\"/></summary>\n"
         "    public class UsesShared { }\n"
         "}\n"},
        {"one/One.csproj", DM_EMPTY_PROJECT},
        {"one/ToolOne.cs", "namespace Acme.Kit\n"
                           "{\n"
                           "    public partial class Tool { public void OnlyOne() { } }\n"
                           "    public partial class Gear\n"
                           "    {\n"
                           "        public void OnlyOne() { }\n"
                           "        public void Mesh(int teeth) { }\n"
                           "        public int Spare;\n"
                           "        public class Inner { public void Deep() { } }\n"
                           "    }\n"
                           "\n"
                           "    /// <summary><see cref=\"Tool\"/> <see cref=\"Tool.Common\"/> <see "
                           "cref=\"Tool.OnlyOne\"/>\n"
                           "    /// <see cref=\"Tool.OnlyTwo\"/></summary>\n"
                           "    public class UsesOne { }\n"
                           "}\n"},
        {"two/Two.csproj", DM_EMPTY_PROJECT},
        {"two/ToolTwo.cs", "namespace Acme.Kit\n"
                           "{\n"
                           "    public partial class Tool { public void OnlyTwo() { } }\n"
                           "}\n"},
        {"three/Three.csproj", DM_EMPTY_PROJECT},
        {"three/UsesThree.cs",
         "namespace Acme.Kit\n"
         "{\n"
         "    /// <summary><see cref=\"Gear\"/> <see cref=\"Gear.Turn\"/> <see "
         "cref=\"Gear.OnlyOne\"/>\n"
         "    /// <see cref=\"Gear.Gone\"/> <see cref=\"Gear.Mesh\"/> <see "
         "cref=\"Gear.Inner.Deep\"/>\n"
         "    /// </summary>\n"
         "    public class UsesThree { }\n"
         "}\n"},
        {"four/Four.csproj", DM_EMPTY_PROJECT},
        {"four/ToolFour.cs",
         "namespace Acme.Kit\n"
         "{\n"
         "    public class Tool { public void OnlyFour() { } }\n"
         "\n"
         "    /// <summary><see cref=\"Tool.Common\"/> <see cref=\"Tool.OnlyFour\"/></summary>\n"
         "    public class UsesFour { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        /* an assembly's partial type: its own parts and the shared trees' */
        {"one/ToolOne.cs", "Tool", "ToolOne.UsesOne", "one.ToolOne.Tool", NULL, "two.ToolTwo.Tool",
         NULL},
        {"one/ToolOne.cs", "Tool.Common", "ToolOne.UsesOne", "kit.ToolShared.Tool.Common", NULL,
         NULL, NULL},
        {"one/ToolOne.cs", "Tool.OnlyOne", "ToolOne.UsesOne", "one.ToolOne.Tool.OnlyOne", NULL,
         NULL, NULL},
        /* ... and not another assembly's */
        {"one/ToolOne.cs", "Tool.OnlyTwo", "ToolOne.UsesOne", NULL, "missing",
         "two.ToolTwo.Tool.OnlyTwo", NULL},
        /* an assembly without parts of its own sees the shared parts */
        {"three/UsesThree.cs", "Gear", "UsesThree.UsesThree", "kit.ToolShared.Gear", NULL, NULL,
         NULL},
        {"three/UsesThree.cs", "Gear.Turn", "UsesThree.UsesThree", "kit.ToolShared.Gear.Turn", NULL,
         NULL, NULL},
        /* a name another assembly's part declares: not bound, not missing */
        {"three/UsesThree.cs", "Gear.OnlyOne", "UsesThree.UsesThree", NULL, "ambiguous",
         "one.ToolOne.Gear.OnlyOne", NULL},
        {"three/UsesThree.cs", "Gear.Gone", "UsesThree.UsesThree", NULL, "missing", NULL, NULL},
        /* ... also where the shared parts have a member of that name, and
         * where something further out has: the name is the part's first */
        {"three/UsesThree.cs", "Gear.Mesh", "UsesThree.UsesThree", NULL, "ambiguous",
         "kit.ToolShared.Gear.Mesh", NULL},
        {"shared/kit/ToolShared.cs", "Spare", "ToolShared.Gear.Turn", NULL, "ambiguous",
         "kit.ToolShared.Spare", NULL},
        /* ... and a type nested in such a part, on the way to its member */
        {"three/UsesThree.cs", "Gear.Inner.Deep", "UsesThree.UsesThree", NULL, "ambiguous",
         "one.ToolOne.Gear.Inner.Deep", NULL},
        /* ... and so from the shared tree itself */
        {"shared/kit/ToolShared.cs", "Gear.Turn", "ToolShared.UsesShared",
         "kit.ToolShared.Gear.Turn", NULL, NULL, NULL},
        {"shared/kit/ToolShared.cs", "Gear.OnlyOne", "ToolShared.UsesShared", NULL, "ambiguous",
         "one.ToolOne.Gear.OnlyOne", NULL},
        /* decoy: a complete type has no further parts */
        {"four/ToolFour.cs", "Tool.Common", "ToolFour.UsesFour", NULL, "missing",
         "kit.ToolShared.Tool.Common", NULL},
        {"four/ToolFour.cs", "Tool.OnlyFour", "ToolFour.UsesFour", "four.ToolFour.Tool.OnlyFour",
         NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("parts", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A shared tree is the unit of the files no project file stands above: the
 * largest directory around them with no project file in or below it. Its
 * files see its declarations as their own, whatever directory of it they
 * stand in; two trees' declarations of one name are not chosen between; one
 * tree's partial and complete declarations of a name are one type. Decoy: a
 * project is one unit whatever its directories. */
TEST(doc_mentions_cs_shared_trees) {
    static const dm_source_t files[] = {
        {"core/sys/Thing.cs", "namespace Acme.Sys\n"
                              "{\n"
                              "    public class Thing { public void InCore() { } }\n"
                              "    public class OnlyCore { }\n"
                              "    public partial class Part { public void Same() { } }\n"
                              "}\n"},
        {"core/sys/Wrap.cs", "namespace Acme.Sys\n"
                             "{\n"
                             "    public partial class Wrap\n"
                             "    {\n"
                             "        /// <summary><see cref=\"Wrap\"/></summary>\n"
                             "        public void Real() { }\n"
                             "    }\n"
                             "}\n"},
        {"core/sys/WrapOther.cs", "namespace Acme.Sys\n"
                                  "{\n"
                                  "    public class Wrap { public void Real() { } }\n"
                                  "}\n"},
        {"core/text/UsesCore.cs",
         "namespace Acme.Sys.Text\n"
         "{\n"
         "    /// <summary><see cref=\"Thing\"/> <see cref=\"Thing.InCore\"/> <see "
         "cref=\"Thing.InBase\"/>\n"
         "    /// <see cref=\"Part\"/> <see cref=\"Part.Same\"/></summary>\n"
         "    public class UsesCore { }\n"
         "}\n"},
        {"base/sys/Thing.cs", "namespace Acme.Sys\n"
                              "{\n"
                              "    public class Thing { public void InBase() { } }\n"
                              "    public partial class Part { public void Same() { } }\n"
                              "}\n"},
        {"base/UsesBase.cs", "namespace Acme.Sys\n"
                             "{\n"
                             "    /// <summary><see cref=\"Thing\"/></summary>\n"
                             "    public class UsesBase { }\n"
                             "}\n"},
        {"app/App.csproj", DM_EMPTY_PROJECT},
        {"app/UsesApp.cs",
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Acme.Sys.Thing\"/> <see cref=\"Acme.Sys.OnlyCore\"/>\n"
         "    /// <see cref=\"Acme.Sys.Part\"/> <see cref=\"Acme.Sys.Part.Same\"/></summary>\n"
         "    public class UsesApp { }\n"
         "}\n"},
        {"app/inner/Deep.cs", "namespace Acme.App\n{\n    public class Deep { }\n}\n"},
        {"app/other/UsesDeep.cs", "namespace Acme.App\n"
                                  "{\n"
                                  "    /// <summary><see cref=\"Deep\"/></summary>\n"
                                  "    public class UsesDeep { }\n"
                                  "}\n"},
    };
    static const dm_want_t wants[] = {
        /* its own tree's declaration, from another directory of the tree */
        {"core/text/UsesCore.cs", "Thing", "UsesCore.UsesCore", "core.sys.Thing.Thing", NULL,
         "base.sys.Thing.Thing", NULL},
        {"core/text/UsesCore.cs", "Thing.InCore", "UsesCore.UsesCore",
         "core.sys.Thing.Thing.InCore", NULL, NULL, NULL},
        {"core/text/UsesCore.cs", "Thing.InBase", "UsesCore.UsesCore", NULL, "missing",
         "base.sys.Thing.Thing.InBase", NULL},
        {"base/UsesBase.cs", "Thing", "UsesBase.UsesBase", "base.sys.Thing.Thing", NULL,
         "core.sys.Thing.Thing", NULL},
        /* parts in two trees: the own tree's part and member */
        {"core/text/UsesCore.cs", "Part", "UsesCore.UsesCore", "core.sys.Thing.Part", NULL,
         "base.sys.Thing.Part", NULL},
        {"core/text/UsesCore.cs", "Part.Same", "UsesCore.UsesCore", "core.sys.Thing.Part.Same",
         NULL, "base.sys.Thing.Part.Same", NULL},
        /* one tree's partial and complete declarations of a name: one type */
        {"core/sys/Wrap.cs", "Wrap", "Wrap.Wrap.Real", "core.sys.Wrap.Wrap", NULL, "WrapOther.Wrap",
         NULL},
        /* from outside the trees nothing is chosen between two of them */
        {"app/UsesApp.cs", "Acme.Sys.Thing", "UsesApp.UsesApp", NULL, "ambiguous",
         "core.sys.Thing.Thing", NULL},
        {"app/UsesApp.cs", "Acme.Sys.Part", "UsesApp.UsesApp", NULL, "ambiguous",
         "core.sys.Thing.Part", NULL},
        {"app/UsesApp.cs", "Acme.Sys.Part.Same", "UsesApp.UsesApp", NULL, "ambiguous",
         "core.sys.Thing.Part.Same", NULL},
        /* ... and what one tree declares binds */
        {"app/UsesApp.cs", "Acme.Sys.OnlyCore", "UsesApp.UsesApp", "core.sys.Thing.OnlyCore", NULL,
         NULL, DM_EXACT},
        /* decoy: the directories of a project are one unit */
        {"app/other/UsesDeep.cs", "Deep", "UsesDeep.UsesDeep", "inner.Deep.Deep", NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("trees", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A repository without any project file is one unit: a `global using` in one
 * directory serves the files of every other one. */
TEST(doc_mentions_cs_no_project_files) {
    static const dm_source_t files[] = {
        {"far/Far.cs", "namespace Acme.Far\n{\n    public class Remote { }\n}\n"},
        {"conf/Globals.cs", "global using Acme.Far;\n"},
        {"use/Uses.cs", "namespace Acme.Use\n"
                        "{\n"
                        "    /// <summary><see cref=\"Remote\"/></summary>\n"
                        "    public class Uses { }\n"
                        "}\n"},
    };
    static const dm_want_t wants[] = {
        {"use/Uses.cs", "Remote", "Uses.Uses", "far.Far.Remote", NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("noproj", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A contract and its one implementation are one type: an assembly whose
 * every declaration of a type stands in a project directory named `ref`, and
 * the one declaration of that name and arity the shared trees hold. The
 * implementation's node binds, the stub's only where the implementation has
 * none, and no binding that exists only through the join is exact. Decoys:
 * two shared trees declare the name; the assembly has a declaration that is
 * no stub; the arity differs. */
TEST(doc_mentions_cs_contract_join) {
    static const dm_source_t files[] = {
        {"shared/impl/Bag.cs", "namespace Acme.Coll\n"
                               "{\n"
                               "    public class Bag { public void Add() { } }\n"
                               "    public class Twice { }\n"
                               "    public class Mix { public void Shared() { } }\n"
                               "    public class Pair { }\n"
                               "    public partial class Spread { public void A() { } }\n"
                               "    public class Rivaled { }\n"
                               "}\n"},
        {"shared/impl/Ghost.cs", "namespace Acme.Coll\n"
                                 "{\n"
                                 "    public class Ghost { public void Boo() { } }\n"
                                 "    public class Ghost<T> { }\n"
                                 "}\n"},
        {"second/Twice.cs", "namespace Acme.Coll\n"
                            "{\n"
                            "    public class Twice { }\n"
                            "    public partial class Spread { public void B() { } }\n"
                            "}\n"},
        {"facade/ref/Acme.Facade.csproj", DM_EMPTY_PROJECT},
        {"facade/ref/Stubs.cs",
         "namespace Acme.Coll\n"
         "{\n"
         "    public partial class Bag { public void Add() { } public void OnlyStub() { } }\n"
         "    public partial class Twice { }\n"
         "    public partial class Pair<T> { }\n"
         "    public partial class Ghost { public void Boo() { } }\n"
         "    public partial class Spread { }\n"
         "    public partial class Rivaled { }\n"
         "\n"
         "    /// <summary><see cref=\"Bag\"/> <see cref=\"Bag.OnlyStub\"/> <see cref=\"Twice\"/>\n"
         "    /// <see cref=\"Spread\"/> <see cref=\"Rivaled\"/></summary>\n"
         "    public partial class UsesFacade { }\n"
         "\n"
         "    /// <summary><see cref=\"Acme.Coll.Bag\"/> <see cref=\"Acme.Coll.Bag.Add\"/>\n"
         "    /// </summary>\n"
         "    public partial class UsesFacadeFull { }\n"
         "}\n"},
        {"mixed/ref/Acme.Mixed.csproj", DM_EMPTY_PROJECT},
        {"mixed/ref/Stubs.cs", "namespace Acme.Coll\n"
                               "{\n"
                               "    public partial class Mix { public void Own() { } }\n"
                               "}\n"},
        {"mixed/src/Acme.Mixed.csproj", DM_EMPTY_PROJECT},
        {"mixed/src/Mix.cs",
         "namespace Acme.Coll\n"
         "{\n"
         "    public class Mix { public void Own() { } }\n"
         "\n"
         "    /// <summary><see cref=\"Mix.Own\"/> <see cref=\"Mix.Shared\"/></summary>\n"
         "    public class UsesMixed { }\n"
         "}\n"},
        {"rival/Acme.Rival.csproj", DM_EMPTY_PROJECT},
        {"rival/Rivaled.cs", "namespace Acme.Coll\n{\n    public class Rivaled { }\n}\n"},
        {"app/App.csproj", DM_EMPTY_PROJECT},
        {"app/UsesApp.cs",
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Acme.Coll.Bag\"/> <see cref=\"Acme.Coll.Bag.Add\"/>\n"
         "    /// <see cref=\"Acme.Coll.Twice\"/> <see cref=\"Acme.Coll.Mix\"/>\n"
         "    /// <see cref=\"Acme.Coll.Pair\"/> <see cref=\"Acme.Coll.Pair{T}\"/>\n"
         "    /// <see cref=\"Acme.Coll.Ghost\"/></summary>\n"
         "    public class UsesApp { }\n"
         "}\n"},
        {"app/UsesAlias.cs", "using B = Acme.Coll.Bag;\n"
                             "\n"
                             "namespace Acme.App\n"
                             "{\n"
                             "    /// <summary><see cref=\"B\"/></summary>\n"
                             "    public class UsesAlias { }\n"
                             "}\n"},
    };
    static const dm_want_t wants[] = {
        /* the stub is no second type: the implementation binds, and never as
         * an exact binding -- by its full name, by an alias */
        {"app/UsesApp.cs", "Acme.Coll.Bag", "UsesApp.UsesApp", "impl.Bag.Bag", NULL,
         "ref.Stubs.Bag", DM_UNIQUE},
        {"app/UsesApp.cs", "Acme.Coll.Bag.Add", "UsesApp.UsesApp", "impl.Bag.Bag.Add", NULL,
         "ref.Stubs.Bag.Add", DM_UNIQUE},
        {"app/UsesAlias.cs", "B", "UsesAlias.UsesAlias", "impl.Bag.Bag", NULL, NULL, DM_UNIQUE},
        /* from the contract's own assembly: the implementation's node, and
         * the stub's own member where the implementation has none */
        {"facade/ref/Stubs.cs", "Bag", "Stubs.UsesFacade", "impl.Bag.Bag", NULL, "ref.Stubs.Bag",
         DM_UNIQUE},
        {"facade/ref/Stubs.cs", "Bag.OnlyStub", "Stubs.UsesFacade", "ref.Stubs.Bag.OnlyStub", NULL,
         NULL, NULL},
        {"facade/ref/Stubs.cs", "Acme.Coll.Bag", "Stubs.UsesFacadeFull", "impl.Bag.Bag", NULL,
         "ref.Stubs.Bag", DM_UNIQUE},
        {"facade/ref/Stubs.cs", "Acme.Coll.Bag.Add", "Stubs.UsesFacadeFull", "impl.Bag.Bag.Add",
         NULL, "ref.Stubs.Bag.Add", DM_UNIQUE},
        /* the implementation without a node: the stub's node stands in */
        {"app/UsesApp.cs", "Acme.Coll.Ghost", "UsesApp.UsesApp", "ref.Stubs.Ghost", NULL,
         "impl.Ghost.Ghost", DM_UNIQUE},
        /* decoy: two shared trees declare the name -- complete in each, or a
         * part in each: no join, and the contract's assembly sees its stub */
        {"app/UsesApp.cs", "Acme.Coll.Twice", "UsesApp.UsesApp", NULL, "ambiguous",
         "impl.Bag.Twice", NULL},
        {"facade/ref/Stubs.cs", "Twice", "Stubs.UsesFacade", "ref.Stubs.Twice", NULL,
         "impl.Bag.Twice", NULL},
        {"facade/ref/Stubs.cs", "Spread", "Stubs.UsesFacade", "ref.Stubs.Spread", NULL,
         "impl.Bag.Spread", NULL},
        /* decoy: another assembly holds an implementation of the name too --
         * the contract is not joined to one of two implementations */
        {"facade/ref/Stubs.cs", "Rivaled", "Stubs.UsesFacade", "ref.Stubs.Rivaled", NULL,
         "impl.Bag.Rivaled", NULL},
        /* decoy: the assembly has a declaration that is no stub */
        {"app/UsesApp.cs", "Acme.Coll.Mix", "UsesApp.UsesApp", NULL, "ambiguous", "impl.Bag.Mix",
         NULL},
        {"mixed/src/Mix.cs", "Mix.Own", "Mix.UsesMixed", "src.Mix.Mix.Own", NULL, NULL, NULL},
        {"mixed/src/Mix.cs", "Mix.Shared", "Mix.UsesMixed", NULL, "missing", "impl.Bag.Mix.Shared",
         NULL},
        /* decoy: another arity is another type -- each is the only one of its
         * arity, and bound without the join */
        {"app/UsesApp.cs", "Acme.Coll.Pair", "UsesApp.UsesApp", "impl.Bag.Pair", NULL, NULL,
         DM_EXACT},
        {"app/UsesApp.cs", "Acme.Coll.Pair{T}", "UsesApp.UsesApp", "ref.Stubs.Pair", NULL, NULL,
         DM_EXACT},
    };
    ASSERT_EQ(dm_check_repo("join", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* A project file of an SDK that compiles nothing (NoTargets: it only runs
 * build steps) is the project of no source file: the sources below it that
 * have no nearer project are a shared tree, and a contract finds its one
 * implementation there. Decoy: a project file that compiles keeps the files
 * below it, and their types are that assembly's. */
TEST(doc_mentions_cs_idle_project_file) {
    static const dm_source_t files[] = {
        {"libs/build.csproj", "<Project Sdk=\"Microsoft.Build.NoTargets/3.7.0\">\n</Project>\n"},
        {"libs/core/src/Impl.cs", "namespace Acme.Core\n"
                                  "{\n"
                                  "    public class Thing { public void Go() { } }\n"
                                  "}\n"},
        {"libs/core/ref/Acme.Core.csproj", DM_EMPTY_PROJECT},
        {"libs/core/ref/Stubs.cs", "namespace Acme.Core\n"
                                   "{\n"
                                   "    public partial class Thing { public void Go() { } }\n"
                                   "}\n"},
        {"libs/user/User.csproj", DM_EMPTY_PROJECT},
        {"libs/user/Uses.cs",
         "namespace Acme.User\n"
         "{\n"
         "    /// <summary><see cref=\"Acme.Core.Thing\"/> <see cref=\"Acme.Apps.Gizmo\"/>\n"
         "    /// </summary>\n"
         "    public class Uses { }\n"
         "}\n"},
        {"apps/Apps.csproj", DM_EMPTY_PROJECT},
        {"apps/tool/src/Impl.cs", "namespace Acme.Apps\n{\n    public class Gizmo { }\n}\n"},
        {"apps/tool/ref/Tool.csproj", DM_EMPTY_PROJECT},
        {"apps/tool/ref/Stubs.cs", "namespace Acme.Apps\n"
                                   "{\n"
                                   "    public partial class Gizmo { }\n"
                                   "}\n"},
    };
    static const dm_want_t wants[] = {
        {"libs/user/Uses.cs", "Acme.Core.Thing", "Uses.Uses", "core.src.Impl.Thing", NULL,
         "core.ref.Stubs.Thing", DM_UNIQUE},
        {"libs/user/Uses.cs", "Acme.Apps.Gizmo", "Uses.Uses", NULL, "ambiguous",
         "tool.src.Impl.Gizmo", NULL},
    };
    ASSERT_EQ(dm_check_repo("idle", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* For product code a namespace that only test code declares does not exist:
 * it does not stand in the way of a type a using brings in, and a name that
 * is not under it is judged without it -- while a name that is under it is
 * test code's (test_only_target). Decoys: a namespace product code declares
 * does stand there (the global namespace's own names come before the file's
 * usings), and test code sees the test namespace. */
TEST(doc_mentions_cs_test_namespaces) {
    static const dm_source_t files[] = {
        {"src/Lib/Widget.cs", "namespace Acme.Lib\n"
                              "{\n"
                              "    public class Widget { }\n"
                              "    public class Gadget { }\n"
                              "}\n"},
        {"src/App/Uses.cs",
         "using Acme.Lib;\n"
         "\n"
         "namespace Acme.App\n"
         "{\n"
         "    /// <summary><see cref=\"Widget\"/> <see cref=\"Gadget\"/></summary>\n"
         "    public class Uses { }\n"
         "\n"
         "    /// <summary><see cref=\"Probe.Rig\"/> <see cref=\"Probe.Nothing\"/>\n"
         "    /// <see cref=\"Acme.Lib.Extra.Bonus\"/> <see cref=\"Acme.Lib.Extra.Nothing\"/>\n"
         "    /// <see cref=\"T:Probe.Rig\"/></summary>\n"
         "    public class UsesFull { }\n"
         "}\n"},
        {"tests/Widget/WidgetTests.cs", "namespace Widget\n{\n    public class Fixture { }\n}\n"},
        {"tests/Probe/Rig.cs", "namespace Probe\n{\n    public class Rig { }\n}\n"},
        {"tests/Lib/Extra.cs", "namespace Acme.Lib.Extra\n{\n    public class Bonus { }\n}\n"},
        {"src/Gadget/Kinds.cs", "namespace Gadget\n{\n    public class Kind { }\n}\n"},
        {"tests/App/UsesTests.cs", "using Acme.Lib;\n"
                                   "\n"
                                   "namespace Acme.App.Tests\n"
                                   "{\n"
                                   "    /// <summary><see cref=\"Widget\"/></summary>\n"
                                   "    public class UsesTests { }\n"
                                   "}\n"},
    };
    static const dm_want_t wants[] = {
        {"src/App/Uses.cs", "Widget", "Uses.Uses", "Lib.Widget.Widget", NULL, NULL, NULL},
        {"src/App/Uses.cs", "Gadget", "Uses.Uses", NULL, "graph_gap", "Lib.Widget.Gadget", NULL},
        {"tests/App/UsesTests.cs", "Widget", "UsesTests.UsesTests", NULL, "graph_gap",
         "Lib.Widget.Widget", NULL},
        /* through a test-only namespace: what is there is test code's ... */
        {"src/App/Uses.cs", "Probe.Rig", "Uses.UsesFull", NULL, "test_only_target", "Probe.Rig.Rig",
         NULL},
        {"src/App/Uses.cs", "Acme.Lib.Extra.Bonus", "Uses.UsesFull", NULL, "test_only_target",
         "Lib.Extra.Bonus", NULL},
        {"src/App/Uses.cs", "T:Probe.Rig", "Uses.UsesFull", NULL, "test_only_target",
         "Probe.Rig.Rig", NULL},
        /* ... and what is not there is judged as if the namespace were not:
         * no namespace of the repository, and a namespace without the name */
        {"src/App/Uses.cs", "Probe.Nothing", "Uses.UsesFull", NULL, "external", NULL, NULL},
        {"src/App/Uses.cs", "Acme.Lib.Extra.Nothing", "Uses.UsesFull", NULL, "missing", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("testns", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* Members: the type that has the name decides, and the written parameter
 * list is matched against ITS overloads -- exactly first, a type variable by
 * its position, a type nothing is known about only when nothing matches
 * exactly. Operators and indexers are declared or they are not. */
TEST(doc_mentions_cs_members) {
    static const dm_source_t files[] = {
        {"src/Members.cs",
         "namespace Acme.M\n"
         "{\n"
         "    public class Value { public Value(int x) { } }\n"
         "    public class Two { public Two(long x) { } }\n"
         "\n"
         "    public class Box<T>\n"
         "    {\n"
         "        public void Put(T x) { }\n"
         "        public void Two(int a) { }\n"
         "        public void Two(string a) { }\n"
         "        public void Gen() { }\n"
         "        public void Gen<U>() { }\n"
         "        public void Dup(int a) { }\n"
         "        public void Dup<U>(int a) { }\n"
         "        public void OnlyGen<U>(U u) { }\n"
         "        public void Map<K, V>(K key, V value) { }\n"
         "        public void Loose(Unknown.Thing<int> x) { }\n"
         "        public int Value { get; set; }\n"
         "        public int field;\n"
         "        public event System.Action Changed;\n"
         "        public int this[int i] { get { return i; } }\n"
         "        public static Box<T> operator +(Box<T> a, Box<T> b) { return a; }\n"
         "        public void Item(string key) { }\n"
         "\n"
         "        /// <summary>\n"
         "        /// <see cref=\"Put(T)\"/> <see cref=\"Put(int)\"/> <see cref=\"Two(int)\"/>\n"
         "        /// <see cref=\"Two(long)\"/> <see cref=\"Value(int)\"/>\n"
         "        /// <see cref=\"Gen\"/> <see cref=\"Gen{U}\"/> <see cref=\"OnlyGen\"/>\n"
         "        /// <see cref=\"Dup(int)\"/> <see cref=\"Dup{U}(int)\"/> <see cref=\"Dup\"/>\n"
         "        /// <see cref=\"Map{A, B}(A, B)\"/> <see cref=\"Map{A, B}(B, A)\"/>\n"
         "        /// <see cref=\"Loose(Thing{int})\"/> <see cref=\"Loose(Other)\"/>\n"
         "        /// <see cref=\"get_Value\"/> <see cref=\"get_field\"/> <see "
         "cref=\"add_Changed\"/>\n"
         "        /// <see cref=\"Item(string)\"/> <see cref=\"Item(int)\"/>\n"
         "        /// <see cref=\"this[int]\"/> <see cref=\"operator +\"/> <see "
         "cref=\"operator -\"/>\n"
         "        /// </summary>\n"
         "        public void Doc() { }\n"
         "    }\n"
         "}\n"},
        {"src/Plain.cs",
         "namespace Acme.M\n"
         "{\n"
         "    public class Plain\n"
         "    {\n"
         "        /// <summary><see cref=\"this[int]\"/> <see cref=\"operator +\"/>\n"
         "        /// <see cref=\"Box{T}.this[int]\"/> <see cref=\"Box{T}.operator +\"/>\n"
         "        /// <see cref=\"Plain.this[int]\"/> <see cref=\"Box{T}.get_Value\"/>\n"
         "        /// <see cref=\"Box{T}.get_field\"/></summary>\n"
         "        public void Doc() { }\n"
         "    }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        {"src/Members.cs", "Put(T)", "Members.Box.Doc", "Members.Box.Put", NULL, NULL, NULL},
        /* the type has `Put`, and no overload of it takes an int */
        {"src/Members.cs", "Put(int)", "Members.Box.Doc", NULL, "missing", NULL, NULL},
        {"src/Members.cs", "Two(int)", "Members.Box.Doc", "Members.Box.Two", NULL, NULL, NULL},
        /* ... and nothing further out is asked: not the class Two, whose
         * constructor takes a long, and not the class Value for a property */
        {"src/Members.cs", "Two(long)", "Members.Box.Doc", NULL, "missing", "Members.Two.Two",
         NULL},
        {"src/Members.cs", "Value(int)", "Members.Box.Doc", NULL, "missing", "Members.Value.Value",
         NULL},
        /* without type arguments a non-generic method stands before a generic one */
        {"src/Members.cs", "Gen", "Members.Box.Doc", "Members.Box.Gen", NULL, NULL, NULL},
        {"src/Members.cs", "Gen{U}", "Members.Box.Doc", "Members.Box.Gen", NULL, NULL, NULL},
        {"src/Members.cs", "OnlyGen", "Members.Box.Doc", "Members.Box.OnlyGen", NULL, NULL, NULL},
        /* ... with a parameter list too: both take an int, and that is no ambiguity */
        {"src/Members.cs", "Dup(int)", "Members.Box.Doc", "Members.Box.Dup", NULL, NULL, NULL},
        {"src/Members.cs", "Dup{U}(int)", "Members.Box.Doc", "Members.Box.Dup", NULL, NULL, NULL},
        {"src/Members.cs", "Dup", "Members.Box.Doc", "Members.Box.Dup", NULL, NULL, NULL},
        /* a type variable is its position, whatever it is called */
        {"src/Members.cs", "Map{A, B}(A, B)", "Members.Box.Doc", "Members.Box.Map", NULL, NULL,
         NULL},
        {"src/Members.cs", "Map{A, B}(B, A)", "Members.Box.Doc", NULL, "missing", NULL, NULL},
        /* the declared parameter type is the same written text */
        {"src/Members.cs", "Loose(Thing{int})", "Members.Box.Doc", "Members.Box.Loose", NULL, NULL,
         NULL},
        {"src/Members.cs", "Loose(Other)", "Members.Box.Doc", NULL, "missing", NULL, NULL},
        /* an accessor names its property or its event, nothing else */
        {"src/Members.cs", "get_Value", "Members.Box.Doc", "Members.Box.Value", NULL, NULL, NULL},
        {"src/Members.cs", "get_field", "Members.Box.Doc", NULL, "missing", "Members.Box.field",
         NULL},
        {"src/Members.cs", "add_Changed", "Members.Box.Doc", NULL, "graph_gap", NULL, NULL},
        /* a method named Item is a method; without one, `Item(int)` is how
         * a doc ID spells the indexer */
        {"src/Members.cs", "Item(string)", "Members.Box.Doc", "Members.Box.Item", NULL, NULL, NULL},
        {"src/Members.cs", "Item(int)", "Members.Box.Doc", NULL, "missing", NULL, NULL},
        /* declared operators and indexers have no node: a gap */
        {"src/Members.cs", "this[int]", "Members.Box.Doc", NULL, "graph_gap", NULL, NULL},
        {"src/Members.cs", "operator +", "Members.Box.Doc", NULL, "graph_gap", NULL, NULL},
        /* ... and one the type does not declare is not there */
        {"src/Members.cs", "operator -", "Members.Box.Doc", NULL, "missing", NULL, NULL},
        {"src/Plain.cs", "this[int]", "Plain.Plain.Doc", NULL, "missing", NULL, NULL},
        {"src/Plain.cs", "operator +", "Plain.Plain.Doc", NULL, "missing", NULL, NULL},
        {"src/Plain.cs", "Plain.this[int]", "Plain.Plain.Doc", NULL, "missing", NULL, NULL},
        {"src/Plain.cs", "Box{T}.this[int]", "Plain.Plain.Doc", NULL, "graph_gap", NULL, NULL},
        {"src/Plain.cs", "Box{T}.operator +", "Plain.Plain.Doc", NULL, "graph_gap", NULL, NULL},
        /* through the type's name too: a property has accessors, a field has none */
        {"src/Plain.cs", "Box{T}.get_Value", "Plain.Plain.Doc", "Members.Box.Value", NULL, NULL,
         NULL},
        {"src/Plain.cs", "Box{T}.get_field", "Plain.Plain.Doc", NULL, "missing",
         "Members.Box.field", NULL},
    };
    ASSERT_EQ(dm_check_repo("members", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    /* `Item(int)` is written twice in Plain.cs with two outcomes, so it is
     * asked per definition: nothing declared (missing) and an indexer (gap) */
    static const dm_source_t item_files[] = {
        {"src/NoIndexer.cs", "namespace Acme.M\n"
                             "{\n"
                             "    public class NoIndexer\n"
                             "    {\n"
                             "        /// <summary><see cref=\"Item(int)\"/></summary>\n"
                             "        public void Doc() { }\n"
                             "    }\n"
                             "}\n"},
        {"src/Indexer.cs", "namespace Acme.M\n"
                           "{\n"
                           "    public class Indexer\n"
                           "    {\n"
                           "        public int this[int i] { get { return i; } }\n"
                           "\n"
                           "        /// <summary><see cref=\"Item(int)\"/></summary>\n"
                           "        public void Doc() { }\n"
                           "    }\n"
                           "}\n"},
    };
    static const dm_want_t item_wants[] = {
        {"src/NoIndexer.cs", "Item(int)", "NoIndexer.NoIndexer.Doc", NULL, "missing", NULL, NULL},
        {"src/Indexer.cs", "Item(int)", "Indexer.Indexer.Doc", NULL, "graph_gap", NULL, NULL},
    };
    ASSERT_EQ(
        dm_check_repo("item", item_files, DM_COUNT(item_files), item_wants, DM_COUNT(item_wants)),
        0);
    PASS();
}

/* Constructors: a parameter list on a type's name, however the type is
 * named; `Foo.Foo`; and `Foo(int)` written inside a generic `Foo<T>`. The
 * constructors no source writes are declared and have no node. */
TEST(doc_mentions_cs_constructors) {
    static const dm_source_t files[] = {
        {"src/Ctors.cs",
         "namespace Acme.C\n"
         "{\n"
         "    public class Widget { }\n"
         "    public class Gadget\n"
         "    {\n"
         "        public Gadget(int size) { }\n"
         "        public class Part { public Part(string s) { } }\n"
         "    }\n"
         "    public record Rec(int Width);\n"
         "    public class Prim(int seed) { }\n"
         "    public struct Point { public Point(int x) { } }\n"
         "    public interface IThing { }\n"
         "\n"
         "    public class Gen<T>\n"
         "    {\n"
         "        public Gen(T first) { }\n"
         "\n"
         "        /// <summary><see cref=\"Gen(T)\"/> <see cref=\"Gen\"/> <see "
         "cref=\"Gen{T}.Gen\"/>\n"
         "        /// </summary>\n"
         "        public void Doc() { }\n"
         "    }\n"
         "\n"
         "    /// <summary>\n"
         "    /// <see cref=\"Widget()\"/> <see cref=\"Widget(int)\"/> <see cref=\"Gadget()\"/>\n"
         "    /// <see cref=\"Rec(int)\"/> <see cref=\"Rec(Rec)\"/> <see cref=\"Prim(int)\"/>\n"
         "    /// <see cref=\"Point()\"/> <see cref=\"Point.Point\"/> <see cref=\"IThing()\"/>\n"
         "    /// </summary>\n"
         "    public class Rows { }\n"
         "\n"
         "    /// <summary><see cref=\"Gadget(int)\"/></summary>\n"
         "    public class Simple { }\n"
         "\n"
         "    /// <summary><see cref=\"Acme.C.Gadget(int)\"/></summary>\n"
         "    public class Qualified { }\n"
         "\n"
         "    /// <summary><see cref=\"Gadget.Part(string)\"/></summary>\n"
         "    public class Nested { }\n"
         "\n"
         "    /// <summary><see cref=\"Gadget.Gadget\"/></summary>\n"
         "    public class ByName { }\n"
         "\n"
         "    /// <summary><see cref=\"Point(int)\"/></summary>\n"
         "    public class OfStruct { }\n"
         "}\n"},
        {"src/Outside.cs", "namespace Acme.C\n"
                           "{\n"
                           "    /// <summary><see cref=\"Gen{T}.Gen\"/></summary>\n"
                           "    public class Outside { }\n"
                           "}\n"},
    };
    static const dm_want_t wants[] = {
        /* a class that writes no constructor has one: declared, no node */
        {"src/Ctors.cs", "Widget()", "Ctors.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ctors.cs", "Widget(int)", "Ctors.Rows", NULL, "missing", NULL, NULL},
        /* ... and a class that writes one has no other */
        {"src/Ctors.cs", "Gadget()", "Ctors.Rows", NULL, "missing", NULL, NULL},
        /* primary constructors and a record's copy constructor: declared, no node */
        {"src/Ctors.cs", "Rec(int)", "Ctors.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ctors.cs", "Rec(Rec)", "Ctors.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ctors.cs", "Prim(int)", "Ctors.Rows", NULL, "graph_gap", NULL, NULL},
        /* a struct keeps its parameterless constructor beside the one it writes */
        {"src/Ctors.cs", "Point()", "Ctors.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ctors.cs", "Point.Point", "Ctors.Rows", NULL, "ambiguous", NULL, NULL},
        {"src/Ctors.cs", "IThing()", "Ctors.Rows", NULL, "missing", NULL, NULL},
        {"src/Ctors.cs", "Gadget(int)", "Ctors.Simple", "Ctors.Gadget.Gadget", NULL, NULL, NULL},
        /* the type may be named by any path */
        {"src/Ctors.cs", "Acme.C.Gadget(int)", "Ctors.Qualified", "Ctors.Gadget.Gadget", NULL, NULL,
         DM_EXACT},
        {"src/Ctors.cs", "Gadget.Part(string)", "Ctors.Nested", "Ctors.Gadget.Part.Part", NULL,
         NULL, NULL},
        {"src/Ctors.cs", "Gadget.Gadget", "Ctors.ByName", "Ctors.Gadget.Gadget", NULL, NULL, NULL},
        {"src/Ctors.cs", "Point(int)", "Ctors.OfStruct", "Ctors.Point.Point", NULL, NULL, NULL},
        /* inside Gen<T>: `Gen(T)` is its constructor although no type `Gen`
         * without type arguments is in scope; a bare `Gen` names nothing */
        {"src/Ctors.cs", "Gen(T)", "Ctors.Gen.Doc", "Ctors.Gen.Gen", NULL, NULL, NULL},
        {"src/Ctors.cs", "Gen", "Ctors.Gen.Doc", NULL, "missing", "Ctors.Gen", NULL},
        /* `Gen{T}.Gen` is the constructor from outside the type; written on the
         * generic type itself, without parentheses, the compiler binds nothing */
        {"src/Outside.cs", "Gen{T}.Gen", "Outside.Outside", "Ctors.Gen.Gen", NULL, NULL, NULL},
        {"src/Ctors.cs", "Gen{T}.Gen", "Ctors.Gen.Doc", NULL, "missing", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("ctors", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* The compiler looks up no inherited member in a cref: not through a class's
 * base class, not through an interface's base interface, not through the
 * roots every type has. A member is named through the type that declares it. */
TEST(doc_mentions_cs_inherited_members) {
    static const dm_source_t files[] = {
        {"src/Sys.cs",
         "namespace System\n"
         "{\n"
         "    public class Object { public virtual string ToString() { return null; } }\n"
         "    public class ValueType { public string Kind() { return null; } }\n"
         "}\n"},
        {"src/Inherit.cs",
         "namespace Acme.I\n"
         "{\n"
         "    public class Base\n"
         "    {\n"
         "        public void Run() { }\n"
         "        public void Put(int x) { }\n"
         "        public class Nested { }\n"
         "    }\n"
         "    public interface IBase { void Ping(); }\n"
         "    public interface IDerived : IBase { void Own(); }\n"
         "    public class Ping { }\n"
         "\n"
         "    /// <summary><see cref=\"Base.Run\"/> <see cref=\"Ping\"/></summary>\n"
         "    public class Derived : Base, IBase\n"
         "    {\n"
         "        void IBase.Ping() { }\n"
         "        public void Put(string s) { }\n"
         "\n"
         "        /// <summary><see cref=\"Run\"/> <see cref=\"Derived.Run\"/> <see "
         "cref=\"Nested\"/>\n"
         "        /// <see cref=\"Put(int)\"/> <see cref=\"ToString\"/></summary>\n"
         "        public void Doc() { }\n"
         "    }\n"
         "\n"
         "    /// <summary><see cref=\"IDerived.Own\"/> <see cref=\"IBase.Ping\"/></summary>\n"
         "    public class Declared { }\n"
         "\n"
         "    /// <summary><see cref=\"IDerived.Ping\"/></summary>\n"
         "    public class ThroughInterface { }\n"
         "\n"
         "    public record class RecC(int A);\n"
         "\n"
         "    /// <summary><see cref=\"RecC.ToString\"/> <see cref=\"RecC.Kind\"/> <see "
         "cref=\"RecC.A\"/>\n"
         "    /// </summary>\n"
         "    public class OfRecord { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        /* through the declaring type: bound */
        {"src/Inherit.cs", "Base.Run", "Inherit.Derived", "Inherit.Base.Run", NULL, NULL, NULL},
        /* a class finds no member of an interface it implements: `Ping` is
         * the class of that name */
        {"src/Inherit.cs", "Ping", "Inherit.Derived", "Inherit.Ping", NULL, "Inherit.IBase.Ping",
         NULL},
        /* by a simple name, and through the derived type's name: not bound */
        {"src/Inherit.cs", "Run", "Inherit.Derived.Doc", NULL, "missing", "Inherit.Base.Run", NULL},
        {"src/Inherit.cs", "Derived.Run", "Inherit.Derived.Doc", NULL, "missing", NULL, NULL},
        {"src/Inherit.cs", "Nested", "Inherit.Derived.Doc", NULL, "missing", "Inherit.Base.Nested",
         NULL},
        /* the type has `Put`; the overload asked for is its base class's */
        {"src/Inherit.cs", "Put(int)", "Inherit.Derived.Doc", NULL, "missing", "Inherit.Base.Put",
         NULL},
        {"src/Inherit.cs", "ToString", "Inherit.Derived.Doc", NULL, "missing",
         "Sys.Object.ToString", NULL},
        /* an interface's own members, and not its base interface's */
        {"src/Inherit.cs", "IDerived.Own", "Inherit.Declared", "Inherit.IDerived.Own", NULL, NULL,
         NULL},
        {"src/Inherit.cs", "IBase.Ping", "Inherit.Declared", "Inherit.IBase.Ping", NULL, NULL,
         NULL},
        {"src/Inherit.cs", "IDerived.Ping", "Inherit.ThroughInterface", NULL, "missing",
         "Inherit.IBase.Ping", NULL},
        /* a record class is no value type, and its roots are not searched */
        {"src/Inherit.cs", "RecC.ToString", "Inherit.OfRecord", NULL, "missing",
         "Sys.Object.ToString", NULL},
        {"src/Inherit.cs", "RecC.Kind", "Inherit.OfRecord", NULL, "missing", "Sys.ValueType.Kind",
         NULL},
        {"src/Inherit.cs", "RecC.A", "Inherit.OfRecord", "Inherit.RecC.A", NULL, NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("inherit", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* Doc IDs: a full name from the global namespace, the letter saying what it
 * names. `M:` without parentheses is the overload without parameters; a type
 * parameter is written as its position. */
TEST(doc_mentions_cs_doc_ids) {
    static const dm_source_t files[] = {
        {"src/Ids.cs",
         "namespace Acme.D\n"
         "{\n"
         "    public class Outer\n"
         "    {\n"
         "        public class Inner { }\n"
         "        public void Go() { }\n"
         "        public void Go(int n) { }\n"
         "        public void Take<T>(T item, int n) { }\n"
         "        public void Take<T>(int n, T item) { }\n"
         "        public int Prop { get; set; }\n"
         "        public int Fld;\n"
         "        public event System.Action Evt;\n"
         "        public Outer() { }\n"
         "        public Outer(string s) { }\n"
         "        static Outer() { }\n"
         "        public int this[int i] { get { return i; } }\n"
         "        public static Outer operator +(Outer a, Outer b) { return a; }\n"
         "    }\n"
         "    public class Box<T>\n"
         "    {\n"
         "        public void Put(T item) { }\n"
         "        public void Put(int n) { }\n"
         "        public class In<U> { public void Both(T a, U b) { } }\n"
         "    }\n"
         "\n"
         "    /// <summary>\n"
         "    /// <see cref=\"T:Acme.D.Nope\"/> <see cref=\"T:Other.Thing\"/> <see "
         "cref=\"T:Acme.D.Box\"/>\n"
         "    /// <see cref=\"N:Acme.D\"/> <see cref=\"N:Acme.Nope\"/> <see cref=\"T:Acme.D\"/>\n"
         "    /// <see cref=\"M:Acme.D.Outer.Go(System.String)\"/> <see "
         "cref=\"E:Acme.D.Outer.Evt\"/>\n"
         "    /// <see cref=\"F:Acme.D.Outer.Prop\"/> <see "
         "cref=\"P:Acme.D.Outer.Item(System.Int32)\"/>\n"
         "    /// <see cref=\"M:Acme.D.Box`1.In`1.Both(`1,`0)\"/>\n"
         "    /// <see cref=\"M:Acme.D.Outer.op_Addition(Acme.D.Outer,Acme.D.Outer)\"/>\n"
         "    /// <see cref=\"M:Acme.D.Outer.op_Subtraction(Acme.D.Outer,Acme.D.Outer)\"/>\n"
         "    /// </summary>\n"
         "    public class Rows { }\n"
         "\n"
         "    /// <summary><see cref=\"T:Acme.D.Outer\"/></summary>\n"
         "    public class OfType { }\n"
         "    /// <summary><see cref=\"T:Acme.D.Outer.Inner\"/></summary>\n"
         "    public class OfNested { }\n"
         "    /// <summary><see cref=\"T:Acme.D.Box`1\"/></summary>\n"
         "    public class OfGeneric { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Outer.Go\"/></summary>\n"
         "    public class OfMethod { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Outer.Go(System.Int32)\"/></summary>\n"
         "    public class OfOverload { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Outer.Take``1(``0,System.Int32)\"/></summary>\n"
         "    public class OfGenericMethod { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Outer.#ctor(System.String)\"/></summary>\n"
         "    public class OfCtor { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Outer.#cctor\"/></summary>\n"
         "    public class OfStaticCtor { }\n"
         "    /// <summary><see cref=\"P:Acme.D.Outer.Prop\"/></summary>\n"
         "    public class OfProperty { }\n"
         "    /// <summary><see cref=\"F:Acme.D.Outer.Fld\"/></summary>\n"
         "    public class OfField { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Box`1.Put(`0)\"/></summary>\n"
         "    public class OfSlot { }\n"
         "    /// <summary><see cref=\"M:Acme.D.Box`1.In`1.Both(`0,`1)\"/></summary>\n"
         "    public class OfNestedSlots { }\n"
         "}\n"},
    };
    static const dm_want_t wants[] = {
        {"src/Ids.cs", "T:Acme.D.Outer", "Ids.OfType", "Ids.Outer", NULL, NULL, DM_EXACT},
        {"src/Ids.cs", "T:Acme.D.Outer.Inner", "Ids.OfNested", "Ids.Outer.Inner", NULL, NULL,
         DM_EXACT},
        {"src/Ids.cs", "T:Acme.D.Box`1", "Ids.OfGeneric", "Ids.Box", NULL, NULL, DM_EXACT},
        /* the namespace is the repository's: the type is not there */
        {"src/Ids.cs", "T:Acme.D.Nope", "Ids.Rows", NULL, "missing", NULL, NULL},
        {"src/Ids.cs", "T:Acme.D.Box", "Ids.Rows", NULL, "missing", "Ids.Box", NULL},
        {"src/Ids.cs", "T:Acme.D", "Ids.Rows", NULL, "missing", NULL, NULL},
        /* ... a namespace the repository does not declare is outside */
        {"src/Ids.cs", "T:Other.Thing", "Ids.Rows", NULL, "external", NULL, NULL},
        {"src/Ids.cs", "N:Acme.D", "Ids.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ids.cs", "N:Acme.Nope", "Ids.Rows", NULL, "external", NULL, NULL},
        /* `M:` without parentheses: the overload without parameters, not the group */
        {"src/Ids.cs", "M:Acme.D.Outer.Go", "Ids.OfMethod", "Ids.Outer.Go", NULL, NULL, DM_EXACT},
        {"src/Ids.cs", "M:Acme.D.Outer.Go(System.Int32)", "Ids.OfOverload", "Ids.Outer.Go", NULL,
         NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Outer.Go(System.String)", "Ids.Rows", NULL, "missing", NULL, NULL},
        /* a method's type parameter by its position: the overload it is in */
        {"src/Ids.cs", "M:Acme.D.Outer.Take``1(``0,System.Int32)", "Ids.OfGenericMethod",
         "Ids.Outer.Take", NULL, NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Outer.#ctor(System.String)", "Ids.OfCtor", "Ids.Outer.Outer", NULL,
         NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Outer.#cctor", "Ids.OfStaticCtor", "Ids.Outer.Outer", NULL, NULL,
         NULL},
        /* the letter names the kind */
        {"src/Ids.cs", "P:Acme.D.Outer.Prop", "Ids.OfProperty", "Ids.Outer.Prop", NULL, NULL, NULL},
        {"src/Ids.cs", "F:Acme.D.Outer.Fld", "Ids.OfField", "Ids.Outer.Fld", NULL, NULL, NULL},
        {"src/Ids.cs", "F:Acme.D.Outer.Prop", "Ids.Rows", NULL, "missing", "Ids.Outer.Prop", NULL},
        {"src/Ids.cs", "E:Acme.D.Outer.Evt", "Ids.Rows", NULL, "graph_gap", NULL, NULL},
        {"src/Ids.cs", "P:Acme.D.Outer.Item(System.Int32)", "Ids.Rows", NULL, "graph_gap", NULL,
         NULL},
        /* a type's parameter by its position, counted on from its outer types */
        {"src/Ids.cs", "M:Acme.D.Box`1.Put(`0)", "Ids.OfSlot", "Ids.Box.Put", NULL, NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Box`1.In`1.Both(`0,`1)", "Ids.OfNestedSlots", "Ids.Box.In.Both",
         NULL, NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Box`1.In`1.Both(`1,`0)", "Ids.Rows", NULL, "missing", NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Outer.op_Addition(Acme.D.Outer,Acme.D.Outer)", "Ids.Rows", NULL,
         "graph_gap", NULL, NULL},
        {"src/Ids.cs", "M:Acme.D.Outer.op_Subtraction(Acme.D.Outer,Acme.D.Outer)", "Ids.Rows", NULL,
         "missing", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("ids", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* The reasons. `external` is what the repository's own structure says is
 * outside: a namespace it does not declare. No name is outside because it is
 * on a list -- a repository that IS Xunit declares Xunit. A reference too
 * long to be read is not judged by its beginning; a product reference never
 * binds what only a test declares, and does not count it either. */
TEST(doc_mentions_cs_reasons) {
    /* `Known.M(TTT...T, int)`: cut at a buffer's end it would read as a call
     * with ONE parameter of a type nothing is known about, which M(int) fits */
    char longref[1300];
    char long_cs[1600];
    memset(longref, 'T', sizeof(longref));
    memcpy(longref, "Known.M(", 8);
    snprintf(longref + 1100, sizeof(longref) - 1100, ", int)");
    snprintf(long_cs, sizeof(long_cs),
             "namespace Acme.R\n"
             "{\n"
             "    public class Known { public void M(int a) { } }\n"
             "\n"
             "    /// <summary><see cref=\"%s\"/></summary>\n"
             "    public class TooLong { }\n"
             "}\n",
             longref);
    const dm_source_t files[] = {
        {"src/Declared.cs", "namespace Xunit.Sdk\n"
                            "{\n"
                            "    public class Runner { }\n"
                            "}\n"
                            "namespace Acme.R\n"
                            "{\n"
                            "    public class Here { }\n"
                            "}\n"},
        {"src/Structure.cs",
         "using Xunit.Sdk;\n"
         "\n"
         "namespace Acme.R\n"
         "{\n"
         "    /// <summary><see cref=\"Xunit.Sdk.Nope\"/> <see cref=\"Xunit.Assert\"/>\n"
         "    /// <see cref=\"Outside.Lib.Thing\"/> <see cref=\"Acme.R.Nope\"/>\n"
         "    /// <see cref=\"NotAnywhere\"/> <see cref=\"Xunit.Sdk.Runner\"/></summary>\n"
         "    public class Structure { }\n"
         "}\n"},
        {"src/OpenScope.cs", "using Moq;\n"
                             "\n"
                             "namespace Acme.R\n"
                             "{\n"
                             "    /// <summary><see cref=\"NotAnywhere\"/></summary>\n"
                             "    public class OpenScope { }\n"
                             "}\n"},
        /* the repository adds to System and to a namespace under it: polyfills */
        {"src/Polyfill.cs", "namespace System\n"
                            "{\n"
                            "    public class Polyfill { }\n"
                            "}\n"
                            "namespace System.Text.Json\n"
                            "{\n"
                            "    public class Extra { }\n"
                            "}\n"},
        {"src/Standard.cs",
         "using System.Text.Json;\n"
         "\n"
         "namespace Acme.R\n"
         "{\n"
         "    /// <summary><see cref=\"System.Nope\"/> <see cref=\"System.Text.Json.Nope\"/>\n"
         "    /// <see cref=\"System.Text.Nope\"/> <see cref=\"T:System.Nope2\"/>\n"
         "    /// <see cref=\"System.Polyfill\"/> <see cref=\"NotInJson\"/></summary>\n"
         "    public class Standard { }\n"
         "}\n"},
        {"src/Unicode.cs", "namespace Acme.R\n"
                           "{\n"
                           "    public class Gr\xC3\xB6\xC3\x9F"
                           "e { public void L\xC3\xA4nge() { } }\n"
                           "\n"
                           "    /// <summary><see cref=\"Gr\xC3\xB6\xC3\x9F"
                           "e\"/> <see cref=\"Gr\xC3\xB6\xC3\x9F"
                           "e.L\xC3\xA4nge\"/>\n"
                           "    /// <see cref=\"Gr\xC3\xB6\xC3\x9F"
                           "e.Nicht\"/></summary>\n"
                           "    public class Unicode { }\n"
                           "}\n"},
        {"src/Long.cs", long_cs},
        {"src/Mix.cs",
         "namespace Acme.R\n"
         "{\n"
         "    public partial class Mix\n"
         "    {\n"
         "        public void Prod() { }\n"
         "        public void Over(string s) { }\n"
         "    }\n"
         "\n"
         "    /// <summary><see cref=\"Mix.OnlyTest\"/> <see cref=\"Mix.Over\"/> <see "
         "cref=\"Mix.Prod\"/>\n"
         "    /// </summary>\n"
         "    public class UsesMix { }\n"
         "}\n"},
        {"src/tests/MixTests.cs", "namespace Acme.R\n"
                                  "{\n"
                                  "    public partial class Mix\n"
                                  "    {\n"
                                  "        public void OnlyTest() { }\n"
                                  "        public void Over(int a) { }\n"
                                  "    }\n"
                                  "\n"
                                  "    /// <summary><see cref=\"Mix.Over\"/></summary>\n"
                                  "    public class FromTest { }\n"
                                  "}\n"},
    };
    const dm_want_t wants[] = {
        /* a namespace the repository declares: its missing names are missing */
        {"src/Structure.cs", "Xunit.Sdk.Nope", "Structure.Structure", NULL, "missing", NULL, NULL},
        {"src/Structure.cs", "Acme.R.Nope", "Structure.Structure", NULL, "missing", NULL, NULL},
        {"src/Structure.cs", "Xunit.Sdk.Runner", "Structure.Structure", "Declared.Runner", NULL,
         NULL, DM_EXACT},
        /* ... one it only has namespaces under, and one it does not have: outside */
        {"src/Structure.cs", "Xunit.Assert", "Structure.Structure", NULL, "external", NULL, NULL},
        {"src/Structure.cs", "Outside.Lib.Thing", "Structure.Structure", NULL, "external", NULL,
         NULL},
        /* every using of the file names a declared namespace: the scope is closed */
        {"src/Structure.cs", "NotAnywhere", "Structure.Structure", NULL, "missing", NULL, NULL},
        /* ... a using of a namespace the repository does not declare opens it */
        {"src/OpenScope.cs", "NotAnywhere", "OpenScope.OpenScope", NULL, "external", NULL, NULL},
        /* System is the standard library's whoever adds to it: what the
         * repository does not have there is outside, though it declares the
         * namespace -- by a full name, by a doc ID, through a using */
        {"src/Standard.cs", "System.Nope", "Standard.Standard", NULL, "external", NULL, NULL},
        {"src/Standard.cs", "System.Text.Json.Nope", "Standard.Standard", NULL, "external", NULL,
         NULL},
        {"src/Standard.cs", "System.Text.Nope", "Standard.Standard", NULL, "external", NULL, NULL},
        {"src/Standard.cs", "T:System.Nope2", "Standard.Standard", NULL, "external", NULL, NULL},
        {"src/Standard.cs", "NotInJson", "Standard.Standard", NULL, "external", NULL, NULL},
        /* ... and what it has there is its own */
        {"src/Standard.cs", "System.Polyfill", "Standard.Standard", "Polyfill.Polyfill", NULL, NULL,
         DM_EXACT},
        /* identifiers are not ASCII only */
        {"src/Unicode.cs",
         "Gr\xC3\xB6\xC3\x9F"
         "e",
         "Unicode.Unicode",
         "Unicode.Gr\xC3\xB6\xC3\x9F"
         "e",
         NULL, NULL, NULL},
        {"src/Unicode.cs",
         "Gr\xC3\xB6\xC3\x9F"
         "e.L\xC3\xA4nge",
         "Unicode.Unicode",
         "Unicode.Gr\xC3\xB6\xC3\x9F"
         "e.L\xC3\xA4nge",
         NULL, NULL, NULL},
        {"src/Unicode.cs",
         "Gr\xC3\xB6\xC3\x9F"
         "e.Nicht",
         "Unicode.Unicode", NULL, "missing", NULL, NULL},
        /* too long to be read: never resolved by what is left after a cut */
        {"src/Long.cs", longref, "Long.TooLong", NULL, "unparseable", "Long.Known.M", NULL},
        /* only the test part of the type declares OnlyTest */
        {"src/Mix.cs", "Mix.OnlyTest", "Mix.UsesMix", NULL, "test_only_target",
         "MixTests.Mix.OnlyTest", NULL},
        /* ... and its Over(int) is no overload for product code: one Over */
        {"src/Mix.cs", "Mix.Over", "Mix.UsesMix", "Mix.Mix.Over", NULL, "MixTests.Mix.Over", NULL},
        {"src/Mix.cs", "Mix.Prod", "Mix.UsesMix", "Mix.Mix.Prod", NULL, NULL, NULL},
        /* test code sees both parts: an overload group */
        {"src/tests/MixTests.cs", "Mix.Over", "MixTests.FromTest", NULL, "ambiguous", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("reasons", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* The text around a reference: CRLF line ends and a byte-order mark change
 * nothing; a reference inside an XML comment or a CDATA section of the doc
 * text is no reference. */
TEST(doc_mentions_cs_text_forms) {
    const char *crlf = "\xEF\xBB\xBFnamespace Acme.T\r\n"                  /* 1 */
                       "{\r\n"                                             /* 2 */
                       "    public class Target { }\r\n"                   /* 3 */
                       "\r\n"                                              /* 4 */
                       "    /// <summary>First <see cref=\"Target\"/>\r\n" /* 5 */
                       "    /// and <see\r\n"                              /* 6 */
                       "    /// cref=\"Target.Nope\"/>.</summary>\r\n"     /* 7 */
                       "    public class Uses\r\n"                         /* 8 */
                       "    {\r\n"                                         /* 9 */
                       "        public int Field;\r\n"                     /* 10 */
                       "    }\r\n"                                         /* 11 */
                       "}\r\n";                                            /* 12 */
    CBMFileResult *r = dm_extract(crlf, CBM_LANG_CSHARP, "Crlf.cs");
    ASSERT_NOT_NULL(r);
    const CBMDocLink *first = dm_find_token(r, "Target");
    ASSERT_NOT_NULL(first);
    ASSERT_EQ(first->line, 5);
    ASSERT_EQ(first->def_line, 8);
    const CBMDocLink *second = dm_find_token(r, "Target.Nope");
    ASSERT_NOT_NULL(second);
    ASSERT_EQ(second->line, 6);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "R\t1\t0\t1\t12\tAcme.T\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "T\t1\t3\t3\tc\t-\tTarget\t\t\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "T\t1\t8\t11\tc\t-\tUses\t\t\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "M\t10\tv\t0\t1\tField\t\t-\n"));
    ASSERT_NULL(strchr(r->doc_scope, '\r'));
    cbm_free_result(r);

    const char *hidden = "namespace Acme.T\n"
                         "{\n"
                         "    /// <summary>\n"
                         "    /// <!-- <see cref=\"InComment\"/> -->\n"
                         "    /// <![CDATA[ <see cref=\"InCdata\"/> ]]>\n"
                         "    /// <!-- a comment that runs on\n"
                         "    /// <see cref=\"InLongComment\"/> to here --> <see cref=\"Shown\"/>\n"
                         "    /// <code><see cref=\"InCode\"/></code>\n"
                         "    /// </summary>\n"
                         "    public class Hidden { }\n"
                         "}\n";
    r = dm_extract(hidden, CBM_LANG_CSHARP, "Hidden.cs");
    ASSERT_NOT_NULL(r);
    ASSERT_NULL(dm_find_token(r, "InComment"));
    ASSERT_NULL(dm_find_token(r, "InCdata"));
    ASSERT_NULL(dm_find_token(r, "InLongComment"));
    ASSERT_NOT_NULL(dm_find_token(r, "Shown"));
    /* markup inside <code> is markup: the compiler binds a cref there too */
    ASSERT_NOT_NULL(dm_find_token(r, "InCode"));
    cbm_free_result(r);

    /* ... and end to end, through the pipeline */
    const dm_source_t files[] = {{"src/Crlf.cs", crlf}, {"src/Hidden.cs", hidden}};
    static const dm_want_t wants[] = {
        {"src/Crlf.cs", "Target", "Crlf.Uses", "Crlf.Target", NULL, NULL, NULL},
        {"src/Crlf.cs", "Target.Nope", "Crlf.Uses", NULL, "missing", NULL, NULL},
        /* what a comment or a CDATA section holds is no reference: no row */
        {"src/Hidden.cs", "InComment", "Hidden.Hidden", NULL, NULL, NULL, NULL},
        {"src/Hidden.cs", "InCdata", "Hidden.Hidden", NULL, NULL, NULL, NULL},
        {"src/Hidden.cs", "InLongComment", "Hidden.Hidden", NULL, NULL, NULL, NULL},
        {"src/Hidden.cs", "Shown", "Hidden.Hidden", NULL, "missing", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("crlf", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* ── cost and limits ─────────────────────────────────────────────── */

enum { DM_COST_REFS = 40, DM_COST_TEXT = 16384 };

/* What the C# resolver looks at for DM_COST_REFS simple names that are not in
 * scope, documented `types` types deep in a namespace of `depth` segments, in
 * a file with `usings` using directives. `declared`: the names are types of
 * a namespace the file does not import (else no file declares them). 0 when
 * the repository cannot be indexed. */
static uint64_t dm_lookup_work(int depth, int usings, int types, bool declared) {
    char *src = malloc(DM_COST_TEXT);
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_cost_XXXXXX");
    if (!src || !cbm_mkdtemp(tmp)) {
        free(src);
        return 0;
    }
    /* the namespaces the usings name, and (when declared) the names asked
     * for, in a namespace nothing imports */
    size_t w = 0;
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "namespace Far\n{\n");
    for (int i = 0; declared && i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "public class Nowhere%d { }\n", i);
    }
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "}\n");
    for (int i = 0; i < usings; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w,
                              "namespace In.U%d { public class Other%d { } }\n", i, i);
    }
    th_write_file(TH_PATH(tmp, "src/Far.cs"), src);
    w = 0;
    for (int i = 0; i < usings; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "using In.U%d;\n", i);
    }
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "namespace ");
    for (int i = 0; i < depth; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "%sN%d", i ? "." : "", i);
    }
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "\n{\n");
    for (int i = 0; i + 1 < types; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "public class T%d {\n", i);
    }
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "/// <summary>");
    for (int i = 0; i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "<see cref=\"Nowhere%d\"/> ", i);
    }
    w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "</summary>\npublic class Leaf { }\n");
    for (int i = 0; i < types; i++) {
        w += (size_t)snprintf(src + w, DM_COST_TEXT - w, "}\n");
    }
    th_write_file(TH_PATH(tmp, "src/Deep.cs"), src);
    free(src);
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == DM_COST_REFS ? work : 0; /* every name was looked up, and found nowhere */
}

/* The same for DM_COST_REFS references, written in product code, to an
 * operator that only test code declares -- `overloads` times over. */
static uint64_t dm_operator_work(int overloads) {
    char *src = malloc(DM_COST_TEXT * 4);
    size_t cap = DM_COST_TEXT * 4;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_opcost_XXXXXX");
    if (!src || !cbm_mkdtemp(tmp)) {
        free(src);
        return 0;
    }
    th_write_file(TH_PATH(tmp, "src/Vec.cs"),
                  "namespace Geo\n{\n    public partial struct Vec { }\n}\n");
    size_t w =
        (size_t)snprintf(src, cap, "namespace Geo\n{\n    public partial struct Vec\n    {\n");
    for (int i = 0; i < overloads; i++) {
        w += (size_t)snprintf(
            src + w, cap - w,
            "        public static Vec operator +(Vec a, P%03d b) { return a; }\n", i);
    }
    snprintf(src + w, cap - w, "    }\n}\n");
    th_write_file(TH_PATH(tmp, "tests/VecOps.cs"), src);
    w = (size_t)snprintf(src, cap, "namespace Geo\n{\n    /// <summary>");
    for (int i = 0; i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(src + w, cap - w, "<see cref=\"Vec.operator +(Vec, P%03d)\"/> ", i);
    }
    snprintf(src + w, cap - w, "</summary>\n    public class Uses { }\n}\n");
    th_write_file(TH_PATH(tmp, "src/Uses.cs"), src);
    free(src);
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'missing'");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == DM_COST_REFS ? work : 0; /* product code sees none of them */
}

/* The same for DM_COST_REFS references `Big.M(int)` to a method with 100
 * overloads of `params` parameters each: none takes one parameter. */
static uint64_t dm_signature_work(int params) {
    char *src = malloc(DM_COST_TEXT * 4);
    size_t cap = DM_COST_TEXT * 4;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_sigcost_XXXXXX");
    if (!src || !cbm_mkdtemp(tmp)) {
        free(src);
        return 0;
    }
    size_t w = (size_t)snprintf(src, cap, "namespace Sig\n{\n    public class Big\n    {\n");
    for (int i = 0; i < 100; i++) {
        w += (size_t)snprintf(src + w, cap - w, "        public void M(P%03d a0", i);
        for (int p = 1; p < params; p++) {
            w += (size_t)snprintf(src + w, cap - w, ", int a%d", p);
        }
        w += (size_t)snprintf(src + w, cap - w, ") { }\n");
    }
    w += (size_t)snprintf(src + w, cap - w, "    }\n\n    /// <summary>");
    for (int i = 0; i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(src + w, cap - w, "<see cref=\"Big.M(Q%03d)\"/> ", i);
    }
    snprintf(src + w, cap - w, "</summary>\n    public class Uses { }\n}\n");
    th_write_file(TH_PATH(tmp, "src/Big.cs"), src);
    free(src);
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'missing'");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == DM_COST_REFS ? work : 0;
}

/* The same for DM_COST_REFS references in the documentation of a method that
 * shares its line with `members` - 1 other methods. */
static uint64_t dm_line_work(int members) {
    char *src = malloc(DM_COST_TEXT * 2);
    size_t cap = DM_COST_TEXT * 2;
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_linecost_XXXXXX");
    if (!src || !cbm_mkdtemp(tmp)) {
        free(src);
        return 0;
    }
    size_t w = (size_t)snprintf(
        src, cap, "namespace Line\n{\n    public class Wide\n    {\n        /// <summary>");
    for (int i = 0; i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(src + w, cap - w, "<see cref=\"Nowhere%d\"/> ", i);
    }
    w += (size_t)snprintf(src + w, cap - w, "</summary>\n       ");
    for (int i = 0; i < members; i++) {
        w += (size_t)snprintf(src + w, cap - w, " public void M%d() { }", i);
    }
    snprintf(src + w, cap - w, "\n    }\n}\n");
    th_write_file(TH_PATH(tmp, "src/Wide.cs"), src);
    free(src);
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == DM_COST_REFS ? work : 0;
}

enum { DM_TREE_FILES = 120 };

/* What the resolver looks at to find the project of DM_TREE_FILES files that
 * stand `depth` directories deep, in a repository without a project file. */
static uint64_t dm_tree_work(int depth) {
    char tmp[256];
    char dir[256];
    char rel[320];
    char text[192];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_tree_XXXXXX");
    if (depth * 2 >= (int)sizeof(dir) || !cbm_mkdtemp(tmp)) {
        return 0;
    }
    size_t d = 0;
    dir[0] = '\0';
    for (int i = 0; i < depth; i++) {
        d += (size_t)snprintf(dir + d, sizeof(dir) - d, "d/");
    }
    for (int i = 0; i < DM_TREE_FILES; i++) {
        snprintf(rel, sizeof(rel), "%sF%03d.cs", dir, i);
        snprintf(text, sizeof(text), "namespace Deep\n{\n%s    public class F%03d { }\n}\n",
                 i == 0 ? "    /// <summary><see cref=\"Nowhere\"/></summary>\n" : "", i);
        th_write_file(TH_PATH(tmp, rel), text);
    }
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == 1 ? work : 0;
}

/* Every import kind is queried with names absent everywhere or declared only
 * outside the imported scopes. Count visits/probes, never elapsed time. */
typedef struct {
    uint64_t lookup, build;
} dm_import_work_t;

static dm_import_work_t dm_import_lookup_work(int imports, char kind, bool global, bool declared) {
    enum { CAP = DM_COST_TEXT * 4 };
    char *defs = malloc(CAP);
    char *directives = malloc(CAP);
    char *use = malloc(CAP);
    char tmp[256] = "/tmp/cbm_dm_importcost_XXXXXX";
    if (!defs || !directives || !use || !cbm_mkdtemp(tmp)) {
        free(defs);
        free(directives);
        free(use);
        return (dm_import_work_t){0};
    }
    size_t d = 0, u = 0;
    for (int i = 0; i < imports; i++) {
        d += (size_t)snprintf(defs + d, CAP - d,
                              "namespace In.U%d { public static class Other%d { "
                              "public static int Member%d; } }\n",
                              i, i, i);
        if (kind == 'n') {
            u += (size_t)snprintf(directives + u, CAP - u, "%susing In.U%d;\n",
                                  global ? "global " : "", i);
        } else if (kind == 's') {
            u += (size_t)snprintf(directives + u, CAP - u, "%susing static In.U%d.Other%d;\n",
                                  global ? "global " : "", i, i);
        } else {
            u += (size_t)snprintf(directives + u, CAP - u, "%susing Alias%d = In.U%d.Other%d;\n",
                                  global ? "global " : "", i, i, i);
        }
    }
    d += (size_t)snprintf(defs + d, CAP - d, "namespace Far {\n");
    for (int i = 0; declared && i < DM_COST_REFS; i++) {
        d += (size_t)snprintf(defs + d, CAP - d, "public class Nowhere%d { }\n", i);
    }
    snprintf(defs + d, CAP - d, "}\n");
    size_t w = (size_t)snprintf(use, CAP, "%snamespace App {\n/// ", global ? "" : directives);
    for (int i = 0; i < DM_COST_REFS; i++) {
        w += (size_t)snprintf(use + w, CAP - w, "<see cref=\"Nowhere%d\"/> ", i);
    }
    snprintf(use + w, CAP - w, "\npublic class Use { }\n}\n");
    th_write_file(TH_PATH(tmp, "Defs.cs"), defs);
    th_write_file(TH_PATH(tmp, "Use.cs"), use);
    th_write_file(TH_PATH(tmp, "App.csproj"), DM_EMPTY_PROJECT);
    if (global) {
        th_write_file(TH_PATH(tmp, "Globals.cs"), directives);
    }
    free(defs);
    free(directives);
    free(use);
    char db[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    dm_import_work_t work = {0};
    if (dm_index(tmp, db, NULL) == 0) {
        work.lookup = cbm_doclink_cs_test_work();
        work.build = cbm_doclink_cs_test_index_work();
    }
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return rows == DM_COST_REFS ? work : (dm_import_work_t){0};
}

/* Many repository declarations of a name must not burden a scope importing
 * only one of them. Static members have distinct graph owners in this file. */
static uint64_t dm_common_import_work(int declarations) {
    enum { CAP = DM_COST_TEXT * 4 };
    char *defs = malloc(CAP);
    char *use = malloc(CAP);
    char tmp[256] = "/tmp/cbm_dm_commonimport_XXXXXX";
    if (!defs || !use || !cbm_mkdtemp(tmp)) {
        free(defs);
        free(use);
        return 0;
    }
    size_t d = (size_t)snprintf(defs, CAP, "namespace In {\n");
    for (int i = 0; i < declarations; i++) {
        d += (size_t)snprintf(defs + d, CAP - d,
                              "public static class C%d { public static int Common; }\n", i);
    }
    snprintf(defs + d, CAP - d, "}\n");
    size_t u = (size_t)snprintf(use, CAP, "using static In.C0;\nnamespace App {\n/// ");
    for (int i = 0; i < DM_COST_REFS; i++) {
        u += (size_t)snprintf(use + u, CAP - u, "<see cref=\"Common\"/> ");
    }
    snprintf(use + u, CAP - u, "\npublic class Use { }\n}\n");
    th_write_file(TH_PATH(tmp, "Defs.cs"), defs);
    th_write_file(TH_PATH(tmp, "Use.cs"), use);
    free(defs);
    free(use);
    char db[512], props[512];
    snprintf(db, sizeof(db), "%s/cost.db", tmp);
    cbm_doclink_cs_test_work_reset();
    uint64_t work = dm_index(tmp, db, NULL) == 0 ? cbm_doclink_cs_test_work() : 0;
    int edges = 0;
    dm_edge(db, "Use.Use", "Defs.C0.Common", props, sizeof(props), &edges);
    int rows = dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved");
    dm_unlink_db(db);
    th_rmtree(tmp);
    return edges == 1 && rows == 0 ? work : 0;
}

TEST(doc_mentions_cs_import_lookup_cost) {
    bool bounded = true;
    static const char kinds[] = {'n', 'a', 's'};
    for (int k = 0; k < 3; k++) {
        for (int global = 0; global < 2; global++) {
            for (int declared = 0; declared < 2; declared++) {
                dm_import_work_t small =
                    dm_import_lookup_work(60, kinds[k], global != 0, declared != 0);
                dm_import_work_t large =
                    dm_import_lookup_work(120, kinds[k], global != 0, declared != 0);
                double ratio = small.lookup ? (double)large.lookup / (double)small.lookup : 0.0;
                double build_ratio = small.build ? (double)large.build / (double)small.build : 0.0;
                printf("  import lookup kind=%c global=%d declared=%d: %llu -> %llu, %.2f; "
                       "index build %llu -> %llu, %.2f\n",
                       kinds[k], global, declared, (unsigned long long)small.lookup,
                       (unsigned long long)large.lookup, ratio, (unsigned long long)small.build,
                       (unsigned long long)large.build, build_ratio);
                bounded = bounded && small.lookup >= DM_COST_REFS && ratio >= 0.8 && ratio <= 1.4 &&
                          small.build > 0 && build_ratio >= 0.8 && build_ratio <= 3.0;
            }
        }
    }
    uint64_t small = dm_common_import_work(64);
    uint64_t large = dm_common_import_work(128);
    double ratio = small ? (double)large / (double)small : 0.0;
    printf("  common name, one import: %llu -> %llu, %.2f\n", (unsigned long long)small,
           (unsigned long long)large, ratio);
    bounded = bounded && small >= DM_COST_REFS && ratio >= 0.8 && ratio <= 1.4;
    ASSERT_TRUE(bounded);
    PASS();
}

/* Parent/global imports are ready for a child's directives. The child's own
 * directives stay excluded while those targets are resolved. */
TEST(doc_mentions_cs_import_stage_aliases) {
    static const dm_source_t files[] = {
        {"App.csproj", DM_EMPTY_PROJECT},
        {"Defs.cs", "namespace Lib { public class Target { } public class Other { } "
                    "public static class Statics { public static int Value; } }\n"},
        {"Globals.cs", "global using Base = Lib;\nglobal using static Lib.Statics;\n"},
        {"Use.cs", "namespace App {\nusing Parent = Lib;\nnamespace Child {\n"
                   "using Pick = Parent.Target;\nusing Again = Base.Target;\n"
                   "/// <see cref=\"Pick\"/> <see cref=\"Again\"/> <see cref=\"Value\"/>\n"
                   "public class Use { }\n}\nnamespace Sibling {\n"
                   "using Parent = Absent;\nusing Decoy = Parent.Target;\n"
                   "/// <see cref=\"Decoy\"/> <see cref=\"Parent\"/>\n"
                   "public class Mask { }\n}\n}\n"},
        {"Aliases.cs", "using Clash = Lib.Target;\nusing Z = Absent;\n"
                       "using Same = Lib.Target;\nusing Noise = Lib;\nusing Same = Lib.Other;\n"
                       "public class Clash { }\n"
                       "/// <see cref=\"Clash\"/> <see cref=\"Same\"/> <see cref=\"Z\"/>\n"
                       "public class Aliases { }\n"},
    };
    static const dm_want_t wants[] = {
        {"Use.cs", "Pick", "Use.Use", "Defs.Target", NULL, NULL, DM_EXACT},
        {"Use.cs", "Again", "Use.Use", "Defs.Target", NULL, NULL, DM_EXACT},
        {"Use.cs", "Value", "Use.Use", "Defs.Statics.Value", NULL, NULL, NULL},
        {"Use.cs", "Decoy", "Use.Mask", "Defs.Target", NULL, NULL, DM_EXACT},
        {"Use.cs", "Parent", "Use.Mask", NULL, "external", NULL, NULL},
        {"Aliases.cs", "Clash", "Aliases.Aliases", NULL, "ambiguous", "Defs.Target", NULL},
        {"Aliases.cs", "Same", "Aliases.Aliases", NULL, "ambiguous", "Defs.Target", NULL},
        {"Aliases.cs", "Z", "Aliases.Aliases", NULL, "external", NULL, NULL},
    };
    ASSERT_EQ(dm_check_repo("importstage", files, DM_COUNT(files), wants, DM_COUNT(wants)), 0);
    PASS();
}

/* Static imports see both parts of a type; unseen named parts still make an
 * outsider's reference ambiguous. No unrelated directive may hide that fact. */
TEST(doc_mentions_cs_import_static_parts) {
    enum { CAP = DM_COST_TEXT * 4 };
    char *noise = malloc(CAP);
    char *own = malloc(CAP);
    char *outside = malloc(CAP);
    ASSERT_NOT_NULL(noise);
    ASSERT_NOT_NULL(own);
    ASSERT_NOT_NULL(outside);
    size_t n = 0, u = 0;
    for (int i = 0; i < 40; i++) {
        n += (size_t)snprintf(noise + n, CAP - n,
                              "namespace Noise.N%d { public static class K%d { "
                              "public static int Other%d; } }\n",
                              i, i, i);
        u += (size_t)snprintf(own + u, CAP - u, "using static Noise.N%d.K%d;\n", i, i);
    }
    u += (size_t)snprintf(own + u, CAP - u, "using static Lib.Tool;\nusing static Lib.TestOnly;\n");
    memcpy(outside, own, u);
    snprintf(own + u, CAP - u,
             "/// <see cref=\"Common\"/> <see cref=\"Shared\"/> <see cref=\"Own\"/> "
             "<see cref=\"Hidden\"/>\npublic class Use { }\n");
    snprintf(outside + u, CAP - u,
             "/// <see cref=\"Common\"/> <see cref=\"OnlyOne\"/> <see cref=\"Own\"/> "
             "<see cref=\"Gone\"/>\npublic class Outside { }\n");
    const dm_source_t files[] = {
        {"shared/Core.cs", "namespace Lib { public partial class Tool { "
                           "public static int Common; public class Shared { } } }\n"},
        {"shared/Noise.cs", noise},
        {"tests/TestOnly.cs", "namespace Lib { public static class TestOnly { "
                              "public static int Hidden; } }\n"},
        {"one/One.csproj", DM_EMPTY_PROJECT},
        {"one/Part.cs", "namespace Lib { public partial class Tool { "
                        "public static int Own; public class OnlyOne { } } }\n"},
        {"one/Use.cs", own},
        {"two/Two.csproj", DM_EMPTY_PROJECT},
        {"two/Outside.cs", outside},
    };
    static const dm_want_t wants[] = {
        {"one/Use.cs", "Common", "Use.Use", "Core.Tool.Common", NULL, NULL, NULL},
        {"one/Use.cs", "Shared", "Use.Use", "Core.Tool.Shared", NULL, NULL, NULL},
        {"one/Use.cs", "Own", "Use.Use", "Part.Tool.Own", NULL, NULL, NULL},
        {"one/Use.cs", "Hidden", "Use.Use", NULL, "test_only_target", "TestOnly.TestOnly.Hidden",
         NULL},
        {"two/Outside.cs", "Common", "Outside.Outside", "Core.Tool.Common", NULL, NULL, NULL},
        {"two/Outside.cs", "OnlyOne", "Outside.Outside", NULL, "ambiguous", "Part.Tool.OnlyOne",
         NULL},
        {"two/Outside.cs", "Own", "Outside.Outside", NULL, "ambiguous", "Part.Tool.Own", NULL},
        {"two/Outside.cs", "Gone", "Outside.Outside", NULL, "missing", NULL, NULL},
    };
    int bad = dm_check_repo("staticparts", files, DM_COUNT(files), wants, DM_COUNT(wants));
    free(noise);
    free(own);
    free(outside);
    ASSERT_EQ(bad, 0);
    PASS();
}

/* A failed candidate buffer must use the complete existing scan. */
TEST(doc_mentions_cs_import_candidate_allocation) {
    enum { CAP = DM_COST_TEXT * 4, IMPORTS = 128, MATCHES = 40 };
    char *defs = malloc(CAP);
    char *use = malloc(CAP);
    ASSERT_NOT_NULL(defs);
    ASSERT_NOT_NULL(use);
    size_t d = 0, u = 0;
    for (int i = 0; i < IMPORTS; i++) {
        d += (size_t)snprintf(defs + d, CAP - d, "namespace N%d { public class %s { } }\n", i,
                              i < MATCHES ? "Common" : "Other");
        u += (size_t)snprintf(use + u, CAP - u, "using N%d;\n", i);
    }
    snprintf(use + u, CAP - u, "/// <see cref=\"Common\"/>\npublic class Use { }\n");
    const dm_source_t files[] = {{"Defs.cs", defs}, {"Use.cs", use}};
    static const dm_want_t wants[] = {
        {"Use.cs", "Common", "Use.Use", NULL, "ambiguous", NULL, NULL},
    };
    bool ok = true;
    for (int fail = 0; fail < 2; fail++) {
        cbm_doclink_cs_test_fail_candidate_alloc(fail != 0);
        int bad = dm_check_repo("importalloc", files, DM_COUNT(files), wants, DM_COUNT(wants));
        bool hit = cbm_doclink_cs_test_candidate_alloc_failed();
        cbm_doclink_cs_test_fail_candidate_alloc(false);
        printf("  import candidate allocation: injected=%d consumed=%d failures=%d\n", fail, hit,
               bad);
        ok = ok && bad == 0 && hit == (fail != 0);
    }
    free(defs);
    free(use);
    ASSERT_TRUE(ok);
    PASS();
}

/* Enclosing scopes cost one step each. Import lookup uses the queried name;
 * unrelated directives add only index probes. Other fixtures retain their
 * original overload, source-line, signature and project-directory contracts. */
TEST(doc_mentions_cs_lookup_cost) {
    struct {
        const char *what;
        uint64_t small;
        uint64_t large;
        double low;
        double high;
    } runs[] = {
        {"namespace depth 16 -> 32", dm_lookup_work(16, 0, 1, true), dm_lookup_work(32, 0, 1, true),
         1.5, 2.5},
        {"type nesting 16 -> 32", dm_lookup_work(1, 0, 16, true), dm_lookup_work(1, 0, 32, true),
         1.5, 2.5},
        {"usings 60 -> 120", dm_lookup_work(1, 60, 1, true), dm_lookup_work(1, 120, 1, true), 0.9,
         1.4},
        {"usings 60 -> 120, names no file declares", dm_lookup_work(1, 60, 1, false),
         dm_lookup_work(1, 120, 1, false), 0.9, 1.1},
        {"test-only operator overloads 100 -> 200", dm_operator_work(100), dm_operator_work(200),
         0.9, 1.1},
        {"members on the documented line 100 -> 200", dm_line_work(100), dm_line_work(200), 0.9,
         1.1},
        {"parameters of 100 overloads none of which fits 4 -> 8", dm_signature_work(4),
         dm_signature_work(8), 0.9, 1.1},
        {"directory depth 6 -> 48", dm_tree_work(6), dm_tree_work(48), 0.9, 2.5},
    };
    for (size_t i = 0; i < sizeof(runs) / sizeof(runs[0]); i++) {
        double ratio = runs[i].small ? (double)runs[i].large / (double)runs[i].small : 0.0;
        printf("  %s: %llu -> %llu steps, %.2f times the work, want %.1f to %.1f\n", runs[i].what,
               (unsigned long long)runs[i].small, (unsigned long long)runs[i].large, ratio,
               runs[i].low, runs[i].high);
        if (runs[i].small < DM_COST_REFS || ratio < runs[i].low || ratio > runs[i].high) {
            FAIL("lookup cost");
        }
    }
    PASS();
}

enum { DM_MANY = 300, DM_MANY_TEXT = 65536 };

/* No limit decides silently. More usings than any fixed list holds are all
 * asked. A type with more overloads of one name than a lookup compares still
 * binds the overload a reference writes out; what would need the comparison
 * is `ambiguous`, never a guess and never `missing`. The same for a project
 * that holds more complete declarations of one type than a lookup compares
 * (beside another project of the assembly that holds one): none of them is
 * picked. */
TEST(doc_mentions_cs_no_silent_limits) {
    char *decls = malloc(DM_MANY_TEXT);
    char *uses = malloc(DM_MANY_TEXT);
    char *big = malloc(DM_MANY_TEXT);
    char *flav = malloc(DM_MANY_TEXT);
    ASSERT_NOT_NULL(decls);
    ASSERT_NOT_NULL(uses);
    ASSERT_NOT_NULL(big);
    ASSERT_NOT_NULL(flav);
    size_t d = 0;
    size_t u = 0;
    size_t b = 0;
    size_t f = (size_t)snprintf(flav, DM_MANY_TEXT, "namespace Lib\n{\n");
    for (int i = 0; i < DM_MANY; i++) {
        f += (size_t)snprintf(flav + f, DM_MANY_TEXT - f, "    public class Flav { }\n");
    }
    snprintf(flav + f, DM_MANY_TEXT - f, "}\n");
    for (int i = 0; i < DM_MANY; i++) {
        d += (size_t)snprintf(decls + d, DM_MANY_TEXT - d,
                              "namespace Many.N%03d { public class C%03d { } }\n", i, i);
        u += (size_t)snprintf(uses + u, DM_MANY_TEXT - u, "using Many.N%03d;\n", i);
    }
    snprintf(uses + u, DM_MANY_TEXT - u,
             "namespace App\n"
             "{\n"
             "    /// <summary><see cref=\"C000\"/> <see cref=\"C299\"/></summary>\n"
             "    public class Uses { }\n"
             "}\n");
    b += (size_t)snprintf(big + b, DM_MANY_TEXT - b,
                          "namespace App\n{\n    public class Big\n    {\n");
    for (int i = 0; i < DM_MANY; i++) {
        b += (size_t)snprintf(big + b, DM_MANY_TEXT - b, "        public void M(P%03d a) { }\n", i);
    }
    snprintf(big + b, DM_MANY_TEXT - b,
             "    }\n"
             "\n"
             "    /// <summary><see cref=\"Big.M(P299)\"/></summary>\n"
             "    public class Written { }\n"
             "\n"
             "    /// <summary><see cref=\"Big.M\"/> <see cref=\"Big.M(Other)\"/></summary>\n"
             "    public class Unwritten { }\n"
             "}\n");
    const dm_source_t files[] = {
        {"src/Decls.cs", decls},
        {"src/Uses.cs", uses},
        {"src/Big.cs", big},
        {"a/Lib.csproj", DM_EMPTY_PROJECT},
        {"a/Many.cs", flav},
        {"a/UsesA.cs", "namespace Lib\n"
                       "{\n"
                       "    /// <summary><see cref=\"Flav\"/></summary>\n"
                       "    public class UsesA { }\n"
                       "}\n"},
        {"b/Lib.csproj", DM_EMPTY_PROJECT},
        {"b/Flav.cs", "namespace Lib\n{\n    public class Flav { }\n}\n"},
        {"b/UsesB.cs", "namespace Lib\n"
                       "{\n"
                       "    /// <summary><see cref=\"Flav\"/></summary>\n"
                       "    public class UsesB { }\n"
                       "}\n"},
    };
    static const dm_want_t wants[] = {
        {"src/Uses.cs", "C000", "Uses.Uses", "Decls.C000", NULL, NULL, NULL},
        {"src/Uses.cs", "C299", "Uses.Uses", "Decls.C299", NULL, NULL, NULL},
        {"src/Big.cs", "Big.M(P299)", "Big.Written", "Big.Big.M", NULL, NULL, NULL},
        {"src/Big.cs", "Big.M", "Big.Unwritten", NULL, "ambiguous", "Big.Big.M", NULL},
        {"src/Big.cs", "Big.M(Other)", "Big.Unwritten", NULL, "ambiguous", "Big.Big.M", NULL},
        /* more declarations in the own project than a lookup compares: none
         * is picked -- and the other project of the assembly binds its own */
        {"a/UsesA.cs", "Flav", "UsesA.UsesA", NULL, "ambiguous", "Many.Flav", NULL},
        {"b/UsesB.cs", "Flav", "UsesB.UsesB", "b.Flav.Flav", NULL, "Many.Flav", NULL},
    };
    int bad = dm_check_repo("limits", files, DM_COUNT(files), wants, DM_COUNT(wants));
    free(decls);
    free(uses);
    free(big);
    free(flav);
    ASSERT_EQ(bad, 0);
    PASS();
}

enum { DM_WIDE = 66 }; /* more assemblies than one lookup compares */

/* The number of assemblies that hold a part or a stub of a type decides
 * nothing. A contract is joined to its one implementation however many
 * assemblies hold that contract; where the implementation has no node, the
 * one stub-only assembly's stub stands in however many assemblies have parts
 * of the type; and a name is told from what an unseen part declares by
 * looking the name up, not by counting the assemblies: one that no part
 * declares is `missing`, and an unrelated name written inside such a type is
 * bound as anywhere else. */
TEST(doc_mentions_cs_many_assemblies) {
    static const char stub[] = "namespace Acme.Coll\n"
                               "{\n"
                               "    public partial class Thing { public void Go() { } }\n"
                               "\n"
                               "    /// <summary><see cref=\"Thing\"/></summary>\n"
                               "    public partial class UsesStub { }\n"
                               "}\n";
    static const char part[] = "namespace Acme.Coll\n"
                               "{\n"
                               "    public partial class Phantom { public void Own() { } }\n"
                               "    public partial class Wide { public void Extra() { } }\n"
                               "}\n";
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_wide_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    th_write_file(TH_PATH(tmp, "shared/impl/Things.cs"),
                  "namespace Acme.Coll\n"
                  "{\n"
                  "    public class Thing { public void Go() { } }\n"
                  "    public partial class Phantom { public void Boo() { } }\n"
                  "    public partial class Phantom<T> { }\n"
                  "\n"
                  "    public partial class Wide\n"
                  "    {\n"
                  "        /// <summary><see cref=\"Thing\"/></summary>\n"
                  "        public void Common() { }\n"
                  "    }\n"
                  "\n"
                  "    /// <summary><see cref=\"Phantom\"/> <see cref=\"Wide.Extra\"/>\n"
                  "    /// <see cref=\"Wide.Nothing\"/></summary>\n"
                  "    public class UsesShared { }\n"
                  "}\n");
    th_write_file(TH_PATH(tmp, "facade/ref/Facade.csproj"), DM_EMPTY_PROJECT);
    th_write_file(TH_PATH(tmp, "facade/ref/Stubs.cs"),
                  "namespace Acme.Coll\n"
                  "{\n"
                  "    public partial class Phantom { public void Boo() { } }\n"
                  "}\n");
    for (int i = 0; i < DM_WIDE; i++) {
        char rel[64];
        snprintf(rel, sizeof(rel), "s%02d/ref/S%02d.csproj", i, i);
        th_write_file(TH_PATH(tmp, rel), DM_EMPTY_PROJECT);
        snprintf(rel, sizeof(rel), "s%02d/ref/Stub.cs", i);
        th_write_file(TH_PATH(tmp, rel), stub);
        snprintf(rel, sizeof(rel), "p%02d/P%02d.csproj", i, i);
        th_write_file(TH_PATH(tmp, rel), DM_EMPTY_PROJECT);
        snprintf(rel, sizeof(rel), "p%02d/Part.cs", i);
        th_write_file(TH_PATH(tmp, rel), part);
    }
    static const dm_want_t wants[] = {
        /* DM_WIDE assemblies hold the contract: each is joined */
        {"s00/ref/Stub.cs", "Thing", "s00.ref.Stub.UsesStub", "impl.Things.Thing", NULL,
         "s00.ref.Stub.Thing", DM_UNIQUE},
        {"s65/ref/Stub.cs", "Thing", "s65.ref.Stub.UsesStub", "impl.Things.Thing", NULL,
         "s65.ref.Stub.Thing", DM_UNIQUE},
        /* DM_WIDE assemblies have parts of the type, one has only its stub:
         * that stub stands in for the implementation without a node */
        {"shared/impl/Things.cs", "Phantom", "Things.UsesShared", "facade.ref.Stubs.Phantom", NULL,
         NULL, DM_UNIQUE},
        /* what the unseen parts declare, and what they do not */
        {"shared/impl/Things.cs", "Wide.Extra", "Things.UsesShared", NULL, "ambiguous", NULL, NULL},
        {"shared/impl/Things.cs", "Wide.Nothing", "Things.UsesShared", NULL, "missing", NULL, NULL},
        {"shared/impl/Things.cs", "Thing", "Things.Wide.Common", "impl.Things.Thing", NULL, NULL,
         NULL},
    };
    char db[512];
    snprintf(db, sizeof(db), "%s/wide.db", tmp);
    int bad = dm_index(tmp, db, NULL) == 0 ? 0 : -1;
    for (int i = 0; bad >= 0 && i < DM_COUNT(wants); i++) {
        bad += dm_want_failed(db, &wants[i]);
    }
    dm_unlink_db(db);
    th_rmtree(tmp);
    ASSERT_EQ(bad, 0);
    PASS();
}

/* A using directive of a file whose type has a base list is no private
 * matter of that file: the base list is resolved through it, and what a type
 * derives from decides how another file's reference to its members comes
 * out. Changing it must not be repaired as if only the file had changed. */
TEST(doc_mentions_incremental_using_of_based_type) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_incu_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "src/V1.cs"),
                  "namespace Lib.V1\n{\n    public class Base { public void Run() { } }\n}\n");
    th_write_file(TH_PATH(repo, "src/F.cs"), "using Lib.V1;\n"
                                             "\n"
                                             "namespace App\n"
                                             "{\n"
                                             "    public class Job : Base { }\n"
                                             "}\n");
    th_write_file(TH_PATH(repo, "src/G.cs"), "namespace App\n"
                                             "{\n"
                                             "    /// <summary><see cref=\"Job.Nope\"/></summary>\n"
                                             "    public class Uses { }\n"
                                             "}\n");
    char inc_db[512];
    char full_db[512];
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    char reason[64];
    /* Job derives from a class of the repository: what it lacks is missing */
    dm_row(inc_db, "src/G.cs", "Job.Nope", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "missing");
    /* the using now names a namespace the repository does not declare: Job's
     * base is outside, and so may be the member G.cs asks for */
    th_write_file(TH_PATH(repo, "src/F.cs"), "using Lib.V9;\n"
                                             "\n"
                                             "namespace App\n"
                                             "{\n"
                                             "    public class Job : Base { }\n"
                                             "}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "using of a type with a base list changed",
                      CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    dm_row(inc_db, "src/G.cs", "Job.Nope", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "external");
    dm_unlink_db(inc_db);
    dm_unlink_db(full_db);
    th_rmtree(tmp);
    PASS();
}

enum { DM_PROJECTS = 40 };

/* A directory may hold any number of project files, and every one of them
 * sets global usings of the files there: none is left out, whatever order a
 * directory listing would have had. */
TEST(doc_mentions_msbuild_many_project_files) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_many_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    enum { SHARED_RECORDS = 512 };
    char *props = dm_repeated("<Project><PropertyGroup>", "<Shared>G</Shared>", SHARED_RECORDS,
                              "</PropertyGroup></Project>");
    ASSERT_NOT_NULL(props);
    th_write_file(TH_PATH(tmp, "Directory.Build.props"), props);
    free(props);
    char *targets =
        dm_repeated("<Project><PropertyGroup>", "<TargetShared>$(Shared)</TargetShared>",
                    SHARED_RECORDS, "</PropertyGroup></Project>");
    ASSERT_NOT_NULL(targets);
    th_write_file(TH_PATH(tmp, "Directory.Build.targets"), targets);
    free(targets);
    char *decls = malloc(DM_MANY_TEXT);
    char *uses = malloc(DM_MANY_TEXT);
    ASSERT_NOT_NULL(decls);
    ASSERT_NOT_NULL(uses);
    size_t d = 0;
    size_t u = (size_t)snprintf(uses, DM_MANY_TEXT, "namespace App\n{\n    /// <summary>");
    for (int i = 0; i < DM_PROJECTS; i++) {
        char rel[64];
        char xml[256];
        snprintf(rel, sizeof(rel), "src/Many/P%02d.csproj", i);
        snprintf(xml, sizeof(xml),
                 "<Project Sdk=\"Microsoft.NET.Sdk\">\n"
                 "  <ItemGroup><Using Include=\"$(TargetShared).N%02d\" /></ItemGroup>\n"
                 "</Project>\n",
                 i);
        th_write_file(TH_PATH(tmp, rel), xml);
        d += (size_t)snprintf(decls + d, DM_MANY_TEXT - d,
                              "namespace G.N%02d { public class K%02d { } }\n", i, i);
        u += (size_t)snprintf(uses + u, DM_MANY_TEXT - u, "<see cref=\"K%02d\"/> ", i);
    }
    snprintf(uses + u, DM_MANY_TEXT - u, "</summary>\n    public class Uses { }\n}\n");
    th_write_file(TH_PATH(tmp, "src/Decl.cs"), decls);
    th_write_file(TH_PATH(tmp, "src/Many/Uses.cs"), uses);
    free(decls);
    free(uses);
    char db[512];
    snprintf(db, sizeof(db), "%s/many.db", tmp);
    cbm_msb_test_cost_reset();
    ASSERT_EQ(dm_index(tmp, db, NULL), 0);
    uint64_t records = 0;
    uint64_t peak = 0;
    cbm_msb_test_cost(&records, &peak);
    fprintf(stderr,
            "msbuild pipeline projects=%d shared=%d interpreted=%llu work=%llu "
            "peak=%llu live=%llu\n",
            DM_PROJECTS, SHARED_RECORDS, (unsigned long long)records,
            (unsigned long long)cbm_msb_test_work(), (unsigned long long)peak,
            (unsigned long long)cbm_msb_test_value_live_bytes());
    ASSERT_EQ(dm_mentions_from(db, "Uses.Uses"), DM_PROJECTS);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved"), 0);
    bool shared_once = records <= 2 * SHARED_RECORDS + 16 * DM_PROJECTS;
    bool released = cbm_msb_test_value_live_bytes() == 0;
    dm_unlink_db(db);
    th_rmtree(tmp);
    ASSERT_TRUE(shared_once);
    ASSERT_TRUE(released);
    PASS();
}

enum { DM_NEST = 70, DM_NEST_KEPT = 64, DM_NEST_TEXT = 8192 };

static int dm_count_lines(const char *blob, const char *prefix) {
    int n = 0;
    size_t pl = strlen(prefix);
    for (const char *p = blob; p && *p;) {
        n += strncmp(p, prefix, pl) == 0;
        p = strchr(p, '\n');
        p = p ? p + 1 : NULL;
    }
    return n;
}

/* The scope records nesting to a depth no program has. What nests deeper is
 * not placed, and says so: the type names are kept (to be resolved to
 * nothing else), the lines have no scope. Nothing is cut silently. */
TEST(doc_mentions_cs_scan_nesting_limits) {
    char *src = malloc(DM_NEST_TEXT);
    ASSERT_NOT_NULL(src);
    size_t w = (size_t)snprintf(src, DM_NEST_TEXT, "namespace N\n{\n");
    for (int i = 0; i < DM_NEST; i++) {
        w += (size_t)snprintf(src + w, DM_NEST_TEXT - w, "public class T%d\n{\n", i);
    }
    for (int i = 0; i <= DM_NEST; i++) {
        w += (size_t)snprintf(src + w, DM_NEST_TEXT - w, "}\n");
    }
    CBMFileResult *r = dm_scope(src);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_EQ(dm_count_lines(r->doc_scope, "T\t"), DM_NEST_KEPT);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\tT63\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\tT64\t"));
    ASSERT_EQ(dm_count_lines(r->doc_scope, "Q\t"), DM_NEST - DM_NEST_KEPT);
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nQ\tT64\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nQ\tT69\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nX\t")); /* the lines of the 65th type */
    cbm_free_result(r);

    /* ... and a namespace of more segments than that */
    w = (size_t)snprintf(src, DM_NEST_TEXT, "namespace ");
    for (int i = 0; i < DM_NEST; i++) {
        w += (size_t)snprintf(src + w, DM_NEST_TEXT - w, "%sA%d", i ? "." : "", i);
    }
    snprintf(src + w, DM_NEST_TEXT - w, "\n{\n    public class Deep { }\n}\n");
    r = dm_scope(src);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->doc_scope);
    ASSERT_NULL(strstr(r->doc_scope, "\nR\t"));
    ASSERT_NULL(strstr(r->doc_scope, "\nT\t"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nQ\tDeep\n"));
    ASSERT_NOT_NULL(strstr(r->doc_scope, "\nX\t"));
    cbm_free_result(r);
    free(src);
    PASS();
}

/* Run one statement against the database; returns the rows it changed, -1
 * when it failed. */
static int dm_exec(const char *db, const char *sql) {
    sqlite3 *h = NULL;
    int changed = -1;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READWRITE, NULL) == SQLITE_OK &&
        sqlite3_exec(h, sql, NULL, NULL, NULL) == SQLITE_OK) {
        changed = sqlite3_changes(h);
    }
    sqlite3_close(h);
    return changed;
}

/* A stored scope this code did not write -- a damaged row -- is not read
 * around: the file's declarations would silently be missing from every
 * lookup, and references to them would be reported `missing`. The layer
 * fails for that run, visibly, and the next index rebuilds from the files. */
TEST(doc_mentions_cs_damaged_stored_scope) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dm_dmg_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[400];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    th_write_file(TH_PATH(repo, "src/A.cs"), "namespace N\n{\n    public class Target { }\n}\n");
    th_write_file(TH_PATH(repo, "src/B.cs"), "namespace N\n"
                                             "{\n"
                                             "    /// <summary><see cref=\"Target\"/></summary>\n"
                                             "    public class Uses { }\n"
                                             "}\n");
    char db[512];
    snprintf(db, sizeof(db), "%s/dmg.db", tmp);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    char props[512];
    int n = 0;
    dm_edge(db, "B.Uses", "A.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    /* one field too many in A's type record: no record this code writes */
    ASSERT_EQ(dm_exec(db, "UPDATE lsp_surface SET defs_json = "
                          "replace(defs_json, '\\nT\\t', '\\nT\\tX\\t') "
                          "WHERE rel_path = 'src/A.cs' AND instr(defs_json, '\\nT\\t') > 0"),
              1);
    th_write_file(TH_PATH(repo, "src/B.cs"),
                  "namespace N\n"
                  "{\n"
                  "    /// <summary>Again <see cref=\"Target\"/></summary>\n"
                  "    public class Uses { }\n"
                  "}\n");
    cbm_pipeline_incremental_test_reset_faults();
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'"), 1);
    /* never the row an index without A's declarations would write */
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE raw = 'Target'"), 0);
    /* the next run rebuilds everything, and the edge is back */
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    ASSERT_EQ(cbm_pipeline_incremental_test_last_route(), CBM_INCREMENTAL_ROUTE_FORCED_FULL);
    ASSERT_EQ(dm_count(db, "SELECT COUNT(*) FROM doc_link_unresolved WHERE reason = 'error'"), 0);
    dm_edge(db, "B.Uses", "A.Target", props, sizeof(props), &n);
    ASSERT_EQ(n, 1);
    dm_unlink_db(db);
    th_rmtree(tmp);
    PASS();
}

SUITE(doc_mentions) {
    RUN_TEST(doc_mentions_extract_cs_tokens);
    RUN_TEST(doc_mentions_cs_scope_blob);
    RUN_TEST(doc_mentions_cs_declarator_modifiers);
    RUN_TEST(doc_mentions_cs_namespace_scopes);
    RUN_TEST(doc_mentions_cs_namespace_boundaries);
    RUN_TEST(doc_mentions_cs_control_scopes);
    RUN_TEST(doc_mentions_cs_control_publication);
    RUN_TEST(doc_mentions_cs_utf8_observation);
    RUN_TEST(doc_mentions_cs_namespace_publication);
    RUN_TEST(doc_mentions_cs_scope_parse_errors);
    RUN_TEST(doc_mentions_cs_norm_type);
    RUN_TEST(doc_mentions_msbuild_usings);
    RUN_TEST(doc_mentions_msbuild_targets_isolation);
    RUN_TEST(doc_mentions_msbuild_targets_failure);
    RUN_TEST(doc_mentions_msbuild_items_isolation);
    RUN_TEST(doc_mentions_msbuild_items_failure);
    RUN_TEST(doc_mentions_msbuild_items_lifetime);
    RUN_TEST(doc_mentions_msbuild_items_work);
    RUN_TEST(doc_mentions_msbuild_targets_work);
    RUN_TEST(doc_mentions_msbuild_nearest_work);
    RUN_TEST(doc_mentions_msbuild_shared_closure_work);
    RUN_TEST(doc_mentions_msbuild_shared_file_work);
    RUN_TEST(doc_mentions_msbuild_shared_closure_isolation);
    RUN_TEST(doc_mentions_msbuild_components_failure);
    RUN_TEST(doc_mentions_msbuild_components_revision);
    RUN_TEST(doc_mentions_msbuild_components_mixed_absence);
    RUN_TEST(doc_mentions_msbuild_prefix_work);
    RUN_TEST(doc_mentions_msbuild_prefix_isolation);
    RUN_TEST(doc_mentions_msbuild_prefix_failure);
    RUN_TEST(doc_mentions_msbuild_eval_storage);
    RUN_TEST(doc_mentions_msbuild_value_lifetimes);
    RUN_TEST(doc_mentions_msbuild_value_allocation);
    RUN_TEST(doc_mentions_resolver_rules);
    RUN_TEST(doc_mentions_resolver_arity_and_members);
    RUN_TEST(doc_mentions_resolver_parse_errors);
    RUN_TEST(doc_mentions_ship_gate);
    RUN_TEST(doc_mentions_file_node_lookup);
    RUN_TEST(doc_mentions_index_status_and_delete);
    RUN_TEST(doc_mentions_index_status_preview_bounds);
    RUN_TEST(doc_mentions_index_status_preview_text);
    RUN_TEST(doc_mentions_index_status_preview_order);
    RUN_TEST(doc_mentions_index_status_preview_failure);
    RUN_TEST(doc_mentions_scope_delta_rules);
    RUN_TEST(doc_mentions_incremental_equals_full);
    RUN_TEST(doc_mentions_parallel_equals_sequential);
    RUN_TEST(doc_mentions_cs_scan_dollar_run);
    RUN_TEST(doc_mentions_cs_scan_branch_memory);
    RUN_TEST(doc_mentions_cs_scan_branch_merge_work);
    RUN_TEST(doc_mentions_cs_scan_declarator_modifier_work);
    RUN_TEST(doc_mentions_cs_shared_doc_sources);
    RUN_TEST(doc_mentions_cs_shared_doc_work);
    RUN_TEST(doc_mentions_cs_shared_doc_replay);
    RUN_TEST(doc_mentions_cs_shared_doc_allocation_failure);
    RUN_TEST(doc_mentions_cs_scan_header_reads);
    RUN_TEST(doc_mentions_cs_scan_nested_holes);
    RUN_TEST(doc_mentions_cs_scan_holes_stack);
    RUN_TEST(doc_mentions_msbuild_blob);
    RUN_TEST(doc_mentions_msbuild_import_group_blob_growth);
    RUN_TEST(doc_mentions_msbuild_import_group_roundtrip);
    RUN_TEST(doc_mentions_msbuild_legacy_import_blob);
    RUN_TEST(doc_mentions_msbuild_bad_import_group_blob);
    RUN_TEST(doc_mentions_msbuild_imports);
    RUN_TEST(doc_mentions_msbuild_conditions);
    RUN_TEST(doc_mentions_msbuild_unknown_spreads);
    RUN_TEST(doc_mentions_msbuild_gate);
    RUN_TEST(doc_mentions_incremental_project_files);
#ifndef _WIN32
    RUN_TEST(doc_mentions_msbuild_linked_project_file);
    RUN_TEST(doc_mentions_msbuild_linked_import_directory);
#endif
    RUN_TEST(doc_mentions_cs_lookup_order);
    RUN_TEST(doc_mentions_cs_type_parameters);
    RUN_TEST(doc_mentions_cs_usings);
    RUN_TEST(doc_mentions_cs_entities);
    RUN_TEST(doc_mentions_cs_assemblies_by_name);
    RUN_TEST(doc_mentions_cs_reference_source);
    RUN_TEST(doc_mentions_cs_assemblies_differ);
    RUN_TEST(doc_mentions_cs_shared_parts);
    RUN_TEST(doc_mentions_cs_shared_trees);
    RUN_TEST(doc_mentions_cs_no_project_files);
    RUN_TEST(doc_mentions_cs_contract_join);
    RUN_TEST(doc_mentions_cs_idle_project_file);
    RUN_TEST(doc_mentions_cs_test_namespaces);
    RUN_TEST(doc_mentions_cs_members);
    RUN_TEST(doc_mentions_cs_constructors);
    RUN_TEST(doc_mentions_cs_inherited_members);
    RUN_TEST(doc_mentions_cs_doc_ids);
    RUN_TEST(doc_mentions_cs_reasons);
    RUN_TEST(doc_mentions_cs_text_forms);
    RUN_TEST(doc_mentions_cs_lookup_cost);
    RUN_TEST(doc_mentions_cs_import_lookup_cost);
    RUN_TEST(doc_mentions_cs_import_stage_aliases);
    RUN_TEST(doc_mentions_cs_import_static_parts);
    RUN_TEST(doc_mentions_cs_import_candidate_allocation);
    RUN_TEST(doc_mentions_cs_no_silent_limits);
    RUN_TEST(doc_mentions_cs_many_assemblies);
    RUN_TEST(doc_mentions_incremental_using_of_based_type);
    RUN_TEST(doc_mentions_msbuild_many_project_files);
    RUN_TEST(doc_mentions_cs_scan_nesting_limits);
    RUN_TEST(doc_mentions_cs_damaged_stored_scope);
}
