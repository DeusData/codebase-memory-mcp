/*
 * test_doc_mentions_php.c -- PHP doc-comment references (PHPDoc @see and
 * {@see}).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── extraction ──────────────────────────────────────────────────── */

static const char *const PHP_USER =
    "<?php\n"                                                     /* 1 */
    "namespace App\\Model;\n"                                     /* 2 */
    "\n"                                                          /* 3 */
    "use App\\Util\\Helper as H;\n"                               /* 4 */
    "use App\\Util\\{Strings, Numbers as Num, function twice};\n" /* 5 */
    "use function App\\Util\\clamp;\n"                            /* 6 */
    "\n"                                                          /* 7 */
    "/**\n"                                                       /* 8 */
    " * A user. {@see H::format()} and {@see Strings}.\n"         /* 9 */
    " * Not `{@see Hidden}`, nor {@seealso Also}.\n"              /* 10 */
    " * ```php\n"                                                 /* 11 */
    " * {@see Fenced}\n"                                          /* 12 */
    " * ```\n"                                                    /* 13 */
    " * @see Num::MAX.\n"                                         /* 14 */
    " * @see \\App\\Util\\Helper::$cache description\n"           /* 15 */
    " * @see clamp()\n"                                           /* 16 */
    " * @see https://example.com/doc\n"                           /* 17 */
    " * @see {@see Inline}\n"                                     /* 18 */
    " * @seealso Nope\n"                                          /* 19 */
    " */\n"                                                       /* 20 */
    "class User extends Base implements \\Countable, Shape\n"     /* 21 */
    "{\n"                                                         /* 22 */
    "    use Named, \\App\\Util\\Loud;\n"                         /* 23 */
    "    const LIMIT = 3;\n"                                      /* 24 */
    "    public string $name = '', $alias;\n"                     /* 25 */
    "    /** Counts. {@see parent::boot()} */\n"                  /* 26 */
    "    public function count(): int { return 0; }\n"            /* 27 */
    "    public function __construct(private int $age) {}\n"      /* 28 */
    "}\n"                                                         /* 29 */
    "\n"                                                          /* 30 */
    "interface Shape extends \\JsonSerializable {}\n"             /* 31 */
    "trait Named { public function label() {} }\n"                /* 32 */
    "enum Suit: string { case Hearts = 'H'; }\n"                  /* 33 */
    "function make_user() {}\n";                                  /* 34 */

static bool php_token_is(const CBMFileResult *r, const char *raw, uint32_t line, int syntax) {
    if (dm_count_tokens(r, raw) != 1) {
        printf("  token %s: %d found\n", raw, dm_count_tokens(r, raw));
        return false;
    }
    const CBMDocLink *t = dm_find_token(r, raw);
    if (t->line != line || t->syntax != syntax) {
        printf("  token %s: line %u syntax %d\n", raw, t->line, t->syntax);
        return false;
    }
    return true;
}

TEST(doc_mentions_php_tokens) {
    CBMFileResult *r = dm_extract(PHP_USER, CBM_LANG_PHP, "src/Model/User.php");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(php_token_is(r, "H::format()", 9, CBM_DOCLINK_PHP_SEE));
    ASSERT_TRUE(php_token_is(r, "Strings", 9, CBM_DOCLINK_PHP_SEE));
    /* sentence punctuation is dropped; a description may follow */
    ASSERT_TRUE(php_token_is(r, "Num::MAX", 14, CBM_DOCLINK_PHP_SEE));
    ASSERT_TRUE(php_token_is(r, "\\App\\Util\\Helper::$cache", 15, CBM_DOCLINK_PHP_SEE));
    ASSERT_TRUE(php_token_is(r, "clamp()", 16, CBM_DOCLINK_PHP_SEE));
    ASSERT_TRUE(php_token_is(r, "https://example.com/doc", 17, CBM_DOCLINK_HREF));
    /* an inline tag in an @see line is the tag's, once */
    ASSERT_TRUE(php_token_is(r, "Inline", 18, CBM_DOCLINK_PHP_SEE));
    ASSERT_TRUE(php_token_is(r, "parent::boot()", 26, CBM_DOCLINK_PHP_SEE));
    /* a code span, {@seealso}, a fenced block, @seealso: no token */
    ASSERT_EQ(dm_count_tokens(r, "Hidden"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Also"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Fenced"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Nope"), 0);
    ASSERT_EQ(r->doc_links.count, 8);
    cbm_free_result(r);
    PASS();
}

static bool php_has_line(const char *blob, const char *line) {
    size_t n = strlen(line);
    for (const char *p = blob; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len == n && memcmp(p, line, n) == 0) {
            return true;
        }
        p = nl ? nl + 1 : NULL;
    }
    printf("  no line: %s\n", line);
    return false;
}

TEST(doc_mentions_php_scope_blob) {
    CBMFileResult *r = dm_extract(PHP_USER, CBM_LANG_PHP, "src/Model/User.php");
    ASSERT_NOT_NULL(r);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT(strncmp(s, "php1\n", 5) == 0);
    ASSERT(php_has_line(s, "N\t0\tApp\\Model\t2\t35"));
    ASSERT(php_has_line(s, "U\t0\tc\tH\tApp\\Util\\Helper"));
    ASSERT(php_has_line(s, "U\t0\tc\tStrings\tApp\\Util\\Strings"));
    ASSERT(php_has_line(s, "U\t0\tc\tNum\tApp\\Util\\Numbers"));
    ASSERT(php_has_line(s, "U\t0\tf\ttwice\tApp\\Util\\twice"));
    ASSERT(php_has_line(s, "U\t0\tf\tclamp\tApp\\Util\\clamp"));
    /* supertypes qualified as PHP resolves them in the file */
    ASSERT(php_has_line(s, "T\tc\tApp\\Model\\User\t-\t21\t29"));
    ASSERT(php_has_line(s, "B\te\tApp\\Model\\Base"));
    ASSERT(php_has_line(s, "B\ti\tCountable"));
    ASSERT(php_has_line(s, "B\ti\tApp\\Model\\Shape"));
    ASSERT(php_has_line(s, "B\tt\tApp\\Model\\Named"));
    ASSERT(php_has_line(s, "B\tt\tApp\\Util\\Loud"));
    ASSERT(php_has_line(s, "K\tc\tLIMIT"));
    ASSERT(php_has_line(s, "K\tp\tname"));
    ASSERT(php_has_line(s, "K\tp\talias"));
    ASSERT(php_has_line(s, "K\tm\tcount"));
    ASSERT(php_has_line(s, "K\tp\tage"));
    ASSERT(php_has_line(s, "T\ti\tApp\\Model\\Shape\t-\t31\t31"));
    ASSERT(php_has_line(s, "B\ti\tJsonSerializable"));
    ASSERT(php_has_line(s, "T\tt\tApp\\Model\\Named\t-\t32\t32"));
    ASSERT(php_has_line(s, "K\tm\tlabel"));
    ASSERT(php_has_line(s, "T\te\tApp\\Model\\Suit\t-\t33\t33"));
    ASSERT(php_has_line(s, "K\te\tHearts"));
    ASSERT(php_has_line(s, "F\tApp\\Model\\make_user\t-"));
    /* the persisted blob keeps no line and no import */
    char *p = cbm_doclink_php_portable_scope(s);
    ASSERT_NOT_NULL(p);
    ASSERT(php_has_line(p, "N\t0\tApp\\Model"));
    ASSERT(php_has_line(p, "T\tc\tApp\\Model\\User\t-"));
    ASSERT(php_has_line(p, "B\te\tApp\\Model\\Base"));
    ASSERT(strstr(p, "\nU\t") == NULL);
    cbm_free(CBM_MEM_CLASS_OTHER, p);
    cbm_free_result(r);
    PASS();
}

/* Two namespace blocks of one file declare the same names: the graph keeps
 * one node for each pair, so the scope flags them. */
TEST(doc_mentions_php_scope_blocks) {
    CBMFileResult *r = dm_extract("<?php\n"
                                  "namespace A\\One {\n"
                                  "use X\\Y;\n"
                                  "class Same extends Y {}\n"
                                  "function same() {}\n"
                                  "}\n"
                                  "namespace A\\Two {\n"
                                  "class Same {}\n"
                                  "function same() {}\n"
                                  "function lone() {}\n"
                                  "if (!function_exists('cond')) {\n"
                                  "    function cond() {}\n"
                                  "}\n"
                                  "}\n",
                                  CBM_LANG_PHP, "src/blocks.php");
    ASSERT_NOT_NULL(r);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT(php_has_line(s, "N\t0\tA\\One\t2\t6"));
    ASSERT(php_has_line(s, "N\t1\tA\\Two\t7\t14"));
    ASSERT(php_has_line(s, "U\t0\tc\tY\tX\\Y"));
    ASSERT(php_has_line(s, "T\tc\tA\\One\\Same\td\t4\t4"));
    ASSERT(php_has_line(s, "B\te\tX\\Y"));
    ASSERT(php_has_line(s, "T\tc\tA\\Two\\Same\td\t8\t8"));
    ASSERT(php_has_line(s, "F\tA\\One\\same\td"));
    ASSERT(php_has_line(s, "F\tA\\Two\\same\td"));
    ASSERT(php_has_line(s, "F\tA\\Two\\lone\t-"));
    /* a conditional declaration */
    ASSERT(php_has_line(s, "F\tA\\Two\\cond\t-"));
    cbm_free_result(r);
    PASS();
}

/* ── resolution ──────────────────────────────────────────────────── */

static void php_write_repo(const char *repo) {
    th_write_file(TH_PATH(repo, "src/Util/Helper.php"),
                  "<?php\n"
                  "namespace App\\Util;\n"
                  "\n"
                  "class Helper { public static $cache; public static function format() {} }\n"
                  "class Strings {}\n"
                  "class Numbers { const MAX = 9; }\n"
                  "trait Loud { public function shout() {} }\n"
                  "function clamp($v) { return $v; }\n");
    th_write_file(TH_PATH(repo, "src/Model/Base.php"),
                  "<?php\n"
                  "namespace App\\Model;\n"
                  "\n"
                  "abstract class Base {\n"
                  "    /** The hook, {@see \\App\\Util\\Strings}. */\n"
                  "    protected $hook;\n"
                  "    public function boot() {}\n"
                  "    public function mixedCase() {}\n"
                  "}\n");
    th_write_file(TH_PATH(repo, "src/Dup.php"), "<?php\n"
                                                "namespace App\\One;\n"
                                                "class Same {}\n"
                                                "function same() {}\n"
                                                "namespace App\\Two;\n"
                                                "class Same {}\n"
                                                "function same() {}\n");
    th_write_file(TH_PATH(repo, "src/Twin/A.php"), "<?php\nnamespace App\\Twin;\nclass Twin {}\n");
    th_write_file(TH_PATH(repo, "src/Twin/B.php"), "<?php\nnamespace App\\Twin;\nclass Twin {}\n");
    th_write_file(TH_PATH(repo, "src/global.php"), "<?php\n"
                                                   "function global_helper() {}\n"
                                                   "function shadowed() {}\n"
                                                   "function other() {}\n"
                                                   "function size() {}\n"
                                                   "class GlobalThing {}\n");
    th_write_file(
        TH_PATH(repo, "src/Model/User.php"),
        "<?php\n"
        "namespace App\\Model;\n"
        "\n"
        "use App\\Util\\Helper as H;\n"
        "use App\\Util\\{Strings, Numbers as Num};\n"
        "use function App\\Util\\clamp;\n"
        "use Psr\\Log\\LoggerInterface;\n"
        "\n"
        "/**\n"
        " * Names: {@see H::format()}, {@see Strings}, {@see Num::MAX}, {@see H::$cache},\n"
        " * {@see clamp()}, {@see \\GlobalThing}, {@see GlobalThing},\n"
        " * {@see LoggerInterface}, {@see \\Exception}, {@see strlen()},\n"
        " * {@see Base::boot()}, {@see User::shout()}, {@see Base::nope()},\n"
        " * {@see \\App\\Twin\\Twin}, {@see \\App\\One\\Same}, {@see \\App\\One\\same()},\n"
        " * {@see App\\Util\\Strings}, {@see Nope}, {@see Suit::Hearts}.\n"
        " */\n"
        "class User extends Base\n"
        "{\n"
        "    use \\App\\Util\\Loud;\n"
        "    const LIMIT = 3;\n"
        "    public $name;\n"
        "    /**\n"
        "     * {@see self::size()}, {@see static::LIMIT}, {@see parent::boot()},\n"
        "     * {@see parent::gone()}, {@see self::$name}, {@see User::MIXEDCASE}.\n"
        "     */\n"
        "    public function count(): int { return 0; }\n"
        "    public function size(): int { return 0; }\n"
        "    /** {@see h::FORMAT()}, {@see shadowed()}, {@see global_helper()},\n"
        "     * {@see helper_here()}, {@see size()}, {@see other()}. */\n"
        "    public function other() {}\n"
        "}\n"
        "enum Suit { case Hearts; }\n"
        "function helper_here() {}\n"
        "function shadowed() {}\n");
}

static bool php_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 1) {
        printf("  edge %s -> %s: %d found\n", src, tgt, n);
        return false;
    }
    return true;
}

static bool php_row(const char *db, const char *raw, const char *reason) {
    char got[64];
    dm_row(db, "src/Model/User.php", raw, got, sizeof(got), NULL, 0);
    if (strcmp(got, reason) != 0) {
        printf("  row [%s]: \"%s\", expected \"%s\"\n", raw, got, reason);
        return false;
    }
    return true;
}

/* A repository indexed once for the resolution tests. */
static char g_php_tmp[256];
static char g_php_db[512];

static bool php_indexed(void) {
    if (g_php_db[0]) {
        return true;
    }
    snprintf(g_php_tmp, sizeof(g_php_tmp), "/tmp/cbm_dlphp_XXXXXX");
    if (!cbm_mkdtemp(g_php_tmp)) {
        return false;
    }
    char repo[512];
    snprintf(repo, sizeof(repo), "%s/repo", g_php_tmp);
    snprintf(g_php_db, sizeof(g_php_db), "%s/g.db", g_php_tmp);
    php_write_repo(repo);
    return dm_index(repo, g_php_db, NULL) == 0;
}

TEST(doc_mentions_php_resolution) {
    ASSERT_TRUE(php_indexed());
    const char *db = g_php_db;
    const char *u = "src.Model.User.User";
    /* use imports: an alias, a group member, a function import */
    ASSERT_TRUE(php_edge(db, u, "src.Util.Helper.Helper.format"));
    ASSERT_TRUE(php_edge(db, u, "src.Util.Helper.Strings"));
    ASSERT_TRUE(php_edge(db, u, "src.Util.Helper.clamp"));
    /* the current namespace; a fully qualified global type */
    ASSERT_TRUE(php_edge(db, u, "src.Model.Base.Base.boot"));
    ASSERT_TRUE(php_edge(db, u, "src.global.GlobalThing"));
    /* a member a used trait declares */
    ASSERT_TRUE(php_edge(db, u, "src.Util.Helper.Loud.shout"));
    /* constants, properties and enum cases have no node */
    ASSERT_TRUE(php_row(db, "Num::MAX", "graph_gap"));
    ASSERT_TRUE(php_row(db, "H::$cache", "graph_gap"));
    ASSERT_TRUE(php_row(db, "Suit::Hearts", "graph_gap"));
    /* an unqualified type is the namespace's: no global fallback */
    ASSERT_TRUE(php_row(db, "GlobalThing", "missing"));
    ASSERT_TRUE(php_row(db, "App\\Util\\Strings", "missing"));
    ASSERT_TRUE(php_row(db, "Nope", "missing"));
    /* a dependency, a built-in class, a built-in function */
    ASSERT_TRUE(php_row(db, "LoggerInterface", "external"));
    ASSERT_TRUE(php_row(db, "\\Exception", "external"));
    ASSERT_TRUE(php_row(db, "strlen()", "external"));
    /* every supertype in the repository: missing */
    ASSERT_TRUE(php_row(db, "Base::nope()", "missing"));
    /* two files declare it; one file declares it twice (one node) */
    ASSERT_TRUE(php_row(db, "\\App\\Twin\\Twin", "ambiguous"));
    ASSERT_TRUE(php_row(db, "\\App\\One\\Same", "graph_gap"));
    ASSERT_TRUE(php_row(db, "\\App\\One\\same()", "graph_gap"));
    /* a property's doc block belongs to its class */
    ASSERT_TRUE(php_edge(db, "src.Model.Base.Base", "src.Util.Helper.Strings"));
    /* aliases and method names compare without case */
    ASSERT_TRUE(php_edge(db, "src.Model.User.User.other", "src.Util.Helper.Helper.format"));
    PASS();
}

/* PHP's global fallback for an unqualified function, beyond phpDocumentor's
 * resolver: the namespace's own function first, then the global one. */
TEST(doc_mentions_php_global_fallback) {
    ASSERT_TRUE(php_indexed());
    const char *db = g_php_db;
    const char *o = "src.Model.User.User.other";
    ASSERT_TRUE(php_edge(db, o, "src.Model.User.shadowed"));
    ASSERT_TRUE(php_edge(db, o, "src.global.global_helper"));
    ASSERT_TRUE(php_edge(db, o, "src.Model.User.helper_here"));
    char props[512];
    int n = 0;
    dm_edge(db, o, "src.global.shadowed", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    PASS();
}

/* self, static and parent, beyond phpDocumentor's resolver: the type the doc
 * sits in and its superclass. */
TEST(doc_mentions_php_self_parent) {
    ASSERT_TRUE(php_indexed());
    const char *db = g_php_db;
    const char *c = "src.Model.User.User.count";
    ASSERT_TRUE(php_edge(db, c, "src.Model.User.User.size"));
    ASSERT_TRUE(php_edge(db, c, "src.Model.Base.Base.boot"));
    /* a bare member: no constant of that name, so a method, without case */
    ASSERT_TRUE(php_edge(db, c, "src.Model.Base.Base.mixedCase"));
    ASSERT_TRUE(php_row(db, "static::LIMIT", "graph_gap"));
    ASSERT_TRUE(php_row(db, "self::$name", "graph_gap"));
    ASSERT_TRUE(php_row(db, "parent::gone()", "missing"));
    PASS();
}

/* A bare name() in a type's doc: the type's own method first, though a
 * global function has the name too. */
TEST(doc_mentions_php_current_class) {
    ASSERT_TRUE(php_indexed());
    const char *db = g_php_db;
    const char *o = "src.Model.User.User.other";
    ASSERT_TRUE(php_edge(db, o, "src.Model.User.User.size"));
    char props[512];
    int n = 0;
    dm_edge(db, o, "src.global.size", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    /* the documented method itself: a self-mention, neither edge nor row */
    ASSERT_TRUE(php_row(db, "other()", ""));
    dm_edge(db, o, "src.global.other", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    PASS();
}

static void php_indexed_cleanup(void) {
    if (g_php_db[0]) {
        th_cleanup(g_php_tmp);
        g_php_db[0] = '\0';
    }
}

/* ── incremental == full; one worker == several ──────────────────── */

TEST(doc_mentions_php_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlphp_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    th_write_file(TH_PATH(repo, "src/lib.php"),
                  "<?php\nnamespace Lib;\nfunction a() {}\nfunction b() {}\n");
    th_write_file(TH_PATH(repo, "src/use.php"), "<?php\n"
                                                "namespace App;\n"
                                                "/** Uses {@see \\Lib\\a()}. */\n"
                                                "function user() {}\n");
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    ASSERT_TRUE(php_edge(inc_db, "src.use.user", "src.lib.a"));

    /* a doc edit: the file is repaired alone */
    th_write_file(TH_PATH(repo, "src/use.php"), "<?php\n"
                                                "namespace App;\n"
                                                "/** Uses {@see \\Lib\\a()} and {@see Nope}. */\n"
                                                "function user() {}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);

    /* a body edit in the target file: its persisted scope is unchanged */
    th_write_file(TH_PATH(repo, "src/lib.php"),
                  "<?php\nnamespace Lib;\n\nfunction a() { return 1; }\nfunction b() {}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(php_edge(inc_db, "src.use.user", "src.lib.a"));

    /* the function goes: the files that name it are repaired, nothing else */
    th_write_file(TH_PATH(repo, "src/lib.php"), "<?php\nnamespace Lib;\nfunction b() {}\n");
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "function removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
        0);
    char props[512];
    int n = 0;
    dm_edge(inc_db, "src.use.user", "src.lib.a", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    th_cleanup(tmp);
    PASS();
}

TEST(doc_mentions_php_workers_agree) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlphp_par_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char par_db[512];
    char seq_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    enum { MODULES = 60 }; /* past the parallel threshold */
    for (int i = 0; i < MODULES; i++) {
        char rel[64];
        char src[1024];
        int next = (i + 1) % MODULES;
        snprintf(rel, sizeof(rel), "src/M%02d.php", i);
        snprintf(src, sizeof(src),
                 "<?php\n"
                 "namespace App\\M%02d;\n"
                 "use App\\M%02d\\K%02d;\n"
                 "/** F%02d; see {@see K%02d::run()}, {@see K%02d::$gone},\n"
                 " * {@see \\Exception}, {@see Nope} and {@see \\App\\M%02d\\f%02d()}. */\n"
                 "function f%02d() {}\n"
                 "class K%02d {\n  public function run() {}\n}\n",
                 i, next, next, i, next, next, next, next, i, i);
        th_write_file(TH_PATH(repo, rel), src);
    }
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    ASSERT_TRUE(php_edge(par_db, "src.M00.f00", "src.M01.K01.run"));
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_mentions_php) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_php_tokens);
    RUN_TEST(doc_mentions_php_scope_blob);
    RUN_TEST(doc_mentions_php_scope_blocks);
    RUN_TEST(doc_mentions_php_resolution);
    RUN_TEST(doc_mentions_php_global_fallback);
    RUN_TEST(doc_mentions_php_self_parent);
    RUN_TEST(doc_mentions_php_current_class);
    php_indexed_cleanup();
    RUN_TEST(doc_mentions_php_incremental_equals_full);
    RUN_TEST(doc_mentions_php_workers_agree);
}
