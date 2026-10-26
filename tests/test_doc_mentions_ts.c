/*
 * test_doc_mentions_ts.c -- TypeScript and JavaScript doc-comment references
 * (JSDoc and TSDoc {@link}, {@linkcode}, @see).
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

static const char *const TS_WIDGET =
    "import { Helper, gadget as g } from \"./helper\";\n"                      /* 1 */
    "import * as ns from \"./helper\";\n"                                      /* 2 */
    "import Def from \"./def\";\n"                                             /* 3 */
    "\n"                                                                       /* 4 */
    "/**\n"                                                                    /* 5 */
    " * The widget. See {@link Helper} and {@link Widget#spin | spin}, also\n" /* 6 */
    " * {@linkcode ns.Helper}. Not `{@link Hidden}` nor {@linkplain Plain}.\n" /* 7 */
    " * ```ts\n"                                                               /* 8 */
    " * {@link Fenced}\n"                                                      /* 9 */
    " * ```\n"                                                                 /* 10 */
    " * @see Gadget.\n"                                                        /* 11 */
    " * @see {@link Other}\n"                                                  /* 12 */
    " * @see https://example.org/w\n"                                          /* 13 */
    " * @see aria-colcount for details\n"                                      /* 14 */
    " */\n"                                                                    /* 15 */
    "export class Widget {\n"                                                  /* 16 */
    "  /**\n"                                                                  /* 17 */
    "   * Spins n times, see {@link n} and {@link Widget.size}.\n"             /* 18 */
    "   */\n"                                                                  /* 19 */
    "  spin(n: number): void {}\n"                                             /* 20 */
    "}\n"                                                                      /* 21 */
    "\n"                                                                       /* 22 */
    "export namespace Space {\n"                                               /* 23 */
    "  export namespace Inner {\n"                                             /* 24 */
    "    export function deep() {}\n"                                          /* 25 */
    "  }\n"                                                                    /* 26 */
    "}\n"                                                                      /* 27 */
    "function hidden() {}\n"                                                   /* 28 */
    "export * from \"./a\";\n"                                                 /* 29 */
    "export * as all from \"./b\";\n"                                          /* 30 */
    "export { x as y } from \"./c\";\n"                                        /* 31 */
    "export { hidden as shown };\n"                                            /* 32 */
    "export default Widget;\n";                                                /* 33 */

static bool ts_token_is(const CBMFileResult *r, const char *raw, uint32_t line, int syntax) {
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

TEST(doc_mentions_ts_tokens) {
    CBMFileResult *r = dm_extract(TS_WIDGET, CBM_LANG_TYPESCRIPT, "src/widget.ts");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(ts_token_is(r, "Helper", 6, CBM_DOCLINK_TS_LINK));
    ASSERT_TRUE(ts_token_is(r, "Widget#spin", 6, CBM_DOCLINK_TS_LINK));
    ASSERT_TRUE(ts_token_is(r, "ns.Helper", 7, CBM_DOCLINK_TS_LINKCODE));
    ASSERT_TRUE(ts_token_is(r, "Gadget", 11, CBM_DOCLINK_TS_SEE));
    /* an inline tag in an @see line is the tag's, once */
    ASSERT_TRUE(ts_token_is(r, "Other", 12, CBM_DOCLINK_TS_LINK));
    ASSERT_TRUE(ts_token_is(r, "https://example.org/w", 13, CBM_DOCLINK_HREF));
    ASSERT_TRUE(ts_token_is(r, "Widget.size", 18, CBM_DOCLINK_TS_LINK));
    /* a code span, {@linkplain}, a fenced block, prose, the method's own
     * parameter: no token */
    ASSERT_EQ(dm_count_tokens(r, "Hidden"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Plain"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Fenced"), 0);
    ASSERT_EQ(dm_count_tokens(r, "aria-colcount"), 0);
    ASSERT_EQ(dm_count_tokens(r, "n"), 0);
    ASSERT_EQ(r->doc_links.count, 7);
    cbm_free_result(r);
    PASS();
}

static bool ts_has_line(const char *blob, const char *line) {
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

TEST(doc_mentions_ts_scope_blob) {
    CBMFileResult *r = dm_extract(TS_WIDGET, CBM_LANG_TYPESCRIPT, "src/widget.ts");
    ASSERT_NOT_NULL(r);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT(strncmp(s, "ts1\n", 4) == 0);
    ASSERT(ts_has_line(s, "I\tHelper\tn\t./helper\tHelper"));
    ASSERT(ts_has_line(s, "I\tg\tn\t./helper\tgadget"));
    ASSERT(ts_has_line(s, "I\tns\ts\t./helper\t-"));
    ASSERT(ts_has_line(s, "I\tDef\td\t./def\t-"));
    ASSERT(ts_has_line(s, "D\tWidget\te"));
    ASSERT(ts_has_line(s, "D\tSpace\te"));
    ASSERT(ts_has_line(s, "D\thidden\t-"));
    ASSERT(ts_has_line(s, "N\tSpace\t23\t27"));
    ASSERT(ts_has_line(s, "N\tSpace.Inner\t24\t26"));
    ASSERT(ts_has_line(s, "A\t./a"));
    ASSERT(ts_has_line(s, "S\tall\t./b"));
    ASSERT(ts_has_line(s, "F\ty\t./c\tx"));
    ASSERT(ts_has_line(s, "L\tshown\thidden"));
    ASSERT(ts_has_line(s, "L\tdefault\tWidget"));
    ASSERT(ts_has_line(s, "M"));
    /* the persisted blob keeps no line */
    char *p = cbm_doclink_ts_portable_scope(s);
    ASSERT_NOT_NULL(p);
    ASSERT(ts_has_line(p, "N\tSpace"));
    ASSERT(ts_has_line(p, "N\tSpace.Inner"));
    ASSERT(ts_has_line(p, "D\tWidget\te"));
    cbm_free(CBM_MEM_CLASS_OTHER, p);
    cbm_free_result(r);
    PASS();
}

/* ── resolution ──────────────────────────────────────────────────── */

static void ts_write_repo(const char *repo) {
    th_write_file(TH_PATH(repo, "src/helper.ts"), "/** A helper. */\n"
                                                  "export class Helper {\n"
                                                  "  run(): void {}\n"
                                                  "}\n"
                                                  "export function gadget() {}\n"
                                                  "function internal() {}\n");
    th_write_file(TH_PATH(repo, "src/def.ts"), "export default class Def {}\n");
    th_write_file(TH_PATH(repo, "src/barrel/index.ts"),
                  "export * from \"../helper\";\nexport * from \"./more\";\n"
                  "export function inBarrel() {}\n");
    th_write_file(TH_PATH(repo, "src/barrel/more.ts"), "export function extra() {}\n");
    th_write_file(TH_PATH(repo, "src/dup1.ts"), "export function twin() {}\n");
    th_write_file(TH_PATH(repo, "src/dup2.ts"), "export function twin() {}\n");
    th_write_file(TH_PATH(repo, "src/twins.ts"),
                  "export * from \"./dup1\";\nexport * from \"./dup2\";\n");
    /* functions declared in a function body: visible there, under file-level
     * names in the graph */
    th_write_file(TH_PATH(repo, "src/closure.ts"),
                  "export function outer() {\n"
                  "  /** Calls {@link helperInside} and {@link uniqueInside}. */\n"
                  "  function inside() {}\n"
                  "  function helperInside() {}\n"
                  "  function uniqueInside() {}\n"
                  "}\n"
                  "export function other() {\n"
                  "  function helperInside() {}\n"
                  "}\n");
    /* a script: no import, no export, so its names are global */
    th_write_file(TH_PATH(repo, "src/global.js"), "function legacyGlobal() {}\n");
    /* a tsconfig path alias and a workspace package */
    th_write_file(TH_PATH(repo, "tsconfig.json"),
                  "{ \"compilerOptions\": { \"paths\": { \"@lib/*\": [\"src/lib/*\"] } } }\n");
    th_write_file(TH_PATH(repo, "src/lib/aliased.ts"), "export function aliased() {}\n");
    th_write_file(TH_PATH(repo, "packages/util/package.json"),
                  "{ \"name\": \"@acme/util\", \"main\": \"src/index.ts\" }\n");
    th_write_file(TH_PATH(repo, "packages/util/src/index.ts"), "export function utilFn() {}\n");
    th_write_file(TH_PATH(repo, "src/widget.ts"),
                  "import { aliased } from \"@lib/aliased\";\n"
                  "import { utilFn } from \"@acme/util\";\n"
                  "import { inBarrel } from \"./barrel\";\n"
                  "import { Helper, gadget as g } from \"./helper\";\n"
                  "import * as ns from \"./helper\";\n"
                  "import Def from \"./def\";\n"
                  "import { extra } from \"./barrel\";\n"
                  "import { twin } from \"./twins\";\n"
                  "import { Thing } from \"some-package\";\n"
                  "import { Gone } from \"./nowhere\";\n"
                  "\n"
                  "/**\n"
                  " * Links: {@link Helper}, {@link Helper.run}, {@link g}, {@link ns.gadget},\n"
                  " * {@link Def}, {@link extra}, {@link twin}, {@link Thing}, {@link Gone},\n"
                  " * {@link Promise}, {@link legacyGlobal}, {@link Nope}, {@link spin},\n"
                  " * {@link Space.Inner.deep}, {@link Helper#missingMember}, {@link aliased},\n"
                  " * {@link utilFn}, {@link inBarrel}.\n"
                  " */\n"
                  "export class Widget {\n"
                  "  spin(): void {}\n"
                  "  /** Turns, see {@link this.spin}. */\n"
                  "  turn(): void {}\n"
                  "}\n"
                  "\n"
                  "export namespace Space {\n"
                  "  export namespace Inner {\n"
                  "    /** Deep, see {@link deep2}. */\n"
                  "    export function deep() {}\n"
                  "    export function deep2() {}\n"
                  "  }\n"
                  "}\n");
}

static bool ts_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 1) {
        printf("  edge %s -> %s: %d found\n", src, tgt, n);
        return false;
    }
    return true;
}

static bool ts_row(const char *db, const char *raw, const char *reason) {
    char got[64];
    dm_row(db, "src/widget.ts", raw, got, sizeof(got), NULL, 0);
    if (strcmp(got, reason) != 0) {
        printf("  row [%s]: \"%s\", expected \"%s\"\n", raw, got, reason);
        return false;
    }
    return true;
}

TEST(doc_mentions_ts_resolution) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlts_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    ts_write_repo(repo);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    const char *w = "src.widget.Widget";
    /* imports: named, aliased, a member of one, a namespace, a default */
    ASSERT_TRUE(ts_edge(db, w, "src.helper.Helper"));
    ASSERT_TRUE(ts_edge(db, w, "src.helper.Helper.run"));
    ASSERT_TRUE(ts_edge(db, w, "src.helper.gadget"));
    ASSERT_TRUE(ts_edge(db, w, "src.def.Def"));
    /* through an export * barrel; two barrels that answer differently */
    ASSERT_TRUE(ts_edge(db, w, "src.barrel.more.extra"));
    ASSERT_TRUE(ts_row(db, "twin", "ambiguous"));
    /* a package, a relative import the index does not hold, the standard
     * library */
    ASSERT_TRUE(ts_row(db, "Thing", "external"));
    ASSERT_TRUE(ts_row(db, "Gone", "not_indexed"));
    ASSERT_TRUE(ts_row(db, "Promise", "external"));
    /* a script's global; nothing anywhere */
    ASSERT_TRUE(ts_edge(db, w, "src.global.legacyGlobal"));
    ASSERT_TRUE(ts_row(db, "Nope", "missing"));
    /* the class's own member; a path through namespaces */
    ASSERT_TRUE(ts_edge(db, w, "src.widget.Widget.spin"));
    ASSERT_TRUE(ts_edge(db, w, "src.widget.Space.Inner.deep"));
    /* a class member without a node of its own */
    ASSERT_TRUE(ts_row(db, "Helper#missingMember", "graph_gap"));
    /* a tsconfig path alias; a workspace package's entry module */
    ASSERT_TRUE(ts_edge(db, w, "src.lib.aliased.aliased"));
    ASSERT_TRUE(ts_edge(db, w, "packages.util.src.utilFn"));
    /* a declaration of an index file (its QN has no `index`) */
    ASSERT_TRUE(ts_edge(db, w, "src.barrel.inBarrel"));
    /* this. is the container's; a namespace's own names first */
    ASSERT_TRUE(ts_edge(db, "src.widget.Widget.turn", "src.widget.Widget.spin"));
    ASSERT_TRUE(ts_edge(db, "src.widget.Space.Inner.deep", "src.widget.Space.Inner.deep2"));
    /* a function body's own declaration binds; two of one name share one node */
    ASSERT_TRUE(ts_edge(db, "src.closure.inside", "src.closure.uniqueInside"));
    char reason[64];
    dm_row(db, "src/closure.ts", "helperInside", reason, sizeof(reason), NULL, 0);
    ASSERT_STR_EQ(reason, "graph_gap");
    th_cleanup(tmp);
    PASS();
}

/* ── incremental == full; one worker == several ──────────────────── */

TEST(doc_mentions_ts_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlts_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    th_write_file(TH_PATH(repo, "src/lib.ts"), "export function a() {}\nexport function b() {}\n");
    th_write_file(TH_PATH(repo, "src/use.ts"), "import { a } from \"./lib\";\n"
                                               "/** Uses {@link a}. */\n"
                                               "export function use() {}\n");
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    ASSERT_TRUE(ts_edge(inc_db, "src.use.use", "src.lib.a"));

    /* a doc edit: the file is repaired alone */
    th_write_file(TH_PATH(repo, "src/use.ts"), "import { a } from \"./lib\";\n"
                                               "/** Uses {@link a}, and {@link Nope}. */\n"
                                               "export function use() {}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);

    /* a body edit in the target file: its scope is unchanged */
    th_write_file(TH_PATH(repo, "src/lib.ts"),
                  "export function a() { return 1; }\nexport function b() {}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(ts_edge(inc_db, "src.use.use", "src.lib.a"));

    /* an export goes: the declaration's record changes, a full run */
    th_write_file(TH_PATH(repo, "src/lib.ts"), "function a() {}\nexport function b() {}\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "export removed", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    char props[512];
    int n = 0;
    dm_edge(inc_db, "src.use.use", "src.lib.a", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    th_cleanup(tmp);
    PASS();
}

TEST(doc_mentions_ts_workers_agree) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlts_par_XXXXXX");
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
        snprintf(rel, sizeof(rel), "src/m%02d.ts", i);
        snprintf(src, sizeof(src),
                 "import { F%02d, K%02d } from \"./m%02d\";\n"
                 "/** F%02d; see {@link F%02d}, {@link K%02d.run}, {@link K%02d#gone},\n"
                 " * {@link Promise}, {@link Nope} and {@link F%02d}. */\n"
                 "export function F%02d() {}\n"
                 "export class K%02d {\n  run() {}\n}\n",
                 next, next, next, i, next, next, next, i, i, i);
        th_write_file(TH_PATH(repo, rel), src);
    }
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    ASSERT_TRUE(ts_edge(par_db, "src.m00.F00", "src.m01.K01.run"));
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_mentions_ts) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_ts_tokens);
    RUN_TEST(doc_mentions_ts_scope_blob);
    RUN_TEST(doc_mentions_ts_resolution);
    RUN_TEST(doc_mentions_ts_incremental_equals_full);
    RUN_TEST(doc_mentions_ts_workers_agree);
}
