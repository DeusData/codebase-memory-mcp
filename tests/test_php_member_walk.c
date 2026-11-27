/*
 * test_php_member_walk.c — PHP on the shared member lookup (lsp/member_walk.h),
 * and the PHP imports the engine reads (slice B4, at the end).
 *
 * Each fixture is indexed through the real pipeline twice: as it is (fewer than
 * 50 files: the sequential resolver) and padded past 50 files (the parallel
 * resolver); both must give the same CALLS edge. The PHP each fixture encodes
 * was run with php 8.4 (docker php:8.4-cli-bookworm, script on stdin); the
 * method PHP runs is named in a comment beside each assertion.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    const char *name;
    const char *content;
} PmwFile;

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
    cbm_store_t *store;
} PmwProj;

enum { PMW_PAD = 52, PMW_FILES_MAX = 80 };

static void pmw_close(PmwProj *p) {
    if (p->store) {
        cbm_store_close(p->store);
    }
    if (p->srv) {
        cbm_mcp_server_free(p->srv);
    }
    free(p->project);
    if (p->tmpdir[0]) {
        th_rmtree(p->tmpdir);
    }
    if (p->dbpath[0]) {
        unlink(p->dbpath);
        char side[600];
        snprintf(side, sizeof(side), "%s-wal", p->dbpath);
        unlink(side);
        snprintf(side, sizeof(side), "%s-shm", p->dbpath);
        unlink(side);
    }
    memset(p, 0, sizeof(*p));
}

/* Write the files (plus PMW_PAD Python files when padded) and index them. */
static bool pmw_index(PmwProj *p, const PmwFile *files, int n, bool padded) {
    memset(p, 0, sizeof(*p));
    snprintf(p->tmpdir, sizeof(p->tmpdir), "/tmp/cbm_pmw_XXXXXX");
    if (!cbm_mkdtemp(p->tmpdir)) {
        p->tmpdir[0] = '\0';
        return false;
    }
    int total = padded ? n + PMW_PAD : n;
    for (int i = 0; i < total; i++) {
        char path[700];
        char body[96];
        const char *name;
        const char *content;
        char pad_name[64];
        if (i < n) {
            name = files[i].name;
            content = files[i].content;
        } else {
            snprintf(pad_name, sizeof(pad_name), "pad/pad_%02d.py", i - n);
            snprintf(body, sizeof(body), "def pad_%02d():\n    return %d\n", i - n, i - n);
            name = pad_name;
            content = body;
        }
        snprintf(path, sizeof(path), "%s/%s", p->tmpdir, name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(p->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f) {
            return false;
        }
        fputs(content, f);
        fclose(f);
    }
    p->project = cbm_project_name_from_path(p->tmpdir);
    if (!p->project) {
        return false;
    }
    const char *home = getenv("HOME");
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home ? home : "/tmp");
    cbm_mkdir(cache_dir);
    snprintf(p->dbpath, sizeof(p->dbpath), "%s/%s.db", cache_dir, p->project);
    unlink(p->dbpath);
    p->srv = cbm_mcp_server_new(NULL);
    if (!p->srv) {
        return false;
    }
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", p->tmpdir);
    char *resp = cbm_mcp_handle_tool(p->srv, "index_repository", args);
    free(resp);
    p->store = cbm_store_open_path(p->dbpath);
    return p->store != NULL;
}

static bool pmw_tail(const char *qn, const char *tail) {
    size_t q = qn ? strlen(qn) : 0;
    size_t t = strlen(tail);
    return q >= t && strcmp(qn + q - t, tail) == 0 && (q == t || qn[q - t - 1] == '.');
}

/* The strategy of the CALLS edge from a node whose QN ends in src_tail to one
 * whose QN ends in tgt_tail, into out; false when there is no such edge. */
static bool pmw_edge(PmwProj *p, const char *src_tail, const char *tgt_tail, char *out,
                     size_t out_len) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    bool found = false;
    if (out_len) {
        out[0] = '\0';
    }
    if (cbm_store_find_edges_by_type(p->store, p->project, "CALLS", &edges, &n) != CBM_STORE_OK) {
        return false;
    }
    for (int i = 0; i < n && !found; i++) {
        cbm_node_t src;
        cbm_node_t tgt;
        if (cbm_store_find_node_by_id(p->store, edges[i].source_id, &src) != CBM_STORE_OK) {
            continue;
        }
        if (cbm_store_find_node_by_id(p->store, edges[i].target_id, &tgt) != CBM_STORE_OK) {
            cbm_node_free_fields(&src);
            continue;
        }
        if (pmw_tail(src.qualified_name, src_tail) && pmw_tail(tgt.qualified_name, tgt_tail)) {
            found = true;
            const char *props = edges[i].properties_json ? edges[i].properties_json : "";
            const char *s = strstr(props, "\"strategy\":\"");
            if (s && out_len) {
                s += strlen("\"strategy\":\"");
                size_t k = 0;
                while (s[k] && s[k] != '"' && k + 1 < out_len) {
                    out[k] = s[k];
                    k++;
                }
                out[k] = '\0';
            }
        }
        cbm_node_free_fields(&src);
        cbm_node_free_fields(&tgt);
    }
    cbm_store_free_edges(edges, n);
    return found;
}

static bool pmw_php_strategy(const char *s) {
    return strncmp(s, "php_", 4) == 0;
}

/* One expectation on a fixture: the edge src -> tgt, and whether the PHP
 * engine drew it (strategy php_*) or not; want_edge false = no edge at all.
 * want_edge false with want_php true: no edge the PHP engine drew, whatever a
 * name guess does (main's guesses differ by route on some shapes). */
typedef struct {
    const char *src;
    const char *tgt;
    bool want_edge;
    bool want_php; /* when want_edge: drawn by the PHP engine (true) or by a name guess */
} PmwExpect;

/* Index the fixture on both routes and check every expectation on each. */
static int pmw_check(const PmwFile *files, int n, const PmwExpect *want, int nwant) {
    int failures = 0;
    for (int route = 0; route < 2; route++) {
        PmwProj p;
        if (!pmw_index(&p, files, n, route == 1)) {
            printf("  [pmw] index failed (route %d)\n", route);
            pmw_close(&p);
            return failures + 1;
        }
        for (int i = 0; i < nwant; i++) {
            char strategy[64];
            bool edge = pmw_edge(&p, want[i].src, want[i].tgt, strategy, sizeof(strategy));
            bool no_typed = !want[i].want_edge && want[i].want_php;
            bool ok = no_typed ? !edge || !pmw_php_strategy(strategy)
                               : edge == want[i].want_edge &&
                                     (!edge || pmw_php_strategy(strategy) == want[i].want_php);
            if (!ok) {
                printf("  [pmw] %s route: %s -> %s: edge=%d strategy=%s (want edge=%d php=%d)\n",
                       route ? "parallel" : "sequential", want[i].src, want[i].tgt, edge,
                       strategy, want[i].want_edge, want[i].want_php);
                failures++;
            }
        }
        pmw_close(&p);
    }
    return failures;
}

/* The parent's concrete method, not the interface's declaration: today's
 * walk searched the interfaces first (a stack over the bases, last listed
 * first). php: `$l->both()` runs Base::both. Partner: a method only the
 * interface declares answers from the interface. */
TEST(phpmw_parent_method_beats_interface_declaration) {
    static const PmwFile f[] = {
        {"src/Shapes.php", "<?php\n"
                           "interface Ifc { public function both(): string;"
                           " public function idecl(): string; }\n"
                           "class Base { public function both(): string { return 'b'; } }\n"
                           "abstract class Leaf extends Base implements Ifc {}\n"
                           "class Caller {\n"
                           "    public function run(Leaf $l): void { $l->both(); }\n"
                           "    public function decl(Leaf $l): void { $l->idecl(); }\n"
                           "}\n"},
    };
    static const PmwExpect want[] = {
        {"Caller.run", "Base.both", true, true},
        {"Caller.run", "Ifc.both", false, false},
        {"Caller.decl", "Ifc.idecl", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 3), 0);
    PASS();
}

/* A chain deeper than today's walk could see (it stopped after 32 types). */
TEST(phpmw_chain_past_the_old_cap) {
    static char src[4096];
    size_t used = (size_t)snprintf(src, sizeof(src),
                                   "<?php\nclass C0 { public function rootm(): int { return 0; } }\n");
    for (int i = 1; i <= 40; i++) {
        used += (size_t)snprintf(src + used, sizeof(src) - used, "class C%d extends C%d {}\n", i,
                                 i - 1);
    }
    snprintf(src + used, sizeof(src) - used,
             "class Caller { public function run(C40 $c): void { $c->rootm(); } }\n");
    PmwFile f[] = {{"src/Chain.php", src}};
    static const PmwExpect want[] = {
        {"Caller.run", "C0.rootm", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 1), 0);
    PASS();
}

/* A parent outside the project: the lookup passes it as an unknown place, so
 * the interface's declaration behind it is a possible answer (prefix not
 * complete), drawn as today's engine drew it. php: Vendor's both() runs; the
 * edge names the declaration the call is checked against. Kept, not new:
 * the base draws the same. Partner: the class's own method. */
TEST(phpmw_vendor_parent_keeps_todays_answer) {
    static const PmwFile f[] = {
        {"src/Kid.php", "<?php\n"
                        "namespace App;\n"
                        "use Vendor\\Pkg\\Base;\n"
                        "interface Ifc { public function both(): string; }\n"
                        "class Kid extends Base implements Ifc {\n"
                        "    public function own(): int { return 1; }\n"
                        "}\n"
                        "class Caller {\n"
                        "    public function run(Kid $k): void { $k->both(); $k->own(); }\n"
                        "}\n"},
    };
    static const PmwExpect want[] = {
        {"Caller.run", "Ifc.both", true, true},
        {"Caller.run", "Kid.own", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* A trait from outside the project beside the parent's method: passed as an
 * unknown place, the parent's method a possible answer, drawn as today (the
 * vendor trait could shadow it; then PHP runs the trait's). Kept, not new.
 * Partner: the same name on an unrelated class gets no edge. */
TEST(phpmw_vendor_trait_keeps_todays_answer) {
    static const PmwFile f[] = {
        {"app/Models/Contact.php",
         "<?php\nnamespace App\\Models;\nuse Vendor\\Pkg\\HasFactory;\n"
         "class BaseModel { public function helper(): int { return 1; }"
         " public function getName(): string { return 'b'; } }\n"
         "class Contact extends BaseModel {\n    use HasFactory;\n"
         "    public function run(): void { $this->helper(); $this->getName(); }\n}\n"
         "class Other { public function getName(): string { return 'o'; } }\n"},
    };
    static const PmwExpect want[] = {
        {"Contact.run", "BaseModel.helper", true, true},
        {"Contact.run", "BaseModel.getName", true, true},
        {"Contact.run", "Other.getName", false, false},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 3), 0);
    PASS();
}

/* `B::m insteadof A`: the trait the clause names answers (today the clause was
 * never read and the first trait's copy won). php: B::m runs, its Y return
 * makes pick() Y::pick. */
TEST(phpmw_insteadof_picks_the_named_trait) {
    static const PmwFile f[] = {
        {"src/Pick.php", "<?php\n"
                         "class X { public function pick(): string { return 'x'; } }\n"
                         "class Y { public function pick(): string { return 'y'; } }\n"
                         "trait A { public function m(): X { return new X(); } }\n"
                         "trait B { public function m(): Y { return new Y(); } }\n"
                         "class C {\n"
                         "    use A, B { B::m insteadof A; }\n"
                         "    public function run(): void { $this->m()->pick(); }\n"
                         "}\n"},
    };
    static const PmwExpect want[] = {
        {"C.run", "Y.pick", true, true},
        {"C.run", "X.pick", false, false},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* `make as build`: the alias answers with the trait's method (today the
 * clause was never read). php: build() runs T::make; spin() is Z::spin.
 * Partner: the original name answers too. */
TEST(phpmw_trait_alias_answers) {
    static const PmwFile f[] = {
        {"src/Alias.php", "<?php\n"
                          "class Z { public function spin(): int { return 1; } }\n"
                          "class Q { public function spin(): int { return 2; } }\n"
                          "trait T { public function make(): Z { return new Z(); } }\n"
                          "class C {\n"
                          "    use T { make as build; }\n"
                          "    public function viaAlias(): void { $this->build()->spin(); }\n"
                          "    public function viaName(): void { $this->make()->spin(); }\n"
                          "}\n"},
    };
    static const PmwExpect want[] = {
        {"C.viaAlias", "Z.spin", true, true},
        {"C.viaName", "Z.spin", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* A trait used by a class declared in another file: today the cross-file pass
 * knew nothing of it (trait uses were not extracted). The caller's per-file
 * registry holds its own module and the modules it imports, so the trait
 * answers when it is declared there (a trait in a module the caller does not
 * import stays unknown, as a parent class there does). php: widget() runs
 * HasWidget::widget, spin() is Widget::spin. */
TEST(phpmw_cross_file_trait_member) {
    static const PmwFile f[] = {
        {"lib/User.php", "<?php\nnamespace Lib;\n"
                         "class Widget { public function spin(): int { return 1; } }\n"
                         "trait HasWidget { public function widget(): Widget"
                         " { return new Widget(); } }\n"
                         "class User { use HasWidget; }\n"},
        {"lib/Gadget.php", "<?php\nnamespace Lib;\n"
                           "class Gadget { public function spin(): int { return 2; } }\n"},
        {"app/Run.php", "<?php\nnamespace App;\n"
                        "use Lib\\User;\n"
                        "class Runner {\n"
                        "    public function run(User $u): void { $u->widget()->spin(); }\n"
                        "}\n"},
    };
    static const PmwExpect want[] = {
        {"Runner.run", "Widget.spin", true, true},
        {"Runner.run", "Gadget.spin", false, false},
    };
    ASSERT_EQ(pmw_check(f, 3, want, 2), 0);
    PASS();
}

/* A trait's abstract method is a requirement the parent satisfies. php:
 * `$this->need()` in a class using the trait runs Base::need. (With no
 * implementation anywhere the declaration answers: member_walk's
 * member_walk_mixin_requirement_defers.) */
TEST(phpmw_trait_abstract_method_is_a_requirement) {
    static const PmwFile f[] = {
        {"src/Req.php", "<?php\n"
                        "trait Needs { abstract public function need(): int;"
                        " public function useIt(): int { return $this->need(); } }\n"
                        "class Base { public function need(): int { return 1; } }\n"
                        "class Kid extends Base { use Needs;"
                        " public function run(): int { return $this->need(); } }\n"},
    };
    static const PmwExpect want[] = {
        {"Kid.run", "Base.need", true, true},
        {"Kid.run", "Needs.need", false, false},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* A trait spelled in another file names, there, a vendor trait; a project
 * trait with the same short name is not it (F2, fx/vtrait; php: the vendor
 * trait runs, `Right`, never Wrong::spin). Nothing is drawn from the wrong
 * trait. Partner: a class whose file imports the project trait gets its
 * member, typed. */
TEST(phpmw_trait_resolved_where_written) {
    static const PmwFile f[] = {
        {"src/Other/Stuff.php",
         "<?php\nnamespace App\\Other;\n"
         "trait HasThing { public function thing(): Wrong { return new Wrong(); } }\n"
         "class Wrong { public function spin() { return 'Wrong'; } }\n"
         "class Helper { public static function make() { return 1; } }\n"},
        {"src/Models/Model.php", "<?php\nnamespace App\\Models;\nuse Vendor\\Pkg\\HasThing;\n"
                                 "class Model { use HasThing; }\n"},
        {"src/Models/Good.php", "<?php\nnamespace App\\Models;\nuse App\\Other\\HasThing;\n"
                                "class Good { use HasThing; }\n"},
        {"src/Other2/Right.php", "<?php\nnamespace App\\Other2;\n"
                                 "class Right { public function spin() { return 'Right'; } }\n"},
        {"src/Http/Caller.php",
         "<?php\nnamespace App\\Http;\nuse App\\Models\\Model;\nuse App\\Models\\Good;\n"
         "use App\\Other\\Helper;\n"
         "class Caller {\n"
         "    public function run(Model $m) { Helper::make(); return $m->thing()->spin(); }\n"
         "    public function ok(Good $g) { return $g->thing()->spin(); }\n"
         "}\n"},
    };
    static const PmwExpect want[] = {
        {"Caller.run", "Wrong.spin", false, false},
        {"Caller.ok", "Wrong.spin", true, true},
    };
    ASSERT_EQ(pmw_check(f, 5, want, 2), 0);
    PASS();
}

/* A file with several namespaces (phpunit's bundled tools): a trait declared
 * in a later one is known by its own namespace, not the file's first. php:
 * Zs::spin runs. Partner: the same-named method of another class gets no
 * edge. */
TEST(phpmw_trait_in_a_later_namespace) {
    static const PmwFile f[] = {
        {"src/Bundle.php", "<?php\nnamespace App;\nuse Lib\\HasZ;\n"
                           "class Zdecoy { public function spin(): int { return 2; } }\n"
                           "class UsesZ {\n    use HasZ;\n"
                           "    public function run(): void { $this->z()->spin(); }\n}\n"
                           "namespace Lib;\n"
                           "class Zs { public function spin(): int { return 1; } }\n"
                           "trait HasZ { public function z(): Zs { return new Zs; } }\n"},
    };
    static const PmwExpect want[] = {
        {"UsesZ.run", "Zs.spin", true, true},
        {"UsesZ.run", "Zdecoy.spin", false, false},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* A trait's abstract method beside a vendor trait: the vendor trait may
 * implement it (php: fx/req2, `Zv`, its m runs), so the requirement itself
 * answers, as today's flattened copy did, and the parent's m is not reached
 * (F3). Partner: the requirement's return type still types the chained call,
 * as on main. */
TEST(phpmw_requirement_beside_vendor_trait) {
    static const PmwFile f[] = {
        {"src/Req2.php",
         "<?php\nnamespace App\\Req2;\nuse Vendor\\Pkg\\VTrait;\n"
         "class Zi { public function spin() { return 'Zi'; } }\n"
         "class Zp extends Zi { public function spin() { return 'Zp'; } }\n"
         "class Zv extends Zp { public function spin() { return 'Zv'; } }\n"
         "trait NeedM { abstract public function m(): Zi; }\n"
         "class P { public function m(): Zp { return new Zp; } }\n"
         "class C extends P {\n    use NeedM, VTrait;\n"
         "    public function run() { return $this->m()->spin(); }\n}\n"
         "class QCaller { public function go(C $c) { return $c->m()->spin(); } }\n"},
    };
    static const PmwExpect want[] = {
        {"C.run", "P.m", false, false},
        {"C.run", "Zp.spin", false, false},
        {"C.run", "Zi.spin", true, true},
        {"QCaller.go", "P.m", false, false},
        {"QCaller.go", "Zi.spin", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 5), 0);
    PASS();
}

/* Behind a vendor parent both interfaces declare m (fx/twoifc): a possible
 * answer, drawn because today's engine drew one here; the walk's order (the
 * first interface, as php's member table) stands where the old stack took
 * the last (F5, an accepted change). Partner: the other declaration gets no
 * edge. */
TEST(phpmw_possible_answer_in_the_walks_order) {
    static const PmwFile f[] = {
        {"src/Two.php", "<?php\nnamespace App\\Two;\nuse Vendor\\Pkg\\VBase;\n"
                        "interface I1 { public function m(): int; }\n"
                        "interface I2 { public function m(): int; }\n"
                        "class C extends VBase implements I1, I2 {}\n"
                        "class TCaller { public function go(C $c) { return $c->m(); } }\n"},
    };
    static const PmwExpect want[] = {
        {"TCaller.go", "I1.m", true, true},
        {"TCaller.go", "I2.m", false, false},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 2), 0);
    PASS();
}

/* A possible trait member is drawn when every class or trait on its `use`
 * path, from the holder down to the trait that uses the owner, is declared in
 * the file walked; the owner may be anywhere (review 3; what main's
 * flattening reached). fx/tt: all in one file (php: Za for all four; main
 * types all four). fx/ttx: the owner Inner in another file, the path P, Outer
 * here, behind the vendor trait V (php: Za for every V php accepts, review
 * 3; main typed): kept. fx/d1x: depth 1, the owner T2 imported (php: Za; main
 * typed): kept. Negative partner fx/pathx: the path leaves the file (P here
 * uses Outer of another file; php: Za, but main draws nothing there and the
 * walk's answer is only possible): no edge; without V the hit is proven and
 * drawn. fx/ttown and fx/d1y below: a non-leaf trait's own member. */
TEST(phpmw_possible_trait_member_same_file) {
    static const PmwFile f[] = {
        {"src/TT.php", "<?php\nnamespace App\\TT;\nuse Vendor\\Pkg\\V;\n"
                       "class Za { public function spin() { return 'Za'; } }\n"
                       "class Zb { public function spin() { return 'Zb'; } }\n"
                       "trait Inner { public function deep(): Za { return new Za; } }\n"
                       "trait Outer { use Inner; }\n"
                       "class P { use Outer; }\n"
                       "class PB { public function deep(): Zb { return new Zb; } }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class Direct { use Outer; }\n"
                       "class DirectV { use Outer, V; }\n"
                       "class TCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Plain $p) { return $p->deep()->spin(); }\n"
                       "    public function c(Direct $d) { return $d->deep()->spin(); }\n"
                       "    public function d(DirectV $d) { return $d->deep()->spin(); }\n"
                       "}\n"},
    };
    static const PmwExpect want[] = {
        {"TCall.a", "Za.spin", true, true},
        {"TCall.b", "Za.spin", true, true},
        {"TCall.c", "Za.spin", true, true},
        {"TCall.d", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(f, 1, want, 4), 0);
    static const PmwFile g[] = {
        {"src/In/Inner.php", "<?php\nnamespace App\\In;\n"
                             "class Za { public function spin() { return 'Za'; } }\n"
                             "class Zb { public function spin() { return 'Zb'; } }\n"
                             "trait Inner { public function deep(): Za { return new Za; } }\n"},
        {"src/TT.php", "<?php\nnamespace App\\TT;\nuse Vendor\\Pkg\\V;\nuse App\\In\\Inner;\n"
                       "trait Outer { use Inner; }\n"
                       "class P { use Outer; }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class TCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Plain $p) { return $p->deep()->spin(); }\n"
                       "}\n"},
    };
    static const PmwExpect gwant[] = {
        {"TCall.a", "Za.spin", true, true},
        {"TCall.b", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(g, 2, gwant, 2), 0);
    static const PmwFile d[] = {
        {"src/Lib/T2.php", "<?php\nnamespace App\\Lib;\n"
                           "class Za { public function spin() { return 'Za'; } }\n"
                           "class Zb { public function spin() { return 'Zb'; } }\n"
                           "trait T2 { public function deep(): Za { return new Za; } }\n"},
        {"src/D1.php", "<?php\nnamespace App\\D1;\nuse Vendor\\Pkg\\V;\nuse App\\Lib\\T2;\n"
                       "class P { use T2; }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class DCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Plain $p) { return $p->deep()->spin(); }\n"
                       "}\n"},
    };
    static const PmwExpect dwant[] = {
        {"DCall.a", "Za.spin", true, true},
        {"DCall.b", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(d, 2, dwant, 2), 0);
    static const PmwFile x[] = {
        {"src/In/Inner.php", "<?php\nnamespace App\\In;\n"
                             "class Za { public function spin() { return 'Za'; } }\n"
                             "class Zb { public function spin() { return 'Zb'; } }\n"
                             "trait Inner { public function deep(): Za { return new Za; } }\n"},
        {"src/Lib/Outer.php",
         "<?php\nnamespace App\\Lib;\nuse App\\In\\Inner;\ntrait Outer { use Inner; }\n"},
        {"src/TT.php", "<?php\nnamespace App\\TT;\nuse Vendor\\Pkg\\V;\nuse App\\Lib\\Outer;\n"
                       "use App\\In\\Inner;\n"
                       "class P { use Outer; }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class TCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Plain $p) { return $p->deep()->spin(); }\n"
                       "}\n"},
    };
    /* No edge at all for TCall.a: TT.php imports neither Za nor Zb, so no
     * name guess stands in (one would differ by route, main's). */
    static const PmwExpect xwant[] = {
        {"TCall.a", "Za.spin", false, false},
        {"TCall.b", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(x, 3, xwant, 2), 0);
    /* A member a trait declares itself while it also uses a trait (review 4,
     * F-A): the path below its owner is empty. fx/ttown, all in one file (php:
     * O1 Hidden->shallow Za, O2 Za, O3 Hidden->deep Za; main types a and c);
     * fx/d1y, depth 1, the owner T2 { use U; deep } in another file (php: Y1,
     * Y2 Za; main types both). */
    static const PmwFile o[] = {
        {"src/TO.php",
         "<?php\nnamespace App\\TO;\nuse Vendor\\Pkg\\V;\n"
         "class Za { public function spin() { return 'Za'; } }\n"
         "class Zb { public function spin() { return 'Zb'; } }\n"
         "trait Inner { public function deep(): Za { return new Za; } }\n"
         "trait Outer { use Inner; public function shallow(): Za { return new Za; } }\n"
         "class P { use Outer; }\n"
         "class Hidden extends P { use V; }\n"
         "class Plain extends P {}\n"
         "class TCall {\n"
         "    public function a(Hidden $h) { return $h->shallow()->spin(); }\n"
         "    public function b(Plain $p) { return $p->shallow()->spin(); }\n"
         "    public function c(Hidden $h) { return $h->deep()->spin(); }\n"
         "}\n"},
    };
    static const PmwExpect owant[] = {
        {"TCall.a", "Za.spin", true, true},
        {"TCall.b", "Za.spin", true, true},
        {"TCall.c", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(o, 1, owant, 3), 0);
    static const PmwFile y[] = {
        {"src/Lib/T2.php", "<?php\nnamespace App\\Lib;\n"
                           "class Za { public function spin() { return 'Za'; } }\n"
                           "class Zb { public function spin() { return 'Zb'; } }\n"
                           "trait U { public function other() { return 1; } }\n"
                           "trait T2 { use U; public function deep(): Za { return new Za; } }\n"},
        {"src/D1.php", "<?php\nnamespace App\\D1;\nuse Vendor\\Pkg\\V;\nuse App\\Lib\\T2;\n"
                       "class P { use T2; }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class DCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Plain $p) { return $p->deep()->spin(); }\n"
                       "}\n"},
    };
    static const PmwExpect ywant[] = {
        {"DCall.a", "Za.spin", true, true},
        {"DCall.b", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(y, 2, ywant, 2), 0);
    /* fx/u2 (review 5): T (here) uses U (U { use W; deep }) and the leaf U2
     * (deep2), both in another file; P uses T; behind V at depth 2 (php: U1-U4
     * Za; main types all four). */
    static const PmwFile u[] = {
        {"src/Lib/U.php", "<?php\nnamespace App\\Lib;\n"
                          "class Za { public function spin() { return 'Za'; } }\n"
                          "class Zb { public function spin() { return 'Zb'; } }\n"
                          "trait W { public function other() { return 1; } }\n"
                          "trait U { use W; public function deep(): Za { return new Za; } }\n"
                          "trait U2 { public function deep2(): Za { return new Za; } }\n"},
        {"src/TT.php", "<?php\nnamespace App\\U2;\nuse Vendor\\Pkg\\V;\nuse App\\Lib\\U;\n"
                       "use App\\Lib\\U2;\n"
                       "trait T { use U, U2; }\n"
                       "class P { use T; }\n"
                       "class Hidden extends P { use V; }\n"
                       "class Plain extends P {}\n"
                       "class UCall {\n"
                       "    public function a(Hidden $h) { return $h->deep()->spin(); }\n"
                       "    public function b(Hidden $h) { return $h->deep2()->spin(); }\n"
                       "    public function c(Plain $p) { return $p->deep()->spin(); }\n"
                       "    public function d(Plain $p) { return $p->deep2()->spin(); }\n"
                       "}\n"},
    };
    static const PmwExpect uwant[] = {
        {"UCall.a", "Za.spin", true, true},
        {"UCall.b", "Za.spin", true, true},
        {"UCall.c", "Za.spin", true, true},
        {"UCall.d", "Za.spin", true, true},
    };
    ASSERT_EQ(pmw_check(u, 2, uwant, 4), 0);
    PASS();
}

/* A trait's requirement in another file, beside a vendor trait (review 3,
 * fx/rqx; php: the vendor trait's m runs, Zv): the per-file pass, which
 * cannot see NeedM, must not leave the parent's P.m standing; the
 * requirement's copy answers, as on main (Zi.spin typed by its declared
 * type, no edge the engine draws to P.m). Positive partner fx/rqxp: without
 * the vendor trait the parent implements it (php: P::m runs, Zp): P.m and
 * Zp.spin typed (main misses both). */
TEST(phpmw_requirement_in_another_file) {
    static const char *need = "<?php\nnamespace App\\Lib;\n"
                              "class Zi { public function spin() { return 'Zi'; } }\n"
                              "class Zp extends Zi { public function spin() { return 'Zp'; } }\n"
                              "class Zv extends Zp { public function spin() { return 'Zv'; } }\n"
                              "trait NeedM { abstract public function m(): Zi; }\n";
    const PmwFile f[] = {
        {"src/Lib/NeedM.php", need},
        {"src/RQ.php", "<?php\nnamespace App\\RQ;\nuse Vendor\\Pkg\\VTrait;\nuse App\\Lib\\NeedM;\n"
                       "use App\\Lib\\Zp;\n"
                       "class P { public function m(): Zp { return new Zp; } }\n"
                       "class C extends P {\n    use NeedM, VTrait;\n"
                       "    public function run() { return $this->m()->spin(); }\n}\n"},
    };
    static const PmwExpect want[] = {
        {"C.run", "P.m", false, true}, /* no typed edge (main guesses it by name on one route) */
        {"C.run", "Zi.spin", true, true},
        {"C.run", "Zp.spin", false, true},
    };
    ASSERT_EQ(pmw_check(f, 2, want, 3), 0);
    const PmwFile g[] = {
        {"src/Lib/NeedM.php", need},
        {"src/RQ.php", "<?php\nnamespace App\\RQ;\nuse App\\Lib\\NeedM;\nuse App\\Lib\\Zp;\n"
                       "class P { public function m(): Zp { return new Zp; } }\n"
                       "class D extends P {\n    use NeedM;\n"
                       "    public function run() { return $this->m()->spin(); }\n}\n"},
    };
    static const PmwExpect gwant[] = {
        {"D.run", "P.m", true, true},
        {"D.run", "Zp.spin", true, true},
    };
    ASSERT_EQ(pmw_check(g, 2, gwant, 2), 0);
    PASS();
}

/* "Declared in this file" compares the module exactly (N1): the module of
 * src/Foo.php is a prefix of every class of src/Foo/Kid.php (fx/prefix). A
 * possible trait member of such a class is not drawn (php: the vendor trait's
 * m runs, Zv; main's guess stays); the control layout (fx/prefixctl) draws
 * nothing either. Positive partner: the same shape with the classes really
 * declared in the caller's file is drawn typed. */
TEST(phpmw_same_file_is_exact) {
    static const char *kid = "<?php\nnamespace App\\Foo;\nuse Vendor\\Pkg\\V;\n"
                             "class Zt { public function spin() { return 'Zt'; } }\n"
                             "class Zv extends Zt { public function spin() { return 'Zv'; } }\n"
                             "trait T { public function m(): Zt { return new Zt; } }\n"
                             "class P { use T; }\n"
                             "class Kid extends P { use V; }\n";
    const PmwFile f[] = {
        {"src/Foo/Kid.php", kid},
        {"src/Foo.php",
         "<?php\nnamespace App;\nuse App\\Foo\\Kid;\n"
         "class Caller { public function go(Kid $k) { return $k->m()->spin(); } }\n"},
    };
    static const PmwExpect want[] = {
        {"Caller.go", "Zt.spin", true, false}, /* main's guess, as on main */
    };
    ASSERT_EQ(pmw_check(f, 2, want, 1), 0);
    const PmwFile g[] = {
        {"src/Foo/Kid.php", kid},
        {"src/Bar.php",
         "<?php\nnamespace App;\nuse App\\Foo\\Kid;\n"
         "class Caller { public function go(Kid $k) { return $k->m()->spin(); } }\n"},
    };
    static const PmwExpect gwant[] = {
        {"Caller.go", "Zt.spin", false, false},
    };
    ASSERT_EQ(pmw_check(g, 2, gwant, 1), 0);
    static const PmwFile h[] = {
        {"src/Foo.php",
         "<?php\nnamespace App;\nuse Vendor\\Pkg\\V;\n"
         "class Zh { public function spin() { return 'Zh'; } }\n"
         "class Zw extends Zh { public function spin() { return 'Zw'; } }\n"
         "trait H { public function h(): Zh { return new Zh; } }\n"
         "class PH { use H; }\n"
         "class KidH extends PH { use V; }\n"
         "class Caller { public function here(KidH $k) { return $k->h()->spin(); } }\n"},
    };
    static const PmwExpect hwant[] = {
        {"Caller.here", "Zh.spin", true, true},
    };
    ASSERT_EQ(pmw_check(h, 1, hwant, 1), 0);
    PASS();
}

/* A 40,000-deep chain of traits using traits (F1, fx/deep40k): no worker
 * crashes, the file is indexed on both routes; the member at the bottom is the
 * using class's copy (main's guess draws the call, as on main) and its return
 * type types the chained call (php: Zd::spin runs; main guessed the pair). */
TEST(phpmw_trait_chain_40k) {
    enum { DEPTH = 40000 };
    size_t cap = (size_t)DEPTH * 40 + 1024;
    char *src = (char *)malloc(cap);
    ASSERT_NOT_NULL(src);
    size_t used = (size_t)snprintf(src, cap,
                                   "<?php\nnamespace App\\Deep;\n"
                                   "class Zd { public function spin() { return 'Zd'; } }\n"
                                   "trait T0 { public function m0(): Zd { return new Zd; } }\n");
    for (int i = 1; i <= DEPTH; i++) {
        used += (size_t)snprintf(src + used, cap - used, "trait T%d { use T%d; }\n", i, i - 1);
    }
    snprintf(src + used, cap - used,
             "class D { use T%d; public function run() { return $this->m0()->spin(); } }\n"
             "class DCaller { public function go(D $d) { return $d->m0()->spin(); } }\n",
             DEPTH);
    PmwFile f[] = {{"src/Deep.php", src}};
    static const PmwExpect want[] = {
        {"D.run", "T0.m0", true, false},
        {"D.run", "Zd.spin", true, true},
        {"DCaller.go", "Zd.spin", true, true},
    };
    int failures = pmw_check(f, 1, want, 3);
    free(src);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* The extractor records what the lookup needs and INHERITS does not take:
 * the kind of each base, the traits, and their rules. */
TEST(phpmw_extractor_records_kinds_traits_rules) {
    const char *src = "<?php\n"
                      "interface I1 {} interface I2 {}\n"
                      "trait T1 { public function m() {} } trait T2 { public function m() {} }\n"
                      "class P {}\n"
                      "class C extends P implements I1, \\Ns\\I2 {\n"
                      "    use T1, T2 { T1::m insteadof T2; T2::m as m2; m as protected; }\n"
                      "    use \\Ns\\T3;\n"
                      "}\n"
                      "interface J extends I1, I2 {}\n";
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), CBM_LANG_PHP, "test", "main.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    const CBMDefinition *c = NULL;
    const CBMDefinition *j = NULL;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "C") == 0) {
            c = d;
        } else if (d->name && strcmp(d->name, "J") == 0) {
            j = d;
        }
    }
    ASSERT_NOT_NULL(c);
    ASSERT_NOT_NULL(j);
    ASSERT_NOT_NULL(c->type_facts);
    ASSERT_NOT_NULL(j->type_facts);
    const CBMDefTypeFacts *cf = c->type_facts;
    const CBMDefTypeFacts *jf = j->type_facts;
    ASSERT_STR_EQ(cf->base_kinds, "eii");
    ASSERT_NOT_NULL(cf->mixins);
    /* Traits as the file names them (global namespace here). */
    ASSERT_STR_EQ(cf->mixins[0], "\\T1");
    ASSERT_STR_EQ(cf->mixins[1], "\\T2");
    ASSERT_STR_EQ(cf->mixins[2], "\\Ns\\T3");
    ASSERT_NULL(cf->mixins[3]);
    ASSERT_STR_EQ(cf->mixin_kinds, "ttt");
    ASSERT_STR_EQ(cf->member_rules, "x \\T1 m \\T2;a \\T2 m m2");
    /* Traits stay out of base_classes (INHERITS). */
    ASSERT_STR_EQ(c->base_classes[0], "P");
    ASSERT_NULL(c->base_classes[3]);
    ASSERT_STR_EQ(jf->base_kinds, "ee");
    ASSERT_NULL(jf->mixins);
    cbm_free_result(r);
    PASS();
}

/* A trait's name is qualified where the class is written, by PHP's own rule:
 * a `use` alias (plain, `as`, grouped), else the namespace; a bracketed
 * namespace has its own uses; a function `use` aliases no class. */
TEST(phpmw_extractor_qualifies_trait_names) {
    const char *src = "<?php\n"
                      "namespace App\\Models;\n"
                      "use Vendor\\Pkg\\HasThing;\n"
                      "use App\\Traits\\{Greets, Waves as W};\n"
                      "use function App\\Helpers\\Local;\n"
                      "class M { use HasThing, Greets, W, Local, Sub\\Deep; }\n"
                      "class R { use namespace\\Sub\\Rel, NameSpace\\Rel2; }\n"
                      "namespace Other;\n"
                      "class N { use HasThing; }\n";
    CBMFileResult *r =
        cbm_extract_file(src, (int)strlen(src), CBM_LANG_PHP, "test", "m.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    const CBMDefTypeFacts *mf = NULL;
    const CBMDefTypeFacts *nf = NULL;
    const CBMDefTypeFacts *rf = NULL;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "M") == 0) {
            mf = d->type_facts;
        } else if (d->name && strcmp(d->name, "N") == 0) {
            nf = d->type_facts;
        } else if (d->name && strcmp(d->name, "R") == 0) {
            rf = d->type_facts;
        }
    }
    ASSERT_NOT_NULL(mf);
    ASSERT_NOT_NULL(nf);
    ASSERT_NOT_NULL(rf);
    /* `namespace\X` is relative to the current namespace, the keyword in any
     * case (review 2, C8: php resolves it to App\Q\Sub\Rel there). */
    ASSERT_STR_EQ(rf->mixins[0], "\\App\\Models\\Sub\\Rel");
    ASSERT_STR_EQ(rf->mixins[1], "\\App\\Models\\Rel2");
    ASSERT_STR_EQ(mf->mixins[0], "\\Vendor\\Pkg\\HasThing");
    ASSERT_STR_EQ(mf->mixins[1], "\\App\\Traits\\Greets");
    ASSERT_STR_EQ(mf->mixins[2], "\\App\\Traits\\Waves");
    ASSERT_STR_EQ(mf->mixins[3], "\\App\\Models\\Local");
    ASSERT_STR_EQ(mf->mixins[4], "\\App\\Models\\Sub\\Deep");
    /* `namespace Other;` starts anew: no alias carries over. */
    ASSERT_STR_EQ(nf->mixins[0], "\\Other\\HasThing");
    /* Each type's own namespace (the file-level one keeps only the first). */
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "M") == 0) {
            ASSERT_STR_EQ(d->decl_namespace, "App\\Models");
        } else if (d->name && strcmp(d->name, "N") == 0) {
            ASSERT_STR_EQ(d->decl_namespace, "Other");
        }
    }
    cbm_free_result(r);
    const char *br = "<?php\n"
                     "namespace A { use X\\Y; }\n"
                     "namespace B { use Q\\Y; class K { use Y; } }\n";
    r = cbm_extract_file(br, (int)strlen(br), CBM_LANG_PHP, "test", "b.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    const CBMDefTypeFacts *kf = NULL;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->name && strcmp(d->name, "K") == 0) {
            kf = d->type_facts;
        }
    }
    ASSERT_NOT_NULL(kf);
    ASSERT_STR_EQ(kf->mixins[0], "\\Q\\Y");
    cbm_free_result(r);
    PASS();
}

/* `use namespace\Sub\Rel` names App\Q\Sub\Rel, never the App\Q\Rel beside it
 * (review 2, C8; php: ZRel), from a caller in the class's own file (the
 * engine's own reader) and in another file (the extractor's names). Both
 * callers' files import the traits and ZRel, as fx/qual's XCall does: what
 * a file sees of other files' types comes through its imports. */
TEST(phpmw_relative_trait_name_through_the_pipeline) {
    static const PmwFile f[] = {
        {"src/Q.php", "<?php\nnamespace App\\Q;\nuse App\\Q\\Rel as QRel;\n"
                      "use App\\Q\\Sub\\Rel as SubRel;\nuse App\\Z\\ZRel;\n"
                      "class C8 { use namespace\\Sub\\Rel; }\n"
                      "class QCall { public function h8(C8 $x) { return $x->m()->spin(); } }\n"},
        {"src/QDecoy.php",
         "<?php\nnamespace App\\Q;\n"
         "trait Rel { public function m(): \\App\\Z\\ZDecoy { return new \\App\\Z\\ZDecoy; } }\n"},
        {"src/QSub.php",
         "<?php\nnamespace App\\Q\\Sub;\n"
         "trait Rel { public function m(): \\App\\Z\\ZRel { return new \\App\\Z\\ZRel; } }\n"},
        {"src/Z.php", "<?php\nnamespace App\\Z;\n"
                      "class ZRel { public function spin() { return 'ZRel'; } }\n"
                      "class ZDecoy { public function spin() { return 'ZDecoy'; } }\n"},
        {"src/Http/XCall.php",
         "<?php\nnamespace App\\Http;\nuse App\\Q\\C8;\nuse App\\Q\\Rel as QRel;\n"
         "use App\\Q\\Sub\\Rel as SubRel;\nuse App\\Z\\ZRel;\n"
         "class XCall { public function g8(C8 $x) { return $x->m()->spin(); } }\n"},
    };
    static const PmwExpect want[] = {
        {"XCall.g8", "ZRel.spin", true, true},
        {"QCall.h8", "ZRel.spin", true, true},
        {"XCall.g8", "ZDecoy.spin", false, false},
        {"QCall.h8", "ZDecoy.spin", false, false},
    };
    ASSERT_EQ(pmw_check(f, 5, want, 4), 0);
    PASS();
}

/* ── The PHP imports the engine reads (slice B4) ────────────────────── */

/* The caller's file of the B4 fixtures: one class import, then the uses
 * `extra` adds (a global function or constant, which the project does not
 * declare), then a promoted property typed by the imported interface. */
#define PMW_USEFN_CALLER(extra)                                                         \
    "<?php\nnamespace App\\Controllers;\n\nuse App\\Dbal\\DatabaseInterface;\n\n" extra \
    "\nfinal class ViewController\n{\n"                                                 \
    "    public function __construct(private DatabaseInterface $dbi) {}\n"              \
    "    public function run(string $u): void { $this->dbi->getTable($u); }\n}\n"
#define PMW_USEFN_FILES(extra)                                                            \
    {"composer.json", "{\"autoload\": {\"psr-4\": {\"App\\\\\": \"src/\"}}}\n"},          \
    {"src/Dbal/DatabaseInterface.php",                                                    \
     "<?php\nnamespace App\\Dbal;\n"                                                      \
     "interface DatabaseInterface { public function getTable(string $t): void; }\n"},     \
    {"src/Other/Wrong.php",                                                               \
     "<?php\nnamespace App\\Other;\n"                                                     \
     "class Wrong { public function getTable(string $t): void {} }\n"},                   \
    {"src/Controllers/ViewController.php", PMW_USEFN_CALLER(extra)}

/* `use function` beside a class import (phpmyadmin's controllers): the
 * function is PHP's own, so the file's imports are complete and the class
 * import reaches the engine as the class. Before, one unresolved global
 * function use made the whole file's imports unknown, the class import reached
 * the engine as a File QN, and the typed call was lost. php: the property's
 * type is App\Dbal\DatabaseInterface and Db::getTable (its implementation)
 * runs. Partner: no edge to the other class's same-named method. */
TEST(phpmw_use_function_keeps_class_imports) {
    static const PmwFile f[] = {PMW_USEFN_FILES("use function is_array;\nuse function sprintf;\n")};
    static const PmwExpect want[] = {
        {"ViewController.run", "DatabaseInterface.getTable", true, true},
        {"ViewController.run", "Wrong.getTable", false, false},
    };
    ASSERT_EQ(pmw_check(f, 4, want, 2), 0);
    PASS();
}

/* The same with `use const` (PHP's own constant). php: as above. */
TEST(phpmw_use_const_keeps_class_imports) {
    static const PmwFile f[] = {PMW_USEFN_FILES("use const PHP_EOL;\n")};
    static const PmwExpect want[] = {
        {"ViewController.run", "DatabaseInterface.getTable", true, true},
        {"ViewController.run", "Wrong.getTable", false, false},
    };
    ASSERT_EQ(pmw_check(f, 4, want, 2), 0);
    PASS();
}

/* An aliased global function use (`use function sprintf as fmt;`, php: valid,
 * fmt is the builtin) gets no absence proof, as an aliased class use gets
 * none: the file's imports stay unknown and the typed call is not drawn by the
 * engine (the answer of the base). Binds the alias exception: with it removed
 * the file is complete and the engine draws the call. */
TEST(phpmw_aliased_global_use_function_keeps_the_file_unknown) {
    static const PmwFile f[] = {PMW_USEFN_FILES("use function sprintf as fmt;\n")};
    static const PmwExpect want[] = {
        {"ViewController.run", "DatabaseInterface.getTable", false, true},
        {"ViewController.run", "Wrong.getTable", false, false},
    };
    ASSERT_EQ(pmw_check(f, 4, want, 2), 0);
    PASS();
}

/* A `use const X;` beside the project's own `const X = 1;`: the graph has no
 * node for a PHP constant, so nothing it holds can be what the use names, and
 * the file's imports are complete (the typed call is drawn). Pins that the
 * proof claims no more than this; to revisit when constants get nodes. php:
 * X is the project's constant, Db::getTable runs. */
TEST(phpmw_use_const_beside_project_constant) {
    static const PmwFile f[] = {
        PMW_USEFN_FILES("use const X;\n"),
        {"src/consts.php", "<?php\nconst X = 1;\n"},
    };
    static const PmwExpect want[] = {
        {"ViewController.run", "DatabaseInterface.getTable", true, true},
        {"ViewController.run", "Wrong.getTable", false, false},
    };
    ASSERT_EQ(pmw_check(f, 5, want, 2), 0);
    PASS();
}

SUITE(php_member_walk) {
    RUN_TEST(phpmw_extractor_records_kinds_traits_rules);
    RUN_TEST(phpmw_extractor_qualifies_trait_names);
    RUN_TEST(phpmw_relative_trait_name_through_the_pipeline);
    RUN_TEST(phpmw_trait_resolved_where_written);
    RUN_TEST(phpmw_trait_in_a_later_namespace);
    RUN_TEST(phpmw_requirement_beside_vendor_trait);
    RUN_TEST(phpmw_possible_answer_in_the_walks_order);
    RUN_TEST(phpmw_possible_trait_member_same_file);
    RUN_TEST(phpmw_requirement_in_another_file);
    RUN_TEST(phpmw_same_file_is_exact);
    RUN_TEST(phpmw_trait_chain_40k);
    RUN_TEST(phpmw_parent_method_beats_interface_declaration);
    RUN_TEST(phpmw_chain_past_the_old_cap);
    RUN_TEST(phpmw_vendor_parent_keeps_todays_answer);
    RUN_TEST(phpmw_vendor_trait_keeps_todays_answer);
    RUN_TEST(phpmw_insteadof_picks_the_named_trait);
    RUN_TEST(phpmw_trait_alias_answers);
    RUN_TEST(phpmw_cross_file_trait_member);
    RUN_TEST(phpmw_trait_abstract_method_is_a_requirement);
    RUN_TEST(phpmw_use_function_keeps_class_imports);
    RUN_TEST(phpmw_use_const_keeps_class_imports);
    RUN_TEST(phpmw_aliased_global_use_function_keeps_the_file_unknown);
    RUN_TEST(phpmw_use_const_beside_project_constant);
}
