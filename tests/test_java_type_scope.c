/*
 * test_java_type_scope.c — Java on the declared-type step (slice B1).
 *
 * One project is indexed with cbm_pipeline_run and the CALLS edges are read
 * back from the store. Every expected target below is what javac and java
 * decide for the same sources (the program is compiled and run in the status
 * file of the slice): each call prints the method it runs. Every callee has a
 * same-named method in decoy.Decoys, so a guess by name shows as an edge
 * there, and each "no edge" case has a positive partner in the fixture.
 *
 * Shapes: a single-type import of a project class; one of a library class
 * while the project has a class of that simple name elsewhere; two on-demand
 * imports of which one has the name, and two that both have it (javac
 * rejects the use); a java.lang name shadowed by a project class of the own
 * package (through another file's return type); a nested type written
 * unqualified inside its outer class and qualified outside; a static import
 * of a method and of a member type; the default package; a generic return
 * type whose argument the declaring file imports; a field inherited from
 * another file's superclass; a JDK collection receiver; a package over two
 * source trees (the class in one of them; the same class in both); member
 * types inherited from a project supertype of another file (also through an
 * enclosing class and another file's return type); type parameters named
 * like classes of the own package; a decorator whose interface method is a
 * library super-interface's. A "no pick" case gives each twin a method that
 * returns its own class, so a pick would show on the following call.
 *
 * The fixture runs on the sequential route and, padded past
 * MIN_FILES_FOR_PARALLEL, on the parallel one; an import edit through the
 * incremental closure repair gives a full index's engine edges, and a body
 * edit of a file with a library-receiver call gives a full index's edges.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "foundation/arena.h"
#include "lsp/java_lsp.h"
#include "lsp/lsp_work.h"
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/type_scope.h"
#include "sqlite3.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Fixture (the javac-checked program) ─────────────────────────────── */

typedef struct {
    const char *path;
    const char *text;
} jts_file_t;

static const jts_file_t JTS_FILES[] = {
    {"src/main/java/decoy/Decoys.java",
     "package decoy;\n\n"
     "public class Decoys {\n"
     "    public String getId() { return \"decoy.Decoys.getId\"; }\n"
     "    public String spin() { return \"decoy.Decoys.spin\"; }\n"
     "    public String begin() { return \"decoy.Decoys.begin\"; }\n"
     "    public String depth() { return \"decoy.Decoys.depth\"; }\n"
     "    public String helper() { return \"decoy.Decoys.helper\"; }\n"
     "    public String weigh() { return \"decoy.Decoys.weigh\"; }\n"
     "    public String boot() { return \"decoy.Decoys.boot\"; }\n"
     "    public String turn() { return \"decoy.Decoys.turn\"; }\n"
     "    public String run() { return \"decoy.Decoys.run\"; }\n"
     "    public String ping() { return \"decoy.Decoys.ping\"; }\n"
     "    public String probe() { return \"decoy.Decoys.probe\"; }\n"
     "    public boolean isEmpty() { return true; }\n"
     "    public String put(Object k, Object v) { return \"decoy.Decoys.put\"; }\n"
     "    public Object getKey() { return null; }\n"
     "    public String v2() { return \"decoy.Decoys.v2\"; }\n"
     "    public String area2() { return \"decoy.Decoys.area2\"; }\n"
     "    public String get() { return \"decoy.Decoys.get\"; }\n"
     "}\n"},
    {"src/main/java/other/Service.java",
     "package other;\n\n"
     "public class Service {\n"
     "    public String run() { return \"other.Service.run\"; }\n"
     "}\n"},
    {"src/main/java/lib/Foo.java",
     "package lib;\n\n"
     "public class Foo {\n"
     "    public String getId() { return \"lib.Foo.getId\"; }\n"
     "}\n"},
    {"src/main/java/lib/Outer.java",
     "package lib;\n\n"
     "public class Outer {\n"
     "    public static class Inner {\n"
     "        public String depth() { return \"lib.Outer.Inner.depth\"; }\n"
     "    }\n\n"
     "    public Inner make() { return new Inner(); }\n"
     "}\n"},
    {"src/main/java/lib/Util.java",
     "package lib;\n\n"
     "public class Util {\n"
     "    public static String helper() { return \"lib.Util.helper\"; }\n"
     "}\n"},
    {"src/main/java/lib2/Item.java",
     "package lib2;\n\n"
     "public class Item {\n"
     "    public String weigh() { return \"lib2.Item.weigh\"; }\n"
     "}\n"},
    {"src/main/java/lib/Repo.java",
     "package lib;\n\n"
     "import lib2.Item;\n\n"
     "public class Repo {\n"
     "    public java.util.List<Item> all() {\n"
     "        java.util.List<Item> out = new java.util.ArrayList<>();\n"
     "        out.add(new Item());\n"
     "        return out;\n"
     "    }\n"
     "}\n"},
    {"src/main/java/lib/Engine.java",
     "package lib;\n\n"
     "public class Engine {\n"
     "    public String boot() { return \"lib.Engine.boot\"; }\n"
     "}\n"},
    {"src/main/java/lib/Base.java",
     "package lib;\n\n"
     "public class Base {\n"
     "    public Engine engine = new Engine();\n"
     "}\n"},
    {"src/main/java/lib/Car.java",
     "package lib;\n\n"
     "public class Car extends Base {\n"
     "}\n"},
    {"src/main/java/p1/Widget.java",
     "package p1;\n\n"
     "public class Widget {\n"
     "    public String spin() { return \"p1.Widget.spin\"; }\n"
     "}\n"},
    {"src/main/java/p2/Gadget.java",
     "package p2;\n\n"
     "public class Gadget {\n"
     "    public String turn() { return \"p2.Gadget.turn\"; }\n"
     "}\n"},
    {"src/main/java/p1/Twin.java",
     "package p1;\n\n"
     "public class Twin {\n"
     "    public String turn() { return \"p1.Twin.turn\"; }\n\n"
     "    public Tag1 tag() { return new Tag1(); }\n"
     "}\n"},
    {"src/main/java/p2/Twin.java",
     "package p2;\n\n"
     "public class Twin {\n"
     "    public String turn() { return \"p2.Twin.turn\"; }\n\n"
     "    public Tag2 tag() { return new Tag2(); }\n"
     "}\n"},
    {"src/main/java/app/Process.java",
     "package app;\n\n"
     "public class Process {\n"
     "    public String begin() { return \"app.Process.begin\"; }\n"
     "}\n"},
    {"src/main/java/app/Factory.java",
     "package app;\n\n"
     "public class Factory {\n"
     "    public Process make() { return new Process(); }\n"
     "}\n"},
    /* One package over two source trees: Probe is in the test tree only;
     * Dup is in both (no identity). */
    {"src/main/java/split/Anchor.java",
     "package split;\n\n"
     "public class Anchor {\n"
     "}\n"},
    {"src/main/java/split/Dup.java",
     "package split;\n\n"
     "public class Dup {\n"
     "    public String probe() { return \"split.Dup.probe (main)\"; }\n\n"
     "    public DupTagM tagDup() { return new DupTagM(); }\n"
     "}\n"},
    {"src/test/java/split/Probe.java",
     "package split;\n\n"
     "public class Probe {\n"
     "    public String probe() { return \"split.Probe.probe\"; }\n"
     "}\n"},
    {"src/test/java/split/Dup.java",
     "package split;\n\n"
     "public class Dup {\n"
     "    public String probe() { return \"split.Dup.probe (test)\"; }\n\n"
     "    public DupTagT tagDup() { return new DupTagT(); }\n"
     "}\n"},
    {"src/test/java/user/SplitUse.java",
     "package user;\n\n"
     "import split.Dup;\n"
     "import split.Probe;\n\n"
     "public class SplitUse {\n"
     "    String splitTree(Probe p) { return p.probe(); }\n\n"
     "    String splitDup(Dup d) { return d.probe(); }\n\n"
     "    String splitDupTag(Dup d) { return d.tagDup().sealDup(); }\n"
     "}\n"},
    {"src/main/java/user/Main.java",
     "package user;\n\n"
     "import app.Factory;\n"
     "import ext.Service;\n"
     "import java.util.Set;\n"
     "import lib.Car;\n"
     "import lib.Foo;\n"
     "import lib.Outer;\n"
     "import lib.Repo;\n"
     "import p1.*;\n"
     "import p2.*;\n\n"
     "import static lib.Util.helper;\n\n"
     "public class Main {\n"
     "    String singleImport(Foo f) { return f.getId(); }\n\n"
     "    String externalImport(Service s) { return s.run(); }\n\n"
     "    String onDemandOne(Widget w) { return w.spin(); }\n\n"
     "    String onDemandOther(Gadget g) { return g.turn(); }\n\n"
     "    String javaLangShadow(Factory f) { return f.make().begin(); }\n\n"
     "    String nestedFromOuter(Outer o) { return o.make().depth(); }\n\n"
     "    String nestedQualified(Outer.Inner i) { return i.depth(); }\n\n"
     "    String staticImport() { return helper(); }\n\n"
     "    String genericReturn(Repo r) { return r.all().get(0).weigh(); }\n\n"
     "    String inheritedField(Car c) { return c.engine.boot(); }\n\n"
     "    boolean jdkReceiver(Set<String> names) { return names.isEmpty(); }\n"
     "}\n"},
    /* A library receiver (a Guava Multimap) and a JDK one (java.util.Map) in one
     * method: both calls named put, neither a project method. */
    {"src/main/java/user/Mixed.java",
     "package user;\n\n"
     "import com.google.common.collect.HashMultimap;\n"
     "import com.google.common.collect.Maps;\n"
     "import com.google.common.collect.Multimap;\n"
     "import java.util.List;\n"
     "import java.util.Map;\n\n"
     "public class Mixed {\n"
     "    int mixedReceivers(List<String> keys) {\n"
     "        Multimap<Long, String> grouped = HashMultimap.create();\n"
     "        for (String k : keys) {\n"
     "            grouped.put(1L, k);\n"
     "        }\n"
     "        Map<String, String> plain = Maps.newHashMap();\n"
     "        for (String k : keys) {\n"
     "            plain.put(k, k);\n"
     "        }\n"
     "        return grouped.size() + plain.size();\n"
     "    }\n"
     "}\n"},
    {"src/main/java/user/StaticType.java",
     "package user;\n\n"
     "import static lib.Outer.Inner;\n\n"
     "public class StaticType {\n"
     "    String viaStatic(Inner i) { return i.depth(); }\n"
     "}\n"},
    {"src/main/java/user/Ambig.java",
     "package user;\n\n"
     "import p1.*;\n"
     "import p2.*;\n\n"
     "public class Ambig {\n"
     "    String twin(Twin t) { return t.turn(); }\n\n"
     "    String twinTag(Twin t) { return t.tag().seal(); }\n"
     "}\n"},
    /* A class with a declared constructor and an annotation type of the same
     * name in another package (apollo's audit entity/annotation pair); Make
     * imports the class. */
    {"src/main/java/audit/entity/Entry.java",
     "package audit.entity;\n\n"
     "public class Entry {\n"
     "    private final String who;\n\n"
     "    public Entry() { this.who = \"audit.entity.Entry.<init>\"; }\n\n"
     "    public String who() { return who; }\n"
     "}\n"},
    {"src/main/java/audit/annotation/Entry.java",
     "package audit.annotation;\n\n"
     "public @interface Entry {\n"
     "    String value() default \"\";\n"
     "}\n"},
    {"src/main/java/user/Make.java",
     "package user;\n\n"
     "import audit.entity.Entry;\n\n"
     "public class Make {\n"
     "    Entry constructorImport() { return new Entry(); }\n"
     "}\n"},
    /* A top-level class and a nested class of one simple name in one package
     * (one QN in the namespace form, so no answer for it), at the repository
     * root: the per-file run's row for `new Mark()` names the import's class
     * and joins. */
    {"coll/Mark.java",
     "package coll;\n\n"
     "public class Mark {\n"
     "    public String tag() { return \"coll.Mark.tag\"; }\n"
     "}\n"},
    {"coll/Holder3.java",
     "package coll;\n\n"
     "public class Holder3 {\n"
     "    public static class Mark {\n"
     "        public String tag() { return \"coll.Holder3.Mark.tag\"; }\n"
     "    }\n"
     "}\n"},
    {"rootuser/UsesMark.java",
     "package rootuser;\n\n"
     "import coll.Mark;\n\n"
     "public class UsesMark {\n"
     "    Mark makeMark() { return new Mark(); }\n"
     "}\n"},
    /* Member types inherited from a supertype: Entry in a HashMap
     * subclass (java.util.Map.Entry, a library type), and Part2 from a class of
     * the same file. */
    {"src/main/java/lib/LruCache.java",
     "package lib;\n\n"
     "import java.util.HashMap;\n\n"
     "public class LruCache<K, V> extends HashMap<K, V> {\n"
     "    boolean firstKeyNull(Entry<K, V> eldest) {\n"
     "        return eldest.getKey() == null;\n"
     "    }\n"
     "}\n"},
    {"src/main/java/lib/Nest2.java",
     "package lib;\n\n"
     "public class Nest2 {\n"
     "    public static class Part2 {\n"
     "        public String v2() { return \"lib.Nest2.Part2.v2\"; }\n"
     "    }\n"
     "}\n\n"
     "class Nest2Sub extends Nest2 {\n"
     "    String usePart(Part2 p) { return p.v2(); }\n"
     "}\n"},
    /* A decorator: Framed implements Shape2 and delegates to another Shape2;
     * Square2 implements it elsewhere. */
    {"src/main/java/iface2/Shape2.java",
     "package iface2;\n\n"
     "public interface Shape2 {\n"
     "    String area2();\n"
     "}\n"},
    {"src/main/java/iface2/Square2.java",
     "package iface2;\n\n"
     "public class Square2 implements Shape2 {\n"
     "    public String area2() { return \"iface2.Square2.area2\"; }\n"
     "}\n"},
    {"src/main/java/user/Framed.java",
     "package user;\n\n"
     "import iface2.Shape2;\n\n"
     "public class Framed implements Shape2 {\n"
     "    private final Shape2 inner;\n\n"
     "    public Framed(Shape2 inner) { this.inner = inner; }\n\n"
     "    public String area2() { return inner.area2(); }\n"
     "}\n"},
    /* 24: member types inherited from a project supertype of another file
     * and package: Base24 declares Key24 and Val25; mtwin.Key24 (imported)
     * and msub.Val25 (own package) are same-named twins. Each class's pick
     * returns another class, so a wrong receiver type shows on hit24/hit25. */
    {"src/main/java/hit/Right24.java",
     "package hit;\n\n"
     "public class Right24 {\n"
     "    public String hit24() { return \"hit.Right24.hit24\"; }\n"
     "}\n"},
    {"src/main/java/hit/Wrong24.java",
     "package hit;\n\n"
     "public class Wrong24 {\n"
     "    public String hit24() { return \"hit.Wrong24.hit24\"; }\n"
     "}\n"},
    {"src/main/java/hit/Right25.java",
     "package hit;\n\n"
     "public class Right25 {\n"
     "    public String hit25() { return \"hit.Right25.hit25\"; }\n"
     "}\n"},
    {"src/main/java/hit/Wrong25.java",
     "package hit;\n\n"
     "public class Wrong25 {\n"
     "    public String hit25() { return \"hit.Wrong25.hit25\"; }\n"
     "}\n"},
    {"src/main/java/mbase/Base24.java",
     "package mbase;\n\n"
     "import hit.*;\n\n"
     "public class Base24 {\n"
     "    public static class Key24 {\n"
     "        public Right24 pick24() { return new Right24(); }\n"
     "    }\n\n"
     "    public static class Val25 {\n"
     "        public Right25 pick25() { return new Right25(); }\n"
     "    }\n"
     "}\n"},
    {"src/main/java/mtwin/Key24.java",
     "package mtwin;\n\n"
     "import hit.*;\n\n"
     "public class Key24 {\n"
     "    public Wrong24 pick24() { return new Wrong24(); }\n"
     "}\n"},
    {"src/main/java/msub/Val25.java",
     "package msub;\n\n"
     "import hit.*;\n\n"
     "public class Val25 {\n"
     "    public Wrong25 pick25() { return new Wrong25(); }\n"
     "}\n"},
    {"src/main/java/msub/Sub24.java",
     "package msub;\n\n"
     "import mbase.Base24;\n"
     "import mtwin.Key24;\n\n"
     "public class Sub24 extends Base24 {\n"
     "    String inhImport(Key24 k) { return k.pick24().hit24(); }\n\n"
     "    String inhPackage(Val25 v) { return v.pick25().hit25(); }\n\n"
     "    public Val25 mk25() { return new Val25(); }\n\n"
     "    static class Inner24 {\n"
     "        String inhEnclosing(Key24 k) { return k.pick24().hit24(); }\n"
     "    }\n"
     "}\n"},
    {"src/main/java/muse/Ret24.java",
     "package muse;\n\n"
     "public class Ret24 {\n"
     "    String inhReturn() { return new msub.Sub24().mk25().pick25().hit25(); }\n"
     "}\n"},
    /* 26: type parameters named like classes of the declaring package. */
    {"src/main/java/tp/Item26.java",
     "package tp;\n\n"
     "public class Item26 {\n"
     "    public String weigh26() { return \"tp.Item26.weigh26\"; }\n"
     "}\n"},
    {"src/main/java/tp/Other26.java",
     "package tp;\n\n"
     "public class Other26 {\n"
     "    public String weigh26() { return \"tp.Other26.weigh26\"; }\n"
     "}\n"},
    {"src/main/java/tp/Box26.java",
     "package tp;\n\n"
     "public class Box26<Item26> {\n"
     "    private final Item26 v;\n\n"
     "    public Box26(Item26 v) { this.v = v; }\n\n"
     "    public Item26 get26() { return v; }\n\n"
     "    public Other26 other26() { return new Other26(); }\n\n"
     "    public static <Item26> Item26 pick26(Item26 x) { return x; }\n"
     "}\n"},
    {"src/main/java/tpu/Use26.java",
     "package tpu;\n\n"
     "import tp.Box26;\n"
     "import tp.Other26;\n\n"
     "public class Use26 {\n"
     "    String tpClass(Box26<Other26> b) { return b.get26().weigh26(); }\n\n"
     "    String tpMethod() { return Box26.pick26(new Other26()).weigh26(); }\n\n"
     "    String tpPartner(Box26<Other26> b) { return b.other26().weigh26(); }\n"
     "}\n"},
    /* 23b: a decorator over an interface whose method is a library
     * super-interface's (Supplier.get); decoy.Decoys has get() and area2(). */
    {"src/main/java/iface3/Shape3.java",
     "package iface3;\n\n"
     "public interface Shape3 extends java.util.function.Supplier<String> {\n"
     "}\n"},
    {"src/main/java/user/Framed3.java",
     "package user;\n\n"
     "import iface3.Shape3;\n\n"
     "public class Framed3 implements Shape3 {\n"
     "    private final Shape3 inner;\n\n"
     "    public Framed3(Shape3 inner) { this.inner = inner; }\n\n"
     "    public String get() { return inner == null ? \"user.Framed3.get\" : inner.get(); }\n"
     "}\n"},
    /* Reveal classes of the two "no pick" cases: each twin's tag method
     * returns its own class. */
    {"src/main/java/split/DupTagM.java",
     "package split;\n\n"
     "public class DupTagM {\n"
     "    public String sealDup() { return \"split.DupTagM.sealDup\"; }\n"
     "}\n"},
    {"src/test/java/split/DupTagT.java",
     "package split;\n\n"
     "public class DupTagT {\n"
     "    public String sealDup() { return \"split.DupTagT.sealDup\"; }\n"
     "}\n"},
    {"src/main/java/p1/Tag1.java",
     "package p1;\n\n"
     "public class Tag1 {\n"
     "    public String seal() { return \"p1.Tag1.seal\"; }\n"
     "}\n"},
    {"src/main/java/p2/Tag2.java",
     "package p2;\n\n"
     "public class Tag2 {\n"
     "    public String seal() { return \"p2.Tag2.seal\"; }\n"
     "}\n"},
    /* 27: a member type inherited through an interface whose extends list
     * names the supertype qualified (the reviewer's fx5; javac
     * invokevirtual i1/I1$Z1.pickC, then ec/RightC.hitC). */
    {"src/main/java/i1/I1.java",
     "package i1;\n\n"
     "import ec.*;\n\n"
     "public interface I1 {\n"
     "    class Z1 {\n"
     "        public RightC pickC() { return new RightC(); }\n"
     "    }\n"
     "}\n"},
    {"src/main/java/i2/I2.java",
     "package i2;\n\n"
     "public interface I2 extends i1.I1 {\n"
     "}\n"},
    {"src/main/java/i3/Z1.java",
     "package i3;\n\n"
     "import ec.*;\n\n"
     "public class Z1 {\n"
     "    public WrongC pickC() { return new WrongC(); }\n"
     "}\n"},
    {"src/main/java/i4/Impl.java",
     "package i4;\n\n"
     "import i2.I2;\n"
     "import i3.Z1;\n\n"
     "public class Impl implements I2 {\n"
     "    String ifaceDeep(Z1 z) { return z.pickC().hitC(); }\n"
     "    public static void main(String[] a) { System.out.println(new Impl().ifaceDeep(new Z1())); }\n"
     "}\n"},
    {"src/main/java/i7/I7.java",
     "package i7;\n\n"
     "public interface I7 extends i1.I1, java.lang.Comparable<String> {\n"
     "}\n"},
    {"src/main/java/i7/Impl7.java",
     "package i7;\n\n"
     "import i3.Z1;\n\n"
     "public abstract class Impl7 implements I7 {\n"
     "    String ifaceDotted2(Z1 z) { return z.pickC().hitC(); }\n"
     "}\n"},
    {"src/main/java/ec/RightC.java",
     "package ec;\n\n"
     "public class RightC {\n"
     "    public String hitC() { return \"RightC.hitC\"; }\n"
     "}\n"},
    {"src/main/java/ec/WrongC.java",
     "package ec;\n\n"
     "public class WrongC {\n"
     "    public String hitC() { return \"WrongC.hitC\"; }\n"
     "}\n"},
    {"src/main/java/Def.java",
     "public class Def {\n"
     "    public String ping() { return \"Def.ping\"; }\n"
     "}\n"},
    {"src/main/java/DefUser.java",
     "public class DefUser {\n"
     "    String usePing(Def d) { return d.ping(); }\n"
     "}\n"},
};

static void jts_write_fixture(const char *tmp, int filler) {
    for (size_t i = 0; i < sizeof(JTS_FILES) / sizeof(JTS_FILES[0]); i++) {
        th_write_file(TH_PATH(tmp, JTS_FILES[i].path), JTS_FILES[i].text);
    }
    for (int i = 0; i < filler; i++) {
        char rel[128];
        char body[256];
        snprintf(rel, sizeof(rel), "src/main/java/fill/Fill%d.java", i);
        snprintf(body, sizeof(body),
                 "package fill;\n\npublic class Fill%d {\n"
                 "    public int value%d() { return %d; }\n}\n",
                 i, i, i);
        th_write_file(TH_PATH(tmp, rel), body);
    }
}

/* ── Store reads ─────────────────────────────────────────────────────── */

typedef struct {
    char tmp[256];
    char db_path[512];
    char project[512];
    sqlite3 *db;
} jts_index_t;

static int jts_index(jts_index_t *ix, int filler) {
    memset(ix, 0, sizeof(*ix));
    snprintf(ix->tmp, sizeof(ix->tmp), "/tmp/cbm_jts_XXXXXX");
    if (!cbm_mkdtemp(ix->tmp)) {
        return -1;
    }
    jts_write_fixture(ix->tmp, filler);
    snprintf(ix->db_path, sizeof(ix->db_path), "%s/jts.db", ix->tmp);
    cbm_pipeline_t *p = cbm_pipeline_new(ix->tmp, ix->db_path, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }
    int rc = cbm_pipeline_run(p);
    snprintf(ix->project, sizeof(ix->project), "%s", cbm_pipeline_project_name(p));
    cbm_pipeline_free(p);
    if (rc != 0 || sqlite3_open_v2(ix->db_path, &ix->db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        return -1;
    }
    return 0;
}

static void jts_close(jts_index_t *ix) {
    if (ix->db) {
        sqlite3_close(ix->db);
    }
    th_cleanup(ix->tmp);
}

/* The CALLS edges from method `caller` whose callee text ends in `leaf`, as
 * "<qn without project and source root>|<strategy>" lines; their count. */
static int jts_targets(jts_index_t *ix, const char *caller, const char *leaf, char *out,
                       size_t out_len) {
    out[0] = '\0';
    const char *sql = "SELECT t.qualified_name, json_extract(e.properties,'$.strategy'), "
                      "json_extract(e.properties,'$.callee') FROM edges e "
                      "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = 'CALLS' AND s.name = ?1";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(ix->db, sql, -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(st, 1, caller, -1, SQLITE_STATIC);
    int n = 0;
    size_t llen = strlen(leaf);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(st, 0);
        const char *strategy = (const char *)sqlite3_column_text(st, 1);
        const char *callee = (const char *)sqlite3_column_text(st, 2);
        if (!qn || !callee) {
            continue;
        }
        size_t clen = strlen(callee);
        bool leaf_match = (clen == llen && strcmp(callee, leaf) == 0) ||
                          (clen > llen && callee[clen - llen - 1] == '.' &&
                           strcmp(callee + clen - llen, leaf) == 0);
        if (!leaf_match) {
            continue;
        }
        const char *root = strstr(qn, ".java.");
        const char *shown = root ? root + strlen(".java.") : qn;
        size_t used = strlen(out);
        snprintf(out + used, out_len - used, "%s|%s\n", shown, strategy ? strategy : "");
        n++;
    }
    sqlite3_finalize(st);
    return n;
}

/* Exactly one CALLS edge for (caller, leaf): to `want` (QN below the source
 * root), drawn by the type engine (an lsp_* strategy). */
static int jts_expect_engine(jts_index_t *ix, const char *caller, const char *leaf,
                             const char *want) {
    char got[4096];
    int n = jts_targets(ix, caller, leaf, got, sizeof(got));
    char line[1024];
    snprintf(line, sizeof(line), "%s|lsp_", want);
    if (n == 1 && strncmp(got, line, strlen(line)) == 0) {
        return 1;
    }
    printf("\n  %s -> %s: want one engine edge to %s, have %d:\n%s", caller, leaf, want, n, got);
    return 0;
}

/* No CALLS edge for (caller, leaf) reaches a node whose QN contains `bad`. */
static int jts_expect_none_to(jts_index_t *ix, const char *caller, const char *leaf,
                              const char *bad) {
    char got[4096];
    (void)jts_targets(ix, caller, leaf, got, sizeof(got));
    if (!strstr(got, bad)) {
        return 1;
    }
    printf("\n  %s -> %s: an edge reaches %s:\n%s", caller, leaf, bad, got);
    return 0;
}

/* No engine edge for (caller, leaf): the engine gives no answer there. */
static int jts_expect_no_engine(jts_index_t *ix, const char *caller, const char *leaf) {
    char got[4096];
    (void)jts_targets(ix, caller, leaf, got, sizeof(got));
    if (!strstr(got, "|lsp_")) {
        return 1;
    }
    printf("\n  %s -> %s: want no engine edge:\n%s", caller, leaf, got);
    return 0;
}

/* No engine edge for (caller, leaf) reaches a node whose QN contains `bad`
 * (a name-chain edge may: it is no engine answer). */
static int jts_expect_no_engine_to(jts_index_t *ix, const char *caller, const char *leaf,
                                   const char *bad) {
    char got[4096];
    (void)jts_targets(ix, caller, leaf, got, sizeof(got));
    for (const char *line = got; *line;) {
        const char *nl = strchr(line, '\n');
        size_t len = nl ? (size_t)(nl - line) : strlen(line);
        char one[1024];
        snprintf(one, sizeof(one), "%.*s", (int)len, line);
        if (strstr(one, bad) && strstr(one, "|lsp_")) {
            printf("\n  %s -> %s: an engine edge reaches %s:\n%s", caller, leaf, bad, got);
            return 0;
        }
        line = nl ? nl + 1 : line + len;
    }
    return 1;
}

/* ── Checks ──────────────────────────────────────────────────────────── */

typedef int (*jts_check_fn)(jts_index_t *ix);

static int jts_run(int filler, jts_check_fn check) {
    jts_index_t ix;
    if (jts_index(&ix, filler) != 0) {
        printf("\n  indexing the fixture failed\n");
        jts_close(&ix);
        return 0;
    }
    int ok = check(&ix);
    jts_close(&ix);
    return ok;
}

/* 1: `import lib.Foo;` binds the project class. */
static int jts_check_single_import(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "singleImport", "getId", "lib.Foo.getId");
    ok &= jts_expect_none_to(ix, "singleImport", "getId", "Decoys");
    return ok;
}

/* 2: `import ext.Service;` (a library) binds no project class, though the
 * project has other.Service; partner: shape 1. */
static int jts_check_external_import(jts_index_t *ix) {
    int ok = jts_expect_none_to(ix, "externalImport", "run", "other.Service");
    ok &= jts_expect_none_to(ix, "externalImport", "run", "Decoys");
    return ok;
}

/* 3 and 4: one answer across the open packages, or none. */
static int jts_check_on_demand(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "onDemandOne", "spin", "p1.Widget.spin");
    ok &= jts_expect_engine(ix, "onDemandOther", "turn", "p2.Gadget.turn");
    ok &= jts_expect_no_engine(ix, "twin", "turn");
    /* The twins' tag() return different classes: a pick types seal(). */
    ok &= jts_expect_no_engine(ix, "twinTag", "seal");
    return ok;
}

/* 5: Factory.make()'s Process is app.Process (own package), not java.lang's. */
static int jts_check_java_lang_shadow(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "javaLangShadow", "begin", "app.Process.begin");
    ok &= jts_expect_engine(ix, "javaLangShadow", "make", "app.Factory.make");
    return ok;
}

/* 6: Inner written unqualified inside Outer, and Outer.Inner outside. */
static int jts_check_nested(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "nestedFromOuter", "depth", "lib.Outer.Inner.depth");
    ok &= jts_expect_engine(ix, "nestedQualified", "depth", "lib.Outer.Inner.depth");
    return ok;
}

/* 7: a static import of a method, and of a member type. */
static int jts_check_static(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "staticImport", "helper", "lib.Util.helper");
    ok &= jts_expect_engine(ix, "viaStatic", "depth", "lib.Outer.Inner.depth");
    return ok;
}

/* 8: the default package (same directory). */
static int jts_check_default_package(jts_index_t *ix) {
    return jts_expect_engine(ix, "usePing", "ping", "Def.ping");
}

/* 9: List<Item> written with java.util qualified; Item from the declaring
 * file's import. */
static int jts_check_generic_return(jts_index_t *ix) {
    return jts_expect_engine(ix, "genericReturn", "weigh", "lib2.Item.weigh");
}

/* 10: the field engine Car inherits from Base (another file). */
static int jts_check_inherited_field(jts_index_t *ix) {
    return jts_expect_engine(ix, "inheritedField", "boot", "lib.Engine.boot");
}

/* A JDK collection receiver never binds a project method by name, also
 * beside a library receiver of the same method name in one method. */
static int jts_check_jdk_receiver(jts_index_t *ix) {
    int ok = jts_expect_none_to(ix, "jdkReceiver", "isEmpty", "Decoys");
    ok &= jts_expect_none_to(ix, "mixedReceivers", "put", "Decoys");
    return ok;
}

/* 11: `new Entry()` under `import audit.entity.Entry;` runs the declared
 * constructor of audit.entity.Entry, never the annotation type of the same
 * name in audit.annotation. */
static int jts_check_constructor_import(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "constructorImport", "Entry", "audit.entity.Entry.Entry");
    ok &= jts_expect_none_to(ix, "constructorImport", "Entry", "audit.annotation");
    return ok;
}

/* 20: `new Mark()` under `import coll.Mark;` where coll.Holder3.Mark shares the
 * namespace-form QN: the cross run has no answer for the class, and the
 * per-file run's row (the import's class) draws the edge, as on the base;
 * never the name chain. */
static int jts_check_unresolved_ctor(jts_index_t *ix) {
    char got[4096];
    int n = jts_targets(ix, "makeMark", "Mark", got, sizeof(got));
    if (n == 1 && strstr(got, ".coll.Mark|lsp_constructor")) {
        return 1;
    }
    printf("\n  makeMark -> Mark: want one engine edge to coll.Mark, have %d:\n%s", n, got);
    return 0;
}

/* 22: a member type inherited from a supertype (JLS 8.5). Entry in a
 * HashMap subclass is java.util.Map.Entry: getKey() is a library call,
 * never a project method by name; Part2, inherited from a class of the same
 * file, binds its v2(). */
static int jts_check_inherited_member_type(jts_index_t *ix) {
    int ok = jts_expect_none_to(ix, "firstKeyNull", "getKey", "Decoys");
    ok &= jts_expect_engine(ix, "usePart", "v2", "lib.Nest2.Part2.v2");
    return ok;
}

/* 23: a decorator's call to its delegate, inner.area2() inside Framed.area2,
 * binds Shape2.area2 (javap: invokeinterface iface2/Shape2.area2): never the
 * calling method itself, the one implementer the calling file declares. */
static int jts_check_decorator(jts_index_t *ix) {
    /* One edge: never decoy.Decoys.area2, never Framed.area2 itself. */
    int ok = jts_expect_engine(ix, "area2", "area2", "iface2.Shape2.area2");
    /* 23b: inner.get() inside Framed3.get binds Supplier.get (javap:
     * invokeinterface iface3/Shape3.get, a library method): no project
     * method, neither the decoy nor the calling method. */
    ok &= jts_expect_none_to(ix, "get", "get", "Decoys");
    ok &= jts_expect_none_to(ix, "get", "get", "user.Framed3.get");
    return ok;
}

/* A package over two source trees: the class found in exactly one of them
 * answers; the class both define answers nothing. */
static int jts_check_split_package(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "splitTree", "probe", "split.Probe.probe");
    ok &= jts_expect_no_engine(ix, "splitDup", "probe");
    /* The two Dups' tagDup() return different classes: a pick types sealDup(). */
    ok &= jts_expect_no_engine(ix, "splitDupTag", "sealDup");
    return ok;
}

/* 24: a member type inherited from a project supertype of another file and
 * package shadows the single-type import, the own package and the
 * same-named classes (JLS 6.4.1, 8.5): Key24 and Val25 in Sub24 (extends
 * Base24) are Base24's, also inside Sub24's nested Inner24 and through
 * Sub24.mk25's return type in another file (javap: Base24$Key24.pick24,
 * then Right24.hit24). */
static int jts_check_inherited_project_member(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "inhImport", "hit24", "hit.Right24.hit24");
    ok &= jts_expect_engine(ix, "inhPackage", "hit25", "hit.Right25.hit25");
    ok &= jts_expect_engine(ix, "inhEnclosing", "hit24", "hit.Right24.hit24");
    ok &= jts_expect_engine(ix, "inhReturn", "hit25", "hit.Right25.hit25");
    return ok;
}

/* 26: a type parameter named like a class of the declaring package is the
 * parameter (class Box26<Item26>, method <Item26> pick26), never tp.Item26:
 * javac runs Other26.weigh26 for both. The step gives the spelling no
 * identity and its erasure no answer (the type argument is not the step's),
 * so no engine edge reaches Item26; partner: other26()'s Other26 types the
 * call. */
static int jts_check_type_param(jts_index_t *ix) {
    int ok = jts_expect_no_engine_to(ix, "tpClass", "weigh26", "tp.Item26");
    ok &= jts_expect_no_engine_to(ix, "tpMethod", "weigh26", "tp.Item26");
    ok &= jts_expect_engine(ix, "tpPartner", "weigh26", "tp.Other26.weigh26");
    return ok;
}

/* 27: Impl implements I2, `interface I2 extends i1.I1` (qualified), and I1
 * declares Z1: Z1 in Impl is I1.Z1, which shadows the imported i3.Z1; the
 * same through `interface I7 extends i1.I1, java.lang.Comparable<String>`. */
static int jts_check_qualified_interface(jts_index_t *ix) {
    int ok = jts_expect_engine(ix, "ifaceDeep", "hitC", "ec.RightC.hitC");
    ok &= jts_expect_engine(ix, "ifaceDotted2", "hitC", "ec.RightC.hitC");
    return ok;
}

/* Both routes draw the same engine edges for the fixture. Name-chain edges
 * are left out: their strategy and pick already differ between the routes on
 * main (route differences D5, D9). */
static char *jts_engine_edges(jts_index_t *ix) {
    const char *sql = "SELECT group_concat(x, char(10)) FROM (SELECT s.name || '|' || "
                      "coalesce(json_extract(e.properties,'$.callee'),'') || '|' || "
                      "substr(t.qualified_name, length(?1) + 2) || '|' || "
                      "json_extract(e.properties,'$.strategy') AS x FROM edges e "
                      "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = 'CALLS' AND s.file_path NOT LIKE '%/fill/%' "
                      "AND json_extract(e.properties,'$.strategy') LIKE 'lsp%' ORDER BY x)";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(ix->db, sql, -1, &st, NULL) != SQLITE_OK) {
        return NULL;
    }
    sqlite3_bind_text(st, 1, ix->project, -1, SQLITE_STATIC);
    char *out = NULL;
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        out = strdup((const char *)sqlite3_column_text(st, 0));
    }
    sqlite3_finalize(st);
    return out;
}

TEST(java_type_scope_routes_agree) {
    jts_index_t seq;
    jts_index_t par;
    int seq_rc = jts_index(&seq, 0);
    int par_rc = jts_index(&par, 60);
    char *a = seq_rc == 0 ? jts_engine_edges(&seq) : NULL;
    char *b = par_rc == 0 ? jts_engine_edges(&par) : NULL;
    int same = a && b && strcmp(a, b) == 0;
    if (!same) {
        printf("\n  sequential:\n%s\n  parallel:\n%s\n", a ? a : "(none)", b ? b : "(none)");
    }
    free(a);
    free(b);
    jts_close(&seq);
    jts_close(&par);
    ASSERT_TRUE(same);
    PASS();
}

/* ── Incremental: a one-file import edit ─────────────────────────────── */

static bool jts_pipeline(const char *repo, const char *db, cbm_incremental_route_t *route) {
    cbm_pipeline_t *p = cbm_pipeline_new(repo, db, CBM_MODE_FULL);
    if (!p) {
        return false;
    }
    bool ok = cbm_pipeline_set_project_name(p, "jtsinc");
    cbm_pipeline_set_persistence(p, false);
    ok = ok && cbm_pipeline_run(p) == 0;
    if (route) {
        *route = cbm_pipeline_incremental_test_last_route();
    }
    cbm_pipeline_free(p);
    return ok;
}

/* A file's whole text (malloc'd), or NULL. */
static char *jts_read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    char *out = NULL;
    if (fseek(f, 0, SEEK_END) == 0) {
        long n = ftell(f);
        if (n >= 0 && fseek(f, 0, SEEK_SET) == 0) {
            out = (char *)malloc((size_t)n + 1);
            if (out && fread(out, 1, (size_t)n, f) != (size_t)n) {
                free(out);
                out = NULL;
            }
            if (out) {
                out[n] = '\0';
            }
        }
    }
    fclose(f);
    return out;
}

static bool jts_open(jts_index_t *ix, const char *db_path) {
    memset(ix, 0, sizeof(*ix));
    snprintf(ix->project, sizeof(ix->project), "%s", "jtsinc");
    if (sqlite3_open_v2(db_path, &ix->db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(ix->db);
        ix->db = NULL;
        return false;
    }
    return true;
}

/* `import lib.Foo;` in Main.java becomes `import lib.*;`. The closure repair
 * re-parses Main.java only; Main's genericReturn reads Repo.all()'s
 * List<Item>, whose Item identity (lib2.Item, from Repo.java's import) comes
 * from the stored surface of a file the repair does not re-parse, in a
 * package Main does not import. The repaired graph's engine edges are a full
 * index's of the edited tree. */
TEST(java_type_scope_incremental_import_edit) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_jtsi_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    jts_write_fixture(repo, 0);
    cbm_pipeline_incremental_test_reset_faults();
    bool ok = jts_pipeline(repo, inc_db, NULL);
    char main_path[600];
    snprintf(main_path, sizeof(main_path), "%s/src/main/java/user/Main.java", repo);
    char *text = jts_read_file(main_path);
    const char *old = "import lib.Foo;\n";
    char *at = text ? strstr(text, old) : NULL;
    ok = ok && at;
    if (at) {
        char edited[8192];
        snprintf(edited, sizeof(edited), "%.*simport lib.*;\n%s", (int)(at - text), text,
                 at + strlen(old));
        th_write_file(main_path, edited);
    }
    free(text);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    ok = ok && jts_pipeline(repo, inc_db, &route) && jts_pipeline(repo, full_db, NULL);
    if (route != CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR) {
        printf("\n  the edit did not take the closure repair (route %d)\n", (int)route);
        ok = false;
    }
    jts_index_t inc;
    jts_index_t full;
    bool inc_open = jts_open(&inc, inc_db);
    bool full_open = jts_open(&full, full_db);
    bool opened = inc_open && full_open;
    char *a = opened ? jts_engine_edges(&inc) : NULL;
    char *b = opened ? jts_engine_edges(&full) : NULL;
    bool same = a && b && strcmp(a, b) == 0;
    if (!same) {
        printf("\n  incremental:\n%s\n  full:\n%s\n", a ? a : "(none)", b ? b : "(none)");
    }
    ok = ok && same && opened &&
         jts_expect_engine(&inc, "genericReturn", "weigh", "lib2.Item.weigh") &&
         jts_expect_engine(&inc, "singleImport", "getId", "lib.Foo.getId");
    free(a);
    free(b);
    sqlite3_close(inc.db);
    sqlite3_close(full.db);
    th_cleanup(tmp);
    ASSERT_TRUE(ok);
    PASS();
}

/* Every CALLS edge out of user/Main.java, one sorted line each, the project
 * prefix dropped. */
static char *jts_main_edges(jts_index_t *ix) {
    const char *sql = "SELECT group_concat(x, char(10)) FROM (SELECT s.name || '|' || "
                      "coalesce(json_extract(e.properties,'$.callee'),'') || '|' || "
                      "substr(t.qualified_name, length(?1) + 2) || '|' || "
                      "json_extract(e.properties,'$.strategy') AS x FROM edges e "
                      "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
                      "WHERE e.type = 'CALLS' AND s.file_path LIKE '%user/Main.java' ORDER BY x)";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(ix->db, sql, -1, &st, NULL) != SQLITE_OK) {
        return NULL;
    }
    sqlite3_bind_text(st, 1, ix->project, -1, SQLITE_STATIC);
    char *out = NULL;
    if (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char *text = sqlite3_column_text(st, 0);
        out = strdup(text ? (const char *)text : "");
    }
    sqlite3_finalize(st);
    return out;
}

/* A body edit (a newline) of Main.java, which calls run() on ext.Service (a
 * library type; the project has other.Service): the closure repair re-parses
 * Main.java alone and has no shared registry, and its CALLS edges must be a
 * full index's: no edge for that call (a library receiver), never the name
 * guess other.Service.run or Decoys.run. */
TEST(java_type_scope_incremental_library_receiver) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_jtsl_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    jts_write_fixture(repo, 0);
    cbm_pipeline_incremental_test_reset_faults();
    bool ok = jts_pipeline(repo, inc_db, NULL);
    char main_path[600];
    snprintf(main_path, sizeof(main_path), "%s/src/main/java/user/Main.java", repo);
    char *text = jts_read_file(main_path);
    ok = ok && text;
    if (text) {
        char edited[8192];
        snprintf(edited, sizeof(edited), "%s\n", text);
        th_write_file(main_path, edited);
    }
    free(text);
    cbm_incremental_route_t route = CBM_INCREMENTAL_ROUTE_NONE;
    ok = ok && jts_pipeline(repo, inc_db, &route) && jts_pipeline(repo, full_db, NULL);
    if (route != CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR) {
        printf("\n  the edit did not take the closure repair (route %d)\n", (int)route);
        ok = false;
    }
    jts_index_t inc;
    jts_index_t full;
    bool inc_open = jts_open(&inc, inc_db);
    bool full_open = jts_open(&full, full_db);
    bool opened = inc_open && full_open;
    char *a = opened ? jts_main_edges(&inc) : NULL;
    char *b = opened ? jts_main_edges(&full) : NULL;
    bool same = a && b && strcmp(a, b) == 0;
    if (!same) {
        printf("\n  incremental:\n%s\n  full:\n%s\n", a ? a : "(none)", b ? b : "(none)");
    }
    ok = ok && same && opened && jts_expect_none_to(&inc, "externalImport", "run", "Service") &&
         jts_expect_none_to(&inc, "externalImport", "run", "Decoys");
    free(a);
    free(b);
    sqlite3_close(inc.db);
    sqlite3_close(full.db);
    th_cleanup(tmp);
    ASSERT_TRUE(ok);
    PASS();
}

/* ── Growth (counted work, never time) ───────────────────────────────── */

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* The package table: `packages` packages over `dirs` directories each. Its
 * build sorts (n log n comparisons); a lookup is a binary search plus the
 * package's own modules. Returns the build's work; *lookup_work the work of
 * 100 lookups of present packages. */
static uint64_t jts_table_work(int packages, int dirs, uint64_t *lookup_work) {
    CBMArena arena;
    cbm_arena_init(&arena);
    int n = packages * dirs;
    const char **pkg = calloc((size_t)n, sizeof(char *));
    const char **mod = calloc((size_t)n, sizeof(char *));
    for (int p = 0; p < packages; p++) {
        for (int d = 0; d < dirs; d++) {
            pkg[p * dirs + d] = cbm_arena_sprintf(&arena, "com.acme.p%d", (p * 7919) % packages);
            mod[p * dirs + d] =
                cbm_arena_sprintf(&arena, "proj.m%d.src.main.java.com.acme.p%d", d, p);
        }
    }
    uint64_t before = cbm_pxc_packages_test_work();
    const cbm_pxc_packages_t *t = cbm_pxc_packages_from_pairs(&arena, pkg, mod, n);
    uint64_t build = cbm_pxc_packages_test_work() - before;
    before = cbm_pxc_packages_test_work();
    for (int i = 0; i < 100; i++) {
        char key[64];
        snprintf(key, sizeof(key), "com.acme.p%d", (i * 31) % packages);
        const char *const *mods = NULL;
        (void)cbm_pxc_package_modules(t, key, strlen(key), &mods);
    }
    *lookup_work = cbm_pxc_packages_test_work() - before;
    free(pkg);
    free(mod);
    cbm_arena_destroy(&arena);
    return build;
}

/* Packages x directories at 1x, 4x, 16x: the build stays within n log n,
 * a lookup within log n (no lookup walks the table). */
TEST(java_type_scope_growth_package_table) {
    uint64_t look1 = 0;
    uint64_t look4 = 0;
    uint64_t look16 = 0;
    uint64_t b1 = jts_table_work(50, 3, &look1);
    uint64_t b4 = jts_table_work(200, 3, &look4);
    uint64_t b16 = jts_table_work(800, 3, &look16);
    double logs = log2(2400.0) / log2(150.0); /* the log factor between 1x and 16x */
    printf("\n  table build %llu / %llu / %llu, 100 lookups %llu / %llu / %llu\n",
           (unsigned long long)b1, (unsigned long long)b4, (unsigned long long)b16,
           (unsigned long long)look1, (unsigned long long)look4, (unsigned long long)look16);
    ASSERT_TRUE(b1 > 0 && look1 > 0);
    ASSERT_TRUE((double)b16 <= (double)b1 * 16.0 * logs * 1.25);
    ASSERT_TRUE((double)look16 <= (double)look1 * logs * 1.25);
    PASS();
}

/* One file's step over its import rows: `imports` single-type imports of
 * project classes of one package (in one directory) among `classes` classes,
 * the calling-file table built and read. Returns the counted work (the step's
 * and the table's). *identities: the identities found. */
static uint64_t jts_rows_work(int imports, int classes, int *identities) {
    cbm_registry_t *reg = cbm_registry_new();
    CBMArena arena;
    cbm_arena_init(&arena);
    const char **types = calloc((size_t)classes, sizeof(char *));
    for (int c = 0; c < classes; c++) {
        char name[32];
        snprintf(name, sizeof(name), "C%d", c);
        char *qn = cbm_arena_sprintf(&arena, "proj.src.main.java.q.C%d", c);
        cbm_registry_add(reg, name, qn, "Class", false);
        types[c] = qn;
    }
    const char *pkg[] = {"q"};
    const char *mod[] = {"proj.src.main.java.q"};
    const cbm_pxc_packages_t *t =
        cbm_pxc_packages_from_pairs_types(&arena, pkg, mod, 1, types, classes);
    free(types);
    CBMImport *rows = calloc((size_t)imports, sizeof(CBMImport));
    for (int i = 0; i < imports; i++) {
        int c = (int)(((long long)i * 7919) % classes);
        rows[i].local_name = cbm_arena_sprintf(&arena, "C%d", c);
        rows[i].module_path = cbm_arena_sprintf(&arena, "q.C%d", c);
    }
    CBMFileResult *fr = calloc(1, sizeof(*fr));
    fr->imports.items = rows;
    fr->imports.count = imports;
    fr->namespace_name = "app";
    cbm_pipeline_ctx_t *ctx = calloc(1, sizeof(*ctx));
    ctx->registry = reg;
    uint64_t before = cbm_type_scope_test_work() + cbm_pxc_packages_test_work();
    CBMArena scratch;
    cbm_arena_init(&scratch);
    cbm_type_scope_file_t *fs = cbm_pxc_file_scope(ctx, CBM_LANG_JAVA, fr, "proj.src.main.java.app",
                                                   NULL, NULL, 0, t, &scratch);
    const char *const *locals = NULL;
    const char *const *qns = NULL;
    *identities = cbm_type_scope_file_imports(fs, &locals, &qns);
    cbm_type_scope_file_end(fs);
    uint64_t work = cbm_type_scope_test_work() + cbm_pxc_packages_test_work() - before;
    cbm_arena_destroy(&scratch);
    free(ctx);
    free(fr);
    free(rows);
    cbm_registry_free(reg);
    cbm_arena_destroy(&arena);
    return work;
}

/* A file with very many imports: the work grows with the imports (1x, 4x,
 * 16x) and not faster; every import of a project class finds its one class. */
TEST(java_type_scope_growth_many_imports) {
    int id1 = 0;
    int id4 = 0;
    int id16 = 0;
    uint64_t w1 = jts_rows_work(100, 4000, &id1);
    uint64_t w4 = jts_rows_work(400, 4000, &id4);
    uint64_t w16 = jts_rows_work(1600, 4000, &id16);
    printf("\n  imports 100/400/1600: work %llu / %llu / %llu, identities %d / %d / %d\n",
           (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16, id1, id4, id16);
    ASSERT_TRUE(id1 > 0 && id4 > id1 && id16 > id4);
    ASSERT_TRUE((double)w16 <= (double)w1 * 16.0 * 1.25);
    PASS();
}

/* A package with very many classes: a file importing a few of them does the
 * same work whatever the package's size (1x, 4x, 16x): nothing scans the
 * package. */
TEST(java_type_scope_growth_large_package) {
    int ida = 0;
    int idb = 0;
    int idc = 0;
    uint64_t w1 = jts_rows_work(20, 1000, &ida);
    uint64_t w4 = jts_rows_work(20, 4000, &idb);
    uint64_t w16 = jts_rows_work(20, 16000, &idc);
    printf("\n  package 1000/4000/16000 classes, 20 imports: work %llu / %llu / %llu\n",
           (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16);
    ASSERT_TRUE(ida == 20 && idb == 20 && idc == 20);
    ASSERT_TRUE((double)w16 <= (double)w1 * 1.25);
    PASS();
}

/* One package `big` over `m` directories, one class C<i> each (the review's
 * generator gen3.py): every directory's file of package big imports two
 * classes of big by name and writes a third of another directory, which its
 * own package names. Over all m files: the table's and the type index's
 * build, each file's import map and the third spelling. Returns the counted
 * work; *identities the identities found (3 per file). */
static uint64_t jts_spread_work(int m, int *identities) {
    cbm_registry_t *reg = cbm_registry_new();
    CBMArena arena;
    cbm_arena_init(&arena);
    const char **pkg = calloc((size_t)m, sizeof(char *));
    const char **mod = calloc((size_t)m, sizeof(char *));
    const char **types = calloc((size_t)m, sizeof(char *));
    for (int i = 0; i < m; i++) {
        char name[32];
        snprintf(name, sizeof(name), "C%d", i);
        pkg[i] = "big";
        mod[i] = cbm_arena_sprintf(&arena, "proj.m%d.src.main.java.big", i);
        types[i] = cbm_arena_sprintf(&arena, "%s.C%d", mod[i], i);
        cbm_registry_add(reg, name, types[i], "Class", false);
    }
    cbm_pipeline_ctx_t *ctx = calloc(1, sizeof(*ctx));
    ctx->registry = reg;
    uint64_t before = cbm_type_scope_test_work() + cbm_pxc_packages_test_work();
    const cbm_pxc_packages_t *t = cbm_pxc_packages_from_pairs_types(&arena, pkg, mod, m, types, m);
    *identities = 0;
    for (int i = 0; i < m; i++) {
        CBMImport rows[2];
        memset(rows, 0, sizeof(rows));
        for (int r = 0; r < 2; r++) {
            int c = (i + 1 + r) % m;
            rows[r].local_name = cbm_arena_sprintf(&arena, "C%d", c);
            rows[r].module_path = cbm_arena_sprintf(&arena, "big.C%d", c);
        }
        CBMFileResult fr;
        memset(&fr, 0, sizeof(fr));
        fr.imports.items = rows;
        fr.imports.count = 2;
        fr.namespace_name = "big";
        CBMArena scratch;
        cbm_arena_init(&scratch);
        cbm_type_scope_file_t *fs =
            cbm_pxc_file_scope(ctx, CBM_LANG_JAVA, &fr, mod[i], NULL, NULL, 0, t, &scratch);
        const char *const *locals = NULL;
        const char *const *qns = NULL;
        int n = cbm_type_scope_file_imports(fs, &locals, &qns);
        for (int k = 0; k < n; k++) {
            *identities += qns[k] && strncmp(qns[k], "proj.m", 6) == 0;
        }
        char third[32];
        snprintf(third, sizeof(third), "C%d", (i + m - 1) % m);
        *identities += cbm_type_scope_resolve(fs, third, strlen(third)) != NULL;
        cbm_type_scope_file_end(fs);
        cbm_arena_destroy(&scratch);
    }
    uint64_t work = cbm_type_scope_test_work() + cbm_pxc_packages_test_work() - before;
    free(ctx);
    free(pkg);
    free(mod);
    free(types);
    cbm_registry_free(reg);
    cbm_arena_destroy(&arena);
    return work;
}

/* Directories per package at 1x, 4x, 16x (100 / 400 / 1,600): the work over
 * all files grows with them and a log factor (the sorted index), never with
 * their square; every file finds its three classes. */
TEST(java_type_scope_growth_package_spread) {
    int id1 = 0;
    int id4 = 0;
    int id16 = 0;
    uint64_t w1 = jts_spread_work(100, &id1);
    uint64_t w4 = jts_spread_work(400, &id4);
    uint64_t w16 = jts_spread_work(1600, &id16);
    double logs = log2(1600.0) / log2(100.0);
    printf("\n  one package over 100/400/1600 directories: work %llu / %llu / %llu, "
           "identities %d / %d / %d\n",
           (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16, id1, id4,
           id16);
    ASSERT_TRUE(id1 == 300 && id4 == 1200 && id16 == 4800);
    ASSERT_TRUE((double)w16 <= (double)w1 * 16.0 * logs * 1.25);
    PASS();
}

/* The inherited-member walk over a supertype chain of n types: C<i> extends
 * C<i+1> and C<i+2> (a diamond at every step: each type is met twice), the
 * last declaring the member type M; and the same chain closed into a cycle
 * (the last extends C0) without M. Returns the counted work; *found whether
 * M was found. */
static uint64_t jts_chain_work(int n, bool cycle, bool *found) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, &arena);
    for (int i = 0; i < n; i++) {
        CBMRegisteredType t;
        memset(&t, 0, sizeof(t));
        t.qualified_name = cbm_arena_sprintf(&arena, "ch.C%d", i);
        t.short_name = cbm_arena_sprintf(&arena, "C%d", i);
        const char **sup = (const char **)cbm_arena_alloc(&arena, 3 * sizeof(char *));
        int k = 0;
        if (i + 1 < n) {
            sup[k++] = cbm_arena_sprintf(&arena, "ch.C%d", i + 1);
        } else if (cycle) {
            sup[k++] = "ch.C0";
        }
        if (i + 2 < n) {
            sup[k++] = cbm_arena_sprintf(&arena, "ch.C%d", i + 2);
        }
        sup[k] = NULL;
        t.embedded_types = sup;
        cbm_registry_add_type(&reg, t);
    }
    if (!cycle) {
        CBMRegisteredType m;
        memset(&m, 0, sizeof(m));
        m.qualified_name = cbm_arena_sprintf(&arena, "ch.C%d.M", n - 1);
        m.short_name = "M";
        cbm_registry_add_type(&reg, m);
    }
    cbm_registry_finalize(&reg);
    (void)cbm_lsp_work_take();
    *found = cbm_java_test_inherited_member(&reg, &arena, "ch.C0", "M") != NULL;
    uint64_t work = cbm_lsp_work_take();
    cbm_arena_destroy(&arena);
    return work;
}

/* A supertype chain of 50 / 200 / 800 types: the member at its end is found
 * (no cap decides it), the cycle ends, and the counted work grows linearly
 * with the chain. */
TEST(java_type_scope_growth_supertype_chain) {
    bool f1 = false;
    bool f4 = false;
    bool f16 = false;
    bool c1 = true;
    bool c16 = true;
    uint64_t w1 = jts_chain_work(50, false, &f1);
    uint64_t w4 = jts_chain_work(200, false, &f4);
    uint64_t w16 = jts_chain_work(800, false, &f16);
    uint64_t y1 = jts_chain_work(50, true, &c1);
    uint64_t y16 = jts_chain_work(800, true, &c16);
    printf("\n  chain 50/200/800: work %llu / %llu / %llu, found %d %d %d; "
           "cycle 50/800: work %llu / %llu\n",
           (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16, f1, f4, f16,
           (unsigned long long)y1, (unsigned long long)y16);
    ASSERT_TRUE(f1 && f4 && f16);
    ASSERT_TRUE(!c1 && !c16);
    ASSERT_TRUE(w1 > 0 && y1 > 0);
    ASSERT_TRUE((double)w16 <= (double)w1 * 16.0 * 1.25);
    ASSERT_TRUE((double)y16 <= (double)y1 * 16.0 * 1.25);
    PASS();
}
#endif

#define JTS_ROUTES(name, check)                \
    TEST(java_type_scope_##name##_sequential) { \
        ASSERT_TRUE(jts_run(0, check));         \
        PASS();                                 \
    }                                           \
    TEST(java_type_scope_##name##_parallel) {   \
        ASSERT_TRUE(jts_run(60, check));        \
        PASS();                                 \
    }

JTS_ROUTES(single_import, jts_check_single_import)
JTS_ROUTES(external_import, jts_check_external_import)
JTS_ROUTES(on_demand, jts_check_on_demand)
JTS_ROUTES(java_lang_shadow, jts_check_java_lang_shadow)
JTS_ROUTES(nested, jts_check_nested)
JTS_ROUTES(static_import, jts_check_static)
JTS_ROUTES(default_package, jts_check_default_package)
JTS_ROUTES(generic_return, jts_check_generic_return)
JTS_ROUTES(inherited_field, jts_check_inherited_field)
JTS_ROUTES(jdk_receiver, jts_check_jdk_receiver)
JTS_ROUTES(split_package, jts_check_split_package)
JTS_ROUTES(constructor_import, jts_check_constructor_import)
JTS_ROUTES(unresolved_ctor, jts_check_unresolved_ctor)
JTS_ROUTES(inherited_member_type, jts_check_inherited_member_type)
JTS_ROUTES(decorator, jts_check_decorator)
JTS_ROUTES(inherited_project_member, jts_check_inherited_project_member)
JTS_ROUTES(type_param, jts_check_type_param)
JTS_ROUTES(qualified_interface, jts_check_qualified_interface)

SUITE(java_type_scope) {
    RUN_TEST(java_type_scope_single_import_sequential);
    RUN_TEST(java_type_scope_single_import_parallel);
    RUN_TEST(java_type_scope_external_import_sequential);
    RUN_TEST(java_type_scope_external_import_parallel);
    RUN_TEST(java_type_scope_on_demand_sequential);
    RUN_TEST(java_type_scope_on_demand_parallel);
    RUN_TEST(java_type_scope_java_lang_shadow_sequential);
    RUN_TEST(java_type_scope_java_lang_shadow_parallel);
    RUN_TEST(java_type_scope_nested_sequential);
    RUN_TEST(java_type_scope_nested_parallel);
    RUN_TEST(java_type_scope_static_import_sequential);
    RUN_TEST(java_type_scope_static_import_parallel);
    RUN_TEST(java_type_scope_default_package_sequential);
    RUN_TEST(java_type_scope_default_package_parallel);
    RUN_TEST(java_type_scope_generic_return_sequential);
    RUN_TEST(java_type_scope_generic_return_parallel);
    RUN_TEST(java_type_scope_inherited_field_sequential);
    RUN_TEST(java_type_scope_inherited_field_parallel);
    RUN_TEST(java_type_scope_jdk_receiver_sequential);
    RUN_TEST(java_type_scope_jdk_receiver_parallel);
    RUN_TEST(java_type_scope_split_package_sequential);
    RUN_TEST(java_type_scope_split_package_parallel);
    RUN_TEST(java_type_scope_constructor_import_sequential);
    RUN_TEST(java_type_scope_constructor_import_parallel);
    RUN_TEST(java_type_scope_unresolved_ctor_sequential);
    RUN_TEST(java_type_scope_unresolved_ctor_parallel);
    RUN_TEST(java_type_scope_inherited_member_type_sequential);
    RUN_TEST(java_type_scope_inherited_member_type_parallel);
    RUN_TEST(java_type_scope_decorator_sequential);
    RUN_TEST(java_type_scope_decorator_parallel);
    RUN_TEST(java_type_scope_inherited_project_member_sequential);
    RUN_TEST(java_type_scope_inherited_project_member_parallel);
    RUN_TEST(java_type_scope_type_param_sequential);
    RUN_TEST(java_type_scope_type_param_parallel);
    RUN_TEST(java_type_scope_qualified_interface_sequential);
    RUN_TEST(java_type_scope_qualified_interface_parallel);
    RUN_TEST(java_type_scope_routes_agree);
    RUN_TEST(java_type_scope_incremental_import_edit);
    RUN_TEST(java_type_scope_incremental_library_receiver);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    RUN_TEST(java_type_scope_growth_package_table);
    RUN_TEST(java_type_scope_growth_many_imports);
    RUN_TEST(java_type_scope_growth_large_package);
    RUN_TEST(java_type_scope_growth_package_spread);
    RUN_TEST(java_type_scope_growth_supertype_chain);
#endif
}
