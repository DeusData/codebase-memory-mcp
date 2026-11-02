/* Strict static selection for Java, Kotlin and C#, through the whole pipeline.
 * Every rule has its positive and a decoy: a project declaration of the same
 * spelling that the language's rules do not select. Forms are taken from real
 * code seen in Exposed, Elasticsearch and System.Text.Json. */
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "sqlite3.h"
#include "resolver_quality.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *path, *text;
} OOFile;

typedef struct {
    const char *root;
    cbm_store_t *store;
} OOIndex;

/* >= 50 files and four workers force pass_parallel.c. */
static const char *oo_filler(int i, const char *extension, char *body, size_t capacity) {
    if (!strcmp(extension, "java"))
        snprintf(body, capacity, "package pad;\nclass Filler%d { int f() { return %d; } }\n", i, i);
    else if (!strcmp(extension, "kt"))
        snprintf(body, capacity, "package pad\nclass Filler%d { fun f(): Int = %d }\n", i, i);
    else
        snprintf(body, capacity, "namespace Pad { class Filler%d { int F() { return %d; } } }\n", i,
                 i);
    return body;
}

static bool oo_open(OOIndex *index, const OOFile *files, int count, bool parallel,
                    const char *extension) {
    index->store = NULL;
    index->root = th_mktempdir("cbm_oo");
    if (!index->root)
        return false;
    char repo[768], database[768];
    snprintf(repo, sizeof(repo), "%s/repo", index->root);
    snprintf(database, sizeof(database), "%s/index.db", index->root);
    if (!cbm_mkdir_p(repo, 0755))
        return false;
    for (int i = 0; i < count; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", repo, files[i].path);
        char *slash = strrchr(path, '/');
        *slash = '\0';
        if (!cbm_mkdir_p(path, 0755))
            return false;
        *slash = '/';
        if (th_write_file(path, files[i].text) != 0)
            return false;
    }
    char *saved = NULL;
    if (parallel) {
        for (int i = 0; i < 52; i++) {
            char name[256], body[160];
            snprintf(name, sizeof(name), "%s/Filler%d.%s", repo, i, extension);
            if (th_write_file(name, oo_filler(i, extension, body, sizeof(body))) != 0)
                return false;
        }
        const char *old = getenv("CBM_WORKERS");
        saved = old ? strdup(old) : NULL;
        cbm_setenv("CBM_WORKERS", "4", 1);
    }
    cbm_pipeline_t *pipeline = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "oo") ? cbm_pipeline_run(pipeline)
                                                                        : -1;
    cbm_pipeline_free(pipeline);
    if (parallel) {
        if (saved) {
            cbm_setenv("CBM_WORKERS", saved, 1);
            free(saved);
        } else {
            cbm_unsetenv("CBM_WORKERS");
        }
    }
    index->store = rc == 0 ? cbm_store_open_path_existing(database) : NULL;
    return index->store != NULL;
}

static void oo_close(OOIndex *index) {
    if (index->store)
        cbm_store_close(index->store);
    index->store = NULL;
    if (index->root)
        th_rmtree(index->root);
    index->root = NULL;
}

/* Edges between nodes whose qualified names end in these exact dotted tails. */
static int oo_edges(const OOIndex *index, const char *type, const char *source, const char *target) {
    sqlite3_stmt *statement = NULL;
    int edges = -1;
    if (sqlite3_prepare_v2(cbm_store_get_db(index->store),
                           "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                           "JOIN nodes t ON t.id=e.target_id WHERE e.type=?1 "
                           "AND ('.'||s.qualified_name) LIKE '%.'||?2 "
                           "AND ('.'||t.qualified_name) LIKE '%.'||?3",
                           -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, type, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 2, source, -1, SQLITE_STATIC);
        sqlite3_bind_text(statement, 3, target, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            edges = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return edges;
}

/* Emitted occurrences spelled exactly so: edges cannot tell a right site from a wrong one. */
static int oo_emitted_spelled(const OOIndex *index, const char *spelling) {
    sqlite3_stmt *statement = NULL;
    int rows = -1;
    if (sqlite3_prepare_v2(cbm_store_get_db(index->store),
                           "SELECT count(*) FROM resolver_occurrences WHERE emitted=1 AND spelling=?1",
                           -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, spelling, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW)
            rows = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return rows;
}

#define OO_EXPECT(type, source, target, expected)                                          \
    do {                                                                                   \
        int oo_got = oo_edges(&index, type, source, target);                               \
        if (oo_got != (expected)) {                                                        \
            printf("  FAIL %s:%d: %s %s -> %s: got %d, expected %d\n", __FILE__, __LINE__, \
                   type, source, target, oo_got, (expected));                              \
            failed++;                                                                      \
        }                                                                                  \
    } while (0)

/* ── Java ───────────────────────────────────────────────────────────── */

static const OOFile JAVA_FILES[] = {
    {"org/acme/core/Base.java",
     "package org.acme.core;\npublic class Base {\n"
     "  public void inherited() {}\n"
     "  private void hidden() {}\n"
     "  public void overload(int a) {}\n"
     "  public void overload(int a, int b) {}\n"
     "}\n"},
    {"org/acme/core/Store.java",
     "package org.acme.core;\npublic class Store {\n"
     "  public int value;\n"
     "  public Store chain() { return this; }\n"
     "  public int get() { return value; }\n"
     "  public static Store open() { return new Store(); }\n"
     "}\n"},
    {"org/acme/core/Sized.java",
     "package org.acme.core;\npublic class Sized {\n"
     "  public Sized(int n) {}\n"
     "  public Sized(int n, int m) {}\n"
     "}\n"},
    {"org/acme/core/Plain.java", "package org.acme.core;\npublic class Plain {}\n"},
    {"org/acme/core/Api.java",
     "package org.acme.core;\npublic interface Api {\n  void run();\n"
     "  default void twice() { run(); }\n}\n"},
    {"org/acme/core/Impl.java",
     "package org.acme.core;\npublic class Impl implements Api {\n  public void run() {}\n}\n"},
    {"org/acme/core/Box.java",
     "package org.acme.core;\npublic class Box<T> {\n  public T get() { return null; }\n}\n"},
    {"org/acme/util/Helpers.java",
     "package org.acme.util;\npublic class Helpers {\n"
     "  public static int imported() { return 1; }\n"
     "  public static int onDemand() { return 2; }\n"
     "  public static int shadowed() { return 3; }\n"
     "  public void instanceOnly() {}\n"
     "}\n"},
    {"org/acme/other/Decoy.java",
     "package org.acme.other;\npublic class Decoy {\n"
     "  public void inherited() {}\n"
     "  public int get() { return 0; }\n"
     "  public void run() {}\n"
     "  public void local() {}\n"
     "}\n"},
    {"org/acme/app/Main.java",
     "package org.acme.app;\n"
     "import org.acme.core.*;\n"
     "import org.acme.other.Decoy;\n"
     "import org.acme.util.Helpers;\n"
     "import static org.acme.util.Helpers.imported;\n"
     "import static org.acme.util.Helpers.shadowed;\n"
     "public class Main extends Base implements Runnable2 {\n"
     "  Store field = new Store();\n"
     "  Decoy shadowed_field;\n"
     "  public void run() {}\n"
     "  int shadowed() { return 9; }\n"
     "  void callsInherited() { inherited(); }\n"
     "  void callsOverloads() { overload(1); overload(1, 2); }\n"
     "  void callsHidden() { hidden(); }\n"
     "  void callsImported() { imported(); }\n"
     "  void callsShadowed() { shadowed(); }\n"
     "  void callsStatic() { Helpers.imported(); Store.open(); }\n"
     "  void callsInstanceThroughClass() { Helpers.instanceOnly(); }\n"
     "  void callsField() { field.get(); this.field.chain().get(); }\n"
     "  void callsVar() { var s = new Store(); s.get(); }\n"
     "  void callsCast(Object o) { ((Store) o).get(); }\n"
     "  void callsTernary(boolean b) { (b ? field : null).get(); }\n"
     "  void callsInterface(Api api) { api.run(); }\n"
     "  void callsGeneric(Box<Store> box) { box.get().get(); }\n"
     "  void callsNew() { new Plain(); new Sized(1); new Sized(1, 2); }\n"
     "  void readsField() { int v = field.value; }\n"
     "  void shadowsField(Decoy field) { field.get(); }\n"
     "  void lambdaShadows() { Run2 r = field -> field.get(); }\n"
     "  void patternShadows(Object o) { if (o instanceof Decoy field) { field.get(); } }\n"
     "  void loopShadows(java.util.List<Decoy> xs) { for (Decoy field : xs) { field.get(); } }\n"
     "  void anonymous() { new Decoy() { void local() {} void g() { local(); inherited(); } }; }\n"
     "  void reference() { Runnable2 r = this::callsNew; }\n"
     "  Store instanceInit = Store.open();\n"
     "}\n"},
    {"org/acme/app/Runnable2.java",
     "package org.acme.app;\npublic interface Runnable2 { void run(); }\n"},
    {"org/acme/app/Run2.java",
     "package org.acme.app;\npublic interface Run2 { int go(org.acme.other.Decoy d); }\n"},
    {"org/acme/app/Extern.java",
     "package org.acme.app;\n"
     "import static org.acme.util.Helpers.*;\n"
     "public class Extern extends com.external.Parent {\n"
     "  void star() { onDemand(); }\n"
     "  void own() { mine(); }\n"
     "  void mine() {}\n"
     "  void typeShaped() { org.acme.util.Helpers.imported(); }\n"
     "}\n"},
    {"org/acme/app/Closed.java",
     "package org.acme.app;\n"
     "import static org.acme.util.Helpers.*;\n"
     "public class Closed {\n  void star() { onDemand(); }\n}\n"},
};

static int oo_java_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_FILES, (int)(sizeof(JAVA_FILES) / sizeof(JAVA_FILES[0])), parallel,
                 "java")) {
        oo_close(&index);
        return 1;
    }
    /* An inherited member, by its declaring class, never by its leaf name. */
    OO_EXPECT("CALLS", "Main.callsInherited", "core.Base.inherited", 1);
    OO_EXPECT("CALLS", "Main.callsInherited", "other.Decoy.inherited", 0);
    /* Overloads of one owner are one node. */
    OO_EXPECT("CALLS", "Main.callsOverloads", "core.Base.overload", 1);
    /* A private member of a superclass is not inherited. */
    OO_EXPECT("CALLS", "Main.callsHidden", "core.Base.hidden", 0);
    OO_EXPECT("CALLS", "Main.callsImported", "util.Helpers.imported", 1);
    /* JLS 15.12.1: the class's own member hides the single-static import. */
    OO_EXPECT("CALLS", "Main.callsShadowed", "app.Main.shadowed", 1);
    OO_EXPECT("CALLS", "Main.callsShadowed", "util.Helpers.shadowed", 0);
    OO_EXPECT("CALLS", "Main.callsStatic", "util.Helpers.imported", 1);
    OO_EXPECT("CALLS", "Main.callsStatic", "core.Store.open", 1);
    /* An instance method cannot be reached through its class name. */
    OO_EXPECT("CALLS", "Main.callsInstanceThroughClass", "util.Helpers.instanceOnly", 0);
    OO_EXPECT("CALLS", "Main.callsField", "core.Store.get", 1);
    OO_EXPECT("CALLS", "Main.callsField", "core.Store.chain", 1);
    OO_EXPECT("CALLS", "Main.callsVar", "core.Store.get", 1);
    OO_EXPECT("CALLS", "Main.callsCast", "core.Store.get", 1);
    OO_EXPECT("CALLS", "Main.callsTernary", "core.Store.get", 1);
    /* The declared method, never the implementation. */
    OO_EXPECT("CALLS", "Main.callsInterface", "core.Api.run", 1);
    OO_EXPECT("CALLS", "Main.callsInterface", "core.Impl.run", 0);
    OO_EXPECT("CALLS", "Main.callsGeneric", "core.Box.get", 1);
    OO_EXPECT("CALLS", "Main.callsGeneric", "core.Store.get", 1);
    /* The selected constructor when declared, else the class stands for it. */
    OO_EXPECT("CALLS", "Main.callsNew", "core.Sized.Sized", 1);
    OO_EXPECT("CALLS", "Main.callsNew", "core.Plain", 1);
    OO_EXPECT("USAGE", "Main.readsField", "core.Store.value", 1);
    /* Every binder shadows the field. */
    OO_EXPECT("CALLS", "Main.shadowsField", "other.Decoy.get", 1);
    OO_EXPECT("CALLS", "Main.shadowsField", "core.Store.get", 0);
    OO_EXPECT("CALLS", "Main.lambdaShadows", "core.Store.get", 0);
    OO_EXPECT("CALLS", "Main.patternShadows", "other.Decoy.get", 1);
    OO_EXPECT("CALLS", "Main.patternShadows", "core.Store.get", 0);
    OO_EXPECT("CALLS", "Main.loopShadows", "other.Decoy.get", 1);
    OO_EXPECT("CALLS", "Main.loopShadows", "core.Store.get", 0);
    /* The anonymous body's own member hides the supertype's. Its methods have no
     * node, so their calls belong to the enclosing method, like a lambda's. */
    OO_EXPECT("CALLS", "Main.anonymous", "other.Decoy.local", 0);
    OO_EXPECT("CALLS", "Main.anonymous", "other.Decoy.inherited", 1);
    OO_EXPECT("CALL_REFERENCE", "Main.reference", "app.Main.callsNew", 1);
    OO_EXPECT("CALLS", "Api.twice", "core.Api.run", 1);
    /* A class-level initializer is sourced from its File node (#787). */
    OO_EXPECT("CALLS", "Main.java.__file__", "core.Store.open", 1);
    /* An unindexed supertype could declare the name, hiding the static import. */
    OO_EXPECT("CALLS", "Extern.star", "util.Helpers.onDemand", 0);
    OO_EXPECT("CALLS", "Closed.star", "util.Helpers.onDemand", 1);
    /* But it cannot take away a member the class itself declares. */
    OO_EXPECT("CALLS", "Extern.own", "app.Extern.mine", 1);
    OO_EXPECT("CALLS", "Extern.typeShaped", "util.Helpers.imported", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_java_static_selection) {
    ASSERT_EQ(oo_java_check(false), 0);
    PASS();
}

TEST(resolver_oo_java_static_selection_parallel) {
    ASSERT_EQ(oo_java_check(true), 0);
    PASS();
}

/* ── Kotlin ─────────────────────────────────────────────────────────── */

static const OOFile KOTLIN_FILES[] = {
    {"core/Store.kt",
     "package org.acme.core\n"
     "open class Store {\n"
     "    val name: String = \"\"\n"
     "    fun get(): Int = 1\n"
     "    fun chain(): Store = this\n"
     "    operator fun plus(other: Store): Store = this\n"
     "    operator fun get(index: Int): Store = this\n"
     "    infix fun with(other: Store): Store = this\n"
     "    fun memberOverExtension(): Int = 1\n"
     "    companion object {\n"
     "        fun make(): Store = Store()\n"
     "    }\n"
     "}\n"
     "class Child : Store()\n"
     "object Single {\n    fun one(): Int = 1\n}\n"
     "fun Store.ext(): Int = 2\n"
     "fun <T : Store> T.bounded(): T = this\n"
     "fun packageFunction(): Int = 4\n"
     "private fun fileOnly(): Int = 5\n"
     "val topProperty: Store = Store()\n"
     "val Store.extProp: Store get() = this\n"
     "fun <T> Iterable<T>.onIterable(): Int = 1\n"
     "fun Collection<Int>.onCollection(): Int = 1\n"
     "class Rows : Iterable<Store> {\n"
     "    override fun iterator(): Iterator<Store> = TODO()\n"
     "}\n"
     "fun <T> middleVararg(first: Int, vararg rest: Store, block: () -> Unit): Int = 1\n"
     "fun <TBOUND : Store> TBOUND.boundName(other: TBOUND): TBOUND = this\n"
     "class Holder {\n    var count: Int = 0\n}\n"
     "fun String.shout(): String = this\n"
     "fun Int.onlyInt(): Int = this\n"},
    {"core/Tx.kt",
     "package org.acme.core\n"
     "class Tx {\n    fun exec(): Int = 1\n}\n"
     "fun inTx(block: Tx.() -> Unit) {}\n"
     "fun withIt(block: (Store) -> Unit) {}\n"
     "fun Tx.txExt(): Int = 1\n"
     "fun Store.memberOverExtension(): Int = 3\n"
     "fun Tx.pick(): Int = 2\n"
     "class Outer {\n"
     "    fun pick(): Int = 1\n"
     "    fun runs() { inTx { pick() } }\n"
     "    fun own() { pick() }\n"
     "}\n"},
    {"core/Dsl.kt",
     "package org.acme.core\n"
     "open class Table {\n"
     "    fun integer(name: String): Column = Column()\n"
     "    fun Column.asKey(): Column = this\n"
     "    fun Column.both(): Int = 1\n"
     "}\n"
     "class Column {\n    fun nullable(): Column = this\n    fun use(): Int = 1\n}\n"
     "fun Column.both(): Int = 2\n"
     "fun <T : Table> T.clear(): Int { build { }; return 0 }\n"
     "fun build(block: () -> Unit): Int = 1\n"
     "object Users : Table() {\n    val id = integer(\"id\")\n"
     "    val alias = integer(\"a\").nullable()\n"
     "    fun keyed(c: Column) { c.asKey().use() }\n"
     "    fun inside(c: Column) { c.both() }\n"
     "}\n"
     "class NotTable {\n"
     "    fun outsideMember(c: Column) { c.asKey() }\n"
     "    fun outsideBoth(c: Column) { c.both() }\n"
     "}\n"},
    {"core/Decoys.kt",
     "package org.acme.decoy\n"
     "fun exec(): Int = 0\n"
     "fun packageFunction(): Int = 0\n"
     "fun helper(): Int = 0\n"
     "class Other {\n    fun get(): Int = 0\n    fun ext(): Int = 0\n}\n"
     "fun Other.onlyOther(): Int = 0\n"
     "val Other.otherProp: Int get() = 0\n"},
    {"app/Same.kt",
     "package org.acme.app\n"
     "fun sameFile(): Int = 1\n"
     "fun helper(): Int = 7\n"
     "fun assertEquals(a: Int, b: Int): Int = 8\n"},
    {"app/Main.kt",
     "package org.acme.app\n"
     "import org.acme.core.*\n"
     "import org.acme.decoy.Other\n"
     "class Main {\n"
     "    private val store = Store()\n"
     "    private val text: String? = null\n"
     "    private val holder = Holder()\n"
     "    fun writesMember() { holder.count = 2 }\n"
     "    fun writesOwn() { this.holder.count = 3 }\n"
     "    fun externalExtension() { text?.shout() }\n"
     "    fun externalWrongReceiver() { text?.onlyInt() }\n"
     "    fun extensionProperty(s: Store) { s.extProp.get() }\n"
     "    fun listIsIterable(l: List<Int>) { l.onIterable() }\n"
     "    fun projectIsIterable(r: Rows) { r.onIterable() }\n"
     "    fun setIsCollection(v: Set<Int>) { v.onCollection() }\n"
     "    fun stringIsNoCollection(t: String) { t.onCollection() }\n"
     "    fun storeIsNoIterable(s: Store) { s.onIterable() }\n"
     "    fun varargInTheMiddle(s: Store) { middleVararg<Int>(1, s, s) { } }\n"
     "    fun wrongExtensionProperty(s: Store) { s.otherProp }\n"
     "    fun objectValue() { val t = object : Store() { fun own() = 1 }; t.get(); t.ext() }\n"
     "    fun objectOwnMember() {\n"
     "        val t = object : Store() { fun memberOverExtension(): Int = 9 }\n"
     "        t.memberOverExtension()\n"
     "    }\n"
     "    fun bare() { sameFile() }\n"
     "    fun local() { val s = Store(); s.get() }\n"
     "    fun annotated(s: Store) { s.chain().get() }\n"
     "    fun field() { store.get(); this.store.get() }\n"
     "    fun safe(s: Store?) { s?.get(); s!!.get() }\n"
     "    fun cast(o: Any) { (o as Store).get() }\n"
     "    fun smart(o: Any) { if (o is Store) o.get() }\n"
     "    fun whenSmart(o: Any) { when (o) { is Store -> o.get(); else -> {} } }\n"
     "    fun extension(s: Store) { s.ext() }\n"
     "    fun extensionOnSubtype(c: Child) { c.ext() }\n"
     "    fun bound(c: Child) { c.bounded() }\n"
     "    fun memberWins(s: Store) { s.memberOverExtension() }\n"
     "    fun companion() { Store.make() }\n"
     "    fun singleton() { Single.one() }\n"
     "    fun constructs() { Store() }\n"
     "    fun inherited(c: Child) { c.get() }\n"
     "    fun receiverLambda() { inTx { exec() } }\n"
     "    fun receiverExtension() { inTx { txExt() } }\n"
     "    fun itLambda() { withIt { it.get() } }\n"
     "    fun stdlibLet(s: Store) { s.let { it.get() } }\n"
     "    fun stdlibApply(s: Store) { s.apply { get() } }\n"
     "    fun stdlibPlainLambda() { listOf(1).forEach { helper() } }\n"
     "    fun unknownLambda() { com.ext.mystery { helper() } }\n"
     "    fun operators(s: Store) { val t = s + s; val u = s[1]; val v = s with t }\n"
     "    fun inferred() { Users.id.use(); Users.alias.use() }\n"
     "    fun topLevelProperty() { topProperty.get() }\n"
     "    fun shadowed(other: Other) { val store = other; store.get() }\n"
     "    fun lambdaParam() { withIt { store -> store.get() } }\n"
     "    fun localFunction() { fun helper(): Int = 1; helper() }\n"
     "    fun wrongReceiver(o: Other) { o.onlyOther() }\n"
     "    fun notImportedFunction() { packageFunction() }\n"
     "    fun privateElsewhere() { fileOnly() }\n"
     "    fun anonymous() { val o = object : Store() { fun get2() = get() } }\n"
     "}\n"},
    {"app/Shadow.kt",
     "package org.acme.app\n"
     "import kotlin.test.assertEquals\n"
     "class Shadow {\n"
     "    fun explicitExternal() { assertEquals(1, 2) }\n"
     "}\n"},
    {"app/Base.kt",
     "package org.acme.app\n"
     "class Exceptional : com.external.Base() {\n"
     "    fun own() { mine() }\n"
     "    fun mine() {}\n"
     "    fun notProved() { helper() }\n"
     "}\n"},
};

static int oo_kotlin_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, KOTLIN_FILES, (int)(sizeof(KOTLIN_FILES) / sizeof(KOTLIN_FILES[0])),
                 parallel, "kt")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Main.bare", "Same.sameFile", 1);
    /* `val s = Store(); s.get()` is the form the receiver walk exists for. */
    OO_EXPECT("CALLS", "Main.local", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.local", "Decoys.Other.get", 0);
    OO_EXPECT("CALLS", "Main.annotated", "Store.Store.chain", 1);
    OO_EXPECT("CALLS", "Main.annotated", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.field", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.safe", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.cast", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.smart", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.whenSmart", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.extension", "Store.ext", 1);
    OO_EXPECT("CALLS", "Main.extension", "Decoys.Other.ext", 0);
    OO_EXPECT("CALLS", "Main.extensionOnSubtype", "Store.ext", 1);
    OO_EXPECT("CALLS", "Main.bound", "Store.bounded", 1);
    /* A member beats an extension of the same name. */
    OO_EXPECT("CALLS", "Main.memberWins", "Store.Store.memberOverExtension", 1);
    OO_EXPECT("CALLS", "Main.memberWins", "Tx.memberOverExtension", 0);
    OO_EXPECT("CALLS", "Main.companion", "Store.Store.Companion.make", 1);
    OO_EXPECT("CALLS", "Main.singleton", "Store.Single.one", 1);
    OO_EXPECT("CALLS", "Main.constructs", "Store.Store", 1);
    OO_EXPECT("CALLS", "Main.inherited", "Store.Store.get", 1);
    /* The lambda's declared receiver is an implicit receiver. */
    OO_EXPECT("CALLS", "Main.receiverLambda", "Tx.Tx.exec", 1);
    OO_EXPECT("CALLS", "Main.receiverLambda", "Decoys.exec", 0);
    OO_EXPECT("CALLS", "Main.receiverExtension", "Tx.txExt", 1);
    OO_EXPECT("CALLS", "Main.itLambda", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.stdlibLet", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.stdlibApply", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.stdlibPlainLambda", "Same.helper", 1);
    /* Nothing says this lambda lacks a receiver that declares the name. */
    OO_EXPECT("CALLS", "Main.unknownLambda", "Same.helper", 0);
    OO_EXPECT("CALLS", "Main.operators", "Store.Store.plus", 1);
    OO_EXPECT("CALLS", "Main.operators", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.operators", "Store.Store.with", 1);
    /* A property's type is read from its initializer's declared results. */
    OO_EXPECT("CALLS", "Main.inferred", "Dsl.Column.use", 1);
    OO_EXPECT("CALLS", "Main.topLevelProperty", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.shadowed", "Decoys.Other.get", 1);
    OO_EXPECT("CALLS", "Main.shadowed", "Store.Store.get", 0);
    OO_EXPECT("CALLS", "Main.lambdaParam", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.localFunction", "Same.helper", 0);
    /* An extension of another receiver type does not apply; here it does. */
    OO_EXPECT("CALLS", "Main.wrongReceiver", "Decoys.onlyOther", 0);
    /* Not imported, and the package has none. */
    OO_EXPECT("CALLS", "Main.notImportedFunction", "Decoys.packageFunction", 0);
    OO_EXPECT("CALLS", "Main.notImportedFunction", "Store.packageFunction", 1);
    /* private means this file. */
    OO_EXPECT("CALLS", "Main.privateElsewhere", "Store.fileOnly", 0);
    /* An explicit import outside the project shadows the package's namesake. */
    OO_EXPECT("CALLS", "Shadow.explicitExternal", "Same.assertEquals", 0);
    /* An extension on a value whose type lies outside the project. */
    OO_EXPECT("CALLS", "Main.externalExtension", "Store.shout", 1);
    OO_EXPECT("CALLS", "Main.externalWrongReceiver", "Store.onlyInt", 0);
    /* `holder.count = 2` writes count and reads holder. */
    OO_EXPECT("WRITES", "Main.writesMember", "Store.Holder.count", 1);
    OO_EXPECT("WRITES", "Main.writesMember", "Main.Main.holder", 0);
    OO_EXPECT("USAGE", "Main.writesMember", "Main.Main.holder", 1);
    OO_EXPECT("WRITES", "Main.writesOwn", "Store.Holder.count", 1);
    /* A type parameter's name is not a mention of its bound. */
    if (oo_emitted_spelled(&index, "TBOUND") != 0) {
        printf("  FAIL %s:%d: a type parameter's name was bound\n", __FILE__, __LINE__);
        failed++;
    }
    /* An extension's receiver takes the standard library's subtypes, and only those. */
    OO_EXPECT("CALLS", "Main.listIsIterable", "Store.onIterable", 1);
    OO_EXPECT("CALLS", "Main.projectIsIterable", "Store.onIterable", 1);
    OO_EXPECT("CALLS", "Main.setIsCollection", "Store.onCollection", 1);
    OO_EXPECT("CALLS", "Main.stringIsNoCollection", "Store.onCollection", 0);
    OO_EXPECT("CALLS", "Main.storeIsNoIterable", "Store.onIterable", 0);
    /* Past a vararg, position does not say which parameter an argument lands on. */
    OO_EXPECT("CALLS", "Main.varargInTheMiddle", "Store.middleVararg", 1);
    /* Extension properties: only the receiver they are declared on. */
    OO_EXPECT("USAGE", "Main.extensionProperty", "Store.extProp", 1);
    OO_EXPECT("CALLS", "Main.extensionProperty", "Store.Store.get", 1);
    OO_EXPECT("USAGE", "Main.wrongExtensionProperty", "Decoys.otherProp", 0);
    /* An object expression is a value of its supertype, minus its own members. */
    OO_EXPECT("CALLS", "Main.objectValue", "Store.Store.get", 1);
    OO_EXPECT("CALLS", "Main.objectValue", "Store.ext", 1);
    OO_EXPECT("CALLS", "Main.objectOwnMember", "Store.Store.memberOverExtension", 0);
    OO_EXPECT("CALLS", "Main.objectOwnMember", "Tx.memberOverExtension", 0);
    /* A member extension needs its dispatch receiver in scope, and it beats a
     * top-level extension of the same name. */
    OO_EXPECT("CALLS", "Users.keyed", "Dsl.Table.asKey", 1);
    OO_EXPECT("CALLS", "Users.keyed", "Dsl.Column.use", 1);
    OO_EXPECT("CALLS", "Users.inside", "Dsl.Table.both", 1);
    OO_EXPECT("CALLS", "Users.inside", "Dsl.both", 0);
    OO_EXPECT("CALLS", "NotTable.outsideMember", "Dsl.Table.asKey", 0);
    OO_EXPECT("CALLS", "NotTable.outsideBoth", "Dsl.both", 1);
    OO_EXPECT("CALLS", "NotTable.outsideBoth", "Dsl.Table.both", 0);
    /* Kotlin spec 11.3: an extension on the inner receiver beats the outer's member. */
    OO_EXPECT("CALLS", "Outer.runs", "Tx.pick", 1);
    OO_EXPECT("CALLS", "Outer.runs", "Tx.Outer.pick", 0);
    OO_EXPECT("CALLS", "Outer.own", "Tx.Outer.pick", 1);
    /* A receiver that is a type parameter has its bound's members, and no others. */
    OO_EXPECT("CALLS", "Dsl.clear", "Dsl.build", 1);
    /* Unknown supertype: its own members stay, free functions cannot be proved. */
    OO_EXPECT("CALLS", "Exceptional.own", "Base.Exceptional.mine", 1);
    OO_EXPECT("CALLS", "Exceptional.notProved", "Same.helper", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_kotlin_static_selection) {
    ASSERT_EQ(oo_kotlin_check(false), 0);
    PASS();
}

TEST(resolver_oo_kotlin_static_selection_parallel) {
    ASSERT_EQ(oo_kotlin_check(true), 0);
    PASS();
}

/* ── C# ─────────────────────────────────────────────────────────────── */

static const OOFile CSHARP_FILES[] = {
    {"Core/Store.cs",
     "namespace Acme.Core {\n"
     "    public class Store {\n"
     "        public int Value;\n"
     "        public int Size { get; set; }\n"
     "        public Store Chain() { return this; }\n"
     "        public int Get() { return 1; }\n"
     "        public void Put(int v) { }\n"
     "        public static Store Open() { return new Store(); }\n"
     "    }\n"
     "    public class Store<T> {\n"
     "        public T Only() { return default; }\n"
     "    }\n"
     "    public static class StoreExtensions {\n"
     "        public static int Ext(this Store s) { return 1; }\n"
     "        public static int Get(this Store s, int ignored) { return 2; }\n"
     "    }\n"
     "    public interface IApi { void Run(); }\n"
     "    public class Impl : IApi { public void Run() { } }\n"
     "    public class Parent { public void Inherited() { } }\n"
     "}\n"},
    {"Core/Partial.A.cs",
     "namespace Acme.Core {\n"
     "    public partial class Writer {\n"
     "        public void First() { Second(); }\n"
     "    }\n"
     "}\n"},
    {"Core/Partial.B.cs",
     "namespace Acme.Core {\n"
     "    public partial class Writer {\n"
     "        private void Second() { }\n"
     "    }\n"
     "}\n"},
    {"Other/Decoy.cs",
     "namespace Acme.Other {\n"
     "    public class Decoy {\n"
     "        public int Get() { return 0; }\n"
     "        public void Run() { }\n"
     "    }\n"
     "    public static class DecoyExtensions {\n"
     "        public static int NotInScope(this Acme.Core.Store s) { return 0; }\n"
     "    }\n"
     "}\n"},
    {"Util/Helpers.cs",
     "namespace Acme.Util {\n"
     "    public static class Helpers {\n"
     "        public static int Imported() { return 1; }\n"
     "    }\n"
     "}\n"},
    {"App/Main.cs",
     "using Acme.Core;\n"
     "using static Acme.Util.Helpers;\n"
     "using OtherAlias = Acme.Other.Decoy;\n"
     "namespace Acme.App {\n"
     "    public class Sibling { public int Near() { return 1; } }\n"
     "    public class Main : Parent {\n"
     "        private readonly Store _store = new Store();\n"
     "        private Store _target = new();\n"
     "        private System.Action Run;\n"
     "        public void Declared(Store s) { s.Get(); }\n"
     "        public void Var() { var s = new Store(); s.Chain().Get(); }\n"
     "        public void Field() { _store.Put(1); this._store.Put(2); }\n"
     "        public void Prop(Store s) { s.Size = 1; }\n"
     "        public void Inherits() { Inherited(); base.Inherited(); }\n"
     "        public void Conditional(Store s) { s?.Get(); s!.Get(); }\n"
     "        public void Cast(object o) { ((Store)o).Get(); (o as Store)?.Get(); }\n"
     "        public void Pattern(object o) { if (o is Store st) { st.Get(); } }\n"
     "        public void Sibling2() { new Sibling().Near(); }\n"
     "        public void Alias() { new OtherAlias().Get(); }\n"
     "        public void UsingStatic() { Imported(); }\n"
     "        public void Interface(IApi api) { api.Run(); }\n"
     "        public void Extension(Store s) { s.Ext(); }\n"
     "        public void InstanceBeatsExtension(Store s) { s.Get(); }\n"
     "        public void ExtensionByArity(Store s) { s.Get(1); }\n"
     "        public void ExtensionNotInScope(Store s) { s.NotInScope(); }\n"
     "        public void Generic(Store<int> g) { g.Only(); }\n"
     "        public void Shadow(OtherAlias _store) { _store.Get(); }\n"
     "        public void LambdaShadow() { System.Func<OtherAlias, int> f = _store => _store.Get(); }\n"
     "        public void OutShadow() { Make(out OtherAlias _store); _store.Get(); }\n"
     "        public void Make(out OtherAlias d) { d = null; }\n"
     "        public void DelegateMember() { Run(); }\n"
     "        public void LocalFunction() { void Imported() { } Imported(); }\n"
     "        public void Constructs() { new Store(); }\n"
     "    }\n"
     "    public class External : Com.External.Thing {\n"
     "        public void NotProved() { Imported(); }\n"
     "        public void Own() { Mine(); }\n"
     "        void Mine() { }\n"
     "    }\n"
     "}\n"},
};

static int oo_csharp_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CSHARP_FILES, (int)(sizeof(CSHARP_FILES) / sizeof(CSHARP_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Main.Declared", "Store.Store.Get", 1);
    OO_EXPECT("CALLS", "Main.Declared", "Decoy.Decoy.Get", 0);
    OO_EXPECT("CALLS", "Main.Var", "Store.Store.Chain", 1);
    OO_EXPECT("CALLS", "Main.Var", "Store.Store.Get", 1);
    /* `_store.Put(1)` with a declared field type. */
    OO_EXPECT("CALLS", "Main.Field", "Store.Store.Put", 1);
    OO_EXPECT("WRITES", "Main.Prop", "Store.Store.Size", 1);
    OO_EXPECT("CALLS", "Main.Inherits", "Store.Parent.Inherited", 1);
    OO_EXPECT("CALLS", "Main.Conditional", "Store.Store.Get", 1);
    OO_EXPECT("CALLS", "Main.Cast", "Store.Store.Get", 1);
    OO_EXPECT("CALLS", "Main.Pattern", "Store.Store.Get", 1);
    /* A partial class's other file is still this type. */
    OO_EXPECT("CALLS", "Writer.First", "Partial.B.Writer.Second", 1);
    /* The enclosing namespace sees its own types with no using. */
    OO_EXPECT("CALLS", "Main.Sibling2", "Main.Sibling.Near", 1);
    OO_EXPECT("CALLS", "Main.Alias", "Decoy.Decoy.Get", 1);
    OO_EXPECT("CALLS", "Main.UsingStatic", "Helpers.Helpers.Imported", 1);
    OO_EXPECT("CALLS", "Main.Interface", "Store.IApi.Run", 1);
    OO_EXPECT("CALLS", "Main.Interface", "Store.Impl.Run", 0);
    OO_EXPECT("CALLS", "Main.Extension", "Store.StoreExtensions.Ext", 1);
    /* An applicable instance member beats an extension method. */
    OO_EXPECT("CALLS", "Main.InstanceBeatsExtension", "Store.Store.Get", 1);
    OO_EXPECT("CALLS", "Main.InstanceBeatsExtension", "Store.StoreExtensions.Get", 0);
    /* No instance overload takes this argument count; the extension does. */
    OO_EXPECT("CALLS", "Main.ExtensionByArity", "Store.StoreExtensions.Get", 1);
    OO_EXPECT("CALLS", "Main.ExtensionNotInScope", "Decoy.DecoyExtensions.NotInScope", 0);
    /* `Store<T>` is not `Store`. */
    OO_EXPECT("CALLS", "Main.Generic", "Store.Store.Only", 1);
    /* Every binder shadows the field. */
    OO_EXPECT("CALLS", "Main.Shadow", "Decoy.Decoy.Get", 1);
    OO_EXPECT("CALLS", "Main.Shadow", "Store.Store.Get", 0);
    OO_EXPECT("CALLS", "Main.LambdaShadow", "Store.Store.Get", 0);
    OO_EXPECT("CALLS", "Main.OutShadow", "Decoy.Decoy.Get", 1);
    OO_EXPECT("CALLS", "Main.OutShadow", "Store.Store.Get", 0);
    /* A delegate-typed member answers the name; `using static` is not reached. */
    OO_EXPECT("CALLS", "Main.DelegateMember", "Decoy.Decoy.Run", 0);
    OO_EXPECT("CALLS", "Main.LocalFunction", "Helpers.Helpers.Imported", 0);
    OO_EXPECT("CALLS", "Main.Constructs", "Store.Store", 1);
    /* Unknown supertype: its own members stay, `using static` cannot be proved. */
    OO_EXPECT("CALLS", "External.NotProved", "Helpers.Helpers.Imported", 0);
    OO_EXPECT("CALLS", "External.Own", "Main.External.Mine", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_static_selection) {
    ASSERT_EQ(oo_csharp_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_static_selection_parallel) {
    ASSERT_EQ(oo_csharp_check(true), 0);
    PASS();
}

/* One Rust crate with no src/main.rs fails the import context. That says
 * nothing about Java's namespaces (found on Elasticsearch's libs). */
/* A work budget must not decide graph content: the last site of a very large file
 * binds exactly like the first. Real case: a 4,000-line form-binding test in Play lost
 * thousands of sites to a per-file proof budget, in no particular order of importance. */
/* A long import list is searched another way than a short one; the answer is the same. */
static int oo_long_imports_binds(const char *extension, const char *unused_import,
                                 const char *real_import, const char *head, const char *main_body,
                                 const char *service_path, const char *service, const char *source,
                                 const char *target) {
    enum { UNUSED = 24 };
    char text[4096];
    size_t at = (size_t)snprintf(text, sizeof(text), "%s", head);
    for (int i = 0; i < UNUSED; i++)
        at += (size_t)snprintf(text + at, sizeof(text) - at, unused_import, i, i);
    at += (size_t)snprintf(text + at, sizeof(text) - at, "%s%s", real_import, main_body);
    char main_path[64];
    snprintf(main_path, sizeof(main_path), "client/Main.%s", extension);
    OOFile files[] = {{main_path, text}, {service_path, service}};
    OOIndex index;
    bool opened = oo_open(&index, files, 2, false, extension);
    int edges = opened ? oo_edges(&index, "CALLS", source, target) : -1;
    oo_close(&index);
    return edges;
}
TEST(resolver_oo_long_import_list_still_binds) {
    ASSERT_EQ(oo_long_imports_binds("java", "import other.Unused%d;\n", "import api.Service;\n",
                                    "package client;\n",
                                    "class Main { void run(Service s) { s.call(); } }\n",
                                    "api/Service.java",
                                    "package api;\npublic class Service { public void call() {} }\n",
                                    "Main.run", "Service.call"),
              1);
    ASSERT_EQ(oo_long_imports_binds("kt", "import other.Unused%d\n", "import api.Service\n",
                                    "package client\n",
                                    "class Main { fun run(s: Service) { s.call() } }\n",
                                    "api/Service.kt",
                                    "package api\nclass Service { fun call() {} }\n", "Main.run",
                                    "Service.call"),
              1);
    ASSERT_EQ(oo_long_imports_binds(
                  "cs", "using Unused%d = Other.Unused%d;\n", "using S = Api.Service;\n", "",
                  "namespace Client { class Main { void Run(S s) { s.Call(); } } }\n", "api/Service.cs",
                  "namespace Api { public class Service { public void Call() {} } }\n", "Main.Run",
                  "Service.Call"),
              1);
    PASS();
}

TEST(resolver_oo_large_file_binds_its_last_site) {
    enum { SITES = 40000 };
    const char *head = "package big;\nclass Big {\n  void target() {}\n  void many() {\n";
    const char *tail = "  }\n  void last() { target(); }\n}\n";
    size_t capacity = strlen(head) + strlen(tail) + (size_t)SITES * 14 + 1;
    char *text = malloc(capacity);
    ASSERT_NOT_NULL(text);
    char *at = text + snprintf(text, capacity, "%s", head);
    for (int i = 0; i < SITES; i++) {
        memcpy(at, "    target();\n", 14);
        at += 14;
    }
    snprintf(at, capacity - (size_t)(at - text), "%s", tail);
    OOFile files[] = {{"big/Big.java", text}};
    OOIndex index;
    bool opened = oo_open(&index, files, 1, false, "java");
    int failed = 0;
    if (opened) {
        OO_EXPECT("CALLS", "Big.many", "Big.target", 1);
        OO_EXPECT("CALLS", "Big.last", "Big.target", 1);
    }
    oo_close(&index);
    free(text);
    ASSERT_TRUE(opened);
    ASSERT_EQ(failed, 0);
    PASS();
}

/* The grammar cannot read `ref partial struct`, or an `#if` that splits a construct.
 * Whole files came back as one error, and with them every type they declared. */
static const OOFile REPAIR_FILES[] = {
    {"Repair/Reader.cs",
     "namespace Acme.Repair {\n"
     "    public ref partial struct Reader {\n"
     "        public int Next() { return Peek(); }\n"
     "        private int Peek() { return 1; }\n"
     "    }\n"
     "}\n"},
    {"Repair/Split.cs",
     "namespace Acme.Repair {\n"
     "    public class Base { public int Inherited() { return 1; } }\n"
     "    public class Split : Base {\n"
     "        public int First() { return Second(); }\n"
     "        public int Second() {\n"
     "#if DEBUG\n"
     "            if (First() > 0) {\n"
     "#else\n"
     "            if (First() > 1) {\n"
     "#endif\n"
     "                return Inherited();\n"
     "            }\n"
     "            return 0;\n"
     "        }\n"
     "    }\n"
     "}\n"},
    {"Repair/Overloads.Int.cs",
     "namespace Acme.Repair {\n"
     "    public partial class Writer {\n"
     "        void Put(int value) { Put(value, 1); }\n"
     "        void Put(int value, int width) { }\n"
     "        void Wide(long value) { }\n"
     "        void Twin(int value) { }\n"
     "    }\n"
     "}\n"},
    {"Repair/Overloads.Text.cs",
     "namespace Acme.Repair {\n"
     "    public partial class Writer {\n"
     "        void Put(string value) { }\n"
     "        void PutInt(int n) { Put(n); }\n"
     "        void PutText(string t) { Put(t); }\n"
     "        void PutUnknown(Outside o) { Put(o); }\n"
     "        void Wide(int value) { }\n"
     "        void Twin(long value) { }\n"
     "        void UsesWide(int n) { Wide(n); }\n"
     "        void UsesTwin(long n) { Twin(n); }\n"
     "        void UsesLiteral() { Wide(1L); }\n"
     "        void Named() { var s = nameof(PutInt); }\n"
     "    }\n"
     "}\n"},
};

static int oo_repair_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, REPAIR_FILES, (int)(sizeof(REPAIR_FILES) / sizeof(REPAIR_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Reader.Next", "Reader.Peek", 1);
    OO_EXPECT("CALLS", "Split.First", "Split.Second", 1);
    OO_EXPECT("CALLS", "Split.Second", "Split.First", 1);
    OO_EXPECT("CALLS", "Split.Second", "Base.Inherited", 1);
    /* Overloads in two files of one partial class are two nodes. An exact match of
     * every argument is the best conversion in all three languages. */
    OO_EXPECT("CALLS", "Writer.PutInt", "Overloads.Int.Writer.Put", 1);
    OO_EXPECT("CALLS", "Writer.PutInt", "Overloads.Text.Writer.Put", 0);
    OO_EXPECT("CALLS", "Writer.PutText", "Overloads.Text.Writer.Put", 1);
    OO_EXPECT("CALLS", "Writer.PutText", "Overloads.Int.Writer.Put", 0);
    OO_EXPECT("CALLS", "Writer.UsesWide", "Overloads.Text.Writer.Wide", 1);
    OO_EXPECT("CALLS", "Writer.UsesWide", "Overloads.Int.Writer.Wide", 0);
    OO_EXPECT("CALLS", "Writer.UsesTwin", "Overloads.Text.Writer.Twin", 1);
    OO_EXPECT("CALLS", "Writer.UsesTwin", "Overloads.Int.Writer.Twin", 0);
    OO_EXPECT("CALLS", "Writer.UsesLiteral", "Overloads.Int.Writer.Wide", 1);
    OO_EXPECT("CALLS", "Writer.UsesLiteral", "Overloads.Text.Writer.Wide", 0);
    /* An argument of unknown type leaves them competing. */
    OO_EXPECT("CALLS", "Writer.PutUnknown", "Overloads.Int.Writer.Put", 0);
    OO_EXPECT("CALLS", "Writer.PutUnknown", "Overloads.Text.Writer.Put", 0);
    /* nameof names a symbol. It neither calls it nor takes it as a callable. */
    OO_EXPECT("CALL_REFERENCE", "Writer.Named", "Overloads.Text.Writer.PutInt", 0);
    OO_EXPECT("CALLS", "Writer.Named", "Overloads.Text.Writer.PutInt", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_repaired_parse_and_exact_overloads) {
    ASSERT_EQ(oo_repair_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_repaired_parse_and_exact_overloads_parallel) {
    ASSERT_EQ(oo_repair_check(true), 0);
    PASS();
}

/* A .NET reference assembly: <library>/ref is a project of its own beside <library>/src, and
 * declares one assembly's public surface with `throw null` bodies (dotnet/runtime). A caller
 * means the type, whichever of the two the compiler read. The decoys are a `ref` directory
 * that is no project, and a `ref` project with nothing beside it: both are plain duplicates. */
static const OOFile REFERENCE_FILES[] = {
    {"src/libraries/System.Demo/ref/System.Demo.csproj",
     "<Project Sdk=\"Microsoft.NET.Sdk\"><ItemGroup>"
     "<Compile Include=\"System.Demo.cs\" /></ItemGroup></Project>\n"},
    {"src/libraries/System.Demo/ref/System.Demo.cs",
     "namespace System.Demo {\n"
     "    public partial class Widget {\n"
     "        public Widget() { }\n"
     "        public int Count { get { throw null; } }\n"
     "        public void Add(int value) { }\n"
     "        public static System.Demo.Widget Create() { throw null; }\n"
     "        public partial class Nested { public void Inner() { } }\n"
     "    }\n"
     "    public static partial class WidgetExtensions {\n"
     "        public static void Twist(this System.Demo.Widget widget) { }\n"
     "    }\n"
     "    public partial class OnlyInReference { public void Solo() { } }\n"
     "}\n"},
    {"src/libraries/System.Demo/src/System.Demo.csproj",
     "<Project Sdk=\"Microsoft.NET.Sdk\"><ItemGroup>"
     "<Compile Include=\"System/Demo/Widget.cs\" /></ItemGroup></Project>\n"},
    {"src/libraries/System.Demo/src/System/Demo/Widget.cs",
     "namespace System.Demo {\n"
     "    public class Widget {\n"
     "        public Widget() { }\n"
     "        public int Count => 0;\n"
     "        public void Add(int value) { }\n"
     "        public static Widget Create() { return new Widget(); }\n"
     "        public class Nested { public void Inner() { } }\n"
     "    }\n"
     "    public static class WidgetExtensions {\n"
     "        public static void Twist(this Widget widget) { }\n"
     "    }\n"
     "}\n"},
    {"src/libraries/System.Demo/tests/WidgetTests.cs",
     "using System.Demo;\n"
     "namespace Acme.Tests {\n"
     "    public class WidgetTests {\n"
     "        void Constructs() { var made = new Widget(); }\n"
     "        void Calls(Widget widget) { widget.Add(1); }\n"
     "        void Static() { Widget.Create(); }\n"
     "        void Reads(Widget widget) { var count = widget.Count; }\n"
     "        void Extension(Widget widget) { widget.Twist(); }\n"
     "        void Inner(Widget.Nested nested) { nested.Inner(); }\n"
     "        void Solo(OnlyInReference only) { only.Solo(); }\n"
     "    }\n"
     "}\n"},
    /* A directory that merely has this name. */
    {"plain/ref/Thing.cs", "namespace Acme.Plain { public class Thing { public void Go() { } } }\n"},
    {"plain/src/Thing.cs", "namespace Acme.Plain { public class Thing { public void Go() { } } }\n"},
    {"plain/Use.cs",
     "using Acme.Plain;\n"
     "namespace Acme.Use { public class Use { void Plain(Thing thing) { thing.Go(); } } }\n"},
    /* A project called ref with no implementation tree beside it. */
    {"lonely/ref/Lonely.csproj",
     "<Project Sdk=\"Microsoft.NET.Sdk\"><ItemGroup /></Project>\n"},
    {"lonely/ref/Thing.cs",
     "namespace Acme.Lonely { public class Thing { public void Go() { } } }\n"},
    {"other/Thing.cs", "namespace Acme.Lonely { public class Thing { public void Go() { } } }\n"},
    {"lonely/Use.cs",
     "using Acme.Lonely;\n"
     "namespace Acme.Use { public class UseLonely { void Lonely(Thing thing) { thing.Go(); } } }\n"},
};

static int oo_reference_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, REFERENCE_FILES,
                 (int)(sizeof(REFERENCE_FILES) / sizeof(REFERENCE_FILES[0])), parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "WidgetTests.Calls", "src.System.Demo.Widget.Widget.Add", 1);
    OO_EXPECT("CALLS", "WidgetTests.Calls", "ref.System.Demo.Widget.Add", 0);
    OO_EXPECT("CALLS", "WidgetTests.Static", "src.System.Demo.Widget.Widget.Create", 1);
    OO_EXPECT("CALLS", "WidgetTests.Static", "ref.System.Demo.Widget.Create", 0);
    OO_EXPECT("CALLS", "WidgetTests.Constructs", "src.System.Demo.Widget.Widget.Widget", 1);
    OO_EXPECT("CALLS", "WidgetTests.Constructs", "ref.System.Demo.Widget.Widget", 0);
    OO_EXPECT("CALLS", "WidgetTests.Extension", "src.System.Demo.Widget.WidgetExtensions.Twist", 1);
    OO_EXPECT("CALLS", "WidgetTests.Extension", "ref.System.Demo.WidgetExtensions.Twist", 0);
    OO_EXPECT("CALLS", "WidgetTests.Inner", "src.System.Demo.Widget.Widget.Nested.Inner", 1);
    OO_EXPECT("USAGE", "WidgetTests.Reads", "src.System.Demo.Widget.Widget.Count", 1);
    OO_EXPECT("USAGE", "WidgetTests.Reads", "ref.System.Demo.Widget.Count", 0);
    /* The sole declaration of its name is the target, stub or not. */
    OO_EXPECT("CALLS", "WidgetTests.Solo", "ref.System.Demo.OnlyInReference.Solo", 1);
    /* Neither of these is a reference assembly, so nothing tells the two apart. */
    OO_EXPECT("CALLS", "Use.Plain", "ref.Thing.Thing.Go", 0);
    OO_EXPECT("CALLS", "Use.Plain", "src.Thing.Thing.Go", 0);
    OO_EXPECT("CALLS", "UseLonely.Lonely", "lonely.ref.Thing.Thing.Go", 0);
    OO_EXPECT("CALLS", "UseLonely.Lonely", "other.Thing.Thing.Go", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_reference_assembly_stands_behind) {
    ASSERT_EQ(oo_reference_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_reference_assembly_stands_behind_parallel) {
    ASSERT_EQ(oo_reference_check(true), 0);
    PASS();
}

/* `suspend` is a type modifier in front of the function type, as on every lambda parameter of
 * Exposed's coroutine API: `statement: suspend R2dbcTransaction.(row: Row) -> Unit`. The lambda
 * still has that receiver and those parameters. */
static const OOFile SUSPEND_FILES[] = {
    {"acme/Scopes.kt",
     "package acme\n"
     "class Tx { fun commit() {} }\n"
     "class Other { fun commit() {} }\n"
     "class Row { fun cell() {} }\n"
     "fun Tx.helper() {}\n"
     "fun Other.helper() {}\n"
     "fun withSuspend(statement: suspend Tx.(row: Row) -> Unit) {}\n"
     "fun withBare(statement: suspend (Row) -> Unit) {}\n"
     "fun withDefaulted(limit: Int = 0, statement: suspend Tx.(Row) -> Unit) {}\n"
     "class Annotated(val handler: @Marker Row)\n"
     "annotation class Marker\n"},
    {"acme/Main.kt",
     "package acme\n"
     "class Main {\n"
     "    fun commit() {}\n"
     "    fun viaReceiver() { withSuspend { row -> commit() } }\n"
     "    fun viaParameter() { withSuspend { row -> row.cell() } }\n"
     "    fun viaExtension() { withSuspend { row -> helper() } }\n"
     "    fun viaIt() { withBare { it.cell() } }\n"
     "    fun viaDefaulted() { withDefaulted { it.cell() } }\n"
     "    fun viaModifiedProperty(a: Annotated) { a.handler.cell() }\n"
     "    fun plainLambdaHasNoReceiver() { withBare { commit() } }\n"
     "}\n"},
};

static int oo_suspend_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, SUSPEND_FILES, (int)(sizeof(SUSPEND_FILES) / sizeof(SUSPEND_FILES[0])),
                 parallel, "kt")) {
        oo_close(&index);
        return 1;
    }
    /* The function type's receiver is the innermost one, ahead of the enclosing class. */
    OO_EXPECT("CALLS", "Main.viaReceiver", "Scopes.Tx.commit", 1);
    OO_EXPECT("CALLS", "Main.viaReceiver", "Main.Main.commit", 0);
    OO_EXPECT("CALLS", "Main.viaReceiver", "Scopes.Other.commit", 0);
    OO_EXPECT("CALLS", "Main.viaParameter", "Scopes.Row.cell", 1);
    /* An extension applies to the receiver the lambda has, not to a namesake's. */
    OO_EXPECT("CALLS", "Main.viaExtension", "Scopes.helper", 1);
    OO_EXPECT("CALLS", "Main.viaIt", "Scopes.Row.cell", 1);
    OO_EXPECT("CALLS", "Main.viaDefaulted", "Scopes.Row.cell", 1);
    OO_EXPECT("CALLS", "Main.viaModifiedProperty", "Scopes.Row.cell", 1);
    /* No receiver in the function type: the enclosing class answers. */
    OO_EXPECT("CALLS", "Main.plainLambdaHasNoReceiver", "Main.Main.commit", 1);
    OO_EXPECT("CALLS", "Main.plainLambdaHasNoReceiver", "Scopes.Tx.commit", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_kotlin_suspend_function_type) {
    ASSERT_EQ(oo_suspend_check(false), 0);
    PASS();
}

TEST(resolver_oo_kotlin_suspend_function_type_parallel) {
    ASSERT_EQ(oo_suspend_check(true), 0);
    PASS();
}

/* A type mention at class level and a generic head, in Java and C#. Forms from Elasticsearch
 * (`private final RemoteConnectionManager m;`, `ActionListener<Void> listener`) and
 * System.Text.Json. A type parameter of the same name is not the class. */
static const OOFile JAVA_TYPE_FILES[] = {
    {"acme/fields/Main.java",
     "package acme.fields;\n"
     "import acme.other.Imported;\n"
     "import java.util.List;\n"
     "public class Main {\n"
     "    private final Service service = null;\n"
     "    private Holder<Service> generic;\n"
     "    static Imported imported;\n"
     "    Holder<Service> signature(Holder<Service> parameter, List<Service> external) {\n"
     "        Holder<Service> local = parameter;\n"
     "        return local;\n"
     "    }\n"
     "}\n"
     "class Service {}\n"
     "class Holder<T> {}\n"
     "class Box<Service> {\n"
     "    Service held;\n"
     "    Service give(Service value) { return value; }\n"
     "}\n"},
    {"acme/other/Imported.java", "package acme.other;\npublic class Imported {}\n"},
    {"acme/other/Service.java", "package acme.other;\npublic class Service {}\n"},
    {"acme/plain/Bare.java",
     "package acme.plain;\n"
     "class Bare {\n"
     "    private final OnlyHere here = null;\n"
     "}\n"
     "class OnlyHere {}\n"},
};

static int oo_java_type_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_TYPE_FILES,
                 (int)(sizeof(JAVA_TYPE_FILES) / sizeof(JAVA_TYPE_FILES[0])), parallel, "java")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("USAGE", "Bare.java.__file__", "acme.plain.OnlyHere", 1);
    OO_EXPECT("USAGE", "Main.java.__file__", "acme.fields.Holder", 1);
    OO_EXPECT("USAGE", "Main.java.__file__", "acme.other.Imported", 1);
    OO_EXPECT("USAGE", "Main.java.__file__", "acme.other.Service", 0);
    OO_EXPECT("USES_TYPE", "Main.signature", "acme.fields.Holder", 1);
    OO_EXPECT("USES_TYPE", "Main.signature", "acme.fields.Service", 1);
    OO_EXPECT("USAGE", "Main.signature", "acme.fields.Holder", 1);
    /* `Service` inside Box is Box's own type parameter. */
    OO_EXPECT("USES_TYPE", "Box.give", "acme.fields.Service", 0);
    OO_EXPECT("USAGE", "Box.give", "acme.fields.Service", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_java_class_level_and_generic_type_mentions) {
    ASSERT_EQ(oo_java_type_check(false), 0);
    PASS();
}

TEST(resolver_oo_java_class_level_and_generic_type_mentions_parallel) {
    ASSERT_EQ(oo_java_type_check(true), 0);
    PASS();
}

static const OOFile CS_TYPE_FILES[] = {
    {"Gen/Main.cs",
     "using Acme.Other;\n"
     "namespace Acme.Gen {\n"
     "    public class Service {}\n"
     "    public class Holder<T> {}\n"
     "    public class Main {\n"
     "        Holder<Service> Signature(Holder<Service> parameter) {\n"
     "            Holder<Service> local = parameter;\n"
     "            return local;\n"
     "        }\n"
     "        System.Collections.Generic.List<Service> External() { return null; }\n"
     "        Imported<int> Elsewhere() { return null; }\n"
     "    }\n"
     "    public class Box<Service> {\n"
     "        Service Give(Service value) { return value; }\n"
     "    }\n"
     "    public class Twin {\n"
     "        Holder Arity() { return null; }\n"
     "    }\n"
     "}\n"},
    {"Gen/Imported.cs", "namespace Acme.Other { public class Imported<T> {} }\n"},
    {"Gen/Plain.cs", "namespace Acme.Gen { public class Holder {} }\n"},
};

static int oo_csharp_type_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_TYPE_FILES, (int)(sizeof(CS_TYPE_FILES) / sizeof(CS_TYPE_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("USES_TYPE", "Main.Signature", "Main.Holder", 1);
    OO_EXPECT("USAGE", "Main.Signature", "Main.Holder", 1);
    OO_EXPECT("USAGE", "Main.Signature", "Main.Service", 1);
    /* `Holder<T>` and `Holder` are two types: the arity selects. */
    OO_EXPECT("USES_TYPE", "Main.Signature", "Plain.Holder", 0);
    OO_EXPECT("USES_TYPE", "Twin.Arity", "Plain.Holder", 1);
    OO_EXPECT("USES_TYPE", "Twin.Arity", "Main.Holder", 0);
    OO_EXPECT("USES_TYPE", "Main.Elsewhere", "Imported.Imported", 1);
    OO_EXPECT("USES_TYPE", "Box.Give", "Main.Service", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_generic_type_mentions) {
    ASSERT_EQ(oo_csharp_type_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_generic_type_mentions_parallel) {
    ASSERT_EQ(oo_csharp_type_check(true), 0);
    PASS();
}

/* A lambda's parameters take their types from the declaration the call selects: a project functional
 * interface with one abstract method, or a JDK one the declaring file imports. Forms from Elasticsearch:
 * `checker.register("x", b -> b.field(...))`, `listener.delegateFailure((l, r) -> l.onResponse(r))`,
 * `map.forEach((k, v) -> v.go())`, `entry.getValue().go()`. */
static const OOFile JAVA_LAMBDA_FILES[] = {
    {"acme/lam/Types.java",
     "package acme.lam;\n"
     "import java.util.List;\n"
     "import java.util.Map;\n"
     "import java.util.function.Consumer;\n"
     "import java.util.function.BiConsumer;\n"
     "import acme.fake.Handler;\n"
     "public class Types {\n"
     "    public interface Check<T, E extends Exception> {\n"
     "        void accept(T value) throws E;\n"
     "        default Check<T, E> then() { return this; }\n"
     "        boolean equals(Object other);\n"
     "    }\n"
     "    public interface TwoMethods<T> { void one(T a); void two(T a); }\n"
     "    public interface Inherited<T> extends Check<T, Exception> {}\n"
     "    public interface Replaced<T> extends Check<T, Exception> {\n"
     "        default void accept(T value) {}\n"
     "        void other(Doc doc);\n"
     "    }\n"
     "    public static abstract class NotFunctional<T> { abstract void run(T a); }\n"
     "    public static class Doc { public void go() {} public Doc next() { return this; } }\n"
     "    public static class Other { public void go() {} }\n"
     "    public void check(Check<Doc, Exception> c) {}\n"
     "    public void jdk(Consumer<Doc> c) {}\n"
     "    public void jdkSuper(Consumer<? super Doc> c) {}\n"
     "    public void jdkTwo(BiConsumer<Doc, Other> c) {}\n"
     "    public void two(TwoMethods<Doc> t) {}\n"
     "    public void abstractClass(NotFunctional<Doc> t) {}\n"
     "    public void inherited(Inherited<Doc> i) {}\n"
     "    public void replaced(Replaced<Other> r) {}\n"
     "    public void fake(Handler<Doc> h) {}\n"
     "    public <U> void generic(Consumer<U> c) {}\n"
     "    public <U> void each(List<U> items, Consumer<U> c) {}\n"
     "    public <U> U first(List<U> items) { return null; }\n"
     "    public <U> void pick(U one, U two, Consumer<U> c) {}\n"
     "    public Consumer<Doc> makes() { return d -> d.go(); }\n"
     "    public Consumer<Doc> kept = d -> d.next();\n"
     "    public void overload(Consumer<Doc> c) {}\n"
     "    public void overload(String notAFunction) {}\n"
     "    public void varargs(Consumer<Doc>... all) {}\n"
     "    public Types(Consumer<Doc> c) {}\n"
     "    public Types() {}\n"
     "}\n"},
    {"acme/lam/Use.java",
     "package acme.lam;\n"
     "import java.util.List;\n"
     "import java.util.Map;\n"
     "import java.util.Optional;\n"
     "import acme.lam.Types.Doc;\n"
     "import acme.lam.Types.Other;\n"
     "public class Use {\n"
     "    Types t;\n"
     "    List<Doc> docs;\n"
     "    Map<String, Doc> byName;\n"
     "    void viaCheck() { t.check(d -> d.go()); }\n"
     "    void viaBlock() { t.check(d -> { d.next().go(); }); }\n"
     "    void viaJdk() { t.jdk(d -> d.go()); }\n"
     "    void viaJdkSuper() { t.jdkSuper(d -> d.go()); }\n"
     "    void viaJdkTwo() { t.jdkTwo((d, o) -> o.go()); }\n"
     "    void viaInherited() { t.inherited(d -> d.go()); }\n"
     "    void viaOverload() { t.overload(d -> d.go()); }\n"
     "    void viaVarargs() { t.varargs(d -> d.go()); }\n"
     "    void viaConstructor() { new Types(d -> d.go()); }\n"
     "    void viaWritten() { t.jdk((Doc d) -> d.go()); }\n"
     "    void viaForEach() { docs.forEach(d -> d.go()); }\n"
     "    void viaStream() { docs.stream().filter(d -> d.next() != null).forEach(d -> d.go()); }\n"
     "    void viaMap() { docs.stream().map(d -> d.next()).forEach(n -> n.go()); }\n"
     "    void viaMapForEach() { byName.forEach((k, v) -> v.go()); }\n"
     "    void viaOptional(Optional<Doc> o) { o.ifPresent(d -> d.go()); }\n"
     "    void viaEntries() { for (Map.Entry<String, Doc> e : byName.entrySet()) { e.getValue().go(); } }\n"
     "    void viaValues() { for (var d : byName.values()) { d.go(); } }\n"
     "    void viaInference() { t.each(docs, d -> d.go()); }\n"
     "    void viaInferredResult() { t.first(docs).go(); }\n"
     "    void disagreeing(Doc d1, Other o1) { t.pick(d1, o1, x -> x.go()); }\n"
     "    void viaLocal() { java.util.function.Consumer<Doc> c = d -> d.go(); }\n"
     "    void twoAbstract() { t.two(d -> d.go()); }\n"
     "    void classTarget() { t.abstractClass(d -> d.go()); }\n"
     "    void replacedTarget() { t.replaced(d -> d.go()); }\n"
     "    void lookalike() { t.fake(d -> d.go()); }\n"
     "    void unboundParameter() { t.generic(d -> d.go()); }\n"
     "    void shadowedParameter(Other d) { t.check(d2 -> d.go()); }\n"
     "}\n"},
    {"acme/fake/Handler.java",
     "package acme.fake;\n"
     "public abstract class Handler<T> { public abstract void handle(Object o); }\n"},
    /* A library's own Consumer takes whatever it takes: nothing here says it is the JDK's. */
    {"acme/lam/Foreign.java",
     "package acme.lam;\n"
     "import org.other.Consumer;\n"
     "public class Foreign {\n"
     "    public void take(Consumer<Types.Doc> c) {}\n"
     "    void foreignConsumer() { take(d -> d.go()); }\n"
     "}\n"},
};

static int oo_java_lambda_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_LAMBDA_FILES,
                 (int)(sizeof(JAVA_LAMBDA_FILES) / sizeof(JAVA_LAMBDA_FILES[0])), parallel,
                 "java")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Use.viaCheck", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaBlock", "Types.Doc.next", 1);
    OO_EXPECT("CALLS", "Use.viaBlock", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaJdk", "Types.Doc.go", 1);
    /* JLS 9.9: `? super Doc` gives the lambda a Doc. */
    OO_EXPECT("CALLS", "Use.viaJdkSuper", "Types.Doc.go", 1);
    /* The second parameter is the second argument's type. */
    OO_EXPECT("CALLS", "Use.viaJdkTwo", "Types.Other.go", 1);
    OO_EXPECT("CALLS", "Use.viaJdkTwo", "Types.Doc.go", 0);
    OO_EXPECT("CALLS", "Use.viaInherited", "Types.Doc.go", 1);
    /* A lambda is no String, so that overload is out. */
    OO_EXPECT("CALLS", "Use.viaOverload", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaVarargs", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaConstructor", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaWritten", "Types.Doc.go", 1);
    /* JDK collections on a known element type. */
    OO_EXPECT("CALLS", "Use.viaForEach", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaStream", "Types.Doc.next", 1);
    OO_EXPECT("CALLS", "Use.viaStream", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaMap", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaMapForEach", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaOptional", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaEntries", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaValues", "Types.Doc.go", 1);
    /* One argument settles the method's type parameter. Two that disagree settle nothing. */
    OO_EXPECT("CALLS", "Use.viaInference", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaInferredResult", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.disagreeing", "Types.Doc.go", 0);
    OO_EXPECT("CALLS", "Use.disagreeing", "Types.Other.go", 0);
    /* A declared target needs no call. */
    OO_EXPECT("CALLS", "Use.viaLocal", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Types.makes", "Types.Doc.go", 1);
    /* No single abstract method, or not an interface: the parameter stays unknown. */
    OO_EXPECT("CALLS", "Use.twoAbstract", "Types.Doc.go", 0);
    OO_EXPECT("CALLS", "Use.classTarget", "Types.Doc.go", 0);
    /* A subinterface implemented `accept`, so its one abstract method is `other`, taking a Doc
     * where the type argument says Other. */
    OO_EXPECT("CALLS", "Use.replacedTarget", "Types.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.replacedTarget", "Types.Other.go", 0);
    /* A project class that is called Handler is not a JDK interface. */
    OO_EXPECT("CALLS", "Use.lookalike", "Types.Doc.go", 0);
    OO_EXPECT("CALLS", "Foreign.foreignConsumer", "Foreign.take", 1);
    OO_EXPECT("CALLS", "Foreign.foreignConsumer", "Types.Doc.go", 0);
    /* The method's own type parameter is settled by nothing here. */
    OO_EXPECT("CALLS", "Use.unboundParameter", "Types.Doc.go", 0);
    OO_EXPECT("CALLS", "Use.unboundParameter", "Types.Other.go", 0);
    /* The lambda's parameter is d2. `d` is still the method's parameter, an Other. */
    OO_EXPECT("CALLS", "Use.shadowedParameter", "Types.Other.go", 1);
    OO_EXPECT("CALLS", "Use.shadowedParameter", "Types.Doc.go", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_java_lambda_parameters_from_the_selected_declaration) {
    ASSERT_EQ(oo_java_lambda_check(false), 0);
    PASS();
}

TEST(resolver_oo_java_lambda_parameters_from_the_selected_declaration_parallel) {
    ASSERT_EQ(oo_java_lambda_check(true), 0);
    PASS();
}

/* C# overload choice sees user-defined conversions, arrays reaching collection interfaces, and
 * named arguments; lambdas take their parameters from a delegate; a keyword type is its System type where
 * the project declares it (a core library). Forms from System.Text.Json and System.Linq. */
static const OOFile CS_CORE_FILES[] = {
    {"Corelib/String.cs",
     "namespace System {\n"
     "    public class Object { public virtual string ToString() { return null; } }\n"
     "    public sealed partial class String { public int Length => 0; public string Trim() { return this; } "
     "public static bool IsNullOrEmpty(string value) { return true; } }\n"
     "    public delegate void Action<in T>(T obj);\n"
     "    public delegate TResult Func<in T, out TResult>(T arg);\n"
     "    public readonly ref struct Span<T> { public int Length => 0; }\n"
     "    public readonly ref struct ReadOnlySpan<T> {\n"
     "        public int Length => 0;\n"
     "        public static implicit operator ReadOnlySpan<T>(Span<T> span) { return default; }\n"
     "        public static implicit operator ReadOnlySpan<T>(T[] array) { return default; }\n"
     "    }\n"
     "}\n"
     "namespace System.Collections.Generic { public interface IEnumerable<out T> {} }\n"},
    {"Corelib/Client.cs",
     "using System;\n"
     "using System.Collections.Generic;\n"
     "namespace Acme.Conv {\n"
     "    public enum Mode { One }\n"
     "    public class Doc { public void Go() {} public Doc Next() { return this; } }\n"
     "    public class Other { public void Go() {} }\n"
     "    public class Writer {\n"
     "        public static void Read(ReadOnlySpan<byte> data) {}\n"
     "        public static void Sized(int length, bool last, Doc doc) {}\n"
     "        public static void Each(IEnumerable<int> items) {}\n"
     "        public static void Apply(Action<Doc> act) {}\n"
     "        public static void Map(Func<Doc, Other> map) {}\n"
     "        public static void NotADelegate(Doc doc) {}\n"
     "        public static void Only(Action<Doc> act) {}\n"
     "        public static void Only(int notDelegate) {}\n"
     "        public static void Ref(ref int value) {}\n"
     "        public static void Plain(Other other) {}\n"
     "        public static void Each<T>(IEnumerable<T> items, Action<T> act) {}\n"
     "    }\n"
     "    public static class Seq {\n"
     "        public static IEnumerable<T> Keep<T>(this IEnumerable<T> items, Func<T, bool> keep) { return items; }\n"
     "        public static IEnumerable<T> Keep<T>(this IEnumerable<T> items, Func<T, int, bool> keep) { return items; }\n"
     "        public static T Head<T>(this IEnumerable<T> items) { return default; }\n"
     "    }\n"
     "    public class Main {\n"
     "        void FromInference(Doc[] docs) { Writer.Each(docs, d => d.Go()); }\n"
     "        void FromReceiver(Doc[] docs) { docs.Keep(d => { d.Go(); return true; }); }\n"
     "        void ChainResult(Doc[] docs) { docs.Keep(d => true).Head().Go(); }\n"
     "        void DeclaredLambda() { Action<Doc> a = d => d.Go(); }\n"
     "        void FromSpan(Span<byte> span) { Writer.Read(span); }\n"
     "        void FromArray(byte[] array) { Writer.Read(array); }\n"
     "        void ArrayToEnumerable(int[] items) { Writer.Each(items); }\n"
     "        void Named(Doc doc) { Writer.Sized(last: true, length: 1, doc: doc); }\n"
     "        void Lambda() { Writer.Apply(d => d.Go()); }\n"
     "        void LambdaBlock() { Writer.Apply(d => { d.Next().Go(); }); }\n"
     "        void LambdaParens() { Writer.Apply((d) => d.Go()); }\n"
     "        void LambdaFunc() { Writer.Map(d => { d.Go(); return null; }); }\n"
     "        void LambdaOverload() { Writer.Only(d => d.Go()); }\n"
     "        void RefArgument(int v) { Writer.Ref(ref v); }\n"
     "        void WrongType(Doc doc) { Writer.Plain(doc); }\n"
     "        void Keyword(string text) { text.Trim(); var n = text.Length; string.IsNullOrEmpty(text); }\n"
     "        void Literal() { \"abc\".Trim(); }\n"
     "    }\n"
     "    public class Typed<TValue> where TValue : Doc {\n"
     "        void Bound(TValue value) { value.Go(); }\n"
     "    }\n"
     "    public class Statics<TSelf> where TSelf : IMaker {\n"
     "        void Make() { TSelf.Create(); }\n"
     "    }\n"
     "    public interface IMaker { static abstract void Create(); }\n"
     "}\n"},
};

static int oo_csharp_core_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_CORE_FILES, (int)(sizeof(CS_CORE_FILES) / sizeof(CS_CORE_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    /* Span and arrays reach ReadOnlySpan through its implicit operators. */
    OO_EXPECT("CALLS", "Main.FromSpan", "Writer.Read", 1);
    OO_EXPECT("CALLS", "Main.FromArray", "Writer.Read", 1);
    OO_EXPECT("CALLS", "Main.ArrayToEnumerable", "Writer.Each", 1);
    /* Named arguments can come in any order. */
    OO_EXPECT("CALLS", "Main.Named", "Writer.Sized", 1);
    OO_EXPECT("CALLS", "Main.Lambda", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Main.LambdaBlock", "Client.Doc.Next", 1);
    OO_EXPECT("CALLS", "Main.LambdaParens", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Main.LambdaFunc", "Client.Doc.Go", 1);
    /* A lambda is no int. */
    OO_EXPECT("CALLS", "Main.LambdaOverload", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Main.RefArgument", "Writer.Ref", 1);
    OO_EXPECT("CALLS", "Main.FromInference", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Main.FromReceiver", "Client.Doc.Go", 1);
    /* `d => ...` takes one parameter: the two-parameter overload is out, and both share a node. */
    OO_EXPECT("CALLS", "Main.ChainResult", "Client.Seq.Keep", 1);
    OO_EXPECT("CALLS", "Main.ChainResult", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Main.DeclaredLambda", "Client.Doc.Go", 1);
    /* A class with no conversion still does not fit. */
    OO_EXPECT("CALLS", "Main.WrongType", "Writer.Plain", 0);
    OO_EXPECT("CALLS", "Main.Keyword", "String.String.Trim", 1);
    OO_EXPECT("USAGE", "Main.Keyword", "String.String.Length", 1);
    OO_EXPECT("CALLS", "Main.Keyword", "String.String.IsNullOrEmpty", 1);
    OO_EXPECT("CALLS", "Main.Literal", "String.String.Trim", 1);
    /* A type parameter's receiver reads its bound for members. */
    OO_EXPECT("CALLS", "Typed.Bound", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Statics.Make", "Client.IMaker.Create", 1);
    /* `TSelf` spells the parameter, never the constraint's declaration. */
    OO_EXPECT("USAGE", "Statics.Make", "Client.IMaker", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_conversions_lambdas_and_keyword_types) {
    ASSERT_EQ(oo_csharp_core_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_conversions_lambdas_and_keyword_types_parallel) {
    ASSERT_EQ(oo_csharp_core_check(true), 0);
    PASS();
}

/* Elsewhere a keyword is only a keyword, and the System delegates come from `using System`. */
static const OOFile CS_PLAIN_FILES[] = {
    {"Plain/Client.cs",
     "using System;\n"
     "using System.Collections.Generic;\n"
     "namespace Acme.Plain {\n"
     "    public class Doc { public void Go() {} public string Trim() { return null; } }\n"
     "    public class Writer {\n"
     "        public static void Apply(Action<Doc> act) {}\n"
     "        public static void Map(Func<Doc, int> map) {}\n"
     "    }\n"
     "    public class Main {\n"
     "        List<Doc> docs;\n"
     "        void Lambda() { Writer.Apply(d => d.Go()); }\n"
     "        void LambdaFunc() { Writer.Map(d => { d.Go(); return 1; }); }\n"
     "        void Linq() { docs.ForEach(d => d.Go()); }\n"
     "        void Keyword(string text) { text.Trim(); }\n"
     "    }\n"
     "}\n"},
    {"Plain/Lookalike.cs",
     "namespace Acme.Other {\n"
     "    public class Doc { public void Go() {} }\n"
     "    public class Writer { public static void Apply(Action<Doc> act) {} }\n"
     "    public class Main { void NoUsing() { Writer.Apply(d => d.Go()); } }\n"
     "}\n"},
};

static int oo_csharp_plain_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_PLAIN_FILES, (int)(sizeof(CS_PLAIN_FILES) / sizeof(CS_PLAIN_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Client.Main.Lambda", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Client.Main.LambdaFunc", "Client.Doc.Go", 1);
    OO_EXPECT("CALLS", "Client.Main.Linq", "Client.Doc.Go", 1);
    /* A string is no Doc, and no project type declares what a keyword is. */
    OO_EXPECT("CALLS", "Client.Main.Keyword", "Client.Doc.Trim", 0);
    /* `Action` without `using System` could be any type of that name. */
    OO_EXPECT("CALLS", "Lookalike.Main.NoUsing", "Lookalike.Doc.Go", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_system_delegates_without_a_core_library) {
    ASSERT_EQ(oo_csharp_plain_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_system_delegates_without_a_core_library_parallel) {
    ASSERT_EQ(oo_csharp_plain_check(true), 0);
    PASS();
}

/* One qualified name, several declarations: documentation snippets that are each a Gradle project
 * (Exposed: `org.example.tables.CitiesTable`), a type's platform files (dotnet: `Foo.Unix.cs`, `Foo.Windows.cs`). A
 * type's own body means that declaration. A project's own declaration shadows another's. Nothing
 * else tells them apart, so a caller outside abstains. */
static const OOFile KOTLIN_VARIANT_FILES[] = {
    {"lib/build.gradle.kts", "plugins { kotlin(\"jvm\") }\n"},
    {"lib/src/main/kotlin/base/Table.kt",
     "package base\n"
     "open class Table { fun varchar(name: String) {} }\n"},
    {"snippets/one/build.gradle.kts", "plugins { kotlin(\"jvm\") }\n"},
    {"snippets/one/src/main/kotlin/example/Cities.kt",
     "package example\n"
     "import base.Table\n"
     "object Cities : Table() {\n"
     "    val name = varchar(\"name\")\n"
     "    fun count() {}\n"
     "    fun own() { count() }\n"
     "}\n"},
    {"snippets/one/src/main/kotlin/example/Use.kt",
     "package example\n"
     "fun viaProjectOne() { Cities.count() }\n"},
    {"snippets/two/build.gradle.kts", "plugins { kotlin(\"jvm\") }\n"},
    {"snippets/two/src/main/kotlin/example/Cities.kt",
     "package example\n"
     "import base.Table\n"
     "object Cities : Table() {\n"
     "    val name = varchar(\"name\")\n"
     "    fun count() {}\n"
     "    fun own() { count() }\n"
     "}\n"},
    {"snippets/two/src/main/kotlin/example/Use.kt",
     "package example\n"
     "fun viaProjectTwo() { Cities.count() }\n"},
    {"tools/build.gradle.kts", "plugins { kotlin(\"jvm\") }\n"},
    {"tools/src/main/kotlin/other/Elsewhere.kt",
     "package other\n"
     "import example.Cities\n"
     "fun fromOutside() { Cities.count() }\n"},
};

static int oo_kotlin_variant_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, KOTLIN_VARIANT_FILES,
                 (int)(sizeof(KOTLIN_VARIANT_FILES) / sizeof(KOTLIN_VARIANT_FILES[0])), parallel,
                 "kt")) {
        oo_close(&index);
        return 1;
    }
    /* Each body reaches its own member, and the one supertype. */
    OO_EXPECT("CALLS", "one.src.main.kotlin.example.Cities.Cities.own",
              "one.src.main.kotlin.example.Cities.Cities.count", 1);
    OO_EXPECT("CALLS", "one.src.main.kotlin.example.Cities.Cities.own",
              "two.src.main.kotlin.example.Cities.Cities.count", 0);
    OO_EXPECT("CALLS", "two.src.main.kotlin.example.Cities.Cities.own",
              "two.src.main.kotlin.example.Cities.Cities.count", 1);
    OO_EXPECT("CALLS", "one.src.main.kotlin.example.Cities", "base.Table.Table.varchar", 1);
    /* Each project's caller reaches its own project's declaration. */
    OO_EXPECT("CALLS", "one.src.main.kotlin.example.Use.viaProjectOne",
              "one.src.main.kotlin.example.Cities.Cities.count", 1);
    OO_EXPECT("CALLS", "one.src.main.kotlin.example.Use.viaProjectOne",
              "two.src.main.kotlin.example.Cities.Cities.count", 0);
    OO_EXPECT("CALLS", "two.src.main.kotlin.example.Use.viaProjectTwo",
              "two.src.main.kotlin.example.Cities.Cities.count", 1);
    /* Another project reads one of them, and nothing here says which. */
    OO_EXPECT("CALLS", "Elsewhere.fromOutside", "one.src.main.kotlin.example.Cities.Cities.count",
              0);
    OO_EXPECT("CALLS", "Elsewhere.fromOutside", "two.src.main.kotlin.example.Cities.Cities.count",
              0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_kotlin_same_name_in_two_projects) {
    ASSERT_EQ(oo_kotlin_variant_check(false), 0);
    PASS();
}

TEST(resolver_oo_kotlin_same_name_in_two_projects_parallel) {
    ASSERT_EQ(oo_kotlin_variant_check(true), 0);
    PASS();
}

static const OOFile CS_VARIANT_FILES[] = {
    {"src/Pal/Pal.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"src/Pal/Pal.Unix.cs",
     "namespace Acme.Pal {\n"
     "    internal static class Native {\n"
     "        public static void Open() { Close(); }\n"
     "        public static void Close() {}\n"
     "        public class Nested { public void Inner() { Close(); } }\n"
     "    }\n"
     "}\n"},
    {"src/Pal/Pal.Windows.cs",
     "namespace Acme.Pal {\n"
     "    internal static class Native {\n"
     "        public static void Open() { Close(); }\n"
     "        public static void Close() {}\n"
     "        public class Nested { public void Inner() { Close(); } }\n"
     "    }\n"
     "}\n"},
    {"src/Pal/Caller.cs",
     "namespace Acme.Pal {\n"
     "    public class Caller { void Both() { Native.Open(); } }\n"
     "}\n"},
    /* The same internal helper in two assemblies, each with its own copy: System.ThrowHelper. */
    {"src/One/One.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"src/One/Helper.cs",
     "namespace System { internal static class Helper { public static void Fail() {} } }\n"},
    {"src/One/Use.cs",
     "namespace Acme.One { public class UseOne { void Run() { System.Helper.Fail(); } } }\n"},
    {"src/Two/Two.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"src/Two/Helper.cs",
     "namespace System { internal static class Helper { public static void Fail() {} } }\n"},
    {"src/Two/Use.cs",
     "using System;\n"
     "namespace Acme.Two { public class UseTwo { void Run() { Helper.Fail(); } } }\n"},
    {"src/Three/Three.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"src/Three/Use.cs",
     "using System;\n"
     "namespace Acme.Three { public class UseThree { void Run() { Helper.Fail(); } } }\n"},
};

static int oo_csharp_variant_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_VARIANT_FILES,
                 (int)(sizeof(CS_VARIANT_FILES) / sizeof(CS_VARIANT_FILES[0])), parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Pal.Unix.Native.Open", "Pal.Unix.Native.Close", 1);
    OO_EXPECT("CALLS", "Pal.Unix.Native.Open", "Pal.Windows.Native.Close", 0);
    OO_EXPECT("CALLS", "Pal.Windows.Native.Open", "Pal.Windows.Native.Close", 1);
    OO_EXPECT("CALLS", "Pal.Unix.Native.Nested.Inner", "Pal.Unix.Native.Close", 1);
    /* Two in one project: nothing says which this configuration compiles. */
    OO_EXPECT("CALLS", "Caller.Both", "Pal.Unix.Native.Open", 0);
    OO_EXPECT("CALLS", "Caller.Both", "Pal.Windows.Native.Open", 0);
    OO_EXPECT("CALLS", "UseOne.Run", "One.Helper.Helper.Fail", 1);
    OO_EXPECT("CALLS", "UseOne.Run", "Two.Helper.Helper.Fail", 0);
    OO_EXPECT("CALLS", "UseTwo.Run", "Two.Helper.Helper.Fail", 1);
    OO_EXPECT("CALLS", "UseTwo.Run", "One.Helper.Helper.Fail", 0);
    OO_EXPECT("CALLS", "UseThree.Run", "One.Helper.Helper.Fail", 0);
    OO_EXPECT("CALLS", "UseThree.Run", "Two.Helper.Helper.Fail", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_same_name_in_one_file_and_one_project) {
    ASSERT_EQ(oo_csharp_variant_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_same_name_in_one_file_and_one_project_parallel) {
    ASSERT_EQ(oo_csharp_variant_check(true), 0);
    PASS();
}

static const OOFile JAVA_VARIANT_FILES[] = {
    {"a/pom.xml", "<project></project>\n"},
    {"a/src/main/java/org/x/Util.java",
     "package org.x;\npublic class Util { public static void go() {} public static class In { public static void run() {} } }\n"},
    {"a/src/main/java/org/y/Client.java",
     "package org.y;\n"
     "import org.x.Util;\n"
     "import static org.x.Util.go;\n"
     "public class Client {\n"
     "    void imported() { Util.go(); }\n"
     "    void qualified() { org.x.Util.go(); }\n"
     "    void nested() { org.x.Util.In.run(); }\n"
     "    void statically() { go(); }\n"
     "}\n"},
    {"b/pom.xml", "<project></project>\n"},
    {"b/src/main/java/org/x/Util.java",
     "package org.x;\npublic class Util { public static void go() {} public static class In { public static void run() {} } }\n"},
    {"c/pom.xml", "<project></project>\n"},
    {"c/src/main/java/org/z/Reader.java",
     "package org.z;\n"
     "import org.x.Util;\n"
     "public class Reader {\n"
     "    void imported() { Util.go(); }\n"
     "    void qualified() { org.x.Util.go(); }\n"
     "    void nested() { org.x.Util.In.run(); }\n"
     "}\n"},
};

static int oo_java_variant_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_VARIANT_FILES,
                 (int)(sizeof(JAVA_VARIANT_FILES) / sizeof(JAVA_VARIANT_FILES[0])), parallel,
                 "java")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Client.imported", "a.src.main.java.org.x.Util.go", 1);
    OO_EXPECT("CALLS", "Client.imported", "b.src.main.java.org.x.Util.go", 0);
    OO_EXPECT("CALLS", "Client.qualified", "a.src.main.java.org.x.Util.go", 1);
    OO_EXPECT("CALLS", "Client.nested", "a.src.main.java.org.x.Util.In.run", 1);
    OO_EXPECT("CALLS", "Client.statically", "a.src.main.java.org.x.Util.go", 1);
    /* A third project could read either. A nested name inherits its outer type's doubt. */
    OO_EXPECT("CALLS", "Reader.imported", "a.src.main.java.org.x.Util.go", 0);
    OO_EXPECT("CALLS", "Reader.imported", "b.src.main.java.org.x.Util.go", 0);
    OO_EXPECT("CALLS", "Reader.qualified", "a.src.main.java.org.x.Util.go", 0);
    OO_EXPECT("CALLS", "Reader.nested", "a.src.main.java.org.x.Util.In.run", 0);
    OO_EXPECT("CALLS", "Reader.nested", "b.src.main.java.org.x.Util.In.run", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_java_same_name_in_two_projects) {
    ASSERT_EQ(oo_java_variant_check(false), 0);
    PASS();
}

TEST(resolver_oo_java_same_name_in_two_projects_parallel) {
    ASSERT_EQ(oo_java_variant_check(true), 0);
    PASS();
}

/* Exposed's tests are built on local anonymous tables: `val t = object : Table() { val c = varchar("c") }`
 * then `t.c.isNotNull()`, `PrimaryKey(c)` inside, `val text = text("x")` beside the inherited function,
 * `Words.upsert(onUpdate = { ... }) { ... }`, `for ((k, v) in map)`. */
static const OOFile KOTLIN_OBJECT_FILES[] = {
    {"acme/Lib.kt",
     "package acme\n"
     "class Op { fun and() {} }\n"
     "class Column { fun isNotNull(): Op = Op()\n fun defaulted(x: Int): Column = this }\n"
     "open class Table {\n"
     "    inner class PrimaryKey(val col: Column)\n"
     "    fun text(name: String): Column = Column()\n"
     "    fun varchar(name: String): Column = Column()\n"
     "}\n"
     "data class Row(val left: Column, val right: Op)\n"
     "class Plain(val left: Column)\n"
     "fun <T : Table> T.upsert(vararg keys: Column, onUpdate: (T.() -> Unit)? = null, body: T.(Int) -> Unit) {}\n"
     "fun helper(first: Int = 0, onRow: (Row) -> Unit, onOp: (Op) -> Unit) {}\n"
     "fun Table.stamp(name: String): Column = Column()\n"
     "fun Column.day(): Op = Op()\n"
     "object Cities : Table() { val at = stamp(\"at\")\n val plain = varchar(\"p\") }\n"},
    {"acme/Main.kt",
     "package acme\n"
     "class Main {\n"
     "    fun anonymousProperty() {\n"
     "        val t = object : Table() { val c = varchar(\"c\")\n val d = text(\"d\").defaulted(1) }\n"
     "        t.c.isNotNull()\n"
     "        t.d.isNotNull()\n"
     "        t.varchar(\"x\")\n"
     "    }\n"
     "    fun declaredProperty() {\n"
     "        val t = object : Table() { val c: Op = Op() }\n"
     "        t.c.and()\n"
     "    }\n"
     "    fun ownMemberRead() {\n"
     "        val t = object : Table() { val c = varchar(\"c\")\n val e = c.isNotNull() }\n"
     "    }\n"
     "    fun sameNameFunction() {\n"
     "        val t = object : Table() { val text = text(\"x\") }\n"
     "        t.text.isNotNull()\n"
     "    }\n"
     "    fun ownFunctionHides() {\n"
     "        val t = object : Table() { fun text(name: String): Op = Op()\n val z = text(\"q\") }\n"
     "    }\n"
     "    fun nestedOfSuper() {\n"
     "        val t = object : Table() { val c = varchar(\"c\")\n val pk = PrimaryKey(c) }\n"
     "    }\n"
     "    fun pairParts(p: Pair<Column, Op>) { val (a, b) = p\n a.isNotNull()\n b.and() }\n"
     "    fun pairNames(p: Pair<Column, Op>) { p.first.isNotNull()\n p.second.and() }\n"
     "    fun mapLoop(m: Map<Column, Op>) { for ((k, v) in m) { k.isNotNull()\n v.and() } }\n"
     "    fun entryLoop(m: Map<Column, Op>) { for (e in m) { e.value.and()\n e.key.isNotNull() } }\n"
     "    fun dataParts(r: Row) { val (l, o) = r\n l.isNotNull()\n o.and() }\n"
     "    fun notData(r: Plain) { val (l) = r\n l.isNotNull() }\n"
     "    fun lambdaParts(m: List<Pair<Column, Op>>) { m.forEach { (c, o) -> c.isNotNull()\n o.and() } }\n"
     "    fun swappedParts(r: Row) { val (first, second) = r\n second.and() }\n"
     "    fun namedLambda(t: Table) { t.upsert(onUpdate = { varchar(\"a\") }) { text(\"b\") } }\n"
     "    fun namedByName() { helper(onOp = { it.and() }, onRow = { it.left.isNotNull() }) }\n"
     "    fun extensionInitializer() { Cities.at.day()\n Cities.plain.day() }\n"
     "}\n"},
};

static int oo_kotlin_object_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, KOTLIN_OBJECT_FILES,
                 (int)(sizeof(KOTLIN_OBJECT_FILES) / sizeof(KOTLIN_OBJECT_FILES[0])), parallel,
                 "kt")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Main.anonymousProperty", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.anonymousProperty", "Lib.Table.varchar", 1);
    OO_EXPECT("CALLS", "Main.declaredProperty", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.ownMemberRead", "Lib.Column.isNotNull", 1);
    /* A property does not hide a function of its name: the function comes first. */
    OO_EXPECT("CALLS", "Main.sameNameFunction", "Lib.Table.text", 1);
    OO_EXPECT("CALLS", "Main.sameNameFunction", "Lib.Column.isNotNull", 1);
    /* A function the object declares itself does. */
    OO_EXPECT("CALLS", "Main.ownFunctionHides", "Lib.Table.text", 0);
    OO_EXPECT("CALLS", "Main.nestedOfSuper", "Lib.Table.PrimaryKey", 1);
    OO_EXPECT("CALLS", "Main.pairParts", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.pairParts", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.pairNames", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.pairNames", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.mapLoop", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.mapLoop", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.entryLoop", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.entryLoop", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.dataParts", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.dataParts", "Lib.Op.and", 1);
    /* A class that is not `data` has no componentN to read. */
    OO_EXPECT("CALLS", "Main.notData", "Lib.Column.isNotNull", 0);
    OO_EXPECT("CALLS", "Main.lambdaParts", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.lambdaParts", "Lib.Op.and", 1);
    /* By position, whatever the local is called. */
    OO_EXPECT("CALLS", "Main.swappedParts", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.swappedParts", "Lib.Column.isNotNull", 0);
    /* A named lambda is no positional vararg, and has the receiver of the parameter it names. */
    OO_EXPECT("CALLS", "Main.namedLambda", "Lib.upsert", 1);
    OO_EXPECT("CALLS", "Main.namedLambda", "Lib.Table.varchar", 1);
    OO_EXPECT("CALLS", "Main.namedLambda", "Lib.Table.text", 1);
    OO_EXPECT("CALLS", "Main.namedByName", "Lib.Op.and", 1);
    OO_EXPECT("CALLS", "Main.namedByName", "Lib.Column.isNotNull", 1);
    OO_EXPECT("CALLS", "Main.extensionInitializer", "Lib.day", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_kotlin_anonymous_objects_destructuring_named_lambdas) {
    ASSERT_EQ(oo_kotlin_object_check(false), 0);
    PASS();
}

TEST(resolver_oo_kotlin_anonymous_objects_destructuring_named_lambdas_parallel) {
    ASSERT_EQ(oo_kotlin_object_check(true), 0);
    PASS();
}

/* Three spellings on which the C# grammar loses a whole file, and with it every type the file declares:
 * a dereference of a parenthesis (Task.cs, Vector2.cs), `async` as a name (every `bool async` test),
 * a doubled quote in a verbatim interpolated string. The caller's site spans stay exact. */
static const OOFile CS_PARSE_FILES[] = {
    {"Parse/Deref.cs",
     "namespace Acme.Parse {\n"
     "    public unsafe struct Vec {\n"
     "        public static Vec Load(float* source) => *(Vec*)source;\n"
     "        public static float Next(float* p) { return *(p + 1); }\n"
     "        public void Store(float* destination) => *(Vec*)destination = this;\n"
     "        public ref Vec Self(void* p) { return ref *(Vec*)p; }\n"
     "        public int Width() { return 1; }\n"
     "        public int Twice() { return Width() * (2); }\n"
     "    }\n"
     "    public class Use { void Run(float* p) { var v = Vec.Load(p); v.Width(); } }\n"
     "}\n"},
    {"Parse/AsyncName.cs",
     "using System.Threading.Tasks;\n"
     "namespace Acme.Parse {\n"
     "    public class Worker {\n"
     "        public async Task Go(bool async) {\n"
     "            if (async) { Done(); }\n"
     "            await Run(async: true);\n"
     "            var other = async ? 1 : 2;\n"
     "        }\n"
     "        public Task Run(bool async) { return null; }\n"
     "        public void Done() {}\n"
     "        public async Task Lambda() { await Task.Run(async () => { Done(); }); }\n"
     "    }\n"
     "}\n"},
    {"Parse/Quotes.cs",
     "namespace Acme.Parse {\n"
     "    public class Text {\n"
     "        public string Quoted(string name) { Note(); return $@\"\"\"{name}\"\"\"; }\n"
     "        public void Note() {}\n"
     "        public void After() { Note(); }\n"
     "    }\n"
     "}\n"},
};

static int oo_csharp_parse_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_PARSE_FILES, (int)(sizeof(CS_PARSE_FILES) / sizeof(CS_PARSE_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Use.Run", "Deref.Vec.Load", 1);
    OO_EXPECT("CALLS", "Use.Run", "Deref.Vec.Width", 1);
    /* A star after a name is multiplication, and stays. */
    OO_EXPECT("CALLS", "Vec.Twice", "Deref.Vec.Width", 1);
    OO_EXPECT("CALLS", "Worker.Go", "AsyncName.Worker.Done", 1);
    OO_EXPECT("CALLS", "Worker.Go", "AsyncName.Worker.Run", 1);
    /* The modifier is still the modifier. */
    OO_EXPECT("CALLS", "Worker.Lambda", "AsyncName.Worker.Done", 1);
    OO_EXPECT("CALLS", "Text.Quoted", "Quotes.Text.Note", 1);
    OO_EXPECT("CALLS", "Text.After", "Quotes.Text.Note", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_syntax_that_loses_a_whole_file) {
    ASSERT_EQ(oo_csharp_parse_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_syntax_that_loses_a_whole_file_parallel) {
    ASSERT_EQ(oo_csharp_parse_check(true), 0);
    PASS();
}

/* An array or a BCL collection reaches a project's extension on a sequence interface. A partial type has
 * one node that stands for it. `using static` brings nested types in. Attributes and annotations name their
 * class and read their arguments. An extension in another test project is not in scope. */
static const OOFile CS_MORE_FILES[] = {
    {"More/Ext.cs",
     "using System.Collections.Generic;\n"
     "namespace Acme.More {\n"
     "    public class Bag : IEnumerable<int> { }\n"
     "    public class Plain { }\n"
     "    public static class Sequences {\n"
     "        public static int Tally<T>(this IEnumerable<T> items) { return 0; }\n"
     "        public static int Lone(this Plain plain) { return 0; }\n"
     "        public static int OnlyList<T>(this IList<T> items) { return 0; }\n"
     "    }\n"
     "    public class Use {\n"
     "        void FromArray(int[] values) { values.Tally(); }\n"
     "        void FromList(List<int> values) { values.Tally(); }\n"
     "        void FromBag(Bag bag) { bag.Tally(); }\n"
     "        void FromPlain(Plain plain) { plain.Tally(); plain.Lone(); }\n"
     "        void Dictionary(Dictionary<int, int> map) { map.OnlyList(); }\n"
     "    }\n"
     "}\n"},
    {"More/Value.Overloads.cs",
     "namespace Acme.More { public partial class Value {\n"
     "    public static Value Create(int value) { return null; }\n"
     "    public static Value Create(int? value) { return null; }\n"
     "} }\n"},
    {"More/Value.cs",
     "namespace Acme.More { public partial class Value {\n"
     "    public static Value Create<T>(T value, string note) { return null; }\n"
     "    public static class Kinds { public const int One = 1; }\n"
     "} }\n"},
    {"More/UseValue.cs",
     "namespace Acme.More { public class UseValue {\n"
     "    void Make() { Value.Create(10); }\n"
     "    int Nested() { return Value.Kinds.One; }\n"
     "} }\n"},
    {"More/Part.A.cs", "namespace Acme.More { public partial class Whole { public void One() {} } }\n"},
    {"More/Whole.cs", "namespace Acme.More { public partial class Whole { public void Two() {} } }\n"},
    {"More/Part.Z.cs", "namespace Acme.More { public partial class Whole { public void Three() {} } }\n"},
    {"More/UseWhole.cs",
     "namespace Acme.More {\n"
     "    public class UseWhole {\n"
     "        Whole Make() { return new Whole(); }\n"
     "        void Call(Whole whole) { whole.One(); whole.Two(); whole.Three(); }\n"
     "    }\n"
     "}\n"},
    {"More/Static.cs",
     "using static Acme.More.Helpers;\n"
     "namespace Acme.More {\n"
     "    public static class Helpers { public class Inner { public void Go() {} } public static void Free() {} }\n"
     "    public class UseStatic { void Run(Inner inner) { inner.Go(); Free(); } }\n"
     "}\n"},
    {"More/Attr.cs",
     "using System;\n"
     "namespace Acme.More {\n"
     "    public enum Level { High }\n"
     "    public class TagAttribute : Attribute { public TagAttribute(Level level) {} }\n"
     "    public class Marked {}\n"
     "    [Tag(Level.High)] public class Tagged { [Tag(Level.High)] public void Method() {} }\n"
     "    [Marked] public class NotAnAttributeName {}\n"
     "    public class Param { public void Take([Tag(Level.High)] int value) {} }\n"
     "}\n"},
    {"proj/Lib.Tests/Lib.Tests.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"proj/Lib.Tests/Helpers.cs",
     "namespace System { public static class TestHelpers {\n"
     "    public static int Blend<T>(this System.Collections.Generic.IEnumerable<T> a, T b) { return 0; }\n"
     "} }\n"},
    {"proj/Lib.Tests/Own.cs",
     "using System.Collections.Generic;\n"
     "namespace System.Own { public class OwnUse { void Run(List<int> items) { items.Blend(1); } } }\n"},
    {"proj/Other.Tests/Other.Tests.csproj", "<Project Sdk=\"Microsoft.NET.Sdk\"></Project>\n"},
    {"proj/Other.Tests/Use.cs",
     "using System.Collections.Generic;\n"
     "namespace System.Other { public class ElseUse { void Run(List<int> items) { items.Blend(1); } } }\n"},
};

static int oo_csharp_more_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CS_MORE_FILES, (int)(sizeof(CS_MORE_FILES) / sizeof(CS_MORE_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Use.FromArray", "Ext.Sequences.Tally", 1);
    OO_EXPECT("CALLS", "Use.FromList", "Ext.Sequences.Tally", 1);
    OO_EXPECT("CALLS", "Use.FromBag", "Ext.Sequences.Tally", 1);
    /* Plain implements nothing: only its own extension applies. */
    OO_EXPECT("CALLS", "Use.FromPlain", "Ext.Sequences.Tally", 0);
    OO_EXPECT("CALLS", "Use.FromPlain", "Ext.Sequences.Lone", 1);
    OO_EXPECT("CALLS", "Use.Dictionary", "Ext.Sequences.OnlyList", 0);
    /* The file named after the type stands for it, wherever the first path would sort. */
    OO_EXPECT("CALLS", "UseWhole.Make", "More.Whole.Whole", 1);
    OO_EXPECT("USES_TYPE", "UseWhole.Make", "More.Whole.Whole", 1);
    OO_EXPECT("USAGE", "UseWhole.Call", "More.Whole.Whole", 1);
    OO_EXPECT("USAGE", "UseWhole.Call", "Part.A.Whole", 0);
    /* A member keeps its own file's node. */
    OO_EXPECT("CALLS", "UseWhole.Call", "Part.A.Whole.One", 1);
    OO_EXPECT("CALLS", "UseWhole.Call", "More.Whole.Whole.Two", 1);
    OO_EXPECT("CALLS", "UseWhole.Call", "Part.Z.Whole.Three", 1);
    OO_EXPECT("CALLS", "UseValue.Make", "Value.Overloads.Value.Create", 1);
    OO_EXPECT("USAGE", "UseValue.Nested", "More.Value.Value.Kinds", 1);
    OO_EXPECT("CALLS", "UseStatic.Run", "Static.Helpers.Inner.Go", 1);
    OO_EXPECT("CALLS", "UseStatic.Run", "Static.Helpers.Free", 1);
    /* `[Tag]` is TagAttribute, and its argument an expression. */
    OO_EXPECT("USAGE", "More.Attr", "Attr.TagAttribute", 1);
    OO_EXPECT("USAGE", "More.Attr", "Attr.Level", 1);
    OO_EXPECT("DECORATES", "Attr.Tagged", "Attr.TagAttribute", 1);
    OO_EXPECT("USAGE", "Tagged.Method", "Attr.TagAttribute", 1);
    OO_EXPECT("USAGE", "More.Attr", "Attr.Marked", 1);
    /* A test project's helper from outside it is no candidate. */
    OO_EXPECT("CALLS", "OwnUse.Run", "Helpers.TestHelpers.Blend", 1);
    OO_EXPECT("CALLS", "ElseUse.Run", "Helpers.TestHelpers.Blend", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_extension_receivers_partial_types_attributes) {
    ASSERT_EQ(oo_csharp_more_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_extension_receivers_partial_types_attributes_parallel) {
    ASSERT_EQ(oo_csharp_more_check(true), 0);
    PASS();
}

/* Annotations name their type and read their arguments, in Java and Kotlin. */
static const OOFile JAVA_ANNOTATION_FILES[] = {
    {"acme/anno/Nullable.java", "package acme.anno;\npublic @interface Nullable {}\n"},
    {"acme/anno/Scope.java", "package acme.anno;\npublic enum Scope { INTERNAL }\n"},
    {"acme/anno/Serverless.java",
     "package acme.anno;\npublic @interface Serverless { Scope value(); }\n"},
    {"acme/anno/Outer.java",
     "package acme.anno;\n"
     "public class Outer { public enum Kind { ONE } }\n"},
    {"acme/anno/Nested.java",
     "package acme.anno;\n"
     "class Nested { Object read() { return Outer.Kind.ONE; } }\n"},
    {"acme/anno/Use.java",
     "package acme.anno;\n"
     "import java.lang.Override;\n"
     "@Serverless(Scope.INTERNAL)\n"
     "public class Use {\n"
     "    @Nullable private String field;\n"
     "    @Nullable public String method(@Nullable String parameter) { return parameter; }\n"
     "    @Override public String toString() { return null; }\n"
     "}\n"},
};

static const OOFile KOTLIN_ANNOTATION_FILES[] = {
    {"acme/Anno.kt",
     "package acme\n"
     "annotation class InternalApi\n"
     "annotation class Tagged(val why: String)\n"
     "const val REASON = \"r\"\n"
     "@InternalApi\n"
     "fun top() {}\n"
     "@Tagged(REASON)\n"
     "class Holder {\n"
     "    @InternalApi\n"
     "    fun member() {}\n"
     "    @InternalApi\n"
     "    val field = 1\n"
     "    @Suppress(\"x\")\n"
     "    fun outside() {}\n"
     "}\n"},
};

static int oo_annotation_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_ANNOTATION_FILES,
                 (int)(sizeof(JAVA_ANNOTATION_FILES) / sizeof(JAVA_ANNOTATION_FILES[0])), parallel,
                 "java")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("USAGE", "Use.java.__file__", "acme.anno.Serverless", 1);
    OO_EXPECT("USAGE", "Use.java.__file__", "acme.anno.Scope", 1);
    OO_EXPECT("USAGE", "Use.java.__file__", "acme.anno.Nullable", 1);
    OO_EXPECT("USAGE", "Use.method", "acme.anno.Nullable", 1);
    OO_EXPECT("USAGE", "Nested.read", "acme.anno.Outer", 1);
    OO_EXPECT("USAGE", "Nested.read", "Outer.Kind", 1);
    oo_close(&index);
    if (!oo_open(&index, KOTLIN_ANNOTATION_FILES,
                 (int)(sizeof(KOTLIN_ANNOTATION_FILES) / sizeof(KOTLIN_ANNOTATION_FILES[0])),
                 parallel, "kt")) {
        oo_close(&index);
        return failed + 1;
    }
    OO_EXPECT("USAGE", "Anno.top", "Anno.InternalApi", 1);
    OO_EXPECT("USAGE", "Holder.member", "Anno.InternalApi", 1);
    OO_EXPECT("USAGE", "acme.Anno", "Anno.Tagged", 1);
    OO_EXPECT("DECORATES", "Anno.Holder", "Anno.Tagged", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_annotations_name_their_type) {
    ASSERT_EQ(oo_annotation_check(false), 0);
    PASS();
}

TEST(resolver_oo_annotations_name_their_type_parallel) {
    ASSERT_EQ(oo_annotation_check(true), 0);
    PASS();
}

/* Spellings of .NET's own sources the grammar cannot read, each of which lost nodes or whole files. */
static const OOFile CSHARP_SYNTAX_FILES[] = {
    {"Intrinsics/Vector4x.cs",
     "namespace Intrinsics {\n"
     "    public interface IConst<T> { }\n"
     "    public readonly struct Vector4x<T> { }\n"
     "    public static class Vector4x {\n"
     "        public static int Create(int x) { return x; }\n"
     "        /// <typeparam name=\"T\">The type.</typeparam>\n"
     "        extension<T>(Vector4x<T>)\n"
     "            where T : IConst<T>\n"
     "        {\n"
     "            public static int Pi\n"
     "            {\n"
     "                get => Create(1);\n"
     "            }\n"
     "            public static int Tau() { return Create(2); }\n"
     "        }\n"
     "        public static int After() { return Create(3); }\n"
     "    }\n"
     "    public static class UseVector {\n"
     "        public static int Run() { return Vector4x.After() + Vector4x.Tau(); }\n"
     "    }\n"
     "}\n"},
    {"Extensions/Bags.cs",
     "namespace Extensions {\n"
     "    public class Bag<T> { public int Count() { return 0; } }\n"
     "    public static class Bags {\n"
     "        extension<T>(Bag<T> bag) where T : class\n"
     "        {\n"
     "            public int Size() { return bag.Count(); }\n"
     "        }\n"
     "        public static int After(Bag<int> bag) { return bag.Count(); }\n"
     "    }\n"
     "}\n"},
    {"Links/Lists.cs",
     "namespace Links {\n"
     "    public class Node { public Node Next; public Node Prev; public void Link() {} }\n"
     "    public class Lists {\n"
     "        public void Add(Node node) {\n"
     "            node.Next?.Prev = node;\n"
     "            node.Link();\n"
     "        }\n"
     "        public bool Read(Node node) { return node?.Prev == node; }\n"
     "    }\n"
     "}\n"},
    {"Pointers/Reader.cs",
     "namespace Pointers {\n"
     "    public static class Helper {\n"
     "        public static int One() { return 1; }\n"
     "        public static int Two() { return 2; }\n"
     "    }\n"
     "    public unsafe class Reader {\n"
     "        public int Next(char* p) {\n"
     "            char c = *p;\n"
     "            c = *++p;\n"
     "            if (c == 'x') { c = *++p; }\n"
     "            return Helper.One();\n"
     "        }\n"
     "        public int Store(byte* p) {\n"
     "            if (p != null)\n"
     "                *(byte*)(p + 1) = (byte)Helper.One();\n"
     "            *(int*)p = 0;\n"
     "            return Helper.Two();\n"
     "        }\n"
     "        public void After() { Helper.Two(); }\n"
     "        public int Scale() { return Helper.One() *(Helper.Two()); }\n"
     "        void extension(int value) {}\n"
     "        void Plain(char* p) {\n"
     "            char c = *++p;\n"
     "            extension(1);\n"
     "        }\n"
     "    }\n"
     "}\n"},
    {"Natives/Native.cs",
     "namespace Natives {\n"
     "    public static class Native {\n"
     "        public static safe extern double Acos(double d);\n"
     "    }\n"
     "    public closed class Shape { public void Draw() {} }\n"
     "    public class UseNative {\n"
     "        public double Run(Shape shape) {\n"
     "            bool safe = true;\n"
     "            bool closed = safe;\n"
     "            shape.Draw();\n"
     "            return Native.Acos(1.0);\n"
     "        }\n"
     "    }\n"
     "}\n"},
};

static int oo_csharp_syntax_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CSHARP_SYNTAX_FILES,
                 (int)(sizeof(CSHARP_SYNTAX_FILES) / sizeof(CSHARP_SYNTAX_FILES[0])), parallel,
                 "cs")) {
        oo_close(&index);
        return 1;
    }
    /* A block with no receiver name: its members stand as the static class's. */
    OO_EXPECT("CALLS", "Vector4x.After", "Vector4x.Create", 1);
    OO_EXPECT("CALLS", "Vector4x.Tau", "Vector4x.Create", 1);
    OO_EXPECT("CALLS", "UseVector.Run", "Vector4x.After", 1);
    OO_EXPECT("CALLS", "UseVector.Run", "Vector4x.Tau", 1);
    /* A named receiver stays bound in the block. */
    OO_EXPECT("CALLS", "Bags.Size", "Bag.Count", 1);
    OO_EXPECT("CALLS", "Bags.After", "Bag.Count", 1);
    /* `a?.b = c` writes b. `a?.b == c` still does not. */
    OO_EXPECT("WRITES", "Lists.Add", "Node.Prev", 1);
    OO_EXPECT("WRITES", "Lists.Read", "Node.Prev", 0);
    OO_EXPECT("CALLS", "Lists.Add", "Node.Link", 1);
    /* A dereference is read, written through, or a multiplication. */
    OO_EXPECT("CALLS", "Reader.Next", "Helper.One", 1);
    OO_EXPECT("CALLS", "Reader.Store", "Helper.One", 1);
    OO_EXPECT("CALLS", "Reader.Store", "Helper.Two", 1);
    OO_EXPECT("CALLS", "Reader.After", "Helper.Two", 1);
    OO_EXPECT("CALLS", "Reader.Scale", "Helper.One", 1);
    OO_EXPECT("CALLS", "Reader.Scale", "Helper.Two", 1);
    /* A method named extension is called like any other. */
    OO_EXPECT("CALLS", "Reader.Plain", "Reader.extension", 1);
    /* `safe` and `closed` are modifiers only between words that show it. */
    OO_EXPECT("CALLS", "UseNative.Run", "Native.Acos", 1);
    OO_EXPECT("CALLS", "UseNative.Run", "Shape.Draw", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_syntax_the_grammar_lacks) {
    ASSERT_EQ(oo_csharp_syntax_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_syntax_the_grammar_lacks_parallel) {
    ASSERT_EQ(oo_csharp_syntax_check(true), 0);
    PASS();
}

/* What runs in a member with no node of its own belongs to the nearest declaration that has one. */
static const OOFile CSHARP_CALLER_FILES[] = {
    {"Client/Lib.cs",
     "namespace Client {\n"
     "    public static class Lib {\n"
     "        public static int Implicit(int x) { return x; }\n"
     "        public static int Explicit(int x) { return x; }\n"
     "        public static int Indexed(int x) { return x; }\n"
     "        public static int Arrow() { return 0; }\n"
     "        public static int Getter() { return 0; }\n"
     "        public static int Setter(int x) { return x; }\n"
     "        public static int Init() { return 0; }\n"
     "        public static int Local(int x) { return x; }\n"
     "        public static int Misread() { return 0; }\n"
     "    }\n"
     "}\n"},
    {"Client/Main.cs",
     "namespace Client {\n"
     "    public class Main {\n"
     "        public static implicit operator Main(int v) { Lib.Implicit(v); return null; }\n"
     "        public static explicit operator int(Main m) => Lib.Explicit(1);\n"
     "        public int this[int i] { get { return Lib.Indexed(i); } }\n"
     "        public int Prop => Lib.Arrow();\n"
     "        public int Body {\n"
     "            get { return Lib.Getter(); }\n"
     "            set => Lib.Setter(value);\n"
     "        }\n"
     "        public int Auto { get; set; } = Lib.Init();\n"
     "        public void Run() {\n"
     "            Hoisted();\n"
     "            int Inner(int x) { return Lib.Local(x); }\n"
     "            Inner(1);\n"
     "            void Hoisted() {}\n"
     "        }\n"
     "        public void Hoisted(int unused = 0) {}\n"
     "        public void Outer() {\n"
     "            public void Swallowed() { Lib.Misread(); }\n"
     "        }\n"
     "    }\n"
     "}\n"},
};

static const OOFile KOTLIN_CALLER_FILES[] = {
    {"lib/Worker.kt", "package lib\nobject Worker {\n    fun go() {}\n}\n"},
    {"app/Main.kt",
     "package app\n"
     "import lib.Worker\n"
     "fun outer() {\n"
     "    fun inner() { Worker.go() }\n"
     "    inner()\n"
     "}\n"
     "class Box {\n"
     "    fun member() {\n"
     "        fun helper() { Worker.go() }\n"
     "        helper()\n"
     "    }\n"
     "}\n"},
};

static int oo_caller_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CSHARP_CALLER_FILES,
                 (int)(sizeof(CSHARP_CALLER_FILES) / sizeof(CSHARP_CALLER_FILES[0])), parallel,
                 "cs")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Main.operator implicit", "Lib.Implicit", 1);
    OO_EXPECT("CALLS", "Main.operator explicit", "Lib.Explicit", 1);
    OO_EXPECT("CALLS", "Main.this", "Lib.Indexed", 1);
    OO_EXPECT("CALLS", "Main.Prop", "Lib.Arrow", 1);
    OO_EXPECT("CALLS", "Main.Body", "Lib.Getter", 1);
    OO_EXPECT("CALLS", "Main.Body", "Lib.Setter", 1);
    OO_EXPECT("CALLS", "Main.Auto", "Lib.Init", 1);
    OO_EXPECT("CALLS", "Main.Run", "Lib.Local", 1);
    /* A local function hides a member of its name from the whole block, above it too. */
    OO_EXPECT("CALLS", "Main.Run", "Main.Hoisted", 0);
    /* An access modifier: not a local function, so not a part of the method around it. */
    OO_EXPECT("CALLS", "Main.Outer", "Lib.Misread", 0);
    oo_close(&index);
    if (!oo_open(&index, KOTLIN_CALLER_FILES,
                 (int)(sizeof(KOTLIN_CALLER_FILES) / sizeof(KOTLIN_CALLER_FILES[0])), parallel,
                 "kt")) {
        oo_close(&index);
        return failed + 1;
    }
    OO_EXPECT("CALLS", "Main.outer", "Worker.go", 1);
    OO_EXPECT("CALLS", "Box.member", "Worker.go", 1);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_callers_without_a_node_of_their_own) {
    ASSERT_EQ(oo_caller_check(false), 0);
    PASS();
}

TEST(resolver_oo_callers_without_a_node_of_their_own_parallel) {
    ASSERT_EQ(oo_caller_check(true), 0);
    PASS();
}

/* The C# language's own member lookup, as .NET's core library leans on it. */
static const OOFile CSHARP_LOOKUP_FILES[] = {
    {"System/Core.cs",
     "namespace System {\n"
     "    public class Object {\n"
     "        public virtual string ToString() { return null; }\n"
     "        public Type GetType() { return null; }\n"
     "        public static bool ReferenceEquals(object a, object b) { return false; }\n"
     "    }\n"
     "    public abstract class ValueType { public override string ToString() { return null; } }\n"
     "    public abstract class Enum { public bool HasFlag(Enum flag) { return false; } }\n"
     "    public abstract class Type { public object GetMethod(string name) { return null; } }\n"
     "    public abstract class Array {\n"
     "        public object GetValue(int index) { return null; }\n"
     "        public int Length { get { return 0; } }\n"
     "    }\n"
     "}\n"},
    {"Client/Types.cs",
     "namespace Client {\n"
     "    public class Doc { public void Go() {} }\n"
     "    public class Other { public void Go() {} }\n"
     "    public class Stream {}\n"
     "    public class FileStream : Stream {}\n"
     "    public class Plain {}\n"
     "    public struct Point {}\n"
     "    public interface IOld { void Put(object key, object value); Other Size { get; } }\n"
     "    public class Dict : IOld {\n"
     "        void IOld.Put(object key, object value) {}\n"
     "        public void Put(string key, string value) {}\n"
     "        Other IOld.Size => null;\n"
     "        public Doc Size => null;\n"
     "    }\n"
     "    public class Base { public void Log(object o) {} }\n"
     "    public class Derived : Base { public void Log(Stream s) {} }\n"
     "    public class Bag {\n"
     "        public Doc this[string key] { get { return null; } }\n"
     "        public Other this[int index] { get { return null; } }\n"
     "    }\n"
     "    public static class Box { public static T Cast<T>(object o) { return default; } }\n"
     "    public static class Lib { public static void Run() {} }\n"
     "    public class Span {}\n"
     "    public enum Color { Red }\n"
     "    public class Task<T> {}\n"
     "    public class Later { public Task<Doc> Fetch() { return null; } }\n"
     "    public static class Text { public static void Show(this string s) {} }\n"
     "    public class Holder { public object Field; }\n"
     "    public class Node2 { public static implicit operator Node2(bool value) { return null; } }\n"
     "    public partial class Sink {\n"
     "        public void Add(Node2 value) {}\n"
     "    }\n"
     "    public class Bytes {}\n"
     "    public partial class Spans {\n"
     "        public void Write(System.ReadOnlySpan<byte> utf8) {}\n"
     "    }\n"
     "}\n"},
    {"Client/Sink.Generic.cs",
     "namespace Client {\n"
     "    public partial class Sink {\n"
     "        public void Add<T>(T value) {}\n"
     "    }\n"
     "}\n"},
    {"Client/Spans.Chars.cs",
     "namespace Client {\n"
     "    public partial class Spans {\n"
     "        public void Write(System.ReadOnlySpan<char> chars) {}\n"
     "    }\n"
     "}\n"},
    {"Client/Writer.Float.cs",
     "namespace Client {\n"
     "    public partial class Writer {\n"
     "        public void Put(Span name, float value) {}\n"
     "        public bool Parse(Span text, out bool value) { value = false; return true; }\n"
     "    }\n"
     "}\n"},
    {"Client/Writer.Double.cs",
     "namespace Client {\n"
     "    public partial class Writer {\n"
     "        public void Put(Span name, double value) {}\n"
     "        public bool Parse(Span text, out int value) { value = 0; return true; }\n"
     "    }\n"
     "}\n"},
    {"Client/Use.cs",
     "namespace Client {\n"
     "    public class Use {\n"
     "        void ViaClass(Dict d, Mystery m) { d.Put(m.X, m.Y); }\n"
     "        void ViaInterface(IOld i, Mystery m) { i.Put(m.X, m.Y); }\n"
     "        void Explicit(Dict d) { d.Size.Go(); }\n"
     "        void Derives(Derived d, FileStream f) { d.Log(f); }\n"
     "        void BaseOnly(Derived d, Plain p) { d.Log(p); }\n"
     "        void UnknownArg(Derived d, Mystery m) { d.Log(m.X); }\n"
     "        void Indexed(Bag b) { b[\"k\"].Go(); }\n"
     "        void IndexedByInt(Bag b) { b[1].Go(); }\n"
     "        void Spelled(object o) { Box.Cast<Doc>(o).Go(); }\n"
     "        void Rooted() { global::Client.Lib.Run(); }\n"
     "        void FloatLiteral(Writer w, Span s) { w.Put(s, 10.1f); }\n"
     "        void DoubleLiteral(Writer w, Span s) { w.Put(s, 10.1); }\n"
     "        void UnknownLiteral(Writer w, Span s, Mystery m) { w.Put(s, m.X); }\n"
     "        void OutBool(Writer w, Span s) { w.Parse(s, out bool v); }\n"
     "        void OutInt(Writer w, Span s) { w.Parse(s, out int v); }\n"
     "        void TypeOf() { typeof(Doc).GetMethod(\"Go\"); }\n"
     "        void ArrayMember(int[] items) { items.GetValue(0); }\n"
     "        void ObjectMember(Plain p) { p.GetType(); }\n"
     "        void BareObject(Plain p) { ReferenceEquals(p, p); }\n"
     "        void StructMember(Point p) { p.ToString(); }\n"
     "        void EnumMember(Color c) { c.HasFlag(Color.Red); }\n"
     "        async void Awaited(Later l) { (await l.Fetch()).Go(); }\n"
     "        void Concatenated(Mystery m) { (\"a\" + m.X).Show(); }\n"
     "        void Sums(int a, int b) { (a + b).Show(); }\n"
     "        void GenericWins(Sink s) { s.Add(false); }\n"
     "        void BytesToSpan(Spans s, byte[] bytes) { s.Write(bytes); }\n"
     "        void CharsToSpan(Spans s, char[] chars) { s.Write(chars); }\n"
     "        void EmptyPattern(Doc d) { if (d is { } bound) { bound.Go(); } }\n"
     "        void TypedPattern(object o) { if (o is Doc { } bound) { bound.Go(); } }\n"
     "        void TypeNotShadowed(object o) { if (o is Doc { } bound) { Doc made = null; made.Go(); } }\n"
     "    }\n"
     "}\n"},
    {"Core/src/Core.projitems", "<Project></Project>\n"},
    {"Core/src/ThrowHelper.cs",
     "namespace Shared { public static class ThrowHelper { public static void Fail() {} } }\n"},
    {"Core/src/Array2.cs",
     "namespace Shared { public class Array2 { void Run() { ThrowHelper.Fail(); } } }\n"},
    {"Memory/src/Memory.csproj", "<Project></Project>\n"},
    {"Memory/src/ThrowHelper.cs",
     "namespace Shared { public static class ThrowHelper { public static void Fail() {} } }\n"},
    {"Memory/src/Slice.cs",
     "namespace Shared { public class Slice { void Run() { ThrowHelper.Fail(); } } }\n"},
    {"Floating/Loose.cs",
     "namespace Shared { public class Loose { void Run() { ThrowHelper.Fail(); } } }\n"},
};

static int oo_csharp_lookup_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, CSHARP_LOOKUP_FILES,
                 (int)(sizeof(CSHARP_LOOKUP_FILES) / sizeof(CSHARP_LOOKUP_FILES[0])), parallel,
                 "cs")) {
        oo_close(&index);
        return 1;
    }
    /* An interface's members are no class's. Through the interface they are the only ones. */
    OO_EXPECT("CALLS", "Use.ViaClass", "Dict.Put", 1);
    OO_EXPECT("CALLS", "Use.ViaClass", "IOld.Put", 0);
    OO_EXPECT("CALLS", "Use.ViaInterface", "IOld.Put", 1);
    OO_EXPECT("CALLS", "Use.ViaInterface", "Dict.Put", 0);
    /* An explicit implementation is no member: Size is the public one. */
    OO_EXPECT("CALLS", "Use.Explicit", "Doc.Go", 1);
    OO_EXPECT("CALLS", "Use.Explicit", "Other.Go", 0);
    /* A more derived applicable method hides the base's. Not applicable, or not known to be: */
    OO_EXPECT("CALLS", "Use.Derives", "Derived.Log", 1);
    OO_EXPECT("CALLS", "Use.Derives", "Base.Log", 0);
    OO_EXPECT("CALLS", "Use.BaseOnly", "Base.Log", 1);
    OO_EXPECT("CALLS", "Use.BaseOnly", "Derived.Log", 0);
    OO_EXPECT("CALLS", "Use.UnknownArg", "Derived.Log", 0);
    OO_EXPECT("CALLS", "Use.UnknownArg", "Base.Log", 0);
    /* An indexer's declared result, by its argument. */
    OO_EXPECT("CALLS", "Use.Indexed", "Doc.Go", 1);
    OO_EXPECT("CALLS", "Use.Indexed", "Other.Go", 0);
    OO_EXPECT("CALLS", "Use.IndexedByInt", "Other.Go", 1);
    OO_EXPECT("CALLS", "Use.IndexedByInt", "Doc.Go", 0);
    /* Type arguments a call spells, and the namespace root. */
    OO_EXPECT("CALLS", "Use.Spelled", "Doc.Go", 1);
    OO_EXPECT("CALLS", "Use.Rooted", "Lib.Run", 1);
    /* An identical parameter is the better conversion, across a partial type's files. */
    OO_EXPECT("CALLS", "Use.FloatLiteral", "Writer.Float.Writer.Put", 1);
    OO_EXPECT("CALLS", "Use.FloatLiteral", "Writer.Double.Writer.Put", 0);
    OO_EXPECT("CALLS", "Use.DoubleLiteral", "Writer.Double.Writer.Put", 1);
    OO_EXPECT("CALLS", "Use.DoubleLiteral", "Writer.Float.Writer.Put", 0);
    OO_EXPECT("CALLS", "Use.UnknownLiteral", "Writer.Float.Writer.Put", 0);
    OO_EXPECT("CALLS", "Use.UnknownLiteral", "Writer.Double.Writer.Put", 0);
    OO_EXPECT("CALLS", "Use.OutBool", "Writer.Float.Writer.Parse", 1);
    OO_EXPECT("CALLS", "Use.OutInt", "Writer.Double.Writer.Parse", 1);
    /* The core library's own types, where the project declares them. */
    OO_EXPECT("CALLS", "Use.TypeOf", "Type.GetMethod", 1);
    OO_EXPECT("CALLS", "Use.ArrayMember", "Array.GetValue", 1);
    OO_EXPECT("CALLS", "Use.ObjectMember", "Object.GetType", 1);
    OO_EXPECT("CALLS", "Use.BareObject", "Object.ReferenceEquals", 1);
    OO_EXPECT("CALLS", "Use.StructMember", "Object.ToString", 0);
    OO_EXPECT("CALLS", "Use.StructMember", "ValueType.ToString", 1);
    OO_EXPECT("CALLS", "Use.EnumMember", "Enum.HasFlag", 1);
    OO_EXPECT("CALLS", "Use.Awaited", "Doc.Go", 1);
    /* A string on either side of `+` is a string. Two ints are not. */
    OO_EXPECT("CALLS", "Use.Concatenated", "Text.Show", 1);
    OO_EXPECT("CALLS", "Use.Sums", "Text.Show", 0);
    /* A generic parameter takes the argument as it is; a user conversion does not. */
    OO_EXPECT("CALLS", "Use.GenericWins", "Sink.Generic.Sink.Add", 1);
    OO_EXPECT("CALLS", "Use.GenericWins", "Types.Sink.Add", 0);
    /* An array reaches a span of its own element only. */
    OO_EXPECT("CALLS", "Use.BytesToSpan", "Types.Spans.Write", 1);
    OO_EXPECT("CALLS", "Use.BytesToSpan", "Spans.Chars.Spans.Write", 0);
    OO_EXPECT("CALLS", "Use.CharsToSpan", "Spans.Chars.Spans.Write", 1);
    OO_EXPECT("CALLS", "Use.CharsToSpan", "Types.Spans.Write", 0);
    /* A pattern binds the type it spells, or what it is matched against. */
    OO_EXPECT("CALLS", "Use.EmptyPattern", "Doc.Go", 1);
    OO_EXPECT("CALLS", "Use.TypedPattern", "Doc.Go", 1);
    OO_EXPECT("CALLS", "Use.TypeNotShadowed", "Doc.Go", 1);
    /* A shared project is a project: each reader means the one its project compiles. */
    OO_EXPECT("CALLS", "Array2.Run", "Core.src.ThrowHelper.ThrowHelper.Fail", 1);
    OO_EXPECT("CALLS", "Array2.Run", "Memory.src.ThrowHelper.ThrowHelper.Fail", 0);
    OO_EXPECT("CALLS", "Slice.Run", "Memory.src.ThrowHelper.ThrowHelper.Fail", 1);
    OO_EXPECT("CALLS", "Slice.Run", "Core.src.ThrowHelper.ThrowHelper.Fail", 0);
    /* In no project: two declarations, no way to choose. */
    OO_EXPECT("CALLS", "Loose.Run", "Core.src.ThrowHelper.ThrowHelper.Fail", 0);
    OO_EXPECT("CALLS", "Loose.Run", "Memory.src.ThrowHelper.ThrowHelper.Fail", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_csharp_member_lookup) {
    ASSERT_EQ(oo_csharp_lookup_check(false), 0);
    PASS();
}

TEST(resolver_oo_csharp_member_lookup_parallel) {
    ASSERT_EQ(oo_csharp_lookup_check(true), 0);
    PASS();
}

/* What a JDK factory holds is spelled by its arguments, and Class<T> by its class literal. */
static const OOFile JAVA_FACTORY_FILES[] = {
    {"acme/Doc.java", "package acme;\npublic class Doc { public void go() {} }\n"},
    {"acme/Other.java", "package acme;\npublic class Other { public void go() {} }\n"},
    {"acme/Holder.java",
     "package acme;\nimport java.util.List;\n"
     "public class Holder { public List<Doc> docs() { return null; } }\n"},
    {"acme/Registry.java",
     "package acme;\nimport java.util.stream.Stream;\n"
     "public class Registry {\n"
     "    public <T> T find(Class<T> type) { return null; }\n"
     "    public <T> Stream<T> all(Class<T> type) { return null; }\n"
     "}\n"},
    {"acme/Use.java",
     "package acme;\n"
     "import java.util.Arrays;\nimport java.util.List;\nimport java.util.Optional;\n"
     "public class Use {\n"
     "    void viaOptional(Doc d) { Optional.ofNullable(d).ifPresent(x -> x.go()); }\n"
     "    void viaListOf(Doc d) { List.of(d).forEach(x -> x.go()); }\n"
     "    void viaArrays(Doc[] items) { Arrays.stream(items).forEach(x -> x.go()); }\n"
     "    void disagreeing(Doc d, Other o) { List.of(d, o).forEach(x -> x.go()); }\n"
     "    void viaFlatMap(List<Holder> holders) {\n"
     "        holders.stream().flatMap(h -> h.docs().stream()).forEach(x -> x.go());\n"
     "    }\n"
     "    void viaClass(Registry r) { r.find(Doc.class).go(); }\n"
     "    void viaClassStream(Registry r) { r.all(Other.class).forEach(x -> x.go()); }\n"
     "}\n"},
    {"plain/Optional.java",
     "package plain;\npublic class Optional { public static Other ofNullable(Object o) { return null; } }\n"},
    {"plain/Other.java", "package plain;\npublic class Other { public void go() {} }\n"},
    {"plain/Use2.java",
     "package plain;\n"
     "public class Use2 { void own(acme.Doc d) { Optional.ofNullable(d).go(); } }\n"},
};

static int oo_java_factory_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, JAVA_FACTORY_FILES,
                 (int)(sizeof(JAVA_FACTORY_FILES) / sizeof(JAVA_FACTORY_FILES[0])), parallel,
                 "java")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("CALLS", "Use.viaOptional", "acme.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaListOf", "acme.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaArrays", "acme.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.disagreeing", "acme.Doc.go", 0);
    OO_EXPECT("CALLS", "Use.disagreeing", "acme.Other.go", 0);
    OO_EXPECT("CALLS", "Use.viaFlatMap", "acme.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaClass", "acme.Doc.go", 1);
    OO_EXPECT("CALLS", "Use.viaClassStream", "acme.Other.go", 1);
    /* A project's own Optional is itself, not the JDK's. */
    OO_EXPECT("CALLS", "Use2.own", "plain.Other.go", 1);
    OO_EXPECT("CALLS", "Use2.own", "acme.Doc.go", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_java_factories_and_class_literals) {
    ASSERT_EQ(oo_java_factory_check(false), 0);
    PASS();
}

TEST(resolver_oo_java_factories_and_class_literals_parallel) {
    ASSERT_EQ(oo_java_factory_check(true), 0);
    PASS();
}

/* A lambda's receiver is what settles it. A type parameter nothing settled says nothing about what
 * the receiver lacks, so a name it does not show may still be its own. */
static const OOFile KOTLIN_RECEIVER_FILES[] = {
    {"lib/Lib.kt",
     "package lib\n"
     "open class Table\n"
     "class Stmt\n"
     "class Tx { val id = 1 }\n"
     "object Users : Table() { val id = 2 }\n"
     "fun <T : Table> T.insert(body: T.(Stmt) -> Unit) {}\n"
     "fun <T : Table> make(body: T.() -> Unit) {}\n"
     "fun <T : Table> T.push(query: Stmt, columns: List<Int>? = null): Int = 0\n"
     "fun <T : Table> T.push(body: T.(Stmt) -> Unit): Stmt = Stmt()\n"
     "fun tx(body: Tx.() -> Unit) {}\n"},
    {"app/Main.kt",
     "package app\n"
     "import lib.*\n"
     "fun settled() { tx { Users.insert { println(id) } } }\n"
     "fun unsettled() { tx { make<Users> { println(id) } } }\n"
     "fun plain() { tx { println(id) } }\n"
     "fun overloaded() { tx { Users.push { println(id) } } }\n"
     "fun anonymous() {\n"
     "    val tester = object : Table() { val id = 3 }\n"
     "    tx { tester.insert { println(id) } }\n"
     "}\n"},
};

static const OOFile CSHARP_INITIALIZER_FILES[] = {
    {"Init/Options.cs",
     "namespace Init {\n"
     "    public class Item {}\n"
     "    public class Options { public System.Collections.Generic.List<Item> Items; public string Name; }\n"
     "    public class Use {\n"
     "        Options Make() { return new Options { Items = { new Item() }, Name = \"n\" }; }\n"
     "    }\n"
     "}\n"},
};

static int oo_receiver_check(bool parallel) {
    OOIndex index;
    int failed = 0;
    if (!oo_open(&index, KOTLIN_RECEIVER_FILES,
                 (int)(sizeof(KOTLIN_RECEIVER_FILES) / sizeof(KOTLIN_RECEIVER_FILES[0])), parallel,
                 "kt")) {
        oo_close(&index);
        return 1;
    }
    OO_EXPECT("USAGE", "Main.settled", "Users.id", 1);
    OO_EXPECT("USAGE", "Main.settled", "Tx.id", 0);
    OO_EXPECT("USAGE", "Main.unsettled", "Tx.id", 0);
    OO_EXPECT("USAGE", "Main.plain", "Tx.id", 1);
    /* A closing lambda is no List: of the overloads on one node, the function type is the one. */
    OO_EXPECT("USAGE", "Main.overloaded", "Users.id", 1);
    OO_EXPECT("USAGE", "Main.overloaded", "Tx.id", 0);
    /* An anonymous object's own id: not the transaction's, though the object's supertype lacks it. */
    OO_EXPECT("USAGE", "Main.anonymous", "Tx.id", 0);
    oo_close(&index);
    if (!oo_open(&index, CSHARP_INITIALIZER_FILES,
                 (int)(sizeof(CSHARP_INITIALIZER_FILES) / sizeof(CSHARP_INITIALIZER_FILES[0])),
                 parallel, "cs")) {
        oo_close(&index);
        return failed + 1;
    }
    OO_EXPECT("WRITES", "Use.Make", "Options.Name", 1);
    OO_EXPECT("WRITES", "Use.Make", "Options.Items", 0);
    oo_close(&index);
    return failed;
}

TEST(resolver_oo_lambda_receivers_and_collection_initializers) {
    ASSERT_EQ(oo_receiver_check(false), 0);
    PASS();
}

TEST(resolver_oo_lambda_receivers_and_collection_initializers_parallel) {
    ASSERT_EQ(oo_receiver_check(true), 0);
    PASS();
}

static const OOFile MIXED_FILES[] = {
    {"native/Cargo.toml", "[package]\nname = \"native\"\nversion = \"0.1.0\"\n"},
    {"native/src/lib.rs", "pub fn f() -> i32 { 1 }\n"},
    {"org/acme/core/Util.java", "package org.acme.core;\npublic class Util {}\n"},
    {"org/acme/app/Main.java",
     "package org.acme.app;\nimport org.acme.core.Util;\npublic class Main {\n}\n"},
};

TEST(resolver_oo_rust_context_failure_keeps_java_imports) {
    OOIndex index;
    int failed = 0;
    ASSERT(oo_open(&index, MIXED_FILES, (int)(sizeof(MIXED_FILES) / sizeof(MIXED_FILES[0])), false,
                   "java"));
    OO_EXPECT("IMPORTS", "Main.java.__file__", "Util.java.__file__", 1);
    oo_close(&index);
    ASSERT_EQ(failed, 0);
    PASS();
}

/* Over the memory budget every result is parked on disk and read back only for a moment.
 * The declaration index outlives each read, so it must own the names it keeps: it held
 * pointers into the freed result, and a resolve worker read them. */
TEST(resolver_oo_declaration_index_outlives_a_spilled_result) {
    char root[256];
    snprintf(root, sizeof(root), "/tmp/cbm_oo_spill_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(root));
    char repo[512], database[512];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    bool ok = cbm_mkdir_p(repo, 0755) == true;
    static const RQ_Source sources[] = {
        {.path = "app/Store.java",
         .text = "package app;\nclass Store {\n  static int count;\n  int read() { return count; }\n}\n"},
        {.path = "app/Use.java",
         .text = "package app;\nclass Use {\n  int run(Store store) { return store.read(); }\n}\n"}};
    for (size_t i = 0; ok && i < sizeof(sources) / sizeof(sources[0]); i++)
        ok = th_write_file(TH_PATH(repo, sources[i].path), sources[i].text) == 0;
    for (int i = 0; ok && i < 55; i++) {
        char path[40];
        snprintf(path, sizeof(path), "SpillPadding%02d.java", i);
        ok = th_write_file(TH_PATH(repo, path), "// unrelated source file\n") == 0;
    }
    static const RQ_Expected expected[] = {
        {.case_id = "spilled-member-call",
         .site = {.file = "app/Use.java", .caller = "app.Use.run", .needle = "store.read()"},
         .relation = "CALLS",
         .target = {.file = "app/Store.java",
                    .suffix = "app.Store.read",
                    .label = "Method",
                    .declaration_line = 4,
                    .declaration_text = "  int read() { return count; }"},
         .mode = RQ_REQUIRED},
        {.case_id = "spilled-static-field",
         .site = {.file = "app/Store.java", .caller = "app.Store.read", .needle = "count", .occurrence_index = 1},
         .relation = "USAGE",
         .target = {.file = "app/Store.java",
                    .suffix = "app.Store.count",
                    .label = "Field",
                    .declaration_line = 3,
                    .declaration_text = "  static int count;"},
         .mode = RQ_REQUIRED}};
    cbm_setenv("CBM_MEM_SPILL", "1", 1);
    cbm_setenv("CBM_WORKERS", "4", 1);
    cbm_pipeline_t *pipeline = ok ? cbm_pipeline_new(repo, database, CBM_MODE_FULL) : NULL;
    ok = pipeline && cbm_pipeline_set_project_name(pipeline, "oo-spill") &&
         cbm_pipeline_run(pipeline) == 0;
    cbm_pipeline_free(pipeline);
    cbm_unsetenv("CBM_WORKERS");
    cbm_unsetenv("CBM_MEM_SPILL");
    cbm_store_t *store = ok ? cbm_store_open_path_existing(database) : NULL;
    RQ_Report report = {0};
    ok = store && rq_check(store, "oo-spill", "oo-spill", sources, 2, expected, 2, NULL, 0, &report,
                           stderr);
    if (store)
        cbm_store_close(store);
    th_cleanup(root);
    ASSERT(ok);
    ASSERT_EQ(report.found, 2);
    PASS();
}

SUITE(resolver_oo) {
    RUN_TEST(resolver_oo_declaration_index_outlives_a_spilled_result);
    RUN_TEST(resolver_oo_long_import_list_still_binds);
    RUN_TEST(resolver_oo_java_static_selection);
    RUN_TEST(resolver_oo_java_static_selection_parallel);
    RUN_TEST(resolver_oo_kotlin_static_selection);
    RUN_TEST(resolver_oo_kotlin_static_selection_parallel);
    RUN_TEST(resolver_oo_csharp_static_selection);
    RUN_TEST(resolver_oo_csharp_static_selection_parallel);
    RUN_TEST(resolver_oo_rust_context_failure_keeps_java_imports);
    RUN_TEST(resolver_oo_large_file_binds_its_last_site);
    RUN_TEST(resolver_oo_csharp_repaired_parse_and_exact_overloads);
    RUN_TEST(resolver_oo_csharp_repaired_parse_and_exact_overloads_parallel);
    RUN_TEST(resolver_oo_csharp_reference_assembly_stands_behind);
    RUN_TEST(resolver_oo_csharp_reference_assembly_stands_behind_parallel);
    RUN_TEST(resolver_oo_kotlin_suspend_function_type);
    RUN_TEST(resolver_oo_kotlin_suspend_function_type_parallel);
    RUN_TEST(resolver_oo_java_class_level_and_generic_type_mentions);
    RUN_TEST(resolver_oo_java_class_level_and_generic_type_mentions_parallel);
    RUN_TEST(resolver_oo_csharp_generic_type_mentions);
    RUN_TEST(resolver_oo_csharp_generic_type_mentions_parallel);
    RUN_TEST(resolver_oo_java_lambda_parameters_from_the_selected_declaration);
    RUN_TEST(resolver_oo_java_lambda_parameters_from_the_selected_declaration_parallel);
    RUN_TEST(resolver_oo_csharp_conversions_lambdas_and_keyword_types);
    RUN_TEST(resolver_oo_csharp_conversions_lambdas_and_keyword_types_parallel);
    RUN_TEST(resolver_oo_csharp_system_delegates_without_a_core_library);
    RUN_TEST(resolver_oo_csharp_system_delegates_without_a_core_library_parallel);
    RUN_TEST(resolver_oo_kotlin_same_name_in_two_projects);
    RUN_TEST(resolver_oo_kotlin_same_name_in_two_projects_parallel);
    RUN_TEST(resolver_oo_csharp_same_name_in_one_file_and_one_project);
    RUN_TEST(resolver_oo_csharp_same_name_in_one_file_and_one_project_parallel);
    RUN_TEST(resolver_oo_java_same_name_in_two_projects);
    RUN_TEST(resolver_oo_java_same_name_in_two_projects_parallel);
    RUN_TEST(resolver_oo_kotlin_anonymous_objects_destructuring_named_lambdas);
    RUN_TEST(resolver_oo_kotlin_anonymous_objects_destructuring_named_lambdas_parallel);
    RUN_TEST(resolver_oo_csharp_syntax_that_loses_a_whole_file);
    RUN_TEST(resolver_oo_csharp_syntax_that_loses_a_whole_file_parallel);
    RUN_TEST(resolver_oo_csharp_extension_receivers_partial_types_attributes);
    RUN_TEST(resolver_oo_csharp_extension_receivers_partial_types_attributes_parallel);
    RUN_TEST(resolver_oo_annotations_name_their_type);
    RUN_TEST(resolver_oo_annotations_name_their_type_parallel);
    RUN_TEST(resolver_oo_csharp_syntax_the_grammar_lacks);
    RUN_TEST(resolver_oo_csharp_syntax_the_grammar_lacks_parallel);
    RUN_TEST(resolver_oo_callers_without_a_node_of_their_own);
    RUN_TEST(resolver_oo_callers_without_a_node_of_their_own_parallel);
    RUN_TEST(resolver_oo_csharp_member_lookup);
    RUN_TEST(resolver_oo_csharp_member_lookup_parallel);
    RUN_TEST(resolver_oo_java_factories_and_class_literals);
    RUN_TEST(resolver_oo_java_factories_and_class_literals_parallel);
    RUN_TEST(resolver_oo_lambda_receivers_and_collection_initializers);
    RUN_TEST(resolver_oo_lambda_receivers_and_collection_initializers_parallel);
}
