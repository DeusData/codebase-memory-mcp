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
     "    fun externalExtension() { text?.shout() }\n"
     "    fun externalWrongReceiver() { text?.onlyInt() }\n"
     "    fun extensionProperty(s: Store) { s.extProp.get() }\n"
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

SUITE(resolver_oo) {
    RUN_TEST(resolver_oo_java_static_selection);
    RUN_TEST(resolver_oo_java_static_selection_parallel);
    RUN_TEST(resolver_oo_kotlin_static_selection);
    RUN_TEST(resolver_oo_kotlin_static_selection_parallel);
    RUN_TEST(resolver_oo_csharp_static_selection);
    RUN_TEST(resolver_oo_csharp_static_selection_parallel);
    RUN_TEST(resolver_oo_rust_context_failure_keeps_java_imports);
}
