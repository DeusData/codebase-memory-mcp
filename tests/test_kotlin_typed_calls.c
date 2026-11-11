/*
 * test_kotlin_typed_calls.c — Kotlin member calls through a class PROPERTY
 * (primary-constructor `val`, body property, `this.prop`) bind to the
 * property's declared type, on both resolver venues (#1555).
 *
 * The cross-file Kotlin walk resolved a typed method PARAMETER by type, but
 * a property-typed receiver fell to the name guess: the cross registry held
 * the class from extraction defs (properties untyped, constructor vals
 * absent), and the walk's typed AST registration of the same class was
 * shadowed by that earlier entry. Each fixture call has a same-named decoy
 * method on another type, so a name guess is visible as a wrong target:
 *   - Service.log: `audit.inserir()` on `audit: Audit`, next to
 *     `repository: Repo` whose interface also declares inserir (the
 *     sequential venue's guess picks Repo.inserir);
 *   - two bounded contexts whose use cases both inject `repository`
 *     (the parallel venue's receiver-name guess bound the feriado use case
 *     to CompromissoRepository.inserir, the #1555 report).
 * One fixture, padded past MIN_FILES_FOR_PARALLEL, is indexed through the
 * sequential resolver (CBM_INDEX_SINGLE_THREAD=1) and the parallel one.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "sqlite3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const KTC_FILES[][2] = {
    {"src/main/kotlin/com/acme/a/Repo.kt",
     "package com.acme.a\n\ninterface Repo {\n    fun inserir(x: String): Long\n}\n"},
    {"src/main/kotlin/com/acme/b/Audit.kt",
     "package com.acme.b\n\nclass Audit {\n    fun inserir(x: String): Long = 0L\n}\n"},
    {"src/main/kotlin/com/acme/app/Service.kt",
     "package com.acme.app\n\nimport com.acme.a.Repo\nimport com.acme.b.Audit\n\n"
     "class Service(private val repository: Repo, private val audit: Audit) {\n"
     "    fun save(x: String): Long = repository.inserir(x)\n"
     "    fun log(x: String): Long = audit.inserir(x)\n}\n\n"
     "class Holder {\n    private val audit: Audit = Audit()\n"
     "    fun hold(x: String): Long = this.audit.inserir(x)\n}\n"},
    {"src/main/kotlin/com/acme/agenda/domain/CompromissoRepository.kt",
     "package com.acme.agenda.domain\n\ninterface CompromissoRepository {\n"
     "    fun inserir(titulo: String): Long\n}\n"},
    {"src/main/kotlin/com/acme/feriado/domain/FeriadoRepository.kt",
     "package com.acme.feriado.domain\n\ninterface FeriadoRepository {\n"
     "    fun inserir(nome: String): Long\n}\n"},
    {"src/main/kotlin/com/acme/agenda/app/CriarCompromissoUseCase.kt",
     "package com.acme.agenda.app\n\nimport com.acme.agenda.domain.CompromissoRepository\n\n"
     "class CriarCompromissoUseCase(private val repository: CompromissoRepository) {\n"
     "    fun executar(titulo: String): Long = repository.inserir(titulo)\n}\n"},
    {"src/main/kotlin/com/acme/feriado/app/CriarFeriadoUseCase.kt",
     "package com.acme.feriado.app\n\nimport com.acme.feriado.domain.FeriadoRepository\n\n"
     "class CriarFeriadoUseCase(private val repository: FeriadoRepository) {\n"
     "    fun executar(nome: String): Long = repository.inserir(nome)\n}\n"},
    /* Guard: inside an extension function, a bare call names the receiver's
     * MEMBER before a same-named extension of the same file (Kotlin
     * resolution); main gets this right and the own-file overlay must too. */
    {"src/main/kotlin/com/acme/b/Builders.kt",
     "package com.acme.b\n\nclass Builder {\n    fun add(x: Int): Boolean = true\n"
     "    fun put(k: String, v: Int): Int = v\n}\n\n"
     "fun Builder.add(s: String): Boolean = add(s.length)\n\n"
     "fun Builder.putAll(k: String) { put(k, 1) }\n\n"
     "fun Builder.put(k: String): Int = put(k, 0)\n"},
    /* kotlin.Any members on a receiver typed by ANOTHER file. The per-file
     * pass cannot type `type`, so it emits the kotlin.Any placeholder; the
     * cross pass names the declared override. Both rows at one occurrence
     * were a tombstone in the call join and the call fell to a name guess
     * (AllTypes is the decoy). A class with no override keeps kotlin.Any, and
     * so does a stdlib receiver type (`count: Int`), whose seeded
     * kotlin.Int.toString names no node and left the call to the guess. */
    {"src/main/kotlin/com/acme/c/Proto.kt",
     "package com.acme.c\n\nclass ProtoType {\n    private val s: String = \"\"\n"
     "    override fun hashCode() = s.length\n\n    override fun toString() = s\n}\n\n"
     "class Plain(val n: Int)\n"},
    {"src/main/kotlin/com/acme/d/Member.kt",
     "package com.acme.d\n\nimport com.acme.c.Plain\nimport com.acme.c.ProtoType\n\n"
     "class Member(val type: ProtoType, val plain: Plain, val count: Int) {\n"
     "    override fun hashCode() = type.hashCode() * 37\n\n"
     "    fun label(): String = type.toString()\n\n"
     "    fun plainHash(): Int = plain.hashCode()\n\n"
     "    fun countText(): String = count.toString()\n}\n"},
    {"src/main/kotlin/com/acme/e/AllTypes.kt",
     "package com.acme.e\n\nclass AllTypes {\n    override fun hashCode(): Int = 1\n\n"
     "    override fun toString(): String = \"all\"\n}\n"},
    /* A property READ on a now-typed receiver (a nested class's property, whose
     * QN the graph spells differently): it must keep the property's own node
     * (USAGE) and never become a CALL_REFERENCE to a same-named function in
     * another file (okhttp `server.webSocket` -> a test fun `webSocket`). */
    {"src/main/kotlin/com/acme/w/SocketTest.kt",
     "package com.acme.w\n\nclass SocketTest {\n"
     "    private val server: Streams = Streams()\n\n"
     "    fun loop(): Boolean {\n        return server.webSocket != null\n    }\n\n"
     "    class Streams {\n        var webSocket: String? = null\n    }\n}\n"},
    {"src/main/kotlin/com/acme/w2/ModernTest.kt",
     "package com.acme.w2\n\nclass ModernTest {\n    fun webSocket(): Int = 1\n}\n"},
    /* A constructor val next to a same-named member function: a bare
     * `handshake()` calls the function (the String? value has no invoke), as
     * main resolves it; typing the val must not shadow the member (okhttp
     * RealConnection). Other.handshake is the decoy a guess would hit. */
    {"src/main/kotlin/com/acme/h/Conn.kt",
     "package com.acme.h\n\ninterface Connection {\n    fun handshake(): String?\n}\n\n"
     "class RealConnection(private val handshake: String?) : Connection {\n"
     "    override fun handshake(): String? = handshake\n\n"
     "    fun isEligible(): Boolean = check(handshake()!!.length)\n\n"
     "    private fun check(n: Int): Boolean = n > 0\n}\n"},
    {"src/main/kotlin/com/acme/h2/Other.kt",
     "package com.acme.h2\n\nclass Other {\n    fun handshake(): String? = null\n}\n"},
    /* Inside `repo.let { .. }` / `repo.run { .. }` the now-typed receiver Repo
     * becomes the lambda's implicit receiver; the enclosing class's member
     * stays callable bare (Kotlin's implicit-receiver chain). */
    {"src/main/kotlin/com/acme/l/Svc.kt",
     "package com.acme.l\n\nimport com.acme.a.Repo\n\n"
     "class Svc(private val repo: Repo) {\n    fun ping(): Int = 2\n\n"
     "    fun viaLet(): Int = repo.let { ping() }\n\n"
     "    fun viaRun(): Int = repo.run { ping() }\n}\n"},
    {"src/main/kotlin/com/acme/l2/Pinger.kt",
     "package com.acme.l2\n\nclass Pinger {\n    fun ping(): Int = 3\n}\n"},
    /* Guard for the receiver chain: inside a companion object, `Ledger(x)` is
     * a constructor call (here the secondary constructor), never a member
     * method of the enclosing class. */
    {"src/main/kotlin/com/acme/k/Ledger.kt",
     "package com.acme.k\n\nclass Ledger private constructor(val a: String, val b: String) {\n"
     "    constructor(a: String) : this(a, \"gen\")\n\n"
     "    companion object {\n        fun of(x: String) = Ledger(x)\n    }\n}\n"},
};

static int ktc_write_fixture(const char *dir) {
    for (size_t i = 0; i < sizeof(KTC_FILES) / sizeof(KTC_FILES[0]); i++) {
        if (th_write_file(TH_PATH(dir, KTC_FILES[i][0]), KTC_FILES[i][1]) != 0) {
            return -1;
        }
    }
    /* Past MIN_FILES_FOR_PARALLEL (50): the parallel venue is selected. */
    for (int i = 0; i < 55; i++) {
        char rel[96];
        char body[128];
        snprintf(rel, sizeof(rel), "src/main/kotlin/com/acme/pad/Pad%d.kt", i);
        snprintf(body, sizeof(body), "package com.acme.pad\n\nclass Pad%d {\n    fun v%d(): Int = %d\n}\n",
                 i, i, i);
        if (th_write_file(TH_PATH(dir, rel), body) != 0) {
            return -1;
        }
    }
    return 0;
}

/* Target QNs of the CALLS edges whose source QN ends in `.<src_tail>` and whose
 * callee text ends in `.<leaf>`, one per line into out; returns the count. */
static int ktc_targets(sqlite3 *db, const char *src_tail, const char *leaf, char *out,
                       size_t cap) {
    out[0] = '\0';
    static const char sql[] =
        "SELECT t.qualified_name FROM edges e JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'CALLS' "
        "AND s.qualified_name LIKE '%.' || ?1 "
        "AND (json_extract(e.properties, '$.callee') = ?2 "
        "OR json_extract(e.properties, '$.callee') LIKE '%.' || ?2) ORDER BY 1";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(st, 1, src_tail, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, leaf, -1, SQLITE_STATIC);
    int n = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(st, 0);
        size_t used = strlen(out);
        snprintf(out + used, cap - used, "%s\n", qn ? qn : "");
        n++;
    }
    sqlite3_finalize(st);
    return n;
}

/* Exactly one such edge, and its target QN ends in `.<want_tail>`. */
static int ktc_expect(sqlite3 *db, const char *venue, const char *src_tail, const char *leaf,
                      const char *want_tail) {
    char got[2048];
    int n = ktc_targets(db, src_tail, leaf, got, sizeof(got));
    size_t gl = strlen(got);
    size_t wl = strlen(want_tail);
    if (n == 1 && gl > wl + 1 && got[gl - wl - 2] == '.' &&
        strncmp(got + gl - wl - 1, want_tail, wl) == 0) {
        return 1;
    }
    printf("\n  [%s] %s -> .%s: want one edge to ...%s, have %d:\n%s", venue, src_tail, leaf,
           want_tail, n, got);
    return 0;
}

/* ktc_expect, and the edge comes from the resolver (an lsp_* strategy), not a
 * name guess that happens to land on the same target. */
static int ktc_expect_lsp(sqlite3 *db, const char *venue, const char *src_tail, const char *leaf,
                          const char *want_tail) {
    if (!ktc_expect(db, venue, src_tail, leaf, want_tail)) {
        return 0;
    }
    static const char sql[] =
        "SELECT coalesce(json_extract(e.properties, '$.strategy'), '') FROM edges e "
        "JOIN nodes s ON s.id = e.source_id WHERE e.type = 'CALLS' "
        "AND s.qualified_name LIKE '%.' || ?1 "
        "AND (json_extract(e.properties, '$.callee') = ?2 "
        "OR json_extract(e.properties, '$.callee') LIKE '%.' || ?2)";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, 1, src_tail, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, leaf, -1, SQLITE_STATIC);
    char strategy[64] = "";
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        snprintf(strategy, sizeof(strategy), "%s", s ? s : "");
    }
    sqlite3_finalize(st);
    if (strncmp(strategy, "lsp_", 4) == 0) {
        return 1;
    }
    printf("\n  [%s] %s -> .%s: right target, but strategy '%s' (want lsp_*)\n", venue,
           src_tail, leaf, strategy);
    return 0;
}

/* The reference edges (USAGE / CALL_REFERENCE) from the method whose QN ends in
 * `.<src_tail>` to nodes named `name`: exactly one, a USAGE onto a non-callable. */
static int ktc_expect_property_usage(sqlite3 *db, const char *venue, const char *src_tail,
                                     const char *name) {
    static const char sql[] =
        "SELECT e.type || ' ' || t.label || ' ' || t.qualified_name FROM edges e "
        "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
        "WHERE e.type IN ('USAGE', 'CALL_REFERENCE', 'CALLS') "
        "AND s.qualified_name LIKE '%.' || ?1 AND t.name = ?2 ORDER BY 1";
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(st, 1, src_tail, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, name, -1, SQLITE_STATIC);
    char got[1024] = "";
    int n = 0;
    bool ok = false;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *row = (const char *)sqlite3_column_text(st, 0);
        size_t used = strlen(got);
        snprintf(got + used, sizeof(got) - used, "%s\n", row ? row : "");
        ok = row && strncmp(row, "USAGE ", 6) == 0 && strstr(row, " Function ") == NULL &&
             strstr(row, " Method ") == NULL;
        n++;
    }
    sqlite3_finalize(st);
    if (n == 1 && ok) {
        return 1;
    }
    printf("\n  [%s] %s -> %s: want one USAGE onto the property, have %d:\n%s", venue, src_tail,
           name, n, got);
    return 0;
}

static int ktc_index(const char *dir, const char *db_name, bool single_thread, char *db_out,
                     size_t cap) {
    snprintf(db_out, cap, "%s/%s", dir, db_name);
    char *old_single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *saved_single = old_single ? strdup(old_single) : NULL;
    char *old_workers = getenv("CBM_WORKERS");
    char *saved_workers = old_workers ? strdup(old_workers) : NULL;
    if (single_thread) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
        cbm_setenv("CBM_WORKERS", "4", 1);
    }
    int rc = -1;
    cbm_pipeline_t *p = cbm_pipeline_new(dir, db_out, CBM_MODE_FULL);
    if (p) {
        rc = cbm_pipeline_run(p);
        cbm_pipeline_free(p);
    }
    if (saved_single) {
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", saved_single, 1);
        free(saved_single);
    } else {
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    }
    if (saved_workers) {
        cbm_setenv("CBM_WORKERS", saved_workers, 1);
        free(saved_workers);
    } else {
        cbm_unsetenv("CBM_WORKERS");
    }
    return rc;
}

static int ktc_check_venue(const char *dir, const char *venue, bool single_thread) {
    char db_path[512];
    char name[64];
    snprintf(name, sizeof(name), "ktc-%s.db", venue);
    if (ktc_index(dir, name, single_thread, db_path, sizeof(db_path)) != 0) {
        printf("\n  [%s] index failed\n", venue);
        return 0;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return 0;
    }
    int ok = 1;
    /* Constructor vals, each next to a same-named decoy on the other type. */
    ok &= ktc_expect(db, venue, "Service.save", "inserir", "Repo.inserir");
    ok &= ktc_expect(db, venue, "Service.log", "inserir", "Audit.inserir");
    /* Body property read through `this`. */
    ok &= ktc_expect(db, venue, "Holder.hold", "inserir", "Audit.inserir");
    /* #1555: two ports sharing `inserir`, both injected as `repository`. */
    ok &= ktc_expect(db, venue, "CriarFeriadoUseCase.executar", "inserir",
                     "FeriadoRepository.inserir");
    ok &= ktc_expect(db, venue, "CriarCompromissoUseCase.executar", "inserir",
                     "CompromissoRepository.inserir");
    /* Guard: a member wins over a same-named extension of the same file. */
    ok &= ktc_expect(db, venue, "Builders.putAll", "put", "Builder.put");
    ok &= ktc_expect(db, venue, "Builders.add", "add", "Builder.add");
    /* kotlin.Any members: the declared override, else kotlin.Any (external). */
    ok &= ktc_expect_lsp(db, venue, "Member.hashCode", "hashCode", "ProtoType.hashCode");
    ok &= ktc_expect_lsp(db, venue, "Member.label", "toString", "ProtoType.toString");
    ok &= ktc_expect_lsp(db, venue, "Member.plainHash", "hashCode", "Any.hashCode");
    ok &= ktc_expect_lsp(db, venue, "Member.countText", "toString", "Any.toString");
    /* A typed property read keeps the property node, never a same-named function. */
    ok &= ktc_expect_property_usage(db, venue, "SocketTest.loop", "webSocket");
    /* A typed constructor val never shadows the same-named member function. */
    ok &= ktc_expect_lsp(db, venue, "RealConnection.isEligible", "handshake",
                         "RealConnection.handshake");
    /* The enclosing class's member inside a lambda with a typed receiver. */
    ok &= ktc_expect_lsp(db, venue, "Svc.viaLet", "ping", "Svc.ping");
    ok &= ktc_expect_lsp(db, venue, "Svc.viaRun", "ping", "Svc.ping");
    ok &= ktc_expect_lsp(db, venue, "Ledger.kt.__file__", "Ledger", "Ledger.Ledger.Ledger");
    sqlite3_close(db);
    return ok;
}

TEST(kotlin_property_receiver_binds_declared_type_both_venues) {
    char *dir = th_mktempdir("cbm_kt_typed_calls");
    ASSERT_NOT_NULL(dir);
    if (ktc_write_fixture(dir) != 0) {
        th_cleanup(dir);
        FAIL("fixture");
    }
    int seq = ktc_check_venue(dir, "seq", true);
    int par = ktc_check_venue(dir, "par", false);
    th_cleanup(dir);
    ASSERT_TRUE(seq);
    ASSERT_TRUE(par);
    PASS();
}

SUITE(kotlin_typed_calls) {
    RUN_TEST(kotlin_property_receiver_binds_declared_type_both_venues);
}
