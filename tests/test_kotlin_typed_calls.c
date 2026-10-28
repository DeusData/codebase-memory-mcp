/* Kotlin typed receiver quality: preserve the original #1555 fixtures and
 * their same-named decoys. Required explicit-import parameter controls exercise
 * the current exact proof subset in both venues. Unsupported genuine bindings
 * remain counted gaps; wrong targets, roles and callers are bugs.
 *
 * The historical fixture comments below describe pre-proof behavior. In the
 * independent oracle Int.toString is its own override, not Any.toString, and
 * Ledger(x) belongs to the companion function of, never to the File node.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/pipeline.h"
#include "sqlite3.h"
#include "resolver_quality.h"

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

static const char KTC_CONTROL_PATH[] = "src/main/kotlin/com/acme/quality/TypedControls.kt";
static const char KTC_CONTROL[] =
    "package com.acme.quality\n"
    "\n"
    "import com.acme.b.Audit\n"
    "import com.acme.b.Builder\n"
    "\n"
    "fun auditParameter(audit: Audit, text: String): Long {\n"
    "    return audit.inserir(text)\n"
    "}\n"
    "\n"
    "fun builderAddParameter(builder: Builder, number: Int): Boolean {\n"
    "    return builder.add(number)\n"
    "}\n"
    "\n"
    "fun builderPutParameter(builder: Builder, key: String, number: Int): Int {\n"
    "    return builder.put(key, number)\n"
    "}\n";

/* Exact source goldens close the project CALLS/USAGE domains of their callers.
 * Standard-library let/run and arithmetic are outside that bounded project
 * domain; the two explicitly authored external member sites remain exact.
 * Every missing relationship is counted, with WHY and TRIED at its case. */
static const RQ_Expected KTC_EXPECTED[] = {
    {.case_id = "constructor-interface",
     .site = {.file = "src/main/kotlin/com/acme/app/Service.kt",
              .caller = "Service.Service.save",
              .needle = "repository.inserir(x)",
              .occurrence_index = 0,
              .start_byte = 174,
              .end_byte = 195},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/a/Repo.kt",
                .suffix = "Repo.Repo.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(x: String): Long"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-constructor-interface",
     .why = "Primary-constructor property receiver and interface dispatch are outside the complete "
            "exact-owner proof subset.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "constructor-property",
     .site = {.file = "src/main/kotlin/com/acme/app/Service.kt",
              .caller = "Service.Service.log",
              .needle = "audit.inserir(x)",
              .occurrence_index = 0,
              .start_byte = 227,
              .end_byte = 243},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Audit.kt",
                .suffix = "Audit.Audit.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(x: String): Long = 0L"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-constructor-property",
     .why = "The Audit target is exact, but the Service constructor property is not declared into "
            "the proof type environment.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "body-property-this",
     .site = {.file = "src/main/kotlin/com/acme/app/Service.kt",
              .caller = "Service.Holder.hold",
              .needle = "this.audit.inserir(x)",
              .occurrence_index = 0,
              .start_byte = 333,
              .end_byte = 354},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Audit.kt",
                .suffix = "Audit.Audit.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(x: String): Long = 0L"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-body-property-this",
     .why = "Body property through this requires a proven property binding; the owner domain "
            "rejects property-bearing classes.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "feriado-interface",
     .site = {.file = "src/main/kotlin/com/acme/feriado/app/CriarFeriadoUseCase.kt",
              .caller = "CriarFeriadoUseCase.CriarFeriadoUseCase.executar",
              .needle = "repository.inserir(nome)",
              .occurrence_index = 0,
              .start_byte = 190,
              .end_byte = 214},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/feriado/domain/FeriadoRepository.kt",
                .suffix = "FeriadoRepository.FeriadoRepository.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(nome: String): Long"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-feriado-interface",
     .why = "Declared imported interface dispatch through a constructor property lacks receiver "
            "and interface proof.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "agenda-interface",
     .site = {.file = "src/main/kotlin/com/acme/agenda/app/CriarCompromissoUseCase.kt",
              .caller = "CriarCompromissoUseCase.CriarCompromissoUseCase.executar",
              .needle = "repository.inserir(titulo)",
              .occurrence_index = 0,
              .start_byte = 202,
              .end_byte = 228},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/agenda/domain/CompromissoRepository.kt",
                .suffix = "CompromissoRepository.CompromissoRepository.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(titulo: String): Long"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-agenda-interface",
     .why = "Declared imported interface dispatch through a constructor property lacks receiver "
            "and interface proof.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "extension-put-member",
     .site = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
              .caller = "Builders.putAll",
              .needle = "put(k, 1)",
              .occurrence_index = 0,
              .start_byte = 200,
              .end_byte = 209},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
                .suffix = "Builders.Builder.put",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = "    fun put(k: String, v: Int): Int = v"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-extension-put-member",
     .why = "Extension-function implicit receiver priority is intentionally excluded; the "
            "applicable two-argument member is independently declared.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "extension-add-member",
     .site = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
              .caller = "Builders.add",
              .needle = "add(s.length)",
              .occurrence_index = 0,
              .start_byte = 153,
              .end_byte = 166},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
                .suffix = "Builders.Builder.add",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun add(x: Int): Boolean = true"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-extension-add-member",
     .why = "Extension-function receiver priority and String.length argument typing require "
            "separate proof; the Int member wins over the String extension.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "declared-hash-override",
     .site = {.file = "src/main/kotlin/com/acme/d/Member.kt",
              .caller = "Member.Member.hashCode",
              .needle = "type.hashCode()",
              .occurrence_index = 0,
              .start_byte = 173,
              .end_byte = 188},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/c/Proto.kt",
                .suffix = "Proto.ProtoType.hashCode",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = "    override fun hashCode() = s.length"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-declared-hash-override",
     .why = "Constructor property receiver and override declarations are outside the current "
            "plain-owner/callable proof subset.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "declared-string-override",
     .site = {.file = "src/main/kotlin/com/acme/d/Member.kt",
              .caller = "Member.Member.label",
              .needle = "type.toString()",
              .occurrence_index = 0,
              .start_byte = 221,
              .end_byte = 236},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/c/Proto.kt",
                .suffix = "Proto.ProtoType.toString",
                .label = "Method",
                .declaration_line = 7,
                .declaration_text = "    override fun toString() = s"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-declared-string-override",
     .why = "Constructor property receiver and override declarations are outside the current "
            "plain-owner/callable proof subset.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "inherited-any-hash",
     .site = {.file = "src/main/kotlin/com/acme/d/Member.kt",
              .caller = "Member.Member.plainHash",
              .needle = "plain.hashCode()",
              .occurrence_index = 0,
              .start_byte = 265,
              .end_byte = 281},
     .relation = "CALLS",
     .target = {.file = "<kotlin-builtins>",
                .label = "Method",
                .domain = RQ_TARGET_EXTERNAL,
                .external_identity = "kotlin.Any.hashCode"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-inherited-any-hash",
     .why = "Plain inherits Any.hashCode, but constructor-property and inherited external-method "
            "evidence are not admitted.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "int-string-override",
     .site = {.file = "src/main/kotlin/com/acme/d/Member.kt",
              .caller = "Member.Member.countText",
              .needle = "count.toString()",
              .occurrence_index = 0,
              .start_byte = 313,
              .end_byte = 329},
     .relation = "CALLS",
     .target = {.file = "<kotlin-builtins>",
                .label = "Method",
                .domain = RQ_TARGET_EXTERNAL,
                .external_identity = "kotlin.Int.toString"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-int-string-override",
     .why =
         "Int declares its own toString override; its exact external identity is not materialized "
         "and a constructor-property proof is absent. Any.toString is a wrong target.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "nested-property-value",
     .site = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
              .caller = "SocketTest.SocketTest.loop",
              .needle = "webSocket",
              .occurrence_index = 0,
              .start_byte = 132,
              .end_byte = 141},
     .relation = "USAGE",
     .target = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
                .suffix = "SocketTest.Streams.webSocket",
                .label = "Variable",
                .declaration_line = 11,
                .declaration_text = "        var webSocket: String? = null"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-nested-property-value",
     .why = "Nested member property binding lacks strict receiver/value proof; a same-named "
            "function is never a valid substitute.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "nested-property-receiver",
     .site = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
              .caller = "SocketTest.SocketTest.loop",
              .needle = "server",
              .occurrence_index = 1,
              .start_byte = 125,
              .end_byte = 131},
     .relation = "USAGE",
     .target = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
                .suffix = "SocketTest.SocketTest.server",
                .label = "Variable",
                .declaration_line = 4,
                .declaration_text = "    private val server: Streams = Streams()"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-nested-property-receiver",
     .why = "The receiver is itself a read of the exact server property; class-property value "
            "bindings are outside the strict proof subset.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "member-over-value",
     .site = {.file = "src/main/kotlin/com/acme/h/Conn.kt",
              .caller = "Conn.RealConnection.isEligible",
              .needle = "handshake()",
              .occurrence_index = 2,
              .start_byte = 232,
              .end_byte = 243},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/h/Conn.kt",
                .suffix = "Conn.RealConnection.handshake",
                .label = "Method",
                .declaration_line = 8,
                .declaration_text = "    override fun handshake(): String? = handshake"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-member-over-value",
     .why = "The String? constructor property has no invoke; the declared method is genuine, but "
            "inheritance/override owner evidence is excluded.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "nested-check-call",
     .site = {.file = "src/main/kotlin/com/acme/h/Conn.kt",
              .caller = "Conn.RealConnection.isEligible",
              .needle = "check(handshake()!!.length)",
              .occurrence_index = 0,
              .start_byte = 226,
              .end_byte = 253},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/h/Conn.kt",
                .suffix = "Conn.RealConnection.check",
                .label = "Method",
                .declaration_line = 12,
                .declaration_text = "    private fun check(n: Int): Boolean = n > 0"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-nested-check-call",
     .why = "The outer private check call is genuine too; inherited owner context and nested "
            "expression argument proof are unsupported.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "let-enclosing-member",
     .site = {.file = "src/main/kotlin/com/acme/l/Svc.kt",
              .caller = "Svc.Svc.viaLet",
              .needle = "ping()",
              .occurrence_index = 1,
              .start_byte = 140,
              .end_byte = 146},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/l/Svc.kt",
                .suffix = "Svc.Svc.ping",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = "    fun ping(): Int = 2"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-let-enclosing-member",
     .why = "The enclosing Svc member remains callable inside let; lambda/implicit receiver chains "
            "and constructor-property owners are not admitted.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "run-enclosing-member",
     .site = {.file = "src/main/kotlin/com/acme/l/Svc.kt",
              .caller = "Svc.Svc.viaRun",
              .needle = "ping()",
              .occurrence_index = 2,
              .start_byte = 185,
              .end_byte = 191},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/l/Svc.kt",
                .suffix = "Svc.Svc.ping",
                .label = "Method",
                .declaration_line = 6,
                .declaration_text = "    fun ping(): Int = 2"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-run-enclosing-member",
     .why = "The enclosing Svc member remains callable inside run; lambda/implicit receiver chains "
            "and constructor-property owners are not admitted.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "companion-secondary-constructor",
     .site = {.file = "src/main/kotlin/com/acme/k/Ledger.kt",
              .caller = "Ledger.Ledger.of",
              .needle = "Ledger(x)",
              .occurrence_index = 0,
              .start_byte = 181,
              .end_byte = 190},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/k/Ledger.kt",
                .suffix = "Ledger.Ledger.Ledger",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    constructor(a: String) : this(a, \"gen\")"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "kotlin-typed-companion-secondary-constructor",
     .why =
         "The exact secondary constructor is materialized, but anonymous companion method "
         "extraction loses of; File attribution is incorrect and constructor proof is unsupported.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "required-audit-parameter",
     .site = {.file = "src/main/kotlin/com/acme/quality/TypedControls.kt",
              .caller = "TypedControls.auditParameter",
              .needle = "audit.inserir(text)",
              .occurrence_index = 0,
              .start_byte = 143,
              .end_byte = 162},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Audit.kt",
                .suffix = "Audit.Audit.inserir",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun inserir(x: String): Long = 0L"},
     .mode = RQ_REQUIRED,
     .gap_id = NULL,
     .why = "Explicit import, exact final plain class and typed function parameters provide "
            "supported receiver and scalar argument proof.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "required-builder-add-parameter",
     .site = {.file = "src/main/kotlin/com/acme/quality/TypedControls.kt",
              .caller = "TypedControls.builderAddParameter",
              .needle = "builder.add(number)",
              .occurrence_index = 0,
              .start_byte = 243,
              .end_byte = 262},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
                .suffix = "Builders.Builder.add",
                .label = "Method",
                .declaration_line = 4,
                .declaration_text = "    fun add(x: Int): Boolean = true"},
     .mode = RQ_REQUIRED,
     .gap_id = NULL,
     .why = "Explicit import, exact final plain class and typed function parameters provide "
            "supported receiver and scalar argument proof.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."},
    {.case_id = "required-builder-put-parameter",
     .site = {.file = "src/main/kotlin/com/acme/quality/TypedControls.kt",
              .caller = "TypedControls.builderPutParameter",
              .needle = "builder.put(key, number)",
              .occurrence_index = 0,
              .start_byte = 352,
              .end_byte = 376},
     .relation = "CALLS",
     .target = {.file = "src/main/kotlin/com/acme/b/Builders.kt",
                .suffix = "Builders.Builder.put",
                .label = "Method",
                .declaration_line = 5,
                .declaration_text = "    fun put(k: String, v: Int): Int = v"},
     .mode = RQ_REQUIRED,
     .gap_id = NULL,
     .why = "Explicit import, exact final plain class and typed function parameters provide "
            "supported receiver and scalar argument proof.",
     .tried = "Independent source declaration/occurrence audit; current kt_proof_class_fact, "
              "kt_proof_exact_owner and kt_proof_function scope reviewed; original same-named "
              "decoys retained."}};
static const RQ_Forbidden KTC_FORBIDDEN[] = {
    {.case_id = "property-not-calls",
     .site = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
              .caller = "SocketTest.SocketTest.loop",
              .needle = "webSocket",
              .occurrence_index = 0,
              .start_byte = 132,
              .end_byte = 141},
     .relation = "CALLS",
     .any_target = true},
    {.case_id = "property-not-call_reference",
     .site = {.file = "src/main/kotlin/com/acme/w/SocketTest.kt",
              .caller = "SocketTest.SocketTest.loop",
              .needle = "webSocket",
              .occurrence_index = 0,
              .start_byte = 132,
              .end_byte = 141},
     .relation = "CALL_REFERENCE",
     .any_target = true},
    {.case_id = "ledger-not-file-attributed",
     .site = {.file = "src/main/kotlin/com/acme/k/Ledger.kt",
              .caller = "Ledger.kt.__file__",
              .needle = "Ledger(x)",
              .occurrence_index = 0,
              .start_byte = 181,
              .end_byte = 190},
     .relation = "CALLS",
     .any_target = true}};

static int ktc_write_fixture(const char *dir) {
    for (size_t i = 0; i < sizeof(KTC_FILES) / sizeof(KTC_FILES[0]); i++) {
        if (th_write_file(TH_PATH(dir, KTC_FILES[i][0]), KTC_FILES[i][1]) != 0) {
            return -1;
        }
    }
    if (th_write_file(TH_PATH(dir, KTC_CONTROL_PATH), KTC_CONTROL) != 0)
        return -1;
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
        rc = cbm_pipeline_set_project_name(p, "ktc-quality") ? cbm_pipeline_run(p) : -1;
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
    cbm_store_t *store = cbm_store_open_path_existing(db_path);
    if (!store)
        return 0;
    RQ_Source sources[sizeof(KTC_FILES) / sizeof(KTC_FILES[0]) + 1] = {0};
    size_t count = sizeof(KTC_FILES) / sizeof(KTC_FILES[0]);
    for (size_t i = 0; i < count; i++)
        sources[i] = (RQ_Source){.path = KTC_FILES[i][0], .text = KTC_FILES[i][1]};
    sources[count++] = (RQ_Source){.path = KTC_CONTROL_PATH, .text = KTC_CONTROL};
    RQ_Report report;
    bool ok = rq_check_project_declarations(
        store, "ktc-quality", venue, sources, count, KTC_EXPECTED,
        sizeof(KTC_EXPECTED) / sizeof(KTC_EXPECTED[0]), KTC_FORBIDDEN,
        sizeof(KTC_FORBIDDEN) / sizeof(KTC_FORBIDDEN[0]), &report, stderr);
    cbm_store_close(store);
    return ok && report.genuine == 21 && report.found >= 3 && !report.required_missing;
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

/* The source scope walker retains the outer Ledger class across the unnamed
 * companion. Assert the authored raw owner/constructor identity independently
 * of the persisted graph, whose missing companion method is a quality gap. */
TEST(kotlin_typed_ledger_raw_owner_and_constructor_identity) {
    const char *path = "src/main/kotlin/com/acme/k/Ledger.kt";
    const char *source = NULL;
    for (size_t i = 0; i < sizeof(KTC_FILES) / sizeof(KTC_FILES[0]); i++)
        if (!strcmp(KTC_FILES[i][0], path))
            source = KTC_FILES[i][1];
    ASSERT_NOT_NULL(source);
    const char *site = strstr(source, "Ledger(x)");
    ASSERT_NOT_NULL(site);
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_KOTLIN,
                                             "ktc-raw", path, 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    size_t calls = 0, constructors = 0;
    bool owner = true;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->site_start_byte == (uint32_t)(site - source) &&
            call->site_end_byte == (uint32_t)(site - source + strlen("Ledger(x)"))) {
            calls++;
            owner = owner && rq_equal(call->enclosing_func_qn,
                                      "ktc-raw.src.main.kotlin.com.acme.k.Ledger.Ledger.of");
        }
    }
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (rq_equal(def->label, "Method") && def->start_line == 4 &&
            rq_suffix(def->qualified_name, def->name, "Ledger.Ledger.Ledger"))
            constructors++;
    }
    cbm_free_result(result);
    ASSERT_EQ(calls, 1);
    ASSERT_TRUE(owner);
    ASSERT_EQ(constructors, 1);
    PASS();
}

/* External expected identities cannot borrow a project declaration or a
 * universal namesake. This checks the bounded oracle extension itself. */
TEST(kotlin_typed_external_expected_identity_is_exact) {
    RQ_Target target = {.file = "<kotlin-builtins>",
                        .label = "Method",
                        .domain = RQ_TARGET_EXTERNAL,
                        .external_identity = "kotlin.Int.toString"};
    ASSERT_TRUE(rq_external_target(&target));
    target.external_identity = "kotlin.Any.hashCode";
    ASSERT_TRUE(rq_external_target(&target));
    target.external_identity = "kotlin.Any.toString";
    ASSERT_FALSE(rq_external_target(&target));
    target.external_identity = "kotlin.Int.toString";
    target.file = "src/main/kotlin/com/acme/d/Member.kt";
    ASSERT_FALSE(rq_external_target(&target));
    target.file = "<kotlin-builtins>";
    target.label = "Class";
    ASSERT_FALSE(rq_external_target(&target));
    target.label = "Method";
    target.suffix = "Int.toString";
    ASSERT_FALSE(rq_external_target(&target));
    PASS();
}

SUITE(kotlin_typed_calls) {
    RUN_TEST(kotlin_property_receiver_binds_declared_type_both_venues);
    RUN_TEST(kotlin_typed_external_expected_identity_is_exact);
    RUN_TEST(kotlin_typed_ledger_raw_owner_and_constructor_identity);
}
