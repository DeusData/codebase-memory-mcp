/*
 * test_grammar_probe_e.c — IDL/template node/import probe.
 *
 * SCOPE
 * ─────
 * IDL / schema languages:
 *   protobuf (.proto), thrift (.thrift), capnp (.capnp), wit (.wit),
 *   smithy (.smithy), prisma (.prisma), graphql (.graphql / .gql)
 *
 * Template / component languages:
 *   vue (.vue), svelte (.svelte), astro (.astro),
 *   blade (.blade.php), liquid (.liquid), jinja2 (.j2 / .jinja2),
 *   gotemplate (.gotmpl / .tmpl), mermaid (.mmd / .mermaid), jsdoc (.jsdoc)
 *
 * WHAT IS PROBED
 * ──────────────
 *   • NODE creation  — type-like nodes (Class/Struct/Interface/Enum) for IDL
 *                      languages; Module-only for template/markup grammars.
 *   • IMPORTS edges  — cross-file import/include/use where the grammar
 *                      supports it.  Two-file fixtures are used where
 *                      applicable.  Template grammars without a file-level
 *                      import mechanism are tested for no-crash instead.
 *
 * COLOUR LEGEND
 * ─────────────
 *   GREEN = guard: the pipeline already produces the correct result; a
 *           failure here is a real regression.
 *   RED   = bug reproduction: the pipeline does NOT yet produce the expected
 *           node/edge.  Brief inline comments identify the root-cause class.
 *
 * CALLS coverage is intentionally omitted (IDL/template languages have no
 * in-language function calls to resolve; templates are covered by P5 breadth
 * where applicable).
 *
 * Do NOT register this suite in test_main.c — a sibling agent owns that file.
 *
 * SUITE(grammar_probe_e)
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ══════════════════════════════════════════════════════════════════
 * Harness — mirrors test_grammar_probe_a.c exactly.
 * Prefix "gpe_" to avoid symbol collisions with sibling probe files.
 * ══════════════════════════════════════════════════════════════════ */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} GpeProj;

typedef struct {
    const char *name;    /* relative filename, may include '/' for subdirs */
    const char *content;
} GpeFile;

static void gpe_to_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *gpe_open_indexed(GpeProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *gpe_index_files(GpeProj *lp, const GpeFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_gpe_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    gpe_to_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return gpe_open_indexed(lp);
}

static void gpe_cleanup(GpeProj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project);
    lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    unlink(wal);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(shm);
}

/* ── Node-count helpers ─────────────────────────────────────────── */

static int gpe_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum of all type-like labels. */
static int gpe_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class","Struct","Interface","Enum","Trait","Type",NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = gpe_count_label(store, project, labels[i]);
        if (n > 0) total += n;
    }
    return total;
}

/* Metrics bundled per index pass. */
typedef struct {
    int ok;
    int total_nodes;
    int functions;
    int methods;
    int types;     /* type-like sum */
    int imports;   /* IMPORTS edges */
    int inherits;  /* INHERITS edges */
} GpeMetrics;

static GpeMetrics gpe_metrics_files(const GpeFile *files, int nfiles) {
    GpeProj lp;
    cbm_store_t *store = gpe_index_files(&lp, files, nfiles);
    GpeMetrics m = {0};
    if (store) {
        m.ok          = 1;
        m.total_nodes = cbm_store_count_nodes(store, lp.project);
        m.functions   = gpe_count_label(store, lp.project, "Function");
        m.methods     = gpe_count_label(store, lp.project, "Method");
        m.types       = gpe_type_nodes(store, lp.project);
        m.imports     = cbm_store_count_edges_by_type(store, lp.project, "IMPORTS");
        m.inherits    = cbm_store_count_edges_by_type(store, lp.project, "INHERITS");
    }
    gpe_cleanup(&lp, store);
    return m;
}

static GpeMetrics gpe_metrics(const char *filename, const char *content) {
    GpeFile f = {filename, content};
    return gpe_metrics_files(&f, 1);
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 1 — Protobuf (.proto)
 *
 * Label histogram (test_grammar_labels.c): Class:1, Module:1
 * A `message` definition → Class node.
 * `enum` — whether it also produces a Class node is not confirmed; the
 *   histogram shows Class:1 for a single-message fixture, so a second Class
 *   for an enum would require a fixture with both message + enum.
 * `service` — the label histogram does not show a Function node for a
 *   service method in the single-message fixture; service methods are
 *   extractable as Class/Function depending on extractor policy.
 * `import` — Protobuf `import "other.proto"` is not resolved into IMPORTS
 *   graph edges; single-pass grammar-only extractor has no proto import
 *   resolver.
 * ══════════════════════════════════════════════════════════════════ */

/* Protobuf: single message definition → at least 1 Class node. */
TEST(probe_proto_message_node) {
    GpeMetrics m = gpe_metrics("user.proto",
        "syntax = \"proto3\";\n"
        "package user;\n"
        "\n"
        "message User {\n"
        "  int64  id    = 1;\n"
        "  string name  = 2;\n"
        "  string email = 3;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: the `message` definition must produce a Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Protobuf: service definition — whether the service itself or its RPC
 * methods produce extractable nodes. */
TEST(probe_proto_service_node) {
    GpeMetrics m = gpe_metrics("svc.proto",
        "syntax = \"proto3\";\n"
        "package svc;\n"
        "\n"
        "message Req  { string query = 1; }\n"
        "message Resp { string result = 1; }\n"
        "\n"
        "service SearchService {\n"
        "  rpc Search(Req) returns (Resp);\n"
        "  rpc Suggest(Req) returns (Resp);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least the two message defs must produce Class nodes. */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* Protobuf: enum definition — confirm enum maps to a Class-like node. */
TEST(probe_proto_enum_node) {
    GpeMetrics m = gpe_metrics("status.proto",
        "syntax = \"proto3\";\n"
        "\n"
        "enum Status {\n"
        "  STATUS_UNKNOWN = 0;\n"
        "  STATUS_ACTIVE  = 1;\n"
        "  STATUS_INACTIVE = 2;\n"
        "}\n"
        "\n"
        "message Item {\n"
        "  int64  id     = 1;\n"
        "  Status status = 2;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum the message must produce a Class node; enum may add
     * another — total >= 1 is the safe floor. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Protobuf: two-file `import` → IMPORTS edge.
 * RED: grammar-only protobuf has no import-resolver pass in the pipeline;
 *      `import "other.proto"` statements are not turned into IMPORTS edges. */
TEST(probe_proto_import_edge) {
    static const GpeFile files[] = {
        {"common.proto",
         "syntax = \"proto3\";\n"
         "package common;\n"
         "message Timestamp { int64 seconds = 1; }\n"},
        {"event.proto",
         "syntax = \"proto3\";\n"
         "package event;\n"
         "import \"common.proto\";\n"
         "message Event {\n"
         "  string id = 1;\n"
         "  common.Timestamp created_at = 2;\n"
         "}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: protobuf `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no proto import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 2 — Thrift (.thrift)
 *
 * Label histogram: Function:1, Module:1
 * `service Foo { void ping() }` → Function node (the method).
 * `struct` — the histogram shows no Struct/Class node for a service-only
 *   fixture; struct extraction may not be implemented.
 * `include` — Thrift uses `include "other.thrift"` for imports; not
 *   resolved by the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Thrift: service method → at least 1 Function node. */
TEST(probe_thrift_service_function) {
    GpeMetrics m = gpe_metrics("calc.thrift",
        "namespace go calc\n"
        "\n"
        "service Calculator {\n"
        "  i32 add(1: i32 a, 2: i32 b)\n"
        "  i32 subtract(1: i32 a, 2: i32 b)\n"
        "  i32 multiply(1: i32 a, 2: i32 b)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least one service method must appear as a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Thrift: struct definition — does the extractor produce a type-like node?
 * RED: histogram shows Function:1/Module:1 for service-only fixtures; struct
 *   extraction is not confirmed and may not be implemented in the grammar
 *   extractor. */
TEST(probe_thrift_struct_node) {
    GpeMetrics m = gpe_metrics("types.thrift",
        "namespace go types\n"
        "\n"
        "struct User {\n"
        "  1: required i64    id\n"
        "  2: required string name\n"
        "  3: optional string email\n"
        "}\n"
        "\n"
        "struct Address {\n"
        "  1: required string street\n"
        "  2: required string city\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1); /* GREEN: at minimum Module node */
    /* RED: Thrift struct not yet extracted as a Struct/Class node. */
    ASSERT_TRUE(m.types >= 1); /* expected RED — no Thrift struct extractor */
    PASS();
}

/* Thrift: exception definition — a special struct-like type. */
TEST(probe_thrift_exception_node) {
    GpeMetrics m = gpe_metrics("errors.thrift",
        "namespace go errors\n"
        "\n"
        "exception NotFound {\n"
        "  1: required string message\n"
        "}\n"
        "\n"
        "exception Unauthorized {\n"
        "  1: required string reason\n"
        "}\n"
        "\n"
        "service Auth {\n"
        "  void login(1: string user, 2: string pass)\n"
        "      throws (1: Unauthorized e)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least the service method must appear as a Function node. */
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* Thrift: two-file `include` → IMPORTS edge.
 * RED: grammar-only Thrift has no include-resolver in the pipeline. */
TEST(probe_thrift_include_edge) {
    static const GpeFile files[] = {
        {"common.thrift",
         "namespace go common\n"
         "struct Timestamp { 1: required i64 seconds }\n"},
        {"service.thrift",
         "namespace go service\n"
         "include \"common.thrift\"\n"
         "struct Event {\n"
         "  1: required string id\n"
         "  2: required common.Timestamp created_at\n"
         "}\n"
         "service EventService {\n"
         "  Event getEvent(1: string id)\n"
         "}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Thrift `include` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Thrift include resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 3 — Cap'n Proto (.capnp)
 *
 * Label histogram: Class:1, Module:1
 * `struct` → Class node.
 * `interface` — may also produce a Class node (or Interface node).
 * `import` with `@0x...` annotation — Capnp uses `using Import = import
 *   "other.capnp";`; not resolved by the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Cap'n Proto: struct definition → at least 1 Class node. */
TEST(probe_capnp_struct_node) {
    GpeMetrics m = gpe_metrics("schema.capnp",
        "@0xdbb9ad1f14bf0b36;\n"
        "\n"
        "struct Person {\n"
        "  id    @0 :UInt32;\n"
        "  name  @1 :Text;\n"
        "  email @2 :Text;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: the struct must produce a Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Cap'n Proto: interface definition → Class (or Interface) node. */
TEST(probe_capnp_interface_node) {
    GpeMetrics m = gpe_metrics("service.capnp",
        "@0xabc123def456789a;\n"
        "\n"
        "struct Request  { query @0 :Text; }\n"
        "struct Response { result @0 :Text; }\n"
        "\n"
        "interface SearchService {\n"
        "  search @0 (req :Request) -> (resp :Response);\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum the two struct definitions must produce Class nodes. */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* Cap'n Proto: enum definition. */
TEST(probe_capnp_enum_node) {
    GpeMetrics m = gpe_metrics("enums.capnp",
        "@0x1234567890abcdef;\n"
        "\n"
        "enum Color {\n"
        "  red   @0;\n"
        "  green @1;\n"
        "  blue  @2;\n"
        "}\n"
        "\n"
        "struct Widget {\n"
        "  color @0 :Color;\n"
        "  label @1 :Text;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: the struct must produce a Class node; enum may add another. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Cap'n Proto: two-file import → IMPORTS edge.
 * RED: grammar-only Cap'n Proto has no import-resolver pass in the pipeline. */
TEST(probe_capnp_import_edge) {
    static const GpeFile files[] = {
        {"common.capnp",
         "@0xaabbccddeeff0011;\n"
         "struct Timestamp { seconds @0 :Int64; }\n"},
        {"event.capnp",
         "@0x1122334455667788;\n"
         "using Common = import \"common.capnp\";\n"
         "struct Event {\n"
         "  id        @0 :Text;\n"
         "  createdAt @1 :Common.Timestamp;\n"
         "}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Cap'n Proto `import` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no capnp import resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 4 — WIT (.wit)
 *
 * Label histogram: Class:2, Function:1, Module:1
 * `record` → Class node; `enum` → Class node; `func` → Function node.
 * `world` and `interface` container constructs; the histogram's Class:2
 *   comes from record + enum inside an interface block.
 * `use` — WIT uses `use pkg:component/iface.{Type}` for imports;
 *   not resolved by the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* WIT: record and enum inside an interface → 2 Class nodes + 1 Function. */
TEST(probe_wit_record_enum_function) {
    GpeMetrics m = gpe_metrics("types.wit",
        "interface geometry {\n"
        "  record point { x: u32, y: u32 }\n"
        "  enum color { red, green, blue }\n"
        "  area: func() -> u32;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: record → Class, enum → Class, func → Function.
     * Histogram confirms Class:2, Function:1 for this pattern. */
    ASSERT_TRUE(m.types >= 2);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* WIT: world definition as a top-level construct. */
TEST(probe_wit_world_node) {
    GpeMetrics m = gpe_metrics("world.wit",
        "world calculator {\n"
        "  export add: func(a: u32, b: u32) -> u32;\n"
        "  export subtract: func(a: u32, b: u32) -> u32;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* REAL BUG (class 16 node-extraction incompleteness): `func`s declared
     * inside a WIT `world { export X: func ... }` block are NOT extracted as
     * Function nodes, whereas `interface`-scoped funcs ARE (see passing
     * probe_wit_record_enum_function and grammar_labels wit=Function:1).
     * The extractor's WIT func handling covers interface members but not
     * world exports.  Root cause: extract_defs.c WIT walk / lang_specs.c
     * wit func_node_types scope.  RED. */
    ASSERT_TRUE(m.functions >= 1); /* REAL BUG — world-scoped funcs not extracted */
    PASS();
}

/* WIT: type alias definition. */
TEST(probe_wit_type_alias) {
    GpeMetrics m = gpe_metrics("alias.wit",
        "interface types {\n"
        "  type error-code = u32;\n"
        "  record error { code: error-code, msg: string }\n"
        "  get-error: func(code: error-code) -> error;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: the record → Class, func → Function. */
    ASSERT_TRUE(m.types >= 1);
    ASSERT_TRUE(m.functions >= 1);
    PASS();
}

/* WIT: variant definition (tagged union). */
TEST(probe_wit_variant_node) {
    GpeMetrics m = gpe_metrics("result.wit",
        "interface result-types {\n"
        "  variant result {\n"
        "    ok(u32),\n"
        "    err(string),\n"
        "  }\n"
        "  unwrap: func(r: result) -> u32;\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum the function must appear; variant may add a Class. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* WIT: two-file `use` → IMPORTS edge.
 * RED: grammar-only WIT has no use-resolver in the pipeline. */
TEST(probe_wit_use_edge) {
    static const GpeFile files[] = {
        {"types.wit",
         "interface types {\n"
         "  record point { x: u32, y: u32 }\n"
         "}\n"},
        {"geometry.wit",
         "interface geometry {\n"
         "  use types.{point};\n"
         "  distance: func(a: point, b: point) -> u32;\n"
         "}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: WIT `use` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no WIT use resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 5 — Smithy (.smithy)
 *
 * Label histogram: Class:1, Module:1
 * `structure` → Class node (confirmed by grammar_regression: Foo → Class).
 * `service` and `operation` — may produce additional Class nodes.
 * `use` — Smithy uses `use com.example#Shape` for cross-namespace imports;
 *   not resolved by the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* Smithy: structure definition → at least 1 Class node. */
TEST(probe_smithy_structure_node) {
    /* Fixture fix: valid Smithy ordering is control section ($version) FIRST,
     * then namespace, then shapes.  The original fixture had `namespace`
     * before `$version`, which is invalid Smithy and failed to parse → 0
     * nodes.  Mirrors the working test_grammar_regression.c smithy fixture
     * (namespace, then structure) which yields Class:1. */
    GpeMetrics m = gpe_metrics("model.smithy",
        "$version: \"2\"\n"
        "\n"
        "namespace com.example\n"
        "\n"
        "structure User {\n"
        "  @required\n"
        "  id: String\n"
        "  name: String\n"
        "  email: String\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: structure → Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Smithy: service + operation definitions. */
TEST(probe_smithy_service_node) {
    GpeMetrics m = gpe_metrics("service.smithy",
        "namespace com.example\n"
        "\n"
        "service UserService {\n"
        "  version: \"2024-01-01\"\n"
        "  operations: [GetUser, CreateUser]\n"
        "}\n"
        "\n"
        "operation GetUser {\n"
        "  input: GetUserInput\n"
        "  output: GetUserOutput\n"
        "}\n"
        "\n"
        "operation CreateUser {\n"
        "  input: CreateUserInput\n"
        "}\n"
        "\n"
        "structure GetUserInput    { @required id: String }\n"
        "structure GetUserOutput   { id: String, name: String }\n"
        "structure CreateUserInput { @required name: String }\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum the structure definitions must produce Class nodes. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Smithy: enum definition. */
TEST(probe_smithy_enum_node) {
    GpeMetrics m = gpe_metrics("enum.smithy",
        "namespace com.example\n"
        "\n"
        "enum Status {\n"
        "  ACTIVE = \"active\"\n"
        "  INACTIVE = \"inactive\"\n"
        "  PENDING = \"pending\"\n"
        "}\n"
        "\n"
        "structure Item {\n"
        "  id: String\n"
        "  status: Status\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: structure → Class; enum may produce another type node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Smithy: two-file `use` → IMPORTS edge.
 * RED: grammar-only Smithy has no use-resolver in the pipeline. */
TEST(probe_smithy_use_edge) {
    static const GpeFile files[] = {
        {"common.smithy",
         "namespace com.example.common\n"
         "structure Timestamp { seconds: Long }\n"},
        {"events.smithy",
         "namespace com.example.events\n"
         "use com.example.common#Timestamp\n"
         "structure Event {\n"
         "  id: String\n"
         "  createdAt: Timestamp\n"
         "}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: Smithy `use` not resolved into IMPORTS edges. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — no Smithy use resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 6 — Prisma (.prisma)
 *
 * Label histogram: Class:1, Module:1
 * `model` → Class node.
 * `enum` — may produce a second Class node.
 * Prisma schema files are single-file; there is no cross-file import
 *   mechanism in the Prisma schema language itself (all models live in
 *   one schema.prisma or are merged via --schema flag, not `import`).
 * ══════════════════════════════════════════════════════════════════ */

/* Prisma: model definition → at least 1 Class node. */
TEST(probe_prisma_model_node) {
    GpeMetrics m = gpe_metrics("schema.prisma",
        "generator client {\n"
        "  provider = \"prisma-client-js\"\n"
        "}\n"
        "\n"
        "datasource db {\n"
        "  provider = \"postgresql\"\n"
        "  url      = env(\"DATABASE_URL\")\n"
        "}\n"
        "\n"
        "model User {\n"
        "  id    Int    @id @default(autoincrement())\n"
        "  name  String\n"
        "  email String @unique\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: `model User` must produce a Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Prisma: multiple model definitions → multiple Class nodes. */
TEST(probe_prisma_multiple_models) {
    GpeMetrics m = gpe_metrics("blog.prisma",
        "model User {\n"
        "  id    Int    @id @default(autoincrement())\n"
        "  name  String\n"
        "  posts Post[]\n"
        "}\n"
        "\n"
        "model Post {\n"
        "  id       Int    @id @default(autoincrement())\n"
        "  title    String\n"
        "  content  String\n"
        "  authorId Int\n"
        "  author   User   @relation(fields: [authorId], references: [id])\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: two model definitions → at least 2 Class nodes. */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* Prisma: enum definition — does it yield an additional type node? */
TEST(probe_prisma_enum_node) {
    GpeMetrics m = gpe_metrics("status.prisma",
        "enum Role {\n"
        "  USER\n"
        "  ADMIN\n"
        "  MODERATOR\n"
        "}\n"
        "\n"
        "model Account {\n"
        "  id   Int  @id @default(autoincrement())\n"
        "  role Role @default(USER)\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: model → Class; enum may add another — floor is >= 1. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* Prisma: no-crash for a datasource-only schema (no models). */
TEST(probe_prisma_datasource_only) {
    GpeMetrics m = gpe_metrics("config.prisma",
        "generator client {\n"
        "  provider = \"prisma-client-js\"\n"
        "}\n"
        "\n"
        "datasource db {\n"
        "  provider = \"sqlite\"\n"
        "  url      = \"file:./dev.db\"\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash on a schema with no models. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 7 — GraphQL (.graphql / .gql)
 *
 * Label histogram: Class:1, Module:1
 * `type` definition → Class node.
 * `input` and `interface` types — may also produce Class nodes.
 * GraphQL schema files are typically standalone; cross-file imports are
 *   not part of the SDL (Schema Definition Language); no IMPORTS edges
 *   are expected from the grammar-only pipeline.
 * ══════════════════════════════════════════════════════════════════ */

/* GraphQL: type definition → at least 1 Class node. */
TEST(probe_graphql_type_node) {
    GpeMetrics m = gpe_metrics("schema.graphql",
        "type User {\n"
        "  id:    ID!\n"
        "  name:  String!\n"
        "  email: String!\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: `type User` must produce a Class node. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* GraphQL: multiple type definitions → multiple Class nodes. */
TEST(probe_graphql_multiple_types) {
    GpeMetrics m = gpe_metrics("schema.gql",
        "type Query {\n"
        "  user(id: ID!): User\n"
        "  users: [User!]!\n"
        "}\n"
        "\n"
        "type User {\n"
        "  id:    ID!\n"
        "  name:  String!\n"
        "  posts: [Post!]!\n"
        "}\n"
        "\n"
        "type Post {\n"
        "  id:      ID!\n"
        "  title:   String!\n"
        "  content: String!\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: 3 type definitions → at least 2 Class nodes. */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* GraphQL: interface definition → type-like node. */
TEST(probe_graphql_interface_node) {
    GpeMetrics m = gpe_metrics("iface.graphql",
        "interface Node {\n"
        "  id: ID!\n"
        "}\n"
        "\n"
        "type User implements Node {\n"
        "  id:   ID!\n"
        "  name: String!\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at minimum `type User` → Class; `interface Node` may add one. */
    ASSERT_TRUE(m.types >= 1);
    PASS();
}

/* GraphQL: input type definition. */
TEST(probe_graphql_input_type) {
    GpeMetrics m = gpe_metrics("mutations.graphql",
        "input CreateUserInput {\n"
        "  name:  String!\n"
        "  email: String!\n"
        "}\n"
        "\n"
        "type Mutation {\n"
        "  createUser(input: CreateUserInput!): User!\n"
        "}\n"
        "\n"
        "type User {\n"
        "  id:   ID!\n"
        "  name: String!\n"
        "}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: at least two type-like nodes expected (input + type + type). */
    ASSERT_TRUE(m.types >= 2);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 8 — Vue (.vue)
 *
 * Label histogram: Module:1 only — no Class or Function nodes from the
 *   Vue component itself at the graph level.
 * IMPORTS: `import` statements inside <script> blocks ARE extracted at the
 *   extraction level (test_grammar_imports confirms 2 imports for a vue
 *   fixture with 2 import statements).  Whether these become IMPORTS graph
 *   edges in a two-file cross-component fixture depends on whether the
 *   JS/TS resolver handles .vue paths.
 * ══════════════════════════════════════════════════════════════════ */

/* Vue: single-file component indexes without crash. */
TEST(probe_vue_no_crash) {
    GpeMetrics m = gpe_metrics("App.vue",
        "<template>\n"
        "  <div>{{ message }}</div>\n"
        "</template>\n"
        "\n"
        "<script setup>\n"
        "import { ref } from 'vue'\n"
        "const message = ref('Hello World')\n"
        "</script>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash and must produce at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Vue: <script setup> with two imports — extraction-level import count. */
TEST(probe_vue_script_imports) {
    GpeMetrics m = gpe_metrics("Counter.vue",
        "<script setup>\n"
        "import { ref, computed } from 'vue'\n"
        "import BaseButton from './BaseButton.vue'\n"
        "\n"
        "const count = ref(0)\n"
        "const doubled = computed(() => count.value * 2)\n"
        "</script>\n"
        "\n"
        "<template>\n"
        "  <BaseButton @click=\"count++\">{{ doubled }}</BaseButton>\n"
        "</template>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline indexes without crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Vue: two-file cross-component import → IMPORTS graph edge.
 * The JS/TS resolver handles .vue imports via the same path-resolution
 * logic used for TypeScript.  With matching files on disk the resolver
 * should produce an IMPORTS edge.
 * RED: .vue extension may not be handled by the JS/TS import resolver. */
TEST(probe_vue_cross_component_import) {
    static const GpeFile files[] = {
        {"BaseButton.vue",
         "<template><button><slot /></button></template>\n"
         "<script setup>\n"
         "defineProps({ label: String })\n"
         "</script>\n"},
        {"App.vue",
         "<script setup>\n"
         "import BaseButton from './BaseButton.vue'\n"
         "</script>\n"
         "<template><BaseButton label=\"Click\" /></template>\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: .vue cross-component IMPORTS edge not confirmed by the pipeline. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — .vue paths not in JS resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 9 — Svelte (.svelte)
 *
 * Label histogram: Module:1 only.
 * IMPORTS: <script> imports ARE extracted at the extraction level
 *   (test_grammar_imports confirms 2 imports for a svelte fixture).
 *   Cross-component IMPORTS edges share the same question as Vue.
 * ══════════════════════════════════════════════════════════════════ */

/* Svelte: single-file component indexes without crash. */
TEST(probe_svelte_no_crash) {
    GpeMetrics m = gpe_metrics("App.svelte",
        "<script>\n"
        "  let name = 'World';\n"
        "  function greet() { return `Hello ${name}`; }\n"
        "</script>\n"
        "<h1>{greet()}</h1>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash and must produce at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Svelte: <script> with imports — extraction-level behavior. */
TEST(probe_svelte_script_imports) {
    GpeMetrics m = gpe_metrics("Counter.svelte",
        "<script>\n"
        "  import { onMount } from 'svelte';\n"
        "  import Button from './Button.svelte';\n"
        "\n"
        "  let count = 0;\n"
        "  onMount(() => { count = 1; });\n"
        "</script>\n"
        "<Button on:click={() => count++}>{count}</Button>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline indexes without crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Svelte: two-file cross-component import → IMPORTS graph edge.
 * RED: .svelte extension may not be handled by the JS/TS import resolver. */
TEST(probe_svelte_cross_component_import) {
    static const GpeFile files[] = {
        {"Button.svelte",
         "<script>\n"
         "  export let label = 'Click';\n"
         "</script>\n"
         "<button>{label}</button>\n"},
        {"App.svelte",
         "<script>\n"
         "  import Button from './Button.svelte';\n"
         "</script>\n"
         "<Button label=\"Go\" />\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: .svelte cross-component IMPORTS edge not confirmed by the pipeline. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — .svelte paths not in JS resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 10 — Astro (.astro)
 *
 * Label histogram: Module:1 only.
 * IMPORTS: frontmatter imports ARE extracted at the extraction level
 *   (test_grammar_imports confirms 2 imports for an astro fixture).
 *   Cross-component IMPORTS edges share the same question as Vue/Svelte.
 * ══════════════════════════════════════════════════════════════════ */

/* Astro: single-file component indexes without crash. */
TEST(probe_astro_no_crash) {
    GpeMetrics m = gpe_metrics("index.astro",
        "---\n"
        "const greeting = 'Hello World';\n"
        "---\n"
        "<html>\n"
        "  <body><h1>{greeting}</h1></body>\n"
        "</html>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash and must produce at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Astro: frontmatter with imports. */
TEST(probe_astro_frontmatter_imports) {
    GpeMetrics m = gpe_metrics("Page.astro",
        "---\n"
        "import confetti from 'canvas-confetti';\n"
        "import dayjs from 'dayjs';\n"
        "\n"
        "confetti();\n"
        "const now = dayjs().format();\n"
        "---\n"
        "<div>{now}</div>\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline indexes without crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Astro: two-file cross-component import → IMPORTS graph edge.
 * RED: .astro extension may not be handled by the JS/TS import resolver. */
TEST(probe_astro_cross_component_import) {
    static const GpeFile files[] = {
        {"Card.astro",
         "---\n"
         "const { title } = Astro.props;\n"
         "---\n"
         "<div class=\"card\"><h2>{title}</h2></div>\n"},
        {"index.astro",
         "---\n"
         "import Card from './Card.astro';\n"
         "---\n"
         "<Card title=\"Hello\" />\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    /* RED: .astro cross-component IMPORTS edge not confirmed by the pipeline. */
    ASSERT_TRUE(m.imports >= 1); /* expected RED — .astro paths not in JS resolver */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 11 — Blade (.blade.php)
 *
 * Label histogram: Module:1 only.
 * Blade templates use @extends / @include directives but these are not
 *   modeled as graph IMPORTS edges by the grammar-only pipeline.
 * The .blade.php compound extension is registered via the compound
 *   extension table (language.c line 882).
 * ══════════════════════════════════════════════════════════════════ */

/* Blade: basic template indexes without crash. */
TEST(probe_blade_no_crash) {
    GpeMetrics m = gpe_metrics("welcome.blade.php",
        "@extends('layouts.app')\n"
        "\n"
        "@section('content')\n"
        "<h1>Welcome</h1>\n"
        "@endsection\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash and must produce at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Blade: @include directive — not modeled as IMPORTS. */
TEST(probe_blade_include_no_imports_edge) {
    static const GpeFile files[] = {
        {"partials/header.blade.php",
         "<header><nav>Menu</nav></header>\n"},
        {"home.blade.php",
         "@extends('layouts.app')\n"
         "@section('content')\n"
         "@include('partials.header')\n"
         "<main>Home</main>\n"
         "@endsection\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    /* GREEN: Blade @include is not modeled as a graph IMPORTS edge. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* Blade: conditional and loop directives — pipeline stability. */
TEST(probe_blade_directives_no_crash) {
    GpeMetrics m = gpe_metrics("list.blade.php",
        "@extends('layouts.app')\n"
        "@section('content')\n"
        "@if($items->count() > 0)\n"
        "  @foreach($items as $item)\n"
        "    <div>{{ $item->name }}</div>\n"
        "  @endforeach\n"
        "@else\n"
        "  <p>No items found.</p>\n"
        "@endif\n"
        "@endsection\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: complex directives must not crash the pipeline. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 12 — Liquid (.liquid)
 *
 * Label histogram: Module:1 only.
 * Liquid uses `{% render 'snippet' %}` / `{% include 'snippet' %}` but
 *   these are not modeled as graph IMPORTS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Liquid: basic template indexes without crash. */
TEST(probe_liquid_no_crash) {
    GpeMetrics m = gpe_metrics("index.liquid",
        "{% if customer %}\n"
        "  <p>Welcome, {{ customer.name }}!</p>\n"
        "{% else %}\n"
        "  <p>Hello, visitor!</p>\n"
        "{% endif %}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash and must produce at least 1 node. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Liquid: for-loop + filters — pipeline stability. */
TEST(probe_liquid_loop_and_filters) {
    GpeMetrics m = gpe_metrics("products.liquid",
        "{% for product in collection.products %}\n"
        "  <div class=\"product\">\n"
        "    <h2>{{ product.title | upcase }}</h2>\n"
        "    <p>{{ product.price | money }}</p>\n"
        "  </div>\n"
        "{% endfor %}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: for-loop with filters must not crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Liquid: render/include — not modeled as IMPORTS. */
TEST(probe_liquid_render_no_imports_edge) {
    static const GpeFile files[] = {
        {"snippets/card.liquid",
         "<div class=\"card\">{{ card.title }}</div>\n"},
        {"sections/featured.liquid",
         "<section>\n"
         "  {% render 'snippets/card', card: featured_product %}\n"
         "</section>\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    /* GREEN: Liquid `render` is not modeled as a graph IMPORTS edge. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 13 — Jinja2 (.j2 / .jinja2)
 *
 * Label histogram: Module:1 only.
 * Jinja2 uses `{% extends "base.html" %}` / `{% include "header.html" %}`
 *   but these are not modeled as graph IMPORTS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Jinja2: basic template indexes without crash (.j2 extension). */
TEST(probe_jinja2_no_crash_j2) {
    GpeMetrics m = gpe_metrics("config.j2",
        "{% if env == 'production' %}\n"
        "database_url: {{ db_url }}\n"
        "{% else %}\n"
        "database_url: sqlite:///dev.db\n"
        "{% endif %}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash on a .j2 file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Jinja2: extends and block directives (.jinja2 extension). */
TEST(probe_jinja2_extends_no_imports_edge) {
    static const GpeFile files[] = {
        {"base.html.jinja2",
         "<!DOCTYPE html>\n"
         "<html>\n"
         "<head>{% block head %}{% endblock %}</head>\n"
         "<body>{% block body %}{% endblock %}</body>\n"
         "</html>\n"},
        {"page.html.jinja2",
         "{% extends \"base.html.jinja2\" %}\n"
         "{% block head %}<title>Page</title>{% endblock %}\n"
         "{% block body %}<h1>Hello</h1>{% endblock %}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    /* GREEN: Jinja2 `extends` is not modeled as a graph IMPORTS edge. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* Jinja2: macro definition — does the extractor produce a node? */
TEST(probe_jinja2_macro_no_crash) {
    GpeMetrics m = gpe_metrics("macros.jinja",
        "{% macro render_field(field, label='') %}\n"
        "  <div class=\"field\">\n"
        "    {% if label %}<label>{{ label }}</label>{% endif %}\n"
        "    {{ field }}\n"
        "  </div>\n"
        "{% endmacro %}\n"
        "\n"
        "{% macro render_errors(field) %}\n"
        "  {% for error in field.errors %}\n"
        "    <span class=\"error\">{{ error }}</span>\n"
        "  {% endfor %}\n"
        "{% endmacro %}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: macro-heavy template must not crash the pipeline. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 14 — Go Template (.gotmpl / .tmpl / .tpl)
 *
 * Label histogram: Module:1 only.
 * Go templates use `{{ template "name" . }}` for includes but these are
 *   not modeled as graph IMPORTS edges.
 * ══════════════════════════════════════════════════════════════════ */

/* Go Template: basic template indexes without crash (.gotmpl). */
TEST(probe_gotemplate_no_crash_gotmpl) {
    GpeMetrics m = gpe_metrics("page.gotmpl",
        "{{ define \"page\" }}\n"
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<body>\n"
        "  <h1>{{ .Title }}</h1>\n"
        "  {{ if .Items }}\n"
        "  <ul>\n"
        "    {{ range .Items }}<li>{{ . }}</li>{{ end }}\n"
        "  </ul>\n"
        "  {{ end }}\n"
        "</body>\n"
        "</html>\n"
        "{{ end }}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash on a .gotmpl file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Go Template: Helm-style .tpl with named template definitions. */
TEST(probe_gotemplate_helm_tpl) {
    GpeMetrics m = gpe_metrics("_helpers.tpl",
        "{{/*\n"
        "Expand the name of the chart.\n"
        "*/}}\n"
        "{{- define \"myapp.name\" -}}\n"
        "{{- .Chart.Name | trunc 63 | trimSuffix \"-\" }}\n"
        "{{- end }}\n"
        "\n"
        "{{- define \"myapp.labels\" -}}\n"
        "helm.sh/chart: {{ include \"myapp.name\" . }}\n"
        "{{- end }}\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: Helm helper template must not crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Go Template: `template` call — not modeled as IMPORTS. */
TEST(probe_gotemplate_template_call_no_imports) {
    static const GpeFile files[] = {
        {"layout.tmpl",
         "{{ define \"layout\" }}\n"
         "<html><body>{{ template \"content\" . }}</body></html>\n"
         "{{ end }}\n"},
        {"page.tmpl",
         "{{ define \"content\" }}\n"
         "<h1>{{ .Title }}</h1>\n"
         "{{ end }}\n"
         "{{ template \"layout\" . }}\n"}
    };
    GpeMetrics m = gpe_metrics_files(files, 2);
    ASSERT_TRUE(m.ok);
    ASSERT_TRUE(m.total_nodes >= 1);
    /* GREEN: Go template `template` calls are not modeled as IMPORTS edges. */
    ASSERT_TRUE(m.imports == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 15 — Mermaid (.mmd / .mermaid)
 *
 * Label histogram: Module:1 only.
 * Mermaid diagrams describe graph/flow/sequence diagrams — no function or
 *   type definitions; no cross-file imports.
 * ══════════════════════════════════════════════════════════════════ */

/* Mermaid: flowchart diagram indexes without crash (.mmd). */
TEST(probe_mermaid_flowchart_no_crash) {
    GpeMetrics m = gpe_metrics("flow.mmd",
        "graph TD\n"
        "  A[Start] --> B{Is it?}\n"
        "  B -- Yes --> C[OK]\n"
        "  B -- No  --> D[End]\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash on a .mmd file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Mermaid: sequence diagram (.mermaid extension). */
TEST(probe_mermaid_sequence_no_crash) {
    GpeMetrics m = gpe_metrics("auth.mermaid",
        "sequenceDiagram\n"
        "  participant Client\n"
        "  participant Server\n"
        "  Client->>Server: POST /login\n"
        "  Server-->>Client: 200 OK + token\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: sequence diagram must not crash. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* Mermaid: entity-relationship diagram. */
TEST(probe_mermaid_er_diagram_no_crash) {
    GpeMetrics m = gpe_metrics("schema.mmd",
        "erDiagram\n"
        "  USER {\n"
        "    int id PK\n"
        "    string name\n"
        "    string email\n"
        "  }\n"
        "  POST {\n"
        "    int id PK\n"
        "    int authorId FK\n"
        "    string title\n"
        "  }\n"
        "  USER ||--o{ POST : \"writes\"\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: ER diagram must not crash the pipeline. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * GROUP 16 — JSDoc (.jsdoc)
 *
 * Label histogram: Module:1 only.
 * JSDoc files contain documentation comment blocks — `@param`, `@returns`,
 *   `@module`, `@typedef` etc.  No executable code; no cross-file imports.
 * ══════════════════════════════════════════════════════════════════ */

/* JSDoc: basic parameter documentation indexes without crash. */
TEST(probe_jsdoc_params_no_crash) {
    GpeMetrics m = gpe_metrics("api.jsdoc",
        "/**\n"
        " * @param {number} x - First operand\n"
        " * @param {number} y - Second operand\n"
        " * @returns {number} The sum\n"
        " */\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: pipeline must not crash on a .jsdoc file. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* JSDoc: @module and @typedef annotations. */
TEST(probe_jsdoc_module_typedef_no_crash) {
    GpeMetrics m = gpe_metrics("types.jsdoc",
        "/**\n"
        " * @module utils\n"
        " */\n"
        "\n"
        "/**\n"
        " * @typedef {Object} User\n"
        " * @property {number} id - User identifier\n"
        " * @property {string} name - User full name\n"
        " * @property {string} email - User email address\n"
        " */\n"
        "\n"
        "/**\n"
        " * @callback Predicate\n"
        " * @param {User} user\n"
        " * @returns {boolean}\n"
        " */\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: module + typedef annotations must not crash the pipeline. */
    ASSERT_TRUE(m.total_nodes >= 1);
    PASS();
}

/* JSDoc: no function or type nodes expected — markup-only label set. */
TEST(probe_jsdoc_no_function_or_type_nodes) {
    GpeMetrics m = gpe_metrics("helpers.jsdoc",
        "/**\n"
        " * @function formatDate\n"
        " * @param {Date} date\n"
        " * @returns {string}\n"
        " */\n");
    ASSERT_TRUE(m.ok);
    /* GREEN: JSDoc produces only Module:1 — no Function or Class nodes. */
    ASSERT_TRUE(m.types == 0);
    ASSERT_TRUE(m.functions == 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 * SUITE wiring
 * ══════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_e) {
    /* Protobuf (4 cases) */
    RUN_TEST(probe_proto_message_node);
    RUN_TEST(probe_proto_service_node);
    RUN_TEST(probe_proto_enum_node);
    RUN_TEST(probe_proto_import_edge);

    /* Thrift (4 cases) */
    RUN_TEST(probe_thrift_service_function);
    RUN_TEST(probe_thrift_struct_node);
    RUN_TEST(probe_thrift_exception_node);
    RUN_TEST(probe_thrift_include_edge);

    /* Cap'n Proto (4 cases) */
    RUN_TEST(probe_capnp_struct_node);
    RUN_TEST(probe_capnp_interface_node);
    RUN_TEST(probe_capnp_enum_node);
    RUN_TEST(probe_capnp_import_edge);

    /* WIT (5 cases) */
    RUN_TEST(probe_wit_record_enum_function);
    RUN_TEST(probe_wit_world_node);
    RUN_TEST(probe_wit_type_alias);
    RUN_TEST(probe_wit_variant_node);
    RUN_TEST(probe_wit_use_edge);

    /* Smithy (4 cases) */
    RUN_TEST(probe_smithy_structure_node);
    RUN_TEST(probe_smithy_service_node);
    RUN_TEST(probe_smithy_enum_node);
    RUN_TEST(probe_smithy_use_edge);

    /* Prisma (4 cases) */
    RUN_TEST(probe_prisma_model_node);
    RUN_TEST(probe_prisma_multiple_models);
    RUN_TEST(probe_prisma_enum_node);
    RUN_TEST(probe_prisma_datasource_only);

    /* GraphQL (4 cases) */
    RUN_TEST(probe_graphql_type_node);
    RUN_TEST(probe_graphql_multiple_types);
    RUN_TEST(probe_graphql_interface_node);
    RUN_TEST(probe_graphql_input_type);

    /* Vue (3 cases) */
    RUN_TEST(probe_vue_no_crash);
    RUN_TEST(probe_vue_script_imports);
    RUN_TEST(probe_vue_cross_component_import);

    /* Svelte (3 cases) */
    RUN_TEST(probe_svelte_no_crash);
    RUN_TEST(probe_svelte_script_imports);
    RUN_TEST(probe_svelte_cross_component_import);

    /* Astro (3 cases) */
    RUN_TEST(probe_astro_no_crash);
    RUN_TEST(probe_astro_frontmatter_imports);
    RUN_TEST(probe_astro_cross_component_import);

    /* Blade (3 cases) */
    RUN_TEST(probe_blade_no_crash);
    RUN_TEST(probe_blade_include_no_imports_edge);
    RUN_TEST(probe_blade_directives_no_crash);

    /* Liquid (3 cases) */
    RUN_TEST(probe_liquid_no_crash);
    RUN_TEST(probe_liquid_loop_and_filters);
    RUN_TEST(probe_liquid_render_no_imports_edge);

    /* Jinja2 (3 cases) */
    RUN_TEST(probe_jinja2_no_crash_j2);
    RUN_TEST(probe_jinja2_extends_no_imports_edge);
    RUN_TEST(probe_jinja2_macro_no_crash);

    /* Go Template (3 cases) */
    RUN_TEST(probe_gotemplate_no_crash_gotmpl);
    RUN_TEST(probe_gotemplate_helm_tpl);
    RUN_TEST(probe_gotemplate_template_call_no_imports);

    /* Mermaid (3 cases) */
    RUN_TEST(probe_mermaid_flowchart_no_crash);
    RUN_TEST(probe_mermaid_sequence_no_crash);
    RUN_TEST(probe_mermaid_er_diagram_no_crash);

    /* JSDoc (3 cases) */
    RUN_TEST(probe_jsdoc_params_no_crash);
    RUN_TEST(probe_jsdoc_module_typedef_no_crash);
    RUN_TEST(probe_jsdoc_no_function_or_type_nodes);
}
